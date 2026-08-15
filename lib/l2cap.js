const { Duplex } = require('streamx')
const b4a = require('b4a')
const safetyCatch = require('safety-catch')

const { TYPE_CLOSE, SID_LEN, encodeFrame } = require('./frames')

const OPEN_TIMEOUT = 3000

// Byte pipe over an L2CAP channel — the GattStream-shaped wrapper so the
// transport can treat both pipes identically. The channel is already a
// reliable ordered duplex with credit-based flow control, so no framing or
// fragmentation is needed.
class L2CapStream extends Duplex {
  constructor(channel, { onclose } = {}) {
    super()
    this.channel = channel
    this._onclose = onclose || null

    channel.on('data', (data) => {
      // propagate read backpressure: hold the channel while our buffer is full
      if (!this.push(b4a.from(data))) channel.pause()
    })
    channel.on('end', () => this.push(null))
    channel.on('error', () => this.destroy())
    channel.on('close', () => this.destroy())
  }

  _read(cb) {
    this.channel.resume()
    cb(null)
  }

  _write(chunk, cb) {
    if (this.channel.write(chunk)) cb(null)
    else this.channel.once('drain', () => cb(null))
  }

  receive(buffer) {
    this.push(buffer)
  }

  remoteEnd() {
    this.push(null)
  }

  _destroy(cb) {
    const onclose = this._onclose
    this._onclose = null
    if (onclose) {
      try {
        onclose()
      } catch {
        // teardown is best-effort
      }
    }
    try {
      this.channel.destroy()
    } catch {
      // channel may already be gone
    }
    cb(null)
  }
}

// Accumulates the 8-byte session-id preamble from a fresh server-side channel,
// then hands (sid, leftover) to the callback exactly once. Owns the channel's
// data events until then so no bytes race past the switchover.
function readSidPreamble(channel, onSid, timeout) {
  let buf = b4a.alloc(0)
  let done = false

  const finish = (sid, leftover) => {
    if (done) return
    done = true
    clearTimeout(timer)
    channel.removeListener('data', onData)
    onSid(sid, leftover)
  }

  const onData = (data) => {
    buf = b4a.concat([buf, b4a.from(data)])
    if (buf.byteLength >= SID_LEN) finish(buf.subarray(0, SID_LEN), buf.subarray(SID_LEN))
  }

  const timer = setTimeout(() => finish(null, null), timeout)
  if (timer.unref) timer.unref()
  channel.on('data', onData)
}

/**
 * The L2CAP channel machinery: publishes one channel per server, opens
 * channels from the central under a deadline (the original macOS→iOS failure
 * hangs forever with no error), and matches incoming channels to sessions via
 * an 8-byte sid preamble. A failed open aborts the dial — the transport's
 * cooldown/redial cycle is the retry loop.
 */
module.exports = class L2CapPipe {
  constructor(transport, { timeout } = {}) {
    this.transport = transport
    this.psm = null
    this._timeout = timeout ?? OPEN_TIMEOUT
  }

  // ─── server (peripheral) side ─────────────────────────────────────────────

  publish() {
    const server = this.transport.server
    if (this.psm !== null || typeof server.publishChannel !== 'function') return
    try {
      // unencrypted: Noise on top provides the crypto; encryption here would
      // demand BLE pairing and stall centrals that never trigger the dialog
      server.publishChannel({})
    } catch (err) {
      safetyCatch(err)
    }
  }

  // fresh listener, fresh psm — the next HELLO advertises it
  cycle() {
    this.unpublish()
    this.publish()
  }

  // Retract the published channel on suspend — a listener that lived through
  // destroyed channels wedges, and the psm rides HELLO so a fresh one on
  // resume costs nothing.
  unpublish() {
    const server = this.transport.server
    if (this.psm === null) return
    if (typeof server?.unpublishChannel === 'function') {
      try {
        server.unpublishChannel(this.psm)
      } catch (err) {
        safetyCatch(err)
      }
    }
    this.psm = null
  }

  // Incoming L2CAP channel: the central writes its 8-byte session id first,
  // matching the channel to the session negotiated over GATT.
  acceptServer(channel) {
    const transport = this.transport
    transport._log('server l2cap channel open, awaiting sid')
    readSidPreamble(
      channel,
      (sid, leftover) => {
        const sidHex = sid !== null ? b4a.toString(sid, 'hex') : null
        const session = sidHex !== null ? transport._sessions.get(sidHex) : undefined
        if (!session || session.stream) {
          transport._log('server l2cap channel unmatched', sidHex ?? 'timeout')
          try {
            channel.destroy()
          } catch (err) {
            safetyCatch(err)
          }
          return
        }
        if (session.pipeTimer) clearTimeout(session.pipeTimer)
        session.pipeTimer = null
        session.stream = new L2CapStream(channel, {
          onclose: () => transport._closeServerSession(sidHex, session.sid)
        })
        session.conn = transport._onChannel(session.stream, false, null)
        if (leftover.byteLength) session.stream.receive(leftover)
        transport._log('server pipe: l2cap', sidHex)
      },
      this._timeout
    )
  }

  // ─── central side ─────────────────────────────────────────────────────────

  // One open attempt under a deadline; failure aborts the dial and the
  // transport's cooldown/redial cycle tries again.
  async openCentral(peripheral, sess, hello) {
    const psm = hello.psm
    const transport = this.transport
    const gone = () =>
      peripheral._session !== sess || transport.closing || transport.closed || transport._suspended
    sess.upgrading = true
    try {
      const started = Date.now()
      transport._log(`l2cap open psm=${psm}`, peripheral.id)
      const channel = await this._openChannel(peripheral, psm)
      if (gone()) {
        if (channel) {
          try {
            channel.destroy()
          } catch (err) {
            safetyCatch(err)
          }
        }
        return
      }
      if (channel) {
        transport._log(`l2cap open ok in ${Date.now() - started}ms`, peripheral.id)
        this._bindCentral(peripheral, sess, channel)
        return
      }
      transport._log(`l2cap open failed after ${Date.now() - started}ms`, peripheral.id)
      transport._abortDial(peripheral, 'l2cap-failed')
    } finally {
      sess.upgrading = false
    }
  }

  _openChannel(peripheral, psm) {
    return new Promise((resolve) => {
      let done = false
      const finish = (channel) => {
        if (done) {
          if (channel) {
            try {
              channel.destroy()
            } catch (err) {
              safetyCatch(err)
            }
          }
          return
        }
        done = true
        clearTimeout(timer)
        peripheral.removeListener('channelOpen', finish)
        resolve(channel || null)
      }
      const timer = setTimeout(() => finish(null), this._timeout)
      if (timer.unref) timer.unref()
      peripheral.once('channelOpen', finish)
      try {
        peripheral.openL2CAPChannel(psm)
      } catch (err) {
        safetyCatch(err)
        finish(null)
      }
    })
  }

  _bindCentral(peripheral, sess, channel) {
    const transport = this.transport
    // sid preamble first: the server matches the channel to the session with it
    try {
      channel.write(sess.sid)
    } catch (err) {
      safetyCatch(err)
      transport._log('l2cap preamble write failed', peripheral.id)
      transport._abortDial(peripheral, 'l2cap-failed')
      return
    }
    const stream = new L2CapStream(channel, {
      onclose: () => {
        transport._gatt.send(peripheral, encodeFrame(TYPE_CLOSE, sess.sid), true).catch(safetyCatch)
        // iOS reuses peripheral objects across reconnects — stale refs here
        // would make the next HELLO look like a live session and get dropped
        peripheral._stream = null
        peripheral._session = null
        const d = transport._devices.get(peripheral.id)
        if (d) d.peripheral = null
        try {
          transport.central.disconnect(peripheral)
        } catch (err) {
          safetyCatch(err)
        }
      }
    })
    peripheral._stream = stream
    transport._clearDial(peripheral.id)
    peripheral._conn = transport._onChannel(stream, true, peripheral.id)
    transport._log('central pipe: l2cap', peripheral.id)
  }
}
