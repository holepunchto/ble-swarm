const { Duplex } = require('streamx')
const b4a = require('b4a')
const safetyCatch = require('safety-catch')

const { SID_LEN } = require('./frames')

const OPEN_TIMEOUT = 3000

// Byte pipe over an L2CAP channel — the GattStream-shaped wrapper so the
// transport can treat both pipes identically. The channel is already a
// reliable ordered duplex with credit-based flow control, so no framing or
// fragmentation is needed.
class L2CapStream extends Duplex {
  constructor(channel) {
    super()
    this.channel = channel

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
    try {
      this.channel.destroy()
    } catch {
      // channel may already be gone
    }
    cb(null)
  }
}

// Accumulates the 8-byte session-id preamble from a fresh server-side channel.
// Resolves { id, rest } — rest being any bytes delivered past the id (the
// channel is a byte stream; the first Noise bytes may arrive glued to it).
// Owns the channel's data events until then so no bytes race past the
// switchover. Timeout resolves { id: null, rest: null }.
function readIdPreamble(channel, timeout) {
  return new Promise((resolve) => {
    let buf = b4a.alloc(0)

    const finish = (id, rest) => {
      clearTimeout(timer)
      channel.removeListener('data', onData)
      resolve({ id, rest })
    }

    const onData = (data) => {
      buf = b4a.concat([buf, b4a.from(data)])
      if (buf.byteLength >= SID_LEN) {
        finish(b4a.toString(buf.subarray(0, SID_LEN), 'hex'), buf.subarray(SID_LEN))
      }
    }

    const timer = setTimeout(() => finish(null, null), timeout)
    if (timer.unref) timer.unref()
    channel.on('data', onData)
  })
}

/**
 * The L2CAP channel machinery: publishes one channel per server, opens
 * channels from the central under a deadline (the original macOS→iOS failure
 * hangs forever with no error), and matches incoming channels to sessions via
 * an 8-byte id preamble. A failed open aborts the dial — the transport's
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
  async acceptServer(channel) {
    this.transport._log('server l2cap channel open, awaiting id')
    const { id, rest } = await readIdPreamble(channel, this._timeout)
    const session = id !== null ? this.transport._sessions.get(id) : undefined
    if (!session || session.stream) {
      this.transport._log('server l2cap channel unmatched', id ?? 'timeout')
      try {
        channel.destroy()
      } catch (err) {
        safetyCatch(err)
      }
      return
    }
    const stream = new L2CapStream(channel)
    this.transport._bindServerStream(session, stream, 'l2cap')
    if (rest.byteLength) stream.receive(rest)
  }

  // ─── central side ─────────────────────────────────────────────────────────

  // One open attempt under a deadline; failure aborts the dial and the
  // transport's cooldown/redial cycle tries again.
  async openCentral(peripheral, sess, hello) {
    const psm = hello.psm
    const gone = () =>
      peripheral._session !== sess ||
      this.transport.closing ||
      this.transport.closed ||
      this.transport._suspended
    sess.upgrading = true
    try {
      const started = Date.now()
      this.transport._log(`l2cap open psm=${psm}`, peripheral.id)
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
        this.transport._log(`l2cap open ok in ${Date.now() - started}ms`, peripheral.id)
        this._bindCentral(peripheral, sess, channel)
        return
      }
      this.transport._log(`l2cap open failed after ${Date.now() - started}ms`, peripheral.id)
      this.transport._abortDial(peripheral, 'l2cap-failed')
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
    try {
      channel.write(b4a.from(sess.id, 'hex'))
    } catch (err) {
      safetyCatch(err)
      this.transport._log('l2cap preamble write failed', peripheral.id)
      this.transport._abortDial(peripheral, 'l2cap-failed')
      return
    }
    const stream = new L2CapStream(channel)
    this.transport._bindCentralStream(peripheral, sess, stream, 'l2cap')
  }
}
