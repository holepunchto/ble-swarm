const { Duplex } = require('streamx')
const safetyCatch = require('safety-catch')

const { TYPE_DATA, HEADER, encodeFrame } = require('./frames')

const NOTIFY_RETRY = 250
const WRITE_TIMEOUT = 8000

// A single GATT write/notify caps at ATT_MTU − 3; 150 stays under the common
// un-negotiated 185 MTU. Raised live when the link negotiates a bigger MTU.
const DEFAULT_PAYLOAD = 150

// Dumb byte-carrying duplex for the GATT pipe. Framing and session logic live
// in the pipe; this only fragments outbound writes and pushes inbound bytes.
class GattStream extends Duplex {
  constructor({ send, maxPayload = DEFAULT_PAYLOAD } = {}) {
    super()
    this.maxPayload = maxPayload
    this._send = send
  }

  async _write(chunk, cb) {
    try {
      for (let offset = 0; offset < chunk.byteLength; offset += this.maxPayload) {
        await this._send(chunk.subarray(offset, offset + this.maxPayload))
      }
      cb(null)
    } catch (err) {
      cb(err)
    }
  }

  receive(buffer) {
    this.push(buffer)
  }

  remoteEnd() {
    this.push(null)
  }
}

/**
 * The GATT characteristic machinery: the serialized server notify queue, the
 * central write chain, and the framed byte pipe (GattStream) built on them.
 * Control frames ride this machinery too — GATT is the control plane's carrier
 * even when session data flows over another pipe.
 */
module.exports = class GattPipe {
  constructor(transport, { maxPayload, ackWrites } = {}) {
    this.transport = transport
    this.dataChar = null
    this._maxPayload = maxPayload ?? 0
    this._ackWrites = ackWrites ?? true
    /** serialized server notify queue: { frame, resolve, reject } */
    this._queue = []
    this._retry = null
  }

  // ─── server (peripheral) side ─────────────────────────────────────────────

  // Serialize notifications through the single characteristic: updateValue
  // returns false when the peripheral's queue is full — hold the head frame and
  // retry on the next 'readyToUpdate', preserving order. A true return only
  // means queued locally, not delivered — lost frames surface as a Noise
  // stream failure upstream, never as silent corruption.
  notify(frame) {
    return new Promise((resolve, reject) => {
      this._queue.push({ frame, resolve, reject })
      this.drain()
    })
  }

  drain() {
    if (this._retry) {
      clearTimeout(this._retry)
      this._retry = null
    }
    while (this._queue.length) {
      const item = this._queue[0]
      let ok
      try {
        ok = this.transport.server.updateValue(this.dataChar, item.frame)
      } catch (err) {
        this._queue.shift()
        item.reject(err)
        continue
      }
      if (!ok) {
        // readyToUpdate is the fast path but can be missed — never stall the
        // queue waiting for it
        this._retry = setTimeout(() => {
          this._retry = null
          this.drain()
        }, NOTIFY_RETRY)
        if (this._retry.unref) this._retry.unref()
        return
      }
      this._queue.shift()
      item.resolve()
    }
  }

  openServer(sidHex, session) {
    const sid = session.sid
    const stream = new GattStream({
      send: (payload) => this.notify(encodeFrame(TYPE_DATA, sid, payload))
    })
    this.transport._bindServerStream(sidHex, session, stream, 'gatt')
  }

  // reject everything queued — radio down, suspend or close
  reset(reason) {
    if (this._retry) clearTimeout(this._retry)
    this._retry = null
    for (const item of this._queue) item.reject(new Error(reason))
    this._queue = []
  }

  // ─── central side ─────────────────────────────────────────────────────────

  // hello is unused — the gatt pipe needs nothing beyond the session
  openCentral(peripheral, sess, hello) {
    const stream = new GattStream({
      maxPayload: this.getMaxPayload(peripheral),
      send: (payload) => this.send(peripheral, encodeFrame(TYPE_DATA, sess.sid, payload))
    })
    this.transport._bindCentralStream(peripheral, sess, stream, 'gatt')
  }

  getMaxPayload(peripheral) {
    const budget = peripheral._mtu ? peripheral._mtu - 3 - HEADER : this._maxPayload
    return budget > 0 ? budget : undefined
  }

  // One write in flight per peripheral: chain each write behind the previous.
  // Control frames always use acknowledged writes; DATA follows the configured
  // mode (without-response skips a radio round-trip per chunk).
  send(peripheral, frame, withResponse = this._ackWrites) {
    const prev = peripheral._writeChain || Promise.resolve()
    const next = prev.then(() => this._writeOnce(peripheral, frame, withResponse))
    peripheral._writeChain = next.catch(safetyCatch) // keep the chain alive
    return next
  }

  _writeOnce(peripheral, frame, withResponse) {
    if (!withResponse) {
      return new Promise((resolve, reject) => {
        try {
          peripheral.write(peripheral._char, frame, false)
          resolve()
        } catch (err) {
          reject(err)
        }
      })
    }
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer)
        peripheral.removeListener('write', onWrite)
        peripheral.removeListener('error', onError)
      }
      const onWrite = () => {
        cleanup()
        resolve()
      }
      const onError = (err) => {
        cleanup()
        reject(err)
      }
      // a dropped completion event must not wedge the write chain — the
      // keepalive/timeout layer decides whether the link itself is dead
      const timer = setTimeout(() => {
        cleanup()
        reject(new Error('write completion timed out'))
      }, WRITE_TIMEOUT)
      if (timer.unref) timer.unref()
      peripheral.once('write', onWrite)
      peripheral.once('error', onError)
      try {
        peripheral.write(peripheral._char, frame, true)
      } catch (err) {
        cleanup()
        reject(err)
      }
    })
  }
}
