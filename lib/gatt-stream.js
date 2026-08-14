const { Duplex } = require('streamx')

// A single GATT write/notify caps at ATT_MTU − 3; 150 stays under the common
// un-negotiated 185 MTU. Raised live when the link negotiates a bigger MTU.
const DEFAULT_PAYLOAD = 150

// Dumb byte-carrying duplex for the GATT transport. Framing and session logic
// live in the transport; this only fragments outbound writes and pushes
// inbound payload bytes.
module.exports = class GattStream extends Duplex {
  constructor({ send, onclose, maxPayload = DEFAULT_PAYLOAD } = {}) {
    super()
    this.maxPayload = maxPayload
    this._send = send
    this._onclose = onclose || null
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
    cb(null)
  }
}
