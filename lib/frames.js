const b4a = require('b4a')

// Wire frame both directions: [type:1][sessionId:8][payload].
// OPEN carries the initiator's 32-byte public key, HELLO the responder's —
// both sides can refuse a link (dup peer, policy) before any Noise work.
const TYPE_OPEN = 1
const TYPE_DATA = 2
const TYPE_CLOSE = 3
const TYPE_HELLO = 4
const SID_LEN = 8
const HEADER = 1 + SID_LEN
const KEY_LEN = 32

// OPEN/HELLO payloads optionally extend past the key with a capability flag
// byte (and, in HELLO, a 2-byte big-endian PSM): [key:32][flags:1][psm:2].
// Old peers send bare keys (flags 0).
const PIPE_L2CAP = 0x01

const EMPTY = b4a.alloc(0)

// The session id is a hex string end to end — the one canonical form (it is
// also the sessions map key). The codec is the only place it touches bytes.
function encodeFrame(type, id, payload = EMPTY) {
  const out = b4a.allocUnsafe(HEADER + payload.byteLength)
  out[0] = type
  b4a.write(out, id, 1, SID_LEN, 'hex')
  if (payload.byteLength) b4a.copy(payload, out, HEADER)
  return out
}

function decodeFrame(buf) {
  if (!buf || buf.byteLength < HEADER) return null
  return {
    type: buf[0],
    id: b4a.toString(buf.subarray(1, HEADER), 'hex'),
    payload: buf.subarray(HEADER)
  }
}

function encodeKeyPayload(key, flags, psm) {
  if (flags === 0) return key
  if (psm === null) return b4a.concat([key, b4a.from([flags])])
  return b4a.concat([key, b4a.from([flags, (psm >> 8) & 0xff, psm & 0xff])])
}

function decodeKeyPayload(payload) {
  return {
    key: payload.subarray(0, KEY_LEN),
    flags: payload.byteLength > KEY_LEN ? payload[KEY_LEN] : 0,
    psm:
      payload.byteLength >= KEY_LEN + 3 ? (payload[KEY_LEN + 1] << 8) | payload[KEY_LEN + 2] : null
  }
}

module.exports = {
  TYPE_OPEN,
  TYPE_DATA,
  TYPE_CLOSE,
  TYPE_HELLO,
  SID_LEN,
  HEADER,
  KEY_LEN,
  PIPE_L2CAP,
  encodeFrame,
  decodeFrame,
  encodeKeyPayload,
  decodeKeyPayload
}
