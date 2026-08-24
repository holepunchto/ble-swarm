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

// OPEN/HELLO payloads extend past the key with a flag byte whose bits announce
// which optional fields follow, in order: [key:32][flags:1]([psm:2])([n:1][topic:32]xN).
// flags 0 is a bare key.
const PIPE_L2CAP = 0x01 // this peer offers an l2cap data pipe
const HAS_PSM = 0x02 // a 2-byte big-endian PSM follows (HELLO)
const HAS_TOPICS = 0x04 // a topic list follows: [count:1][topic:32] x count
const TOPIC_LEN = 32

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

function encodeKeyPayload(key, flags, psm, topics) {
  const hasPsm = psm !== null && psm !== undefined
  const hasTopics = topics && topics.length > 0
  let f = flags
  if (hasPsm) f |= HAS_PSM
  if (hasTopics) f |= HAS_TOPICS
  if (f === 0) return key
  const parts = [key, b4a.from([f])]
  if (hasPsm) parts.push(b4a.from([(psm >> 8) & 0xff, psm & 0xff]))
  if (hasTopics) {
    parts.push(b4a.from([topics.length]))
    for (const t of topics) parts.push(t)
  }
  return b4a.concat(parts)
}

function decodeKeyPayload(payload) {
  const key = payload.subarray(0, KEY_LEN)
  const flags = payload.byteLength > KEY_LEN ? payload[KEY_LEN] : 0
  let off = KEY_LEN + 1
  let psm = null
  if (flags & HAS_PSM) {
    psm = (payload[off] << 8) | payload[off + 1]
    off += 2
  }
  const topics = []
  if (flags & HAS_TOPICS) {
    const n = payload[off++]
    for (let i = 0; i < n; i++) {
      topics.push(payload.subarray(off, off + TOPIC_LEN))
      off += TOPIC_LEN
    }
  }
  return { key, flags, psm, topics }
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
  HAS_PSM,
  HAS_TOPICS,
  encodeFrame,
  decodeFrame,
  encodeKeyPayload,
  decodeKeyPayload
}
