const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const { createSwarm, once, until, link, linked, makeMockBluetooth } = require('./helpers')

// Tests drive the transport's real timers (dial intervals, scan cycles), so a
// heavily loaded machine slips them. Declare a generous deadline so slowness
// never fails a logically-correct test; a genuine hang still surfaces.
test.configure({ timeout: 120000 })

test('links two swarms, dedupes to one channel and exchanges data', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  t.is(a.state, 'on')

  const [ca, cb] = await linked(a, b)
  t.ok(b4a.equals(ca.remotePublicKey, cb.publicKey), 'keys cross-match')
  t.ok(b4a.equals(cb.remotePublicKey, ca.publicKey), 'keys cross-match')

  ca.write(b4a.from('over ble'))
  t.alike(await once(cb, 'data'), b4a.from('over ble'), 'a -> b')
  cb.write(b4a.from('right back'))
  t.alike(await once(ca, 'data'), b4a.from('right back'), 'b -> a')
})

test('shouldConnect refusal prevents any link', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { shouldConnect: () => false })
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  await new Promise((resolve) => setTimeout(resolve, 200))

  t.is(a.connections.size, 0)
  t.is(b.connections.size, 0)
})

test('suspend halts io and resume restores; both preserve the start intent', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  const transport = a.transport
  await linked(a, b)

  // suspend: radio down, all io halted, but "started" intent kept
  await a.suspend()
  t.is(a.suspended, true, 'suspended is a public flag (hyperswarm parity)')
  t.is(a.started, true, 'suspend does not clear the start intent')
  t.is(a.state, 'off', 'suspended reads as off')
  await until(() => a.connections.size === 0)
  await until(() => b.connections.size === 0)

  // resume: same managers, relinks
  await a.resume()
  t.is(a.transport, transport, 'resume reuses the managers, no rebuild')
  await linked(a, b)
  t.pass('relinked after suspend/resume')
})

test('resume respects stop: a disabled swarm stays off', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)

  await a.start()
  await a.suspend()
  await a.stop() // user disables while suspended
  t.is(a.started, false)

  await a.resume() // app foregrounds — must NOT turn the radio back on
  t.is(a.started, false, 'resume did not re-enable a stopped swarm')
  t.is(a.state, 'off')
})

test('start while suspended holds the radio down until resume', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.suspend() // host suspends before the user ever enables
  await a.start() // user enables while suspended: intent set, radio stays down
  t.is(a.started, true)
  t.is(a.state, 'off', 'no radio while suspended')

  await b.start()
  await a.resume() // now the radio actually comes up
  await linked(a, b)
  t.pass('linked once resumed')
})

test('stop drops links fast and start reuses the same transport', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  await linked(a, b)

  const transport = a.transport
  await a.stop()
  await b.stop()
  t.is(a.state, 'off')
  await until(() => a.connections.size === 0)
  await until(() => b.connections.size === 0)

  await a.start()
  await b.start()
  t.is(a.transport, transport, 'toggle reuses the managers — one service per process')
  await linked(a, b)
  t.pass('relinked after toggle')
})

test('central adopts the negotiated mtu for outbound chunks', async (t) => {
  const backend = makeMockBluetooth({ mtu: 247 })
  const a = createSwarm(t, backend, { pipe: 'gatt' })
  const b = createSwarm(t, backend, { pipe: 'gatt' })

  await a.start()
  await b.start()
  const links = await linked(a, b)

  const central = links.find((conn) => conn._peripheralId !== null)
  t.ok(central, 'one side is the central')
  // mtu − 3 (ATT) − 9 (frame header)
  t.is(central.rawStream.maxPayload, 235)
})

test('missing backend reports unsupported and start is a safe no-op', async (t) => {
  const bt = createSwarm(t, null)
  t.is(bt.state, 'unsupported')
  await bt.start()
  t.is(bt.state, 'unsupported')
  t.is(bt.connections.size, 0)
})

test('connections exposes the live noise streams', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  await linked(a, b)

  const conns = [...a.connections]
  t.is(conns.length, 1)
  t.ok(b4a.equals(conns[0].remotePublicKey, link(b).publicKey))
})

test('hyperswarm-shaped api: connections Set, peers Map, connecting, destroy', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  t.ok(a.connections instanceof Set, 'connections is a Set')
  t.ok(a.peers instanceof Map, 'peers is a Map')
  t.is(a.connections.size, 0, 'count via connections.size')

  await a.start()
  await b.start()
  const [ca] = await linked(a, b)

  t.is(a.connections.size, 1, 'one live link')
  t.is(a.peers.size, 1, 'peers Map keyed by remote key')
  t.ok(a.peers.has(b4a.toString(ca.remotePublicKey, 'hex')), 'keyed by remote public key hex')
  t.is(typeof a.connecting, 'number', 'connecting is a number')

  await a.destroy() // alias for close()
  t.ok(a.destroyed, 'destroyed flag set (hyperswarm parity)')
})

test('pipe l2cap links over a channel and exchanges data', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  const [ca, cb] = await linked(a, b)

  t.ok(ca.rawStream.channel, 'a side rides an l2cap channel')
  t.ok(cb.rawStream.channel, 'b side rides an l2cap channel')

  ca.write(b4a.from('over l2cap'))
  t.alike(await once(cb, 'data'), b4a.from('over l2cap'), 'a -> b')
  cb.write(b4a.from('right back'))
  t.alike(await once(ca, 'data'), b4a.from('right back'), 'b -> a')
})

test('pipe l2cap links when the id preamble arrives glued to handshake bytes', async (t) => {
  // the channel is a byte stream: coalesced deliveries put the first Noise
  // bytes in the same 'data' event as the id — the rest must reach the session
  const backend = makeMockBluetooth({ coalesce: true })
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  const [ca, cb] = await linked(a, b)

  ca.write(b4a.from('coalesced'))
  t.alike(await once(cb, 'data'), b4a.from('coalesced'), 'a -> b')
})

test('frames: the id round-trips the codec as the same hex string', (t) => {
  const { TYPE_DATA, encodeFrame, decodeFrame } = require('../lib/frames')
  const id = 'a1b2c3d4e5f60718'
  const f = decodeFrame(encodeFrame(TYPE_DATA, id, b4a.from('payload')))
  t.is(f.type, TYPE_DATA)
  t.is(f.id, id)
  t.alike(b4a.from(f.payload), b4a.from('payload'))
  t.is(decodeFrame(b4a.alloc(4)), null, 'short buffer decodes to null')
})

test('pipe l2cap refuses a gatt peer instead of degrading', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'gatt' })

  await a.start()
  await b.start()
  await new Promise((resolve) => setTimeout(resolve, 300))

  t.is(a.connections.size, 0, 'l2cap never carries a gatt link')
  t.is(b.connections.size, 0)
})

test('pipe l2cap never links when the open hangs', async (t) => {
  const backend = makeMockBluetooth({ l2cap: 'silent' })
  const fast = { timeout: 50 }
  const a = createSwarm(t, backend, { pipe: 'l2cap', l2cap: fast })
  const b = createSwarm(t, backend, { pipe: 'l2cap', l2cap: fast })

  await a.start()
  await b.start()
  await new Promise((resolve) => setTimeout(resolve, 500))

  t.is(a.connections.size, 0, 'no link — the failure stays visible')
  t.is(b.connections.size, 0)
})

test('an early l2cap channel error does not crash the process', async (t) => {
  const EventEmitter = require('events')
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { l2cap: { timeout: 50 } })
  await a.start()

  // a channel that errors before its id preamble ever arrives
  const channel = new EventEmitter()
  channel.destroy = () => {}
  a.transport.server.emit('channelOpen', channel)
  channel.emit('error', new Error('boom'))

  await new Promise((resolve) => setTimeout(resolve, 100))
  t.pass('survived an error before the stream bound')
})

test('rate-limited discoveries dial the strongest signal first', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  await a.start()

  const tr = a.transport
  const dialed = []
  tr.central.connect = (p) => dialed.push(p.id) // record order, never link

  // inside one dial window all discoveries are held; the flush must sort
  tr._lastDial = Date.now()
  tr._onDiscover({ id: 'weak', rssi: -90 })
  tr._onDiscover({ id: 'strong', rssi: -40 })
  tr._onDiscover({ id: 'mystery' }) // no rssi reported — ranks last
  tr._onDiscover({ id: 'cooled', rssi: -10 })
  t.is(dialed.length, 0, 'all held for the window')

  // guards re-run at flush time: the strongest candidate went cold meanwhile
  tr._device('cooled').coolUntil = Date.now() + 60000

  await until(() => dialed.length === 3)
  t.alike(dialed, ['strong', 'weak', 'mystery'], 'strongest first, no rssi last, cooled skipped')
})

test('psm rotation waits for pending sessions instead of breaking them', async (t) => {
  const { TYPE_OPEN, PIPE_L2CAP, encodeFrame, encodeKeyPayload } = require('../lib/frames')
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  await a.start()

  const tr = a.transport
  await until(() => tr._l2cap.psm !== null)
  const psm = tr._l2cap.psm
  // near-zero keys: smaller than any real key, so the initiator tie-break
  // always accepts these OPENs
  const open = (id, fill) =>
    tr._onServerFrame(
      encodeFrame(TYPE_OPEN, id, encodeKeyPayload(b4a.alloc(32, fill), PIPE_L2CAP, null))
    )

  // two inbound sessions, both still waiting for their l2cap channel
  open('11'.repeat(8), 0)
  open('22'.repeat(8), 1)
  t.is(tr._sessions.size, 2)
  open('11'.repeat(8), 0)
  t.is(tr._sessions.size, 2, 'a duplicate OPEN is ignored')

  // one dies — the other is mid-handshake, so the psm must survive
  tr._closeServerSession('11'.repeat(8))
  t.is(tr._l2cap.psm, psm, 'rotation deferred while a session is pending')

  // the last pending session binds its stream — now the deferred rotation lands
  const { Duplex } = require('streamx')
  tr._bindServerStream(tr._sessions.get('22'.repeat(8)), new Duplex(), 'l2cap')
  await until(() => tr._l2cap.psm !== null && tr._l2cap.psm !== psm)
  t.pass('listener rotated to a fresh psm once nothing was pending')
})

test('pipe l2cap never links on a backend without channel support', async (t) => {
  const backend = makeMockBluetooth({ l2cap: 'none' })
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  await new Promise((resolve) => setTimeout(resolve, 300))

  t.is(a.connections.size, 0, 'no psm to offer, sessions refuse')
  t.is(b.connections.size, 0)
})

test('pipe gatt never uses l2cap even when the backend supports it', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'gatt' })
  const b = createSwarm(t, backend, { pipe: 'gatt' })

  await a.start()
  await b.start()
  const [ca, cb] = await linked(a, b)

  t.absent(ca.rawStream.channel)
  t.absent(cb.rawStream.channel)
})

test('pipe l2cap relinks after a toggle', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  await linked(a, b)

  await a.stop()
  await until(() => a.connections.size === 0)
  await until(() => b.connections.size === 0)

  await a.start()
  const [ca, cb] = await linked(a, b)
  t.ok(ca.rawStream.channel, 'relinked over l2cap')
  t.ok(cb.rawStream.channel, 'relinked over l2cap')
})

test('pipe l2cap recovers once a hung radio heals', async (t) => {
  const backend = makeMockBluetooth({ l2cap: 'silent' })
  const fast = { timeout: 50 }
  const a = createSwarm(t, backend, { pipe: 'l2cap', l2cap: fast })
  const b = createSwarm(t, backend, { pipe: 'l2cap', l2cap: fast })

  await a.start()
  await b.start()
  await new Promise((resolve) => setTimeout(resolve, 300))
  t.is(a.connections.size, 0, 'no link while the open hangs')

  backend.setL2cap('ok')
  const [ca] = await linked(a, b)
  t.ok(ca.rawStream.channel, 'the dial cycle retried and linked over l2cap')
})

test('pipe gatt carries bulk data intact', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'gatt' })
  const b = createSwarm(t, backend, { pipe: 'gatt' })

  await a.start()
  await b.start()
  const [ca, cb] = await linked(a, b)

  const payload = crypto.randomBytes(64 * 1024)
  const received = []
  let total = 0
  const done = new Promise((resolve) => {
    cb.on('data', (chunk) => {
      received.push(chunk)
      total += chunk.byteLength
      if (total >= payload.byteLength) resolve()
    })
  })
  ca.write(payload)
  await done
  t.alike(b4a.concat(received), payload, '64KB crossed the gatt pipe intact')
})

test('pipe l2cap carries bulk data intact', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  const [ca, cb] = await linked(a, b)

  const payload = crypto.randomBytes(64 * 1024)
  const received = []
  let total = 0
  const done = new Promise((resolve) => {
    cb.on('data', (chunk) => {
      received.push(chunk)
      total += chunk.byteLength
      if (total >= payload.byteLength) resolve()
    })
  })
  ca.write(payload)
  await done
  t.alike(b4a.concat(received), payload, '64KB crossed the l2cap pipe intact')
})

test('destroying a link relinks without a toggle', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  const [ca] = await linked(a, b)

  ca.destroy()
  await until(() => a.connections.size === 0)

  await linked(a, b)
  t.pass('both sides recovered on their own')
})

test('three swarms mesh pairwise', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)
  const c = createSwarm(t, backend)

  await a.start()
  await b.start()
  await c.start()

  await until(() => a.connections.size === 2)
  await until(() => b.connections.size === 2)
  await until(() => c.connections.size === 2)
  t.pass('every pair linked')
})

test('pipe l2cap relinks after a radio power cycle', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  await linked(a, b)

  a.transport.server.state = 'poweredOff'
  a.transport.server.emit('stateChange', 'poweredOff')
  a.transport.central.state = 'poweredOff'
  a.transport.central.emit('stateChange', 'poweredOff')
  await until(() => a.connections.size === 0)

  const wedged = a.transport
  a.transport.server.state = 'poweredOn'
  a.transport.server.emit('stateChange', 'poweredOn')
  a.transport.central.state = 'poweredOn'
  a.transport.central.emit('stateChange', 'poweredOn')

  await until(() => a.transport && a.transport !== wedged)
  const [ca] = await linked(a, b)
  t.ok(ca.rawStream.channel, 'the rebuilt transport republished and relinked over l2cap')
})

test('relinks after a radio power cycle', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  await linked(a, b)

  // radio dies: service, advertising, scans and links all invalidated
  a.transport.server.state = 'poweredOff'
  a.transport.server.emit('stateChange', 'poweredOff')
  a.transport.central.state = 'poweredOff'
  a.transport.central.emit('stateChange', 'poweredOff')
  await until(() => a.connections.size === 0)

  // radio returns: the facade must abandon the wedged managers, build a
  // fresh transport and relink without an app-level toggle
  const wedged = a.transport
  a.transport.server.state = 'poweredOn'
  a.transport.server.emit('stateChange', 'poweredOn')
  a.transport.central.state = 'poweredOn'
  a.transport.central.emit('stateChange', 'poweredOn')

  await until(() => a.transport && a.transport !== wedged)
  t.not(a.transport, wedged, 'transport rebuilt with fresh managers')
  await linked(a, b)
  t.pass('relinked after power cycle')
})

test('a crowd meshes within its link caps', async (t) => {
  const backend = makeMockBluetooth()
  const swarms = []
  for (let i = 0; i < 8; i++) swarms.push(createSwarm(t, backend))
  await Promise.all(swarms.map((s) => s.start()))

  // everyone finds at least one peer — gossip covers the rest of the crowd
  await until(() => swarms.every((s) => s.connections.size >= 1))
  for (const s of swarms) {
    t.ok(s.connections.size >= 1, 'device linked')
    t.ok(s.connections.size <= 12, 'links stay within maxOutbound + maxInbound')
  }

  // the dial machinery settles instead of storming
  await until(() => swarms.every((s) => !s.transport._isDialing()))
  t.pass('dialing settled')
})

test('topics scope discovery and setTopic retunes live', async (t) => {
  const backend = makeMockBluetooth()
  const OTHER = crypto.hash(b4a.from('another-topic'))
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend, { topic: OTHER })

  await a.start()
  await b.start()

  // different topics: discovery never crosses
  await new Promise((resolve) => setTimeout(resolve, 300))
  t.is(a.connections.size, 0, 'no link across topics')
  t.is(b.connections.size, 0, 'no link across topics')

  // retune b onto a's topic: they link
  await b.setTopic(require('./helpers').TOPIC)
  await linked(a, b)
  t.is(a.connections.size, 1, 'linked after retune')

  // tune b away again: the link drops and stays down
  await b.setTopic(OTHER)
  await until(() => a.connections.size === 0 && b.connections.size === 0)
  t.pass('links dropped after tuning away')
})

test('setTopic with the current topic is a no-op', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  await a.start()

  let suspends = 0
  const original = a.transport.suspend.bind(a.transport)
  a.transport.suspend = async () => {
    suspends++
    return original()
  }

  const before = a.transport.discoveryUUID
  await a.setTopic(require('./helpers').TOPIC)
  t.is(a.transport.discoveryUUID, before, 'uuid unchanged')
  t.is(suspends, 0, 'radio untouched')
  t.alike(a.topic, require('./helpers').TOPIC, 'topic getter reflects the active topic')
})

test('setTopic before start applies on the first start', async (t) => {
  const backend = makeMockBluetooth()
  const OTHER = crypto.hash(b4a.from('pre-start-topic'))
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend, { topic: OTHER })

  await b.setTopic(require('./helpers').TOPIC)
  await a.start()
  await b.start()

  await linked(a, b)
  t.pass('linked on the topic set before start')
})

test('setTopic while stopped sticks across the next start', async (t) => {
  const backend = makeMockBluetooth()
  const OTHER = crypto.hash(b4a.from('while-stopped-topic'))
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  await linked(a, b)

  await b.stop()
  await b.setTopic(OTHER)
  await b.start()

  await until(() => a.connections.size === 0)
  await new Promise((resolve) => setTimeout(resolve, 300))
  t.is(a.connections.size, 0, 'stayed apart after restart on another topic')

  await b.setTopic(require('./helpers').TOPIC)
  await linked(a, b)
  t.pass('relinked after switching back live')
})

test('topics partition a crowd and the hosted service stays fixed', async (t) => {
  const backend = makeMockBluetooth()
  const OTHER = crypto.hash(b4a.from('crowd-topic'))
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)
  const c = createSwarm(t, backend, { topic: OTHER })

  await a.start()
  await b.start()
  await c.start()

  await linked(a, b)
  t.not(a.transport.discoveryUUID, a.transport.serviceUUID, 'topic uuid is not the hosted uuid')
  t.is(a.transport.serviceUUID, c.transport.serviceUUID, 'hosted service uuid is shared')
  t.not(a.transport.discoveryUUID, c.transport.discoveryUUID, 'topics diverge in discovery')

  await new Promise((resolve) => setTimeout(resolve, 300))
  t.is(c.connections.size, 0, 'other topic stays alone')
})
