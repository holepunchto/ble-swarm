const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const {
  TOPIC,
  createSwarm,
  once,
  until,
  link,
  linked,
  sleep,
  setRadioState,
  powerCycle,
  makeMockBluetooth
} = require('./helpers')

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
  t.alike(a.status(), { state: 'on', peerCount: 1 }, 'status reflects the link')
  t.ok(b4a.equals(ca.remotePublicKey, cb.publicKey), 'keys cross-match')
  t.ok(b4a.equals(cb.remotePublicKey, ca.publicKey), 'keys cross-match')

  ca.write(b4a.from('over ble'))
  t.alike(await once(cb, 'data'), b4a.from('over ble'), 'a -> b')
  cb.write(b4a.from('right back'))
  t.alike(await once(ca, 'data'), b4a.from('right back'), 'b -> a')
})

for (const blind of ['smaller', 'bigger']) {
  test(`links when only one side discovers the other (${blind} key blind)`, async (t) => {
    const backend = makeMockBluetooth()
    const [small, big] = [crypto.keyPair(), crypto.keyPair()].sort((x, y) =>
      b4a.compare(x.publicKey, y.publicKey)
    )
    const a = createSwarm(t, backend, { keyPair: blind === 'smaller' ? small : big })
    const b = createSwarm(t, backend, { keyPair: blind === 'smaller' ? big : small })
    const create = a._createTransport.bind(a)
    a._createTransport = () => {
      const transport = create()
      transport._onDiscover = () => {}
      return transport
    }

    await a.start()
    await b.start()

    const [ca, cb] = await linked(a, b)
    ca.write(b4a.from('one way'))
    t.alike(await once(cb, 'data'), b4a.from('one way'), 'a -> b')
  })
}

for (const n of [2, 4]) {
  test(`${n} swarms that see each other link without a duplicate handover`, async (t) => {
    const backend = makeMockBluetooth()
    const swarms = []
    let events = 0
    for (let i = 0; i < n; i++) {
      const s = createSwarm(t, backend)
      s.on('connection', () => events++)
      swarms.push(s)
    }
    for (const s of swarms) await s.start()

    await until(() => swarms.every((s) => s.connections.size === n - 1))
    await sleep(1000)
    const live = swarms.reduce((sum, s) => sum + s.connections.size, 0)
    t.is(live, n * (n - 1), 'full mesh')
    t.is(events, live, 'one connection event per live link')
  })
}

test('shouldConnect refusal prevents any link', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { shouldConnect: () => false })
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  await sleep(200)

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

test('resume cycles the l2cap listener — a dead session must not pin the psm', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  await linked(a, b)

  const before = a.transport._l2cap.psm
  t.ok(before !== null, 'listener published while linked')

  await a.suspend()
  await a.resume()

  await until(() => a.transport._l2cap.psm !== null)
  const after = a.transport._l2cap.psm
  t.not(after, before, 'resume republished the listener on a fresh psm')

  await linked(a, b)
  t.pass('relinked after suspend/resume on the new psm')
})

test('a final refusal releases the physical link', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'gatt' })

  await a.start()
  await b.start()

  // mismatched pipes refuse in both directions, neither side may hold the link
  const released = (bt) => {
    for (const d of bt.transport._devices.values()) {
      if (d.coolUntil > Date.now() && !d.peripheral) return true
    }
    return false
  }
  await until(() => released(a) && released(b))

  t.is(a.transport.peers.size, 0, 'no link formed')
  t.pass('both sides dropped the physical link on the final refusal')
})

test('a closed l2cap channel leaves the central map', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  await linked(a, b)

  const size = () =>
    a.transport._l2cap._centralChannels.size + b.transport._l2cap._centralChannels.size
  t.is(size(), 1, 'the dialing side holds its channel while linked')

  await b.suspend()
  await until(() => size() === 0)
  t.pass('the map is empty once the channel closed')
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

test('a backend that throws while loading reports unsupported', (t) => {
  const { spawnSync } = require('child_process')
  const path = require('path')

  // the android condition resolves #bluetooth to the native addon, which cannot
  // load here — the same shape as a device whose OS rejects the addon
  const { status, stdout, stderr } = spawnSync(
    process.execPath,
    ['--conditions=android', '-p', "new (require('.'))().state"],
    { cwd: path.join(__dirname, '..'), encoding: 'utf8' }
  )

  t.is(status, 0, stderr)
  t.is(stdout.trim(), 'unsupported')
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
  await sleep(300)

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
  await sleep(500)

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

  await sleep(100)
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
  // hold the reference now: a stalled machine can reap the pending session
  // from the map before the bind below runs
  const pending = tr._sessions.get('22'.repeat(8))

  // one dies — the other is mid-handshake, so the psm must survive
  tr._closeServerSession('11'.repeat(8))
  t.is(tr._l2cap.psm, psm, 'rotation deferred while a session is pending')

  // the last pending session binds its stream — now the deferred rotation lands
  const { Duplex } = require('streamx')
  tr._bindServerStream(pending, new Duplex(), 'l2cap')
  await until(() => tr._l2cap.psm !== null && tr._l2cap.psm !== psm)
  t.pass('listener rotated to a fresh psm once nothing was pending')
})

test('pipe l2cap never links on a backend without channel support', async (t) => {
  const backend = makeMockBluetooth({ l2cap: 'none' })
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  await sleep(300)

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
  await sleep(300)
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

  setRadioState(a, 'poweredOff')
  await until(() => a.connections.size === 0)

  const wedged = a.transport
  setRadioState(a, 'poweredOn')

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
  setRadioState(a, 'poweredOff')
  await until(() => a.connections.size === 0)

  // radio returns: the facade must abandon the wedged managers, build a
  // fresh transport and relink without an app-level toggle
  const wedged = a.transport
  setRadioState(a, 'poweredOn')

  await until(() => a.transport && a.transport !== wedged)
  t.not(a.transport, wedged, 'transport rebuilt with fresh managers')
  await linked(a, b)
  t.pass('relinked after power cycle')
})

test('android: a power cycle destroys the old managers before building new ones', async (t) => {
  const log = await powerCycle(t, 'android')
  t.alike(log.slice(log.indexOf('destroy 0')), ['destroy 0', 'new 1', 'add 1'])
})

test('ios: a power cycle removes the old service before building new managers', async (t) => {
  const log = await powerCycle(t, 'ios')
  t.alike(
    log,
    ['remove 0', 'new 1', 'add 1'],
    'old service never re-added, managers never destroyed'
  )
})

// Contention makes dial cooldowns escalate, so the mesh's tail latency can
// pass the default deadline on a loaded machine — the assertions never flake.
test('a crowd meshes within its link caps', { timeout: 300000 }, async (t) => {
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
  await sleep(300)
  t.is(a.connections.size, 0, 'no link across topics')
  t.is(b.connections.size, 0, 'no link across topics')

  // retune b onto a's topic: they link
  await b.setTopic(TOPIC)
  await linked(a, b)
  t.is(a.connections.size, 1, 'linked after retune')

  // tune b away again: the link drops and stays down
  await b.setTopic(OTHER)
  await until(() => a.connections.size === 0 && b.connections.size === 0)
  t.pass('links dropped after tuning away')
})

test('no topic: the tag-derived default applies and peers link', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { topic: undefined })
  const b = createSwarm(t, backend, { topic: undefined })

  await a.start()
  await b.start()

  await linked(a, b)
  t.is(a.topic, null, 'no explicit topic set')
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
  await a.setTopic(TOPIC)
  t.is(a.transport.discoveryUUID, before, 'uuid unchanged')
  t.is(suspends, 0, 'radio untouched')
  t.alike(a.topic, TOPIC, 'topic getter reflects the active topic')
})

test('setTopic before start applies on the first start', async (t) => {
  const backend = makeMockBluetooth()
  const OTHER = crypto.hash(b4a.from('pre-start-topic'))
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend, { topic: OTHER })

  await b.setTopic(TOPIC)
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
  await sleep(300)
  t.is(a.connections.size, 0, 'stayed apart after restart on another topic')

  await b.setTopic(TOPIC)
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

  await sleep(300)
  t.is(c.connections.size, 0, 'other topic stays alone')
})

test('a stop racing a topic switch keeps the radio down', async (t) => {
  const backend = makeMockBluetooth()
  const OTHER = crypto.hash(b4a.from('race-topic'))
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend, { topic: OTHER })

  await a.start()
  await b.start()

  // stop lands while setTopic is bouncing the radio — the stop must win
  const race = a.setTopic(OTHER)
  await a.stop()
  await race

  t.is(a.state, 'off', 'radio stayed down')
  await sleep(300)
  t.is(a.connections.size, 0, 'no links while stopped')

  // the switched topic still applies on the next start
  await a.start()
  await linked(a, b)
  t.pass('came back up on the new topic')
})

function quiet(tr) {
  const dialed = []
  tr.central.connect = (p) => dialed.push(p.id)
  return dialed
}

const KEY_A = 'aa'.repeat(32)

function hear(tr, id) {
  tr.central.emit('discover', { id, rssi: -50 })
}

test('discovered lists an advertiser the link caps leave unlinked', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  const b = createSwarm(t, backend, { maxOutbound: 0 })

  t.alike(a.discovered, [], 'nothing before start')
  await a.start()
  await b.start()

  await until(() => a.discovered.length === 1)
  const [entry] = a.discovered
  t.is(entry.id, b.transport.server._addr)
  t.is(entry.linked, false)
  t.is(entry.publicKey, null, 'no session got as far as its key')
})

test('discovered marks a linked device', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)
  await a.start()
  await b.start()
  await linked(a, b)

  await until(() => a.discovered.length > 0 && a.discovered.every((e) => e.linked))
  t.pass('every entry for b is linked')
})

test('discovered learns a device key and announces it', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  let updates = 0
  a.on('update', () => updates++)

  tr._identify('x', b4a.from(KEY_A, 'hex'))
  t.alike(a.discovered[0].publicKey, b4a.from(KEY_A, 'hex'))
  t.is(updates, 1)
})

test('discovered announces arrivals, departures and re-hearings once per window', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  let updates = 0
  a.on('update', () => updates++)
  hear(tr, 'x')
  t.is(updates, 1, 'arrival')
  hear(tr, 'x')
  t.is(updates, 1, 'a re-report in the same window is quiet')

  tr._endScanWindow()
  t.is(updates, 1, 'nothing changed in that window')
  hear(tr, 'x')
  hear(tr, 'x')
  tr._endScanWindow()
  t.is(updates, 2, 're-heard in a new window: one update when it closes')
  t.is(a.discovered[0].heard, 1)

  tr._endScanWindow()
  t.is(a.discovered.length, 1, 'kept through one complete window unheard')
  tr._endScanWindow()
  t.is(a.discovered.length, 0, 'gone after the second')
  t.is(updates, 3, 'departure announced')
})

test('a scan cut short by a dial is no evidence a device left', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  for (let i = 0; i < 10; i++) {
    tr._startScan()
    tr._stopScan()
  }
  t.is(a.discovered.length, 1, 'still listed however long the radio was busy dialing')
})

test('a scan that ran a full window completes it however it stopped', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  tr._stopScan()
  tr._startScan()
  tr._scanStartedAt = Date.now() - 6000
  const before = tr._scanWindows
  tr._stopScan()
  t.is(tr._scanWindows, before + 1)
})

test('suspend and stop clear discovered', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  quiet(a.transport)
  hear(a.transport, 'x')
  await a.suspend()
  t.alike(a.discovered, [], 'cleared on suspend')

  await a.resume()
  quiet(a.transport)
  hear(a.transport, 'x')
  await a.stop()
  t.alike(a.discovered, [], 'cleared on stop')
})

test('a device whose link drops stays listed until it ages out', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend)
  const neverDials = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()
  await neverDials.start()
  await linked(a, neverDials)

  neverDials.transport.server.stopAdvertising()
  neverDials.transport._destroyPeers()
  await until(() => a.connections.size === 0)
  t.is(a.discovered.length, 1, 'it may well still be advertising')
  t.is(a.discovered[0].linked, false)

  a.transport._endScanWindow()
  a.transport._endScanWindow()
  t.is(a.discovered.length, 1, 'kept through the first window it went unheard')
  a.transport._endScanWindow()
  t.is(a.discovered.length, 0)
})

test('a replayed discovery is not a new hearing', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  tr._endScanWindow()
  tr._onDiscover({ id: 'x', rssi: -50 })
  t.is(a.discovered[0].heard, 0)
})

test('a scheduled scan stop completes the window', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  tr._stopScan()
  tr._startScan()
  tr._scanStartedAt = Date.now() - 4999
  const before = tr._scanWindows
  tr._stopScan(true)
  t.is(tr._scanWindows, before + 1)
})

test('an entry is linked only once its link is up', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  tr._identify('x', b4a.from(KEY_A, 'hex'))
  tr._device('x').linked = true
  t.is(a.discovered[0].linked, false, 'the channel is bound but the handshake is not done')
})

test('a peer refused by policy is still identified', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { shouldConnect: () => false })
  const b = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()
  await b.start()

  await until(() => a.discovered.length === 1 && a.discovered[0].publicKey !== null)
  t.alike(a.discovered[0].publicKey, b.transport.keyPair.publicKey)
  t.is(a.discovered[0].linked, false)
})

test('a peer that dialed us is listed without its key', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  const b = createSwarm(t, backend)
  await a.start()
  await b.start()
  await until(() => a.connections.size === 1 && a.discovered.length === 1)

  t.is(a.discovered[0].publicKey, null, 'a central gives our server no address to match')
  t.is(a.discovered[0].linked, false)
})

test('clearing discovered also forgets what was re-heard', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  tr._endScanWindow()
  hear(tr, 'x')
  tr._clearDeviceTimers()

  let updates = 0
  a.on('update', () => updates++)
  tr._endScanWindow()
  t.is(updates, 0)
})

function helloFrom(tr, id, key) {
  const { TYPE_HELLO, encodeFrame, encodeKeyPayload } = require('../lib/frames')
  const sid = 'a1b2c3d4e5f60718'
  const peripheral = { id, _session: { id: sid } }
  tr._sendClose = () => {}
  tr._disconnect = () => {}
  return tr._onCentralNotify(peripheral, encodeFrame(TYPE_HELLO, sid, encodeKeyPayload(key, 0)))
}

test('our own reflection is not identified', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  await helloFrom(tr, 'x', tr.keyPair.publicKey)
  t.is(a.discovered[0].publicKey, null)
})

test('a malformed key is not identified', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  await helloFrom(tr, 'x', b4a.alloc(16, 1))
  t.is(a.discovered[0].publicKey, null)
})

test('turning the radio off announces the emptied list', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  const announced = []
  a.on('update', () => announced.push(a.discovered.length))
  setRadioState(a, 'poweredOff')
  t.ok(announced.length > 0)
  t.ok(
    announced.every((count) => count === 0),
    'no update carries the list from before the wipe'
  )
})

test('a radio going off after resetting still announces the emptied list', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  setRadioState(a, 'resetting')
  hear(tr, 'x')
  const announced = []
  a.on('update', () => announced.push(a.discovered.length))
  setRadioState(a, 'poweredOff')
  t.alike(announced, [0])
})

test('a refused HELLO identifies the device by the key it claims', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0 })
  await a.start()

  const tr = a.transport
  quiet(tr)
  hear(tr, 'x')
  await helloFrom(tr, 'x', b4a.from(KEY_A, 'hex'))
  t.alike(a.discovered[0].publicKey, b4a.from(KEY_A, 'hex'))
})

test('a HELLO refused by policy is identified by its key even when that peer is linked', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { maxOutbound: 0, shouldConnect: () => false })
  await a.start()

  const tr = a.transport
  quiet(tr)
  tr.peers.set(KEY_A, { destroy() {} })
  hear(tr, 'x')
  await helloFrom(tr, 'x', b4a.from(KEY_A, 'hex'))
  t.alike(
    a.discovered[0].publicKey,
    b4a.from(KEY_A, 'hex'),
    'a policy may refuse a peer it reaches another way'
  )
})
