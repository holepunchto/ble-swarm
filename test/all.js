const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const BluetoothSwarm = require('..')
const { makeMockBluetooth } = require('./mock-radio')

const TOPIC = crypto.hash(b4a.from('keet-bluetooth-test'))

function createSwarm(t, backend, opts = {}) {
  const bt = new BluetoothSwarm({
    backend,
    keyPair: crypto.keyPair(),
    topic: TOPIC,
    ...opts
  })
  t.teardown(() => bt.close())
  return bt
}

function once(emitter, event) {
  return new Promise((resolve) => emitter.once(event, resolve))
}

// poll with a live timer: transport timers are unref'd, so waiting purely on
// 'update' events lets the loop empty and trips brittle's deadlock detector
async function until(bt, fn) {
  while (!fn()) await new Promise((resolve) => setTimeout(resolve, 25))
}

function link(bt) {
  return bt.transport.peers.values().next().value
}

// Both sides dial, so the first-tracked channel can be retired for its
// duplicate right after peers hits 1 — settle until both ends hold a live one.
async function linked(a, b) {
  await until(a, () => a.peers === 1)
  await until(b, () => b.peers === 1)
  while (true) {
    await new Promise((resolve) => setTimeout(resolve, 25))
    const ca = link(a)
    const cb = link(b)
    if (a.peers === 1 && b.peers === 1 && ca && cb && !ca.destroyed && !cb.destroyed) {
      return [ca, cb]
    }
  }
}

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

  t.is(a.peers, 0)
  t.is(b.peers, 0)
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
  await until(a, () => a.peers === 0)
  await until(b, () => b.peers === 0)

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
  t.is(bt.peers, 0)
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

test('pipe l2cap refuses a gatt peer instead of degrading', async (t) => {
  const backend = makeMockBluetooth()
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'gatt' })

  await a.start()
  await b.start()
  await new Promise((resolve) => setTimeout(resolve, 300))

  t.is(a.peers, 0, 'l2cap never carries a gatt link')
  t.is(b.peers, 0)
})

test('pipe l2cap never links when the open hangs', async (t) => {
  const backend = makeMockBluetooth({ l2cap: 'silent' })
  const fast = { timeout: 50 }
  const a = createSwarm(t, backend, { pipe: 'l2cap', l2cap: fast })
  const b = createSwarm(t, backend, { pipe: 'l2cap', l2cap: fast })

  await a.start()
  await b.start()
  await new Promise((resolve) => setTimeout(resolve, 500))

  t.is(a.peers, 0, 'no link — the failure stays visible')
  t.is(b.peers, 0)
})

test('pipe l2cap never links on a backend without channel support', async (t) => {
  const backend = makeMockBluetooth({ l2cap: 'none' })
  const a = createSwarm(t, backend, { pipe: 'l2cap' })
  const b = createSwarm(t, backend, { pipe: 'l2cap' })

  await a.start()
  await b.start()
  await new Promise((resolve) => setTimeout(resolve, 300))

  t.is(a.peers, 0, 'no psm to offer, sessions refuse')
  t.is(b.peers, 0)
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
  await until(a, () => a.peers === 0)
  await until(b, () => b.peers === 0)

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
  t.is(a.peers, 0, 'no link while the open hangs')

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
  await until(a, () => a.peers === 0)

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

  await until(a, () => a.peers === 2)
  await until(b, () => b.peers === 2)
  await until(c, () => c.peers === 2)
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
  await until(a, () => a.peers === 0)

  const wedged = a.transport
  a.transport.server.state = 'poweredOn'
  a.transport.server.emit('stateChange', 'poweredOn')
  a.transport.central.state = 'poweredOn'
  a.transport.central.emit('stateChange', 'poweredOn')

  await until(a, () => a.transport && a.transport !== wedged)
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
  await until(a, () => a.peers === 0)

  // radio returns: the facade must abandon the wedged managers, build a
  // fresh transport and relink without an app-level toggle
  const wedged = a.transport
  a.transport.server.state = 'poweredOn'
  a.transport.server.emit('stateChange', 'poweredOn')
  a.transport.central.state = 'poweredOn'
  a.transport.central.emit('stateChange', 'poweredOn')

  await until(a, () => a.transport && a.transport !== wedged)
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
  await until(swarms[0], () => swarms.every((s) => s.peers >= 1))
  for (const s of swarms) {
    t.ok(s.peers >= 1, 'device linked')
    t.ok(s.peers <= 12, 'links stay within maxOutbound + maxInbound')
  }

  // the dial machinery settles instead of storming
  await until(swarms[0], () => swarms.every((s) => !s.transport._isDialing()))
  t.pass('dialing settled')
})
