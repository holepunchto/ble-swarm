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
  const a = createSwarm(t, backend)
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  const links = await linked(a, b)

  const central = links.find((conn) => conn._peripheralId != null)
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
