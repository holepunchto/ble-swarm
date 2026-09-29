const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const BluetoothSwarm = require('../..')
const { makeMockBluetooth, makeStateBackend } = require('../../mock.js')

// Shared topic all test swarms tune to.
const TOPIC = crypto.hash(b4a.from('keet-bluetooth-test'))

// Load the swarm as if on another platform
function loadSwarm(platform) {
  require('which-runtime')
  const paths = [require.resolve('../..'), require.resolve('../../lib/transport')]
  const runtime = require.resolve('which-runtime')
  const saved = [runtime, ...paths].map((p) => [p, require.cache[p]])
  require.cache[runtime] = {
    ...saved[0][1],
    exports: {
      ...saved[0][1].exports,
      isMac: platform === 'darwin',
      isIOS: platform === 'ios',
      isAndroid: platform === 'android'
    }
  }
  for (const p of paths) delete require.cache[p]
  try {
    return require('../..')
  } finally {
    for (const [p, mod] of saved) require.cache[p] = mod
  }
}

function createSwarm(t, backend, { platform, ...opts } = {}) {
  const Swarm = platform ? loadSwarm(platform) : BluetoothSwarm
  const bt = new Swarm({
    backend,
    keyPair: crypto.keyPair(),
    topic: TOPIC,
    ...opts
  })
  t.teardown(() => bt.close())
  return bt
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// Drive the mock radio's power state on both roles, as a platform would.
function setRadioState(bt, state) {
  for (const node of [bt.transport.server, bt.transport.central]) {
    node.state = state
    node.emit('stateChange', state)
  }
}

function once(emitter, event) {
  return new Promise((resolve) => emitter.once(event, resolve))
}

// Poll with a live timer: transport timers are unref'd, so waiting purely on
// 'update' events lets the loop empty and trips brittle's deadlock detector.
async function until(fn) {
  while (!fn()) await new Promise((resolve) => setTimeout(resolve, 25))
}

// The first live link on a swarm.
function link(bt) {
  return bt.transport.peers.values().next().value
}

// Both sides dial, so the first-tracked channel can be retired for its
// duplicate right after peers hits 1 — settle until both ends hold a live one.
async function linked(a, b) {
  await until(() => a.connections.size === 1)
  await until(() => b.connections.size === 1)
  while (true) {
    await new Promise((resolve) => setTimeout(resolve, 25))
    const ca = link(a)
    const cb = link(b)
    if (
      a.connections.size === 1 &&
      b.connections.size === 1 &&
      ca &&
      cb &&
      !ca.destroyed &&
      !cb.destroyed
    ) {
      return [ca, cb]
    }
  }
}

async function powerCycle(t, platform) {
  const backend = makeMockBluetooth()
  const log = []
  let seq = 0
  class Server extends backend.Server {
    constructor() {
      super()
      this.n = seq++
      log.push(`new ${this.n}`)
    }

    addService(service) {
      log.push(`add ${this.n}`)
      super.addService(service)
    }

    destroy() {
      log.push(`destroy ${this.n}`)
    }

    removeAllServices() {
      log.push(`remove ${this.n}`)
      super.removeAllServices()
    }
  }

  const a = createSwarm(t, { ...backend, Server }, { platform })
  const b = createSwarm(t, backend)

  await a.start()
  await b.start()
  await linked(a, b)

  log.length = 0
  const wedged = a.transport
  setRadioState(a, 'poweredOff')
  await until(() => a.connections.size === 0)
  setRadioState(a, 'poweredOn')

  await until(() => a.transport && a.transport !== wedged)
  await linked(a, b)
  return log
}

module.exports = {
  TOPIC,
  sleep,
  setRadioState,
  createSwarm,
  once,
  until,
  link,
  linked,
  powerCycle,
  makeMockBluetooth,
  makeStateBackend
}
