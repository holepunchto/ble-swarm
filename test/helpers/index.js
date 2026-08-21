const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const BluetoothSwarm = require('../..')
const { makeMockBluetooth, makeStateBackend } = require('./radio')

// Shared topic all test swarms tune to.
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

module.exports = {
  TOPIC,
  createSwarm,
  once,
  until,
  link,
  linked,
  makeMockBluetooth,
  makeStateBackend
}
