// End-to-end bench over BluetoothSwarm. Run on two Macs:
//   Mac A:  bare bench/peer.js
//   Mac B:  bare bench/peer.js send
// The sender pushes 256 KiB over the NoiseSecretStream; the receiver reports
// throughput. Tune writePayload / write mode via argv to compare configs:
//   bare bench/peer.js send 244 0     (payload 244, write-without-response)
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const BluetoothSwarm = require('..')

const send = Bare.argv[2] === 'send'
const writePayload = parseInt(Bare.argv[3] || '0', 10) || 0
const writeWithResponse = (Bare.argv[4] || '1') === '1'
const TOTAL = 256 * 1024

const bt = new BluetoothSwarm({
  keyPair: crypto.keyPair(),
  topic: crypto.hash(b4a.from('keet-bluetooth-bench')),
  writePayload,
  writeWithResponse
})

bt.on('update', () => console.log('state:', bt.state, 'peers:', bt.peers))
bt.on('connection', (conn) => {
  console.log('connected, initiator:', conn.isInitiator)
  if (send) {
    const started = Date.now()
    const chunk = b4a.alloc(4096, 0xab)
    let sent = 0
    const pump = () => {
      while (sent < TOTAL) {
        sent += chunk.byteLength
        if (!conn.write(chunk)) return conn.once('drain', pump)
      }
      conn.end()
      console.log(`sent ${TOTAL} bytes in ${((Date.now() - started) / 1000).toFixed(1)}s`)
    }
    pump()
  } else {
    const started = Date.now()
    let received = 0
    conn.on('data', (data) => {
      received += data.byteLength
      if (received % (32 * 1024) < data.byteLength) {
        const secs = (Date.now() - started) / 1000
        console.log(`${received} bytes, ${(received / secs / 1024).toFixed(2)} KiB/s`)
      }
    })
    conn.on('end', () => {
      const secs = (Date.now() - started) / 1000
      console.log(`DONE: ${received} bytes in ${secs.toFixed(1)}s = ${(received / secs / 1024).toFixed(2)} KiB/s`)
      Bare.exit(0)
    })
  }
})

bt.start().then(() => console.log('started, waiting for a peer…'))
