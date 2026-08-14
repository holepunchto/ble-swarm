// Phase 0 spike — central side. Run on Mac B:  bare bench/spike-central.js [payloadSize] [withResponse]
// Scans, connects, subscribes, then measures write->notify echo round-trips and throughput.
// Try: payloadSize 150 / 182 / 244 / 509, withResponse 1 / 0 — records which combos survive on Apple.
const { Central } = require('bare-bluetooth')

const SERVICE_UUID = 'ce1af000-0000-1000-8000-00805f9b34fb'
const DATA_UUID = 'ce1af001-0000-1000-8000-00805f9b34fb'

const payloadSize = parseInt(Bare.argv[2] || '150', 10)
const withResponse = (Bare.argv[3] || '1') === '1'
const TOTAL = 200 // echoes to collect per run

const central = new Central()
let peripheral = null
let dataChar = null
let sent = 0
let echoed = 0
let echoedBytes = 0
let started = 0

central
  .on('stateChange', (state) => {
    console.log('central state:', state)
    if (state === 'poweredOn') central.startScan([SERVICE_UUID])
  })
  .on('discover', (discovered) => {
    console.log('discovered peripheral, connecting…')
    central.stopScan()
    central.connect(discovered)
  })
  .on('connect', (p) => {
    console.log('connected')
    peripheral = p
    p.on('servicesDiscover', (services) => p.discoverCharacteristics(services[0], [DATA_UUID]))
    p.on('characteristicsDiscover', (_, chars) => {
      dataChar = chars[0]
      p.subscribe(dataChar)
    })
    p.on('notifyState', () => {
      console.log(`subscribed — bench: payload=${payloadSize} withResponse=${withResponse}`)
      started = Date.now()
      pump()
    })
    p.on('notify', (_, value) => {
      echoed++
      echoedBytes += value.byteLength
      if (echoed === TOTAL) done()
      else pump()
    })
    p.on('error', (err) => console.error('peripheral error:', err))
  })
  .on('error', (err) => console.error('central error:', err))

function pump () {
  // one echo in flight at a time — measures full round-trip; raw one-way rate is ~2x
  if (sent >= TOTAL) return
  sent++
  const buf = Buffer.alloc(payloadSize, 0xab)
  buf.writeUInt32LE(sent, 0)
  peripheral.write(dataChar, buf, withResponse)
}

function done () {
  const secs = (Date.now() - started) / 1000
  console.log(`DONE: ${TOTAL} round-trips, ${echoedBytes} bytes echoed in ${secs.toFixed(1)}s`)
  console.log(`round-trip rate: ${(TOTAL / secs).toFixed(1)}/s, echo throughput: ${(echoedBytes / secs / 1024).toFixed(2)} KiB/s`)
  console.log(`lost: ${sent - echoed} (nonzero => this payload/mode combo drops writes)`)
  Bare.exit(0)
}

setTimeout(() => {
  console.log(`TIMEOUT: sent=${sent} echoed=${echoed} — combo payload=${payloadSize} withResponse=${withResponse} unreliable`)
  Bare.exit(1)
}, 120000)
