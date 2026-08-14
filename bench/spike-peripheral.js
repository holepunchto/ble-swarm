// Phase 0 spike — peripheral side. Run on Mac A:  bare bench/spike-peripheral.js
// Advertises the test service and echoes every write back as a notification.
const { Server, Service, Characteristic } = require('bare-bluetooth')

const SERVICE_UUID = 'ce1af000-0000-1000-8000-00805f9b34fb'
const DATA_UUID = 'ce1af001-0000-1000-8000-00805f9b34fb'

const data = new Characteristic(DATA_UUID, { write: true, writeWithoutResponse: true, notify: true })
const server = new Server()

let received = 0
let bytes = 0

server
  .on('stateChange', (state) => {
    console.log('server state:', state)
    if (state === 'poweredOn') server.addService(new Service(SERVICE_UUID, [data]))
  })
  .on('serviceAdd', () => {
    server.startAdvertising({ serviceUUIDs: [SERVICE_UUID] })
    console.log('advertising', SERVICE_UUID)
  })
  .on('writeRequest', (requests) => {
    for (const req of requests) {
      server.respondToRequest(req, Server.ATT_SUCCESS) // ack first, always
      received++
      bytes += req.data.byteLength
      server.updateValue(data, req.data) // echo back via notify
    }
  })
  .on('subscribe', () => console.log('central subscribed'))
  .on('error', (err) => console.error('server error:', err))

setInterval(() => {
  if (received) console.log(`received ${received} writes, ${bytes} bytes`)
}, 2000)
