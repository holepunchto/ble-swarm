const { Server, Service, Characteristic } = require('bare-bluetooth')

let phase = 'first'
const s1 = new Server()
s1.on('stateChange', (st) => {
  console.log('s1 state:', st)
  if (st !== 'poweredOn' || phase !== 'first') return
  phase = 'added'
  s1.addService(new Service('ce1af000-0000-1000-8000-00805f9b34fb', [
    new Characteristic('ce1af001-0000-1000-8000-00805f9b34fb', { write: true, notify: true })
  ]))
})
s1.on('serviceAdd', () => {
  console.log('s1 service added — destroying s1')
  try {
    s1.destroy()
    console.log('s1.destroy() returned without throwing')
  } catch (err) {
    console.log('s1.destroy() threw:', err.message)
  }
  setTimeout(() => {
    console.log('still alive 1s after destroy — creating s2')
    const s2 = new Server()
    s2.on('stateChange', (st) => {
      console.log('s2 state:', st)
      if (st !== 'poweredOn') return
      s2.addService(new Service('ce1af000-0000-1000-8000-00805f9b34fb', [
        new Characteristic('ce1af001-0000-1000-8000-00805f9b34fb', { write: true, notify: true })
      ]))
    })
    s2.on('serviceAdd', () => {
      s2.startAdvertising({ serviceUUIDs: ['ce1af000-0000-1000-8000-00805f9b34fb'] })
      console.log('s2 serving + advertising — SURVIVED')
      setTimeout(() => { console.log('DESTROY_IS_SAFE_ON_MACOS'); Bare.exit(0) }, 3000)
    })
    s2.on('error', (e) => console.log('s2 error:', e.message))
  }, 1000)
})
s1.on('error', (e) => console.log('s1 error:', e.message))
setTimeout(() => { console.log('TIMEOUT'); Bare.exit(1) }, 20000)
