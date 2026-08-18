const EventEmitter = require('bare-events')
const b4a = require('b4a')

// A shared in-process "radio": every Server/Central built from one
// makeMockBluetooth() sees the same airwaves. Faithful to the GATT surface the
// transport uses: advertise/scan/discover/connect → discover service + data
// characteristic → subscribe → central writes framed bytes (writeRequest, the
// server responds then routes them) and pushes framed bytes back via updateValue
// → notify. A device is one Server + one Central; a central never discovers its
// own server (iOS never delivers a device its own advertisement). Error/
// permission paths are the radio layer and are verified on hardware.

const DEFAULT_MTU = 247

// l2cap modes: 'ok' (channels open), 'silent' (openL2CAPChannel never answers —
// the iOS-peripheral/macOS-central hang), 'none' (no publishChannel support,
// like an old backend). Mutable via setL2cap so tests can heal the radio.
// coalesce: merge back-to-back channel writes into one 'data' delivery — the
// byte-stream reality where the id preamble arrives glued to the first
// handshake bytes.
function makeMockBluetooth({ mtu: maxMtu = DEFAULT_MTU, l2cap = 'ok', coalesce = false } = {}) {
  let addrSeq = 0
  let psmSeq = 0x80

  class MockChannel extends EventEmitter {
    constructor() {
      super()
      this._peer = null
      this.destroyed = false
    }

    write(data) {
      if (this.destroyed) return true
      const peer = this._peer
      if (coalesce) {
        // hold each delivery until a second write arrives (or 20ms passes) so
        // back-to-back writes — id preamble + first handshake bytes — always
        // land as ONE 'data' event, like a real byte stream may deliver them
        this._pending = this._pending ? b4a.concat([this._pending, b4a.from(data)]) : b4a.from(data)
        this._pendingWrites = (this._pendingWrites || 0) + 1
        const flush = () => {
          clearTimeout(this._flushTimer)
          this._flushTimer = null
          this._pendingWrites = 0
          const buf = this._pending
          this._pending = null
          if (buf && !peer.destroyed) peer.emit('data', buf)
        }
        if (this._pendingWrites >= 2) flush()
        else if (!this._flushTimer) this._flushTimer = setTimeout(flush, 20)
        return true
      }
      const buf = b4a.from(data)
      queueMicrotask(() => {
        if (!peer.destroyed) peer.emit('data', buf)
      })
      return true // the mock never backpressures
    }

    end() {
      const peer = this._peer
      queueMicrotask(() => {
        if (!peer.destroyed) peer.emit('end')
      })
    }

    // real channels are streamx duplexes — the mock never backpressures
    pause() {}
    resume() {}

    destroy() {
      if (this.destroyed) return
      this.destroyed = true
      const peer = this._peer
      queueMicrotask(() => {
        this.emit('close')
        if (!peer.destroyed) peer.destroy()
      })
    }
  }

  function channelPair() {
    const a = new MockChannel()
    const b = new MockChannel()
    a._peer = b
    b._peer = a
    return [a, b]
  }
  const advertisers = new Set() // Server instances currently advertising
  const scanners = new Set() // { central, uuids, opts }
  let pendingServer = null // last Server awaiting its device's Central

  function announce(server) {
    for (const s of scanners) {
      if (s.central._ownServer === server) continue // own advertisement — not delivered
      if (!s.uuids.includes(server._uuid)) continue
      queueMicrotask(() => s.central.emit('discover', { id: server._addr }))
    }
  }

  class Characteristic {
    constructor(uuid, opts = {}) {
      this.uuid = uuid
      this.opts = opts
    }
  }

  class Service {
    constructor(uuid, characteristics = []) {
      this.uuid = uuid
      this.characteristics = characteristics
    }
  }

  class Server extends EventEmitter {
    constructor() {
      super()
      this._addr = `addr-${addrSeq++}`
      this._uuid = null
      this._service = null
      this._subscribers = new Set() // connected Peripherals notifying from us
      this.state = 'unknown'
      pendingServer = this
      queueMicrotask(() => {
        this.state = 'poweredOn'
        this.emit('stateChange', 'poweredOn')
      })
    }
    addService(service) {
      this._service = service
      queueMicrotask(() => this.emit('serviceAdd', service.uuid))
    }
    startAdvertising({ serviceUUIDs }) {
      this._uuid = serviceUUIDs[0]
      advertisers.add(this)
      announce(this)
    }
    stopAdvertising() {
      advertisers.delete(this)
    }
    destroy() {}
    respondToRequest(req, _status, _data) {
      const p = req && req._peripheral
      if (p) queueMicrotask(() => p.emit('write', p._char || null))
    }
    updateValue(_char, data) {
      const frame = b4a.from(data) // detach from the caller's buffer
      for (const p of this._subscribers) {
        queueMicrotask(() => p.emit('notify', p._char || null, frame))
      }
      return true // the mock queue is never full
    }
  }

  if (l2cap !== 'none') {
    Server.prototype.publishChannel = function publishChannel() {
      if (this._psm === undefined) this._psm = psmSeq++
      queueMicrotask(() => this.emit('channelPublish', this._psm))
    }
    Server.prototype.unpublishChannel = function unpublishChannel(_psm) {
      this._psm = undefined // the next publish gets a fresh psm, like the OS
    }
  }

  class Peripheral extends EventEmitter {
    constructor(server) {
      super()
      this.id = server._addr
      this._server = server
      this._char = null
    }
    discoverServices() {
      queueMicrotask(() =>
        this.emit('servicesDiscover', this._server._service ? [this._server._service] : [])
      )
    }
    discoverCharacteristics(service) {
      queueMicrotask(() => this.emit('characteristicsDiscover', service, service.characteristics))
    }
    subscribe(char) {
      this._char = char
      this._server._subscribers.add(this)
      queueMicrotask(() => {
        this.emit('notifyState', char, true)
        this._server.emit('subscribe', this, char && char.uuid)
      })
    }
    unsubscribe(char) {
      this._server._subscribers.delete(this)
      queueMicrotask(() => {
        this.emit('notifyState', char, false)
        this._server.emit('unsubscribe', this, char && char.uuid)
      })
    }
    write(char, data, withResponse = true) {
      const req = {
        characteristicUuid: char && char.uuid,
        data: b4a.from(data),
        responseNeeded: withResponse,
        // a write-without-response has no completion, so respondToRequest is inert
        _peripheral: withResponse ? this : null
      }
      queueMicrotask(() => this._server.emit('writeRequest', [req]))
    }
    requestMtu(mtu) {
      const negotiated = Math.min(mtu, maxMtu)
      queueMicrotask(() => this.emit('mtuChanged', negotiated))
    }

    openL2CAPChannel(psm) {
      if (l2cap === 'silent') return // the iOS hang: no answer, no error
      if (this._server._psm !== psm) return // wrong psm: also silence
      const [mine, theirs] = channelPair()
      queueMicrotask(() => {
        this.emit('channelOpen', mine)
        this._server.emit('channelOpen', theirs)
      })
    }
  }

  class Central extends EventEmitter {
    constructor() {
      super()
      this._scan = null
      this._ownServer = pendingServer // pair with this device's server
      pendingServer = null
      this.state = 'unknown'
      queueMicrotask(() => {
        this.state = 'poweredOn'
        this.emit('stateChange', 'poweredOn')
      })
    }
    startScan(uuids, opts = {}) {
      this._scan = { central: this, uuids, opts }
      scanners.add(this._scan)
      for (const server of advertisers) announce(server)
    }
    stopScan() {
      if (this._scan) scanners.delete(this._scan)
      this._scan = null
    }
    connect(discovered) {
      const server = [...advertisers].find((s) => s._addr === discovered.id)
      if (!server) return
      queueMicrotask(() => this.emit('connect', new Peripheral(server)))
    }
    destroy() {}
    disconnect(peripheral) {
      if (peripheral && peripheral._server) peripheral._server._subscribers.delete(peripheral)
      queueMicrotask(() => this.emit('disconnect', peripheral || null))
    }
  }

  Server.ATT_SUCCESS = 0

  const setL2cap = (mode) => {
    l2cap = mode
  }

  return { Central, Server, Peripheral, Service, Characteristic, setL2cap }
}

// A backend stuck in a given adapter state — for the facade state-machine tests.
function makeStateBackend(state) {
  class Node extends EventEmitter {
    constructor() {
      super()
      this.state = state
      queueMicrotask(() => this.emit('stateChange', state))
    }
    startScan() {}
    stopScan() {}
    startAdvertising() {}
    stopAdvertising() {}
    addService() {}
    respondToRequest() {}
    updateValue() {
      return true
    }
    connect() {}
    disconnect() {}
  }
  class Characteristic {
    constructor(uuid, opts = {}) {
      this.uuid = uuid
      this.opts = opts
    }
  }
  class Service {
    constructor(uuid, chars) {
      this.uuid = uuid
      this.characteristics = chars
    }
  }
  Node.ATT_SUCCESS = 0
  return { Central: Node, Server: Node, Service, Characteristic }
}

module.exports = { makeMockBluetooth, makeStateBackend }
