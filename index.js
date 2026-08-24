const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const { isMac } = require('which-runtime')

const BLETransport = require('./lib/transport')

const backend = require('#bluetooth')

/**
 * Bluetooth LE hyperswarm transport. Two orthogonal axes drive the radio:
 * start()/stop() is the durable user intent (enabled), suspend()/resume() is
 * the transient host-lifecycle pause (app background/foreground). The radio is
 * live only while started AND not suspended. The underlying managers are
 * created once and suspended/resumed, never destroyed.
 *
 * @example
 * const bt = new BluetoothSwarm({ keyPair, topic })
 * bt.on('connection', (conn) => { ... }) // NoiseSecretStream, deduped
 * await bt.start()          // user enables
 * await bt.suspend()        // app backgrounds — radio down, intent kept
 * await bt.resume()         // app foregrounds — radio back up
 */
module.exports = class BluetoothSwarm extends ReadyResource {
  constructor(opts = {}) {
    super()
    this.started = false
    this.transport = null
    this.suspended = false
    this._opts = opts
    this._backend = opts.backend !== undefined ? opts.backend : backend
    this._online = opts.online === true
    this._gen = 0
  }

  get supported() {
    return this._backend !== null
  }

  get state() {
    if (!this.supported) return 'unsupported'
    if (!this.started || !this.transport) return 'off'
    return this.transport.state
  }

  get destroyed() {
    return this.closed
  }

  get peers() {
    return this.transport ? this.transport.peers : new Map()
  }

  get connections() {
    return new Set(this.transport ? this.transport.peers.values() : [])
  }

  get connecting() {
    return this.transport ? this.transport.connecting : 0
  }

  status() {
    return { state: this.state, peers: this.connections.size }
  }

  // One service per process: managers are reused across toggles (there is no
  // removeService, an abandoned manager's service lingers in the shared GATT
  // db capturing dialers, and destroying them kills peer connections without
  // iOS ever delivering a disconnect — remote centrals zombie). Only after a
  // radio power cycle — which wipes the db — is a rebuild with fresh managers
  // safe (and necessary: cycled managers wedge). The l2cap listener IS
  // retracted and republished across a toggle: one that lives through
  // destroyed channels wedges silently.
  async start() {
    if (!this.supported || this.started || this.closing || this.closed) return
    this.started = true
    if (!this.suspended) await this._activate()
    this.emit('update')
  }

  async _activate() {
    if (this.transport && !this.transport.radioCycled) {
      this.transport.resume()
    } else {
      this._abandon()
      this.transport = this._createTransport()
      await this.transport.ready()
    }
  }

  _abandon() {
    const old = this.transport
    if (!old) return
    this.transport = null
    old
      .suspend()
      .catch(safetyCatch)
      .then(() => {
        if (isMac) old.destroyManagers()
      })
  }

  _createTransport() {
    const gen = ++this._gen
    const transport = new BLETransport({
      ...this._opts,
      gen,
      backend: this._backend,
      online: this._online,
      onconnection: (conn) => this.emit('connection', conn)
    })
    transport.on('update', () => this.emit('update'))
    transport.on('radio-cycled', () => this._rebuild(transport))
    return transport
  }

  // A radio power cycle wedges the surviving native managers — abandon them
  // (never destroy: native double-free) and start over with fresh ones.
  async _rebuild(old) {
    if (this.transport !== old || this.closing || this.closed) return
    this._abandon()
    if (this.started && !this.suspended) {
      this.transport = this._createTransport()
      await this.transport.ready().catch(safetyCatch)
    }
    this.emit('update')
  }

  async stop() {
    if (!this.started) return
    this.started = false
    if (this.transport && !this.suspended) await this.transport.suspend()
    this.emit('update')
  }

  async suspend() {
    if (this.suspended) return
    this.suspended = true
    if (this.started && this.transport) await this.transport.suspend()
    this.emit('update')
  }

  async resume() {
    if (!this.suspended) return
    this.suspended = false
    if (this.started) await this._activate()
    this.emit('update')
  }

  destroy() {
    return this.close()
  }

  setOnline(online) {
    this._online = online === true
    if (this.transport) this.transport.setOnline(this._online)
  }

  // Switch the single active topic. Applies live when the radio is up, and
  // sticks for every later start/rebuild.
  async setTopic(topic) {
    this._opts = { ...this._opts, topic }
    if (this.transport) await this.transport.setTopic(topic)
    this.emit('update')
  }

  async _close() {
    this.started = false
    if (this.transport) await this.transport.close()
  }
}

module.exports.toServiceUUID = BLETransport.toServiceUUID
