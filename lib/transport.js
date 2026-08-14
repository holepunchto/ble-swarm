const NoiseSecretStream = require('@hyperswarm/secret-stream')
const { isAndroid } = require('which-runtime')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const { hash, randomBytes } = require('hypercore-crypto')
const safetyCatch = require('safety-catch')

const GattStream = require('./gatt-stream')

// iOS never answers an L2CAP channel opened by a Mac central and gives no L2CAP
// disconnect signal, so one data characteristic (write + notify) carries a
// framed byte stream both ways instead.
const DATA_UUID = 'ce1a0004-0000-1000-8000-00805f9b34fb'

// Wire frame both directions: [type:1][sessionId:8][payload].
// OPEN carries the initiator's 32-byte public key, HELLO the responder's —
// both sides can refuse a link (dup peer, policy) before any Noise work.
const TYPE_OPEN = 1
const TYPE_DATA = 2
const TYPE_CLOSE = 3
const TYPE_HELLO = 4
const SID_LEN = 8
const HEADER = 1 + SID_LEN
const KEY_LEN = 32

const DEFAULT_MAX_OUTBOUND = 4
const DEFAULT_MAX_INBOUND = 8
const CONNECT_TIMEOUT = 15000
// per-peer dial backoff: eager while unlinked, patient once linked; the
// cooldown grows exponentially per consecutive failure.
const DIAL_COOLDOWN_BASE = 8000
const DIAL_COOLDOWN_BASE_LONELY = 2000
const DIAL_COOLDOWN_MAX = 30000
// one radio can't usefully dial faster than this
const DIAL_MIN_INTERVAL = 500
// iOS reports a peripheral once per scan session; restart to re-report a
// re-advertised peer.
const SCAN_RESTART_LONELY = 5000
// continuous scanning is the dominant battery cost, so duty-cycle it once
// linked — and even while lonely when the internet path is up.
const SCAN_DUTY_ON = 5000
const SCAN_DUTY_OFF = 25000
const SCAN_DUTY_OFF_ONLINE = 55000
// iOS never signals disconnect for a vanished peer: keepalive pings and the
// timeout are the liveness detector.
const KEEPALIVE = 5000
const TIMEOUT = 15000
// Android lets the central ask for the max; the peer answers with what it got
const REQUEST_MTU = 517
const DRAIN_MS = 300
const NOTIFY_RETRY = 250
const WRITE_TIMEOUT = 8000

const EMPTY = b4a.alloc(0)

const STATE = {
  poweredOn: 'on',
  poweredOff: 'waiting',
  unauthorized: 'unauthorized',
  unsupported: 'unsupported',
  resetting: 'waiting',
  unknown: 'waiting'
}

function frame(type, sid, payload = EMPTY) {
  const out = b4a.allocUnsafe(HEADER + payload.byteLength)
  out[0] = type
  b4a.copy(sid, out, 1)
  if (payload.byteLength) b4a.copy(payload, out, HEADER)
  return out
}

function parseFrame(buf) {
  if (!buf || buf.byteLength < HEADER) return null
  const sid = buf.subarray(1, HEADER)
  return {
    type: buf[0],
    sid,
    sidHex: b4a.toString(sid, 'hex'),
    payload: buf.subarray(HEADER)
  }
}

// Derive a stable 128-bit BLE service UUID from a topic. Only devices that
// compute the same UUID ever discover each other.
function toServiceUUID(topic, tag = 'keet-ble') {
  const h = hash([b4a.from(tag), topic])
  const hex = b4a.toString(h.subarray(0, 16), 'hex')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`
}

const uuidEq = (a, b) =>
  String(a || '')
    .toLowerCase()
    .replace(/-/g, '') ===
  String(b || '')
    .toLowerCase()
    .replace(/-/g, '')

const findByUUID = (items, uuid) => (items || []).find((i) => uuidEq(i.uuid, uuid)) || null

/**
 * Dual-role BLE transport: advertises + scans one service UUID and turns each
 * discovered peer into a NoiseSecretStream, delivered via `onconnection` once
 * the handshake completes and duplicate links are retired. The server adds one
 * data characteristic (write + notify); framed bytes flow central→server as
 * GATT writes and server→central as notifications. `backend` is bare-bluetooth
 * in production and a mock in tests.
 */
module.exports = class BLETransport extends ReadyResource {
  constructor({
    backend,
    keyPair,
    topic,
    onconnection,
    shouldConnect,
    tag = 'keet-ble',
    maxOutbound = DEFAULT_MAX_OUTBOUND,
    maxInbound = DEFAULT_MAX_INBOUND,
    writePayload = 0,
    writeWithResponse = true,
    scanOptions,
    online = false,
    debug = false,
    gen = 0
  }) {
    super()
    this.backend = backend
    this.keyPair = keyPair
    this.serviceUUID = toServiceUUID(topic, tag)
    this.onconnection = onconnection
    this.shouldConnect = shouldConnect || null
    this.maxOutbound = maxOutbound
    this.maxInbound = maxInbound
    this.writePayload = writePayload
    this.writeWithResponse = writeWithResponse
    this.scanOptions =
      scanOptions === undefined && isAndroid && backend
        ? { scanMode: backend.Central.SCAN_MODE_LOW_LATENCY }
        : scanOptions

    this.state = 'off'
    this.central = null
    this.server = null
    /** live links keyed by remote public key hex */
    this.peers = new Map()

    this._dataChar = null
    /** sessionId hex → { stream, sid } for server-side (peripheral) sessions */
    this._sessions = new Map()
    /** serialized server notify queue: { frame, resolve, reject } */
    this._notifyQueue = []
    this._scanning = false
    this._advertising = false
    this._serviceAdded = false
    /** peripheral id → per-peer dial state */
    this._devices = new Map()
    this._lastDial = 0
    this._scanTimer = null
    this._suspended = false
    this._online = online
    this._notifyRetry = null
    this._radioWasDown = false
    this._healing = false
    this._debug = debug === true
    this._gen = gen
    this._log('transport created')
  }

  _log(...args) {
    if (this._debug) console.log(`[ble#${this._gen}]`, ...args) // eslint-disable-line no-console
  }

  get linkCount() {
    return this.peers.size
  }

  // Relax the scan cadence while the internet path is up — BLE is a fallback
  // then, not the lifeline.
  setOnline(online) {
    online = online === true
    if (online === this._online) return
    this._online = online
    if (this._suspended || this.closing || this.closed) return
    if (!this._scanning) this._startScan()
    else this._armScanRestart()
  }

  _device(id) {
    let d = this._devices.get(id)
    if (!d) {
      d = {
        timer: null,
        retry: null,
        linked: false,
        coolUntil: 0,
        failures: 0,
        peerKey: null,
        peripheral: null
      }
      this._devices.set(id, d)
    }
    return d
  }

  _prune(id) {
    const d = this._devices.get(id)
    if (
      d &&
      !d.timer &&
      !d.retry &&
      !d.linked &&
      !d.coolUntil &&
      !d.failures &&
      !d.peerKey &&
      !d.peripheral
    ) {
      this._devices.delete(id)
    }
  }

  _clearDeviceTimers() {
    for (const d of this._devices.values()) {
      if (d.timer) clearTimeout(d.timer)
      if (d.retry) clearTimeout(d.retry)
      d.timer = null
      d.retry = null
    }
  }

  // Both sides can refuse a link before any Noise work: our own reflection,
  // or a peer the host already reaches another way (policy callback).
  _accept(publicKey) {
    if (publicKey.byteLength !== KEY_LEN) return false
    if (b4a.equals(publicKey, this.keyPair.publicKey)) return false
    if (this.shouldConnect && !this.shouldConnect(publicKey)) return false
    return true
  }

  // Deterministic direction: the smaller static key is the one that initiates
  _weInitiate(remoteKey) {
    return b4a.compare(this.keyPair.publicKey, remoteKey) < 0
  }

  _initiatorWins(initiatorKey) {
    return b4a.compare(initiatorKey, this.keyPair.publicKey) < 0
  }

  async _open() {
    const { Central, Server, Service, Characteristic } = this.backend

    this.server = new Server()
    this.server.on('stateChange', (s) => {
      this._log('server state', s)
      this._onState(s)
      if (s === 'poweredOn') this._startServer(Service, Characteristic)
      else if (s === 'poweredOff' || s === 'resetting') this._onRadioDown()
    })
    this.server.on('serviceAdd', () => {
      this._serviceAdded = true
      this._maybeAdvertise()
    })
    this.server.on('subscribe', (...args) => this._log('server subscribe', args[1] ?? ''))
    this.server.on('writeRequest', (reqs) => this._onWriteRequests(reqs))
    this.server.on('readyToUpdate', () => this._drainNotify())
    // writeRequests carry no central identifier, so an unsubscribe can't be
    // mapped to a session; teardown is left to the keepalive/timeout.
    this.server.on('unsubscribe', (...args) => this._log('server unsubscribe', args[1] ?? ''))
    this.server.on('error', safetyCatch)

    this.central = new Central()
    this.central.on('stateChange', (s) => {
      this._log('central state', s)
      this._onState(s)
      if (s === 'poweredOn') {
        if (this._radioWasDown) this._scheduleSelfHeal()
        else this._startScan()
      } else if (s === 'poweredOff' || s === 'resetting') this._onRadioDown()
    })
    this.central.on('discover', (peripheral) => this._onDiscover(peripheral))
    this.central.on('connect', (peripheral) => this._onConnect(peripheral))
    this.central.on('disconnect', () => {})
    this.central.on('error', (err) => this._onCentralError(err))

    this._startServer(Service, Characteristic)
    this._startScan()
    this.state = 'starting'
  }

  _startServer(Service, Characteristic) {
    if (this._suspended || this.closing || this.closed) return
    if (this.server.state !== 'poweredOn') return
    if (!this._serviceAdded) {
      this._dataChar = new Characteristic(DATA_UUID, {
        write: true,
        writeWithoutResponse: true,
        notify: true
      })
      this.server.addService(new Service(this.serviceUUID, [this._dataChar]))
    }
  }

  _maybeAdvertise() {
    if (this._advertising || !this._serviceAdded) return
    this._advertising = true
    this._log('advertising', this.serviceUUID)
    this.server.startAdvertising({ serviceUUIDs: [this.serviceUUID] })
  }

  // ─── server (peripheral) side ─────────────────────────────────────────────

  _onWriteRequests(requests) {
    const ok = this.server.constructor.ATT_SUCCESS ?? 0
    for (const req of requests) {
      // must respond within ms or the central times out — before any parsing
      if (req.responseNeeded !== false) this.server.respondToRequest(req, ok)
      if (this._suspended) continue
      this._onServerFrame(req.data)
    }
  }

  _onServerFrame(data) {
    const f = parseFrame(data)
    if (!f) return
    if (f.type === TYPE_OPEN) {
      if (this._sessions.has(f.sidHex)) return
      if (
        this._sessions.size >= this.maxInbound ||
        !this._accept(f.payload) ||
        !this._initiatorWins(f.payload)
      ) {
        // wrong direction or unwanted peer: refuse with a CLOSE so the dialer
        // backs off. Only the smaller key ever initiates — duplicate channels
        // share one radio link and tearing one down kills both.
        this._log('server refused OPEN', f.sidHex)
        this._enqueueNotify(frame(TYPE_CLOSE, f.sid)).catch(safetyCatch)
        return
      }
      this._log('server accepted OPEN', f.sidHex)
      const sid = b4a.from(f.sid) // copy: f.sid views the transient request buffer
      const stream = new GattStream({
        send: (payload) => this._enqueueNotify(frame(TYPE_DATA, sid, payload)),
        onclose: () => this._closeServerSession(f.sidHex, sid)
      })
      const session = { stream, sid, conn: null }
      this._sessions.set(f.sidHex, session)
      session.conn = this._onChannel(stream, false, null)
      this._enqueueNotify(frame(TYPE_HELLO, sid, this.keyPair.publicKey)).catch(safetyCatch)
    } else if (f.type === TYPE_DATA) {
      const s = this._sessions.get(f.sidHex)
      if (s) s.stream.receive(b4a.from(f.payload))
    } else if (f.type === TYPE_CLOSE) {
      const s = this._sessions.get(f.sidHex)
      if (s) {
        this._log('server session closed by remote', f.sidHex)
        this._sessions.delete(f.sidHex)
        s.stream.remoteEnd()
      }
    }
  }

  _closeServerSession(sidHex, sid) {
    if (!this._sessions.has(sidHex)) return
    this._sessions.delete(sidHex)
    this._enqueueNotify(frame(TYPE_CLOSE, sid)).catch(safetyCatch)
  }

  // Serialize notifications through the single characteristic: updateValue
  // returns false when the peripheral's queue is full — hold the head frame and
  // retry on the next 'readyToUpdate', preserving order.
  _enqueueNotify(f) {
    return new Promise((resolve, reject) => {
      this._notifyQueue.push({ frame: f, resolve, reject })
      this._drainNotify()
    })
  }

  _drainNotify() {
    if (this._notifyRetry) {
      clearTimeout(this._notifyRetry)
      this._notifyRetry = null
    }
    while (this._notifyQueue.length) {
      const item = this._notifyQueue[0]
      let ok
      try {
        ok = this.server.updateValue(this._dataChar, item.frame)
      } catch (err) {
        this._notifyQueue.shift()
        item.reject(err)
        continue
      }
      if (!ok) {
        // readyToUpdate is the fast path but can be missed — never stall the
        // queue waiting for it
        this._notifyRetry = setTimeout(() => {
          this._notifyRetry = null
          this._drainNotify()
        }, NOTIFY_RETRY)
        if (this._notifyRetry.unref) this._notifyRetry.unref()
        return
      }
      this._notifyQueue.shift()
      item.resolve()
    }
  }

  // ─── scanning ─────────────────────────────────────────────────────────────

  _startScan() {
    if (this._suspended || this.closing || this.closed) return
    if (this._scanning || this.central.state !== 'poweredOn') return
    this._scanning = true
    this._log('scan start')
    this.central.startScan([this.serviceUUID], this.scanOptions)
    this._armScanRestart()
  }

  // Lonely + offline: cycle the scan every SCAN_RESTART_LONELY so a
  // re-advertised peer is re-reported. Linked (or lonely-but-online):
  // duty-cycle on/dark to save the battery for when BLE is the only path.
  _armScanRestart() {
    if (this._scanTimer) clearTimeout(this._scanTimer)
    const duty = this.linkCount > 0 || this._online
    const dark = this.linkCount > 0 ? SCAN_DUTY_OFF : SCAN_DUTY_OFF_ONLINE
    const delay = duty ? (this._scanning ? SCAN_DUTY_ON : dark) : SCAN_RESTART_LONELY
    this._scanTimer = setTimeout(() => {
      this._scanTimer = null
      if (this.closing || this.closed || this._suspended) return
      // never toggle the scan mid-dial (would kill the connect) — defer a phase
      if (this._isDialing()) {
        this._armScanRestart()
        return
      }
      if (this.linkCount > 0 || this._online) {
        if (this._scanning) {
          this._stopScan()
          this._armScanRestart()
        } else {
          this._startScan()
        }
      } else {
        this._stopScan()
        this._startScan()
      }
    }, delay)
    if (this._scanTimer.unref) this._scanTimer.unref()
  }

  _stopScan() {
    if (!this._scanning) return
    this._scanning = false
    try {
      this.central.stopScan()
    } catch (err) {
      safetyCatch(err)
    }
  }

  // A radio power-cycle invalidates the GATT service, advertising, scans,
  // subscriptions and every open link — reset the bookkeeping so the
  // poweredOn handlers bootstrap everything from scratch.
  _onRadioDown() {
    this._log('radio down — resetting')
    this._radioWasDown = true
    this.radioCycled = true // the OS wiped the GATT db — a rebuild is safe
    // Apple and Android both drop the GATT db on power-off, so the service
    // must be re-added on recovery (verified: without it, dialers find no
    // service and never even reach OPEN).
    this._serviceAdded = false
    this._advertising = false
    this._scanning = false
    this._clearDeviceTimers()
    this._devices.clear()
    for (const conn of this.peers.values()) {
      try {
        conn.destroy()
      } catch (err) {
        safetyCatch(err)
      }
    }
    for (const { stream } of this._sessions.values()) {
      try {
        stream.destroy()
      } catch (err) {
        safetyCatch(err)
      }
    }
    this._sessions.clear()
    if (this._notifyRetry) clearTimeout(this._notifyRetry)
    this._notifyRetry = null
    for (const item of this._notifyQueue) item.reject(new Error('radio down'))
    this._notifyQueue = []
  }

  // After a power cycle the surviving native managers are unreliable (writes
  // and subscriptions still flow but notifications die silently), so recovery
  // needs brand-new manager objects — signal the owner to rebuild.
  _scheduleSelfHeal() {
    if (!this._radioWasDown) return
    if (this._healing || this.closing || this.closed || this._suspended) return
    this._radioWasDown = false
    this._healing = true
    this._log('radio recovered — requesting rebuild')
    this.emit('radio-cycled')
  }

  _onState(raw) {
    const next = STATE[raw] ?? 'waiting'
    if (next === this.state) return
    this._log('state', raw, '->', next)
    this.state = next
    this.emit('update')
  }

  // ─── central side ─────────────────────────────────────────────────────────

  _onDiscover(peripheral) {
    if (this.closing || this.closed || this._suspended) return
    const d = this._devices.get(peripheral.id)
    if (d) {
      if (d.linked) return
      if (d.peerKey && this.peers.has(d.peerKey)) return
      if (d.coolUntil > Date.now()) return
      if (d.timer) return
    }
    if (this.linkCount >= this.maxOutbound) return // gossip covers the rest
    if (Date.now() - this._lastDial < DIAL_MIN_INTERVAL) {
      // don't drop a rate-limited discovery — the radio may not re-report it
      // until the next scan cycle
      const dev = this._device(peripheral.id)
      if (!dev.retry) {
        dev.retry = setTimeout(() => {
          dev.retry = null
          this._onDiscover(peripheral)
        }, DIAL_MIN_INTERVAL)
        if (dev.retry.unref) dev.retry.unref()
      }
      return
    }
    // dial every discovery; a redundant link is dropped by _track's dedup
    this._lastDial = Date.now()
    this._log('dialing', peripheral.id)
    const timer = setTimeout(() => this._abortDial(peripheral, 'timeout'), CONNECT_TIMEOUT)
    this._device(peripheral.id).timer = timer
    try {
      this._stopScan()
      this.central.connect(peripheral)
    } catch (err) {
      this._abortDial(peripheral, err)
    }
  }

  _onConnect(peripheral) {
    this._device(peripheral.id).peripheral = peripheral
    peripheral.on('error', () => this._abortDial(peripheral, 'peripheral-error'))
    peripheral.once('servicesDiscover', (services) => {
      const svc = findByUUID(services, this.serviceUUID)
      if (svc) peripheral.discoverCharacteristics(svc, [DATA_UUID])
      else this._abortDial(peripheral, 'no-service')
    })
    peripheral.once('characteristicsDiscover', (_svc, chars) => {
      const dataChar = findByUUID(chars, DATA_UUID)
      if (dataChar) peripheral.subscribe(dataChar)
      else this._abortDial(peripheral, 'no-data-char')
    })
    peripheral.once('notifyState', (char, isNotifying) => {
      if (!isNotifying) {
        this._abortDial(peripheral, 'subscribe-failed')
        return
      }
      this._startCentralSession(peripheral, char)
    })
    // both sides open a session: on iOS the peer id isn't known until after
    // connect, so a tie-break yields only post-handshake and deadlocks if the
    // other side never dials back. _track keeps the first, drops the dup.
    peripheral.discoverServices([this.serviceUUID])
  }

  // Send OPEN with our key, then wait for the server's HELLO (or CLOSE) before
  // any Noise work — the dial timer keeps running until the server answers.
  _startCentralSession(peripheral, char) {
    const sid = randomBytes(SID_LEN)
    peripheral._session = { sidHex: b4a.toString(sid, 'hex'), sid }
    peripheral._char = char
    peripheral.on('notify', (_char, data) => this._onCentralNotify(peripheral, data))
    peripheral.on('mtuChanged', (mtu) => {
      peripheral._mtu = mtu
      if (peripheral._stream) peripheral._stream.maxPayload = this._payloadFor(peripheral)
    })
    this._log('central sent OPEN', peripheral.id)
    this._centralSend(peripheral, char, frame(TYPE_OPEN, sid, this.keyPair.publicKey), true).catch(
      safetyCatch
    )
    try {
      peripheral.requestMtu(REQUEST_MTU) // no-op on Apple, mtuChanged on Android
    } catch (err) {
      safetyCatch(err)
    }
  }

  _payloadFor(peripheral) {
    const budget = peripheral._mtu ? peripheral._mtu - 3 - HEADER : this.writePayload
    return budget > 0 ? budget : undefined
  }

  _onCentralNotify(peripheral, data) {
    const sess = peripheral._session
    if (!sess) return
    const f = parseFrame(data)
    if (!f || f.sidHex !== sess.sidHex) return
    if (f.type === TYPE_HELLO) {
      if (peripheral._stream) return
      if (!this._accept(f.payload)) {
        this._log('central refused HELLO', peripheral.id)
        this._refuseCentral(peripheral, sess)
        return
      }
      if (!this._weInitiate(f.payload)) {
        // bigger key yields: the peer's central dials our server instead.
        // Remember the mapping so rediscovery stays quiet while their link
        // lives, and NEVER hang up the radio — both directions share one
        // physical link, so a disconnect here kills their channel too.
        this._device(peripheral.id).peerKey = b4a.toString(f.payload, 'hex')
        this._log('central yielded to', peripheral.id)
        this._yieldCentral(peripheral, sess)
        return
      }
      this._log('central got HELLO, opening stream', peripheral.id)
      this._openCentralStream(peripheral, sess)
    } else if (f.type === TYPE_DATA) {
      if (peripheral._stream) peripheral._stream.receive(b4a.from(f.payload))
    } else if (f.type === TYPE_CLOSE) {
      peripheral._session = null
      if (peripheral._stream) {
        peripheral._stream.remoteEnd()
      } else {
        // refused pre-handshake — long cooldown so rediscovery doesn't spin
        const d = this._device(peripheral.id)
        d.coolUntil = Date.now() + DIAL_COOLDOWN_MAX
        d.peripheral = null
        this._clearDial(peripheral.id)
        try {
          this.central.disconnect(peripheral)
        } catch (err) {
          safetyCatch(err)
        }
        this._startScan()
      }
    }
  }

  // close our probe session but keep the shared radio link alive
  _yieldCentral(peripheral, sess) {
    this._centralSend(peripheral, peripheral._char, frame(TYPE_CLOSE, sess.sid), true).catch(
      safetyCatch
    )
    peripheral._session = null
    const d = this._device(peripheral.id)
    d.coolUntil = Date.now() + DIAL_COOLDOWN_MAX
    d.peripheral = peripheral
    this._clearDial(peripheral.id)
    this._startScan()
  }

  _refuseCentral(peripheral, sess) {
    this._centralSend(peripheral, peripheral._char, frame(TYPE_CLOSE, sess.sid), true).catch(
      safetyCatch
    )
    peripheral._session = null
    const d = this._device(peripheral.id)
    d.coolUntil = Date.now() + DIAL_COOLDOWN_MAX
    d.peripheral = null
    this._clearDial(peripheral.id)
    try {
      this.central.disconnect(peripheral)
    } catch (err) {
      safetyCatch(err)
    }
    this._startScan()
  }

  _openCentralStream(peripheral, sess) {
    const stream = new GattStream({
      maxPayload: this._payloadFor(peripheral),
      send: (payload) =>
        this._centralSend(peripheral, peripheral._char, frame(TYPE_DATA, sess.sid, payload)),
      onclose: () => {
        this._centralSend(
          peripheral,
          peripheral._char,
          frame(TYPE_CLOSE, sess.sid),
          true
        ).catch(safetyCatch)
        const d = this._devices.get(peripheral.id)
        if (d) d.peripheral = null
        try {
          this.central.disconnect(peripheral)
        } catch (err) {
          safetyCatch(err)
        }
      }
    })
    peripheral._stream = stream
    this._clearDial(peripheral.id)
    peripheral._conn = this._onChannel(stream, true, peripheral.id)
  }

  // One write in flight per peripheral: chain each write behind the previous.
  // Control frames always use acknowledged writes; DATA follows the configured
  // mode (without-response skips a radio round-trip per chunk).
  _centralSend(peripheral, char, f, withResponse = this.writeWithResponse) {
    const prev = peripheral._writeChain || Promise.resolve()
    const next = prev.then(() => this._writeOnce(peripheral, char, f, withResponse))
    peripheral._writeChain = next.catch(safetyCatch) // keep the chain alive
    return next
  }

  _writeOnce(peripheral, char, f, withResponse) {
    if (!withResponse) {
      return new Promise((resolve, reject) => {
        try {
          peripheral.write(char, f, false)
          resolve()
        } catch (err) {
          reject(err)
        }
      })
    }
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer)
        peripheral.removeListener('write', onWrite)
        peripheral.removeListener('error', onErr)
      }
      const onWrite = () => {
        cleanup()
        resolve()
      }
      const onErr = (err) => {
        cleanup()
        reject(err)
      }
      // a dropped completion event must not wedge the write chain — the
      // keepalive/timeout layer decides whether the link itself is dead
      const timer = setTimeout(() => {
        cleanup()
        reject(new Error('write completion timed out'))
      }, WRITE_TIMEOUT)
      if (timer.unref) timer.unref()
      peripheral.once('write', onWrite)
      peripheral.once('error', onErr)
      try {
        peripheral.write(char, f, true)
      } catch (err) {
        cleanup()
        reject(err)
      }
    })
  }

  _abortDial(peripheral, reason) {
    this._log('dial abort', peripheral?.id, reason)
    const id = peripheral?.id
    if (id != null) {
      const d = this._device(id)
      d.failures += 1
      const base = this.linkCount === 0 ? DIAL_COOLDOWN_BASE_LONELY : DIAL_COOLDOWN_BASE
      d.coolUntil =
        Date.now() + Math.min(DIAL_COOLDOWN_MAX, base * 2 ** Math.min(4, d.failures - 1))
      d.peripheral = null
    }
    this._clearDial(id)
    try {
      this.central.disconnect(peripheral)
    } catch (err) {
      safetyCatch(err)
    }
    this._startScan()
  }

  _clearDial(id) {
    if (id == null) return
    const d = this._devices.get(id)
    if (!d) return
    if (d.timer) clearTimeout(d.timer)
    d.timer = null
    this._prune(id)
  }

  _isDialing() {
    for (const d of this._devices.values()) if (d.timer) return true
    return false
  }

  // iOS & Android report errored connects as 'error' with a code, not
  // 'disconnect' — without this a failed dial pins the peripheral forever.
  _onCentralError(err) {
    safetyCatch(err)
    const code = err && err.code
    if (code === 'CONNECTION_FAILED' || code === 'DISCONNECT') {
      for (const [id, d] of this._devices) if (d.timer) this._clearDial(id)
    }
  }

  // ─── links ────────────────────────────────────────────────────────────────

  _onChannel(stream, isInitiator, peripheralId) {
    if (this.closing || this.closed || this._suspended) {
      stream.destroy()
      return null
    }
    this._log('channel created, initiator:', isInitiator)
    const conn = new NoiseSecretStream(isInitiator, stream, { keyPair: this.keyPair })
    if (conn.rawStream && conn.rawStream.rtt === undefined) conn.rawStream.rtt = 0
    conn.setKeepAlive(KEEPALIVE)
    conn.setTimeout(TIMEOUT)
    conn.on('error', safetyCatch)
    stream.on('error', safetyCatch)
    // marked at channel-open (not handshake-open): rediscovery must not dial a
    // peripheral whose channel is still handshaking
    if (peripheralId != null) {
      const d = this._device(peripheralId)
      d.linked = true
      d.coolUntil = 0
      d.failures = 0
      conn.once('close', () => {
        const rec = this._devices.get(peripheralId)
        if (rec) {
          rec.linked = false
          this._prune(peripheralId)
        }
      })
    }
    conn.on('open', () => this._track(conn, peripheralId, isInitiator))
    conn.on('close', () => this._untrack(conn))
    this._startScan()
    return conn
  }

  _track(conn, peripheralId, isInitiator) {
    if (peripheralId != null && conn.remotePublicKey) {
      this._device(peripheralId).peerKey = b4a.toString(conn.remotePublicKey, 'hex')
    }
    if (this.closing || this.closed) return
    const key = b4a.toString(conn.remotePublicKey, 'hex')
    const existing = this.peers.get(key)
    if (existing && existing !== conn) {
      // Both sides dial, so a pair links twice. Dropping a channel closes it for
      // BOTH devices, so both must retire the SAME channel. Deterministic
      // winner: keep the channel whose initiator has the smaller static key.
      const initiatorIsUsSmaller = b4a.compare(conn.publicKey, conn.remotePublicKey) < 0
      const preferred = isInitiator === initiatorIsUsSmaller
      if (!preferred) {
        conn.destroy()
        return
      }
    }
    this._log('track', key.slice(0, 12), 'initiator:', isInitiator)
    conn._peripheralId = peripheralId
    conn._peerKey = key
    this.peers.set(key, conn)
    if (existing && existing !== conn) existing.destroy() // retire the loser channel
    if (this.onconnection) this.onconnection(conn)
    this.emit('update')
  }

  _untrack(conn) {
    const key = conn._peerKey
    this._log('untrack', (key || '').slice(0, 12))
    if (key && this.peers.get(key) === conn) this.peers.delete(key)
    if (!this.closing && !this.closed) {
      // last link gone → hunt immediately instead of waiting out a dark window
      if (this.linkCount === 0 && !this._suspended) this._startScan()
      this.emit('update')
    }
  }

  // Best-effort TYPE_CLOSE to every live session, then wait up to DRAIN_MS for
  // the frames to flush. Resolves regardless: suspend must never hang on a
  // wedged radio.
  async _sayGoodbye() {
    const sent = []
    for (const { sid } of this._sessions.values()) {
      sent.push(this._enqueueNotify(frame(TYPE_CLOSE, sid)).catch(safetyCatch))
    }
    for (const d of this._devices.values()) {
      const peripheral = d.peripheral
      const sess = peripheral && peripheral._session
      if (!sess || !peripheral._char) continue
      const f = frame(TYPE_CLOSE, sess.sid)
      sent.push(this._centralSend(peripheral, peripheral._char, f, true).catch(safetyCatch))
    }
    if (!sent.length) return
    await Promise.race([Promise.all(sent), new Promise((r) => setTimeout(r, DRAIN_MS))])
  }

  /**
   * Pause radio activity but KEEP the Server/Central instances and the
   * registered GATT service alive — the toggle-friendly counterpart to _close.
   * CoreBluetooth managers can't be destroy()ed (native double-free), so one
   * transport is reused across toggles rather than recreated. Idempotent.
   */
  async suspend() {
    this._log('suspending')
    this._suspended = true
    if (this._scanTimer) clearTimeout(this._scanTimer)
    this._scanTimer = null
    this._clearDeviceTimers()
    // goodbye BEFORE teardown so the remote reacts in <1s instead of waiting
    // out the keepalive: close frames, a short drain, then an ACL disconnect.
    await this._sayGoodbye()
    for (const d of this._devices.values()) {
      if (!d.peripheral) continue
      try {
        this.central.disconnect(d.peripheral)
      } catch (err) {
        safetyCatch(err)
      }
    }
    this._devices.clear()
    try {
      this._stopScan()
    } catch (err) {
      safetyCatch(err)
    }
    try {
      this.server?.stopAdvertising()
    } catch (err) {
      safetyCatch(err)
    }
    this._advertising = false
    for (const conn of this.peers.values()) {
      try {
        conn.destroy()
      } catch (err) {
        safetyCatch(err)
      }
    }
    this.peers.clear()
    for (const { stream } of this._sessions.values()) {
      try {
        stream.destroy()
      } catch (err) {
        safetyCatch(err)
      }
    }
    this._sessions.clear()
    if (this._notifyRetry) clearTimeout(this._notifyRetry)
    this._notifyRetry = null
    for (const item of this._notifyQueue) item.reject(new Error('suspended'))
    this._notifyQueue = []
    this.state = 'off'
    this.emit('update')
  }

  /**
   * Restart advertising + scanning on the SAME Server/Central. The service was
   * never removed so advertising resumes immediately. Idempotent.
   */
  resume() {
    this._suspended = false
    if (this.closing || this.closed) return
    this._advertising = false
    this._maybeAdvertise()
    this._startScan()
    const raw = this.central?.state ?? this.server?.state
    this.state = STATE[raw] ?? 'waiting'
    this.emit('update')
  }

  // macOS only: destroying the managers is safe there (verified) and is the
  // only way to truly unregister a zombie service — there is no removeService.
  // On iOS destroy() double-frees natively, so phones keep the managers.
  destroyManagers() {
    if (this._managersDestroyed) return
    this._managersDestroyed = true
    this._log('destroying managers')
    try {
      if (typeof this.server?.destroy === 'function') this.server.destroy()
    } catch (err) {
      safetyCatch(err)
    }
    try {
      if (typeof this.central?.destroy === 'function') this.central.destroy()
    } catch (err) {
      safetyCatch(err)
    }
  }

  async _close() {
    if (this._scanTimer) clearTimeout(this._scanTimer)
    this._scanTimer = null
    // never call central/server.destroy() — it double-frees in native teardown;
    // stop advertising/scanning and let the runtime reclaim.
    this._clearDeviceTimers()
    this._devices.clear()
    try {
      this.central?.stopScan()
    } catch (err) {
      safetyCatch(err)
    }
    try {
      this.server?.stopAdvertising()
    } catch (err) {
      safetyCatch(err)
    }
    for (const conn of this.peers.values()) {
      try {
        conn.destroy()
      } catch (err) {
        safetyCatch(err)
      }
    }
    this.peers.clear()
    if (this._notifyRetry) clearTimeout(this._notifyRetry)
    this._notifyRetry = null
    for (const item of this._notifyQueue) item.reject(new Error('closed'))
    this._notifyQueue = []
    this._sessions.clear()
    this.state = 'off'
  }
}

module.exports.toServiceUUID = toServiceUUID
