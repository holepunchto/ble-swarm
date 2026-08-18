const NoiseSecretStream = require('@hyperswarm/secret-stream')
const { isAndroid, isMac } = require('which-runtime')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const { hash, randomBytes } = require('hypercore-crypto')
const safetyCatch = require('safety-catch')

const GattPipe = require('./gatt')
const L2CapPipe = require('./l2cap')
const {
  TYPE_OPEN,
  TYPE_DATA,
  TYPE_CLOSE,
  TYPE_HELLO,
  SID_LEN,
  KEY_LEN,
  PIPE_L2CAP,
  encodeFrame,
  decodeFrame,
  encodeKeyPayload,
  decodeKeyPayload
} = require('./frames')

// iOS never answers an L2CAP channel opened by a Mac central and gives no L2CAP
// disconnect signal, so one data characteristic (write + notify) carries the
// control frames — and, on the gatt pipe, the data too.
const DATA_UUID = 'ce1a0004-0000-1000-8000-00805f9b34fb'

// a pending server session must see its pipe (channel id or first GATT data)
// within this window or it is reaped
const PIPE_PENDING_TIMEOUT = 20000

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
// after losing the last link or resuming from a toggle, hunt at the eager
// cadence even while online — a peer that just vanished is probably coming
// back, and the 55s dark windows would make relinking feel broken
const HUNT_WINDOW = 60000
// iOS never signals disconnect for a vanished peer: keepalive pings and the
// timeout are the liveness detector.
const KEEPALIVE = 5000
const TIMEOUT = 15000
// Android lets the central ask for the max; the peer answers with what it got
const REQUEST_MTU = 517
const DRAIN_MS = 300

const STATE = {
  poweredOn: 'on',
  poweredOff: 'waiting',
  unauthorized: 'unauthorized',
  unsupported: 'unsupported',
  resetting: 'waiting',
  unknown: 'waiting'
}

// Derive a stable 128-bit BLE service UUID from a topic. Only devices that
// compute the same UUID ever discover each other.
function toServiceUUID(topic, tag = 'ble-swarm') {
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
 * Dual-role BLE control plane: advertises + scans one service UUID, negotiates
 * sessions over OPEN/HELLO/CLOSE frames, picks a data pipe (gatt or l2cap) per
 * session, and turns each link into a NoiseSecretStream delivered via
 * `onconnection` once the handshake completes and duplicate links are retired.
 * The pipes own the byte movement; this class owns lifecycle, dial policy,
 * frame routing and link tracking. `backend` is bare-bluetooth in production
 * and a mock in tests.
 */
module.exports = class BLETransport extends ReadyResource {
  constructor({
    backend,
    keyPair,
    topic,
    onconnection,
    shouldConnect,
    tag = 'ble-swarm',
    maxOutbound = DEFAULT_MAX_OUTBOUND,
    maxInbound = DEFAULT_MAX_INBOUND,
    scanOptions,
    online = false,
    debug = false,
    gen = 0,
    // data pipe: 'l2cap' (default, a real channel per session) or 'gatt'
    // (frames over the characteristic). The dev picks one; peers on
    // mismatched pipes refuse each other loudly instead of silently degrading.
    pipe = 'l2cap',
    gatt = {},
    l2cap = {}
  }) {
    super()
    this.backend = backend
    this.keyPair = keyPair
    this.serviceUUID = toServiceUUID(topic, tag)
    this.onconnection = onconnection
    this.shouldConnect = shouldConnect || null
    this.maxOutbound = maxOutbound
    this.maxInbound = maxInbound
    this.scanOptions =
      scanOptions === undefined && isAndroid && backend
        ? { scanMode: backend.Central.SCAN_MODE_LOW_LATENCY }
        : scanOptions

    this.pipe = pipe
    this.state = 'off'
    this.central = null
    this.server = null
    /** live links keyed by remote public key hex */
    this.peers = new Map()

    this._gatt = new GattPipe(this, gatt)
    this._l2cap = new L2CapPipe(this, l2cap)
    this._pipes = { gatt: this._gatt, l2cap: this._l2cap }
    /** id → { id, stream, conn, pipeTimer } for server-side (peripheral) sessions */
    this._sessions = new Map()
    this._scanning = false
    this._advertising = false
    this._serviceAdded = false
    /** peripheral id → per-peer dial state */
    this._devices = new Map()
    /** rate-limited discoveries held for the next dial window */
    this._candidates = new Map()
    this._dialTimer = null
    this._lastDial = 0
    this._scanTimer = null
    this._huntUntil = 0
    this._cyclePending = false
    this._suspended = false
    this._online = online
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
    if (d && !d.timer && !d.linked && !d.coolUntil && !d.failures && !d.peerKey && !d.peripheral) {
      this._devices.delete(id)
    }
  }

  _clearDeviceTimers() {
    for (const d of this._devices.values()) {
      if (d.timer) clearTimeout(d.timer)
      d.timer = null
    }
    if (this._dialTimer) clearTimeout(this._dialTimer)
    this._dialTimer = null
    this._candidates.clear()
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

  // ─── pipe policy ──────────────────────────────────────────────────────────

  _capabilities() {
    return this.pipe === 'l2cap' ? PIPE_L2CAP : 0
  }

  // Decides a session's pipe from what both sides offered. Returns null when
  // no pipe suits both — links never silently ride the wrong pipe.
  _pipeFor(remoteFlags, psm) {
    if (this.pipe === 'gatt') return 'gatt'
    return (remoteFlags & PIPE_L2CAP) !== 0 && psm !== null ? 'l2cap' : null
  }

  // ─── lifecycle ────────────────────────────────────────────────────────────

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
    this.server.on('readyToUpdate', () => this._gatt.drain())
    this.server.on('channelPublish', (psm) => {
      this._l2cap.psm = psm
      this._log('l2cap channel published, psm', psm)
    })
    this.server.on('channelOpen', (channel) => this._onServerChannel(channel))
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
      this._gatt.dataChar = new Characteristic(DATA_UUID, {
        write: true,
        writeWithoutResponse: true,
        notify: true
      })
      this.server.addService(new Service(this.serviceUUID, [this._gatt.dataChar]))
    }
    if (this.pipe === 'l2cap') this._l2cap.publish()
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
    const f = decodeFrame(data)
    if (!f) return
    // an OPEN for a live session is a dup; every other type needs one
    const s = this._sessions.get(f.id)
    if (f.type === TYPE_OPEN) {
      if (s) return
      const { key, flags } = decodeKeyPayload(f.payload)
      if (
        this._sessions.size >= this.maxInbound ||
        !this._accept(key) ||
        !this._initiatorWins(key)
      ) {
        // wrong direction or unwanted peer — refuse so the dialer backs off
        this._log('server refused OPEN', f.id)
        this._notifyClose(f.id)
        return
      }
      const name = this._pipeFor(flags, this._l2cap.psm)
      if (name === null) {
        this._log('server refused OPEN, pipe mismatch', f.id)
        this._notifyClose(f.id)
        return
      }
      this._log('server accepted OPEN', f.id)
      const session = { id: f.id, stream: null, conn: null, pipeTimer: null }
      this._sessions.set(f.id, session)
      if (name === 'l2cap') {
        // the session has no stream until the central opens our L2CAP channel
        // and its id preamble matches — reap it if that never happens
        session.pipeTimer = setTimeout(() => {
          session.pipeTimer = null
          if (this._sessions.get(f.id) !== session || session.stream) return
          this._log('server pending pipe timed out', f.id)
          this._sessions.delete(f.id)
          this._notifyClose(f.id)
        }, PIPE_PENDING_TIMEOUT)
        if (session.pipeTimer.unref) session.pipeTimer.unref()
        const hello = encodeKeyPayload(this.keyPair.publicKey, PIPE_L2CAP, this._l2cap.psm)
        this._gatt.notify(encodeFrame(TYPE_HELLO, f.id, hello)).catch(safetyCatch)
      } else {
        this._gatt.openServer(session)
        this._gatt.notify(encodeFrame(TYPE_HELLO, f.id, this.keyPair.publicKey)).catch(safetyCatch)
      }
    } else if (f.type === TYPE_DATA) {
      if (!s) return
      if (!s.stream) {
        // gatt data on a session awaiting its l2cap channel is a protocol
        // violation — close instead of silently degrading
        this._log('server refused gatt data on l2cap session', f.id)
        this._closeServerSession(f.id)
        return
      }
      s.stream.receive(b4a.from(f.payload))
    } else if (f.type === TYPE_CLOSE) {
      if (!s) return
      this._log('server session closed by remote', f.id)
      this._reapSession(f.id, s)
      if (s.stream) s.stream.remoteEnd()
    }
  }

  _onServerChannel(channel) {
    if (this.closing || this.closed || this._suspended) {
      try {
        channel.destroy()
      } catch (err) {
        safetyCatch(err)
      }
      return
    }
    this._l2cap.acceptServer(channel)
  }

  _reapSession(id, session) {
    if (session.pipeTimer) clearTimeout(session.pipeTimer)
    session.pipeTimer = null
    this._sessions.delete(id)
    // A dead session leaves its channel state on the shared ACL, and the OS
    // refuses a second open to the same psm ("L2CAP PSM already connected").
    // Rotate the listener so the next HELLO carries a virgin psm.
    if (this.pipe === 'l2cap' && !this._suspended && !this.closing && !this.closed) {
      this._maybeCycle()
    }
  }

  // Never yank the psm out from under a session still opening its channel —
  // hold the rotation until the last pending session resolves (bind or reap).
  _maybeCycle() {
    for (const s of this._sessions.values()) {
      if (s.stream) continue
      this._cyclePending = true
      return
    }
    this._cyclePending = false
    this._l2cap.cycle()
  }

  _closeServerSession(id) {
    const session = this._sessions.get(id)
    if (!session) return
    this._reapSession(id, session)
    this._notifyClose(id)
  }

  _notifyClose(id) {
    this._gatt.notify(encodeFrame(TYPE_CLOSE, id)).catch(safetyCatch)
  }

  // A pipe hands its finished stream here — session wiring and close-time
  // cleanup are transport concerns shared by every pipe.
  _bindServerStream(session, stream, name) {
    if (session.pipeTimer) clearTimeout(session.pipeTimer)
    session.pipeTimer = null
    session.stream = stream
    stream.on('close', () => this._closeServerSession(session.id))
    session.conn = this._onChannel(stream, false, null)
    this._log(`server pipe: ${name}`, session.id)
    if (this._cyclePending) this._maybeCycle()
  }

  _bindCentralStream(peripheral, sess, stream, name) {
    stream.on('close', () => {
      this._gatt.send(peripheral, encodeFrame(TYPE_CLOSE, sess.id), true).catch(safetyCatch)
      // iOS reuses peripheral objects across reconnects — stale refs here
      // would make the next HELLO look like a live session and get dropped
      peripheral._stream = null
      peripheral._session = null
      const d = this._devices.get(peripheral.id)
      if (d) d.peripheral = null
      try {
        this.central.disconnect(peripheral)
      } catch (err) {
        safetyCatch(err)
      }
    })
    peripheral._stream = stream
    this._clearDial(peripheral.id)
    peripheral._conn = this._onChannel(stream, true, peripheral.id)
    this._log(`central pipe: ${name}`, peripheral.id)
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
    const hunting = this.linkCount === 0 && Date.now() < this._huntUntil
    const duty = this.linkCount > 0 || (this._online && !hunting)
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
      const stillHunting = this.linkCount === 0 && Date.now() < this._huntUntil
      if (this.linkCount > 0 || (this._online && !stillHunting)) {
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
    // both platforms drop the GATT db on power-off: re-add the service on recovery
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
    this._reapAllSessions()
    this._gatt.reset('radio down')
  }

  _reapAllSessions() {
    for (const { stream, pipeTimer } of this._sessions.values()) {
      if (pipeTimer) clearTimeout(pipeTimer)
      try {
        if (stream) stream.destroy()
      } catch (err) {
        safetyCatch(err)
      }
    }
    this._sessions.clear()
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
    const wait = DIAL_MIN_INTERVAL - (Date.now() - this._lastDial)
    if (wait > 0) {
      // hold rate-limited discoveries (the radio may not re-report them until
      // the next scan cycle) and dial the strongest signal when the window opens
      this._candidates.set(peripheral.id, peripheral)
      if (!this._dialTimer) {
        this._dialTimer = setTimeout(() => {
          this._dialTimer = null
          this._flushCandidates()
        }, wait)
        if (this._dialTimer.unref) this._dialTimer.unref()
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

  // strongest signal first — the nearest peer makes the best link. The first
  // dial re-rate-limits the rest, so each window dials the strongest remaining.
  _flushCandidates() {
    const held = [...this._candidates.values()].sort((a, b) => (b.rssi ?? -100) - (a.rssi ?? -100))
    this._candidates.clear()
    for (const peripheral of held) this._onDiscover(peripheral)
  }

  _onConnect(peripheral) {
    this._device(peripheral.id).peripheral = peripheral
    peripheral.on('error', (err) => {
      const reason = err?.message ?? err?.code ?? 'peripheral-error'
      // iOS emits benign error events during openL2CAPChannel — aborting here
      // would hang up the ACL under the in-flight open. The open's own
      // deadline already covers a genuinely dead link.
      if (peripheral._session?.upgrading) {
        this._log('peripheral error during channel open (ignored):', reason)
        return
      }
      this._abortDial(peripheral, reason)
    })
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
    const id = b4a.toString(randomBytes(SID_LEN), 'hex')
    peripheral._session = { id }
    peripheral._char = char
    peripheral.on('notify', (_char, data) => this._onCentralNotify(peripheral, data))
    peripheral.on('mtuChanged', (mtu) => {
      peripheral._mtu = mtu
      if (peripheral._stream) peripheral._stream.maxPayload = this._gatt.getMaxPayload(peripheral)
    })
    this._log('central sent OPEN', peripheral.id)
    const open = encodeKeyPayload(this.keyPair.publicKey, this._capabilities(), null)
    this._gatt.send(peripheral, encodeFrame(TYPE_OPEN, id, open), true).catch(safetyCatch)
    try {
      peripheral.requestMtu(REQUEST_MTU) // no-op on Apple, mtuChanged on Android
    } catch (err) {
      safetyCatch(err)
    }
  }

  async _onCentralNotify(peripheral, data) {
    const sess = peripheral._session
    if (!sess) return
    const f = decodeFrame(data)
    if (!f || f.id !== sess.id) return
    if (f.type === TYPE_HELLO) {
      if (peripheral._stream || sess.upgrading) return
      const { key, flags, psm } = decodeKeyPayload(f.payload)
      if (!this._accept(key)) {
        this._log('central refused HELLO', peripheral.id)
        this._endCentral(peripheral, sess)
        return
      }
      if (!this._weInitiate(key)) {
        // bigger key yields: the peer's central dials our server instead.
        // Remember the mapping so rediscovery stays quiet while their link
        // lives.
        this._device(peripheral.id).peerKey = b4a.toString(key, 'hex')
        this._log('central yielded to', peripheral.id)
        this._endCentral(peripheral, sess, { keep: true })
        return
      }
      const name = this._pipeFor(flags, psm)
      if (name === null) {
        this._log('central refused HELLO, pipe mismatch', peripheral.id)
        this._endCentral(peripheral, sess)
        return
      }
      this._log(`central got HELLO, opening ${name} pipe`, peripheral.id)
      try {
        await this._pipes[name].openCentral(peripheral, sess, { flags, psm })
      } catch (err) {
        safetyCatch(err)
      }
    } else if (f.type === TYPE_DATA) {
      if (peripheral._stream) peripheral._stream.receive(b4a.from(f.payload))
    } else if (f.type === TYPE_CLOSE) {
      if (peripheral._stream) {
        peripheral._session = null
        peripheral._stream.remoteEnd()
      } else {
        this._endCentral(peripheral, null)
      }
    }
  }

  // End our probe session pre-handshake, with a long cooldown so rediscovery
  // doesn't spin. `sess` sends a CLOSE first (null when the remote closed).
  // `keep` leaves the shared radio link up — a yielding central must NEVER
  // disconnect: both directions ride one physical link, hanging up kills the
  // peer's channel too.
  _endCentral(peripheral, sess, { keep = false } = {}) {
    this._log(
      'central session ended',
      peripheral.id,
      sess ? 'local' : 'remote-close',
      keep ? 'keep' : 'drop'
    )
    if (sess) this._gatt.send(peripheral, encodeFrame(TYPE_CLOSE, sess.id), true).catch(safetyCatch)
    peripheral._session = null
    const d = this._device(peripheral.id)
    d.coolUntil = Date.now() + DIAL_COOLDOWN_MAX
    d.peripheral = keep ? peripheral : null
    this._clearDial(peripheral.id)
    if (!keep) {
      try {
        this.central.disconnect(peripheral)
      } catch (err) {
        safetyCatch(err)
      }
    }
    this._startScan()
  }

  _abortDial(peripheral, reason) {
    this._log('dial abort', peripheral?.id, reason)
    const id = peripheral?.id
    if (id) {
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
    if (!id) return
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
    this._log('central error', code ?? err?.message)
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
    if (peripheralId !== null) {
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
    if (peripheralId !== null && conn.remotePublicKey) {
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
      // last link gone → hunt eagerly instead of waiting out a dark window
      if (this.linkCount === 0 && !this._suspended) {
        this._huntUntil = Date.now() + HUNT_WINDOW
        this._startScan()
        this._armScanRestart()
      }
      this.emit('update')
    }
  }

  // Best-effort TYPE_CLOSE to every live session, then a short drain. Resolves
  // regardless: suspend must never hang on a wedged radio.
  async _sayGoodbye() {
    const sent = []
    for (const id of this._sessions.keys()) {
      sent.push(this._gatt.notify(encodeFrame(TYPE_CLOSE, id)).catch(safetyCatch))
    }
    for (const d of this._devices.values()) {
      const peripheral = d.peripheral
      const sess = peripheral && peripheral._session
      if (!sess || !peripheral._char) continue
      const f = encodeFrame(TYPE_CLOSE, sess.id)
      sent.push(this._gatt.send(peripheral, f, true).catch(safetyCatch))
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
    this._reapAllSessions()
    this._gatt.reset('suspended')
    // macOS only: a listener that lives through destroyed channels wedges the
    // resumed manager. iOS tolerates no manager surgery — keep its listener.
    this._cyclePending = false // suspend tears the sessions down; resume republishes fresh
    if (isMac) this._l2cap.unpublish()
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
    this._huntUntil = Date.now() + HUNT_WINDOW
    if (this.pipe === 'l2cap') this._l2cap.publish()
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
    for (const { pipeTimer } of this._sessions.values()) {
      if (pipeTimer) clearTimeout(pipeTimer)
    }
    this._sessions.clear()
    this._gatt.reset('closed')
    this.state = 'off'
  }
}

module.exports.toServiceUUID = toServiceUUID
