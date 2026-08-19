# ble-swarm

> [!IMPORTANT]
> This module is experimental. The API is subject to change and may break at any time.

Bluetooth LE hyperswarm transport. Nearby peers discover each other by
advertising and scanning a topic-derived GATT service UUID, negotiate a session
over a single write+notify characteristic, and come out the other side as
Noise-encrypted duplex streams — the same connections hyperswarm emits, so a BLE
link can be fed straight into corestore replication.

Session data moves over one of two pipes, chosen up front: `gatt` (a framed
byte stream over the characteristic — works everywhere) or `l2cap` (a real
L2CAP channel per session — faster, with native flow control).

```
npm install ble-swarm
```

Built on [bare-bluetooth](https://github.com/holepunchto/bare-bluetooth):
macOS 13+, iOS and Android. On other platforms the module loads fine and
reports `unsupported`.

## Usage

```js
const BluetoothSwarm = require('ble-swarm')

const bt = new BluetoothSwarm({ keyPair, topic })

bt.on('connection', (conn) => {
  // conn is a NoiseSecretStream: publicKey, remotePublicKey, isInitiator.
  // Handshake done, duplicate links already deduped.
  store.replicate(conn)
})

await bt.start() // toggle on
await bt.stop() // toggle off — radios suspend, managers are reused
```

## API

#### `const bt = new BluetoothSwarm(options)`

Options:

```js
{
  keyPair,                 // static Noise keypair, e.g. the hyperswarm keypair
  topic,                   // 32-byte topic the service UUID derives from
  tag: 'ble-swarm',        // UUID namespace
  shouldConnect (key) {},  // return false to refuse a peer pre-handshake
  maxOutbound: 4,          // concurrent outbound links
  maxInbound: 8,           // concurrent inbound sessions
  online: false,           // relax scanning while the internet path is up
  pipe: 'l2cap',           // data pipe: 'l2cap' (default) or 'gatt' — must match on both peers
  gatt: {
    maxPayload: 0,      // outbound chunk size; 0 = negotiated from the MTU
    ackWrites: true     // acknowledged writes for data frames
  },
  l2cap: {
    timeout: 3000       // deadline for a channel open
  }
}
```

#### `bt.state`

`unsupported | unauthorized | off | starting | waiting | on`

#### `bt.peers`

Live link count.

#### `await bt.start()` / `await bt.stop()`

Toggle the radio. The underlying Central/Server managers are created once and
suspended/resumed — CoreBluetooth managers cannot be destroyed safely.

#### `bt.setOnline(online)`

Hint from the host: while `true`, scanning duty-cycles lazily (BLE is a
fallback); while `false`, it hunts aggressively (BLE is the only path).

#### `bt.on('connection', conn)` / `bt.on('update')`

`connection` fires per deduped, opened link. `update` fires on any state or
peer-count change.

## The two pipes

BLE offers two ways to move bytes and the dev picks one — peers on mismatched
pipes refuse each other loudly instead of silently degrading.

**`gatt`** frames a byte stream over the single characteristic: writes carry
data one way, notifications the other. Slower (chunked, one write in flight),
but it behaves the same on every platform.

**`l2cap`** opens a dedicated L2CAP channel per session — an ordered byte pipe
with credit-based flow control in the controller, several times faster and
cheaper per byte. Historically iOS never answered an L2CAP channel opened by a
macOS central and gave no error for it, so every open runs under a deadline: a
hang becomes a visible failed dial that the normal cooldown/redial cycle
retries.

Either way, the characteristic still carries the OPEN/HELLO/CLOSE control
frames — that's where keys are exchanged, unwanted peers refused, and the
l2cap PSM delivered. With Noise on top the result is an encrypted duplex that
looks just like any other socket to the layers above.

## Design notes

- One data characteristic carries `[type:1][sessionId:8][payload]` frames;
  OPEN/HELLO payloads are `[key:32][flags:1][psm:2]`.
- An l2cap central writes its 8-byte session id first so the server can match
  the channel to the session negotiated over GATT.
- Both sides advertise and scan; a pair may link twice and both ends retire the
  same duplicate deterministically.
- OPEN/HELLO frames carry the static public keys, so duplicate or unwanted
  peers are refused before any Noise work.
- Liveness is keepalive/timeout based — iOS emits no disconnect for a vanished
  peer.
- Android centrals negotiate the MTU up (`requestMtu`) and size gatt chunks to
  it.

## License

Apache-2.0
