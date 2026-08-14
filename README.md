# ble-swarm

Hyperswarm-shaped swarm over Bluetooth LE. Nearby peers discover each other by
advertising and scanning a topic-derived GATT service UUID, speak a framed byte
stream over a single write+notify characteristic, and come out the other side
as Noise-encrypted duplex streams — the same shape hyperswarm emits, so a BLE
link can be fed straight into corestore replication.

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
  tag: 'keet-ble',         // UUID namespace
  shouldConnect (key) {},  // return false to refuse a peer pre-handshake
  maxOutbound: 4,          // concurrent outbound links
  maxInbound: 8,           // concurrent inbound sessions
  writePayload: 0,         // fixed outbound chunk size; 0 = default/negotiated
  writeWithResponse: true, // acknowledged GATT writes for data frames
  online: false            // relax scanning while the internet path is up
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

## Design notes

- No L2CAP: iOS never answers an L2CAP channel opened by a macOS central. One
  data characteristic carries `[type:1][sessionId:8][payload]` frames.
- Both sides advertise and scan; a pair may link twice and both ends retire the
  same duplicate deterministically.
- OPEN/HELLO frames carry the static public keys, so duplicate or unwanted
  peers are refused before any Noise work.
- Liveness is keepalive/timeout based — iOS emits no disconnect for a vanished
  peer.
- Android centrals negotiate the MTU up (`requestMtu`) and size chunks to it.

## License

Apache-2.0
