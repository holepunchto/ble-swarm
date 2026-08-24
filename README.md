# ble-swarm

> [!IMPORTANT]
> This module is experimental. The API is subject to change and may break at any time.

Bluetooth LE transport shaped like [hyperswarm](https://github.com/holepunchto/hyperswarm). Nearby peers discover each other by advertising and scanning a topic-derived service UUID, negotiate a session over a single write+notify characteristic, and come out the other side as Noise-encrypted duplex streams. They are the same connections hyperswarm emits, so a BLE link feeds straight into corestore replication.

The topic scopes discovery only: it is hashed into the advertised and scanned uuid, while the data service itself sits on a fixed per-tag uuid. That split is what makes `setTopic()` a live switch — retuning changes what the radio broadcasts and hunts for, never the GATT database.

Session data moves over one of two pipes, chosen up front: `l2cap` (a real L2CAP channel per session, faster and with native flow control) or `gatt` (a framed byte stream over the characteristic, works everywhere).

## Installation

```
npm install ble-swarm
```

Built on [bare-bluetooth](https://github.com/holepunchto/bare-bluetooth): macOS 13+, iOS and Android. On other platforms the module loads fine and reports `unsupported`.

## Usage

```js
const BluetoothSwarm = require('ble-swarm')

const bt = new BluetoothSwarm({ keyPair, topic })

bt.on('connection', (conn) => {
  // conn is a NoiseSecretStream: handshake done, duplicate links already deduped
  store.replicate(conn)
})

await bt.start() // scan + advertise

await bt.setTopic(otherTopic) // retune discovery live, links move to the new topic

await bt.suspend() // app backgrounds: radio io paused, start intent kept
await bt.resume() // app foregrounds: radio io restored
```

## API

#### `const bt = new BluetoothSwarm(opts = {})`

Construct a new swarm. `opts` can include:

- `keyPair`: the static Noise keypair used for identity and encryption, e.g. the hyperswarm keypair.
- `topic`: the 32-byte topic the service UUID derives from. Only peers with the same topic discover each other.
- `tag`: namespace for the service UUID. Defaults to `'ble-swarm'`.
- `shouldConnect(remotePublicKey)`: return `false` to refuse a peer before the handshake.
- `maxOutbound`: concurrent outbound dials. Defaults to `4`.
- `maxInbound`: concurrent inbound sessions. Defaults to `8`.
- `online`: relax scanning while an internet path is up (BLE is the fallback then). Defaults to `false`.
- `pipe`: data pipe, `'l2cap'` (default) or `'gatt'`. Must match on both peers.
- `gatt` / `l2cap`: per-pipe options.

#### `bt.connecting`

Number of outbound dials in progress.

#### `bt.connections`

A Set of the live `NoiseSecretStream`s.

#### `bt.peers`

A Map of the live links, keyed by remote public key hex string.

#### `bt.state`

One of `unsupported | unauthorized | off | starting | waiting | on`.

#### `bt.supported`

`true` when a Bluetooth backend is present (macOS 13+, iOS, Android), `false` elsewhere.

#### `bt.destroyed`

`true` once the swarm has been closed (hyperswarm parity for `closed`).

#### `bt.on('connection', (conn) => {})`

Emitted per deduped, opened link. `conn` is a Noise-encrypted Duplex stream.

#### `bt.on('update', () => {})`

Emitted on any state or peer-count change. Useful for user interfaces.

#### `await bt.start()`

Start the radio: scan and advertise the service UUID. This is the durable "enabled" intent.

#### `await bt.stop()`

Stop scanning and advertising. The underlying managers are reused, never destroyed.

#### `await bt.suspend()`

Pause all radio io for a transient host-lifecycle pause (app backgrounded), while keeping the start/stop intent so `resume()` restores exactly that. The radio is live only while started and not suspended.

#### `await bt.resume()`

Restore the radio io paused by `suspend()`.

#### `bt.topic`

The current 32-byte topic Buffer, or `null` when none was given (the tag-derived default applies then).

#### `await bt.setTopic(topic)`

Switch the single active topic, live. Only the advertised and scanned uuid changes — the hosted data service sits on a fixed per-tag uuid, so no GATT surgery is involved. Existing links are dropped: a switch tunes away from the old topic's peers. `topic` must be a 32-byte Buffer.

#### `bt.setOnline(online)`

Hint from the host: while `true`, scanning duty-cycles lazily (BLE is a fallback); while `false`, it hunts aggressively (BLE is the only path).

#### `bt.status()`

A `{ state, peers }` snapshot, handy for reporting up to the host.

#### `await bt.destroy()`

Tear down for good, alias for `close()`. Stops the radio and releases the managers. The instance is single-use afterwards.

## The two pipes

BLE offers two ways to move bytes and the dev picks one. Peers on mismatched pipes refuse each other loudly instead of silently degrading.

**`gatt`** frames a byte stream over the single characteristic: writes carry data one way, notifications the other. Slower (chunked, one write in flight), but it behaves the same on every platform.

**`l2cap`** opens a dedicated L2CAP channel per session, an ordered byte pipe with credit-based flow control in the controller, several times faster and cheaper per byte. Historically iOS never answered an L2CAP channel opened by a macOS central and gave no error for it, so every open runs under a deadline: a hang becomes a visible failed dial that the normal cooldown/redial cycle retries.

Either way, the characteristic still carries the OPEN/HELLO/CLOSE control frames. That is where keys are exchanged, unwanted peers refused, and the l2cap PSM delivered. With Noise on top the result is an encrypted duplex that looks just like any other socket to the layers above.

## Design notes

- One data characteristic carries `[type:1][sessionId:8][payload]` frames. OPEN/HELLO payloads are `[key:32][flags:1]` followed by an optional PSM.
- An l2cap central writes its 8-byte session id first so the server can match the channel to the session negotiated over GATT.
- Discovery and hosting are split: the topic-derived uuid is advertised and scanned, the data service lives on a fixed per-tag uuid. Bluetooth offers no way to remove a hosted service, so a topic switch only restarts advertising and scanning.
- Both sides advertise and scan; a pair may link twice and both ends retire the same duplicate deterministically.
- OPEN/HELLO carry the static public keys, so duplicate or unwanted peers are refused before any Noise work.
- Liveness is keepalive/timeout based, since iOS emits no disconnect for a vanished peer.
- Android centrals negotiate the MTU up (`requestMtu`) and size gatt chunks to it.

## License

Apache-2.0
