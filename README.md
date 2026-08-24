# ble-swarm

> [!IMPORTANT]
> This module is experimental. The API is subject to change and may break at any time.

Bluetooth LE transport shaped like [hyperswarm](https://github.com/holepunchto/hyperswarm). Nearby peers discover each other over a single fixed service UUID, negotiate the topics they share and a session in the handshake, and come out the other side as Noise-encrypted duplex streams. They are the same connections hyperswarm emits, so a BLE link feeds straight into corestore replication.

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
bt.join(otherTopic) // also keep links that share this topic

await bt.suspend() // app backgrounds: radio io paused, start intent kept
await bt.resume() // app foregrounds: radio io restored
```

## API

#### `const bt = new BluetoothSwarm(opts = {})`

Construct a new swarm. `opts` can include:

- `keyPair`: the static Noise keypair used for identity and encryption, e.g. the hyperswarm keypair.
- `topic`: a 32-byte topic to join on construction.
- `topics`: an array of 32-byte topics to join on construction.
- `tag`: namespace for the fixed discovery UUID. Defaults to `'ble-swarm'`.
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

#### `bt.join(topic)`

Add a topic to the set. A link survives the handshake only if the two peers share at least one topic. `topic` must be a 32-byte Buffer. Takes effect on new links.

#### `bt.leave(topic)`

Remove a topic from the set. `topic` must be a 32-byte Buffer.

#### `bt.topics()`

The current set of joined topics, as an array of 32-byte Buffers.

#### `bt.setOnline(online)`

Hint from the host: while `true`, scanning duty-cycles lazily (BLE is a fallback); while `false`, it hunts aggressively (BLE is the only path).

#### `bt.status()`

A `{ state, peers }` snapshot, handy for reporting up to the host.

#### `await bt.destroy()`

Tear down for good, alias for `close()`. Stops the radio and releases the managers. The instance is single-use afterwards.

## Topics

Topics scope discovery, not what is shared. Every peer with the same `tag` advertises and scans one fixed service UUID, so discovery finds all nearby peers; the topic sets are then exchanged in the OPEN/HELLO handshake and a link is kept only if they overlap. A peer with no shared topic is refused on the control plane, before any L2CAP channel opens. Identity and encryption stay the Noise keypair, and replication over the link stays capability-gated, so two peers who share a topic but no data sync nothing.

## The two pipes

BLE offers two ways to move bytes and the dev picks one. Peers on mismatched pipes refuse each other loudly instead of silently degrading.

**`gatt`** frames a byte stream over the single characteristic: writes carry data one way, notifications the other. Slower (chunked, one write in flight), but it behaves the same on every platform.

**`l2cap`** opens a dedicated L2CAP channel per session, an ordered byte pipe with credit-based flow control in the controller, several times faster and cheaper per byte. Historically iOS never answered an L2CAP channel opened by a macOS central and gave no error for it, so every open runs under a deadline: a hang becomes a visible failed dial that the normal cooldown/redial cycle retries.

Either way, the characteristic still carries the OPEN/HELLO/CLOSE control frames. That is where keys are exchanged, unwanted peers refused, and the l2cap PSM delivered. With Noise on top the result is an encrypted duplex that looks just like any other socket to the layers above.

## Design notes

- One data characteristic carries `[type:1][sessionId:8][payload]` frames. OPEN/HELLO payloads are `[key:32][flags:1]` followed by an optional PSM and topic list, the flag bits announcing which follow.
- An l2cap central writes its 8-byte session id first so the server can match the channel to the session negotiated over GATT.
- Both sides advertise and scan the one fixed UUID; a pair may link twice and both ends retire the same duplicate deterministically.
- OPEN/HELLO carry the static public keys and the topic sets, so duplicate, unwanted, or non-overlapping peers are refused before any Noise work.
- Liveness is keepalive/timeout based, since iOS emits no disconnect for a vanished peer.
- Android centrals negotiate the MTU up (`requestMtu`) and size gatt chunks to it.

## License

Apache-2.0
