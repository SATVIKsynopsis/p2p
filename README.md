# Small P2P file sharing project

This university project implements framed TCP messages, piece storage, SHA-256 piece verification, concurrent downloads across known peers, and a small DHT discovery service. It is a teaching prototype, not a production BitTorrent client.

## Data flow

```text
DHT
 ↓
Peer discovery
 ↓
TCP connection
 ↓
Handshake (protocol-aware peers)
 ↓
Bitfield
 ↓
Piece requests
 ↓
Piece transfer
 ↓
SHA-256 verification
 ↓
Have propagation
 ↓
File reconstruction
```

The downloader keeps a shared set of in-flight piece indices and assigns distinct available pieces to concurrent connection tasks. Failed connections stop their own task; other peer tasks continue. Pieces are hash-checked before insertion, then the manager writes pieces in index order during reconstruction.

## Protocol

Messages use the existing bincode codec and four-byte length framing. Supported messages are `Handshake`, `Bitfield`, `Have`, `Request`, `Piece`, `Choke`, `Unchoke`, `Ping`, and `Pong`. A connection can exchange a symmetric handshake by sending first and then receiving; empty IDs, self IDs, and unexpected message types fail validation. The explicit bitfield exchange requires successful handshake completion. Existing lower-level transfer APIs remain available for simple seeders that already know their bitfield.

`Have` marks a piece in a remote bitfield. It is idempotent; an index beyond the advertised bitfield is rejected. Owners should call `Peer::announce_have` after adding a piece to broadcast it to connected peers.

Choke state is per connection. A locally choked peer ignores incoming requests through `Peer::handle_message`, while direct uploader request handling returns `PermissionDenied`. Download requests fail while the remote peer is known to be choking.

## Fairness

`Peer::select_unchoked(limit)` deterministically ranks peers by pieces received from that peer, then successful requests, uploaded pieces, and peer ID. A contributor meeting the default one-piece threshold can receive a slot; before anyone qualifies, deterministic bootstrap slots remain open up to the limit. `Peer::reconsider_unchoked` sends wire choke changes when the selection changes. The serving process applies a configurable slot limit, announced-piece threshold, and reconsideration interval through `FairnessConfig` (defaults: four slots, one distinct Have, five seconds). Equal-score peers rotate through the available slots each interval so peers beyond the initial slots are not permanently starved. Its Have count is an availability signal, not proof that the peer uploaded bytes. Contribution metrics are per connection and reset when the connection is recreated. This simple policy does not implement tit-for-tat incentives or persistent reputation.

## Running

The CLI supports three modes:

```sh
cargo run -- dht-server 0.0.0.0:5001
cargo run -- seed <peer_id> <listen_addr> <dht_addr> <file> <piece_size> [piece_ranges]
cargo run -- download <peer_id> <dht_addr> <output> <total_pieces> <piece_size>
```

Start the DHT server, start seed processes, then run the downloader. A seed's optional piece range accepts values such as `0-2,5,7-9` or `all`; all seeders must use the same file and piece size. The downloader requires the total piece count and piece size because this prototype does not distribute a metadata manifest or filename. The seed mode binds the given TCP address and uses the actual bound address when `:0` is supplied. The `seed` and `download` commands use the same framed TCP protocol, symmetric handshake, bitfield exchange, uploader/downloader, and DHT APIs as the library tests.

For example, for a file that produces eight pieces at the selected piece size, start three seeders with ranges `0-1`, `2-3`, and `4-7`, then download with `total_pieces=8` and that same `piece_size`.

To print loopback timing samples for one, two, and three peers (eight pieces in each run), use `cargo test --test scaling_demo -- --nocapture`. These samples measure only the local test setup; they do not establish a general scaling or speedup claim.

## Scope and limitations

The implementation has no production wire compatibility, authentication, encryption, robust tracker lifecycle, piece rarity strategy, bounded frame-size defense, or durable contribution history. Bitfield storage is byte-based and does not carry an independent piece-count field. The DHT is an in-memory centralized TCP registry, not Kademlia. Seed processes send Join and piece announcements at startup, then Remove their peer and piece records when the server exits or they receive Ctrl+C. A peer that disappears without a graceful shutdown is removed after five minutes without a Join or Store; cleanup is applied when a Find request arrives, so active seeders should reannounce periodically if they run longer than five minutes. Performance varies with machine, loopback scheduling, file size, and peer availability; no speedup guarantee is implied. The scaling test measures one, two, and three loopback peers with eight pieces each. It is a demo measurement, not a general performance claim.

