# bus-bridge

[![crates.io](https://img.shields.io/crates/v/bus-bridge.svg)](https://crates.io/crates/bus-bridge)
[![docs.rs](https://docs.rs/bus-bridge/badge.svg)](https://docs.rs/bus-bridge)

Reference transports for the crossing contract of
[bus-contract](https://github.com/quazardous/bus-contract)
(`bus-contract-crossing`): one logical bus across a process boundary.

The contract says what it means for an event or a request to cross a
process: relayable points and their codecs, versions, the origin of
what comes in, authorities, deadlines that fail closed. It leaves the
transport out. This crate is a transport, written against the
contract's API only, so it joins any two buses that offer `Relay`
([bus-kernel](https://github.com/quazardous/bus-kernel) is the
reference implementation).

- **An endpoint per bus.** `Endpoint` serves its bus's relayable
  entries to the peer from the cursor the peer gives, takes the peer's
  in, answers the peer's asks on its bus, and sends its own asks through
  the contract's `far_listener` (`Endpoint::listen_far`).
- **Origin from the link.** Everything taken in carries the origin the
  link was configured with (`Config::peer`), never one the peer writes.
- **Reconnection from a cursor.** The connecting side dials again when
  the connection drops; the sender resumes from the cursor the receiver
  took up to. Entries that left the sender's window meanwhile, or that
  a restarted sender no longer has, are reported (`LinkEvent::Lost` with
  `Lost::Gap` or `Lost::Restarted`), not silently skipped.
- **Start where you choose.** A first connection reads the peer's
  whole window by default; `Config::start(Start::Present)` starts from
  the peer's present instead (for a process that attaches to a
  long-running peer), and `Start::At(cursor)` from a cursor saved
  earlier.
- **Back-pressure.** A bound on work in flight per flow
  (`Config::max_in_flight`): entries sent and not yet taken, asks sent
  and not yet answered, peer asks being answered. A full flow waits and
  is reported (`LinkEvent::Saturated`); counters are in
  `Endpoint::stats`.
- **Receipts.** The sender learns what the receiver did with each entry:
  taken (its number there), refused (why), or a duplicate dropped.
- **Two transports, one wire format.** `transport::memory` joins two
  endpoints in one process; `unix` (feature `unix`) joins two processes
  over a Unix socket whose file is readable by its owner only. Both
  carry the same length-prefixed frames (`wire`).
- **Peer check.** On every connection, both ends check who the peer is
  before the handshake (`Config::peer_policy`): by default its user id,
  read by the kernel, must be this process's. A refused peer is closed
  and reported (`LinkEvent::Refused`); nothing crosses.
- **Conformance across the link.** `RemoteRelay` calls a peer's relay
  through the link, and `LinkedBus` is a bus whose relay is a remote
  one. The tests run bus-kernel's whole crossing conformance group on
  such a bus, over the in-process link and over a Unix socket, and
  check it gives the same verdicts as on the kernel itself.

```rust,ignore
use std::time::Duration;
use bus_bridge::{Config, Endpoint, transport::memory};
use bus_bridge::bus_contract::{ListenOpts, PointKey};
use bus_bridge::bus_contract_crossing::Origin;

let (dial, accept) = memory::pair();
let node = Endpoint::new(node_bus, Config::new("node", Origin::process("desktop")))?;
let desktop = Endpoint::new(desktop_bus, Config::new("desktop", Origin::process("node")))?;
node.accept(accept);
desktop.connect(dial);

// The desktop's relayable request `Delete` is answered by the node's
// listeners, within two seconds or refused.
desktop.listen_far(PointKey::of::<Delete>(), "node", Duration::from_secs(2), ListenOpts::new())?;
```

Over a socket, the node binds `bus_bridge::unix::UnixAcceptor::bind(path)`
and the desktop dials `bus_bridge::unix::UnixDialer::new(path)`.

### Peer policy

`Config::peer_policy` (also parsed from text, for a configuration file):

| Policy | Text | Who gets in |
|---|---|---|
| `PeerPolicy::SameUser` (default) | `same-user` | A peer running as this process's user. |
| `PeerPolicy::Uids(set)` | `uids:1000,1001` | A peer running as one of these users. A peer whose user cannot be read is refused. |
| `PeerPolicy::Any` | `any` | Anyone the socket lets connect. **Unsafe on a host with several users** unless access to the socket is controlled otherwise. |

The peer's user id comes from the kernel: `SO_PEERCRED` on Linux and
Android (through [rustix](https://crates.io/crates/rustix)),
`getpeereid` on macOS, iOS and the BSDs (through
[nix](https://crates.io/crates/nix)); the crate itself has no `unsafe`.
On a Unix with neither call, `same-user` falls back to the socket
file's mode `0600` and reports each connection it let in that way
(`LinkEvent::PeerUnverified`). The in-process transport's peer is the
process itself, so every policy lets it in.

## What crossing over this bridge guarantees, and what it does not

What crossing *means* is the contract's: read the crossing section of
bus-contract's
[guarantees page](https://github.com/quazardous/bus-contract/blob/main/GUARANTEES.md#crossing)
first. This bridge adds what the transport decides.

| | Guaranteed | Not guaranteed |
|---|---|---|
| **Order** | Entries of one sender reach the receiver in the sender's journal order. | No order between the two directions, nor between entries and asks. |
| **Loss on disconnect** | Entries published while the link is down are sent on reconnect, from the cursor the receiver took up to. Entries missed are reported to the receiver: `Lost::Gap` (they left the sender's window), `Lost::Restarted` (the sender restarted). | Missed entries are not sent again: whoever follows the peer's state resyncs from it. Nothing is kept on disk. |
| **Duplicates** | None across a reconnection: an entry the receiver already took is dropped (a `Duplicate` receipt). | After the *receiving* process restarts, it starts from nothing and gets the sender's whole window again, entries it took before included. |
| **Asks** | With no link, or no answer within the ask's deadline (link dropped, peer slow, asks in flight at their bound), the request is refused: no answer is a no. | An ask is never sent again by the link. **Not a distributed transaction**: what the far side's listeners did before the deadline ran out is not undone. |
| **Back-pressure** | At most `max_in_flight` entries, asks and answers in flight per link. A full flow waits (entries in the sender's journal, asks within their deadline) and is reported. | A journal that overflows its window while the flow waits loses entries (reported as above). |
| **Origin** | Every entry taken in and every ask answered carries the link's configured peer origin. | The origin names the link, not a person: the contract's authorities protect against mistakes, not attackers. |
| **Peer identity** | Both ends check the peer's user id, read by the kernel, against `Config::peer_policy` before the handshake (same user by default); a refused peer is closed and reported, and nothing crosses. On a Unix with no peer credential call, the default relies on the socket file's mode `0600` and says so. | No identity finer than the user id: any process of an allowed user gets in. No encryption. `PeerPolicy::Any` checks nothing. |

TCP, TLS and remote discovery are not part of this release; the
`Dial` / `Accept` / `Stream` traits leave room for them.

## Credits

- The publication receipt per entry follows
  [qubit-event-bus](https://github.com/qubit-ltd/rs-event-bus)'s
  `PublishReceipt`, and the saturation report its capacity monitor
  (`EventBus::local_with_monitor`).
- The bound on work in flight follows
  [eventbus-contract](https://github.com/mapseekai/eventbus-contract)'s
  `SubscriptionConfig::max_in_flight`.
- Stating plainly what is guaranteed and what is not, as the table above
  does, follows both projects' documentation.
- The in-memory host journal of the tests comes from bus-kernel's tests.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
