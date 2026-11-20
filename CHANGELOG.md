# Changelog

## 0.2.0

- Peer check on every connection, on the accepting and the dialing
  side, before the handshake: `Config::peer_policy` (a new field, hence
  0.2.0) takes a `PeerPolicy`: `SameUser` (the default: the peer's user
  id must be this process's), `Uids(set)`, or `Any` (unchecked, unsafe
  on a host with several users). It parses from and shows as
  `same-user`, `uids:1000,1001` and `any`.
- A refused peer is closed before anything crosses and reported as
  `LinkEvent::Refused { reason }`; the dialing side dials again after
  `Config::reconnect`.
- The Unix socket transport reads the peer's user id from the kernel:
  `SO_PEERCRED` on Linux and Android (rustix), `getpeereid` on macOS,
  iOS and the BSDs (nix). The crate still forbids `unsafe`. On a Unix
  with neither, `SameUser` falls back to the socket file's mode `0600`
  and reports `LinkEvent::PeerUnverified { reason }`.
- `transport::Stream` gains `peer_identity` (default
  `PeerIdentity::Unknown`), so other transports can report who the
  peer is; the in-process transport reports `PeerIdentity::SameProcess`.

## 0.1.0

First release: reference transports for bus-contract's crossing
contract (`bus-contract-crossing` 0.2), using its API only.

- `Endpoint`: one bus's end of a link. Serves the bus's relayable
  entries from the cursor the peer gives and takes the peer's in with
  the link's configured origin; answers the peer's asks and sends its
  own through the contract's `far_listener` (`Endpoint::listen_far`);
  reconnects (the dialing side) and resumes from the cursor taken,
  dropping entries already taken; reports lost entries
  (`LinkEvent::Lost`), connections, receipts and saturation; counters in
  `Endpoint::stats`.
- Back-pressure: `Config::max_in_flight` bounds entries in flight, asks
  in flight and peer asks being answered, after eventbus-contract's
  `SubscriptionConfig::max_in_flight`
  (<https://github.com/mapseekai/eventbus-contract>). A full flow is
  reported (`LinkEvent::Saturated`), after qubit-event-bus's capacity
  monitor (<https://github.com/qubit-ltd/rs-event-bus>).
- Publication receipts per entry (`wire::Receipt`: taken, refused,
  duplicate), after qubit-event-bus's `PublishReceipt`.
- Transports: `transport::memory` (two endpoints in one process) and
  `unix` (feature `unix`): a Unix socket, its file mode `0600`. One
  length-prefixed wire format (`wire`) for both.
- `RemoteRelay` and `LinkedBus`: the peer's relay called through the
  link, and a bus whose relay is behind it. The tests run bus-kernel
  0.3.1's crossing conformance group (15 checks) across the in-process
  link and across a Unix socket, with the same verdicts as on the
  kernel itself.
- README: what crossing over this bridge guarantees and what it does not
  (order, loss on disconnect, duplicates, asks, back-pressure, origin),
  after the guarantees pages of the two projects above.
