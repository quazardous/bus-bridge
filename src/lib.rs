//! Reference transports for bus-contract's crossing contract
//! (`bus-contract-crossing`): a link that makes two buses one logical
//! bus across a process boundary.
//!
//! The crossing contract says what it means for an event or a request
//! to cross: relayable points, codecs, versions, origins, authorities,
//! deadlines that fail closed. It leaves out how the text moves. This
//! crate moves it, using the contract's API only, so it works with any
//! bus that offers `Relay` (bus-kernel is the reference one):
//!
//! - [`Endpoint`]: one bus's end of a link. It serves its bus's
//!   relayable entries to the peer from the cursor the peer gives,
//!   takes the peer's in with the link's [`Origin`](bus_contract_crossing::Origin)
//!   (never one the peer writes), answers the peer's asks, and sends its
//!   own through the contract's `far_listener` ([`Endpoint::listen_far`]).
//!   It reconnects and resumes from the cursor it took up to, reports
//!   what was lost ([`LinkEvent::Lost`]), and bounds the work in flight
//!   ([`Config::max_in_flight`], [`LinkEvent::Saturated`]).
//! - [`transport`]: what a link runs over. [`transport::memory`] joins
//!   two endpoints in one process; `unix` (feature `unix`, on Unix) joins two
//!   processes over a Unix socket. Both carry the same frames
//!   ([`wire`]).
//! - [`RemoteRelay`] and [`LinkedBus`]: the peer's relay called through
//!   the link, so a check written against `Relay` (bus-kernel's crossing
//!   conformance group) runs with the far side behind the link.
//!
//! # Two buses, one link
//!
//! ```ignore
//! use bus_bridge::{Config, Endpoint, transport::memory};
//! use bus_contract_crossing::Origin;
//!
//! let (dial, accept) = memory::pair();
//! let node = Endpoint::new(node_bus, Config::new("node", Origin::process("desktop")))?;
//! let desktop = Endpoint::new(desktop_bus, Config::new("desktop", Origin::process("node")))?;
//! node.accept(accept);
//! desktop.connect(dial);
//! // A relayable request of the desktop answered by the node's listeners:
//! desktop.listen_far(PointKey::of::<Delete>(), "node", Duration::from_secs(2), ListenOpts::new())?;
//! ```
//!
//! # What crossing over this bridge guarantees, and what it does not
//!
//! The meaning of what crosses is the contract's: read bus-contract's
//! guarantees page, crossing section
//! (<https://github.com/quazardous/bus-contract/blob/main/GUARANTEES.md#crossing>).
//! This crate adds what the transport decides:
//!
//! - **Order.** Entries of one sender reach the receiver in the
//!   sender's journal order. There is no order between the two
//!   directions, nor between entries and asks.
//! - **Loss on disconnect.** Entries published while the link is down
//!   stay in the sender's journal and are sent on reconnect, from the
//!   cursor the receiver took up to. Entries that left the sender's
//!   window meanwhile are lost, and the receiver is told
//!   ([`LinkEvent::Lost`] with `Lost::Gap`); after the sender restarts,
//!   its old entries are gone (`Lost::Restarted`). Lost entries are never
//!   sent again: whoever follows the peer's state resyncs from it.
//! - **Duplicates.** None across a reconnection: the receiver drops an
//!   entry it already took ([`wire::Outcome::Duplicate`]). After the
//!   *receiving* process restarts, it starts from nothing and gets the
//!   sender's whole window again, entries it took before included.
//! - **Asks.** An ask with no link, or whose answer does not come within
//!   its deadline (the link dropped, the peer is slow, the asks in flight
//!   stayed at their bound), is refused: no answer is a no. An ask is not
//!   sent again by the link. This is not a distributed transaction: what
//!   the far side's listeners did before the deadline ran out is not
//!   undone.
//! - **Back-pressure.** At most [`Config::max_in_flight`] entries sent
//!   and not taken, asks sent and not answered, and peer asks being
//!   answered. A full flow waits (entries in the journal, asks within
//!   their deadline) and is reported ([`LinkEvent::Saturated`]); a
//!   journal that overflows its window while the flow waits loses
//!   entries, reported as above.
//! - **Origin.** Every entry taken in and every ask answered carries
//!   [`Config::peer`]: the link decides who the peer is, not the peer.
//!   The Unix socket is made readable by its owner only; there is no
//!   encryption and no authentication beyond that. The contract's
//!   authorities protect against mistakes, not attackers.
//! - **Persistence.** None. A cursor is kept in memory, by the
//!   receiving end, for as long as its process runs.
//!
//! # Credits
//!
//! - The publication receipt per entry ([`wire::Receipt`]) follows
//!   qubit-event-bus's `PublishReceipt`, and the saturation report
//!   ([`LinkEvent::Saturated`]) its capacity monitor
//!   (`EventBus::local_with_monitor`):
//!   <https://github.com/qubit-ltd/rs-event-bus>.
//! - The bound on work in flight ([`Config::max_in_flight`]) follows
//!   eventbus-contract's `SubscriptionConfig::max_in_flight`:
//!   <https://github.com/mapseekai/eventbus-contract>.
//! - Stating plainly what crossing guarantees and what it does not, as
//!   above, follows both projects' documentation.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod link;
mod remote;
pub mod transport;
#[cfg(all(unix, feature = "unix"))]
pub mod unix;
pub mod wire;

pub use link::{Config, Endpoint, Flow, LinkError, LinkEvent, LinkStats};
pub use remote::{LinkedBus, RemoteRelay};

pub use bus_contract;
pub use bus_contract_crossing;
