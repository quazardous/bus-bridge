//! The link: one bus's end of a connection to another bus.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use bus_contract::{Bus, ListenOpts, PointKey};
use bus_contract_crossing::{
    Answer, BusCrossing, Cursor, Lost, Origin, Relay, RelayedAnswer, RelayedAsk, far_listener,
};
use bus_contract_journal::BusJournal;

use crate::peer::{PeerPolicy, Verdict, own_uid};
use crate::transport::{Accept, Dial, Stream};
use crate::wire::{self, Call, Frame, Outcome, PROTOCOL, Receipt, Reply};

/// How a link behaves. [`Config::new`] gives the defaults.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Config {
    /// This end's name, sent to the peer at the handshake (shown in its
    /// [`LinkEvent::Connected`], never used as an origin).
    pub name: String,
    /// The origin set on everything that comes in on this link: the
    /// entries taken in and the asks answered. It comes from how the
    /// link was made (which process was started, which socket accepted),
    /// never from what the peer writes.
    pub peer: Origin,
    /// The most work in flight on the link, each way and for each flow:
    /// entries sent and not yet taken by the peer, asks sent and not yet
    /// answered, asks of the peer being answered here. Past it, the
    /// sender waits (entries stay in its journal; an ask waits within
    /// its deadline) and the link reports [`LinkEvent::Saturated`].
    ///
    /// A bound on work in flight per link follows eventbus-contract's
    /// `SubscriptionConfig::max_in_flight`
    /// (<https://github.com/mapseekai/eventbus-contract>).
    pub max_in_flight: usize,
    /// The most entries in one frame.
    pub batch: usize,
    /// How long the connecting side waits before dialing again after the
    /// connection dropped or a dial failed.
    pub reconnect: Duration,
    /// How often the sending side looks for new entries when its journal
    /// cannot wake it.
    pub poll: Duration,
    /// How long a call on the peer's relay ([`crate::RemoteRelay`]) waits
    /// for its reply, an answer excepted (it waits for its ask's
    /// deadline).
    pub call_timeout: Duration,
    /// Whether a call on this end's relay made through the peer's
    /// [`crate::RemoteRelay`] keeps the origin the caller claims. Off by
    /// default: the origin is [`Config::peer`], whatever the peer wrote.
    /// Turn it on only for a peer trusted to speak for others (a hub that
    /// forwards for its own peers, or a conformance run).
    pub trust_claimed_origin: bool,
    /// Which peers the link accepts, checked on every connection before
    /// the handshake ([`PeerPolicy`]). [`PeerPolicy::SameUser`] by
    /// default; [`PeerPolicy::Any`] is unsafe on a host with several
    /// users.
    pub peer_policy: PeerPolicy,
    /// Where this end starts reading the peer's journal on its first
    /// connection ([`Start`]): [`Start::Oldest`] by default. Once
    /// entries were taken, reconnections resume exactly from the cursor
    /// taken, whatever this says.
    pub start: Start,
    /// Whether this end sends its bus's relayable entries to the peer.
    /// On by default; off ([`Config::receive_only`]), the link carries
    /// entries one way only: this end takes the peer's in, answers its
    /// asks and sends its own, but its own entries stay here.
    pub send_entries: bool,
}

/// Where an end starts reading the peer's journal before it has taken
/// anything ([`Config::start`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Start {
    /// From the oldest entry the peer still keeps: its whole window is
    /// sent (the default).
    #[default]
    Oldest,
    /// From the peer's present: only entries the peer journals after
    /// the first handshake are sent, none of its past. For a process
    /// that attaches to a long-running peer and follows its state some
    /// other way (it reads that state, then follows the facts). A peer
    /// older than bus-bridge 0.2.1 does not know this request and sends
    /// its whole window, reported as [`Lost::Restarted`].
    Present,
    /// From a cursor saved earlier ([`LinkStats::taken`]), exactly as a
    /// reconnection resumes: entries after it are sent, entries that
    /// left the peer's window meanwhile are reported lost.
    At(Cursor),
}

impl Config {
    /// A link named `name` whose peer is `peer`: 64 in flight, frames of
    /// 256 entries, reconnect after 100 ms, poll every 20 ms, calls wait
    /// 5 s, claimed origins not trusted, peers of this user only.
    pub fn new(name: impl Into<String>, peer: Origin) -> Config {
        Config {
            name: name.into(),
            peer,
            max_in_flight: 64,
            batch: 256,
            reconnect: Duration::from_millis(100),
            poll: Duration::from_millis(20),
            call_timeout: Duration::from_secs(5),
            trust_claimed_origin: false,
            peer_policy: PeerPolicy::SameUser,
            start: Start::Oldest,
            send_entries: true,
        }
    }

    /// Sending none of this end's entries to the peer
    /// ([`Config::send_entries`]): a follower that only takes the
    /// peer's facts in.
    pub fn receive_only(mut self) -> Config {
        self.send_entries = false;
        self
    }

    /// Starting from elsewhere than the peer's oldest entry
    /// ([`Config::start`]).
    pub fn start(mut self, start: Start) -> Config {
        self.start = start;
        self
    }

    /// With another peer policy ([`Config::peer_policy`]).
    pub fn peer_policy(mut self, policy: PeerPolicy) -> Config {
        self.peer_policy = policy;
        self
    }

    /// With another bound on work in flight ([`Config::max_in_flight`]),
    /// at least 1.
    pub fn max_in_flight(mut self, max: usize) -> Config {
        self.max_in_flight = max.max(1);
        self
    }

    /// With another reconnection delay ([`Config::reconnect`]).
    pub fn reconnect(mut self, delay: Duration) -> Config {
        self.reconnect = delay;
        self
    }

    /// Trusting the origin a remote relay caller claims
    /// ([`Config::trust_claimed_origin`]).
    pub fn trust_claimed_origin(mut self) -> Config {
        self.trust_claimed_origin = true;
        self
    }
}

/// Which flow of a link is full ([`LinkEvent::Saturated`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Flow {
    /// Entries sent, not yet taken by the peer.
    Entries,
    /// Asks sent, not yet answered.
    Asks,
    /// Asks of the peer (and calls on this relay) being answered here.
    Answers,
}

/// What a link reports ([`Endpoint::on_event`]).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LinkEvent {
    /// The handshake with the peer is done.
    Connected {
        /// The name the peer gave (only informative).
        peer: String,
    },
    /// The connection ended; the connecting side dials again.
    Disconnected {
        /// Why.
        reason: String,
    },
    /// Entries of the peer's journal were missed (on reconnecting: it
    /// dropped them from its window, or it restarted). They are not sent
    /// again: whoever follows the peer's state resyncs from it.
    Lost(Lost),
    /// A flow reached [`Config::max_in_flight`]: the sender waits.
    ///
    /// A capacity monitor that reports saturation follows
    /// qubit-event-bus's `EventBus::local_with_monitor`
    /// (<https://github.com/qubit-ltd/rs-event-bus>).
    Saturated {
        /// The full flow.
        flow: Flow,
        /// The bound reached.
        max: usize,
    },
    /// What the peer did with an entry this end sent.
    Receipt(Receipt),
    /// The peer failed the link's peer check ([`Config::peer_policy`]):
    /// the connection was closed before the handshake, nothing crossed.
    /// The connecting side dials again after [`Config::reconnect`].
    Refused {
        /// Why.
        reason: String,
    },
    /// The peer was let in without its identity being read: the
    /// transport or the platform has no peer credentials, and the
    /// policy ([`PeerPolicy::SameUser`]) falls back to the transport's
    /// own access control (the Unix socket's file mode `0600`).
    PeerUnverified {
        /// Why.
        reason: String,
    },
    /// This end asked to start from the peer's present
    /// ([`Start::Present`]) and the peer answered with it: entries after
    /// `cursor` will be taken in, none before. Reported before any of
    /// them is, so a sink can draw the line between what came by another
    /// way and what comes by the link.
    FromPresent {
        /// The peer's cursor at the handshake.
        cursor: Cursor,
    },
}

/// A link's counters at a moment ([`Endpoint::stats`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LinkStats {
    /// Whether the handshake is done and the connection up.
    pub connected: bool,
    /// Entries sent, not yet taken by the peer.
    pub entries_in_flight: usize,
    /// Asks sent, not yet answered.
    pub asks_in_flight: usize,
    /// Asks of the peer being answered here.
    pub answering: usize,
    /// [`Config::max_in_flight`].
    pub max_in_flight: usize,
    /// How many times a flow was found full.
    pub saturated: u64,
    /// How many connections were made (the first one included).
    pub connections: u64,
    /// How many times the peer reported entries lost.
    pub lost: u64,
    /// The cursor of the peer's journal this end has taken up to.
    pub taken: Cursor,
}

type Sink = Arc<dyn Fn(&LinkEvent) + Send + Sync>;

/// One open connection.
struct Conn {
    epoch: u64,
    writer: Mutex<Box<dyn Stream>>,
}

impl Conn {
    /// Writes a frame; on failure the connection is shut, so its reader
    /// ends.
    fn send(&self, frame: &Frame) -> bool {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if wire::write_frame(&mut **writer, frame).is_ok() {
            return true;
        }
        writer.shutdown();
        false
    }

    fn shutdown(&self) {
        self.writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .shutdown();
    }
}

#[derive(Default)]
struct State {
    epoch: u64,
    conn: Option<Arc<Conn>>,
    /// The peer's handshake came on `conn`.
    ready: bool,
    /// The cursor of the peer's journal taken up to; kept across
    /// reconnections, lost with this process.
    taken: Cursor,
    /// Where the next entries sent start, in this end's journal.
    out: Cursor,
    /// This connection's handshake asked for the peer's present, not
    /// answered yet.
    asked_present: bool,
    entries_in_flight: usize,
    asks: HashMap<(u64, u64), Sender<RelayedAnswer>>,
    asks_in_flight: usize,
    answering: usize,
    calls: HashMap<u64, Sender<Reply>>,
}

pub(crate) struct Inner {
    bus: Arc<dyn Bus>,
    config: Config,
    closed: AtomicBool,
    state: Mutex<State>,
    changed: Condvar,
    sinks: RwLock<Vec<Sink>>,
    next_call: AtomicU64,
    saturated: AtomicU64,
    connections: AtomicU64,
    lost: AtomicU64,
}

/// One bus's end of a link to another bus.
///
/// Each end serves its bus's relayable entries to the peer, from where
/// the peer says it stands, and takes in the peer's with
/// [`Config::peer`] as their origin; it answers the peer's asks on its
/// bus and sends its own ([`Endpoint::listen_far`]). One end dials
/// ([`Endpoint::connect`]) and dials again when the connection drops;
/// the other accepts ([`Endpoint::accept`]). Dropping the endpoint
/// closes the link.
pub struct Endpoint {
    inner: Arc<Inner>,
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoint")
            .field("name", &self.inner.config.name)
            .field("peer", &self.inner.config.peer)
            .field("stats", &self.stats())
            .finish()
    }
}

/// Why [`Endpoint::new`] or [`Endpoint::listen_far`] refused.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LinkError {
    /// The bus does not offer the crossing contract (`Relay`).
    NoRelay,
    /// The point is not declared on the bus as a relayable request.
    NotARelayableRequest(String),
    /// The bus refused the far listener.
    Refused(String),
}

impl fmt::Display for LinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinkError::NoRelay => f.write_str("the bus does not offer the crossing contract"),
            LinkError::NotARelayableRequest(p) => {
                write!(f, "`{p}` is not declared as a relayable request")
            }
            LinkError::Refused(why) => write!(f, "the bus refused the far listener: {why}"),
        }
    }
}

impl std::error::Error for LinkError {}

impl Endpoint {
    /// An end of a link for `bus`, which must offer the crossing
    /// contract. Not connected yet: see [`Endpoint::connect`] and
    /// [`Endpoint::accept`].
    pub fn new(bus: Arc<dyn Bus>, config: Config) -> Result<Endpoint, LinkError> {
        if bus.relay().is_none() {
            return Err(LinkError::NoRelay);
        }
        let state = State {
            taken: match config.start {
                Start::At(cursor) => cursor,
                Start::Oldest | Start::Present => Cursor::START,
            },
            ..State::default()
        };
        Ok(Endpoint {
            inner: Arc::new(Inner {
                bus,
                config,
                closed: AtomicBool::new(false),
                state: Mutex::new(state),
                changed: Condvar::new(),
                sinks: RwLock::default(),
                next_call: AtomicU64::new(1),
                saturated: AtomicU64::new(0),
                connections: AtomicU64::new(0),
                lost: AtomicU64::new(0),
            }),
        })
    }

    /// Dials the peer with `dial` from a thread of the link's, and again
    /// after [`Config::reconnect`] whenever the connection drops, until
    /// the endpoint is closed.
    pub fn connect(&self, dial: impl Dial) {
        let inner = self.inner.clone();
        thread::Builder::new()
            .name("bus-bridge-dial".into())
            .spawn(move || {
                while !inner.is_closed() {
                    match dial.dial() {
                        Ok(stream) if !inner.admit(&*stream) => stream.shutdown(),
                        Ok(stream) => {
                            let reason = inner.session(stream);
                            inner.emit(&LinkEvent::Disconnected { reason });
                        }
                        Err(e) => {
                            if inner.is_closed() {
                                break;
                            }
                            inner.emit(&LinkEvent::Disconnected {
                                reason: format!("cannot dial: {e}"),
                            });
                        }
                    }
                    if !inner.is_closed() {
                        thread::sleep(inner.config.reconnect);
                    }
                }
            })
            .expect("a thread for the link");
    }

    /// Accepts the peer's connections with `accept` from a thread of the
    /// link's, one at a time, until the endpoint is closed.
    pub fn accept(&self, accept: impl Accept) {
        let inner = self.inner.clone();
        thread::Builder::new()
            .name("bus-bridge-accept".into())
            .spawn(move || {
                while !inner.is_closed() {
                    match accept.accept(inner.config.poll.max(Duration::from_millis(5))) {
                        Ok(Some(stream)) if !inner.admit(&*stream) => stream.shutdown(),
                        Ok(Some(stream)) => {
                            let reason = inner.session(stream);
                            inner.emit(&LinkEvent::Disconnected { reason });
                        }
                        Ok(None) => {}
                        Err(_) => break,
                    }
                }
            })
            .expect("a thread for the link");
    }

    /// Calls `sink` with everything the link reports, from the link's
    /// threads.
    pub fn on_event(&self, sink: impl Fn(&LinkEvent) + Send + Sync + 'static) {
        self.inner
            .sinks
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::new(sink));
    }

    /// Whether the handshake is done, waiting at most `timeout` for it.
    pub fn wait_connected(&self, timeout: Duration) -> bool {
        let s = self.inner.lock();
        let (s, _) = self
            .inner
            .changed
            .wait_timeout_while(s, timeout, |s| !s.ready)
            .unwrap_or_else(PoisonError::into_inner);
        s.ready
    }

    /// Drops the current connection, as a network failure would: the
    /// connecting side dials again.
    pub fn disconnect(&self) {
        let conn = self.inner.lock().conn.clone();
        if let Some(conn) = conn {
            conn.shutdown();
        }
    }

    /// Closes the link: no more connections, the current one dropped.
    /// Asks waiting for an answer are refused.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.disconnect();
        self.inner.changed.notify_all();
    }

    /// The link's counters now.
    pub fn stats(&self) -> LinkStats {
        let s = self.inner.lock();
        LinkStats {
            connected: s.ready,
            entries_in_flight: s.entries_in_flight,
            asks_in_flight: s.asks_in_flight,
            answering: s.answering,
            max_in_flight: self.inner.config.max_in_flight,
            saturated: self.inner.saturated.load(Ordering::Relaxed),
            connections: self.inner.connections.load(Ordering::Relaxed),
            lost: self.inner.lost.load(Ordering::Relaxed),
            taken: s.taken,
        }
    }

    /// Sends `ask` to the peer's relay and waits for its answer, at most
    /// its deadline: `None` when no answer came (no connection, the
    /// connection dropped, the asks in flight stayed at their bound, the
    /// peer too slow). What [`far_listener`]'s `send` does.
    pub fn ask(&self, ask: RelayedAsk) -> Option<RelayedAnswer> {
        self.inner.ask(ask)
    }

    /// Registers on this end's bus, on the relayable request `point`, the
    /// listener that stands for the peer's listeners
    /// ([`far_listener`]): when it runs, the request is asked of the
    /// peer, which answers within `deadline` or the request is refused.
    /// The listener is `id`, threaded, owned by `bus-bridge`, with
    /// `deadline`; `opts` may place it (after, before, priority).
    pub fn listen_far(
        &self,
        point: PointKey,
        id: impl Into<String>,
        deadline: Duration,
        opts: ListenOpts,
    ) -> Result<(), LinkError> {
        let info = self
            .inner
            .bus
            .points()
            .into_iter()
            .find(|p| p.id == point.id);
        let Some((codec, version)) = info.and_then(|p| {
            bus_contract_crossing::codec(&p.facets)
                .filter(|c| c.answers())
                .map(|c| (*c, p.version))
        }) else {
            return Err(LinkError::NotARelayableRequest(point.id.into()));
        };
        let link: Weak<Inner> = Arc::downgrade(&self.inner);
        let listener = far_listener(codec, deadline, version, move |ask| {
            link.upgrade()?.ask(ask)
        });
        self.inner
            .bus
            .listen_any(
                point,
                id.into(),
                opts.deadline(deadline).threaded().owner("bus-bridge"),
                listener,
            )
            .map_err(|e| LinkError::Refused(e.to_string()))
    }

    /// A handle on the peer's relay through this link
    /// ([`crate::RemoteRelay`]).
    pub fn remote_relay(&self) -> crate::RemoteRelay {
        crate::RemoteRelay::new(self.inner.clone())
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.close();
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn relay(&self) -> &dyn Relay {
        self.bus.relay().expect("checked at Endpoint::new")
    }

    /// Checks the peer of a new connection against the policy; reports
    /// a refusal or an unverified peer.
    fn admit(&self, stream: &dyn Stream) -> bool {
        let verdict = match stream.peer_identity() {
            Ok(peer) => self.config.peer_policy.check(&peer, own_uid()),
            Err(e) => Verdict::Refuse(format!("cannot read the peer's credentials: {e}")),
        };
        match verdict {
            Verdict::Pass => true,
            Verdict::Unverified(reason) => {
                self.emit(&LinkEvent::PeerUnverified { reason });
                true
            }
            Verdict::Refuse(reason) => {
                self.emit(&LinkEvent::Refused { reason });
                false
            }
        }
    }

    fn emit(&self, event: &LinkEvent) {
        let sinks = self
            .sinks
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for sink in sinks {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(event)));
        }
    }

    fn saturated(&self, flow: Flow) {
        self.saturated.fetch_add(1, Ordering::Relaxed);
        self.emit(&LinkEvent::Saturated {
            flow,
            max: self.config.max_in_flight,
        });
    }

    /// Runs one connection until it ends; why it ended.
    fn session(self: &Arc<Inner>, mut stream: Box<dyn Stream>) -> String {
        let writer = match stream.try_clone_stream() {
            Ok(w) => w,
            Err(e) => return format!("cannot share the connection: {e}"),
        };
        let (conn, resume) = {
            let mut s = self.lock();
            if let Some(old) = s.conn.take() {
                old.shutdown();
            }
            s.epoch += 1;
            let conn = Arc::new(Conn {
                epoch: s.epoch,
                writer: Mutex::new(writer),
            });
            s.conn = Some(conn.clone());
            s.ready = false;
            // Nothing taken yet and asked to start from the peer's
            // present: say so; the peer answers with its cursor.
            let resume = if s.taken == Cursor::START && self.config.start == Start::Present {
                wire::RESUME_PRESENT
            } else {
                s.taken
            };
            s.asked_present = resume == wire::RESUME_PRESENT;
            (conn, resume)
        };
        if self.is_closed() {
            conn.shutdown();
        }
        let hello = Frame::Hello {
            protocol: PROTOCOL,
            name: self.config.name.clone(),
            resume,
        };
        let reason = if conn.send(&hello) {
            loop {
                match wire::read_frame(&mut *stream) {
                    Ok(frame) => {
                        if let Err(reason) = self.on_frame(&conn, frame) {
                            break reason;
                        }
                    }
                    Err(e) => break e.to_string(),
                }
            }
        } else {
            "the handshake could not be sent".to_owned()
        };
        conn.shutdown();
        {
            let mut s = self.lock();
            if s.conn.as_ref().is_some_and(|c| c.epoch == conn.epoch) {
                s.conn = None;
                s.ready = false;
                s.entries_in_flight = 0;
                // Dropping the senders wakes the waiters: no answer.
                s.asks.clear();
                s.calls.clear();
            }
        }
        self.changed.notify_all();
        if self.is_closed() {
            "closed".to_owned()
        } else {
            reason
        }
    }

    fn on_frame(self: &Arc<Inner>, conn: &Arc<Conn>, frame: Frame) -> Result<(), String> {
        match frame {
            Frame::Hello {
                protocol,
                name,
                resume,
            } => {
                if protocol != PROTOCOL {
                    return Err(format!(
                        "the peer speaks protocol {protocol}, this end {PROTOCOL}"
                    ));
                }
                // The peer starts from this end's present: it is sent the
                // cursor now (an empty frame), so that a reconnection
                // resumes from there even if nothing was sent meanwhile.
                let present = (resume == wire::RESUME_PRESENT).then(|| self.relay().cursor());
                {
                    let mut s = self.lock();
                    s.out = present.unwrap_or(resume);
                    s.entries_in_flight = 0;
                    s.ready = true;
                }
                if let Some(next) = present {
                    let frame = Frame::Entries {
                        entries: Vec::new(),
                        lost: None,
                        next,
                    };
                    if !conn.send(&frame) {
                        return Err("the peer's present could not be sent".to_owned());
                    }
                }
                self.connections.fetch_add(1, Ordering::Relaxed);
                self.changed.notify_all();
                self.emit(&LinkEvent::Connected { peer: name });
                if !self.config.send_entries {
                    return Ok(());
                }
                let (inner, conn) = (self.clone(), conn.clone());
                thread::Builder::new()
                    .name("bus-bridge-pump".into())
                    .spawn(move || inner.pump(&conn))
                    .map_err(|e| format!("no thread for the pump: {e}"))?;
            }
            Frame::Entries {
                entries,
                lost,
                next,
            } => {
                let asked_present = std::mem::take(&mut self.lock().asked_present);
                if asked_present && entries.is_empty() && lost.is_none() {
                    self.emit(&LinkEvent::FromPresent { cursor: next });
                }
                if let Some(lost) = lost {
                    self.lost.fetch_add(1, Ordering::Relaxed);
                    self.emit(&LinkEvent::Lost(lost));
                }
                let taken = self.lock().taken;
                let relay = self.relay();
                let receipts = entries
                    .into_iter()
                    .map(|entry| {
                        let (seq, kind) = (entry.seq, entry.kind.clone());
                        // Taken before this connection: a resend.
                        let outcome = if next.run == taken.run && seq <= taken.seq {
                            Outcome::Duplicate
                        } else {
                            match relay.ingest(&self.config.peer, entry) {
                                Ok(here) => Outcome::Taken(here),
                                Err(e) => Outcome::Refused(e.to_string()),
                            }
                        };
                        Receipt { seq, kind, outcome }
                    })
                    .collect();
                self.lock().taken = next;
                conn.send(&Frame::Taken { next, receipts });
            }
            Frame::Taken { receipts, .. } => {
                {
                    let mut s = self.lock();
                    s.entries_in_flight = s.entries_in_flight.saturating_sub(receipts.len());
                }
                self.changed.notify_all();
                for r in receipts {
                    self.emit(&LinkEvent::Receipt(r));
                }
            }
            Frame::Ask(ask) => {
                let refused =
                    |reason: &str| Frame::Answer(RelayedAnswer::to(&ask, Answer::refused(reason)));
                if !self.start_answering() {
                    conn.send(&refused("the link is saturated: too many asks in flight"));
                    self.saturated(Flow::Answers);
                    return Ok(());
                }
                let (inner, conn) = (self.clone(), conn.clone());
                thread::spawn(move || {
                    let answer = inner.relay().answer(&inner.config.peer, ask);
                    inner.end_answering();
                    conn.send(&Frame::Answer(answer));
                });
            }
            Frame::Answer(answer) => {
                if let Some(tx) = self.lock().asks.remove(&(answer.run, answer.id)) {
                    let _ = tx.send(answer);
                }
            }
            Frame::Call { id, call } => {
                if !self.start_answering() {
                    self.saturated(Flow::Answers);
                    // No reply: the caller's timeout refuses.
                    return Ok(());
                }
                let (inner, conn) = (self.clone(), conn.clone());
                thread::spawn(move || {
                    let reply = inner.serve(call);
                    inner.end_answering();
                    conn.send(&Frame::Reply { id, reply });
                });
            }
            Frame::Reply { id, reply } => {
                if let Some(tx) = self.lock().calls.remove(&id) {
                    let _ = tx.send(reply);
                }
            }
        }
        Ok(())
    }

    fn start_answering(&self) -> bool {
        let mut s = self.lock();
        if s.answering >= self.config.max_in_flight {
            return false;
        }
        s.answering += 1;
        true
    }

    fn end_answering(&self) {
        self.lock().answering -= 1;
    }

    /// The origin of a call made by the peer on this end's relay.
    fn call_origin(&self, claimed: Origin) -> Origin {
        if self.config.trust_claimed_origin {
            claimed
        } else {
            self.config.peer.clone()
        }
    }

    fn serve(&self, call: Call) -> Reply {
        let relay = self.relay();
        match call {
            Call::Cursor => Reply::Cursor(relay.cursor()),
            Call::Resume { from, limit } => {
                Reply::Resumed(relay.resume(from, usize::try_from(limit).unwrap_or(usize::MAX)))
            }
            Call::Ingest { origin, entry } => {
                Reply::Ingested(relay.ingest(&self.call_origin(origin), entry))
            }
            Call::Answer { origin, ask } => {
                Reply::Answered(relay.answer(&self.call_origin(origin), ask))
            }
        }
    }

    /// Sends this end's relayable entries to the peer while `conn` is the
    /// current connection, at most [`Config::max_in_flight`] not yet
    /// taken.
    fn pump(&self, conn: &Conn) {
        let relay = self.relay();
        let journal = self.bus.journal();
        let max = self.config.max_in_flight;
        // The last journal number a wait returned: the next wait is for
        // a newer one, so an entry the relay does not send (not
        // relayable) wakes the pump once, not in a loop.
        let mut seen = 0;
        loop {
            let (from, room) = {
                let mut s = self.lock();
                let mut stalled = false;
                loop {
                    let current = s.conn.as_ref().is_some_and(|c| c.epoch == conn.epoch);
                    if self.is_closed() || !current {
                        return;
                    }
                    if s.entries_in_flight < max {
                        break;
                    }
                    if !stalled {
                        stalled = true;
                        drop(s);
                        self.saturated(Flow::Entries);
                        s = self.lock();
                        continue;
                    }
                    s = self
                        .changed
                        .wait_timeout(s, self.config.poll)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
                (s.out, max - s.entries_in_flight)
            };
            let read = relay.resume(from, room.min(self.config.batch));
            if read.entries.is_empty() && read.lost.is_none() {
                self.lock().out = read.next;
                if read.next.run != from.run {
                    seen = 0;
                }
                let Some(journal) = journal else {
                    // A journal that cannot wait: do not spin.
                    thread::sleep(self.config.poll / 4);
                    continue;
                };
                // A new entry wakes the pump, which reads it at once;
                // a wait that times out reads again too.
                if let Some(entry) = journal.wait(&[], read.next.seq.max(seen), self.config.poll) {
                    seen = entry.seq;
                }
                continue;
            }
            {
                let mut s = self.lock();
                if !s.conn.as_ref().is_some_and(|c| c.epoch == conn.epoch) {
                    return;
                }
                s.entries_in_flight += read.entries.len();
                s.out = read.next;
            }
            let frame = Frame::Entries {
                entries: read.entries,
                lost: read.lost,
                next: read.next,
            };
            if !conn.send(&frame) {
                return;
            }
        }
    }

    fn ask(&self, ask: RelayedAsk) -> Option<RelayedAnswer> {
        let start = Instant::now();
        let key = (ask.run, ask.id);
        let (conn, rx) = {
            let mut s = self.lock();
            let mut stalled = false;
            loop {
                let conn = s.conn.clone().filter(|_| s.ready && !self.is_closed())?;
                if s.asks_in_flight < self.config.max_in_flight {
                    let (tx, rx) = mpsc::channel();
                    s.asks.insert(key, tx);
                    s.asks_in_flight += 1;
                    break (conn, rx);
                }
                if !stalled {
                    stalled = true;
                    drop(s);
                    self.saturated(Flow::Asks);
                    s = self.lock();
                    continue;
                }
                let left = ask.deadline.checked_sub(start.elapsed())?;
                s = self
                    .changed
                    .wait_timeout(s, left)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
        };
        let deadline = ask.deadline;
        let answer = if conn.send(&Frame::Ask(ask)) {
            deadline
                .checked_sub(start.elapsed())
                .and_then(|left| rx.recv_timeout(left).ok())
        } else {
            None
        };
        {
            let mut s = self.lock();
            s.asks.remove(&key);
            s.asks_in_flight -= 1;
        }
        self.changed.notify_all();
        answer
    }

    /// Calls the peer's relay; `None` when no reply came within
    /// `timeout`.
    pub(crate) fn call(&self, call: Call, timeout: Duration) -> Option<Reply> {
        let id = self.next_call.fetch_add(1, Ordering::Relaxed);
        let (conn, rx) = {
            let mut s = self.lock();
            let conn = s.conn.clone().filter(|_| s.ready && !self.is_closed())?;
            let (tx, rx) = mpsc::channel();
            s.calls.insert(id, tx);
            (conn, rx)
        };
        let reply = if conn.send(&Frame::Call { id, call }) {
            rx.recv_timeout(timeout).ok()
        } else {
            None
        };
        self.lock().calls.remove(&id);
        reply
    }

    pub(crate) fn call_timeout(&self) -> Duration {
        self.config.call_timeout
    }
}
