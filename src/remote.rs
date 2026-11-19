//! The peer's relay, called through a link, and a bus whose relay is
//! the peer's.

use std::any::TypeId;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, PoisonError, RwLock};

use bus_contract::{
    Altered, AnyInput, AnyListener, AnyLocalListener, AnyValue, Bus, Cast, CloneFn, Collected,
    FailureSink, FreshFn, ListenOpts, ListenerInfo, PendingRun, PendingWaker, PointInfo, PointKey,
    PointSpec, Provided, Refused,
};
use bus_contract_crossing::{
    Answer, Authority, Crossing, Cursor, IngestError, Origin, Relay, RelayedAnswer, RelayedAsk,
    RelayedEntry, Resumed,
};

use crate::link::Inner;
use crate::wire::{Call, Reply};

/// The peer's relay, reached through a link ([`crate::Endpoint::remote_relay`]):
/// each call of [`Relay`] is a frame to the peer and its reply.
///
/// The peer serves the call on its own relay. Unless the peer's link
/// trusts claimed origins ([`crate::Config::trust_claimed_origin`]), the
/// origin given to [`Relay::ingest`] and [`Relay::answer`] is replaced
/// there by the link's own.
///
/// Authorities added here ([`Relay::add_authority`]) run on this side,
/// before the call leaves: they are shown every crossing in the order
/// they were added, before the peer decodes, records or runs anything,
/// and one refusal (or a panic) refuses it. The peer's own authorities
/// run after them.
///
/// When the link is down or the peer does not reply in time: `cursor`
/// is [`Cursor::START`], `resume` returns nothing from where it was
/// asked, `ingest` is refused, `answer` is a refusal (no answer is a no).
pub struct RemoteRelay {
    link: Arc<Inner>,
    authorities: RwLock<Vec<Authority>>,
}

impl fmt::Debug for RemoteRelay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteRelay").finish_non_exhaustive()
    }
}

impl RemoteRelay {
    pub(crate) fn new(link: Arc<Inner>) -> RemoteRelay {
        RemoteRelay {
            link,
            authorities: RwLock::default(),
        }
    }

    fn authorize(&self, crossing: &Crossing<'_>) -> Result<(), String> {
        let all = self
            .authorities
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for authority in all {
            match catch_unwind(AssertUnwindSafe(|| authority(crossing))) {
                Ok(Ok(())) => {}
                Ok(Err(reason)) => return Err(reason),
                Err(_) => return Err("an authority panicked".to_owned()),
            }
        }
        Ok(())
    }
}

impl Relay for RemoteRelay {
    fn cursor(&self) -> Cursor {
        match self.link.call(Call::Cursor, self.link.call_timeout()) {
            Some(Reply::Cursor(c)) => c,
            _ => Cursor::START,
        }
    }

    fn resume(&self, from: Cursor, limit: usize) -> Resumed {
        let call = Call::Resume {
            from,
            limit: limit as u64,
        };
        match self.link.call(call, self.link.call_timeout()) {
            Some(Reply::Resumed(r)) => r,
            _ => Resumed::new(Vec::new(), from),
        }
    }

    fn ingest(&self, origin: &Origin, entry: RelayedEntry) -> Result<u64, IngestError> {
        let crossing = Crossing::new(origin, &entry.kind, entry.version, false);
        if let Err(reason) = self.authorize(&crossing) {
            return Err(IngestError::Refused {
                kind: entry.kind,
                reason,
            });
        }
        let kind = entry.kind.clone();
        let call = Call::Ingest {
            origin: origin.clone(),
            entry,
        };
        match self.link.call(call, self.link.call_timeout()) {
            Some(Reply::Ingested(r)) => r,
            _ => Err(IngestError::Refused {
                kind,
                reason: "no reply from the far side of the link".into(),
            }),
        }
    }

    fn answer(&self, origin: &Origin, ask: RelayedAsk) -> RelayedAnswer {
        let crossing = Crossing::new(origin, &ask.kind, ask.version, true);
        if let Err(reason) = self.authorize(&crossing) {
            return RelayedAnswer::to(&ask, Answer::refused(reason));
        }
        let call = Call::Answer {
            origin: origin.clone(),
            ask: ask.clone(),
        };
        // The far side answers within the ask's deadline; a little more
        // for the frames.
        let wait = ask.deadline + self.link.call_timeout();
        match self.link.call(call, wait) {
            Some(Reply::Answered(a)) => a,
            _ => RelayedAnswer::to(
                &ask,
                Answer::Refused {
                    listener: None,
                    owner: None,
                    reason: "no answer from the far side of the link".into(),
                    failed: true,
                },
            ),
        }
    }

    fn add_authority(&self, authority: Authority) {
        self.authorities
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .push(authority);
    }
}

/// A bus whose crossing is the far side's: every method goes to the
/// local bus, but its `Relay` capability is a [`RemoteRelay`].
///
/// What it is for: running a check, or a tool, that speaks to a relay
/// through the bus it holds, with the relay behind a link. This crate's
/// tests run bus-kernel's crossing conformance group on one, its relay
/// across an in-process link and across a Unix socket. It holds the
/// endpoints it is given ([`LinkedBus::hold`]) and closes them when
/// dropped.
pub struct LinkedBus {
    local: Arc<dyn Bus>,
    relay: RemoteRelay,
    held: Vec<crate::Endpoint>,
}

impl fmt::Debug for LinkedBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkedBus")
            .field("held", &self.held)
            .finish_non_exhaustive()
    }
}

impl LinkedBus {
    /// `local`, with `relay` as its `Relay` capability.
    pub fn new(local: Arc<dyn Bus>, relay: RemoteRelay) -> LinkedBus {
        LinkedBus {
            local,
            relay,
            held: Vec::new(),
        }
    }

    /// Keeps `endpoint` open as long as the bus lives.
    pub fn hold(mut self, endpoint: crate::Endpoint) -> LinkedBus {
        self.held.push(endpoint);
        self
    }
}

static AS_RELAY: Cast<dyn Relay> = |bus| {
    bus.downcast_ref::<LinkedBus>()
        .map(|b| &b.relay as &dyn Relay)
};

impl Bus for LinkedBus {
    fn declare_point(&self, point: PointKey, spec: PointSpec) -> Result<(), Refused> {
        self.local.declare_point(point, spec)
    }

    fn listen_any(
        &self,
        point: PointKey,
        id: String,
        opts: ListenOpts,
        run: AnyListener,
    ) -> Result<(), Refused> {
        self.local.listen_any(point, id, opts, run)
    }

    fn listen_local_any(
        &self,
        point: PointKey,
        id: String,
        opts: ListenOpts,
        run: AnyLocalListener,
    ) -> Result<(), Refused> {
        self.local.listen_local_any(point, id, opts, run)
    }

    fn run_pending(&self) -> Vec<PendingRun> {
        self.local.run_pending()
    }

    fn add_pending_waker(&self, wake: PendingWaker) {
        self.local.add_pending_waker(wake);
    }

    fn unlisten(&self, point: &str, id: &str) -> bool {
        self.local.unlisten(point, id)
    }

    fn forget(&self, owner: &str) -> usize {
        self.local.forget(owner)
    }

    fn alter_any(
        &self,
        point: PointKey,
        input: AnyInput,
        value: AnyValue,
        clone: CloneFn,
    ) -> Altered<AnyValue> {
        self.local.alter_any(point, input, value, clone)
    }

    fn collect_any(&self, point: PointKey, input: AnyInput, fresh: FreshFn) -> Collected<AnyValue> {
        self.local.collect_any(point, input, fresh)
    }

    fn add_failure_sink(&self, sink: FailureSink) {
        self.local.add_failure_sink(sink);
    }

    fn points(&self) -> Vec<PointInfo> {
        self.local.points()
    }

    fn listeners(&self, point: &str) -> Option<Vec<ListenerInfo>> {
        self.local.listeners(point)
    }

    fn capability(&self, contract: TypeId) -> Option<Provided<'_>> {
        if contract == TypeId::of::<dyn Relay>() {
            Some(Provided::new::<dyn Relay>(self, &AS_RELAY))
        } else {
            self.local.capability(contract)
        }
    }
}
