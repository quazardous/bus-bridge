//! Two kernels, two "processes" in one test, joined by a link.

#[path = "support/memory.rs"]
mod memory;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bus_bridge::bus_contract::{
    Bus, Facet, HookError, ListenOpts, Point, PointDecl, PointId, PointKey, PointKind, declare_all,
};
use bus_bridge::bus_contract_crossing::{Codec, Lost, Origin, Relayed, RelayedRequest, relayable};
use bus_bridge::transport::{Accept, Dial, memory as pipe};
use bus_bridge::wire::Outcome;
use bus_bridge::{Config, Endpoint, Flow, LinkError, LinkEvent};
use bus_kernel::{BusJournal, Kernel};

/// A relayable event.
struct Synced;
impl Point for Synced {
    const ID: PointId = PointId::new("test.synced");
    const KIND: PointKind = PointKind::Event;
    type Input = u32;
    type Value = ();
}
impl Relayed for Synced {
    fn encode(n: &u32) -> String {
        n.to_string()
    }
    fn decode(text: &str) -> Result<u32, String> {
        text.parse().map_err(|e| format!("{e}"))
    }
}

/// A relayable request: the value lists who handled it.
struct Delete;
impl Point for Delete {
    const ID: PointId = PointId::new("test.delete");
    const KIND: PointKind = PointKind::Request;
    type Input = String;
    type Value = Vec<String>;
}
impl Relayed for Delete {
    fn encode(path: &String) -> String {
        path.clone()
    }
    fn decode(text: &str) -> Result<String, String> {
        Ok(text.to_owned())
    }
}
impl RelayedRequest for Delete {
    fn encode_value(v: &Vec<String>) -> String {
        v.join(",")
    }
    fn decode_value(text: &str) -> Result<Vec<String>, String> {
        Ok(text
            .split(',')
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect())
    }
}

const SYNCED: Codec = Codec::of::<Synced>();
const DELETE: Codec = Codec::request::<Delete>();
const SYNCED_FACETS: &[Facet] = &[relayable(&SYNCED)];
const DELETE_FACETS: &[Facet] = &[relayable(&DELETE)];

const WAIT: Duration = Duration::from_secs(10);

fn bus(window: usize) -> Arc<Kernel> {
    let k = memory::kernel(true, window);
    let table = [
        PointDecl::of::<Synced>("Synced.").facets(SYNCED_FACETS),
        PointDecl::of::<Delete>("Delete?").facets(DELETE_FACETS),
    ];
    let refused = declare_all(&*k, &table);
    assert!(refused.is_empty(), "{refused:?}");
    k
}

fn heard(bus: &Kernel) -> Arc<Mutex<Vec<u32>>> {
    let heard: Arc<Mutex<Vec<u32>>> = Arc::default();
    let h = heard.clone();
    bus.listen::<Synced>("test.heard", ListenOpts::new(), move |n, _| {
        h.lock().unwrap().push(*n);
        Ok(())
    })
    .unwrap();
    heard
}

fn events(end: &Endpoint) -> Arc<Mutex<Vec<LinkEvent>>> {
    let seen: Arc<Mutex<Vec<LinkEvent>>> = Arc::default();
    let s = seen.clone();
    end.on_event(move |e| s.lock().unwrap().push(e.clone()));
    seen
}

/// Waits until `f` holds.
fn until(what: &str, f: impl Fn() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < WAIT, "timed out: {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

/// `a` accepts, `b` dials.
fn join(
    a: &Arc<Kernel>,
    b: &Arc<Kernel>,
    b_config: Config,
    (dial, accept): (impl Dial, impl Accept),
) -> (Endpoint, Endpoint) {
    let ea = Endpoint::new(a.clone(), Config::new("a", Origin::process("b"))).unwrap();
    let eb = Endpoint::new(b.clone(), b_config).unwrap();
    ea.accept(accept);
    eb.connect(dial);
    assert!(ea.wait_connected(WAIT) && eb.wait_connected(WAIT));
    (ea, eb)
}

fn b_config() -> Config {
    Config::new("b", Origin::process("a"))
}

#[test]
fn events_cross_both_ways_with_the_origin_of_the_link() {
    let (a, b) = (bus(1024), bus(1024));
    let (ha, hb) = (heard(&a), heard(&b));
    let (ea, _eb) = join(&a, &b, b_config(), pipe::pair());
    let seen = events(&ea);

    a.journal().unwrap().publish::<Synced>(7).unwrap();
    until("b hears 7", || *hb.lock().unwrap() == [7]);
    let kept = b.journal().unwrap().since(0, 10).entries;
    let entry = kept.iter().find(|e| e.kind == Synced::ID.as_str()).unwrap();
    assert_eq!(entry.origin, Some(Origin::process("a")), "set by the link");

    b.journal().unwrap().publish::<Synced>(8).unwrap();
    until("a hears 8", || ha.lock().unwrap().contains(&8));
    assert_eq!(*ha.lock().unwrap(), [7, 8]);
    thread::sleep(Duration::from_millis(100));
    // Each side heard each event once: its own, and the other's taken
    // in, never sent back.
    assert_eq!(*hb.lock().unwrap(), [7, 8], "no echo back to b");
    assert_eq!(*ha.lock().unwrap(), [7, 8], "no echo back to a");

    until("a's receipt", || {
        seen.lock().unwrap().iter().any(|e| {
            matches!(e, LinkEvent::Receipt(r) if r.kind == Synced::ID.as_str()
                && matches!(r.outcome, Outcome::Taken(_)))
        })
    });
}

#[test]
fn a_refused_entry_comes_back_as_a_receipt() {
    let (a, b) = (bus(1024), bus(1024));
    b.relay_authorities_refuse_all();
    let (ea, _eb) = join(&a, &b, b_config(), pipe::pair());
    let seen = events(&ea);
    a.journal().unwrap().publish::<Synced>(1).unwrap();
    until("a refusal receipt", || {
        seen.lock().unwrap().iter().any(|e| {
            matches!(e, LinkEvent::Receipt(r) if matches!(&r.outcome, Outcome::Refused(why) if why.contains("closed")))
        })
    });
}

trait RefuseAll {
    fn relay_authorities_refuse_all(&self);
}

impl RefuseAll for Kernel {
    fn relay_authorities_refuse_all(&self) {
        use bus_bridge::bus_contract_crossing::BusCrossing;
        self.relay().unwrap().authorize(|_| Err("closed".into()));
    }
}

#[test]
fn a_request_is_answered_by_the_far_listeners() {
    let (a, b) = (bus(1024), bus(1024));
    let (ea, _eb) = join(&a, &b, b_config(), pipe::pair());
    a.listen::<Delete>("a.confirm", ListenOpts::new(), |_, did| {
        did.push("a.confirm".into());
        Ok(())
    })
    .unwrap();
    ea.listen_far(
        PointKey::of::<Delete>(),
        "a.far",
        Duration::from_secs(5),
        ListenOpts::new().after("a.confirm"),
    )
    .unwrap();
    let asked: Arc<Mutex<Vec<String>>> = Arc::default();
    let s = asked.clone();
    b.listen::<Delete>("b.vault", ListenOpts::new(), move |path, did| {
        s.lock().unwrap().push(path.clone());
        did.push("b.vault".into());
        Ok(())
    })
    .unwrap();

    let out = a.alter::<Delete>("/x.jpg".into(), Vec::new());
    assert!(out.refused.is_none(), "{:?}", out.refused);
    assert_eq!(out.value, ["a.confirm", "b.vault"]);
    assert_eq!(*asked.lock().unwrap(), ["/x.jpg"]);

    b.listen::<Delete>("b.guard", ListenOpts::new().before("b.vault"), |_, _| {
        Err(HookError::refuse("read-only"))
    })
    .unwrap();
    let out = a.alter::<Delete>("/y.jpg".into(), Vec::new());
    let refusal = out.refused.expect("vetoed on the far side");
    assert!(refusal.reason.contains("b.guard") && refusal.reason.contains("read-only"));
    assert_eq!(out.value, ["a.confirm"]);
}

#[test]
fn listen_far_needs_a_relayable_request() {
    let a = bus(16);
    let ea = Endpoint::new(a.clone(), Config::new("a", Origin::process("b"))).unwrap();
    assert_eq!(
        ea.listen_far(
            PointKey::of::<Synced>(),
            "a.far",
            Duration::from_secs(1),
            ListenOpts::new()
        ),
        Err(LinkError::NotARelayableRequest(Synced::ID.as_str().into()))
    );
    let bare: Arc<dyn Bus> = Arc::new(Kernel::new());
    assert!(matches!(
        Endpoint::new(bare, Config::new("x", Origin::process("y"))),
        Err(LinkError::NoRelay)
    ));
}

#[test]
fn with_no_link_an_ask_is_refused_at_once() {
    let a = bus(16);
    let ea = Endpoint::new(a.clone(), Config::new("a", Origin::process("b"))).unwrap();
    ea.listen_far(
        PointKey::of::<Delete>(),
        "a.far",
        Duration::from_secs(5),
        ListenOpts::new(),
    )
    .unwrap();
    let start = Instant::now();
    let out = a.alter::<Delete>("/x".into(), Vec::new());
    assert!(out.refused.is_some(), "no answer is a no");
    assert!(start.elapsed() < Duration::from_secs(1), "not left waiting");
}

#[test]
fn a_reconnection_resumes_from_the_cursor_without_duplicates() {
    let (a, b) = (bus(1024), bus(1024));
    let hb = heard(&b);
    let (ea, eb) = join(
        &a,
        &b,
        b_config().reconnect(Duration::from_millis(300)),
        pipe::pair(),
    );
    let journal = a.journal().unwrap();
    journal.publish::<Synced>(1).unwrap();
    until("b hears 1", || *hb.lock().unwrap() == [1]);

    eb.disconnect();
    until("down", || !ea.stats().connected);
    journal.publish::<Synced>(2).unwrap();
    journal.publish::<Synced>(3).unwrap();
    until("b hears 2 and 3", || hb.lock().unwrap().len() == 3);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(*hb.lock().unwrap(), [1, 2, 3], "each once, in order");
    assert!(eb.stats().connections >= 2, "reconnected");
    assert_eq!(eb.stats().lost, 0);
}

#[test]
fn entries_that_left_the_window_meanwhile_are_reported_lost() {
    let (a, b) = (bus(4), bus(1024));
    let hb = heard(&b);
    let (ea, eb) = join(
        &a,
        &b,
        b_config().reconnect(Duration::from_millis(500)),
        pipe::pair(),
    );
    let seen = events(&eb);
    let journal = a.journal().unwrap();
    journal.publish::<Synced>(0).unwrap();
    until("b hears 0", || *hb.lock().unwrap() == [0]);

    eb.disconnect();
    until("down", || !ea.stats().connected);
    for n in 1..=10 {
        journal.publish::<Synced>(n).unwrap();
    }
    until("b hears the last", || hb.lock().unwrap().contains(&10));
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|e| *e == LinkEvent::Lost(Lost::Gap)),
        "the gap reported"
    );
    assert_eq!(*hb.lock().unwrap(), [0, 7, 8, 9, 10], "the window kept");
    assert_eq!(eb.stats().lost, 1);
}

#[test]
fn asks_past_the_bound_wait_and_are_reported() {
    let (a, b) = (bus(1024), bus(1024));
    let (ea, _eb) = join(&a, &b, b_config(), pipe::pair());
    // One ask in flight at a time on a's side.
    drop(ea);
    let ea = Endpoint::new(
        a.clone(),
        Config::new("a2", Origin::process("b")).max_in_flight(1),
    )
    .unwrap();
    let eb = Endpoint::new(b.clone(), b_config()).unwrap();
    let (dial, accept) = pipe::pair();
    ea.accept(accept);
    eb.connect(dial);
    assert!(ea.wait_connected(WAIT));
    let seen = events(&ea);
    ea.listen_far(
        PointKey::of::<Delete>(),
        "a.far",
        Duration::from_secs(5),
        ListenOpts::new(),
    )
    .unwrap();
    b.listen::<Delete>("b.slow", ListenOpts::new(), |_, did| {
        thread::sleep(Duration::from_millis(200));
        did.push("b.slow".into());
        Ok(())
    })
    .unwrap();
    let asks: Vec<_> = (0..2)
        .map(|i| {
            let a = a.clone();
            thread::spawn(move || a.alter::<Delete>(format!("/{i}"), Vec::new()))
        })
        .collect();
    for ask in asks {
        let out = ask.join().unwrap();
        assert!(out.refused.is_none(), "{:?}", out.refused);
        assert_eq!(out.value, ["b.slow"]);
    }
    assert!(
        seen.lock().unwrap().iter().any(|e| matches!(
            e,
            LinkEvent::Saturated {
                flow: Flow::Asks,
                max: 1
            }
        )),
        "the full flow reported"
    );
    assert!(ea.stats().saturated >= 1);
}

#[test]
fn an_ask_past_its_deadline_is_refused_when_the_bound_stays_full() {
    let (a, b) = (bus(1024), bus(1024));
    let ea = Endpoint::new(
        a.clone(),
        Config::new("a", Origin::process("b")).max_in_flight(1),
    )
    .unwrap();
    let eb = Endpoint::new(b.clone(), b_config()).unwrap();
    let (dial, accept) = pipe::pair();
    ea.accept(accept);
    eb.connect(dial);
    assert!(ea.wait_connected(WAIT));
    ea.listen_far(
        PointKey::of::<Delete>(),
        "a.far",
        Duration::from_millis(150),
        ListenOpts::new(),
    )
    .unwrap();
    b.listen::<Delete>("b.slow", ListenOpts::new(), |_, _| {
        thread::sleep(Duration::from_millis(400));
        Ok(())
    })
    .unwrap();
    let first = {
        let a = a.clone();
        thread::spawn(move || a.alter::<Delete>("/1".into(), Vec::new()))
    };
    thread::sleep(Duration::from_millis(30));
    let second = a.alter::<Delete>("/2".into(), Vec::new());
    assert!(second.refused.is_some(), "waited its deadline for a slot");
    assert!(first.join().unwrap().refused.is_some(), "too slow");
}

#[cfg(all(unix, feature = "unix"))]
mod unix {
    use super::*;
    use bus_bridge::unix::{UnixAcceptor, UnixDialer};

    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("bus-bridge-{name}-{}.sock", std::process::id()))
    }

    #[test]
    fn events_and_asks_cross_a_unix_socket() {
        let path = path("both");
        let (a, b) = (bus(1024), bus(1024));
        let hb = heard(&b);
        let accept = UnixAcceptor::bind(&path).unwrap();
        let (ea, _eb) = join(&a, &b, b_config(), (UnixDialer::new(&path), accept));
        a.journal().unwrap().publish::<Synced>(5).unwrap();
        until("b hears 5", || *hb.lock().unwrap() == [5]);
        ea.listen_far(
            PointKey::of::<Delete>(),
            "a.far",
            Duration::from_secs(5),
            ListenOpts::new(),
        )
        .unwrap();
        b.listen::<Delete>("b.vault", ListenOpts::new(), |_, did| {
            did.push("b.vault".into());
            Ok(())
        })
        .unwrap();
        let out = a.alter::<Delete>("/x".into(), Vec::new());
        assert_eq!(out.value, ["b.vault"]);
    }

    #[test]
    fn the_socket_file_is_its_owners_only() {
        use std::os::unix::fs::PermissionsExt;
        let path = path("mode");
        let accept = UnixAcceptor::bind(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        assert!(UnixAcceptor::bind(&path).is_err(), "in use");
        drop(accept);
        assert!(!path.exists(), "removed when dropped");
    }

    /// The far process restarts: a new journal run on the same socket.
    #[test]
    fn a_peer_that_restarted_is_reported() {
        let path = path("restart");
        let b = bus(1024);
        let hb = heard(&b);
        let eb = Endpoint::new(b.clone(), b_config().reconnect(Duration::from_millis(50))).unwrap();
        let seen = events(&eb);

        let a = bus(1024);
        let ea = Endpoint::new(a.clone(), Config::new("a", Origin::process("b"))).unwrap();
        ea.accept(UnixAcceptor::bind(&path).unwrap());
        eb.connect(UnixDialer::new(&path));
        assert!(eb.wait_connected(WAIT));
        a.journal().unwrap().publish::<Synced>(1).unwrap();
        until("b hears 1", || *hb.lock().unwrap() == [1]);
        drop(ea);
        until("down", || !eb.stats().connected);
        // Let the old acceptor's thread end and free the path.
        thread::sleep(Duration::from_millis(100));

        let a2 = bus(1024);
        a2.journal().unwrap().publish::<Synced>(2).unwrap();
        let ea2 = Endpoint::new(a2.clone(), Config::new("a", Origin::process("b"))).unwrap();
        ea2.accept(UnixAcceptor::bind(&path).unwrap());
        until("b hears 2", || hb.lock().unwrap().contains(&2));
        assert!(
            seen.lock()
                .unwrap()
                .iter()
                .any(|e| *e == LinkEvent::Lost(Lost::Restarted))
        );
        assert_eq!(*hb.lock().unwrap(), [1, 2]);
    }
}
