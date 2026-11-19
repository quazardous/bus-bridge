//! bus-kernel's crossing conformance group, run on a bus whose relay is
//! behind a link: every `Relay` call the checks make (cursor, resume,
//! ingest, answer) is a frame across the link to the far end, which
//! serves it on its kernel. Run over the in-process transport and over a
//! Unix socket, it must pass as it does on the kernel itself.

#[path = "support/memory.rs"]
mod memory;

use std::sync::Arc;
use std::time::Duration;

use bus_bridge::bus_contract::Bus;
use bus_bridge::bus_contract_crossing::Origin;
use bus_bridge::transport::{Accept, Dial, memory as pipe};
use bus_bridge::{Config, Endpoint, LinkedBus};
use bus_kernel::conformance::{self, Contract, Outcome};

/// The far kernel served through a link: the bus the checks get holds
/// its points and listeners, its relay is reached across the link.
fn linked(dial: impl Dial, accept: impl Accept) -> Arc<dyn Bus> {
    let far = memory::full();
    // The near end's own bus: an empty kernel, so nothing it sends back
    // lands in the far journal the checks read.
    let near = memory::full();
    let serve = Endpoint::new(
        far.clone(),
        // The checks name the origin of each call: the far end keeps
        // what the caller claims.
        Config::new("far", Origin::process("conformance")).trust_claimed_origin(),
    )
    .unwrap();
    serve.accept(accept);
    let client = Endpoint::new(near, Config::new("near", Origin::process("far"))).unwrap();
    client.connect(dial);
    assert!(client.wait_connected(Duration::from_secs(10)), "linked");
    Arc::new(
        LinkedBus::new(far, client.remote_relay())
            .hold(client)
            .hold(serve),
    )
}

fn in_process() -> Arc<dyn Bus> {
    let (dial, accept) = pipe::pair();
    linked(dial, accept)
}

fn kernel() -> Arc<dyn Bus> {
    memory::full()
}

fn assert_passes(make: conformance::Make) {
    let report = conformance::report_contract(Contract::Crossing, make);
    assert_eq!(report.len(), 15, "the whole crossing group");
    for (name, outcome) in &report {
        assert_eq!(*outcome, Outcome::Passed, "{name}");
    }
}

#[test]
fn the_crossing_group_passes_across_an_in_process_link() {
    assert_passes(in_process);
}

#[test]
fn the_kernel_itself_gives_the_same_verdicts() {
    let direct = conformance::report_contract(Contract::Crossing, kernel);
    let linked = conformance::report_contract(Contract::Crossing, in_process);
    assert_eq!(direct, linked);
}

#[cfg(all(unix, feature = "unix"))]
mod unix {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    use bus_bridge::unix::{UnixAcceptor, UnixDialer};

    fn socket_path() -> std::path::PathBuf {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        std::env::temp_dir().join(format!(
            "bus-bridge-conf-{}-{}.sock",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn over_a_socket() -> Arc<dyn Bus> {
        let path = socket_path();
        let accept = UnixAcceptor::bind(&path).unwrap();
        linked(UnixDialer::new(&path), accept)
    }

    #[test]
    fn the_crossing_group_passes_across_a_unix_socket() {
        assert_passes(over_a_socket);
    }
}
