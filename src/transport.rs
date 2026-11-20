//! What a link runs over: a byte stream, opened by dialing or accepting.
//!
//! A transport gives a [`Stream`] each time a connection opens: the
//! connecting side's [`Dial`] (called again to reconnect) and the other
//! side's [`Accept`]. The link writes and reads its frames on it
//! ([`crate::wire`]). The in-process transport is [`memory`]; the Unix
//! socket one is `unix` (feature `unix`).

use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read, Write};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use crate::peer::PeerIdentity;

/// A connection: bytes both ways. The link reads it on one thread and
/// writes it from others, through a clone.
pub trait Stream: Read + Write + Send {
    /// Another handle on the same connection (the writing half).
    fn try_clone_stream(&self) -> io::Result<Box<dyn Stream>>;

    /// Closes the connection both ways: a blocked read on any handle
    /// returns.
    fn shutdown(&self);

    /// Who the peer is, read from the connection, for the link's peer
    /// check ([`crate::Config::peer_policy`]). An error refuses the
    /// peer: a transport that should know and cannot fails closed.
    ///
    /// The default is [`PeerIdentity::Unknown`]: the transport cannot
    /// tell, and the link relies on the transport's own access control
    /// (see [`PeerPolicy::SameUser`](crate::PeerPolicy::SameUser)).
    fn peer_identity(&self) -> io::Result<PeerIdentity> {
        Ok(PeerIdentity::Unknown)
    }
}

/// The connecting side of a transport: opens a connection to the peer,
/// and again to reconnect.
pub trait Dial: Send + Sync + 'static {
    /// A new connection to the peer.
    fn dial(&self) -> io::Result<Box<dyn Stream>>;
}

/// The accepting side of a transport.
pub trait Accept: Send + 'static {
    /// The next connection, waiting at most `wait`; `None` when none
    /// came. An error ends the accepting side.
    fn accept(&self, wait: Duration) -> io::Result<Option<Box<dyn Stream>>>;
}

/// The in-process transport: two endpoints of one process joined by
/// memory pipes, with the same frames as a socket.
pub mod memory {
    use super::*;

    /// One way of a connection: bytes and whether it is closed.
    #[derive(Default)]
    struct Pipe {
        state: Mutex<PipeState>,
        changed: Condvar,
    }

    #[derive(Default)]
    struct PipeState {
        bytes: VecDeque<u8>,
        closed: bool,
    }

    impl Pipe {
        fn lock(&self) -> std::sync::MutexGuard<'_, PipeState> {
            self.state.lock().unwrap_or_else(PoisonError::into_inner)
        }

        fn close(&self) {
            self.lock().closed = true;
            self.changed.notify_all();
        }
    }

    /// One end of an in-process connection.
    pub struct MemoryStream {
        incoming: Arc<Pipe>,
        outgoing: Arc<Pipe>,
    }

    impl fmt::Debug for MemoryStream {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MemoryStream").finish_non_exhaustive()
        }
    }

    impl Read for MemoryStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let mut state = self.incoming.lock();
            loop {
                if !state.bytes.is_empty() {
                    let n = buf.len().min(state.bytes.len());
                    for (slot, byte) in buf.iter_mut().zip(state.bytes.drain(..n)) {
                        *slot = byte;
                    }
                    return Ok(n);
                }
                if state.closed {
                    return Ok(0);
                }
                state = self
                    .incoming
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
    }

    impl Write for MemoryStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut state = self.outgoing.lock();
            if state.closed {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            state.bytes.extend(buf);
            drop(state);
            self.outgoing.changed.notify_all();
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Stream for MemoryStream {
        fn try_clone_stream(&self) -> io::Result<Box<dyn Stream>> {
            Ok(Box::new(MemoryStream {
                incoming: self.incoming.clone(),
                outgoing: self.outgoing.clone(),
            }))
        }

        fn shutdown(&self) {
            self.incoming.close();
            self.outgoing.close();
        }

        fn peer_identity(&self) -> io::Result<PeerIdentity> {
            Ok(PeerIdentity::SameProcess)
        }
    }

    /// The two ends of one connection.
    pub fn connection() -> (MemoryStream, MemoryStream) {
        let (a, b) = (Arc::new(Pipe::default()), Arc::new(Pipe::default()));
        (
            MemoryStream {
                incoming: a.clone(),
                outgoing: b.clone(),
            },
            MemoryStream {
                incoming: b,
                outgoing: a,
            },
        )
    }

    /// The connecting side of an in-process transport ([`pair`]).
    #[derive(Clone)]
    pub struct MemoryDialer {
        to: Arc<Mutex<Sender<MemoryStream>>>,
    }

    impl fmt::Debug for MemoryDialer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MemoryDialer").finish_non_exhaustive()
        }
    }

    impl Dial for MemoryDialer {
        fn dial(&self) -> io::Result<Box<dyn Stream>> {
            let (here, there) = connection();
            self.to
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .send(there)
                .map_err(|_| io::Error::from(io::ErrorKind::ConnectionRefused))?;
            Ok(Box::new(here))
        }
    }

    /// The accepting side of an in-process transport ([`pair`]).
    pub struct MemoryAcceptor {
        from: Mutex<Receiver<MemoryStream>>,
    }

    impl fmt::Debug for MemoryAcceptor {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MemoryAcceptor").finish_non_exhaustive()
        }
    }

    impl Accept for MemoryAcceptor {
        fn accept(&self, wait: Duration) -> io::Result<Option<Box<dyn Stream>>> {
            match self
                .from
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .recv_timeout(wait)
            {
                Ok(stream) => Ok(Some(Box::new(stream))),
                Err(RecvTimeoutError::Timeout) => Ok(None),
                Err(RecvTimeoutError::Disconnected) => {
                    Err(io::Error::from(io::ErrorKind::NotConnected))
                }
            }
        }
    }

    /// An in-process transport: what one endpoint dials, the other
    /// accepts.
    pub fn pair() -> (MemoryDialer, MemoryAcceptor) {
        let (to, from) = mpsc::channel();
        (
            MemoryDialer {
                to: Arc::new(Mutex::new(to)),
            },
            MemoryAcceptor {
                from: Mutex::new(from),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::memory::*;
    use super::*;

    #[test]
    fn a_memory_connection_carries_bytes_and_closes() {
        let (dialer, acceptor) = pair();
        let mut a = dialer.dial().unwrap();
        let mut b = acceptor.accept(Duration::from_secs(1)).unwrap().unwrap();
        a.write_all(b"hello").unwrap();
        let mut got = [0u8; 5];
        b.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"hello");
        let mut reader = b.try_clone_stream().unwrap();
        let t = std::thread::spawn(move || {
            let mut buf = [0u8; 1];
            reader.read(&mut buf).unwrap()
        });
        a.shutdown();
        assert_eq!(t.join().unwrap(), 0, "a blocked read ends at shutdown");
        assert!(b.write_all(b"x").is_err());
        assert!(
            acceptor
                .accept(Duration::from_millis(10))
                .unwrap()
                .is_none()
        );
    }
}
