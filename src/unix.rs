//! The Unix socket transport (feature `unix`): a link between two
//! processes of one machine.
//!
//! The accepting side binds a socket file ([`UnixAcceptor::bind`]) and
//! makes it readable and writable by its own user only (mode `0600`).
//! Put the socket in a directory that user owns (`$XDG_RUNTIME_DIR`, for
//! instance). The connecting side ([`UnixDialer`]) dials the path again
//! each time the link reconnects.
//!
//! # Peer identity
//!
//! On each connection, both sides read the peer's user id from the
//! kernel and check it against [`Config::peer_policy`](crate::Config::peer_policy)
//! before the handshake: `SO_PEERCRED` on Linux and Android (through
//! rustix), `getpeereid` on macOS, iOS and the BSDs (through nix). On a
//! Unix with neither, the peer is [`PeerIdentity::Unknown`]: the default
//! policy then relies on the file mode `0600` alone and says so
//! ([`LinkEvent::PeerUnverified`](crate::LinkEvent::PeerUnverified)),
//! and a policy naming uids refuses every peer.

use std::fmt;
use std::io;
use std::net::Shutdown;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use crate::peer::PeerIdentity;
use crate::transport::{Accept, Dial, Stream};

impl Stream for UnixStream {
    fn try_clone_stream(&self) -> io::Result<Box<dyn Stream>> {
        Ok(Box::new(self.try_clone()?))
    }

    fn shutdown(&self) {
        let _ = UnixStream::shutdown(self, Shutdown::Both);
    }

    fn peer_identity(&self) -> io::Result<PeerIdentity> {
        peer_identity(self)
    }
}

/// The peer's user id, from the kernel.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn peer_identity(stream: &UnixStream) -> io::Result<PeerIdentity> {
    let cred = rustix::net::sockopt::socket_peercred(stream)?;
    Ok(PeerIdentity::Uid(cred.uid.as_raw()))
}

/// The peer's user id, from the kernel.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn peer_identity(stream: &UnixStream) -> io::Result<PeerIdentity> {
    let (uid, _) = nix::unistd::getpeereid(stream).map_err(io::Error::from)?;
    Ok(PeerIdentity::Uid(uid.as_raw()))
}

/// No peer credential call this crate knows on this platform.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
fn peer_identity(_: &UnixStream) -> io::Result<PeerIdentity> {
    Ok(PeerIdentity::Unknown)
}

/// The connecting side: dials a socket path.
#[derive(Clone, Debug)]
pub struct UnixDialer {
    path: PathBuf,
}

impl UnixDialer {
    /// Dials `path` (each time the link connects).
    pub fn new(path: impl Into<PathBuf>) -> UnixDialer {
        UnixDialer { path: path.into() }
    }
}

impl Dial for UnixDialer {
    fn dial(&self) -> io::Result<Box<dyn Stream>> {
        Ok(Box::new(UnixStream::connect(&self.path)?))
    }
}

/// The accepting side: a bound socket file.
pub struct UnixAcceptor {
    listener: UnixListener,
    path: PathBuf,
}

impl fmt::Debug for UnixAcceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixAcceptor")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl UnixAcceptor {
    /// Binds `path`, replacing a socket file left there by a process
    /// that ended (any other kind of file is an error), with mode `0600`.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixAcceptor> {
        let path = path.as_ref().to_path_buf();
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if !meta.file_type().is_socket() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{} exists and is not a socket", path.display()),
                ));
            }
            if UnixStream::connect(&path).is_ok() {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("{} is served by a running process", path.display()),
                ));
            }
            std::fs::remove_file(&path)?;
        }
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        Ok(UnixAcceptor { listener, path })
    }

    /// The socket file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Accept for UnixAcceptor {
    fn accept(&self, wait: Duration) -> io::Result<Option<Box<dyn Stream>>> {
        let until = Instant::now() + wait;
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false)?;
                    return Ok(Some(Box::new(stream)));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= until {
                        return Ok(None);
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }
}

impl Drop for UnixAcceptor {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
