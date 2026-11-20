//! Who may be on the other end of a link: the peer's identity as the
//! transport reads it ([`PeerIdentity`]) and the rule it is checked
//! against ([`PeerPolicy`], [`Config::peer_policy`](crate::Config::peer_policy)).
//!
//! The check runs on every connection, on the accepting side and on the
//! dialing side, before the handshake: a refused peer is closed and
//! reported ([`LinkEvent::Refused`](crate::LinkEvent::Refused)), and
//! nothing crosses.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// The peer's identity, as the transport reads it
/// ([`Stream::peer_identity`](crate::transport::Stream::peer_identity)).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerIdentity {
    /// The peer runs as this user id, read by the kernel from the
    /// connection (`SO_PEERCRED` on Linux and Android, `getpeereid` on
    /// macOS and the BSDs).
    Uid(u32),
    /// The peer is in this process (the in-process transport): it is
    /// this process's user, whatever the policy.
    SameProcess,
    /// The transport cannot tell who the peer is: the platform has no
    /// peer credential call, or the transport does not read one.
    Unknown,
}

/// Which peers a link accepts ([`Config::peer_policy`](crate::Config::peer_policy)).
///
/// Parsed from and shown as `same-user`, `any`, or `uids:1000,1001`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerPolicy {
    /// The default: the peer's user id must be this process's.
    ///
    /// Where the transport cannot read the peer's identity
    /// ([`PeerIdentity::Unknown`]), the link falls back to what the
    /// transport itself guarantees (for the Unix socket: its file mode
    /// `0600`, so only this user can connect) and reports that it did
    /// ([`LinkEvent::PeerUnverified`](crate::LinkEvent::PeerUnverified)).
    #[default]
    SameUser,
    /// The peer's user id must be one of these. A peer whose identity
    /// cannot be read is refused.
    Uids(BTreeSet<u32>),
    /// Any peer the transport lets connect, unchecked.
    ///
    /// **Unsafe on a host with several users** when the socket is
    /// reachable by others: any process that can connect then speaks
    /// for the peer. Only for a socket whose access is controlled
    /// otherwise, or for tests.
    Any,
}

/// What a check of a peer decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The peer is accepted.
    Pass,
    /// The peer is accepted without being checked; why.
    Unverified(String),
    /// The peer is refused; why.
    Refuse(String),
}

impl PeerPolicy {
    /// Checks `peer` against the policy, `own` being this process's
    /// user id (if the platform has one).
    pub(crate) fn check(&self, peer: &PeerIdentity, own: Option<u32>) -> Verdict {
        match (self, peer) {
            (PeerPolicy::Any, _) | (_, PeerIdentity::SameProcess) => Verdict::Pass,
            (PeerPolicy::SameUser, PeerIdentity::Uid(uid)) => match own {
                Some(own) if own == *uid => Verdict::Pass,
                Some(own) => Verdict::Refuse(format!(
                    "the peer runs as uid {uid}, this process as uid {own}"
                )),
                None => Verdict::Refuse(format!(
                    "the peer runs as uid {uid}, and this process's uid is unknown"
                )),
            },
            (PeerPolicy::SameUser, PeerIdentity::Unknown) => Verdict::Unverified(
                "no peer credentials on this transport or platform: relying on the \
                 transport's own access control (the Unix socket's file mode 0600)"
                    .into(),
            ),
            (PeerPolicy::Uids(uids), PeerIdentity::Uid(uid)) => {
                if uids.contains(uid) {
                    Verdict::Pass
                } else {
                    Verdict::Refuse(format!("the peer runs as uid {uid}, not an allowed one"))
                }
            }
            (PeerPolicy::Uids(_), PeerIdentity::Unknown) => Verdict::Refuse(
                "the peer's uid cannot be read, and the policy names allowed uids".into(),
            ),
        }
    }
}

impl fmt::Display for PeerPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerPolicy::SameUser => f.write_str("same-user"),
            PeerPolicy::Any => f.write_str("any"),
            PeerPolicy::Uids(uids) => {
                f.write_str("uids:")?;
                for (i, uid) in uids.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{uid}")?;
                }
                Ok(())
            }
        }
    }
}

/// A peer policy that does not parse ([`PeerPolicy::from_str`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsePolicyError(String);

impl fmt::Display for ParsePolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "not a peer policy: {:?} (expected same-user, any or uids:<uid>[,<uid>...])",
            self.0
        )
    }
}

impl std::error::Error for ParsePolicyError {}

impl FromStr for PeerPolicy {
    type Err = ParsePolicyError;

    fn from_str(s: &str) -> Result<PeerPolicy, ParsePolicyError> {
        let error = || ParsePolicyError(s.to_owned());
        match s.trim() {
            "same-user" => Ok(PeerPolicy::SameUser),
            "any" => Ok(PeerPolicy::Any),
            other => {
                let list = other.strip_prefix("uids:").ok_or_else(error)?;
                let uids = list
                    .split(',')
                    .map(|uid| uid.trim().parse::<u32>())
                    .collect::<Result<BTreeSet<u32>, _>>()
                    .map_err(|_| error())?;
                Ok(PeerPolicy::Uids(uids))
            }
        }
    }
}

/// This process's user id, where the platform has one the crate can
/// read without `unsafe`.
pub(crate) fn own_uid() -> Option<u32> {
    #[cfg(all(feature = "unix", any(target_os = "linux", target_os = "android")))]
    {
        Some(rustix::process::getuid().as_raw())
    }
    #[cfg(all(
        feature = "unix",
        any(
            target_os = "macos",
            target_os = "ios",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly"
        )
    ))]
    {
        Some(nix::unistd::getuid().as_raw())
    }
    #[cfg(not(all(
        feature = "unix",
        any(
            target_os = "linux",
            target_os = "android",
            target_os = "macos",
            target_os = "ios",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly"
        )
    )))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_parse_and_show() {
        assert_eq!("same-user".parse(), Ok(PeerPolicy::SameUser));
        assert_eq!(" any ".parse(), Ok(PeerPolicy::Any));
        assert_eq!(
            "uids:1001, 1000".parse(),
            Ok(PeerPolicy::Uids([1000, 1001].into()))
        );
        assert_eq!(PeerPolicy::default(), PeerPolicy::SameUser);
        for text in ["same-user", "any", "uids:7,1000"] {
            assert_eq!(text.parse::<PeerPolicy>().unwrap().to_string(), text);
        }
        for bad in ["", "root", "uids:", "uids:x", "uids:1,-2", "same_user"] {
            assert!(bad.parse::<PeerPolicy>().is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn same_user_passes_its_own_uid_and_refuses_another() {
        let p = PeerPolicy::SameUser;
        assert_eq!(p.check(&PeerIdentity::Uid(1000), Some(1000)), Verdict::Pass);
        assert!(matches!(
            p.check(&PeerIdentity::Uid(1001), Some(1000)),
            Verdict::Refuse(_)
        ));
        assert!(matches!(
            p.check(&PeerIdentity::Uid(1000), None),
            Verdict::Refuse(_)
        ));
        assert_eq!(p.check(&PeerIdentity::SameProcess, None), Verdict::Pass);
        assert!(matches!(
            p.check(&PeerIdentity::Unknown, Some(1000)),
            Verdict::Unverified(_)
        ));
    }

    #[test]
    fn uids_and_any() {
        let p = PeerPolicy::Uids([7, 9].into());
        assert_eq!(p.check(&PeerIdentity::Uid(9), Some(1000)), Verdict::Pass);
        assert!(matches!(
            p.check(&PeerIdentity::Uid(1000), Some(1000)),
            Verdict::Refuse(_)
        ));
        assert!(matches!(
            p.check(&PeerIdentity::Unknown, Some(1000)),
            Verdict::Refuse(_)
        ));
        let any = PeerPolicy::Any;
        assert_eq!(any.check(&PeerIdentity::Uid(0), Some(1000)), Verdict::Pass);
        assert_eq!(any.check(&PeerIdentity::Unknown, None), Verdict::Pass);
    }
}
