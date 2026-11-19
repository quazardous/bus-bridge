//! The wire format: what a link sends, as length-prefixed frames.
//!
//! Each frame is a 4-byte big-endian length, then that many bytes: a tag
//! byte and the frame's fields. Integers are little-endian, strings are
//! a 4-byte length and UTF-8, an option is a 0/1 byte then its value.
//! Payloads (an event's input, a request's value) are the text the
//! point's contract `Codec` wrote; this format only carries them.

use std::io::{self, Read, Write};
use std::time::Duration;

use bus_contract_crossing::{
    Answer, Cursor, IngestError, Lost, Origin, RelayedAnswer, RelayedAsk, RelayedEntry, Resumed,
};

/// The protocol version a link speaks; a peer of another version is
/// refused at the handshake.
pub const PROTOCOL: u32 = 1;

/// The largest frame a link reads: a bigger one is a broken or hostile
/// peer, and the connection is dropped.
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// What crosses a link.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Frame {
    /// The first frame each side sends: its protocol, its name (only
    /// shown in events, never used as an origin), and the cursor of the
    /// peer's journal it has taken up to.
    Hello {
        /// [`PROTOCOL`].
        protocol: u32,
        /// The sender's name.
        name: String,
        /// Where the sender wants the peer's entries to resume.
        resume: Cursor,
    },
    /// Entries of the sender's journal, what was lost before them, and
    /// the cursor after them.
    Entries {
        /// The relayable entries, oldest first.
        entries: Vec<RelayedEntry>,
        /// Whether entries were missed before these.
        lost: Option<Lost>,
        /// Where the reader stands after them.
        next: Cursor,
    },
    /// What the receiver did with an [`Frame::Entries`]: the cursor it
    /// took up to and one receipt per entry.
    Taken {
        /// The cursor taken up to.
        next: Cursor,
        /// One per entry, in order.
        receipts: Vec<Receipt>,
    },
    /// A request asked of the peer.
    Ask(RelayedAsk),
    /// The answer to an [`Frame::Ask`].
    Answer(RelayedAnswer),
    /// A call on the peer's relay ([`crate::RemoteRelay`]).
    Call {
        /// The call's number, echoed by its [`Frame::Reply`].
        id: u64,
        /// What is called.
        call: Call,
    },
    /// The reply to a [`Frame::Call`].
    Reply {
        /// The call's number.
        id: u64,
        /// What it returned.
        reply: Reply,
    },
}

/// What happened to one entry sent across a link: its publication
/// receipt.
///
/// The idea of a receipt per publication, saying who took it and who
/// refused it and why, comes from qubit-event-bus's `PublishReceipt`
/// (<https://github.com/qubit-ltd/rs-event-bus>).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Receipt {
    /// The entry's number in the sender's journal.
    pub seq: u64,
    /// Its point's id.
    pub kind: String,
    /// What the receiver did.
    pub outcome: Outcome,
}

/// What a receiver did with an entry ([`Receipt`]).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// Taken in: its number in the receiver's journal.
    Taken(u64),
    /// Refused, nothing recorded: why ([`IngestError`]'s text).
    Refused(String),
    /// Already taken before (a resend after a reconnect): dropped.
    Duplicate,
}

/// A call on a relay across a link ([`crate::RemoteRelay`]).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Call {
    /// `Relay::cursor`.
    Cursor,
    /// `Relay::resume`.
    Resume {
        /// From.
        from: Cursor,
        /// At most.
        limit: u64,
    },
    /// `Relay::ingest`, with the origin the caller claims.
    Ingest {
        /// The claimed origin.
        origin: Origin,
        /// The entry.
        entry: RelayedEntry,
    },
    /// `Relay::answer`, with the origin the caller claims.
    Answer {
        /// The claimed origin.
        origin: Origin,
        /// The ask.
        ask: RelayedAsk,
    },
}

/// What a [`Call`] returned.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reply {
    /// For [`Call::Cursor`].
    Cursor(Cursor),
    /// For [`Call::Resume`].
    Resumed(Resumed),
    /// For [`Call::Ingest`].
    Ingested(Result<u64, IngestError>),
    /// For [`Call::Answer`].
    Answered(RelayedAnswer),
}

/// Why a frame could not be read.
#[derive(Debug)]
pub enum WireError {
    /// The stream failed or ended.
    Io(io::Error),
    /// The bytes are not a frame of this protocol.
    Malformed(String),
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Io(e) => write!(f, "{e}"),
            WireError::Malformed(why) => write!(f, "malformed frame: {why}"),
        }
    }
}

impl std::error::Error for WireError {}

impl From<io::Error> for WireError {
    fn from(e: io::Error) -> WireError {
        WireError::Io(e)
    }
}

/// Writes `frame` to `out`, length first, in one write.
pub fn write_frame(out: &mut dyn Write, frame: &Frame) -> io::Result<()> {
    let body = encode(frame);
    let mut bytes = Vec::with_capacity(body.len() + 4);
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&body);
    out.write_all(&bytes)?;
    out.flush()
}

/// Reads one frame from `input`.
pub fn read_frame(input: &mut dyn Read) -> Result<Frame, WireError> {
    let mut len = [0u8; 4];
    input.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(WireError::Malformed(format!(
            "{len} bytes, more than {MAX_FRAME}"
        )));
    }
    let mut body = vec![0u8; len];
    input.read_exact(&mut body)?;
    decode(&body)
}

/// The bytes of `frame`, without its length.
pub fn encode(frame: &Frame) -> Vec<u8> {
    let mut w = Writer(Vec::new());
    match frame {
        Frame::Hello {
            protocol,
            name,
            resume,
        } => {
            w.u8(1);
            w.u32(*protocol);
            w.str(name);
            w.cursor(*resume);
        }
        Frame::Entries {
            entries,
            lost,
            next,
        } => {
            w.u8(2);
            w.entries(entries);
            w.lost(*lost);
            w.cursor(*next);
        }
        Frame::Taken { next, receipts } => {
            w.u8(3);
            w.cursor(*next);
            w.u32(receipts.len() as u32);
            for r in receipts {
                w.u64(r.seq);
                w.str(&r.kind);
                match &r.outcome {
                    Outcome::Taken(seq) => {
                        w.u8(0);
                        w.u64(*seq);
                    }
                    Outcome::Refused(why) => {
                        w.u8(1);
                        w.str(why);
                    }
                    Outcome::Duplicate => w.u8(2),
                }
            }
        }
        Frame::Ask(ask) => {
            w.u8(4);
            w.ask(ask);
        }
        Frame::Answer(answer) => {
            w.u8(5);
            w.answer(answer);
        }
        Frame::Call { id, call } => {
            w.u8(6);
            w.u64(*id);
            match call {
                Call::Cursor => w.u8(0),
                Call::Resume { from, limit } => {
                    w.u8(1);
                    w.cursor(*from);
                    w.u64(*limit);
                }
                Call::Ingest { origin, entry } => {
                    w.u8(2);
                    w.origin(origin);
                    w.entry(entry);
                }
                Call::Answer { origin, ask } => {
                    w.u8(3);
                    w.origin(origin);
                    w.ask(ask);
                }
            }
        }
        Frame::Reply { id, reply } => {
            w.u8(7);
            w.u64(*id);
            match reply {
                Reply::Cursor(c) => {
                    w.u8(0);
                    w.cursor(*c);
                }
                Reply::Resumed(r) => {
                    w.u8(1);
                    w.entries(&r.entries);
                    w.lost(r.lost);
                    w.cursor(r.next);
                }
                Reply::Ingested(Ok(seq)) => {
                    w.u8(2);
                    w.u64(*seq);
                }
                Reply::Ingested(Err(e)) => {
                    w.u8(3);
                    w.ingest_error(e);
                }
                Reply::Answered(a) => {
                    w.u8(4);
                    w.answer(a);
                }
            }
        }
    }
    w.0
}

/// The frame in `body` (without its length).
pub fn decode(body: &[u8]) -> Result<Frame, WireError> {
    let mut r = Reader { bytes: body, at: 0 };
    let frame = match r.u8()? {
        1 => Frame::Hello {
            protocol: r.u32()?,
            name: r.str()?,
            resume: r.cursor()?,
        },
        2 => Frame::Entries {
            entries: r.entries()?,
            lost: r.lost()?,
            next: r.cursor()?,
        },
        3 => {
            let next = r.cursor()?;
            let n = r.len()?;
            let mut receipts = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                let seq = r.u64()?;
                let kind = r.str()?;
                let outcome = match r.u8()? {
                    0 => Outcome::Taken(r.u64()?),
                    1 => Outcome::Refused(r.str()?),
                    2 => Outcome::Duplicate,
                    t => return Err(bad(format!("receipt tag {t}"))),
                };
                receipts.push(Receipt { seq, kind, outcome });
            }
            Frame::Taken { next, receipts }
        }
        4 => Frame::Ask(r.ask()?),
        5 => Frame::Answer(r.answer()?),
        6 => {
            let id = r.u64()?;
            let call = match r.u8()? {
                0 => Call::Cursor,
                1 => Call::Resume {
                    from: r.cursor()?,
                    limit: r.u64()?,
                },
                2 => Call::Ingest {
                    origin: r.origin()?,
                    entry: r.entry()?,
                },
                3 => Call::Answer {
                    origin: r.origin()?,
                    ask: r.ask()?,
                },
                t => return Err(bad(format!("call tag {t}"))),
            };
            Frame::Call { id, call }
        }
        7 => {
            let id = r.u64()?;
            let reply = match r.u8()? {
                0 => Reply::Cursor(r.cursor()?),
                1 => {
                    let entries = r.entries()?;
                    let lost = r.lost()?;
                    let mut resumed = Resumed::new(entries, r.cursor()?);
                    resumed.lost = lost;
                    Reply::Resumed(resumed)
                }
                2 => Reply::Ingested(Ok(r.u64()?)),
                3 => Reply::Ingested(Err(r.ingest_error()?)),
                4 => Reply::Answered(r.answer()?),
                t => return Err(bad(format!("reply tag {t}"))),
            };
            Frame::Reply { id, reply }
        }
        t => return Err(bad(format!("frame tag {t}"))),
    };
    if r.at != body.len() {
        return Err(bad(format!("{} bytes left over", body.len() - r.at)));
    }
    Ok(frame)
}

fn bad(why: String) -> WireError {
    WireError::Malformed(why)
}

struct Writer(Vec<u8>);

impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn str(&mut self, s: &str) {
        self.u32(s.len() as u32);
        self.0.extend_from_slice(s.as_bytes());
    }
    fn opt_str(&mut self, s: Option<&str>) {
        match s {
            Some(s) => {
                self.u8(1);
                self.str(s);
            }
            None => self.u8(0),
        }
    }
    fn bool(&mut self, b: bool) {
        self.u8(u8::from(b));
    }
    fn cursor(&mut self, c: Cursor) {
        self.u64(c.run);
        self.u64(c.seq);
    }
    fn lost(&mut self, lost: Option<Lost>) {
        self.u8(match lost {
            None => 0,
            Some(Lost::Gap) => 1,
            Some(Lost::Restarted) => 2,
            // A kind of loss this release does not know: the reader is
            // told it lost entries, the most cautious reading.
            Some(_) => 1,
        });
    }
    fn origin(&mut self, o: &Origin) {
        self.str(&o.process);
        self.opt_str(o.agent.as_deref());
    }
    fn entry(&mut self, e: &RelayedEntry) {
        self.u64(e.seq);
        self.str(&e.kind);
        match e.cause {
            Some(c) => {
                self.u8(1);
                self.u64(c);
            }
            None => self.u8(0),
        }
        self.u32(e.version);
        self.str(&e.text);
    }
    fn entries(&mut self, entries: &[RelayedEntry]) {
        self.u32(entries.len() as u32);
        for e in entries {
            self.entry(e);
        }
    }
    fn ask(&mut self, a: &RelayedAsk) {
        self.u64(a.run);
        self.u64(a.id);
        self.str(&a.kind);
        self.u32(a.version);
        self.u64(u64::try_from(a.deadline.as_nanos()).unwrap_or(u64::MAX));
        self.str(&a.input);
        self.str(&a.value);
    }
    fn answer(&mut self, a: &RelayedAnswer) {
        self.u64(a.run);
        self.u64(a.id);
        match &a.answer {
            Answer::Done { value } => {
                self.u8(0);
                self.str(value);
            }
            Answer::Refused {
                listener,
                owner,
                reason,
                failed,
            } => {
                self.u8(1);
                self.opt_str(listener.as_deref());
                self.opt_str(owner.as_deref());
                self.str(reason);
                self.bool(*failed);
            }
            // An answer this release does not know: refused, as no
            // answer would be.
            other => {
                self.u8(1);
                self.opt_str(None);
                self.opt_str(None);
                self.str(&format!("an answer this link cannot carry: {other:?}"));
                self.bool(true);
            }
        }
    }
    fn ingest_error(&mut self, e: &IngestError) {
        match e {
            IngestError::Decode { kind, reason } => {
                self.u8(0);
                self.str(kind);
                self.str(reason);
            }
            IngestError::NotRelayable { kind } => {
                self.u8(1);
                self.str(kind);
            }
            IngestError::Refused { kind, reason } => {
                self.u8(2);
                self.str(kind);
                self.str(reason);
            }
            IngestError::Version { kind, sent, here } => {
                self.u8(3);
                self.str(kind);
                self.u32(*sent);
                self.u32(*here);
            }
            other => {
                self.u8(2);
                self.str("");
                self.str(&other.to_string());
            }
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], WireError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| bad("truncated".into()))?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8")))
    }
    fn len(&mut self) -> Result<usize, WireError> {
        let n = self.u32()? as usize;
        if n > self.bytes.len() - self.at {
            return Err(bad(format!("a length of {n} past the frame")));
        }
        Ok(n)
    }
    fn str(&mut self) -> Result<String, WireError> {
        let n = self.len()?;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| bad("text not UTF-8".into()))
    }
    fn opt_str(&mut self) -> Result<Option<String>, WireError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.str().map(Some),
            t => Err(bad(format!("option tag {t}"))),
        }
    }
    fn bool(&mut self) -> Result<bool, WireError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            t => Err(bad(format!("bool {t}"))),
        }
    }
    fn cursor(&mut self) -> Result<Cursor, WireError> {
        Ok(Cursor {
            run: self.u64()?,
            seq: self.u64()?,
        })
    }
    fn lost(&mut self) -> Result<Option<Lost>, WireError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(Lost::Gap)),
            2 => Ok(Some(Lost::Restarted)),
            t => Err(bad(format!("lost tag {t}"))),
        }
    }
    fn origin(&mut self) -> Result<Origin, WireError> {
        let mut o = Origin::process(self.str()?);
        o.agent = self.opt_str()?;
        Ok(o)
    }
    fn entry(&mut self) -> Result<RelayedEntry, WireError> {
        let seq = self.u64()?;
        let kind = self.str()?;
        let cause = match self.u8()? {
            0 => None,
            1 => Some(self.u64()?),
            t => return Err(bad(format!("cause tag {t}"))),
        };
        let version = self.u32()?;
        let mut e = RelayedEntry::new(seq, kind, self.str()?);
        e.cause = cause;
        e.version = version;
        Ok(e)
    }
    fn entries(&mut self) -> Result<Vec<RelayedEntry>, WireError> {
        let n = self.len()?;
        (0..n).map(|_| self.entry()).collect()
    }
    fn ask(&mut self) -> Result<RelayedAsk, WireError> {
        let run = self.u64()?;
        let id = self.u64()?;
        let kind = self.str()?;
        let version = self.u32()?;
        let deadline = Duration::from_nanos(self.u64()?);
        let input = self.str()?;
        let value = self.str()?;
        let mut a = RelayedAsk::new(run, id, kind, deadline, input, value);
        a.version = version;
        Ok(a)
    }
    fn answer(&mut self) -> Result<RelayedAnswer, WireError> {
        let run = self.u64()?;
        let id = self.u64()?;
        let answer = match self.u8()? {
            0 => Answer::Done { value: self.str()? },
            1 => Answer::Refused {
                listener: self.opt_str()?,
                owner: self.opt_str()?,
                reason: self.str()?,
                failed: self.bool()?,
            },
            t => return Err(bad(format!("answer tag {t}"))),
        };
        Ok(answer_to(run, id, answer))
    }
    fn ingest_error(&mut self) -> Result<IngestError, WireError> {
        Ok(match self.u8()? {
            0 => IngestError::Decode {
                kind: self.str()?,
                reason: self.str()?,
            },
            1 => IngestError::NotRelayable { kind: self.str()? },
            2 => IngestError::Refused {
                kind: self.str()?,
                reason: self.str()?,
            },
            3 => IngestError::Version {
                kind: self.str()?,
                sent: self.u32()?,
                here: self.u32()?,
            },
            t => return Err(bad(format!("ingest error tag {t}"))),
        })
    }
}

/// An answer naming the ask `run`/`id`.
pub(crate) fn answer_to(run: u64, id: u64, answer: Answer) -> RelayedAnswer {
    let ask = RelayedAsk::new(run, id, "", Duration::ZERO, "", "");
    RelayedAnswer::to(&ask, answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round(frame: Frame) {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &frame).unwrap();
        let back = read_frame(&mut &bytes[..]).unwrap();
        assert_eq!(back, frame);
    }

    #[test]
    fn every_frame_reads_back() {
        let cursor = Cursor { run: 7, seq: 42 };
        let mut entry = RelayedEntry::new(3, "app.synced", "{\"n\":1} é");
        entry.cause = Some(2);
        entry.version = 4;
        let mut ask = RelayedAsk::new(9, 1, "app.delete", Duration::from_millis(1500), "/a", "x");
        ask.version = 2;
        round(Frame::Hello {
            protocol: PROTOCOL,
            name: "node".into(),
            resume: cursor,
        });
        round(Frame::Entries {
            entries: vec![entry.clone(), RelayedEntry::new(4, "k", "")],
            lost: Some(Lost::Restarted),
            next: cursor,
        });
        round(Frame::Taken {
            next: cursor,
            receipts: vec![
                Receipt {
                    seq: 1,
                    kind: "k".into(),
                    outcome: Outcome::Taken(5),
                },
                Receipt {
                    seq: 2,
                    kind: "k".into(),
                    outcome: Outcome::Refused("no".into()),
                },
                Receipt {
                    seq: 3,
                    kind: "k".into(),
                    outcome: Outcome::Duplicate,
                },
            ],
        });
        round(Frame::Ask(ask.clone()));
        round(Frame::Answer(RelayedAnswer::to(
            &ask,
            Answer::Refused {
                listener: Some("far.guard".into()),
                owner: None,
                reason: "read-only".into(),
                failed: false,
            },
        )));
        round(Frame::Answer(RelayedAnswer::to(
            &ask,
            Answer::Done { value: "v".into() },
        )));
        let origin = Origin::process("agent-socket").agent("claude");
        for call in [
            Call::Cursor,
            Call::Resume {
                from: cursor,
                limit: 10,
            },
            Call::Ingest {
                origin: origin.clone(),
                entry: entry.clone(),
            },
            Call::Answer {
                origin,
                ask: ask.clone(),
            },
        ] {
            round(Frame::Call { id: 1, call });
        }
        let mut resumed = Resumed::new(vec![entry], cursor);
        resumed.lost = Some(Lost::Gap);
        for reply in [
            Reply::Cursor(cursor),
            Reply::Resumed(resumed),
            Reply::Ingested(Ok(3)),
            Reply::Ingested(Err(IngestError::Version {
                kind: "k".into(),
                sent: 1,
                here: 2,
            })),
            Reply::Ingested(Err(IngestError::Decode {
                kind: "k".into(),
                reason: "r".into(),
            })),
            Reply::Answered(RelayedAnswer::to(&ask, Answer::refused("gone"))),
        ] {
            round(Frame::Reply { id: 2, reply });
        }
    }

    #[test]
    fn broken_frames_are_refused() {
        assert!(matches!(decode(&[]), Err(WireError::Malformed(_))));
        assert!(matches!(decode(&[99]), Err(WireError::Malformed(_))));
        // A string length past the frame.
        assert!(matches!(
            decode(&[1, 1, 0, 0, 0, 255, 255, 255, 255]),
            Err(WireError::Malformed(_))
        ));
        // Bytes left over.
        let mut body = encode(&Frame::Call {
            id: 1,
            call: Call::Cursor,
        });
        body.push(0);
        assert!(matches!(decode(&body), Err(WireError::Malformed(_))));
        // A frame longer than the limit is not read.
        let len = ((MAX_FRAME + 1) as u32).to_be_bytes();
        assert!(matches!(
            read_frame(&mut &len[..]),
            Err(WireError::Malformed(_))
        ));
    }
}
