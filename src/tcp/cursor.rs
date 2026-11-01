//! Streaming-SELECT cursor over the TCP connection actor.
//!
//! Pairs with `crate::tcp::connection_actor::ConnectionCmd::ExecuteStream`.
//!
//! [`TcpRawCursor`] yields whole [`DecodedBlock`]s one at a time; callers
//! walk the columnar container themselves, with no row-serde bridge.
//!
//! A per-row deserialising cursor that bridges to the upstream
//! `crate::Row` trait needs a transpose pass from the column-oriented
//! [`DecodedBlock`] back to per-row RowBinary, so it lands with its
//! implementation rather than as an always-erroring placeholder.
//!
//! # Cancel-on-drop
//!
//! Dropping the cursor drops its `mpsc::Receiver`, which trips the
//! `results.closed()` watch the actor's `do_execute_stream` runs. That
//! triggers a protocol Cancel plus a bounded drain inside the actor
//! itself, then releases the pool slot the cursor was holding.

use deadpool::managed::Object;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::native::decode::DecodedBlock;
use crate::tcp::pool::TcpConnectionManager;
use crate::tcp::reader::ServerPacket;

/// Whole-block streaming-SELECT cursor.
///
/// Returns one [`DecodedBlock`] per [`Self::next_block`] call until the
/// server emits `EndOfStream` (`Ok(None)`) or an Exception
/// (`Err(Error::ServerException)`). Schema blocks (`num_rows == 0`) are
/// surfaced too, so callers that care about the announced
/// `(name, type_name)` pairs can read them; payload blocks always carry
/// their own schema via [`DecodedBlock::schema`].
#[non_exhaustive]
pub struct TcpRawCursor {
    rx: mpsc::Receiver<Result<ServerPacket>>,
    /// The pool slot stays checked out for the life of the cursor, so the
    /// connection streaming these blocks cannot be handed to another
    /// caller mid-stream.
    pool_slot: Option<Object<TcpConnectionManager>>,
    /// `true` once an EndOfStream or an error has been observed;
    /// [`Self::next_block`] then returns `Ok(None)` without touching `rx`.
    done: bool,
    /// `true` only after EndOfStream. A closed channel without it means
    /// the actor died mid-stream, which is an error, not a finished
    /// result set.
    saw_eos: bool,
}

impl TcpRawCursor {
    /// Construct a raw cursor over the actor's reply channel.
    ///
    /// `rx` is the receiver half of the mpsc pair passed into
    /// [`crate::tcp::connection_actor::ConnectionHandle::execute_stream`];
    /// dropping `self` drops `rx`, which trips the actor-side
    /// `results.closed()` watch and triggers Cancel + drain.
    pub(crate) fn from_receiver(rx: mpsc::Receiver<Result<ServerPacket>>) -> Self {
        Self {
            rx,
            pool_slot: None,
            done: false,
            saw_eos: false,
        }
    }

    /// Hold `slot` until the cursor drops. The dispatch helper calls this
    /// with the object it acquired, because a slot released at dispatch
    /// time would let the next caller take a connection that is still
    /// streaming.
    pub(crate) fn hold_pool_slot(&mut self, slot: Object<TcpConnectionManager>) {
        self.pool_slot = Some(slot);
    }

    /// Pull the next [`DecodedBlock`] off the stream. Returns:
    ///
    /// - `Ok(Some(block))` for both schema (`num_rows == 0`) and
    ///   payload (`num_rows > 0`) blocks. Callers can filter by
    ///   `block.num_rows == 0` if they want payload-only iteration.
    /// - `Ok(None)` when the server has emitted EndOfStream -- the
    ///   stream is fully drained, the connection is reusable.
    ///
    /// # Errors
    ///
    /// - [`Error::ServerException`] if the server returned an Exception
    ///   mid-stream; the actor leaves the connection reusable.
    /// - [`Error::Custom`] if the actor exited before EndOfStream.
    /// - Any I/O or decode error the actor forwarded; the actor poisons
    ///   the connection on those.
    pub async fn next_block(&mut self) -> Result<Option<DecodedBlock>> {
        if self.done {
            return Ok(None);
        }
        loop {
            match self.rx.recv().await {
                None => {
                    self.done = true;
                    if self.saw_eos {
                        return Ok(None);
                    }
                    return Err(Error::Custom(
                        "tcp: connection actor exited before EndOfStream".into(),
                    ));
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Err(e);
                }
                Some(Ok(ServerPacket::EndOfStream)) => {
                    self.saw_eos = true;
                    self.done = true;
                    return Ok(None);
                }
                Some(Ok(ServerPacket::DataBlock(block))) => {
                    return Ok(Some(block));
                }
                Some(Ok(ServerPacket::Data {
                    num_rows, columns, ..
                })) => {
                    // The schema block declares the columns but carries no
                    // values; `schema.len()` is the authoritative column
                    // count and a payload block follows with the values.
                    return Ok(Some(DecodedBlock {
                        columns: Vec::new(),
                        schema: columns,
                        num_rows,
                        kind: crate::native::BlockKind::Data,
                    }));
                }
                // Progress / ProfileInfo / TableColumns / TimezoneUpdate
                // are not row data; keep pulling until we see a block,
                // EndOfStream, or an Exception.
                Some(Ok(_)) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::decode::DecodedColumn;
    use crate::tcp::reader::ServerPacket;

    // Compile-time assertion: the cursor must be Send so callers can
    // move it across `.await` points and pass it to spawned tasks.
    static_assertions::assert_impl_all!(TcpRawCursor: Send);

    #[tokio::test]
    async fn raw_cursor_terminates_on_end_of_stream() {
        let (tx, rx) = mpsc::channel(4);
        let mut cur = TcpRawCursor::from_receiver(rx);
        tx.send(Ok(ServerPacket::EndOfStream)).await.unwrap();
        assert!(cur.next_block().await.unwrap().is_none());
        // Repeated calls after EndOfStream stay `Ok(None)` -- callers
        // that don't drop the cursor immediately must not see a panic
        // on the closed receiver.
        assert!(cur.next_block().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raw_cursor_yields_data_block_then_eos() {
        let (tx, rx) = mpsc::channel(4);
        let mut cur = TcpRawCursor::from_receiver(rx);
        let block = DecodedBlock {
            columns: vec![DecodedColumn::UInt64(vec![1, 2, 3])],
            schema: vec![("n".into(), "UInt64".into())],
            num_rows: 3,
            kind: crate::native::BlockKind::Data,
        };
        tx.send(Ok(ServerPacket::DataBlock(block))).await.unwrap();
        tx.send(Ok(ServerPacket::EndOfStream)).await.unwrap();

        let first = cur.next_block().await.unwrap().expect("block");
        assert_eq!(first.num_rows, 3);
        match &first.columns[0] {
            DecodedColumn::UInt64(v) => assert_eq!(v, &vec![1u64, 2, 3]),
            other => panic!("expected UInt64, got {other:?}"),
        }

        assert!(cur.next_block().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raw_cursor_surfaces_server_error() {
        let (tx, rx) = mpsc::channel(4);
        let mut cur = TcpRawCursor::from_receiver(rx);
        let err = Error::ServerException {
            code: 60,
            name: Some("DB::Exception".into()),
            message: "table not found".into(),
            stack_trace: None,
        };
        tx.send(Err(err)).await.unwrap();
        let result = cur.next_block().await;
        match result {
            Err(Error::ServerException { code, .. }) => assert_eq!(code, 60),
            other => panic!("expected ServerException, got {other:?}"),
        }
        // After surfacing an error the cursor is terminal too.
        assert!(cur.next_block().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raw_cursor_skips_protocol_chatter() {
        use crate::tcp::protocol::{ProfileInfo, Progress};
        let (tx, rx) = mpsc::channel(8);
        let mut cur = TcpRawCursor::from_receiver(rx);
        tx.send(Ok(ServerPacket::Progress(Progress {
            rows_read: 1,
            bytes_read: 8,
            total_rows_to_read: 0,
            written_rows: 0,
            written_bytes: 0,
        })))
        .await
        .unwrap();
        tx.send(Ok(ServerPacket::ProfileInfo(ProfileInfo {
            rows: 1,
            blocks: 1,
            bytes: 8,
            applied_limit: false,
            rows_before_limit: 0,
        })))
        .await
        .unwrap();
        tx.send(Ok(ServerPacket::EndOfStream)).await.unwrap();
        assert!(cur.next_block().await.unwrap().is_none());
    }

    /// A channel that closes without EndOfStream means the actor died
    /// mid-stream; reporting that as a clean end would silently truncate
    /// the result set.
    #[tokio::test]
    async fn raw_cursor_errors_when_the_actor_dies_before_end_of_stream() {
        let (tx, rx) = mpsc::channel::<Result<ServerPacket>>(4);
        let mut cur = TcpRawCursor::from_receiver(rx);
        drop(tx);
        let err = cur
            .next_block()
            .await
            .expect_err("a closed channel without EndOfStream is an error");
        match err {
            Error::Custom(msg) => assert!(
                msg.contains("before EndOfStream"),
                "expected a truncated-stream message, got {msg}"
            ),
            other => panic!("expected Custom, got {other:?}"),
        }
        // Terminal afterwards, like every other end state.
        assert!(cur.next_block().await.unwrap().is_none());
    }
}
