//! Background-task socket-state owner for ClickHouse TCP connections.
//!
//! Built on the generic `crate::worker::CommandWorker` primitive. The
//! actor owns the writer half of a [`MaybeTlsStream`]; an independent
//! reader sub-task owns the read half and forwards every decoded server
//! packet through a bounded mpsc channel, so the read loop keeps running
//! while a write command is in flight.
//!
//! # Why an actor instead of borrowed I/O
//!
//! Holding `&mut Connection` across the two halves for the duration of a
//! query is structurally cancellation-unsafe: any `tokio::select!`,
//! `tokio::time::timeout` or caller disconnect that drops the future
//! mid-`read_packet()` leaves the socket in an unknown state, and the
//! only recovery is tearing the connection down. Owning the I/O inside a
//! long-lived task means a dropped caller future never disturbs the
//! wire; the actor observes the reply channel closing and reacts at
//! protocol level (Cancel packet, bounded drain, return to pool).
//!
//! Three capabilities only this shape unlocks:
//!
//! 1. Protocol-level Cancel during an in-flight query -- the writer is
//!    free to send it because no caller holds it.
//! 2. Full-duplex Exception detection during INSERT -- the reader
//!    sub-task surfaces a server-side rejection before the next block
//!    goes on the wire.
//! 3. Idle keepalive -- Ping between commands without racing a caller
//!    for the writer.
//!
//! # Internal layout
//!
//! ```text
//!     callers                  ConnectionActor
//!   (ConnectionHandle           (CommandWorker)            reader_task
//!    .ping().await)                                       (independent)
//!         |                          |                          |
//!         v                          |                          |
//!   WorkerHandle<Cmd>                |                          |
//!         |                          |                          |
//!         v                          |                          |
//!     mpsc::channel ----------> handle(Cmd) ----------> writer half ----> socket
//!                                    ^                                    |
//!                                    |                                    |
//!                                pkt_rx <---------- pkt_tx <-- read_packet
//! ```
//!
//! # Reader-half ownership
//!
//! `crate::tcp::connect::split_buffered` splits the stream before the
//! actor is spawned, so the actor owns the writer half and only that,
//! and the reader task owns the read half and only that. The halves
//! `tokio::io::split` returns are `Send + 'static` whenever the stream
//! is, which is what `tokio::spawn` needs for the reader sub-task.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Sleep;

use crate::error::{Error, Result};
use crate::tcp::client_info::ClientInfo;
use crate::tcp::connect;
use crate::tcp::protocol::ServerHello;
use crate::tcp::reader::{self, ServerPacket};
use crate::tcp::transport::MaybeTlsStream;
use crate::tcp::writer::{self, CLIENT_NAME, CLIENT_VERSION_MAJOR_STR, CLIENT_VERSION_MINOR_STR};
use crate::worker::{self, CommandWorker, WorkerControl, WorkerHandle};

/// Bounded internal channel between the reader sub-task and the actor's
/// command loop. The reader decodes inline, so a queued packet is a whole
/// materialised block: at ClickHouse's 65,409-row default block size a
/// 20-column `UInt64` result is ~10 MB per slot, and this cap is what
/// bounds per-connection memory. Four slots keep the reader a step ahead
/// of the actor; falling behind blocks it, which propagates kernel TCP
/// backpressure to the server.
const PACKET_CHANNEL_CAPACITY: usize = 4;

/// Interval the reusable idle timer parks at when no `read_timeout` is
/// configured. The timer is re-armed on every packet wait, so this only
/// costs a wakeup on a command that has been waiting this long, and the
/// wait resumes rather than failing.
const IDLE_TIMER_PARK: Duration = Duration::from_secs(3600);

/// Capacity of the actor's command mpsc. 16 covers the "one in-flight
/// plus a few queued" pattern that pooled connections see; high
/// fan-in callers (an inserter feeding hundreds of blocks/sec) should
/// be served by a different connection entirely.
const DEFAULT_CMD_CHANNEL: usize = 16;

/// Capacity of the per-stream mpsc that
/// [`ConnectionHandle::execute_stream_cursor`] allocates between the
/// actor and the [`crate::tcp::cursor::TcpRawCursor`]. Every slot is a
/// decoded block, so it is bounded for the same reason
/// [`PACKET_CHANNEL_CAPACITY`] is; four keeps the cursor supplied
/// without doubling the buffered block count.
const STREAM_CHANNEL_CAPACITY: usize = 4;

/// Upper bound on how long the actor waits to drain response packets to
/// EndOfStream after sending a Cancel or after a server Exception. A
/// wedged server that ignores Cancel must not hold the actor task, and
/// thereby its pool slot, indefinitely.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound on flushing and closing the write half at shutdown. Past it the
/// actor drops the socket unflushed, since the peer is not reading.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Cap on a single INSERT block's pre-encoded payload. One oversized
/// block stalls the actor for the length of its socket write and blocks
/// every other command on that connection, so blocks past this cap are
/// rejected rather than transmitted. The server's `max_insert_block_size`
/// default of 1M rows sits far below this ceiling at any sane row width.
const MAX_INSERT_BLOCK_BYTES: usize = 512 * 1024 * 1024;

/// Runtime state of the actor. `Idle` post-handshake, between commands,
/// and after FinishInsert or a surfaced Exception; `InsertActive`
/// between a successful `BeginInsert` and its matching `FinishInsert`.
///
/// A runtime enum rather than type-state: the actor is a single
/// sequential task driving a wire protocol, and out-of-state commands
/// reply with an error instead of panicking so the caller can recover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorState {
    Idle,
    InsertActive,
}

/// Commands the [`ConnectionActor`] accepts.
///
/// Each variant embeds its own reply channel -- `oneshot` for single
/// replies, `mpsc` for streams -- so the actor never has to track
/// caller identity.
pub(crate) enum ConnectionCmd {
    /// Send a Ping; reply with `Ok(())` when Pong arrives, `Err` on
    /// I/O failure or a server Exception in place of Pong.
    Ping { reply: oneshot::Sender<Result<()>> },
    /// Run a query that does not produce client-visible rows (DDL,
    /// `SET`, INSERT-with-no-data, etc.). The actor writes the Query
    /// packet, an empty-block terminator, then drains response packets
    /// until EndOfStream (`Ok`) or a server Exception (`Err`). Receiver
    /// drop mid-flight triggers a protocol-level Cancel followed by a
    /// bounded drain so the connection stays reusable.
    ExecuteQuery {
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Begin an INSERT session. The actor writes the Query packet
    /// (typically `INSERT INTO ... FORMAT Native`), drains protocol
    /// chatter until the server's schema-block Data packet arrives,
    /// transitions to [`ActorState::InsertActive`], and replies with
    /// the `(name, type_name)` column pairs the schema block carried.
    /// An empty vec means the schema block carried no column metadata.
    BeginInsert {
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
        reply: oneshot::Sender<Result<Vec<(String, String)>>>,
    },
    /// Send a single Native-format data block during an in-flight
    /// INSERT. The actor drains already-queued server packets first, so
    /// a rejection of an earlier block aborts the INSERT before any more
    /// bytes go on the wire -- the full-duplex win the HTTP transport
    /// cannot get.
    SendInsertBlock {
        column_bytes: Vec<u8>,
        num_columns: u64,
        num_rows: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Terminate an INSERT session. The actor writes the empty-block
    /// sentinel, drains response packets to EndOfStream (or surfaces
    /// an Exception that arrives in between), then returns the actor
    /// to [`ActorState::Idle`] so the connection can be reused.
    FinishInsert { reply: oneshot::Sender<Result<()>> },
    /// Run a streaming SELECT, forwarding each server packet through
    /// `results`. `issued` answers once the query is on the wire, or with
    /// the error that kept it off. Receiver drop triggers a protocol Cancel
    /// plus a bounded drain; a mid-stream Exception is forwarded as `Err`.
    /// Decoding happens inside the reader sub-task, so this command is
    /// purely a forwarding pipeline.
    ExecuteStream {
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
        issued: oneshot::Sender<Result<()>>,
        results: mpsc::Sender<Result<ServerPacket>>,
    },
}

/// Cheap-clone send-side handle to a [`ConnectionActor`]. Every field is
/// reference-counted, so cloning is `O(1)`.
///
/// The actor task lives exactly as long as the last clone: the shared
/// `WorkerControl` drops with it and signals graceful shutdown, which
/// closes the writer half and aborts the reader sub-task.
/// [`Self::close`] takes that shutdown explicitly and waits for the
/// task, so a panic inside it surfaces.
#[derive(Clone)]
#[non_exhaustive]
pub struct ConnectionHandle {
    inner: WorkerHandle<ConnectionCmd>,
    server_hello: Arc<ServerHello>,
    poisoned: Arc<AtomicBool>,
    /// Owns the actor task. Shared so every clone counts toward the
    /// task's lifetime; `Option` so [`ConnectionHandle::close`] can take
    /// the control out and consume-await it from behind the `Arc`.
    control: Arc<tokio::sync::Mutex<Option<WorkerControl<ConnectionCmd>>>>,
}

impl ConnectionHandle {
    /// Shut the actor down and wait for its task to finish.
    ///
    /// Dropping the last clone signals the same shutdown but returns
    /// immediately; this waits, so the socket and the reader sub-task are
    /// provably gone when the call returns. Idempotent -- the second call
    /// finds the control already taken and returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// [`Error::Custom`] wrapping the `JoinError` if the actor task
    /// panicked.
    pub async fn close(self) -> Result<()> {
        let control = self.control.lock().await.take();
        match control {
            Some(control) => control.shutdown().await.map_err(|e| {
                Error::Custom(format!(
                    "tcp: connection actor task did not exit cleanly: {e}"
                ))
            }),
            None => Ok(()),
        }
    }

    /// True until the connection is poisoned, which the pool consults on
    /// `recycle`. Poisoning is one-way: a poisoned handle never recovers.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        !self.poisoned.load(Ordering::Acquire)
    }

    /// Mark the connection as broken. Idempotent and lock-free.
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    /// Negotiated server hello info (immutable after handshake).
    pub(crate) fn server_hello(&self) -> &ServerHello {
        &self.server_hello
    }

    /// Send a Ping and await Pong. Packets that interleave are drained
    /// inside the actor.
    ///
    /// # Errors
    ///
    /// [`Error::Custom`] if the actor has exited or dropped the reply
    /// channel; any writer or reader error otherwise. An I/O failure
    /// poisons the connection.
    pub async fn ping(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::Ping { reply })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("tcp: connection actor dropped ping reply".into()))?
    }

    /// Run a query that streams no rows back (DDL, `SET`,
    /// INSERT-without-data, `KILL QUERY`). Returns on EndOfStream, on an
    /// Exception, or -- if the caller's future is dropped mid-flight --
    /// once the actor has sent Cancel and drained.
    ///
    /// Dropping the returned future closes the reply channel, which the
    /// actor observes and answers at protocol level, so a cancellation
    /// never poisons the connection.
    ///
    /// # Errors
    ///
    /// [`Error::Custom`] if the actor has exited or dropped the reply
    /// channel, [`Error::ServerException`] if the server returned one in
    /// place of EndOfStream, or a writer / reader / drain-timeout error.
    pub async fn execute_query(
        &self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::ExecuteQuery {
                query_id,
                query,
                extra_settings,
                params,
                reply,
            })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await.map_err(|_| {
            Error::Custom("tcp: connection actor dropped execute_query reply".into())
        })?
    }

    /// Begin an INSERT session and return the `(name, type_name)` pairs
    /// the server echoed in its schema block.
    ///
    /// `query` is the full SQL, typically
    /// `INSERT INTO <table> FORMAT Native`. [`Self::send_insert_block`]
    /// then writes blocks and [`Self::finish_insert`] ends the session.
    ///
    /// # Errors
    ///
    /// [`Error::Custom`] if the actor is already in an INSERT or has
    /// exited, [`Error::ServerException`] if the server rejected the
    /// INSERT before its schema block, or a writer / reader error.
    pub async fn begin_insert(
        &self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
    ) -> Result<Vec<(String, String)>> {
        let (reply, rx) = oneshot::channel();
        // Dropping this future after the command is queued leaves the actor
        // to finish opening an INSERT nobody will complete, and the handle
        // goes back to the pool before the actor notices.
        let abandoned = PoisonOnDrop::arm(&self.poisoned);
        self.inner
            .send(ConnectionCmd::BeginInsert {
                query_id,
                query,
                extra_settings,
                params,
                reply,
            })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        let outcome = rx.await.map_err(|_| {
            Error::Custom("tcp: connection actor dropped begin_insert reply".into())
        })?;
        abandoned.disarm();
        outcome
    }

    /// Send one Native-format block during an in-flight INSERT.
    ///
    /// `column_bytes` is the pre-encoded payload from
    /// [`crate::native::encode_columns`]; the actor never re-encodes. It
    /// drains queued server packets first, so a rejection of an earlier
    /// block aborts the INSERT before more bytes go on the wire.
    ///
    /// # Errors
    ///
    /// [`Error::Custom`] if no INSERT is active or the actor has exited,
    /// [`Error::ServerException`] from full-duplex detection of an
    /// earlier block's rejection, or an I/O error.
    pub async fn send_insert_block(
        &self,
        column_bytes: Vec<u8>,
        num_columns: u64,
        num_rows: u64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::SendInsertBlock {
                column_bytes,
                num_columns,
                num_rows,
                reply,
            })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await.map_err(|_| {
            Error::Custom("tcp: connection actor dropped send_insert_block reply".into())
        })?
    }

    /// Run a streaming SELECT and forward server packets through
    /// `results` in wire order: the schema block
    /// ([`ServerPacket::Data`] with `num_rows == 0`), zero or more
    /// payload blocks, then [`ServerPacket::EndOfStream`], which the
    /// cursor uses as its "no more rows" sentinel.
    ///
    /// Dropping the receiver triggers a protocol Cancel plus a bounded
    /// drain inside the actor, leaving the connection reusable.
    ///
    /// Returns once the query is issued, so a failure to put it on the
    /// wire is this call's error, inside a caller's retry, rather than the
    /// first frame of a cursor already handed back.
    ///
    /// # Errors
    ///
    /// [`Error::Custom`] if the actor has exited, or whatever kept the
    /// query off the wire (a write failure or timeout, a poisoned
    /// connection, an INSERT in progress). Every later error reaches the
    /// caller through `results`.
    pub(crate) async fn execute_stream(
        &self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
        results: mpsc::Sender<Result<ServerPacket>>,
    ) -> Result<()> {
        let (issued, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::ExecuteStream {
                query_id,
                query,
                extra_settings,
                params,
                issued,
                results,
            })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("tcp: connection actor dropped the stream".into()))?
    }

    /// Open a streaming SELECT and hand back a
    /// [`crate::tcp::cursor::TcpRawCursor`] that yields decoded
    /// blocks until `EndOfStream`.
    ///
    /// Allocates a small bounded mpsc pair, dispatches `ExecuteStream`,
    /// and wraps the receiver.
    ///
    /// # Errors
    ///
    /// [`Error::Custom`] if the actor has exited, or whatever kept the
    /// query off the wire (a write failure or timeout, a poisoned
    /// connection, an INSERT in progress): the query is issued before the
    /// cursor is returned. Errors after that reach the caller via
    /// `TcpRawCursor::next_block()`.
    pub async fn execute_stream_cursor(
        &self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
    ) -> Result<crate::tcp::cursor::TcpRawCursor> {
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        self.execute_stream(query_id, query, extra_settings, params, tx)
            .await?;
        Ok(crate::tcp::cursor::TcpRawCursor::from_receiver(rx))
    }

    /// Terminate an in-flight INSERT session: write the empty Data block
    /// the server takes as end-of-input, drain to EndOfStream, and
    /// return to `Idle` so the connection can be reused.
    ///
    /// # Errors
    ///
    /// [`Error::Custom`] if no INSERT is active or the actor has exited,
    /// [`Error::ServerException`] if the server rejected the INSERT on
    /// commit, or an I/O or drain-timeout error.
    pub async fn finish_insert(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::FinishInsert { reply })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await.map_err(|_| {
            Error::Custom("tcp: connection actor dropped finish_insert reply".into())
        })?
    }
}

/// Poisons the connection when dropped while armed, so a cancelled caller
/// future cannot hand the pool a connection in an unknown protocol state.
struct PoisonOnDrop<'a>(Option<&'a AtomicBool>);

impl<'a> PoisonOnDrop<'a> {
    fn arm(flag: &'a AtomicBool) -> Self {
        Self(Some(flag))
    }

    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for PoisonOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(flag) = self.0 {
            flag.store(true, Ordering::Release);
        }
    }
}

/// Internal message carried over the reader -> actor mpsc; splitting
/// `Packet` from `Error` keeps every consumer off `Result` matching.
enum ReaderMessage {
    Packet(ServerPacket),
    Error(Error),
}

/// Tunables threaded into the actor at spawn time. Kept as a small
/// struct (rather than positional `spawn` args) so future per-connection
/// dials can be added without churning every `spawn` call site.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ActorConfig {
    /// Per-operation I/O deadline. When `Some(d)`, a query, stream or
    /// begin-insert read that goes `d` without receiving ANY packet, or an
    /// INSERT block or finish write that takes longer than `d`, is treated
    /// as a stalled server: the connection is poisoned and the caller sees
    /// [`Error::TimedOut`]. The read timer resets on every packet, so it
    /// bounds the gap BETWEEN packets, not the whole query. `None` leaves
    /// I/O bounded only by caller-side cancellation.
    pub read_timeout: Option<Duration>,
    /// Quota key echoed into every Query packet's `ClientInfo`, so the
    /// server attributes the query to the same quota the handshake
    /// addendum named.
    pub quota_key: String,
}

/// Background task that owns the writer half and the receive end of
/// the reader -> actor channel. Implements the crate's `CommandWorker`
/// trait, whose generic runner handles the lifecycle plumbing (command
/// loop, drain on shutdown, panic surfacing).
pub struct ConnectionActor {
    writer: BufWriter<WriteHalf<MaybeTlsStream>>,
    pkt_rx: mpsc::Receiver<ReaderMessage>,
    /// Aborted in `Drop`. Dropping a `JoinHandle` does NOT cancel the
    /// task, and the reader parks in `read_packet` until the server's own
    /// receive timeout, so without the abort every recycled connection
    /// leaks a task, an FD and a socket.
    reader_task: JoinHandle<()>,
    server_hello: Arc<ServerHello>,
    poisoned: Arc<AtomicBool>,
    /// Idle or InsertActive. Gates which commands are accepted in
    /// `CommandWorker::handle` -- see [`ActorState`] rustdoc.
    state: ActorState,
    /// Per-operation I/O deadline; see [`ActorConfig::read_timeout`].
    read_timeout: Option<Duration>,
    /// Quota key echoed into every Query packet's `ClientInfo`.
    quota_key: String,
    /// One timer for the life of the connection, re-armed per packet
    /// wait. A fresh `tokio::time::sleep` per loop iteration allocates a
    /// timer entry per packet on a streaming SELECT.
    idle_sleep: Pin<Box<Sleep>>,
}

/// One outcome of waiting for the next server packet.
enum PacketWait {
    /// The idle timer fired.
    Idle,
    /// The reader sub-task delivered a message, or closed (`None`).
    Message(Option<ReaderMessage>),
}

/// One outcome of a drain-loop wait that also watches its caller.
enum QueryEvent {
    /// The caller dropped its receiver.
    CallerGone,
    /// The idle timer fired.
    Idle,
    /// The reader sub-task delivered a message, or closed (`None`).
    Message(Option<ReaderMessage>),
}

impl ConnectionActor {
    /// Spawn an actor over a freshly handshaken stream, returning the
    /// cheap-clone handle that owns its task.
    ///
    /// `crate::tcp::connect::split_buffered` splits the stream: the read
    /// half goes to the reader sub-task, the write half to the actor. On
    /// shutdown the actor closes the write half and its `Drop` aborts
    /// the reader task, which releases the socket.
    #[must_use]
    pub fn spawn(stream: MaybeTlsStream, server_hello: ServerHello) -> ConnectionHandle {
        Self::spawn_with_config(stream, server_hello, ActorConfig::default())
    }

    /// Spawn an actor with explicit [`ActorConfig`] tunables; the pool's
    /// `Manager::create` uses this form to thread its dials through.
    #[must_use]
    pub fn spawn_with_config(
        stream: MaybeTlsStream,
        server_hello: ServerHello,
        config: ActorConfig,
    ) -> ConnectionHandle {
        let (reader_half, writer_half) = connect::split_buffered(stream);

        let server_hello = Arc::new(server_hello);
        let poisoned = Arc::new(AtomicBool::new(false));

        // Reader sub-task: owns the read half, feeds packets through
        // the bounded mpsc until the socket closes or the actor's
        // receiver drops.
        let revision = server_hello.revision;
        let (pkt_tx, pkt_rx) = mpsc::channel(PACKET_CHANNEL_CAPACITY);
        let reader_task = tokio::spawn(reader_loop(
            reader_half,
            pkt_tx,
            revision,
            Arc::clone(&poisoned),
        ));

        let actor = ConnectionActor {
            writer: writer_half,
            pkt_rx,
            reader_task,
            server_hello: Arc::clone(&server_hello),
            poisoned: Arc::clone(&poisoned),
            state: ActorState::Idle,
            read_timeout: config.read_timeout,
            quota_key: config.quota_key,
            idle_sleep: Box::pin(tokio::time::sleep(IDLE_TIMER_PARK)),
        };

        let control = worker::spawn(actor, DEFAULT_CMD_CHANNEL);
        let inner = control.handle();
        // The last handle drop signals shutdown, which closes the socket.
        ConnectionHandle {
            inner,
            server_hello,
            poisoned,
            control: Arc::new(tokio::sync::Mutex::new(Some(control))),
        }
    }

    /// Re-arm the idle timer for the next packet wait. With no
    /// `read_timeout` configured it parks at [`IDLE_TIMER_PARK`], and the
    /// wait resumes rather than failing when that elapses.
    fn arm_idle(&mut self) {
        let gap = self.read_timeout.unwrap_or(IDLE_TIMER_PARK);
        self.idle_sleep
            .as_mut()
            .reset(tokio::time::Instant::now() + gap);
    }

    /// Wait for the next reader message under the idle timer.
    async fn next_packet(&mut self) -> PacketWait {
        self.arm_idle();
        let Self {
            idle_sleep, pkt_rx, ..
        } = self;
        tokio::select! {
            biased;
            () = idle_sleep.as_mut() => PacketWait::Idle,
            msg = pkt_rx.recv() => PacketWait::Message(msg),
        }
    }

    /// Poison and return [`Error::TimedOut`] when the fired idle timer is
    /// a real deadline; `None` means it was only the park interval.
    fn idle_timed_out(&self) -> Option<Error> {
        self.read_timeout?;
        self.poisoned.store(true, Ordering::Release);
        Some(Error::TimedOut)
    }

    /// `ClientInfo` for one client-originated query. `query_id` is echoed
    /// into `initial_query_id` so `system.query_log.initial_query_id`
    /// matches the id the caller traces and `KILL QUERY` matches on.
    fn client_info_for(&self, query_id: &str) -> ClientInfo {
        let mut info = ClientInfo::for_initial_query(
            CLIENT_NAME,
            CLIENT_VERSION_MAJOR_STR.parse().unwrap_or(0),
            CLIENT_VERSION_MINOR_STR.parse().unwrap_or(0),
            self.server_hello.revision,
            &self.quota_key,
        );
        info.initial_query_id.push_str(query_id);
        info
    }

    /// Write the Query packet and the empty-block terminator that ends
    /// the pre-query phase, together under the I/O deadline. Either failing
    /// leaves the wire half-written, so every caller poisons on `Err`.
    async fn issue_query(
        &mut self,
        query_id: &str,
        query: &str,
        extra_settings: &[(String, String)],
        params: &[(String, String)],
    ) -> Result<()> {
        let client_info = self.client_info_for(query_id);
        let revision = self.server_hello.revision;
        let writer = &mut self.writer;
        let write = async {
            writer::send_query(
                writer,
                revision,
                query_id,
                query,
                extra_settings,
                params,
                &client_info,
            )
            .await?;
            writer::send_empty_block(writer, revision).await
        };
        bounded_io(self.read_timeout, write).await
    }

    /// Send a Ping and wait for the Pong. Stray Progress / Log /
    /// ProfileEvents packets that interleave between Ping and Pong
    /// are logged at trace level and discarded; a server Exception
    /// surfaces as `Error::ServerException`. Reader sub-task exit or
    /// any I/O error poisons the connection.
    async fn do_ping(&mut self) -> Result<()> {
        bounded_io(self.read_timeout, writer::send_ping(&mut self.writer))
            .await
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Release);
            })?;
        loop {
            let msg = match self.next_packet().await {
                PacketWait::Idle => match self.idle_timed_out() {
                    Some(e) => return Err(e),
                    None => continue,
                },
                PacketWait::Message(msg) => msg,
            };
            match msg {
                Some(ReaderMessage::Packet(ServerPacket::Pong)) => return Ok(()),
                Some(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                    // Server-side rejection. Not an I/O failure -- the
                    // socket is still usable -- but the caller needs
                    // to see the error.
                    return Err(exc.into_error());
                }
                Some(ReaderMessage::Packet(other)) => {
                    tracing::trace!(
                        target: "clickhouse::tcp",
                        ?other,
                        "ignoring interleaved packet during ping"
                    );
                }
                Some(ReaderMessage::Error(e)) => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(e);
                }
                None => {
                    // Reader sub-task exited without sending an Error
                    // (e.g. its sender dropped). Treat as a torn-down
                    // socket; poison and surface.
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::Custom("tcp: reader sub-task exited mid-ping".into()));
                }
            }
        }
    }

    /// Write a Query packet (followed by an empty-block terminator)
    /// and drain server response packets until EndOfStream or an
    /// Exception. Watches the reply channel via `reply.closed()` so a
    /// caller-side cancellation triggers a protocol-level Cancel and a
    /// bounded drain instead of tearing the socket down.
    async fn do_execute_query(
        &mut self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
        mut reply: oneshot::Sender<Result<()>>,
    ) {
        if let Err(e) = self
            .issue_query(&query_id, &query, &extra_settings, &params)
            .await
        {
            self.poisoned.store(true, Ordering::Release);
            let _ = reply.send(Err(e));
            return;
        }

        // Drain response packets. `biased;` orders the cancel branch
        // first so a reply-drop racing against an in-flight packet
        // always wins -- we never want to deliver a successful Ok to
        // a caller that has already moved on.
        loop {
            self.arm_idle();
            let event = {
                let Self {
                    idle_sleep, pkt_rx, ..
                } = &mut *self;
                tokio::select! {
                    biased;
                    () = reply.closed() => QueryEvent::CallerGone,
                    () = idle_sleep.as_mut() => QueryEvent::Idle,
                    msg = pkt_rx.recv() => QueryEvent::Message(msg),
                }
            };

            match event {
                QueryEvent::CallerGone => {
                    self.cancel_and_drain("caller dropped the reply").await;
                    return;
                }
                QueryEvent::Idle => {
                    // The timer resets per packet, so reaching here means
                    // a stalled backend rather than a slow-but-
                    // progressing one.
                    if let Some(e) = self.idle_timed_out() {
                        let _ = reply.send(Err(e));
                        return;
                    }
                }
                QueryEvent::Message(Some(ReaderMessage::Packet(ServerPacket::EndOfStream))) => {
                    let _ = reply.send(Ok(()));
                    return;
                }
                QueryEvent::Message(Some(ReaderMessage::Packet(ServerPacket::Exception(exc)))) => {
                    // An Exception is terminal and leaves the connection
                    // reusable: the server sends EndOfStream only on the
                    // success path, so a drain here would stall to
                    // DRAIN_TIMEOUT and poison a healthy connection.
                    let _ = reply.send(Err(exc.into_error()));
                    return;
                }
                // Progress / Log / ProfileInfo / ProfileEvents and the
                // rest carry nothing an execute_query caller wants.
                QueryEvent::Message(Some(ReaderMessage::Packet(_))) => {}
                QueryEvent::Message(Some(ReaderMessage::Error(e))) => {
                    self.poisoned.store(true, Ordering::Release);
                    let _ = reply.send(Err(e));
                    return;
                }
                QueryEvent::Message(None) => {
                    self.poisoned.store(true, Ordering::Release);
                    let _ = reply.send(Err(Error::Custom(
                        "tcp: reader sub-task exited mid-query".into(),
                    )));
                    return;
                }
            }
        }
    }

    /// Send a protocol Cancel and drain to EndOfStream so the next
    /// caller starts on a clean stream pointer. `reason` names the
    /// trigger in the warning a failure logs.
    ///
    /// The Cancel write is bounded too, by the I/O deadline or else by
    /// [`DRAIN_TIMEOUT`], so a peer that stopped reading cannot hold the
    /// actor before the drain's own bound starts.
    async fn cancel_and_drain(&mut self, reason: &str) {
        let limit = self.read_timeout.unwrap_or(DRAIN_TIMEOUT);
        if let Err(e) = bounded_io(Some(limit), writer::send_cancel(&mut self.writer)).await {
            self.poisoned.store(true, Ordering::Release);
            tracing::warn!(
                target: "clickhouse::tcp",
                error = %e,
                reason,
                "failed to send Cancel"
            );
            return;
        }
        if let Err(e) = self.drain_to_end_of_stream().await {
            // drain_to_end_of_stream poisons on its own failure modes.
            tracing::warn!(
                target: "clickhouse::tcp",
                error = %e,
                reason,
                "drain after Cancel did not reach EndOfStream"
            );
        }
    }

    /// Open an INSERT session and return the `(name, type_name)` pairs
    /// the server's schema block carried.
    ///
    /// The state transition lives in the `handle()` arm, so an `Err`
    /// return leaves the actor in `Idle` and the connection usable for
    /// further commands.
    async fn do_begin_insert(
        &mut self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
    ) -> Result<Vec<(String, String)>> {
        // The empty trailing block signals end-of-prequery and prompts
        // the server to answer with its schema block.
        if let Err(e) = self
            .issue_query(&query_id, &query, &extra_settings, &params)
            .await
        {
            self.poisoned.store(true, Ordering::Release);
            return Err(e);
        }

        // Drain until the schema block (Data with num_rows == 0) or
        // an Exception. Progress / Log / TableColumns are normal
        // pre-schema chatter -- discard.
        loop {
            let msg = match self.next_packet().await {
                PacketWait::Idle => match self.idle_timed_out() {
                    Some(e) => return Err(e),
                    None => continue,
                },
                PacketWait::Message(msg) => msg,
            };
            match msg {
                Some(ReaderMessage::Packet(ServerPacket::Data { columns, .. })) => {
                    // `read_packet` routes num_rows > 0 through the
                    // DataBlock variant, so a Data packet here is the
                    // schema block.
                    return Ok(columns);
                }
                Some(ReaderMessage::Packet(ServerPacket::DataBlock(block))) => {
                    // A payload block before the schema block is a
                    // protocol violation -- INSERT clients always see
                    // schema first. Poison and surface so the caller
                    // doesn't sit in a broken INSERT.
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::BadResponse(format!(
                        "tcp: server sent payload Data block (num_rows={}) \
                         before INSERT schema block",
                        block.num_rows
                    )));
                }
                Some(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                    return Err(exc.into_error());
                }
                Some(ReaderMessage::Packet(ServerPacket::EndOfStream)) => {
                    // The server finished without asking for data, which
                    // means it feeds itself (`INSERT ... SELECT`);
                    // `begin_insert` is for client-fed INSERT only.
                    return Err(Error::BadResponse(
                        "tcp: server returned EndOfStream before INSERT schema block \
                         (was this INSERT ... SELECT?)"
                            .into(),
                    ));
                }
                Some(ReaderMessage::Packet(_)) => {}
                Some(ReaderMessage::Error(e)) => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(e);
                }
                None => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::Custom(
                        "tcp: reader sub-task exited mid begin_insert".into(),
                    ));
                }
            }
        }
    }

    /// Send one Native block during an in-flight INSERT, draining
    /// already-queued server packets first: a constraint-violating row
    /// in block N surfaces on block N+1's send, with no socket teardown.
    ///
    /// State returns to `Idle` on Exception or I/O failure; a successful
    /// write keeps the session in `InsertActive`.
    async fn do_send_insert_block(
        &mut self,
        column_bytes: Vec<u8>,
        num_columns: u64,
        num_rows: u64,
        reply: oneshot::Sender<Result<()>>,
    ) {
        // Nothing has hit the wire, so the session stays in
        // InsertActive and the caller can re-send a smaller block.
        if column_bytes.len() > MAX_INSERT_BLOCK_BYTES {
            let _ = reply.send(Err(Error::Custom(format!(
                "tcp: INSERT block of {} bytes exceeds the {MAX_INSERT_BLOCK_BYTES} byte cap; \
                 split the batch into smaller blocks",
                column_bytes.len()
            ))));
            return;
        }

        // Full-duplex check FIRST. `try_recv` is non-blocking, so we
        // drain everything queued without waiting for new packets.
        loop {
            match self.pkt_rx.try_recv() {
                Ok(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                    // Terminal, and the connection stays usable: return to
                    // Idle and surface without draining, because no
                    // EndOfStream follows an Exception.
                    self.state = ActorState::Idle;
                    let _ = reply.send(Err(exc.into_error()));
                    return;
                }
                // Progress / Log -- keep draining.
                Ok(ReaderMessage::Packet(_)) => {}
                Ok(ReaderMessage::Error(e)) => {
                    self.poisoned.store(true, Ordering::Release);
                    self.state = ActorState::Idle;
                    let _ = reply.send(Err(e));
                    return;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // The reader sub-task is gone, so nothing will ever
                    // observe the server's answer to this block.
                    self.poisoned.store(true, Ordering::Release);
                    self.state = ActorState::Idle;
                    let _ = reply.send(Err(Error::Custom(
                        "tcp: reader sub-task exited mid-INSERT".into(),
                    )));
                    return;
                }
            }
        }

        let write = writer::send_data_block(
            &mut self.writer,
            self.server_hello.revision,
            "",
            &column_bytes,
            num_columns,
            num_rows,
        );
        let result = bounded_io(self.read_timeout, write).await;
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
            self.state = ActorState::Idle;
        }
        let _ = reply.send(result);
    }

    /// Terminate an INSERT session: write the empty-block sentinel,
    /// drain response packets to EndOfStream (or surface an
    /// Exception). Caller's `handle()` arm returns the actor to Idle
    /// regardless of outcome.
    async fn do_finish_insert(&mut self) -> Result<()> {
        let write = writer::send_empty_block(&mut self.writer, self.server_hello.revision);
        if let Err(e) = bounded_io(self.read_timeout, write).await {
            self.poisoned.store(true, Ordering::Release);
            return Err(e);
        }
        self.drain_to_end_of_stream().await
    }

    /// Streaming SELECT body: forward every packet the reader sub-task
    /// emits into `results` in wire order, so the cursor can stitch the
    /// blocks back together. `Pong`, `Log` and `ProfileEvents` are
    /// dropped here.
    ///
    /// Cancellation works the same way as
    /// [`Self::do_execute_query`]: `results.closed()` wins the
    /// `tokio::select!` with `biased;` and triggers a protocol Cancel
    /// plus a bounded drain.
    async fn do_execute_stream(
        &mut self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        params: Vec<(String, String)>,
        issued: oneshot::Sender<Result<()>>,
        results: mpsc::Sender<Result<ServerPacket>>,
    ) {
        if let Err(e) = self
            .issue_query(&query_id, &query, &extra_settings, &params)
            .await
        {
            self.poisoned.store(true, Ordering::Release);
            let _ = issued.send(Err(e));
            return;
        }
        // A caller that gave up before the query went out still gets the
        // Cancel and drain below, through `results.closed()`.
        let _ = issued.send(Ok(()));

        loop {
            self.arm_idle();
            let event = {
                let Self {
                    idle_sleep, pkt_rx, ..
                } = &mut *self;
                tokio::select! {
                    biased;
                    () = results.closed() => QueryEvent::CallerGone,
                    () = idle_sleep.as_mut() => QueryEvent::Idle,
                    msg = pkt_rx.recv() => QueryEvent::Message(msg),
                }
            };

            match event {
                QueryEvent::CallerGone => {
                    self.cancel_and_drain("stream receiver dropped").await;
                    return;
                }
                QueryEvent::Idle => {
                    if let Some(e) = self.idle_timed_out() {
                        let _ = results.send(Err(e)).await;
                        return;
                    }
                }
                QueryEvent::Message(Some(ReaderMessage::Packet(ServerPacket::EndOfStream))) => {
                    // The cursor's "no more rows" sentinel; the stream is
                    // complete whether or not the send lands.
                    let _ = results.send(Ok(ServerPacket::EndOfStream)).await;
                    return;
                }
                QueryEvent::Message(Some(ReaderMessage::Packet(ServerPacket::Exception(exc)))) => {
                    // Terminal, and no EndOfStream follows, so draining
                    // here would stall to DRAIN_TIMEOUT and poison a
                    // healthy connection.
                    let _ = results.send(Err(exc.into_error())).await;
                    return;
                }
                // Protocol chatter with no cursor relevance; the reader
                // has already consumed the block bytes behind the marker.
                QueryEvent::Message(Some(ReaderMessage::Packet(
                    ServerPacket::Pong | ServerPacket::Log | ServerPacket::ProfileEvents,
                ))) => {}
                QueryEvent::Message(Some(ReaderMessage::Packet(pkt))) => {
                    if results.send(Ok(pkt)).await.is_err() {
                        // The receiver went away between the select and
                        // this send.
                        self.cancel_and_drain("stream receiver dropped mid-send")
                            .await;
                        return;
                    }
                }
                QueryEvent::Message(Some(ReaderMessage::Error(e))) => {
                    self.poisoned.store(true, Ordering::Release);
                    let _ = results.send(Err(e)).await;
                    return;
                }
                QueryEvent::Message(None) => {
                    self.poisoned.store(true, Ordering::Release);
                    let _ = results
                        .send(Err(Error::Custom(
                            "tcp: reader sub-task exited mid-stream".into(),
                        )))
                        .await;
                    return;
                }
            }
        }
    }

    /// Drain response packets until EndOfStream, bounded by
    /// [`DRAIN_TIMEOUT`]. Used after sending Cancel (post caller-drop)
    /// or after a server Exception so the next caller starts on a
    /// clean stream pointer.
    ///
    /// On timeout the connection is poisoned, so the pool's `recycle`
    /// refuses it on the `is_alive()` check.
    async fn drain_to_end_of_stream(&mut self) -> Result<()> {
        let drain = async {
            loop {
                match self.pkt_rx.recv().await {
                    Some(ReaderMessage::Packet(ServerPacket::EndOfStream)) => return Ok(()),
                    Some(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                        // The caller already has its own error; this one
                        // only affects the drain's return value.
                        return Err(exc.into_error());
                    }
                    Some(ReaderMessage::Packet(_)) => {}
                    Some(ReaderMessage::Error(e)) => {
                        self.poisoned.store(true, Ordering::Release);
                        return Err(e);
                    }
                    None => {
                        self.poisoned.store(true, Ordering::Release);
                        return Err(Error::Custom(
                            "tcp: reader sub-task exited mid-drain".into(),
                        ));
                    }
                }
            }
        };
        let Ok(drained) = tokio::time::timeout(DRAIN_TIMEOUT, drain).await else {
            self.poisoned.store(true, Ordering::Release);
            return Err(Error::Custom(
                "tcp: drain to EndOfStream exceeded 30s timeout".into(),
            ));
        };
        drained
    }
}

impl CommandWorker for ConnectionActor {
    type Command = ConnectionCmd;

    fn name() -> &'static str {
        "clickhouse.tcp-connection"
    }

    async fn handle(&mut self, cmd: Self::Command) {
        // A command queued behind the one that poisoned the stream must not reach it.
        // An open INSERT still drains first, so a rejection buffered ahead of a close surfaces.
        let continues_insert = matches!(
            cmd,
            ConnectionCmd::SendInsertBlock { .. } | ConnectionCmd::FinishInsert { .. }
        );
        if self.poisoned.load(Ordering::Acquire) && !continues_insert {
            reject_poisoned(cmd);
            return;
        }

        // State gate: Ping / ExecuteQuery / BeginInsert / ExecuteStream
        // only in `Idle`, SendInsertBlock / FinishInsert only in
        // `InsertActive`. An out-of-state command replies with an error
        // and leaves the actor alive so the caller can recover.
        match (self.state, cmd) {
            (
                ActorState::InsertActive,
                ConnectionCmd::Ping { reply } | ConnectionCmd::ExecuteQuery { reply, .. },
            ) => {
                let _ = reply.send(Err(Error::Custom("tcp: actor busy in INSERT".into())));
            }
            (ActorState::InsertActive, ConnectionCmd::BeginInsert { reply, .. }) => {
                let _ = reply.send(Err(Error::Custom("tcp: actor already in INSERT".into())));
            }
            (ActorState::InsertActive, ConnectionCmd::ExecuteStream { issued, .. }) => {
                // The misuse is the open call's error; the caller can
                // finish_insert and retry.
                let _ = issued.send(Err(Error::Custom("tcp: actor busy in INSERT".into())));
            }
            (
                ActorState::Idle,
                ConnectionCmd::SendInsertBlock { reply, .. }
                | ConnectionCmd::FinishInsert { reply },
            ) => {
                let _ = reply.send(Err(Error::Custom("tcp: no INSERT session active".into())));
            }
            (ActorState::Idle, ConnectionCmd::Ping { reply }) => {
                let result = self.do_ping().await;
                if reply.send(result).is_err() {
                    // The socket is in a known state, so this is only
                    // worth a warn.
                    tracing::warn!(
                        target: "clickhouse::tcp",
                        "ping reply dropped by caller before send"
                    );
                }
            }
            (
                ActorState::Idle,
                ConnectionCmd::ExecuteQuery {
                    query_id,
                    query,
                    extra_settings,
                    params,
                    reply,
                },
            ) => {
                // do_execute_query owns the reply Sender for the full
                // duration so it can watch `reply.closed()` and react
                // to caller-side cancellation at protocol level.
                self.do_execute_query(query_id, query, extra_settings, params, reply)
                    .await;
            }
            (
                ActorState::Idle,
                ConnectionCmd::BeginInsert {
                    query_id,
                    query,
                    extra_settings,
                    params,
                    reply,
                },
            ) => {
                let result = self
                    .do_begin_insert(query_id, query, extra_settings, params)
                    .await;
                if result.is_ok() {
                    self.state = ActorState::InsertActive;
                }
                if reply.send(result).is_err() {
                    tracing::warn!(
                        target: "clickhouse::tcp",
                        "begin_insert reply dropped by caller before send"
                    );
                }
            }
            (
                ActorState::InsertActive,
                ConnectionCmd::SendInsertBlock {
                    column_bytes,
                    num_columns,
                    num_rows,
                    reply,
                },
            ) => {
                self.do_send_insert_block(column_bytes, num_columns, num_rows, reply)
                    .await;
            }
            (ActorState::InsertActive, ConnectionCmd::FinishInsert { reply }) => {
                let result = self.do_finish_insert().await;
                // The session is over either way, so return to Idle and
                // leave the connection reusable for non-INSERT commands.
                self.state = ActorState::Idle;
                if reply.send(result).is_err() {
                    tracing::warn!(
                        target: "clickhouse::tcp",
                        "finish_insert reply dropped by caller before send"
                    );
                }
            }
            (
                ActorState::Idle,
                ConnectionCmd::ExecuteStream {
                    query_id,
                    query,
                    extra_settings,
                    params,
                    issued,
                    results,
                },
            ) => {
                // do_execute_stream owns `results` for the full command
                // lifetime so it can watch `results.closed()` and react
                // to receiver-drop at protocol level.
                self.do_execute_stream(query_id, query, extra_settings, params, issued, results)
                    .await;
            }
        }
    }

    /// Close the write half so the server sees FIN. `WriteHalf` has no
    /// `Drop` of its own, so nothing else ends the session from our side.
    /// Bounded because the close flushes buffered bytes first, and a peer that
    /// stopped reading would hold this task and its fd for the kernel's
    /// retransmission window.
    async fn on_shutdown(&mut self) {
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, self.writer.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::debug!(
                target: "clickhouse::tcp",
                error = %e,
                "closing the writer half on shutdown failed"
            ),
            Err(_) => tracing::debug!(
                target: "clickhouse::tcp",
                "closing the writer half on shutdown timed out"
            ),
        }
    }
}

impl Drop for ConnectionActor {
    fn drop(&mut self) {
        // The reader owns the read half; aborting it is what releases the
        // socket, because it is parked in `read_packet` and will not
        // observe the closed writer for the server's whole idle timeout.
        self.reader_task.abort();
        if self.poisoned.load(Ordering::Acquire) {
            tracing::warn!(
                target: "clickhouse::tcp",
                "connection actor dropped while poisoned"
            );
        } else {
            tracing::debug!(
                target: "clickhouse::tcp",
                "connection actor dropped cleanly"
            );
        }
    }
}

/// Run one socket write under the connection's I/O deadline. A write that
/// outlasts it leaves a partial frame behind, so every caller poisons on `Err`.
async fn bounded_io<F>(limit: Option<Duration>, io: F) -> Result<()>
where
    F: std::future::Future<Output = Result<()>>,
{
    match limit {
        Some(limit) => tokio::time::timeout(limit, io)
            .await
            .unwrap_or(Err(Error::TimedOut)),
        None => io.await,
    }
}

/// Answer a command without touching the socket. [`Error::Transient`] because
/// a fresh connection may succeed where this one cannot.
fn reject_poisoned(cmd: ConnectionCmd) {
    let refusal = || Error::Transient("tcp: connection is poisoned; command not sent".into());
    match cmd {
        ConnectionCmd::Ping { reply }
        | ConnectionCmd::ExecuteQuery { reply, .. }
        | ConnectionCmd::SendInsertBlock { reply, .. }
        | ConnectionCmd::FinishInsert { reply } => {
            let _ = reply.send(Err(refusal()));
        }
        ConnectionCmd::BeginInsert { reply, .. } => {
            let _ = reply.send(Err(refusal()));
        }
        ConnectionCmd::ExecuteStream { issued, .. } => {
            let _ = issued.send(Err(refusal()));
        }
    }
}

/// Reader sub-task body: own the read half, forward packets one at a
/// time, and on I/O or decode failure poison the handle, send one `Error`
/// and exit. Poisoning here rather than when the actor reads the `Error` is
/// what lets the pool refuse a connection the server closed while it sat
/// idle, since no command is running to consume the message. A closed
/// receiver means the actor has already torn the connection down.
async fn reader_loop(
    mut r: BufReader<ReadHalf<MaybeTlsStream>>,
    tx: mpsc::Sender<ReaderMessage>,
    server_revision: u64,
    poisoned: Arc<AtomicBool>,
) {
    loop {
        match reader::read_packet(&mut r, server_revision).await {
            Ok(pkt) => {
                if tx.send(ReaderMessage::Packet(pkt)).await.is_err() {
                    // Actor dropped the receiver. Exit silently;
                    // nothing else can usefully observe the socket.
                    return;
                }
            }
            Err(e) => {
                poisoned.store(true, Ordering::Release);
                let _ = tx.send(ReaderMessage::Error(e)).await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::io::ClickHouseWrite;
    use crate::tcp::mock::{write_schema_block, write_uint64_payload_block};
    use crate::tcp::protocol::{ClientPacketId, DBMS_TCP_PROTOCOL_VERSION, ServerPacketId};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    // Compile-time check: ConnectionHandle must be Send + Sync so it
    // can cross task boundaries through any pool. A regression here
    // would silently break the actor's async-friendly story.
    static_assertions::assert_impl_all!(ConnectionHandle: Send, Sync);

    /// Pair an actor over a loopback `TcpStream` with the server side of
    /// the same connection, so the wire format can be scripted directly.
    /// A real loopback rather than `tokio::io::duplex` because
    /// `MaybeTlsStream::Plain` wraps a concrete `TcpStream`.
    async fn paired() -> (ConnectionHandle, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect_fut = TcpStream::connect(addr);
        let accept_fut = async { listener.accept().await.unwrap().0 };
        let (client, server) = tokio::join!(connect_fut, accept_fut);
        let client = client.unwrap();
        let _ = client.set_nodelay(true);
        let _ = server.set_nodelay(true);

        let stream = MaybeTlsStream::Plain(client);
        let hello = ServerHello {
            server_name: "mock".to_string(),
            version: (1, 0, 0),
            revision: DBMS_TCP_PROTOCOL_VERSION,
            timezone: None,
            display_name: None,
        };
        let handle = ConnectionActor::spawn(stream, hello);
        (handle, server)
    }

    /// Like [`paired`] but threads an explicit [`ActorConfig`] (e.g. a
    /// `read_timeout`) into the spawned actor.
    async fn paired_with_config(config: ActorConfig) -> (ConnectionHandle, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect_fut = TcpStream::connect(addr);
        let accept_fut = async { listener.accept().await.unwrap().0 };
        let (client, server) = tokio::join!(connect_fut, accept_fut);
        let client = client.unwrap();
        let _ = client.set_nodelay(true);
        let _ = server.set_nodelay(true);
        let stream = MaybeTlsStream::Plain(client);
        let hello = ServerHello {
            server_name: "mock".to_string(),
            version: (1, 0, 0),
            revision: DBMS_TCP_PROTOCOL_VERSION,
            timezone: None,
            display_name: None,
        };
        let handle = ConnectionActor::spawn_with_config(stream, hello, config);
        (handle, server)
    }

    /// Read one byte from the server side.
    async fn read_byte(server: &mut TcpStream) -> u8 {
        let mut buf = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(server, &mut buf)
            .await
            .unwrap();
        buf[0]
    }

    #[tokio::test]
    async fn ping_happy_path() {
        let (handle, mut server) = paired().await;

        // Server side: read the Ping varint, reply with Pong.
        let server_task = tokio::spawn(async move {
            let id = read_byte(&mut server).await;
            assert_eq!(u64::from(id), ClientPacketId::Ping as u64);
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            // Hold the connection open so the read half stays usable;
            // dropping `server` here would close the socket and
            // poison the actor on the next read.
            server
        });

        handle.ping().await.expect("ping should succeed");
        assert!(handle.is_alive());

        // Release the server side; reader sub-task will exit on the
        // resulting socket close, but the test has already passed.
        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn ping_during_reader_disconnect() {
        let (handle, server) = paired().await;

        // Drop the server side immediately. The reader sub-task will
        // observe EOF on its next read; the ping should surface as
        // an error and the handle should be poisoned.
        drop(server);

        let result = handle.ping().await;
        assert!(result.is_err(), "ping should fail after server hangup");
        assert!(
            !handle.is_alive(),
            "handle should be poisoned after reader-task exit"
        );
    }

    /// `WriteHalf` has no `Drop`, and the reader parks in `read_packet`
    /// until the server's own receive timeout, so without the explicit
    /// close and abort every recycled connection leaks a task, an FD and
    /// a socket.
    #[tokio::test]
    async fn dropping_the_last_handle_closes_the_socket_and_exits_the_reader() {
        use tokio::io::AsyncReadExt;
        let (handle, mut server) = paired().await;
        drop(handle);

        // A closed client side reads as EOF, not as a stall.
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), server.read(&mut buf))
            .await
            .expect("the socket must close promptly, not wait on a server timeout")
            .expect("read after client close");
        assert_eq!(n, 0, "expected EOF on the server side, got {n} bytes");
    }

    /// `close` is the awaited form of the same shutdown, so callers that
    /// need the socket provably gone (the pool refusing a connection)
    /// have something to wait on.
    #[tokio::test]
    async fn close_waits_for_the_actor_and_is_idempotent() {
        use tokio::io::AsyncReadExt;
        let (handle, mut server) = paired().await;
        let second = handle.clone();

        handle.close().await.expect("close should succeed");

        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), server.read(&mut buf))
            .await
            .expect("close must have shut the socket down")
            .expect("read after close");
        assert_eq!(n, 0);

        // The control is already taken, so a second close is a no-op.
        second.close().await.expect("second close should be Ok");
    }

    /// A server that ignores Cancel must not hold the actor, and thereby
    /// its pool slot, past [`DRAIN_TIMEOUT`]. The connection is poisoned
    /// so the pool refuses it rather than handing on a stream pointer
    /// parked mid-result.
    ///
    /// Every wait here is driven explicitly -- a real socket read, or
    /// [`tokio::time::advance`]. Nothing may wait on a timer firing by
    /// itself: auto-advance is skipped whenever the driver was woken
    /// (tokio `runtime/time/mod.rs`, `park_thread_timeout`), and the
    /// reader sub-task's registered socket read makes that routine under
    /// load, so a `tokio::time::timeout` here hangs instead of firing.
    #[tokio::test(start_paused = true)]
    async fn drain_timeout_poisons_when_cancel_is_ignored() {
        use tokio::io::AsyncReadExt;
        let (handle, mut server) = paired().await;

        let cancel_handle = handle.clone();
        let exec = tokio::spawn(async move {
            cancel_handle
                .execute_query(
                    "q_drain".into(),
                    "SELECT sleep(9)".into(),
                    Vec::new(),
                    Vec::new(),
                )
                .await
        });

        // The Query packet reaching the server proves the actor is past
        // `issue_query` and sitting in its drain loop.
        let mut buf = [0u8; 4096];
        let n = server.read(&mut buf).await.expect("the Query packet");
        assert!(n > 0, "the actor must have issued the query");

        // Aborting drops the reply receiver, which is the caller-gone
        // path: Cancel, then the bounded drain. The server stays silent,
        // so the drain can only end on its own timeout.
        exec.abort();

        for _ in 0..10 {
            if !handle.is_alive() {
                break;
            }
            tokio::time::advance(DRAIN_TIMEOUT).await;
            tokio::task::yield_now().await;
        }
        assert!(
            !handle.is_alive(),
            "a drain that never reaches EndOfStream must poison the connection"
        );

        drop(server);
    }

    /// A server that stops reading fills the socket buffers and parks the
    /// write for the kernel's whole retransmission window. `read_timeout` is
    /// the per-operation I/O deadline, so it must bound the write too, and
    /// the half-written frame means the connection cannot be reused.
    #[tokio::test]
    async fn a_block_write_that_outlasts_read_timeout_times_out_and_poisons() {
        use tokio::io::AsyncReadExt;
        let (handle, mut server) = paired_with_config(ActorConfig {
            read_timeout: Some(Duration::from_millis(300)),
            ..ActorConfig::default()
        })
        .await;

        let opener = handle.clone();
        let begin = tokio::spawn(async move {
            opener
                .begin_insert(
                    "q_stall".into(),
                    "INSERT INTO t FORMAT Native".into(),
                    Vec::new(),
                    Vec::new(),
                )
                .await
        });
        let mut buf = [0u8; 4096];
        let n = server.read(&mut buf).await.expect("the Query packet");
        assert!(n > 0);
        write_schema_block(&mut server, &[("n", "UInt64")]).await;
        begin.await.unwrap().expect("the INSERT opens");

        // Far past any loopback socket buffer, and the server never reads.
        let block = vec![0u8; 64 * 1024 * 1024];
        let err = tokio::time::timeout(
            Duration::from_secs(10),
            handle.send_insert_block(block, 1, 1),
        )
        .await
        .expect("the write must stop at read_timeout, not wait on the peer")
        .expect_err("a stalled write is an error");
        assert!(matches!(err, Error::TimedOut), "got {err:?}");
        assert!(
            !handle.is_alive(),
            "a half-written frame poisons the connection"
        );
        drop(server);
    }

    /// The Query packet and its terminator are writes like any other: a peer
    /// that stops reading must not park the actor past `read_timeout`.
    #[tokio::test]
    async fn a_query_write_that_outlasts_read_timeout_times_out_and_poisons() {
        let (handle, server) = paired_with_config(ActorConfig {
            read_timeout: Some(Duration::from_millis(300)),
            ..ActorConfig::default()
        })
        .await;

        // Far past any loopback socket buffer, and the server never reads.
        let query = format!("SELECT '{}'", "x".repeat(64 * 1024 * 1024));
        let err = tokio::time::timeout(
            Duration::from_secs(10),
            handle.execute_query("q_stall".into(), query, Vec::new(), Vec::new()),
        )
        .await
        .expect("the write must stop at read_timeout, not wait on the peer")
        .expect_err("a stalled write is an error");
        assert!(matches!(err, Error::TimedOut), "got {err:?}");
        assert!(
            !handle.is_alive(),
            "a half-written query poisons the connection"
        );
        drop(server);
    }

    /// Shutdown flushes whatever a timed-out write left in the buffer. A peer
    /// that never reads would hold that flush, the actor task and the fd for
    /// the kernel's retransmission window unless the close is bounded.
    #[tokio::test]
    async fn closing_a_connection_whose_peer_stopped_reading_is_bounded() {
        use tokio::io::AsyncReadExt;
        let (handle, mut server) = paired_with_config(ActorConfig {
            read_timeout: Some(Duration::from_millis(200)),
            ..ActorConfig::default()
        })
        .await;

        let opener = handle.clone();
        let begin = tokio::spawn(async move {
            opener
                .begin_insert(
                    "q_close".into(),
                    "INSERT INTO t FORMAT Native".into(),
                    Vec::new(),
                    Vec::new(),
                )
                .await
        });
        let mut buf = [0u8; 4096];
        let n = server.read(&mut buf).await.expect("the Query packet");
        assert!(n > 0);
        write_schema_block(&mut server, &[("n", "UInt64")]).await;
        begin.await.unwrap().expect("the INSERT opens");

        // Blocks under the 64 KiB write buffer flush quickly until the kernel
        // buffers fill. The block that hits the wall times out with its bytes
        // still in the buffer, which is what shutdown then has to flush.
        let mut sent = 0;
        let err = loop {
            match handle.send_insert_block(vec![0u8; 32 * 1024], 1, 1).await {
                Ok(()) => sent += 1,
                Err(e) => break e,
            }
            assert!(sent < 10_000, "the kernel buffers never filled");
        };
        assert!(matches!(err, Error::TimedOut), "got {err:?}");

        let started = tokio::time::Instant::now();
        tokio::time::timeout(SHUTDOWN_TIMEOUT * 2, handle.close())
            .await
            .expect("close must give up on a peer that is not reading")
            .expect("the actor exits cleanly");
        assert!(
            started.elapsed() >= SHUTDOWN_TIMEOUT,
            "the flush had nowhere to go, so close waited out its bound: {:?}",
            started.elapsed()
        );

        // The socket is released: draining what was sent ends in EOF or reset.
        let mut sink = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), server.read_to_end(&mut sink))
            .await
            .expect("the server must see the connection end");
    }

    /// A server that rejects an INSERT and then closes poisons the connection
    /// through the reader, but the rejection is already queued. The caller
    /// must see it, not a transient refusal a retry loop would replay.
    #[tokio::test]
    async fn an_insert_rejection_queued_before_a_close_still_reaches_the_caller() {
        use tokio::io::AsyncReadExt;
        let (handle, mut server) = paired().await;

        let opener = handle.clone();
        let begin = tokio::spawn(async move {
            opener
                .begin_insert(
                    "q_reject".into(),
                    "INSERT INTO t FORMAT Native".into(),
                    Vec::new(),
                    Vec::new(),
                )
                .await
        });
        let mut buf = [0u8; 4096];
        let n = server.read(&mut buf).await.expect("the Query packet");
        assert!(n > 0);
        write_schema_block(&mut server, &[("n", "UInt64")]).await;
        begin.await.unwrap().expect("the INSERT opens");

        server
            .write_var_uint(ServerPacketId::Exception as u64)
            .await
            .unwrap();
        server.write_i32_le(469).await.unwrap();
        server.write_string(b"DB::Exception").await.unwrap();
        server.write_string(b"constraint violated").await.unwrap();
        server.write_string(b"").await.unwrap();
        AsyncWriteExt::write_u8(&mut server, 0).await.unwrap();
        server.flush().await.unwrap();
        server.shutdown().await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while handle.is_alive() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the reader poisons the connection on the close");

        let err = handle
            .send_insert_block(Vec::new(), 1, 0)
            .await
            .expect_err("the buffered rejection must surface");
        assert!(
            matches!(err, Error::ServerException { code: 469, .. }),
            "got {err:?}"
        );
        assert!(!err.is_retriable(), "a constraint violation is terminal");
    }

    /// A poisoned connection may still hold the previous answer on the wire,
    /// so a command queued behind the poisoning one must be refused unsent.
    #[tokio::test]
    async fn a_command_on_a_poisoned_connection_is_refused_without_a_write() {
        use tokio::io::AsyncReadExt;
        let (handle, mut server) = paired().await;
        handle.poison();

        let err = tokio::time::timeout(Duration::from_secs(2), handle.ping())
            .await
            .expect("a poisoned connection must answer, not wait on the server")
            .expect_err("the ping must be refused");
        assert!(
            err.is_retriable(),
            "a fresh connection may succeed: {err:?}"
        );

        drop(handle);
        let mut rest = Vec::new();
        server.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty(), "nothing may reach the wire: {rest:?}");
    }

    /// A caller that drops `begin_insert` after the Query packet went out
    /// cannot be told the INSERT opened, so the actor lands in `InsertActive`
    /// with nobody to finish it. The handle goes back to the pool at once, so
    /// it must already be poisoned or the next borrower gets "actor busy".
    #[tokio::test]
    async fn dropping_begin_insert_in_flight_poisons_the_connection() {
        use tokio::io::AsyncReadExt;
        let (handle, mut server) = paired().await;

        let opener = handle.clone();
        let begin = tokio::spawn(async move {
            opener
                .begin_insert(
                    "q_cancel".into(),
                    "INSERT INTO t FORMAT Native".into(),
                    Vec::new(),
                    Vec::new(),
                )
                .await
        });

        // The Query packet reaching the server proves the command is in flight.
        let mut buf = [0u8; 4096];
        let n = server.read(&mut buf).await.expect("the Query packet");
        assert!(n > 0, "the actor must have issued the query");

        begin.abort();
        let _ = begin.await;

        assert!(
            !handle.is_alive(),
            "a begin_insert the caller abandoned must poison the connection"
        );
    }

    /// The pool hands the same connection to the next caller, so a
    /// cursor dropped mid-stream has to leave the socket on a clean
    /// packet boundary.
    #[tokio::test]
    async fn dropping_the_cursor_cancels_and_leaves_the_connection_reusable() {
        let (handle, mut server) = paired().await;

        let server_task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            write_uint64_payload_block(&mut server, &[1, 2, 3]).await;

            // Wait for the Cancel the cursor drop triggers, then answer
            // EndOfStream so the actor's drain completes.
            let mut chunk = [0u8; 4096];
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let mut saw_cancel = false;
            while tokio::time::Instant::now() < deadline {
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(left, server.read(&mut chunk)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => {
                        if chunk[..n].contains(&(ClientPacketId::Cancel as u8)) {
                            saw_cancel = true;
                            break;
                        }
                    }
                    _ => break,
                }
            }
            assert!(saw_cancel, "dropping the cursor must send Cancel");
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();

            // Then serve the follow-up Ping.
            let id = read_byte(&mut server).await;
            assert_eq!(u64::from(id), ClientPacketId::Ping as u64);
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_drop".into(),
                "SELECT number AS n FROM numbers(3)".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");
        // Take the schema block only, then abandon the stream.
        let schema = cursor.next_block().await.unwrap().expect("schema block");
        assert_eq!(schema.num_rows, 0);
        drop(cursor);

        handle
            .ping()
            .await
            .expect("the connection must be reusable after a cursor drop");
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    /// One oversized block would hold the actor for its whole socket
    /// write, so it is rejected before any byte goes on the wire and the
    /// session stays open for a smaller one.
    #[tokio::test]
    async fn send_insert_block_rejects_oversized_block_and_stays_in_insert() {
        let (handle, mut server) = paired().await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            tokio::time::sleep(Duration::from_millis(400)).await;
            server
        });

        handle
            .begin_insert(
                "q_big".into(),
                "INSERT INTO t FORMAT Native".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("begin_insert should succeed");

        let err = handle
            .send_insert_block(vec![0u8; MAX_INSERT_BLOCK_BYTES + 1], 1, 1)
            .await
            .expect_err("an oversized block must be refused");
        match err {
            Error::Custom(msg) => assert!(msg.contains("exceeds the"), "got {msg}"),
            other => panic!("expected Custom, got {other:?}"),
        }

        // Still in InsertActive: a normal block is accepted.
        handle
            .send_insert_block(Vec::new(), 1, 0)
            .await
            .expect("the session must survive a refused block");
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    /// A SELECT issued during an INSERT is caller misuse; the cursor
    /// must surface it rather than hang, and the actor must survive.
    #[tokio::test]
    async fn execute_stream_during_insert_fails_before_a_cursor_is_returned() {
        let (handle, mut server) = paired().await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            tokio::time::sleep(Duration::from_millis(400)).await;
            server
        });

        handle
            .begin_insert(
                "q_busy_stream".into(),
                "INSERT INTO t FORMAT Native".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("begin_insert should succeed");

        // The cursor is handed back only once the query is issued, so the
        // misuse is the call's own error and a retry policy can act on it.
        let Err(err) = handle
            .execute_stream_cursor("rs_busy".into(), "SELECT 1".into(), Vec::new(), Vec::new())
            .await
        else {
            panic!("a SELECT during an INSERT must fail before a cursor is returned");
        };
        match err {
            Error::Custom(msg) => assert!(msg.contains("busy in INSERT"), "got {msg}"),
            other => panic!("expected Custom, got {other:?}"),
        }
        assert!(handle.is_alive(), "misuse must not poison the connection");

        let _server = server_task.await.unwrap();
    }

    /// A rejected INSERT leaves the socket in a known state, so the
    /// actor stays in Idle and the connection stays poolable.
    #[tokio::test]
    async fn begin_insert_exception_leaves_actor_idle_and_alive() {
        let (handle, mut server) = paired().await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_server_exception(&mut server, 60, "no such table").await;
            // Then answer the Ping that proves the actor is Idle.
            let id = read_byte(&mut server).await;
            assert_eq!(u64::from(id), ClientPacketId::Ping as u64);
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let err = handle
            .begin_insert(
                "q_reject".into(),
                "INSERT INTO nope FORMAT Native".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect_err("the server rejected the INSERT");
        match err {
            Error::ServerException { code, .. } => assert_eq!(code, 60),
            other => panic!("expected ServerException, got {other:?}"),
        }

        assert!(handle.is_alive(), "a rejection must not poison");
        handle
            .ping()
            .await
            .expect("the actor must be back in Idle after a rejected INSERT");

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn double_drop_safety() {
        let (handle, mut server) = paired().await;

        // Issue a ping; the server-side task is intentionally lazy
        // (sleeps before replying) so the call is in flight when we
        // drop the second handle clone.
        let clone = handle.clone();
        let ping_fut = tokio::spawn(async move { clone.ping().await });

        // Read the Ping byte to confirm the actor sent it, then
        // reply slowly so the future is alive across the drop.
        let server_task = tokio::spawn(async move {
            let _ = read_byte(&mut server).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        // Drop the original handle while the ping is in flight; the
        // worker stays alive because the spawned ping-future still
        // holds its own clone.
        drop(handle);

        // Ping completes normally despite the drop.
        let ping_result = ping_fut.await.unwrap();
        assert!(
            ping_result.is_ok(),
            "ping should still succeed: {ping_result:?}"
        );

        // Server side cleans up.
        let _server = server_task.await.unwrap();
    }

    // -----------------------------------------------------------------
    // ExecuteQuery
    // -----------------------------------------------------------------

    /// Drain (discard) bytes from the server side of the loopback until
    /// either `cancel_byte_seen` flips to true (a Cancel packet has
    /// arrived) or `eof` is observed. The Query packet plus its
    /// empty-block terminator is verbose; tests just need to swallow
    /// the bytes so the kernel buffer doesn't fill and stall the actor.
    async fn drain_client_bytes(server: &mut TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        // Best-effort drain with a short overall budget -- tests that
        // need the bytes back inspect `buf` after.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, server.read(&mut chunk)).await {
                Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                // EOF, a read error, or the deadline: the caller inspects
                // whatever arrived before this point.
                _ => break,
            }
        }
        buf
    }

    #[tokio::test]
    async fn execute_query_happy_path() {
        let (handle, mut server) = paired().await;

        // Server side: swallow whatever the client writes, then send
        // EndOfStream so do_execute_query returns Ok.
        let server_task = tokio::spawn(async move {
            // Brief drain so the actor's writes don't stall on a full
            // socket buffer (the Query packet is small enough that
            // this never blocks in practice, but a real read keeps
            // the symmetry obvious).
            let _drained = drain_client_bytes(&mut server).await;
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let result = handle
            .execute_query("q1".into(), "SELECT 1".into(), Vec::new(), Vec::new())
            .await;
        assert!(result.is_ok(), "execute_query should succeed: {result:?}");
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_query_surfaces_server_exception() {
        let (handle, mut server) = paired().await;

        // Server side: drain client bytes, then write a query Exception
        // and NOTHING after it. A real server does not send EndOfStream
        // after a query Exception (it is terminal), so the actor must
        // surface the error promptly WITHOUT draining. If it drained, it
        // would block until DRAIN_TIMEOUT and then poison the
        // connection -- which the is_alive assertion below would catch.
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            server
                .write_var_uint(ServerPacketId::Exception as u64)
                .await
                .unwrap();
            server.write_i32_le(60i32).await.unwrap();
            server
                .write_string("DB::Exception".as_bytes())
                .await
                .unwrap();
            server
                .write_string("table not found".as_bytes())
                .await
                .unwrap();
            server.write_string("".as_bytes()).await.unwrap();
            tokio::io::AsyncWriteExt::write_u8(&mut server, 0u8)
                .await
                .unwrap(); // obsolete has_nested byte
            server.flush().await.unwrap();
            server
        });

        let err = handle
            .execute_query(
                "q2".into(),
                "SELECT * FROM nope".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect_err("expected server Exception to surface");
        match err {
            Error::ServerException { code, .. } => assert_eq!(code, 60),
            other => panic!("expected ServerException, got {other:?}"),
        }
        // A query Exception is terminal and non-fatal: the connection
        // stays alive and reusable (NOT poisoned, NOT drained).
        assert!(
            handle.is_alive(),
            "connection should remain alive after a server Exception"
        );

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_query_cancel_on_reply_drop() {
        let (handle, mut server) = paired().await;

        // Server side: drain the initial Query bytes, then sit quiet
        // -- no EndOfStream yet, so the actor stays in the drain loop.
        // Once the client drops the reply, the actor will send Cancel;
        // we read until we observe the Cancel varint (0x03), then
        // reply EndOfStream so the actor's bounded drain finishes.
        let server_task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            // Drain bytes until we have a fair chance of seeing the
            // Query+empty-block stream fully written.
            let mut initial = Vec::new();
            let mut chunk = [0u8; 4096];
            // Read with a small timeout per chunk so we eventually stop
            // and let the test issue the drop.
            loop {
                match tokio::time::timeout(Duration::from_millis(50), server.read(&mut chunk)).await
                {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => initial.extend_from_slice(&chunk[..n]),
                    _ => break,
                }
            }
            assert!(
                !initial.is_empty(),
                "server should have seen at least the Query packet"
            );

            // Now keep reading; we expect a Cancel byte (0x03) to
            // arrive after the test drops the future.
            let mut saw_cancel = false;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(remaining, server.read(&mut chunk)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => {
                        if chunk[..n].contains(&(ClientPacketId::Cancel as u8)) {
                            saw_cancel = true;
                            break;
                        }
                    }
                    _ => break,
                }
            }
            assert!(saw_cancel, "server should have observed Cancel byte");

            // Reply EndOfStream so the actor's drain finishes within
            // the timeout, leaving the connection reusable.
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        // Issue execute_query but drop the future quickly via a
        // timeout that resolves before the server sends EndOfStream.
        // `tokio::time::timeout` drops the inner future on expiry,
        // which drops the oneshot::Receiver -- the cancel path.
        let cancel_handle = handle.clone();
        let exec_fut = cancel_handle.execute_query(
            "qcancel".into(),
            "SELECT sleep(10)".into(),
            Vec::new(),
            Vec::new(),
        );
        // Brief timeout so the actor has time to flush the Query
        // packet to the server before we drop the future.
        let timeout_result = tokio::time::timeout(Duration::from_millis(150), exec_fut).await;
        assert!(
            timeout_result.is_err(),
            "execute_query should not have completed inside the timeout"
        );

        // Wait for the server-side task to observe the Cancel and send
        // EndOfStream.
        let mut server = server_task.await.unwrap();

        // The actor must still be alive (not poisoned) and the
        // connection still usable -- prove it by reusing the same
        // handle for a ping.
        assert!(
            handle.is_alive(),
            "handle should remain alive after a successful cancel-and-drain"
        );

        // Drive a ping over the same connection. The server-side task
        // now answers Pong; the actor must see it cleanly.
        let ping_server_task = tokio::spawn(async move {
            let id = read_byte(&mut server).await;
            assert_eq!(u64::from(id), ClientPacketId::Ping as u64);
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });
        handle
            .ping()
            .await
            .expect("connection should be reusable after cancel");
        let _server = ping_server_task.await.unwrap();
    }

    // -----------------------------------------------------------------
    // INSERT lifecycle (BeginInsert / SendInsertBlock / FinishInsert)
    // -----------------------------------------------------------------

    /// Write a single server Exception packet with the supplied code +
    /// message, and nothing after it. This is the realistic terminal
    /// shape: a real server sends NO EndOfStream after a query
    /// Exception, so the actor must surface the error without draining.
    async fn write_server_exception(server: &mut TcpStream, code: i32, message: &str) {
        server
            .write_var_uint(ServerPacketId::Exception as u64)
            .await
            .unwrap();
        server.write_i32_le(code).await.unwrap();
        server.write_string(b"DB::Exception").await.unwrap();
        server.write_string(message.as_bytes()).await.unwrap();
        server.write_string(b"").await.unwrap();
        AsyncWriteExt::write_u8(server, 0).await.unwrap(); // obsolete has_nested byte
        server.flush().await.unwrap();
    }

    #[tokio::test]
    async fn insert_state_machine_rejects_ping_when_busy() {
        let (handle, mut server) = paired().await;

        // Server side: drain the BeginInsert Query bytes, send the
        // schema block, then hold the connection open so the actor
        // sits in InsertActive while we issue the ping.
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            // Hold the connection -- keep the server side alive so the
            // ping rejection is observed before the actor sees EOF.
            tokio::time::sleep(Duration::from_millis(200)).await;
            server
        });

        // Drive BeginInsert; the call returns once the schema block
        // arrives.
        let headers = handle
            .begin_insert(
                "q_busy".into(),
                "INSERT INTO t FORMAT Native".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("begin_insert should succeed against scripted server");
        assert_eq!(headers, vec![("n".to_string(), "UInt64".to_string())]);

        // Ping while busy -- must reject without exiting InsertActive.
        let err = handle
            .ping()
            .await
            .expect_err("ping during InsertActive must error");
        match err {
            Error::Custom(msg) => assert!(
                msg.contains("busy"),
                "expected 'busy' in ping error, got {msg}"
            ),
            other => panic!("expected Custom busy error, got {other:?}"),
        }
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn send_insert_block_without_begin_errs() {
        let (handle, _server) = paired().await;

        let err = handle
            .send_insert_block(Vec::new(), 0, 0)
            .await
            .expect_err("send_insert_block in Idle must error");
        match err {
            Error::Custom(msg) => assert!(
                msg.contains("no INSERT session"),
                "expected 'no INSERT session' in error, got {msg}"
            ),
            other => panic!("expected Custom error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn finish_insert_returns_to_idle() {
        let (handle, mut server) = paired().await;

        // Server side: drain BeginInsert bytes, send schema block.
        // Then drain SendInsertBlock + FinishInsert bytes, send
        // EndOfStream so do_finish_insert returns Ok. Finally
        // answer the post-finish Ping with Pong.
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            // Drain SendInsertBlock + FinishInsert (just bytes; we
            // do not parse them here).
            let _block_bytes = drain_client_bytes(&mut server).await;
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            // Now serve the Ping that follows: read the Ping varint
            // and reply Pong.
            let id = read_byte(&mut server).await;
            assert_eq!(u64::from(id), ClientPacketId::Ping as u64);
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let headers = handle
            .begin_insert(
                "q_finish".into(),
                "INSERT INTO t FORMAT Native".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("begin_insert should succeed");
        assert_eq!(headers.len(), 1);

        // Send one (empty) block -- the actor is a transport here,
        // it does not validate the payload.
        handle
            .send_insert_block(Vec::new(), 1, 0)
            .await
            .expect("send_insert_block should succeed");

        handle
            .finish_insert()
            .await
            .expect("finish_insert should return Ok on EndOfStream");

        // Prove the actor returned to Idle: a Ping must now succeed.
        handle
            .ping()
            .await
            .expect("connection should be reusable after finish_insert");
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn full_duplex_exception_aborts_send() {
        let (handle, mut server) = paired().await;

        // Server side: drain BeginInsert bytes, send schema block, then
        // push a single Exception (no EndOfStream after it -- the
        // realistic terminal shape) simulating a constraint violation
        // surfaced before the client's second SendInsertBlock.
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            // Push an Exception "between blocks" -- give the actor a
            // moment to handle the schema block first.
            tokio::time::sleep(Duration::from_millis(20)).await;
            write_server_exception(&mut server, 241, "MEMORY_LIMIT_EXCEEDED").await;
            server
        });

        handle
            .begin_insert(
                "q_fd".into(),
                "INSERT INTO t FORMAT Native".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("begin_insert should succeed");

        // Wait long enough for the Exception to land in the reader's
        // mpsc queue, then call send_insert_block -- the try_recv
        // drain must see the Exception and abort before writing.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let err = handle
            .send_insert_block(Vec::new(), 1, 0)
            .await
            .expect_err("send_insert_block must surface the queued Exception");
        match err {
            Error::ServerException { code, .. } => assert_eq!(code, 241),
            other => panic!("expected ServerException(241), got {other:?}"),
        }

        // Actor should be Idle again; connection still usable for
        // non-INSERT commands.
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    // -----------------------------------------------------------------
    // ExecuteStream (streaming SELECT)
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn execute_stream_yields_schema_then_payload_then_eos() {
        let (handle, mut server) = paired().await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            // Schema block (num_rows = 0) then a payload with three
            // values then EndOfStream.
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            write_uint64_payload_block(&mut server, &[10, 20, 30]).await;
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_stream_unit".into(),
                "SELECT number AS n FROM numbers(3)".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");

        // First block: schema (num_rows = 0, schema vec non-empty).
        let schema_block = cursor
            .next_block()
            .await
            .expect("schema block decode")
            .expect("schema block");
        assert_eq!(schema_block.num_rows, 0);
        assert_eq!(
            schema_block.schema,
            vec![("n".to_string(), "UInt64".to_string())]
        );

        // Second block: payload (num_rows = 3, UInt64 column).
        let payload = cursor
            .next_block()
            .await
            .expect("payload decode")
            .expect("payload block");
        assert_eq!(payload.num_rows, 3);
        match &payload.columns[0] {
            crate::native::DecodedColumn::UInt64(values) => {
                assert_eq!(values, &vec![10u64, 20, 30]);
            }
            other => panic!("expected UInt64, got {other:?}"),
        }

        // Terminal EndOfStream surfaces as Ok(None); cursor remains
        // safe to poll past completion.
        assert!(cursor.next_block().await.unwrap().is_none());
        assert!(cursor.next_block().await.unwrap().is_none());
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_stream_surfaces_server_exception() {
        let (handle, mut server) = paired().await;

        // Server side: drain Query bytes, then a single Exception (no
        // EndOfStream after it -- the realistic terminal shape).
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            // Need not send a schema block first -- the server can
            // reject the query before any data flows.
            write_server_exception(&mut server, 60, "table not found").await;
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_stream_err".into(),
                "SELECT * FROM doesnt_exist".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");

        let err = cursor
            .next_block()
            .await
            .expect_err("server Exception must surface");
        match err {
            Error::ServerException { code, .. } => assert_eq!(code, 60),
            other => panic!("expected ServerException, got {other:?}"),
        }
        // Subsequent polls return Ok(None) -- cursor is terminal after
        // surfacing the error.
        assert!(cursor.next_block().await.unwrap().is_none());

        // Connection still usable after a server-side rejection.
        assert!(handle.is_alive());
        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_query_read_timeout_poisons() {
        // Server drains the Query bytes then goes silent -- it never
        // sends EndOfStream. With a short read_timeout the actor must
        // surface a retriable TimedOut (not hang) and poison the
        // connection so the pool drops it.
        let (handle, mut server) = paired_with_config(ActorConfig {
            read_timeout: Some(Duration::from_millis(80)),
            ..ActorConfig::default()
        })
        .await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            // Hold the socket open but stay silent -- a stalled backend.
            tokio::time::sleep(Duration::from_millis(500)).await;
            server
        });

        let err = handle
            .execute_query(
                "rq_timeout".into(),
                "SELECT sleep(9)".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect_err("a silent server must surface TimedOut, not hang");
        assert!(matches!(err, Error::TimedOut), "got {err:?}");
        assert!(err.is_retriable(), "TimedOut must be retriable");
        assert!(!handle.is_alive(), "timed-out connection must be poisoned");

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_stream_read_timeout_poisons() {
        // Server sends the schema block then stalls before any payload.
        // The cursor's first post-schema poll must surface TimedOut and
        // poison the connection -- not block until the caller gives up.
        let (handle, mut server) = paired_with_config(ActorConfig {
            read_timeout: Some(Duration::from_millis(80)),
            ..ActorConfig::default()
        })
        .await;

        let server_task = tokio::spawn(async move {
            // Send the schema block immediately (do NOT drain first --
            // drain_client_bytes runs a 200ms budget, which would delay
            // the schema past the 80ms read_timeout). The client's small
            // Query packet stays buffered in the kernel; the actor's send
            // does not stall. After the schema, go silent so the next
            // per-packet idle window elapses.
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_timeout".into(),
                "SELECT number AS n FROM numbers(9)".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");

        // Schema block arrives first.
        let schema = cursor.next_block().await.unwrap().expect("schema block");
        assert_eq!(schema.num_rows, 0);

        // Next poll waits on a silent server -> TimedOut, not a hang.
        let err = cursor
            .next_block()
            .await
            .expect_err("silent server mid-stream must surface TimedOut");
        assert!(matches!(err, Error::TimedOut), "got {err:?}");
        assert!(!handle.is_alive(), "timed-out connection must be poisoned");

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_stream_aligns_rows_across_multiple_blocks() {
        // Two payload blocks of different widths back-to-back then EOS.
        // The cursor must yield each block's rows in order, with no
        // cross-boundary misalignment.
        let (handle, mut server) = paired().await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            write_uint64_payload_block(&mut server, &[10, 20, 30]).await;
            write_uint64_payload_block(&mut server, &[40, 50]).await;
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_multiblock".into(),
                "SELECT number AS n FROM numbers(5)".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");

        // Schema, then block 1 (3 rows), then block 2 (2 rows), then EOS.
        let schema = cursor.next_block().await.unwrap().expect("schema block");
        assert_eq!(schema.num_rows, 0);

        let b1 = cursor.next_block().await.unwrap().expect("first payload");
        assert_eq!(b1.num_rows, 3);
        match &b1.columns[0] {
            crate::native::DecodedColumn::UInt64(v) => assert_eq!(v, &vec![10u64, 20, 30]),
            other => panic!("expected UInt64, got {other:?}"),
        }

        let b2 = cursor.next_block().await.unwrap().expect("second payload");
        assert_eq!(b2.num_rows, 2);
        match &b2.columns[0] {
            crate::native::DecodedColumn::UInt64(v) => assert_eq!(v, &vec![40u64, 50]),
            other => panic!("expected UInt64, got {other:?}"),
        }

        assert!(cursor.next_block().await.unwrap().is_none());
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }
}
