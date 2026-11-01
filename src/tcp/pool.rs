//! deadpool-managed connection pool for the TCP transport.
//!
//! HOT PATH. Every operation acquires from here, so this module is held to a
//! 90% line-coverage target rather than the repo's 80% floor, and meets it.
//! See `docs/coverage.md`.
//!
//! Wraps [`crate::tcp::connection_actor::ConnectionHandle`] in a
//! [`deadpool::managed::Pool`] so callers can acquire ready-to-use
//! connections without having to drive the connect + handshake +
//! actor-spawn sequence themselves. `recycle` returns the handle when
//! it is alive (i.e. not poisoned by an I/O failure mid-command) and
//! drops it otherwise -- the next caller then sees a freshly
//! handshaken connection courtesy of `Manager::create`.
//!
//! # Recycle semantics
//!
//! [`ConnectionHandle::is_alive`] checks the poisoned flag only; it does
//! not probe the socket. The actor's reader sub-task poisons on any I/O
//! failure and the writer arms poison on any send failure, so a torn
//! connection is flagged before its channel ever closes.
//!
//! Refusing a connection shuts its actor down and waits, so the socket
//! and the reader sub-task are gone before the slot refills. That wait
//! is what `recycle_timeout` bounds.
//!
//! Timeout configuration requires deadpool's `Runtime` to be set, which
//! is why [`build_pool`] threads `Runtime::Tokio1` through.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use deadpool::Runtime;
use deadpool::managed::{self, Metrics, Pool, RecycleError, RecycleResult};

use crate::error::{Error, Result};
use crate::tcp::connect::{self, ConnectKind};
use crate::tcp::connection_actor::{ActorConfig, ConnectionActor, ConnectionHandle};
use crate::tcp::handshake::HandshakeConfig;
use crate::tcp::retry::RetryPolicy;

/// Default maximum number of pooled TCP connections per pool.
///
/// Matches the existing HTTP pool default so a user switching
/// transports keeps the same concurrency ceiling. Callers that need
/// a different value tune through [`PoolConfig::max_size`] before
/// [`build_pool`] is called.
pub(crate) const DEFAULT_MAX_SIZE: usize = 8;

/// Default upper bound on how long a caller waits for a pool slot.
///
/// Bounded so a saturated pool surfaces an `Err` to the caller
/// instead of blocking indefinitely. 30s mirrors the ceiling the
/// HTTP pool applies to its checkout path; users that need tighter
/// bounds tune through [`PoolConfig::acquire_timeout`].
pub(crate) const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default upper bound on connect + handshake. Real handshakes
/// against a healthy LAN server complete in tens of milliseconds;
/// 10s is conservative head-room against transient packet loss.
pub(crate) const DEFAULT_CREATE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default upper bound on the recycle check. The liveness test is a
/// single atomic load, but refusing a connection also awaits its actor
/// shutdown, so this bounds a writer close against an unresponsive peer.
pub(crate) const DEFAULT_RECYCLE_TIMEOUT: Duration = Duration::from_secs(5);

/// deadpool `Manager` implementation for ClickHouse TCP connections.
///
/// `create` opens a fresh socket via
/// [`crate::tcp::connect::open_handshaken`], drives the handshake,
/// then spawns the connection actor and returns its cheap-clone
/// handle. `recycle` returns `Ok(())` when the handle is alive and
/// drops it otherwise via [`RecycleError::Message`]; deadpool then
/// calls `create` again for the next caller so the pool slot
/// refills.
#[non_exhaustive]
pub struct TcpConnectionManager {
    /// Candidate server addresses as the caller supplied them.
    /// [`build_pool`] asserts this is non-empty; `Manager::create` checks
    /// it again rather than trusting it, since this field is public. Each
    /// is resolved per connection via the async resolver rather than
    /// pinned to a `SocketAddr`, so new A records are picked up on
    /// reconnect.
    pub endpoints: Vec<String>,
    /// Round-robin cursor shared across the pool, so concurrent `create`
    /// calls spread their starting endpoint instead of all hammering
    /// endpoint 0. Wrapping at `usize::MAX` is harmless: the value is
    /// only used modulo `endpoints.len()`.
    pub next: Arc<AtomicUsize>,
    /// Plain TCP vs TLS selection. Carries the SNI on the TLS arm so
    /// the manager re-uses the same name across reconnects without
    /// having to re-derive it from the address.
    pub kind: ConnectKind,
    /// Handshake parameters (database, credentials, client name,
    /// chunked-mode preference). Cloned into each `create` call so
    /// the manager itself stays cheap to share across the pool.
    pub config: HandshakeConfig,
    /// Maximum connection age; `None` keeps connections for life. See
    /// [`PoolConfig::max_lifetime`].
    pub max_lifetime: Option<Duration>,
    /// Per-operation I/O deadline threaded to each spawned actor; see
    /// [`PoolConfig::read_timeout`]. `None` leaves I/O bounded only by
    /// caller-side cancellation.
    pub read_timeout: Option<Duration>,
    /// Budget for one whole `create`, shared across the endpoints it walks;
    /// see [`PoolConfig::create_timeout`], whose `None` leaves only an
    /// endpoint's last address unbounded.
    pub create_timeout: Option<Duration>,
}

impl TcpConnectionManager {
    /// Resolve `endpoint`, connect, handshake and spawn the actor within
    /// `budget`. Resolution goes through tokio's async resolver, not the
    /// blocking std `ToSocketAddrs`.
    async fn connect_endpoint(
        &self,
        endpoint: &str,
        budget: Option<Duration>,
    ) -> Result<ConnectionHandle> {
        let began = tokio::time::Instant::now();
        let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(endpoint)
            .await
            .map_err(|e| Error::Connect(format!("tcp: cannot resolve {endpoint:?}: {e}")))?
            .collect();
        let kind = self.kind.for_endpoint(endpoint);
        let budget = budget.map(|b| b.saturating_sub(began.elapsed()));
        self.connect_first(endpoint, addrs, &kind, budget).await
    }

    /// Connect to the first of `addrs` that completes a handshake, so a name
    /// whose first record is unreachable (`localhost` as `::1` against an
    /// IPv4-only server) or silent still connects.
    async fn connect_first(
        &self,
        endpoint: &str,
        addrs: impl IntoIterator<Item = std::net::SocketAddr>,
        kind: &ConnectKind,
        budget: Option<Duration>,
    ) -> Result<ConnectionHandle> {
        let addrs: Vec<std::net::SocketAddr> = addrs.into_iter().collect();
        let began = tokio::time::Instant::now();
        let mut last_err = None;
        for (i, &addr) in addrs.iter().enumerate() {
            let remaining = budget.map(|b| b.saturating_sub(began.elapsed()));
            if remaining.is_some_and(|r| r.is_zero()) {
                break;
            }
            let attempt = connect::open_handshaken(addr, kind, &self.config);
            let outcome = match attempt_limit(remaining, addrs.len() - i) {
                Some(limit) => tokio::time::timeout(limit, attempt)
                    .await
                    .unwrap_or_else(|_| {
                        Err(Error::Connect(format!(
                            "tcp: {endpoint:?} at {addr} did not answer within {limit:?}"
                        )))
                    }),
                None => attempt.await,
            };
            match outcome {
                Ok((stream, hello)) => {
                    return Ok(ConnectionActor::spawn_with_config(
                        stream,
                        hello,
                        ActorConfig {
                            read_timeout: self.read_timeout,
                            quota_key: self.config.quota_key.clone(),
                        },
                    ));
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            Error::Connect(format!("tcp: {endpoint:?} resolved to no addresses"))
        }))
    }
}

/// Smallest slice of the create budget one endpoint gets. A TLS connect and
/// handshake over a WAN link is several round trips, so a slice under this
/// would fail a healthy but distant endpoint just because the list is long.
const MIN_ENDPOINT_SLICE: Duration = Duration::from_secs(1);

/// The share of `remaining` one attempt may spend with `attempts_left`
/// still to try, floored at [`MIN_ENDPOINT_SLICE`] and capped at `remaining`.
fn endpoint_slice(remaining: Duration, attempts_left: usize) -> Duration {
    let share = remaining / u32::try_from(attempts_left.max(1)).unwrap_or(u32::MAX);
    share.max(MIN_ENDPOINT_SLICE).min(remaining)
}

/// How long one address may take, with `left` addresses still to try. With no
/// budget, every address but the last gets [`DEFAULT_CREATE_TIMEOUT`], so a
/// silent one cannot hold back the next and the last keeps the caller's
/// unbounded wait.
fn attempt_limit(remaining: Option<Duration>, left: usize) -> Option<Duration> {
    match remaining {
        Some(remaining) => Some(endpoint_slice(remaining, left)),
        None if left > 1 => Some(DEFAULT_CREATE_TIMEOUT),
        None => None,
    }
}

impl managed::Manager for TcpConnectionManager {
    type Type = ConnectionHandle;
    type Error = Error;

    async fn create(&self) -> Result<ConnectionHandle> {
        // Round-robin connect-failover: pick a starting endpoint via the
        // shared atomic cursor, walk the list from there in one pass,
        // return the first that connects.
        //
        // `build_pool` asserts a non-empty list; a directly-constructed
        // manager has no such guarantee, and the modulo below divides by zero.
        let n = self.endpoints.len();
        if n == 0 {
            return Err(Error::Connect(
                "tcp: no endpoints configured for this pool".to_string(),
            ));
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        let began = tokio::time::Instant::now();
        let mut last_err: Option<Error> = None;
        for i in 0..n {
            let endpoint = &self.endpoints[(start + i) % n];
            let outcome = match self.create_timeout {
                Some(total) => {
                    let remaining = total.saturating_sub(began.elapsed());
                    if remaining.is_zero() {
                        break;
                    }
                    let slice = endpoint_slice(remaining, n - i);
                    // The outer bound also covers name resolution.
                    tokio::time::timeout(slice, self.connect_endpoint(endpoint, Some(slice)))
                        .await
                        .unwrap_or_else(|_| {
                            Err(Error::Connect(format!(
                                "tcp: {endpoint:?} did not answer within {slice:?}"
                            )))
                        })
                }
                None => self.connect_endpoint(endpoint, None).await,
            };
            match outcome {
                Ok(handle) => return Ok(handle),
                // Remember and try the next endpoint -- a single dead or
                // unresolvable host must not sink the whole pass.
                Err(e) => last_err = Some(e),
            }
        }
        // Every endpoint failed this pass. Surface the last error unchanged so
        // it stays typed for retry classification. `last_err` is only unset
        // when the budget was spent before the first attempt.
        Err(last_err.unwrap_or_else(|| {
            Error::Connect("tcp: create timeout spent before any endpoint was tried".to_string())
        }))
    }

    async fn recycle(&self, obj: &mut ConnectionHandle, metrics: &Metrics) -> RecycleResult<Error> {
        // deadpool tracks the creation time in `Metrics`, so the handle
        // carries no timestamp of its own.
        match recycle_refusal(self.max_lifetime, metrics.created.elapsed(), obj.is_alive()) {
            Some(reason) => {
                close_refused(obj).await;
                Err(RecycleError::message(reason))
            }
            None => Ok(()),
        }
    }
}

/// Why a recycled connection is refused, or `None` to reuse it.
///
/// Pure, and the pool tests' mock manager calls this same function, so a
/// decision changed here changes what those tests observe -- a mock with its
/// own copy of the rule would keep passing while production drifted.
fn recycle_refusal(
    max_lifetime: Option<Duration>,
    created_elapsed: Duration,
    is_alive: bool,
) -> Option<&'static str> {
    if max_lifetime.is_some_and(|max| created_elapsed >= max) {
        return Some("connection exceeded max_lifetime");
    }
    (!is_alive).then_some("connection poisoned")
}

/// Shut the actor behind a refused handle down and wait for it, so the
/// socket, the actor task and its reader sub-task are gone before the
/// slot refills. Cloning is `O(1)` and `close` takes the shared control,
/// so this shuts down the same actor the caller was holding. Bounded by
/// deadpool's `recycle_timeout`.
async fn close_refused(obj: &ConnectionHandle) {
    if let Err(e) = obj.clone().close().await {
        tracing::warn!(
            target: "clickhouse::tcp",
            error = %e,
            "refused connection did not shut down cleanly"
        );
    }
}

/// Concrete pool type for TCP connections.
///
/// Type alias rather than a newtype so callers interact with deadpool's
/// surface directly: `pool.get().await` returns an `Object` that derefs
/// to `ConnectionHandle`, and dropping it returns the connection.
pub type NativePool = Pool<TcpConnectionManager>;

/// Pool configuration knobs surfaced to callers. Maps to a
/// [`deadpool::managed::PoolConfig`] at build time but exposes only
/// the dials this transport actually cares about; the deadpool
/// queue-mode and any future hooks are pool-internal concerns.
///
/// Defaults are conservative: 8 connections, 30 s acquire wait,
/// 10 s create timeout, 5 s recycle timeout. Tune by mutating the
/// struct before handing it to [`build_pool`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct PoolConfig {
    /// Maximum number of connections the pool will hold.
    pub max_size: usize,
    /// How long a caller may wait for a free pool slot before
    /// `Pool::get` returns `Err`. `None` waits indefinitely.
    pub acquire_timeout: Option<Duration>,
    /// How long `Manager::create` may take in total. The budget is split
    /// across the endpoints it walks, each attempt getting the remaining
    /// time divided by the endpoints left (never under one second), so a
    /// silent first endpoint still leaves time for the next. An endpoint's
    /// share is split the same way across the addresses its name resolves to.
    /// `None` bounds every address but an endpoint's last at the 10s default
    /// and waits on the last indefinitely.
    pub create_timeout: Option<Duration>,
    /// How long `Manager::recycle` may take before deadpool aborts
    /// it. `None` waits indefinitely.
    pub recycle_timeout: Option<Duration>,
    /// Maximum age of a pooled connection. On `recycle`, a connection
    /// older than this is dropped (and a fresh one created on the next
    /// acquire) rather than reused -- bounding connection-state drift on
    /// long-lived pools (server restarts, rotated DNS, server-local
    /// session state), mirroring clickhouse-go's `ConnMaxLifetime`.
    /// `None` (default) keeps connections for life. Idle-socket death is
    /// handled separately by the actor's reader sub-task (poison on
    /// EOF/I/O error); this dial is purely an age bound.
    pub max_lifetime: Option<Duration>,
    /// Per-operation I/O deadline, threaded into every spawned actor.
    /// A query/stream/begin-insert read that goes this long without
    /// receiving ANY packet, or an INSERT block or finish write that takes
    /// this long, poisons the connection and surfaces [`Error::TimedOut`].
    /// The read timer resets on each packet, so it bounds the gap BETWEEN
    /// packets, not total query time -- a long streaming SELECT that keeps
    /// delivering blocks never trips it. A block write is bounded as a
    /// whole, so size the timeout for the largest block. `None` (default)
    /// leaves I/O bounded only by caller-side cancellation.
    pub read_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_MAX_SIZE,
            acquire_timeout: Some(DEFAULT_ACQUIRE_TIMEOUT),
            create_timeout: Some(DEFAULT_CREATE_TIMEOUT),
            recycle_timeout: Some(DEFAULT_RECYCLE_TIMEOUT),
            max_lifetime: None,
            read_timeout: None,
        }
    }
}

/// Aggregated knobs [`crate::TcpClient`]'s builders thread through to
/// [`build_pool`], held on the client so a builder that changes a dial can
/// discard the pool and rebuild from them.
///
/// Endpoints are stored as the original strings the caller supplied
/// (e.g. `["127.0.0.1:9000"]`) so we can re-resolve at rebuild time
/// rather than caching a `SocketAddr` that may have gone stale.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct TcpClientConfig {
    /// Candidate server addresses as the caller supplied them, resolved at
    /// pool-build time so a hostname that gains new A records picks them up
    /// on rebuild. `TcpClient::new` / `new_tls` set a single-element list and
    /// `with_addrs` replaces it.
    pub endpoints: Vec<String>,
    /// Plain vs TLS transport selection. Defaults to `Plain`; set by
    /// `TcpClient::new_tls` under the `tls` feature.
    pub kind: ConnectKindConfig,
    /// Handshake parameters (database, credentials, quota_key).
    pub handshake: HandshakeConfig,
    /// Pool dials.
    pub pool: PoolConfig,
    /// Bounded-backoff retry policy for idempotent operations
    /// (SELECT, opt-in `ExecuteQuery`, Ping). `None` (default) means
    /// no extra passes -- endpoint failover still happens inside
    /// `create`. Consulted at dispatch time by
    /// [`crate::tcp::client_ext`], so changing it does NOT require a
    /// pool rebuild.
    pub retry: Option<RetryPolicy>,
}

/// Cargo-feature-independent mirror of [`ConnectKind`].
///
/// `ConnectKind` itself feature-gates its `Tls` arm so a build
/// without `tls` cannot construct it. `Client` holds
/// the choice unconditionally so its struct shape is identical
/// across feature configurations -- the conversion to `ConnectKind`
/// happens at pool-build time, where the feature gate is in scope.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum ConnectKindConfig {
    /// Plain TCP; no TLS upgrade.
    #[default]
    Plain,
    /// TLS over TCP. Held even when `tls` is off so the
    /// struct layout stays stable; pool build then returns an error
    /// if the feature is missing rather than silently downgrading to
    /// plain.
    Tls {
        /// SNI sent in the ClientHello and the name every endpoint's
        /// certificate is validated against. `None` validates each endpoint
        /// against its own host, so failover to another host verifies.
        server_name: Option<String>,
    },
}

/// Build-time TLS intent for the TCP pool's connect path.
///
/// Distinguishes "no trust was configured" (-> default anchors are fine)
/// from "a trust WAS configured but could not be resolved" (-> fail
/// closed, never silently fall back to broad default trust). Carried as
/// a build-time-only input rather than on `PoolConfig` because it is not
/// a runtime dial.
#[cfg(feature = "tls")]
#[non_exhaustive]
pub enum TcpTls {
    /// Caller never customised trust. The TLS arm resolves the default
    /// native+webpki anchors (happy path).
    NotConfigured,
    /// A trust was configured and resolved to this config.
    Resolved(std::sync::Arc<tokio_rustls::rustls::ClientConfig>),
    /// A trust WAS configured but failed to resolve. Refuse to build a
    /// TLS pool rather than fall back to broad default trust.
    ConfiguredButFailed,
}

/// Build a TCP connection pool over the supplied endpoint list,
/// handshake config, and dials. `Runtime::Tokio1` is supplied
/// unconditionally, because deadpool refuses to build once any timeout
/// is set without it.
///
/// # Panics
///
/// If `endpoints` is empty. A pool that can never connect is a caller bug,
/// caught here rather than deferred to the first acquire; the `TcpClient`
/// builders assert non-emptiness before reaching this.
///
/// # Errors
///
/// [`Error::Custom`] when [`deadpool::managed::PoolBuilder::build`]
/// fails. Its only documented failure is `NoRuntimeSpecified`, which
/// supplying a runtime rules out; the mapping is explicit so a future
/// deadpool build failure surfaces typed rather than as a panic.
// The `Tls` arm moves `server_name` out of `kind`; without that feature the
// only arm is a unit variant, so nothing consumes it.
#[cfg_attr(not(feature = "tls"), allow(clippy::needless_pass_by_value))]
pub fn build_pool(
    endpoints: Vec<String>,
    kind: ConnectKindConfig,
    config: HandshakeConfig,
    pool_cfg: PoolConfig,
    // Build-time TLS intent from the `Client` (its `with_tls_*`
    // builders, or `with_tls_config`). See [`TcpTls`]. Carried as a
    // separate arg rather than on `PoolConfig` because it is a build-
    // time-only input, not a runtime dial.
    #[cfg(feature = "tls")] tls: TcpTls,
) -> Result<NativePool> {
    assert!(
        !endpoints.is_empty(),
        "build_pool requires a non-empty endpoint list"
    );
    let kind = match kind {
        ConnectKindConfig::Plain => ConnectKind::Plain,
        // Resolve the shared trust to one ClientConfig at pool build.
        // Fail-closed: a configured-but-unresolved trust must NOT fall
        // back to default anchors (that would silently broaden trust).
        // It builds a TlsFailClosed pool rather than erroring here, so a
        // builder chain like `tcp_tls().with_tls_roots_exclusive()
        // .try_with_tls_root_ca(..)` does not panic on the transient
        // unresolved state -- the refusal surfaces at connect time, never
        // as a silent downgrade to plain/HTTP or to default trust.
        #[cfg(feature = "tls")]
        ConnectKindConfig::Tls { server_name } => match tls {
            TcpTls::Resolved(config) => ConnectKind::Tls {
                server_name,
                config,
            },
            TcpTls::NotConfigured => {
                let config = crate::tls::build_client_config(&crate::tls::TlsConfigSource::Trust(
                    crate::tls::TlsTrust::default(),
                ))?;
                ConnectKind::Tls {
                    server_name,
                    config,
                }
            }
            TcpTls::ConfiguredButFailed => ConnectKind::TlsFailClosed,
        },
        #[cfg(not(feature = "tls"))]
        ConnectKindConfig::Tls { .. } => {
            return Err(Error::Custom(
                "tcp: TLS requested but the `tls` feature is not enabled".into(),
            ));
        }
    };
    let manager = TcpConnectionManager {
        endpoints,
        next: Arc::new(AtomicUsize::new(0)),
        kind,
        config,
        max_lifetime: pool_cfg.max_lifetime,
        read_timeout: pool_cfg.read_timeout,
        create_timeout: pool_cfg.create_timeout,
    };
    // The manager splits the create budget across endpoints itself, so
    // deadpool's whole-create abort would only cut the last slice short.
    Pool::builder(manager)
        .max_size(pool_cfg.max_size)
        .wait_timeout(pool_cfg.acquire_timeout)
        .create_timeout(None)
        .recycle_timeout(pool_cfg.recycle_timeout)
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(|e| Error::Custom(format!("tcp: failed to build pool: {e}")))
}

#[cfg(test)]
mod tests {
    //! A mock `Manager` hands out `ConnectionHandle`s built over
    //! loopback `TcpStream`s, so deadpool's recycle protocol is
    //! exercised against the real `is_alive` / `poison` contract with no
    //! ClickHouse server involved.
    use super::*;
    use crate::tcp::connection_actor::ConnectionActor;
    use crate::tcp::protocol::{DBMS_TCP_PROTOCOL_VERSION, ServerHello};
    use crate::tcp::transport::MaybeTlsStream;
    // `Arc`, `AtomicUsize`, `Ordering` are brought in via `super::*`
    // (the manager now uses them for the round-robin cursor).
    use tokio::net::{TcpListener, TcpStream};

    /// Spin up a loopback `TcpStream` and wrap it in a
    /// `ConnectionHandle`. Each call binds an ephemeral port; the
    /// server-side `TcpStream` is dropped immediately so the
    /// connection is half-closed -- the handle still works for
    /// `is_alive` / `poison` semantics, which is all the pool tests
    /// need.
    async fn fabricate_handle() -> ConnectionHandle {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect_fut = TcpStream::connect(addr);
        let accept_fut = async { listener.accept().await.unwrap().0 };
        let (client, _server) = tokio::join!(connect_fut, accept_fut);
        let client = client.unwrap();
        let _ = client.set_nodelay(true);
        let stream = MaybeTlsStream::Plain(client);
        let hello = ServerHello {
            server_name: "mock".to_string(),
            version: (1, 0, 0),
            revision: DBMS_TCP_PROTOCOL_VERSION,
            timezone: None,
            display_name: None,
        };
        ConnectionActor::spawn(stream, hello)
    }

    /// Mock manager: each `create` returns a freshly fabricated
    /// handle and bumps a counter so the test can assert how many
    /// distinct handles the pool has produced. `recycle` delegates to the
    /// production [`recycle_refusal`], so these tests cover it.
    struct TestManager {
        creates: Arc<AtomicUsize>,
        max_lifetime: Option<Duration>,
    }

    impl TestManager {
        fn new() -> Self {
            Self {
                creates: Arc::new(AtomicUsize::new(0)),
                max_lifetime: None,
            }
        }
    }

    impl managed::Manager for TestManager {
        type Type = ConnectionHandle;
        type Error = Error;

        async fn create(&self) -> Result<ConnectionHandle> {
            self.creates.fetch_add(1, Ordering::AcqRel);
            Ok(fabricate_handle().await)
        }

        fn recycle(
            &self,
            obj: &mut ConnectionHandle,
            metrics: &Metrics,
        ) -> impl std::future::Future<Output = RecycleResult<Error>> {
            // The production decision itself, not a copy of it. Only the
            // actor shutdown is skipped: a fabricated handle has no actor.
            std::future::ready(
                match recycle_refusal(self.max_lifetime, metrics.created.elapsed(), obj.is_alive())
                {
                    Some(reason) => Err(RecycleError::message(reason)),
                    None => Ok(()),
                },
            )
        }
    }

    fn build_test_pool(mgr: TestManager, cfg: PoolConfig) -> Pool<TestManager> {
        Pool::<TestManager>::builder(mgr)
            .max_size(cfg.max_size)
            .wait_timeout(cfg.acquire_timeout)
            .create_timeout(cfg.create_timeout)
            .recycle_timeout(cfg.recycle_timeout)
            .runtime(Runtime::Tokio1)
            .build()
            .expect("test pool builds")
    }

    /// Poisoning a handle before returning it to the pool must cause
    /// the next acquire to hit `create` again -- the poisoned
    /// connection is dropped, not reused.
    #[tokio::test]
    async fn pool_recycle_drops_poisoned_handle() {
        let mgr = TestManager::new();
        let creates = Arc::clone(&mgr.creates);
        let pool = build_test_pool(mgr, PoolConfig::default());

        let first = pool.get().await.expect("acquire");
        assert_eq!(creates.load(Ordering::Acquire), 1);
        // Poison and return to the pool. recycle should refuse.
        first.poison();
        assert!(!first.is_alive());
        drop(first);

        let _second = pool.get().await.expect("acquire again");
        assert_eq!(
            creates.load(Ordering::Acquire),
            2,
            "poisoned handle should have been dropped and a fresh one created"
        );
    }

    /// A healthy handle returned to the pool must be re-used on the
    /// next acquire -- `create` should NOT be called a second time.
    #[tokio::test]
    async fn pool_recycle_keeps_alive_handle() {
        let mgr = TestManager::new();
        let creates = Arc::clone(&mgr.creates);
        let pool = build_test_pool(mgr, PoolConfig::default());

        let first = pool.get().await.expect("acquire");
        assert_eq!(creates.load(Ordering::Acquire), 1);
        assert!(first.is_alive());
        drop(first);

        let _second = pool.get().await.expect("acquire again");
        assert_eq!(
            creates.load(Ordering::Acquire),
            1,
            "alive handle should have been reused, not re-created"
        );
    }

    /// A connection older than `max_lifetime` must be dropped on the
    /// next acquire rather than reused, even though it is still alive.
    /// Mirrors go's `ConnMaxLifetime` / `database/sql` lifetime cap and
    /// guards against long-lived TCP sessions accumulating server-side
    /// state. The age check reads deadpool's `Metrics.created`, so no
    /// per-handle timestamp is needed.
    #[tokio::test]
    async fn pool_recycle_drops_aged_connection() {
        let creates = Arc::new(AtomicUsize::new(0));
        let mgr = TestManager {
            creates: Arc::clone(&creates),
            max_lifetime: Some(Duration::from_millis(10)),
        };
        let pool = build_test_pool(mgr, PoolConfig::default());

        let first = pool.get().await.expect("acquire");
        assert_eq!(creates.load(Ordering::Acquire), 1);
        assert!(first.is_alive(), "fresh handle is alive");
        drop(first);

        // Let the connection age past max_lifetime before re-acquiring.
        tokio::time::sleep(Duration::from_millis(30)).await;

        let _second = pool.get().await.expect("acquire again");
        assert_eq!(
            creates.load(Ordering::Acquire),
            2,
            "connection older than max_lifetime should be dropped and re-created"
        );
    }

    /// The refusal rule itself, including the boundary the pool tests
    /// cannot hit precisely: age is refused at `>= max`, not `> max`, and
    /// age outranks liveness so an aged-but-alive connection still goes.
    #[test]
    fn recycle_refusal_covers_age_then_liveness() {
        let ms = Duration::from_millis;
        let cases: [(Option<Duration>, Duration, bool, Option<&str>); 6] = [
            (None, ms(10_000), true, None),
            (None, ms(0), false, Some("connection poisoned")),
            (Some(ms(10)), ms(9), true, None),
            (
                Some(ms(10)),
                ms(10),
                true,
                Some("connection exceeded max_lifetime"),
            ),
            (
                Some(ms(10)),
                ms(11),
                false,
                Some("connection exceeded max_lifetime"),
            ),
            (Some(ms(10)), ms(9), false, Some("connection poisoned")),
        ];
        for (max, elapsed, alive, want) in cases {
            assert_eq!(
                recycle_refusal(max, elapsed, alive),
                want,
                "max={max:?} elapsed={elapsed:?} alive={alive}"
            );
        }
    }

    /// `max_size` must cap concurrent acquires: once `max_size`
    /// handles are checked out, a further acquire blocks until one
    /// is returned. Verify by issuing N+1 acquires under a tight
    /// max_size and checking the last one only completes after a
    /// drop.
    #[tokio::test]
    async fn pool_max_size_blocks_excess_acquires() {
        let cfg = PoolConfig {
            max_size: 2,
            // Disable wait_timeout for this test -- we want the third
            // acquire to block, not error.
            acquire_timeout: None,
            ..PoolConfig::default()
        };
        let mgr = TestManager::new();
        let pool = build_test_pool(mgr, cfg);

        let a = pool.get().await.expect("a");
        let b = pool.get().await.expect("b");

        // Third acquire must not complete while a + b are held.
        let pool_clone = pool.clone();
        let third = tokio::spawn(async move { pool_clone.get().await.is_ok() });

        // Give the third acquire a moment to run; it should still be
        // pending because both slots are taken.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !third.is_finished(),
            "third acquire should block while pool is saturated"
        );

        // Releasing one slot unblocks the third acquire.
        drop(a);
        let acquired = tokio::time::timeout(Duration::from_secs(2), third)
            .await
            .expect("third acquire should unblock once a slot is freed")
            .expect("join");
        assert!(acquired, "third acquire should succeed after a drop");

        drop(b);
    }

    /// A `create` that never completes must surface through the pool's
    /// own `create_timeout` rather than hanging the acquire; the mapped
    /// error has to stay retriable so the next pass tries another
    /// endpoint.
    #[tokio::test]
    async fn create_timeout_surfaces_as_a_pool_error() {
        struct StalledManager;

        impl managed::Manager for StalledManager {
            type Type = ConnectionHandle;
            type Error = Error;

            async fn create(&self) -> Result<ConnectionHandle> {
                std::future::pending::<()>().await;
                unreachable!("the create timeout fires first")
            }

            fn recycle(
                &self,
                _: &mut ConnectionHandle,
                _: &Metrics,
            ) -> impl std::future::Future<Output = RecycleResult<Error>> {
                std::future::ready(Ok(()))
            }
        }

        let pool = Pool::<StalledManager>::builder(StalledManager)
            .max_size(1)
            .create_timeout(Some(Duration::from_millis(20)))
            .runtime(Runtime::Tokio1)
            .build()
            .expect("test pool builds");

        let Ok(acquired) = tokio::time::timeout(Duration::from_secs(2), pool.get()).await else {
            panic!("acquire hung past the create timeout")
        };
        let Err(err) = acquired else {
            panic!("create never completes; acquire must fail")
        };
        let mapped = crate::tcp::retry::map_pool_error(err);
        assert!(
            matches!(mapped, Error::Transient(_)),
            "a create timeout must map to a transient error, got {mapped:?}"
        );
        assert!(crate::tcp::retry::is_retriable_transport(&mapped));
    }

    /// A bounded `acquire_timeout` must surface as an `Err` when the
    /// pool stays saturated; the caller never hangs indefinitely.
    #[tokio::test]
    async fn pool_acquire_timeout_returns_err() {
        let cfg = PoolConfig {
            max_size: 1,
            acquire_timeout: Some(Duration::from_millis(10)),
            ..PoolConfig::default()
        };
        let mgr = TestManager::new();
        let pool = build_test_pool(mgr, cfg);

        // Hold the only slot.
        let _held = pool.get().await.expect("first acquire");

        // The second acquire must return Err within a generous bound;
        // 200 ms gives plenty of headroom over the 10 ms timeout to
        // avoid flakiness on a loaded CI runner while still proving
        // the call does not hang.
        let start = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_millis(200), pool.get()).await;
        let elapsed = start.elapsed();
        let Ok(acquired) = result else {
            panic!("acquire hung past the wait_timeout bound")
        };
        let Err(_refused) = acquired else {
            panic!("acquire should not have succeeded with one held slot")
        };
        assert!(
            elapsed < Duration::from_millis(200),
            "acquire should have errored quickly, took {elapsed:?}"
        );
    }

    // ---- Endpoint rotation + connect-failover -------------------------
    //
    // These exercise the REAL `TcpConnectionManager::create` against
    // loopback listeners running a minimal mock ClickHouse handshake
    // (read client Hello -> write ServerHello -> read addendum). That is
    // the smallest server that lets `open_handshaken` succeed, so we can
    // observe WHICH endpoint a given `create` landed on (round-robin)
    // and that a refused endpoint is skipped within one pass (failover).

    // `Manager` provides `create`, which these tests call directly on
    // the real manager (not via the pool); the server side of the
    // handshake is scripted in `crate::tcp::mock`.
    use crate::tcp::connect::ConnectKind;
    use crate::tcp::mock::serve_one_handshake;
    use deadpool::managed::Manager as _;

    /// Bind a loopback listener and spawn a task that accepts up to
    /// `accepts` connections, each tagged with its 0-based endpoint
    /// index pushed onto the shared `order` log so a test can read the
    /// acceptance order. Returns the bound address.
    async fn spawn_mock_endpoint(
        idx: usize,
        accepts: usize,
        order: Arc<std::sync::Mutex<Vec<usize>>>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            for _ in 0..accepts {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                order.lock().unwrap().push(idx);
                serve_one_handshake(&mut sock).await;
            }
        });
        addr
    }

    /// Build a real `TcpConnectionManager` over the supplied endpoints
    /// (plain, default handshake, no lifetime / read timeout). Shares
    /// the round-robin cursor seeded at 0 so the first `create` starts
    /// on endpoint 0.
    fn manager_over(endpoints: Vec<String>) -> TcpConnectionManager {
        TcpConnectionManager {
            endpoints,
            next: Arc::new(AtomicUsize::new(0)),
            kind: ConnectKind::Plain,
            config: HandshakeConfig::default(),
            max_lifetime: None,
            read_timeout: None,
            create_timeout: None,
        }
    }

    /// Successive `create` calls must land on endpoints in rotation:
    /// with the cursor seeded at 0, three creates over three endpoints
    /// hit 0, 1, 2 in order.
    #[tokio::test]
    async fn create_round_robins_across_endpoints() {
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut endpoints = Vec::new();
        for idx in 0..3 {
            endpoints.push(spawn_mock_endpoint(idx, 1, Arc::clone(&order)).await);
        }
        let mgr = manager_over(endpoints);

        for _ in 0..3 {
            mgr.create()
                .await
                .expect("create connects to the next endpoint");
        }

        let observed = order.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![0, 1, 2],
            "three creates should round-robin across the three endpoints in order"
        );
    }

    /// A refusing endpoint (a bound-then-immediately-dropped listener,
    /// so connects are refused) must be skipped within a single
    /// `create` pass, connecting to the next accepting endpoint.
    #[tokio::test]
    async fn create_fails_over_to_next_endpoint() {
        // A listener bound then dropped frees its port; connects to it
        // are refused (ECONNREFUSED) rather than hanging.
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let refusing = dead.local_addr().unwrap().to_string();
        drop(dead);

        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let accepting = spawn_mock_endpoint(1, 1, Arc::clone(&order)).await;

        // Cursor seeded at 0 => the first create starts on the refusing
        // endpoint, must skip it, and connect to the accepting one in
        // the SAME pass.
        let mgr = manager_over(vec![refusing, accepting]);
        mgr.create()
            .await
            .expect("create should fail over to the accepting endpoint");

        let observed = order.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![1],
            "create should have connected only to the accepting endpoint after skipping the refusing one"
        );
    }

    /// One name resolving to several addresses is tried address by address,
    /// and an empty resolution names the endpoint.
    #[tokio::test]
    async fn an_endpoint_walks_every_address_it_resolves_to() {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let refusing = dead.local_addr().unwrap();
        drop(dead);
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let accepting: std::net::SocketAddr = spawn_mock_endpoint(1, 1, Arc::clone(&order))
            .await
            .parse()
            .unwrap();
        let mgr = manager_over(vec!["dual.example:9000".to_string()]);

        mgr.connect_first(
            "dual.example:9000",
            [refusing, accepting],
            &ConnectKind::Plain,
            None,
        )
        .await
        .expect("the second address must be reached");
        assert_eq!(order.lock().unwrap().clone(), vec![1]);

        let Err(err) = mgr
            .connect_first("dual.example:9000", [], &ConnectKind::Plain, None)
            .await
        else {
            panic!("no address cannot connect")
        };
        assert!(err.to_string().contains("dual.example:9000"), "{err}");
    }

    /// An address that accepts and never answers must not hold back the
    /// healthy address after it under the same name.
    #[tokio::test]
    async fn a_silent_first_address_leaves_budget_for_the_second() {
        let hole = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent = hole.local_addr().unwrap();
        let holder = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = hole.accept().await {
                held.push(sock);
            }
        });
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let live: std::net::SocketAddr = spawn_mock_endpoint(1, 1, Arc::clone(&order))
            .await
            .parse()
            .unwrap();
        let mgr = manager_over(vec!["dual.example:9000".to_string()]);

        // The outer timeout only stops a regression hanging the suite.
        let outcome = tokio::time::timeout(
            Duration::from_secs(8),
            mgr.connect_first(
                "dual.example:9000",
                [silent, live],
                &ConnectKind::Plain,
                Some(Duration::from_secs(4)),
            ),
        )
        .await;
        holder.abort();
        outcome
            .expect("the endpoint answered inside its budget")
            .expect("the live address must be reached");
        assert_eq!(order.lock().unwrap().clone(), vec![1]);
    }

    /// A host that accepts and never answers holds a connect for the whole
    /// create budget unless each endpoint gets its own slice of it.
    #[tokio::test]
    async fn a_silent_first_endpoint_leaves_budget_for_the_second() {
        let hole = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent = hole.local_addr().unwrap().to_string();
        let holder = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = hole.accept().await {
                held.push(sock);
            }
        });
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let live = spawn_mock_endpoint(1, 1, Arc::clone(&order)).await;

        let mut mgr = manager_over(vec![silent, live]);
        mgr.create_timeout = Some(Duration::from_secs(4));
        let started = tokio::time::Instant::now();
        mgr.create()
            .await
            .expect("the live endpoint must still be reached");

        assert!(
            started.elapsed() < Duration::from_secs(4),
            "failover finished inside the create budget, took {:?}",
            started.elapsed()
        );
        assert_eq!(order.lock().unwrap().clone(), vec![1]);
        holder.abort();
    }

    /// `(remaining budget, addresses left, limit)`: a budget splits like the
    /// endpoint one, and no budget bounds all but the last address.
    #[test]
    fn attempt_limit_bounds_every_address_but_an_unbudgeted_last() {
        let s = Duration::from_secs;
        for (remaining, left, expected) in [
            (Some(s(4)), 2, Some(s(2))),
            (Some(s(4)), 1, Some(s(4))),
            (None, 3, Some(DEFAULT_CREATE_TIMEOUT)),
            (None, 2, Some(DEFAULT_CREATE_TIMEOUT)),
            (None, 1, None),
        ] {
            assert_eq!(
                attempt_limit(remaining, left),
                expected,
                "{remaining:?} over {left}"
            );
        }
    }

    #[test]
    fn endpoint_slice_divides_the_budget_with_a_floor_and_a_cap() {
        let s = Duration::from_secs;
        let ms = Duration::from_millis;
        for (remaining, left, expected) in [
            (s(10), 1, s(10)),
            (s(10), 2, s(5)),
            (s(10), 3, s(10) / 3),
            // The floor keeps a long list from starving each endpoint.
            (s(10), 20, s(1)),
            // The cap keeps the floor from outrunning the budget.
            (ms(400), 3, ms(400)),
        ] {
            assert_eq!(
                endpoint_slice(remaining, left),
                expected,
                "{remaining:?} over {left}"
            );
        }
    }

    /// A name that will not resolve fails BEFORE the connect, on a
    /// different branch from a refused port, and the comment there claims
    /// one bad host must not sink the pass. That claim was never tested.
    #[tokio::test]
    async fn create_fails_over_past_an_unresolvable_endpoint() {
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let accepting = spawn_mock_endpoint(1, 1, Arc::clone(&order)).await;

        // `.invalid` is reserved by RFC 2606 and never resolves.
        let mgr = manager_over(vec!["no-such-host.invalid:9000".to_string(), accepting]);
        mgr.create()
            .await
            .expect("an unresolvable endpoint must be skipped, not fatal");

        assert_eq!(
            order.lock().unwrap().clone(),
            vec![1],
            "create should have reached the accepting endpoint"
        );
    }

    /// Every endpoint unresolvable surfaces the resolver's error rather
    /// than the empty-list fallback, so the message names the host.
    #[tokio::test]
    async fn create_surfaces_the_resolver_error_when_no_endpoint_resolves() {
        let mgr = manager_over(vec![
            "no-such-host-a.invalid:9000".to_string(),
            "no-such-host-b.invalid:9000".to_string(),
        ]);
        let Err(err) = mgr.create().await else {
            panic!("no endpoint resolves; create must not connect")
        };
        let msg = err.to_string();
        assert!(
            msg.contains("no-such-host-b.invalid"),
            "expected the last endpoint's resolver error, got: {msg}"
        );
    }

    /// The empty-endpoint arm is unreachable through `build_pool`, which
    /// asserts a non-empty list, but `create` still has to answer rather
    /// than panic on an `unwrap` of `last_err`.
    #[tokio::test]
    async fn create_over_no_endpoints_errors_rather_than_panicking() {
        let mgr = manager_over(Vec::new());
        let Err(err) = mgr.create().await else {
            panic!("an empty endpoint list cannot produce a connection")
        };
        assert!(err.to_string().contains("no endpoints configured"), "{err}");
    }

    /// When every endpoint refuses, a single `create` pass tries them
    /// all and surfaces the last connect error (typed) -- it does not
    /// hang or panic.
    #[tokio::test]
    async fn create_surfaces_error_when_all_endpoints_refuse() {
        let mut refusing = Vec::new();
        for _ in 0..2 {
            let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
            refusing.push(dead.local_addr().unwrap().to_string());
            drop(dead);
        }
        let mgr = manager_over(refusing);
        // `ConnectionHandle` is not `Debug`, so avoid `expect_err`;
        // match the Result directly.
        // let-else, not `expect_err`: `ConnectionHandle` is not `Debug`.
        let Err(err) = mgr.create().await else {
            panic!("all endpoints refuse; create must not connect")
        };
        // A refused connect round-trips io::Error -> Error::Other; the
        // classifier must treat it as a retriable transport failure.
        assert!(
            crate::tcp::retry::is_retriable_transport(&err),
            "an all-endpoints-refused connect error should be retriable, got {err:?}"
        );
    }

    /// Fail-closed: a TLS pool whose trust was configured but failed to
    /// resolve (`TcpTls::ConfiguredButFailed`) BUILDS (so a transient
    /// unresolved state mid-builder-chain does not panic, and we never
    /// silently downgrade the transport), but its connect kind is
    /// `TlsFailClosed` so every connection attempt refuses rather than
    /// falling back to default native+webpki anchors.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn tls_pool_fails_closed_when_configured_trust_unresolved() {
        let pool = build_pool(
            vec!["127.0.0.1:9440".to_string()],
            ConnectKindConfig::Tls {
                server_name: Some("example.invalid".to_string()),
            },
            HandshakeConfig::default(),
            PoolConfig::default(),
            TcpTls::ConfiguredButFailed,
        )
        .expect("configured-but-failed trust still BUILDS a (fail-closed) pool");
        // Connecting must refuse -- fail closed, no default-trust fallback.
        let Err(err) = pool.get().await else {
            panic!("a fail-closed TLS pool must refuse to connect")
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("refusing to connect with default trust"),
            "expected fail-closed connect error, got: {msg}"
        );
    }
}
