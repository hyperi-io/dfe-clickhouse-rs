//! Standalone [`TcpClient`] over the ClickHouse native protocol.
//!
//! `TcpClient` owns its own endpoint list, credentials, settings, roles
//! and TLS trust rather than borrowing them from `clickhouse::Client`,
//! whose equivalent fields are private and have no public readers.
//!
//! # Surface limits
//!
//! - No JWT / access-token arm -- the TCP protocol has no JWT auth path.
//! - No shared HTTP transport -- a consumer wanting both keeps a
//!   `clickhouse::Client` alongside a `TcpClient`.
//! - Reads are column-typed, not row-typed: RowBinary bytes in,
//!   `DecodedBlock` out, values by column name via
//!   [`crate::native::FromColumn`].

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::error::Result;
use crate::tcp::client_ext::{
    self, TcpInsertSession, execute_query_via_pool, execute_stream_via_pool, insert_native_via_pool,
};
use crate::tcp::cursor::TcpRawCursor;
use crate::tcp::pool::{NativePool, TcpClientConfig, build_pool};
use crate::tcp::query::TcpQuery;
use crate::tcp::retry::RetryPolicy;

/// Setting names ClickHouse itself defines, as protocol-level string
/// literals.
mod settings {
    pub(super) const DATABASE: &str = "database";
    pub(super) const ROLE: &str = "role";
    /// Makes the server emit JSON columns as String in Native output, which
    /// reads every JSON column the path-based serialisation refuses.
    pub(super) const JSON_AS_STRING: &str = "output_format_native_write_json_as_string";
}

/// A ClickHouse client that speaks ONLY the native TCP protocol
/// (port 9000).
///
/// Owns its endpoint list, credentials, per-session settings, role set,
/// retry policy and TLS trust. Cloning shares the pool once it exists.
///
/// The pool is built on first use and discarded by any `with_*` builder
/// that changes a handshake or pool input, so a builder chain of N calls
/// builds one pool rather than N.
#[derive(Clone)]
#[non_exhaustive]
pub struct TcpClient {
    config: TcpClientConfig,
    /// Session settings sent in each Query packet (not handshake).
    settings: Vec<(String, String)>,
    /// `SET ROLE`-equivalent, emitted as repeated `role` settings.
    roles: Vec<String>,
    /// Send [`settings::JSON_AS_STRING`] with every query.
    json_as_string: bool,
    /// Explicit TLS trust. `None` means "not configured" -- the pool's
    /// TLS arm then resolves the default native+webpki anchors.
    #[cfg(feature = "tls")]
    tls: Option<crate::tls::TlsConfigSource>,
    /// Per-query settings, rebuilt by the builders that feed them and
    /// shared by `Arc` clone into every dispatch.
    query_settings: Arc<[(String, String)]>,
    /// Built on first use; cleared by any builder that changes an input.
    pool: OnceLock<Arc<NativePool>>,
}

impl TcpClient {
    /// Open a plain-TCP client against a single `host:port` endpoint.
    #[must_use]
    pub fn new(addr: impl Into<String>) -> Self {
        let mut this = Self {
            config: TcpClientConfig {
                endpoints: vec![addr.into()],
                ..TcpClientConfig::default()
            },
            settings: Vec::new(),
            roles: Vec::new(),
            json_as_string: true,
            #[cfg(feature = "tls")]
            tls: None,
            query_settings: Arc::from([]),
            pool: OnceLock::new(),
        };
        this.refresh_query_settings();
        this
    }

    /// Open a TLS-over-TCP client. `server_name` is the SNI and the name the
    /// certificate is verified against.
    ///
    /// When `server_name` is `addr`'s own host, every endpoint is verified
    /// against its own host, those [`Self::with_addrs`] adds included, so
    /// failing over to a second host verifies that host's certificate. Any
    /// other name is kept for every endpoint, for a caller connecting by
    /// address to a certificate issued to a name.
    #[cfg(feature = "tls")]
    #[must_use]
    pub fn new_tls(addr: impl Into<String>, server_name: impl Into<String>) -> Self {
        let addr = addr.into();
        let server_name = server_name.into();
        let host = crate::tcp::connect::endpoint_host(&addr);
        let explicit = (!server_name.eq_ignore_ascii_case(host)).then_some(server_name);
        let mut this = Self::new(addr);
        this.config.kind = crate::tcp::pool::ConnectKindConfig::Tls {
            server_name: explicit,
        };
        this.discard_pool();
        this
    }

    /// Replace the endpoint list. Round-robin failover across it happens
    /// inside the pool manager's `create`.
    ///
    /// # Panics
    ///
    /// If `addrs` yields no endpoints.
    #[must_use]
    pub fn with_addrs(mut self, addrs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let endpoints: Vec<String> = addrs.into_iter().map(Into::into).collect();
        assert!(
            !endpoints.is_empty(),
            "with_addrs requires at least one endpoint"
        );
        self.config.endpoints = endpoints;
        self.discard_pool();
        self
    }

    /// Handshake database, and the `database` session setting for
    /// queries and INSERTs.
    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.config.handshake.database = database.into();
        self.refresh_query_settings();
        self.discard_pool();
        self
    }

    /// Handshake user.
    #[must_use]
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.config.handshake.user = user.into();
        self.discard_pool();
        self
    }

    /// Handshake password.
    #[must_use]
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.config.handshake.password = password.into();
        self.discard_pool();
        self
    }

    /// Quota key emitted in the post-Hello addendum and in every Query
    /// packet's `ClientInfo`.
    #[must_use]
    pub fn with_quota_key(mut self, quota_key: impl Into<String>) -> Self {
        self.config.handshake.quota_key = quota_key.into();
        self.discard_pool();
        self
    }

    /// Add one per-session setting, sent in every Query packet.
    #[must_use]
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.push((name.into(), value.into()));
        self.refresh_query_settings();
        self
    }

    /// Ask the server to send JSON columns as String, which is the default.
    /// Turning it off yields the path-based JSON serialisation, rebuilt into
    /// nested documents printed as the server prints them. A column it cannot
    /// print that way (typed paths, dates, arrays of objects, values past
    /// `max_dynamic_paths` or `max_dynamic_types`) fails the query with
    /// [`crate::Error::Unsupported`] naming the column.
    /// `docs/type-support.md` lists each case.
    #[must_use]
    pub fn with_json_as_string(mut self, on: bool) -> Self {
        self.json_as_string = on;
        self.refresh_query_settings();
        self
    }

    /// Replace the role set. Emitted as repeated `role` settings.
    #[must_use]
    pub fn with_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.roles = roles.into_iter().map(Into::into).collect();
        self.refresh_query_settings();
        self
    }

    /// Bounded-backoff retry for idempotent operations.
    #[must_use]
    pub fn with_retry(mut self, retry: impl Into<Option<RetryPolicy>>) -> Self {
        self.config.retry = retry.into();
        self
    }

    /// Maximum pooled connections.
    #[must_use]
    pub fn with_pool_size(mut self, n: usize) -> Self {
        self.config.pool.max_size = n;
        self.discard_pool();
        self
    }

    /// How long a caller may wait for a free pool slot.
    #[must_use]
    pub fn with_pool_acquire_timeout(mut self, d: Option<Duration>) -> Self {
        self.config.pool.acquire_timeout = d;
        self.discard_pool();
        self
    }

    /// How long `Manager::create` (connect + handshake) may take in total,
    /// split across the configured endpoints so a silent one cannot use the
    /// whole budget.
    #[must_use]
    pub fn with_pool_create_timeout(mut self, d: Option<Duration>) -> Self {
        self.config.pool.create_timeout = d;
        self.discard_pool();
        self
    }

    /// Maximum age of a pooled connection before recycle drops it.
    #[must_use]
    pub fn with_pool_max_lifetime(mut self, d: Option<Duration>) -> Self {
        self.config.pool.max_lifetime = d;
        self.discard_pool();
        self
    }

    /// Per-operation I/O deadline threaded into every spawned actor: the gap
    /// between packets on a read, and the whole of an INSERT block or finish
    /// write. A timeout poisons the connection and returns
    /// [`crate::Error::TimedOut`].
    #[must_use]
    pub fn with_read_timeout(mut self, d: Option<Duration>) -> Self {
        self.config.pool.read_timeout = d;
        self.discard_pool();
        self
    }

    /// Supply an already-built rustls config (Go's `Options.TLS` analog).
    #[cfg(feature = "tls")]
    #[must_use]
    pub fn with_tls_config(mut self, cfg: Arc<rustls::ClientConfig>) -> Self {
        self.tls = Some(crate::tls::TlsConfigSource::Explicit(cfg));
        self.discard_pool();
        self
    }

    /// Supply a declarative trust description, resolved at pool build.
    #[cfg(feature = "tls")]
    #[must_use]
    pub fn with_tls_trust(mut self, trust: crate::tls::TlsTrust) -> Self {
        self.tls = Some(crate::tls::TlsConfigSource::Trust(trust));
        self.discard_pool();
        self
    }

    /// The underlying pool, exposed so callers can read deadpool's
    /// `status()` for metrics. Builds it if this is the first use.
    ///
    /// # Panics
    ///
    /// If the pool cannot be built. `Runtime::Tokio1` is always supplied,
    /// so deadpool's only documented build failure is unreachable.
    pub fn pool(&self) -> &Arc<NativePool> {
        self.pool.get_or_init(|| {
            #[cfg(feature = "tls")]
            let tls = match &self.tls {
                None => crate::tcp::pool::TcpTls::NotConfigured,
                Some(src) => match crate::tls::build_client_config(src) {
                    Ok(cfg) => crate::tcp::pool::TcpTls::Resolved(cfg),
                    // Fail closed: a configured-but-unresolvable trust must
                    // never fall back to broad default anchors.
                    Err(_) => crate::tcp::pool::TcpTls::ConfiguredButFailed,
                },
            };
            // deadpool's only documented build failure is NoRuntimeSpecified,
            // which `build_pool` rules out by always supplying Runtime::Tokio1.
            #[allow(clippy::expect_used)]
            Arc::new(
                build_pool(
                    self.config.endpoints.clone(),
                    self.config.kind.clone(),
                    self.config.handshake.clone(),
                    self.config.pool,
                    #[cfg(feature = "tls")]
                    tls,
                )
                .expect("tcp: pool build failed despite Runtime::Tokio1 being set"),
            )
        })
    }

    /// The configured retry policy, if any.
    pub fn retry(&self) -> Option<RetryPolicy> {
        self.config.retry
    }

    /// Per-query / per-INSERT settings: database, the JSON-as-String flag,
    /// plain settings, roles. Cloning the returned `Arc` is `O(1)`.
    pub fn insert_settings(&self) -> Arc<[(String, String)]> {
        Arc::clone(&self.query_settings)
    }

    /// Round-trip a Ping over a pooled connection.
    ///
    /// # Errors
    ///
    /// A pool-acquire failure, or any error from the Ping round trip.
    ///
    /// # Panics
    ///
    /// See [`Self::pool`].
    pub async fn ping(&self) -> Result<()> {
        let conn = self
            .pool()
            .get()
            .await
            .map_err(crate::tcp::retry::map_pool_error)?;
        conn.ping().await
    }

    /// Stage `sql` for [`TcpQuery::execute`] or
    /// [`TcpQuery::fetch_blocks`].
    pub fn query(&self, sql: &str) -> TcpQuery<'_> {
        TcpQuery::new(self, sql)
    }

    /// Run a statement that does not stream rows (DDL, `SET`,
    /// `INSERT ... VALUES`, a mutation). Auto-retry is opt-in via
    /// `idempotent` -- see [`execute_query_via_pool`].
    ///
    /// # Errors
    ///
    /// A pool-acquire failure, [`crate::Error::ServerException`] if the
    /// server rejected the statement, or a transport error.
    ///
    /// # Panics
    ///
    /// See [`Self::pool`].
    pub async fn execute_query(
        &self,
        query_id: &str,
        query: &str,
        params: &[(String, String)],
        idempotent: bool,
    ) -> Result<()> {
        execute_query_via_pool(
            self.pool(),
            query_id,
            query,
            &self.query_settings,
            params,
            self.config.retry,
            idempotent,
        )
        .await
    }

    /// Open a streaming SELECT, yielding decoded Native blocks.
    ///
    /// Row-typed `fetch::<T>()` needs upstream's `pub(crate)` RowBinary
    /// deserialiser and is not available yet; read columns by name off
    /// each [`crate::native::DecodedBlock`] instead.
    ///
    /// # Errors
    ///
    /// A pool-acquire failure, or any error from issuing the query.
    ///
    /// # Panics
    ///
    /// See [`Self::pool`].
    pub async fn execute_stream(
        &self,
        query_id: &str,
        query: &str,
        params: &[(String, String)],
    ) -> Result<TcpRawCursor> {
        execute_stream_via_pool(
            self.pool(),
            query_id,
            query,
            &self.query_settings,
            params,
            self.config.retry,
        )
        .await
    }

    /// Open an INSERT session. `sql` is the full statement, typically
    /// `INSERT INTO <table> (...) FORMAT Native`.
    ///
    /// Blocks are supplied as bytes already encoded by
    /// [`crate::native::encode_columns`].
    ///
    /// # Errors
    ///
    /// A pool-acquire failure, [`crate::Error::ServerException`] if the
    /// server rejected the INSERT, or a transport error.
    ///
    /// # Panics
    ///
    /// See [`Self::pool`].
    pub async fn insert_native(&self, query_id: &str, sql: &str) -> Result<TcpInsertSession> {
        insert_native_via_pool(self.pool(), query_id, sql, &self.query_settings).await
    }

    /// Default rows per Native block on the insert path.
    pub const DEFAULT_INSERT_BLOCK_ROWS: u64 = client_ext::DEFAULT_TCP_INSERT_BLOCK_ROWS;

    /// Recompute the per-query settings after a builder changed one of
    /// their inputs.
    fn refresh_query_settings(&mut self) {
        let mut out = Vec::with_capacity(2 + self.settings.len() + self.roles.len());
        out.push((
            settings::DATABASE.to_string(),
            self.config.handshake.database.clone(),
        ));
        if self.json_as_string {
            out.push((settings::JSON_AS_STRING.to_string(), "1".to_string()));
        }
        out.extend(self.settings.iter().cloned());
        for role in &self.roles {
            out.push((settings::ROLE.to_string(), role.clone()));
        }
        self.query_settings = Arc::from(out);
    }

    /// Drop any built pool so the next use rebuilds from the current
    /// config.
    fn discard_pool(&mut self) {
        self.pool = OnceLock::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADDR: &str = "127.0.0.1:9000";

    /// Each of these writes exactly one config field and nothing reads it
    /// back, so a builder assigning the wrong one would go unnoticed.
    #[test]
    fn every_builder_writes_the_field_it_names() {
        let client = TcpClient::new(ADDR)
            .with_user("reader")
            .with_password("pw")
            .with_quota_key("q1")
            .with_pool_create_timeout(Some(Duration::from_secs(7)))
            .with_pool_max_lifetime(Some(Duration::from_secs(11)))
            .with_pool_acquire_timeout(Some(Duration::from_secs(13)));

        assert_eq!(client.config.handshake.user, "reader");
        assert_eq!(client.config.handshake.password, "pw");
        assert_eq!(client.config.handshake.quota_key, "q1");
        assert_eq!(
            client.config.pool.create_timeout,
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            client.config.pool.max_lifetime,
            Some(Duration::from_secs(11))
        );
        assert_eq!(
            client.config.pool.acquire_timeout,
            Some(Duration::from_secs(13))
        );
        // Untouched by the chain above.
        assert_eq!(client.config.handshake.database, "default");
    }

    #[test]
    #[should_panic(expected = "with_addrs requires at least one endpoint")]
    fn with_addrs_rejects_an_empty_endpoint_list() {
        let _ = TcpClient::new(ADDR).with_addrs(Vec::<String>::new());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tls_builders_record_the_source_they_were_given() {
        use crate::tls::{TlsConfigSource, TlsTrust};

        assert!(TcpClient::new(ADDR).tls.is_none(), "unset until asked for");

        let trust = TcpClient::new(ADDR).with_tls_trust(TlsTrust {
            exclusive: true,
            ..TlsTrust::default()
        });
        match &trust.tls {
            Some(TlsConfigSource::Trust(t)) => assert!(t.exclusive),
            other => panic!("with_tls_trust must store a Trust source, got {other:?}"),
        }
    }

    /// `(addr, server_name, name every endpoint is held to)`: the address's own
    /// host is the per-endpoint default, and any other name is an override.
    #[cfg(feature = "tls")]
    #[test]
    fn new_tls_keeps_only_a_name_that_differs_from_the_host() {
        use crate::tcp::pool::ConnectKindConfig;

        for (addr, name, held) in [
            ("ch-1.example:9440", "ch-1.example", None),
            ("CH-1.example:9440", "ch-1.example", None),
            ("[::1]:9440", "::1", None),
            ("10.0.0.5:9440", "ch.example", Some("ch.example")),
            ("ch-1.example:9440", "ch.example", Some("ch.example")),
        ] {
            let client = TcpClient::new_tls(addr, name);
            match &client.config.kind {
                ConnectKindConfig::Tls { server_name } => {
                    assert_eq!(server_name.as_deref(), held, "{addr} as {name}");
                }
                ConnectKindConfig::Plain => panic!("new_tls must select TLS"),
            }
        }
    }

    /// The pool is built on first use and shared by clones from then on;
    /// `with_*` builders that change a handshake or pool input discard it,
    /// and the per-query ones must not.
    #[test]
    fn the_pool_is_built_once_and_only_discarded_by_its_own_inputs() {
        let client = TcpClient::new(ADDR);
        let first = Arc::clone(client.pool());
        assert!(
            Arc::ptr_eq(&first, client.pool()),
            "a second call must not rebuild"
        );

        let shared = client.clone();
        assert!(
            Arc::ptr_eq(&first, shared.pool()),
            "a clone taken after the build shares the pool"
        );

        let per_query = shared.with_setting("max_execution_time", "30");
        assert!(
            Arc::ptr_eq(&first, per_query.pool()),
            "a session setting rides on the Query packet and needs no rebuild"
        );

        let rebuilt = per_query.with_database("other");
        assert!(
            !Arc::ptr_eq(&first, rebuilt.pool()),
            "the database is a handshake input, so the pool must be rebuilt"
        );
    }
}
