// Project:   dfe-clickhouse
// File:      src/unified.rs
// Purpose:   One client dispatching over HTTP and TCP transports
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One client over both transports. Reads land in [`Columns`] whichever
//! transport answered.
//!
//! Both arms decode with this crate's own Native codec, so a type reads the
//! same way on either -- `JSON`, `Variant` and `Dynamic` included, which
//! upstream's Native reader rejects on the declared type name. The HTTP arm
//! gets there by asking the server for the same wire shape the TCP handshake
//! negotiates; see [`UnifiedClient::fetch_columns`].

use clickhouse::Client;

use crate::error::{Error, Result};
use crate::native::decode::decode_block;
use crate::native::io::ClickHouseRead;
use crate::native::{DecodedBlock, FromColumn};
use crate::tcp::TcpClient;
use crate::tcp::protocol::DBMS_TCP_PROTOCOL_VERSION;

/// Sent on the HTTP arm to match the [`TcpClient`] default.
const JSON_AS_STRING: &str = "output_format_native_write_json_as_string";

/// URL parameter, not a setting: `HTTPHandler.cpp:318-321` reads it off the
/// query string and calls `setClientProtocolVersion`, which is what makes
/// `NativeWriter` emit block info, the per-column custom-serialization flag
/// and the V2 JSON/Dynamic serialisation (`NativeWriter.cpp:81-90,:100,:125`).
/// Left unset the server writes revision 0 -- a different wire shape from the
/// one the TCP transport negotiates, and one this decoder does not read.
const CLIENT_PROTOCOL_VERSION: &str = "client_protocol_version";

/// Which wire protocol a [`UnifiedClient`] answers on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Transport {
    /// Upstream's HTTP client.
    Http,
    /// This crate's native TCP client.
    Tcp,
}

/// A ClickHouse client over either transport, cloned as cheaply as the inner
/// client. Configure that inner client with its own builder.
#[derive(Clone)]
#[non_exhaustive]
pub enum UnifiedClient {
    /// Speaks HTTP through upstream's client.
    Http(Client),
    /// Speaks the native protocol through this crate's pooled TCP client.
    Tcp(TcpClient),
}

impl UnifiedClient {
    /// Which transport this client answers on.
    #[must_use]
    pub fn transport(&self) -> Transport {
        match self {
            Self::Http(_) => Transport::Http,
            Self::Tcp(_) => Transport::Tcp,
        }
    }

    /// The inner HTTP client, for the operations only HTTP has -- notably
    /// `insert_formatted_with`, since the native protocol accepts no format
    /// but Native. `None` on the TCP arm.
    #[must_use]
    pub fn as_http(&self) -> Option<&Client> {
        match self {
            Self::Http(client) => Some(client),
            Self::Tcp(_) => None,
        }
    }

    /// The inner TCP client, for the operations only the native protocol has.
    /// `None` on the HTTP arm.
    #[must_use]
    pub fn as_tcp(&self) -> Option<&TcpClient> {
        match self {
            Self::Tcp(client) => Some(client),
            Self::Http(_) => None,
        }
    }

    /// Run a statement that returns no rows. `sql` goes over the wire verbatim
    /// -- no `?` binding, no `?fields`.
    ///
    /// # Errors
    ///
    /// Whatever the transport returns: a connection failure, or the server's
    /// own rejection of the statement.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        match self {
            Self::Http(client) => Ok(client.query_raw(sql).execute().await?),
            Self::Tcp(client) => client.query(sql).execute().await,
        }
    }

    /// Round-trip the server.
    ///
    /// # Errors
    ///
    /// A transport failure, or a credential the server rejects.
    pub async fn ping(&self) -> Result<()> {
        match self {
            // HTTP has no ping frame.
            Self::Http(client) => Ok(client.query_raw("SELECT 1").execute().await?),
            Self::Tcp(client) => client.ping().await,
        }
    }

    /// Run `sql` -- verbatim, as for [`Self::execute`] -- and collect the whole
    /// result set column-wise.
    ///
    /// # Errors
    ///
    /// A server rejection, or a column the block decoder does not carry.
    pub async fn fetch_columns(&self, sql: &str) -> Result<Columns> {
        let blocks = match self {
            Self::Http(client) => {
                let cursor = client
                    .query_raw(sql)
                    .with_setting(JSON_AS_STRING, "1")
                    .with_setting(
                        CLIENT_PROTOCOL_VERSION,
                        DBMS_TCP_PROTOCOL_VERSION.to_string(),
                    )
                    .fetch_bytes("Native")?;
                decode_native_stream(cursor).await?
            }
            Self::Tcp(client) => client.query(sql).fetch_blocks().await?,
        };
        Ok(Columns { blocks })
    }

    /// Open a dynamic insert, schema from `cache` else `system.columns`. TCP
    /// resolves it here because `DynamicInsert::tcp` needs it up front.
    ///
    /// # Errors
    ///
    /// TCP only: [`crate::Error::Dynamic`] if the schema query fails or the
    /// table has no columns. Both are retriable once the cache is cold.
    #[cfg(feature = "dynamic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "dynamic")))]
    pub async fn dynamic_insert(
        &self,
        database: &str,
        table: &str,
        cache: std::sync::Arc<crate::dynamic::DynamicSchemaCache>,
    ) -> Result<crate::dynamic::DynamicInsert> {
        use crate::dynamic::{DynamicInsert, fetch_dynamic_schema_tcp};

        // Two arms, one of which returns: `if let` would have to name the
        // other arm's binding a second time to reach the TCP client.
        #[allow(clippy::single_match_else)]
        let client = match self {
            Self::Http(client) => {
                return Ok(DynamicInsert::http(client.clone(), database, table, cache));
            }
            Self::Tcp(client) => client,
        };

        let full = format!("{database}.{table}");
        let schema = if let Some(cached) = cache.get(&full) {
            cached
        } else {
            // Boxed: a cold read only, and inline it would size every caller's
            // future to match.
            let fetched = Box::pin(fetch_dynamic_schema_tcp(client, database, table, None)).await?;
            cache.insert(&full, std::sync::Arc::clone(&fetched));
            fetched
        };
        Ok(DynamicInsert::tcp(client.clone(), database, table, schema).with_schema_cache(cache))
    }
}

/// A whole result set, column-oriented. Blocks are held as the server sent
/// them and flattened on read.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Columns {
    blocks: Vec<DecodedBlock>,
}

impl Columns {
    /// Every value of column `name`, in server order, across every block.
    ///
    /// # Errors
    ///
    /// [`crate::Error::SchemaMismatch`] if a block that declares columns does
    /// not declare `name`, or if the column does not read as `T`.
    pub fn get<T: FromColumn>(&self, name: &str) -> Result<Vec<T>> {
        let mut values = Vec::with_capacity(self.rows());
        for block in &self.blocks {
            values.extend(block.column_as::<T>(name)?);
        }
        Ok(values)
    }

    /// Rows across every block; schema-only and empty blocks count nothing.
    ///
    /// A block's declared count is saturated into `usize`, which only bites on
    /// a 32-bit target that could not have held the block anyway.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.blocks
            .iter()
            .map(|b| usize::try_from(b.num_rows).unwrap_or(usize::MAX))
            .sum()
    }

    /// Whether the result set carries no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows() == 0
    }

    /// The blocks as the server sent them.
    ///
    /// [`Self::get`] flattens these, which is what most callers want. Reach
    /// for the blocks when the split itself matters -- a column type
    /// [`FromColumn`] has no route for, or per-block accounting.
    #[must_use]
    pub fn blocks(&self) -> &[DecodedBlock] {
        &self.blocks
    }
}

/// Decode a `FORMAT Native` body: block info, the column and row counts, then
/// the block, repeated until the stream ends.
///
/// The same [`decode_block`] the TCP transport uses, at the same revision, so
/// a type that reads on one transport reads on the other. There is no
/// terminator in the format -- the body simply stops -- so the end is a read
/// that returns no bytes where the next block's first byte would be.
async fn decode_native_stream<R: ClickHouseRead>(mut r: R) -> Result<Vec<DecodedBlock>> {
    use crate::native::io::read_var_uint_or_eof;

    let mut blocks = Vec::new();
    loop {
        // The block info's first field id doubles as the end probe: the
        // server writes the info section for every block at a non-zero
        // client revision, which is what this reader asks for.
        let Some(field1) = read_var_uint_or_eof(&mut r).await? else {
            return Ok(blocks);
        };
        if field1 != 1 {
            return Err(Error::BadResponse(format!(
                "native over http: block info field id {field1} (expected 1)"
            )));
        }
        finish_block_info(&mut r).await?;

        let num_columns = r.read_var_uint().await?;
        let num_rows = r.read_var_uint().await?;
        blocks.push(decode_block(&mut r, num_columns, num_rows, DBMS_TCP_PROTOCOL_VERSION).await?);
    }
}

/// The rest of a block info section, after its first field id was read as the
/// end-of-stream probe.
async fn finish_block_info<R: ClickHouseRead>(r: &mut R) -> Result<()> {
    use tokio::io::AsyncReadExt;

    let _is_overflows = r.read_u8().await?;
    let field2 = r.read_var_uint().await?;
    if field2 != 2 {
        return Err(Error::BadResponse(format!(
            "native over http: block info field id {field2} (expected 2)"
        )));
    }
    let _bucket_num = r.read_i32_le().await?;
    let terminator = r.read_var_uint().await?;
    if terminator != 0 {
        return Err(Error::BadResponse(format!(
            "native over http: block info terminator {terminator} (expected 0)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clickhouse::test::{Mock, handlers};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::native::io::{ClickHouseBytesWrite, ClickHouseRead, ClickHouseWrite};
    use crate::tcp::mock::{serve_one_handshake, write_schema_block, write_string_payload_block};
    use crate::tcp::protocol::ServerPacketId;

    const NAMES: [&str; 3] = ["id", "tag", "doc"];
    const TYPES: [&str; 3] = ["UInt64", "String", "JSON"];

    /// Answer the handshake, then run `script` on the pool's one connection.
    async fn scripted<F, Fut>(script: F) -> (UnifiedClient, JoinHandle<TcpStream>)
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: Future<Output = TcpStream> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            serve_one_handshake(&mut sock).await;
            script(sock).await
        });
        (UnifiedClient::Tcp(TcpClient::new(addr)), server)
    }

    async fn serve_two_string_columns(mut sock: TcpStream) -> TcpStream {
        write_schema_block(&mut sock, &[("name", "String"), ("col_type", "String")]).await;
        write_string_payload_block(&mut sock, &[("name", &NAMES), ("col_type", &TYPES)]).await;
        end_of_stream(&mut sock).await;
        sock
    }

    /// Same, plus the zero-column block a real server appends to every result.
    async fn serve_two_string_columns_then_empty(mut sock: TcpStream) -> TcpStream {
        write_schema_block(&mut sock, &[("name", "String"), ("col_type", "String")]).await;
        write_string_payload_block(&mut sock, &[("name", &NAMES), ("col_type", &TYPES)]).await;
        write_schema_block(&mut sock, &[]).await;
        end_of_stream(&mut sock).await;
        sock
    }

    async fn end_of_stream(sock: &mut TcpStream) {
        sock.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();
        sock.flush().await.unwrap();
    }

    /// The same two columns as a `format=Native` HTTP body.
    /// The body a server writes for `FORMAT Native` once the request carries
    /// `client_protocol_version`: block info, the counts, then each column
    /// with its custom-serialization flag. Byte-for-byte what the TCP Data
    /// packet carries after its header, which is the point of the adapter.
    fn native_body() -> Vec<u8> {
        let mut out = Vec::new();
        // Block info: is_overflows, bucket_num, terminator.
        out.put_var_uint(1);
        out.push(0);
        out.put_var_uint(2);
        out.extend_from_slice(&(-1i32).to_le_bytes());
        out.put_var_uint(0);

        out.put_var_uint(2);
        out.put_var_uint(NAMES.len() as u64);
        for (name, values) in [("name", NAMES), ("col_type", TYPES)] {
            out.put_string(name);
            out.put_string("String");
            out.push(0);
            for value in values {
                out.put_string(value);
            }
        }
        out
    }

    /// One RowBinary `u8` per row is one byte, so the body is these bytes.
    fn serve_native_body(mock: &Mock) {
        mock.add(handlers::provide::<u8>(native_body()));
    }

    #[test]
    fn transport_names_the_variant() {
        assert_eq!(
            UnifiedClient::Http(Client::default()).transport(),
            Transport::Http
        );
        assert_eq!(
            UnifiedClient::Tcp(TcpClient::new("127.0.0.1:9000")).transport(),
            Transport::Tcp
        );
    }

    #[tokio::test]
    async fn tcp_execute_runs_a_statement() {
        let (client, server) = scripted(|mut sock: TcpStream| async move {
            end_of_stream(&mut sock).await;
            sock
        })
        .await;

        client
            .execute("CREATE TABLE t (a UInt8) ENGINE = Memory")
            .await
            .expect("the server answered EndOfStream");

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_ping_round_trips() {
        let (client, server) = scripted(|mut sock: TcpStream| async move {
            let _ping_packet_id = sock.read_var_uint().await.unwrap();
            sock.write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            sock.flush().await.unwrap();
            sock
        })
        .await;

        client.ping().await.expect("the server answered Pong");

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_fetch_columns_reads_by_name() {
        let (client, server) = scripted(serve_two_string_columns).await;

        let columns = client.fetch_columns("SELECT name, col_type").await.unwrap();

        assert_eq!(columns.rows(), 3);
        assert!(!columns.is_empty());
        assert_eq!(columns.get::<String>("name").unwrap(), NAMES);
        assert_eq!(columns.get::<String>("col_type").unwrap(), TYPES);

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn http_execute_sends_the_sql_verbatim() {
        let mock = Mock::new();
        let recorded = mock.add(handlers::record_ddl());
        let client = UnifiedClient::Http(Client::default().with_url(mock.url()));

        client.execute("SELECT position('a?b', '?')").await.unwrap();

        assert_eq!(recorded.query().await, "SELECT position('a?b', '?')");
    }

    #[tokio::test]
    async fn http_ping_sends_select_1() {
        let mock = Mock::new();
        let recorded = mock.add(handlers::record_ddl());
        let client = UnifiedClient::Http(Client::default().with_url(mock.url()));

        client.ping().await.unwrap();

        assert_eq!(recorded.query().await, "SELECT 1");
    }

    /// The point of the adapter: one Native payload, two transports, one answer.
    #[tokio::test]
    async fn both_transports_read_the_same_payload_identically() {
        let mock = Mock::new();
        serve_native_body(&mock);
        let http = UnifiedClient::Http(Client::default().with_url(mock.url()));
        let (tcp, server) = scripted(serve_two_string_columns).await;

        let over_http = http.fetch_columns("SELECT name, col_type").await.unwrap();
        let over_tcp = tcp.fetch_columns("SELECT name, col_type").await.unwrap();

        assert_eq!(over_http.rows(), over_tcp.rows());
        assert_eq!(
            over_http.get::<String>("name").unwrap(),
            over_tcp.get::<String>("name").unwrap()
        );
        assert_eq!(
            over_http.get::<Vec<u8>>("col_type").unwrap(),
            over_tcp.get::<Vec<u8>>("col_type").unwrap()
        );

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn an_undeclared_column_is_an_error_on_both_transports() {
        let mock = Mock::new();
        serve_native_body(&mock);
        let http = UnifiedClient::Http(Client::default().with_url(mock.url()));
        let (tcp, server) = scripted(serve_two_string_columns).await;

        let over_http = http.fetch_columns("SELECT name, col_type").await.unwrap();
        let over_tcp = tcp.fetch_columns("SELECT name, col_type").await.unwrap();

        assert!(over_http.get::<String>("missing").is_err());
        assert!(over_tcp.get::<String>("missing").is_err());
        assert!(over_http.get::<u64>("name").is_err());
        assert!(over_tcp.get::<u64>("name").is_err());

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn a_trailing_empty_block_reads_as_no_values() {
        let (client, server) = scripted(serve_two_string_columns_then_empty).await;

        let columns = client.fetch_columns("SELECT name, col_type").await.unwrap();

        assert_eq!(columns.rows(), 3);
        assert_eq!(columns.get::<String>("name").unwrap(), NAMES);

        let _sock = server.await.unwrap();
    }

    /// The server splits a result set into blocks of its own choosing, so a
    /// read has to concatenate them in the order they arrived.
    #[tokio::test]
    async fn get_flattens_values_across_blocks_in_server_order() {
        let (client, server) = scripted(|mut sock: TcpStream| async move {
            write_schema_block(&mut sock, &[("name", "String")]).await;
            write_string_payload_block(&mut sock, &[("name", &["a", "b"])]).await;
            write_string_payload_block(&mut sock, &[("name", &["c"])]).await;
            write_string_payload_block(&mut sock, &[("name", &["d", "e"])]).await;
            end_of_stream(&mut sock).await;
            sock
        })
        .await;

        let columns = client.fetch_columns("SELECT name").await.unwrap();

        assert_eq!(columns.rows(), 5);
        assert_eq!(
            columns.get::<String>("name").unwrap(),
            ["a", "b", "c", "d", "e"]
        );

        let _sock = server.await.unwrap();
    }

    /// A ping is only useful if a server rejection reaches the caller rather
    /// than reading as a healthy round trip.
    #[tokio::test]
    async fn ping_surfaces_a_server_rejection() {
        let mock = Mock::new();
        // 60 = UNKNOWN_TABLE, returned the way the server returns one.
        mock.add(handlers::exception(60));
        let client = UnifiedClient::Http(Client::default().with_url(mock.url()));

        let err = client.ping().await.expect_err("the mock rejects");

        assert!(
            matches!(err, crate::Error::BadResponse(_)),
            "a rejection must not read as success: {err:?}"
        );
    }

    /// The transport-specific escape hatches: each arm hands back its own
    /// client and nothing else.
    #[test]
    fn as_http_and_as_tcp_expose_only_their_own_arm() {
        let http = UnifiedClient::Http(Client::default());
        assert!(http.as_http().is_some());
        assert!(http.as_tcp().is_none());

        let tcp = UnifiedClient::Tcp(TcpClient::new("127.0.0.1:9000"));
        assert!(tcp.as_tcp().is_some());
        assert!(tcp.as_http().is_none());
    }

    #[cfg(feature = "dynamic")]
    async fn serve_system_columns(mut sock: TcpStream) -> TcpStream {
        let header = [
            ("name", "String"),
            ("col_type", "String"),
            ("default_kind", "String"),
        ];
        write_schema_block(&mut sock, &header).await;
        write_string_payload_block(
            &mut sock,
            &[
                ("name", &["id", "tag"]),
                ("col_type", &["UInt64", "String"]),
                ("default_kind", &["", "DEFAULT"]),
            ],
        )
        .await;
        end_of_stream(&mut sock).await;
        sock
    }

    #[cfg(feature = "dynamic")]
    #[tokio::test]
    async fn tcp_dynamic_insert_reads_the_schema_once_then_caches_it() {
        use std::time::Duration;

        let (client, server) = scripted(serve_system_columns).await;
        let cache = crate::dynamic::DynamicSchemaCache::new(Duration::from_secs(300));

        let insert = client
            .dynamic_insert("dfe", "events", cache.clone())
            .await
            .expect("the mock served system.columns");

        let schema = insert.schema().expect("tcp resolves the schema up front");
        assert_eq!(schema.table, "dfe.events");
        assert_eq!(schema.len(), 2);
        assert!(schema.column("id").is_some());
        assert_eq!(schema.optional_columns().count(), 1);

        // The mock is scripted for one query, so a second fetch finds nothing.
        let cached = client
            .dynamic_insert("dfe", "events", cache)
            .await
            .expect("second call is served from the cache");
        assert_eq!(cached.schema().expect("cached schema").len(), 2);

        let _sock = server.await.unwrap();
    }
}
