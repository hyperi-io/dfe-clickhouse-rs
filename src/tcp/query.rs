// Project:   dfe-clickhouse
// File:      src/tcp/query.rs
// Purpose:   Query builder over the TCP transport
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`TcpQuery`] -- the `sql` -> `execute` / `fetch_blocks` builder over
//! [`TcpClient`].

use crate::error::Result;
use crate::native::DecodedBlock;
use crate::tcp::client::TcpClient;

/// A statement staged against a [`TcpClient`].
///
/// Shaped like upstream's `Query` so the unified client can offer one
/// call shape over both transports.
#[non_exhaustive]
pub struct TcpQuery<'a> {
    client: &'a TcpClient,
    sql: String,
    query_id: String,
    /// Server-side query parameters, sent in the Query packet's
    /// parameters section rather than interpolated into the SQL.
    params: Vec<(String, String)>,
}

impl<'a> TcpQuery<'a> {
    pub(crate) fn new(client: &'a TcpClient, sql: &str) -> Self {
        Self {
            client,
            sql: sql.to_owned(),
            query_id: String::new(),
            params: Vec::new(),
        }
    }

    /// Set the `query_id` the server records in `system.query_log` and
    /// matches on in `KILL QUERY`. Empty (the default) lets the server
    /// assign one.
    #[must_use]
    pub fn with_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.query_id = query_id.into();
        self
    }

    /// Bind a server-side parameter that the SQL references as
    /// `{name:Type}`, e.g. `{database:String}`.
    ///
    /// The value travels in the Query packet's own parameters section,
    /// never interpolated into the SQL, so it cannot change the
    /// statement's shape.
    ///
    /// `value` is a ClickHouse **Field dump**, which is what the server
    /// restores the parameters section with (`Field::restoreFromDump`,
    /// `src/Core/Field.cpp:705`): a quoted literal for a string, and
    /// likewise for a number, because the server casts the string to the
    /// type the placeholder declares. `NULL` binds a null. A bare `41`
    /// is refused with `CANNOT_RESTORE_FROM_FIELD_DUMP` (536); the
    /// prefixed forms (`UInt64_41`, `Float64_1.5`) are accepted too.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use dfe_clickhouse::TcpClient;
    /// # async fn run(client: &TcpClient) -> dfe_clickhouse::Result<()> {
    /// let blocks = client
    ///     .query("SELECT name FROM system.columns WHERE database = {db:String}")
    ///     .param("db", "'default'")
    ///     .fetch_blocks()
    ///     .await?;
    /// # let _ = blocks;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.params.push((name.into(), value.into()));
        self
    }

    /// Run a statement that streams no rows -- DDL, `SET`, a mutation.
    ///
    /// Never auto-retried: an arbitrary statement is not safe to replay.
    ///
    /// # Errors
    ///
    /// [`crate::Error::ServerException`] if the server rejected the
    /// statement, or a transport error if the connection failed.
    pub async fn execute(self) -> Result<()> {
        self.client
            .execute_query(&self.query_id, &self.sql, &self.params, false)
            .await
    }

    /// Run the query and collect every block the server sends, header
    /// block included. Values come out via
    /// [`DecodedBlock::column_as`], which yields nothing for the header
    /// block. A `WITH TOTALS` or `extremes` query adds blocks whose
    /// [`DecodedBlock::kind`] is not `Data`, so the result set is the
    /// flat-map across the `Data` blocks.
    ///
    /// # Errors
    ///
    /// [`crate::Error::ServerException`] if the server rejected the
    /// query, or a transport error if the stream broke mid-result.
    pub async fn fetch_blocks(self) -> Result<Vec<DecodedBlock>> {
        let mut cursor = self
            .client
            .execute_stream(&self.query_id, &self.sql, &self.params)
            .await?;
        let mut blocks = Vec::new();
        while let Some(block) = cursor.next_block().await? {
            blocks.push(block);
        }
        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use crate::native::io::ClickHouseWrite;
    use crate::tcp::client::TcpClient;
    use crate::tcp::mock::{serve_one_handshake, write_schema_block, write_uint64_payload_block};
    use crate::tcp::protocol::ServerPacketId;

    /// Full path: pool dial + handshake, Query packet, header block,
    /// payload block, EndOfStream, then typed reads by column name.
    #[tokio::test]
    async fn fetch_blocks_round_trips_a_select() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            serve_one_handshake(&mut sock).await;
            // The client's Query packet stays in the kernel buffer; the
            // server does not have to read it to reply.
            write_schema_block(&mut sock, &[("n", "UInt64")]).await;
            write_uint64_payload_block(&mut sock, &[10, 20, 30]).await;
            sock.write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            sock.flush().await.unwrap();
            sock
        });

        let client = TcpClient::new(addr);
        let blocks = client
            .query("SELECT number AS n FROM numbers(3)")
            .with_query_id("q_round_trip")
            .fetch_blocks()
            .await
            .expect("fetch_blocks should drain to EndOfStream");

        assert_eq!(blocks.len(), 2, "header block then one payload block");
        assert_eq!(blocks[0].num_rows, 0);
        assert!(
            blocks[0].column("n").is_none(),
            "the header block declares 'n' but carries no values"
        );

        let values: Vec<u64> = blocks
            .iter()
            .flat_map(|b| b.column_as::<u64>("n").expect("n reads as UInt64"))
            .collect();
        assert_eq!(values, vec![10, 20, 30]);

        let _sock = server.await.unwrap();
    }
}
