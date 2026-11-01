// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-clickhouse
// File:      src/dynamic/insert.rs
// Purpose:   DynamicInsert adapter over the clickhouse-rs RowBinary/Native sinks
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Single-table dynamic insert with schema-mismatch recovery.
//!
//! [`DynamicInsert`] encodes each `Map<String, Value>` against a
//! [`DynamicSchema`] and ships the rows through one of two sinks, both fed the
//! same [`DynamicRow::encode_into`] bytes:
//!
//! - HTTP -- `Client::insert_formatted_with(... FORMAT RowBinary)`, written
//!   row-wise. The schema is read from `system.columns` on the first write and
//!   cached.
//! - TCP (`tcp` feature) -- `TcpClient::insert_native(... FORMAT Native)`, with
//!   the rows transposed into columnar blocks by
//!   [`crate::native::encode_columns`]. The schema is supplied by the caller,
//!   which [`crate::unified::UnifiedClient::dynamic_insert`] reads over the
//!   same TCP connection before opening the insert.
//!
//! On a schema-mismatch error the HTTP path invalidates the cached schema so
//! the next insert re-fetches.

use std::sync::Arc;

use serde_json::{Map, Value};

use clickhouse::Client;

use super::encode::{ColumnDef, DynamicRow};
use super::error::DynamicError;
use super::schema::{DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};

#[cfg(feature = "tcp")]
use crate::native::{ColumnSchema, encode_columns};
#[cfg(feature = "tcp")]
use crate::tcp::{TcpClient, TcpInsertSession};

/// The server setting that makes a RowBinary reader take a length-prefixed
/// string as a JSON value, which is how the dynamic encoder writes JSON
/// columns on the wire.
const JSON_AS_STRING_SETTING: &str = "input_format_binary_read_json_as_string";

/// Flush a TCP block once it reaches this many encoded bytes, whichever of
/// bytes or rows comes first. The row cap alone lets a wide row buffer a
/// gigabyte before the first send.
#[cfg(feature = "tcp")]
const BLOCK_FLUSH_BYTES: usize = 1 << 20;

/// Backtick-quote a SQL identifier using upstream's own escaper, so an
/// identifier is quoted the same way here and in `clickhouse::Client`.
///
/// Upstream's `sql::escape` module is `pub(crate)` as of 0.15.2, so `_priv` is
/// the only public route to it.
fn escape_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    let _ = clickhouse::_priv::sql_escape_identifier(name, &mut out);
    out
}

/// The client rows are written through, and where the schema comes from.
enum Backend {
    Http {
        client: Client,
        cache: Arc<DynamicSchemaCache>,
    },
    #[cfg(feature = "tcp")]
    Tcp(TcpClient),
}

/// The sink opened on the first write, once the column subset is fixed.
enum OpenSink {
    Http(clickhouse::insert_formatted::BufInsertFormatted),
    #[cfg(feature = "tcp")]
    Tcp {
        session: TcpInsertSession,
        /// Native block headers for the insert columns, parsed once.
        native: Vec<ColumnSchema>,
        /// RowBinary rows held for the next Native block.
        block: RowBlock,
    },
}

/// Rows buffered for the next Native block, with the row buffers recycled
/// between blocks.
///
/// [`crate::native::encode_columns`] transposes from `&[Vec<u8>]`, so each row
/// stays its own `Vec`; `spare` hands the emptied ones back on the next block
/// so a steady-state insert allocates once, not once per row.
#[cfg(feature = "tcp")]
#[derive(Default)]
struct RowBlock {
    rows: Vec<Vec<u8>>,
    spare: Vec<Vec<u8>>,
    bytes: usize,
}

#[cfg(feature = "tcp")]
impl RowBlock {
    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Encode one row into a recycled buffer. A row that fails to encode
    /// returns its buffer to the pool and leaves the block unchanged.
    fn push(&mut self, row: &DynamicRow<'_>) -> Result<(), DynamicError> {
        let mut buf = self.spare.pop().unwrap_or_default();
        buf.clear();
        if let Err(e) = row.encode_into(&mut buf) {
            self.spare.push(buf);
            return Err(e);
        }
        self.bytes += buf.len();
        self.rows.push(buf);
        Ok(())
    }

    /// Whether the block has hit either flush trigger.
    fn is_full(&self, row_cap: u64) -> bool {
        self.bytes >= BLOCK_FLUSH_BYTES || self.rows.len() as u64 >= row_cap
    }

    /// Take the rows for a send, keeping their buffers for the next block.
    fn recycle(&mut self) {
        self.spare.append(&mut self.rows);
        self.bytes = 0;
    }
}

/// The open INSERT: the column subset fixed by the first row, and the sink
/// carrying it.
struct Active {
    columns: Vec<ColumnDef>,
    sink: OpenSink,
}

/// Dynamic insert for a single table.
///
/// Encodes runtime-shaped `Map<String, Value>` rows to ClickHouse using a
/// schema fetched from `system.columns`. As simple to drive as JSONEachRow,
/// but binary on the wire so the server skips JSON parsing.
#[must_use = "a DynamicInsert must be finished with `.end().await` to commit the rows"]
#[non_exhaustive]
pub struct DynamicInsert {
    backend: Backend,
    database: String,
    table: String,
    schema: Option<Arc<DynamicSchema>>,
    active: Option<Active>,
    rows_written: u64,
}

impl DynamicInsert {
    /// Insert over HTTP. The schema is fetched from `system.columns` on the
    /// first write and cached in `schema_cache`.
    pub fn http(
        client: Client,
        database: &str,
        table: &str,
        schema_cache: Arc<DynamicSchemaCache>,
    ) -> Self {
        Self {
            backend: Backend::Http {
                client,
                cache: schema_cache,
            },
            database: database.to_string(),
            table: table.to_string(),
            schema: None,
            active: None,
            rows_written: 0,
        }
    }

    /// Insert over the native protocol against a caller-supplied schema.
    /// Nothing is fetched or cached here -- the caller resolved the schema
    /// before opening the insert.
    #[cfg(feature = "tcp")]
    pub fn tcp(client: TcpClient, database: &str, table: &str, schema: Arc<DynamicSchema>) -> Self {
        Self {
            backend: Backend::Tcp(client),
            database: database.to_string(),
            table: table.to_string(),
            schema: Some(schema),
            active: None,
            rows_written: 0,
        }
    }

    fn full_table(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }

    /// Load the schema (from cache, else `system.columns`) if not already held.
    async fn ensure_schema(&mut self) -> Result<(), DynamicError> {
        if self.schema.is_some() {
            return Ok(());
        }
        let full = self.full_table();
        let schema = match &self.backend {
            Backend::Http { client, cache } => {
                if let Some(cached) = cache.get(&full) {
                    cached
                } else {
                    let fetched = fetch_dynamic_schema(client, &self.database, &self.table).await?;
                    cache.insert(&full, Arc::clone(&fetched));
                    fetched
                }
            }
            #[cfg(feature = "tcp")]
            Backend::Tcp(_) => return Ok(()),
        };
        self.schema = Some(schema);
        Ok(())
    }

    /// On the first row, fix the column subset and open the sink.
    ///
    /// `raw_names` are column names supplied via raw passthrough (e.g. `_json`)
    /// that must be in the INSERT even if absent from the row map.
    // The only await is opening the TCP session; the HTTP sink opens
    // synchronously. Callers await this either way, so the signature is fixed.
    #[cfg_attr(
        not(feature = "tcp"),
        allow(clippy::unused_async, clippy::unused_async_trait_impl)
    )]
    async fn ensure_active(
        &mut self,
        row: &Map<String, Value>,
        raw_names: &[&str],
    ) -> Result<(), DynamicError> {
        if self.active.is_some() {
            return Ok(());
        }
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| DynamicError::EncodingError {
                column: String::new(),
                message: "schema not available after fetch".to_string(),
            })?;

        let columns = select_columns(row, raw_names, schema);
        if columns.is_empty() {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "no columns to insert for this row".to_string(),
            });
        }
        let json_columns = schema.has_json_columns();

        let target = format!(
            "{}.{}",
            escape_ident(&self.database),
            escape_ident(&self.table)
        );
        let cols_sql = columns
            .iter()
            .map(|c| escape_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");

        let sink = match &self.backend {
            Backend::Http { client, .. } => {
                let sql = format!("INSERT INTO {target} ({cols_sql}) FORMAT RowBinary");
                let mut client = client.clone();
                if json_columns {
                    client = client.with_setting(JSON_AS_STRING_SETTING, "1");
                }
                OpenSink::Http(client.insert_formatted_with(sql).buffered())
            }
            #[cfg(feature = "tcp")]
            Backend::Tcp(client) => {
                let full = self.full_table();
                let sql = format!("INSERT INTO {target} ({cols_sql}) FORMAT Native");
                let headers: Vec<(String, String)> = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.ty.raw.clone()))
                    .collect();
                let native =
                    ColumnSchema::from_headers(&headers).map_err(|e| classify_error(&full, e))?;
                // Empty query id: the server allocates one.
                let session = client
                    .insert_native("", &sql)
                    .await
                    .map_err(|e| classify_error(&full, e))?;
                OpenSink::Tcp {
                    session,
                    native,
                    block: RowBlock::default(),
                }
            }
        };

        self.active = Some(Active { columns, sink });
        Ok(())
    }

    /// Encode and buffer a row for insert. Fetches the schema and opens the
    /// sink on the first call.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicError`] on schema fetch failure, an unsupported column
    /// type, an encoding failure, or a transport error.
    pub async fn write_map(&mut self, row: &Map<String, Value>) -> Result<(), DynamicError> {
        self.write(row, &[], None).await
    }

    /// Like [`write_map`][Self::write_map], but `raw` names one column written
    /// from pre-encoded bytes (e.g. the original payload for `_json`), skipping
    /// a re-serialise. The column is included in the INSERT even when the row
    /// map does not carry it.
    ///
    /// # Errors
    ///
    /// As [`write_map`][Self::write_map].
    pub async fn write_map_with_raw(
        &mut self,
        row: &Map<String, Value>,
        passthrough: (&str, &[u8]),
    ) -> Result<(), DynamicError> {
        self.write(row, &[passthrough.0], Some(passthrough)).await
    }

    async fn write(
        &mut self,
        row: &Map<String, Value>,
        raw_names: &[&str],
        passthrough: Option<(&str, &[u8])>,
    ) -> Result<(), DynamicError> {
        self.ensure_schema().await?;
        self.ensure_active(row, raw_names).await?;

        #[cfg(feature = "tcp")]
        let full = self.full_table();
        let Active { columns, sink } =
            self.active
                .as_mut()
                .ok_or_else(|| DynamicError::EncodingError {
                    column: String::new(),
                    message: "insert sink not initialised".to_string(),
                })?;

        let dynamic_row = match passthrough {
            Some((json_col, raw_bytes)) => DynamicRow::with_raw(row, columns, raw_bytes, json_col),
            None => DynamicRow::new(row, columns),
        };

        match sink {
            OpenSink::Http(insert) => insert.write_buffered(&dynamic_row.encode()?),
            #[cfg(feature = "tcp")]
            OpenSink::Tcp {
                session,
                native,
                block,
            } => {
                block.push(&dynamic_row)?;
                if block.is_full(TcpClient::DEFAULT_INSERT_BLOCK_ROWS) {
                    send_block(session, native, block, &full).await?;
                }
            }
        }
        self.rows_written += 1;
        Ok(())
    }

    /// Flush and finalise the INSERT, returning the number of rows written.
    ///
    /// On a schema-mismatch error the cached schema is invalidated so the next
    /// insert re-fetches from `system.columns`.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicError`] if the server rejects the insert.
    pub async fn end(mut self) -> Result<u64, DynamicError> {
        let full = self.full_table();
        let rows_written = self.rows_written;
        let outcome = match self.active.take() {
            None => Ok(()),
            Some(active) => match active.sink {
                OpenSink::Http(mut insert) => {
                    insert.end().await.map_err(|e| classify_error(&full, e))
                }
                #[cfg(feature = "tcp")]
                OpenSink::Tcp {
                    session,
                    native,
                    mut block,
                } => match send_block(&session, &native, &mut block, &full).await {
                    Ok(()) => session.finish().await.map_err(|e| classify_error(&full, e)),
                    Err(e) => {
                        session.abort();
                        Err(e)
                    }
                },
            },
        };
        if let Err(err) = outcome {
            if matches!(err, DynamicError::SchemaMismatch { .. }) {
                self.invalidate_schema();
            }
            return Err(err);
        }
        Ok(rows_written)
    }

    /// Invalidate the cached schema, forcing a re-fetch on the next insert.
    /// A no-op on the TCP path, where the caller owns the schema.
    pub fn invalidate_schema(&mut self) {
        let full = self.full_table();
        match &self.backend {
            Backend::Http { cache, .. } => {
                cache.invalidate(&full);
                self.schema = None;
            }
            #[cfg(feature = "tcp")]
            Backend::Tcp(_) => {}
        }
    }

    /// Number of rows written so far.
    #[must_use]
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// The resolved schema, if it has been loaded.
    #[must_use]
    pub fn schema(&self) -> Option<&DynamicSchema> {
        self.schema.as_deref()
    }
}

/// Transpose the buffered RowBinary rows into one Native block and send it.
#[cfg(feature = "tcp")]
async fn send_block(
    session: &TcpInsertSession,
    native: &[ColumnSchema],
    block: &mut RowBlock,
    full_table: &str,
) -> Result<(), DynamicError> {
    if block.is_empty() {
        return Ok(());
    }
    let bytes = encode_columns(&block.rows, native, session.server_revision)
        .map_err(|e| classify_error(full_table, e))?;
    let rows = block.rows.len() as u64;
    block.recycle();
    session
        .send_block(bytes, native.len() as u64, rows)
        .await
        .map_err(|e| classify_error(full_table, e))
}

/// Choose the columns to include in the INSERT: every column present in the
/// row, every raw-passthrough column, and every required column (no
/// server-side default). Columns that have a default and are not supplied are
/// omitted so the server fills them (e.g. `_uuid`, `_timestamp_load`).
fn select_columns(
    row: &Map<String, Value>,
    raw_names: &[&str],
    schema: &DynamicSchema,
) -> Vec<ColumnDef> {
    schema
        .columns
        .iter()
        .filter(|c| {
            row.contains_key(&c.name) || raw_names.contains(&c.name.as_str()) || !c.has_default
        })
        .cloned()
        .collect()
}

/// ClickHouse error codes that say the cached schema no longer matches the
/// table, so re-fetching it and retrying is the right response. Names are the
/// upstream `ErrorCodes.cpp` ones.
const SCHEMA_DRIFT_CODES: &[i32] = &[
    6,   // CANNOT_PARSE_TEXT
    8,   // THERE_IS_NO_COLUMN
    16,  // NO_SUCH_COLUMN_IN_TABLE
    26,  // CANNOT_PARSE_QUOTED_STRING
    27,  // CANNOT_PARSE_INPUT_ASSERTION_FAILED
    41,  // CANNOT_PARSE_DATETIME
    44,  // ILLEGAL_COLUMN
    47,  // UNKNOWN_IDENTIFIER
    53,  // TYPE_MISMATCH
    117, // INCORRECT_DATA
];

/// Whether a fresh attempt may succeed because nothing is wrong with the rows:
/// a timeout, a failed connect, a connection the server dropped, a poisoned
/// handle or a server code worth retrying.
fn is_transport_failure(e: &crate::Error) -> bool {
    #[cfg(feature = "tcp")]
    {
        crate::tcp::retry::is_retriable_transport(e)
    }
    #[cfg(not(feature = "tcp"))]
    {
        e.is_retriable()
    }
}

/// Classify a sink error as a transport failure (retry the batch), schema drift
/// (the cached schema is stale) or a rejection of the rows themselves.
///
/// Transport comes first: a timeout or a dropped connection says nothing about
/// the rows, and reporting it as an encoding error gets good rows dead-lettered.
///
/// The code is read structurally wherever the error carries one, so this does
/// not become a second opinion on what the server's message wording means. The
/// substrings are the fallback for a rejection that carries no code at all --
/// a proxy page, or a body [`ServerException::parse`] declines.
///
/// [`ServerException::parse`]: crate::ext::ServerException::parse
fn classify_error(full_table: &str, e: impl Into<crate::Error>) -> DynamicError {
    let err: crate::Error = e.into();
    if is_transport_failure(&err) {
        return DynamicError::Transport {
            table: full_table.to_string(),
            source: Box::new(err),
        };
    }
    let code = match &err {
        crate::Error::ServerException { code, .. } => Some(*code),
        crate::Error::BadResponse(body) => {
            crate::ext::ServerException::parse(&clickhouse::error::Error::BadResponse(body.clone()))
                .map(|exc| exc.code)
        }
        _ => None,
    };

    let msg = err.to_string();
    let mismatch = match code {
        Some(code) => SCHEMA_DRIFT_CODES.contains(&code),
        None => {
            msg.contains("UNKNOWN_IDENTIFIER")
                || msg.contains("NO_SUCH_COLUMN")
                || msg.contains("THERE_IS_NO_COLUMN")
                || msg.contains("TYPE_MISMATCH")
                || msg.contains("ILLEGAL_COLUMN")
                || msg.contains("CANNOT_PARSE")
                || msg.contains("cannot parse")
                || msg.contains("INCORRECT_DATA")
                || msg.contains("incorrect data")
        }
    };

    if mismatch {
        DynamicError::SchemaMismatch {
            table: full_table.to_string(),
            message: msg,
        }
    } else {
        DynamicError::EncodingError {
            column: String::new(),
            message: msg,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> DynamicSchema {
        DynamicSchema::from_columns(
            "db.t",
            vec![
                ColumnDef::with_default_kind("id", "UInt64", ""),
                ColumnDef::with_default_kind("name", "String", ""),
                ColumnDef::with_default_kind("_uuid", "UUID", "DEFAULT"),
                ColumnDef::with_default_kind("_json", "JSON", ""),
            ],
        )
    }

    #[test]
    fn select_columns_includes_present_and_required_omits_absent_default() {
        let s = schema();
        let row = json!({ "id": 1, "name": "a" });
        let obj = row.as_object().unwrap().clone();
        let cols = select_columns(&obj, &[], &s);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        // id, name present; _json required (no default); _uuid omitted (default, absent)
        assert_eq!(names, vec!["id", "name", "_json"]);
    }

    #[test]
    fn select_columns_includes_raw_passthrough_column() {
        let s = schema();
        let row = json!({ "id": 1 });
        let obj = row.as_object().unwrap().clone();
        let cols = select_columns(&obj, &["_json"], &s);
        assert!(cols.iter().any(|c| c.name == "_json"));
    }

    /// The code decides where the server sent one, so a reworded message does
    /// not change the classification.
    #[test]
    fn classify_reads_the_error_code_not_the_message() {
        let drift = clickhouse::error::Error::BadResponse(
            "Code: 47. DB::Exception: Missing columns: 'nope' (UNKNOWN_IDENTIFIER) \
             (version 26.2.4.23)"
                .into(),
        );
        assert!(
            matches!(
                classify_error("db.t", drift),
                DynamicError::SchemaMismatch { .. }
            ),
            "code 47 is schema drift"
        );

        // 469 = VIOLATED_CONSTRAINT: a real rejection of the rows, but not
        // schema drift, so the cached schema must not be thrown away over it.
        let not_drift = clickhouse::error::Error::BadResponse(
            "Code: 469. DB::Exception: Constraint violated (VIOLATED_CONSTRAINT)".into(),
        );
        assert!(matches!(
            classify_error("db.t", not_drift),
            DynamicError::EncodingError { .. }
        ));

        // A typed variant carries the code without any text to parse.
        assert!(matches!(
            classify_error(
                "db.t",
                crate::Error::ServerException {
                    code: 117,
                    name: None,
                    message: "incorrect data".into(),
                    stack_trace: None,
                }
            ),
            DynamicError::SchemaMismatch { .. }
        ));

        // `Code: N` is upstream's stand-in for a body it could not read.
        assert!(matches!(
            classify_error(
                "db.t",
                clickhouse::error::Error::BadResponse("Code: 117".into())
            ),
            DynamicError::SchemaMismatch { .. }
        ));
    }

    /// A body carrying no code at all still classifies off its text.
    #[test]
    fn classify_falls_back_to_the_message_without_a_code() {
        let e = clickhouse::error::Error::Custom("incorrect data".into());
        assert!(matches!(
            classify_error("db.t", e),
            DynamicError::SchemaMismatch { .. }
        ));

        let e = clickhouse::error::Error::Custom("network reset".into());
        assert!(matches!(
            classify_error("db.t", e),
            DynamicError::EncodingError { .. }
        ));
    }

    /// A failure that says nothing about the rows must stay retriable, or the
    /// caller dead-letters a good batch on a timeout. A rejection of the rows
    /// itself must not, or it retries forever.
    #[test]
    fn classify_keeps_transport_failures_retriable_and_row_failures_terminal() {
        use std::io::{Error as Io, ErrorKind};

        let io = |kind| crate::Error::from(Io::new(kind, "io"));
        let server = |code| crate::Error::ServerException {
            code,
            name: None,
            message: "rejected".into(),
            stack_trace: None,
        };
        let transport: Vec<(&str, crate::Error)> = vec![
            ("timeout", crate::Error::TimedOut),
            ("connect", crate::Error::Connect("refused".into())),
            ("transient", crate::Error::Transient("poisoned".into())),
            ("network", crate::Error::Network("down".into())),
            ("broken pipe", io(ErrorKind::BrokenPipe)),
            ("eof", io(ErrorKind::UnexpectedEof)),
            ("reset", io(ErrorKind::ConnectionReset)),
            ("memory limit", server(241)),
            ("too many parts", server(252)),
        ];
        for (what, err) in transport {
            let classified = classify_error("db.t", err);
            assert!(
                matches!(classified, DynamicError::Transport { .. }),
                "{what}: {classified:?}"
            );
            assert!(classified.is_retriable(), "{what}");
            assert!(crate::Error::from(classified).is_retriable(), "{what}");
        }

        let terminal: Vec<(&str, crate::Error)> = vec![
            ("constraint", server(469)),
            ("custom", crate::Error::Custom("bad row".into())),
            ("io other kind", io(ErrorKind::PermissionDenied)),
        ];
        for (what, err) in terminal {
            let classified = classify_error("db.t", err);
            assert!(
                matches!(classified, DynamicError::EncodingError { .. }),
                "{what}: {classified:?}"
            );
            assert!(!classified.is_retriable(), "{what}");
        }
    }

    /// The same classification, reached through the real sink: a server that
    /// never answers the INSERT, and one that hangs up after opening it.
    #[cfg(feature = "tcp")]
    mod over_tcp {
        use std::time::Duration;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        use super::*;
        use crate::tcp::mock::{serve_one_handshake, write_schema_block};

        fn rows() -> (Arc<DynamicSchema>, Map<String, Value>) {
            let schema = Arc::new(DynamicSchema::from_columns(
                "db.t",
                vec![
                    ColumnDef::new("n", "UInt64"),
                    ColumnDef::new("_json", "String"),
                ],
            ));
            let row = json!({ "n": 1 }).as_object().unwrap().clone();
            (schema, row)
        }

        /// A DynamicInsert against a mock that answers the handshake and then
        /// runs `script`, with a read timeout short enough to hit.
        async fn insert_against<F, Fut>(
            script: F,
        ) -> (DynamicInsert, tokio::task::JoinHandle<TcpStream>)
        where
            F: FnOnce(TcpStream) -> Fut + Send + 'static,
            Fut: std::future::Future<Output = TcpStream> + Send + 'static,
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let server = tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                serve_one_handshake(&mut sock).await;
                script(sock).await
            });
            let client = TcpClient::new(addr).with_read_timeout(Some(Duration::from_millis(250)));
            let (schema, _) = rows();
            (DynamicInsert::tcp(client, "db", "t", schema), server)
        }

        /// Opening the INSERT times out waiting for the schema block, through
        /// both entry points that open it.
        #[tokio::test]
        async fn a_timeout_opening_the_insert_is_retriable() {
            let (_, row) = rows();
            for with_raw in [false, true] {
                let (mut insert, server) = insert_against(|sock| async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    sock
                })
                .await;
                let err = if with_raw {
                    insert.write_map_with_raw(&row, ("_json", b"{}")).await
                } else {
                    insert.write_map(&row).await
                }
                .expect_err("the server never answers the INSERT");
                assert!(
                    matches!(&err, DynamicError::Transport { source, .. }
                        if matches!(**source, crate::Error::TimedOut)),
                    "with_raw={with_raw}: {err:?}"
                );
                assert!(err.is_retriable());
                server.abort();
            }
        }

        /// The server opens the INSERT and then closes: committing the block
        /// fails in transit, not because of the rows.
        #[tokio::test]
        async fn a_connection_dropped_before_commit_is_retriable() {
            let (_, row) = rows();
            let (mut insert, server) = insert_against(|mut sock| async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                write_schema_block(&mut sock, &[("n", "UInt64"), ("_json", "String")]).await;
                sock.shutdown().await.unwrap();
                sock
            })
            .await;
            insert.write_map(&row).await.expect("the row buffers");

            let err = insert.end().await.expect_err("the server hung up");
            assert!(matches!(err, DynamicError::Transport { .. }), "got {err:?}");
            assert!(err.is_retriable());
            let _sock = server.await.unwrap();
        }
    }

    #[test]
    fn escape_ident_quotes_and_escapes() {
        assert_eq!(escape_ident("plain"), "`plain`");
        assert_eq!(escape_ident("we`ird"), "`we\\`ird`");
    }

    /// Every type the encoder writes has to parse as a Native block header,
    /// or the TCP sink cannot declare the column.
    #[cfg(feature = "tcp")]
    #[test]
    fn native_headers_cover_every_encoder_type() {
        let ok = [
            ("a", "UInt64"),
            ("b", "Nullable(String)"),
            ("c", "LowCardinality(Nullable(String))"),
            ("d", "Array(Int64)"),
            ("e", "Map(String, UInt32)"),
            ("f", "DateTime64(3, 'UTC')"),
            ("g", "Decimal(18, 2)"),
            ("h", "Enum8('a' = 1)"),
            ("i", "FixedString(16)"),
            ("j", "IPv6"),
            ("k", "JSON"),
        ];
        for (name, ty) in ok {
            let headers = vec![(name.to_string(), ty.to_string())];
            assert!(
                ColumnSchema::from_headers(&headers).is_ok(),
                "{ty} must parse as a Native column header"
            );
        }

        // The encoder already writes JSON as a length-prefixed string, so the
        // block declares the column as String and the server casts it back.
        let json = vec![("data".to_string(), "JSON".to_string())];
        let native = ColumnSchema::from_headers(&json).unwrap();
        let block = encode_columns(&[vec![2u8, b'{', b'}']], &native, 0).unwrap();
        assert_eq!(block.as_slice(), b"\x04data\x06String\x02{}");
    }

    /// Bytes, not just rows: a wide row must not buffer to the row cap first.
    #[cfg(feature = "tcp")]
    #[test]
    fn a_block_flushes_on_bytes_long_before_the_row_cap() {
        let columns = [ColumnDef::new("s", "String")];
        let wide = "x".repeat(64 * 1024);
        let row = json!({ "s": wide }).as_object().unwrap().clone();

        let mut block = RowBlock::default();
        let mut pushed = 0u64;
        while !block.is_full(TcpClient::DEFAULT_INSERT_BLOCK_ROWS) {
            block
                .push(&DynamicRow::new(&row, &columns))
                .expect("a String column always encodes");
            pushed += 1;
            assert!(pushed < 1_000, "the byte trigger never fired");
        }
        assert!(block.bytes >= BLOCK_FLUSH_BYTES);
        assert!(pushed < TcpClient::DEFAULT_INSERT_BLOCK_ROWS);
    }

    /// The recycled buffers come back empty, so a second block does not
    /// inherit the first block's bytes.
    #[cfg(feature = "tcp")]
    #[test]
    fn recycling_a_block_reuses_the_buffers_and_resets_the_byte_count() {
        let columns = [ColumnDef::new("s", "String")];
        let row = json!({"s": "abc"}).as_object().unwrap().clone();

        let mut block = RowBlock::default();
        for _ in 0..4 {
            block.push(&DynamicRow::new(&row, &columns)).unwrap();
        }
        assert_eq!(block.rows.len(), 4);
        assert_eq!(block.bytes, 4 * 4);

        block.recycle();
        assert!(block.is_empty());
        assert_eq!(block.bytes, 0);
        assert_eq!(block.spare.len(), 4);

        block.push(&DynamicRow::new(&row, &columns)).unwrap();
        assert_eq!(block.rows.len(), 1);
        assert_eq!(block.rows[0], vec![3, b'a', b'b', b'c']);
        assert_eq!(block.bytes, 4);
    }

    /// A row the encoder rejects must not leave a half-written row behind.
    #[cfg(feature = "tcp")]
    #[test]
    fn a_rejected_row_leaves_the_block_unchanged() {
        let columns = [ColumnDef::new("a", "UInt8"), ColumnDef::new("b", "UInt8")];
        let good = json!({"a": 1, "b": 2}).as_object().unwrap().clone();
        let bad = json!({"a": 1, "b": 300}).as_object().unwrap().clone();

        let mut block = RowBlock::default();
        block.push(&DynamicRow::new(&good, &columns)).unwrap();
        assert!(block.push(&DynamicRow::new(&bad, &columns)).is_err());

        assert_eq!(block.rows.len(), 1);
        assert_eq!(block.bytes, 2);
        assert_eq!(block.rows[0], vec![1, 2]);
    }
}
