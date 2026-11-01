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
//! [`DynamicSchema`] and ships the rows through one of two sinks:
//!
//! - HTTP -- `Client::insert_formatted_with(... FORMAT RowBinaryWithDefaults)`,
//!   naming every insertable column, written row-wise. A row that omits a
//!   defaulted column flags it, and the server fills its default, so a batch is
//!   one INSERT whatever its rows carry. The schema is read from
//!   `system.columns` on the first write and cached.
//! - TCP (`tcp` feature) -- `TcpClient::insert_native(... FORMAT Native)`, with
//!   [`DynamicRow::encode_into`] rows transposed into columnar blocks by
//!   [`crate::native::encode_columns`]. The schema is supplied by the caller,
//!   which [`crate::unified::UnifiedClient::dynamic_insert`] reads over the
//!   same TCP connection before opening the insert.
//!
//! Both sinks send once 1 MiB is buffered, so memory holds one chunk of the
//! batch, never the whole of it.
//!
//! Native has no per-row default, so a TCP INSERT names only the defaulted
//! columns its first row supplies. A row that supplies a different set commits
//! the rows so far and opens a new INSERT for itself, so no supplied value is
//! dropped and no omitted one gets the type's zero in place of the server's
//! default.
//!
//! On a schema-mismatch error the HTTP path invalidates the cached schema so
//! the next insert re-fetches, unless [`SchemaRefresh::AtExpiry`] says to keep
//! it until its TTL. [`InsertTimeouts`] bounds each step on the client side.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Map, Value};

use clickhouse::Client;
use clickhouse::insert_formatted::InsertFormatted;

use super::encode::{ColumnDef, DynamicRow};
use super::error::DynamicError;
#[cfg(feature = "tcp")]
use super::schema::within;
use super::schema::{DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};

#[cfg(feature = "tcp")]
use crate::native::{ColumnSchema, encode_columns};
#[cfg(feature = "tcp")]
use crate::tcp::{TcpClient, TcpInsertSession};

/// The server setting that makes a RowBinary reader take a length-prefixed
/// string as a JSON value, which is how the dynamic encoder writes JSON
/// columns on the wire.
const JSON_AS_STRING_SETTING: &str = "input_format_binary_read_json_as_string";

/// The server setting that makes a `RowBinaryWithDefaults` reader fill a
/// flagged column with its DEFAULT expression. Off, it writes the type's zero.
/// Set on every HTTP insert, so a client default or a user profile cannot turn
/// it off: a request setting overrides both, and a profile constraint that
/// forbids it fails the INSERT loudly.
const DEFAULTS_FOR_OMITTED_SETTING: &str = "input_format_defaults_for_omitted_fields";

/// Send the buffered rows once they reach this many encoded bytes: an HTTP
/// chunk, or a TCP block if the row cap has not come first. A row cap alone
/// lets a wide row buffer a gigabyte before the first send.
const FLUSH_BYTES: usize = 1 << 20;

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

/// Client-side bounds on one dynamic insert. `None` leaves a step unbounded
/// here, under whatever bound the client itself carries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct InsertTimeouts {
    /// Bound on the schema read, on opening the INSERT, and on handing over
    /// each chunk or block.
    pub send: Option<Duration>,
    /// Bound on the server's answer once every row is sent.
    pub end: Option<Duration>,
}

impl InsertTimeouts {
    /// Bounds on sending and on the server's final answer.
    #[must_use]
    pub const fn new(send: Option<Duration>, end: Option<Duration>) -> Self {
        Self { send, end }
    }
}

/// What a schema-mismatch rejection does to the cached schema.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaRefresh {
    /// Drop it, so the next insert reads the table again.
    #[default]
    OnMismatch,
    /// Keep it until the cache's TTL expires.
    AtExpiry,
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

/// The sink carrying one INSERT.
enum OpenSink {
    Http {
        insert: InsertFormatted,
        /// `RowBinaryWithDefaults` rows held for the next chunk.
        chunk: Vec<u8>,
    },
    #[cfg(feature = "tcp")]
    Tcp {
        session: TcpInsertSession,
        /// [`default_shape`] of the row that opened it.
        shape: Vec<bool>,
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
        self.bytes >= FLUSH_BYTES || self.rows.len() as u64 >= row_cap
    }

    /// Take the rows for a send, keeping their buffers for the next block.
    fn recycle(&mut self) {
        self.spare.append(&mut self.rows);
        self.bytes = 0;
    }
}

/// The open INSERT: the columns it names, and the sink carrying it.
struct Active {
    columns: Vec<ColumnDef>,
    sink: OpenSink,
}

/// Dynamic insert for a single table.
///
/// Encodes runtime-shaped `Map<String, Value>` rows to ClickHouse using a
/// schema fetched from `system.columns`. As simple to drive as JSONEachRow,
/// but binary on the wire so the server skips JSON parsing.
///
/// Over HTTP a batch is one INSERT. Over TCP, rows that supply different sets
/// of defaulted columns go out as separate INSERTs, each committed when the
/// next set arrives and the last by [`end`][Self::end]. An error can therefore
/// follow rows already committed: retry the whole batch, which at-least-once
/// delivery tolerates.
#[must_use = "a DynamicInsert must be finished with `.end().await` to commit the rows"]
#[non_exhaustive]
pub struct DynamicInsert {
    backend: Backend,
    database: String,
    table: String,
    schema: Option<Arc<DynamicSchema>>,
    active: Option<Active>,
    rows_written: u64,
    timeouts: InsertTimeouts,
    refresh: SchemaRefresh,
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
            timeouts: InsertTimeouts::default(),
            refresh: SchemaRefresh::default(),
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
            timeouts: InsertTimeouts::default(),
            refresh: SchemaRefresh::default(),
        }
    }

    /// Bound each step on the client side (default: unbounded here).
    pub fn with_timeouts(mut self, timeouts: InsertTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Say what a schema-mismatch rejection does to the cached schema
    /// (default [`SchemaRefresh::OnMismatch`]).
    pub fn with_schema_refresh(mut self, refresh: SchemaRefresh) -> Self {
        self.refresh = refresh;
        self
    }

    fn full_table(&self) -> String {
        full_name(&self.database, &self.table)
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
                    // Boxed: the read runs once per cache miss, and inline its
                    // future would size every `write_map` future to match.
                    let fetched = Box::pin(fetch_dynamic_schema(
                        client,
                        &self.database,
                        &self.table,
                        self.timeouts.send,
                    ))
                    .await?;
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

    /// With no INSERT open, choose its columns and open the sink: every
    /// insertable column over HTTP, the subset `row` fixes over TCP.
    ///
    /// `raw_names` are column names supplied via raw passthrough (e.g. `_json`)
    /// that must be in the INSERT even if absent from the row map.
    // The only await is opening the TCP session; the HTTP sink opens
    // synchronously. Callers await this either way, so the signature is fixed.
    #[cfg_attr(
        not(feature = "tcp"),
        allow(
            clippy::unused_async,
            clippy::unused_async_trait_impl,
            unused_variables
        )
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

        let columns = match &self.backend {
            Backend::Http { .. } => insertable_columns(schema),
            #[cfg(feature = "tcp")]
            Backend::Tcp(_) => select_columns(row, raw_names, schema),
        };
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
                let sql = format!("INSERT INTO {target} ({cols_sql}) FORMAT RowBinaryWithDefaults");
                let mut client = client
                    .clone()
                    .with_setting(DEFAULTS_FOR_OMITTED_SETTING, "1");
                if json_columns {
                    client = client.with_setting(JSON_AS_STRING_SETTING, "1");
                }
                OpenSink::Http {
                    insert: client
                        .insert_formatted_with(sql)
                        .with_timeouts(self.timeouts.send, self.timeouts.end),
                    chunk: Vec::new(),
                }
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
                let session = within(self.timeouts.send, client.insert_native("", &sql))
                    .await
                    .map_err(|e| classify_error(&full, e))?;
                OpenSink::Tcp {
                    session,
                    shape: default_shape(row, raw_names, schema).collect(),
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
        let outcome = self.write_row(row, raw_names, passthrough).await;
        self.after(outcome)
    }

    async fn write_row(
        &mut self,
        row: &Map<String, Value>,
        raw_names: &[&str],
        passthrough: Option<(&str, &[u8])>,
    ) -> Result<(), DynamicError> {
        self.ensure_schema().await?;
        #[cfg(feature = "tcp")]
        if let (
            Some(Active {
                sink: OpenSink::Tcp { shape, .. },
                ..
            }),
            Some(schema),
        ) = (&self.active, &self.schema)
            && !shape
                .iter()
                .copied()
                .eq(default_shape(row, raw_names, schema))
        {
            Box::pin(self.commit()).await?;
        }
        // Boxed, as the commit above: both run once per INSERT, and inline
        // they would size every row's `write_map` future to match. The check
        // keeps the allocation off rows that find the INSERT open.
        if self.active.is_none() {
            Box::pin(self.ensure_active(row, raw_names)).await?;
        }

        let Self {
            database,
            table,
            active,
            #[cfg(feature = "tcp")]
            timeouts,
            ..
        } = self;
        let Some(Active { columns, sink, .. }) = active.as_mut() else {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "insert sink not initialised".to_string(),
            });
        };

        let dynamic_row = match passthrough {
            Some((json_col, raw_bytes)) => DynamicRow::with_raw(row, columns, raw_bytes, json_col),
            None => DynamicRow::new(row, columns),
        };

        match sink {
            OpenSink::Http { insert, chunk } => {
                let start = chunk.len();
                if let Err(e) = dynamic_row.encode_with_defaults_into(chunk) {
                    chunk.truncate(start);
                    return Err(e);
                }
                if chunk.len() >= FLUSH_BYTES {
                    send_chunk(insert, chunk, &full_name(database, table)).await?;
                }
            }
            #[cfg(feature = "tcp")]
            OpenSink::Tcp {
                session,
                native,
                block,
                ..
            } => {
                block.push(&dynamic_row)?;
                if block.is_full(TcpClient::DEFAULT_INSERT_BLOCK_ROWS) {
                    send_block(
                        session,
                        native,
                        block,
                        &full_name(database, table),
                        timeouts.send,
                    )
                    .await?;
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
        let outcome = self.commit().await;
        self.after(outcome)?;
        Ok(self.rows_written)
    }

    /// Send what is buffered and commit the open INSERT, if there is one.
    async fn commit(&mut self) -> Result<(), DynamicError> {
        let full = self.full_table();
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        match active.sink {
            OpenSink::Http {
                mut insert,
                mut chunk,
            } => {
                send_chunk(&mut insert, &mut chunk, &full).await?;
                insert.end().await.map_err(|e| classify_error(&full, e))
            }
            #[cfg(feature = "tcp")]
            OpenSink::Tcp {
                session,
                native,
                mut block,
                ..
            } => match send_block(&session, &native, &mut block, &full, self.timeouts.send).await {
                Ok(()) => within(self.timeouts.end, session.finish())
                    .await
                    .map_err(|e| classify_error(&full, e)),
                Err(e) => {
                    session.abort();
                    Err(e)
                }
            },
        }
    }

    /// Pass `outcome` through, invalidating the cached schema when the server
    /// rejected the rows as drift from it.
    fn after(&mut self, outcome: Result<(), DynamicError>) -> Result<(), DynamicError> {
        if self.refresh == SchemaRefresh::OnMismatch
            && let Err(DynamicError::SchemaMismatch { .. }) = &outcome
        {
            self.invalidate_schema();
        }
        outcome
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

    /// The resolved schema the rows are encoded against, if it has been
    /// loaded. Shared, so a caller can keep it past [`Self::end`] and compare
    /// it with the table as it stands after a rejection.
    #[must_use]
    pub fn schema(&self) -> Option<&Arc<DynamicSchema>> {
        self.schema.as_ref()
    }
}

/// `database.table`, as errors and the schema cache name it.
fn full_name(database: &str, table: &str) -> String {
    format!("{database}.{table}")
}

/// Send the buffered RowBinary rows as one HTTP chunk. A rejection the server
/// has already sent arrives here as the server's own error.
async fn send_chunk(
    insert: &mut InsertFormatted,
    chunk: &mut Vec<u8>,
    full_table: &str,
) -> Result<(), DynamicError> {
    if chunk.is_empty() {
        return Ok(());
    }
    insert
        .send(Bytes::from(std::mem::take(chunk)))
        .await
        .map_err(|e| classify_error(full_table, e))
}

/// Transpose the buffered RowBinary rows into one Native block and send it,
/// within `bound`.
#[cfg(feature = "tcp")]
async fn send_block(
    session: &TcpInsertSession,
    native: &[ColumnSchema],
    block: &mut RowBlock,
    full_table: &str,
    bound: Option<Duration>,
) -> Result<(), DynamicError> {
    if block.is_empty() {
        return Ok(());
    }
    let bytes = encode_columns(&block.rows, native, session.server_revision)
        .map_err(|e| classify_error(full_table, e))?;
    let rows = block.rows.len() as u64;
    block.recycle();
    within(bound, session.send_block(bytes, native.len() as u64, rows))
        .await
        .map_err(|e| classify_error(full_table, e))
}

/// Every column an INSERT may name. The server computes `MATERIALIZED` and
/// `ALIAS` columns itself and refuses a value for either.
fn insertable_columns(schema: &DynamicSchema) -> Vec<ColumnDef> {
    schema
        .columns
        .iter()
        .filter(|c| !matches!(c.default_kind.as_str(), "MATERIALIZED" | "ALIAS"))
        .cloned()
        .collect()
}

/// Choose the columns to include in a TCP INSERT: every column present in the
/// row, every raw-passthrough column, and every required column (no
/// server-side default). Columns that have a default and are not supplied are
/// omitted so the server fills them (e.g. `_uuid`, `_timestamp_load`).
#[cfg(feature = "tcp")]
fn select_columns(
    row: &Map<String, Value>,
    raw_names: &[&str],
    schema: &DynamicSchema,
) -> Vec<ColumnDef> {
    schema
        .columns
        .iter()
        .filter(|c| supplies(row, raw_names, c) || !c.has_default)
        .cloned()
        .collect()
}

/// For each defaulted column of the schema, in order, whether `row` supplies
/// it. Rows with equal shapes share a TCP INSERT's column list.
#[cfg(feature = "tcp")]
fn default_shape<'a>(
    row: &'a Map<String, Value>,
    raw_names: &'a [&str],
    schema: &'a DynamicSchema,
) -> impl Iterator<Item = bool> + 'a {
    schema
        .columns
        .iter()
        .filter(|c| c.has_default)
        .map(move |c| supplies(row, raw_names, c))
}

/// Whether the row map or a raw passthrough carries `column`.
#[cfg(feature = "tcp")]
fn supplies(row: &Map<String, Value>, raw_names: &[&str], column: &ColumnDef) -> bool {
    row.contains_key(&column.name) || raw_names.contains(&column.name.as_str())
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
/// The code is read structurally wherever the error carries one, typed over TCP
/// or the `Code: N` head of an HTTP body, so this does not become a second
/// opinion on what the server's message wording means. The substrings are the
/// fallback for a rejection that carries no code at all, such as a proxy page.
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
        crate::Error::BadResponse(body) => crate::error::bad_response_code(body),
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
    #[cfg(feature = "tcp")]
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

    /// HTTP names every column the server will take a value for, so one INSERT
    /// carries any row, and leaves out the ones it computes itself.
    #[test]
    fn insertable_columns_skip_only_computed_columns() {
        let mut s = schema();
        s.columns
            .push(ColumnDef::with_default_kind("m", "UInt8", "MATERIALIZED"));
        s.columns
            .push(ColumnDef::with_default_kind("a", "UInt8", "ALIAS"));
        s.columns
            .push(ColumnDef::with_default_kind("e", "UInt8", "EPHEMERAL"));
        let names: Vec<String> = insertable_columns(&s).into_iter().map(|c| c.name).collect();
        assert_eq!(names, ["id", "name", "_uuid", "_json", "e"]);
    }

    #[cfg(feature = "tcp")]
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

    #[cfg(feature = "tcp")]
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
            (
                "too many parts over http",
                crate::Error::BadResponse(
                    "Code: 252. DB::Exception: Too many parts. (TOO_MANY_PARTS)".into(),
                ),
            ),
            (
                "unknown insert status over http",
                crate::Error::BadResponse("Code: 319".into()),
            ),
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

        /// The insert's own bounds end each step against a server that opens
        /// the INSERT and then goes quiet, with no read timeout on the client:
        /// `send` on opening it, `end` on the server's final answer.
        #[tokio::test]
        async fn the_insert_timeouts_bound_a_server_that_goes_quiet() {
            let (_, row) = rows();
            for open_answers in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap().to_string();
                let server = tokio::spawn(async move {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    serve_one_handshake(&mut sock).await;
                    if open_answers {
                        let mut buf = [0u8; 4096];
                        let _ = sock.read(&mut buf).await;
                        write_schema_block(&mut sock, &[("n", "UInt64"), ("_json", "String")])
                            .await;
                    }
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    sock
                });
                let (schema, _) = rows();
                let bound = Some(Duration::from_millis(250));
                let mut insert = DynamicInsert::tcp(TcpClient::new(addr), "db", "t", schema)
                    .with_timeouts(InsertTimeouts::new(bound, bound));
                let err = match insert.write_map(&row).await {
                    Err(e) => e,
                    Ok(()) => insert.end().await.expect_err("the server never commits"),
                };
                assert!(err.is_timed_out(), "open answers {open_answers}: {err:?}");
                assert!(err.is_retriable());
                server.abort();
            }
        }
    }

    /// A schema mismatch drops the cached schema by default, and keeps it with
    /// [`SchemaRefresh::AtExpiry`].
    #[test]
    fn a_mismatch_refreshes_the_cache_only_when_asked_to() {
        for (refresh, kept) in [
            (SchemaRefresh::OnMismatch, false),
            (SchemaRefresh::AtExpiry, true),
        ] {
            let cache = DynamicSchemaCache::new(std::time::Duration::from_secs(300));
            cache.insert("db.t", Arc::new(schema()));
            let mut insert = DynamicInsert::http(Client::default(), "db", "t", Arc::clone(&cache))
                .with_schema_refresh(refresh);
            let mismatch = Err(DynamicError::SchemaMismatch {
                table: "db.t".into(),
                message: "drift".into(),
            });
            assert!(insert.after(mismatch).is_err());
            assert_eq!(cache.get("db.t").is_some(), kept, "{refresh:?}");
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
        assert!(block.bytes >= FLUSH_BYTES);
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
