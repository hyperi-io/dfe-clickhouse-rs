// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-clickhouse
// File:      src/dynamic/error.rs
// Purpose:   DynamicError for schema-driven inserts
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Error types for dynamic (schema-driven) inserts.

/// Errors specific to dynamic schema-driven inserts.
///
/// `#[non_exhaustive]`: a caller matching on this must carry a `_` arm. The
/// variants stay constructible so a consumer can build one in its own tests.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DynamicError {
    /// Column type string could not be parsed.
    #[error("unsupported type '{type_str}' for column '{column}'")]
    UnsupportedType {
        /// Column the type was declared on.
        column: String,
        /// The type string as `system.columns` reported it.
        type_str: String,
    },
    /// Value could not be encoded for the target column type.
    #[error("encoding error for column '{column}': {message}")]
    EncodingError {
        /// Column the value belonged to; empty for a whole-row failure.
        column: String,
        /// What the encoder could not do.
        message: String,
    },
    /// Schema mismatch detected -- server rejected the insert. Retriable once
    /// the cached schema is invalidated.
    #[error("schema mismatch for table '{table}': {message}")]
    SchemaMismatch {
        /// Fully qualified `database.table`.
        table: String,
        /// The server's rejection text.
        message: String,
    },
    /// Schema fetch from `system.columns` failed. Retriable: a cold cache
    /// after a restart hits this on a transport blip.
    #[error("failed to fetch schema for '{table}': {source}")]
    SchemaFetch {
        /// Fully qualified `database.table`.
        table: String,
        /// The query failure on either transport, kept typed so a caller can
        /// classify it, and boxed because [`crate::Error`] holds a
        /// [`DynamicError`].
        #[source]
        source: Box<crate::Error>,
    },
    /// Table has no columns (or does not exist). Retriable against a cluster
    /// that is still replicating the DDL.
    #[error("table '{table}' has no columns or does not exist")]
    EmptySchema {
        /// Fully qualified `database.table`.
        table: String,
    },
    /// The insert failed in transit or on a transient server condition (a
    /// timeout, a failed connect, a connection the server dropped, a server
    /// code worth retrying). Nothing is wrong with the rows, so the batch is
    /// retriable and must not be dead-lettered.
    #[error("transport failure inserting into '{table}': {source}")]
    Transport {
        /// Fully qualified `database.table`.
        table: String,
        /// The underlying failure, boxed because [`crate::Error`] holds a
        /// [`DynamicError`].
        #[source]
        source: Box<crate::Error>,
    },
}

impl DynamicError {
    /// Whether a fresh attempt may succeed: a transport failure, or a stale,
    /// missing or unreadable schema that a re-read fixes. An unsupported type,
    /// a bad value and a schema-valid rejection are not.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::Transport { .. }
                | Self::SchemaFetch { .. }
                | Self::SchemaMismatch { .. }
                | Self::EmptySchema { .. }
        )
    }

    /// Whether the schema read or the insert ran past its client-side bound,
    /// for a caller that widens the bound on the next attempt.
    #[must_use]
    pub fn is_timed_out(&self) -> bool {
        matches!(
            self,
            Self::SchemaFetch { source, .. } | Self::Transport { source, .. }
                if matches!(**source, crate::Error::TimedOut)
        )
    }
}
