// Project:   dfe-clickhouse
// File:      src/error.rs
// Purpose:   Crate error enum, upstream mapping and retry classification
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Contains [`Error`] and corresponding [`Result`].

use serde::{de, ser};
use std::{error::Error as StdError, fmt, io, result, str::Utf8Error};

/// A result with a specified [`Error`] type.
pub type Result<T, E = Error> = result::Result<T, E>;

type BoxedError = Box<dyn StdError + Send + Sync>;

/// Represents all possible errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum Error {
    #[error("invalid params: {0}")]
    InvalidParams(#[source] BoxedError),
    #[error("network error: {0}")]
    Network(#[source] BoxedError),
    #[error("compression error: {0}")]
    Compression(#[source] BoxedError),
    #[error("decompression error: {0}")]
    Decompression(#[source] BoxedError),
    #[error("no rows returned by a query that expected to return at least one row")]
    RowNotFound,
    #[error("sequences must have a known size ahead of time")]
    SequenceMustHaveLength,
    #[error("`deserialize_any` is not supported")]
    DeserializeAnyNotSupported,
    #[error("not enough data, probably a row type mismatches a database schema")]
    NotEnoughData,
    #[error("string is not valid utf8")]
    InvalidUtf8Encoding(#[from] Utf8Error),
    #[error("tag for enum is not valid")]
    InvalidTagEncoding(usize),
    #[error("max number of types in the Variant data type is 255, got {0}")]
    VariantDiscriminatorIsOutOfBound(usize),
    #[error("a custom error message from serde: {0}")]
    Custom(String),
    /// Background worker task exited; the inserter is no longer
    /// accepting commands. Terminal: construct a fresh inserter,
    /// don't retry the same handle.
    #[error("background worker task has exited; the inserter is no longer accepting commands")]
    WorkerExited,
    /// Structured server-side exception parsed from the response.
    /// Preferred over [`Error::BadResponse`] when the server returned
    /// a recognisable `Code: NNN. DB::Exception: ...` body. Callers
    /// can match on `code` for typed handling and use
    /// [`Error::is_retriable`] for conservative retry classification.
    #[error("server error code {code}: {message}")]
    ServerException {
        /// `X-ClickHouse-Exception-Code` from the response header.
        /// Signed `int` on the wire (cpp-client + server source);
        /// codes are small positives in practice.
        code: i32,
        /// Exception name extracted from `(UPPERCASE_NAME)` near
        /// the end of the message. `None` when the parser couldn't
        /// recognise it (older CH versions, custom error paths).
        name: Option<String>,
        /// Cleaned message body, stripped of the `Code: N.`,
        /// `DB::Exception:` prefix, exception-name parens, and
        /// `(version ...)` suffix.
        message: String,
        /// Server-side stack trace if `Stack trace:` was present in
        /// the body.
        stack_trace: Option<String>,
    },
    #[error("bad response: {0}")]
    BadResponse(String),
    /// A transport-level failure that a fresh attempt may not hit --
    /// a pool-acquire timeout, a closed pool, a hook failure. Typed so
    /// retry classification does not have to match on message text.
    #[error("transient transport failure: {0}")]
    Transient(String),
    /// A connection could not be opened: an unresolvable host, a host
    /// that resolved to no addresses, an empty endpoint list. Typed for
    /// the same reason as [`Error::Transient`].
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("timeout expired")]
    TimedOut,
    #[error("error while parsing columns header from the response: {0}")]
    InvalidColumnsHeader(#[source] BoxedError),
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
    /// A well-formed value this crate cannot decode or print as the server
    /// does. The same query fails the same way, so it is not retriable.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// A dynamic-insert failure, kept typed so a caller can tell a stale
    /// schema (re-fetch and retry) from a permanent rejection.
    #[cfg(feature = "dynamic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "dynamic")))]
    #[error(transparent)]
    Dynamic(#[from] crate::dynamic::DynamicError),
    #[error("{0}")]
    Other(BoxedError),
}

// Variant-by-variant where the name matches, `Other` otherwise.
// Upstream's enum is `#[non_exhaustive]`, so a variant added there later
// lands in `Other` until this mapping is extended.
impl From<clickhouse::error::Error> for Error {
    fn from(error: clickhouse::error::Error) -> Self {
        use clickhouse::error::Error as Up;
        match error {
            Up::InvalidParams(e) => Self::InvalidParams(e),
            Up::Network(e) => Self::Network(e),
            Up::Compression(e) => Self::Compression(e),
            Up::Decompression(e) => Self::Decompression(e),
            Up::RowNotFound => Self::RowNotFound,
            Up::SequenceMustHaveLength => Self::SequenceMustHaveLength,
            Up::DeserializeAnyNotSupported => Self::DeserializeAnyNotSupported,
            Up::NotEnoughData => Self::NotEnoughData,
            Up::InvalidUtf8Encoding(e) => Self::InvalidUtf8Encoding(e),
            Up::InvalidTagEncoding(n) => Self::InvalidTagEncoding(n),
            Up::VariantDiscriminatorIsOutOfBound(n) => Self::VariantDiscriminatorIsOutOfBound(n),
            Up::Custom(s) => Self::Custom(s),
            Up::BadResponse(s) => Self::BadResponse(s),
            Up::TimedOut => Self::TimedOut,
            Up::InvalidColumnsHeader(e) => Self::InvalidColumnsHeader(e),
            Up::SchemaMismatch(s) => Self::SchemaMismatch(s),
            Up::Unsupported(s) => Self::Unsupported(s),
            other => Self::Other(Box::new(other)),
        }
    }
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        io::Error::other(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        // Unwraps an `Error` this crate boxed on the way out, so a round trip
        // through `io::Error` keeps the variant.
        match error.downcast::<Self>() {
            Ok(inner) => inner,
            Err(other) => Self::Other(other.into()),
        }
    }
}

impl Error {
    /// Conservative retriability classification.
    ///
    /// Returns `true` for errors that *might* succeed on retry --
    /// transport-level (`Network`, `TimedOut`, `Transient`, `Connect`),
    /// a known set of transient server-side codes, and the dynamic-insert
    /// failures that clear once the schema is re-read.
    ///
    /// Returns `false` for everything else, including unknown server codes:
    /// a caller wanting a more aggressive retry should match on the variant.
    /// The code table is [`is_retriable_code`][Self::is_retriable_code].
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            // `Transient` and `Connect` are only ever built for a
            // condition a fresh attempt may not hit, so they classify
            // here rather than on their message text.
            Self::Network(_) | Self::TimedOut | Self::Transient(_) | Self::Connect(_) => true,
            Self::ServerException { code, .. } => Self::is_retriable_code(*code),
            // A stale, missing or unreadable schema is fixed by re-reading it;
            // an unsupported type or a bad value is not.
            #[cfg(feature = "dynamic")]
            Self::Dynamic(e) => e.is_retriable(),
            _ => false,
        }
    }

    /// ClickHouse error codes considered transient for retry. See
    /// `src/Common/ErrorCodes.cpp` upstream for the canonical list.
    /// Conservative -- false negatives are expected, false positives
    /// should be rare. Each entry names its upstream macro.
    #[must_use]
    pub fn is_retriable_code(code: i32) -> bool {
        matches!(
            code,
            // TIMEOUT_EXCEEDED
            159 |
            // TOO_SLOW (merge queue lag; usually transient)
            160 |
            // TOO_MANY_SIMULTANEOUS_QUERIES
            202 |
            // SOCKET_TIMEOUT
            209 |
            // NETWORK_ERROR
            210 |
            // ABORTED (operator KILL or shutdown -- replayable on
            // another node)
            236 |
            // MEMORY_LIMIT_EXCEEDED (often transient under brief
            // memory pressure)
            241 |
            // TOO_MANY_PARTS (transient under heavy ingest)
            252 |
            // ALL_CONNECTION_TRIES_FAILED
            279 |
            // LIMIT_EXCEEDED (transient quota)
            290 |
            // UNKNOWN_STATUS_OF_INSERT (the "did my INSERT land?"
            // ambiguity code -- documented mitigation is retry with
            // an insert_deduplication_token, which the
            // batch_isolation token variant ships)
            319 |
            // RECEIVED_ERROR_TOO_MANY_REQUESTS (server proxied a 429
            // from a downstream resource -- S3, Kafka, HTTP storage)
            364 |
            // PART_IS_TEMPORARILY_LOCKED (mutate/merge contention)
            384 |
            // CANNOT_SCHEDULE_TASK (task scheduler full)
            439 |
            // DEADLOCK_AVOIDED (server-side avoidance; succeeds on retry)
            473 |
            // UNKNOWN_STATUS_OF_TRANSACTION (transaction analogue of
            // UNKNOWN_STATUS_OF_INSERT -- same "did it land?" ambiguity)
            659 |
            // TOO_MANY_UNAVAILABLE_SHARDS (transient cluster condition)
            904 |
            // KEEPER_EXCEPTION (replicated-metadata hiccup)
            999
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use std::io;

    #[test]
    fn roundtrip_io_error() {
        let orig = Error::NotEnoughData;

        // Error -> io::Error
        let orig_str = orig.to_string();
        let io = io::Error::from(orig);
        assert_eq!(io.kind(), io::ErrorKind::Other);
        assert_eq!(io.to_string(), orig_str);

        // io::Error -> Error
        let orig = Error::from(io);
        assert!(matches!(orig, Error::NotEnoughData));
    }

    #[test]
    fn error_traits() {
        fn assert_traits<T: std::error::Error + Send + Sync>() {}

        assert_traits::<Error>();
    }

    #[test]
    fn is_retriable_classifies_known_transient_codes() {
        let make = |code: i32| Error::ServerException {
            code,
            name: None,
            message: String::new(),
            stack_trace: None,
        };

        // Sampled from the retriable table -- known transient codes.
        for &code in &[
            159i32, 160, 202, 209, 210, 236, 241, 252, 279, 290, 319, 364, 384, 439, 473, 659, 904,
            999,
        ] {
            assert!(make(code).is_retriable(), "code {code} should be retriable");
        }

        // Sampled known-not-retriable codes (auth, schema, parse).
        for &code in &[36i32, 60, 117, 192, 195, 469] {
            assert!(
                !make(code).is_retriable(),
                "code {code} should NOT be retriable"
            );
        }
    }

    #[test]
    fn is_retriable_for_transport_errors() {
        assert!(Error::TimedOut.is_retriable());
        assert!(Error::Transient("pool acquire timed out".into()).is_retriable());
        assert!(Error::Connect("no address for host".into()).is_retriable());
        // A foreign `io::Error` lands in `Other`, which stays terminal.
        let foreign: Error = io::Error::new(io::ErrorKind::ConnectionReset, "reset").into();
        assert!(!foreign.is_retriable());
    }

    #[test]
    fn is_retriable_code_defaults_unknown_to_not_retriable() {
        // A ClickHouse release can ship codes this table has never seen; until
        // it is extended, a caller sees them as terminal rather than looping.
        for &code in &[0i32, 1, 12345, 99999, i32::MAX] {
            assert!(
                !Error::is_retriable_code(code),
                "unknown code {code} should default to NOT retriable"
            );
        }
    }

    /// The consumer branches on this to decide between re-reading the schema
    /// and dead-lettering the batch, so the split has to hold.
    #[cfg(feature = "dynamic")]
    #[test]
    fn dynamic_schema_failures_are_retriable_but_bad_data_is_not() {
        use crate::dynamic::DynamicError;

        let retriable = [
            DynamicError::EmptySchema {
                table: "db.t".into(),
            },
            DynamicError::SchemaMismatch {
                table: "db.t".into(),
                message: "drift".into(),
            },
            DynamicError::SchemaFetch {
                table: "db.t".into(),
                source: clickhouse::error::Error::TimedOut,
            },
            DynamicError::Transport {
                table: "db.t".into(),
                source: Box::new(Error::TimedOut),
            },
        ];
        for e in retriable {
            assert!(Error::from(e).is_retriable());
        }

        let terminal = [
            DynamicError::UnsupportedType {
                column: "c".into(),
                type_str: "Point".into(),
            },
            DynamicError::EncodingError {
                column: "c".into(),
                message: "UInt8 value out of range".into(),
            },
        ];
        for e in terminal {
            assert!(!Error::from(e).is_retriable());
        }
    }

    /// `#[error(transparent)]`, so the wrapper adds no prefix and `source()`
    /// reaches the original query failure.
    #[cfg(feature = "dynamic")]
    #[test]
    fn dynamic_errors_pass_through_display_and_source() {
        use std::error::Error as _;

        let err = Error::from(crate::dynamic::DynamicError::SchemaFetch {
            table: "db.t".into(),
            source: clickhouse::error::Error::TimedOut,
        });
        assert_eq!(
            err.to_string(),
            "failed to fetch schema for 'db.t': timeout expired"
        );
        // `transparent` forwards `source()`, so the query failure is reachable.
        let source = err.source().expect("the query failure is the source");
        assert_eq!(source.to_string(), "timeout expired");
    }
}
