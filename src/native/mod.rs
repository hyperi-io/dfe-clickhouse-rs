//! `ClickHouse` Native columnar block format -- the `format=Native` payload
//! encoding, independent of the transport that carries it.
//!
//! HOT PATH. Every row in and out of the server passes through here, so this
//! module is held to a 90% line-coverage target rather than the repo's 80%
//! floor, and every file in it meets that today. `docs/coverage.md` carries
//! the numbers and how to measure them. Adding a wire format here means adding
//! the test with it, or the next reader inherits a claim that is no longer
//! true.
//!
//! Upstream's `src/rowbinary/` is row-oriented; this module is the columnar
//! primitive that lets the server write blocks into its merge tree without a
//! rows-to-columns transpose.
//!
//! # Module map
//!
//! - `columns`: type-name parser and the column reader that re-serialises
//!   native cells as `RowBinary`.
//! - `encode`: INSERT-block encoder, shared by the HTTP and TCP transports.
//! - `decode`: SELECT-block decoder producing the typed column buffers
//!   [`DecodedBlock`] carries.
//! - `sparse`: sparse-column wire format (offset list + non-default values).
//! - `io`: varint and length-prefixed-string helpers over
//!   [`tokio::io::AsyncRead`]/[`tokio::io::AsyncWrite`] and [`bytes::BufMut`].
//!
//! The TCP transport in `src/tcp/` is a separate wire protocol that happens to
//! carry Native-format data blocks; HTTP carries the same blocks under
//! `format=Native`.

// The reader and decoder exist for the TCP transport; a build without it
// carries the codec with no in-tree caller. Scoped to that build so dead code
// still warns in the default one.
#![cfg_attr(not(feature = "tcp"), allow(dead_code))]
// Every name in this module's docs is a Rust or wire type, so the crate-root
// relaxation for README prose is reversed here.
#![warn(clippy::doc_markdown)]

// Private, like upstream's own `src/native/` keeps `BlockReader` and
// `BlockWriter`. The types a caller needs are re-exported below, so the
// module layout stays free to move without a breaking change.
pub(crate) mod columns;
pub(crate) mod decode;
pub(crate) mod encode;
pub(crate) mod io;
pub(crate) mod sparse;
pub(crate) mod type_name;

pub use columns::ColumnType;
pub use decode::{DecodedBlock, DecodedColumn, FromColumn};
pub use encode::{ColumnSchema, encode_columns};
