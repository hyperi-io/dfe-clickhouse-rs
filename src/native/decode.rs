//! Native columnar block decoder -- the inverse of
//! [`crate::native::encode_columns`], mirroring [`NativeReader.cpp`].
//!
//! Reads a Native-format data block off any [`ClickHouseRead`] source into a
//! [`DecodedBlock`]: one [`DecodedColumn`] per declared column, holding typed
//! value buffers the cursor can index row by row without a second transpose.
//! `Variant`, `Dynamic` and `JSON` columns arrive as
//! [`DecodedColumn::Json`], one document per row, rendered by
//! [`crate::native::columns::read_column`]. A `Point` arrives as the
//! `Tuple(Float64, Float64)` it is on the wire (the geo shapes as arrays of
//! it), `BFloat16` widened to `Float32`, `Time` as `Int32` seconds and
//! `Time64` as `Int64` ticks at the column's precision.
//!
//! [`NativeReader.cpp`]: https://github.com/ClickHouse/ClickHouse/blob/master/src/Formats/NativeReader.cpp

use std::pin::Pin;

use tokio::io::AsyncReadExt;

use crate::error::{Error, Result};
use crate::native::columns::{self, ColumnType};
use crate::native::io::{ClickHouseRead, read_exact_grown, with_cap};

/// Upper bound on composite-type nesting `decode_column` will descend,
/// backstopping the parse-time guard in [`crate::native::columns::ColumnType`]
/// for a type built outside the parser.
const MAX_DECODE_DEPTH: usize = 32;

/// Hard cap on the `num_rows` a Data packet header may declare. The count is a
/// server-controlled varuint that sizes every per-column buffer in the block.
const MAX_BLOCK_ROWS: u64 = 1 << 28;

/// Hard cap on the `num_columns` a Data packet header may declare.
const MAX_BLOCK_COLUMNS: u64 = 1 << 16;

/// Bytes a block's sparse columns may expand to, summed over them. A dense
/// column is backed byte for byte by the wire, but a sparse one's defaults are
/// not: a few bytes can declare 2^28 rows of `UInt256`, an 8 GiB zero fill.
pub(crate) const MAX_SPARSE_EXPANSION_BYTES: usize = 1 << 30;

/// Reject a length a server declared but cannot back with payload.
fn refused(what: &str, got: u64, cap: u64) -> Error {
    Error::BadResponse(format!(
        "native: block declares {got} {what}, above the {cap} cap"
    ))
}

/// Fixed-width little-endian scalar decoded a whole column at a time: one
/// `read_exact` and one conversion pass, rather than an await point per row.
trait LeScalar: Sized + Copy + Default {
    const WIDTH: usize;
    fn from_le_slice(bytes: &[u8]) -> Self;
}

macro_rules! impl_le_scalar {
    ($($t:ty),* $(,)?) => {
        $(
            impl LeScalar for $t {
                const WIDTH: usize = std::mem::size_of::<$t>();
                #[inline]
                fn from_le_slice(bytes: &[u8]) -> Self {
                    // The sole caller feeds `chunks_exact(WIDTH)`, so the
                    // fallback arm is unreachable.
                    bytes
                        .try_into()
                        .map_or_else(|_| Self::default(), <$t>::from_le_bytes)
                }
            }
        )*
    };
}

impl_le_scalar!(u16, i16, u32, i32, u64, i64, u128, i128, f32, f64);

/// Read `n` little-endian `T` values in bulk: one `read_exact` of the
/// whole column followed by a single conversion pass.
async fn read_le_column<R: ClickHouseRead, T: LeScalar>(r: &mut R, n: usize) -> Result<Vec<T>> {
    let total = n.checked_mul(T::WIDTH).ok_or_else(|| {
        Error::BadResponse("native: numeric column byte length overflows usize".into())
    })?;
    let raw = read_exact_grown(r, total).await?;
    let mut out = with_cap(n)?;
    // `as_chunks` wants a literal width and `T::WIDTH` is an associated const,
    // so the const-generic form does not compile here.
    out.extend(raw.chunks_exact(T::WIDTH).map(T::from_le_slice));
    Ok(out)
}

/// Read `n` raw 32-byte little-endian values in bulk (the backing store
/// for `Int256`/`UInt256`/`Decimal256`, which have no native Rust
/// scalar). One `read_exact` + a chunked copy.
async fn read_u256_column<R: ClickHouseRead>(r: &mut R, n: usize) -> Result<Vec<[u8; 32]>> {
    let total = n.checked_mul(32).ok_or_else(|| {
        Error::BadResponse("native: 256-bit column byte length overflows usize".into())
    })?;
    let raw = read_exact_grown(r, total).await?;
    let mut out = with_cap(n)?;
    out.extend(raw.as_chunks::<32>().0.iter().copied());
    Ok(out)
}

/// One column of a decoded Native block, runtime-typed.
///
/// Scalar variants hold contiguous buffers in the columnar layout the wire
/// produced; composite variants box their child columns.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DecodedColumn {
    // Numeric scalars.
    /// `UInt8` values in wire order -- `Bool` decodes here too.
    UInt8(Vec<u8>),
    /// `UInt16` values in wire order.
    UInt16(Vec<u16>),
    /// `UInt32` values in wire order.
    UInt32(Vec<u32>),
    /// `UInt64` values in wire order.
    UInt64(Vec<u64>),
    /// `Int8` values in wire order -- `Enum8` decodes here too, because its
    /// ordinals are signed.
    Int8(Vec<i8>),
    /// `Int16` values in wire order -- `Enum16` decodes here too.
    Int16(Vec<i16>),
    /// `Int32` values in wire order.
    Int32(Vec<i32>),
    /// `Int64` values in wire order.
    Int64(Vec<i64>),
    /// `Int128` values in wire order.
    Int128(Vec<i128>),
    /// `UInt128` values in wire order.
    UInt128(Vec<u128>),
    /// 256-bit integers carried as raw little-endian 32-byte values
    /// (Rust has no native i256/u256). Callers interpret as needed.
    Int256(Vec<[u8; 32]>),
    /// `UInt256` values as raw little-endian 32-byte blocks.
    UInt256(Vec<[u8; 32]>),
    /// `Float32` values in wire order.
    Float32(Vec<f32>),
    /// `Float64` values in wire order.
    Float64(Vec<f64>),

    /// `Decimal(P, S)` decoded as its backing little-endian integer
    /// (32/64/128/256-bit by precision). `precision` (total digits) and
    /// `scale` (fractional digits) are carried from the column type --
    /// the value is `backing / 10^scale`. They are NOT on the wire; the
    /// decoder lifts them from the parsed `ColumnType`.
    Decimal32 {
        /// Total decimal digits, from the column type.
        precision: u8,
        /// Fractional digits, from the column type.
        scale: u8,
        /// Backing `Int32` values in wire order.
        values: Vec<i32>,
    },
    /// `Decimal(P, S)` on an `Int64` backing integer.
    Decimal64 {
        /// Total decimal digits, from the column type.
        precision: u8,
        /// Fractional digits, from the column type.
        scale: u8,
        /// Backing `Int64` values in wire order.
        values: Vec<i64>,
    },
    /// `Decimal(P, S)` on an `Int128` backing integer.
    Decimal128 {
        /// Total decimal digits, from the column type.
        precision: u8,
        /// Fractional digits, from the column type.
        scale: u8,
        /// Backing `Int128` values in wire order.
        values: Vec<i128>,
    },
    /// `Decimal(P, S)` on an `Int256` backing integer.
    Decimal256 {
        /// Total decimal digits, from the column type.
        precision: u8,
        /// Fractional digits, from the column type.
        scale: u8,
        /// Backing `Int256` values as raw little-endian 32-byte blocks.
        values: Vec<[u8; 32]>,
    },

    // Variable-length scalars.
    /// `String` values as raw bytes -- `ClickHouse` does not guarantee UTF-8.
    String(Vec<Vec<u8>>),
    /// One JSON document per row, as UTF-8 text with no length prefix. Carries
    /// `JSON`, `Object('json')`, `Variant` and `Dynamic` columns, whose wire
    /// shapes have no single Rust type.
    Json(Vec<Vec<u8>>),
    /// `FixedString(N)` rows concatenated -- row `i` is `bytes[i * width..][..width]`.
    FixedString {
        /// `N` from the column type, in bytes.
        width: usize,
        /// Every row concatenated, `width` bytes each.
        bytes: Vec<u8>,
    },

    // Date / time.
    /// `Date`: unsigned days since the Unix epoch.
    Date(Vec<u16>),
    /// `Date32`: signed days since the Unix epoch.
    Date32(Vec<i32>),
    /// `DateTime`: unsigned seconds since the Unix epoch.
    DateTime(Vec<u32>),
    /// `DateTime64`: Int64 ticks at `precision` sub-second digits, with the
    /// optional IANA `timezone` from the type name. Both come from the
    /// parsed `ColumnType`, not the wire.
    DateTime64 {
        /// Sub-second digits, from the column type.
        precision: u8,
        /// IANA timezone from the column type, absent when the type omits it.
        timezone: Option<String>,
        /// `Int64` tick counts in wire order.
        values: Vec<i64>,
    },

    // Network.
    /// `UUID` values as their 16 wire bytes.
    Uuid(Vec<[u8; 16]>),
    /// `IPv4` addresses as their backing `UInt32`.
    Ipv4(Vec<u32>),
    /// `IPv6` addresses as their 16 wire bytes.
    Ipv6(Vec<[u8; 16]>),

    // Composites.
    /// `mask[i] == 1` => row `i` is null; `child[i]` still exists with a
    /// placeholder value (matching the Native wire shape).
    Nullable {
        /// One byte per row -- 1 marks the row NULL.
        mask: Vec<u8>,
        /// Values for every row, null slots included.
        child: Box<DecodedColumn>,
    },
    /// Cumulative end-offsets; row `i` spans `child[offsets[i-1]..offsets[i]]`.
    Array {
        /// Cumulative element end-offsets, one per row.
        offsets: Vec<u64>,
        /// Flat element column indexed by `offsets`.
        child: Box<DecodedColumn>,
    },
    /// Per-block dictionary + per-row indices. Resolved at access time;
    /// the cursor narrows to `child[indices[row]]` when iterating.
    LowCardinality {
        /// Per-block dictionary -- the inner type with `Nullable` stripped.
        dict: Box<DecodedColumn>,
        /// One unsigned dictionary index per row, at the width the block declared.
        indices: Box<DecodedColumn>,
        /// True when the LC inner type is `Nullable(T)`. Dictionary
        /// index 0 then represents NULL.
        is_nullable_inner: bool,
    },
    /// One decoded column per tuple field, in declaration order.
    Tuple(Vec<DecodedColumn>),
    /// `Map(K, V)` as cumulative pair offsets over flat key and value columns.
    Map {
        /// Cumulative key-value pair end-offsets, one per row.
        offsets: Vec<u64>,
        /// Flat key column indexed by `offsets`.
        keys: Box<DecodedColumn>,
        /// Flat value column indexed by `offsets`.
        values: Box<DecodedColumn>,
    },
}

impl DecodedColumn {
    /// Number of logical rows this column carries.
    ///
    /// For composite variants the count is the OUTER row count, not
    /// the cumulative child-element count.
    // One arm per variant: merging the identical bodies would fold
    // unrelated wire types (`UInt32`, `DateTime`, `IPv4`) into one pattern.
    #[allow(clippy::match_same_arms)]
    #[must_use]
    pub fn row_count(&self) -> usize {
        match self {
            Self::UInt8(v) => v.len(),
            Self::UInt16(v) => v.len(),
            Self::UInt32(v) => v.len(),
            Self::UInt64(v) => v.len(),
            Self::Int8(v) => v.len(),
            Self::Int16(v) => v.len(),
            Self::Int32(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Int128(v) => v.len(),
            Self::UInt128(v) => v.len(),
            Self::Int256(v) => v.len(),
            Self::UInt256(v) => v.len(),
            Self::Float32(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::Decimal32 { values, .. } => values.len(),
            Self::Decimal64 { values, .. } => values.len(),
            Self::Decimal128 { values, .. } => values.len(),
            Self::Decimal256 { values, .. } => values.len(),
            Self::String(v) | Self::Json(v) => v.len(),
            Self::FixedString { width, bytes } => bytes.len().checked_div(*width).unwrap_or(0),
            Self::Date(v) => v.len(),
            Self::Date32(v) => v.len(),
            Self::DateTime(v) => v.len(),
            Self::DateTime64 { values, .. } => values.len(),
            Self::Uuid(v) => v.len(),
            Self::Ipv4(v) => v.len(),
            Self::Ipv6(v) => v.len(),
            Self::Nullable { mask, .. } => mask.len(),
            Self::Array { offsets, .. } => offsets.len(),
            Self::LowCardinality { indices, .. } => indices.row_count(),
            Self::Tuple(fields) => fields.first().map_or(0, Self::row_count),
            Self::Map { offsets, .. } => offsets.len(),
        }
    }

    /// Scatter a sparsely-serialised column back to its full length.
    ///
    /// A sparse column carries only its non-default values; `positions[i]` is
    /// the row the `i`th of them belongs to. Everything else is the type's
    /// default, which on the wire means zero, empty, or the epoch.
    ///
    /// ClickHouse sparse-serialises scalars only, so a composite arriving this
    /// way is a wire the decoder does not understand and says so rather than
    /// guessing.
    ///
    /// # Errors
    ///
    /// [`Error::BadResponse`] if a position is past `num_rows`, if the value
    /// count does not match the position count, or for a composite column.
    pub(crate) fn expand_sparse(
        self,
        positions: &[usize],
        num_rows: usize,
        budget: &mut usize,
    ) -> Result<Self> {
        if self.row_count() != positions.len() {
            return Err(Error::BadResponse(format!(
                "native: sparse column has {} values for {} positions",
                self.row_count(),
                positions.len()
            )));
        }
        if let Some(&past) = positions.iter().find(|&&p| p >= num_rows) {
            return Err(Error::BadResponse(format!(
                "native: sparse position {past} is past the block's {num_rows} rows"
            )));
        }

        macro_rules! scatter {
            ($variant:ident, $values:expr, $default:expr) => {
                Self::$variant(scatter_values(
                    $values, positions, num_rows, $default, budget,
                )?)
            };
        }

        Ok(match self {
            Self::UInt8(v) => scatter!(UInt8, v, 0),
            Self::UInt16(v) => scatter!(UInt16, v, 0),
            Self::UInt32(v) => scatter!(UInt32, v, 0),
            Self::UInt64(v) => scatter!(UInt64, v, 0),
            Self::Int8(v) => scatter!(Int8, v, 0),
            Self::Int16(v) => scatter!(Int16, v, 0),
            Self::Int32(v) => scatter!(Int32, v, 0),
            Self::Int64(v) => scatter!(Int64, v, 0),
            Self::Int128(v) => scatter!(Int128, v, 0),
            Self::UInt128(v) => scatter!(UInt128, v, 0),
            Self::Int256(v) => scatter!(Int256, v, [0u8; 32]),
            Self::UInt256(v) => scatter!(UInt256, v, [0u8; 32]),
            Self::Float32(v) => scatter!(Float32, v, 0.0),
            Self::Float64(v) => scatter!(Float64, v, 0.0),
            Self::String(v) => scatter!(String, v, Vec::new()),
            Self::Json(v) => scatter!(Json, v, Vec::new()),
            Self::Date(v) => scatter!(Date, v, 0),
            Self::Date32(v) => scatter!(Date32, v, 0),
            Self::DateTime(v) => scatter!(DateTime, v, 0),
            Self::Ipv4(v) => scatter!(Ipv4, v, 0),
            Self::Uuid(v) => scatter!(Uuid, v, [0u8; 16]),
            Self::Ipv6(v) => scatter!(Ipv6, v, [0u8; 16]),
            Self::Decimal32 {
                precision,
                scale,
                values,
            } => Self::Decimal32 {
                precision,
                scale,
                values: scatter_values(values, positions, num_rows, 0i32, budget)?,
            },
            Self::Decimal64 {
                precision,
                scale,
                values,
            } => Self::Decimal64 {
                precision,
                scale,
                values: scatter_values(values, positions, num_rows, 0i64, budget)?,
            },
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => Self::Decimal128 {
                precision,
                scale,
                values: scatter_values(values, positions, num_rows, 0i128, budget)?,
            },
            Self::Decimal256 {
                precision,
                scale,
                values,
            } => Self::Decimal256 {
                precision,
                scale,
                values: scatter_values(values, positions, num_rows, [0u8; 32], budget)?,
            },
            Self::DateTime64 {
                precision,
                timezone,
                values,
            } => Self::DateTime64 {
                precision,
                timezone,
                values: scatter_values(values, positions, num_rows, 0i64, budget)?,
            },
            Self::FixedString { width, bytes } => {
                // Both factors are wire-derived, so the product is checked --
                // a release build wraps it and then indexes past the buffer.
                let total = num_rows.checked_mul(width).ok_or_else(|| {
                    Error::BadResponse(
                        "native: sparse FixedString byte length overflows usize".into(),
                    )
                })?;
                spend_sparse_budget(budget, total)?;
                let mut out = with_cap(total)?;
                out.resize(total, 0u8);
                for (i, &row) in positions.iter().enumerate() {
                    out[row * width..][..width].copy_from_slice(&bytes[i * width..][..width]);
                }
                Self::FixedString { width, bytes: out }
            }
            other => {
                return Err(Error::BadResponse(format!(
                    "native: {} cannot be sparse-serialised",
                    other.type_label()
                )));
            }
        })
    }

    /// Variant name for an error message.
    fn type_label(&self) -> &'static str {
        match self {
            Self::Nullable { .. } => "a Nullable column",
            Self::Array { .. } => "an Array column",
            Self::LowCardinality { .. } => "a LowCardinality column",
            Self::Tuple(_) => "a Tuple column",
            Self::Map { .. } => "a Map column",
            _ => "this column",
        }
    }
}

/// Scatter `values` into a full-length column, `values[i]` landing at
/// `positions[i]` and every other row taking `default`.
///
/// `num_rows` is a count the server declared, so the buffer is sized through
/// the fallible [`with_cap`] rather than `vec![_; n]`, which commits the whole
/// claim and aborts the process when the allocator cannot back it. Callers
/// check every position against `num_rows` first, so the indexing cannot panic.
fn scatter_values<T: Clone>(
    values: Vec<T>,
    positions: &[usize],
    num_rows: usize,
    default: T,
    budget: &mut usize,
) -> Result<Vec<T>> {
    spend_sparse_budget(budget, num_rows.saturating_mul(std::mem::size_of::<T>()))?;
    // The reservation covers the full length, so `resize` cannot reallocate.
    let mut out = with_cap(num_rows)?;
    out.resize(num_rows, default);
    for (value, &row) in values.into_iter().zip(positions) {
        out[row] = value;
    }
    Ok(out)
}

/// Take `bytes` of sparse expansion from what the block has left, or refuse it.
fn spend_sparse_budget(budget: &mut usize, bytes: usize) -> Result<()> {
    *budget = budget.checked_sub(bytes).ok_or_else(|| {
        Error::BadResponse(format!(
            "native: sparse columns would expand past the {MAX_SPARSE_EXPANSION_BYTES}-byte \
             per-block cap"
        ))
    })?;
    Ok(())
}

/// A decoded Native data block: ordered list of columns plus the
/// `(name, type_name)` schema the server announced.
///
/// The schema is copied per block so one cursor survives the mid-stream
/// schema renegotiation an INSERT-SELECT can emit.
#[derive(Debug, Clone)]
pub struct DecodedBlock {
    /// One decoded column per schema entry, in wire order.
    pub columns: Vec<DecodedColumn>,
    /// `(name, type_name)` per column, as the server announced them.
    pub schema: Vec<(String, String)>,
    /// Row count the Data packet header declared.
    pub num_rows: u64,
}

impl DecodedBlock {
    /// The column the server announced under `name`. `None` for a column
    /// the block does not declare, and for the header block, which
    /// declares its columns and carries no values.
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&DecodedColumn> {
        let index = self.schema.iter().position(|(n, _)| n == name)?;
        self.columns.get(index)
    }

    /// Every value of column `name`, converted to `T`. The header block and
    /// the server's trailing empty block both yield an empty vector, so this
    /// flat-maps across a whole result set without filtering.
    ///
    /// # Errors
    ///
    /// [`Error::SchemaMismatch`] if a block that declares columns does not
    /// declare `name`, or if the column's wire type does not read as `T`.
    pub fn column_as<T: FromColumn>(&self, name: &str) -> Result<Vec<T>> {
        // A result set ends with a block that declares nothing and carries
        // nothing (`TCPHandler.cpp`, `sendData(state, {})`).
        if self.schema.is_empty() {
            return Ok(Vec::new());
        }
        let index = self
            .schema
            .iter()
            .position(|(n, _)| n == name)
            .ok_or_else(|| {
                Error::SchemaMismatch(format!("native: block declares no column '{name}'"))
            })?;
        match self.columns.get(index) {
            Some(column) => T::from_column(column, name, &self.schema[index].1),
            None => Ok(Vec::new()),
        }
    }
}

/// Typed read of one [`DecodedColumn`].
///
/// Implemented for every scalar backing the decoder produces, for
/// `Option<T>` wherever `T` has a route, and for `String` / `Vec<u8>`
/// (which also carry `JSON`, `Variant` and `Dynamic` document text).
/// Composites -- `Array`, `Map`, `Tuple`, `LowCardinality` -- have no
/// flat Rust shape, so they go through [`DecodedBlock::column`] and match
/// on the variant directly.
pub trait FromColumn: Sized {
    /// Convert every value in `column`. `name` and `type_name` come from
    /// the block schema and appear in the mismatch error.
    ///
    /// # Errors
    ///
    /// [`Error::SchemaMismatch`] if `column` holds a different wire type,
    /// or [`Error::InvalidUtf8Encoding`] if a `String` column carries
    /// bytes that are not UTF-8.
    fn from_column(column: &DecodedColumn, name: &str, type_name: &str) -> Result<Vec<Self>>;
}

fn wrong_type(name: &str, type_name: &str, expected: &str) -> Error {
    Error::SchemaMismatch(format!(
        "native: column '{name}' is {type_name}, which does not read as {expected}"
    ))
}

/// A `JSON`, `Variant` or `Dynamic` column reads as its per-row document text.
impl FromColumn for String {
    fn from_column(column: &DecodedColumn, name: &str, type_name: &str) -> Result<Vec<Self>> {
        match column {
            DecodedColumn::String(values) | DecodedColumn::Json(values) => values
                .iter()
                .map(|v| Ok(std::str::from_utf8(v)?.to_owned()))
                .collect(),
            _ => Err(wrong_type(name, type_name, "String")),
        }
    }
}

/// Raw `String` bytes -- `ClickHouse` does not guarantee UTF-8.
impl FromColumn for Vec<u8> {
    fn from_column(column: &DecodedColumn, name: &str, type_name: &str) -> Result<Vec<Self>> {
        match column {
            DecodedColumn::String(values) | DecodedColumn::Json(values) => Ok(values.clone()),
            _ => Err(wrong_type(name, type_name, "String")),
        }
    }
}

/// One impl per Rust type, listing every `DecodedColumn` variant that has
/// that backing. A `ClickHouse` type is not always its own variant --
/// `Date` is `UInt16`, `IPv4` is `UInt32`, a `Decimal(P, S)` is its backing
/// integer -- so a route reads the backing and the caller applies the
/// meaning, with the scale and timezone available from
/// [`DecodedBlock::column`].
///
/// Each row names its own binder because a `pat_param` fragment is opaque:
/// an identifier bound inside it belongs to the call site, so a `values`
/// written in this macro's body would not be the same one.
macro_rules! from_column_backing {
    ($(
        $(#[$meta:meta])*
        $rust:ty, $bind:ident, $label:literal { $( $pattern:pat_param ),+ $(,)? }
    )*) => {
        $(
            $(#[$meta])*
            impl FromColumn for $rust {
                fn from_column(
                    column: &DecodedColumn,
                    name: &str,
                    type_name: &str,
                ) -> Result<Vec<Self>> {
                    match column {
                        $( $pattern )|+ => Ok($bind.clone()),
                        _ => Err(wrong_type(name, type_name, $label)),
                    }
                }
            }
        )*
    };
}

from_column_backing! {
    u8, values, "UInt8" { DecodedColumn::UInt8(values) }
    /// `Date` is days since the epoch on a `UInt16` backing.
    u16, values, "UInt16" {
        DecodedColumn::UInt16(values),
        DecodedColumn::Date(values),
    }
    /// `DateTime` (epoch seconds) and `IPv4` share the `UInt32` backing.
    u32, values, "UInt32" {
        DecodedColumn::UInt32(values),
        DecodedColumn::DateTime(values),
        DecodedColumn::Ipv4(values),
    }
    u64, values, "UInt64" { DecodedColumn::UInt64(values) }
    u128, values, "UInt128" { DecodedColumn::UInt128(values) }
    i8, values, "Int8" { DecodedColumn::Int8(values) }
    i16, values, "Int16" { DecodedColumn::Int16(values) }
    /// `Date32` is signed days since the epoch; `Decimal32` its backing.
    i32, values, "Int32" {
        DecodedColumn::Int32(values),
        DecodedColumn::Date32(values),
        DecodedColumn::Decimal32 { values, .. },
    }
    /// `DateTime64` ticks and `Decimal64` backings are both `Int64`.
    i64, values, "Int64" {
        DecodedColumn::Int64(values),
        DecodedColumn::DateTime64 { values, .. },
        DecodedColumn::Decimal64 { values, .. },
    }
    i128, values, "Int128" {
        DecodedColumn::Int128(values),
        DecodedColumn::Decimal128 { values, .. },
    }
    f32, values, "Float32" { DecodedColumn::Float32(values) }
    f64, values, "Float64" { DecodedColumn::Float64(values) }
    /// 256-bit values have no native Rust type; the raw little-endian
    /// blocks are handed back unchanged.
    [u8; 32], values, "a 256-bit column" {
        DecodedColumn::Int256(values),
        DecodedColumn::UInt256(values),
        DecodedColumn::Decimal256 { values, .. },
    }
    /// `UUID` and `IPv6` are both 16 raw wire bytes.
    [u8; 16], values, "a 16-byte column" {
        DecodedColumn::Uuid(values),
        DecodedColumn::Ipv6(values),
    }
}

/// `Bool` is `UInt8` on the wire; any non-zero byte is true.
impl FromColumn for bool {
    fn from_column(column: &DecodedColumn, name: &str, type_name: &str) -> Result<Vec<Self>> {
        match column {
            DecodedColumn::UInt8(values) => Ok(values.iter().map(|&b| b != 0).collect()),
            _ => Err(wrong_type(name, type_name, "Bool")),
        }
    }
}

/// Any route that works on `T` works on `Nullable(T)`.
///
/// The Native wire carries a value for every row, null slots included, so
/// the child converts whole and the mask then selects.
impl<T: FromColumn> FromColumn for Option<T> {
    fn from_column(column: &DecodedColumn, name: &str, type_name: &str) -> Result<Vec<Self>> {
        let DecodedColumn::Nullable { mask, child } = column else {
            return Err(wrong_type(name, type_name, "a Nullable column"));
        };
        let values = T::from_column(child, name, type_name)?;
        Ok(mask
            .iter()
            .zip(values)
            .map(|(&is_null, v)| if is_null == 0 { Some(v) } else { None })
            .collect())
    }
}

/// Consume a block's info section: `(field_id varuint, value)` pairs then a
/// zero terminator, per `NativeWriter.cpp:126` `block.info.write` and the cpp
/// `ReadBlock` at 854-877. `NativeWriter` emits it whenever the client
/// revision is above zero, so both transports read it here.
///
/// The values mean nothing to a non-distributed client, but the field ids are
/// asserted rather than skipped: a misaligned or hostile encoder then surfaces
/// as a clean error instead of a silently mis-parsed block. Field 3
/// (`out_of_order_buckets`) appears only above the 54459 pin, so a revision
/// bump past 54480 must revisit this.
///
/// # Errors
///
/// [`Error::BadResponse`] on an unexpected field id or terminator.
pub(crate) async fn read_block_info<R: ClickHouseRead>(r: &mut R) -> Result<()> {
    let field1 = r.read_var_uint().await?;
    if field1 != 1 {
        return Err(Error::BadResponse(format!(
            "native: block info field id {field1} (expected 1 = is_overflows)"
        )));
    }
    let _is_overflows = r.read_u8().await?;
    let field2 = r.read_var_uint().await?;
    if field2 != 2 {
        return Err(Error::BadResponse(format!(
            "native: block info field id {field2} (expected 2 = bucket_num)"
        )));
    }
    let _bucket_num = r.read_i32_le().await?;
    let terminator = r.read_var_uint().await?;
    if terminator != 0 {
        return Err(Error::BadResponse(format!(
            "native: block info terminator {terminator} (expected 0)"
        )));
    }
    Ok(())
}

/// Read a column's custom-serialization header and report whether it is sparse.
///
/// TWO bytes, not one. `NativeWriter::write` writes the `has_custom_serialization`
/// bool, and then -- only when it is set -- `SerializationInfo::serialializeKindStackBinary`
/// writes a SECOND byte naming the kind stack. Reading the flag alone and
/// treating 1 as "sparse" leaves the kind byte on the wire, and one byte of
/// drift turns the next column's offsets into garbage; the symptom is an
/// "unknown server packet id" several columns later, nowhere near the cause.
///
/// The kind byte's first four values predate kind stacks, so a stack of exactly
/// `{Default, Sparse}` reads as 1 on every server that has ever sent sparse.
/// Everything past that is a serialisation we cannot decode, so it is named and
/// refused rather than guessed at.
async fn read_serialization_kind<R: ClickHouseRead>(r: &mut R, name: &str) -> Result<bool> {
    match r.read_u8().await? {
        0 => return Ok(false),
        1 => {}
        flag => {
            return Err(Error::BadResponse(format!(
                "native: column '{name}' has custom-serialization flag {flag} \
                 -- the server writes a bool here"
            )));
        }
    }

    // KindStackBinarySerializationType, from SerializationInfo.cpp.
    let kind = match r.read_u8().await? {
        0 => return Ok(false), // {Default} -- the flag was set, the stack is plain
        1 => return Ok(true),  // {Default, Sparse}
        2 => "Detached",
        3 => "Detached over Sparse",
        4 => "Replicated",
        5 => {
            // A combination stack: varuint length, then one byte per kind.
            // Consumed so the message can be specific, not to keep reading --
            // an unsupported serialisation ends the query either way.
            let num_kinds = r.read_var_uint().await?;
            if num_kinds > MAX_BLOCK_COLUMNS {
                return Err(refused("serialization kinds", num_kinds, MAX_BLOCK_COLUMNS));
            }
            for _ in 0..num_kinds {
                let _kind = r.read_u8().await?;
            }
            "a combination"
        }
        other => {
            return Err(Error::BadResponse(format!(
                "native: column '{name}' announced serialization kind {other}, \
                 which this decoder does not know"
            )));
        }
    };
    Err(Error::BadResponse(format!(
        "native: column '{name}' uses {kind} serialization -- \
         only default and sparse are supported"
    )))
}

/// Read one Native-format data block body off the wire.
///
/// Stream pointer position on entry MUST be immediately after the
/// `num_columns` + `num_rows` varuint pair the caller already consumed
/// from the block header. The decoder consumes exactly the column-payload
/// bytes for `num_columns` columns at `num_rows` rows; on return the stream
/// pointer is aligned for whatever the transport puts next.
///
/// `server_revision` decides whether the per-column custom-serialization
/// flag byte is on the wire -- 25.x servers are always above the gate.
///
/// A block at `num_rows == 0` declares its columns and carries no payload for
/// any of them, the per-type serialization prefix included.
///
/// # Errors
///
/// - [`Error::BadResponse`] if a column header is malformed (varuint
///   overflow, length cap exceeded) or a composite-column offset list
///   is non-monotonic.
/// - I/O errors from the underlying reader propagate untouched.
pub(crate) async fn decode_block<R: ClickHouseRead>(
    r: &mut R,
    num_columns: u64,
    num_rows: u64,
    server_revision: u64,
) -> Result<DecodedBlock> {
    let has_custom_ser = server_revision
        >= crate::native::encode::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;

    // Both counts are varuints the server chose; five bytes of header can ask
    // for terabytes of column buffer, so they are capped before anything is
    // sized from them.
    if num_columns > MAX_BLOCK_COLUMNS {
        return Err(refused("columns", num_columns, MAX_BLOCK_COLUMNS));
    }
    if num_rows > MAX_BLOCK_ROWS {
        return Err(refused("rows", num_rows, MAX_BLOCK_ROWS));
    }
    let column_count = usize::try_from(num_columns).unwrap_or(0);

    let mut schema = with_cap(column_count)?;
    let mut columns = with_cap(column_count)?;
    let mut sparse_budget = MAX_SPARSE_EXPANSION_BYTES;

    for _ in 0..num_columns {
        let name = r.read_utf8_string().await?;
        let type_name = r.read_utf8_string().await?;
        let sparse = if has_custom_ser {
            read_serialization_kind(r, &name).await?
        } else {
            false
        };

        let col = match ColumnType::parse(&type_name) {
            Some(ct) => {
                // A header block declares its columns and carries no bytes
                // for any of them, the serialisation prefix included.
                let mut prefixes = Vec::new();
                if num_rows > 0 {
                    decode_prefixes(r, &ct, 0, &mut prefixes).await?;
                }
                let mut headers = prefixes.into_iter();
                if sparse && num_rows > 0 {
                    // Offsets first, then only the non-default values, so the
                    // dense read is sized by the offset count rather than the
                    // block's row count.
                    let rows = usize::try_from(num_rows).unwrap_or(usize::MAX);
                    let mut state = crate::native::sparse::SparseDeserializeState::default();
                    let positions =
                        crate::native::sparse::read_sparse_offsets(r, rows, &mut state).await?;
                    let present = u64::try_from(positions.len()).unwrap_or(u64::MAX);
                    let dense = decode_column(r, &ct, present, server_revision, 0, &mut headers)
                        .await
                        .map_err(|e| name_the_column(&name, e))?;
                    dense.expand_sparse(&positions, rows, &mut sparse_budget)?
                } else {
                    decode_column(r, &ct, num_rows, server_revision, 0, &mut headers)
                        .await
                        .map_err(|e| name_the_column(&name, e))?
                }
            }
            None => {
                // The stream pointer cannot advance past a type of unknown
                // size, so the connection is poisoned rather than desynced.
                return Err(Error::BadResponse(format!(
                    "native: server announced unknown column type '{type_name}' for column '{name}'"
                )));
            }
        };

        schema.push((name, type_name));
        columns.push(col);
    }

    Ok(DecodedBlock {
        columns,
        schema,
        num_rows,
    })
}

/// An [`Error::Unsupported`] from a column's values, with the column named.
fn name_the_column(name: &str, e: Error) -> Error {
    match e {
        Error::Unsupported(m) => Error::Unsupported(format!("column '{name}': {m}")),
        other => other,
    }
}

/// Consume the serialisation prefixes for `col_type`'s whole tree, in the
/// order the server writes them.
///
/// `NativeWriter.cpp:93-94` calls `serializeBinaryBulkStatePrefix` for the
/// entire column before `serializeBinaryBulkWithMultipleStreams`, so every
/// nested prefix precedes ALL of that column's data. Reading a prefix inline
/// where its child sits agrees with that only when nothing is written ahead of
/// the child: true inside a `Tuple`, false behind an `Array`'s offsets.
///
/// `LowCardinality` alone is hoisted here, because its prefix is a fixed
/// 8-byte version this decoder discards. `Dynamic`, `Variant` and `JSON`
/// carry variable-length prefixes the data phase reads values out of, so they
/// stay inline and are still wrong when nested behind data -- see the
/// `known_broken` cases in `tests/wire_docker.rs`.
fn decode_prefixes<'a, R: ClickHouseRead + 'a>(
    r: &'a mut R,
    col_type: &'a ColumnType,
    depth: usize,
    headers: &'a mut Vec<columns::DynamicHeader>,
) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_DECODE_DEPTH {
            return Err(Error::BadResponse(format!(
                "native: column type nesting exceeds {MAX_DECODE_DEPTH} levels"
            )));
        }
        match col_type {
            // The dictionary is part of the data phase, so this does not
            // recurse into the inner type.
            ColumnType::LowCardinality(_) => {
                let _version = r.read_u64_le().await?;
            }
            // `SerializationVariant.cpp:161-182`: the discriminator mode,
            // then every element's prefix.
            ColumnType::Variant(variant_types) => {
                columns::read_variant_mode(r).await?;
                for variant in variant_types {
                    decode_prefixes(r, variant, depth + 1, headers).await?;
                }
            }
            ColumnType::Nullable(inner)
            | ColumnType::Array(inner)
            | ColumnType::SimpleAggregateFunction(inner) => {
                decode_prefixes(r, inner, depth + 1, headers).await?;
            }
            ColumnType::Tuple(fields) => {
                for field in fields {
                    decode_prefixes(r, field, depth + 1, headers).await?;
                }
            }
            ColumnType::Map(key, value) => {
                decode_prefixes(r, key, depth + 1, headers).await?;
                decode_prefixes(r, value, depth + 1, headers).await?;
            }
            // `SerializationDynamic.cpp:128-168`: the structure version, then
            // the type list, then the inner Variant's mode. The type list is
            // the one prefix a data phase reads values back out of, so it is
            // carried rather than just consumed.
            ColumnType::Dynamic => {
                let version = r.read_u64_le().await?;
                let has_max_types = match version {
                    1 => true,
                    2 => false,
                    // Only at the top level, where nothing sits between this
                    // prefix and the column's own data.
                    3 if depth == 0 => {
                        headers.push(columns::DynamicHeader::flattened());
                        return Ok(());
                    }
                    3 => {
                        return Err(Error::Unsupported(
                            "a nested Dynamic in the flattened serialisation is not read. \
                             Leave output_format_native_use_flattened_dynamic_and_json_serialization off"
                                .to_owned(),
                        ));
                    }
                    other => {
                        return Err(Error::BadResponse(format!(
                            "native: unsupported Dynamic serialization version: {other}"
                        )));
                    }
                };
                headers.push(columns::read_dynamic_v1v2_prefix(r, has_max_types).await?);
            }
            _ => {}
        }
        Ok(())
    })
}

/// The zero-row shape of `col_type`, for a block that carries no column bytes.
fn empty_column(col_type: &ColumnType) -> DecodedColumn {
    match col_type {
        ColumnType::UInt8 | ColumnType::Bool => DecodedColumn::UInt8(Vec::new()),
        ColumnType::Int8 | ColumnType::Enum8 => DecodedColumn::Int8(Vec::new()),
        ColumnType::UInt16 => DecodedColumn::UInt16(Vec::new()),
        ColumnType::Int16 | ColumnType::Enum16 => DecodedColumn::Int16(Vec::new()),
        ColumnType::UInt32 => DecodedColumn::UInt32(Vec::new()),
        ColumnType::Int32 | ColumnType::Time => DecodedColumn::Int32(Vec::new()),
        ColumnType::UInt64 => DecodedColumn::UInt64(Vec::new()),
        ColumnType::Int64 | ColumnType::Time64 => DecodedColumn::Int64(Vec::new()),
        ColumnType::Int128 => DecodedColumn::Int128(Vec::new()),
        ColumnType::UInt128 => DecodedColumn::UInt128(Vec::new()),
        ColumnType::Int256 => DecodedColumn::Int256(Vec::new()),
        ColumnType::UInt256 => DecodedColumn::UInt256(Vec::new()),
        ColumnType::Float32 | ColumnType::BFloat16 => DecodedColumn::Float32(Vec::new()),
        ColumnType::Float64 => DecodedColumn::Float64(Vec::new()),
        ColumnType::Decimal32 { precision, scale } => DecodedColumn::Decimal32 {
            precision: *precision,
            scale: *scale,
            values: Vec::new(),
        },
        ColumnType::Decimal64 { precision, scale } => DecodedColumn::Decimal64 {
            precision: *precision,
            scale: *scale,
            values: Vec::new(),
        },
        ColumnType::Decimal128 { precision, scale } => DecodedColumn::Decimal128 {
            precision: *precision,
            scale: *scale,
            values: Vec::new(),
        },
        ColumnType::Decimal256 { precision, scale } => DecodedColumn::Decimal256 {
            precision: *precision,
            scale: *scale,
            values: Vec::new(),
        },
        ColumnType::String => DecodedColumn::String(Vec::new()),
        ColumnType::Json
        | ColumnType::NewJson
        | ColumnType::TypedJson
        | ColumnType::Variant(_)
        | ColumnType::Dynamic => DecodedColumn::Json(Vec::new()),
        ColumnType::FixedString(width) => DecodedColumn::FixedString {
            width: *width,
            bytes: Vec::new(),
        },
        ColumnType::Date => DecodedColumn::Date(Vec::new()),
        ColumnType::Date32 => DecodedColumn::Date32(Vec::new()),
        ColumnType::DateTime => DecodedColumn::DateTime(Vec::new()),
        ColumnType::DateTime64 {
            precision,
            timezone,
        } => DecodedColumn::DateTime64 {
            precision: *precision,
            timezone: timezone.clone(),
            values: Vec::new(),
        },
        ColumnType::Uuid => DecodedColumn::Uuid(Vec::new()),
        ColumnType::IPv4 => DecodedColumn::Ipv4(Vec::new()),
        ColumnType::IPv6 => DecodedColumn::Ipv6(Vec::new()),
        ColumnType::Nullable(inner) => DecodedColumn::Nullable {
            mask: Vec::new(),
            child: Box::new(empty_column(inner)),
        },
        ColumnType::Array(inner) => DecodedColumn::Array {
            offsets: Vec::new(),
            child: Box::new(empty_column(inner)),
        },
        ColumnType::Map(key_type, val_type) => DecodedColumn::Map {
            offsets: Vec::new(),
            keys: Box::new(empty_column(key_type)),
            values: Box::new(empty_column(val_type)),
        },
        ColumnType::Tuple(fields) => {
            DecodedColumn::Tuple(fields.iter().map(empty_column).collect())
        }
        ColumnType::LowCardinality(inner) => {
            // The dictionary on the wire is the inner type with `Nullable`
            // stripped; slot 0 is then the null sentinel.
            let dict_type = match inner.as_ref() {
                ColumnType::Nullable(t) => t.as_ref(),
                other => other,
            };
            DecodedColumn::LowCardinality {
                dict: Box::new(empty_column(dict_type)),
                indices: Box::new(DecodedColumn::UInt8(Vec::new())),
                is_nullable_inner: matches!(inner.as_ref(), ColumnType::Nullable(_)),
            }
        }
        ColumnType::SimpleAggregateFunction(inner) => empty_column(inner),
        ColumnType::Point => DecodedColumn::Tuple(vec![
            DecodedColumn::Float64(Vec::new()),
            DecodedColumn::Float64(Vec::new()),
        ]),
    }
}

/// Strip the `RowBinary` length prefix from cells the column reader emits as
/// length-prefixed strings, leaving the JSON document bytes.
fn unwrap_json_cells(cells: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>> {
    cells
        .into_iter()
        .map(|cell| {
            let (len, header) = crate::native::io::get_var_uint(&cell)?;
            let end = usize::try_from(len)
                .ok()
                .and_then(|len| header.checked_add(len))
                .filter(|end| *end <= cell.len())
                .ok_or_else(|| {
                    Error::BadResponse("native: JSON cell length runs past the cell".into())
                })?;
            Ok(cell[header..end].to_vec())
        })
        .collect()
}

/// Decode one column's values into a [`DecodedColumn`].
///
/// `depth` bounds the recursion at [`MAX_DECODE_DEPTH`]; the boxed future
/// breaks the async-fn cycle Rust rejects for infinite future size.
// One exhaustive arm per `ColumnType`: splitting it would scatter the wire
// dispatch across functions with no shared reader state.
#[allow(clippy::too_many_lines)]
fn decode_column<'a, R: ClickHouseRead + 'a>(
    r: &'a mut R,
    col_type: &'a ColumnType,
    num_rows: u64,
    server_revision: u64,
    depth: usize,
    headers: &'a mut std::vec::IntoIter<columns::DynamicHeader>,
) -> Pin<Box<dyn std::future::Future<Output = Result<DecodedColumn>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_DECODE_DEPTH {
            return Err(Error::BadResponse(format!(
                "native: column type nesting exceeds {MAX_DECODE_DEPTH} levels"
            )));
        }
        // `NativeWriter` writes a top-level column's payload only when the
        // block has rows, the serialization prefix included; a nested
        // sub-column at zero elements still carries its prefix.
        if depth == 0 && num_rows == 0 {
            return Ok(empty_column(col_type));
        }

        let n = usize::try_from(num_rows).map_err(|_| {
            Error::BadResponse(format!(
                "native: row count {num_rows} exceeds platform usize"
            ))
        })?;

        match col_type {
            // Bool decodes as its wire byte, which `get::<bool>` reads.
            ColumnType::UInt8 | ColumnType::Bool => {
                let buf = read_exact_grown(r, n).await?;
                Ok(DecodedColumn::UInt8(buf))
            }
            // An `Enum8` ordinal is signed, so it decodes as `Int8` rather
            // than as its unsigned wire byte.
            ColumnType::Int8 | ColumnType::Enum8 => {
                let raw = read_exact_grown(r, n).await?;
                // Reinterpret as i8 without an extra copy: i8 and u8
                // have identical layout, the cast is lossless and the
                // Vec capacity matches.
                // An `Int8` column is two's-complement on the wire, so wrapping
                // the top bit into the sign is the decode, not a defect.
                #[allow(clippy::cast_possible_wrap)]
                let buf: Vec<i8> = raw.into_iter().map(|b| b as i8).collect();
                Ok(DecodedColumn::Int8(buf))
            }
            ColumnType::UInt16 => Ok(DecodedColumn::UInt16(read_le_column::<_, u16>(r, n).await?)),
            ColumnType::Int16 | ColumnType::Enum16 => {
                Ok(DecodedColumn::Int16(read_le_column::<_, i16>(r, n).await?))
            }
            ColumnType::UInt32 => Ok(DecodedColumn::UInt32(read_le_column::<_, u32>(r, n).await?)),
            // `Time` is signed seconds on the wire.
            ColumnType::Int32 | ColumnType::Time => {
                Ok(DecodedColumn::Int32(read_le_column::<_, i32>(r, n).await?))
            }
            ColumnType::UInt64 => Ok(DecodedColumn::UInt64(read_le_column::<_, u64>(r, n).await?)),
            // `Time64` is signed ticks at the column's precision.
            ColumnType::Int64 | ColumnType::Time64 => {
                Ok(DecodedColumn::Int64(read_le_column::<_, i64>(r, n).await?))
            }
            ColumnType::Int128 => Ok(DecodedColumn::Int128(
                read_le_column::<_, i128>(r, n).await?,
            )),
            ColumnType::UInt128 => Ok(DecodedColumn::UInt128(
                read_le_column::<_, u128>(r, n).await?,
            )),
            ColumnType::Int256 => Ok(DecodedColumn::Int256(read_u256_column(r, n).await?)),
            ColumnType::UInt256 => Ok(DecodedColumn::UInt256(read_u256_column(r, n).await?)),
            ColumnType::Float32 => Ok(DecodedColumn::Float32(
                read_le_column::<_, f32>(r, n).await?,
            )),
            ColumnType::Float64 => Ok(DecodedColumn::Float64(
                read_le_column::<_, f64>(r, n).await?,
            )),
            // Precision and scale come from the type name, not the wire, and
            // are surfaced so callers read `backing / 10^scale` directly.
            ColumnType::Decimal32 { precision, scale } => Ok(DecodedColumn::Decimal32 {
                precision: *precision,
                scale: *scale,
                values: read_le_column::<_, i32>(r, n).await?,
            }),
            ColumnType::Decimal64 { precision, scale } => Ok(DecodedColumn::Decimal64 {
                precision: *precision,
                scale: *scale,
                values: read_le_column::<_, i64>(r, n).await?,
            }),
            ColumnType::Decimal128 { precision, scale } => Ok(DecodedColumn::Decimal128 {
                precision: *precision,
                scale: *scale,
                values: read_le_column::<_, i128>(r, n).await?,
            }),
            ColumnType::Decimal256 { precision, scale } => Ok(DecodedColumn::Decimal256 {
                precision: *precision,
                scale: *scale,
                values: read_u256_column(r, n).await?,
            }),
            ColumnType::String => {
                let mut buf = with_cap(n)?;
                for _ in 0..n {
                    // read_string applies the MAX_STRING_SIZE cap.
                    buf.push(r.read_string().await?);
                }
                Ok(DecodedColumn::String(buf))
            }
            // Legacy Object('json') is a plain String on the wire carrying one
            // document per row.
            ColumnType::Json => {
                let mut buf = with_cap(n)?;
                for _ in 0..n {
                    buf.push(r.read_string().await?);
                }
                Ok(DecodedColumn::Json(buf))
            }
            ColumnType::FixedString(width) => {
                let total = width.checked_mul(n).ok_or_else(|| {
                    Error::BadResponse("native: FixedString block overflow".into())
                })?;
                let bytes = read_exact_grown(r, total).await?;
                Ok(DecodedColumn::FixedString {
                    width: *width,
                    bytes,
                })
            }
            ColumnType::Date => Ok(DecodedColumn::Date(read_le_column::<_, u16>(r, n).await?)),
            ColumnType::Date32 => Ok(DecodedColumn::Date32(read_le_column::<_, i32>(r, n).await?)),
            ColumnType::DateTime => Ok(DecodedColumn::DateTime(
                read_le_column::<_, u32>(r, n).await?,
            )),
            ColumnType::DateTime64 {
                precision,
                timezone,
            } => {
                // Int64 ticks on the wire; precision and timezone come from
                // the type name and are surfaced for formatting.
                Ok(DecodedColumn::DateTime64 {
                    precision: *precision,
                    timezone: timezone.clone(),
                    values: read_le_column::<_, i64>(r, n).await?,
                })
            }
            ColumnType::Uuid => {
                let mut buf = with_cap(n)?;
                for _ in 0..n {
                    let mut slot = [0u8; 16];
                    r.read_exact(&mut slot).await?;
                    buf.push(slot);
                }
                Ok(DecodedColumn::Uuid(buf))
            }
            ColumnType::IPv4 => Ok(DecodedColumn::Ipv4(read_le_column::<_, u32>(r, n).await?)),
            ColumnType::IPv6 => {
                let mut buf = with_cap(n)?;
                for _ in 0..n {
                    let mut slot = [0u8; 16];
                    r.read_exact(&mut slot).await?;
                    buf.push(slot);
                }
                Ok(DecodedColumn::Ipv6(buf))
            }
            ColumnType::Nullable(inner) => {
                let mask = read_exact_grown(r, n).await?;
                let child =
                    decode_column(r, inner, num_rows, server_revision, depth + 1, headers).await?;
                Ok(DecodedColumn::Nullable {
                    mask,
                    child: Box::new(child),
                })
            }
            ColumnType::Array(inner) => {
                let mut offsets = with_cap(n)?;
                let mut prev: u64 = 0;
                for _ in 0..n {
                    let end = r.read_u64_le().await?;
                    if end < prev {
                        return Err(Error::BadResponse(
                            "native: Array column offsets are not monotonically increasing".into(),
                        ));
                    }
                    prev = end;
                    offsets.push(end);
                }
                let total = offsets.last().copied().unwrap_or(0);
                let child =
                    decode_column(r, inner, total, server_revision, depth + 1, headers).await?;
                Ok(DecodedColumn::Array {
                    offsets,
                    child: Box::new(child),
                })
            }
            ColumnType::Tuple(fields) => {
                let mut decoded_fields = with_cap(fields.len())?;
                for field in fields {
                    decoded_fields.push(
                        decode_column(r, field, num_rows, server_revision, depth + 1, headers)
                            .await?,
                    );
                }
                Ok(DecodedColumn::Tuple(decoded_fields))
            }
            ColumnType::Map(key_type, val_type) => {
                let mut offsets = with_cap(n)?;
                let mut prev: u64 = 0;
                for _ in 0..n {
                    let end = r.read_u64_le().await?;
                    if end < prev {
                        return Err(Error::BadResponse(
                            "native: Map column offsets are not monotonically increasing".into(),
                        ));
                    }
                    prev = end;
                    offsets.push(end);
                }
                let total = offsets.last().copied().unwrap_or(0);
                let keys =
                    decode_column(r, key_type, total, server_revision, depth + 1, headers).await?;
                let values =
                    decode_column(r, val_type, total, server_revision, depth + 1, headers).await?;
                Ok(DecodedColumn::Map {
                    offsets,
                    keys: Box::new(keys),
                    values: Box::new(values),
                })
            }
            ColumnType::LowCardinality(inner) => {
                decode_low_cardinality(r, inner, num_rows, server_revision, depth + 1, headers)
                    .await
            }
            ColumnType::SimpleAggregateFunction(inner) => {
                // Wire-compatible with the inner type T.
                decode_column(r, inner, num_rows, server_revision, depth + 1, headers).await
            }
            ColumnType::NewJson | ColumnType::TypedJson => {
                let version = r.read_u64_le().await?;
                if version == columns::JSON_SERIALIZATION_STRING {
                    let mut buf = with_cap(n)?;
                    for _ in 0..n {
                        buf.push(r.read_string().await?);
                    }
                    return Ok(DecodedColumn::Json(buf));
                }
                if matches!(col_type, ColumnType::TypedJson) {
                    return Err(columns::typed_json_unsupported());
                }
                // The path-based serialisations reassemble into one JSON
                // document per row, length-prefixed as RowBinary strings.
                let cells = columns::read_json_body(r, n, version).await?;
                Ok(DecodedColumn::Json(unwrap_json_cells(cells)?))
            }
            ColumnType::Variant(_) => {
                let cells = columns::read_column_data(r, col_type, num_rows).await?;
                Ok(DecodedColumn::Json(unwrap_json_cells(cells)?))
            }
            // The type list came off the wire in the prefix phase, in this
            // same tree order, so the header at the front of the queue is
            // this column's.
            ColumnType::Dynamic => {
                let header = headers.next().ok_or_else(|| {
                    Error::BadResponse("native: Dynamic column has no serialisation prefix".into())
                })?;
                let cells = columns::read_dynamic_v1v2_body(r, n, &header, depth + 1).await?;
                Ok(DecodedColumn::Json(unwrap_json_cells(cells)?))
            }
            // `Tuple(Float64, Float64)` on the wire: every x, then every y.
            ColumnType::Point => {
                let x = read_le_column::<_, f64>(r, n).await?;
                let y = read_le_column::<_, f64>(r, n).await?;
                Ok(DecodedColumn::Tuple(vec![
                    DecodedColumn::Float64(x),
                    DecodedColumn::Float64(y),
                ]))
            }
            // The top 16 bits of an f32, so widening is exact.
            ColumnType::BFloat16 => Ok(DecodedColumn::Float32(
                read_le_column::<_, u16>(r, n)
                    .await?
                    .into_iter()
                    .map(|bits| f32::from_bits(u32::from(bits) << 16))
                    .collect(),
            )),
        }
    })
}

/// `LowCardinality` column wire shape -- per-block dictionary + indices,
/// byte-for-byte what the encoder writes and the server emits.
///
/// ```text
/// u64    version (== 1)
/// u64    flags  (bit 0-1: index size code; bit 8: NEED_GLOBAL_DICTIONARY;
///                bit 9: HAS_ADDITIONAL_KEYS)
/// optional u64 global_dict_size + values
/// optional u64 additional_keys_size + values
/// u64    num_indices  (== num_rows)
/// num_rows x index_byte_width  (1/2/4/8 depending on bits 0-1)
/// ```
async fn decode_low_cardinality<R: ClickHouseRead>(
    r: &mut R,
    inner: &ColumnType,
    num_rows: u64,
    server_revision: u64,
    depth: usize,
    headers: &mut std::vec::IntoIter<columns::DynamicHeader>,
) -> Result<DecodedColumn> {
    let n = usize::try_from(num_rows).map_err(|_| {
        Error::BadResponse(format!(
            "native: row count {num_rows} exceeds platform usize"
        ))
    })?;

    // The version word is not read here: it is this column's serialisation
    // prefix, consumed by `decode_prefixes` before any of the block's data.
    let flags = r.read_u64_le().await?;
    let index_type = (flags & 0x03) as u8;
    let has_global_dict = (flags & 0x100) != 0;
    let has_additional_keys = (flags & 0x200) != 0;
    // Bit 10 (0x400, NeedUpdateDictionary) tells the client to discard a
    // dictionary cached across blocks; this decoder caches none, so it is
    // deliberately not consulted.

    let (dict_type, is_nullable_inner) = if let ColumnType::Nullable(t) = inner {
        (t.as_ref(), true)
    } else {
        (inner, false)
    };

    // cpp-client and clickhouse-go both reject a global dictionary on the
    // client-server path and require the additional-keys bit; the
    // `crate::native::columns` reader applies the same rule.
    if has_global_dict {
        return Err(Error::BadResponse(
            "native: LowCardinality global dictionary is not supported on the client-server \
             path (only per-block additional keys)"
                .into(),
        ));
    }
    if !has_additional_keys {
        return Err(Error::BadResponse(
            "native: LowCardinality block set neither the additional-keys nor the \
             global-dictionary flag"
                .into(),
        ));
    }
    let additional_keys_size = r.read_u64_le().await?;
    let combined = decode_column(
        r,
        dict_type,
        additional_keys_size,
        server_revision,
        depth,
        headers,
    )
    .await?;

    let num_indices = r.read_u64_le().await?;
    if num_indices != num_rows {
        return Err(Error::BadResponse(format!(
            "native: LowCardinality index count {num_indices} != row count {num_rows}"
        )));
    }

    let indices = match index_type {
        0 => {
            let buf = read_exact_grown(r, n).await?;
            DecodedColumn::UInt8(buf)
        }
        1 => DecodedColumn::UInt16(read_le_column::<_, u16>(r, n).await?),
        2 => DecodedColumn::UInt32(read_le_column::<_, u32>(r, n).await?),
        3 => DecodedColumn::UInt64(read_le_column::<_, u64>(r, n).await?),
        other => {
            return Err(Error::BadResponse(format!(
                "native: LowCardinality index type {other} is not valid (expected 0..=3)"
            )));
        }
    };

    Ok(DecodedColumn::LowCardinality {
        dict: Box::new(combined),
        indices: Box::new(indices),
        is_nullable_inner,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::columns::ColumnType;
    use crate::native::encode::{ColumnSchema, encode_columns};
    use crate::native::io::{ClickHouseBytesWrite, ClickHouseWrite};
    use std::io::Cursor;

    // A current TCP revision, above the custom-serialization gate. Inlined so
    // these tests build without the `tcp` feature.
    const REV: u64 = 54459;

    /// Encode a single-column block body: the column-payload bytes alone, since
    /// `decode_block` takes the `(num_columns, num_rows)` pair as parameters.
    fn encode_one_column_block(name: &str, type_name: &str, rows: &[Vec<u8>]) -> Vec<u8> {
        let schema = ColumnSchema::from_headers(&[(name.to_string(), type_name.to_string())])
            .expect("schema parses");
        encode_columns(rows, &schema, REV).expect("encode succeeds")
    }

    async fn decode_via_cursor(bytes: Vec<u8>, num_rows: u64) -> DecodedBlock {
        let mut cur = Cursor::new(bytes);
        decode_block(&mut cur, 1, num_rows, REV).await.unwrap()
    }

    #[tokio::test]
    async fn roundtrip_uint64() {
        let rows: Vec<Vec<u8>> = (0u64..5).map(|v| v.to_le_bytes().to_vec()).collect();
        let bytes = encode_one_column_block("n", "UInt64", &rows);
        let block = decode_via_cursor(bytes, 5).await;
        match &block.columns[0] {
            DecodedColumn::UInt64(values) => assert_eq!(values, &vec![0u64, 1, 2, 3, 4]),
            other => panic!("expected UInt64, got {other:?}"),
        }
        assert_eq!(block.schema[0], ("n".to_string(), "UInt64".to_string()));
        assert_eq!(block.num_rows, 5);
    }

    /// The serialization the server picks under
    /// `output_format_native_write_json_as_string=1`: u64 version 1, then one
    /// length-prefixed document per row.
    #[tokio::test]
    async fn json_column_reads_as_string() {
        let docs = [r#"{"a":1}"#, r#"{"b":"x"}"#];
        let mut body: Vec<u8> = Vec::new();
        body.put_string(b"j");
        body.put_string(b"JSON");
        body.push(0); // custom-serialization flag
        body.extend_from_slice(&1u64.to_le_bytes());
        for doc in docs {
            body.put_string(doc.as_bytes());
        }

        let block = decode_via_cursor(body, docs.len() as u64).await;
        assert_eq!(block.column_as::<String>("j").unwrap(), docs);
    }

    /// A zero-row block carries no column bytes at all, so the JSON arm must
    /// not reach for a serialization version that is not on the wire.
    #[tokio::test]
    async fn json_header_block_consumes_no_column_bytes() {
        let mut body: Vec<u8> = Vec::new();
        body.put_string(b"j");
        body.put_string(b"JSON");
        body.push(0);

        let block = decode_via_cursor(body, 0).await;
        assert_eq!(
            block.column_as::<String>("j").unwrap(),
            [] as [std::string::String; 0]
        );
    }

    #[tokio::test]
    async fn roundtrip_int32_signed() {
        let rows: Vec<Vec<u8>> = [-3i32, -1, 0, 7, 99]
            .iter()
            .map(|v| v.to_le_bytes().to_vec())
            .collect();
        let bytes = encode_one_column_block("v", "Int32", &rows);
        let block = decode_via_cursor(bytes, 5).await;
        match &block.columns[0] {
            DecodedColumn::Int32(values) => assert_eq!(values, &vec![-3i32, -1, 0, 7, 99]),
            other => panic!("expected Int32, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_string() {
        // RowBinary for String: varuint(len) + bytes.
        let strings = ["", "hello", "wide \u{1F600}", "trailing"];
        let rows: Vec<Vec<u8>> = strings
            .iter()
            .map(|s| {
                let mut v = Vec::new();
                let mut len = s.len() as u64;
                loop {
                    let byte = (len & 0x7F) as u8;
                    len >>= 7;
                    if len == 0 {
                        v.push(byte);
                        break;
                    }
                    v.push(byte | 0x80);
                }
                v.extend_from_slice(s.as_bytes());
                v
            })
            .collect();
        let bytes = encode_one_column_block("s", "String", &rows);
        let block = decode_via_cursor(bytes, strings.len() as u64).await;
        match &block.columns[0] {
            DecodedColumn::String(values) => {
                let decoded: Vec<&str> = values
                    .iter()
                    .map(|v| std::str::from_utf8(v).unwrap())
                    .collect();
                assert_eq!(decoded, strings);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_fixed_string() {
        let raw = b"abcdefghij"; // 10 bytes, two rows of width 5
        let rows: Vec<Vec<u8>> = raw.chunks(5).map(<[u8]>::to_vec).collect();
        let bytes = encode_one_column_block("f", "FixedString(5)", &rows);
        let block = decode_via_cursor(bytes, 2).await;
        match &block.columns[0] {
            DecodedColumn::FixedString { width, bytes } => {
                assert_eq!(*width, 5);
                assert_eq!(bytes, raw);
            }
            other => panic!("expected FixedString, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_nullable_uint8() {
        // RowBinary Nullable is a flag byte, and a value only when not null;
        // the encoder zero-fills the null slot, so the child reads [42, 0, 7].
        let rows: Vec<Vec<u8>> = vec![
            vec![0, 42], // value 42
            vec![1],     // null (flag only, canonical RowBinary)
            vec![0, 7],  // value 7
        ];
        let bytes = encode_one_column_block("n", "Nullable(UInt8)", &rows);
        let block = decode_via_cursor(bytes, 3).await;
        match &block.columns[0] {
            DecodedColumn::Nullable { mask, child } => {
                assert_eq!(mask, &vec![0u8, 1, 0]);
                match child.as_ref() {
                    DecodedColumn::UInt8(values) => {
                        // The middle slot's value is the default (0)
                        // because the encoder zero-fills null slots.
                        assert_eq!(values, &vec![42u8, 0, 7]);
                    }
                    other => panic!("expected UInt8 child, got {other:?}"),
                }
            }
            other => panic!("expected Nullable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_array_uint64() {
        // RowBinary Array<UInt64>: varuint(count) + count x u64 LE.
        let varuint = |mut v: u64, out: &mut Vec<u8>| loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        };
        let arrays: [&[u64]; 3] = [&[1, 2, 3], &[], &[42]];
        let rows: Vec<Vec<u8>> = arrays
            .iter()
            .map(|arr| {
                let mut v = Vec::new();
                varuint(arr.len() as u64, &mut v);
                for x in *arr {
                    v.extend_from_slice(&x.to_le_bytes());
                }
                v
            })
            .collect();
        let bytes = encode_one_column_block("a", "Array(UInt64)", &rows);
        let block = decode_via_cursor(bytes, 3).await;
        match &block.columns[0] {
            DecodedColumn::Array { offsets, child } => {
                assert_eq!(offsets, &vec![3u64, 3, 4]);
                match child.as_ref() {
                    DecodedColumn::UInt64(values) => {
                        assert_eq!(values, &vec![1u64, 2, 3, 42]);
                    }
                    other => panic!("expected UInt64 child, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_lowcardinality_string() {
        // RowBinary for LC(String) is just String RowBinary -- the
        // LC dictionary lives only on the Native wire side.
        let varuint = |mut v: u64, out: &mut Vec<u8>| loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        };
        let strings = ["foo", "bar", "foo", "baz", "bar"];
        let rows: Vec<Vec<u8>> = strings
            .iter()
            .map(|s| {
                let mut v = Vec::new();
                varuint(s.len() as u64, &mut v);
                v.extend_from_slice(s.as_bytes());
                v
            })
            .collect();
        let bytes = encode_one_column_block("lc", "LowCardinality(String)", &rows);
        let block = decode_via_cursor(bytes, 5).await;
        match &block.columns[0] {
            DecodedColumn::LowCardinality {
                dict,
                indices,
                is_nullable_inner,
            } => {
                assert!(!*is_nullable_inner);
                match dict.as_ref() {
                    DecodedColumn::String(dict_strings) => {
                        // The dictionary order is encoder-implementation-
                        // defined; assert membership rather than order.
                        let mut set: std::collections::HashSet<&[u8]> =
                            dict_strings.iter().map(Vec::as_slice).collect();
                        assert!(set.remove(b"foo".as_slice()));
                        assert!(set.remove(b"bar".as_slice()));
                        assert!(set.remove(b"baz".as_slice()));
                    }
                    other => panic!("expected String dict, got {other:?}"),
                }
                // Cross-check by walking the index column and
                // reconstructing the row strings.
                let reconstructed = lc_to_strings(dict.as_ref(), indices.as_ref());
                assert_eq!(reconstructed, strings);
            }
            other => panic!("expected LowCardinality, got {other:?}"),
        }
    }

    fn lc_to_strings(dict: &DecodedColumn, indices: &DecodedColumn) -> Vec<String> {
        let DecodedColumn::String(dict_strings) = dict else {
            panic!("dict must be String")
        };
        let idxs: Vec<usize> = match indices {
            DecodedColumn::UInt8(v) => v.iter().map(|&x| usize::from(x)).collect(),
            DecodedColumn::UInt16(v) => v.iter().map(|&x| usize::from(x)).collect(),
            DecodedColumn::UInt32(v) => v.iter().map(|&x| x as usize).collect(),
            DecodedColumn::UInt64(v) => v.iter().map(|&x| usize::try_from(x).unwrap()).collect(),
            _ => panic!("indices must be unsigned int"),
        };
        idxs.into_iter()
            .map(|i| String::from_utf8(dict_strings[i].clone()).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn rejects_unknown_column_type() {
        // Hand-craft a single-column block body whose type_name is
        // not a recognised ColumnType.
        let mut bytes = Vec::new();
        bytes.write_string(b"x").await.unwrap();
        bytes.write_string(b"DefinitelyNotAType").await.unwrap();
        // Custom-serialisation flag byte for modern revisions.
        bytes.push(0u8);
        let mut cur = Cursor::new(bytes);
        let err = decode_block(&mut cur, 1, 0, REV).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("unknown column type"), "got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// The first byte is a bool, so anything but 0 or 1 means the reader and
    /// the server disagree about where the column header starts.
    #[tokio::test]
    async fn rejects_nonzero_custom_serialization_flag() {
        let mut bytes = Vec::new();
        bytes.write_string(b"x").await.unwrap();
        bytes.write_string(b"UInt8").await.unwrap();
        bytes.push(7u8);
        let mut cur = Cursor::new(bytes);
        let err = decode_block(&mut cur, 1, 0, REV).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("custom-serialization flag"), "got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// Every scalar type ClickHouse will sparse-serialise, scattered through
    /// the real decode path rather than by calling `expand_sparse` directly.
    ///
    /// One value at row 1 of 3. The scatter arms are per-variant, so an arm
    /// that filled the wrong default or dropped the length would put the value
    /// in the wrong row -- readable, plausible, and wrong.
    #[tokio::test]
    async fn every_sparse_scalar_arm_scatters_to_full_length() {
        use crate::native::sparse::END_OF_GRANULE_FLAG;

        let cases: &[(&str, Vec<u8>)] = &[
            ("UInt8", vec![7]),
            ("UInt16", 7u16.to_le_bytes().to_vec()),
            ("UInt32", 7u32.to_le_bytes().to_vec()),
            ("UInt64", 7u64.to_le_bytes().to_vec()),
            ("Int8", vec![0xf9]),
            ("Int16", (-7i16).to_le_bytes().to_vec()),
            ("Int32", (-7i32).to_le_bytes().to_vec()),
            ("Int64", (-7i64).to_le_bytes().to_vec()),
            ("Int128", (-7i128).to_le_bytes().to_vec()),
            ("UInt128", 7u128.to_le_bytes().to_vec()),
            ("Int256", vec![7u8; 32]),
            ("UInt256", vec![7u8; 32]),
            ("Float32", 1.5f32.to_le_bytes().to_vec()),
            ("Float64", 1.5f64.to_le_bytes().to_vec()),
            ("Date", 7u16.to_le_bytes().to_vec()),
            ("Date32", 7i32.to_le_bytes().to_vec()),
            ("DateTime", 7u32.to_le_bytes().to_vec()),
            ("DateTime64(3)", 7i64.to_le_bytes().to_vec()),
            ("Decimal32(9, 2)", 7i32.to_le_bytes().to_vec()),
            ("Decimal64(18, 2)", 7i64.to_le_bytes().to_vec()),
            ("Decimal128(38, 2)", 7i128.to_le_bytes().to_vec()),
            ("IPv4", 7u32.to_le_bytes().to_vec()),
            ("IPv6", vec![7u8; 16]),
            ("UUID", vec![7u8; 16]),
            ("String", vec![1, b'x']),
            ("Enum8('a' = 7)", vec![7]),
            ("Enum16('a' = 7)", 7i16.to_le_bytes().to_vec()),
        ];

        for (type_name, value) in cases {
            let mut bytes = Vec::new();
            bytes.write_string(b"c").await.unwrap();
            bytes.write_string(type_name.as_bytes()).await.unwrap();
            bytes.push(1); // has_custom_serialization
            bytes.push(1); // KindStack {Default, Sparse}
            bytes.put_var_uint(1); // one default, then the value at row 1
            bytes.put_var_uint(1 | END_OF_GRANULE_FLAG); // one trailing default
            bytes.extend_from_slice(value);
            let len = bytes.len() as u64;

            let mut cur = Cursor::new(bytes);
            let block = decode_block(&mut cur, 1, 3, REV)
                .await
                .unwrap_or_else(|e| panic!("{type_name} decodes sparse: {e}"));
            assert_eq!(cur.position(), len, "{type_name} left bytes on the wire");
            assert_eq!(
                block.column("c").unwrap().row_count(),
                3,
                "{type_name} did not scatter to the block's full length"
            );
        }
    }

    /// A zero-row block still declares its columns, and each must come back as
    /// the empty shape of its own type -- a caller reading the schema off a
    /// header block otherwise sees the wrong variant for every column.
    #[tokio::test]
    async fn a_header_block_yields_the_empty_shape_of_every_type() {
        let types = [
            "UInt8",
            "Int8",
            "UInt16",
            "Int16",
            "UInt32",
            "Int32",
            "UInt64",
            "Int64",
            "Int128",
            "UInt128",
            "Int256",
            "UInt256",
            "Float32",
            "Float64",
            "Decimal32(9, 2)",
            "Decimal64(18, 2)",
            "Decimal128(38, 2)",
            "Decimal256(76, 2)",
            "String",
            "FixedString(4)",
            "Date",
            "Date32",
            "DateTime",
            "DateTime64(3)",
            "UUID",
            "IPv4",
            "IPv6",
            "Bool",
            "Enum8('a' = 1)",
            "Enum16('a' = 1)",
            "JSON",
            "Dynamic",
            "Nullable(UInt8)",
            "Array(UInt8)",
            "Tuple(UInt8, String)",
            "Map(String, UInt8)",
            "LowCardinality(String)",
        ];

        for type_name in types {
            let mut bytes = Vec::new();
            bytes.write_string(b"c").await.unwrap();
            bytes.write_string(type_name.as_bytes()).await.unwrap();
            bytes.push(0); // no custom serialisation
            let len = bytes.len() as u64;

            let mut cur = Cursor::new(bytes);
            let block = decode_block(&mut cur, 1, 0, REV)
                .await
                .unwrap_or_else(|e| panic!("{type_name} decodes as a header: {e}"));
            assert_eq!(cur.position(), len, "{type_name} read column bytes");
            assert_eq!(
                block.column("c").unwrap().row_count(),
                0,
                "{type_name} reported rows in a header block"
            );
        }
    }

    /// A combination stack carries its own length, so the bytes are consumed
    /// before the refusal -- a caller that retried on the same connection would
    /// otherwise start reading mid-list.
    #[tokio::test]
    async fn a_combination_serialization_kind_is_consumed_then_refused() {
        let mut bytes = Vec::new();
        bytes.write_string(b"x").await.unwrap();
        bytes.write_string(b"UInt8").await.unwrap();
        bytes.push(1); // has_custom_serialization
        bytes.push(5); // KindStackBinarySerializationType::COMBINATION
        bytes.put_var_uint(3);
        bytes.extend_from_slice(&[0, 1, 2]);
        let trailing = bytes.len() as u64;
        bytes.push(0xAB); // a sentinel the refusal must not have eaten

        let mut cur = Cursor::new(bytes);
        let err = decode_block(&mut cur, 1, 0, REV).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("combination"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
        assert_eq!(cur.position(), trailing, "the kind list was not consumed");
    }

    /// The count is a varuint the peer chose, and it drives a read loop.
    #[tokio::test]
    async fn an_absurd_combination_kind_count_is_refused_without_reading_it() {
        let mut bytes = Vec::new();
        bytes.write_string(b"x").await.unwrap();
        bytes.write_string(b"UInt8").await.unwrap();
        bytes.push(1);
        bytes.push(5);
        bytes.put_var_uint(u64::MAX);

        let mut cur = Cursor::new(bytes);
        let err = decode_block(&mut cur, 1, 0, REV).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("serialization kinds"), "got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// A sparse body whose value count disagrees with its offset count would
    /// scatter into the wrong rows, so it is refused rather than zipped short.
    #[test]
    fn sparse_expansion_refuses_a_value_count_that_does_not_match() {
        let err = DecodedColumn::UInt8(vec![1, 2, 3])
            .expand_sparse(&[0, 2], 4, &mut MAX_SPARSE_EXPANSION_BYTES.clone())
            .unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("3 values for 2 positions"), "got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// A position past the block's rows would index out of bounds, which is a
    /// panic rather than an error unless it is caught here first.
    #[test]
    fn sparse_expansion_refuses_a_position_past_the_block() {
        let err = DecodedColumn::UInt8(vec![7])
            .expand_sparse(&[9], 4, &mut MAX_SPARSE_EXPANSION_BYTES.clone())
            .unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("past the block's 4 rows"), "got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// The scatter buffer is sized from a row count the server declared, and a
    /// `Vec` allocation failure aborts the process rather than erroring, which
    /// a library cannot catch.
    #[test]
    fn sparse_expansion_refuses_a_row_count_the_allocator_cannot_back() {
        let err = DecodedColumn::UInt64(vec![7])
            .expand_sparse(&[0], usize::MAX / 4, &mut usize::MAX.clone())
            .unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("column buffer"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// A sparse column's defaults are not backed by the wire, so a block's
    /// expansion is capped across its columns: one 2^28-row `UInt256`
    /// declaration would be an 8 GiB zero fill, and columns that fit alone
    /// still share the cap.
    #[test]
    fn sparse_expansion_stops_at_the_per_block_budget() {
        let mut budget = MAX_SPARSE_EXPANSION_BYTES;
        let err = DecodedColumn::UInt256(Vec::new())
            .expand_sparse(&[], 1 << 28, &mut budget)
            .unwrap_err();
        assert!(
            matches!(&err, Error::BadResponse(m) if m.contains("per-block cap")),
            "{err:?}"
        );

        // A small cap, so the shared spend shows without a large fill.
        let mut budget = 1_024;
        let rows = 64;
        for _ in 0..2 {
            DecodedColumn::UInt64(Vec::new())
                .expand_sparse(&[], rows, &mut budget)
                .expect("each half fits");
        }
        assert_eq!(budget, 0);
        assert!(
            DecodedColumn::UInt8(Vec::new())
                .expand_sparse(&[], 1, &mut budget)
                .is_err(),
            "the third column finds the cap spent"
        );
    }

    /// `num_rows` and `FixedString`'s width are both wire-derived, and the
    /// width parses unbounded, so their product is checked: unchecked, a
    /// release build wraps it and then indexes past the buffer.
    #[test]
    fn sparse_expansion_refuses_a_fixedstring_byte_length_that_overflows() {
        let err = DecodedColumn::FixedString {
            width: usize::MAX / 2 + 1,
            bytes: Vec::new(),
        }
        .expand_sparse(&[], 2, &mut usize::MAX.clone())
        .unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("overflows usize"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// A kind stack we cannot decode is named, not guessed at -- reading the
    /// body under the wrong serialisation would desync the whole connection.
    #[tokio::test]
    async fn rejects_a_serialization_kind_it_cannot_decode() {
        for (kind, want) in [
            (2u8, "Detached"),
            (3u8, "Detached over Sparse"),
            (4u8, "Replicated"),
            (6u8, "kind 6"),
        ] {
            let mut bytes = Vec::new();
            bytes.write_string(b"x").await.unwrap();
            bytes.write_string(b"UInt8").await.unwrap();
            bytes.push(1u8);
            bytes.push(kind);
            let mut cur = Cursor::new(bytes);
            let err = decode_block(&mut cur, 1, 0, REV).await.unwrap_err();
            match err {
                Error::BadResponse(msg) => assert!(msg.contains(want), "got: {msg}"),
                other => panic!("expected BadResponse, got {other:?}"),
            }
        }
    }

    /// The custom flag can be set over a plain `{Default}` stack, which is not
    /// sparse and whose body is an ordinary dense column.
    #[tokio::test]
    async fn a_custom_flag_over_a_default_kind_stack_reads_dense() {
        let mut bytes = Vec::new();
        bytes.write_string(b"v").await.unwrap();
        bytes.write_string(b"UInt8").await.unwrap();
        bytes.push(1u8); // has_custom_serialization
        bytes.push(0u8); // KindStack {Default}
        bytes.extend_from_slice(&[4u8, 5u8, 6u8]);

        let mut cur = Cursor::new(bytes);
        let block = decode_block(&mut cur, 1, 3, REV).await.unwrap();
        match block.column("v").unwrap() {
            DecodedColumn::UInt8(values) => assert_eq!(values, &[4, 5, 6]),
            other => panic!("expected UInt8, got {other:?}"),
        }
    }

    /// A sparse column carries only its non-default values, so the decoder has
    /// to put them back where they came from and fill the rest with defaults.
    #[tokio::test]
    async fn sparse_column_expands_to_full_rows() {
        use crate::native::sparse::END_OF_GRANULE_FLAG;

        let mut bytes = Vec::new();
        bytes.write_string(b"v").await.unwrap();
        bytes.write_string(b"UInt8").await.unwrap();
        bytes.push(1u8); // has_custom_serialization
        bytes.push(1u8); // KindStack {Default, Sparse}
        // Non-defaults at rows 1 and 3 of 5: one default before each, then one
        // trailing default carrying the end-of-granule flag.
        let mut offsets = Vec::new();
        offsets.put_var_uint(1);
        offsets.put_var_uint(1);
        offsets.put_var_uint(1 | END_OF_GRANULE_FLAG);
        bytes.extend_from_slice(&offsets);
        bytes.extend_from_slice(&[7u8, 9u8]);

        let mut cur = Cursor::new(bytes);
        let block = decode_block(&mut cur, 1, 5, REV).await.unwrap();
        match block.column("v").unwrap() {
            DecodedColumn::UInt8(values) => assert_eq!(values, &[0, 7, 0, 9, 0]),
            other => panic!("expected UInt8, got {other:?}"),
        }
    }

    /// A composite has no sparse form, so the decoder names it rather than
    /// scattering into a shape it invented.
    #[tokio::test]
    async fn a_sparse_composite_is_refused() {
        use crate::native::sparse::END_OF_GRANULE_FLAG;

        let mut bytes = Vec::new();
        bytes.write_string(b"v").await.unwrap();
        bytes.write_string(b"Array(UInt8)").await.unwrap();
        bytes.push(1u8); // has_custom_serialization
        bytes.push(1u8); // KindStack {Default, Sparse}
        let mut offsets = Vec::new();
        offsets.put_var_uint(END_OF_GRANULE_FLAG);
        bytes.extend_from_slice(&offsets);

        let mut cur = Cursor::new(bytes);
        let err = decode_block(&mut cur, 1, 1, REV).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("sparse"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn row_count_matches_inner_buffers() {
        let rows: Vec<Vec<u8>> = (0u32..8).map(|v| v.to_le_bytes().to_vec()).collect();
        let bytes = encode_one_column_block("v", "UInt32", &rows);
        let block = decode_via_cursor(bytes, 8).await;
        assert_eq!(block.columns[0].row_count(), 8);
    }

    #[test]
    fn parse_column_type_via_columns_module() {
        for ty in [
            "UInt8",
            "UInt64",
            "Int32",
            "String",
            "FixedString(8)",
            "Nullable(UInt32)",
            "Array(UInt64)",
            "Tuple(UInt8, String)",
            "Map(String, UInt64)",
            "LowCardinality(String)",
            "LowCardinality(Nullable(String))",
            "DateTime",
            "DateTime64(3)",
            "UUID",
            "IPv4",
            "IPv6",
        ] {
            assert!(ColumnType::parse(ty).is_some(), "{ty}");
        }
    }

    #[tokio::test]
    async fn decode_column_rejects_excessive_nesting() {
        // Array(Array(...Array(UInt8)...)) deeper than the cap, built past the
        // parser, which caps separately. Entry is at depth 1 because the
        // zero-row short circuit only applies to a top-level column; each Array
        // level then reads no offset bytes, so an empty reader drives the
        // recursion all the way to the depth guard.
        let mut ct = ColumnType::UInt8;
        for _ in 0..(MAX_DECODE_DEPTH + 5) {
            ct = ColumnType::Array(Box::new(ct));
        }
        let mut cur = Cursor::new(Vec::new());
        let err = decode_column(&mut cur, &ct, 0, REV, 1, &mut Vec::new().into_iter())
            .await
            .unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("nesting"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lowcardinality_rejects_zero_flags() {
        // A LowCardinality payload with neither the global-dictionary
        // nor additional-keys flag set is a shape no current server
        // emits; the decoder rejects it rather than guessing a layout.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // version
        bytes.extend_from_slice(&0u64.to_le_bytes()); // flags = 0
        let ct = ColumnType::LowCardinality(Box::new(ColumnType::String));
        let mut cur = Cursor::new(bytes);
        let err = decode_column(&mut cur, &ct, 1, REV, 0, &mut Vec::new().into_iter())
            .await
            .unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("neither"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// Build a one-column Native block body (name "c", the given type,
    /// the custom-serialization flag byte, then `payload`) and decode
    /// it. REV is above the custom-serialization gate, so the flag byte
    /// is present on the wire.
    async fn decode_one_typed(type_name: &str, num_rows: u64, payload: &[u8]) -> DecodedColumn {
        let mut buf = Vec::new();
        buf.write_string(b"c").await.unwrap();
        buf.write_string(type_name.as_bytes()).await.unwrap();
        buf.push(0u8); // custom-serialization flag
        buf.extend_from_slice(payload);
        let mut cur = Cursor::new(buf);
        let mut block = decode_block(&mut cur, 1, num_rows, REV).await.unwrap();
        block.columns.pop().unwrap()
    }

    /// `(type, rows, payload, what must decode)`: each reads to values a
    /// caller can use, never a column that holds none.
    #[tokio::test]
    async fn bfloat16_time_time64_and_point_decode_to_values() {
        let mut point = Vec::new();
        for f in [1.5f64, -3.0, 2.5, 4.0] {
            point.extend_from_slice(&f.to_le_bytes());
        }
        // 1.5 is 0x3FC0 in BFloat16, the top half of its f32 bits.
        let bf16 = [0x3FC0u16, 0xC040]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<u8>>();
        let time = [3_600i32, -1]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<u8>>();
        let time64 = [1_500i64]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<u8>>();

        match decode_one_typed("Point", 2, &point).await {
            DecodedColumn::Tuple(fields) => match fields.as_slice() {
                [DecodedColumn::Float64(x), DecodedColumn::Float64(y)] => {
                    assert_eq!(x, &[1.5, -3.0]);
                    assert_eq!(y, &[2.5, 4.0]);
                }
                other => panic!("Point fields: {other:?}"),
            },
            other => panic!("Point: {other:?}"),
        }
        match decode_one_typed("BFloat16", 2, &bf16).await {
            DecodedColumn::Float32(v) => assert_eq!(v, [1.5, -3.0]),
            other => panic!("BFloat16: {other:?}"),
        }
        match decode_one_typed("Time", 2, &time).await {
            DecodedColumn::Int32(v) => assert_eq!(v, [3_600, -1]),
            other => panic!("Time: {other:?}"),
        }
        match decode_one_typed("Time64(3)", 1, &time64).await {
            DecodedColumn::Int64(v) => assert_eq!(v, [1_500]),
            other => panic!("Time64: {other:?}"),
        }
        let ring = [2u64.to_le_bytes().to_vec(), point].concat();
        match decode_one_typed("Ring", 1, &ring).await {
            DecodedColumn::Array { offsets, child } => {
                assert_eq!(offsets, [2]);
                assert!(matches!(*child, DecodedColumn::Tuple(_)), "{child:?}");
            }
            other => panic!("Ring: {other:?}"),
        }
    }

    fn le_bytes_i128(vals: &[i128]) -> Vec<u8> {
        vals.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    #[tokio::test]
    async fn roundtrip_int128() {
        let vals = [1i128, -2, i128::MAX, i128::MIN];
        match decode_one_typed("Int128", vals.len() as u64, &le_bytes_i128(&vals)).await {
            DecodedColumn::Int128(v) => assert_eq!(v, vals),
            other => panic!("expected Int128, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_uint128() {
        let vals = [0u128, 1, u128::MAX];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("UInt128", vals.len() as u64, &payload).await {
            DecodedColumn::UInt128(v) => assert_eq!(v, vals),
            other => panic!("expected UInt128, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_int256_raw_le() {
        // Two raw 32-byte LE values: 1, and a value with the top byte set.
        let mut a = [0u8; 32];
        a[0] = 1;
        let mut b = [0u8; 32];
        b[31] = 0x80;
        let payload: Vec<u8> = a.iter().chain(b.iter()).copied().collect();
        match decode_one_typed("Int256", 2, &payload).await {
            DecodedColumn::Int256(v) => assert_eq!(v, vec![a, b]),
            other => panic!("expected Int256, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_decimal64_as_backing_int() {
        // Decimal(18, 4) decodes to its backing i64 (12345 == 1.2345) and
        // surfaces precision=18 + scale=4 from the type name.
        let vals = [12345i64, -67890, 0];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("Decimal(18, 4)", vals.len() as u64, &payload).await {
            DecodedColumn::Decimal64 {
                precision,
                scale,
                values,
            } => {
                assert_eq!(values, vals);
                assert_eq!(precision, 18);
                assert_eq!(scale, 4);
            }
            other => panic!("expected Decimal64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn decimal_sized_form_implies_precision() {
        // The sized form Decimal64(4) carries only scale on the wire-type
        // name; precision is implied by the backing width (64-bit -> 18).
        let vals = [1i64, 2, 3];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("Decimal64(4)", vals.len() as u64, &payload).await {
            DecodedColumn::Decimal64 {
                precision, scale, ..
            } => {
                assert_eq!(precision, 18, "Decimal64 implies precision 18");
                assert_eq!(scale, 4);
            }
            other => panic!("expected Decimal64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn datetime64_surfaces_precision_and_timezone() {
        // DateTime64(3, 'UTC') decodes Int64 ticks and surfaces both the
        // sub-second precision (3) and the IANA timezone ('UTC').
        let vals = [1_700_000_000_000i64, 0, -1];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("DateTime64(3, 'UTC')", vals.len() as u64, &payload).await {
            DecodedColumn::DateTime64 {
                precision,
                timezone,
                values,
            } => {
                assert_eq!(values, vals);
                assert_eq!(precision, 3);
                assert_eq!(timezone.as_deref(), Some("UTC"));
            }
            other => panic!("expected DateTime64, got {other:?}"),
        }
        // Without a timezone arg the field is None.
        match decode_one_typed("DateTime64(9)", 0, &[]).await {
            DecodedColumn::DateTime64 {
                precision,
                timezone,
                ..
            } => {
                assert_eq!(precision, 9);
                assert!(timezone.is_none());
            }
            other => panic!("expected DateTime64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_date32_signed_days() {
        let vals = [0i32, 19_000, -1]; // epoch, ~2022, one day pre-epoch
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("Date32", vals.len() as u64, &payload).await {
            DecodedColumn::Date32(v) => assert_eq!(v, vals),
            other => panic!("expected Date32, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_float64_nan_inf() {
        let vals = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("Float64", vals.len() as u64, &payload).await {
            DecodedColumn::Float64(v) => {
                // Bit patterns, not values: a `Float64` column must survive
                // the wire byte for byte, and NaN never compares equal.
                assert!(v[0].is_nan());
                assert_eq!(v[1].to_bits(), f64::INFINITY.to_bits());
                assert_eq!(v[2].to_bits(), f64::NEG_INFINITY.to_bits());
                assert_eq!(v[3].to_bits(), 1.5f64.to_bits());
            }
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    /// `LowCardinality(String)` column body at an explicit index-size code
    /// (0=u8, 1=u16, 2=u32, 3=u64), so all four widths are reachable without a
    /// dictionary big enough to force them.
    // Narrowing IS the payload: an index is written at the width `index_code`
    // declares, and a dict entry's length byte is the varuint prefix.
    #[allow(clippy::cast_possible_truncation)]
    fn lc_string_payload(index_code: u8, dict: &[&str], idx: &[u64]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&1u64.to_le_bytes()); // version
        let flags = 0x200u64 | u64::from(index_code); // HAS_ADDITIONAL_KEYS | code
        p.extend_from_slice(&flags.to_le_bytes());
        // Additional keys: count, then each String as varuint(len)+bytes
        // (all test dict entries are < 128 bytes -> single length byte).
        p.extend_from_slice(&(dict.len() as u64).to_le_bytes());
        for s in dict {
            assert!(s.len() < 128, "test dict entries stay single-byte-len");
            p.push(s.len() as u8);
            p.extend_from_slice(s.as_bytes());
        }
        // Index count, then indices at the chosen width.
        p.extend_from_slice(&(idx.len() as u64).to_le_bytes());
        for &i in idx {
            match index_code {
                0 => p.push(i as u8),
                1 => p.extend_from_slice(&(i as u16).to_le_bytes()),
                2 => p.extend_from_slice(&(i as u32).to_le_bytes()),
                3 => p.extend_from_slice(&i.to_le_bytes()),
                other => panic!("bad index code {other}"),
            }
        }
        p
    }

    #[tokio::test]
    async fn lowcardinality_decodes_all_index_widths() {
        // One logical column at all four index widths; a width misread desyncs
        // the rest of the block rather than failing loudly.
        let dict = ["a", "b", "c"];
        let idx = [0u64, 1, 0, 2];
        for code in 0u8..=3 {
            let payload = lc_string_payload(code, &dict, &idx);
            let col = decode_one_typed("LowCardinality(String)", idx.len() as u64, &payload).await;
            match col {
                DecodedColumn::LowCardinality {
                    dict,
                    indices,
                    is_nullable_inner,
                } => {
                    assert!(!is_nullable_inner, "code {code}");
                    let got = lc_to_strings(dict.as_ref(), indices.as_ref());
                    assert_eq!(got, vec!["a", "b", "a", "c"], "index code {code}");
                    let width_ok = matches!(
                        (code, indices.as_ref()),
                        (0, DecodedColumn::UInt8(_))
                            | (1, DecodedColumn::UInt16(_))
                            | (2, DecodedColumn::UInt32(_))
                            | (3, DecodedColumn::UInt64(_))
                    );
                    assert!(width_ok, "wrong index variant for code {code}");
                }
                other => panic!("expected LowCardinality, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn lowcardinality_nullable_index_zero_is_null() {
        // LC(Nullable(String)) reserves dictionary slot 0 as the NULL sentinel;
        // the decoder flags it and leaves the null reading to the caller.
        let dict = ["", "x", "y"]; // slot 0 = null placeholder
        let idx = [0u64, 1, 0, 2];
        let payload = lc_string_payload(0, &dict, &idx);
        let col = decode_one_typed(
            "LowCardinality(Nullable(String))",
            idx.len() as u64,
            &payload,
        )
        .await;
        match col {
            DecodedColumn::LowCardinality {
                dict,
                indices,
                is_nullable_inner,
            } => {
                assert!(is_nullable_inner, "Nullable inner must be flagged");
                let idxs = match indices.as_ref() {
                    DecodedColumn::UInt8(v) => v.clone(),
                    other => panic!("expected UInt8 indices, got {other:?}"),
                };
                let nulls: Vec<bool> = idxs.iter().map(|&i| i == 0).collect();
                assert_eq!(nulls, vec![true, false, true, false]);
                match dict.as_ref() {
                    DecodedColumn::String(d) => assert_eq!(d.len(), 3),
                    other => panic!("expected String dict, got {other:?}"),
                }
            }
            other => panic!("expected LowCardinality, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_nested_array_array_uint64() {
        // [[1,2],[3]] and [[4,5,6]]: outer offsets count inner arrays, inner
        // offsets count u64s, both cumulative.
        let mut payload = Vec::new();
        for v in [2u64, 3] {
            payload.extend_from_slice(&v.to_le_bytes()); // outer offsets
        }
        for v in [2u64, 3, 6] {
            payload.extend_from_slice(&v.to_le_bytes()); // inner offsets
        }
        for v in [1u64, 2, 3, 4, 5, 6] {
            payload.extend_from_slice(&v.to_le_bytes()); // u64 values
        }
        let col = decode_one_typed("Array(Array(UInt64))", 2, &payload).await;
        match col {
            DecodedColumn::Array { offsets, child } => {
                assert_eq!(offsets, vec![2u64, 3]);
                match child.as_ref() {
                    DecodedColumn::Array {
                        offsets: inner_off,
                        child: inner_child,
                    } => {
                        assert_eq!(inner_off, &vec![2u64, 3, 6]);
                        match inner_child.as_ref() {
                            DecodedColumn::UInt64(v) => {
                                assert_eq!(v, &vec![1u64, 2, 3, 4, 5, 6]);
                            }
                            other => panic!("expected UInt64, got {other:?}"),
                        }
                    }
                    other => panic!("expected inner Array, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    // The single-byte key lengths ARE the varuint prefix -- every test key is
    // one byte long.
    #[allow(clippy::cast_possible_truncation)]
    #[tokio::test]
    async fn roundtrip_map_string_uint64() {
        // {"a":1,"b":2} and {"c":3}: cumulative pair offsets, then a flat keys
        // column, then a flat values column.
        let mut payload = Vec::new();
        for v in [2u64, 3] {
            payload.extend_from_slice(&v.to_le_bytes()); // offsets
        }
        for s in ["a", "b", "c"] {
            payload.push(s.len() as u8); // varuint len (< 128)
            payload.extend_from_slice(s.as_bytes());
        }
        for v in [1u64, 2, 3] {
            payload.extend_from_slice(&v.to_le_bytes()); // values
        }
        let col = decode_one_typed("Map(String, UInt64)", 2, &payload).await;
        match col {
            DecodedColumn::Map {
                offsets,
                keys,
                values,
            } => {
                assert_eq!(offsets, vec![2u64, 3]);
                match keys.as_ref() {
                    DecodedColumn::String(k) => {
                        let ks: Vec<&str> =
                            k.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
                        assert_eq!(ks, vec!["a", "b", "c"]);
                    }
                    other => panic!("expected String keys, got {other:?}"),
                }
                match values.as_ref() {
                    DecodedColumn::UInt64(v) => assert_eq!(v, &vec![1u64, 2, 3]),
                    other => panic!("expected UInt64 values, got {other:?}"),
                }
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nullable_all_null_uint8() {
        // n mask bytes (1 = null) then the child column, all-null this time.
        let payload = vec![1u8, 1, 1, /* child */ 0, 0, 0];
        let col = decode_one_typed("Nullable(UInt8)", 3, &payload).await;
        match col {
            DecodedColumn::Nullable { mask, child } => {
                assert_eq!(mask, vec![1u8, 1, 1]);
                match child.as_ref() {
                    DecodedColumn::UInt8(v) => assert_eq!(v, &vec![0u8, 0, 0]),
                    other => panic!("expected UInt8 child, got {other:?}"),
                }
            }
            other => panic!("expected Nullable, got {other:?}"),
        }
    }

    /// One-column block with the given declared type and decoded values.
    fn block_of(type_name: &str, column: DecodedColumn) -> DecodedBlock {
        let num_rows = column.row_count() as u64;
        DecodedBlock {
            columns: vec![column],
            schema: vec![("c".to_string(), type_name.to_string())],
            num_rows,
        }
    }

    fn strings(values: &[&str]) -> DecodedColumn {
        DecodedColumn::String(values.iter().map(|s| s.as_bytes().to_vec()).collect())
    }

    #[test]
    fn column_as_string() {
        let block = block_of("String", strings(&["id", "", "\u{1F600}"]));
        assert_eq!(
            block.column_as::<String>("c").unwrap(),
            vec!["id", "", "\u{1F600}"]
        );
    }

    #[test]
    fn column_as_string_rejects_non_utf8() {
        let block = block_of("String", DecodedColumn::String(vec![vec![0xff, 0xfe]]));
        let err = block.column_as::<String>("c").unwrap_err();
        assert!(
            matches!(err, Error::InvalidUtf8Encoding(_)),
            "expected a UTF-8 error, got {err:?}"
        );
    }

    #[test]
    fn column_as_raw_bytes() {
        let block = block_of("String", DecodedColumn::String(vec![vec![0xff, 0x00]]));
        assert_eq!(
            block.column_as::<Vec<u8>>("c").unwrap(),
            vec![vec![0xffu8, 0x00]]
        );
    }

    #[test]
    fn column_as_u64() {
        let block = block_of("UInt64", DecodedColumn::UInt64(vec![0, 1, u64::MAX]));
        assert_eq!(block.column_as::<u64>("c").unwrap(), vec![0, 1, u64::MAX]);
    }

    #[test]
    fn column_as_u8() {
        let block = block_of("UInt8", DecodedColumn::UInt8(vec![0, 1, 255]));
        assert_eq!(block.column_as::<u8>("c").unwrap(), vec![0u8, 1, 255]);
    }

    #[test]
    fn column_as_i64() {
        let block = block_of("Int64", DecodedColumn::Int64(vec![i64::MIN, -1, 0]));
        assert_eq!(
            block.column_as::<i64>("c").unwrap(),
            vec![i64::MIN, -1, 0i64]
        );
    }

    #[test]
    fn column_as_bool_treats_any_nonzero_as_true() {
        let block = block_of("Bool", DecodedColumn::UInt8(vec![0, 1, 7]));
        assert_eq!(
            block.column_as::<bool>("c").unwrap(),
            vec![false, true, true]
        );
    }

    #[test]
    fn column_as_nullable_string() {
        let block = block_of(
            "Nullable(String)",
            DecodedColumn::Nullable {
                mask: vec![0, 1, 0],
                child: Box::new(strings(&["a", "", "b"])),
            },
        );
        assert_eq!(
            block.column_as::<Option<String>>("c").unwrap(),
            vec![Some("a".to_string()), None, Some("b".to_string())]
        );
    }

    /// Every scalar the decoder can produce must have a `column_as` route.
    /// The Docker type matrix reads its columns this way, so a type that
    /// decodes but cannot be read back is a hole the matrix cannot cover.
    /// Split by group only to stay under the function-length lint.
    #[test]
    fn column_as_covers_every_numeric_scalar() {
        assert_eq!(
            block_of("UInt8", DecodedColumn::UInt8(vec![7]))
                .column_as::<u8>("c")
                .unwrap(),
            vec![7u8]
        );
        assert_eq!(
            block_of("UInt16", DecodedColumn::UInt16(vec![7]))
                .column_as::<u16>("c")
                .unwrap(),
            vec![7u16]
        );
        assert_eq!(
            block_of("UInt32", DecodedColumn::UInt32(vec![7]))
                .column_as::<u32>("c")
                .unwrap(),
            vec![7u32]
        );
        assert_eq!(
            block_of("UInt64", DecodedColumn::UInt64(vec![7]))
                .column_as::<u64>("c")
                .unwrap(),
            vec![7u64]
        );
        assert_eq!(
            block_of("UInt128", DecodedColumn::UInt128(vec![7]))
                .column_as::<u128>("c")
                .unwrap(),
            vec![7u128]
        );
        assert_eq!(
            block_of("Int8", DecodedColumn::Int8(vec![-7]))
                .column_as::<i8>("c")
                .unwrap(),
            vec![-7i8]
        );
        assert_eq!(
            block_of("Int16", DecodedColumn::Int16(vec![-7]))
                .column_as::<i16>("c")
                .unwrap(),
            vec![-7i16]
        );
        assert_eq!(
            block_of("Int32", DecodedColumn::Int32(vec![-7]))
                .column_as::<i32>("c")
                .unwrap(),
            vec![-7i32]
        );
        assert_eq!(
            block_of("Int64", DecodedColumn::Int64(vec![-7]))
                .column_as::<i64>("c")
                .unwrap(),
            vec![-7i64]
        );
        assert_eq!(
            block_of("Int128", DecodedColumn::Int128(vec![-7]))
                .column_as::<i128>("c")
                .unwrap(),
            vec![-7i128]
        );
        assert_eq!(
            block_of("Float32", DecodedColumn::Float32(vec![1.5]))
                .column_as::<f32>("c")
                .unwrap(),
            vec![1.5f32]
        );
        assert_eq!(
            block_of("Float64", DecodedColumn::Float64(vec![1.5]))
                .column_as::<f64>("c")
                .unwrap(),
            vec![1.5f64]
        );
    }

    /// Types whose Rust shape is their backing, not a type of their own.
    #[test]
    fn column_as_covers_types_carried_on_another_backing() {
        assert_eq!(
            block_of("Date", DecodedColumn::Date(vec![19_000]))
                .column_as::<u16>("c")
                .unwrap(),
            vec![19_000u16]
        );
        assert_eq!(
            block_of("Date32", DecodedColumn::Date32(vec![-1]))
                .column_as::<i32>("c")
                .unwrap(),
            vec![-1i32]
        );
        assert_eq!(
            block_of("DateTime", DecodedColumn::DateTime(vec![1_700_000_000]))
                .column_as::<u32>("c")
                .unwrap(),
            vec![1_700_000_000u32]
        );
        assert_eq!(
            block_of("IPv4", DecodedColumn::Ipv4(vec![0x0100_007f]))
                .column_as::<u32>("c")
                .unwrap(),
            vec![0x0100_007fu32]
        );
        assert_eq!(
            block_of(
                "DateTime64(3)",
                DecodedColumn::DateTime64 {
                    precision: 3,
                    timezone: None,
                    values: vec![1_700_000_000_000],
                }
            )
            .column_as::<i64>("c")
            .unwrap(),
            vec![1_700_000_000_000i64]
        );
        assert_eq!(
            block_of(
                "Decimal(9, 2)",
                DecodedColumn::Decimal32 {
                    precision: 9,
                    scale: 2,
                    values: vec![12_345],
                }
            )
            .column_as::<i32>("c")
            .unwrap(),
            vec![12_345i32]
        );
        assert_eq!(
            block_of(
                "Decimal(18, 2)",
                DecodedColumn::Decimal64 {
                    precision: 18,
                    scale: 2,
                    values: vec![12_345],
                }
            )
            .column_as::<i64>("c")
            .unwrap(),
            vec![12_345i64]
        );
        assert_eq!(
            block_of(
                "Decimal(38, 2)",
                DecodedColumn::Decimal128 {
                    precision: 38,
                    scale: 2,
                    values: vec![12_345],
                }
            )
            .column_as::<i128>("c")
            .unwrap(),
            vec![12_345i128]
        );
    }

    /// 256-bit and 16-byte columns come back as their raw wire blocks; the
    /// last two cases pin `Bool` and the `Nullable` blanket.
    #[test]
    fn column_as_covers_wide_bool_and_nullable_columns() {
        assert_eq!(
            block_of(
                "Decimal(76, 2)",
                DecodedColumn::Decimal256 {
                    precision: 76,
                    scale: 2,
                    values: vec![[9u8; 32]],
                }
            )
            .column_as::<[u8; 32]>("c")
            .unwrap(),
            vec![[9u8; 32]]
        );
        assert_eq!(
            block_of("Int256", DecodedColumn::Int256(vec![[1u8; 32]]))
                .column_as::<[u8; 32]>("c")
                .unwrap(),
            vec![[1u8; 32]]
        );
        assert_eq!(
            block_of("UInt256", DecodedColumn::UInt256(vec![[2u8; 32]]))
                .column_as::<[u8; 32]>("c")
                .unwrap(),
            vec![[2u8; 32]]
        );
        assert_eq!(
            block_of("UUID", DecodedColumn::Uuid(vec![[3u8; 16]]))
                .column_as::<[u8; 16]>("c")
                .unwrap(),
            vec![[3u8; 16]]
        );
        assert_eq!(
            block_of("IPv6", DecodedColumn::Ipv6(vec![[4u8; 16]]))
                .column_as::<[u8; 16]>("c")
                .unwrap(),
            vec![[4u8; 16]]
        );

        // Bool shares the UInt8 backing; any non-zero byte is true.
        assert_eq!(
            block_of("Bool", DecodedColumn::UInt8(vec![0, 1, 2]))
                .column_as::<bool>("c")
                .unwrap(),
            vec![false, true, true]
        );

        // Nullable routes through whatever route the child has.
        assert_eq!(
            block_of(
                "Nullable(UInt64)",
                DecodedColumn::Nullable {
                    mask: vec![0, 1],
                    child: Box::new(DecodedColumn::UInt64(vec![9, 0])),
                }
            )
            .column_as::<Option<u64>>("c")
            .unwrap(),
            vec![Some(9u64), None]
        );
    }

    #[test]
    fn column_as_names_the_column_and_both_types_on_mismatch() {
        let block = block_of("UInt64", DecodedColumn::UInt64(vec![1]));
        let err = block.column_as::<String>("c").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("'c'"), "message must name the column: {msg}");
        assert!(
            msg.contains("UInt64"),
            "message must name the wire type: {msg}"
        );
        assert!(
            msg.contains("String"),
            "message must name the read type: {msg}"
        );
    }

    #[test]
    fn column_as_rejects_an_undeclared_column() {
        let block = block_of("UInt64", DecodedColumn::UInt64(vec![1]));
        let err = block.column_as::<u64>("missing").unwrap_err();
        assert!(err.to_string().contains("'missing'"), "{err}");
    }

    #[test]
    fn header_block_declares_columns_but_yields_no_values() {
        let block = DecodedBlock {
            columns: Vec::new(),
            schema: vec![("c".to_string(), "UInt64".to_string())],
            num_rows: 0,
        };
        assert!(block.column("c").is_none());
        assert_eq!(block.column_as::<u64>("c").unwrap(), [] as [u64; 0]);
    }

    /// The server ends every result set with a block that declares no
    /// columns, so a `column_as` sweep over the whole set must not read it
    /// as a schema mismatch.
    #[test]
    fn trailing_empty_block_yields_no_values() {
        let block = DecodedBlock {
            columns: Vec::new(),
            schema: Vec::new(),
            num_rows: 0,
        };
        assert_eq!(block.column_as::<u64>("c").unwrap(), [] as [u64; 0]);
    }

    #[tokio::test]
    async fn fixed_string_preserves_nul_and_padding() {
        // Embedded NULs and trailing zero-padding are data, never terminators.
        let raw = [b'a', 0, b'b', 0, b'a', b'b', 0, 0];
        let col = decode_one_typed("FixedString(4)", 2, &raw).await;
        match col {
            DecodedColumn::FixedString { width, bytes } => {
                assert_eq!(width, 4);
                assert_eq!(bytes, raw);
            }
            other => panic!("expected FixedString, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lowcardinality_header_block_consumes_no_column_bytes() {
        // A zero-row block carries no payload for the column, the
        // LowCardinality version and flags words included.
        let mut body: Vec<u8> = Vec::new();
        body.put_string(b"lc");
        body.put_string(b"LowCardinality(String)");
        body.push(0); // custom-serialization flag
        let len = body.len();
        let mut cursor = Cursor::new(body);
        let block = decode_block(&mut cursor, 1, 0, REV).await.unwrap();
        assert_eq!(cursor.position(), len as u64, "read past the column header");
        assert_eq!(block.columns[0].row_count(), 0);
    }

    #[tokio::test]
    async fn decode_block_rejects_an_implausible_row_count() {
        let mut cursor = Cursor::new(Vec::new());
        let err = decode_block(&mut cursor, 1, MAX_BLOCK_ROWS + 1, REV)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("rows, above the"), "{err}");
        assert_eq!(cursor.position(), 0, "rejected before reading a byte");
    }

    #[tokio::test]
    async fn decode_block_rejects_an_implausible_column_count() {
        let mut cursor = Cursor::new(Vec::new());
        let err = decode_block(&mut cursor, MAX_BLOCK_COLUMNS + 1, 1, REV)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("columns, above the"), "{err}");
        assert_eq!(cursor.position(), 0, "rejected before reading a byte");
    }

    #[tokio::test]
    async fn roundtrip_nullable_lowcardinality() {
        // LC(Nullable(String)) over ["a", NULL, "a"]: the dictionary reserves
        // slot 0 for NULL, so the two "a" rows share slot 1.
        let payload = lc_string_payload(0, &["", "a"], &[1, 0, 1]);
        match decode_one_typed("LowCardinality(Nullable(String))", 3, &payload).await {
            DecodedColumn::LowCardinality {
                dict,
                indices,
                is_nullable_inner,
            } => {
                assert!(is_nullable_inner);
                match indices.as_ref() {
                    DecodedColumn::UInt8(v) => assert_eq!(v, &vec![1u8, 0, 1]),
                    other => panic!("expected UInt8 indices, got {other:?}"),
                }
                match dict.as_ref() {
                    DecodedColumn::String(d) => assert_eq!(d.len(), 2),
                    other => panic!("expected String dict, got {other:?}"),
                }
            }
            other => panic!("expected LowCardinality, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_map_nullable_value() {
        // One row: {"k": NULL}. The value sub-column is a full Nullable column,
        // so it carries a mask byte and a placeholder value.
        let mut payload = 1u64.to_le_bytes().to_vec();
        payload.extend_from_slice(&[1, b'k']); // keys
        payload.extend_from_slice(&[1, 0]); // null mask, then the placeholder
        match decode_one_typed("Map(String, Nullable(UInt8))", 1, &payload).await {
            DecodedColumn::Map {
                offsets, values, ..
            } => {
                assert_eq!(offsets, vec![1u64]);
                match values.as_ref() {
                    DecodedColumn::Nullable { mask, .. } => assert_eq!(mask, &vec![1u8]),
                    other => panic!("expected Nullable values, got {other:?}"),
                }
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_array_of_tuple() {
        // One row of two (UInt8, UInt16) pairs: the offsets, then the tuple's
        // two field sub-columns flat over both elements.
        let mut payload = 2u64.to_le_bytes().to_vec();
        payload.extend_from_slice(&[1, 2]); // the UInt8 field
        payload.extend_from_slice(&[3, 0, 4, 0]); // the UInt16 field
        match decode_one_typed("Array(Tuple(UInt8, UInt16))", 1, &payload).await {
            DecodedColumn::Array { offsets, child } => {
                assert_eq!(offsets, vec![2u64]);
                match child.as_ref() {
                    DecodedColumn::Tuple(fields) => {
                        assert_eq!(fields.len(), 2);
                        match (&fields[0], &fields[1]) {
                            (DecodedColumn::UInt8(a), DecodedColumn::UInt16(b)) => {
                                assert_eq!(a, &vec![1u8, 2]);
                                assert_eq!(b, &vec![3u16, 4]);
                            }
                            other => panic!("expected (UInt8, UInt16), got {other:?}"),
                        }
                    }
                    other => panic!("expected Tuple child, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn variant_and_dynamic_read_as_json_documents() {
        // Variant(UInt8, String) over [7, "q"]: mode word, discriminators, then
        // each arm's rows.
        let mut payload = 0u64.to_le_bytes().to_vec();
        payload.extend_from_slice(&[0, 1]);
        payload.push(7);
        payload.extend_from_slice(&[1, b'q']);
        match decode_one_typed("Variant(UInt8, String)", 2, &payload).await {
            DecodedColumn::Json(docs) => {
                assert_eq!(docs, vec![b"7".to_vec(), br#""q""#.to_vec()]);
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    /// A decode that stops short of the body it was handed leaves the leftover
    /// bytes for the next packet's header, and the connection reads garbage
    /// from there on. The error that results names the wrong thing: one byte
    /// left behind by the sparse kind-stack bug reported as
    /// `unknown server packet id 78`, columns later.
    ///
    /// So every successful decode must land exactly on the end of its input.
    /// What this CANNOT catch is a fixture built to the same wrong spec as the
    /// decoder -- agreeing with itself is not evidence. That is what the
    /// Docker wire suite is for, where the server writes the bytes.
    #[tokio::test]
    async fn a_decoded_block_lands_exactly_on_the_end_of_its_body() {
        let cases: &[(&str, u64, Vec<Vec<u8>>)] = &[
            (
                "UInt64",
                3,
                (0u64..3).map(|v| v.to_le_bytes().to_vec()).collect(),
            ),
            ("String", 2, vec![b"\x02hi".to_vec(), b"\x00".to_vec()]),
            // A RowBinary NULL is the flag byte alone -- no value follows it.
            ("Nullable(UInt8)", 2, vec![vec![0, 9], vec![1]]),
            ("Array(UInt8)", 2, vec![vec![1, 7], vec![2, 8, 9]]),
            (
                "Map(String, UInt64)",
                1,
                vec![{
                    let mut row = vec![1u8, 1, b'k'];
                    row.extend_from_slice(&5u64.to_le_bytes());
                    row
                }],
            ),
            ("Tuple(UInt8, String)", 1, vec![vec![3, 1, b'x']]),
            (
                "LowCardinality(String)",
                2,
                vec![b"\x01a".to_vec(), b"\x01b".to_vec()],
            ),
        ];

        for (type_name, rows, values) in cases {
            let bytes = encode_one_column_block("c", type_name, values);
            let len = bytes.len() as u64;
            let mut cur = Cursor::new(bytes);
            decode_block(&mut cur, 1, *rows, REV)
                .await
                .unwrap_or_else(|e| panic!("{type_name} decodes: {e}"));
            assert_eq!(
                cur.position(),
                len,
                "{type_name} left {} bytes on the wire",
                len - cur.position()
            );
        }
    }

    /// The same guarantee for a sparse column, built to the spec rather than to
    /// what the decoder happens to do: two header bytes (the
    /// `has_custom_serialization` bool, then the kind-stack byte), the offsets,
    /// then only the non-default values.
    ///
    /// Both assertions earn their place. Reverting the decoder to read one
    /// header byte was checked against this test: it lands on the right END
    /// byte anyway -- the drift cancels out over a fixture this small -- and it
    /// is the scattered VALUES that come back wrong, `[0,42,0,0]` for
    /// `[0,0,42,0]`. A residual-byte check alone would have passed.
    #[tokio::test]
    async fn a_sparse_column_lands_exactly_on_the_end_of_its_body() {
        use crate::native::sparse::END_OF_GRANULE_FLAG;

        let mut bytes = Vec::new();
        bytes.put_string(b"c");
        bytes.put_string(b"UInt8");
        bytes.push(1); // has_custom_serialization
        bytes.push(1); // KindStack {Default, Sparse}
        bytes.put_var_uint(2); // two defaults, then a value at row 2
        bytes.put_var_uint(1 | END_OF_GRANULE_FLAG); // one trailing default
        bytes.push(42);

        let len = bytes.len() as u64;
        let mut cur = Cursor::new(bytes);
        let block = decode_block(&mut cur, 1, 4, REV).await.unwrap();
        assert_eq!(cur.position(), len, "left bytes on the wire");
        match block.column("c").unwrap() {
            DecodedColumn::UInt8(values) => assert_eq!(values, &[0, 0, 42, 0]),
            other => panic!("expected UInt8, got {other:?}"),
        }
    }

    /// Upper bounds on what the property test may generate. A declared count is
    /// server-controlled, so the generator stays inside what a real block can
    /// carry: a test that allocates from an unbounded generated value is a
    /// defect in the test, not a finding about the decoder.
    const PROP_MAX_ROWS: u64 = 1_024;
    const PROP_MAX_COLUMNS: u64 = 64;
    const PROP_MAX_WIRE_BYTES: usize = 64 * 1_024;

    /// A three-column block body covering a fixed width, a length-prefixed type
    /// and a composite, so a truncation can land inside any of them.
    fn valid_block_body() -> Vec<u8> {
        let mut body: Vec<u8> = Vec::new();
        body.put_string(b"n");
        body.put_string(b"UInt64");
        body.push(0);
        body.extend_from_slice(&7u64.to_le_bytes());
        body.put_string(b"s");
        body.put_string(b"String");
        body.push(0);
        body.put_string(b"hello");
        body.put_string(b"a");
        body.put_string(b"Array(UInt8)");
        body.push(0);
        body.extend_from_slice(&2u64.to_le_bytes());
        body.extend_from_slice(&[1, 2]);
        body
    }

    /// A runner that writes its counterexample to a file rather than only to
    /// the log, so a CI failure is reproducible. `Direct` and not `WithSource`
    /// because these run through `TestRunner`, which has no `source_file`.
    fn persisting_runner() -> proptest::test_runner::TestRunner {
        use proptest::test_runner::{Config, FileFailurePersistence, TestRunner};
        TestRunner::new(Config {
            failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
                "proptest-regressions/native-decode.txt",
            ))),
            ..Config::default()
        })
    }

    /// Assert what a successful decode must satisfy: one column per schema
    /// entry, and every column exactly `rows` long. A decoder that fabricated
    /// rows the wire never backed would fail the second.
    fn assert_block_is_consistent(
        block: &DecodedBlock,
        rows: u64,
    ) -> Result<(), proptest::test_runner::TestCaseError> {
        use proptest::prelude::*;
        prop_assert_eq!(block.schema.len(), block.columns.len());
        for column in &block.columns {
            let got = u64::try_from(column.row_count()).expect("row count fits u64");
            prop_assert_eq!(got, rows);
        }
        Ok(())
    }

    /// Truncated and byte-flipped block bodies, at row and column counts drawn
    /// from the generator. Every input must either decode or error, and no
    /// input may panic.
    #[test]
    fn decode_block_survives_truncation_and_garbage() {
        use proptest::prelude::*;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");
        let body = valid_block_body();

        let mut runner = persisting_runner();
        let strategy = (
            0..=body.len(),
            any::<u8>(),
            0..body.len(),
            1..=PROP_MAX_ROWS,
            1..=PROP_MAX_COLUMNS,
        );
        runner
            .run(&strategy, |(cut, noise, at, rows, columns)| {
                let mut wire = body[..cut].to_vec();
                // Flip one byte inside the surviving prefix so the case covers
                // garbage as well as a clean truncation.
                if at < wire.len() {
                    wire[at] = noise;
                }
                let outcome = runtime.block_on(async {
                    decode_block(&mut Cursor::new(wire), columns, rows, REV).await
                });
                if let Ok(block) = outcome {
                    assert_block_is_consistent(&block, rows)?;
                }
                Ok(())
            })
            .expect("no input panics the decoder");
    }

    /// The same guarantee for a body that was never valid to begin with.
    #[test]
    fn decode_block_survives_arbitrary_bytes() {
        use proptest::prelude::*;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");

        let mut runner = persisting_runner();
        let strategy = (
            proptest::collection::vec(any::<u8>(), 0..=PROP_MAX_WIRE_BYTES),
            1..=PROP_MAX_ROWS,
            1..=PROP_MAX_COLUMNS,
        );
        runner
            .run(&strategy, |(wire, rows, columns)| {
                let outcome = runtime.block_on(async {
                    decode_block(&mut Cursor::new(wire), columns, rows, REV).await
                });
                if let Ok(block) = outcome {
                    assert_block_is_consistent(&block, rows)?;
                }
                Ok(())
            })
            .expect("no input panics the decoder");
    }
}
