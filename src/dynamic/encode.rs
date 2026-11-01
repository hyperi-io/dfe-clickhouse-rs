// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-clickhouse
// File:      src/dynamic/encode.rs
// Purpose:   Dynamic Map<String, Value> to RowBinary encoder
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Dynamic RowBinary encoder.
//!
//! Turns a runtime-shaped `serde_json::Map<String, Value>` plus its resolved
//! ClickHouse column schema into RowBinary wire bytes. [`DynamicRow::encode_into`]
//! appends exactly what crosses the wire, so every column's encoding is
//! testable at the byte level with no server.
//!
//! # Coercions
//!
//! Coercions ClickHouse's JSONEachRow path cannot do server-side: epoch
//! magnitude detection (ms/us/ns) and ISO-8601 'T'-separator handling for
//! date/time types, hyphen-less UUID hex, and integer-to-dotted IPv4. Date-time
//! text with no zone is UTC, and an offset (`Z`, `+HH`, `+HHMM`, `+HH:MM`) is
//! applied, so the stored instant is the one the source named.
//!
//! A coercion that would lose information is an error, not a silent write: an
//! out-of-range integer, a decimal past its backing width, an impossible
//! calendar day and a non-ASCII datetime string all fail the row.

use std::borrow::Cow;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use serde_json::{Map, Value};

use crate::native::io::ClickHouseBytesWrite;

use super::datetime::{self, civil_days_from_epoch, days_in_month};
use super::error::DynamicError;
use super::parsed_type::{ParsedType, TypeTag};
use super::shape::json_column_text;

/// A single resolved column for a dynamic insert.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ColumnDef {
    /// Column name, used to look up the value in the row map.
    pub name: String,
    /// Parsed ClickHouse type. Drives wire encoding, and its `raw` is the type
    /// string declared in the Native block header.
    pub ty: ParsedType,
    /// Default kind from system.columns: "", "DEFAULT", "MATERIALIZED",
    /// "ALIAS", "EPHEMERAL". Empty for columns without a server-side default.
    pub default_kind: String,
    /// True when the column has a server-side default and may be omitted from
    /// the INSERT column list (e.g. `_uuid`, `_timestamp_load`).
    pub has_default: bool,
}

impl ColumnDef {
    /// Build a column from a name and a ClickHouse type string. The column is
    /// treated as required (no server-side default).
    ///
    /// The type string is parsed once here and kept verbatim on `ty.raw`.
    #[must_use]
    pub fn new(name: impl Into<String>, type_string: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ty: ParsedType::parse(&type_string.into()),
            default_kind: String::new(),
            has_default: false,
        }
    }

    /// Build a column carrying its `system.columns` default kind. `has_default`
    /// is set when `default_kind` is non-empty, marking the column omittable
    /// from the INSERT list.
    #[must_use]
    pub fn with_default_kind(
        name: impl Into<String>,
        type_string: impl Into<String>,
        default_kind: impl Into<String>,
    ) -> Self {
        let default_kind = default_kind.into();
        let has_default = !default_kind.is_empty();
        let mut column = Self::new(name, type_string);
        column.default_kind = default_kind;
        column.has_default = has_default;
        column
    }
}

/// Runtime-dynamic row: a borrowed JSON object plus the resolved column
/// schema it is encoded against, with optional raw passthrough for one named
/// JSON column.
#[non_exhaustive]
pub struct DynamicRow<'a> {
    row: &'a Map<String, Value>,
    columns: &'a [ColumnDef],
    /// When set, `(column_name, raw_bytes)`: the named JSON column is written
    /// from `raw_bytes` as a length-prefixed string rather than from `row`.
    raw: Option<(&'a str, &'a [u8])>,
}

impl<'a> DynamicRow<'a> {
    /// Encode a row, taking each column's value from `row` by name.
    #[must_use]
    pub fn new(row: &'a Map<String, Value>, columns: &'a [ColumnDef]) -> Self {
        Self {
            row,
            columns,
            raw: None,
        }
    }

    /// Same as [`DynamicRow::new`], but the named JSON column (e.g. `_json`)
    /// is written from `raw_json` -- a zero-copy passthrough of the original
    /// payload as a JSON string -- instead of from `row`.
    #[must_use]
    pub fn with_raw(
        row: &'a Map<String, Value>,
        columns: &'a [ColumnDef],
        raw_json: &'a [u8],
        json_col: &'a str,
    ) -> Self {
        Self {
            row,
            columns,
            raw: Some((json_col, raw_json)),
        }
    }

    /// Append this row's RowBinary wire bytes to `buf`.
    ///
    /// The batching primitive: one arena is reused across a whole block, so a
    /// million-row block is one allocation run rather than a million.
    ///
    /// On error `buf` holds a partial row; the caller must truncate it back to
    /// the length it passed in, or drop the block.
    ///
    /// # Errors
    /// Returns [`DynamicError`] if any column value cannot be encoded for its
    /// target type.
    pub fn encode_into(&self, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
        for col in self.columns {
            self.encode_column(col, buf)?;
        }
        Ok(())
    }

    /// Append this row's `RowBinaryWithDefaults` bytes to `buf`: each column is
    /// prefixed `1` to take the server's default, when the column has one and
    /// the row does not carry it, or `0` and its RowBinary value otherwise.
    ///
    /// On error `buf` holds a partial row, as for [`Self::encode_into`].
    pub(crate) fn encode_with_defaults_into(&self, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
        for col in self.columns {
            let supplied = self.row.contains_key(&col.name)
                || self.raw.is_some_and(|(name, _)| name == col.name);
            if col.has_default && !supplied {
                buf.push(1);
            } else {
                buf.push(0);
                self.encode_column(col, buf)?;
            }
        }
        Ok(())
    }

    /// [`Self::encode_into`] into a fresh `Vec`.
    ///
    /// # Errors
    /// As [`Self::encode_into`].
    pub fn encode(&self) -> Result<Vec<u8>, DynamicError> {
        let mut buf = Vec::with_capacity(256);
        self.encode_into(&mut buf)?;
        Ok(buf)
    }

    /// Encode a single column's value to RowBinary, appending to `buf`.
    ///
    /// Honours the raw `_json` passthrough for the named JSON column.
    fn encode_column(&self, col: &ColumnDef, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
        if let Some((name, bytes)) = self.raw
            && name == col.name
        {
            // Raw passthrough for a JSON/String column: Nullable not-null
            // marker if needed, then the bytes as a length-prefixed string.
            if col.ty.nullable {
                buf.push(0);
            }
            buf.put_string(bytes);
            return Ok(());
        }
        let value = self.row.get(&col.name).unwrap_or(&Value::Null);
        encode_value(value, &col.ty, &col.name, buf)
    }
}

// ---------------------------------------------------------------------------
// Per-value encoding
// ---------------------------------------------------------------------------

fn encode_value(
    value: &Value,
    pt: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    // Nullable wrapper: one level only (ClickHouse forbids Nullable(Nullable)).
    if pt.nullable {
        if value.is_null() {
            buf.push(1);
            return Ok(());
        }
        buf.push(0);
    } else if value.is_null() {
        return write_default(pt, col, buf);
    }

    encode_typed(value, pt, col, buf)
}

#[allow(clippy::too_many_lines)]
fn encode_typed(
    value: &Value,
    pt: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    // A bare `Decimal(P, S)` carries tag Unknown; resolve it to a concrete
    // width by precision so it hits the right Decimal arm below.
    let tag = if pt.base == "Decimal" {
        decimal_tag_for_precision(pt.precision.unwrap_or(38))
    } else {
        pt.tag
    };

    match tag {
        TypeTag::String => {
            let s = value_to_str(value);
            buf.put_string(s.as_bytes());
        }
        TypeTag::FixedString => {
            let s = value_to_str(value);
            let n = pt.fixed_size.unwrap_or(1);
            let bytes = s.as_bytes();
            // CH FixedString is a raw N-byte run with no length prefix: short
            // values are NUL-padded, and a long one fails the row because cutting
            // it would drop bytes, possibly mid-character.
            if bytes.len() > n {
                return Err(enc_err(
                    col,
                    &format!("FixedString({n}) value out of range: {} bytes", bytes.len()),
                ));
            }
            buf.extend_from_slice(bytes);
            buf.resize(buf.len() + (n - bytes.len()), 0);
        }
        TypeTag::Bool => {
            buf.push(u8::from(as_bool(value, col)?));
        }
        // Every narrowing goes through `try_from`: a value the column cannot
        // hold fails the row instead of being written modulo the width.
        TypeTag::UInt8 => {
            buf.push(narrow::<u8>(as_u64(value, col)?, col, "UInt8")?);
        }
        TypeTag::UInt16 => {
            let v = narrow::<u16>(as_u64(value, col)?, col, "UInt16")?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::UInt32 => {
            let v = narrow::<u32>(as_u64(value, col)?, col, "UInt32")?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::UInt64 => {
            buf.extend_from_slice(&as_u64(value, col)?.to_le_bytes());
        }
        TypeTag::UInt128 => {
            buf.extend_from_slice(&as_u128(value, col)?.to_le_bytes());
        }
        TypeTag::UInt256 => {
            buf.extend_from_slice(&as_u256_le(value, col)?);
        }
        TypeTag::Int8 => {
            let v = narrow::<i8>(as_i64(value, col)?, col, "Int8")?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Int16 => {
            let v = narrow::<i16>(as_i64(value, col)?, col, "Int16")?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Enum8 => {
            let v = enum_discriminant(value, &pt.raw, col)?;
            let v = i8::try_from(v).map_err(|_| enc_err(col, "Enum8 value out of range"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Enum16 => {
            let v = enum_discriminant(value, &pt.raw, col)?;
            let v = i16::try_from(v).map_err(|_| enc_err(col, "Enum16 value out of range"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Int32 => {
            let v = narrow::<i32>(as_i64(value, col)?, col, "Int32")?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Int64 => {
            buf.extend_from_slice(&as_i64(value, col)?.to_le_bytes());
        }
        TypeTag::Int128 => {
            buf.extend_from_slice(&as_i128(value, col)?.to_le_bytes());
        }
        TypeTag::Int256 => {
            buf.extend_from_slice(&as_i256_le(value, col)?);
        }
        TypeTag::Float32 => {
            let wide = as_f64(value, col)?;
            #[allow(clippy::cast_possible_truncation)]
            let narrow = wide as f32;
            // f64 -> f32 rounds; only an overflow to infinity loses the value.
            if narrow.is_infinite() && wide.is_finite() {
                return Err(enc_err(col, "Float32 value out of range"));
            }
            buf.extend_from_slice(&narrow.to_le_bytes());
        }
        TypeTag::Float64 => {
            buf.extend_from_slice(&as_f64(value, col)?.to_le_bytes());
        }
        TypeTag::Date => {
            // Date is UInt16 days since 1970-01-01.
            let days = to_epoch_days(value, col)?;
            let days = u16::try_from(days)
                .map_err(|_| enc_err(col, "Date out of range for UInt16 days"))?;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        TypeTag::Date32 => {
            // Date32 is Int32 days since 1970-01-01 (can be negative).
            let days = to_epoch_days(value, col)?;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        TypeTag::DateTime => {
            // DateTime is UInt32 seconds since epoch.
            let secs = to_epoch_seconds(value, col)?;
            let secs = u32::try_from(secs)
                .map_err(|_| enc_err(col, "DateTime out of range for UInt32 seconds"))?;
            buf.extend_from_slice(&secs.to_le_bytes());
        }
        TypeTag::DateTime64 => {
            let precision = pt.precision.unwrap_or(3);
            let ticks = datetime64_to_ticks(value, precision, col)?;
            buf.extend_from_slice(&ticks.to_le_bytes());
        }
        TypeTag::Decimal32 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            let v = i32::try_from(backing).map_err(|_| enc_err(col, "Decimal32 overflow"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Decimal64 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            let v = i64::try_from(backing).map_err(|_| enc_err(col, "Decimal64 overflow"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Decimal128 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            buf.extend_from_slice(&backing.to_le_bytes());
        }
        TypeTag::Decimal256 => {
            // The backing is 256-bit signed, but this encoder scales through
            // i128, so anything past i128 is rejected rather than wrapped.
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            buf.extend_from_slice(&i128_to_i256_le(backing));
        }
        TypeTag::UUID => encode_uuid(value, col, buf)?,
        TypeTag::IPv4 => encode_ipv4(value, col, buf)?,
        TypeTag::IPv6 => encode_ipv6(value, col, buf)?,
        TypeTag::Array => {
            let elem = pt
                .array_element
                .as_ref()
                .ok_or_else(|| enc_err(col, "Array without element type"))?;
            encode_array(value, elem, col, buf)?;
        }
        TypeTag::Map => {
            let (kt, vt) = pt
                .map_types
                .as_ref()
                .ok_or_else(|| enc_err(col, "Map without key/value types"))?;
            encode_map(value, kt, vt, col, buf)?;
        }
        TypeTag::Point => encode_point(value, col, buf)?,
        // JSON goes on the wire as a length-prefixed String of JSON text, in
        // the form `shape::json_column_text` gives any value.
        TypeTag::JSON => buf.put_string(json_column_text(value)),
        // Variant, Dynamic, Tuple and anything the parser did not recognise
        // have no encoding here. Reject rather than write bytes the server
        // would read as a different value.
        TypeTag::Variant | TypeTag::Dynamic | TypeTag::Unknown => {
            return Err(unsupported(col, pt));
        }
    }
    Ok(())
}

/// A `Point` as `RowBinary`: x then y, each a little-endian `Float64`.
///
/// Takes a `[x, y]` pair, the GeoJSON order, or the `{"lat", "lon"}` object an
/// Elastic `geo_point` carries, with longitude as x.
fn encode_point(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let (x, y) = match value {
        Value::Array(pair) => match pair.as_slice() {
            [x, y] => (x, y),
            _ => return Err(enc_err(col, "Point needs exactly two coordinates")),
        },
        Value::Object(obj) => match (obj.get("lon"), obj.get("lat")) {
            (Some(lon), Some(lat)) => (lon, lat),
            _ => return Err(enc_err(col, "Point object needs both lat and lon")),
        },
        _ => return Err(enc_err(col, "Point needs [x, y] or {lat, lon}")),
    };
    buf.extend_from_slice(&as_f64(x, col)?.to_le_bytes());
    buf.extend_from_slice(&as_f64(y, col)?.to_le_bytes());
    Ok(())
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

/// The zero value for a non-Nullable column the row did not supply.
///
/// A type this encoder cannot write is rejected here too: a column is
/// unsupported whether or not the row happens to carry a value for it.
fn write_default(pt: &ParsedType, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    // A bare `Decimal(P, S)` has no fixed_byte_size by base name; resolve it.
    if pt.base == "Decimal" {
        let size = match decimal_tag_for_precision(pt.precision.unwrap_or(38)) {
            TypeTag::Decimal32 => 4,
            TypeTag::Decimal64 => 8,
            TypeTag::Decimal256 => 32,
            _ => 16,
        };
        buf.resize(buf.len() + size, 0);
        return Ok(());
    }
    match pt.tag {
        // JSON's no-value is the empty object, because the column parser
        // rejects empty input (code 117).
        TypeTag::JSON => {
            buf.put_string(b"{}");
            return Ok(());
        }
        // The origin, two zero `Float64`s.
        TypeTag::Point => {
            buf.resize(buf.len() + 16, 0);
            return Ok(());
        }
        TypeTag::Variant | TypeTag::Dynamic | TypeTag::Unknown => {
            return Err(unsupported(col, pt));
        }
        _ => {}
    }
    if let Some(size) = pt.fixed_byte_size() {
        buf.resize(buf.len() + size, 0);
    } else {
        // Variable-length default: empty string / array / map.
        buf.put_var_uint(0);
    }
    Ok(())
}

fn enc_err(col: &str, msg: &str) -> DynamicError {
    DynamicError::EncodingError {
        column: col.to_string(),
        message: msg.to_string(),
    }
}

fn unsupported(col: &str, pt: &ParsedType) -> DynamicError {
    DynamicError::UnsupportedType {
        column: col.to_string(),
        type_str: pt.raw.clone(),
    }
}

/// Narrow a wide integer to the column's width, erroring when it does not fit.
fn narrow<T>(value: impl TryInto<T>, col: &str, type_name: &str) -> Result<T, DynamicError> {
    value
        .try_into()
        .map_err(|_| enc_err(col, &format!("{type_name} value out of range")))
}

// ---------------------------------------------------------------------------
// Scalar coercions
// ---------------------------------------------------------------------------

/// Borrow the string directly when possible; allocate only for non-string
/// values that need stringification.
fn value_to_str(value: &Value) -> Cow<'_, str> {
    match value {
        Value::String(s) => Cow::Borrowed(s.as_str()),
        Value::Number(n) => Cow::Owned(n.to_string()),
        Value::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
        Value::Null => Cow::Borrowed(""),
        other => Cow::Owned(other.to_string()),
    }
}

/// A JSON float converts only when it is whole and inside the target range.
/// The bounds are 2^64 and +/-2^63, each exact in `f64`; an `as` cast alone
/// would saturate `1e30` to the maximum instead of rejecting the value.
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn whole_f64_as_u64(v: f64) -> Option<u64> {
    (v.fract() == 0.0 && (0.0..18_446_744_073_709_551_616.0).contains(&v)).then_some(v as u64)
}

#[allow(clippy::cast_possible_truncation)]
fn whole_f64_as_i64(v: f64) -> Option<i64> {
    let in_range = (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&v);
    (v.fract() == 0.0 && in_range).then_some(v as i64)
}

fn as_u64(value: &Value, col: &str) -> Result<u64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().and_then(|v| u64::try_from(v).ok()))
            .or_else(|| n.as_f64().and_then(whole_f64_as_u64))
            .ok_or_else(|| enc_err(col, "not a valid unsigned integer")),
        Value::Bool(b) => Ok(u64::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<u64>()
            .map_err(|_| enc_err(col, "string not parseable as u64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_i64(value: &Value, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|v| i64::try_from(v).ok()))
            .or_else(|| n.as_f64().and_then(whole_f64_as_i64))
            .ok_or_else(|| enc_err(col, "not a valid integer")),
        Value::Bool(b) => Ok(i64::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| enc_err(col, "string not parseable as i64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_u128(value: &Value, col: &str) -> Result<u128, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .map(u128::from)
            .or_else(|| n.as_i64().and_then(|v| u128::try_from(v).ok()))
            .ok_or_else(|| enc_err(col, "not a valid u128")),
        Value::Bool(b) => Ok(u128::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<u128>()
            .map_err(|_| enc_err(col, "string not parseable as u128")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_i128(value: &Value, col: &str) -> Result<i128, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
            .ok_or_else(|| enc_err(col, "not a valid i128")),
        Value::Bool(b) => Ok(i128::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<i128>()
            .map_err(|_| enc_err(col, "string not parseable as i128")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_f64(value: &Value, col: &str) -> Result<f64, DynamicError> {
    match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| enc_err(col, "not a valid float")),
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| enc_err(col, "string not parseable as f64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_bool(value: &Value, col: &str) -> Result<bool, DynamicError> {
    match value {
        Value::Bool(b) => Ok(*b),
        // Mirrors as_i64: a number the column cannot hold fails the row rather
        // than being written as false.
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|v| i64::try_from(v).ok()))
            .or_else(|| n.as_f64().and_then(whole_f64_as_i64))
            .map(|v| v != 0)
            .ok_or_else(|| enc_err(col, "number not parseable as bool")),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => Ok(true),
            "false" | "no" | "0" | "" => Ok(false),
            _ => Err(enc_err(col, "string not parseable as bool")),
        },
        Value::Null => Ok(false),
        _ => Err(enc_err(col, "expected bool")),
    }
}

/// Sign-extend a 128-bit signed backing value to a 32-byte little-endian
/// 256-bit integer (for `Int256` / `Decimal256`).
fn i128_to_i256_le(v: i128) -> [u8; 32] {
    let mut out = [if v < 0 { 0xFFu8 } else { 0x00u8 }; 32];
    out[..16].copy_from_slice(&v.to_le_bytes());
    out
}

/// Zero-extend a 128-bit unsigned value to a 32-byte little-endian 256-bit
/// integer (for `UInt256`).
fn u128_to_u256_le(v: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&v.to_le_bytes());
    out
}

fn as_i256_le(value: &Value, col: &str) -> Result<[u8; 32], DynamicError> {
    Ok(i128_to_i256_le(as_i128(value, col)?))
}

fn as_u256_le(value: &Value, col: &str) -> Result<[u8; 32], DynamicError> {
    Ok(u128_to_u256_le(as_u128(value, col)?))
}

// ---------------------------------------------------------------------------
// Decimal
// ---------------------------------------------------------------------------

/// Resolve a bare `Decimal(P, S)` to its concrete backing width by precision.
/// ClickHouse uses Decimal32 for P<=9, Decimal64 for P<=18, Decimal128 for
/// P<=38, and Decimal256 above that.
fn decimal_tag_for_precision(precision: u8) -> TypeTag {
    match precision {
        0..=9 => TypeTag::Decimal32,
        10..=18 => TypeTag::Decimal64,
        19..=38 => TypeTag::Decimal128,
        _ => TypeTag::Decimal256,
    }
}

/// 2^127, exact in f64. An `as i128` cast saturates past it, which would store
/// the i128 maximum in place of the value.
const I128_BOUND: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;

/// Compute the backing integer for a `Decimal(P, S)` by scaling the value by
/// `10^scale`. The concrete width is applied by the caller.
///
/// Scaling runs on the decimal TEXT, in i128, so a `Decimal128(38, 10)` keeps
/// all 38 digits; f64 only has 15-17 and would round them away. A JSON number
/// is stringified first, because serde_json's `Display` is the shortest text
/// that round-trips the value.
fn decimal_backing_i128(value: &Value, scale: u8, col: &str) -> Result<i128, DynamicError> {
    let text: Cow<'_, str> = match value {
        Value::Number(n) => Cow::Owned(n.to_string()),
        Value::String(s) => Cow::Borrowed(s.trim()),
        Value::Bool(b) => Cow::Borrowed(if *b { "1" } else { "0" }),
        _ => return Err(enc_err(col, "expected decimal number")),
    };
    if let Some(backing) = decimal_from_text(&text, scale) {
        return Ok(backing);
    }
    // A plain literal that missed overflowed i128, and f64 would round it into range.
    let digits = text.trim_start_matches(['+', '-']);
    if digits.bytes().any(|b| b.is_ascii_digit())
        && digits.bytes().all(|b| b.is_ascii_digit() || b == b'.')
    {
        return Err(enc_err(col, "decimal out of range"));
    }
    // Exponent forms fall back to f64 scaling, which is lossy but still better
    // than refusing the row.
    let f = text
        .parse::<f64>()
        .map_err(|_| enc_err(col, "not parseable as a decimal"))?;
    let scaled = f * 10f64.powi(i32::from(scale));
    if !scaled.is_finite() || scaled >= I128_BOUND || scaled < -I128_BOUND {
        return Err(enc_err(col, "decimal out of range after scaling"));
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(scaled.round() as i128)
}

/// Scale a plain `[+-]?digits[.digits]` literal by `10^scale` in i128, rounding
/// the first dropped fractional digit half-up as ClickHouse does.
///
/// `None` for anything that is not that shape (an exponent, a non-digit) or
/// that overflows i128, leaving the caller to decide.
fn decimal_from_text(text: &str, scale: u8) -> Option<i128> {
    let (negative, digits) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    let mut acc: i128 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().ok()?
    };
    let frac = frac_part.as_bytes();
    for i in 0..usize::from(scale) {
        let digit = i128::from(frac.get(i).map_or(0, |b| b - b'0'));
        acc = acc.checked_mul(10)?.checked_add(digit)?;
    }
    if frac.get(usize::from(scale)).is_some_and(|&b| b >= b'5') {
        acc = acc.checked_add(1)?;
    }
    if negative {
        acc.checked_neg()
    } else {
        Some(acc)
    }
}

// ---------------------------------------------------------------------------
// Date / time
// ---------------------------------------------------------------------------

/// Scale a numeric epoch value down to whole seconds, detecting ms/us/ns by
/// magnitude. Magnitude is taken absolute, so a pre-1970 millisecond epoch
/// scales the same way a post-1970 one does.
fn epoch_to_seconds(ts: i64) -> i64 {
    match ts.unsigned_abs() {
        m if m > 1_000_000_000_000_000_000 => ts / 1_000_000_000,
        m if m > 1_000_000_000_000_000 => ts / 1_000_000,
        m if m > 1_000_000_000_000 => ts / 1_000,
        _ => ts,
    }
}

/// Reject a date that does not exist -- `2026-02-31` would otherwise roll
/// forward to `2026-03-03` and store a day nobody wrote.
fn check_calendar_day(year: i32, month: u32, day: u32) -> bool {
    u32::try_from(year).is_ok_and(|year| day >= 1 && day <= days_in_month(year, month))
}

/// Resolve a JSON value to whole epoch seconds (UTC). Accepts a date/datetime
/// string, a numeric-string epoch, or a number (with magnitude detection).
fn to_epoch_seconds(value: &Value, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => {
            let raw = n
                .as_i64()
                .or_else(|| n.as_f64().and_then(whole_f64_as_i64))
                .ok_or_else(|| enc_err(col, "invalid epoch number"))?;
            Ok(epoch_to_seconds(raw))
        }
        Value::String(s) => {
            let s = s.trim();
            if let Ok(raw) = s.parse::<i64>() {
                return Ok(epoch_to_seconds(raw));
            }
            Ok(datetime::parse(s).map_err(|m| enc_err(col, m))?.secs)
        }
        _ => Err(enc_err(col, "expected datetime string or epoch number")),
    }
}

/// Resolve a JSON value to days since 1970-01-01 (UTC). Accepts a date string,
/// a numeric-string epoch, or a number (with magnitude detection).
fn to_epoch_days(value: &Value, col: &str) -> Result<i32, DynamicError> {
    // A bare "YYYY-MM-DD" has no time component; parse it directly. The ASCII
    // check is what makes the byte indexing below safe on a `&str`.
    if let Value::String(s) = value {
        let s = s.trim();
        if s.len() == 10 && s.is_ascii() && s.as_bytes()[4] == b'-' {
            let year: i32 = s[0..4]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date year"))?;
            let month: u32 = s[5..7]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date month"))?;
            let day: u32 = s[8..10]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date day"))?;
            if !check_calendar_day(year, month, day) {
                return Err(enc_err(col, "invalid Date components"));
            }
            let days = civil_days_from_epoch(year, month, day);
            return i32::try_from(days).map_err(|_| enc_err(col, "Date out of range"));
        }
    }
    let secs = to_epoch_seconds(value, col)?;
    let days = secs.div_euclid(86_400);
    i32::try_from(days).map_err(|_| enc_err(col, "Date out of range"))
}

/// Convert a JSON value to `DateTime64` epoch ticks at the given precision.
fn datetime64_to_ticks(value: &Value, precision: u8, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().and_then(whole_f64_as_i64))
            .ok_or_else(|| enc_err(col, "invalid DateTime64 number")),
        Value::String(s) => {
            let s = s.trim();
            // A numeric string is treated as an already-scaled tick count.
            if let Ok(n) = s.parse::<i64>() {
                return Ok(n);
            }
            datetime::parse(s)
                .map_err(|m| enc_err(col, m))?
                .ticks(precision)
                .ok_or_else(|| enc_err(col, "DateTime64 epoch overflow"))
        }
        _ => Err(enc_err(col, "expected number or datetime string")),
    }
}

// ---------------------------------------------------------------------------
// Enum
// ---------------------------------------------------------------------------

/// Resolve an enum value to its signed discriminant. A numeric value is used
/// directly; a string is mapped via the `Enum8(...)` / `Enum16(...)` member
/// list parsed from the type string.
fn enum_discriminant(value: &Value, type_str: &str, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(_) | Value::Bool(_) => as_i64(value, col),
        Value::String(s) => {
            // A numeric string is treated as the discriminant directly.
            if let Ok(n) = s.trim().parse::<i64>() {
                return Ok(n);
            }
            enum_member_value(type_str, s)
                .ok_or_else(|| enc_err(col, "enum string not found in type definition"))
        }
        _ => Err(enc_err(col, "expected enum value")),
    }
}

/// Look up `name` in an `Enum8(...)`/`Enum16(...)` definition, returning its
/// integer discriminant. Members look like `'name' = 1` separated by commas.
///
/// The split is quote-aware: a member label may itself contain a comma, as in
/// `Enum8('a,b' = 1)`.
fn enum_member_value(type_str: &str, name: &str) -> Option<i64> {
    let open = type_str.find('(')?;
    let close = type_str.rfind(')')?;
    if open >= close {
        return None;
    }
    for member in split_enum_members(&type_str[open + 1..close]) {
        let (label, num) = member.split_once('=')?;
        let label = label.trim().trim_matches('\'').trim_matches('"');
        if label == name {
            return num.trim().parse::<i64>().ok();
        }
    }
    None
}

/// Split an enum member list on the commas that sit outside a quoted label.
fn split_enum_members(inner: &str) -> Vec<&str> {
    let mut members = Vec::new();
    let mut quote: Option<char> = None;
    let mut start = 0;
    let mut escaped = false;
    for (i, c) in inner.char_indices() {
        if escaped {
            escaped = false;
        } else if c == '\\' && quote.is_some() {
            escaped = true;
        } else if let Some(q) = quote {
            if c == q {
                quote = None;
            }
        } else if c == '\'' || c == '"' {
            quote = Some(c);
        } else if c == ',' {
            members.push(&inner[start..i]);
            start = i + 1;
        }
    }
    members.push(&inner[start..]);
    members
}

// ---------------------------------------------------------------------------
// UUID / IP
// ---------------------------------------------------------------------------

fn encode_uuid(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    // Accept hyphen-less hex or any punctuation by keeping only hex digits.
    let hex: String = s.chars().filter(char::is_ascii_hexdigit).collect();
    if hex.len() != 32 {
        return Err(enc_err(col, "invalid UUID length"));
    }
    // ClickHouse RowBinary UUID: two little-endian u64, high word first.
    let high = u64::from_str_radix(&hex[..16], 16).map_err(|_| enc_err(col, "invalid UUID hex"))?;
    let low = u64::from_str_radix(&hex[16..], 16).map_err(|_| enc_err(col, "invalid UUID hex"))?;
    buf.extend_from_slice(&high.to_le_bytes());
    buf.extend_from_slice(&low.to_le_bytes());
    Ok(())
}

fn encode_ipv4(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    // Integer input is interpreted as the host-order address.
    if let Value::Number(n) = value
        && let Some(u) = n.as_u64()
    {
        let addr = Ipv4Addr::from(narrow::<u32>(u, col, "IPv4")?);
        buf.extend_from_slice(&u32::from(addr).to_le_bytes());
        return Ok(());
    }
    let s = value_to_str(value);
    let addr: Ipv4Addr = s.parse().map_err(|_| enc_err(col, "invalid IPv4"))?;
    // ClickHouse stores IPv4 as UInt32 little-endian.
    buf.extend_from_slice(&u32::from(addr).to_le_bytes());
    Ok(())
}

fn encode_ipv6(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    // An IPv6 column takes an IPv4 literal as ClickHouse's `toIPv6` does:
    // `172.17.3.4` is `::ffff:172.17.3.4`.
    let addr: Ipv6Addr = match s.parse::<IpAddr>() {
        Ok(IpAddr::V6(v6)) => v6,
        Ok(IpAddr::V4(v4)) => v4.to_ipv6_mapped(),
        Err(_) => return Err(enc_err(col, "invalid IP address")),
    };
    // 16 octets in network byte order, written verbatim.
    buf.extend_from_slice(&addr.octets());
    Ok(())
}

// ---------------------------------------------------------------------------
// Array / Map
// ---------------------------------------------------------------------------

fn encode_array(
    value: &Value,
    elem: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    let Value::Array(arr) = value else {
        return Err(enc_err(col, "expected array"));
    };
    buf.put_var_uint(arr.len() as u64);
    for item in arr {
        encode_value(item, elem, col, buf)?;
    }
    Ok(())
}

fn encode_map(
    value: &Value,
    key: &ParsedType,
    val: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    let Value::Object(obj) = value else {
        return Err(enc_err(col, "expected object for Map"));
    };
    // A JSON object key is text, so `String` (and `LowCardinality(String)`,
    // which is the bare String on this path) is the only key type that can be
    // written from it. Anything else is rejected rather than written as a
    // length-prefixed string the server would read as a different type.
    if key.tag != TypeTag::String || key.nullable {
        return Err(unsupported(col, key));
    }
    buf.put_var_uint(obj.len() as u64);
    for (k, v) in obj {
        buf.put_string(k.as_bytes());
        encode_value(v, val, col, buf)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn columns_of(cols: &[(&str, &str)]) -> Vec<ColumnDef> {
        cols.iter().map(|(n, t)| ColumnDef::new(*n, *t)).collect()
    }

    /// The row is consumed into its object, so no test row is cloned.
    fn object_of(row: Value) -> Map<String, Value> {
        let Value::Object(obj) = row else {
            panic!("a test row must be a JSON object")
        };
        obj
    }

    /// Encode a one-row map against the given columns and return wire bytes.
    fn enc(row: Value, cols: &[(&str, &str)]) -> Vec<u8> {
        DynamicRow::new(&object_of(row), &columns_of(cols))
            .encode()
            .unwrap()
    }

    fn enc_err_of(row: Value, cols: &[(&str, &str)]) -> DynamicError {
        DynamicRow::new(&object_of(row), &columns_of(cols))
            .encode()
            .unwrap_err()
    }

    // ---- String / FixedString ----

    #[test]
    fn string() {
        assert_eq!(
            enc(json!({"s": "hello"}), &[("s", "String")]),
            vec![5, b'h', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn string_from_number() {
        assert_eq!(
            enc(json!({"s": 42}), &[("s", "String")]),
            vec![2, b'4', b'2']
        );
    }

    #[test]
    fn fixed_string_pad() {
        assert_eq!(
            enc(json!({"f": "ab"}), &[("f", "FixedString(4)")]),
            vec![b'a', b'b', 0, 0]
        );
    }

    // ---- Integers ----

    #[test]
    fn unsigned_widths() {
        assert_eq!(enc(json!({"x": 7}), &[("x", "UInt8")]), vec![7]);
        assert_eq!(
            enc(json!({"x": 300}), &[("x", "UInt16")]),
            300u16.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 42}), &[("x", "UInt32")]),
            42u32.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 42}), &[("x", "UInt64")]),
            42u64.to_le_bytes()
        );
    }

    #[test]
    fn signed_widths() {
        assert_eq!(
            enc(json!({"x": -5}), &[("x", "Int8")]),
            (-5i8).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -300}), &[("x", "Int16")]),
            (-300i16).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -1}), &[("x", "Int32")]),
            (-1i32).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -100}), &[("x", "Int64")]),
            (-100i64).to_le_bytes()
        );
    }

    #[test]
    fn int128_uint128() {
        assert_eq!(
            enc(json!({"x": -100}), &[("x", "Int128")]),
            (-100i128).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 100}), &[("x", "UInt128")]),
            100u128.to_le_bytes()
        );
        // From numeric string (large value beyond i64).
        let v: i128 = 170_141_183_460_469_231_731_687_303_715_884_105_727;
        assert_eq!(
            enc(json!({"x": v.to_string()}), &[("x", "Int128")]),
            v.to_le_bytes()
        );
    }

    #[test]
    fn int256_uint256() {
        let mut expect_neg = [0xFFu8; 32];
        expect_neg[..16].copy_from_slice(&(-1i128).to_le_bytes());
        assert_eq!(enc(json!({"x": -1}), &[("x", "Int256")]), expect_neg);

        let mut expect_pos = [0u8; 32];
        expect_pos[..16].copy_from_slice(&5u128.to_le_bytes());
        assert_eq!(enc(json!({"x": 5}), &[("x", "UInt256")]), expect_pos);
    }

    // ---- Floats / Bool ----

    #[test]
    fn floats() {
        assert_eq!(
            enc(json!({"f": 2.5}), &[("f", "Float64")]),
            2.5f64.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"f": 1.5}), &[("f", "Float32")]),
            1.5f32.to_le_bytes()
        );
    }

    #[test]
    fn bool_native() {
        assert_eq!(enc(json!({"b": true}), &[("b", "Bool")]), vec![1]);
        assert_eq!(enc(json!({"b": false}), &[("b", "Bool")]), vec![0]);
    }

    #[test]
    fn bool_coercions() {
        for (v, want) in [
            (json!(1), 1u8),
            (json!(0), 0),
            (json!("true"), 1),
            (json!("false"), 0),
            (json!("yes"), 1),
            (json!("no"), 0),
            (json!("1"), 1),
            (json!("0"), 0),
        ] {
            assert_eq!(enc(json!({"b": v}), &[("b", "Bool")]), vec![want]);
        }
    }

    #[test]
    fn bool_takes_a_whole_float_and_refuses_the_rest() {
        // 1.0 parses as f64, so the old `as_i64().unwrap_or(0)` stored false.
        assert_eq!(enc(json!({"b": 1.0}), &[("b", "Bool")]), vec![1]);
        assert_eq!(enc(json!({"b": 0.0}), &[("b", "Bool")]), vec![0]);

        for v in [json!(2.5), json!(1e30), json!(-0.5)] {
            let err = enc_err_of(json!({ "b": v }), &[("b", "Bool")]).to_string();
            assert!(
                err.contains("not parseable as bool"),
                "a number the column cannot hold must fail the row, got: {err}"
            );
        }
    }

    // ---- Date / DateTime ----

    #[test]
    fn date_from_string() {
        // 2024-12-25 = day 20082 since epoch.
        let days = u16::try_from(civil_days_from_epoch(2024, 12, 25)).unwrap();
        assert_eq!(
            enc(json!({"d": "2024-12-25"}), &[("d", "Date")]),
            days.to_le_bytes()
        );
    }

    #[test]
    fn date_epoch_string() {
        assert_eq!(
            enc(json!({"d": "1970-01-01"}), &[("d", "Date")]),
            0u16.to_le_bytes()
        );
    }

    #[test]
    fn date32_from_string() {
        let days = i32::try_from(civil_days_from_epoch(2024, 12, 25)).unwrap();
        assert_eq!(
            enc(json!({"d": "2024-12-25"}), &[("d", "Date32")]),
            days.to_le_bytes()
        );
    }

    #[test]
    fn datetime_from_string() {
        // 2024-12-25 10:30:00 UTC = 1735122600. A zone argument changes
        // nothing on the wire.
        for ty in [
            "DateTime",
            "DateTime('UTC')",
            "Nullable(DateTime('Australia/Sydney'))",
        ] {
            let nullable = ty.starts_with("Nullable");
            let mut expected = Vec::new();
            if nullable {
                expected.push(0);
            }
            expected.extend_from_slice(&1_735_122_600u32.to_le_bytes());
            assert_eq!(
                enc(json!({"ts": "2024-12-25 10:30:00"}), &[("ts", ty)]),
                expected,
                "{ty}"
            );
        }
    }

    #[test]
    fn datetime_from_number_seconds() {
        assert_eq!(
            enc(json!({"ts": 1_735_122_600u64}), &[("ts", "DateTime")]),
            1_735_122_600u32.to_le_bytes()
        );
    }

    #[test]
    fn datetime_from_epoch_ms_magnitude() {
        // ms value gets scaled down to seconds.
        assert_eq!(
            enc(json!({"ts": 1_735_122_600_000i64}), &[("ts", "DateTime")]),
            1_735_122_600u32.to_le_bytes()
        );
    }

    // ---- DateTime64 ----

    #[test]
    fn datetime64_from_string_ms() {
        let expected: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095"}),
                &[("ts", "DateTime64(3)")]
            ),
            expected.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_iso8601_t_separator() {
        let expected: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07T07:03:00.095Z"}),
                &[("ts", "DateTime64(3)")]
            ),
            expected.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_from_number_passthrough() {
        let v: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(json!({"ts": v}), &[("ts", "DateTime64(3)")]),
            v.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_precision_6_and_9() {
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095123"}),
                &[("ts", "DateTime64(6)")]
            ),
            1_775_545_380_095_123i64.to_le_bytes()
        );
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095123456"}),
                &[("ts", "DateTime64(9)")]
            ),
            1_775_545_380_095_123_456i64.to_le_bytes()
        );
    }

    /// An offset moves the text to the instant it names, it is never dropped.
    #[test]
    fn datetime64_applies_an_offset() {
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 12:33:00.5+05:30"}),
                &[("ts", "DateTime64(3)")]
            ),
            1_775_545_380_500i64.to_le_bytes()
        );
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07T17:03:00+10"}),
                &[("ts", "DateTime")]
            ),
            1_775_545_380u32.to_le_bytes()
        );
    }

    // ---- Decimal ----

    #[test]
    fn decimal64() {
        // 123.45 at scale 2 -> backing 12345.
        assert_eq!(
            enc(json!({"d": 123.45}), &[("d", "Decimal(18, 2)")]),
            12_345i64.to_le_bytes()
        );
    }

    #[test]
    fn decimal_concrete_widths() {
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal32(2)")]),
            150i32.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal64(2)")]),
            150i64.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal128(2)")]),
            150i128.to_le_bytes()
        );
        let mut expect256 = [0u8; 32];
        expect256[..16].copy_from_slice(&150i128.to_le_bytes());
        assert_eq!(enc(json!({"d": 1.5}), &[("d", "Decimal256(2)")]), expect256);
    }

    // ---- UUID / IP ----

    #[test]
    fn uuid_hyphenated() {
        let bytes = enc(
            json!({"id": "12345678-1234-5678-1234-567812345678"}),
            &[("id", "UUID")],
        );
        let high = 0x1234_5678_1234_5678u64;
        let low = 0x1234_5678_1234_5678u64;
        let mut expected = high.to_le_bytes().to_vec();
        expected.extend_from_slice(&low.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn uuid_hyphenless_matches_hyphenated() {
        let with = enc(
            json!({"id": "12345678-1234-5678-1234-567812345678"}),
            &[("id", "UUID")],
        );
        let without = enc(
            json!({"id": "12345678123456781234567812345678"}),
            &[("id", "UUID")],
        );
        assert_eq!(with, without);
    }

    #[test]
    fn ipv4_dotted() {
        let bytes = enc(json!({"ip": "1.2.3.4"}), &[("ip", "IPv4")]);
        let addr: Ipv4Addr = "1.2.3.4".parse().unwrap();
        assert_eq!(bytes, u32::from(addr).to_le_bytes());
    }

    #[test]
    fn ipv4_from_integer() {
        let addr: Ipv4Addr = "1.2.3.4".parse().unwrap();
        let int = u32::from(addr);
        let bytes = enc(json!({"ip": int}), &[("ip", "IPv4")]);
        assert_eq!(bytes, int.to_le_bytes());
    }

    #[test]
    fn ipv6() {
        let bytes = enc(json!({"ip": "::1"}), &[("ip", "IPv6")]);
        let addr: Ipv6Addr = "::1".parse().unwrap();
        assert_eq!(bytes, addr.octets());

        let mapped: Ipv6Addr = "::ffff:10.0.0.1".parse().unwrap();
        assert_eq!(
            enc(json!({"ip": "10.0.0.1"}), &[("ip", "IPv6")]),
            mapped.octets()
        );
        assert!(matches!(
            enc_err_of(json!({"ip": "10.0.0"}), &[("ip", "IPv6")]),
            DynamicError::EncodingError { .. }
        ));
    }

    // ---- Enum ----

    #[test]
    fn enum8_and_enum16_numeric() {
        // Enum values are written as the underlying int discriminant. The
        // numeric discriminant is taken from the value directly.
        assert_eq!(
            enc(json!({"e": 2}), &[("e", "Enum8('a'=1,'b'=2)")]),
            vec![2]
        );
        assert_eq!(
            enc(json!({"e": 200}), &[("e", "Enum16('x'=100,'y'=200)")]),
            200i16.to_le_bytes()
        );
    }

    #[test]
    fn enum8_string_mapping() {
        // String maps to its discriminant from the type definition.
        assert_eq!(
            enc(
                json!({"e": "high"}),
                &[("e", "Enum8('low'=1, 'medium'=2, 'high'=3)")]
            ),
            vec![3]
        );
        assert_eq!(
            enc(json!({"e": "y"}), &[("e", "Enum16('x'=100, 'y'=200)")]),
            200i16.to_le_bytes()
        );
    }

    #[test]
    fn enum_unknown_string_errors() {
        let e = enc_err_of(json!({"e": "nope"}), &[("e", "Enum8('a'=1)")]);
        assert!(
            matches!(e, DynamicError::EncodingError { .. }),
            "got: {e:?}"
        );
    }

    // ---- Nullable ----

    #[test]
    fn nullable_null() {
        assert_eq!(
            enc(json!({"n": null}), &[("n", "Nullable(String)")]),
            vec![1]
        );
    }

    #[test]
    fn nullable_non_null() {
        assert_eq!(
            enc(json!({"n": "hi"}), &[("n", "Nullable(String)")]),
            vec![0, 2, b'h', b'i']
        );
    }

    #[test]
    fn nullable_datetime64() {
        let mut expected = vec![0u8];
        expected.extend_from_slice(&1_775_545_380_095i64.to_le_bytes());
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095"}),
                &[("ts", "Nullable(DateTime64(3))")]
            ),
            expected
        );
    }

    #[test]
    fn non_nullable_null_gets_default() {
        assert_eq!(enc(json!({}), &[("x", "UInt32")]), 0u32.to_le_bytes());
        // Variable-length default is an empty string.
        assert_eq!(enc(json!({}), &[("s", "String")]), vec![0]);
    }

    // ---- LowCardinality ----

    #[test]
    fn low_cardinality_encodes_inner() {
        // LowCardinality(String) on the INSERT path is just the inner String.
        assert_eq!(
            enc(json!({"c": "x"}), &[("c", "LowCardinality(String)")]),
            vec![1, b'x']
        );
    }

    #[test]
    fn low_cardinality_nullable() {
        assert_eq!(
            enc(
                json!({"c": null}),
                &[("c", "LowCardinality(Nullable(String))")]
            ),
            vec![1]
        );
        assert_eq!(
            enc(
                json!({"c": "x"}),
                &[("c", "LowCardinality(Nullable(String))")]
            ),
            vec![0, 1, b'x']
        );
    }

    // ---- Array / Map ----

    #[test]
    fn array_uint32() {
        let mut expected = vec![3u8];
        for n in [1u32, 2, 3] {
            expected.extend_from_slice(&n.to_le_bytes());
        }
        assert_eq!(
            enc(json!({"a": [1, 2, 3]}), &[("a", "Array(UInt32)")]),
            expected
        );
    }

    #[test]
    fn array_nested() {
        // Array(Array(UInt8)): outer len 2, each inner len + bytes.
        let bytes = enc(json!({"a": [[1, 2], [3]]}), &[("a", "Array(Array(UInt8))")]);
        assert_eq!(bytes, vec![2, /*inner0*/ 2, 1, 2, /*inner1*/ 1, 3]);
    }

    #[test]
    fn array_nullable_elements() {
        // Array(Nullable(UInt8)): len 2, then per element null-marker + value.
        let bytes = enc(json!({"a": [5, null]}), &[("a", "Array(Nullable(UInt8))")]);
        assert_eq!(bytes, vec![2, 0, 5, 1]);
    }

    #[test]
    fn map_string_uint32() {
        let bytes = enc(json!({"m": {"a": 1}}), &[("m", "Map(String, UInt32)")]);
        let mut expected = vec![1u8]; // count
        expected.extend_from_slice(&[1, b'a']); // key "a"
        expected.extend_from_slice(&1u32.to_le_bytes()); // value
        assert_eq!(bytes, expected);
    }

    // ---- Point ----

    /// `(value, x, y)`: a GeoJSON-ordered `[x, y]` pair, and the `{lat, lon}`
    /// object an Elastic `geo_point` carries, longitude as x.
    #[test]
    fn point_takes_a_pair_or_a_lat_lon_object() {
        let cases = [
            (json!([1.5, -2.5]), 1.5f64, -2.5f64),
            (json!([151, -33]), 151.0, -33.0),
            (json!({"lat": -33.86, "lon": 151.21}), 151.21, -33.86),
            (json!({"lon": "151.21", "lat": "-33.86"}), 151.21, -33.86),
        ];
        for (value, x, y) in cases {
            let want = [x.to_le_bytes(), y.to_le_bytes()].concat();
            assert_eq!(
                enc(json!({ "p": value }), &[("p", "Point")]),
                want,
                "{value}"
            );
        }
        // Absent from the row: the origin, as any other non-Nullable default.
        assert_eq!(enc(json!({}), &[("p", "Point")]), vec![0u8; 16]);
    }

    #[test]
    fn point_refuses_anything_that_is_not_two_coordinates() {
        for value in [
            json!([1.0]),
            json!([1.0, 2.0, 3.0]),
            json!(["a", 2.0]),
            json!({"lat": 1.0}),
            json!("POINT(1 2)"),
            json!(7),
        ] {
            let err = enc_err_of(json!({ "p": value }), &[("p", "Point")]);
            assert!(
                matches!(err, DynamicError::EncodingError { .. }),
                "{value}: {err:?}"
            );
        }
    }

    // ---- JSON ----

    #[test]
    fn json_from_object() {
        let bytes = enc(
            json!({"data": {"key": "value", "num": 42}}),
            &[("data", "JSON")],
        );
        let json_str = r#"{"key":"value","num":42}"#;
        let mut expected = Vec::new();
        expected.put_string(json_str);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn json_from_string_verbatim() {
        let payload = r#"{"event":"login","user":"alice"}"#;
        let bytes = enc(json!({"data": payload}), &[("data", "JSON")]);
        let mut expected = Vec::new();
        expected.put_string(payload);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn json_missing_value_becomes_empty_object() {
        assert_eq!(enc(json!({}), &[("data", "JSON")]), vec![2, b'{', b'}']);
    }

    #[test]
    fn json_explicit_null_becomes_empty_object() {
        assert_eq!(
            enc(json!({"data": null}), &[("data", "JSON")]),
            vec![2, b'{', b'}']
        );
    }

    #[test]
    fn json_empty_string_becomes_empty_object() {
        assert_eq!(
            enc(json!({"data": ""}), &[("data", "JSON")]),
            vec![2, b'{', b'}']
        );
    }

    #[test]
    fn nullable_json_null_and_non_null() {
        assert_eq!(enc(json!({"t": null}), &[("t", "Nullable(JSON)")]), vec![1]);
        let bytes = enc(json!({"t": {"env": "prod"}}), &[("t", "Nullable(JSON)")]);
        let json_str = r#"{"env":"prod"}"#;
        let mut expected = vec![0u8];
        expected.put_string(json_str);
        assert_eq!(bytes, expected);
    }

    // ---- JSON root-array wrapper ----

    /// Expected bytes for a JSON column holding `json_str`.
    fn json_col(json_str: &str) -> Vec<u8> {
        let mut expected = Vec::new();
        expected.put_string(json_str);
        expected
    }

    #[test]
    fn json_root_array_is_wrapped() {
        // ECS `tags` is an array, and the JSON column parser takes only an
        // object at the root.
        let bytes = enc(
            json!({"_tags": ["beats", "filebeat"]}),
            &[("_tags", "JSON")],
        );
        assert_eq!(bytes, json_col(r#"{"list":["beats","filebeat"]}"#));
        let bytes = enc(json!({"_tags": []}), &[("_tags", "JSON")]);
        assert_eq!(bytes, json_col(r#"{"list":[]}"#));
    }

    #[test]
    fn json_root_object_is_not_wrapped() {
        let bytes = enc(json!({"data": {"env": "prod"}}), &[("data", "JSON")]);
        assert_eq!(bytes, json_col(r#"{"env":"prod"}"#));
    }

    /// One key per level: `serde_json`'s key order depends on whether anything
    /// in the dependency graph turned on `preserve_order`, and a multi-key
    /// object would pin this test to whichever answer this crate's own build
    /// gives.
    #[test]
    fn json_nested_array_is_not_wrapped() {
        // Only the root is reshaped: an array inside an object already parses,
        // and lands as a Dynamic holding Array(...).
        let bytes = enc(json!({"data": {"tags": ["a", "b"]}}), &[("data", "JSON")]);
        assert_eq!(bytes, json_col(r#"{"tags":["a","b"]}"#));

        let deeper = enc(
            json!({"data": {"inner": {"more": [1, 2]}}}),
            &[("data", "JSON")],
        );
        assert_eq!(deeper, json_col(r#"{"inner":{"more":[1,2]}}"#));
    }

    #[test]
    fn json_root_scalars_and_plain_text_are_wrapped() {
        assert_eq!(
            enc(json!({"d": 42}), &[("d", "JSON")]),
            json_col(r#"{"value":42}"#)
        );
        assert_eq!(
            enc(json!({"d": "plain"}), &[("d", "JSON")]),
            json_col(r#"{"value":"plain"}"#)
        );
    }

    #[test]
    fn nullable_json_root_array_is_wrapped() {
        let bytes = enc(json!({"t": ["a"]}), &[("t", "Nullable(JSON)")]);
        let mut expected = vec![0u8];
        expected.extend_from_slice(&json_col(r#"{"list":["a"]}"#));
        assert_eq!(bytes, expected);
    }

    // ---- Raw passthrough ----

    #[test]
    fn raw_passthrough_for_json_column() {
        let columns = vec![
            ColumnDef::new("id", "UInt32"),
            ColumnDef::new("_json", "Nullable(JSON)"),
        ];
        let row = json!({"id": 1}).as_object().unwrap().clone();
        let raw_json = br#"{"event":"login","user":"alice"}"#;
        let bytes = DynamicRow::with_raw(&row, &columns, raw_json, "_json")
            .encode()
            .unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.push(0); // not null
        expected.put_string(raw_json);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn raw_passthrough_matches_value_path() {
        let columns = vec![ColumnDef::new("data", "JSON")];
        let payload = br#"{"key":"value","num":42}"#;

        let row_value = json!({"data": std::str::from_utf8(payload).unwrap()})
            .as_object()
            .unwrap()
            .clone();
        let via_value = DynamicRow::new(&row_value, &columns).encode().unwrap();

        let row_empty = json!({}).as_object().unwrap().clone();
        let via_raw = DynamicRow::with_raw(&row_empty, &columns, payload, "data")
            .encode()
            .unwrap();

        assert_eq!(via_value, via_raw);
    }

    // ---- Multi-column ----

    #[test]
    fn multi_column_order() {
        let bytes = enc(
            json!({"id": 42, "name": "test"}),
            &[("id", "UInt32"), ("name", "String")],
        );
        let mut expected = Vec::new();
        expected.extend_from_slice(&42u32.to_le_bytes());
        expected.extend_from_slice(&[4, b't', b'e', b's', b't']);
        assert_eq!(bytes, expected);
    }

    // ---- Unsupported types ----

    #[test]
    fn tuple_is_unsupported() {
        let e = enc_err_of(json!({"t": [1, 2]}), &[("t", "Tuple(UInt8, UInt8)")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    #[test]
    fn variant_is_unsupported() {
        let e = enc_err_of(json!({"v": 1}), &[("v", "Variant(UInt8, String)")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    #[test]
    fn dynamic_is_unsupported() {
        let e = enc_err_of(json!({"d": 1}), &[("d", "Dynamic")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    /// A column the encoder cannot write is unsupported for the whole insert,
    /// so the absent-value default path has to reject it too.
    #[test]
    fn an_unsupported_column_errors_whether_or_not_the_row_has_it() {
        for ty in [
            "Variant(UInt8, String)",
            "Dynamic",
            "NotAType",
            "JSON(max_dynamic_paths=lots)",
        ] {
            let with_value = enc_err_of(json!({"c": 1}), &[("c", ty)]);
            assert!(
                matches!(with_value, DynamicError::UnsupportedType { .. }),
                "{ty} with a value: {with_value:?}"
            );
            let without_value = enc_err_of(json!({}), &[("c", ty)]);
            assert!(
                matches!(without_value, DynamicError::UnsupportedType { .. }),
                "{ty} with no value: {without_value:?}"
            );
        }
    }

    // ---- Range and precision ----

    /// A value the column cannot hold fails the row; it is never written
    /// modulo the width.
    #[test]
    fn integer_widths_reject_out_of_range_rather_than_truncating() {
        for (ty, value) in [
            ("UInt8", json!(300)),
            ("UInt16", json!(70_000)),
            ("UInt32", json!(5_000_000_000u64)),
            ("Int8", json!(200)),
            ("Int8", json!(-200)),
            ("Int16", json!(40_000)),
            ("Int32", json!(3_000_000_000u64)),
            // Past i128 the f64 fallback would saturate to the maximum.
            ("Decimal128(38, 10)", json!("1e40")),
            ("Decimal128(2)", json!(1e60)),
            ("Decimal256(2)", json!("-1e60")),
            // A value wider than the column is an error, not a cut-down copy.
            ("FixedString(3)", json!("abcdef")),
            ("FixedString(2)", json!("\u{e9}\u{e9}\u{e9}")),
            // One past each i128 edge: the exact parser overflows, and the
            // f64 rounding of the text would land back on the edge.
            (
                "Decimal128(0)",
                json!("170141183460469231731687303715884105728"),
            ),
            (
                "Decimal128(0)",
                json!("-170141183460469231731687303715884105729"),
            ),
        ] {
            let e = enc_err_of(json!({ "x": value }), &[("x", ty)]);
            assert!(
                matches!(e, DynamicError::EncodingError { .. }),
                "{ty} must reject: {e:?}"
            );
            assert!(format!("{e}").contains("out of range"), "{ty}: {e}");
        }
        // The error names the column and how wide the value was.
        let e = enc_err_of(json!({"tag": "abcdef"}), &[("tag", "FixedString(3)")]);
        let text = e.to_string();
        assert!(text.contains("'tag'") && text.contains("6 bytes"), "{text}");
        // The widest value each column CAN hold still encodes.
        assert_eq!(enc(json!({"x": 255}), &[("x", "UInt8")]), vec![255]);
        assert_eq!(
            enc(json!({"x": -128}), &[("x", "Int8")]),
            (-128i8).to_le_bytes()
        );
    }

    /// The full 38 digits reach the wire, which f64 scaling cannot carry.
    #[test]
    fn decimal128_keeps_all_38_significant_digits() {
        let text = "1234567890123456789012345678.1234567890";
        let expected: i128 = 12_345_678_901_234_567_890_123_456_781_234_567_890;
        assert_eq!(
            enc(json!({ "d": text }), &[("d", "Decimal128(38, 10)")]),
            expected.to_le_bytes()
        );

        // Half-up on the first dropped fractional digit, and a sign.
        assert_eq!(
            enc(json!({"d": "1.005"}), &[("d", "Decimal64(18, 2)")]),
            101i64.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"d": "-1.005"}), &[("d", "Decimal64(18, 2)")]),
            (-101i64).to_le_bytes()
        );
        // A JSON float is stringified first, so 123.45 scales exactly.
        assert_eq!(
            enc(json!({"d": 123.45}), &[("d", "Decimal(18, 2)")]),
            12_345i64.to_le_bytes()
        );
    }

    /// A 19-byte string can carry a multibyte char, so the parser's byte
    /// indexing has to reject it rather than split a char boundary.
    #[test]
    fn datetime_string_rejects_non_ascii_without_panicking() {
        for ty in ["DateTime", "DateTime64(3)", "Date", "Date32"] {
            for value in ["abc\u{e9}12345T00:00:00", "2026-01-0\u{e9}", "\u{4e2d}"] {
                let e = enc_err_of(json!({ "ts": value }), &[("ts", ty)]);
                assert!(
                    matches!(e, DynamicError::EncodingError { .. }),
                    "{ty} / {value:?}: {e:?}"
                );
            }
        }
    }

    #[test]
    fn an_impossible_calendar_day_is_rejected() {
        for (ty, value) in [
            ("Date", "2026-02-31"),
            ("Date", "2026-04-31"),
            ("Date32", "2025-02-29"),
            ("DateTime", "2026-02-30 00:00:00"),
            ("DateTime64(3)", "2026-13-01 00:00:00"),
        ] {
            let e = enc_err_of(json!({ "d": value }), &[("d", ty)]);
            assert!(
                matches!(e, DynamicError::EncodingError { .. }),
                "{ty} / {value}: {e:?}"
            );
        }
        // 2024 is a leap year, so the 29th of February exists.
        let days = u16::try_from(civil_days_from_epoch(2024, 2, 29)).unwrap();
        assert_eq!(
            enc(json!({"d": "2024-02-29"}), &[("d", "Date")]),
            days.to_le_bytes()
        );
    }

    #[test]
    fn map_with_a_non_string_key_is_rejected() {
        for ty in [
            "Map(UInt32, String)",
            "Map(Int64, String)",
            "Map(FixedString(4), String)",
            "Map(UUID, String)",
        ] {
            let e = enc_err_of(json!({"m": {"1": "a"}}), &[("m", ty)]);
            assert!(
                matches!(e, DynamicError::UnsupportedType { .. }),
                "{ty}: {e:?}"
            );
        }
        // LowCardinality(String) keys are the bare String on this path.
        let bytes = enc(
            json!({"m": {"a": 1}}),
            &[("m", "Map(LowCardinality(String), UInt32)")],
        );
        assert_eq!(
            bytes,
            enc(json!({"m": {"a": 1}}), &[("m", "Map(String, UInt32)")])
        );
    }

    #[test]
    fn enum_member_labels_may_contain_a_comma() {
        assert_eq!(
            enc(json!({"e": "a,b"}), &[("e", "Enum8('a,b' = 1, 'c' = 2)")]),
            vec![1]
        );
        assert_eq!(
            enc(json!({"e": "c"}), &[("e", "Enum8('a,b' = 1, 'c' = 2)")]),
            vec![2]
        );
    }

    /// Magnitude detection reads the absolute value, so a pre-1970 timestamp
    /// is scaled down from milliseconds rather than taken as raw seconds.
    #[test]
    fn a_pre_epoch_millisecond_timestamp_scales_like_a_post_epoch_one() {
        assert_eq!(epoch_to_seconds(-1_600_000_000_000), -1_600_000_000);
        assert_eq!(epoch_to_seconds(1_600_000_000_000), 1_600_000_000);
    }

    // ---- civil days ----

    #[test]
    fn civil_days_known() {
        assert_eq!(civil_days_from_epoch(1970, 1, 1), 0);
        assert_eq!(civil_days_from_epoch(2026, 4, 7), 20_550);
        assert_eq!(civil_days_from_epoch(1969, 12, 31), -1);
    }

    // ---- Arena ----

    /// The batching primitive: N rows into one buffer are byte-identical to N
    /// separate `encode()` calls concatenated.
    #[test]
    fn with_defaults_flags_only_an_omitted_defaulted_column() {
        let columns = [
            ColumnDef::new("id", "UInt32"),
            ColumnDef::with_default_kind("d", "String", "DEFAULT"),
            ColumnDef::with_default_kind("j", "String", "DEFAULT"),
        ];
        // (row, raw `j`, bytes): a required column absent from the row still
        // writes its value, and a raw passthrough counts as supplied.
        let cases: [(serde_json::Value, Option<&[u8]>, &[u8]); 3] = [
            (
                json!({"id": 1, "d": "x"}),
                None,
                &[0, 1, 0, 0, 0, 0, 1, b'x', 1],
            ),
            (json!({"id": 1}), None, &[0, 1, 0, 0, 0, 1, 1]),
            (
                json!({}),
                Some(b"{}"),
                &[0, 0, 0, 0, 0, 1, 0, 2, b'{', b'}'],
            ),
        ];
        for (row, raw, want) in cases {
            let obj = row.as_object().unwrap().clone();
            let encoder = match raw {
                Some(bytes) => DynamicRow::with_raw(&obj, &columns, bytes, "j"),
                None => DynamicRow::new(&obj, &columns),
            };
            let mut buf = Vec::new();
            encoder.encode_with_defaults_into(&mut buf).unwrap();
            assert_eq!(buf, want, "{row}");
        }
    }

    #[test]
    fn encode_into_appends_without_disturbing_earlier_rows() {
        let columns = [
            ColumnDef::new("id", "UInt32"),
            ColumnDef::new("s", "String"),
        ];
        let rows = [json!({"id": 1, "s": "a"}), json!({"id": 2, "s": "bb"})];

        let mut arena = Vec::new();
        let mut ends = Vec::new();
        let mut concatenated = Vec::new();
        for row in &rows {
            let obj = row.as_object().unwrap().clone();
            DynamicRow::new(&obj, &columns)
                .encode_into(&mut arena)
                .unwrap();
            ends.push(arena.len());
            concatenated.extend_from_slice(&DynamicRow::new(&obj, &columns).encode().unwrap());
        }

        assert_eq!(arena, concatenated);
        assert_eq!(ends.len(), 2);
        assert_eq!(&arena[..ends[0]], &[1, 0, 0, 0, 1, b'a']);
        assert_eq!(&arena[ends[0]..ends[1]], &[2, 0, 0, 0, 2, b'b', b'b']);
    }

    proptest::proptest! {
        /// The encoder never panics: whatever JSON meets whatever type string,
        /// the answer is bytes or a `DynamicError`.
        #[test]
        fn encoding_any_value_against_any_type_never_panics(
            value in arbitrary_value(),
            ty in proptest::sample::select(TYPE_STRINGS),
        ) {
            let columns = [ColumnDef::new("c", ty)];
            let mut row = Map::new();
            row.insert("c".to_string(), value);
            let mut buf = Vec::new();
            let _ = DynamicRow::new(&row, &columns).encode_into(&mut buf);
        }
    }

    /// Every type string the parser has a branch for, plus the shapes that
    /// exercise the rejection paths.
    const TYPE_STRINGS: &[&str] = &[
        "String",
        "FixedString(4)",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "UInt128",
        "UInt256",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Int128",
        "Int256",
        "Float32",
        "Float64",
        "Bool",
        "Date",
        "Date32",
        "DateTime",
        "DateTime64(3)",
        "DateTime64(9, 'UTC')",
        "Decimal(38, 10)",
        "Decimal32(2)",
        "Decimal64(4)",
        "Decimal128(8)",
        "Decimal256(8)",
        "UUID",
        "IPv4",
        "IPv6",
        "Enum8('a' = 1, 'b' = 2)",
        "Enum16('x' = 100)",
        "Array(UInt32)",
        "Array(Nullable(String))",
        "Map(String, UInt32)",
        "Map(UInt32, String)",
        "JSON",
        "JSON(max_dynamic_paths=2048)",
        "Nullable(JSON)",
        "Nullable(String)",
        "Nullable(DateTime64(3))",
        "LowCardinality(Nullable(String))",
        "Point",
        "Tuple(UInt8, UInt8)",
        "Variant(UInt8, String)",
        "Dynamic",
        "SomeTypeFromTheFuture",
    ];

    /// A bounded `serde_json::Value`: depth, collection size and string length
    /// are all capped so a case stays cheap to encode.
    fn arbitrary_value() -> impl proptest::strategy::Strategy<Value = Value> {
        use proptest::prelude::*;

        let text = proptest::collection::vec(any::<char>(), 0..12)
            .prop_map(|chars| chars.into_iter().collect::<String>());
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| Value::Number(n.into())),
            any::<u64>().prop_map(|n| Value::Number(n.into())),
            (-1e18f64..1e18).prop_filter_map("representable in JSON", |f| {
                serde_json::Number::from_f64(f).map(Value::Number)
            }),
            text.clone().prop_map(Value::String),
        ];
        leaf.prop_recursive(3, 24, 4, move |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                proptest::collection::vec((text.clone(), inner), 0..4)
                    .prop_map(|pairs| Value::Object(pairs.into_iter().collect())),
            ]
        })
    }
}
