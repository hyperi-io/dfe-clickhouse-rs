//! Columnar block encoder for native INSERT.
//!
//! Transposes row-oriented `RowBinary` data (one `Vec<u8>` per row) into the
//! native columnar wire format used by `ClickHouse` data blocks.
//!
//! # Supported types for INSERT
//!
//! All scalar fixed-size types, String, FixedString(N), Point, Nullable(T),
//! LowCardinality(T), Array(T), Map(K, V), Tuple(T1..Tn), and nested combinations.
//! `LowCardinality` is fully encoded with a per-block dictionary + indices.
//! JSON, at any depth, is declared and written as String and cast server-side.
//! Variant and Dynamic are not yet supported.

use crate::error::{Error, Result};
use crate::native::columns::{ColumnType, json_spelled_as_string};
use crate::native::io::ClickHouseBytesWrite;

/// Minimum server protocol revision where a block carries a per-column
/// `custom_serialization` flag byte before the values. The sole definition:
/// both the encoder and the decoder gate on it.
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;

/// Flag bit 9, which `ClickHouse` requires on every client INSERT block's
/// `LowCardinality` column.
const HAS_ADDITIONAL_KEYS: u64 = 1 << 9;

/// A `Point` cell in `RowBinary`: x then y, each a little-endian `Float64`.
const POINT_BYTES: usize = 16;
const POINT_HALF: usize = POINT_BYTES / 2;

/// Column schema entry for a native INSERT block.
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// Column name as declared to the server.
    pub(crate) name: String,
    /// Type name the block declares: the server's, with each JSON spelled
    /// `String`.
    pub(crate) type_name: String,
    /// Parsed column type used for encoding decisions.
    pub(crate) col_type: ColumnType,
}

impl ColumnSchema {
    /// Build a `ColumnSchema` list from server-provided `(name, type_name)` pairs.
    ///
    /// # Errors
    ///
    /// [`Error::BadResponse`] for a type name this codec cannot encode.
    pub fn from_headers(headers: &[(String, String)]) -> Result<Vec<Self>> {
        headers
            .iter()
            .map(|(name, type_name)| {
                let col_type = ColumnType::parse(type_name).ok_or_else(|| {
                    Error::BadResponse(format!(
                        "native INSERT: unsupported column type '{type_name}' \
                             for column '{name}'"
                    ))
                })?;
                Ok(ColumnSchema {
                    name: name.clone(),
                    type_name: json_spelled_as_string(type_name, &col_type).into_owned(),
                    col_type,
                })
            })
            .collect()
    }
}

/// Encode buffered `RowBinary` rows into native columnar block column bytes.
///
/// Returns a flat byte buffer containing, for each column in order:
/// - `string(column_name)`
/// - `string(column_type_name)`
/// - optional custom-serialization flag byte (0x00) for newer servers
/// - column data (native columnar encoding, recursively for Array/Map/Tuple)
///
/// This output is written directly after the block header
/// (`num_columns` + `num_rows`) in a Data packet.
///
/// # Performance
///
/// Every cell is a borrowed slice of the caller's row buffers; the only
/// allocations left are the per-column slice vectors, the `LowCardinality`
/// dictionary index list, and one default buffer per Nullable column.
///
/// # Errors
///
/// Returns `Error::BadResponse` if any row's `RowBinary` data is truncated or
/// contains an unsupported type for INSERT.
pub fn encode_columns(
    rows: &[Vec<u8>],
    columns: &[ColumnSchema],
    revision: u64,
) -> Result<Vec<u8>> {
    let has_custom_ser = revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
    if columns.is_empty() || rows.is_empty() {
        return Ok(Vec::new());
    }

    // Pass 1 -- record per-column byte ranges into the caller's row buffers.
    let n = rows.len();
    let mut per_col: Vec<Vec<&[u8]>> = (0..columns.len()).map(|_| Vec::with_capacity(n)).collect();
    for row in rows {
        let mut pos = 0;
        for (ci, col) in columns.iter().enumerate() {
            let start = pos;
            rb_advance(row, &mut pos, &col.col_type)?;
            per_col[ci].push(&row[start..pos]);
        }
        // A caller narrowing the column set below what the row's RowBinary
        // carries would otherwise ship only the declared prefix of each row.
        if pos != row.len() {
            return Err(Error::BadResponse(format!(
                "native INSERT: row has {} trailing RowBinary bytes after the {} declared column(s)",
                row.len() - pos,
                columns.len(),
            )));
        }
    }

    // Pass 2 -- emit header + native-encoded data for each column, pre-sized
    // from the recorded slice lengths so a wide schema costs no reallocations.
    let payload_size: usize = per_col.iter().flat_map(|c| c.iter().map(|s| s.len())).sum();
    let header_size: usize = columns
        .iter()
        .map(|c| c.name.len() + c.type_name.len() + 16)
        .sum();
    let mut out = Vec::with_capacity(payload_size + header_size);
    for (ci, col) in columns.iter().enumerate() {
        out.put_string(col.name.as_bytes());
        // The server converts block columns to the table's types by position
        // (`InterpreterInsertQuery.cpp`, `makeConvertingActions`), JSON included.
        out.put_string(col.type_name.as_bytes());
        // Newer servers expect a custom-serialization flag byte (0 = normal) per column.
        if has_custom_ser {
            out.push(0u8);
        }
        write_col_prefixes(&col.col_type, &mut out);
        write_col_values(&per_col[ci], &col.col_type, &mut out)?;
    }

    Ok(out)
}

/// Write the serialisation prefixes for `col_type`'s whole tree, in the order
/// the server expects them.
///
/// `NativeWriter.cpp:93-94` calls `serializeBinaryBulkStatePrefix` for the
/// entire column before `serializeBinaryBulkWithMultipleStreams`, so every
/// nested prefix precedes ALL of the column's data. Writing a prefix inline
/// where its child sits agrees with that only when nothing precedes the child:
/// true inside a `Tuple`, false behind an `Array`'s offsets, where the server
/// rejects the block with "Invalid version for `SerializationLowCardinality`
/// key column" (code 117).
///
/// `LowCardinality` alone is hoisted here; its prefix is the fixed version
/// word. `Variant`, `Dynamic` and `JSON` are not encoded by this writer at
/// all, so they have no prefix to place.
fn write_col_prefixes(col_type: &ColumnType, out: &mut Vec<u8>) {
    match col_type {
        // The dictionary belongs to the data phase, so this does not recurse
        // into the inner type.
        ColumnType::LowCardinality(_) => out.extend_from_slice(&1u64.to_le_bytes()),
        ColumnType::Nullable(inner)
        | ColumnType::Array(inner)
        | ColumnType::SimpleAggregateFunction(inner) => write_col_prefixes(inner, out),
        ColumnType::Tuple(fields) => {
            for field in fields {
                write_col_prefixes(field, out);
            }
        }
        ColumnType::Map(key, value) => {
            write_col_prefixes(key, out);
            write_col_prefixes(value, out);
        }
        _ => {}
    }
}

/// Recursively write native columnar data for `values`, one `RowBinary` cell
/// per row.
// One arm per composite wire shape; splitting it would scatter the layout.
#[allow(clippy::too_many_lines)]
fn write_col_values(values: &[&[u8]], col_type: &ColumnType, out: &mut Vec<u8>) -> Result<()> {
    // Fixed-size scalars, String, and FixedString: RowBinary bytes == native bytes.
    if col_type.fixed_size().is_some()
        || matches!(
            col_type,
            ColumnType::String
                | ColumnType::FixedString(_)
                | ColumnType::Json
                | ColumnType::NewJson
                | ColumnType::TypedJson
        )
    {
        for v in values {
            out.extend_from_slice(v);
        }
        return Ok(());
    }

    match col_type {
        ColumnType::Nullable(inner) => {
            // Native: u8[n] null flags, then inner_type[n] values (zero for nulls).
            // Every null row shares one default buffer.
            let mut default = Vec::new();
            rb_write_default(&mut default, inner);
            let mut inner_refs: Vec<&[u8]> = Vec::with_capacity(values.len());
            for v in values {
                let (&flag, rest) = v.split_first().ok_or_else(rb_truncated)?;
                out.push(flag); // RowBinary: 0 = has value, 1 = null
                inner_refs.push(if flag == 0 { rest } else { &default });
            }
            write_col_values(&inner_refs, inner, out)?;
        }

        ColumnType::LowCardinality(inner) => {
            // LowCardinality wire format (ClickHouse native INSERT):
            //   u64 version = 1
            //   u64 flags = HAS_ADDITIONAL_KEYS (bit 9) | index_type (bits 0-1)
            //   u64 dict_size + dict_size values (of dict_type)
            //   u64 num_indices + indices (1/2/4/8 bytes each)
            //
            // For LowCardinality(Nullable(T)), the DICTIONARY type is T (not Nullable(T)).
            // ClickHouse stores nullable LC as a T-typed dict with index 0 always
            // pointing to the default T value (representing NULL).
            //
            // For LowCardinality(T) (non-nullable), dict type is T directly.

            // Determine dict type and extract RowBinary key bytes from each value.
            let (dict_type, is_nullable_inner) =
                if let ColumnType::Nullable(t_inner) = inner.as_ref() {
                    (t_inner.as_ref(), true)
                } else {
                    (inner.as_ref(), false)
                };

            let mut default_val = Vec::new();
            let mut dict: Vec<&[u8]> = Vec::new();
            let mut seen: std::collections::HashMap<&[u8], u32> =
                std::collections::HashMap::default();

            if is_nullable_inner {
                // Index 0 is NULL, written as the default T. It stays out of
                // `seen`, or a non-null default such as "" would map onto it.
                rb_write_default(&mut default_val, dict_type);
                dict.push(&default_val);
            }

            let mut indices: Vec<u32> = Vec::with_capacity(values.len());
            for v in values {
                // A Nullable inner arrives wrapped: [0x01] is NULL, [0x00, T..]
                // is Some(T), and the wrapper is stripped for the dictionary.
                let key: Option<&[u8]> = if is_nullable_inner {
                    match v.split_first() {
                        None | Some((0x01, _)) => None,
                        Some((_, rest)) => Some(rest),
                    }
                } else {
                    Some(v)
                };

                let idx = match key {
                    None => 0,
                    Some(bytes) => {
                        let next = u32::try_from(dict.len()).map_err(|_| {
                            Error::BadResponse(
                                "native INSERT: LowCardinality dictionary exceeds u32 indices"
                                    .to_string(),
                            )
                        })?;
                        *seen.entry(bytes).or_insert_with(|| {
                            dict.push(bytes);
                            next
                        })
                    }
                };
                indices.push(idx);
            }

            // Narrowest index width that addresses the whole dictionary; a u32
            // index vector caps the width at 4 bytes, so code 3 is unreachable.
            let (index_type, index_bytes): (u64, usize) = if dict.len() <= 0x100 {
                (0, 1)
            } else if dict.len() <= 0x1_0000 {
                (1, 2)
            } else {
                (2, 4)
            };

            let flags = HAS_ADDITIONAL_KEYS | index_type;

            // The version word is not written here: it is this column's
            // serialisation prefix, emitted by `write_col_prefixes` ahead of
            // all of the column's data.
            out.extend_from_slice(&flags.to_le_bytes()); // flags
            out.extend_from_slice(&(dict.len() as u64).to_le_bytes()); // dict_size
            // Dictionary type is T, never Nullable(T).
            write_col_values(&dict, dict_type, out)?;
            out.extend_from_slice(&(indices.len() as u64).to_le_bytes()); // num_indices
            for idx in &indices {
                out.extend_from_slice(&idx.to_le_bytes()[..index_bytes]);
            }
        }

        ColumnType::Array(inner) => {
            // Native: u64[n] cumulative offsets, then all elements as a sub-column.
            let mut cum: u64 = 0;
            let mut offsets: Vec<u64> = Vec::with_capacity(values.len());
            let mut all_elems: Vec<&[u8]> = Vec::new();

            for v in values {
                let (count, mut pos) = rb_read_varuint(v, 0)?;
                for _ in 0..count {
                    let start = pos;
                    rb_advance(v, &mut pos, inner)?;
                    all_elems.push(&v[start..pos]);
                }
                cum += count;
                offsets.push(cum);
            }

            for off in &offsets {
                out.extend_from_slice(&off.to_le_bytes());
            }
            write_col_values(&all_elems, inner, out)?;
        }

        ColumnType::Map(key_type, val_type) => {
            // Native: u64[n] cumulative offsets, then key sub-column, then value sub-column.
            let mut cum: u64 = 0;
            let mut offsets: Vec<u64> = Vec::with_capacity(values.len());
            let mut all_keys: Vec<&[u8]> = Vec::new();
            let mut all_vals: Vec<&[u8]> = Vec::new();

            for v in values {
                let (count, mut pos) = rb_read_varuint(v, 0)?;
                for _ in 0..count {
                    let ks = pos;
                    rb_advance(v, &mut pos, key_type)?;
                    all_keys.push(&v[ks..pos]);
                    let vs = pos;
                    rb_advance(v, &mut pos, val_type)?;
                    all_vals.push(&v[vs..pos]);
                }
                cum += count;
                offsets.push(cum);
            }

            for off in &offsets {
                out.extend_from_slice(&off.to_le_bytes());
            }
            write_col_values(&all_keys, key_type, out)?;
            write_col_values(&all_vals, val_type, out)?;
        }

        ColumnType::Tuple(fields) => {
            // Native: each field is a separate sub-column in definition order.
            let mut field_vals: Vec<Vec<&[u8]>> =
                vec![Vec::with_capacity(values.len()); fields.len()];
            for v in values {
                let mut pos = 0;
                for (fi, field_type) in fields.iter().enumerate() {
                    let start = pos;
                    rb_advance(v, &mut pos, field_type)?;
                    field_vals[fi].push(&v[start..pos]);
                }
            }
            for (field, field_type) in field_vals.iter().zip(fields) {
                write_col_values(field, field_type, out)?;
            }
        }

        // The wrapper names the server's merge function and is its inner
        // type on the wire, in both RowBinary and Native.
        ColumnType::SimpleAggregateFunction(inner) => write_col_values(values, inner, out)?,

        ColumnType::Point => {
            // Native: `Tuple(Float64, Float64)`, so every x, then every y.
            for half in [0..POINT_HALF, POINT_HALF..POINT_BYTES] {
                for v in values {
                    out.extend_from_slice(v.get(half.clone()).ok_or_else(rb_truncated)?);
                }
            }
        }

        unsupported => {
            return Err(Error::BadResponse(format!(
                "native INSERT: column type {unsupported:?} is not supported for INSERT"
            )));
        }
    }

    Ok(())
}

/// Advance `pos` past one RowBinary-encoded value of `col_type`.
///
/// `RowBinary` and native wire formats are identical for all scalar types.
/// Only `Nullable` differs: `RowBinary` has a per-row flag followed by the
/// value (or nothing for null), while native packs flags and values separately.
fn rb_advance(data: &[u8], pos: &mut usize, col_type: &ColumnType) -> Result<()> {
    // Fixed-size types: same byte count in RowBinary and native.
    if let Some(size) = col_type.fixed_size() {
        let end = pos.checked_add(size).ok_or_else(rb_truncated)?;
        if end > data.len() {
            return Err(rb_truncated());
        }
        *pos = end;
        return Ok(());
    }

    match col_type {
        ColumnType::String | ColumnType::Json | ColumnType::NewJson | ColumnType::TypedJson => {
            let (len, hdr) = rb_read_varuint(data, *pos)?;
            let end = usize::try_from(len)
                .ok()
                .and_then(|len| pos.checked_add(hdr)?.checked_add(len))
                .ok_or_else(rb_truncated)?;
            if end > data.len() {
                return Err(rb_truncated());
            }
            *pos = end;
        }
        ColumnType::FixedString(n) => {
            let end = pos.checked_add(*n).ok_or_else(rb_truncated)?;
            if end > data.len() {
                return Err(rb_truncated());
            }
            *pos = end;
        }
        ColumnType::Point => {
            let end = pos.checked_add(POINT_BYTES).ok_or_else(rb_truncated)?;
            if end > data.len() {
                return Err(rb_truncated());
            }
            *pos = end;
        }
        ColumnType::Nullable(inner) => {
            if *pos >= data.len() {
                return Err(rb_truncated());
            }
            let flag = data[*pos];
            *pos += 1;
            if flag == 0 {
                rb_advance(data, pos, inner)?;
            }
        }
        ColumnType::LowCardinality(inner) => {
            // RowBinary serialises LowCardinality transparently as the inner type.
            rb_advance(data, pos, inner)?;
        }
        ColumnType::SimpleAggregateFunction(inner) => rb_advance(data, pos, inner)?,
        ColumnType::Array(inner) => {
            let (count, hdr) = rb_read_varuint(data, *pos)?;
            *pos += hdr;
            for _ in 0..count {
                rb_advance(data, pos, inner)?;
            }
        }
        ColumnType::Tuple(fields) => {
            for field in fields {
                rb_advance(data, pos, field)?;
            }
        }
        ColumnType::Map(key_type, val_type) => {
            let (count, hdr) = rb_read_varuint(data, *pos)?;
            *pos += hdr;
            for _ in 0..count {
                rb_advance(data, pos, key_type)?;
                rb_advance(data, pos, val_type)?;
            }
        }
        unsupported => {
            return Err(Error::BadResponse(format!(
                "native INSERT: column type {unsupported:?} is not supported for INSERT"
            )));
        }
    }
    Ok(())
}

/// Write the default (zero) native encoding for `col_type`.
///
/// Used to fill the value slot for NULL rows in a Nullable column  --
/// the native protocol requires value bytes even when the null flag is set.
fn rb_write_default(out: &mut Vec<u8>, col_type: &ColumnType) {
    if let Some(size) = col_type.fixed_size() {
        out.extend(std::iter::repeat_n(0u8, size));
        return;
    }
    match col_type {
        ColumnType::String | ColumnType::Json | ColumnType::NewJson | ColumnType::TypedJson => {
            out.put_var_uint(0); // empty string: single 0x00 varuint
        }
        ColumnType::FixedString(n) => {
            out.extend(std::iter::repeat_n(0u8, *n));
        }
        ColumnType::Point => {
            out.extend(std::iter::repeat_n(0u8, POINT_BYTES));
        }
        ColumnType::LowCardinality(inner) | ColumnType::SimpleAggregateFunction(inner) => {
            rb_write_default(out, inner);
        }
        _ => {
            // Best-effort: empty string for unknown variable-length types
            out.put_var_uint(0);
        }
    }
}

/// Read a varuint from `data` starting at `pos`, returning `(value, bytes_consumed)`.
fn rb_read_varuint(data: &[u8], pos: usize) -> Result<(u64, usize)> {
    let tail = data.get(pos..).ok_or_else(rb_truncated)?;
    match crate::native::io::get_var_uint(tail) {
        Err(Error::NotEnoughData) => Err(rb_truncated()),
        other => other,
    }
}

fn rb_truncated() -> Error {
    Error::BadResponse(
        "native INSERT: RowBinary row data is truncated; \
         does the row struct match the table schema?"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u8_col() -> ColumnSchema {
        ColumnSchema {
            name: "n".to_string(),
            type_name: "UInt8".to_string(),
            col_type: ColumnType::UInt8,
        }
    }

    fn str_col() -> ColumnSchema {
        ColumnSchema {
            name: "s".to_string(),
            type_name: "String".to_string(),
            col_type: ColumnType::String,
        }
    }

    fn nullable_u8_col() -> ColumnSchema {
        ColumnSchema {
            name: "n".to_string(),
            type_name: "Nullable(UInt8)".to_string(),
            col_type: ColumnType::Nullable(Box::new(ColumnType::UInt8)),
        }
    }

    #[test]
    fn test_encode_single_u8_column() {
        // Two rows: UInt8 values 1 and 2
        let rows = vec![vec![1u8], vec![2u8]];
        let cols = vec![u8_col()];
        let out = encode_columns(&rows, &cols, 0).unwrap();

        // string("n") = varuint(1) + "n"
        // string("UInt8") = varuint(5) + "UInt8"
        // data = [1, 2]
        let expected_name = b"\x01n";
        let expected_type = b"\x05UInt8";
        let expected_data = b"\x01\x02";
        assert!(out.starts_with(expected_name));
        let after_name = &out[expected_name.len()..];
        assert!(after_name.starts_with(expected_type));
        let after_type = &after_name[expected_type.len()..];
        assert_eq!(after_type, expected_data);
    }

    #[test]
    fn test_encode_string_column() {
        // One row: String "hi"
        let mut row = Vec::new();
        row.push(0x02u8); // varuint(2)
        row.extend_from_slice(b"hi");
        let rows = vec![row.clone()];
        let cols = vec![str_col()];
        let out = encode_columns(&rows, &cols, 0).unwrap();
        // After header: the string bytes from RowBinary are passed through unchanged
        let after_hdr = out[b"\x01s\x06String".len()..].to_vec();
        assert_eq!(after_hdr, row);
    }

    #[test]
    fn test_encode_nullable_u8_not_null() {
        // One row: Nullable(UInt8) = Some(42)
        // RowBinary: [0x00 (not null), 42]
        let rows = vec![vec![0x00u8, 42u8]];
        let cols = vec![nullable_u8_col()];
        let out = encode_columns(&rows, &cols, 0).unwrap();
        // After header: [0x00 (null flag)] then [42 (value)]
        let hdr_len = b"\x01n\x10Nullable(UInt8)".len();
        let data = &out[hdr_len..];
        assert_eq!(data, &[0x00u8, 42u8]); // flag then value
    }

    #[test]
    fn test_encode_nullable_u8_null() {
        // One row: Nullable(UInt8) = None
        // RowBinary: [0x01 (null)]
        let rows = vec![vec![0x01u8]];
        let cols = vec![nullable_u8_col()];
        let out = encode_columns(&rows, &cols, 0).unwrap();
        // After header: [0x01 (null flag)] then [0x00 (zero default value)]
        let hdr_len = b"\x01n\x10Nullable(UInt8)".len();
        let data = &out[hdr_len..];
        assert_eq!(data, &[0x01u8, 0x00u8]);
    }

    #[test]
    fn json_column_is_declared_and_written_as_string() {
        let cols = ColumnSchema::from_headers(&[("j".to_string(), "JSON".to_string())]).unwrap();
        let rows = vec![b"\x02{}".to_vec(), b"\x07{\"a\":1}".to_vec()];
        let out = encode_columns(&rows, &cols, 0).unwrap();
        assert_eq!(out.as_slice(), b"\x01j\x06String\x02{}\x07{\"a\":1}");
    }

    /// `(server type, type the block declares)`: every JSON in the tree is
    /// declared `String`, because the encoder writes JSON as text at any depth.
    #[test]
    fn json_is_declared_as_string_wherever_it_nests() {
        let cases = [
            ("JSON(max_dynamic_paths=2048)", "String"),
            ("JSON(`a.b` UInt32)", "String"),
            ("Object('json')", "String"),
            ("Nullable(JSON)", "Nullable(String)"),
            ("Array(JSON)", "Array(String)"),
            ("Array(JSON(max_dynamic_paths=2048))", "Array(String)"),
            (
                "Map(LowCardinality(String), JSON(max_dynamic_paths=16))",
                "Map(LowCardinality(String), String)",
            ),
            ("Tuple(UInt8, Array(JSON))", "Tuple(UInt8, Array(String))"),
            // Only the type is rewritten, never a label that spells it.
            (
                "Enum8('JSON' = 1, 'XML' = 2)",
                "Enum8('JSON' = 1, 'XML' = 2)",
            ),
            (
                "Map(LowCardinality(String), String)",
                "Map(LowCardinality(String), String)",
            ),
            ("DateTime64(3, 'UTC')", "DateTime64(3, 'UTC')"),
        ];
        for (server, declared) in cases {
            let cols = ColumnSchema::from_headers(&[("c".to_string(), server.to_string())])
                .unwrap_or_else(|e| panic!("{server}: {e}"));
            assert_eq!(cols[0].type_name, declared, "{server}");
        }
    }

    /// `Point` is two `Float64` sub-columns on the wire: every x, then every y.
    #[test]
    fn a_point_column_writes_the_xs_then_the_ys() {
        let row = |x: f64, y: f64| [x.to_le_bytes(), y.to_le_bytes()].concat();
        let rows = vec![row(1.5, 2.5), row(-3.0, 4.0)];
        let out = encode_columns(&rows, &schema("Point"), 0).expect("encodes");
        let want = [1.5f64, -3.0, 2.5, 4.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<u8>>();
        assert_eq!(body(&out, "Point"), want);
    }

    #[test]
    fn encode_columns_rejects_trailing_rowbinary_bytes() {
        // Regression: encode_columns previously processed columns.len()
        // fields per row and silently ignored trailing RowBinary bytes.
        // If a caller passes T=(id,name) with columns=[id], the encoder
        // would silently drop name's bytes -- silent data loss on both
        // HTTP `with_columns` and TCP `with_columns_tcp` dynamic ctors.
        // Guard: after the inner per-column loop, the encoder must
        // verify pos == row.len() and error if not.
        let rows = vec![vec![1u8, 2u8]]; // 2 bytes, but only 1 column declared
        let cols = vec![ColumnSchema {
            name: "x".to_string(),
            type_name: "UInt8".to_string(),
            col_type: ColumnType::UInt8,
        }];
        let Err(err) = encode_columns(&rows, &cols, 0) else {
            panic!("trailing RowBinary bytes must reject")
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("trailing") || msg.contains("leftover"),
            "expected trailing-byte error, got: {msg}",
        );
    }

    fn schema(type_name: &str) -> Vec<ColumnSchema> {
        ColumnSchema::from_headers(&[("c".to_string(), type_name.to_string())])
            .expect("schema parses")
    }

    /// Strip the `varuint("c") + varuint(type_name)` column header.
    fn body(out: &[u8], type_name: &str) -> Vec<u8> {
        let header = 2 + 1 + type_name.len();
        out[header..].to_vec()
    }

    #[test]
    fn encode_tuple_writes_one_subcolumn_per_field() {
        // Two rows of (UInt8, UInt16): all the u8s, then all the u16s.
        let rows = vec![vec![1u8, 2, 0], vec![3u8, 4, 0]];
        let out = encode_columns(&rows, &schema("Tuple(UInt8, UInt16)"), 0).expect("encodes");
        assert_eq!(
            body(&out, "Tuple(UInt8, UInt16)"),
            vec![1, 3, /* u16s */ 2, 0, 4, 0]
        );
    }

    /// `SimpleAggregateFunction(f, T)` is `T` on the wire, inside a
    /// `Nullable` and an `Array` too.
    #[test]
    fn encode_simple_aggregate_function_as_its_inner_type() {
        let rows = vec![7u64.to_le_bytes().to_vec(), 9u64.to_le_bytes().to_vec()];
        let ty = "SimpleAggregateFunction(sum, UInt64)";
        let out = encode_columns(&rows, &schema(ty), 0).expect("encodes");
        assert_eq!(
            body(&out, ty),
            [7u64.to_le_bytes(), 9u64.to_le_bytes()].concat()
        );

        let ty = "SimpleAggregateFunction(groupArrayArray, Array(String))";
        let rows = vec![vec![1u8, 1, b'a'], vec![0u8]];
        let out = encode_columns(&rows, &schema(ty), 0).expect("encodes");
        // Offsets (1, 1), then the one string.
        let mut want = Vec::new();
        want.extend_from_slice(&1u64.to_le_bytes());
        want.extend_from_slice(&1u64.to_le_bytes());
        want.extend_from_slice(&[1, b'a']);
        assert_eq!(body(&out, ty), want);
    }

    #[test]
    fn encode_map_writes_offsets_then_keys_then_values() {
        // One row: {1: 2, 3: 4}. RowBinary is varuint(count) + k,v pairs.
        let rows = vec![vec![2u8, 1, 2, 3, 4]];
        let out = encode_columns(&rows, &schema("Map(UInt8, UInt8)"), 0).expect("encodes");
        let mut expected = 2u64.to_le_bytes().to_vec();
        expected.extend_from_slice(&[1, 3]); // keys
        expected.extend_from_slice(&[2, 4]); // values
        assert_eq!(body(&out, "Map(UInt8, UInt8)"), expected);
    }

    #[test]
    fn encode_rejects_truncated_array_row() {
        // The row claims three elements and carries one.
        let rows = vec![vec![3u8, 9]];
        let err = encode_columns(&rows, &schema("Array(UInt8)"), 0)
            .expect_err("a truncated array row must reject");
        assert!(err.to_string().contains("truncated"), "{err}");
    }

    #[test]
    fn encode_empty_rows_writes_no_prefix() {
        // A zero-row block carries no column payload at all, so the encoder
        // must not emit a LowCardinality prefix for one.
        let out = encode_columns(&[], &schema("LowCardinality(String)"), 0).expect("encodes");
        assert!(out.is_empty(), "got {out:?}");
    }

    #[test]
    fn encode_lowcardinality_picks_the_narrowest_index_width() {
        // Two distinct keys fit a u8 index, so flags carry index code 0.
        let rows = vec![vec![1u8, b'a'], vec![1u8, b'b'], vec![1u8, b'a']];
        let out = encode_columns(&rows, &schema("LowCardinality(String)"), 0).expect("encodes");
        let data = body(&out, "LowCardinality(String)");
        assert_eq!(&data[..8], &1u64.to_le_bytes(), "version");
        assert_eq!(&data[8..16], &0x200u64.to_le_bytes(), "additional keys, u8");
        assert_eq!(&data[16..24], &2u64.to_le_bytes(), "dictionary size");
    }

    /// `LowCardinality(Nullable(T))` reserves dictionary slot 0 for NULL and
    /// points every NULL row at it, and only NULL rows. Getting this wrong
    /// writes a real value where the caller sent NULL, or NULL where the caller
    /// sent the type's default, and the server accepts either happily.
    #[test]
    fn lowcardinality_nullable_points_every_null_and_only_null_at_slot_zero() {
        // RowBinary Nullable: [0x01] is NULL, [0x00, ...] is Some.
        let rows = vec![
            vec![0x00u8, 1, b'a'], // Some("a")
            vec![0x01u8],          // NULL
            vec![0x00u8, 1, b'b'], // Some("b")
            vec![0x01u8],          // NULL
            vec![0x00u8, 1, b'a'], // Some("a") again -- must reuse its slot
            vec![0x00u8, 0],       // Some("") -- the default, but not NULL
        ];
        let t = "LowCardinality(Nullable(String))";
        let out = encode_columns(&rows, &schema(t), 0).expect("encodes");
        let data = body(&out, t);

        assert_eq!(&data[..8], &1u64.to_le_bytes(), "version");
        assert_eq!(&data[8..16], &0x200u64.to_le_bytes(), "additional keys, u8");
        // NULL, "a", "b", "": "a" is shared, "" is not folded into NULL.
        assert_eq!(&data[16..24], &4u64.to_le_bytes(), "dictionary size");

        assert_eq!(
            &data[24..25],
            &[0],
            "slot 0, NULL, written as the empty string"
        );
        assert_eq!(&data[25..27], b"\x01a");
        assert_eq!(&data[27..29], b"\x01b");
        assert_eq!(&data[29..30], &[0], "the non-null empty string");

        assert_eq!(&data[30..38], &6u64.to_le_bytes(), "index count");
        assert_eq!(
            &data[38..44],
            &[1, 0, 2, 0, 1, 3],
            "only NULLs index slot 0"
        );
    }

    /// The index width widens with the dictionary, and the server reads it from
    /// the flags word -- a mismatch shifts every index by a byte.
    #[test]
    fn a_dictionary_past_256_entries_widens_the_index_to_two_bytes() {
        // 300 distinct two-byte keys, so the dictionary passes the u8 ceiling.
        let rows: Vec<Vec<u8>> = (0..300u16)
            .map(|i| {
                let mut row = vec![2u8];
                row.extend_from_slice(&i.to_le_bytes());
                row
            })
            .collect();
        let t = "LowCardinality(String)";
        let out = encode_columns(&rows, &schema(t), 0).expect("encodes");
        let data = body(&out, t);

        assert_eq!(&data[8..16], &0x201u64.to_le_bytes(), "index code 1, u16");
        assert_eq!(&data[16..24], &300u64.to_le_bytes(), "dictionary size");
    }

    /// A type with no INSERT encoding is named rather than written as
    /// something the server would misread.
    #[test]
    fn a_type_that_cannot_be_inserted_is_named() {
        let rows = vec![vec![0u8; 8]];
        let err = encode_columns(&rows, &schema("Dynamic"), 0)
            .expect_err("Dynamic has no INSERT encoding");
        assert!(
            err.to_string().contains("not supported for INSERT"),
            "{err}"
        );
    }

    /// Every variable-length arm has its own truncation check, and a row that
    /// trips one must reject rather than read past the buffer.
    #[test]
    fn a_truncated_row_rejects_in_every_variable_length_arm() {
        let cases: &[(&str, Vec<u8>)] = &[
            // A string claiming four bytes and carrying one.
            ("String", vec![4, b'x']),
            // FixedString(4) with two bytes.
            ("FixedString(4)", vec![b'x', b'y']),
            // A non-null Nullable whose value is missing.
            ("Nullable(UInt64)", vec![0x00]),
            // A map claiming two pairs and carrying one key.
            ("Map(String, UInt8)", vec![2, 1, b'k']),
            // A tuple missing its second field.
            ("Tuple(UInt8, UInt64)", vec![1]),
        ];
        for (type_name, row) in cases {
            let err = encode_columns(std::slice::from_ref(row), &schema(type_name), 0)
                .expect_err(&format!("{type_name} must reject a truncated row"));
            assert!(
                err.to_string().contains("truncated"),
                "{type_name}: expected a truncation error, got {err}"
            );
        }
    }
}
