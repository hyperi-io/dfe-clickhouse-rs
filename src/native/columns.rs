//! Native column type parser and reader.
//!
//! Reads native columnar data and re-serialises each cell as `RowBinary`, the
//! shape `rowbinary::deserialize_row` consumes. The two formats agree byte for
//! byte on scalars and differ only in layout; `Nullable` is the one type whose
//! structure differs, and `Variant`, `Dynamic` and `JSON` are rendered as one
//! JSON document per row.

use tokio::io::AsyncReadExt;

use std::fmt::Write as _;

use crate::error::{Error, Result};
use crate::native::io::{
    ClickHouseBytesWrite, ClickHouseRead, VAR_UINT_MAX_BYTES, read_exact_grown, with_cap,
};

/// Supported `ClickHouse` column types for native transport.
///
/// `#[non_exhaustive]` so a new server type stays an additive change for code
/// that matches on this enum.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ColumnType {
    /// One raw byte per row.
    UInt8,
    /// One byte per row, `0` or `1`, kept apart from [`Self::UInt8`] so a
    /// value rendered as JSON reads `true` or `false`.
    Bool,
    /// Two raw little-endian bytes per row.
    UInt16,
    /// Four raw little-endian bytes per row.
    UInt32,
    /// Eight raw little-endian bytes per row.
    UInt64,
    /// One raw byte per row, read signed.
    Int8,
    /// Two raw little-endian bytes per row, read signed.
    Int16,
    /// Four raw little-endian bytes per row, read signed.
    Int32,
    /// Eight raw little-endian bytes per row, read signed.
    Int64,
    /// Sixteen raw little-endian bytes per row, read signed.
    Int128,
    /// Sixteen raw little-endian bytes per row.
    UInt128,
    /// Thirty-two raw little-endian bytes per row, read signed.
    Int256,
    /// Thirty-two raw little-endian bytes per row.
    UInt256,
    /// IEEE-754 binary32, four little-endian bytes per row.
    Float32,
    /// IEEE-754 binary64, eight little-endian bytes per row.
    Float64,
    /// Brain float, the top two bytes of an `f32` bit pattern.
    BFloat16,
    /// A raw little-endian backing integer whose width the variant selects;
    /// `precision` and `scale` come from the type name and give the value as
    /// `backing / 10^scale`.
    Decimal32 {
        /// Total significant digits.
        precision: u8,
        /// Fractional digits.
        scale: u8,
    },
    /// [`ColumnType::Decimal32`] over an `Int64` backing integer.
    Decimal64 {
        /// Total significant digits.
        precision: u8,
        /// Fractional digits.
        scale: u8,
    },
    /// [`ColumnType::Decimal32`] over an `Int128` backing integer.
    Decimal128 {
        /// Total significant digits.
        precision: u8,
        /// Fractional digits.
        scale: u8,
    },
    /// [`ColumnType::Decimal32`] over a 256-bit backing integer.
    Decimal256 {
        /// Total significant digits.
        precision: u8,
        /// Fractional digits.
        scale: u8,
    },
    /// Varuint length then that many bytes, per row.
    String,
    /// Exactly N raw bytes per row, with no length prefix.
    FixedString(usize),
    /// Sixteen bytes: two little-endian `u64` halves, most significant first.
    Uuid,
    /// A four-byte little-endian `UInt32`.
    IPv4,
    /// Sixteen bytes in network order.
    IPv6,
    /// Unsigned days since the Unix epoch, two little-endian bytes.
    Date,
    /// Signed days since the Unix epoch, four little-endian bytes.
    Date32,
    /// Unsigned seconds since the Unix epoch, four little-endian bytes.
    DateTime,
    /// `Int64` ticks at a sub-second `precision` of 0..=9, with an optional
    /// IANA `timezone`; both come from the type name, not the wire.
    DateTime64 {
        /// Sub-second digits, 0..=9.
        precision: u8,
        /// IANA timezone name from the type arguments.
        timezone: Option<String>,
    },
    /// Seconds since midnight, as a `UInt32`.
    Time,
    /// Ticks since midnight at the declared precision, as an `Int64`.
    Time64,
    /// One null-flag byte per row, then a full-width value column.
    Nullable(Box<ColumnType>),
    /// A per-block dictionary and per-row indices into it.
    LowCardinality(Box<ColumnType>),
    /// Wire-compatible with `UInt8`.
    Enum8,
    /// Wire-compatible with `UInt16`.
    Enum16,
    /// `SimpleAggregateFunction(func, T)`, wire-compatible with the inner `T`.
    SimpleAggregateFunction(Box<ColumnType>),
    /// `n` cumulative `u64` offsets, then all elements packed as a `T` column.
    Array(Box<ColumnType>),
    /// Each field stored as its own columnar block, in field order.
    Tuple(Vec<ColumnType>),
    /// `n` cumulative `u64` offsets, then the key column, then the value column.
    Map(Box<ColumnType>, Box<ColumnType>),
    /// Legacy `Object('json')`, a length-prefixed String on the wire.
    Json,
    /// A pair of `Float64` stored as two columns, x values then y values.
    Point,
    /// Variant(T1, T2, ...) -- discriminated union; u64 version, then `u8[n]`
    /// discriminators, then the sub-columns in definition order.
    Variant(Vec<ColumnType>),
    /// `JSON` -- u64 version (1 = one document string per row, 2 and 3 the
    /// path-based object formats).
    NewJson,
    /// `JSON` with at least one typed path. Inserts and string-mode reads
    /// treat it as [`Self::NewJson`], and a path-based read is refused,
    /// because the typed sub-columns are not decoded.
    TypedJson,
    /// Standalone `Dynamic` -- u64 version (1 deprecated, 2 intermediate,
    /// 3 flat), then discriminators and per-type column data.
    Dynamic,
}

/// Upper bound on type-name nesting [`ColumnType::parse`] and the
/// well-formedness check before it accept. Both recurse once per wrapper,
/// and `type_name` is server-controlled and capped
/// only at `MAX_STRING_SIZE`, so a deep name would otherwise blow the stack.
pub(crate) const MAX_TYPE_PARSE_DEPTH: usize = 32;

impl ColumnType {
    /// Parse a `ClickHouse` type name into `ColumnType`.
    ///
    /// `None` for an unsupported type, and for nesting past
    /// [`MAX_TYPE_PARSE_DEPTH`].
    // One arm per ClickHouse type name; splitting it would scatter the mapping.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn parse(type_str: &str) -> Option<Self> {
        let type_str = type_str.trim();

        // Parenthesis depth is an upper bound on the recursion below.
        if max_paren_depth(type_str) > MAX_TYPE_PARSE_DEPTH {
            return None;
        }

        if let Some(inner) = strip_outer(type_str, "Nullable") {
            return Self::parse(inner).map(|t| Self::Nullable(Box::new(t)));
        }

        if let Some(inner) = strip_outer(type_str, "LowCardinality") {
            return Self::parse(inner).map(|t| Self::LowCardinality(Box::new(t)));
        }

        if let Some(n_str) = strip_outer(type_str, "FixedString") {
            return n_str.parse::<usize>().ok().map(Self::FixedString);
        }

        if let Some(args_str) = strip_outer(type_str, "DateTime64") {
            // DateTime64(precision) or DateTime64(precision, 'timezone').
            let args = split_type_args(args_str);
            let precision = args
                .first()
                .and_then(|s| s.trim().parse::<u8>().ok())
                .unwrap_or(0);
            let timezone = args
                .get(1)
                .map(|s| s.trim().trim_matches('\'').to_string())
                .filter(|s| !s.is_empty());
            return Some(Self::DateTime64 {
                precision,
                timezone,
            });
        }
        if type_str.starts_with("DateTime(") {
            return Some(Self::DateTime);
        }
        if type_str.starts_with("Time64(") {
            return Some(Self::Time64);
        }

        // Sized Decimal forms take only scale; precision is implied by the
        // backing width (Decimal32->9, 64->18, 128->38, 256->76).
        if let Some(args) = strip_outer(type_str, "Decimal32") {
            let scale = args.trim().parse::<u8>().unwrap_or(0);
            return Some(Self::Decimal32 {
                precision: 9,
                scale,
            });
        }
        if let Some(args) = strip_outer(type_str, "Decimal64") {
            let scale = args.trim().parse::<u8>().unwrap_or(0);
            return Some(Self::Decimal64 {
                precision: 18,
                scale,
            });
        }
        if let Some(args) = strip_outer(type_str, "Decimal128") {
            let scale = args.trim().parse::<u8>().unwrap_or(0);
            return Some(Self::Decimal128 {
                precision: 38,
                scale,
            });
        }
        if let Some(args) = strip_outer(type_str, "Decimal256") {
            let scale = args.trim().parse::<u8>().unwrap_or(0);
            return Some(Self::Decimal256 {
                precision: 76,
                scale,
            });
        }
        // Generic Decimal(precision, scale) -- map to Decimal32/64/128/256 by precision.
        if let Some(args_str) = strip_outer(type_str, "Decimal") {
            let args = split_type_args(args_str);
            if args.len() == 2
                && let Ok(precision) = args[0].trim().parse::<u8>()
                && let Ok(scale) = args[1].trim().parse::<u8>()
            {
                return Some(if precision <= 9 {
                    Self::Decimal32 { precision, scale }
                } else if precision <= 18 {
                    Self::Decimal64 { precision, scale }
                } else if precision <= 38 {
                    Self::Decimal128 { precision, scale }
                } else {
                    Self::Decimal256 { precision, scale }
                });
            }
            return None;
        }

        // Enum8(...) / Enum16(...) -- one and two wire bytes, signed ordinals.
        if type_str.starts_with("Enum8(") {
            return Some(Self::Enum8);
        }
        if type_str.starts_with("Enum16(") {
            return Some(Self::Enum16);
        }

        // Array(T)
        if let Some(inner_str) = strip_outer(type_str, "Array") {
            return Self::parse(inner_str).map(|t| Self::Array(Box::new(t)));
        }

        // Tuple(T1, T2, ...)
        if let Some(args_str) = strip_outer(type_str, "Tuple") {
            let arg_strings = split_type_args(args_str);
            let fields: Vec<ColumnType> =
                arg_strings.iter().filter_map(|s| Self::parse(s)).collect();
            // Only accept if all fields parsed successfully.
            if !fields.is_empty() && fields.len() == arg_strings.len() {
                return Some(Self::Tuple(fields));
            }
            return None;
        }

        // Map(K, V)
        if let Some(args_str) = strip_outer(type_str, "Map") {
            let args = split_type_args(args_str);
            if args.len() == 2 {
                let k = Self::parse(args[0])?;
                let v = Self::parse(args[1])?;
                return Some(Self::Map(Box::new(k), Box::new(v)));
            }
            return None;
        }

        // SimpleAggregateFunction(func, T) -- strip wrapper, read as T
        if type_str.starts_with("SimpleAggregateFunction(") {
            if let Some(rest) = type_str.strip_prefix("SimpleAggregateFunction(") {
                // Find first ", " at depth 0 to split function name from type
                if let Some(comma_pos) = find_first_comma_at_depth0(rest) {
                    let inner_str = rest[comma_pos + 1..].trim();
                    let inner_str = inner_str.strip_suffix(')').unwrap_or(inner_str);
                    if let Some(inner) = Self::parse(inner_str) {
                        return Some(Self::SimpleAggregateFunction(Box::new(inner)));
                    }
                }
            }
            return None;
        }

        // Variant(T1, T2, ...) -- discriminated union
        if let Some(args_str) = strip_outer(type_str, "Variant") {
            let arg_strings = split_type_args(args_str);
            let fields: Vec<ColumnType> =
                arg_strings.iter().filter_map(|s| Self::parse(s)).collect();
            if !fields.is_empty() && fields.len() == arg_strings.len() {
                return Some(Self::Variant(fields));
            }
            return None;
        }

        // Dynamic(N) -- with optional max_types param
        if type_str.starts_with("Dynamic(") {
            return Some(Self::Dynamic);
        }

        if let Some(args) = strip_outer(type_str, "JSON") {
            return json_args(args).map(|kind| match kind {
                JsonArgs::WireNeutral => Self::NewJson,
                JsonArgs::TypedPaths => Self::TypedJson,
            });
        }

        match type_str {
            // Bool is an alias for UInt8 (true=1, false=0) on the wire.
            "UInt8" => Some(Self::UInt8),
            "Bool" => Some(Self::Bool),
            "UInt16" => Some(Self::UInt16),
            "UInt32" => Some(Self::UInt32),
            "UInt64" => Some(Self::UInt64),
            "Int8" => Some(Self::Int8),
            "Int16" => Some(Self::Int16),
            "Int32" => Some(Self::Int32),
            "Int64" => Some(Self::Int64),
            "Int128" => Some(Self::Int128),
            "UInt128" => Some(Self::UInt128),
            "Int256" => Some(Self::Int256),
            "UInt256" => Some(Self::UInt256),
            "Float32" => Some(Self::Float32),
            "Float64" => Some(Self::Float64),
            "BFloat16" => Some(Self::BFloat16),
            "String" => Some(Self::String),
            "UUID" => Some(Self::Uuid),
            "IPv4" => Some(Self::IPv4),
            "IPv6" => Some(Self::IPv6),
            "Date" => Some(Self::Date),
            "Date32" => Some(Self::Date32),
            "DateTime" => Some(Self::DateTime),
            "Time" => Some(Self::Time),
            // New JSON type (ClickHouse 24.x+) -- path-based columnar format.
            "JSON" => Some(Self::NewJson),
            "Dynamic" => Some(Self::Dynamic),
            // Legacy Object('json') -- stored as a plain String on the wire.
            "Object('json')" => Some(Self::Json),
            // Geo types. The shapes are arrays of points on the wire, under
            // their own names.
            "Point" => Some(Self::Point),
            "Ring" | "LineString" => Some(Self::Array(Box::new(Self::Point))),
            "Polygon" | "MultiLineString" => {
                Some(Self::Array(Box::new(Self::Array(Box::new(Self::Point)))))
            }
            "MultiPolygon" => Some(Self::Array(Box::new(Self::Array(Box::new(Self::Array(
                Box::new(Self::Point),
            )))))),
            _ => None,
        }
    }

    /// Fixed wire-format size in bytes; `None` for variable-length types.
    pub(crate) fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::UInt8 | Self::Bool | Self::Int8 | Self::Enum8 => Some(1),
            Self::BFloat16 | Self::UInt16 | Self::Int16 | Self::Date | Self::Enum16 => Some(2),
            Self::UInt32
            | Self::Int32
            | Self::Float32
            | Self::DateTime
            | Self::Date32
            | Self::Decimal32 { .. }
            | Self::IPv4
            | Self::Time => Some(4),
            Self::UInt64
            | Self::Int64
            | Self::Float64
            | Self::DateTime64 { .. }
            | Self::Decimal64 { .. }
            | Self::Time64 => Some(8),
            Self::Int128 | Self::UInt128 | Self::Uuid | Self::IPv6 | Self::Decimal128 { .. } => {
                Some(16)
            }
            Self::Int256 | Self::UInt256 | Self::Decimal256 { .. } => Some(32),
            Self::FixedString(n) => Some(*n),
            Self::String
            | Self::Json
            | Self::Nullable(_)
            | Self::LowCardinality(_)
            | Self::SimpleAggregateFunction(_)
            | Self::Array(_)
            | Self::Tuple(_)
            | Self::Map(_, _)
            | Self::Variant(_)
            | Self::NewJson
            | Self::TypedJson
            | Self::Dynamic
            // Point is Tuple(Float64, Float64) in columnar format -- not a flat 16-byte blob.
            | Self::Point => None,
        }
    }
}

/// Strip a `TypeName(` prefix and its `)` suffix, returning the contents.
///
/// Borrowing rather than building the prefix keeps `ColumnType::parse` free of
/// the 13 `String` allocations per column per block it otherwise makes.
fn strip_outer<'a>(s: &'a str, name: &str) -> Option<&'a str> {
    s.strip_prefix(name)?.strip_prefix('(')?.strip_suffix(')')
}

/// Maximum parenthesis nesting depth in `s`.
fn max_paren_depth(s: &str) -> usize {
    let mut depth = 0usize;
    let mut max = 0usize;
    for b in s.bytes() {
        match b {
            b'(' => {
                depth += 1;
                max = max.max(depth);
            }
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
}

/// What a `JSON(...)` argument list does to the column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JsonArgs {
    /// The two limits and `SKIP`: the wire layout is bare `JSON`'s.
    WireNeutral,
    /// At least one typed path (`a.b UInt32`), which adds sub-columns to the
    /// path-based serialisation. Text in and out is unchanged.
    TypedPaths,
}

/// Classify `JSON(...)` arguments, `None` for any the server would not
/// spell.
pub(crate) fn json_args(args: &str) -> Option<JsonArgs> {
    let mut typed = false;
    for arg in split_type_args(args) {
        let arg = arg.trim();
        // SKIP first: a regexp may itself contain `=`.
        if let Some(rest) = arg.strip_prefix("SKIP") {
            if !rest.starts_with(char::is_whitespace) {
                return None;
            }
            continue;
        }
        let (path, rest) = split_json_path(arg)?;
        match rest.strip_prefix('=') {
            Some(value) => {
                let known = matches!(path, "max_dynamic_paths" | "max_dynamic_types");
                if !known || value.trim().parse::<u64>().is_err() {
                    return None;
                }
            }
            // A typed path: the path, then its type.
            None if !rest.is_empty() => typed = true,
            None => return None,
        }
    }
    Some(if typed {
        JsonArgs::TypedPaths
    } else {
        JsonArgs::WireNeutral
    })
}

/// A JSON argument's leading path, backticked or bare, and the rest with
/// leading whitespace dropped.
fn split_json_path(arg: &str) -> Option<(&str, &str)> {
    let (path, rest) = if let Some(quoted) = arg.strip_prefix('`') {
        let end = quoted.find('`')?;
        (&quoted[..end], &quoted[end + 1..])
    } else {
        let end = arg
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
            .unwrap_or(arg.len());
        (&arg[..end], &arg[end..])
    };
    (!path.is_empty()).then(|| (path, rest.trim_start()))
}

/// `type_str` with each JSON in it spelled `String`, the type an INSERT block
/// declares for JSON so the server parses the text into the column.
///
/// `col_type` is `type_str` already parsed, so the two are walked in step.
pub(crate) fn json_spelled_as_string<'a>(
    type_str: &'a str,
    col_type: &ColumnType,
) -> std::borrow::Cow<'a, str> {
    use std::borrow::Cow;

    let s = type_str.trim();
    let wrapped = |name: &str, inner: &ColumnType| match strip_outer(s, name) {
        Some(arg) => match json_spelled_as_string(arg, inner) {
            Cow::Borrowed(_) => Cow::Borrowed(s),
            Cow::Owned(arg) => Cow::Owned(format!("{name}({arg})")),
        },
        None => Cow::Borrowed(s),
    };
    let listed = |name: &str, fields: &[&ColumnType]| {
        let Some(args) = strip_outer(s, name).map(split_type_args) else {
            return Cow::Borrowed(s);
        };
        if args.len() != fields.len() {
            return Cow::Borrowed(s);
        }
        let spelled: Vec<Cow<'_, str>> = args
            .iter()
            .zip(fields)
            .map(|(arg, field)| json_spelled_as_string(arg, field))
            .collect();
        if spelled.iter().all(|arg| matches!(arg, Cow::Borrowed(_))) {
            return Cow::Borrowed(s);
        }
        Cow::Owned(format!("{name}({})", spelled.join(", ")))
    };

    match col_type {
        // Owned, because a borrow reads as "unchanged" to the wrappers above.
        ColumnType::Json | ColumnType::NewJson | ColumnType::TypedJson => {
            Cow::Owned("String".to_owned())
        }
        ColumnType::Nullable(inner) => wrapped("Nullable", inner),
        ColumnType::LowCardinality(inner) => wrapped("LowCardinality", inner),
        ColumnType::Array(inner) => wrapped("Array", inner),
        ColumnType::Map(key, value) => listed("Map", &[key.as_ref(), value.as_ref()]),
        ColumnType::Tuple(fields) => listed("Tuple", &fields.iter().collect::<Vec<_>>()),
        ColumnType::Variant(fields) => listed("Variant", &fields.iter().collect::<Vec<_>>()),
        ColumnType::UInt8
        | ColumnType::Bool
        | ColumnType::UInt16
        | ColumnType::UInt32
        | ColumnType::UInt64
        | ColumnType::Int8
        | ColumnType::Int16
        | ColumnType::Int32
        | ColumnType::Int64
        | ColumnType::Int128
        | ColumnType::UInt128
        | ColumnType::Int256
        | ColumnType::UInt256
        | ColumnType::Float32
        | ColumnType::Float64
        | ColumnType::BFloat16
        | ColumnType::Decimal32 { .. }
        | ColumnType::Decimal64 { .. }
        | ColumnType::Decimal128 { .. }
        | ColumnType::Decimal256 { .. }
        | ColumnType::String
        | ColumnType::FixedString(_)
        | ColumnType::Uuid
        | ColumnType::IPv4
        | ColumnType::IPv6
        | ColumnType::Date
        | ColumnType::Date32
        | ColumnType::DateTime
        | ColumnType::DateTime64 { .. }
        | ColumnType::Time
        | ColumnType::Time64
        | ColumnType::Enum8
        | ColumnType::Enum16
        | ColumnType::SimpleAggregateFunction(_)
        | ColumnType::Point
        | ColumnType::Dynamic => Cow::Borrowed(s),
    }
}

/// Split a comma-separated type argument list respecting parentheses depth.
///
/// `"String, UInt64"` -> `["String", "UInt64"]`
/// `"Array(String), UInt64"` -> `["Array(String)", "UInt64"]`
///
/// A comma inside a `'...'` literal or a `` `...` `` identifier, as in a
/// `JSON(SKIP REGEXP 'a,b')`, does not split.
fn split_type_args(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        if let Some(open) = quote {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == open {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '`' => quote = Some(c),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                result.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let tail = s[start..].trim();
    if !tail.is_empty() {
        result.push(tail);
    }
    result
}

/// Find the byte offset of the first ',' at parentheses depth 0.
fn find_first_comma_at_depth0(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Per-row `RowBinary` bytes for a single column's values.
///
/// Each element is the RowBinary-encoded bytes for that row's field value.
pub(crate) type ColumnData = Vec<Vec<u8>>;

/// Upper bound on composite nesting the reader descends, matching
/// [`MAX_TYPE_PARSE_DEPTH`]. Variant, Dynamic and JSON columns carry type names
/// on the wire, so nesting is not bounded by the declared column type alone.
const MAX_READ_DEPTH: usize = 32;

/// Upper bound on the arms of a `Variant`; 255 is the server's own limit,
/// because discriminator 255 is reserved for NULL.
const MAX_VARIANT_TYPES: usize = 255;

/// Upper bound on the concrete types a `Dynamic` or JSON path may declare. One
/// slot below [`MAX_VARIANT_TYPES`] because `SharedVariant` is implicit.
const MAX_WIRE_TYPES: usize = 254;

/// Upper bound on the dynamic paths one JSON column may declare, comfortably
/// above the server's `max_dynamic_paths`.
const MAX_JSON_PATHS: usize = 1 << 16;

/// Read all `num_rows` values for `col_type` from the native binary stream.
///
/// Returns per-row `RowBinary` bytes ready for concatenation with other column
/// data. `NativeWriter` emits no bytes at all for a top-level column of a
/// zero-row block, the per-type serialization prefix included; a nested
/// sub-column at zero elements still carries its prefix.
///
/// # Errors
///
/// [`Error::BadResponse`] for a malformed header, an offset list that steps
/// backwards, or a wire count above its cap; I/O errors propagate untouched.
/// Test-only since the prefix phase moved: production reads go through
/// [`crate::native::decode`], which runs one prefix walk over the whole
/// column and then calls [`read_column_data`]. This pairs the two here so the
/// byte-exactness table can drive a payload that carries its own prefix.
#[cfg(test)]
pub(crate) fn read_column<'a, R: ClickHouseRead + 'a>(
    reader: &'a mut R,
    col_type: &'a ColumnType,
    num_rows: u64,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ColumnData>> + Send + 'a>> {
    Box::pin(async move {
        if num_rows > 0 {
            read_prefixes(reader, col_type, 0).await?;
        }
        read_column_at(reader, col_type, num_rows, 0).await
    })
}

/// The data phase alone, for a caller that has already run [`read_prefixes`]
/// over the whole column.
///
/// [`crate::native::decode`] is that caller: its `Array` arm reads the offsets
/// before it delegates the child here, so a prefix walk starting at this
/// point would already be too late. It runs one walk over the whole column
/// first and then calls this.
///
/// # Errors
///
/// As [`read_column`].
pub(crate) fn read_column_data<'a, R: ClickHouseRead + 'a>(
    reader: &'a mut R,
    col_type: &'a ColumnType,
    num_rows: u64,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ColumnData>> + Send + 'a>> {
    read_column_at(reader, col_type, num_rows, 0)
}

/// Consume the serialisation prefixes for `col_type`'s whole tree, in the
/// order the server writes them.
///
/// Mirrors [`crate::native::decode`]'s own prefix phase; see the rationale
/// there. Test-only for the same reason as [`read_column`].
#[cfg(test)]
fn read_prefixes<'a, R: ClickHouseRead + 'a>(
    reader: &'a mut R,
    col_type: &'a ColumnType,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    use tokio::io::AsyncReadExt as _;

    Box::pin(async move {
        if depth > MAX_READ_DEPTH {
            return Err(Error::BadResponse(format!(
                "native protocol: column type nesting exceeds {MAX_READ_DEPTH} levels"
            )));
        }
        match col_type {
            // The dictionary is part of the data phase, so this does not
            // recurse into the inner type.
            ColumnType::LowCardinality(_) => {
                let _version = reader.read_u64_le().await?;
            }
            // `SerializationVariant.cpp:161-182`: the discriminator mode,
            // then every element's prefix. The mode is only validated, never
            // carried, so hoisting it needs no state.
            ColumnType::Variant(variant_types) => {
                read_variant_mode(reader).await?;
                for variant in variant_types {
                    read_prefixes(reader, variant, depth + 1).await?;
                }
            }
            ColumnType::Nullable(inner)
            | ColumnType::Array(inner)
            | ColumnType::SimpleAggregateFunction(inner) => {
                read_prefixes(reader, inner, depth + 1).await?;
            }
            ColumnType::Tuple(fields) => {
                for field in fields {
                    read_prefixes(reader, field, depth + 1).await?;
                }
            }
            ColumnType::Map(key, value) => {
                read_prefixes(reader, key, depth + 1).await?;
                read_prefixes(reader, value, depth + 1).await?;
            }
            _ => {}
        }
        Ok(())
    })
}

/// `depth` bounds the recursion; the boxed future breaks the async-fn cycle
/// Rust would otherwise reject for infinite future size.
fn read_column_at<'a, R: ClickHouseRead + 'a>(
    reader: &'a mut R,
    col_type: &'a ColumnType,
    num_rows: u64,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ColumnData>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_READ_DEPTH {
            return Err(Error::BadResponse(format!(
                "native protocol: column type nesting exceeds {MAX_READ_DEPTH} levels"
            )));
        }
        if depth == 0 && num_rows == 0 {
            return Ok(ColumnData::new());
        }
        let n = usize::try_from(num_rows).map_err(|_| {
            Error::BadResponse(format!(
                "native protocol: row count {num_rows} exceeds platform usize"
            ))
        })?;

        match col_type {
            ColumnType::UInt8
            | ColumnType::Bool
            | ColumnType::Int8
            | ColumnType::Enum8
            | ColumnType::BFloat16
            | ColumnType::UInt16
            | ColumnType::Int16
            | ColumnType::Enum16
            | ColumnType::UInt32
            | ColumnType::Int32
            | ColumnType::Float32
            | ColumnType::Date
            | ColumnType::Date32
            | ColumnType::DateTime
            | ColumnType::Decimal32 { .. }
            | ColumnType::IPv4
            | ColumnType::Time
            | ColumnType::UInt64
            | ColumnType::Int64
            | ColumnType::Float64
            | ColumnType::DateTime64 { .. }
            | ColumnType::Decimal64 { .. }
            | ColumnType::Time64
            | ColumnType::Uuid
            | ColumnType::Int128
            | ColumnType::UInt128
            | ColumnType::IPv6
            | ColumnType::Decimal128 { .. }
            | ColumnType::Int256
            | ColumnType::UInt256
            | ColumnType::Decimal256 { .. } => {
                let size = col_type.fixed_size().ok_or_else(|| {
                    Error::BadResponse(format!(
                        "native protocol: {col_type:?} has no fixed wire width"
                    ))
                })?;
                read_fixed_column(reader, n, size).await
            }

            // Point is Tuple(Float64, Float64) on the wire: all N x-values then
            // all N y-values, transposed here into 16 raw bytes per row.
            ColumnType::Point => {
                let x_col = read_fixed_column(reader, n, 8).await?;
                let y_col = read_fixed_column(reader, n, 8).await?;
                Ok(x_col
                    .into_iter()
                    .zip(y_col)
                    .map(|(mut x, y)| {
                        x.extend_from_slice(&y);
                        x
                    })
                    .collect())
            }

            ColumnType::String | ColumnType::Json => read_string_column(reader, n).await,
            // RowBinary carries FixedString(N) as N raw bytes, no length prefix
            // (upstream `rowbinary/de.rs` reads it as `[u8; N]`).
            ColumnType::FixedString(size) => read_fixed_column(reader, n, *size).await,
            ColumnType::Nullable(inner) => read_nullable_column(reader, n, inner, depth).await,
            ColumnType::LowCardinality(inner) => {
                read_low_cardinality_column(reader, n, inner, depth).await
            }
            ColumnType::SimpleAggregateFunction(inner) => {
                read_column_at(reader, inner, num_rows, depth + 1).await
            }
            ColumnType::Array(inner) => read_array_column(reader, n, inner, depth).await,
            ColumnType::Tuple(fields) => read_tuple_column(reader, n, fields, depth).await,
            ColumnType::Map(key_type, val_type) => {
                read_map_column(reader, n, key_type, val_type, depth).await
            }
            ColumnType::Variant(variant_types) => {
                read_variant_column(reader, n, variant_types, depth).await
            }
            ColumnType::NewJson => read_json_column(reader, n, depth).await,
            ColumnType::TypedJson => read_typed_json_column(reader, n).await,
            ColumnType::Dynamic => read_dynamic_column(reader, n, depth).await,
        }
    })
}

async fn read_fixed_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    size: usize,
) -> Result<ColumnData> {
    let mut result = with_cap(n)?;
    for _ in 0..n {
        result.push(read_exact_grown(reader, size).await?);
    }
    Ok(result)
}

async fn read_string_column<R: ClickHouseRead>(reader: &mut R, n: usize) -> Result<ColumnData> {
    let mut result = with_cap(n)?;
    for _ in 0..n {
        let s = reader.read_string().await?;
        result.push(rowbinary_string(&s));
    }
    Ok(result)
}

/// Wrap `bytes` as a `RowBinary` String cell: varuint length then the bytes.
fn rowbinary_string(bytes: &[u8]) -> Vec<u8> {
    let mut row = Vec::with_capacity(bytes.len() + VAR_UINT_MAX_BYTES);
    row.put_string(bytes);
    row
}

async fn read_nullable_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    inner: &ColumnType,
    depth: usize,
) -> Result<ColumnData> {
    // Native: N null-flags (1 byte each: 1=null, 0=has-value) then N values.
    let null_flags = read_exact_grown(reader, n).await?;

    // Every slot carries value bytes, nulls included.
    let inner_data = read_column_at(reader, inner, n as u64, depth + 1).await?;

    let mut result = with_cap(n)?;
    for (flag, value) in null_flags.into_iter().zip(inner_data) {
        if flag != 0 {
            // NULL -- RowBinary: 1 byte = 1
            result.push(vec![1u8]);
        } else {
            // Not null -- RowBinary: 0 byte then value
            let mut row = Vec::with_capacity(1 + value.len());
            row.push(0u8);
            row.extend_from_slice(&value);
            result.push(row);
        }
    }
    Ok(result)
}

/// `LowCardinality` column reader; the wire shape is documented on
/// [`crate::native::decode`]'s `decode_low_cardinality`.
async fn read_low_cardinality_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    inner: &ColumnType,
    depth: usize,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    // The version word is not read here: it is this column's serialisation
    // prefix, consumed by `read_prefixes` before any of the column's data.
    let state = reader.read_u64_le().await?;
    let index_type = (state & 0x03) as u8;
    let has_global_dict = (state & 0x100) != 0;
    let has_additional_keys = (state & 0x200) != 0;

    // LowCardinality(Nullable(T)) carries a T dictionary whose slot 0 is the
    // null sentinel, so the wire type is always the unwrapped T.
    let (dict_type, is_nullable_inner) = if let ColumnType::Nullable(t) = inner {
        (t.as_ref(), true)
    } else {
        (inner, false)
    };

    // cpp-client and clickhouse-go both reject a global dictionary on the
    // client-server path and require the additional-keys bit; the streaming
    // decoder in `crate::native::decode` applies the same rule.
    if has_global_dict {
        return Err(Error::BadResponse(
            "native protocol: LowCardinality global dictionary is not supported on the \
             client-server path (only per-block additional keys)"
                .into(),
        ));
    }
    if !has_additional_keys {
        return Err(Error::BadResponse(
            "native protocol: LowCardinality block set neither the additional-keys nor the \
             global-dictionary flag"
                .into(),
        ));
    }
    let additional_keys_size = reader.read_u64_le().await?;
    let dict: ColumnData =
        read_column_at(reader, dict_type, additional_keys_size, depth + 1).await?;

    let num_indices = reader.read_u64_le().await?;
    if num_indices != n as u64 {
        return Err(Error::BadResponse(format!(
            "native protocol: LowCardinality index count {num_indices} != row count {n}"
        )));
    }

    let index_bytes = match index_type {
        0 => 1usize,
        1 => 2,
        2 => 4,
        3 => 8,
        other => {
            return Err(Error::BadResponse(format!(
                "native protocol: unknown LowCardinality index type {other}"
            )));
        }
    };

    let dict_size = dict.len();
    let mut result = with_cap(n)?;
    for _ in 0..n {
        let idx = usize::try_from(read_index(reader, index_bytes).await?).unwrap_or(usize::MAX);
        if is_nullable_inner {
            // Index 0 is the null sentinel; every other index is a Some(T).
            if idx == 0 {
                result.push(vec![0x01u8]); // RowBinary Nullable null flag
            } else {
                let value = dict.get(idx).ok_or_else(|| {
                    Error::BadResponse(format!(
                        "native protocol: LowCardinality index {idx} out of range (dict size {dict_size})"
                    ))
                })?;
                let mut rb = vec![0x00u8]; // RowBinary not-null flag
                rb.extend_from_slice(value);
                result.push(rb);
            }
        } else {
            let value = dict.get(idx).ok_or_else(|| {
                Error::BadResponse(format!(
                    "native protocol: LowCardinality index {idx} out of range (dict size {dict_size})"
                ))
            })?;
            result.push(value.clone());
        }
    }
    Ok(result)
}

/// Array(T) column reader, `varuint(count) + count x T` per `RowBinary` row.
///
/// ```text
/// n x u64             cumulative end-offsets (last = total element count)
/// total_elements x T  values packed as a regular T column
/// ```
async fn read_array_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    inner: &ColumnType,
    depth: usize,
) -> Result<ColumnData> {
    let offsets = read_offsets(reader, n).await?;
    let total = offsets.last().copied().unwrap_or(0);
    let all_values = read_column_at(reader, inner, total, depth + 1).await?;

    let mut result = with_cap(n)?;
    let mut prev = 0usize;
    for &end in &offsets {
        let end = checked_span(end, prev, all_values.len(), "Array")?;
        let mut row = Vec::new();
        row.put_var_uint((end - prev) as u64);
        for v in &all_values[prev..end] {
            row.extend_from_slice(v);
        }
        result.push(row);
        prev = end;
    }
    Ok(result)
}

/// Read `n` cumulative end-offsets.
async fn read_offsets<R: ClickHouseRead>(reader: &mut R, n: usize) -> Result<Vec<u64>> {
    use tokio::io::AsyncReadExt as _;

    let mut offsets = with_cap(n)?;
    for _ in 0..n {
        offsets.push(reader.read_u64_le().await?);
    }
    Ok(offsets)
}

/// Narrow one cumulative end-offset to a slice bound.
///
/// An offset below its predecessor, or past the elements the sub-column
/// actually carries, would index out of range.
fn checked_span(end: u64, prev: usize, available: usize, what: &str) -> Result<usize> {
    let end = usize::try_from(end).unwrap_or(usize::MAX);
    if end < prev || end > available {
        return Err(Error::BadResponse(format!(
            "native protocol: {what} column offset {end} is outside \
             {prev}..={available}"
        )));
    }
    Ok(end)
}

/// Tuple(T1, T2, ...) column reader: each field is its own columnar block in
/// field order, concatenated per `RowBinary` row.
async fn read_tuple_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    fields: &[ColumnType],
    depth: usize,
) -> Result<ColumnData> {
    let mut rows: ColumnData = with_cap(n)?;
    rows.resize_with(n, Vec::new);
    for field_type in fields {
        let field_data = read_column_at(reader, field_type, n as u64, depth + 1).await?;
        for (row, cell) in rows.iter_mut().zip(field_data) {
            row.extend_from_slice(&cell);
        }
    }
    Ok(rows)
}

/// Map(K, V) column reader, `varuint(count) + count x (K + V)` per `RowBinary` row.
///
/// ```text
/// n x u64            cumulative end-offsets
/// total_entries x K  key column
/// total_entries x V  value column
/// ```
async fn read_map_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    key_type: &ColumnType,
    val_type: &ColumnType,
    depth: usize,
) -> Result<ColumnData> {
    let offsets = read_offsets(reader, n).await?;
    let total = offsets.last().copied().unwrap_or(0);
    let keys = read_column_at(reader, key_type, total, depth + 1).await?;
    let vals = read_column_at(reader, val_type, total, depth + 1).await?;

    let pairs = keys.len().min(vals.len());
    let mut result = with_cap(n)?;
    let mut prev = 0usize;
    for &end in &offsets {
        let end = checked_span(end, prev, pairs, "Map")?;
        let mut row = Vec::new();
        row.put_var_uint((end - prev) as u64);
        for i in prev..end {
            row.extend_from_slice(&keys[i]);
            row.extend_from_slice(&vals[i]);
        }
        result.push(row);
        prev = end;
    }
    Ok(result)
}

/// Variant(T1, T2, ...) column reader, one RowBinary-String JSON cell per row.
///
/// ```text
/// u64      version
/// n x u8   discriminators  (255 = NULL, else the type index in definition order)
/// per type, in definition order: the rows whose discriminator selected it
/// ```
async fn read_variant_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    variant_types: &[ColumnType],
    depth: usize,
) -> Result<ColumnData> {
    let k = variant_types.len();
    if k > MAX_VARIANT_TYPES {
        return Err(Error::BadResponse(format!(
            "native protocol: Variant declares {k} arms, above the {MAX_VARIANT_TYPES} cap"
        )));
    }
    for ct in variant_types {
        refuse_unprintable_value("Variant", &type_label(ct), ct)?;
    }

    // The mode word is not read here: it is this column's serialisation
    // prefix, consumed by `read_prefixes` before any of the column's data.
    let discriminators = read_exact_grown(reader, n).await?;

    let mut type_counts = vec![0u64; k];
    for &d in &discriminators {
        if (d as usize) < k {
            type_counts[d as usize] += 1;
        }
    }

    let mut type_values: Vec<ColumnData> = with_cap(k)?;
    for (i, col_type) in variant_types.iter().enumerate() {
        type_values.push(read_column_at(reader, col_type, type_counts[i], depth + 1).await?);
    }

    let mut type_cursors = vec![0usize; k];
    let mut result = with_cap(n)?;
    for &d in &discriminators {
        let idx = d as usize;
        let json_bytes: Vec<u8> = if d == NULL_DISCRIMINATOR || idx >= k {
            b"null".to_vec()
        } else {
            let cursor = type_cursors[idx];
            type_cursors[idx] += 1;
            cell_to_json(type_values[idx].get(cursor), &variant_types[idx])
        };
        result.push(rowbinary_string(&json_bytes));
    }
    Ok(result)
}

/// JSON serialization version carrying one JSON string per row, which
/// `output_format_native_write_json_as_string=1` selects.
pub(crate) const JSON_SERIALIZATION_STRING: u64 = 1;

/// Discriminator reserved for NULL in the `Variant` and Dynamic v1/v2 formats.
const NULL_DISCRIMINATOR: u8 = 255;

/// `DiscriminatorsSerializationMode::BASIC` -- one plain discriminator byte per
/// row (`SerializationVariant.h:58`). `COMPACT` prepends a row count and a
/// per-granule format byte, which this reader does not walk.
const VARIANT_MODE_BASIC: u64 = 0;

/// Read and check the `Variant` discriminator-serialization mode.
pub(crate) async fn read_variant_mode<R: ClickHouseRead>(reader: &mut R) -> Result<()> {
    use tokio::io::AsyncReadExt as _;

    let mode = reader.read_u64_le().await?;
    if mode != VARIANT_MODE_BASIC {
        return Err(Error::BadResponse(format!(
            "native protocol: Variant discriminator serialization mode {mode} is not supported \
             (only basic, 0)"
        )));
    }
    Ok(())
}

/// Marker type name for values spilled out of a `Dynamic` type list.
const SHARED_VARIANT: &str = "SharedVariant";

/// New JSON column (`ClickHouse` 24.x+) reader.
async fn read_json_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    depth: usize,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let version = reader.read_u64_le().await?;
    read_json_body_at(reader, n, version, depth).await
}

/// A typed-path JSON column, readable in its one-string-per-row form only.
async fn read_typed_json_column<R: ClickHouseRead>(reader: &mut R, n: usize) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let version = reader.read_u64_le().await?;
    if version != JSON_SERIALIZATION_STRING {
        return Err(typed_json_unsupported());
    }
    read_string_column(reader, n).await
}

/// The refusal for a path-based read of a JSON column with typed paths.
pub(crate) fn typed_json_unsupported() -> Error {
    Error::Unsupported(
        "JSON typed paths have no decoder in the path-based serialisation. \
         Read it with with_json_as_string(true), the default"
            .to_owned(),
    )
}

/// JSON column payload, after the u64 serialization version.
///
/// The values are the wire numbers from `SerializationObject.h:37-56`, which
/// are NOT the ordinals their names suggest -- Object `V1 = 0`, `V2 = 2`,
/// `FLATTENED = 3`, `V3 = 4`:
/// - `1`: each row is a plain JSON string, which is what
///   `output_format_native_write_json_as_string` asks for and the only form
///   this crate produces by default
/// - `2`: path-based object format with Dynamic sub-columns + shared data
/// - `3`: FLATTENED, path-based with no shared data
///
/// Not handled: Object `V1 = 0`, which a server sends when that setting is
/// off, and `V3 = 4`. Both surface as a clean error naming the version.
///
/// # Errors
///
/// As [`read_column`], plus [`Error::BadResponse`] for an unknown `version`.
pub(crate) async fn read_json_body<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    version: u64,
) -> Result<ColumnData> {
    read_json_body_at(reader, n, version, 0).await
}

async fn read_json_body_at<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    version: u64,
    depth: usize,
) -> Result<ColumnData> {
    match version {
        JSON_SERIALIZATION_STRING => read_string_column(reader, n).await,
        2 => read_json_object_v2_column(reader, n, depth).await,
        3 => read_json_object_flattened_column(reader, n, depth).await,
        _ => Err(Error::BadResponse(format!(
            "native protocol: unsupported JSON serialization version: {version}"
        ))),
    }
}

/// Read a varuint header count, rejecting anything above `cap` before it sizes
/// a list no payload has to back.
async fn read_capped_count<R: ClickHouseRead>(
    reader: &mut R,
    cap: usize,
    what: &str,
) -> Result<usize> {
    let raw = reader.read_var_uint().await?;
    let count = usize::try_from(raw).unwrap_or(usize::MAX);
    if count > cap {
        return Err(Error::BadResponse(format!(
            "native protocol: {what} count {raw} is above the {cap} cap"
        )));
    }
    Ok(count)
}

/// Render one cell as JSON, or `null` when the sub-column ran short.
fn cell_to_json(cell: Option<&Vec<u8>>, col_type: &ColumnType) -> Vec<u8> {
    cell.map_or_else(|| b"null".to_vec(), |c| rowbinary_to_json(c, col_type))
}

/// JSON v2 object column reader, after the u64 version has been consumed.
///
/// ```text
/// varuint   numDynamicPaths
/// String[]  pathNames (sorted alphabetically)
/// per path: u64 dynVersion; [v1 only: varuint maxTypes];
///           varuint numTypes; String[] typeNames; u64 variantVersion
/// per path: u8[n] discriminators (into sorted(typeNames + "SharedVariant"),
///           255 = NULL), then the sub-columns in sorted order
/// n x u64   shared data
/// ```
async fn read_json_object_v2_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    depth: usize,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let num_paths = read_capped_count(reader, MAX_JSON_PATHS, "JSON dynamic path").await?;
    let mut path_names: Vec<String> = with_cap(num_paths)?;
    for _ in 0..num_paths {
        path_names.push(reader.read_utf8_string().await?);
    }

    // One sorted (type_name, col_type) list per path, `SharedVariant` included,
    // in the order the discriminators index.
    let mut path_sorted_types: Vec<Vec<(String, ColumnType)>> = with_cap(num_paths)?;
    for path_name in &path_names {
        let dyn_version = reader.read_u64_le().await?;
        if dyn_version == 1 {
            let _max_types = reader.read_var_uint().await?;
        } else if dyn_version != 2 {
            return Err(Error::BadResponse(format!(
                "native protocol: unexpected Dynamic version {dyn_version} in JSON v2 path \"{path_name}\""
            )));
        }

        let num_types = read_capped_count(reader, MAX_WIRE_TYPES, "Dynamic type").await?;
        let mut type_names: Vec<String> = with_cap(num_types + 1)?;
        for _ in 0..num_types {
            type_names.push(reader.read_utf8_string().await?);
        }
        type_names.push(SHARED_VARIANT.to_owned());
        type_names.sort();

        read_variant_mode(reader).await?;

        let types = type_names
            .into_iter()
            .map(|name| {
                let ct = wire_type(&name, "JSON values", JSON_REMEDY)?;
                Ok((name, ct))
            })
            .collect::<Result<Vec<(String, ColumnType)>>>()?;
        for (name, ct) in &types {
            refuse_unprintable(path_name, name, ct)?;
        }

        path_sorted_types.push(types);
    }

    let mut path_discriminators: Vec<Vec<u8>> = with_cap(num_paths)?;
    let mut path_values: Vec<Vec<ColumnData>> = with_cap(num_paths)?;

    for types in &path_sorted_types {
        let k = types.len();

        let discriminators = read_exact_grown(reader, n).await?;

        let mut type_counts = vec![0u64; k];
        for &d in &discriminators {
            if (d as usize) < k {
                type_counts[d as usize] += 1;
            }
        }

        let mut col_values: Vec<ColumnData> = with_cap(k)?;
        for (i, (_, col_type)) in types.iter().enumerate() {
            col_values.push(read_column_at(reader, col_type, type_counts[i], depth + 1).await?);
        }

        path_discriminators.push(discriminators);
        path_values.push(col_values);
    }

    // Shared data is Array(Tuple(String, String)) -- the paths that spilled past
    // `max_dynamic_paths` and their values (`SerializationObjectSharedData.cpp`
    // MAP mode, `DataTypeObject.cpp:583`). Reading only the offsets would
    // desynchronise the block the moment a row actually spills.
    let shared_offsets = read_offsets(reader, n).await?;
    let spilled = shared_offsets.last().copied().unwrap_or(0);
    read_column_at(reader, &ColumnType::String, spilled, depth + 1).await?;
    read_column_at(reader, &ColumnType::String, spilled, depth + 1).await?;
    // Shared-data values are binary-encoded Dynamic values this decoder has no
    // reader for, and dropping them would return a partial document.
    if spilled > 0 {
        return Err(json_spill_unsupported(
            "paths past max_dynamic_paths (shared data)",
        ));
    }

    let mut path_cursors: Vec<Vec<usize>> = path_sorted_types
        .iter()
        .map(|types| vec![0usize; types.len()])
        .collect();

    let mut result = with_cap(n)?;
    // The row index reads across every path's discriminator list, not along one
    // collection, so there is nothing to iterate over.
    #[allow(clippy::needless_range_loop)]
    for row_i in 0..n {
        let mut pairs = Vec::new();
        for (path_idx, path_name) in path_names.iter().enumerate() {
            let disc = path_discriminators[path_idx][row_i] as usize;
            let k = path_sorted_types[path_idx].len();

            // Absent or NULL: the key is omitted from the object.
            if disc == NULL_DISCRIMINATOR as usize || disc >= k {
                continue;
            }

            let cursor = path_cursors[path_idx][disc];
            path_cursors[path_idx][disc] += 1;

            let (type_name, col_type) = &path_sorted_types[path_idx][disc];
            if type_name == SHARED_VARIANT {
                return Err(json_spill_unsupported(
                    "values past max_dynamic_types (SharedVariant)",
                ));
            }
            let value = cell_to_json(path_values[path_idx][disc].get(cursor), col_type);
            pairs.push((path_name.as_str(), value));
        }
        result.push(rowbinary_string(&nest_paths(pairs)?));
    }

    Ok(result)
}

/// The refusal for a JSON value the server stored where the path-based
/// serialisation carries it in a binary encoding this decoder does not read.
fn json_spill_unsupported(what: &str) -> Error {
    Error::Unsupported(format!(
        "JSON {what} has no decoder in the path-based serialisation. \
         Read it with with_json_as_string(true), the default"
    ))
}

/// Refuse a path type the reader cannot print byte for byte as the server
/// does: a date or time, which the server formats in its time zone, or an
/// array of objects (`Array(JSON)`), whose own serialisation prefix the
/// reader does not walk.
fn refuse_unprintable(path: &str, type_name: &str, ct: &ColumnType) -> Result<()> {
    if prints_like_the_server(ct) {
        return Ok(());
    }
    Err(Error::Unsupported(format!(
        "JSON path \"{path}\" holds {type_name}, which the path-based reader does not \
         print the way the server does. Read it with with_json_as_string(true), the default"
    )))
}

/// The same gate for a standalone `Dynamic` or `Variant` column, which has no
/// string-mode setting to point at.
fn refuse_unprintable_value(kind: &str, type_name: &str, ct: &ColumnType) -> Result<()> {
    if prints_like_the_server(ct) {
        return Ok(());
    }
    Err(Error::Unsupported(format!(
        "{kind} values of type {type_name} are not printed the way the server prints them. \
         Read the column cast to String in the query, as CAST(<column> AS String)"
    )))
}

/// A readable name for a parsed type, for a refusal naming it.
fn type_label(ct: &ColumnType) -> String {
    match ct {
        ColumnType::DateTime64 { precision, .. } => format!("DateTime64({precision})"),
        ColumnType::Nullable(inner) => format!("Nullable({})", type_label(inner)),
        ColumnType::Array(inner) => format!("Array({})", type_label(inner)),
        other => format!("{other:?}"),
    }
}

/// The types whose rendering here matches the server's: the scalars it infers
/// from JSON, the narrower integers, `Float32`, and arrays of them.
fn prints_like_the_server(ct: &ColumnType) -> bool {
    match ct {
        ColumnType::Bool
        | ColumnType::Int8
        | ColumnType::Int16
        | ColumnType::Int32
        | ColumnType::Int64
        | ColumnType::UInt8
        | ColumnType::UInt16
        | ColumnType::UInt32
        | ColumnType::UInt64
        | ColumnType::Float32
        | ColumnType::Float64
        | ColumnType::String => true,
        ColumnType::Nullable(inner) | ColumnType::Array(inner) => prints_like_the_server(inner),
        _ => false,
    }
}

/// A row's nested document: keys with either a value or more keys under them.
enum Node {
    Leaf(Vec<u8>),
    Object(Vec<(String, Node)>),
}

fn write_object(children: &[(String, Node)], out: &mut Vec<u8>) {
    out.push(b'{');
    for (i, (key, node)) in children.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(&json_quote_bytes(key.as_bytes()));
        out.push(b':');
        match node {
            Node::Leaf(value) => out.extend_from_slice(value),
            Node::Object(grandchildren) => write_object(grandchildren, out),
        }
    }
    out.push(b'}');
}

fn unnestable(path: &str, why: &str) -> Error {
    Error::Unsupported(format!(
        "JSON path \"{path}\" cannot be rebuilt as a document ({why}). \
         Read it with with_json_as_string(true), the default"
    ))
}

/// One row's `(path, JSON value)` pairs as the document the server prints for
/// them: each path split on `.` into nested objects, siblings in the order of
/// their full paths.
fn nest_paths(mut pairs: Vec<(&str, Vec<u8>)>) -> Result<Vec<u8>> {
    pairs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut root: Vec<(String, Node)> = Vec::new();
    for (path, value) in pairs {
        let segments: Vec<&str> = path.split('.').collect();
        // `%2E` is the server's escape for a dot inside a key.
        if segments
            .iter()
            .any(|s| s.is_empty() || s.to_ascii_uppercase().contains("%2E"))
        {
            return Err(unnestable(path, "a key holding a dot, or an empty key"));
        }
        let Some((last, parents)) = segments.split_last() else {
            return Err(unnestable(path, "no key"));
        };
        let mut level = &mut root;
        for seg in parents {
            let i = if let Some(i) = level.iter().position(|(key, _)| key == seg) {
                i
            } else {
                level.push(((*seg).to_owned(), Node::Object(Vec::new())));
                level.len() - 1
            };
            level = match &mut level[i].1 {
                Node::Object(children) => children,
                Node::Leaf(_) => return Err(unnestable(path, "a value and an object at one key")),
            };
        }
        if level.iter().any(|(key, _)| key == last) {
            return Err(unnestable(path, "a value and an object at one key"));
        }
        level.push(((*last).to_owned(), Node::Leaf(value)));
    }
    let mut out = Vec::new();
    write_object(&root, &mut out);
    Ok(out)
}

/// What a JSON path refusal tells the caller to do instead.
const JSON_REMEDY: &str = "Read it with with_json_as_string(true), the default";

/// What a `Dynamic` or `Variant` refusal tells the caller to do instead.
const CAST_REMEDY: &str =
    "Read the column cast to String in the query, as CAST(<column> AS String)";

/// Parse a type name the server announced inside a `Dynamic` or a JSON path,
/// `kind` and `remedy` wording the refusal for one this decoder cannot
/// represent. `SharedVariant` spill values ride as a String sub-column.
///
/// A malformed name is a corrupt response, [`Error::BadResponse`], checked
/// before parsing so a lenient parse cannot accept it. A well-formed one this
/// decoder cannot represent is a legitimate value it does not support,
/// [`Error::Unsupported`].
fn wire_type(name: &str, kind: &str, remedy: &str) -> Result<ColumnType> {
    if name == SHARED_VARIANT {
        return Ok(ColumnType::String);
    }
    if !crate::native::type_name::is_well_formed(name) {
        return Err(Error::BadResponse(format!(
            "native protocol: '{name}' is not a column type"
        )));
    }
    ColumnType::parse(name).ok_or_else(|| {
        Error::Unsupported(format!(
            "{kind} of type {name} have no decoder here. {remedy}"
        ))
    })
}

/// JSON FLATTENED object column reader, after the u64 version has been
/// consumed. `FLATTENED` is wire value 3; Object `V3` is 4 and unhandled.
///
/// ```text
/// varuint   numDynamicPaths
/// String[]  pathNames
/// per path: u64 dynVersion; varuint numTypes; String[] typeNames
/// per path: discriminators (width by numTypes + 1), then the sub-columns in
///           declaration order
/// ```
/// There is no shared-data section in the flattened form.
async fn read_json_object_flattened_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    depth: usize,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let num_paths = read_capped_count(reader, MAX_JSON_PATHS, "JSON dynamic path").await?;
    let mut path_names: Vec<String> = with_cap(num_paths)?;
    for _ in 0..num_paths {
        path_names.push(reader.read_utf8_string().await?);
    }

    let mut path_col_types: Vec<Vec<ColumnType>> = with_cap(num_paths)?;
    for path_name in &path_names {
        // Each path opens with the flattened Dynamic version before its types.
        let dyn_version = reader.read_u64_le().await?;
        if dyn_version != DYNAMIC_FLATTENED {
            return Err(Error::BadResponse(format!(
                "native protocol: unexpected Dynamic version {dyn_version} in a JSON v3 path"
            )));
        }
        let mut types = Vec::new();
        for (name, ct) in read_flat_wire_types(reader, "JSON values", JSON_REMEDY).await? {
            refuse_unprintable(path_name, &name, &ct)?;
            types.push(ct);
        }
        path_col_types.push(types);
    }

    let mut path_discriminators: Vec<Vec<usize>> = with_cap(num_paths)?;
    let mut path_values: Vec<Vec<ColumnData>> = with_cap(num_paths)?;

    for col_types in &path_col_types {
        let total_types = col_types.len();
        let (discriminators, type_counts) =
            read_discriminators(reader, n, total_types, index_width(total_types)).await?;

        let mut col_values: Vec<ColumnData> = with_cap(total_types)?;
        for (i, col_type) in col_types.iter().enumerate() {
            col_values.push(read_column_at(reader, col_type, type_counts[i], depth + 1).await?);
        }

        path_discriminators.push(discriminators);
        path_values.push(col_values);
    }

    let mut path_cursors: Vec<Vec<usize>> = path_col_types
        .iter()
        .map(|types| vec![0usize; types.len()])
        .collect();

    let mut result = with_cap(n)?;
    // The row index reads across every path's discriminator list, not along one
    // collection, so there is nothing to iterate over.
    #[allow(clippy::needless_range_loop)]
    for row_i in 0..n {
        let mut pairs = Vec::new();
        for (path_idx, path_name) in path_names.iter().enumerate() {
            let disc = path_discriminators[path_idx][row_i];
            // NULL is the type count itself in v3, so the key is omitted.
            if disc >= path_col_types[path_idx].len() {
                continue;
            }

            let cursor = path_cursors[path_idx][disc];
            path_cursors[path_idx][disc] += 1;

            let value = cell_to_json(
                path_values[path_idx][disc].get(cursor),
                &path_col_types[path_idx][disc],
            );
            pairs.push((path_name.as_str(), value));
        }
        result.push(rowbinary_string(&nest_paths(pairs)?));
    }

    Ok(result)
}

/// Read a flattened Dynamic type list, which carries no `SharedVariant` and is
/// indexed in declaration order, so the whole 255-slot range is concrete types.
async fn read_flat_wire_types<R: ClickHouseRead>(
    reader: &mut R,
    kind: &str,
    remedy: &str,
) -> Result<Vec<(String, ColumnType)>> {
    let count = read_capped_count(reader, MAX_VARIANT_TYPES, "Dynamic type").await?;
    let mut types = with_cap(count)?;
    for _ in 0..count {
        let name = reader.read_utf8_string().await?;
        let ct = wire_type(&name, kind, remedy)?;
        types.push((name, ct));
    }
    Ok(types)
}

/// Serialization version a flattened Dynamic sub-column announces, which each
/// JSON v3 path carries ahead of its type list
/// (`SerializationDynamic.cpp:151`).
const DYNAMIC_FLATTENED: u64 = 3;

/// Discriminator width `ClickHouse` picks for `slots` concrete types plus NULL.
///
/// `getSmallestIndexesType(slots + 1)` in `DataTypesNumber.cpp:126` takes the
/// narrowest unsigned type whose range covers the slot count, so the u8 arm
/// runs to 255 types inclusive.
fn index_width(slots: usize) -> usize {
    if slots <= 255 {
        1
    } else if slots <= 65_535 {
        2
    } else if u32::try_from(slots).is_ok() {
        4
    } else {
        8
    }
}

/// Read `n` discriminators of `width` bytes and tally the rows each of `slots`
/// sub-columns owns.
async fn read_discriminators<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    slots: usize,
    width: usize,
) -> Result<(Vec<usize>, Vec<u64>)> {
    let mut discriminators: Vec<usize> = with_cap(n)?;
    let mut counts = vec![0u64; slots];
    for _ in 0..n {
        let disc = usize::try_from(read_index(reader, width).await?).unwrap_or(usize::MAX);
        if disc < slots {
            counts[disc] += 1;
        }
        discriminators.push(disc);
    }
    Ok((discriminators, counts))
}

/// Standalone Dynamic column (`ClickHouse` 24.x+) reader.
///
/// Dispatches based on the wire serialization version prefix:
/// - `1`: deprecated format (maxTypes + totalTypes + sorted types + `SharedVariant` + variantVersion)
/// - `2`: intermediate format (totalTypes + sorted types + `SharedVariant` + variantVersion)
/// - `3`: flat format (totalTypes + types, NULL = totalTypes, no `SharedVariant`)
async fn read_dynamic_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    depth: usize,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let version = reader.read_u64_le().await?;
    match version {
        1 => read_dynamic_v1v2_column(reader, n, true, depth).await,
        2 => read_dynamic_v1v2_column(reader, n, false, depth).await,
        3 => read_dynamic_flattened_column(reader, n, depth).await,
        _ => Err(Error::BadResponse(format!(
            "native protocol: unsupported Dynamic serialization version: {version}"
        ))),
    }
}

/// Dynamic v1/v2 column reader.
///
/// v1 has an extra `maxTypes` varuint before `totalTypes`; v2 does not.
/// Both add `SharedVariant` to the type list and sort alphabetically.
/// NULL discriminator = 255.
async fn read_dynamic_v1v2_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    has_max_types: bool,
    depth: usize,
) -> Result<ColumnData> {
    let header = read_dynamic_v1v2_prefix(reader, has_max_types).await?;
    read_dynamic_v1v2_body(reader, n, &header, depth).await
}

/// The type list a `Dynamic` column's data phase is read against.
pub(crate) struct DynamicHeader {
    type_names: Vec<String>,
    /// The flattened serialisation, whose type list is read with the data.
    flattened: bool,
}

impl DynamicHeader {
    /// The prefix of a flattened `Dynamic`, which carries only its version.
    pub(crate) fn flattened() -> Self {
        Self {
            type_names: Vec::new(),
            flattened: true,
        }
    }
}

/// The prefix half of a v1/v2 `Dynamic` column: the type list, then the inner
/// Variant's discriminator mode.
///
/// `SerializationDynamic.cpp:128-168` writes all of this as the column's
/// serialisation prefix, so for a nested `Dynamic` it precedes the enclosing
/// column's data rather than sitting where the child does.
///
/// # Errors
///
/// [`Error::BadResponse`] for a type count above the cap or a discriminator
/// mode this reader does not support.
pub(crate) async fn read_dynamic_v1v2_prefix<R: ClickHouseRead>(
    reader: &mut R,
    has_max_types: bool,
) -> Result<DynamicHeader> {
    if has_max_types {
        let _max_types = reader.read_var_uint().await?;
    }

    let total_types = read_capped_count(reader, MAX_WIRE_TYPES, "Dynamic type").await?;
    let mut type_names: Vec<String> = with_cap(total_types + 1)?;
    for _ in 0..total_types {
        type_names.push(reader.read_utf8_string().await?);
    }
    type_names.push(SHARED_VARIANT.to_owned());
    type_names.sort();

    read_variant_mode(reader).await?;

    Ok(DynamicHeader {
        type_names,
        flattened: false,
    })
}

/// The data half of a v1/v2 `Dynamic` column, read against the type list its
/// prefix declared.
///
/// # Errors
///
/// As [`read_column`].
pub(crate) async fn read_dynamic_v1v2_body<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    header: &DynamicHeader,
    depth: usize,
) -> Result<ColumnData> {
    if header.flattened {
        return read_dynamic_flattened_column(reader, n, depth).await;
    }
    let type_names = &header.type_names;
    let col_types = type_names
        .iter()
        .map(|name| wire_type(name, "Dynamic values", CAST_REMEDY))
        .collect::<Result<Vec<ColumnType>>>()?;
    // SharedVariant has its own refusal, per value, below.
    for (name, ct) in type_names.iter().zip(&col_types) {
        if name != SHARED_VARIANT {
            refuse_unprintable_value("Dynamic", name, ct)?;
        }
    }

    let k = col_types.len();
    let discriminators = read_exact_grown(reader, n).await?;

    let mut type_counts = vec![0u64; k];
    for &d in &discriminators {
        if (d as usize) < k {
            type_counts[d as usize] += 1;
        }
    }

    let mut type_values: Vec<ColumnData> = with_cap(k)?;
    for (i, col_type) in col_types.iter().enumerate() {
        type_values.push(read_column_at(reader, col_type, type_counts[i], depth + 1).await?);
    }

    let mut type_cursors = vec![0usize; k];
    let mut result = with_cap(n)?;
    for &d in &discriminators {
        let idx = d as usize;
        let json_bytes: Vec<u8> = if d == NULL_DISCRIMINATOR || idx >= k {
            b"null".to_vec()
        } else {
            let cursor = type_cursors[idx];
            type_cursors[idx] += 1;
            // Reading a SharedVariant value as null would drop it unseen.
            if type_names[idx] == SHARED_VARIANT {
                return Err(Error::Unsupported(
                    "Dynamic values past max_types (SharedVariant) have no decoder here. \
                     Read the column cast to String in the query, as CAST(<column> AS String)"
                        .to_owned(),
                ));
            }
            cell_to_json(type_values[idx].get(cursor), &col_types[idx])
        };
        result.push(rowbinary_string(&json_bytes));
    }
    Ok(result)
}

/// Dynamic FLATTENED column reader (`ClickHouse` 25.6+). `FLATTENED` is wire
/// value 3 (`SerializationDynamic.h:49`); Dynamic has no version 3 of its own.
///
/// No `SharedVariant`; NULL discriminator = totalTypes, and the discriminator
/// width scales with it.
async fn read_dynamic_flattened_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    depth: usize,
) -> Result<ColumnData> {
    let mut col_types = Vec::new();
    for (name, ct) in read_flat_wire_types(reader, "Dynamic values", CAST_REMEDY).await? {
        refuse_unprintable_value("Dynamic", &name, &ct)?;
        col_types.push(ct);
    }
    let total_types = col_types.len();
    let (discriminators, type_counts) =
        read_discriminators(reader, n, total_types, index_width(total_types)).await?;

    let mut type_values: Vec<ColumnData> = with_cap(total_types)?;
    for (i, col_type) in col_types.iter().enumerate() {
        type_values.push(read_column_at(reader, col_type, type_counts[i], depth + 1).await?);
    }

    let mut type_cursors = vec![0usize; total_types];
    let mut result = with_cap(n)?;
    for &d in &discriminators {
        // NULL is the type count itself in v3.
        let json_bytes: Vec<u8> = if d >= total_types {
            b"null".to_vec()
        } else {
            let cursor = type_cursors[d];
            type_cursors[d] += 1;
            cell_to_json(type_values[d].get(cursor), &col_types[d])
        };
        result.push(rowbinary_string(&json_bytes));
    }
    Ok(result)
}

/// Convert a RowBinary-encoded value for `col_type` into JSON bytes,
/// best-effort: any parse failure renders as `null` rather than propagating.
fn rowbinary_to_json(bytes: &[u8], col_type: &ColumnType) -> Vec<u8> {
    match rowbinary_to_json_inner(bytes, col_type) {
        Ok((json, _)) => json,
        Err(()) => b"null".to_vec(),
    }
}

/// Inner parser: returns `(json_bytes, bytes_consumed)` or `Err(())` on underflow.
// The date/time arms repeat a width already listed above, and stay separate so
// the comment explaining why they render as bare wire integers sits with them.
#[allow(clippy::too_many_lines, clippy::match_same_arms)]
fn rowbinary_to_json_inner(bytes: &[u8], col_type: &ColumnType) -> Result<(Vec<u8>, usize), ()> {
    macro_rules! fixed {
        ($n:expr, $t:ty) => {{
            (
                <$t>::from_le_bytes(head::<$n>(bytes)?)
                    .to_string()
                    .into_bytes(),
                $n,
            )
        }};
    }

    Ok(match col_type {
        ColumnType::UInt8 => fixed!(1, u8),
        ColumnType::Bool => {
            let byte = head::<1>(bytes)?[0];
            (
                (if byte == 0 { &b"false"[..] } else { b"true" }).to_vec(),
                1,
            )
        }
        ColumnType::UInt16 | ColumnType::Date => fixed!(2, u16),
        ColumnType::UInt32 | ColumnType::Time => fixed!(4, u32),
        ColumnType::UInt64 => fixed!(8, u64),
        // A signed wire byte reinterpreted from its unsigned wire form.
        #[allow(clippy::cast_possible_wrap)]
        ColumnType::Int8 | ColumnType::Enum8 => {
            let [b] = head::<1>(bytes)?;
            ((b as i8).to_string().into_bytes(), 1)
        }
        ColumnType::Int16 | ColumnType::Enum16 => fixed!(2, i16),
        ColumnType::Int32 | ColumnType::Decimal32 { .. } | ColumnType::Date32 => fixed!(4, i32),
        ColumnType::Int64 | ColumnType::Time64 | ColumnType::Decimal64 { .. } => fixed!(8, i64),
        ColumnType::Int128 | ColumnType::Decimal128 { .. } => fixed!(16, i128),
        ColumnType::UInt128 => fixed!(16, u128),
        // A 32-byte big integer has no JSON number form, so it renders as hex.
        ColumnType::Int256 | ColumnType::UInt256 | ColumnType::Decimal256 { .. } => {
            let raw = head::<32>(bytes)?;
            let mut hex = String::with_capacity(66);
            hex.push('"');
            for b in raw.iter().rev() {
                let _ = write!(hex, "{b:02x}");
            }
            hex.push('"');
            (hex.into_bytes(), 32)
        }
        ColumnType::Float32 => {
            let v = f32::from_le_bytes(head::<4>(bytes)?);
            (format_f32_json(v).into_bytes(), 4)
        }
        ColumnType::Float64 => {
            let v = f64::from_le_bytes(head::<8>(bytes)?);
            (format_float_json(v).into_bytes(), 8)
        }
        // BFloat16 is the top half of an f32's bit pattern.
        ColumnType::BFloat16 => {
            let raw = u16::from_le_bytes(head::<2>(bytes)?);
            let v = f32::from_bits(u32::from(raw) << 16);
            (format_float_json(f64::from(v)).into_bytes(), 2)
        }
        // Date, Date32, DateTime and DateTime64 all render as their bare wire
        // integer: days or ticks since the epoch, at the column's precision.
        ColumnType::DateTime => fixed!(4, u32),
        ColumnType::DateTime64 { .. } => fixed!(8, i64),
        // Two little-endian u64 halves, most significant first.
        ColumnType::Uuid => {
            let hi = u64::from_le_bytes(head::<8>(bytes)?);
            let lo = u64::from_le_bytes(head::<8>(bytes.get(8..).ok_or(())?)?);
            let hex = format!("{hi:016x}{lo:016x}");
            let s = format!(
                "\"{}-{}-{}-{}-{}\"",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            );
            (s.into_bytes(), 16)
        }
        // IPv4 is a UInt32 on the wire, rendered in dotted-quad form.
        ColumnType::IPv4 => {
            let addr = std::net::Ipv4Addr::from(u32::from_le_bytes(head::<4>(bytes)?));
            (format!("\"{addr}\"").into_bytes(), 4)
        }
        // IPv6 is 16 network-order bytes, rendered in RFC 5952 compressed form.
        ColumnType::IPv6 => {
            let addr = std::net::Ipv6Addr::from(head::<16>(bytes)?);
            (format!("\"{addr}\"").into_bytes(), 16)
        }
        // Point is a pair of little-endian f64.
        ColumnType::Point => {
            let x = f64::from_le_bytes(head::<8>(bytes)?);
            let y = f64::from_le_bytes(head::<8>(bytes.get(8..).ok_or(())?)?);
            (
                format!("[{},{}]", format_float_json(x), format_float_json(y)).into_bytes(),
                16,
            )
        }
        // RowBinary carries String as varuint(len) + bytes.
        ColumnType::String | ColumnType::Json => {
            let (len, hdr) = slice_var_uint(bytes)?;
            let end = usize::try_from(len)
                .ok()
                .and_then(|len| hdr.checked_add(len))
                .filter(|end| *end <= bytes.len())
                .ok_or(())?;
            (json_quote_bytes(&bytes[hdr..end]), end)
        }
        // RowBinary carries FixedString(N) as N raw bytes, no length prefix.
        ColumnType::FixedString(width) => {
            let cell = bytes.get(..*width).ok_or(())?;
            (json_quote_bytes(cell), *width)
        }
        ColumnType::Nullable(inner) => {
            if bytes.is_empty() {
                return Err(());
            }
            if bytes[0] != 0 {
                (b"null".to_vec(), 1)
            } else {
                let (json, consumed) = rowbinary_to_json_inner(&bytes[1..], inner)?;
                (json, 1 + consumed)
            }
        }
        ColumnType::LowCardinality(inner) => {
            // After LowCardinality expansion, individual cells are the inner type's bytes
            rowbinary_to_json_inner(bytes, inner)?
        }
        ColumnType::SimpleAggregateFunction(inner) => rowbinary_to_json_inner(bytes, inner)?,
        ColumnType::Array(inner) => {
            let (count, hdr) = slice_var_uint(bytes)?;
            let mut pos = hdr;
            let mut json = b"[".to_vec();
            for i in 0..count {
                if i > 0 {
                    json.push(b',');
                }
                let (elem, consumed) = rowbinary_to_json_inner(&bytes[pos..], inner)?;
                json.extend_from_slice(&elem);
                pos += consumed;
            }
            json.push(b']');
            (json, pos)
        }
        ColumnType::Tuple(fields) => {
            let mut pos = 0;
            let mut json = b"[".to_vec();
            for (i, field_type) in fields.iter().enumerate() {
                if i > 0 {
                    json.push(b',');
                }
                let (elem, consumed) = rowbinary_to_json_inner(&bytes[pos..], field_type)?;
                json.extend_from_slice(&elem);
                pos += consumed;
            }
            json.push(b']');
            (json, pos)
        }
        ColumnType::Map(key_type, val_type) => {
            let (count, hdr) = slice_var_uint(bytes)?;
            let mut pos = hdr;
            let mut json = b"{".to_vec();
            for i in 0..count {
                if i > 0 {
                    json.push(b',');
                }
                let (k, kc) = rowbinary_to_json_inner(&bytes[pos..], key_type)?;
                pos += kc;
                json.extend_from_slice(&k);
                json.push(b':');
                let (v, vc) = rowbinary_to_json_inner(&bytes[pos..], val_type)?;
                pos += vc;
                json.extend_from_slice(&v);
            }
            json.push(b'}');
            (json, pos)
        }
        // Dynamic, Variant and JSON cells already hold JSON text, length-
        // prefixed as RowBinary strings, so they are spliced in verbatim.
        ColumnType::Dynamic
        | ColumnType::NewJson
        | ColumnType::TypedJson
        | ColumnType::Variant(_) => {
            let (len, hdr) = slice_var_uint(bytes)?;
            let end = usize::try_from(len)
                .ok()
                .and_then(|len| hdr.checked_add(len))
                .filter(|end| *end <= bytes.len())
                .ok_or(())?;
            (bytes[hdr..end].to_vec(), end)
        }
    })
}

/// [`crate::native::io::get_var_uint`] in the `Result<_, ()>` domain this
/// best-effort renderer works in.
fn slice_var_uint(bytes: &[u8]) -> Result<(u64, usize), ()> {
    crate::native::io::get_var_uint(bytes).map_err(|_| ())
}

/// The leading `N` bytes of a cell, or `Err` when it is short.
fn head<const N: usize>(bytes: &[u8]) -> Result<[u8; N], ()> {
    bytes.get(..N).ok_or(())?.try_into().map_err(|_| ())
}

/// Format a float as the server prints one in JSON: the shortest digits that
/// round-trip, in plain decimal for exponents -6 to 20 and as `1e21` outside
/// them. NaN and infinity have no JSON form and render as `null`.
fn format_float_json(v: f64) -> String {
    if v.is_finite() {
        server_notation(format!("{v}"), format!("{v:e}"))
    } else {
        "null".to_string()
    }
}

/// [`format_float_json`] for a `Float32`, from its own shortest digits rather
/// than those of the widened `f64`.
fn format_f32_json(v: f32) -> String {
    if v.is_finite() {
        server_notation(format!("{v}"), format!("{v:e}"))
    } else {
        "null".to_string()
    }
}

/// Pick the plain or the scientific spelling of one float the way the server
/// does, by its decimal exponent.
fn server_notation(plain: String, scientific: String) -> String {
    let exponent: i32 = scientific
        .rsplit_once('e')
        .and_then(|(_, e)| e.parse().ok())
        .unwrap_or(0);
    if (-6..21).contains(&exponent) {
        plain
    } else {
        scientific
    }
}

/// JSON-quote raw bytes with the escapes the server writes: `/` and the line
/// and paragraph separators included, control bytes as upper-case `\u00XX`.
fn json_quote_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![b'"'];
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let escape: Option<&[u8]> = match b {
            b'"' => Some(br#"\""#),
            b'\\' => Some(br"\\"),
            b'/' => Some(br"\/"),
            0x08 => Some(br"\b"),
            0x0c => Some(br"\f"),
            b'\n' => Some(br"\n"),
            b'\r' => Some(br"\r"),
            b'\t' => Some(br"\t"),
            _ => None,
        };
        match (escape, bytes.get(i..i + 3)) {
            (Some(escaped), _) => out.extend_from_slice(escaped),
            (None, Some([0xe2, 0x80, 0xa8])) => {
                out.extend_from_slice(br"\u2028");
                i += 2;
            }
            (None, Some([0xe2, 0x80, 0xa9])) => {
                out.extend_from_slice(br"\u2029");
                i += 2;
            }
            (None, _) if b < 0x20 => out.extend_from_slice(format!("\\u{b:04X}").as_bytes()),
            (None, _) => out.push(b),
        }
        i += 1;
    }
    out.push(b'"');
    out
}

/// Read one little-endian index or discriminator of `width` bytes.
async fn read_index<R: ClickHouseRead>(reader: &mut R, width: usize) -> Result<u64> {
    Ok(match width {
        1 => u64::from(reader.read_u8().await?),
        2 => u64::from(reader.read_u16_le().await?),
        4 => u64::from(reader.read_u32_le().await?),
        8 => reader.read_u64_le().await?,
        other => {
            return Err(Error::BadResponse(format!(
                "native protocol: {other}-byte index width is not a wire width"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rejects_pathologically_deep_nesting() {
        let deep = format!("{}UInt8{}", "Array(".repeat(1000), ")".repeat(1000));
        assert!(ColumnType::parse(&deep).is_none());
    }

    /// `(type name, parses)`, spelled the way `system.columns` and an INSERT's
    /// schema block report each type.
    #[test]
    fn parse_takes_the_server_spellings_and_refuses_the_rest() {
        let cases: &[(&str, bool)] = &[
            (
                "Array(Map(String, Tuple(UInt8, Array(Nullable(String)))))",
                true,
            ),
            ("JSON", true),
            ("JSON(max_dynamic_paths=2048)", true),
            (
                "JSON(max_dynamic_types=8, max_dynamic_paths=16, SKIP `x.y`, SKIP REGEXP 'z.*')",
                true,
            ),
            // A comma inside a quoted pattern is not an argument separator.
            ("JSON(SKIP REGEXP 'a,b')", true),
            ("JSON(SKIP REGEXP 'k=v')", true),
            ("Nullable(JSON(max_dynamic_paths=2048))", true),
            ("Array(JSON(max_dynamic_paths=2048))", true),
            (
                "Map(LowCardinality(String), JSON(max_dynamic_paths=2048))",
                true,
            ),
            ("Object('json')", true),
            // Typed paths parse, as `TypedJson`.
            ("JSON(`a.b` UInt32)", true),
            ("JSON(a UInt32, max_dynamic_paths=2048)", true),
            ("JSON(`a.b` Enum8('x' = 1))", true),
            ("Array(JSON(`a` String))", true),
            ("JSON(max_dynamic_paths=lots)", false),
            ("JSON(max_dynamic_paths=2048", false),
            ("JSON(unknown_param=1)", false),
            ("JSON(`a.b`)", false),
            ("JSON(`a.b UInt32)", false),
            ("NotAType", false),
            ("Map(String)", false),
        ];
        for (type_name, parses) in cases {
            assert_eq!(
                ColumnType::parse(type_name).is_some(),
                *parses,
                "{type_name}"
            );
        }
    }

    #[test]
    fn max_paren_depth_counts_nesting() {
        assert_eq!(max_paren_depth("UInt8"), 0);
        assert_eq!(max_paren_depth("Array(UInt8)"), 1);
        assert_eq!(max_paren_depth("Array(Array(UInt8))"), 2);
        assert_eq!(max_paren_depth("Tuple(UInt8, Array(String))"), 2);
    }

    /// One `LowCardinality(String)` block: version, flags, dictionary, indices.
    fn lc_string(dict: &[&str], idx: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&1u64.to_le_bytes());
        p.extend_from_slice(&0x200u64.to_le_bytes()); // HAS_ADDITIONAL_KEYS | u8 indices
        p.extend_from_slice(&(dict.len() as u64).to_le_bytes());
        for s in dict {
            p.push(u8::try_from(s.len()).expect("test strings stay under 128 bytes"));
            p.extend_from_slice(s.as_bytes());
        }
        p.extend_from_slice(&(idx.len() as u64).to_le_bytes());
        p.extend_from_slice(idx);
        p
    }

    /// `(type name, wire bytes, row count)`: the reader must consume every byte
    /// listed and no more, so a desync inside one column shows up here rather
    /// than as a corrupt column later in the block.
    fn byte_exact_cases() -> Vec<(&'static str, Vec<u8>, u64)> {
        vec![
            ("UInt8", vec![1, 2, 3], 3),
            ("Int64", vec![0u8; 16], 2),
            ("Float64", vec![0u8; 8], 1),
            ("Decimal(18, 4)", vec![0u8; 8], 1),
            ("Date", vec![0u8; 4], 2),
            ("DateTime64(3, 'UTC')", vec![0u8; 8], 1),
            ("UUID", vec![0u8; 16], 1),
            ("IPv4", vec![0u8; 4], 1),
            ("IPv6", vec![0u8; 16], 1),
            ("Point", vec![0u8; 32], 2),
            ("String", vec![1, b'a', 0, 2, b'b', b'c'], 3),
            ("FixedString(3)", b"abcdef".to_vec(), 2),
            ("Nullable(UInt8)", vec![0, 1, 7, 0], 2),
            (
                "LowCardinality(String)",
                lc_string(&["a", "b"], &[0, 1, 0]),
                3,
            ),
            (
                "LowCardinality(Nullable(String))",
                lc_string(&["", "x"], &[0, 1]),
                2,
            ),
            // Two rows of two u64 elements each.
            (
                "Array(UInt64)",
                {
                    let mut p = Vec::new();
                    p.extend_from_slice(&1u64.to_le_bytes());
                    p.extend_from_slice(&2u64.to_le_bytes());
                    p.extend_from_slice(&[0u8; 16]);
                    p
                },
                2,
            ),
            // One row of one (UInt8, String) pair.
            (
                "Array(Tuple(UInt8, String))",
                {
                    let mut p = Vec::new();
                    p.extend_from_slice(&1u64.to_le_bytes());
                    p.push(9);
                    p.extend_from_slice(&[1, b'z']);
                    p
                },
                1,
            ),
            // One row of one String -> Nullable(UInt8) pair.
            (
                "Map(String, Nullable(UInt8))",
                {
                    let mut p = Vec::new();
                    p.extend_from_slice(&1u64.to_le_bytes());
                    p.extend_from_slice(&[1, b'k']);
                    p.extend_from_slice(&[0, 5]);
                    p
                },
                1,
            ),
            // Fixed-width scalars: the payload is exactly rows x declared
            // width, so a wrong width shows up as a leftover or a short read.
            ("Bool", vec![0, 1, 1], 3),
            ("Int8", vec![0u8; 2], 2),
            ("Int16", vec![0u8; 4], 2),
            ("Int32", vec![0u8; 8], 2),
            ("UInt16", vec![0u8; 4], 2),
            ("UInt32", vec![0u8; 8], 2),
            ("UInt64", vec![0u8; 16], 2),
            ("Float32", vec![0u8; 8], 2),
            ("BFloat16", vec![0u8; 4], 2),
            ("Int128", vec![0u8; 32], 2),
            ("UInt128", vec![0u8; 16], 1),
            ("Int256", vec![0u8; 32], 1),
            ("UInt256", vec![0u8; 64], 2),
            ("Date32", vec![0u8; 8], 2),
            ("DateTime", vec![0u8; 8], 2),
            ("Time", vec![0u8; 8], 2),
            // Enums are their index width, not the member text.
            ("Enum8('a' = 1, 'b' = 2)", vec![1, 2], 2),
            ("Enum16('a' = 1)", vec![1, 0], 1),
            // Decimal width follows precision; the scale never moves it.
            ("Decimal(9, 2)", vec![0u8; 8], 2),
            ("Decimal(38, 2)", vec![0u8; 16], 1),
            ("Decimal(76, 2)", vec![0u8; 32], 1),
            // The wrapper is stripped and the inner type read in its place.
            ("SimpleAggregateFunction(sum, UInt64)", vec![0u8; 16], 2),
            // Top-level Tuple is column-major: the UInt8, then the String.
            ("Tuple(UInt8, String)", vec![9, 1, b'z'], 1),
            // Nullable(String): the mask, then a value for every row, nulls
            // included -- row 1's placeholder is a zero-length string.
            ("Nullable(String)", vec![0, 1, 1, b'a', 0], 2),
            ("Variant(UInt8, String)", variant_payload(), 2),
            ("Dynamic", dynamic_v2_payload(), 2),
            ("Dynamic", dynamic_v3_payload(), 2),
            ("JSON", json_v3_payload(), 2),
            ("JSON", json_v2_payload(&[]), 2),
        ]
    }

    /// Scalars render as the server prints them in JSON, each pinned against
    /// its output: notation edges, escapes and `Bool`.
    #[test]
    fn scalars_render_as_the_server_prints_them() {
        for (v, want) in [
            (1.5, "1.5"),
            (0.000_001, "0.000001"),
            (1e-7, "1e-7"),
            (1e20, "100000000000000000000"),
            (1e21, "1e21"),
            (1e300, "1e300"),
            (1.234_567_890_123_456_8e29, "1.2345678901234568e29"),
            (-0.0, "-0"),
            (f64::NAN, "null"),
        ] {
            assert_eq!(format_float_json(v), want, "{v:e}");
        }
        // Float32 from its own shortest digits: widened, 0.1 reads 0.10000000149011612.
        for (v, want) in [(0.1f32, "0.1"), (1e-7, "1e-7"), (3.4e38, "3.4e38")] {
            assert_eq!(format_f32_json(v), want, "{v:e}");
        }
        for (raw, want) in [
            ("q\"b\\s/n", r#""q\"b\\s\/n""#),
            ("\u{8}\u{c}\r\n\t", r#""\b\f\r\n\t""#),
            ("\u{1}\u{1f}\u{7f}", "\"\\u0001\\u001F\u{7f}\""),
            ("\u{2028}\u{2029}", r#""\u2028\u2029""#),
            ("\u{fc}", "\"\u{fc}\""),
        ] {
            assert_eq!(
                String::from_utf8(json_quote_bytes(raw.as_bytes())).unwrap(),
                want,
                "{raw:?}"
            );
        }
        for (byte, want) in [(0u8, "false"), (1, "true")] {
            assert_eq!(
                rowbinary_to_json(&[byte], &ColumnType::Bool),
                want.as_bytes()
            );
        }
        assert!(matches!(ColumnType::parse("Bool"), Some(ColumnType::Bool)));
        assert!(matches!(
            ColumnType::parse("Array(Nullable(Bool))"),
            Some(ColumnType::Array(_))
        ));
    }

    /// `(paths and values, document)`, the document `None` where the paths
    /// have no faithful nesting.
    #[test]
    fn paths_nest_the_way_the_server_prints_them() {
        let cases: &[(&[(&str, &str)], Option<&str>)] = &[
            (&[], Some("{}")),
            (&[("a", "1")], Some(r#"{"a":1}"#)),
            (&[("a.b", "1")], Some(r#"{"a":{"b":1}}"#)),
            (
                &[("e", "2"), ("a.d", r#""x""#), ("a.b.c", "1")],
                Some(r#"{"a":{"b":{"c":1},"d":"x"},"e":2}"#),
            ),
            // Full-path order: `a.b-` sorts ahead of `a.b.x`.
            (
                &[("a.b.x", "1"), ("a.b-", "2")],
                Some(r#"{"a":{"b-":2,"b":{"x":1}}}"#),
            ),
            (&[("a", "1"), ("a.b", "2")], None),
            (&[("a.b", "1"), ("a.b.c", "2")], None),
            (&[("a%2Eb", "1")], None),
            (&[("a..b", "1")], None),
            (&[(".a", "1")], None),
        ];
        for (pairs, want) in cases {
            let pairs = pairs
                .iter()
                .map(|(p, v)| (*p, v.as_bytes().to_vec()))
                .collect();
            let got = nest_paths(pairs);
            match (want, &got) {
                (Some(want), Ok(doc)) => assert_eq!(doc, want.as_bytes(), "{want}"),
                (None, Err(Error::Unsupported(msg))) => {
                    assert!(msg.contains("with_json_as_string(true)"), "{msg}");
                }
                _ => panic!("{want:?}: got {got:?}"),
            }
        }
    }

    /// A typed-path column reads in the string form and refuses the path-based
    /// one, whose typed sub-columns it does not decode.
    #[tokio::test]
    async fn typed_json_reads_as_text_and_refuses_paths() {
        let mut text = 1u64.to_le_bytes().to_vec();
        text.extend_from_slice(&[7, b'{', b'"', b'a', b'"', b':', b'1', b'}']);
        let cells = read_column(&mut std::io::Cursor::new(text), &ColumnType::TypedJson, 1)
            .await
            .expect("the string form reads");
        assert_eq!(cells, [rowbinary_string(br#"{"a":1}"#)]);

        let err = read_column(
            &mut std::io::Cursor::new(json_v2_payload(&[])),
            &ColumnType::TypedJson,
            2,
        )
        .await
        .expect_err("the path-based form is refused");
        assert!(
            matches!(&err, Error::Unsupported(m) if m.contains("typed paths")),
            "{err}"
        );
    }

    /// A server can announce a name nested far past the parser's limit. It
    /// must come back as a malformed response, checked on a 256 KiB stack so
    /// unbounded recursion fails the test rather than fitting a larger one.
    /// A name at exactly the limit still reads.
    #[test]
    fn a_name_nested_past_the_limit_is_refused_without_recursing() {
        let nested = |depth: usize| format!("{}UInt8{}", "Array(".repeat(depth), ")".repeat(depth));
        let deep = nested(100_000);
        let at_limit = nested(MAX_TYPE_PARSE_DEPTH);
        let past_limit = nested(MAX_TYPE_PARSE_DEPTH + 1);
        let outcome = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || {
                [deep, at_limit, past_limit]
                    .map(|name| wire_type(&name, "Dynamic values", CAST_REMEDY))
            })
            .unwrap()
            .join()
            .expect("the check returns rather than overflowing the stack");
        let [deep, at_limit, past_limit] = outcome;
        assert!(matches!(deep, Err(Error::BadResponse(_))), "{deep:?}");
        assert!(matches!(at_limit, Ok(ColumnType::Array(_))), "{at_limit:?}");
        assert!(
            matches!(past_limit, Err(Error::BadResponse(_))),
            "{past_limit:?}"
        );
    }

    /// `(announced type, unsupported)`: a name spelled like a server type that
    /// the parser cannot represent is an unsupported value, anything else a
    /// malformed response.
    #[test]
    fn an_unparsed_type_name_is_unsupported_only_when_well_formed() {
        let mut wrong = Vec::new();
        for (name, unsupported) in [
            ("Tuple(x UInt8)", true),
            ("IntervalSecond", true),
            ("Tuple(s Enum8('a)' = 1))", true),
            ("Variant(IntervalSecond, String)", true),
            ("Nested(a UInt8, b Ring)", true),
            ("AggregateFunction(quantiles(0.5, 0.9), UInt64)", true),
            ("Map(String, IntervalDay)", true),
            // Balanced, and still not a type.
            ("UInt8;garbage", false),
            ("Array(,)", false),
            ("Map(UInt8)", false),
            ("Map(String, UInt8, UInt8)", false),
            ("Array()", false),
            ("Array(UInt8, UInt8)", false),
            ("Array(Nullable)", false),
            ("UInt8()", false),
            ("Tuple(UInt8,)", false),
            ("Tuple(x UInt8 y)", false),
            ("Array(UInt8) UInt8", false),
            ("Enum8('a' = )", false),
            ("Decimal(1, 2, 3)", false),
            ("Foo(=)", false),
            ("Tuple(x UInt8", false),
            ("Tuple(x))", false),
            ("Foo('a)", false),
            ("9Int", false),
            ("", false),
            ("Int\u{1}", false),
        ] {
            match wire_type(name, "Dynamic values", CAST_REMEDY) {
                Err(Error::Unsupported(m)) if unsupported && m.contains(name) => {}
                Err(Error::BadResponse(_)) if !unsupported => {}
                other => wrong.push(format!("{name:?}: {other:?}")),
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// A JSON path whose announced type the parser cannot represent is
    /// refused as unsupported, with the server's spelling, in both
    /// path-based serialisations.
    #[tokio::test]
    async fn a_json_path_of_an_unparsed_type_is_unsupported() {
        const NAME: &[u8] = b"IntervalSecond";
        let respelt = |wire: Vec<u8>| {
            let uint8 = [5, b'U', b'I', b'n', b't', b'8'];
            let at = wire
                .windows(uint8.len())
                .position(|w| w == uint8)
                .expect("the payload declares UInt8");
            let mut out = wire[..at].to_vec();
            out.push(u8::try_from(NAME.len()).unwrap());
            out.extend_from_slice(NAME);
            out.extend_from_slice(&wire[at + uint8.len()..]);
            out
        };
        for wire in [respelt(json_v2_payload(&[])), respelt(json_v3_payload())] {
            let err = read_column(&mut std::io::Cursor::new(wire), &ColumnType::NewJson, 2)
                .await
                .expect_err("an unparsed path type is refused");
            assert!(
                matches!(&err, Error::Unsupported(m)
                    if m.contains("IntervalSecond") && m.contains("with_json_as_string(true)")),
                "{err:?}"
            );
        }
    }

    /// A V2 value the server moved out of its typed storage is refused with the
    /// remedy, never dropped or read as null.
    #[tokio::test]
    async fn a_spilled_v2_value_is_refused_not_dropped() {
        // Row 0 points at SharedVariant (index 0), whose String sub-column
        // carries one opaque value ahead of the now-empty UInt8 one.
        let into_shared_variant = |mut wire: Vec<u8>, tail: usize| {
            let at = wire.len() - tail;
            wire.splice(at..at + 3, [0, 255, 2, 0x0a, 0x01]);
            wire
        };
        let cases = [
            (
                ColumnType::NewJson,
                "shared data",
                "with_json_as_string(true)",
                json_v2_payload(&[("x", "1")]),
            ),
            // The discriminators sit ahead of one UInt8 byte and two u64 offsets.
            (
                ColumnType::NewJson,
                "SharedVariant",
                "with_json_as_string(true)",
                into_shared_variant(json_v2_payload(&[]), 19),
            ),
            // Here only the UInt8 byte follows them.
            (
                ColumnType::Dynamic,
                "SharedVariant",
                "AS String)",
                into_shared_variant(dynamic_v2_payload(), 3),
            ),
        ];
        for (col_type, what, remedy, wire) in cases {
            let mut cursor = std::io::Cursor::new(wire);
            let err = read_column(&mut cursor, &col_type, 2)
                .await
                .expect_err(what);
            let msg = err.to_string();
            assert!(
                matches!(err, Error::Unsupported(_)) && msg.contains(what) && msg.contains(remedy),
                "{col_type:?} {what}: {msg}"
            );
        }
    }

    /// JSON v2: version, one path, its Dynamic v2 header, discriminators and
    /// data, then the `Array(Tuple(String, String))` shared-data section
    /// carrying `spilled`.
    fn json_v2_payload(spilled: &[(&str, &str)]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&2u64.to_le_bytes()); // JSON version
        p.push(1); // varuint: one dynamic path
        p.extend_from_slice(&[1, b'a']);
        p.extend_from_slice(&2u64.to_le_bytes()); // Dynamic v2
        p.push(1); // varuint: one declared type
        p.extend_from_slice(&[5, b'U', b'I', b'n', b't', b'8']);
        p.extend_from_slice(&0u64.to_le_bytes()); // basic discriminators
        // sorted(["SharedVariant", "UInt8"]) puts UInt8 at index 1.
        p.extend_from_slice(&[1, 255]);
        p.push(8); // the one UInt8 value
        // Shared data: cumulative offsets, then the paths, then the values.
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(&(spilled.len() as u64).to_le_bytes());
        for (path, _) in spilled {
            p.push(u8::try_from(path.len()).expect("test paths stay under 128 bytes"));
            p.extend_from_slice(path.as_bytes());
        }
        for (_, value) in spilled {
            p.push(u8::try_from(value.len()).expect("test values stay under 128 bytes"));
            p.extend_from_slice(value.as_bytes());
        }
        p
    }

    /// `Variant(UInt8, String)`: mode word, discriminators, then each arm.
    fn variant_payload() -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&0u64.to_le_bytes()); // basic discriminators
        p.extend_from_slice(&[0, 1]); // row 0 = UInt8, row 1 = String
        p.push(7); // the one UInt8
        p.extend_from_slice(&[1, b'q']); // the one String
        p
    }

    /// Dynamic v2: version, type count, names, mode word, discriminators, data.
    /// `SharedVariant` sorts ahead of `UInt8`, so discriminator 1 is the `UInt8`.
    fn dynamic_v2_payload() -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&2u64.to_le_bytes());
        p.push(1); // varuint: one declared type
        p.extend_from_slice(&[5, b'U', b'I', b'n', b't', b'8']);
        p.extend_from_slice(&0u64.to_le_bytes()); // basic discriminators
        p.extend_from_slice(&[1, 255]); // UInt8 then NULL
        p.push(3); // the one UInt8 value
        p
    }

    /// Dynamic v3: version, type count, names, then flat discriminators.
    fn dynamic_v3_payload() -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&3u64.to_le_bytes());
        p.push(1);
        p.extend_from_slice(&[5, b'U', b'I', b'n', b't', b'8']);
        p.extend_from_slice(&[0, 1]); // UInt8 then NULL (NULL == the type count)
        p.push(4);
        p
    }

    /// JSON v3: version, one path, the path's flattened Dynamic header, then
    /// its discriminators and data.
    fn json_v3_payload() -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&3u64.to_le_bytes()); // JSON version
        p.push(1); // varuint: one dynamic path
        p.extend_from_slice(&[1, b'a']);
        p.extend_from_slice(&3u64.to_le_bytes()); // flattened Dynamic version
        p.push(1); // varuint: one type
        p.extend_from_slice(&[5, b'U', b'I', b'n', b't', b'8']);
        p.extend_from_slice(&[0, 1]); // UInt8 then NULL
        p.push(6);
        p
    }

    #[tokio::test]
    async fn read_column_consumes_exactly_the_declared_bytes() {
        for (type_name, wire, rows) in byte_exact_cases() {
            let col_type =
                ColumnType::parse(type_name).unwrap_or_else(|| panic!("{type_name} parses"));
            let len = wire.len();
            let mut cursor = std::io::Cursor::new(wire);
            let cells = read_column(&mut cursor, &col_type, rows)
                .await
                .unwrap_or_else(|e| panic!("{type_name}: {e}"));
            assert_eq!(
                cursor.position(),
                len as u64,
                "{type_name} did not consume its whole payload"
            );
            assert_eq!(cells.len() as u64, rows, "{type_name} row count");
        }
    }

    #[tokio::test]
    async fn array_column_rejects_offsets_beyond_the_element_count() {
        // Offsets [6, 5] step backwards, so row 1 would index past the elements
        // the column actually carries.
        let mut wire = Vec::new();
        wire.extend_from_slice(&6u64.to_le_bytes());
        wire.extend_from_slice(&5u64.to_le_bytes());
        wire.extend_from_slice(&[0u8; 5]);
        let col_type = ColumnType::Array(Box::new(ColumnType::UInt8));
        let err = read_column(&mut std::io::Cursor::new(wire), &col_type, 2)
            .await
            .expect_err("a backwards offset must reject");
        assert!(err.to_string().contains("Array column offset"), "{err}");
    }

    #[tokio::test]
    async fn map_column_rejects_offsets_beyond_the_pair_count() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&6u64.to_le_bytes());
        wire.extend_from_slice(&5u64.to_le_bytes());
        wire.extend_from_slice(&[0u8; 5]); // keys
        wire.extend_from_slice(&[0u8; 5]); // values
        let col_type = ColumnType::Map(Box::new(ColumnType::UInt8), Box::new(ColumnType::UInt8));
        let err = read_column(&mut std::io::Cursor::new(wire), &col_type, 2)
            .await
            .expect_err("a backwards offset must reject");
        assert!(err.to_string().contains("Map column offset"), "{err}");
    }

    #[tokio::test]
    async fn dynamic_v3_null_discriminator_is_skipped() {
        // NULL is the type count itself in v3, so row 1 renders as `null` and
        // consumes no value byte.
        let col_type = ColumnType::Dynamic;
        let cells = read_column(
            &mut std::io::Cursor::new(dynamic_v3_payload()),
            &col_type,
            2,
        )
        .await
        .expect("v3 payload decodes");
        assert_eq!(cells[0], b"\x014".to_vec(), "row 0 is the UInt8 value");
        assert_eq!(cells[1], b"\x04null".to_vec(), "row 1 is NULL");
    }

    #[tokio::test]
    async fn a_zero_row_top_level_column_reads_no_bytes() {
        // `NativeWriter` writes no payload for a zero-row block, the
        // LowCardinality prefix included.
        let col_type = ColumnType::LowCardinality(Box::new(ColumnType::String));
        let mut cursor = std::io::Cursor::new(Vec::new());
        let cells = read_column(&mut cursor, &col_type, 0)
            .await
            .expect("a zero-row column reads nothing");
        assert_eq!(cells, [] as [std::vec::Vec<u8>; 0]);
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn ipv6_renders_in_compressed_form() {
        let mut raw = [0u8; 16];
        raw[15] = 1;
        let json = rowbinary_to_json(&raw, &ColumnType::IPv6);
        assert_eq!(json, br#""::1""#.to_vec());
    }

    #[test]
    fn ipv4_renders_in_dotted_quad_form() {
        let raw = 0x0100_007fu32.to_le_bytes();
        let json = rowbinary_to_json(&raw, &ColumnType::IPv4);
        assert_eq!(json, br#""1.0.0.127""#.to_vec());
    }

    #[test]
    fn date_and_datetime_render_as_bare_wire_integers() {
        assert_eq!(
            rowbinary_to_json(&1u16.to_le_bytes(), &ColumnType::Date),
            b"1"
        );
        assert_eq!(
            rowbinary_to_json(&1u32.to_le_bytes(), &ColumnType::DateTime),
            b"1"
        );
    }

    #[test]
    fn fixed_string_renders_as_raw_bytes_with_no_length_prefix() {
        let json = rowbinary_to_json(b"abc", &ColumnType::FixedString(3));
        assert_eq!(json, br#""abc""#.to_vec());
    }

    /// Build a RowBinary string cell: varuint length, then the bytes.
    fn rb_string(s: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.put_var_uint(s.len() as u64);
        out.extend_from_slice(s);
        out
    }

    /// Every rendering arm, driven through the type parser the decoder uses.
    ///
    /// This is what turns a `Variant`, `Dynamic` or `JSON` cell into the text a
    /// caller reads, so a wrong arm here is silently wrong data rather than a
    /// failed query. The expectations are written from the wire format, not
    /// from what the function currently returns.
    #[test]
    fn every_scalar_arm_renders_the_json_the_wire_asked_for() {
        let cases: &[(&str, Vec<u8>, &str)] = &[
            ("UInt8", vec![200], "200"),
            ("UInt16", 4_000u16.to_le_bytes().to_vec(), "4000"),
            ("UInt32", 70_000u32.to_le_bytes().to_vec(), "70000"),
            (
                "UInt64",
                5_000_000_000u64.to_le_bytes().to_vec(),
                "5000000000",
            ),
            // The signed arms reinterpret the unsigned wire byte.
            ("Int8", vec![0xff], "-1"),
            ("Int16", (-2i16).to_le_bytes().to_vec(), "-2"),
            ("Int32", (-3i32).to_le_bytes().to_vec(), "-3"),
            ("Int64", (-4i64).to_le_bytes().to_vec(), "-4"),
            ("Int128", (-5i128).to_le_bytes().to_vec(), "-5"),
            ("UInt128", 6u128.to_le_bytes().to_vec(), "6"),
            ("Enum8('a' = -1)", vec![0xff], "-1"),
            ("Float32", 1.5f32.to_le_bytes().to_vec(), "1.5"),
            ("Float64", (-2.25f64).to_le_bytes().to_vec(), "-2.25"),
            // BFloat16 is the top half of an f32's bits, so 0x3f80 is 1.0.
            ("BFloat16", 0x3f80u16.to_le_bytes().to_vec(), "1"),
            ("Decimal32(2)", 150i32.to_le_bytes().to_vec(), "150"),
            ("Decimal64(2)", 151i64.to_le_bytes().to_vec(), "151"),
            ("Date", 19_000u16.to_le_bytes().to_vec(), "19000"),
            ("Date32", 19_001i32.to_le_bytes().to_vec(), "19001"),
            (
                "DateTime",
                1_700_000_000u32.to_le_bytes().to_vec(),
                "1700000000",
            ),
            (
                "DateTime64(3)",
                1_700_000_000_123i64.to_le_bytes().to_vec(),
                "1700000000123",
            ),
            ("String", rb_string(b"hi"), r#""hi""#),
            ("FixedString(2)", b"ab".to_vec(), r#""ab""#),
            (
                "IPv4",
                0x0100_007fu32.to_le_bytes().to_vec(),
                r#""1.0.0.127""#,
            ),
            ("Nullable(UInt8)", vec![1], "null"),
            ("Nullable(UInt8)", vec![0, 9], "9"),
            ("LowCardinality(String)", rb_string(b"x"), r#""x""#),
            ("SimpleAggregateFunction(any, UInt8)", vec![3], "3"),
            ("Array(UInt8)", vec![2, 7, 8], "[7,8]"),
            ("Array(UInt8)", vec![0], "[]"),
            ("Tuple(UInt8, UInt8)", vec![1, 2], "[1,2]"),
            (
                "Point",
                {
                    let mut v = 1.5f64.to_le_bytes().to_vec();
                    v.extend_from_slice(&2.5f64.to_le_bytes());
                    v
                },
                "[1.5,2.5]",
            ),
            (
                "Map(String, UInt8)",
                {
                    let mut v = vec![1u8];
                    v.extend_from_slice(&rb_string(b"k"));
                    v.push(9);
                    v
                },
                r#"{"k":9}"#,
            ),
        ];

        for (type_str, bytes, want) in cases {
            let ct = ColumnType::parse(type_str).unwrap_or_else(|| panic!("{type_str} parses"));
            let got = rowbinary_to_json(bytes, &ct);
            assert_eq!(
                String::from_utf8_lossy(&got),
                *want,
                "{type_str} rendered the wrong JSON"
            );
        }
    }

    /// A 32-byte integer has no JSON number form, so it renders as a hex string
    /// with the wire's little-endian bytes reversed to reading order.
    #[test]
    fn a_256_bit_integer_renders_as_big_endian_hex() {
        let mut raw = [0u8; 32];
        raw[0] = 0xef;
        raw[1] = 0xbe;
        let json = rowbinary_to_json(&raw, &ColumnType::Int256);
        let text = String::from_utf8(json).unwrap();
        assert!(text.ends_with(r#"beef""#), "got {text}");
        assert_eq!(text.len(), 66, "quotes plus 64 hex digits");
    }

    /// The 16 bytes are TWO little-endian u64s, high half first -- not one
    /// big-endian run -- so the rendered digits are not the byte order.
    #[test]
    fn a_uuid_renders_hyphenated_from_two_le_halves() {
        let mut raw = 0x0123_4567_89ab_cdefu64.to_le_bytes().to_vec();
        raw.extend_from_slice(&0xfedc_ba98_7654_3210u64.to_le_bytes());
        let json = rowbinary_to_json(&raw, &ColumnType::Uuid);
        assert_eq!(json, br#""01234567-89ab-cdef-fedc-ba9876543210""#);
    }

    /// NaN and infinity have no JSON form, so they render as `null` rather than
    /// as text no JSON parser would accept.
    #[test]
    fn floats_without_a_json_form_render_as_null() {
        assert_eq!(
            rowbinary_to_json(&f64::NAN.to_le_bytes(), &ColumnType::Float64),
            b"null"
        );
        assert_eq!(
            rowbinary_to_json(&f64::INFINITY.to_le_bytes(), &ColumnType::Float64),
            b"null"
        );
        assert_eq!(
            rowbinary_to_json(&f32::NEG_INFINITY.to_le_bytes(), &ColumnType::Float32),
            b"null"
        );
    }

    /// A string cell is arbitrary bytes, so anything that would break out of a
    /// JSON string has to be escaped -- including the control characters that
    /// have no short form.
    #[test]
    fn string_rendering_escapes_what_would_break_the_json() {
        let raw = b"a\"b\\c\nd\re\tf\x01g";
        let json = rowbinary_to_json(&rb_string(raw), &ColumnType::String);
        // Concatenated rather than one literal: the expected text is itself
        // full of backslash escapes, and writing them inside a literal makes
        // the test read as though the escapes were the literal's own.
        let want = [
            "\"a", "\\\"", "b", "\\\\", "c", "\\n", "d", "\\r", "e", "\\t", "f", "\\u0001", "g\"",
        ]
        .concat();
        assert_eq!(String::from_utf8(json).unwrap(), want);
    }

    /// Every arm is best-effort: a truncated cell renders as `null` rather than
    /// panicking or propagating, because one bad cell must not fail the block.
    #[test]
    fn a_truncated_cell_renders_as_null_in_every_arm() {
        let types = [
            "UInt64",
            "Int128",
            "Int256",
            "Float64",
            "BFloat16",
            "UUID",
            "IPv6",
            "Point",
            "String",
            "FixedString(4)",
            "Nullable(UInt8)",
            "Array(UInt8)",
            "Tuple(UInt8, UInt8)",
            "Map(String, UInt8)",
            "DateTime64(3)",
        ];
        for type_str in types {
            let ct = ColumnType::parse(type_str).unwrap_or_else(|| panic!("{type_str} parses"));
            // One byte short of anything useful, and empty.
            for cell in [b"\x01".as_slice(), b"".as_slice()] {
                assert_eq!(
                    rowbinary_to_json(cell, &ct),
                    b"null",
                    "{type_str} did not fall back to null on a truncated cell"
                );
            }
        }
    }

    #[test]
    fn index_width_matches_the_server_ladder() {
        // `getSmallestIndexesType(slots + 1)` keeps u8 up to 255 types.
        assert_eq!(index_width(0), 1);
        assert_eq!(index_width(254), 1);
        assert_eq!(index_width(255), 1);
        assert_eq!(index_width(256), 2);
        assert_eq!(index_width(65_535), 2);
        assert_eq!(index_width(65_536), 4);
    }
}
