// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-clickhouse
// File:      src/dynamic/parsed_type.rs
// Purpose:   Runtime ClickHouse type-string parser (ParsedType)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Rich ClickHouse type parser for runtime schema-driven inserts.
//!
//! Parses ClickHouse type strings from `system.columns` into a structured
//! `ParsedType` AST. Handles Nullable, LowCardinality, Array, Map,
//! DateTime64(precision, timezone), Decimal(precision, scale), FixedString(n),
//! Enum8/Enum16, and all scalar types.

use std::fmt;

/// Pre-computed discriminant for the base type, avoiding string comparison
/// on the encode hot path. Resolved once at schema-fetch time, used on
/// every row thereafter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TypeTag {
    /// `String`: a varuint length then the bytes, which need not be UTF-8.
    String,
    /// `FixedString(N)`: exactly N bytes, no length prefix.
    FixedString,
    /// `UInt8`.
    UInt8,
    /// `UInt16`.
    UInt16,
    /// `UInt32`.
    UInt32,
    /// `UInt64`.
    UInt64,
    /// `UInt128`.
    UInt128,
    /// `UInt256`, carried as 32 little-endian bytes.
    UInt256,
    /// `Int8`.
    Int8,
    /// `Int16`.
    Int16,
    /// `Int32`.
    Int32,
    /// `Int64`.
    Int64,
    /// `Int128`.
    Int128,
    /// `Int256`, carried as 32 little-endian bytes.
    Int256,
    /// `Float32`.
    Float32,
    /// `Float64`.
    Float64,
    /// `Bool`, which is a `UInt8` on the wire.
    Bool,
    /// `Date`: unsigned days since the epoch, `UInt16` wide.
    Date,
    /// `Date32`: signed days since the epoch, `Int32` wide.
    Date32,
    /// `DateTime`: epoch seconds, `UInt32` wide.
    DateTime,
    /// `DateTime64(P)`: `Int64` ticks at P sub-second digits.
    DateTime64,
    /// `Decimal(P, S)` with P <= 9, on an `Int32` backing.
    Decimal32,
    /// `Decimal(P, S)` with P <= 18, on an `Int64` backing.
    Decimal64,
    /// `Decimal(P, S)` with P <= 38, on an `Int128` backing.
    Decimal128,
    /// `Decimal(P, S)` above 38 digits, on an `Int256` backing.
    Decimal256,
    /// `UUID`, 16 raw bytes.
    UUID,
    /// `IPv4`, on a `UInt32` backing.
    IPv4,
    /// `IPv6`, 16 raw bytes.
    IPv6,
    /// `Enum8(...)`: the member index, `Int8` wide.
    Enum8,
    /// `Enum16(...)`: the member index, `Int16` wide.
    Enum16,
    /// `Array(T)`; the element type is the parsed type's first child.
    Array,
    /// `Map(K, V)`; the key and value types are the two children.
    Map,
    /// `Point`, which is `Tuple(Float64, Float64)`.
    Point,
    /// `JSON`, written by this encoder as length-prefixed document text.
    JSON,
    /// `Variant(T1, ...)`, which this encoder rejects.
    Variant,
    /// `Dynamic`, which this encoder rejects.
    Dynamic,
    /// A type name this parser does not recognise; the encoder rejects it.
    Unknown,
}

impl TypeTag {
    /// Resolve a base type name to its tag.
    #[must_use]
    pub fn from_base(base: &str) -> Self {
        match base {
            "String" => Self::String,
            "FixedString" => Self::FixedString,
            "UInt8" => Self::UInt8,
            "UInt16" => Self::UInt16,
            "UInt32" => Self::UInt32,
            "UInt64" => Self::UInt64,
            "UInt128" => Self::UInt128,
            "UInt256" => Self::UInt256,
            "Int8" => Self::Int8,
            "Int16" => Self::Int16,
            "Int32" => Self::Int32,
            "Int64" => Self::Int64,
            "Int128" => Self::Int128,
            "Int256" => Self::Int256,
            "Float32" => Self::Float32,
            "Float64" => Self::Float64,
            "Bool" => Self::Bool,
            "Date" => Self::Date,
            "Date32" => Self::Date32,
            "DateTime" => Self::DateTime,
            "DateTime64" => Self::DateTime64,
            "Decimal32" => Self::Decimal32,
            "Decimal64" => Self::Decimal64,
            "Decimal128" => Self::Decimal128,
            "Decimal256" => Self::Decimal256,
            "UUID" => Self::UUID,
            "IPv4" => Self::IPv4,
            "IPv6" => Self::IPv6,
            "Enum8" => Self::Enum8,
            "Enum16" => Self::Enum16,
            "Array" => Self::Array,
            "Map" => Self::Map,
            "Point" => Self::Point,
            "JSON" | "Object" => Self::JSON,
            "Variant" => Self::Variant,
            "Dynamic" => Self::Dynamic,
            _ => Self::Unknown,
        }
    }
}

/// Parsed ClickHouse type information.
///
/// Runtime representation of a ClickHouse column type. Unknown types are
/// preserved as-is for forward compatibility.
///
/// # Examples
///
/// ```
/// use dfe_clickhouse::dynamic::ParsedType;
///
/// let t = ParsedType::parse("LowCardinality(Nullable(String))");
/// assert_eq!(t.base, "String");
/// assert!(t.nullable);
/// assert!(t.low_cardinality);
///
/// let t = ParsedType::parse("DateTime64(3, 'UTC')");
/// assert_eq!(t.base, "DateTime64");
/// assert_eq!(t.precision, Some(3));
/// assert_eq!(t.timezone.as_deref(), Some("UTC"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ParsedType {
    /// Original type string from ClickHouse, verbatim -- what the Native block
    /// header declares.
    pub raw: String,
    /// Base type name (e.g., "String", "Int64", "DateTime64").
    pub base: String,
    /// Pre-computed discriminant for fast dispatch in the encode hot path.
    /// Avoids string comparison on every row.
    pub tag: TypeTag,
    /// Whether wrapped in Nullable().
    pub nullable: bool,
    /// Whether wrapped in LowCardinality().
    pub low_cardinality: bool,
    /// For Array types, the element type.
    pub array_element: Option<Box<ParsedType>>,
    /// For Map types, (key_type, value_type).
    pub map_types: Option<(Box<ParsedType>, Box<ParsedType>)>,
    /// Extended info: precision for DateTime64, Decimal, etc.
    pub precision: Option<u8>,
    /// Extended info: scale for Decimal types.
    pub scale: Option<u8>,
    /// Extended info: timezone for DateTime64.
    pub timezone: Option<String>,
    /// Extended info: size for FixedString.
    pub fixed_size: Option<usize>,
}

impl ParsedType {
    /// Parse a ClickHouse type string into a structured `ParsedType`.
    #[must_use]
    pub fn parse(type_str: &str) -> Self {
        let type_str = type_str.trim();
        Self::parse_inner(type_str, type_str.to_string())
    }

    fn parse_inner(type_str: &str, raw: String) -> Self {
        let mut result = Self {
            raw,
            base: String::new(),
            tag: TypeTag::Unknown,
            nullable: false,
            low_cardinality: false,
            array_element: None,
            map_types: None,
            precision: None,
            scale: None,
            timezone: None,
            fixed_size: None,
        };

        let mut type_str = type_str.trim().to_string();

        // Unwrap wrappers in a loop (handles LowCardinality(Nullable(...)) etc.)
        loop {
            let (unwrapped, is_nullable) = Self::unwrap_wrapper(&type_str, "Nullable");
            if is_nullable {
                result.nullable = true;
                type_str = unwrapped;
                continue;
            }

            let (unwrapped, is_lc) = Self::unwrap_wrapper(&type_str, "LowCardinality");
            if is_lc {
                result.low_cardinality = true;
                type_str = unwrapped;
                continue;
            }

            break;
        }

        // SimpleAggregateFunction(f, T) is T on the wire. The raw text, which
        // the Native header declares, stays as the server reported it.
        if let Some(args) = Self::extract_wrapper(&type_str, "SimpleAggregateFunction")
            && let Some((_, inner)) = Self::split_type_args(&args)
        {
            let mut parsed = Self::parse(&inner);
            parsed.raw = result.raw;
            parsed.nullable |= result.nullable;
            parsed.low_cardinality |= result.low_cardinality;
            return parsed;
        }

        // Typed paths included: an insert sends JSON as text on both transports.
        if let Some(args) = Self::extract_wrapper(&type_str, "JSON") {
            result.base = "JSON".to_string();
            if crate::native::columns::json_args(&args).is_some() {
                result.tag = TypeTag::JSON;
            }
            return result;
        }
        if type_str == "Object('json')" {
            result.base = "Object".to_string();
            result.tag = TypeTag::JSON;
            return result;
        }

        if let Some(inner) = Self::extract_wrapper(&type_str, "Array") {
            result.base = "Array".to_string();
            result.tag = TypeTag::Array;
            result.array_element = Some(Box::new(Self::parse(&inner)));
            return result;
        }

        if let Some(inner) = Self::extract_wrapper(&type_str, "Map")
            && let Some((key, value)) = Self::split_type_args(&inner)
        {
            result.base = "Map".to_string();
            result.tag = TypeTag::Map;
            result.map_types = Some((Box::new(Self::parse(&key)), Box::new(Self::parse(&value))));
            return result;
        }

        // Check for DateTime64(precision, 'timezone')
        if type_str.starts_with("DateTime64") {
            result.base = "DateTime64".to_string();
            result.tag = TypeTag::DateTime64;
            if let Some(inner) = Self::extract_wrapper(&type_str, "DateTime64") {
                let parts: Vec<&str> = inner.splitn(2, ',').collect();
                result.precision = parts.first().and_then(|p| p.trim().parse().ok());
                result.timezone = parts
                    .get(1)
                    .map(|tz| tz.trim().trim_matches('\'').trim_matches('"').to_string());
            }
            return result;
        }

        // DateTime('zone'): the zone rides on the type, the wire is the same
        // UInt32 seconds.
        if let Some(inner) = Self::extract_wrapper(&type_str, "DateTime") {
            result.base = "DateTime".to_string();
            result.tag = TypeTag::DateTime;
            result.timezone = Some(
                inner
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_string(),
            );
            return result;
        }

        // Check for FixedString(N)
        if let Some(inner) = Self::extract_wrapper(&type_str, "FixedString") {
            result.base = "FixedString".to_string();
            result.tag = TypeTag::FixedString;
            result.fixed_size = inner.trim().parse().ok();
            return result;
        }

        // Check for Decimal(P, S) or Decimal32/64/128/256(S)
        if type_str.starts_with("Decimal") {
            result.base = Self::parse_decimal_base(&type_str);
            result.tag = TypeTag::from_base(&result.base);
            if let Some(inner) = Self::extract_parens(&type_str) {
                let parts: Vec<&str> = inner.split(',').collect();
                if parts.len() == 2 {
                    result.precision = parts[0].trim().parse().ok();
                    result.scale = parts[1].trim().parse().ok();
                } else if parts.len() == 1 {
                    result.scale = parts[0].trim().parse().ok();
                }
            }
            return result;
        }

        // Check for Enum8/Enum16
        if type_str.starts_with("Enum8") || type_str.starts_with("Enum16") {
            if type_str.starts_with("Enum8") {
                result.base = "Enum8".to_string();
                result.tag = TypeTag::Enum8;
            } else {
                result.base = "Enum16".to_string();
                result.tag = TypeTag::Enum16;
            }
            return result;
        }

        // Variant(T1, ...) and Dynamic / Dynamic(max_types=N): tagged so the
        // encoder names them in its rejection instead of calling them unknown.
        if type_str.starts_with("Variant(") {
            result.base = "Variant".to_string();
            result.tag = TypeTag::Variant;
            return result;
        }
        if type_str == "Dynamic" || type_str.starts_with("Dynamic(") {
            result.base = "Dynamic".to_string();
            result.tag = TypeTag::Dynamic;
            return result;
        }

        // A geo shape is an array of points on the wire, so it encodes through
        // the Array arm while `base` keeps the name `system.columns` reported.
        if let Some(element) = geo_shape_element(&type_str) {
            result.tag = TypeTag::Array;
            result.array_element = Some(Box::new(Self::parse(element)));
            result.base = type_str;
            return result;
        }

        // Simple type — `type_str` is the owned, unwrapped `String` and is not
        // used after this, so move it rather than clone.
        result.base = type_str;
        result.tag = TypeTag::from_base(&result.base);
        result
    }

    fn unwrap_wrapper(type_str: &str, wrapper: &str) -> (String, bool) {
        match Self::extract_wrapper(type_str, wrapper) {
            Some(inner) => (inner, true),
            None => (type_str.to_string(), false),
        }
    }

    fn extract_wrapper(type_str: &str, wrapper: &str) -> Option<String> {
        type_str
            .strip_prefix(wrapper)?
            .strip_prefix('(')?
            .strip_suffix(')')
            .map(std::string::ToString::to_string)
    }

    fn extract_parens(type_str: &str) -> Option<String> {
        let start = type_str.find('(')?;
        let end = type_str.rfind(')')?;
        if start < end {
            Some(type_str[start + 1..end].to_string())
        } else {
            None
        }
    }

    fn parse_decimal_base(type_str: &str) -> String {
        if type_str.starts_with("Decimal256") {
            "Decimal256".to_string()
        } else if type_str.starts_with("Decimal128") {
            "Decimal128".to_string()
        } else if type_str.starts_with("Decimal64") {
            "Decimal64".to_string()
        } else if type_str.starts_with("Decimal32") {
            "Decimal32".to_string()
        } else {
            "Decimal".to_string()
        }
    }

    /// Split Map(K, V) or similar two-arg types, handling nested parens.
    fn split_type_args(inner: &str) -> Option<(String, String)> {
        let mut depth = 0;
        for (i, c) in inner.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    return Some((
                        inner[..i].trim().to_string(),
                        inner[i + 1..].trim().to_string(),
                    ));
                }
                _ => {}
            }
        }
        None
    }

    /// Coercion category for the base type, for a caller deciding how to shape
    /// a value before handing it over.
    ///
    /// A name this parser does not recognise reports `"Unknown"`, not
    /// `"String"`: the encoder rejects it either way, and calling it a string
    /// would let [`Self::is_string`] agree to stringify a `Variant`.
    #[must_use]
    pub fn category(&self) -> &str {
        match self.base.as_str() {
            "String" | "FixedString" => "String",
            "Int8" | "Int16" | "Int32" | "Int64" | "Int128" | "Int256" => "Int",
            "UInt8" | "UInt16" | "UInt32" | "UInt64" | "UInt128" | "UInt256" => "UInt",
            "Float32" | "Float64" => "Float",
            "Decimal" | "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => "Decimal",
            "Bool" => "Bool",
            "Date" | "Date32" => "Date",
            "DateTime" => "DateTime",
            "DateTime64" => "DateTime64",
            "UUID" => "UUID",
            "IPv4" => "IPv4",
            "IPv6" => "IPv6",
            "Array" => "Array",
            "Map" => "Map",
            "JSON" | "Object" => "JSON",
            "Variant" => "Variant",
            "Dynamic" => "Dynamic",
            "Enum8" | "Enum16" => "Enum",
            "Point" | "Ring" | "Polygon" | "MultiPolygon" | "LineString" | "MultiLineString" => {
                "Geo"
            }
            _ => "Unknown",
        }
    }

    /// Check if this is a numeric type.
    #[must_use]
    pub fn is_numeric(&self) -> bool {
        matches!(self.category(), "Int" | "UInt" | "Float" | "Decimal")
    }

    /// Check if this is a string type.
    #[must_use]
    pub fn is_string(&self) -> bool {
        self.category() == "String"
    }

    /// Check if this is a date/time type.
    #[must_use]
    pub fn is_datetime(&self) -> bool {
        matches!(self.category(), "Date" | "DateTime" | "DateTime64")
    }

    /// Check if this is an IP address type.
    #[must_use]
    pub fn is_ip(&self) -> bool {
        matches!(self.category(), "IPv4" | "IPv6")
    }

    /// Whether JSON appears anywhere in this type, an `Array(JSON)` element or
    /// a `Map` value included.
    pub(crate) fn contains_json(&self) -> bool {
        self.tag == TypeTag::JSON
            || self
                .array_element
                .as_ref()
                .is_some_and(|e| e.contains_json())
            || self
                .map_types
                .as_ref()
                .is_some_and(|(k, v)| k.contains_json() || v.contains_json())
    }

    /// Byte size of this type's fixed-width representation, if applicable.
    ///
    /// Returns `None` for variable-length types (String, Array, Map, JSON).
    #[must_use]
    pub fn fixed_byte_size(&self) -> Option<usize> {
        match self.base.as_str() {
            "UInt8" | "Int8" | "Bool" | "Enum8" => Some(1),
            "UInt16" | "Int16" | "Enum16" | "Date" => Some(2),
            "UInt32" | "Int32" | "Date32" | "DateTime" | "Float32" | "Decimal32" | "IPv4" => {
                Some(4)
            }
            "UInt64" | "Int64" | "Float64" | "Decimal64" | "DateTime64" => Some(8),
            "Int128" | "UInt128" | "Decimal128" | "UUID" | "IPv6" => Some(16),
            "Int256" | "UInt256" | "Decimal256" => Some(32),
            "FixedString" => self.fixed_size,
            _ => None,
        }
    }
}

/// The element type of a ClickHouse geo shape, which is an `Array` of it.
fn geo_shape_element(base: &str) -> Option<&'static str> {
    match base {
        "Ring" | "LineString" => Some("Point"),
        "Polygon" => Some("Ring"),
        "MultiLineString" => Some("LineString"),
        "MultiPolygon" => Some("Polygon"),
        _ => None,
    }
}

impl fmt::Display for ParsedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_types() {
        let t = ParsedType::parse("String");
        assert_eq!(t.base, "String");
        assert!(!t.nullable);
        assert!(!t.low_cardinality);

        let t = ParsedType::parse("Int64");
        assert_eq!(t.base, "Int64");
        assert_eq!(t.category(), "Int");

        let t = ParsedType::parse("Float64");
        assert_eq!(t.base, "Float64");
        assert_eq!(t.category(), "Float");
    }

    #[test]
    fn test_parse_nullable() {
        let t = ParsedType::parse("Nullable(String)");
        assert_eq!(t.base, "String");
        assert!(t.nullable);
        assert!(!t.low_cardinality);
    }

    #[test]
    fn test_parse_low_cardinality() {
        let t = ParsedType::parse("LowCardinality(String)");
        assert_eq!(t.base, "String");
        assert!(!t.nullable);
        assert!(t.low_cardinality);
    }

    #[test]
    fn test_parse_nullable_low_cardinality() {
        let t = ParsedType::parse("LowCardinality(Nullable(String))");
        assert_eq!(t.base, "String");
        assert!(t.nullable);
        assert!(t.low_cardinality);
    }

    #[test]
    fn test_parse_array() {
        let t = ParsedType::parse("Array(Int64)");
        assert_eq!(t.base, "Array");
        assert!(t.array_element.is_some());
        let elem = t.array_element.as_ref().unwrap();
        assert_eq!(elem.base, "Int64");
    }

    #[test]
    fn test_parse_map() {
        let t = ParsedType::parse("Map(String, Int64)");
        assert_eq!(t.base, "Map");
        assert!(t.map_types.is_some());
        let (key, value) = t.map_types.as_ref().unwrap();
        assert_eq!(key.base, "String");
        assert_eq!(value.base, "Int64");
    }

    #[test]
    fn test_parse_datetime64() {
        let t = ParsedType::parse("DateTime64(3)");
        assert_eq!(t.base, "DateTime64");
        assert_eq!(t.precision, Some(3));
        assert!(t.timezone.is_none());

        let t = ParsedType::parse("DateTime64(6, 'UTC')");
        assert_eq!(t.base, "DateTime64");
        assert_eq!(t.precision, Some(6));
        assert_eq!(t.timezone, Some("UTC".to_string()));
    }

    /// A zone argument is part of the column type, not a different type:
    /// `system.columns` reports `DateTime('UTC')` for every zoned column.
    #[test]
    fn test_parse_datetime_with_and_without_a_zone() {
        for (ty, zone) in [
            ("DateTime", None),
            ("DateTime('UTC')", Some("UTC")),
            ("DateTime('Australia/Sydney')", Some("Australia/Sydney")),
            ("Nullable(DateTime('UTC'))", Some("UTC")),
        ] {
            let t = ParsedType::parse(ty);
            assert_eq!(t.tag, TypeTag::DateTime, "{ty}");
            assert_eq!(t.base, "DateTime", "{ty}");
            assert_eq!(t.timezone.as_deref(), zone, "{ty}");
        }
    }

    #[test]
    fn test_parse_fixed_string() {
        let t = ParsedType::parse("FixedString(32)");
        assert_eq!(t.base, "FixedString");
        assert_eq!(t.fixed_size, Some(32));
    }

    #[test]
    fn test_parse_decimal() {
        let t = ParsedType::parse("Decimal(18, 6)");
        assert_eq!(t.base, "Decimal");
        assert_eq!(t.precision, Some(18));
        assert_eq!(t.scale, Some(6));

        let t = ParsedType::parse("Decimal64(4)");
        assert_eq!(t.base, "Decimal64");
        assert_eq!(t.scale, Some(4));
    }

    #[test]
    fn test_parse_enum() {
        let t = ParsedType::parse("Enum8('a' = 1, 'b' = 2)");
        assert_eq!(t.base, "Enum8");

        let t = ParsedType::parse("Enum16('x' = 100)");
        assert_eq!(t.base, "Enum16");
    }

    #[test]
    fn test_categories() {
        assert_eq!(ParsedType::parse("String").category(), "String");
        assert_eq!(ParsedType::parse("Int64").category(), "Int");
        assert_eq!(ParsedType::parse("UInt32").category(), "UInt");
        assert_eq!(ParsedType::parse("Float64").category(), "Float");
        assert_eq!(ParsedType::parse("Bool").category(), "Bool");
        assert_eq!(ParsedType::parse("DateTime").category(), "DateTime");
        assert_eq!(ParsedType::parse("UUID").category(), "UUID");
        assert_eq!(ParsedType::parse("IPv4").category(), "IPv4");
        assert_eq!(ParsedType::parse("JSON").category(), "JSON");
        // Unknown, not String -- otherwise `is_string` would agree to
        // stringify a type the encoder is going to reject.
        assert_eq!(ParsedType::parse("SomeNewType").category(), "Unknown");
        assert!(!ParsedType::parse("SomeNewType").is_string());
    }

    #[test]
    fn test_is_helpers() {
        assert!(ParsedType::parse("Int64").is_numeric());
        assert!(ParsedType::parse("Float64").is_numeric());
        assert!(!ParsedType::parse("String").is_numeric());

        assert!(ParsedType::parse("String").is_string());
        assert!(ParsedType::parse("FixedString(10)").is_string());

        assert!(ParsedType::parse("DateTime").is_datetime());
        assert!(ParsedType::parse("DateTime64(3)").is_datetime());
        assert!(ParsedType::parse("Date").is_datetime());

        assert!(ParsedType::parse("IPv4").is_ip());
        assert!(ParsedType::parse("IPv6").is_ip());
    }

    #[test]
    fn test_fixed_byte_size() {
        assert_eq!(ParsedType::parse("UInt8").fixed_byte_size(), Some(1));
        assert_eq!(ParsedType::parse("Int32").fixed_byte_size(), Some(4));
        assert_eq!(ParsedType::parse("Float64").fixed_byte_size(), Some(8));
        assert_eq!(ParsedType::parse("UUID").fixed_byte_size(), Some(16));
        assert_eq!(ParsedType::parse("Int256").fixed_byte_size(), Some(32));
        assert_eq!(
            ParsedType::parse("FixedString(32)").fixed_byte_size(),
            Some(32)
        );
        assert_eq!(ParsedType::parse("String").fixed_byte_size(), None);
        assert_eq!(ParsedType::parse("Array(Int64)").fixed_byte_size(), None);
    }

    #[test]
    fn test_nested_array_map() {
        let t = ParsedType::parse("Array(Nullable(String))");
        assert_eq!(t.base, "Array");
        let elem = t.array_element.as_ref().unwrap();
        assert_eq!(elem.base, "String");
        assert!(elem.nullable);

        let t = ParsedType::parse("Map(String, Array(UInt64))");
        assert_eq!(t.base, "Map");
        let (k, v) = t.map_types.as_ref().unwrap();
        assert_eq!(k.base, "String");
        assert_eq!(v.base, "Array");
    }

    #[test]
    fn test_display() {
        let t = ParsedType::parse("LowCardinality(Nullable(String))");
        assert_eq!(t.to_string(), "LowCardinality(Nullable(String))");
    }

    /// The wrapper is the inner type on the wire, so the parse is the inner
    /// type's, with the raw text kept for the block header.
    #[test]
    fn a_simple_aggregate_function_parses_as_its_inner_type() {
        let t = ParsedType::parse("SimpleAggregateFunction(sum, UInt64)");
        assert_eq!(t.tag, TypeTag::UInt64);
        assert_eq!(t.raw, "SimpleAggregateFunction(sum, UInt64)");
        let t = ParsedType::parse("SimpleAggregateFunction(anyLast, Nullable(String))");
        assert_eq!(t.tag, TypeTag::String);
        assert!(t.nullable);
        let t = ParsedType::parse("SimpleAggregateFunction(groupUniqArrayArray, Array(UInt32))");
        assert_eq!(t.tag, TypeTag::Array);
    }

    #[test]
    fn a_geo_shape_is_an_array_down_to_its_points() {
        for (raw, arrays) in [
            ("Ring", 1),
            ("LineString", 1),
            ("Polygon", 2),
            ("MultiLineString", 2),
            ("MultiPolygon", 3),
        ] {
            let t = ParsedType::parse(raw);
            assert_eq!(t.base, raw, "base keeps the reported name");
            assert_eq!(t.category(), "Geo", "{raw}");
            assert_eq!(t.fixed_byte_size(), None, "{raw} is variable length");
            let mut level = &t;
            for _ in 0..arrays {
                assert_eq!(level.tag, TypeTag::Array, "{raw}");
                level = level.array_element.as_deref().expect("an element type");
            }
            assert_eq!(level.tag, TypeTag::Point, "{raw} must bottom out at Point");
        }
    }

    #[test]
    fn category_date_types() {
        assert_eq!(ParsedType::parse("Date").category(), "Date");
        assert_eq!(ParsedType::parse("Date32").category(), "Date");
    }

    #[test]
    fn category_decimal_types() {
        assert_eq!(ParsedType::parse("Decimal(18, 4)").category(), "Decimal");
        assert_eq!(ParsedType::parse("Decimal32(2)").category(), "Decimal");
        assert_eq!(ParsedType::parse("Decimal64(4)").category(), "Decimal");
        assert_eq!(ParsedType::parse("Decimal128(8)").category(), "Decimal");
    }

    #[test]
    fn category_int_widths() {
        for t in ["Int8", "Int16", "Int32", "Int64", "Int128", "Int256"] {
            assert_eq!(ParsedType::parse(t).category(), "Int", "Failed for {t}");
        }
    }

    #[test]
    fn category_uint_widths() {
        for t in ["UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256"] {
            assert_eq!(ParsedType::parse(t).category(), "UInt", "Failed for {t}");
        }
    }

    #[test]
    fn category_nullable_preserves_inner() {
        assert_eq!(ParsedType::parse("Nullable(Int64)").category(), "Int");
        assert_eq!(ParsedType::parse("Nullable(UUID)").category(), "UUID");
    }

    #[test]
    fn category_lc_preserves_inner() {
        assert_eq!(
            ParsedType::parse("LowCardinality(String)").category(),
            "String"
        );
    }

    #[test]
    fn parse_empty_string() {
        let t = ParsedType::parse("");
        assert_eq!(t.base, "");
    }

    #[test]
    fn parse_enum_type() {
        let t = ParsedType::parse("Enum8('a' = 1, 'b' = 2)");
        assert_eq!(t.category(), "Enum");
    }

    #[test]
    fn variant_and_dynamic_get_their_own_tags() {
        let v = ParsedType::parse("Variant(UInt8, String)");
        assert_eq!(v.base, "Variant");
        assert_eq!(v.tag, TypeTag::Variant);
        assert_eq!(v.category(), "Variant");

        for raw in ["Dynamic", "Dynamic(max_types=8)"] {
            let d = ParsedType::parse(raw);
            assert_eq!(d.base, "Dynamic", "failed for {raw}");
            assert_eq!(d.tag, TypeTag::Dynamic, "failed for {raw}");
        }
    }

    /// `(server type, tag, nullable)`. Typed paths tag as JSON, because both
    /// transports insert JSON as text.
    #[test]
    fn json_with_parameters_tags_as_json() {
        for (raw, tag, nullable) in [
            ("JSON", TypeTag::JSON, false),
            ("JSON(max_dynamic_paths=2048)", TypeTag::JSON, false),
            (
                "Nullable(JSON(max_dynamic_paths=2048))",
                TypeTag::JSON,
                true,
            ),
            (
                "JSON(max_dynamic_types=8, SKIP `x.y`)",
                TypeTag::JSON,
                false,
            ),
            ("Object('json')", TypeTag::JSON, false),
            ("JSON(`a.b` UInt32)", TypeTag::JSON, false),
            ("JSON(unknown_param=1)", TypeTag::Unknown, false),
            ("JSON(max_dynamic_paths=x)", TypeTag::Unknown, false),
        ] {
            let t = ParsedType::parse(raw);
            assert_eq!(t.tag, tag, "{raw}");
            assert_eq!(t.nullable, nullable, "{raw}");
            assert_eq!(t.raw, raw, "{raw}");
        }
    }

    /// The Native block header is written from `raw`, so the parser must hand
    /// back exactly what the server said, wrappers and spacing included.
    #[test]
    fn parse_preserves_the_raw_type_string() {
        for raw in [
            "LowCardinality(Nullable(String))",
            "DateTime64(6, 'Australia/Sydney')",
            "Map(String, Array(UInt64))",
            "Enum8('a' = 1, 'b' = 2)",
            "SomeTypeFromTheFuture(1, 2)",
        ] {
            assert_eq!(ParsedType::parse(raw).raw, raw);
            assert_eq!(ParsedType::parse(raw).to_string(), raw);
        }
        // `parse` trims, so `raw` is the trimmed form and not the input.
        assert_eq!(ParsedType::parse("  UInt8  ").raw, "UInt8");
    }

    #[test]
    fn parse_nested_array() {
        let t = ParsedType::parse("Array(Array(String))");
        assert_eq!(t.base, "Array");
        let inner = t.array_element.as_ref().unwrap();
        assert_eq!(inner.base, "Array");
        let innermost = inner.array_element.as_ref().unwrap();
        assert_eq!(innermost.base, "String");
    }

    #[test]
    fn parse_fixedstring_with_length() {
        let t = ParsedType::parse("FixedString(16)");
        assert_eq!(t.base, "FixedString");
        assert!(t.is_string());
        assert_eq!(t.category(), "String");
    }
}
