// Project:   dfe-clickhouse
// File:      src/dynamic/shape.rs
// Purpose:   The forms a source value takes to land in a ClickHouse column
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The value rules both insert formats share.
//!
//! The RowBinary encoder applies them as it writes each column. A caller that
//! serialises the row map itself, as a JSONEachRow body does, applies them to
//! the map first with [`shape_json_value`].

use std::borrow::Cow;

use serde_json::{Map, Value};

/// The key a root-level array is stored under in a JSON column.
///
/// `list` is the key DFE's stored data and queries already read (ECS `tags`
/// lands as `_tags.list`), so data written before and after reads one way.
pub const JSON_ARRAY_WRAPPER_KEY: &str = "list";

/// The key a root-level scalar, or text that is not JSON, is stored under in a
/// JSON column.
pub const JSON_SCALAR_WRAPPER_KEY: &str = "value";

/// What a value must become for a JSON column, which takes only an object at
/// its root and refuses anything else with code 117.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonShape {
    /// An object already, stored as it stands.
    AsIs,
    /// Missing or empty, stored as `{}` because a JSON column is not Nullable.
    Empty,
    /// Array text, wrapped without a re-parse.
    WrapText(&'static str),
    /// Not an object, wrapped as the value it is.
    WrapValue(&'static str),
}

/// The one shaping rule for a value bound for a JSON column.
///
/// A `Value::String` there carries JSON text, so the text decides once it
/// parses. Text that does not parse is stored as the string it is, because
/// writing `{not json` verbatim is the code 117 this rule exists to stop.
fn json_shape(value: &Value) -> JsonShape {
    match value {
        Value::Null => JsonShape::Empty,
        Value::Object(_) => JsonShape::AsIs,
        Value::Array(_) => JsonShape::WrapValue(JSON_ARRAY_WRAPPER_KEY),
        Value::Bool(_) | Value::Number(_) => JsonShape::WrapValue(JSON_SCALAR_WRAPPER_KEY),
        Value::String(s) => match s.trim_start().as_bytes().first() {
            None => JsonShape::Empty,
            Some(b'{') if is_whole_json(s) => JsonShape::AsIs,
            Some(b'[') if is_whole_json(s) => JsonShape::WrapText(JSON_ARRAY_WRAPPER_KEY),
            _ => JsonShape::WrapValue(JSON_SCALAR_WRAPPER_KEY),
        },
    }
}

/// The deepest JSON text taken as a document: serde_json's own recursion limit
/// for a `Value`, so text the shaped-value path cannot build is kept as a
/// string on both paths.
const MAX_JSON_DEPTH: usize = 128;

/// Whether `text` is one complete JSON value, no deeper than
/// [`MAX_JSON_DEPTH`], and nothing after it. `IgnoredAny` checks the syntax
/// without building a document.
fn is_whole_json(text: &str) -> bool {
    depth_within(text.as_bytes(), MAX_JSON_DEPTH)
        && serde_json::from_str::<serde::de::IgnoredAny>(text).is_ok()
}

/// Whether no bracket in `text` opens past `limit` levels, brackets inside
/// strings not counted.
fn depth_within(text: &[u8], limit: usize) -> bool {
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    for &b in text {
        match (in_string, escaped, b) {
            (true, true, _) => escaped = false,
            (true, false, b'\\') => escaped = true,
            (true, false, b'"') | (false, _, b'"') => in_string = !in_string,
            (false, _, b'[' | b'{') => {
                depth += 1;
                if depth > limit {
                    return false;
                }
            }
            (false, _, b']' | b'}') => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

/// The JSON text a JSON column takes for `value`. Object text goes through
/// verbatim and array text is wrapped without a re-parse.
pub(crate) fn json_column_text(value: &Value) -> Cow<'_, [u8]> {
    match (json_shape(value), value) {
        (JsonShape::Empty, _) => Cow::Borrowed(b"{}"),
        (JsonShape::AsIs, Value::String(s)) => Cow::Borrowed(s.as_bytes()),
        (JsonShape::AsIs, _) => Cow::Owned(value.to_string().into_bytes()),
        (JsonShape::WrapText(key), Value::String(s)) => Cow::Owned(wrap_json_text(key, s)),
        (JsonShape::WrapText(key) | JsonShape::WrapValue(key), _) => {
            Cow::Owned(wrap_json_text(key, &value.to_string()))
        }
    }
}

/// Apply the JSON column rule to a value in place, for a caller that
/// serialises the row map itself.
///
/// A JSONEachRow body has no read-JSON-as-string setting, so JSON text is
/// parsed back to structure here, and both formats store the same document.
pub fn shape_json_value(value: &mut Value) {
    let shaped = match (json_shape(value), value.take()) {
        (JsonShape::AsIs, Value::String(s)) => json_text_to_value(s),
        (JsonShape::AsIs, taken) => taken,
        (JsonShape::Empty, _) => Value::Object(Map::new()),
        (JsonShape::WrapText(key), Value::String(s)) => wrap_value(key, json_text_to_value(s)),
        (JsonShape::WrapText(key) | JsonShape::WrapValue(key), taken) => wrap_value(key, taken),
    };
    *value = shaped;
}

/// Parse text [`json_shape`] has already accepted. The fallback keeps the
/// string should the two parses ever disagree.
fn json_text_to_value(text: String) -> Value {
    serde_json::from_str(&text).unwrap_or(Value::String(text))
}

/// `{"<key>": <inner>}`.
fn wrap_value(key: &str, inner: Value) -> Value {
    let mut wrapped = Map::with_capacity(1);
    wrapped.insert(key.to_string(), inner);
    Value::Object(wrapped)
}

/// `{"<key>": <json>}` built from JSON text, with no re-parse.
fn wrap_json_text(key: &str, json: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(json.len() + key.len() + 6);
    out.extend_from_slice(b"{\"");
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(b"\":");
    out.extend_from_slice(json.as_bytes());
    out.push(b'}');
    out
}

/// Read a geo point as `(longitude, latitude)` from any form Elasticsearch
/// takes for a `geo_point`: `{"lat": n, "lon": n}`, a GeoJSON point,
/// `"lat,lon"`, `[lon, lat]`, or WKT `POINT (lon lat)`. A third coordinate is
/// read and ignored, as Elasticsearch ignores it.
///
/// The messages name neither the value nor its coordinates, which are
/// customer data.
pub(crate) fn geo_point_lon_lat(value: &Value) -> Result<(f64, f64), &'static str> {
    let (lon, lat) = match value {
        Value::Object(fields) => point_from_object(fields)?,
        Value::Array(coordinates) => point_from_coordinates(coordinates)?,
        Value::String(text) => point_from_text(text)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {
            return Err("geo_point must be an object, a string or an array");
        }
    };
    if !(-90.0..=90.0).contains(&lat) {
        return Err("geo_point latitude is outside -90 to 90");
    }
    if !(-180.0..=180.0).contains(&lon) {
        return Err("geo_point longitude is outside -180 to 180");
    }
    Ok((lon, lat))
}

/// `{"lat": n, "lon": n}`, or a GeoJSON `{"type": "Point", "coordinates": [lon, lat]}`.
fn point_from_object(fields: &Map<String, Value>) -> Result<(f64, f64), &'static str> {
    if let (Some(lat), Some(lon)) = (fields.get("lat"), fields.get("lon")) {
        return Ok((coordinate(lon)?, coordinate(lat)?));
    }
    let geojson_point = fields
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|t| t.eq_ignore_ascii_case("point"));
    match fields.get("coordinates") {
        Some(Value::Array(coordinates)) if geojson_point => point_from_coordinates(coordinates),
        _ => Err("geo_point object needs lat and lon"),
    }
}

/// `[lon, lat]` or `[lon, lat, z]` -- GeoJSON order, longitude first.
fn point_from_coordinates(coordinates: &[Value]) -> Result<(f64, f64), &'static str> {
    match coordinates {
        [lon, lat] | [lon, lat, _] => Ok((coordinate(lon)?, coordinate(lat)?)),
        _ => Err("geo_point array must be [lon, lat]"),
    }
}

/// `"lat,lon"` (latitude first) or WKT `POINT (lon lat)`. A geohash has no
/// decoder here and is refused by name.
fn point_from_text(text: &str) -> Result<(f64, f64), &'static str> {
    let text = text.trim();
    if let Some(keyword) = text.get(..5)
        && keyword.eq_ignore_ascii_case("point")
    {
        return point_from_wkt(&text[5..]);
    }
    let mut parts = text.split(',');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(lat), Some(lon), _, None) => Ok((text_coordinate(lon)?, text_coordinate(lat)?)),
        (Some(word), None, _, _) if is_geohash(word) => Err("geohash geo_points are not supported"),
        _ => Err("geo_point string must be \"lat,lon\" or WKT POINT (lon lat)"),
    }
}

/// The body of a WKT point after its `POINT` keyword: an optional `Z`/`M`/`ZM`
/// tag, then `(lon lat)` with an optional third ordinate.
fn point_from_wkt(body: &str) -> Result<(f64, f64), &'static str> {
    const WKT: &str = "geo_point WKT must read POINT (lon lat)";
    let body = body.trim_start();
    let body = ["ZM", "Z", "M"]
        .iter()
        .find_map(|tag| {
            body.get(..tag.len())
                .filter(|head| head.eq_ignore_ascii_case(tag))
                .map(|_| body[tag.len()..].trim_start())
        })
        .unwrap_or(body);
    let inner = body
        .strip_prefix('(')
        .and_then(|rest| rest.trim_end().strip_suffix(')'))
        .ok_or(WKT)?;
    let mut ordinates = inner.split_ascii_whitespace();
    match (
        ordinates.next(),
        ordinates.next(),
        ordinates.next(),
        ordinates.next(),
    ) {
        (Some(lon), Some(lat), _, None) => Ok((text_coordinate(lon)?, text_coordinate(lat)?)),
        _ => Err(WKT),
    }
}

/// One coordinate from a JSON number or a numeric string, refused unless finite.
fn coordinate(value: &Value) -> Result<f64, &'static str> {
    match value {
        Value::Number(n) => n
            .as_f64()
            .filter(|f| f.is_finite())
            .ok_or("geo_point coordinate is not a finite number"),
        Value::String(s) => text_coordinate(s),
        _ => Err("geo_point coordinate is not a number"),
    }
}

/// One coordinate from text, refused unless it reads as a finite number.
fn text_coordinate(text: &str) -> Result<f64, &'static str> {
    text.trim()
        .parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
        .ok_or("geo_point coordinate is not a finite number")
}

/// Whether a word reads as a geohash: 1 to 12 characters of its base-32
/// alphabet, which leaves out `a`, `i`, `l` and `o`.
fn is_geohash(word: &str) -> bool {
    (1..=12).contains(&word.len()) && word.bytes().all(is_geohash_digit)
}

/// One character of the geohash base-32 alphabet, in either case.
fn is_geohash_digit(byte: u8) -> bool {
    matches!(
        byte.to_ascii_lowercase(),
        b'0'..=b'9' | b'b'..=b'h' | b'j' | b'k' | b'm' | b'n' | b'p'..=b'z'
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn text(value: &Value) -> String {
        String::from_utf8(json_column_text(value).into_owned()).unwrap()
    }

    /// `(value, the column text, the shaped value)`: the two forms agree.
    #[test]
    fn json_text_and_the_shaped_value_store_one_document() {
        for (value, column_text, shaped) in [
            (json!({"a": 1}), r#"{"a":1}"#, json!({"a": 1})),
            (json!(r#"{"a":1}"#), r#"{"a":1}"#, json!({"a": 1})),
            (
                json!(["a", "b"]),
                r#"{"list":["a","b"]}"#,
                json!({"list": ["a", "b"]}),
            ),
            (
                json!(r#"["a"]"#),
                r#"{"list":["a"]}"#,
                json!({"list": ["a"]}),
            ),
            (json!([]), r#"{"list":[]}"#, json!({"list": []})),
            (json!(42), r#"{"value":42}"#, json!({"value": 42})),
            (json!(true), r#"{"value":true}"#, json!({"value": true})),
            (
                json!("not json"),
                r#"{"value":"not json"}"#,
                json!({"value": "not json"}),
            ),
            (
                json!("{not json"),
                r#"{"value":"{not json"}"#,
                json!({"value": "{not json"}),
            ),
            (
                json!(r#"{"a":1} trailing"#),
                r#"{"value":"{\"a\":1} trailing"}"#,
                json!({"value": r#"{"a":1} trailing"#}),
            ),
            (json!(""), "{}", json!({})),
            (Value::Null, "{}", json!({})),
        ] {
            assert_eq!(text(&value), column_text, "{value}");
            let mut shaped_now = value.clone();
            shape_json_value(&mut shaped_now);
            assert_eq!(shaped_now, shaped, "{value}");
        }
    }

    #[test]
    fn every_geo_point_form_reads_as_lon_then_lat() {
        for value in [
            json!({"lat": 41.12, "lon": -71.34}),
            json!({"lat": "41.12", "lon": "-71.34"}),
            json!({"type": "Point", "coordinates": [-71.34, 41.12]}),
            json!([-71.34, 41.12]),
            json!([-71.34, 41.12, 9.0]),
            json!("41.12,-71.34"),
            json!(" 41.12 , -71.34 "),
            json!("41.12,-71.34,9"),
            json!("POINT (-71.34 41.12)"),
            json!("point z (-71.34 41.12 9)"),
        ] {
            assert_eq!(geo_point_lon_lat(&value), Ok((-71.34, 41.12)), "{value}");
        }
    }

    #[test]
    fn a_geo_point_out_of_range_or_malformed_is_refused() {
        for value in [
            json!({"lat": 100, "lon": 0}),
            json!({"lat": 0, "lon": 200}),
            json!([200, 0]),
            json!("91,0"),
            json!({"lat": 1}),
            json!({"type": "LineString", "coordinates": [0, 0]}),
            json!([1]),
            json!("drm3btev3e86"),
            json!("POINT (1)"),
            json!("1,2,3,4"),
            json!(42),
            json!(null),
            json!([f64::MAX, "NaN"]),
        ] {
            assert!(geo_point_lon_lat(&value).is_err(), "{value}");
        }
    }

    /// Text nested past serde_json's recursion limit is kept as a string, not
    /// recursed into.
    #[test]
    fn deeply_nested_text_is_kept_as_a_string() {
        let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
        let shaped = text(&Value::String(deep.clone()));
        assert!(shaped.starts_with(r#"{"value":"[[["#), "{shaped}");
        assert!(!is_whole_json(&deep));

        let quoted = format!(r#"{{"a":"{}"}}"#, "[".repeat(200));
        assert!(is_whole_json(&quoted), "brackets in a string are not depth");
    }
}
