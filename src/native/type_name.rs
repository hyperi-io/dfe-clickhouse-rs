// Project:   dfe-clickhouse
// File:      src/native/type_name.rs
// Purpose:   Strict well-formedness check for a server-announced type name
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Whether a type name a server announced inside a `Dynamic` or a JSON path is
//! spelled like a ClickHouse type, whether or not this crate can represent it.
//!
//! The grammar is the server's own spelling: an identifier, optionally followed
//! by bracketed arguments, each a type, a number, quoted text, an
//! `'text' = number` enum entry, a `key = value` setting, a `name Type` field,
//! or a JSON `SKIP` clause. Known constructors are held to their argument
//! counts, so `Map(UInt8)` is malformed while `Foo(UInt8)` is not.

use crate::native::columns::MAX_TYPE_PARSE_DEPTH;

/// One token of a type name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tok<'a> {
    /// A bare identifier: a type, a field name, a function or a setting.
    Word(&'a str),
    /// A backticked identifier, which can only name a field or a path.
    Name,
    Number,
    /// Single-quoted text.
    Text,
    Open,
    Close,
    Comma,
    Equals,
}

/// Whether `name` is a well-formed ClickHouse type expression.
pub(crate) fn is_well_formed(name: &str) -> bool {
    let Some(toks) = tokenize(name) else {
        return false;
    };
    let mut at = 0;
    ty(&toks, &mut at, 0) && at == toks.len()
}

fn tokenize(s: &str) -> Option<Vec<Tok<'_>>> {
    let bytes = s.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let start = i;
        i += 1;
        match b {
            b' ' => {}
            b'(' => toks.push(Tok::Open),
            b')' => toks.push(Tok::Close),
            b',' => toks.push(Tok::Comma),
            b'=' => toks.push(Tok::Equals),
            b'\'' | b'`' => {
                // Up to the matching quote, a backslash escaping the next byte.
                loop {
                    match bytes.get(i)? {
                        b'\\' => i += 2,
                        &c if c == b => break,
                        _ => i += 1,
                    }
                }
                i += 1;
                toks.push(if b == b'`' { Tok::Name } else { Tok::Text });
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                while bytes
                    .get(i)
                    .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'.')
                {
                    i += 1;
                }
                toks.push(Tok::Word(&s[start..i]));
            }
            b'-' | b'0'..=b'9' => {
                if b == b'-' && !bytes.get(i).is_some_and(u8::is_ascii_digit) {
                    return None;
                }
                while bytes
                    .get(i)
                    .is_some_and(|c| c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'E'))
                {
                    i += 1;
                }
                toks.push(Tok::Number);
            }
            _ => return None,
        }
    }
    Some(toks)
}

/// A type at `at`: an identifier and, optionally, its bracketed arguments,
/// `depth` brackets in. Past the parser's own nesting limit the name is
/// refused before descending, so a server-sent name cannot exhaust the stack.
fn ty(toks: &[Tok<'_>], at: &mut usize, depth: usize) -> bool {
    let Some(Tok::Word(name)) = toks.get(*at).copied() else {
        return false;
    };
    *at += 1;
    let count = if toks.get(*at) == Some(&Tok::Open) {
        if depth >= MAX_TYPE_PARSE_DEPTH {
            return false;
        }
        *at += 1;
        match args(toks, at, depth + 1) {
            Some(n) => Some(n),
            None => return false,
        }
    } else {
        None
    };
    arity_fits(name, count)
}

/// Arguments after an opening bracket, through the closing one.
fn args(toks: &[Tok<'_>], at: &mut usize, depth: usize) -> Option<usize> {
    if toks.get(*at) == Some(&Tok::Close) {
        *at += 1;
        return Some(0);
    }
    let mut count = 0;
    loop {
        if !arg(toks, at, depth) {
            return None;
        }
        count += 1;
        match toks.get(*at)? {
            Tok::Comma => *at += 1,
            Tok::Close => {
                *at += 1;
                return Some(count);
            }
            _ => return None,
        }
    }
}

fn arg(toks: &[Tok<'_>], at: &mut usize, depth: usize) -> bool {
    let next = |n: usize| toks.get(*at + n).copied();
    match (next(0), next(1), next(2)) {
        // `SKIP REGEXP 're'`, `key = value`, `'text' = number`.
        (Some(Tok::Word("SKIP")), Some(Tok::Word("REGEXP")), Some(Tok::Text))
        | (Some(Tok::Word(_)), Some(Tok::Equals), Some(Tok::Number | Tok::Text | Tok::Word(_)))
        | (Some(Tok::Text), Some(Tok::Equals), Some(Tok::Number)) => {
            *at += 3;
            true
        }
        (Some(Tok::Word("SKIP")), Some(Tok::Word(_) | Tok::Name), _) => {
            *at += 2;
            true
        }
        // A field name, then its type.
        (Some(Tok::Word(_) | Tok::Name), Some(Tok::Word(_)), _) => {
            *at += 1;
            ty(toks, at, depth)
        }
        (Some(Tok::Word(_)), _, _) => ty(toks, at, depth),
        (Some(Tok::Number | Tok::Text), _, _) => {
            *at += 1;
            true
        }
        _ => false,
    }
}

/// Whether `name` takes `count` arguments, `None` meaning no brackets.
fn arity_fits(name: &str, count: Option<usize>) -> bool {
    match name {
        "Array" | "Nullable" | "LowCardinality" | "FixedString" | "Object" | "Time64"
        | "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => count == Some(1),
        "Map" => count == Some(2),
        "Decimal" | "DateTime64" => matches!(count, Some(1 | 2)),
        "DateTime" | "Dynamic" => matches!(count, None | Some(1)),
        "Enum8" | "Enum16" | "Variant" | "Nested" | "AggregateFunction" => {
            count.is_some_and(|n| n >= 1)
        }
        "SimpleAggregateFunction" => count.is_some_and(|n| n >= 2),
        "Tuple" | "JSON" => true,
        "UInt8" | "UInt16" | "UInt32" | "UInt64" | "UInt128" | "UInt256" | "Int8" | "Int16"
        | "Int32" | "Int64" | "Int128" | "Int256" | "Float32" | "Float64" | "BFloat16"
        | "String" | "UUID" | "IPv4" | "IPv6" | "Bool" | "Date" | "Date32" | "Time" | "Point"
        | "Ring" | "LineString" | "MultiLineString" | "Polygon" | "MultiPolygon" | "Nothing"
        | "SharedVariant" => count.is_none(),
        // An unknown constructor: bare, or with at least one argument.
        _ => count != Some(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(name, well formed)`, the server's own spellings first.
    #[test]
    fn server_spellings_pass_and_malformed_names_do_not() {
        let mut wrong = Vec::new();
        for (name, ok) in [
            ("UInt8", true),
            ("DateTime64(3, 'UTC')", true),
            ("DateTime('Australia/Sydney')", true),
            ("Enum8('a' = 1, 'b' = -2)", true),
            (r"Enum16('it\'s' = 1)", true),
            ("Nullable(Decimal(9, 2))", true),
            ("Decimal(76, 20)", true),
            ("LowCardinality(Nullable(String))", true),
            ("Map(LowCardinality(String), UInt64)", true),
            (
                "Array(JSON(max_dynamic_types=16, max_dynamic_paths=256))",
                true,
            ),
            (
                "JSON(max_dynamic_types=8, max_dynamic_paths=16, SKIP `x.y`, SKIP REGEXP 'z.*')",
                true,
            ),
            ("JSON(`a.b` UInt32, max_dynamic_paths=2048)", true),
            ("JSON", true),
            ("Tuple(a UInt8, `b c` String)", true),
            ("Tuple(UInt8, String)", true),
            ("Tuple()", true),
            ("SimpleAggregateFunction(sum, UInt64)", true),
            ("AggregateFunction(quantiles(0.5, 0.9), UInt64)", true),
            ("Object('json')", true),
            ("FixedString(16)", true),
            ("Dynamic(max_types=8)", true),
            ("Variant(String, UInt64)", true),
            ("IntervalSecond", true),
            ("Nested(a UInt8, b Ring)", true),
            ("UInt8;garbage", false),
            ("Array(,)", false),
            ("Map(UInt8)", false),
            ("Array()", false),
            ("Array(Nullable)", false),
            ("UInt8()", false),
            ("Tuple(UInt8,)", false),
            ("Tuple(x UInt8 y)", false),
            ("Array(UInt8) UInt8", false),
            ("Enum8('a' = )", false),
            ("Decimal(1, 2, 3)", false),
            ("Foo(=)", false),
            ("Foo()", false),
            ("Foo('a)", false),
            ("Tuple(x UInt8", false),
            ("Tuple(x))", false),
            ("9Int", false),
            ("", false),
            ("Int\u{1}", false),
            ("-", false),
        ] {
            if is_well_formed(name) != ok {
                wrong.push(format!("{name:?} should be {ok}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}
