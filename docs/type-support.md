# Column type support

What the Native codec reads and writes. Every type listed as supported is read back from a real ClickHouse in `tests/wire_docker.rs`, on both transports, with the server building the values. The exceptions are `Time` and `Time64`, which decode as signed seconds and signed ticks at the column's precision but have no row in that matrix.

## Scalars and strings

`UInt8` through `UInt256`, `Int8` through `Int256`, `Float32`, `Float64`,
`BFloat16`, `Decimal32/64/128/256`, `Date`, `Date32`, `DateTime`,
`DateTime64`, `Time`, `Time64`, `UUID`, `IPv4`, `IPv6`, `Enum8`, `Enum16`,
`Point`, `Bool`.

Enum ordinals are signed: `Enum8` decodes as `Int8` and `Enum16` as `Int16`,
so a negative ordinal reads back as itself rather than as its unsigned byte.

A runtime-schema insert takes a `Point` in any form Elasticsearch takes a `geo_point`: `[lon, lat]`, `{"lat", "lon"}`, a GeoJSON point, `"lat,lon"` or WKT `POINT (lon lat)`, with longitude as x. A latitude past 90 or a longitude past 180 is refused, and so is a geohash.

`String` (varuint length then bytes -- ClickHouse strings are bytes, not
guaranteed UTF-8) and `FixedString(N)` (N raw bytes on the wire).

## Composites

`Nullable(T)`, `LowCardinality(T)`, `Array(T)`, `Tuple(T1..Tn)`, `Map(K, V)`,
`SimpleAggregateFunction(f, T)`, nested arbitrarily.

Nested serialisation prefixes are written and read ahead of the enclosing
column's data, which is where `NativeWriter` puts them. Reading them inline
works until an enclosing type writes data of its own first, so
`Array(LowCardinality(String))` and friends need the phased form.

`Ring`, `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` read and insert as the arrays of `Point` they are on the wire.

Reads refuse `AggregateFunction`, a named `Tuple` such as `Tuple(a UInt8, b String)`, and the `Interval` types by name. They refuse a `Nested` column too, which the server keeps only under `flatten_nested = 0`. Under the default `flatten_nested = 1` it splits a `Nested` column into one `Array` per field, and those read. A runtime-schema insert refuses `Tuple`, `Variant` and `Dynamic`.

## Semi-structured

`Variant(T1..Tn)`, `Dynamic` and `JSON` all decode to their per-row document text. `Object('json')`, the type `JSON` replaced, reads as a plain String from a server old enough to still have it.

A `JSON` value travels as text in both directions. An insert sends it as a string, declared `String` over TCP and read as one over HTTP under `input_format_binary_read_json_as_string`, and the server converts. Every query asks for `output_format_native_write_json_as_string=1` by default, so it comes back as text. That holds at any depth, so `Nullable(JSON)`, `Array(JSON)` and `Map(K, JSON)` insert too.

`JSON(max_dynamic_paths=N, max_dynamic_types=N, SKIP path, SKIP REGEXP 're')`
and typed paths such as ``JSON(`a.b` UInt32)`` read and insert like plain
`JSON`, because text goes in and comes out the same.

A `Variant` or `Dynamic` value prints as the server's `toJSONString` prints
it, for `Bool`, integers up to 64 bits, `Float32`, `Float64`, `String` and
arrays of them. Any other type (a date or time, a decimal, a UUID), and a
`Dynamic` value stored past `max_types` (`SharedVariant`), fails the query with
`Error::Unsupported` naming the column, over native TCP and over HTTP through
`UnifiedClient`. The error says to read the column cast to String in the query, as `CAST(d AS String)`. A nested `Dynamic` in the flattened serialisation is refused too. `dynamic_and_variant_values_print_like_the_server_or_refuse` pins `Bool`, `Int64`, `Float32` and `String` values and the date and time refusals.

Two limits worth knowing before sending data:

- A JSON column takes only an object at its root, so the dynamic encoder wraps
  anything else: an array as `{"list": [...]}`, and a number, a bool or text
  that is not JSON as `{"value": ...}`. JSON text in a string is stored as the
  document it holds. An array nested inside an object is untouched.
- An integer too wide for any ClickHouse integer type is stored as a `Float64`
  and silently rounded -- about 17 significant digits survive, with no error.
  Send such fields as strings.

## Path-based JSON reads

`with_json_as_string(false)` turns `output_format_native_write_json_as_string`
off, and the server sends a path-based serialisation instead (V2, or flattened
when asked). The decoder rebuilds each document byte for byte as the server
prints it: `a.b` as `{"a":{"b":...}}`, siblings in full-path order, `Bool` as
`true` or `false`, a float in plain decimal for exponents -6 to 20 and as
`1e21` outside them, strings with the server's escapes (`\/`, `\u2028`).
`path_based_json_reads_byte_for_byte_like_the_server` pins each value type on
both serialisations.

Where it would print something the server would not, the read fails with
`Error::Unsupported` naming the column and pointing at
`with_json_as_string(true)`, the default, which reads all of these whole:

- Typed paths, whose sub-columns it does not decode.
- A date or time, which the server infers from a string and prints in its time
  zone.
- An array of objects, `Array(JSON)` inside a path, whose serialisation prefix
  it does not walk.
- A value at a path and at a path under it in one row (`a` and `a.b`), which
  the server prints as a duplicate key.
- A key holding a dot, escaped as `%2E` in the path.
- A path past `max_dynamic_paths` (shared data) or a type past
  `max_dynamic_types` (`SharedVariant`), stored in a binary encoding it does
  not read (`spilled_values_are_refused_never_dropped`).

## Sparse columns

A column whose defaults pass `ratio_of_defaults_for_sparse_serialization` arrives as an offset list plus only the non-default values, and the decoder scatters it back to the block's full length. `tests/wire_docker.rs` reads a sparse-stored column back raw over TCP, and its `sum` over both transports. Passing does not prove the sparse form reached the wire, because the server may materialise the column before sending it.

The server stores scalars that way, and the elements of a `Tuple`, but not a `Nullable`, `LowCardinality` or `Array` column. Over native TCP a `Tuple` whose elements arrive sparse stalls the read, a defect being fixed in [#46](https://github.com/hyperi-io/dfe-clickhouse-rs/issues/46): the query fails with `Error::TimedOut` only where `with_read_timeout` is set. HTTP reads it. A composite marked sparse as a whole is refused, naming its type.

The header that selects it is TWO bytes, not one, and the second is easy to miss: `NativeWriter` writes the `has_custom_serialization` bool, then, only when it is set, a byte naming the kind stack (0 Default, 1 Sparse, 2 Detached, 3 Detached-over-Sparse, 4 Replicated, 5 a combination with its own varuint count). Reading the flag alone and treating 1 as "sparse" leaves the kind byte on the wire. That one byte of drift corrupts every following column and surfaces as an unknown packet id nowhere near the cause. Only Default and Sparse are decoded, and the rest are refused by name.
