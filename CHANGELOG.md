# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

## [0.1.6](https://github.com/hyperi-io/dfe-clickhouse-rs/compare/v0.1.5...v0.1.6) (2026-11-01)

### Bug Fixes

* manual remediation of LLM auto fixes -- parse parameterised JSON columns ([#35](https://github.com/hyperi-io/dfe-clickhouse-rs/issues/35)) ([c8c3f2b](https://github.com/hyperi-io/dfe-clickhouse-rs/commit/c8c3f2b83a0552b2d99aed702f945e76bcc9fe01))

## [0.1.5](https://github.com/hyperi-io/dfe-clickhouse-rs/compare/v0.1.4...v0.1.5) (2026-10-31)

### Bug Fixes

* close the pool, timeout and error-classification holes in the client ([#29](https://github.com/hyperi-io/dfe-clickhouse-rs/issues/29)) ([6c65c34](https://github.com/hyperi-io/dfe-clickhouse-rs/commit/6c65c34f3048bf4cea7889128cdcf07e1f43eb6f))

## [0.1.4](https://github.com/hyperi-io/dfe-clickhouse-rs/compare/v0.1.3...v0.1.4) (2026-10-29)

### Bug Fixes

* **deps:** update Rust to v1.99.1 ([#27](https://github.com/hyperi-io/dfe-clickhouse-rs/issues/27)) ([3f6e2cf](https://github.com/hyperi-io/dfe-clickhouse-rs/commit/3f6e2cfc13aec3522181e0864c501fc4f612310d))
* rename the crate to dfe-clickhouse ([#28](https://github.com/hyperi-io/dfe-clickhouse-rs/issues/28)) ([04b68a9](https://github.com/hyperi-io/dfe-clickhouse-rs/commit/04b68a99ad5df61dba9b3100a60b9d0d81e5229f))

## [0.1.3](https://github.com/hyperi-io/clickhouse-dfe/compare/v0.1.2...v0.1.3) (2026-10-25)

### Bug Fixes

* allow unused_async_trait_impl on ensure_active without tcp ([#24](https://github.com/hyperi-io/clickhouse-dfe/issues/24)) ([36aa13e](https://github.com/hyperi-io/clickhouse-dfe/commit/36aa13e2e7cbf32ac9a58cd4e24df1b2e1b52aae))
* **deps:** take tokio 1.54.1 and webpki-roots 1.0.11 ([#26](https://github.com/hyperi-io/clickhouse-dfe/issues/26)) ([3e3ee9c](https://github.com/hyperi-io/clickhouse-dfe/commit/3e3ee9c455ab47622f660973ee3081a2f734a166))
* **deps:** update Rust crate rustls to >=0.23.46, <0.24 ([#22](https://github.com/hyperi-io/clickhouse-dfe/issues/22)) ([dca99d8](https://github.com/hyperi-io/clickhouse-dfe/commit/dca99d8b8f4e834683114732c4252b495cf54ba6))
* **deps:** update Rust to v1.99.0 ([#23](https://github.com/hyperi-io/clickhouse-dfe/issues/23)) ([29935de](https://github.com/hyperi-io/clickhouse-dfe/commit/29935de5c4c821fc2c1a2cafb3bf408926842b3a))
* move -D warnings to per-target rustflags ([#21](https://github.com/hyperi-io/clickhouse-dfe/issues/21)) ([8e3cf0e](https://github.com/hyperi-io/clickhouse-dfe/commit/8e3cf0e8574cbcb6aea9ffa3653b9a8a6caffa66)), closes [#20](https://github.com/hyperi-io/clickhouse-dfe/issues/20)

# Changelog

Released sections are rendered by CI at the end of a release and committed
back. `[Unreleased]` is maintained by hand until then. Release notes also
appear on the GitHub Releases page, one per tag.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- TCP transport (connection actor, deadpool pool, retry, TLS trust) and the Native-format wire codec
- Column-typed reads over TCP: `TcpClient::query(sql).fetch_blocks()` plus `DecodedBlock::column_as` over the `FromColumn` trait
- Dynamic layer: runtime-schema inserts from `serde_json::Map` rows over HTTP (`FORMAT RowBinary`) and TCP (`FORMAT Native`)
- JSON columns travel as `String` in both directions, with `TcpClient::with_json_as_string` to opt out; opting out reads the V2 path-based serialisation instead, so both settings work
- The advertised protocol revision is 54473, so `JSON` and `Dynamic` arrive in their V2 serialisation rather than V1, and the server sends sparse columns at all
- Sparse columns read: a column whose defaults pass `ratio_of_defaults_for_sparse_serialization` arrives as offsets plus non-default values and is scattered back to the block's full length (scalars only, which is all the server sparse-serialises)
- `UnifiedClient::fetch_columns` decodes with this crate's Native codec on both transports, so `JSON`, `Variant` and `Dynamic` read over HTTP as well as TCP
- Unified client over both transports: `UnifiedClient`, `Columns::get::<T>` by column name, and `dynamic_insert` resolving its schema over whichever transport is configured
- `ClientExt` on `clickhouse::Client`: ping, kill query, query id, session id, role, plus `ServerException` parsed from a server error body with a retriable-code test
- Server-side query parameters over TCP: `TcpQuery::param` emits the Query packet's parameters section, so `{name:Type}` binds instead of being interpolated
- `column_as` reaches every scalar the decoder produces, and `Option<T>` works wherever `T` does
- A root-level JSON array written to a `JSON` column is wrapped as `{"_values": [...]}` (`dynamic::JSON_ARRAY_WRAPPER_KEY`), because ClickHouse's JSON parser accepts only an object at the root and rejects an array with code 117. An array nested inside an object is untouched

### Fixed

- The per-column custom-serialization header is two bytes, not one: a `has_custom_serialization` bool followed by a kind-stack byte. Reading only the first left the kind byte on the wire, and one byte of drift corrupted every following column, surfacing as an unknown packet id nowhere near the cause
- The Query packet carries the extra-roles string that every server from revision 54472 reads from all clients, not just interserver ones. Without it the server waits for a length-prefixed string that never arrives and the connection blocks
- `Manager::create` took the endpoint count modulo before checking it, so a pool built directly with no endpoints panicked inside an acquire
- Wire-declared lengths are read in bounded chunks rather than allocated up front, so a corrupt length costs one chunk instead of its full claim
- Four Native serialisation formats corrected against ClickHouse 26.3: the JSON v3 per-path version, JSON v2 shared data, the LowCardinality index-width threshold, and the Variant discriminator mode
- An uncapped column count could abort the process, a fixed-offset truncation of a server exception could panic mid-character before authentication, and a nested-exception chain could exhaust the stack
- Recycled connections leaked their socket and reader task; a streaming SELECT released its pool slot before the stream drained
- Retry backoff is jittered, so connections that fail together no longer retry in lockstep
- The pool's recycle decision is one function, so the tests exercise the production rule rather than a copy of it
- A nested `LowCardinality` now reads and writes its serialisation prefix where the server puts it -- ahead of the enclosing column's data, not inline. `Array(LowCardinality(String))` and `Map(String, LowCardinality(Nullable(String)))` could not be read, and inserting either returned server code 117
- `Enum8` and `Enum16` decode as `DecodedColumn::Int8` / `Int16`, matching the signed ordinals ClickHouse defines. A top-level enum column previously decoded unsigned, so `Enum8('b' = -2)` read back as `254`, while the same value inside a `Variant`, `Dynamic` or `JSON` cell rendered correctly as `-2`. Read such a column with `column_as::<i8>` / `::<i16>`

### Removed

- The unused Native block-compression and block-info modules. `lz4` and `zstd` forward to upstream for the HTTP path; the TCP handshake has always negotiated no compression
