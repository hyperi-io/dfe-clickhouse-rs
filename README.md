# dfe-clickhouse

<!-- BADGES:START -->
[![Build Status](https://github.com/hyperi-io/dfe-clickhouse-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-clickhouse-rs/actions)
[![Crates.io](https://img.shields.io/crates/v/dfe-clickhouse?logo=rust)](https://crates.io/crates/dfe-clickhouse)
[![docs.rs](https://img.shields.io/docsrs/dfe-clickhouse?logo=rust)](https://docs.rs/dfe-clickhouse)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/LICENSE)
<!-- BADGES:END -->

HyperI extensions for the official ClickHouse Rust client: TCP transport, dynamic inserts, unified client.

Not a fork. It depends on the published [`clickhouse`](https://crates.io/crates/clickhouse) release, with no `git`, `path` or `[patch]`, and changes nothing in it, so the HTTP client you use keeps working as it is. Same edition, MSRV and rustfmt settings, and upstream's lints plus stricter ones, so [ClickHouse/clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) can cherry-pick from here without a reformat.

## What it gives you

- The native protocol on port 9000, with TLS, a connection pool, endpoint failover and opt-in jittered retry for idempotent calls.
- Runtime-schema inserts: hand it `serde_json::Map` rows, with no Rust struct written per table.
- A per-table schema read from `system.columns` and cached, so rows encode straight to RowBinary or Native.
- An opt-in `UnifiedClient` that runs the same queries and inserts over HTTP or native TCP, the transport chosen by configuration at runtime.
- `JSON`, `Variant` and `Dynamic` columns read back as their document text, over native TCP and over HTTP through `UnifiedClient`. A `JSON` value ClickHouse spilled to shared data or `SharedVariant` reads whole by default, and with `with_json_as_string(false)` the query fails naming the column rather than leaving the value out. A `Dynamic` or `Variant` value the decoder cannot print the way the server does (a `SharedVariant` spill, a date or time) fails the query the same way, and the error says to cast the column to String in the query.
- `ServerException::parse` to read the ClickHouse error code and name out of an HTTP error.

```console
cargo add clickhouse dfe-clickhouse
```

## Extension traits on the HTTP client

`ping`, `kill_query`, `with_query_id`, `with_session_id`, `with_role`, and
`ServerException::parse` for a typed error with the server's error code.

```rust,no_run
use clickhouse::Client;
use dfe_clickhouse::ClientExt;

# async fn example() -> dfe_clickhouse::Result<()> {
let client = Client::default()
    .with_url("http://localhost:8123")
    .with_query_id("nightly-rollup");

client.ping().await?;
# Ok(())
# }
```

## Native TCP

```console
cargo add dfe-clickhouse --features tcp
```

Reads are column-typed, and `JSON`, `Variant` and `Dynamic` decode to their
per-row document text.

Why native, from a run on 2026-11-01 of a 100k-row runtime-schema insert against ClickHouse 26.3 ([insert-formats](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/insert-formats.md#native-against-http-rowbinary)):

- About half the elapsed time: 799 and 920 ms against 1,548-2,281 ms over HTTP RowBinary.
- 16-36% less server CPU: 772 and 889 ms against 997-1,206 ms.
- 7-32% more client CPU, because the client builds the columns: 659 and 692 ms against 523-617 ms.
- No compression: the protocol supports LZ4 blocks, but this client sends them uncompressed, 32 MB on the wire in that run where HTTP with LZ4 sent 6.4 MB. LZ4 on the native protocol is [#33](https://github.com/hyperi-io/dfe-clickhouse-rs/issues/33).

```rust,ignore
use dfe_clickhouse::TcpClient;

let client = TcpClient::new("localhost:9000").with_database("default");
let blocks = client
    .query("SELECT number FROM system.numbers LIMIT 10")
    .fetch_blocks()
    .await?;

for block in &blocks {
    for n in block.column_as::<u64>("number")? {
        println!("{n}");
    }
}
```

## Runtime-schema inserts

```console
cargo add dfe-clickhouse --features dynamic
```

Rows arrive as `serde_json::Map`, column types come from `system.columns`. Binary on the wire either way -- `FORMAT RowBinaryWithNamesAndTypes` over HTTP, `FORMAT Native` over TCP -- so the server parses no row text beyond the documents bound for `JSON` columns.

## Features

| Feature | What it adds |
|---|---|
| `ext` | Extension traits on `clickhouse::Client` (default) |
| `lz4` | LZ4 for the HTTP transport, forwarded to upstream (default) |
| `zstd` | Zstd for the HTTP transport, forwarded to upstream |
| `tcp` | Native TCP transport: connection actor, pool, retry, packet reader and writer |
| `tls` | rustls trust for the TCP transport (implies `tcp`) |
| `dynamic` | Runtime-schema inserts |
| `unified` | One client over both transports (implies `tcp`) |
| `full` | All of the above |

Default is `ext` plus `lz4`, neither of which adds a dependency beyond
upstream's. `tcp` brings deadpool, socket2 and backon.

## Documentation

| Read | When |
|---|---|
| [architecture](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/architecture.md) | Asking why the crate is built the way it is |
| [type-support](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/type-support.md) | Checking whether a column type reads and writes |
| [insert-formats](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/insert-formats.md) | Asking why there is no JSONEachRow option |
| [unified-client](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/unified-client.md) | Choosing a transport at runtime |
| [coverage](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/coverage.md) | Adding to a hot-path module |
| [CONTRIBUTING](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/CONTRIBUTING.md#context) | Changing the crate: where things live, the commands that prove a change, and what tends to bite |

## Issues

Open bugs: [this repo](https://github.com/hyperi-io/dfe-clickhouse-rs/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## Licence

Apache-2.0. See
[LICENSE](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/LICENSE) and
[NOTICE](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/NOTICE).
