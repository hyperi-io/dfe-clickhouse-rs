# dfe-clickhouse

Extensions for the official [`clickhouse`](https://crates.io/crates/clickhouse) Rust client, built for loading data in volume. It adds the native protocol over TCP and TLS, inserts whose columns are only known at runtime, and one client that runs over either transport.

It depends on the published `clickhouse` release and changes nothing in it, so the HTTP client you already use keeps working as it is.

## What it gives you

- The native protocol on port 9000, with TLS, a connection pool, endpoint failover and opt-in jittered retry for idempotent calls.
- Runtime-schema inserts: hand it `serde_json::Map` rows, with no Rust struct written per table. `Tuple`, `Variant` and `Dynamic` columns are refused by name.
- A per-table schema read from `system.columns` and cached, so rows encode straight to RowBinary or Native, never to JSONEachRow text for the server to parse.
- Explicit per-table batches: each insert targets one table and commits on `end()`, sent over TCP as Native blocks flushed at 1 MiB.
- Insert errors classified as transport, schema drift or bad rows. A transport failure can leave it unknown whether the rows landed, so retry only where a duplicate is harmless or deduplicated.
- An opt-in unified client (`unified` feature): one `UnifiedClient` that runs the same queries and inserts over HTTP or native TCP, the transport chosen by configuration at runtime.
- `JSON`, `Variant` and `Dynamic` columns read back as their document text, over native TCP and over HTTP through `UnifiedClient`. A `JSON` value ClickHouse spilled to shared data or `SharedVariant` reads whole by default, and with `with_json_as_string(false)` the query fails naming the column rather than leaving the value out. A `Dynamic` or `Variant` value the decoder cannot print the way the server does (a `SharedVariant` spill, a date or time) fails the query the same way, and the error says to cast the column to String in the query.
- `ServerException::parse` to read the ClickHouse error code and name out of an HTTP error.

## Example

Insert `serde_json::Map` rows over the native protocol:

```console
cargo add dfe-clickhouse --features unified,dynamic
cargo add serde_json
cargo add tokio --features macros,rt-multi-thread
```

```rust,no_run
use std::time::Duration;

use dfe_clickhouse::dynamic::DynamicSchemaCache;
use dfe_clickhouse::{TcpClient, UnifiedClient};
use serde_json::{Map, Value};

#[tokio::main]
async fn main() -> dfe_clickhouse::Result<()> {
    let client = UnifiedClient::Tcp(TcpClient::new("localhost:9000"));
    let schemas = DynamicSchemaCache::new(Duration::from_secs(300));

    let mut insert = client.dynamic_insert("default", "events", schemas).await?;

    let mut row = Map::new();
    row.insert("id".into(), Value::from(1));
    row.insert("message".into(), Value::from("hello"));
    insert.write_map(&row).await?;

    let rows = insert.end().await?;
    println!("inserted {rows} rows");
    Ok(())
}
```

Add `clickhouse` too, swap in `UnifiedClient::Http(clickhouse::Client::default().with_url("http://localhost:8123"))`, and the same rows go over HTTP.

Why native, from a 100k-row runtime-schema insert against ClickHouse 26.3 on 2026-11-01 ([method and full table](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/insert-formats.md#native-against-http-rowbinary)):

- About half the elapsed time: 799 and 920 ms against 1,548-2,281 ms over HTTP RowBinary.
- 16-36% less server CPU: 772 and 889 ms against 997-1,206 ms.
- 7-32% more client CPU, because the client builds the columns: 659 and 692 ms against 523-617 ms.
- No compression yet: the protocol supports LZ4 blocks, but this client sends them uncompressed, 32 MB on the wire where HTTP with LZ4 sends 6.4 MB.

## Features

| Feature | Adds |
|---|---|
| `ext` | Extension traits on `clickhouse::Client` and `ServerException` (default) |
| `lz4` | LZ4 compression on HTTP, from upstream (default) |
| `zstd` | Zstd compression on HTTP, from upstream |
| `tcp` | The native TCP client: pool, retry, wire codec |
| `tls` | rustls for the TCP client, with native or webpki roots (implies `tcp`) |
| `dynamic` | Runtime-schema inserts and the schema cache |
| `unified` | `UnifiedClient` over both transports (implies `tcp`) |
| `full` | All of the above |

API docs are at <https://docs.rs/dfe-clickhouse>, the source and the column type list at <https://github.com/hyperi-io/dfe-clickhouse-rs>. Licensed Apache-2.0: <https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/LICENSE>.
