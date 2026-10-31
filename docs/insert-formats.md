# Insert formats: binary by default, JSONEachRow never

Why every insert path in this crate puts binary on the wire. The code is in
`src/dynamic/insert.rs` and `src/native/encode.rs`; this is the argument.

## JSONEachRow is the easy option and the expensive one

It works, it is trivial to produce, and it hands the server a pile of work the
client had already done. For every row the server parses JSON text, infers
types and validates structure -- to arrive at a layout the client knew before
it sent anything.

ClickHouse's own format benchmark puts JSONEachRow at roughly 17% server CPU
overhead against about 5.5% for Native: three times the parse cost, and it
scales with ingest volume rather than flattening out. On a pipeline moving
10k-20k row batches continuously, that is the difference between one server and
two.

## What this crate sends

| Path | Format | Transport |
|---|---|---|
| `UnifiedClient::dynamic_insert` -> HTTP | RowBinary | HTTP |
| `UnifiedClient::dynamic_insert` -> TCP | Native, columnar | native TCP |
| `TcpClient::insert_native` | Native, columnar | native TCP |
| `clickhouse::Client::insert::<T>` | RowBinary | HTTP (upstream, unchanged) |

There is no JSONEachRow path. `DynamicInsert` takes the same
`Map<String, Value>` rows a JSON encoder would, and encodes them to RowBinary
or transposes them into Native columnar blocks -- the caller's code looks the
same either way, and the server stops parsing text.

Over native TCP the choice does not exist at all: the protocol accepts Native
format and nothing else, which is a server constraint rather than a gap here.

## What the crate does that the server cannot

Encoding client-side is also the only place some conversions can happen. The
coercions in `src/dynamic/encode.rs` -- epoch milliseconds against a
`DateTime64` of a declared precision, a string into an `Enum8` label, an
integer width checked rather than silently truncated -- have no JSONEachRow
equivalent, because by the time the server sees the text the type information
that would resolve them is gone.

## Native against HTTP RowBinary

Measured 2026-11-01 against ClickHouse 26.3.21.7. The same rows go through `UnifiedClient::dynamic_insert` three ways: HTTP RowBinary with LZ4, HTTP RowBinary uncompressed, and native TCP. Native TCP has no compressed option, because this crate negotiates no block codec on the native protocol.

At 100k rows native finished in about half the time and used 16-36% less server CPU. It cost 7-32% more client CPU, because the client builds the columns. At 10k rows the server CPU gap sits inside the noise. Uncompressed, both send about 32 MB per 100k rows, and HTTP with LZ4 sends 6.4 MB.

The setup:

- Server: `clickhouse/clickhouse-server:26.3.21.7` in Docker, default settings, on a 32-core x86_64 VM that was running other builds at the same time.
- Client: `examples/insert_bench.rs`, release build, on the same VM over loopback.
- Table: the `CREATE` in that file. `DateTime64(3, 'UTC')`, `LowCardinality(String)`, `String`, `UInt16`, `UInt64`, `Int32`, `Nullable(String)`, `Map(LowCardinality(String), String)`, `Array(String)` and `JSON(max_dynamic_paths=2048)`, on `MergeTree`. Rows are a web-access event, about 320 bytes each uncompressed.

How it was measured:

- Each cell is one warm-up batch, discarded, then 7 measured batches. The table shows the median. The whole run was done twice, and both passes are shown so the spread is visible.
- `async_insert=0`, so each insert's own `system.query_log` row carries its parse and write cost.
- Client CPU is every thread's run time from `/proc/self/task/*/schedstat`, taken across the insert. Row generation is outside it.
- Server CPU is `UserTimeMicroseconds` plus `SystemTimeMicroseconds` from the insert's `QueryFinish` row. `OSCPUVirtualTimeMicroseconds` matched that sum in every median, so it has no column of its own.
- Bytes are `NetworkReceiveBytes` from the same row.

```text
cargo run --release --features full --example insert_bench
```

Per batch, pass 1 / pass 2:

| Path | Rows | Client CPU ms | Server CPU ms | Bytes received | Elapsed ms |
|---|---|---|---|---|---|
| HTTP RowBinary, LZ4 | 10k | 46.1 / 51.9 | 93.8 / 92.4 | 641,347 | 138.5 / 144.0 |
| HTTP RowBinary, uncompressed | 10k | 58.5 / 52.8 | 100.2 / 113.1 | 3,174,140 / 3,175,338 | 166.3 / 165.6 |
| Native TCP | 10k | 62.7 / 81.0 | 75.8 / 95.0 | 3,191,159 | 104.4 / 125.4 |
| HTTP RowBinary, LZ4 | 100k | 616.5 / 568.2 | 1,205.5 / 1,061.9 | 6,411,370 | 2,281.2 / 1,626.8 |
| HTTP RowBinary, uncompressed | 100k | 553.6 / 522.9 | 997.4 / 1,111.3 | 32,592,605 / 32,599,595 | 1,547.6 / 1,644.5 |
| Native TCP | 100k | 658.6 / 692.0 | 772.1 / 888.7 | 32,111,718 | 798.7 / 920.2 |

The single runs spread wide on the shared VM: 1,720-3,305 ms elapsed for the LZ4 cell at 100k in pass 1. Treat a gap smaller than that spread as noise.

## Sources

- [ClickHouse input format matchup](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient)
- [Altinity ingestion performance](https://kb.altinity.com/altinity-kb-schema-design/ingestion-performance-and-formats/)
