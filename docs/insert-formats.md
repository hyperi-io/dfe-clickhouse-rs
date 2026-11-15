# Insert formats: binary by default, JSONEachRow never

Why every insert path in this crate puts binary on the wire. The code is in `src/dynamic/insert.rs` and `src/native/encode.rs`. This page is the argument.

## JSONEachRow is the easy option and the expensive one

It works, it is trivial to produce, and it hands the server a pile of work the
client had already done. For every row the server parses JSON text, infers
types and validates structure -- to arrive at a layout the client knew before
it sent anything.

ClickHouse's own [format benchmark](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient) measured server CPU per LZ4-compressed insert at 17% for JSONEachRow against 5.5% for Native, about three times the cost. Altinity's [parse-only test](https://kb.altinity.com/altinity-kb-schema-design/ingestion-performance-and-formats/), 100 million rows into a `Null` table, took 30.2 s of user CPU for JSONEachRow, 4.2 s for RowBinary and 1.9 s for Native.

## What this crate sends

| Path | Format | Transport |
|---|---|---|
| `UnifiedClient::dynamic_insert` -> HTTP | RowBinaryWithNamesAndTypes, or RowBinaryWithDefaults for a row that supplies no column | HTTP |
| `UnifiedClient::dynamic_insert` -> TCP | Native, columnar | native TCP |
| `TcpClient::insert_native` | Native, columnar | native TCP |
| `clickhouse::Client::insert::<T>` | RowBinaryWithNamesAndTypes, or RowBinary with validation off | HTTP (upstream, unchanged) |

There is no JSONEachRow path. `DynamicInsert` takes the same `Map<String, Value>` rows a JSON encoder would, and encodes them to RowBinary or transposes them into Native columnar blocks. The caller's code looks the same either way, and the server parses no row text, only the documents bound for `JSON` columns.

Over native TCP the choice does not exist at all: the protocol accepts Native
format and nothing else, which is a server constraint rather than a gap here.

Both formats check the cached schema against the table. The HTTP header declares each column's type, and the server refuses a type the table no longer has with code 117 (`INCORRECT_DATA`). The insert sets `input_format_with_types_use_header=1` itself, so a client default or user profile cannot turn that check off. Over TCP the server answers the INSERT with its own types, and the insert refuses a difference before sending a row. Without either check a stale schema's bytes are read as the new type: "abc" sent as a String lands in a UInt32 column as 1667391747, and 300 sent as a UInt64 lands in a UInt8 column as 44. Both refusals are `DynamicError::SchemaMismatch`, which drops the cached schema.

Neither format has a per-row default. An INSERT names the required columns and the defaulted ones its rows supply, and the server fills the rest from their DEFAULT expressions. Rows that supply a different set go to another INSERT. Over HTTP up to four of those stay open at once, so a batch whose rows carry four sets or fewer is one INSERT per set, and a fifth set commits the oldest first. Over TCP a row that supplies a different set commits the rows so far and opens a new INSERT. A batch whose rows vary that way becomes several INSERTs, and an error late in it can follow rows already committed. Retry the whole batch. Where the client sets `insert_deduplication_token`, each INSERT gets that token plus the defaulted columns its rows supply, so the INSERTs do not suppress each other and a retry of the same rows in the same order lands once. A row that supplies no column at all, on a table whose every column has a default, goes over HTTP as `RowBinaryWithDefaults` with every column flagged, so no value crosses the wire.

Both transports send once 1 MiB of rows is buffered, so a batch never sits whole in memory.

## What the encoder does that JSONEachRow does not

The server parses JSONEachRow against the table's types, so it maps a string to an `Enum8` label itself. Two coercions in `src/dynamic/encode.rs` have no JSONEachRow equivalent. An epoch number is read by its magnitude as seconds, milliseconds, microseconds or nanoseconds, where the server takes it as ticks at the column's precision: `1700000000123` lands in a `DateTime64(6)` as 1970-01-20. And an integer is checked against its column's width, where the server wraps it: 300 lands in a `UInt8` as 44.

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
