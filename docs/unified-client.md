# Unified client: one entry point over two transports

Why this design exists, for anyone weighing it for upstream. The code is in
`src/unified.rs`; this is the argument, not the API reference.

## The problem

`clickhouse::Client` speaks HTTP and nothing else. A native-TCP client is a
separate type with no shared interface, so a consumer that picks its transport
from config -- some deployments expose port 9000 and no HTTP at all -- has to
branch at every call site or wrap both itself.

## The shape

Both backends stay untouched. `UnifiedClient` is a thin dispatch enum over
them:

```rust
pub enum UnifiedClient {
    Http(clickhouse::Client),
    Tcp(TcpClient),
}
```

Three client types coexist, and that is deliberate:

- `clickhouse::Client` for HTTP-specific work such as `JSONEachRow`, which the
  native protocol cannot accept.
- `TcpClient` for direct native access.
- `UnifiedClient` as the entry point that dispatches at runtime.

It exposes the common subset -- `execute`, `ping`, `fetch_columns`,
`dynamic_insert` -- and nothing else. HTTP-only methods stay reachable through
`as_http() -> Option<&clickhouse::Client>`, so a caller that needs
`insert_formatted_with` asks for the HTTP arm by name and gets a clear `None`
on native rather than a confusing runtime error.

## Adding a transport

The enum is the extension point. A new variant needs a dispatch arm in each
method and a constructor; existing transports and every consumer are
unaffected. Nothing in the codec knows which transport carried its bytes.

## Reads over both transports

`fetch_columns` returns column-wise `Columns` on either arm. The HTTP path asks
the server for the TCP wire shape by sending `client_protocol_version`, then
decodes it with this crate's own Native codec rather than upstream's reader --
which refuses `JSON`, `Variant` and `Dynamic` outright. One codec serves both
transports, which is why a `JSON` column reads identically over each.

## Dynamic inserts over both transports

`dynamic_insert` resolves the schema through whichever transport is configured
and returns a `DynamicInsert` bound to it. The TCP arm resolves the schema
eagerly, because `DynamicInsert::tcp` needs it up front; the HTTP arm defers.
Encoding is RowBinaryWithDefaults for HTTP and Native columnar for TCP, sharing the
encoder.

A caller that holds its own client reads a schema with `fetch_dynamic_schema`
(HTTP) or `fetch_dynamic_schema_tcp`, each under an optional bound, and opens
the insert with `DynamicInsert::http` or `DynamicInsert::tcp`.
`with_timeouts(InsertTimeouts::new(send, end))` bounds the schema read, each
send and the server's final answer. A step past its bound fails retriable,
and `DynamicError::is_timed_out` says so. `with_schema_refresh` keeps a cached
schema through a mismatch until its TTL, for a caller that refreshes it
itself. `schema()` hands back the shared schema the rows were encoded against,
to compare with a fresh read (`DynamicSchema::same_columns`) once a rejection
comes back.

`shape_for_type` applies the encoder's value rules to a row map for a
JSONEachRow body, for the columns `needs_shaping` names, so both formats store
the same value.
