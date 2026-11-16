# Architecture: why the crate is built this way

The cross-layer map. Per-layer reasoning lives in code comments at the point of use, and the sibling pages cover coverage, insert formats, type support and the unified client -- this page is the argument for the whole and the invariants that span more than one module.

## Why a separate crate, not a fork

Three gaps in the official client, each of them met in a deployment rather than theoretical.

1. It speaks HTTP and nothing else. Plenty of ClickHouse deployments expose port
   9000 and no HTTP interface at all, so those are unreachable.
2. Its Native reader rejects `JSON`, `Variant` and `Dynamic` by type name, and its RowBinary reader has no route for `Dynamic`.
3. Its inserts take typed Rust values: a derived row struct, or a Native block built column by column. A loader taking arbitrary JSON rows off a topic has the values but not their column types.

Closing them in a fork means carrying a merge against an actively developed client forever.

The crate depends on the published `clickhouse` release and adds layers beside it. Nothing is patched, no upstream source is copied in, and there is no `git =`, `path =` or `[patch.crates-io]` anywhere.

That choice is what keeps the code able to travel back. Same edition, same MSRV, same `rustfmt.toml`, and upstream's lints plus stricter ones, so `ClickHouse/clickhouse-rs` can cherry-pick a file from here without a reformat. The same goal puts design rationale in comments at the point of use rather than in this directory, so a cherry-picked file takes its reasoning with it.

## The layers

Every layer but the codec is a Cargo feature. `ext` adds no dependency beyond upstream's, and the native transport is a deliberate opt-in rather than something every consumer pays for.

Two entry points reach ClickHouse through two transports, and the TCP transport reads and writes through the Native codec.

```mermaid
flowchart LR
    accTitle: L3 Components -- dfe-clickhouse
    accDescr: The unified client and the dynamic insert use both transports, the upstream HTTP client and the TCP transport. Both transports talk to the ClickHouse server, and the TCP transport reads and writes through the Native codec.

    subgraph entry["Entry points"]
        unified[["UnifiedClient"]]
        dynamic[["Dynamic insert"]]
    end

    subgraph transports["Transports"]
        tcp["TCP transport"]
        http["clickhouse::Client"]
    end

    codec["Native codec"]
    server["ClickHouse server"]

    entry --> transports
    tcp --> codec
    tcp --> server
    http --> server

    classDef default fill:#F8FAFC,stroke:#64748B,stroke-width:1px,color:#0F172A
    classDef ext fill:#EEF0F2,stroke:#6B7280,stroke-width:1px,stroke-dasharray:4 3,color:#1F2937
    classDef group fill:none,stroke:#64748B,stroke-width:1px,stroke-dasharray:2 4
    class http,server ext
    class entry,transports group
```

*Figure 1. L3 Components -- dfe-clickhouse. Dashed boxes are external.* Up: top of dfe-clickhouse-rs. Down: [L4 Detail -- Reads over both transports](unified-client.md), [L4 Detail -- Dynamic insert formats](insert-formats.md).

The codec serves more than the TCP transport. `UnifiedClient` decodes an HTTP answer with it, and the dynamic insert builds its TCP blocks with it. The `ext` traits add methods to `clickhouse::Client`, and `tls` gives the TCP transport its rustls trust.

| Feature | Module | Owns |
|---|---|---|
| `ext` (default) | `src/ext.rs` | `ping`, `kill_query`, `with_query_id`, `with_session_id`, `with_role`, and `ServerException::parse` for a typed error with the server's error code |
| `lz4` (default) | -- | forwards upstream's block compression for HTTP. TCP negotiates `None` |
| `tcp` | `src/tcp/`, `src/worker.rs` | connect, handshake, connection actor, pool, retry, protocol, query, reader, writer |
| `tls` | `src/tls.rs` | rustls trust for the TCP transport |
| `dynamic` | `src/dynamic/` | the `system.columns` schema cache, the per-type encoder, `DynamicInsert` |
| `unified` | `src/unified.rs` | `UnifiedClient`, `Columns`, `Transport` |
| -- | `src/native/` | the Native wire codec, compiled unconditionally |

Adding to the default set later is non-breaking. Removing from it is not.

## One codec and one client over both transports

`src/native/` is the middle of everything, and it is the reason the crate hangs
together rather than being two clients in one package.

A read over TCP decodes Native blocks, the one format the native protocol offers. A read over HTTP sends `client_protocol_version`, so the server writes the revision of the Native format the TCP handshake negotiates, then decodes the answer with THIS crate's codec rather than upstream's reader. So a `JSON` column reads identically over either transport, and `Variant` and `Dynamic` take the same route -- one document of text per row.

Inserts are binary on both sides for the same reason: RowBinaryWithNamesAndTypes over HTTP, Native columnar over TCP, sharing the encoder. There is no JSONEachRow path anywhere, and over native TCP the choice does not exist, because the protocol accepts Native and nothing else. Encoding client-side does conversions the server's JSONEachRow parser does not, which [insert-formats](insert-formats.md#what-the-encoder-does-that-jsoneachrow-does-not) lists.

`src/native/` and `src/tcp/pool.rs` are marked HOT PATH in their module docs and held to a higher coverage target than the rest, because every row this crate reads or writes goes through the first and every TCP operation acquires from the second.

A dispatch enum, not a trait, puts one client over both. `UnifiedClient` is a thin enum over `clickhouse::Client` and `TcpClient`. Both
backends stay untouched, and three client types coexist on purpose: the upstream
client for HTTP-only work such as `JSONEachRow`, `TcpClient` for direct native
access, and `UnifiedClient` as the entry point that dispatches at runtime.

It dispatches the common subset. HTTP-only methods stay reachable through `as_http() -> Option<&clickhouse::Client>`, and native-only ones through `as_tcp()`, so a caller asks for an arm by name and gets a clear `None` from the other instead of a confusing runtime error. Another transport is a variant plus a dispatch arm in each method, and nothing in the codec knows which transport brought its bytes.

## Test tiers and invariants

Three test tiers test different things, and two of them run in CI.

| Tier | Needs | In CI | Shows |
|---|---|---|---|
| unit tests, `tests/smoke.rs`, `tests/tcp_public_surface.rs` | nothing -- they construct and configure offline | yes | branches, the public surface, and that the public types are `Send` and `Sync` |
| `tests/wire_docker.rs` | a container runtime, one pinned server per test | yes -- it fails under `$CI` without one and skips with a message elsewhere | the type matrix against a ClickHouse server, which builds every value, so a failure is this crate's decoder |
| `tests/json_tcp_live.rs`, `tests/tcp_params_live.rs`, `tests/unified_live.rs` | a ClickHouse cluster, configured through `CLICKHOUSE_DFE_ENV_FILE` | no -- every one is `#[ignore]` | cluster behaviour: `ON CLUSTER` DDL, Distributed reads, replica sync, transport parity |

The tiers are not interchangeable. A unit test can run a decoder with fixtures that share its misreading of the spec, and it takes a server writing the bytes to catch that ([coverage](coverage.md#what-the-numbers-do-not-tell-you)).

Six invariants span more than one module:

1. The wire format moves between server versions, so the Docker image tag is
   pinned and never `latest`.
2. Raising the advertised protocol revision is never a one-line change. Each
   revision gate adds fields that must be consumed -- in the handshake, and in
   the Progress and ProfileInfo packets -- or the connection stalls against a
   server.
3. No `unsafe`. `unsafe_code = "deny"`, and `unwrap_used` and `expect_used`
   are deny for the crate. The test files allow them at file scope because
   helpers sit outside `#[test]`, where clippy's in-test exemption does not
   reach.
4. Nothing trusts a length off the wire. Wire-declared allocations are
   bounded in both the codec and the transport.
5. Every feature layer is optional, so a change that compiles with the default
   set alone is a broken change. `make check-features` checks both ends of the matrix.
6. A coverage number going up is not evidence that a format is right. A
   hot-path change wants the unit test for the branch AND a matrix row in the
   Docker suite if it touches the wire.
