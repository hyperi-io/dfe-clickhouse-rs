# dfe-clickhouse

<!-- BADGES:START -->
[![Build Status](https://github.com/hyperi-io/dfe-clickhouse-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-clickhouse-rs/actions)
[![Crates.io](https://img.shields.io/crates/v/dfe-clickhouse?logo=rust)](https://crates.io/crates/dfe-clickhouse)
[![docs.rs](https://img.shields.io/docsrs/dfe-clickhouse?logo=rust)](https://docs.rs/dfe-clickhouse)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/LICENSE)
<!-- BADGES:END -->

Extensions for the official [`clickhouse`](https://crates.io/crates/clickhouse) Rust client, built for loading data in volume. It depends on the published `clickhouse` release and changes nothing in it, so the HTTP client you already use keeps working as it is.

## What it gives you

- The native protocol on port 9000, with TLS, a connection pool, endpoint failover and opt-in jittered retry for idempotent calls.
- Runtime-schema inserts: hand it `serde_json::Map` rows, with no Rust struct written per table.
- A per-table schema read from `system.columns` and cached, so rows encode straight to RowBinary or Native.
- An opt-in `UnifiedClient` that runs the same queries and inserts over HTTP or native TCP, the transport chosen by configuration at runtime.
- `JSON`, `Variant` and `Dynamic` columns read back as their document text over native TCP. A value ClickHouse spilled to `SharedVariant` is not decoded: a JSON path stored there is left out, and a `Dynamic` one reads as `null`.
- `ServerException::parse` to read the ClickHouse error code and name out of an HTTP error.

```console
cargo add clickhouse dfe-clickhouse
```

## Extension traits on the HTTP client

`ping`, `kill_query`, `with_query_id`, `with_session_id`, `with_role`, and
`ServerException::parse` for a typed error carrying the server's own code.

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

Rows arrive as `serde_json::Map`, column types come from `system.columns`.
Binary on the wire either way -- `FORMAT RowBinary` over HTTP, `FORMAT Native`
over TCP -- so the server does no JSON parsing.

## Features

| Feature | What it adds |
|---|---|
| `ext` | Extension traits on `clickhouse::Client` (default) |
| `lz4` | LZ4 for the HTTP transport, forwarded to upstream (default) |
| `zstd` | Zstd for the HTTP transport, forwarded to upstream |
| `tcp` | Native TCP transport: connection actor, pool, retry, wire codec |
| `tls` | rustls trust for the TCP transport (implies `tcp`) |
| `dynamic` | Runtime-schema inserts (implies `ext`) |
| `unified` | One client over both transports (implies `tcp`) |
| `full` | All of the above |

Default is `ext` plus `lz4`, neither of which adds a dependency beyond
upstream's. `tcp` brings deadpool, socket2 and backon.

## Documentation

| Read | When |
|---|---|
| [type-support](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/type-support.md) | Checking whether a column type reads and writes |
| [insert-formats](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/insert-formats.md) | Asking why there is no JSONEachRow option |
| [unified-client](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/unified-client.md) | Choosing a transport at runtime |
| [coverage](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/coverage.md) | Adding to a hot-path module |

## Upstream

Not a fork. The dependency on the published `clickhouse` release is the only
link -- no `git`, no `path`, no `[patch]`. Same edition, MSRV, rustfmt settings
and lint set, so
[ClickHouse/clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) can
cherry-pick from here without a reformat or a relicence.

## Licence

Apache-2.0. See
[LICENSE](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/LICENSE) and
[NOTICE](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/NOTICE).

## Context

Depth: [docs/architecture.md](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/architecture.md).

### What this is

Extensions to the official ClickHouse Rust client, in a separate crate that
depends on the published `clickhouse` release. It adds a native TCP transport,
runtime-schema inserts, and one client dispatching over either. Not a fork, not a
rewrite, not a chart, not a schema tool: it runs the SQL it is handed and authors
none, so it creates no database, table, view, role or Kafka topic.

### Where things live

| Path | Holds |
|---|---|
| `src/lib.rs` | crate root, the feature-gated module layers, and the README as the crate docs |
| `src/ext.rs` | extension traits on `clickhouse::Client`, plus the typed server-exception parser |
| `src/native/` | the Native wire codec -- HOT PATH, every row in or out passes through it |
| `src/tcp/` | the TCP transport: connect, handshake, connection actor, pool, retry, protocol, reader, writer |
| `src/dynamic/` | runtime-schema inserts -- the `system.columns` schema cache, encode, insert |
| `src/unified.rs` | `UnifiedClient`, the dispatch enum over both transports |
| `src/tls.rs`, `src/worker.rs` | rustls trust for TCP, and the internal primitive the connection actor uses |
| `tests/wire_docker.rs` | the type matrix against a pinned ClickHouse in Docker |
| `tests/*_live.rs` | ignored by default -- they need a real cluster |
| `docs/` | coverage, insert formats, type support, the unified client, this architecture |

### Commands that prove a change

```bash
hyperi-ci check        # the full local gate, the same suite CI runs
make check-features    # cargo check --all-features, then --no-default-features
```

Three ways green lies, each recorded in the repo:

| It looks like | It is actually | Where |
|---|---|---|
| `-D warnings` covers the crate | It reaches only the `features: all` set. The feature matrix runs `cargo hack --each-feature --no-dev-deps check`, and `check` does not fail on a warning, so feature-gated dead code and feature-conditional lints pass. Four accumulated in the default set before the review gate started linting it on 2026-09-02 | `.hyperi-ci.yaml` |
| The live tests ran | CI never runs the eleven `#[ignore]`d tests in `tests/*_live.rs`. They need a real cluster through `CLICKHOUSE_DFE_ENV_FILE`, and nextest is given no `--run-ignored` | `tests/*_live.rs` |
| Coverage is held | It reports and does not gate. `coverage: false` is deliberate -- hyperi-ci reads `test.min_coverage` for Python only. Measure out of band with `cargo llvm-cov --all-features --all-targets --summary-only`, keeping `--all-targets`, because a `--lib`-only run reads lower on the codec and invents regressions | `docs/coverage.md` |

### What tends to bite

| Don't | Do | Why |
|---|---|---|
| Trust a unit test on a wire format | Add a row to `tests/wire_docker.rs` | The sparse kind-stack bug sat in code the unit tests executed happily -- the fixtures agreed with the decoder because both were written from the same wrong reading of the spec. A real server writing the bytes caught it (`0ee6201`, `docs/coverage.md`) |
| Raise the advertised protocol revision on its own | Consume the fields the new gates add, in the handshake and in Progress and ProfileInfo | `88137d3` advertised 54473 and stalled against a real server until `64baef4` and `aae05d3` read the added fields |
| Add `#[ignore]` to a test in `tests/wire_docker.rs` | Leave it running and let it skip when there is no container runtime | nextest has no `--run-ignored` here, so an ignored test in that file is one CI never runs (`tests/wire_docker.rs:16`) |
| Assume the default feature set is linted | Run `make check-features`, and lint the sets by hand | A lint ratchet macro hid 64 warnings until `66b271f` retired it, and `d49c721` then had to lint the sets nobody was linting |
| Start a container per test and leave it to the runner | Keep the suite in the `docker-server` nextest group | The wire suite leaked a ClickHouse container per run until `db68d25`. nextest gives each test its own process, so an in-binary semaphore cannot serialise them, and seven servers at 2 GiB do not fit a build host (`.config/nextest.toml`) |
| Reach for `git =`, `path =` or `[patch.crates-io]` on `clickhouse` | Depend on the published version | The crates.io release is the only link, which is what lets files travel back to `ClickHouse/clickhouse-rs` without a reformat or a relicence (`CONTRIBUTING.md`) |

### Where this sits

`dfe-stack suite --consumer dfe-clickhouse` and `--producer dfe-clickhouse` both
return this node with an EMPTY edge list -- registered as a general-audience OSS
library with `default_in_pass: false`. The one relationship that matters is
upstream, and the graph does not carry it:

| Repo | Direction | Kind | Mechanism |
|---|---|---|---|
| `ClickHouse/clickhouse-rs` | inbound | published crate, by version | `Cargo.toml` depends on `clickhouse` 0.15.2 with `default-features = false`, and on nothing from that repo's git. Edition 2024, MSRV 1.89.0 and the lint set are kept in step with it so code can be cherry-picked in either direction |

## Issues

Open bugs: [this repo](https://github.com/hyperi-io/dfe-clickhouse-rs/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse+repo%3Ahyperi-io%2Ffactbook-rs)
