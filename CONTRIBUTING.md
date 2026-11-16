# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Stay close to upstream

This crate extends the official ClickHouse Rust client and expects code to travel back and forth with it. Keep files close to upstream: same edition, same MSRV (`rust-version` in `Cargo.toml`), same `rustfmt.toml`, and upstream's lints plus this crate's stricter ones. A file that needs a reformat before it can be cherry-picked has already cost more than it saved.

The dependency on `clickhouse` is a published crates.io release and nothing else. No `git =`, no `path =`, no `[patch.crates-io]`.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

The types, and the version bump each gets:

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Patch (0.0.X) |

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

Scope is optional but recommended. Use the layer the change lands in: `tcp`, `tls`, `native`, `dynamic`, `unified`, `ext`.

## Run the checks locally before you push

Every layer of this crate but the Native codec is behind a Cargo feature, so a change that compiles with the default set alone is a broken change. Check both ends of the feature matrix, then run the gate CI runs (lint, format, tests, secret scan, dependency audit, build), so your change lands green instead of bouncing:

```bash
make check-features
hyperi-ci check
```

`hyperi-ci` is the HyperI CI CLI (public on PyPI). If you do not have it, install it once with `uv tool install hyperi-ci` (or `pipx install hyperi-ci`), or run it ad hoc with `uvx hyperi-ci check`. It runs the project's full local validation, the same suite CI runs, and reports what to fix.

## How to Contribute

1. Fork the repository and create your branch from `main`
2. Make your changes, following the commit message format above
3. Run the tests and `make check-features`
4. Open a pull request that says what changed and why

Before you open it:

- [ ] Commits follow the conventional commit format
- [ ] `make check-features` passes
- [ ] Tests pass
- [ ] Documentation is updated (if applicable)

If you have questions about contributing, please open an issue or contact us.

## Releases

This project follows [Semantic Versioning 2.0.0](https://semver.org/). MAJOR (X.0.0) is a breaking change that needs users to modify their code, MINOR (0.X.0) a backwards-compatible feature, and PATCH (0.0.X) a bug fix. Before 1.0 the API is unstable, and a minor bump can carry a breaking change.

semantic-release sets the version from the commit messages, so never edit a version number by hand. A merge to `main` releases when its commit has a `Release: true` trailer, or when a maintainer dispatches the CI workflow to release. Then:

1. semantic-release analyses commit messages since the last release
2. Determines the next version number based on commit types
3. Generates/updates the CHANGELOG
4. Creates a GitHub release with release notes
5. Publishes the crate to crates.io

## Context

### What this is

Extensions to the official ClickHouse Rust client, in a separate crate that depends on the published `clickhouse` release. It adds a native TCP transport, runtime-schema inserts, and one client dispatching over either. Not a fork, not a rewrite, not a chart, not a schema tool. The SQL it writes itself is its INSERTs, the `system.columns` read, `SELECT 1` and `KILL QUERY`, so it creates no database, table, view, role or Kafka topic. [docs/architecture.md](docs/architecture.md) is the design.

### Where things live

| Path | Holds |
|---|---|
| `src/lib.rs` | crate root, the feature-gated module layers, and the README as the crate docs |
| `src/ext.rs` | extension traits on `clickhouse::Client`, plus the typed server-exception parser |
| `src/native/` | the Native wire codec -- HOT PATH, every row this crate reads or writes passes through it |
| `src/tcp/` | the TCP transport: connect, handshake, connection actor, pool, retry, protocol, reader, writer |
| `src/dynamic/` | runtime-schema inserts -- the `system.columns` schema cache, encode, insert |
| `src/unified.rs` | `UnifiedClient`, the dispatch enum over both transports |
| `src/tls.rs`, `src/worker.rs` | rustls trust for TCP, and the internal primitive the connection actor uses |
| `tests/wire_docker.rs` | the type matrix against a pinned ClickHouse in Docker |
| `tests/*_live.rs` | ignored by default -- they need a ClickHouse cluster |
| `docs/` | the architecture, coverage, insert formats, type support, the unified client, and `crate.md`, the crates.io page |

### Commands that prove a change

```bash
hyperi-ci check        # the full local gate, the same suite CI runs
make check-features    # cargo check --all-features, then --no-default-features
```

Three ways a passing run misleads:

| It looks like | It is actually | Where |
|---|---|---|
| `-D warnings` covers the crate | It reaches the `features: all` set alone. The feature matrix lints each feature set under clippy and names its warnings without failing, because `quality.rust.feature_matrix.warnings` defaults to `warn`, so feature-gated dead code and feature-conditional lints pass | `.hyperi-ci.yaml` |
| The live tests ran | CI never runs the `#[ignore]`d tests in `tests/*_live.rs`. They need a ClickHouse cluster through `CLICKHOUSE_DFE_ENV_FILE`, and CI runs hyperi-ci's `core` test tier, which skips ignored tests | `tests/*_live.rs` |
| Coverage is held | CI does not measure it. `coverage: false` is deliberate, because hyperi-ci reads `test.min_coverage` for Python alone. Measure out of band with `cargo llvm-cov --all-features --all-targets --summary-only`, keeping `--all-targets`, because a `--lib` run reads lower on the codec and invents regressions | `docs/coverage.md` |

### What tends to bite

| Don't | Do | Why |
|---|---|---|
| Trust a unit test on a wire format | Add a row to `tests/wire_docker.rs` | The sparse kind-stack bug sat in code the unit tests ran without complaint, because the fixtures and the decoder were written from the same wrong reading of the spec. A server writing the bytes caught it (`0ee6201`, `docs/coverage.md`) |
| Raise the advertised protocol revision on its own | Consume the fields the higher gates add, in the handshake and in Progress and ProfileInfo | `88137d3` advertised 54473 and stalled against a server until `64baef4` and `aae05d3` read the added fields |
| Add `#[ignore]` to a test in `tests/wire_docker.rs` | Leave it running and let it skip when there is no container runtime | CI runs the `core` test tier, which skips ignored tests, so an ignored test in that file is one CI never runs (`tests/wire_docker.rs:16`) |
| Assume the default feature set is linted | Run `make check-features`, and lint the sets by hand | A lint ratchet macro hid 64 warnings until `66b271f` retired it, and `d49c721` then linted the sets nobody was linting |
| Start a container per test and leave it to the runner | Keep the suite in the `docker-server` nextest group | The wire suite leaked a ClickHouse container per run until `db68d25`. nextest gives each test a process of its own, so an in-binary semaphore cannot serialise them, and a 2 GiB server for every test at once does not fit a build host (`.config/nextest.toml`) |
| Reach for `git =`, `path =` or `[patch.crates-io]` on `clickhouse` | Depend on the published version | The crates.io release is the one link, which is what lets files travel back to `ClickHouse/clickhouse-rs` without a reformat ([Stay close to upstream](#stay-close-to-upstream)) |

### Where this sits

Two DFE apps depend on it: dfe-loader with `ext`, `tls` and `dynamic`, and dfe-fetcher with `ext`, behind its db crate's `clickhouse` feature. `dfe-stack suite --producer dfe-clickhouse-rs` lists the consumers dfe-infra's `suite.yaml` records, where this node is a general-audience OSS library with `default_in_pass: false`. The graph does not carry the upstream relationship:

| Repo | Direction | Kind | Mechanism |
|---|---|---|---|
| `ClickHouse/clickhouse-rs` | inbound | published crate, by version | `Cargo.toml` depends on the published `clickhouse` release, with `default-features = false`, and on nothing from that repo's git. The edition, the MSRV (`rust-version`) and `rustfmt.toml` are kept in step with it, so code travels between the two without a reformat |
| `hyperi-io/dfe-loader` | outbound | crates.io dependency, by range | `Cargo.toml` takes `ext`, `tls` and `dynamic` |
| `hyperi-io/dfe-fetcher` | outbound | crates.io dependency, by range, optional | `crates/db/Cargo.toml` takes `ext` behind the `clickhouse` feature |
