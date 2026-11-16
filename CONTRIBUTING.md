# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Stay close to upstream

This crate extends the official ClickHouse Rust client and expects code to travel back and forth with it. Keep files close to upstream: same edition, same MSRV (`rust-version` in `Cargo.toml`), same `rustfmt.toml`, and upstream's lints plus this crate's stricter ones. A file that needs a reformat before it can be cherry-picked has already cost more than it saved.

The dependency on `clickhouse` is a published crates.io release, and only ever
that. No `git =`, no `path =`, no `[patch.crates-io]`.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### Types

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

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Scope

Scope is optional but recommended. Use the layer the change lands in: `tcp`, `tls`, `native`, `dynamic`, `unified`, `ext`.

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- MAJOR (X.0.0): breaking changes that require users to modify their code
- MINOR (0.X.0): new features that are backwards-compatible
- PATCH (0.0.X): bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

While the crate is pre-1.0 the API is unstable, and a minor bump can carry a
breaking change.

## Feature layers

Every layer of this crate but the Native codec is behind a Cargo feature, so a change that only compiles with the default set is a broken change. Check both ends of the matrix before you push:

```bash
make check-features
```

## Run the checks locally before you push

HyperI projects gate every push through CI (lint, format, tests, secret scan,
dependency audit, build). Run the SAME checks locally first so your change lands
green instead of bouncing:

```bash
hyperi-ci check
```

`hyperi-ci` is the HyperI CI CLI (public on PyPI). If you do not have it, install it once with `uv tool install hyperi-ci` (or `pipx install hyperi-ci`), or run it ad hoc with `uvx hyperi-ci check`. It runs the project's full local validation -- the same suite CI runs -- and reports what to fix. Run it before every push and before opening a pull request.

## How to Contribute

1. Fork the repository and create your branch from `main`
2. Make your changes, following the commit message format above
3. Run the tests and `make check-features`
4. Open a pull request that says what changed and why

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] `make check-features` passes
- [ ] Tests pass
- [ ] Documentation is updated (if applicable)

## CI/CD Workflow

A merge to `main` releases only when its commit carries a `Release: true` trailer, or when a maintainer dispatches the CI workflow to release. Then:

1. semantic-release analyses commit messages since the last release
2. Determines the next version number based on commit types
3. Generates/updates the CHANGELOG
4. Creates a new GitHub release with release notes
5. Publishes the crate to crates.io

## Questions

If you have questions about contributing, please open an issue or contact us.
