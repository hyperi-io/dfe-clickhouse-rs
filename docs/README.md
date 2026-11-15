# dfe-clickhouse docs

Start at the [README](../README.md): it is the GitHub front page, and docs.rs renders it as the crate docs. [crate.md](crate.md) is the crates.io page, so a change to what the crate offers lands in both.

Design rationale lives in code comments at the point of use, so a file
cherry-picked into another repository carries its reasoning with it. That is
deliberate -- this index stays thin rather than duplicating what the source
already says and would drift from.

A layer gets a page here once its reasoning outgrows a comment:

- [architecture.md](architecture.md) -- the cross-layer map: the three gaps the
  crate exists to close, the feature layers, and the invariants that span more
  than one module.
- [coverage.md](coverage.md) -- the 80% floor, the 90% hot-path target, how to
  measure, and where any tracked debt is recorded.
- [insert-formats.md](insert-formats.md) -- why every insert path sends binary,
  and why there is no JSONEachRow option.
- [type-support.md](type-support.md) -- which ClickHouse types the codec reads
  and writes.
- [unified-client.md](unified-client.md) -- one entry point over two transports.
