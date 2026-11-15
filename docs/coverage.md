# Coverage: the floor and the hot-path target

The repo floor is 80% lines. Two modules are marked `HOT PATH` in their own
docs and held to 90% instead: `src/native/` (every row in or out of the server
goes through it) and `src/tcp/pool.rs` (every operation acquires from it).

Tracked debt against either target is recorded here, so the marker comments point at this page rather than each asserting a number that goes stale the next time someone adds a type.

CI does not measure this. `coverage: false` in `.hyperi-ci.yaml` is deliberate: hyperi-ci reads `test.min_coverage` for Python only, so turning it on would gate nothing and cost the runner. Measure out of band:

```text
cargo llvm-cov --all-features --all-targets --summary-only
```

Use `--all-targets`. A `--lib`-only run reads lower on the codec, because the integration suites cover paths the unit tests do not, and comparing one scope against the other invents regressions that are not there.

## What the numbers do not tell you

Coverage counts lines executed, not wire formats proved. The sparse
kind-stack bug sat in code the unit tests executed happily -- the fixtures
agreed with the decoder because both were written from the same wrong reading
of the spec. What caught it was `tests/wire_docker.rs`, where a real server
writes the bytes.

So a hot-path change wants both: the unit test for the branch, and a matrix row in the Docker suite if it touches the wire. A coverage number going up is not on its own evidence that a format is right.

Write the expectations from the wire format, not from what the function returns. The `LowCardinality(Nullable(T))` test in `src/native/encode.rs` asserts the exact dictionary and index bytes derived from the spec, which is the difference between a test that pins behaviour and one that pins a bug. A hot-path gap tends to sit in one family of per-type arms, and one table-driven test per family closes it.

A fallback a comment promises needs a test that reaches it. The one that reached the empty-endpoint fallback in `src/tcp/pool.rs` found it unreachable: `create` divided by the endpoint count first, so an empty list panicked inside a pool acquire until 2fd9269.
