# Handoff Notes

> If you're picking this work up:
> **`SUMMARY.md`** is the current results report — read that first.
> **`PLAN.md`** is the original design doc, now mostly historical.
> This file documents the gotchas, the extension recipe, and the
> things the code deliberately doesn't handle.

## Cluster setting + env var matrix

The bench harness (`pkg/sql/tests/point_select_bench_test.go`) reads
these env vars at server-startup time. The wrapper script
(`run-on-gceworker.sh`) forwards them over ssh.

| env var | default | what it does |
|---|---|---|
| `POINT_SELECT_FAST_PATH` | off | Enables the experimental fast path (`sql.fast_path.point_select.enabled` cluster setting). |
| `POINT_SELECT_PLACEHOLDER_FAST_PATH` | off | Leaves CRDB's existing optimizer `placeholder_fast_path` enabled. Default disables it so the baseline is the unspecialized SQL pipeline. |
| `POINT_SELECT_NODES` | 1 | Number of nodes in the in-process test cluster. We never used >1 for the headline numbers but built the infrastructure in case follow-up needs it. |
| `POINT_SELECT_SPLITS` | nodes*16 if nodes>1 else 0 | Pre-splits applied to the populated table. Only meaningful when nodes>1. |
| `TEST_CPU` | 1 (`./dev bench` default) | **MUST be set to match host vCPU count.** This is the most important confound — see Gotchas. |
| `BENCH_FILTER` | `^BenchmarkPointSelect$` | Standard go test bench filter. Use to pick a specific shape (e.g. `^BenchmarkPointSelectJoin$`). |
| `BENCH_TIME` | `10s` | Per sub-benchmark duration. |
| `BENCH_COUNT` | `5` | Number of repetitions per sub-benchmark. |

## Adding a new query shape — recipe

1. **Define a schema** in `pointSelectSchema` in
   `pkg/sql/tests/point_select_bench_test.go`:
   - `tableDDL` (may include multiple statements separated by `;`)
   - `populateCols` + `populateRow` for the *first* table
   - `postSetupSQL` for additional table data (multi-table case)
   - `stmt`: the prepared SELECT (must take a single `$1` placeholder)
2. **Add a benchmark entry point:**
   ```go
   func BenchmarkPointSelectXXX(b *testing.B) {
       benchmarkPointSelectImpl(b, pointSelectSchemaXXX)
   }
   ```
3. **Extend AST detection** in `pkg/sql/point_select_fast_path.go`:
   - Single-table shapes that already fit (PK / covering / non-covering
     unique secondary index) need no detection changes.
   - Multi-table shapes: add a sibling builder function (model on
     `buildPointSelectJoinFastPath`) and dispatch from
     `buildPointSelectFastPath`.
4. **Extend execution** if the shape needs new mechanics. The
   existing executor handles three KV-Get stages controlled by
   `pkFetch` and `joinFetch` flags; reuse those if you can. Decoding
   handles two encodings (TUPLE for primary indexes, BYTES with
   suffix prefix for unique secondary indexes).
5. **Add a subtest** to `TestPointSelectFastPath` using the
   `insertSpec` pattern. Asserts byte-identical results between
   fast-path-on and fast-path-off.
6. **Smoke locally**, then run on the gceworker:
   ```bash
   BENCH_FILTER='^BenchmarkPointSelectXXX$' TEST_CPU=24 \
     ./run-on-gceworker.sh run run-baseline.sh
   BENCH_FILTER='^BenchmarkPointSelectXXX$' TEST_CPU=24 \
     ./run-on-gceworker.sh run run-fastpath.sh
   ./run-on-gceworker.sh fetch
   ./summarize.sh results/<baseline-ts> results/fastpath-<variant-ts>
   ```

## Gotchas (the things that wasted our time)

- **macOS dramatically understates the win.** Initial runs on an
  M3 Pro showed +4-7% throughput; the same code on Linux showed
  +47-66%. macOS syscall costs dominated the profile. Always
  measure the headline on Linux.
- **`./dev bench` defaults to GOMAXPROCS=1.** Without
  `TEST_CPU=N` matching the host vCPU count, you're benchmarking a
  single-core system and most cores will sit idle. We pin to 24 to
  match the gceworker (`n2-custom-24-32768`). On a different host,
  set to the actual vCPU count.
- **The optimizer's existing `placeholder_fast_path` confounds the
  PK shape.** It already captures a small slice of the win (~13%
  at the 1ms p99 budget for the PK case). Baseline runs disable
  it via the env var so the comparison is fast-path vs.
  unspecialized pipeline, not fast-path vs. existing-fast-path.
- **DistSQL "local" plans still run the DistSQL machinery.** All
  measured queries produce single-node DistSQL plans, but the
  per-execute setup cost (planning context, flow construction,
  receiver init) is what the fast path skips. EXPLAIN (DISTSQL)
  shows `distribution: local` for all our queries.
- **Unique secondary index value encoding is BYTES, not TUPLE.**
  For unique secondary indexes (with or without STORING columns),
  the value tag is `roachpb.ValueType_BYTES` and the bytes start
  with key-side-encoded suffix columns (the implicit PK), followed
  by tuple-encoded storing columns. Calling `Value.GetTuple()`
  fails. We discovered this by writing a non-covering test that
  silently fell through to the slow path while still being
  "correct"; the win was negative until the BYTES decoding was
  added.
- **The detection cache key is `*prep.Statement`.** Each
  prepared-statement reference (across pgx connections, etc.) gets
  its own cache entry. The cache leaks entries when prepared
  statements are deallocated. Acceptable for the experiment; would
  need refcount-tied cleanup for production.

## What the code deliberately does not handle

The fast-path code is research-grade. If you extend it to production-
shaped surface area, you'll need to add:

- Composite (multi-column) primary keys.
- Placeholder types other than `INT8` (the `keyside.Encode` calls
  work for any datum, but the type-equivalence check is narrow).
- Multiple column families (covered columns must currently live in
  family 0).
- `FOR UPDATE` / lock modes / `AS OF SYSTEM TIME`.
- READ COMMITTED isolation (we currently dispatch only on the
  serializable path).
- Per-statement metrics + SQL stats updates (skipped today; the
  slow path does these inside `dispatchToExecutionEngine`).
- EXPLAIN / tracing / `crdb_internal.execution_insights`
  instrumentation hooks.

The "Caveats" section of `SUMMARY.md` says the headline is an
*upper bound* precisely because of these omissions — adding any of
them back will erode the measured savings somewhat.

## Reproducing from scratch on a new host

Tested only against the gceworker pattern (`gceworker.sh create`
provisions an Ubuntu LTS VM with `dev`, bazel, and CRDB
dependencies preinstalled). The wrapper script
(`run-on-gceworker.sh`) hardcodes a few gceworker conventions:

- Resolves the VM name via `id -un` → `gceworker-${user}`.
- Reads zone + project from `~/.ssh/config` entries gceworker.sh
  writes at `start` time, falling back to gceworker.sh's defaults.
- rsyncs the worktree to `~/cockroach-fastpath-experiment` (a
  sibling of the main checkout — does not touch
  `~/go/src/github.com/cockroachdb/cockroach`).
- Uses `AUTOMATION=1` to skip dev's interactive doctor checks
  (the worktree's `.git` is a pointer file that doesn't resolve on
  the remote, breaking some doctor steps).

For a non-gceworker host, simplest path is to run the bench
directly (`./dev bench pkg/sql/tests:tests_test --filter='...'
--test-args='-test.cpu N' -- --test_env=POINT_SELECT_FAST_PATH=1`)
with appropriate env-var prefixes; the wrapper just adds rsync +
ssh on top of that.
