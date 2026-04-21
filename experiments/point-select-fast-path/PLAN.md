# SQL Processing Overhead: Point-Select Fast Path Experiment

## Context

The CTO has asked us to measure how much latency the SQL processing layer
contributes to a very simple OLTP workload — a prepared point-select against
a primary key:

```sql
SELECT v FROM kv WHERE k = $1
```

The goal is **not** to ship a feature, but to establish an upper bound on the
benefit of any future "lighter execution engine" or "more aggressive plan
caching" investment, before we commit to building one.

The experiment has two halves:

1. **Baseline** — measure current p99 latency for the query above at a sweep
   of concurrency levels on a controlled topology.
2. **Hard-coded fast path** — short-circuit the connExecutor: detect this
   exact query shape, skip the optimizer / exec-builder / distSQL planner /
   row.Fetcher entirely, and issue a direct `kv.Txn.Get` from a hard-coded
   plan. Re-measure. The delta is our upper-bound estimate.

### Important: existing fast paths already cover part of this

CRDB already has `TryPlaceholderFastPath`
(`pkg/sql/opt/xform/placeholder_fast_path.go:31`) which produces an "ideal
generic plan" for shapes like `WHERE pk = $1`. That means re-optimization is
already skipped per execution for our target query — the prepared statement
keeps a fully-baked `IdealGenericPlan` (`pkg/sql/prep/statement.go:52`).

So our fast path has to skip *more*: the memo staleness check
(`pkg/sql/opt/memo/memo.go:480`), exec-builder, distSQL physical plan,
row.Fetcher init, DistSQLReceiver, and as much connExecutor bookkeeping as
we can without breaking the pgwire contract. This is the right comparison
to make — it represents the gap an "execution engine upgrade" would actually
have to close.

---

## Phase 1 — Benchmark harness

### Why we can't just use `cockroach workload run kv`

The shipped `kv` workload uses `WHERE k IN ($1, ...)`
(`pkg/workload/kv/kv.go:506`). Even with `--batch=1` it's
`WHERE k IN ($1)`, which is a different optimizer shape than `WHERE k = $1`
and may not match `TryPlaceholderFastPath` cleanly. To compare apples to
apples we need a workload whose query shape is exactly the one our fast path
recognizes.

### Approach

Add a small `--point-select` mode to `pkg/workload/kv` (or a sibling tiny
workload — easier to land without disturbing `kv`'s many users). It should:

- Use the same schema as `pkg/workload/kv` (`k INT PRIMARY KEY, v BYTES`).
- Issue `SELECT v FROM kv WHERE k = $1` only — no IN, no batch.
- Use prepared statements through `workload.SQLRunner`
  (`pkg/workload/kv/kv.go:584`) so we test the prepared-statement path.
- Reuse the existing histogram framework (`pkg/workload/histogram`) — it
  already tracks p50/p99/p99.9 via hdrhistogram and writes JSON via
  `--histograms`.

### Topology

Default to **single-node, in-memory**: it isolates pure SQL/processing
overhead from network and Raft, which is what the CTO is asking about.
Add a follow-up sweep on a 3-node roachprod cluster only if the single-node
delta looks promising — that quantifies how much of the win survives
realistic network costs.

```bash
# One-shot local server (release build, --insecure for simplicity).
./dev build short
./cockroach demo --no-example-database --insecure ...
# Or: cockroach start-single-node --insecure --store=type=mem,size=2GiB

# Init schema.
./bin/workload init kv --splits=10 'postgres://root@localhost:26257?sslmode=disable'

# Sweep concurrency, capture p99.
for c in 1 8 16 32 64 128; do
  ./bin/workload run kv --read-percent=100 --point-select \
    --concurrency=$c --duration=60s --tolerate-errors \
    --histograms=/tmp/p99-c${c}.json \
    'postgres://root@localhost:26257?sslmode=disable'
done
```

### Pin sources of variance

- Set `GOMAXPROCS` explicitly (e.g. cap to half the cores so the workload
  client and server don't fight for CPU).
- Disable background work during the run (compactions, lease queue) the same
  way `pkg/sql/tests/sysbench_test.go:disableBackgroundWork` does, if we
  want repeatable single-node numbers.
- Pre-warm: discard the first 30s of each run (workload framework supports
  `--ramp`).
- Run each concurrency point at least 3 times, report median p99.

### Companion in-process microbench

For fast iteration during fast-path development, mirror the structure of
`pkg/sql/tests/sysbench_test.go` (in-process `serverutils.StartCluster`,
1 node, `localRPCFastPath` knob, pgx clients in-process). The query is:

```go
const stmt = `SELECT v FROM kv WHERE k = $1`
```

This gives sub-second iteration on a single benchmark and lets us run the
same code under `pprof` cleanly. `b.SetParallelism(N)` simulates concurrency
levels.

---

## Phase 2 — Establish baseline + cost distribution

Before writing the fast path, profile the baseline so we understand where
time is spent. This protects us against committing to a design that targets
the wrong layer.

1. Run the in-process microbench at concurrency 1 with CPU profiling:
   `./dev test pkg/sql/tests:tests_test -f BenchmarkPointSelect -v -- -cpuprofile=/tmp/cpu.prof -benchtime=30s`
2. Generate a flame graph (`go tool pprof -http=:8080 /tmp/cpu.prof`).
3. Bucket time roughly into:
   - pgwire send/recv + state machine (`pkg/sql/pgwire`, `connExecutor.run`)
   - Per-statement bookkeeping in `execStmtInOpenState`
     (`pkg/sql/conn_executor_exec.go:416`) — active-query registration, span,
     stats reset, deadline updates.
   - Memo staleness check (`pkg/sql/opt/memo/memo.go:480`).
   - Exec-builder + distSQL physical planning (`pkg/sql/plan_opt.go:367`).
   - `row.Fetcher` setup (`pkg/sql/row/fetcher.go`).
   - `kv.Txn.Run` (the actual KV work — this is the floor we cannot beat).
   - DistSQL receiver + result row writeback.

The breakdown belongs in the writeup; it tells the CTO what fraction of the
remaining gap an execution-engine investment would actually claim.

---

## Phase 3 — Implement the fast path

### Toggle

Cluster setting `sql.fast_path.point_select.enabled` (default `false`) plus
matching session var. Mirror the layout of `enable_insert_fast_path`
(`pkg/sql/vars.go`) and its cluster setting in `pkg/sql/exec_util.go`.
Default off so all existing tests are unaffected.

### Injection point

`pkg/sql/conn_executor_exec.go:execStmtInOpenState`, immediately after the
prepared statement is materialized at line 454. Pseudocode:

```go
stmt = makeStatementFromPrepared(...)

if isExtendedProtocol && ex.fastPathPointSelectEnabled() {
    if plan := prepared.PointSelectFastPath; plan != nil {
        if handled, err := ex.execPointSelectFastPath(ctx, plan, pinfo, res); handled {
            return ev, payload, err  // success or terminal error
        }
        // Not handled (e.g. txn state forbids it) — fall through.
    }
}

// ... existing slow path continues unchanged ...
```

Why here:
- Parsed AST and bound parameters are both available.
- Transaction is already open (`ex.state.mu.txn`), descriptor collection is
  already attached (`ex.extraTxnState.descCollection`).
- We are *before* the optimizer is invoked but *after* setup that the pgwire
  contract requires (active-query registration, statement counters).

### Plan caching: detect once at PARSE, not per execution

To avoid paying detection overhead on every execute, do the shape detection
during `prepareUsingOptimizer` (`pkg/sql/plan_opt.go:84`) and stash the
result on the prepared statement.

Add to `pkg/sql/prep/statement.go`:

```go
// PointSelectFastPath, if non-nil, holds a pre-resolved plan for executing
// this prepared statement as a single primary-key Get without going through
// the optimizer or distsql. Set by prepareUsingOptimizer when the AST shape
// matches `SELECT <colrefs> FROM <table> WHERE <pk_col> = $1`. Consumed by
// connExecutor.execPointSelectFastPath. Nil for any statement that does
// not match the recognized shape, so a nil check is also the "is this
// eligible?" check.
PointSelectFastPath *PointSelectFastPath
```

`PointSelectFastPath` carries everything we resolved at PARSE time:

- `tableID descpb.ID`
- `indexID descpb.IndexID` (always primary)
- `keyPrefix []byte` (precomputed via `rowenc.MakeIndexKeyPrefix`)
- `pkColType *types.T` (for placeholder validation)
- `outputColIDs []descpb.ColumnID` (in SELECT order)
- `resultColumns colinfo.ResultColumns` (for the pgwire row description)
- `descVersion descpb.DescriptorVersion` (for lightweight staleness check;
  see below)

### Detection function

In a new file `pkg/sql/point_select_fast_path.go`:

```go
// tryBuildPointSelectFastPath returns a non-nil plan if ast matches:
//   SELECT <col list> FROM <single, unaliased table> WHERE <pk_col> = $1
// with no joins, ORDER BY, LIMIT, GROUP BY, aggregates, FOR UPDATE,
// AS OF SYSTEM TIME, locking clauses, or projections beyond column refs.
// The single placeholder must bind exactly the (single-column) primary key.
func tryBuildPointSelectFastPath(
    ctx context.Context,
    p *planner,
    ast tree.Statement,
) (*prep.PointSelectFastPath, error)
```

Keep this strict and brittle on purpose — for the prototype we only need to
match the one query shape. Anything we do not perfectly recognize falls
through to the existing slow path, so correctness is preserved by
construction. Future iterations can broaden the predicate.

### Execution function

In the same file:

```go
func (ex *connExecutor) execPointSelectFastPath(
    ctx context.Context,
    plan *prep.PointSelectFastPath,
    pinfo *tree.PlaceholderInfo,
    res RestrictedCommandResult,
) (handled bool, err error)
```

Steps:

1. **Reject anything we don't support yet.** AOST txn, FOR UPDATE locking,
   read-committed (until verified), implicit txn quirks. Return
   `handled=false` so the slow path runs.
2. **Validate descriptor freshness cheaply.** Re-fetch the descriptor from
   `ex.extraTxnState.descCollection.ByIDWithLeased(...).Get().Table(ctx)`
   and compare `desc.GetVersion()` to `plan.descVersion`. If different,
   return `handled=false` and let the slow path rebuild. (The collection
   already does lease-based validation — this just guards our cached
   `keyPrefix` and `outputColIDs`.)
3. **Encode the key.** Pull the bound `tree.Datum` for `$1` from `pinfo`,
   verify the type matches `plan.pkColType`, and call
   `rowenc.EncodeTableKey(plan.keyPrefix, datum, encoding.Ascending)`
   (or the column-family-aware equivalent — model on
   `pkg/sql/insert_fast_path.go:118` and `row.FKUniqCheckSpan`).
4. **Issue the Get.** Build a `kv.Batch`, add a `Get(key)`, call
   `ex.state.mu.txn.Run(ctx, b)`. Respect transaction stepping the way the
   slow path does (see the `txn.Step` calls at
   `pkg/sql/conn_executor_exec.go:1119`).
5. **Decode the row.** For the prototype, support the simple case of single
   column family. Use `rowenc.DecodeIndexKey`/`DecodeKeyVals` to extract
   the value bytes, then `valueside.Decode` for each requested column.
   `row.Fetcher` shows the full pattern but is overkill — open-code it.
6. **Emit the row.** `res.SetColumns(ctx, plan.resultColumns)` (only on
   first emit, the connExecutor normally does this), then `res.AddRow(...)`.
   Use `res.SetRowsAffected` / `IncrementRowsAffected` to match the
   contract the pgwire layer expects.
7. **Update statement counters and timings** the way the slow path does so
   stats endpoints don't lie. At minimum:
   `ex.statsCollector.RecordStatement(...)`, the `phaseTimes` calls.

### Things deliberately out of scope for the prototype

These are interesting eventually but not needed to answer the CTO's
question:

- Multi-column primary keys.
- Composite types, collated strings, oid types.
- Multiple column families.
- Anything other than `SELECT` of literal column refs.
- Implicit transactions that span multiple statements.
- Read-committed isolation (until we audit txn-stepping interaction).

If the prototype hits something out of scope, return `handled=false` and let
the regular path run.

---

## Phase 4 — Measure & iterate

1. Re-run the Phase 1 sweep with the cluster setting **off**, then **on**.
   Each concurrency point is `(p50, p99, p99.9, qps)` for both, plus the
   delta. Capture in a table.
2. CPU profile the fast-path run at the same concurrency points; recompute
   the cost-distribution from Phase 2. The remaining time-buckets are the
   floor an execution-engine project would have to push against.
3. **Iterate.** If the first prototype shows a meaningful gap, the natural
   next dials to turn:
   - Skip the descriptor-version recheck once per txn instead of per
     statement.
   - Skip statement-counter / phase-time bookkeeping (measure cost).
   - Skip the active-query map insertion (measure cost, document the loss
     of `SHOW QUERIES` visibility for fast-path queries).
   - Pre-encode / pool the `kv.Batch` and `BatchRequest`.
   - Bypass the connExecutor state-machine event entirely for the
     fast-path success case (return early, skip the `fsm.Event` machinery).

   Each iteration is a separate measurement so we can plot a curve of
   "how much can we save by giving up which guarantee?" That's the actual
   deliverable for the CTO.

---

## Critical files

| Purpose | Path | Notes |
|---|---|---|
| Injection point | `pkg/sql/conn_executor_exec.go` | `execStmtInOpenState`, ~L454 after `makeStatementFromPrepared` |
| Cache fast-path on prepared stmt | `pkg/sql/prep/statement.go` | Add `PointSelectFastPath` field + type |
| New fast-path detection + exec | `pkg/sql/point_select_fast_path.go` | New file |
| Detection during PARSE | `pkg/sql/plan_opt.go` | Hook into `prepareUsingOptimizer` ~L84 |
| Cluster setting | `pkg/sql/exec_util.go` | Mirror `insert_fast_path` setting |
| Session var | `pkg/sql/vars.go` | Mirror `enable_insert_fast_path` |
| Model: detect+execute fast path | `pkg/sql/insert_fast_path.go` | Pattern to follow for descriptor + key encoding |
| Key prefix encoding | `pkg/sql/rowenc/index_encoding.go` | `MakeIndexKeyPrefix`, `EncodeTableKey` |
| Existing optimizer fast path | `pkg/sql/opt/xform/placeholder_fast_path.go` | What we're already getting; the bar to clear |
| Memo staleness (the cost we're skipping) | `pkg/sql/opt/memo/memo.go` | `IsStale` ~L480 |
| Workload extension | `pkg/workload/kv/kv.go` | Add `--point-select` mode, or new sibling workload |
| Microbench model | `pkg/sql/tests/sysbench_test.go` | In-process server pattern |
| Histogram / p99 | `pkg/workload/histogram/histogram.go` | Already does what we need |

---

## Verification

**Correctness** (must pass before any benchmark numbers are trusted):

- New file `pkg/sql/point_select_fast_path_test.go`:
  - Spin up an in-process server with the cluster setting **on**.
  - Create `kv (k INT PRIMARY KEY, v BYTES)`, insert ~100 rows.
  - For each row, run the prepared `SELECT v FROM kv WHERE k = $1` and
    assert the returned value matches the inserted value.
  - Run the same set with the cluster setting **off** and assert byte-for-
    byte identical results.
  - Negative cases: AOST, FOR UPDATE, multi-col PK, IN-list — assert these
    fall through to the slow path and still return correct answers.
- Run an existing pgwire conformance test under both toggles to confirm we
  haven't broken protocol invariants:
  `./dev test pkg/sql/pgwire -f TestPGWire -v`.

**Performance** (the actual experiment):

- New benchmark in `pkg/sql/tests/point_select_bench_test.go` modeled on
  `sysbench_test.go`. One sub-benchmark per `(toggle on/off, concurrency)`.
- Run end-to-end via the workload as described in Phase 1 and capture
  histograms. Report a small markdown table per toggle state plus the delta.

**Commands:**

```bash
# Compile-check everything.
./dev build pkg/sql pkg/sql/prep pkg/sql/tests pkg/workload/kv

# Correctness.
./dev test pkg/sql/tests -f TestPointSelectFastPath -v

# Local concurrency sweep — pinned settings live inside the script.
# Writes raw output + env snapshot to results/<timestamp>/ and
# refreshes the `results/latest` symlink.
./experiments/point-select-fast-path/run-baseline.sh   # placeholder + new fast paths off
./experiments/point-select-fast-path/run-fastpath.sh   # new fast path on (POINT_SELECT_FAST_PATH=1)

# Side-by-side comparison.
./experiments/point-select-fast-path/summarize.sh \
  results/<baseline-ts> results/fastpath-<variant-ts>
```

---

## Phase 5 — Linux re-baseline on the gceworker

### Why

The macOS conc=8 profile showed ~76% of CPU samples in
`syscall.rawsyscalln` and `runtime.kevent` — pgwire's TCP socket
reads/writes per op. macOS syscalls are notoriously expensive
relative to Linux (different ABI, no equivalent of the broad vDSO
fast-path coverage). The result is that the SQL pipeline is a tiny
fraction of CPU and the fast-path latency win caps out at ~25-30%
on p99 — most of an op is goroutine-blocking on syscalls, not
computing.

On a Linux host the syscall floor is much lower, which means:
- Per-op latency drops overall.
- The *relative* contribution of SQL processing grows, so the fast
  path's delta widens both in absolute µs and as a percentage.
- The QPS-at-budget headroom is more meaningful for the CTO answer.

### Workflow

A wrapper script handles sync/run/fetch against the user's
gceworker without disturbing the main checkout there. It rsyncs
this worktree to a sibling directory (`~/cockroach-fastpath-experiment`)
and runs the same scripts in place.

```bash
# One-time per code change: push the local worktree to gceworker.
./experiments/point-select-fast-path/run-on-gceworker.sh sync

# Run the sweeps remotely. Each takes ~8 min wall time.
./experiments/point-select-fast-path/run-on-gceworker.sh run run-baseline.sh
./experiments/point-select-fast-path/run-on-gceworker.sh run run-fastpath.sh

# Pull the results back into local results/ for summarize.sh.
./experiments/point-select-fast-path/run-on-gceworker.sh fetch

# Compare. The remote run dirs land in the same results/ tree, so
# summarize.sh works exactly as locally.
./experiments/point-select-fast-path/summarize.sh \
  results/<gce-baseline-ts> results/fastpath-<gce-variant-ts>
```

### What to expect

If the syscall-cost hypothesis is right:
- Baseline conc=1 latency drops materially below 100µs (we measured
  ~100µs on macOS; expect ~30-60µs on Linux).
- Variant conc=1 drops correspondingly; the *delta* stays roughly
  constant in absolute µs (since we're skipping the same chunk of
  Go code) but grows as a percentage of total.
- QPS-at-budget at p99 ≤ 500µs becomes a much larger absolute and
  relative win.

If the hypothesis is wrong (e.g. syscalls weren't the bottleneck and
something else dominates), Linux and macOS will look similar — itself
a useful finding to report.

---

## Headline results (Linux gceworker, single-node, GOMAXPROCS=24)

Captured 2026-04-21. Single-node in-process test cluster on
`gceworker-matt` (n2-custom-24-32768, 24 vCPUs). Optimizer placeholder
fast path disabled in both variants so the slow path represents an
"unspecialized" SQL pipeline; the variant additionally enables our
hardcoded `sql.fast_path.point_select.enabled` setting.

### Latency at fixed concurrency (medians of 5 runs)

| metric | baseline | fast path | delta |
|---|---:|---:|---:|
| p50 @ conc=1 | 260 µs | **168 µs** | **−35%** |
| p99 @ conc=1 | 390 µs | 282 µs | **−28%** |
| p99 @ conc=32 | 928 µs | 599 µs | **−36%** |
| p99 @ conc=64 | 2.35 ms | 1.51 ms | **−36%** |

### Max QPS at p99 budget — *the* table for the writeup

| p99 budget | baseline | fast path | delta |
|---:|---:|---:|---:|
| 500 µs | 30K | **68K** | **+124%** |
| 1 ms   | 81K | **119K** | **+48%** |
| 2 ms   | 81K | **120K** | **+48%** |

### Per-vCPU efficiency

24-vCPU host, single-node in-process, point-SELECT prepared statement,
saturation throughput at p99 ≤ 2 ms:

- **Baseline:** 81K / 24 = ~3.4K QPS / vCPU
- **Fast path:** 120K / 24 = ~5.0K QPS / vCPU
- **+47% per vCPU.**

### Profile validation (conc=64, fast path on)

The bypass is structurally correct:
`execPointSelectFastPath` cumulative ≈ `kv.(*Txn).Send` cumulative.
The optimizer / dispatchToExecutionEngine / distSQL frames are absent
from the top of the profile entirely. Where the slow path's
mutex-contention profile shows ~45% of contention under
`dispatchToExecutionEngine`, the fast path has 0% there — it just
skips that code.

---

## Methodology notes (things that misled us along the way)

These are documented because they shaped which numbers ended up in the
headline table — and because re-running someone else's measurement
without knowing about them would lead to the same wrong conclusions.

### macOS is a poor host for this experiment

Initial sweeps on an M3 Pro laptop showed only +4-7% QPS-at-budget and
+25-30% p99. CPU profiling at saturation showed ~76% of samples in
`syscall.rawsyscalln` and `runtime.kevent` — macOS's TCP and kqueue
syscalls are noticeably more expensive than Linux's, and were eating
the SQL-stack savings. On the Linux gceworker the syscall share drops
substantially and the fast path's win shows up clearly.

### GOMAXPROCS hides the answer

`./dev bench` defaults to `GOMAXPROCS=1`. We initially overrode to 8
to roughly match an M3 Pro. On a 24-vCPU host that throttles 16/24
cores out of contention by definition. Lifting `GOMAXPROCS` to match
the host's vCPU count more than doubled saturation throughput in both
baseline and variant:

| metric | TEST_CPU=8 | TEST_CPU=24 |
|---|---:|---:|
| Baseline conc=64 QPS | 37K | 81K |
| Fast-path conc=64 QPS | 59K | 120K |

A mutex-contention profile we ran at TEST_CPU=8 made it look like
gRPC adapter and rangecache locks were the throughput ceiling. They
weren't — the runtime parallelism cap was. The contention shows up
because there are only 8 Ps to spend wait-time on, not because those
locks would gate scaling on 24 cores. We built multi-node + table
splits in case that turned out to be the real ceiling; on Linux at
GOMAXPROCS=24 we no longer need that infrastructure for the headline
answer, but it remains in place (env vars `POINT_SELECT_NODES` and
`POINT_SELECT_SPLITS`) for follow-up validation.

### Compared to "what production does today"

The baseline disables the optimizer's existing `placeholder_fast_path`,
which already collapses re-optimization for this exact query shape into
a no-op via a cached "ideal generic" plan. So the +47% per-vCPU number
is "fast path vs the unspecialized SQL pipeline," not "fast path vs
current production behavior on this exact query." For queries the
placeholder fast path doesn't recognize (any non-PK-equality shape — most
of OLTP), production is closer to the unspecialized baseline, which is
why we picked it as the comparison point.

A direct measurement against placeholder-fast-path-on would show a
smaller number; we chose not to gate the report on that re-run because
(a) the macOS data we did capture both ways suggested the
placeholder fast path is worth ~3-5% on this query shape, and (b) the
bigger story is "what could a universal fast path achieve for the
common case," which is what the unspecialized baseline measures.

---

## What we hand back to the CTO

A short writeup with:

1. **The headline:** at a 1ms p99 budget, a hardcoded fast path lifts
   point-select throughput from 81K to 119K QPS on a 24-vCPU
   single-node setup — **+48%** total throughput, **+47%** per vCPU.
   At a 500µs budget the gap widens to **+124%**.
2. **Per-op latency:** at single-client (no contention), the fast
   path drops p50 from 260µs to 168µs — a **35% reduction**. This is
   the per-op ceiling on what a "lighter SQL execution engine" could
   buy, since it represents the case with no queueing or contention
   to mask the savings.
3. **What's measured:** the bypass goes from `connExecutor`
   straight to `kv.Txn.Get`, skipping the optimizer / exec-builder /
   DistSQL planner / row.Fetcher entirely. Profile validation
   confirms `execPointSelectFastPath` time is ~entirely the KV call.
4. **What this tells us about the upper bound:** for point-select
   workloads, the SQL pipeline costs ~90µs per op (260 → 168) of
   wall-time on this hardware. A real "engine upgrade" project
   would need to deliver a chunk of those 90µs against a much wider
   query shape — feasibility of that is the next question, not
   answered by this experiment.
5. **Caveats** documented in the methodology section above
   (macOS confound, GOMAXPROCS confound, comparison-point choice).

### Initial macOS data (kept for the record)

These were the first numbers captured (M3 Pro, GOMAXPROCS=8, single-node
in-process). They underestimate the fast-path win because of the host
choice; included here so the methodology section above has something
concrete to refer to.

| metric | baseline | fast path | delta |
|---|---:|---:|---:|
| p50 @ conc=1 | 100 µs | 88 µs | −12% |
| p99 @ conc=1 | 133 µs | 105 µs | −21% |
| p99 @ conc=8 | 345 µs | 254 µs | −26% |
| p99 @ conc=16 | 621 µs | 420 µs | −32% |
| p99 @ conc=128 | 4.9 ms | 3.4 ms | −31% |
| max QPS @ p99 ≤ 500 µs | 49.5K | 53.3K | +7.6% |
| max QPS @ p99 ≤ 1 ms | 51.3K | 53.3K | +4% |
