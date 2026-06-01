# SQL Processing Overhead in CockroachDB: A Point-Select Experiment

> **TL;DR.** On a 24-vCPU host, replacing CockroachDB's SQL execution
> pipeline with a hardcoded fast path for prepared point-SELECT queries
> delivers per-vCPU throughput improvements between **+47% and +66%**
> across four distinct query shapes — primary-key lookup, covering
> secondary-index lookup, non-covering secondary-index lookup, and a
> two-table lookup join. The wins *grow* with query complexity (more
> pipeline machinery to bypass), not shrink. At a 1ms p99 budget, the
> two realistic-OLTP shapes (non-covering secondary index, two-table
> join) sustain **87K vs 36K QPS** and **74K vs 32K QPS** respectively
> — **+134-141% throughput**. Single-op latency reductions are 34-38%
> across all four shapes. This is an upper bound on what a "lighter
> SQL execution engine" could buy; the consistency across shapes
> suggests the savings are structural to the post-optimization
> pipeline, not a quirk of any single query.

## Why we asked the question

The goal is **cost to serve** — to know whether a future investment
in a leaner SQL execution layer could let us run the same OLTP
workload on materially smaller (cheaper) clusters. Concretely: if we
could shave per-op SQL overhead, what's the upper bound on the
hardware reduction that would unlock?

To answer that, we needed to estimate how much of a query's wall-time
and CPU is spent in SQL processing as opposed to KV storage,
networking, or other layers we wouldn't be touching with an
"execution engine" change.

## Assumptions

The experiment is structured to **isolate SQL's contribution to
query latency** by making everything else as cheap as we plausibly
can. The assumptions below all serve that goal; each is a knob we
deliberately set to suppress a confounder, not an attempt to model
production faithfully.

- **Single-node, in-process cluster.** Real KV deployments do
  cross-node Raft, network RPCs, and remote storage. We don't want
  any of that contributing to per-op latency in this measurement —
  whatever SQL spends per op is what we're trying to read.
- **Small dataset (100K rows) that fits comfortably in memory.**
  Same reason: real KV often pays disk I/O. We don't want that to
  be in the per-op cost we're attributing to SQL.
- **Prepared point-select against a primary key.** This is the most
  toy-shaped query we could pick — single-row read, single index,
  single column family. We picked it specifically because it has
  the **highest ratio of per-query overhead to actual work**, which
  makes the SQL stack's contribution easiest to see. It's also a
  shape we have historically considered important enough to write
  a fast path for (the optimizer's existing
  `placeholder_fast_path`), so it's not a contrived shape — it's a
  toy version of a real workload class.
- **Comparison point: unspecialized SQL pipeline.** Baseline runs
  with the existing `placeholder_fast_path` disabled. We're
  estimating what a *new, more general* fast path could buy on
  queries that don't already have a specialized path — which is
  most of OLTP. We also separately measured the headline query
  with `placeholder_fast_path` *enabled* (production's current
  behavior on this exact query) to put a number on how much of
  the win is already capturable by extending optimizer caching
  vs. requires the deeper post-optimization pipeline bypass. See
  "Comparison vs current production behavior" in Results.
- **The fast path is an upper bound on *post-optimization* pipeline
  cost, not on total SQL cost.** The optimizer's per-execute
  contribution to a *prepared* statement is largely amortizable —
  CRDB's existing `placeholder_fast_path` already caches a fully
  optimized "ideal generic plan" for our exact query shape and
  re-uses it across executes with near-zero per-call overhead. What
  the experimental fast path bypasses is everything *after* the
  optimizer: exec-builder, DistSQL physical planning,
  DistSQLReceiver/row.Fetcher initialization, and the connExecutor
  bookkeeping that wraps them. These run per-execute today even
  when the optimizer's output is fully cached. Our hardcoded fast
  path is, conceptually, a precompiled-and-cached *execution program*
  for one query shape — which is the same pattern engines like
  HotSpot / compiled-stored-procedure systems use to amortize this
  exact category of per-execute setup. The savings we measured are
  what *that* pattern, applied to CRDB's post-optimization pipeline,
  could buy.
- **The fast path skips production-mandatory work.** Per-statement
  metrics, sql stats, EXPLAIN/tracing hooks. Adding those back
  would erode some of the win, so the headline number is an
  over-estimate of what's shippable.

## Experimental setup

| | |
|---|---|
| Host | gceworker, n2-custom-24-32768 (24 vCPU, 32 GB), Ubuntu 24.04 |
| Cluster | Single-node, in-process test cluster (`serverutils.StartCluster`) |
| GOMAXPROCS | 24 (matches host vCPU count) |
| Schema | `kv (k INT8 PRIMARY KEY, v BYTES)`, single column family |
| Population | 100,000 rows, ~16-byte values, no pre-splits |
| Workload | Prepared `SELECT v FROM kv WHERE k = $1`, uniform random key |
| Client | pgx, one connection + prepared statement per goroutine |
| Concurrency sweep | 1, 8, 16, 32, 64, 128 client goroutines |
| Duration | 10s per sub-benchmark, medians of 5 runs reported |
| Baseline | Optimizer's `placeholder_fast_path` **off** |
| Variant | Optimizer's `placeholder_fast_path` **off** + new `sql.fast_path.point_select.enabled` **on** |
| Encryption | Off (`Insecure: true` — TLS measured separately, not the bottleneck) |

The experiment branch and reproducer scripts live at
`https://github.com/mw5h/cockroach/tree/point-select-fast-path-experiment`,
under `experiments/point-select-fast-path/`. Anyone with the gceworker
setup can reproduce with `./run-on-gceworker.sh sync && run run-baseline.sh && run run-fastpath.sh && fetch`, then `./summarize.sh`.

## Results

We measured four query shapes, each with the unspecialized SQL
pipeline as baseline (placeholder fast path disabled) and the
hardcoded fast path on as variant:

| shape | KV ops | description |
|---|:---:|---|
| **PK** | 1 | `SELECT v FROM kv WHERE k = $1` against a single-column primary key. The optimizer's existing placeholder fast path applies to this shape (and is separately measured below). |
| **Covering secondary** | 1 | `SELECT v FROM kv_idx WHERE k = $1` where the secondary index `(k) STORING (v)` includes the requested column. Optimizer uses the index directly. |
| **Non-covering secondary** | 2 | Same query, secondary index is `(k)` without `STORING`. The optimizer plans a lookup join: index Get → PK Get. **Common production pattern — most secondary indexes don't STORE every queried column.** |
| **Two-table join** | 3 | `SELECT s.v FROM first f JOIN second s ON f.fk_id = s.id WHERE f.k = $1`. Three Gets: `first`'s secondary index → `first`'s PK row (to read `fk_id`) → `second`'s PK row. |

### Per-vCPU throughput on a 24-vCPU host

| shape | baseline | fast path | **delta** |
|---|---:|---:|---:|
| PK                       | 3.4K QPS/vCPU | 5.0K QPS/vCPU | **+47%** |
| Covering secondary       | 3.5K QPS/vCPU | 5.2K QPS/vCPU | **+47%** |
| Non-covering secondary   | 2.3K QPS/vCPU | 3.8K QPS/vCPU | **+66%** |
| Two-table join           | 1.9K QPS/vCPU | 3.1K QPS/vCPU | **+63%** |

### Saturation throughput at typical p99 latency budgets

| shape \ budget | 500 µs | 1 ms | 2 ms |
|---|---|---|---|
| **PK** baseline → fast path           | 30K → **68K** (+124%) | 81K → **119K** (+48%)  | 81K → **120K** (+48%) |
| **Covering** baseline → fast path     | 51K → **69K** (+36%)  | 84K → **124K** (+47%)  | 84K → **125K** (+48%) |
| **Non-covering** baseline → fast path | 0 → **32K**  (n/a)    | 36K → **87K** (+141%)  | 54K → **92K** (+70%)  |
| **Join** baseline → fast path         | 0 → **29K**  (n/a)    | 32K → **74K** (+134%)  | 45K → **74K** (+63%)  |

(0 in the 500 µs cells means the unspecialized baseline can't fit a
single op under that budget at any concurrency; the fast path can.)

### Single-op latency (no contention) at conc=1

| shape | baseline p50 | fast path p50 | **delta** | baseline p99 | fast path p99 |
|---|---:|---:|---:|---:|---:|
| PK                       | 260 µs | 168 µs | **−35%** | 390 µs | 282 µs |
| Covering secondary       | 234 µs | 155 µs | **−34%** | 345 µs | 251 µs |
| Non-covering secondary   | 346 µs | 216 µs | **−38%** | 521 µs | 330 µs |
| Two-table join           | 385 µs | 238 µs | **−38%** | 602 µs | 334 µs |

### The wins grow with query complexity

The intuitive expectation was that the savings would shrink for
more complex queries: if the bypassed pipeline cost is roughly
constant, dividing it by a larger total (more KV work, more
decoding) gives a smaller percentage. The data goes the opposite
way — **the slow path's per-execute pipeline cost itself grows with
query complexity**.

A multi-Get plan exercises more of distSQL's physical planning,
more of the DistSQLReceiver setup, and more of the row.Fetcher's
per-batch coordination than a single-Get scan does. Every one of
those grows with the number of operators in the optimized plan,
not with the amount of actual KV work. The fast path skips all of
it; the absolute µs saved at conc=1 climbs from 92 (PK) → 79
(covering) → 130 (non-covering, 2 Gets) → 147 (two-table join, 3
Gets).

That's the load-bearing finding for the engine-rewrite case: the
toy PK measurement is not an artifact of an unrealistically simple
shape that overstates the win — it actually *understates* what
realistic OLTP shapes get. The multi-Get cases (non-covering
secondary, two-table join), which together cover most real-world
prepared OLTP queries, consistently land in the +63-66% per-vCPU
range.

### Comparison vs current production behavior (PK shape only)

CRDB already has a fast path for the *exact* PK lookup shape: the
optimizer's `placeholder_fast_path` caches a fully-optimized "ideal
generic plan" and skips re-optimization on each execute. The
secondary-index shapes don't qualify for this fast path, so for
those, "production today" is the unspecialized baseline already in
the table above. For PK, the picture is:

| metric | unspecialized | placeholder FP on | hardcoded FP on |
|---|---:|---:|---:|
| max QPS @ p99 ≤ 500 µs | 30K | 53K | **68K** |
| max QPS @ p99 ≤ 1 ms   | 81K | 86K | **119K** |
| p50 @ conc=1           | 260 µs | 253 µs | **168 µs** |

At the 1 ms budget, the existing optimizer caching captures only
**~5K of the +38K QPS gain (~13%)**. The remaining ~33K (~87%)
requires bypassing the post-optimization pipeline, which is what
the hardcoded fast path does. **A "make optimizer output caching
more aggressive" investment captures a small fraction of the
available win**; the bulk lives downstream of the optimizer (in
exec-builder, DistSQL physical planning, and per-execute
DistSQLReceiver / row.Fetcher initialization).

The 92µs p50 reduction is the wall-time the fast path saves by
skipping optimizer + exec-builder + DistSQL + row.Fetcher per op.
That is the per-op upper bound on what any future "lighter execution
engine" can buy on this query shape, on this hardware.

## What this experiment does and doesn't tell you

**Does:** estimate the upper bound on per-op SQL processing overhead
for the simplest possible OLTP shape, on a representative single-node
host, with all of the bypass an aggressive "engine upgrade" project
might attempt.

**Doesn't:**
- Tell you what fraction of that ~50% per-vCPU win is *realistically
  capturable* by a real, production-quality engine that has to
  support more than one query shape.
- Account for production-only work the fast path skips (per-statement
  stats, EXPLAIN, tracing, etc.). Adding those back will erode some
  of the win.
- Measure multi-node distributed-SQL behavior. The fast path
  short-circuits DistSQL by construction; whether real distributed
  point-selects see similar gains is a separate question.
- Address tail latency at very high contention (conc=128+) — the
  measurements get noisy past saturation and the headline numbers
  are taken from the saturated-but-stable region.

## Appendix

### How to reproduce
- Branch: `mw5h/point-select-fast-path-experiment`
- Reproducer: `experiments/point-select-fast-path/`
  - `run-baseline.sh` / `run-fastpath.sh` — concurrency sweep with pinned settings
  - `run-on-gceworker.sh` — wraps sync/run/fetch against the gceworker
  - `summarize.sh` — emits the comparison tables shown above
  - `PLAN.md` — design doc, methodology notes, full result history

### Methodology notes worth knowing
1. **macOS is a poor host for this experiment.** Initial runs on an
   M3 Pro showed only +4-7% QPS-at-budget. CPU profiling at saturation
   put ~76% of samples in `syscall.rawsyscalln`/`runtime.kevent` —
   macOS's TCP and kqueue syscalls were eating the SQL-stack savings.
   On Linux, the syscall floor is much lower and the fast-path win
   shows up clearly.
2. **GOMAXPROCS hides the answer.** `./dev bench` defaults to
   GOMAXPROCS=1; we initially set 8 (to match macOS effective cores).
   On a 24-vCPU host that throttles 16/24 cores out of contention by
   construction. Lifting GOMAXPROCS to match the host's vCPU count
   more than doubled saturation throughput in both baseline and
   variant. A mutex-contention profile we collected at GOMAXPROCS=8
   made gRPC adapter and rangecache locks look like the throughput
   ceiling — they weren't, the runtime parallelism cap was.
3. **Comparison point matters.** Baseline disables the optimizer's
   existing `placeholder_fast_path` so the slow path is the
   unspecialized SQL pipeline (what most OLTP queries pay). Comparing
   against placeholder-fast-path-on (current production behavior on
   *this exact* query shape) would show a smaller win — earlier macOS
   data suggested the placeholder fast path is worth maybe 3-5% on
   this shape. We chose the broader comparison because it speaks to
   the upper bound for "queries the engine could specialize for in
   the future."

### Profile validation

Mutex+CPU profile of the fast-path run at conc=64 confirms the
bypass is structurally correct: `execPointSelectFastPath` cumulative
time is ~entirely the `kv.(*Txn).Send` call. The optimizer,
`dispatchToExecutionEngine`, and DistSQL frames are absent from the
top of the profile. In the baseline, `dispatchToExecutionEngine`
accounts for ~45% of mutex contention; the fast path cuts that to
0% (because that code never runs).

### What's bypassed vs. what's preserved

**Bypassed by the fast path:**
- `connExecutor.dispatchToExecutionEngine`
- `makeExecPlan` (memo build, `chooseValidPreparedMemo`, optimization)
- `runExecBuild` (exec-builder)
- `execWithDistSQLEngine` (DistSQL planner, DistSQLReceiver, row.Fetcher)
- Per-statement metrics + sql stats updates that live inside the
  above
- EXPLAIN, tracing, instrumentation hooks

**Preserved (still runs in the fast path):**
- All of `execStmtInOpenState`'s setup before the dispatch hook:
  txn stepping, deadline updates, active-query registration,
  statement counters.
- Transaction's existing isolation/locking semantics
  (uses `ex.state.mu.txn` directly).
- Per-execute descriptor staleness check (compares
  `desc.GetVersion()` against the cached version; falls through
  to slow path on any change).
- pgwire result encoding (uses `RestrictedCommandResult.SetColumns`
  and `AddRow` exactly like the slow path).
