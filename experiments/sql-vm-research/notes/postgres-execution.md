---
author: assistant (Claude)
date: 2026-04-24
---

# Postgres execution model — research notes

First entry in the state-of-the-art survey for the execution-engine
daydream. Postgres goes first because (a) it is where our tier-1 OLTP
workloads land if we lose them, and (b) it made one structural choice
worth dissecting: the planner's `Plan` tree is the executable artifact;
there is no exec-builder phase between planner and executor.

## 1. Plan cache and the per-execute / per-prepare split

Prepared-statement state lives in two reference-counted objects defined
in `src/include/utils/plancache.h`:

- **`CachedPlanSource`** (lines 103–145) — per-prepared-statement,
  lifetime = the prepared statement. Owns the parse tree
  (`raw_parse_tree`, `analyzed_parse_tree`), the rewritten `query_list`,
  parameter type info, dependency tracking (`relationOids`,
  `invalItems`, `search_path`), the source `MemoryContext`, and adaptive
  costing state (`generic_cost`, `total_custom_cost`,
  `num_custom_plans`, `num_generic_plans`). It caches a pointer to the
  most recent generic plan in `gplan` (line 129).
- **`CachedPlan`** (lines 148–165) — the executable artifact.
  `stmt_list` is a list of `PlannedStmt` nodes; the struct is
  reference-counted (`refcount`), invalidation-tracked (`is_valid`,
  `saved_xmin`), and lives in its own `MemoryContext` so it can be
  dropped wholesale.

`GetCachedPlan` (`src/backend/utils/cache/plancache.c:1370`) is the
reuse decision point:

1. `RevalidateCachedQuery` (line 1395) checks dependencies (table OIDs,
   functions, search path); if anything has changed it re-parse-analyzes,
   otherwise the parse tree is reused as-is.
2. `choose_custom_plan` (line 1199, called at 1398) decides
   generic-vs-custom. Per the docs
   (<https://www.postgresql.org/docs/current/sql-prepare.html>), "the
   first five executions are done with custom plans and the average
   estimated cost of those plans is calculated. Then a generic plan is
   created and its estimated cost is compared to the average custom-plan
   cost." Override via `plan_cache_mode`
   (`auto`/`force_generic_plan`/`force_custom_plan`).
3. If generic, `CheckCachedPlan` (line 978) reacquires locks and
   verifies `is_valid`; on success the plan is returned with
   `refcount++`. Otherwise `BuildCachedPlan` (line 1087) re-runs the
   planner.

For an `EXECUTE` that hits the generic-plan fast path, the per-execute
allocations (from `ExecuteQuery` in
`src/backend/commands/prepare.c:128`) are: an `EState` if there are
bound params (lines 167–171), a `Portal` (line 181), the
`ParamListInfo` (line 224), the per-query `MemoryContext` inside the
`EState` (`standard_ExecutorStart`, `src/backend/executor/execMain.c:149`),
and the `PlanState` mirror of the cached `Plan` tree built by
`ExecInitNode`. Everything else — parse tree, rewritten query, `Plan`
tree, dependency lists — is reused by pointer.

**Direct answer to the load-bearing question:** the `Plan` tree is *not*
walked directly by the executor. Postgres still constructs a parallel
`PlanState` tree per execute. But that walk is mechanical and
one-to-one — there is no intermediate physical-operator representation,
no flow spec, no DistSQL-equivalent. The `Plan` tree the planner
produced is the input to a single recursive `ExecInit*` pass.

## 2. `Plan` node structure and execution model

`Plan` (`src/include/nodes/plannodes.h:178–228`) is the common base for
~40 subtypes (`SeqScan`, `IndexScan`, `NestLoop`, `HashJoin`, `Agg`,
`Sort`, `Gather`, `ModifyTable`, …). Each `Plan` carries `targetlist`,
`qual`, `lefttree`/`righttree`, costing fields, and parallel-safety
flags. Plans are immutable after planning and stored in the
`CachedPlan`'s context.

`PlanState` is the per-execute mutable mirror. Dispatch from `Plan` to
`PlanState` is in `ExecInitNode`
(`src/backend/executor/execProcnode.c:133`): one switch on
`nodeTag(node)`, one case per subtype, each calling a node-specific
initializer that allocates the `PlanState` subtype, opens any needed
relations/index scans, and recurses into children. Per-execute cost is
~one `palloc` per plan node plus per-node setup. For an OLTP
point-select with 1–3 plan nodes this is sub-microsecond work — the
structural equivalent of CRDB's exec-builder, but dramatically lighter
because the input is already an executable shape.

Per-tuple iteration is **pull-based Volcano**, with one optimization
worth borrowing: each `PlanState` holds an `ExecProcNode` function
pointer set at init time
(`src/backend/executor/execProcnode.c:356, 379–410`). The first call
goes through `ExecProcNodeFirst`, which installs either the real node
function or an instrumentation wrapper; subsequent calls dispatch
through the pointer directly — no per-tuple `switch`-on-`nodeTag`.

`MemoryContext` is the other big lever. `standard_ExecutorStart`
creates `estate->es_query_cxt` and switches into it; everything
allocated for the execution lands there. `standard_ExecutorEnd` (lines
467–472) calls `FreeExecutorState`, which destroys the entire context
in O(few `free()`s) regardless of object count. CRDB pays Go-GC tracing
cost for the analogous allocations; arena-style allocation in Go would
be a meaningful change but a fraught one (escape analysis, lifetime
discipline).

## 3. JIT (since v11)

JIT flags (`src/include/jit/jit.h:10–14`) are exhaustive:

- `PGJIT_EXPR` — expression evaluation (`WHERE`, target lists,
  aggregate transitions, projections).
- `PGJIT_DEFORM` — tuple deforming (on-disk row → `Datum[]`,
  specialized to the relation's column layout).
- `PGJIT_PERFORM`/`PGJIT_OPT3`/`PGJIT_INLINE` — meta-flags controlling
  whether to JIT, whether to apply LLVM `-O3`, and whether to inline.

The conventional wisdom is correct: that's the entire list. The
operator tree itself is not compiled; per-tuple dispatch is the function
pointer from §2. There is no Umbra-style whole-pipeline fusion.

Activation is set in `standard_planner` based on plan cost:

```c
if (jit_enabled && jit_above_cost >= 0 &&
    top_plan->total_cost > jit_above_cost) {
    result->jitFlags |= PGJIT_PERFORM;
    if (top_plan->total_cost > jit_optimize_above_cost) /* PGJIT_OPT3 */
    if (top_plan->total_cost > jit_inline_above_cost)   /* PGJIT_INLINE */
}
```

Defaults: `jit_above_cost = 100000`, both higher thresholds = 500000.
OLTP point-selects sit several orders of magnitude below the floor, so
**JIT does not engage on OLTP under default config** — confirmed.
JIT is structurally not part of any OLTP cost story we need to model.

The well-known footgun
(<https://www.postgresql.org/docs/current/jit-decision.html>): "these
cost-based decisions will be made at plan time, not execution time.
This means that when prepared statements are in use, and a generic
plan is used … the values of the configuration parameters in effect at
prepare time control the decisions, not the settings at execution
time." Combined with `auto` flipping to a generic plan after five
executes, an analytics-shaped prepared statement JIT-compiles once on
that switchover and pays compile latency on that single execute. The
`pganalyze` writeup of this is the canonical reference.

Forward reference: what Postgres JIT explicitly does *not* do, and
Umbra does, is fuse the operator pipeline into one compiled function.
Section deferred until Umbra.

## 4. Comparison to CockroachDB

Side by side, per-execute on a steady-state prepared statement
(Postgres generic plan; CRDB without `placeholder_fast_path`, i.e.
typical secondary-index OLTP):

| Phase                     | Postgres                              | CRDB                                                              |
| ------------------------- | ------------------------------------- | ----------------------------------------------------------------- |
| Parse / parse-analyze     | Cached on `CachedPlanSource`          | Cached on `PreparedStatement`                                     |
| Optimize                  | Skipped (`CachedPlan.gplan`)          | Skipped (memo cached) — `makeOptimizerPlan`                       |
| Logical→physical          | **Does not exist as a phase**         | `runExecBuild` builds a `planNode` tree from the memo             |
| Distributed-flow planning | N/A                                   | `DistSQLPlanner.NewPlanningCtx` + `setUpForMainQuery`             |
| Per-execute exec state    | `EState`+`Portal`+`PlanState` (1 palloc/node, 1 switch) | `DistSQLReceiver`, processor specs, `row.Fetcher`, flow goroutines |
| Per-execute allocator     | One `MemoryContext`, freed wholesale  | Go heap, traced by GC                                             |
| Per-tuple dispatch        | Function-pointer Volcano              | Vectorized batches *or* row engine; processor `Run` loops         |

Entry point in CRDB is `dispatchToExecutionEngine`
(`pkg/sql/conn_executor_exec.go:2926`), which calls `makeExecPlan`
(line 3511) and then `execWithDistSQLEngine` (line 3661). What
Postgres skips per execute that CRDB does not:

- **Exec-builder.** `makeExecPlan` re-walks the optimizer's memo to
  produce a fresh `planNode` tree. Postgres's planner output *is* that
  tree.
- **DistSQL physical planner.** `execWithDistSQLEngine` constructs
  `planCtx`, processor specs, and a `DistSQLReceiver` per execute.
  Postgres has a `Portal` and an `EState`.
- **Per-execute `row.Fetcher` init.** Postgres's `IndexScanDesc` is a
  few-field struct. CRDB's `Fetcher` carries column-family decoding
  state, tracing hooks, and the `ColBatchScan`/`Fetcher` abstraction
  split.
- **GC pressure.** Per-execute Postgres allocations are wiped by
  context-destroy; CRDB's are traced.

Disentangling: the gap is mostly (a) **Postgres caches more of the
post-optimization artifact** and (b) **Postgres has fewer translation
phases** — the `Plan` tree *is* the cached executable. (c) memory
contexts vs Go GC and (d) single-process vs distributed-by-default
matter, but the headline is structural caching depth and translation
count, not allocator or distribution.

The point-select fast path measured (a)+(b)+(d) with the allocator
held constant; the 47–66% per-vCPU win is largely that bucket.
Adopting the Postgres pattern in CRDB would need:

1. Caching the post-exec-build artifact, keyed on the same descriptor-
   version invalidation criteria the optimizer cache already uses.
2. Either collapsing exec-build + DistSQL planning into a single phase
   that produces a directly-executable artifact, or caching a
   distributed-plan template and re-binding parameters per execute.
3. Some form of arena-style allocator for per-execute scratch state to
   recover the GC fraction.

None of these is small. (1)/(2) reach into the optimizer/exec-builder
boundary and the DistSQL planner; (3) is a cross-cutting Go concern
attempted unevenly elsewhere (sync.Pool, `pkg/util/intsets`).

## 5. What to borrow / not borrow

**Borrow:**

- **The two-tier cache shape.** `CachedPlanSource` (parse + dependency
  metadata) vs `CachedPlan` (executable artifact, droppable in one
  shot). CRDB's `PreparedStatement` is the first tier; the second is
  what is missing.
- **The "cached artifact is one walk from execution" model.** Even if
  CRDB keeps a richer physical representation, the cached artifact
  past the optimizer should be one mechanical pass away from running,
  not three.
- **The `ExecProcNodeFirst` install-once function pointer.** Strictly
  cheaper than CRDB's per-call interface dispatch, with no downside.
- **Adaptive generic-vs-custom plan selection** (the five-execute
  heuristic and `plan_cache_mode`). `placeholder_fast_path` is the
  generic side; CRDB has no symmetric custom-fallback for
  parameter-skewed cases, and would want one before caching more
  aggressively.

**Don't borrow:**

- **Volcano row-at-a-time as the only shape.** Vectorized execution is
  the right default for analytics; the borrow target is the dispatch
  model, not the data model.
- **JIT in the Postgres style.** Bolt-on LLVM for expression eval and
  tuple deforming is a small win on a 2010 architecture. If we don't
  do whole-pipeline compilation, JIT is not worth the build/deploy
  complexity.
- **Single-process executor as a structural commitment.** The reason
  Postgres's per-execute is light is the same reason it can't
  parallelize across nodes; we want the lightness without surrendering
  distribution.

---

**Open questions / followups:**

- Quantify per-execute `PlanState` build cost in absolute µs on the
  point-select hardware. "Sub-microsecond" is asserted, not measured.
- Compare Postgres's invalidation (`relationOids` + `saved_xmin`) to
  CRDB's descriptor-version check; is there a correctness gap either
  way?
- The `MemoryContext` arena story deserves its own note when the
  implementation-strategy section comes around — Go has no first-class
  equivalent and every workaround has failure modes.
