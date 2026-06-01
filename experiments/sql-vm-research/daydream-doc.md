# A Hypothetical CockroachDB SQL Execution Engine

> **Status:** sections 1 and 2 drafted from existing artifacts. Sections
> 3-7 are stubs awaiting research. See *Phase plan* at the bottom for
> what each section becomes when it lands.

---

## 1. Problem statement

CockroachDB's SQL execution layer pays per-execute costs that scale
with the number of operators in an optimized plan rather than with
the amount of work the query actually does. Phase 1 of this
investigation (see `../point-select-fast-path/SUMMARY.md`) measured
those costs for four representative OLTP shapes and showed that
bypassing the post-optimization pipeline entirely — exec-builder,
DistSQL physical planning, operator-tree construction, `row.Fetcher`
init — recovers between **+47% and +66% per-vCPU throughput** on a
24-vCPU host, and grows with query complexity rather than shrinking.
The save is structural; the per-op pipeline cost has to be paid every
time today, even on prepared statements where the optimizer's output
is fully cached.

That experiment is a SWAG, not a design. It hardcodes four query
shapes with a manual key-encoder and a manual KV-Get sequence, and
skips production-mandatory work (per-statement metrics, EXPLAIN
hooks, observability). The real question is whether a generalizable
execution layer — one that can take *any* prepared statement and run
it with comparable per-execute overhead, while preserving the
production contracts the fast path skipped — is buildable, and what
it would look like.

The strategic context is **expansion within existing accounts, not
new-logo wins.** Most of our customers run CockroachDB for tier 0
(mission-critical, hard-distributed-SQL) workloads. Those same
customers have tier 1 workloads — business-critical-but-not-
mission-critical OLTP — where they'd *prefer* to keep using us
because operating a single database is easier than running two.
Today CockroachDB's per-query cost pushes those tier 1 workloads
onto a cheaper second product (Postgres / Aurora / equivalent
single-region OLTP engine). The Phase 1 finding is that an
unspecialized OLTP query in CockroachDB pays meaningful per-execute
SQL processing overhead that has no analog in Postgres-class
engines; reducing it is part of what would make tier 1 expansion
viable inside accounts where we've already won tier 0.

The bar is **justifiable**, not **cheapest**. Total cost of operating
a database includes much more than per-query CPU — operational
simplicity (the very thing that makes consolidating onto CRDB
attractive), support, multi-region value, etc. — and a separate set
of cost-to-serve reduction efforts is already in flight elsewhere in
the org. The engine rewrite needs to be a credible contribution to
that broader story, not the lone solution. Concretely, that means
preserving the distributed correctness, observability, and
compatibility properties that justified our presence in tier 0,
while delivering per-vCPU OLTP throughput improvements large enough
to be load-bearing in the overall pitch.

### Constraints we will hold fixed

- **Stays in Go.** Rewriting in another language is technically
  possible but a hard organizational sell.
- **Coexists with the current engines.** `colexec` and `rowexec` keep
  shipping; the new engine adopts query shapes incrementally and
  falls through to the existing path on anything unrecognized. The
  Phase 1 fast path's dispatch model is the obvious template.
- **Allocation discipline is a soft target.** Go's GC is one of the
  largest single costs in the system at OLTP scale. The realistic
  goal is a per-query arena/scratch model where intermediate state
  lives in a pre-allocated context passed by pointer, not where the
  engine literally never allocates.

### Audience and scope of this document

This is an internal research note, not a public design RFC. It
assumes the reader knows CockroachDB internals — `connExecutor`,
`colexec`, `rowexec`, the optimizer/exec-builder split, DistSQL
physical planning. It does *not* re-litigate Phase 1,
network/pgwire/KV overhead, or optimizer-side changes.

---

## 2. What Phase 1 told us

The full writeup lives at `../point-select-fast-path/SUMMARY.md`.
The findings that load-bear on this document:

### The pipeline cost is downstream of the optimizer

For a prepared statement, the optimizer's per-execute cost is
already amortized: `placeholder_fast_path` caches a fully-optimized
"ideal generic plan" and re-uses it across executes with near-zero
overhead. The 87% of the Phase 1 win that *isn't* attributable to
optimizer caching lives in:

1. Exec-builder constructing a fresh operator tree per execute
2. DistSQL physical planner producing a fresh distributed plan per
   execute (even for "local" plans — the same machinery runs)
3. `row.Fetcher` initialization per execute
4. `DistSQLReceiver` setup per execute
5. Per-execute `connExecutor` bookkeeping (active-query map,
   statement counters, phase timings)

These are the layers the new engine has to replace, cache, or skip.

### The win grows with query complexity, not shrinks

Naive expectation: the bypassed pipeline cost is roughly fixed, so
its percentage shrinks against more KV work. Observed: pipeline cost
itself scales with the number of operators in the optimized plan.
Absolute µs saved at conc=1, by shape: 92 (PK, 1 Get) → 79
(covering secondary, 1 Get) → 130 (non-covering, 2 Gets) → 147
(two-table join, 3 Gets). The two multi-Get shapes — which together
cover most real prepared OLTP — are exactly where the win is largest.

This is the most important framing for the rewrite case: **a leaner
engine's payoff is largest on the queries we care about most**, not
on toy queries that overstate the benefit.

### What's already proven feasible

The Phase 1 fast path demonstrated, in shipping code, that:

- Per-prepared-statement detection and caching works (a `*prep.Statement`
  carries a fast-path plan struct, populated lazily, validated against
  descriptor version on EXECUTE).
- A direct `kv.Txn.Get` can replace the entire row.Fetcher /
  DistSQLReceiver / pgwire-emit chain while remaining
  byte-for-byte compatible with the slow path's results.
- Multi-Get shapes (non-covering secondary, two-table lookup join)
  are tractable as a sequence of staged Gets with intermediate
  decoding, without bringing back a generalized operator tree.

The gap the rewrite has to close is generality: the fast path covers
4 hardcoded shapes with INT8 placeholders, single column families,
no FOR UPDATE / AOST / RC / EXPLAIN / metrics. A real engine has to
broaden the shape coverage *and* re-add the production contracts,
without giving back the per-execute savings.

### What Phase 1 explicitly did not address

These are the open questions that motivate Phase 2:

- How does the win evolve as we re-add production-mandatory work?
  The Phase 1 number is an upper bound that hasn't paid for
  observability, statement statistics, EXPLAIN, tracing, or metrics
  propagation.
- How much of the savings comes from skipping per-execute *allocation*
  vs. per-execute *computation*? Phase 1 doesn't separate these — it
  skips both. A new engine that skips computation but not allocation
  would underperform.
- What's the structural form of "the cached execution program"?
  Phase 1 cached a `pointSelectFastPath` struct (parameters + a
  hardcoded executor function). Generalizing requires either a
  bytecode + interpreter, a per-statement codegen artifact, or
  pre-instantiated operator graphs — each with very different
  trade-offs.
- Multi-node distributed execution. Phase 1's queries all produced
  single-node DistSQL plans. The new engine has to decide whether
  it ever distributes.

These are sections 4-6 of this document.

---

## 3. State of the art

> **Status:** stub. Phase A of the research plan.

This section will compare modern execution engines along the axes
that matter for this decision. Not a literature survey for its own
sake — a focused catalogue of techniques to potentially borrow.

Systems to cover (depth varies):

- **Postgres** (incl. Aurora) — the OLTP engine our tier 1
  workloads probably land on if we don't keep them. Notable as
  a design reference because the cached executable artifact
  (`CachedPlan.stmt_list` of `PlannedStmt`/`Plan` nodes) is one
  mechanical `ExecInitNode` pass away from running — one switch
  on `nodeTag`, one `palloc` per plan node, no DistSQL-equivalent
  flow construction, no exec-builder rebuild from a memo. CRDB's
  per-execute pipeline does materially more work for the
  structurally-equivalent path. Memory-context cleanup is the
  other big lever (the entire per-execute allocation set is freed
  in O(few `free()`s) regardless of object count, vs Go-GC tracing
  cost for the analogous CRDB allocations). The v11+ LLVM JIT is
  a separate, smaller story (expression eval + tuple deforming
  only; default thresholds put it well above OLTP plan costs, so
  it doesn't engage on the workload class we care about). Full
  notes in `notes/postgres-execution.md`.
- **MySQL / InnoDB** — iterator-based execution, no codegen.
  Useful as a second OLTP-engine reference point.
- **Clustrix / MariaDB Xpand** — distributed C database with a
  Cascades optimizer. Three properties stand out: (a) every query
  is autoparameterized at parse time, so the post-optimization
  cache helps *all* SQL traffic, not only explicitly-prepared
  statements; (b) the cache contract is a uniform
  `(program, parameter_vector)` interface — the executor never
  sees raw plans; (c) the execution substrate is a bytecode VM
  with native code layered in via a single `VM_NATIVE` opcode,
  not a separate JIT path. The per-execute work past a cache hit
  collapses to "look up program; invoke with params" — strictly
  less per-execute work than even Postgres. Distributed
  fragment-shipping is part of the system but explicitly out of
  scope for our v1; flagged because the architecture is
  hospitable to it. Full notes in `notes/clustrix-xpand.md`.
- HyPer / Umbra (data-centric LLVM JIT; Umbra adds Flying Start
  adaptive interpretation+compilation)
- Photon (Databricks; vectorized C++ that displaced Spark's JVM
  whole-stage codegen — the writeup includes their reasoning for
  *not* doing JIT)
- Velox (Meta; vectorized C++; the most industrially-adopted
  modern engine)
- DuckDB (vectorized C++, no JIT — counterfactual)
- ClickHouse (vectorized C++ + optional LLVM JIT for expressions —
  the hybrid)
- Spark Tungsten / Whole-Stage Codegen (JVM bytecode at query
  time; closest analog to a hypothetical Go equivalent)
- Snowflake / Redshift / BigQuery / F1 Lightning (whatever's been
  publicly disclosed)
- TiDB (Go-hosted, vectorized, no codegen — the closest cousin
  technologically; what they tried and dropped is valuable)

**Output:** a comparison table (rows = engines, columns = execution
model, dispatch granularity, specialization strategy, memory model,
target workload) plus 2-3 paragraphs of "what we should and shouldn't
borrow."

Reading order:
1. Postgres prepared-statement / plan-cache architecture (the
   structural-cost reference; source + docs are canonical)
2. Neumann 2011 (data-centric codegen origin)
3. Kohn/Leis/Neumann 2018 (Umbra Flying Start)
4. Zukowski's MonetDB/X100 dissertation or VLDB 2005 paper
5. Photon paper, SIGMOD 2022
6. Velox paper, VLDB 2022
7. Postgres v11 LLVM JIT design + the public postmortems

Per-paper / per-system notes go in `notes/`.

---

## 4. Go-specific design space

> **Status:** stub. Phase B of the research plan.

Takes the techniques from Section 3 and evaluates which can be done
in Go, with what cost. This is where the "compile to native and
dynamically load" idea gets a concrete examination.

Codegen options to evaluate, each with a worked example, an
allocation analysis, and a kill-criterion:

| Option | What it is |
|---|---|
| Bytecode VM | Compile to opcodes at PARSE time, interpret with a tight switch |
| Templated specialization (extend `execgen`) | Build-time codegen of Go for known shapes |
| wazero WASM | Compile to WASM at PARSE time, wazero JITs to native at load |
| `plugin` AOT | Generate Go source per prepared stmt, `go build -buildmode=plugin`, `dlopen` |
| cgo to generated C | Generate C, compile, dlopen, call via cgo (one transition per query) |
| Hand-rolled Plan9 asm for inner loops | What `runtime/internal` does |

Go-runtime questions to dig into:

- wazero call boundary cost in 2026; goroutine preemption +
  GC-pause interaction; memory budgeting.
- Current state of `plugin` (was abandoned-feeling last I checked).
- Whether escape analysis can keep an entire query context on the
  stack; the prepared-statement contract that would enable that;
  prior art in CRDB hot paths or pebble.
- Existing arena/pool patterns in CRDB to borrow rather than
  reinvent (`colmem.Allocator`, `valueside`, `treeprinter`, etc.).

---

## 5. Allocation discipline

> **Status:** stub. Phase C of the research plan.

Independent of codegen, what does an allocation-disciplined Go
execution engine look like?

Topics:

- Per-query context struct at the root of the call frame (the
  user's daydream). API shape that lets downstream operators
  bump-allocate from it; how to handle slice growth without escape.
- Pre-sized result buffers at PARSE time when the schema is known;
  avoiding `coldata.Batch` resize-on-overflow.
- Reusable per-prepared-statement state — the post-optimizer plan
  cache that Phase 1 implicitly motivated. Fully baked operator tree
  + scratch buffers attached to `prep.Statement`, taken under
  exclusive use during EXECUTE.
- What the GC actually costs us today — quantitative reference
  (production GC trace, microbench) before speculating.
- Patterns that survive Go's non-generational GC vs ones that
  fight it.

---

## 6. CRDB integration sketch

> **Status:** stub. Phase D of the research plan.

The new engine slots in *where*, coexists *how*?

- Dispatch model — likely analogous to Phase 1: per-prepared-statement
  detection at PARSE time, cached eligibility, runtime bypass with
  fallthrough.
- Initial query-shape coverage. v0 = Phase 1's four shapes (we
  already know they work); what's v1, v2?
- Plan format — three concrete proposals: parsed bytecode, pointer
  to JIT-compiled function, struct of pre-instantiated operators.
  Trade-offs.
- Memory ownership — who allocates the per-execute context, who
  releases it, integration with `mon.BoundAccount`.
- Distributed execution — single bullet, not a workstream. State
  whether v1 is gateway-only or ever distributes.
- Observability — how each of EXPLAIN, tracing,
  `crdb_internal.execution_insights`, statement statistics get
  re-added without giving back the savings.

Output: 2-3 candidate architectures with explicit trade-offs. Not
picking one yet.

---

## 7. Recommendation + smallest-experiment proposal

> **Status:** stub. Phase E of the research plan.

Synthesize. Pick one architecture. Identify the single biggest
remaining unknown and propose the smallest experiment that would
resolve it before committing engineering effort.

The "smallest experiment" likely involves a focused microbench (not
a production prototype) pitting the proposed mechanism against an
honestly-tuned `colexec` baseline on one carefully chosen query
shape. *That* experiment then informs whether to commit real
engineering effort.

---

## Phase plan

The five research phases below produce sections 3-7 in order. Each
phase ends with the section drafted in this document, reviewed,
and explicitly green-lit before the next phase begins.

| Phase | Produces | Done when |
|---|---|---|
| A | Section 3 | Comparison table filled for every listed system; user signs off "I understand the landscape" |
| B | Section 4 | Each Go-codegen option has worked example + allocation analysis + kill-criterion; user picks 1-2 to take into D |
| C | Section 5 | Per-query-context proposal sketched as a Go interface; today's allocation cost has at least one quantitative reference (production GC trace or microbench) |
| D | Section 6 | 2-3 architectures sketched at system-diagram + paragraph level; trade-offs articulated in one sentence per pair |
| E | Section 7 | Recommended architecture, named smallest-experiment, effort estimate |

The "user signs off on this section" check at each phase boundary
is the primary protection against scope creep.

## Out of scope

- Production code changes. This document produces text.
- Optimizer-side changes. We're past the optimizer in everything
  here.
- Network / pgwire / KV-side overhead (covered in Phase 1; cited,
  not re-litigated).
- Distributed execution as a primary topic.
- Changing programming language.
- Picking bytecode vs. native vs. anything else before Phase A is
  done. The user has a bias; the survey may strengthen, weaken,
  or redirect it.
