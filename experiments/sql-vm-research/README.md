# SQL Execution Engine Research

This directory holds research and design exploration toward a possible
CockroachDB SQL execution engine investment. The work is motivated by
the point-select fast-path experiment in
[`../point-select-fast-path/`](../point-select-fast-path/), which
measured an upper bound on what a leaner SQL execution layer could
buy on representative OLTP workloads.

This document is the entry point. It summarizes what we've learned,
sketches two candidate paths forward, and points to the deeper
artifacts for anyone who wants to follow the reasoning.

> **Current status (2026-05-14):** the four-system prior-art survey is
> complete (Postgres, MonetDB/X100, Clustrix, SQLite VDBE, TiDB/TiKV in
> [`notes/`](notes/)). The bytecode-VM-vs-Volcano-with-caching tradeoff
> is captured in [`discussions/`](discussions/). The two synthesis-style
> top-level docs ([`RESEARCH.md`](RESEARCH.md),
> [`daydream-doc.md`](daydream-doc.md)) were drafted by earlier
> assistant sessions and have not been reviewed by the project owner —
> they should be read as raw material, not as canonical positions. No
> architectural decisions have been committed to.
>
> Likely next moves: (a) the project owner's review of the two
> unreviewed synthesis docs, or (b) a scoped minimal VM prototype to
> test specific design assumptions before broader investment.

---

## What the fast-path experiment measured

The experiment hardcoded an end-to-end execution path for four
prepared OLTP query shapes (PK lookup, covering secondary lookup,
non-covering secondary lookup, two-table lookup join), bypassing the
normal SQL pipeline. On a 24-vCPU Linux host, the bypass delivered:

- **+47-66% per-vCPU throughput** across all four shapes.
- **+124% to +141%** at common p99 latency budgets (500µs and 1ms)
  on the multi-Get shapes.
- **34-38%** single-op latency reductions.

Crucially, **the win grew with query complexity** — absolute µs saved
per query rose from 92 (PK lookup, 1 KV Get) to 147 (two-table join,
3 KV Gets) — because the per-execute pipeline cost scales with
operator-tree size, not with KV work done.

The savings are structural. The hardcoded path skips: exec-builder
(operator tree construction per execute), DistSQL physical planning
(every execute, even for single-node "local" plans), `row.Fetcher`
and `DistSQLReceiver` init, and per-execute `connExecutor`
bookkeeping. All of this runs every execute today — even on prepared
statements where the optimizer's output is fully cached.

Full writeup: [`../point-select-fast-path/SUMMARY.md`](../point-select-fast-path/SUMMARY.md).

---

## The question this raises

The fast-path number is an *upper bound*, not a design. The hardcoded
path covers four shapes with a manual key encoder and skips
production-mandatory work — per-statement metrics, EXPLAIN, tracing,
statement statistics, observability hooks.

The general question:

> Can the per-execute SQL pipeline cost be eliminated *generally* —
> for any prepared statement, while preserving all the production
> contracts the fast path skipped — in production-shippable form?

This document explores two candidate paths.

---

## Two candidate approaches

Both approaches can capture the immediate Phase 1-style win. They
differ substantially in what they enable later.

### Approach A: Volcano with aggressive caching

Keep the iterator-based operator-tree substrate that underpins
`rowexec` and `colexec` today. Eliminate the per-execute rebuild by
caching the fully-instantiated operator tree on the prepared statement
(Postgres's `CachedPlan` pattern), and use install-once function-
pointer dispatch (Postgres's `ExecProcNodeFirst` pattern). Push
allocation discipline through the operator interface.

**Strengths**

- Familiar substrate; existing executor code mostly carries forward.
- Incremental — start by caching what's already being constructed.
- Observability (EXPLAIN, tracing) comes essentially for free.
- No new compiler; exec-builder is reused.
- Vectorization integration is natural (`colexec` stays as-is).

**Weaknesses**

- Allocation discipline cuts against the operator abstraction —
  operators are designed to be self-contained, so threading a
  per-query arena requires coordinated changes across many call
  sites.
- Adding native compilation later is a paradigm shift (separate JIT
  path, à la Postgres v11 LLVM, or whole-pipeline compilation à la
  HyPer).
- Eventual KV-side pushdown of execution work is hard — operator
  methods are not statically analyzable, so sandboxing is difficult.

### Approach B: Bytecode VM

Compile prepared statements to a small bytecode program at PARSE
time. Each program declares its register-file shape at compile time;
the executor binds one allocation per invocation. Programs run on a
fixed-size pool of long-lived worker goroutines whose stacks stay
bounded — the interpreter is a flat dispatch loop, not a chain of
recursive operator calls.

**Strengths**

- Compile-time register allocation is the cleanest available answer
  to the "no runtime allocations" goal — the program declares its
  scratch shape, the executor provides it.
- Bounded goroutine stacks open the door to a worker-pool execution
  model. Volcano operators recurse through Go's call stack;
  bytecode doesn't. Go stacks grow on demand and don't shrink back,
  so deep operator trees can leave goroutines with permanently large
  stacks — observed in production CRDB escalations.
- Sandboxable by construction — bytecode programs are finite,
  statically-analyzable opcode sequences. Resource bounds, opcode
  allowlists, and effect typing are mechanical. Relevant for any
  eventual KV-side execution pushdown.
- Native code can be added later as a single opcode (the Clustrix
  `VM_NATIVE` pattern) without changing the engine interface.
- The cache contract — programs identified by ID, invoked with a
  parameter vector — decouples cleanly from execution mechanics.
  Supports autoparameterization (extending the win to non-prepared
  queries) as a follow-on.

**Weaknesses**

- New substrate; team unfamiliarity.
- Need to build a plan-to-bytecode compiler. SQLite VDBE has 192
  opcodes; a CRDB equivalent is probably in the 150-250 range.
- Observability needs explicit design — bytecode is opaque without
  debug-info layers, though SQLite's `EXPLAIN` returning the
  bytecode listing is a known-good model.
- Vectorization integration is harder. Hosting both row-mode and
  vector-mode opcodes in one VM has real design cost.

### Orthogonal: data layout

A separable axis is **what data the engine processes** — rows,
vectors, or columnar batches. Both approaches above can in principle
host either; CRDB already has `colexec` for the vectorized path. The
known tension is that **vectorization does not help OLTP point-shaped
queries** (per-batch overheads don't amortize across single-row
results), so the workload class the fast-path experiment measured is
exactly where the extend-colexec path doesn't apply. Extending
colexec coverage is a real but related-and-separable workstream; it
does not displace the substrate question.

---

## Side-by-side

| Dimension | Volcano + caching | Bytecode VM |
|---|---|---|
| Per-execute pipeline cost | Eliminated | Eliminated |
| Allocation discipline | Plumbed, against the grain | Native, compile-time fixed |
| Goroutine stack / memory | Per-query stacks; grow and don't shrink | Bounded; worker-pool feasible |
| Observability | Self-describing operator tree | Needs explicit debug-info layer |
| Future KV pushdown | Hard; needs DAG-style retrofit | Easy; bytecode is sandboxable |
| Future native code | Paradigm shift | One additional opcode |
| Compiler | Reuse exec-builder | New codebase |
| Familiarity | High | Low |

---

## What this document does not resolve

- **Implementation cost.** "Lower risk" and "substantial complexity"
  are qualitative. Translating either path into engineering estimates
  needs design work neither has had.
- **Decomposition of the Phase 1 win.** The fast-path experiment
  skipped per-execute allocation, computation, and dispatch all
  together. The relative weights would inform which path captures
  more of the win at less complexity. The fast-path test code makes
  this measurable; it hasn't been measured.
- **The decision itself.** Approach A captures most of the immediate
  win at lower architectural risk. Approach B opens longer-term
  option value (KV pushdown, native compilation,
  autoparameterization). Whether the option value is worth the
  near-term complexity is a judgment the prior art in this directory
  does not resolve.

---

## Read more

In rough order of accessibility:

- [`discussions/bytecode-vs-volcano.md`](discussions/bytecode-vs-volcano.md)
  — the load-bearing tradeoff analysis behind the two-approaches
  section above. Status: open.
- [`notes/`](notes/) — per-system deep dives, with source/paper
  citations:
  - [`postgres-execution.md`](notes/postgres-execution.md) — plan
    cache architecture, `ExecInitNode`, JIT scope.
  - [`clustrix-xpand.md`](notes/clustrix-xpand.md) — distributed
    bytecode + native VM with autoparameterization; firsthand
    description.
  - [`monetdb-x100.md`](notes/monetdb-x100.md) — vectorized
    execution, per-vector primitives, OLTP-vs-vectorized tension.
  - [`sqlite-vdbe.md`](notes/sqlite-vdbe.md) — the canonical
    small-bytecode-VM-for-SQL reference; opcode set, dispatch loop,
    PREPARE/EXECUTE wiring.
  - [`tidb-tikv.md`](notes/tidb-tikv.md) — TiDB+TiKV: typed
    protobuf operator DAG (not bytecode) shipped to a separately-
    deployed Rust storage tier; per-Range scope; coprocessor cache
    pattern; autoparameterized non-prepared plan cache.
- [`RESEARCH.md`](RESEARCH.md) — earlier cross-system synthesis with
  concrete v1 design sketches. Pre-dates this README and treats some
  open questions as more decided than they are; read as raw material.
- [`daydream-doc.md`](daydream-doc.md) — alternative framing of the
  same investigation drafted independently; useful for cross-checking.
