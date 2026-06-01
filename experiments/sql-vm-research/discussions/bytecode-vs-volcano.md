---
author: assistant (Claude), captured at user request
date: 2026-04-30
status: open
---

# Bytecode VM vs. Volcano-with-aggressive-caching

The dominant pattern in production database execution engines is the
Volcano iterator model: each operator implements a `Next()` interface,
operators chain into trees, the executor walks the tree per execute.
CRDB's `rowexec` and `colexec` are both shaped this way; so are
Postgres, MySQL, MonetDB/X100, DuckDB, Velox, ClickHouse, TiDB. The
two prior-art entries on the bytecode-VM side of the substrate axis —
SQLite VDBE and Clustrix/Xpand — are noticeably outnumbered.

Given that consensus, what specifically argues for bytecode over an
optimized Volcano-style execution? This document is the first
discussion point in the workspace; it tries to lay out the real case
on each side without rationalizing toward either answer.

## What bytecode actually buys

1. **Compile-time scratch allocation (the Clustrix point).** A bytecode
   program declares its register file shape at compile time; the
   executor binds one allocation at invocation. This is the cleanest
   answer we've seen to the "no runtime allocations" goal. Volcano
   operators are self-contained and allocate their own internal state
   at construction; getting them to share a per-query arena cuts
   against the abstraction — operators have to plumb in arena handles,
   slice growth has to be coordinated, the "operator trees are
   independent" property gets weakened. Achievable in Volcano but works
   against the grain.

2. **Native code as one opcode (Clustrix `VM_NATIVE`).** A bytecode VM
   is a clean substrate for adding native compilation later — just
   another opcode the interpreter dispatches to. Adding native to
   Volcano requires either compiling whole pipelines (HyPer-style, big
   lift) or a parallel JIT execution path (separate code, separate
   bugs). The bytecode-then-native progression is staged and
   incremental; Volcano-then-JIT is a paradigm shift.

3. **Sandboxing for eventual KV pushdown.** This matters specifically
   because of the "leave the door open to KV pushdown" constraint. A
   bytecode program is a finite, statically-analyzable sequence of
   opcodes; you can allowlist a subset for KV-side execution, bound
   resource consumption, reason about side effects. An operator tree
   with arbitrary methods on each node is harder to bound — TiKV ended
   up reinventing this as a typed protobuf DAG specifically to get the
   constraints. With bytecode you get them by construction.

4. **Cleaner compile/execute separation (engineering hygiene).** With
   Volcano, the operator tree IS the cached artifact, so changes to
   operator semantics affect both compile-time and runtime concerns.
   With bytecode the IR is a stable contract — compiler and executor
   can evolve independently. This is more "future-proofing" than
   "performance," but real.

5. **Tighter dispatch loop.** A flat `for { switch op { ... } }`
   interpreter has monomorphic dispatch (one branch site, predictable);
   Volcano's per-operator interface dispatch is megamorphic. CPU win
   in absolute terms is probably a few percent — not the headline.

6. **Bounded goroutine stack; worker-pool execution becomes feasible.**
   Bytecode interpretation is a flat dispatch loop — it doesn't recurse
   through Go's call stack. Volcano operators do: every `Next()` call
   into a child operator pushes a Go stack frame, so deep operator
   trees cost goroutine stack per query. Go's stacks start small
   (~2KB) but grow on demand and **don't shrink back** in normal
   operation; once a goroutine has handled a deep query, its stack
   stays large. At high concurrency this leads to real memory bloat —
   a phenomenon that has shown up in production CRDB escalations
   involving goroutines with very large stacks.

   With bytecode, query programs can run on a fixed-size pool of
   long-lived worker goroutines whose stacks stay bounded, since the
   interpreter needs only modest constant-depth Go frames regardless
   of program complexity. The cold-path cases that DO need deep stacks
   (planning and compiling on cache miss) can be handled outside the
   pool, and become rarer as the cache warms.

   This connects to the per-core-executor question listed as open
   elsewhere in the workspace: a worker-pool model is structurally
   easier to build on a bounded-stack substrate than on a Volcano
   substrate where each query needs its own goroutine to hold its
   operator-chain frames.

7. **Autoparameterization-friendly contract.** Bytecode's
   `(program, parameter_vector)` interface decouples program shape from
   parameter values cleanly. Operator trees can carry literals inside
   nodes; rebinding requires either tree-walking or careful operator
   design.

## Where the Volcano alternative is genuinely competitive

The biggest single contributor to the Phase 1 +47-66% per-vCPU win was
**skipping the per-execute pipeline rebuild.** That's achievable in a
Volcano world by:

- Caching the whole operator tree + pre-instantiated state on the
  prepared statement (the Postgres `CachedPlan` pattern).
- Function-pointer dispatch via the `ExecProcNodeFirst` install-once
  trick.
- Some allocation discipline on the cached operators.

This is a real alternative path. It captures most of the immediate
Phase 1-style wins without adopting an unfamiliar architecture. The
rough comparison:

| Dimension | Bytecode VM | Volcano + aggressive caching |
|---|---|---|
| Per-execute pipeline-rebuild cost | Eliminated | Eliminated |
| Per-tuple dispatch | Tighter (monomorphic switch) | Better than today (function pointer, no megamorphic) but not as tight |
| Allocation discipline | Native (compile-time register file) | Plumbed in (against the grain) |
| Goroutine stack / memory | Bounded; worker-pool model feasible | Per-query goroutines; stacks grow with operator depth and don't shrink |
| Future KV pushdown | Easier (sandboxable bytecode) | Harder (DAG-style retrofit needed) |
| Future native code | Easier (`VM_NATIVE` opcode) | Harder (separate JIT path) |
| Compiler complexity | New codebase | Reuse exec-builder |
| Team familiarity | New substrate | Familiar |
| Vectorization integration | Harder | Natural |
| Observability (EXPLAIN, tracing) | Need debug-info layer | Self-describing operator tree |

## Honest read

**Bytecode wins on the architectural dimensions** — allocation
discipline, KV-pushdown hospitability, native-code evolution path,
compile/execute separation — **and on operational/concurrency
characteristics** — bounded goroutine stacks, worker-pool feasibility,
predictable memory behavior under load. **Volcano-with-caching wins
on the immediate-adoption dimensions** — familiarity, incremental
rollout, no new compiler to write, self-describing observability.

If the goal were narrowly "capture the Phase 1 win in production,"
Volcano-with-aggressive-caching is the lower-risk path. If the goal
includes "build a substrate that can grow into native compilation and
KV pushdown over the next several years," bytecode is the better
foundation — but with a substantial near-term complexity cost paid
for option value that may or may not get exercised.

The decision isn't "which is technically superior" — both can deliver
the immediate win — it's **do we want to spend the architectural
complexity now to enable the option value later?** That's a strategic
call, not a technical one, and the prior art alone doesn't resolve it.

## Things this document does not attempt

- **Quantify either path's actual implementation cost.** "Substantial
  complexity" and "lower risk" are qualitative; turning them into
  story-point estimates needs design work neither has had.
- **Enumerate which Phase 1-style cost buckets each path captures and
  which it leaves on the table.** The "biggest single contributor was
  per-execute pipeline rebuild" line is from the Phase 1 fast-path
  experience, but the decomposition (allocation vs computation vs
  dispatch) hasn't been measured separately.
- **Address the colexec-extension question directly.** Extending
  colexec is a third path orthogonal to both — Volcano substrate,
  vector data — and it has its own arguments captured separately in
  the existing `tidb_tikv_research.md` and `notes/monetdb-x100.md`
  files.
- **Take a position.** The "honest read" section frames the decision
  but does not recommend an answer.
