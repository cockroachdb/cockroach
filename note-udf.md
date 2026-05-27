# Deferred SQL Routine Body Building

## 1. Background: The Life of a SQL Query

When CockroachDB receives a query, it goes through two major phases.
The orchestration lives in `plan_opt.go`:

```
  SQL text
     │
     ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  PLANNING                                                    │
 │                                                              │
 │  1. optbuilder: SQL AST ──► RelExpr (memo)                   │
 │     Entry: optbuilder.Builder.Build()                        │
 │            (optbuilder/builder.go:271)                        │
 │     Called from plan_opt.go:616                               │
 │                                                              │
 │  2. optimizer: explore alternatives, find cheapest plan       │
 │     Entry: xform.Optimizer.Optimize()                        │
 │            (xform/optimizer.go:250)                           │
 │     Called from plan_opt.go:644                               │
 │                                                              │
 │  3. execbuilder: optimized RelExpr ──► execution nodes       │
 │     Entry: execbuilder.Builder.Build()                       │
 │            (execbuilder/builder.go:297)                       │
 │     Called from plan_opt.go:1090                              │
 │                                                              │
 └──────────────────────────┬───────────────────────────────────┘
                            │
                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  EXECUTION                                                   │
 │                                                              │
 │  Run the execution nodes, scan tables, evaluate expressions, │
 │  return rows.                                                │
 └──────────────────────────────────────────────────────────────┘
```

The **optbuilder** is responsible for turning SQL text into a tree of
relational expressions (RelExpr) inside a structure called the **memo**.
This includes resolving table names, checking privileges, inferring
types, and building the full logical plan.

### Normal SQL statements vs UDF body statements

For a normal SQL statement, all three planning steps happen at plan
time. For a UDF body, only the optbuilder step happens at plan time —
the optimizer and execbuilder run at execution time, per invocation:

```
  Normal statement:                     Query with UDF: SELECT my_udf(x) FROM t
  SELECT * FROM t WHERE x > 1
                                        PLAN TIME
  PLAN TIME                             ┌──────────────────────────────────────────────┐
  ┌─────────────────────────────────┐   │                                              │
  │                                 │   │  1. optbuilder (outer query)                 │
  │  1. optbuilder                  │   │     Builder.Build()       (builder.go:271)   │
  │     Builder.Build()             │   │     Called from plan_opt.go:616              │
  │     (builder.go:271)            │   │     └── encounters my_udf(x), calls:         │
  │     Called from plan_opt.go:616 │   │         buildRoutine()    (routine.go:220)   │
  │                                 │   │         └── builds body: AST ──► RelExpr     │
  │  2. optimizer                   │   │             Stored in UDFDefinition.Body     │   <- THIS is what we defer
  │     Optimizer.Optimize()        │   │  2. optimizer (outer query)                  │   
  │     (optimizer.go:250)          │   │     UDF body carried as opaque data —        │   
  │     Called from plan_opt.go:644 │   │     NOT optimized here.                      │   
  │                                 │   │                                              │   
  │  3. execbuilder                 │   │  3. execbuilder (outer query)                │   
  │     Builder.Build()             │   │     buildUDF()            (scalar.go:982)    │   
  │     (builder.go:297)            │   │     └── creates RoutinePlanGenerator closure │   
  │     Called from plan_opt.go:1090│   │        buildRoutinePlanGenerator             │  
  │                                 │   │                           (scalar.go:1163)   │   
  └──────────────┬──────────────────┘   └──────────────────────┬───────────────────────┘   
                 │                                             │                           
  EXEC TIME      │                      EXEC TIME              │                           
  ┌──────────────▼──────────────────┐   ┌──────────────────────▼───────────────────────┐   
  │                                 │   │                                              │   
  │  Run execution nodes,           │   │  Outer query runs. When my_udf(x) is         │    
  │  return rows.                   │   │  evaluated, the closure runs:                │   
  │                                 │   │                                              │   
  └─────────────────────────────────┘   │  For each body statement:                    │   
                                        │                                              │   
                                        │   a. Copy into fresh memo, sub arg values    │   
                                        │      f.CopyAndReplace()  (scalar.go:1356)    │   
                                        │                                              │   
                                        │   b. Optimize the fresh memo                 │   
                                        │      o.Optimize()        (scalar.go:1364)    │   
                                        │                                              │   
                                        │   c. Exec-build into execution plan          │   
                                        │      eb.Build()     (scalar.go:1391-1403)    │   
                                        │                                              │   
                                        │   d. Execute and pass to routine executor    │   
                                        │      fn(plan, ...)       (scalar.go:1448)    │   
                                        │                                              │   
                                        └──────────────────────────────────────────────┘   
```

The key insight: even in the **eager** (pre-existing) approach, the
optimizer and execbuilder steps for UDF bodies already happen at
execution time. The optbuilder step (AST → RelExpr in
`buildRoutine`) was the only part done at plan time. **That's the
single step this PR defers.**

## 2. The Problem: Why Defer?

Building UDF bodies at plan time has downsides:

1. **Wasted work**: The UDF body is compiled even if the function is
   never called at runtime (e.g., inside an untaken `IF` branch).

2. **Blocks future features**: Statement hints (injecting optimizer
   hints into routine bodies at retry time) need to re-build bodies
   with different contexts — impossible if they're baked into the plan.

3. **Privilege ordering quirk**: Errors for tables inside the UDF body
   surface at plan time, before the EXECUTE privilege check on the
   function itself. PostgreSQL checks EXECUTE first.

4. **Parameter representation dilemma**: Currently, UDF body statements
   are built with outer column references for parameters. This enables
   UDF inlining (replacing the call with the body expression in the
   caller's plan), because the outer columns connect the body to the
   caller's scope. However, outer columns also trigger decorrelation
   rules during normalization, which can produce suboptimal plans when
   the UDF is *not* inlined — decorrelation rearranges operators in
   ways that are hard to reverse, and the resulting plan may be worse
   than a simple correlated execution.

   The alternative — using placeholders — is better for non-inlined
   execution (placeholders are replaced with concrete argument values
   at execution time, enabling filter push-down), but worse for
   inlining (pushing down a placeholder before replacing it with an
   outer-column argument produces a plan that's harder to decorrelate
   after inlining, leading to regressions).

   With deferred build, this becomes a non-problem: we can **choose
   the representation at the point of use**. When inlining, build
   with outer columns; when executing directly, supply the concrete
   argument values during the build itself, bypassing both outer
   columns and placeholders entirely. This also opens the door to
   **generic query plans for routines** — build once with
   placeholders, optimize, and reuse across executions, analogous to
   how prepared statements work today.

The fix: **defer** building the UDF body from plan time to execution
time — only build it when (and if) the function is actually invoked.

```
  BEFORE (eager):                    AFTER (deferred):

  PLAN TIME                          PLAN TIME
  ┌──────────────────┐               ┌──────────────────┐
  │ Build outer query │               │ Build outer query │
  │ Build UDF body    │◄─ expensive   │ Save UDF ASTs     │◄─ cheap
  │ (AST ──► RelExpr) │               │ (just store them) │
  └────────┬─────────┘               └────────┬─────────┘
           │                                   │
  EXEC TIME                          EXEC TIME
  ┌────────▼─────────┐               ┌────────▼─────────┐
  │ Execute plan      │               │ When UDF called:  │
  │ (body already     │               │  Build body now   │◄─ on demand
  │  built)           │               │  (AST ──► RelExpr)│
  └──────────────────┘               │  Then execute it  │
                                     └──────────────────┘
```

## 3. The Architectural Challenge

This sounds simple — just delay the work. But there's a fundamental
package dependency constraint:

```
  optbuilder ──depends──► memo
  execbuilder ──depends──► memo
  execbuilder ──CANNOT depend──► optbuilder   ✗
```

At execution time, the **execbuilder** needs to build the UDF body
(an optbuilder operation), but it **cannot import optbuilder**. We
can't just call `optbuilder.BuildBody()` from the execbuilder.

## 4. Existing Precedent: The PostQueryBuilder Pattern

CockroachDB already solved this exact problem for **FK cascades** and
**AFTER triggers**. These are "post-queries" — queries that must be
built after the main statement executes. For example, when you DELETE
from a parent table, the cascade DELETE in the child table is built
and executed afterward, not at plan time.

The solution uses an **interface in `memo`** — the one package both
optbuilder and execbuilder can see:

```
  ┌────────────────────────────────────────────────────┐
  │                       memo                          │
  │                                                     │
  │   PostQueryBuilder interface {                      │
  │       Build(ctx, semaCtx, evalCtx, catalog,         │
  │             factory, binding, bindingProps, colMap)  │
  │         ──► (RelExpr, error)                        │
  │   }                                                 │
  │                  (memo/expr.go:1413)                 │
  └────────────▲───────────────────────▲────────────────┘
               │ implements            │ calls through
        ┌──────┴──────────┐     ┌──────┴──────────────┐
        │   optbuilder     │     │   execbuilder        │
        │                  │     │                      │
        │ onDeleteCascade  │     │ planPostQuery()      │
        │   Builder{}      │     │   builder.Build(...) │
        │ (fk_cascade.go)  │     │ (post_queries.go:204)│
        │                  │     │                      │
        │ rowLevelAfter    │     │                      │
        │   TriggerBuilder │     │                      │
        │   {} (trigger.go)│     │                      │
        └─────────────────┘     └──────────────────────┘
```

**How PostQueryBuilder works:**

- At **plan time**, the optbuilder creates a struct (e.g.,
  `onDeleteCascadeBuilder` in `fk_cascade.go:53`) that captures
  everything needed to build the cascade later: the parent/child
  table references, FK constraint metadata, and a snapshot of the
  mutation tracking state (via `stmtTreeInitFn`).

- At **execution time**, the execbuilder calls `builder.Build()` in
  `post_queries.go:204`. This creates a fresh optbuilder internally
  via `buildTriggerCascadeHelper` (`fk_cascade.go:1066`):

  ```go
  // fk_cascade.go:1066
  func buildTriggerCascadeHelper(
      ctx, semaCtx, evalCtx, catalog, factoryI,
      stmtTreeInitFn func() statementTree,
      fn func(b *Builder) memo.RelExpr,
  ) (_ memo.RelExpr, retErr error) {
      factory := factoryI.(*norm.Factory)
      b := New(ctx, semaCtx, evalCtx, catalog, factory, nil)
      if stmtTreeInitFn != nil {
          b.stmtTree = stmtTreeInitFn()  // ◄─ restore mutation context
      }
      b.stmtTree.Push()
      defer b.stmtTree.Pop()
      defer errorutil.MaybeCatchPanic(&retErr, nil)
      return fn(b), nil
  }
  ```

  This helper creates a new `Builder`, injects the statement tree
  snapshot, and runs the build function — all without the execbuilder
  ever importing optbuilder.

## 5. Our Solution: RoutineBodyBuilder

We follow the same pattern with a new interface:

```
  ┌────────────────────────────────────────────────────┐
  │                       memo                          │
  │                                                     │
  │   RoutineBodyBuilder interface {                    │
  │       Build(ctx, semaCtx, evalCtx, catalog,         │
  │             factory)                                │
  │         ──► ([]RelExpr, []*Required, ColList, err)  │
  │   }                                                 │
  │                  (memo/expr.go:1450)                 │
  └────────────▲───────────────────────▲────────────────┘
               │ implements            │ calls through
        ┌──────┴──────────┐     ┌──────┴──────────────┐
        │   optbuilder     │     │   execbuilder        │
        │                  │     │                      │
        │ sqlRoutineBody   │     │ buildRoutinePlan     │
        │   Builder{}      │     │   Generator():       │
        │ (routine.go:1031)│     │   bodyBuilder.Build()│
        │                  │     │ (scalar.go:1220)     │
        └─────────────────┘     └──────────────────────┘
```

### Similarities with PostQueryBuilder

Both patterns share the same architectural approach:

- **Interface in `memo`** breaks the package dependency cycle.
- **Capture at plan time, build at execution time**: The implementing
  struct stores metadata and ASTs at plan time; `Build()` creates a
  fresh `Builder` to construct RelExprs at execution time.
- **Statement tree snapshot** tracks mutation state for conflict
  detection (see below).
- **Thread-safe**: `Build()` does not mutate captured state, so it's
  safe to call concurrently if the plan is cached and reused.
- **Panic-to-error conversion**: Both use `errorutil.MaybeCatchPanic`
  to convert optbuilder panics into errors.

### Differences from PostQueryBuilder

| Aspect | PostQueryBuilder | RoutineBodyBuilder |
|--------|------------------|-------------------|
| **Purpose** | FK cascades, AFTER triggers | SQL routine bodies |
| **Input binding** | Receives mutation buffer via `WithScan` (`binding`, `bindingProps`, `colMap`) | None — routine is standalone |
| **Build parameters** | 8 (includes binding plumbing) | 5 (just context + factory) |
| **Return value** | Single `RelExpr` | `[]RelExpr` + `[]*Required` + `ColList` (multiple body statements + params) |
| **Statement tree capture** | `GetInitFnForPostQuery`: ancestors only | `GetInitFnForDeferredRoutine`: all levels |
| **Helper function** | Uses `buildTriggerCascadeHelper` | Inlines equivalent logic in `Build()` |

### Statement Tree: The Multiple Mutation Problem

Both PostQueryBuilder and RoutineBodyBuilder must handle the **multiple
mutation conflict** problem. CockroachDB's **statement tree** tracks
which tables are mutated across CTEs and subqueries within a single
statement. It detects conflicts like two CTEs both writing to the same
table, which could produce non-deterministic results.

When building is deferred, the builder needs a snapshot of the mutation
state from plan time — but the snapshot must include mutations that
may be registered *after* the snapshot is taken (by sibling CTEs that
appear later in the planning order). This is solved by capturing
**pointers** to the statement tree nodes, so late-arriving mutations
are visible when the deferred builder runs.

PostQueryBuilder uses `GetInitFnForPostQuery()` (`statement_tree.go:242`):

```go
// Captures ancestor levels only (stmts[:len-1]).
// Post-queries are children of the current statement —
// only ancestors can conflict.
ancestorStatements := make([]*statementTreeNode, len(st.stmts)-1)
copy(ancestorStatements, st.stmts[:len(st.stmts)-1])
```

RoutineBodyBuilder uses `GetInitFnForDeferredRoutine()`
(`statement_tree.go:212`):

```go
// Captures ALL levels (stmts[:]), including the current one.
// Deferred routines may appear alongside sibling CTEs at
// the same level, and those mutations must be visible.
ancestorStatements := make([]*statementTreeNode, len(st.stmts))
copy(ancestorStatements, st.stmts)
```

The difference in what's captured:

```
  Statement tree stack during planning:

  GetInitFnForPostQuery:          GetInitFnForDeferredRoutine:

  ┌─────────────┐                 ┌─────────────┐
  │ level 0     │ ◄─ captured     │ level 0     │ ◄─ captured
  ├─────────────┤                 ├─────────────┤
  │ level 1     │ ◄─ captured     │ level 1     │ ◄─ captured
  ├─────────────┤                 ├─────────────┤
  │ level 2     │ ✗  excluded     │ level 2     │ ◄─ captured
  │ (current)   │    (post-query  │ (current)   │    (sibling CTEs
  └─────────────┘     is a child) └─────────────┘     may conflict)
```

## 6. Cases Where Eager Build Is Still Required

Not all UDFs can be deferred. Three cases force eager building:

```
  ┌──────────────────────────────────┐
  │     Can we defer this UDF?        │
  └───────────────┬──────────────────┘
                  │
        ┌─────────▼──────────┐
        │ Return type is      │── Yes ──► EAGER
        │ AnyTuple (RECORD)?  │          (must infer return
        └─────────┬──────────┘           type from body)
                  │ No
        ┌─────────▼──────────┐
        │ Inside non-ANALYZE  │── Yes ──► EAGER
        │ EXPLAIN?            │          (EXPLAIN (OPT) formats
        └─────────┬──────────┘           the memo directly)
                  │ No
        ┌─────────▼──────────┐
        │ Could be inlined?   │── Yes ──► EAGER
        │ (1 stmt, non-SRF,  │          (inlining substitutes
        │  non-volatile)      │           body into caller plan)
        └─────────┬──────────┘
                  │ No
                  ▼
               DEFERRED ✓
```

**AnyTuple**: When a function returns `RECORD`, the actual column
types must be inferred from the body. Can't defer what you need to
determine the return type.

**EXPLAIN (non-ANALYZE)**: `EXPLAIN (OPT)` formats the memo directly.
Without the body, it would show `body (deferred)` instead of the full
optimizer plan. Note that `EXPLAIN ANALYZE` is different — the
conn_executor strips the EXPLAIN ANALYZE wrapper before the optbuilder
sees it (`conn_executor_exec.go:558`), so the inner statement builds
normally with deferred build active. The deferred body is then built
and executed at runtime, and the execution plan tree (which EXPLAIN
ANALYZE renders) shows the UDF body's execution naturally (actually,
just print the function name, that's it) — no
special handling needed, unlike EXPLAIN (OPT) which must force eager
build.

**Inlineable UDFs**: The optimizer can replace simple UDF calls with
their body expression (like function inlining in a compiler). This
requires the body at plan time.

## 7. EXPLAIN ANALYZE (DEBUG) Bundles

EXPLAIN ANALYZE (DEBUG) produces a ZIP bundle containing optimizer
plans, table stats, and schema for all referenced tables. With deferred
build, the plan-time memo lacks the UDF body's table references.

Rather than trying to discover table references at plan time (e.g., by
walking the AST), we propagate execution-time metadata back:

```
  PLAN TIME                          EXEC TIME
  ┌──────────────────────┐           ┌──────────────────────┐
  │ memo has:             │           │ Deferred body built   │
  │  - outer query tables │           │ in fresh memo:        │
  │  - body (deferred)    │           │  - body table refs    │
  │  - no body tables     │           │  - optimizer plan     │
  └──────────┬───────────┘           └──────────┬───────────┘
             │                                   │
             │         ┌───────────────┐         │
             └────────►│ eval.Context   │◄───────┘
                       │               │
                       │ DeferredRoutine│  ◄─ accumulated
                       │   OptPlans     │     during execution
                       │ DeferredRoutine│
                       │   TableRefs    │
                       └───────┬───────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Bundle collector    │
                    │                       │
                    │ plan-time tables      │
                    │   ∪                   │
                    │ execution-time tables │
                    │   = all tables        │
                    │                       │
                    │ opt-vv.txt            │
                    │ opt-vv-deferred-      │
                    │   <func>.txt          │
                    └──────────────────────┘
```

The bundle collector unions plan-time metadata with execution-time
table references, ensuring stats and schema are collected for all
tables. Deferred body optimizer plans are emitted as supplementary
`opt-vv-deferred-<func>.txt` files.

### Why separate files instead of amending opt-vv.txt?

`opt-vv.txt` is generated by formatting the **plan-time memo**
(`addOptPlans` in `explain_bundle.go:391`), which calls
`f.FormatExpr(b.plan.mem.RootExpr())`. That memo is frozen after
optimization (`plan_opt.go:1159`) and is never updated during
execution. For deferred routines, `Body == nil` in this memo —
the body's RelExprs were never built there.

The deferred body is built at execution time in a **fresh, ephemeral
memo** (`scalar.go:1225`), which is discarded after exec-building.
Grafting those RelExprs back into the plan-time memo is not feasible:
the two memos have independent column ID namespaces, metadata, and
cost structures. Merging memos is not supported by the infrastructure.

So instead, we capture the execution-time optimizer plan as a
formatted string (`DeferredRoutineOptPlans` in `eval.Context`,
populated at `scalar.go:1250`) and emit it as a supplementary file.
This gives the **actually-executed** plan rather than a re-built
approximation.

### Comparison with post-query handling in bundles

FK cascades and AFTER triggers face the same structural problem:
they are also built in fresh memos at execution time
(`post_queries.go:218`), so their optimizer plans don't appear in
`opt-vv.txt` either.

However, post-queries and deferred routines diverge in what the
bundle captures:

| Aspect | Post-queries (cascades/triggers) | Deferred routines |
|--------|----------------------------------|-------------------|
| **Table discovery** | `VisitFKReferenceTables()` (`opt/util.go:16`) walks the FK graph from plan-time metadata — catalog structure makes targets discoverable without building | `DeferredRoutineTableRefs` captured at execution time (`scalar.go:1268`) — body tables aren't discoverable from catalog without parsing |
| **Execution plan** | Appears inline in `plan.txt` under `fk-cascade` nodes; also gets dedicated `vec-N-postquery.txt` and `distsql-N-postquery.html` files | Appears inline in `plan.txt` under the routine's execution node |
| **Optimizer plan** | **Not captured** — no `opt-vv-cascade-*.txt` files exist | Captured in `opt-vv-deferred-<func>.txt` via `DeferredRoutineOptPlans` |

Post-queries never needed optimizer-level bundle detail because
cascades are mechanical FK-driven mutations with predictable plans.
UDF bodies can have complex optimizer behavior (join ordering, index
selection, subqueries), making the optimizer plan valuable for
debugging — hence the extra capture mechanism.

## 8. The Commit Sequence

The 8 commits are structured so that **no commit changes observable
behavior until commit 5**, making each independently reviewable:

```
  C1-C4: Infrastructure (no behavior change)
  ──────────────────────────────────────────

  C1  Extract buildSQLRoutineBodyStmts()
      Pure code move — pulls the body-building loop into a
      method so that C5 can conditionally skip it.

  C2  Define RoutineBodyBuilder interface + statement tree
      Adds the interface to memo and GetInitFnForDeferredRoutine()
      to statement_tree. Dead code — nothing uses it yet.

  C3  Implement sqlRoutineBodyBuilder
      The struct that captures ASTs and metadata at plan time,
      builds RelExprs at execution time. Dead code — nothing
      creates it yet.

  C4  Nil-body plumbing
      Adds guards in execbuilder, memo formatter, and norm
      factory for Body == nil. No behavior change because
      Body is never nil yet.

  C5: The switch (behavior changes)
  ─────────────────────────────────

  C5  Enable deferred build
      Adds the eager/deferred branch in buildRoutine. Forces
      eager for AnyTuple and EXPLAIN.

  C6-C8: Tests, edge cases, observability
  ────────────────────────────────────────

  C6  Logic tests for mutation conflicts and privilege ordering.

  C7  Force eager build for inlineable UDFs
      Detected post-C5: inlining needs the body at plan time.

  C8  EXPLAIN ANALYZE (DEBUG) bundle support
      Propagates execution-time optimizer plans and table refs
      back to the bundle collector via eval.Context.
```
