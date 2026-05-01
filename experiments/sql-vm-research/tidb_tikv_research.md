# TiDB / TiKV: Architecture Notes for the CockroachDB SQL Executor Redesign

> **Why this exists.** TiDB+TiKV is the closest production analog to a "Go SQL pod
> talking to a Rust storage pod over RPC, with computation pushed down via a small
> portable program" architecture that CockroachDB is considering. This report is
> a deep read of their design, drawn from the open-source repos
> (`pingcap/tidb`, `tikv/tikv`, `pingcap/tipb`), official dev guides, blog posts,
> and the LFX 2021 mentorship retrospective on the v2 plugin work. Every
> non-obvious claim has a reference at the bottom.
>
> All file paths quoted below are inside the `tikv/tikv`, `pingcap/tidb`, or
> `pingcap/tipb` repos at HEAD as of the clone date.

---

## TL;DR — the four things that matter most

1. **The coprocessor wire format is not bytecode.** It is a protobuf-encoded
   *operator DAG* (`tipb.DAGRequest`) carrying a list of `tipb.Executor` nodes
   plus a forest of `tipb.Expr` trees. Each scalar function is identified by a
   numeric `ScalarFuncSig` enum (~700 entries). TiKV interprets the tree using
   pre-compiled Rust functions; TiDB never ships native code or VM bytecode to
   TiKV. The executor set on TiKV is intentionally small: TableScan, IndexScan,
   Selection, Projection, Limit, TopN, Simple/Hash/Stream Aggregation,
   IndexLookUp. **Joins, sorts, window, exchange, and CTEs are explicitly
   rejected on TiKV** — see `BatchExecutorsRunner::check_supported` in
   `components/tidb_query_executors/src/runner.rs`.
2. **There is a separate "Coprocessor v2" framework** (`tikv/src/coprocessor_v2/`)
   that loads native Rust dylibs via `libloading` and hands them raw bytes plus
   `RawStorage`. It was built in 2021 as an LFX mentorship project; **it was
   never adopted to replace v1**, and the v1 DAG coprocessor is what every
   real workload uses. There is no WASM sandbox; v2 plugins run with full
   process privileges. The original RFC said the long-term goal was to
   re-implement the SQL coprocessor *as* a v2 plugin; that has not happened.
3. **Sandboxing is by construction, not by isolation.** TiKV does not run
   untrusted code. Resource control is a per-endpoint memory quota
   (default `max(total_mem * 12.5%, 500 MiB)`), a soft per-request deadline
   (`end_point_request_max_handle_duration`), a recursion-depth limit on the
   protobuf decoder (default 1000), a row batch ceiling, and a multi-level
   feedback queue scheduler (yatp) that demotes long-running queries.
4. **Per-request overhead is real but mitigated.** The coprocessor splits one
   logical scan into one RPC *per Region* (default ~96 MiB/Region), so a wide
   scan fans out to many concurrent requests. PingCAP has invested heavily in:
   (a) a coprocessor cache keyed on data version, (b) batching small TiKV
   regions into a "batch coprocessor" path for TiFlash, (c) `CopSmallTaskRow=32`
   special-casing for tiny scans, and (d) "Chunk RPC" to skip a row→column
   recoding that was once consuming **50% of TiDB CPU**.

The most important contrast with CockroachDB: TiDB shipped a *protobuf operator
tree*, not bytecode, and they accept the cost of doing so (~700 hand-coded
function signatures, mixed-version handled by negotiating a function-support
bitmap *in the SQL layer at plan time*, not by versioning the wire format).

---

## 1. The TiKV Coprocessor (v1) Framework

### 1.1 Wire format

A coprocessor request is a **gRPC `Coprocessor.Request`** (defined in
`kvproto/coprocessor.proto`):

```protobuf
message Request {
    kvrpcpb.Context context = 1;   // region/peer/isolation level/locks
    int64           tp      = 2;   // 103=DAG, 104=Analyze, 105=Checksum
    bytes           data    = 3;   // tipb-encoded payload
    uint64          start_ts = 7;  // MVCC timestamp
    repeated KeyRange ranges = 4;  // sub-region ranges to scan
    uint64 paging_size = ...;      // for cursor-style paging
    bool   is_cache_enabled = ...; // coprocessor cache
    uint64 cache_if_match_version = ...;
}
```

When `tp == 103` (DAG), `data` is a `tipb.DAGRequest`:

```protobuf
message DAGRequest {
    repeated Executor executors = 2;  // post-order, leaf-first
    int64  time_zone_offset = 3;
    uint64 flags = 4;                  // truncate handling, sql_mode bits
    repeated uint32 output_offsets = 5; // which columns to ship back
    EncodeType encode_type = 8;        // TypeDefault | TypeChunk | TypeCHBlock
    uint64 sql_mode = 9;
    string time_zone_name = 11;
    bool   is_rpn_expr = 15;           // RPN-form expressions (post-2019)
    Executor root_executor = 17;       // tree form, TiFlash only
    ...
}
```

The `executors` list is a **post-order, leaf-first** linear encoding of an
operator tree: `[scan, selection, agg, topN]` means the data flows
scan→selection→agg→topN. (TiFlash uses `root_executor` for true tree shape
since it supports multi-input operators like joins; TiKV doesn't.)

Each `Executor` is a tagged union:

```protobuf
message Executor {
    ExecType tp = 1;
    optional TableScan tbl_scan = 2;
    optional IndexScan idx_scan = 3;
    optional Selection selection = 4;
    optional Aggregation aggregation = 5;
    optional TopN topN = 6;
    optional Limit limit = 7;
    // ... Projection, IndexLookUp, and TiFlash-only Join/Window/Exchange/CTE ...
}
```

Expressions are `tipb.Expr` trees:

```protobuf
message Expr {
    ExprType tp = 1;          // Null, Int64, ColumnRef, ScalarFunc, Count, ...
    bytes    val = 2;          // for constants
    repeated Expr children = 3; // for ScalarFunc
    ScalarFuncSig sig = 4;     // numeric signature into tikv's eval table
    FieldType field_type = 5;
    bool has_distinct = 7;
}

message RpnExpr { repeated Expr exprs = 1; }   // post-order linear form
```

`ScalarFuncSig` is a **flat enum of ~700 typed function signatures** like
`PlusInt = 203`, `LTReal = 101`, `RegexpUTF8Sig = 4312`, `JsonExtractSig = 5001`,
`VecCosineDistanceSig = 5116`. There is one signature per (function, operand
type) combination. This is critical: the plan tree carries already-resolved
type-monomorphized function IDs, so TiKV does not need a type checker — it
dispatches on a u32. (See `tipb/proto/expression.proto` lines 99–783.)

**So is it bytecode?** No. It is a *type-erased operator tree with monomorphized
function IDs and pre-resolved column ordinals*. Conceptually it sits between
"AST" and "bytecode": it has the verbosity of an AST but the no-typecheck
property of bytecode. There is no jump/branch, no register file, no
instruction pointer.

### 1.2 What runs inside the coprocessor

From `BatchExecutorsRunner::check_supported`
(`tikv/components/tidb_query_executors/src/runner.rs`), TiKV explicitly
**accepts**:

| Executor              | Notes                                                  |
| --------------------- | ------------------------------------------------------ |
| `TableScan`           | Scans `t_<tableID>_r_<handle>` keys in a range         |
| `IndexScan`           | Scans `t_<tableID>_i_<idxID>_<idxKey>`                 |
| `IndexLookUp`         | Index → row handles → table rows in one round trip     |
| `Selection`           | WHERE filter; vectorized over chunks                   |
| `Projection`          | Adds computed columns                                  |
| `Limit`               | Counts and stops                                       |
| `TopN`                | In-memory heap                                         |
| `SimpleAggregation`   | No GROUP BY                                            |
| `FastHashAggregation` | GROUP BY a single column whose values fit a hash table |
| `SlowHashAggregation` | General GROUP BY                                       |
| `StreamAggregation`   | GROUP BY on already-sorted input                       |

And explicitly **rejects** on TiKV (returns `"executor not implemented"`):

`Join`, `Sort`, `Window`, `ExchangeSender`, `ExchangeReceiver`,
`PartitionTableScan`, `Expand`, `Expand2`, `BroadcastQuery`, `CTESink`,
`CTESource`, `Kill`. All of those are TiFlash-only operators that travel over
the same protobuf shapes.

The boundary is therefore: **per-Region streaming pipelines with one input
and zero or one output stream.** No shuffle. No build/probe. No state shared
between requests. This is exactly what a Range-scoped pushdown engine needs
to look like, and the constraint falls out naturally: you can only cheaply do
work that needs a single Region's worth of data.

Aggregation is split: the coprocessor returns *partial* aggregates
(`AggFunctionMode::Partial1Mode` / `Partial2Mode`), and TiDB's planner inserts
a final-mode aggregator on the SQL side. `count(*)` becomes "TiKV returns
per-region counts; TiDB sums them."

### 1.3 Sandboxing and resource control

The coprocessor never executes user code, so the threat model is "well-formed
plans that may be expensive," not "adversarial code." Mitigations live in
`tikv/src/coprocessor/endpoint.rs` and `tikv/src/server/config.rs`:

| Limit                                  | Default                                    | Mechanism                                             |
| -------------------------------------- | ------------------------------------------ | ----------------------------------------------------- |
| `end_point_memory_quota`               | `max(total_mem * 0.125, 500 MiB)`          | `MemoryQuota` ref-counted per request; reject on full |
| `end_point_request_max_handle_duration`| (cpu-derived)                              | `Deadline` checked at every executor `next_batch()`   |
| `end_point_recursion_limit`            | 1000                                       | `CodedInputStream::set_recursion_limit` on protobuf   |
| `end_point_batch_row_limit`            | 64                                         | Initial batch size; doubles up to `BATCH_MAX_SIZE`    |
| `end_point_max_concurrency`            | `max(cpu_cores, 4)`                        | Tokio `Semaphore` permit per "heavy" request          |
| `LIGHT_TASK_THRESHOLD`                 | 5 ms                                       | Requests under this skip the semaphore entirely       |
| Read pool (yatp)                       | Multi-level feedback queue                 | Long requests demoted to lower-priority queue         |

The yatp scheduler is the most interesting piece: it's a custom Rust
work-stealing pool with **multi-level feedback queues**. New requests start in
the high-priority queue; tasks that have accumulated CPU drop to lower
priorities. This keeps OLTP point reads from being blocked behind a TPC-H
table scan in the same pool. Migration from `tokio-threadpool` to yatp
(PR #6375 / #6401, 2020) cut p99 read latency by ~14% and p999 by ~20% under
mixed workloads.

There is no per-coprocessor-request CPU quota in the kernel-cgroups sense.
Resource isolation is by (a) the soft deadline, (b) the MLFQ scheduler, and
(c) the new "Resource Control" subsystem (TiDB 7.x) which integrates with
named resource groups via `kvproto::resource_manager::Consumption`. The newer
system reports RU (Request Unit) consumption back in
`ExecutorExecutionSummary.ru_consumption`.

### 1.4 Wire format versioning

This is the single most surprising design choice and it is worth describing
carefully because it is **not** how I would have predicted a serious
distributed system would handle this.

- `tipb` is a **single shared protobuf repo** consumed by both TiDB and TiKV
  builds. It uses proto2 with optional fields. New executor types are added
  by appending to `enum ExecType`; new functions are added by appending to
  `enum ScalarFuncSig`.
- There is **no version field in `DAGRequest`**. Compatibility is achieved by:
  1. **Forward-compatible add-only enums.** Old TiKV reads new field IDs as
     unknown and (because of the `Executor` tagged-union pattern) will see an
     `ExecType` it doesn't understand and fail the request. There is no
     graceful degradation at the wire layer.
  2. **Function-support negotiation in the SQL planner.** TiDB asks "does this
     storage node support `ScalarFuncSig::X`?" via a per-operator
     `IsExprPushedDownToTiKV(...)` whitelist that is tied to TiKV version.
     Functions that aren't in the whitelist are simply not pushed down — they
     run in TiDB instead. New functions are first added to TiDB-only
     evaluation, then to TiKV, then unlocked in the planner whitelist a
     release later.
  3. **Mixed-version clusters work** because the planner has a list of
     supported pushdowns per TiKV minor version (resolved via `pd-client` at
     plan time), and any function not on the *minimum* version's list across
     reachable TiKVs falls back to TiDB-side evaluation.

This is engineering pragmatism, not elegance. The cost is that the planner
carries a giant table of "what every TiKV version supports," and adding a new
pushdown function involves PRs in three repos (tipb → tikv → tidb) plus a
release-gating dance. The benefit is that the wire format itself never has to
version anything — it is just append-only protobuf.

There is no equivalent of CockroachDB's cluster-version gates *for the
coprocessor protocol*. Cluster versions exist in TiDB for SQL/DDL features,
but the kv layer's compatibility story is purely "which functions are on this
TiKV minor version."

### 1.5 Per-request overhead

PingCAP has not published a single number like "X µs per coprocessor RPC,"
but multiple proxies exist:

- **Region-fanout cost.** A scan crossing 100 Regions becomes 100 gRPC RPCs.
  The TiDB-side `CopIterator` parallelizes them with concurrency
  `tidb_distsql_scan_concurrency` (default 15). Initial response batch is 32
  rows (`CopSmallTaskRow`), doubling up to `BATCH_MAX_SIZE`. There is a
  dedicated "small task" path (`smallConcPerCore=20`) for queries below
  `CopSmallTaskRow` to amortize per-RPC fixed cost.
- **Protobuf encode/decode dominated CPU until "Chunk RPC."** Pre-Chunk-RPC,
  TiDB spent ~50% of its CPU decoding the row-format response and re-encoding
  it as columns. The fix: have TiKV emit `EncodeType::TypeChunk`
  (column-major) directly. This is the biggest published efficiency win in
  the project's history but the absolute number was given as "≈50% CPU
  reduction in TiDB," not as a latency delta.
- **Coprocessor cache.** TiKV maintains a "data version" per Region; TiDB can
  send `cache_if_match_version` and TiKV returns just `is_cache_hit=true` if
  the data hasn't changed. This is a TiDB-side cache (TiKV only validates),
  populated for analytical queries with stable scans.

Realistic mental model for per-request overhead: a few hundred microseconds of
gRPC + protobuf framing + region routing + lock check, plus the actual scan
work. For point reads, the system explicitly bypasses the coprocessor entirely
and uses `kv get` against the storage read pool.

### 1.6 What workloads benefit / get hurt

**Big wins:**
- Aggregations over filtered scans (`SELECT count(*) WHERE ...`,
  `SELECT sum(x) GROUP BY y`). TiKV ships back kilobytes instead of gigabytes.
- Range-with-predicate (`SELECT * WHERE x > ? LIMIT 100`) — Selection +
  Limit collapse the result set at storage.
- Index lookups with predicates (IndexLookUp executor pushes the WHERE down to
  the row-fetch step).
- Reading a small subset of columns from a wide table (Projection + column
  pruning).

**Neutral or hurt:**
- Pure point lookups by primary key — overhead of building/parsing a DAG
  plan exceeds the work. TiDB explicitly bypasses the coprocessor for these
  via `tikvrpc.Get`.
- Joins — must be done in TiDB. Non-pushable predicates that reference
  multiple tables stay in TiDB.
- Anything that needs ORDER BY across regions — TopN can be done per region,
  but the merge happens in TiDB.
- Functions not on the per-version pushdown whitelist (silently degrades).

### 1.7 Evolution and lessons

Major waypoints:
- **2016** Original DAG coprocessor (Volcano-style, row-at-a-time `next()`).
- **2018** Vectorized "batch executors" (32-row → 1024-row chunks) lift
  scan/aggregation throughput several-fold.
- **2019** RPN expressions (`is_rpn_expr=true`), expression interpreter rewrite,
  yatp scheduler. The RPN form is a flat `Vec<Expr>` rather than a tree —
  better cache locality during evaluation. TiDB still emits both forms; TiKV
  uses RPN if present.
- **2020** Unified read pool: storage `kv get` and coprocessor share one yatp
  pool, simplifying tuning.
- **2021** Coprocessor v2 plugin framework (LFX mentorship). Runs arbitrary
  Rust dylibs against `RawStorage`. **Never deployed at scale.** The
  long-term plan to re-implement v1 as a v2 plugin was abandoned.
- **2022+** TiFlash and the MPP engine become the home for "complex"
  pushdowns (joins, exchanges, window, CTEs). TiKV coprocessor stays
  intentionally narrow. The pipeline execution model in TiFlash adopts
  morsel-driven parallelism (Leis et al., 2014) explicitly.

The biggest *lesson* visible in the code is the **executor/expression split
from the framework**. By 2019 the entirety of `tidb_query_*` had been
extracted into separate crates (`tidb_query_aggr`, `tidb_query_expr`,
`tidb_query_executors`, `tidb_query_datatype`, `tidb_query_common`). This made
compile times tractable and let the executors evolve without touching the
endpoint. **Anyone designing this for CockroachDB should plan to factor the
"executor library" out of the storage server crate from day one.**

The second lesson — implicit in the v2 abandonment — is that **opening up
"arbitrary Rust dylibs in your storage server" is a security/operational
nightmare nobody actually wants.** Even PingCAP, with full control of the
deployment, has not pushed this beyond LFX prototype.

---

## 2. TiDB SQL Executor Architecture

### 2.1 Execution model

TiDB is **Volcano + intra-operator parallelism, with vectorized chunks**:

- The outer skeleton is Volcano: `Open() / Next() / Close()` on every
  operator. `Next` is **not thread-safe** — only one goroutine ever calls
  `Next` on a given operator.
- Each `Next` call returns a `Chunk` of up to 1024 rows in **column-major
  Apache-Arrow-like layout** (`tidb/pkg/util/chunk/chunk.go`). Columns are
  separately allocated; fixed-width columns store data inline, variable-width
  use offset arrays.
- **Parallelism is internal to operators.** A HashJoin operator launches N
  inner workers + 1 outer worker; HashAgg launches partial + final worker
  pools. They communicate via Go channels carrying `*Chunk` pointers. The
  parent operator's `Next` just pops from the result channel.
- This is **not pipelined parallelism.** Each operator is its own little
  goroutine universe, hidden behind the single-threaded Volcano interface.

There is **no bytecode VM**. Expressions are evaluated by walking
`expression.Expression` trees (with a `VecEval*` fast path that operates
column-at-a-time and accounts for ~10× speedup over the row-at-a-time path).
Vectorizing >500 builtin functions was a multi-year community effort using Go
text/template-generated code (`builtin_*_vec_generated.go`).

### 2.2 What gets pushed down

Decision happens during physical planning in
`pkg/planner/core/rule_predicate_push_down.go`,
`rule_aggregation_push_down.go`, `rule_topn_push_down.go`. The general rules:

- Scans always push to the storage layer.
- Selection: pushed if every referenced function is in the per-engine
  pushdown whitelist (`expression.IsPushDownExpr`).
- Aggregation: split into partial (storage) + final (TiDB) when all input
  expressions are pushable.
- TopN: pushed as `partial TopN` in storage + final TopN in TiDB.
- Limit: pushed below TopN/Projection where safe.

The planner has separate pushdown matrices for `tikv`, `tiflash`, and
`tidb`. The same DAG protobuf can target either; the runtime picks the engine
based on table replicas and cost.

`EXPLAIN` annotates each operator with a "task" column: `cop[tikv]`,
`cop[tiflash]`, `mpp[tiflash]`, `root` (TiDB). Reviewing an `EXPLAIN` is the
canonical way to see pushdown decisions.

### 2.3 Memory management

- **Chunks are pooled** (`pkg/util/chunk/pool.go`). Each operator typically
  reuses a single `Chunk` across `Next` calls — `chk.Reset()` keeps backing
  arrays.
- **Per-query memory tracker.** A `memory.Tracker` is attached to the session.
  Each operator's allocations report to its tracker, which composes into the
  query tracker, which composes into the session tracker. When a quota is
  hit, an **OOMAction** chain fires: rate-limit, then spill (HashJoin and
  Sort can spill to disk via `chunk.RowContainer`), then cancel.
- **No arena allocator.** TiDB relies on Go's GC. Chunk reuse is the main
  defense against allocator churn. Several PRs over the years have reduced
  per-row allocations to near zero in vectorized expression paths
  (the published 10× benchmark shows `0 B/op 0 allocs/op` for the vectorized
  expression eval).
- **Spilling is operator-specific**, not a generic mechanism. Sort, HashJoin,
  and HashAgg have their own external algorithms.

There is nothing analogous to a per-query arena that frees in O(1) at end of
query. The Go GC handles that, with the cost being unpredictable
goroutine-pause behavior under heavy load. **This is the single largest known
operational complaint about TiDB at scale.**

### 2.4 Prepared statements / plan caching

Two-level cache (`pkg/planner/core/plan_cache.go`):

1. **Prepared plan cache** (the classic case): SQL is parsed to AST, AST is
   parameterized, and the **physical plan** is cached keyed by
   `(db_name, stmt_id, schema_version, sql_mode, time_zone, ...)`. Re-execute
   does parameter substitution + scan-range adjustment + validity check, then
   skips parse/optimize entirely.
2. **Non-prepared plan cache** (TiDB 6.0+): even ad-hoc SQL gets parameterized
   on the fly (`SELECT * FROM t WHERE a=1` → `... WHERE a=?`) and looked up
   in the same cache. PingCAP reports 4–15% TPC-C / Sysbench / banking
   workload speedups from this alone.

The cache is **session-local**, which is a significant scalability problem
TiDB has acknowledged repeatedly (every session pays a cold-start). There
have been proposals for cross-session cache but none have shipped.

The cached object is the *physical plan* (`PhysicalPlan` tree), not the
executor tree, not bytecode. Building executors from the plan is fast and
happens per-execute. Parameter values are substituted into `expression.Constant`
nodes that were left as deferred placeholders.

### 2.5 Concurrency model

It is **goroutine-per-query at the request layer, plus
goroutines-per-operator-worker inside the query**. There is no per-core
pinned executor or async runtime. A query does not have a fixed thread
budget — it acquires goroutines as needed for parallel operators and releases
them at `Close`.

This is essentially the same model as CockroachDB today. The differences are
operational, not architectural:

- TiDB has a sharper distinction between "stateless SQL pod" (TiDB) and
  "stateful storage pod" (TiKV), which lets you scale each independently —
  but the SQL pod itself is a vanilla Go server.
- TiDB has a **distsql layer** (`pkg/distsql/`) that mediates the streaming
  fan-in from many concurrent coprocessor RPCs. It buffers, orders, and
  back-pressures region results into the per-operator chunk channels.
- The TiFlash MPP path *does* introduce something like async pipeline
  scheduling — but only inside TiFlash (C++/ClickHouse-derived), not in TiDB
  itself.

Bottom line: TiDB has **not** moved to a per-core executor model. They went
the other direction — push more work into TiFlash's morsel-driven pipeline
engine when latency-sensitive analytics demand it.

---

## 3. Quantitative Outcomes

Hard numbers PingCAP has published. Caveats: most come from PingCAP's own
benchmarks and should be treated as upper bounds.

| Metric                                               | Value                          | Source                                                   |
| ---------------------------------------------------- | ------------------------------ | -------------------------------------------------------- |
| Vectorized vs row expression eval                    | **~5.4× faster** (38461 ns/op → 7056 ns/op, 1024-row chunk) | PingCAP 10× blog (single-function benchmark)             |
| End-to-end TPC-H (TiDB 3.0 → 4.0)                    | **~2× faster on average**      | PingCAP "How we improved TPC-H by 100%"                  |
| Chunk RPC CPU savings on TiDB side                   | **~50% reduction** in TiDB CPU during data transfer | Same blog                                                |
| Read p99 from yatp migration                         | **~14% reduction** (mixed)     | TiKV PR #6375                                            |
| Read p999 from yatp migration                        | **~20% reduction** (mixed)     | TiKV PR #6375                                            |
| Non-prepared plan cache speedup                      | 4% TPC-C, 10%+ banking, 15% Sysbench RangeScan | PingCAP 6.0 release notes                            |
| TPC-H TiDB 5.0 MPP vs Greenplum 6.15                 | **2–3×** typical, **8×** on some queries | TiDB 5.0 release notes                            |
| HashJoin redesign in TiDB 8.5                        | **up to 5× faster**            | PingCAP blog "Hash Join Improvements in 8.5"             |

What is **not** published anywhere I could find:

- Per-coprocessor-RPC overhead in absolute µs.
- Network bytes saved from coprocessor pushdown (vs. shipping all rows back),
  in any form. There are anecdotal "we ship kilobytes instead of gigabytes"
  but no benchmark.
- Direct apples-to-apples TiDB vs. CockroachDB throughput-per-vCPU. Each
  vendor publishes their own optimized numbers; the closest neutral
  comparison is a 2020 blog (`blog.dalun.tw`) using tiny VMs and a
  KubeCon 2019 GO-JEK YCSB study, neither rigorous.
- Memory cost per concurrent coprocessor request.

**Methodology credibility check:** the Vectorized blog publishes its
benchmark code (`go test -bench`); the TPC-H numbers are from PingCAP's own
TPC-H 50G report which documents schema, scale factor, and hardware. The
Yatp PR has flame graphs but only relative numbers. None of this would pass
a TPC audit, but the methodology is at least transparent.

---

## 4. What's Notably Different from CockroachDB

### What TiDB+TiKV has that CockroachDB doesn't

1. **Pushdown of computation as a first-class wire-level concept.** CockroachDB
   has DistSender + DistSQL flows that ship work to gateway nodes for the
   appropriate Range, but the Range-leaseholder doesn't run a pluggable
   evaluator: it returns rows to the SQL gateway, which evaluates filters.
   The MVCC scan can apply some predicates (e.g., for the "fetch latest
   version" case), but there is no "operator DAG over the wire to the
   storage replica."
2. **A separate stateless SQL pod from a storage pod.** Independent scaling.
   CockroachDB has all-in-one nodes; you can still run "compute" SQL nodes
   that don't hold data, but the binary is the same.
3. **Mixed-engine planning (TiKV row + TiFlash columnar).** A single SQL
   query can route subplans to row-store TiKV and column-store TiFlash
   simultaneously and join the results in TiDB. CockroachDB has no
   columnar replica equivalent.
4. **Vectorized chunk-based execution end-to-end.** CockroachDB has a
   vectorized executor (`colexec`), but it's a parallel implementation
   chosen by the optimizer, not the universal execution model.
5. **A custom MLFQ work-stealing pool (yatp) with explicit task priorities.**
   CockroachDB uses Go goroutines + admission control queues; less precise.
6. **Resource Control / Request Units** as a first-class throttle integrated
   from query planning down to storage I/O.

### What CockroachDB has that TiDB+TiKV doesn't

1. **One language end-to-end.** No protobuf-versioning dance for new
   functions. Adding a builtin is a single PR. Refactoring storage internals
   doesn't require touching `tipb` and a separate Rust crate.
2. **Geo-distributed transactions with formal correctness focus.** CockroachDB
   has paid more attention to follower reads, locality-aware leaseholder
   placement, and serializable isolation guarantees as a primary feature.
3. **Single deployment unit.** No separate TiKV / PD / TiDB / TiFlash binaries
   to deploy and version-skew-manage.
4. **A cluster-versioning framework (`pkg/clusterversion`)** that gates
   *protocol* changes, not just function support. TiDB/TiKV don't really have
   this — their compatibility is per-feature, ad hoc, and tracked by
   per-version pushdown matrices.
5. **The vectorized executor (`colexec`) is fully integrated with the SQL
   planner**, not a TiFlash-style separate engine.
6. **Backup/restore, changefeeds, and SQL-layer features** are uniformly part
   of the same binary — no equivalent of TiCDC as a separate process.

### Where each architecture has clear advantages

**TiDB+TiKV wins when:**
- Workload is dominated by analytical scans where pushdown matters
  (10×–100× network reduction is realistic for filtered aggregations).
- You want to scale SQL compute independently of storage (a write-heavy ETL
  hour wants more TiKV; a query-heavy report hour wants more TiDB).
- You want HTAP via a separate columnar replica without changing the SQL
  layer.

**CockroachDB wins when:**
- Workload is primarily distributed OLTP with multi-region transactional
  guarantees.
- You value operational simplicity (one binary to deploy and version).
- You're adding new SQL features or builtins frequently (one-language
  development is much faster than the three-repo dance).
- Your query mix is heavy on point reads and small scans where
  coprocessor-style pushdown adds overhead, not value.

### Surprising things worth flagging

1. **The wire format is not bytecode.** I expected something LLVM-IR-ish or
   WASM-ish given the "send a small program" framing. It is just a typed
   operator tree with integer function IDs. The implication for CockroachDB:
   **don't reach for a VM on day one.** A typed protobuf tree handles
   everything TiKV does, and adding a VM adds an entire language-design
   problem on top of an already-hard one.
2. **Coprocessor v2 (the actual plugin/dylib framework) is dead.** Despite a
   stated "long-term goal" to re-implement v1 as a v2 plugin, this never
   happened. PingCAP has not invested in it for 4+ years. **Take it as
   evidence that "open up arbitrary code execution in your storage layer"
   does not survive contact with operations**, even when one company controls
   both ends.
3. **There is no version field in `DAGRequest`.** Compatibility is handled
   *exclusively* by the SQL planner refusing to push down functions that the
   target version doesn't support. The cost is a per-version function support
   matrix that TiDB carries; the benefit is that the wire format itself never
   evolves. If CockroachDB has cluster-version gating already, the function
   matrix becomes trivial.
4. **Coprocessor cache hit returns ~zero bytes.** A repeated analytical scan
   over unchanged data costs one round trip per Region for the version check
   — no row data on the wire at all. CockroachDB's range cache and
   leaseholder routing handle the metadata lookup, but there is no equivalent
   "validate-and-skip" pattern for the data itself.
5. **TiKV's executor library is a separate crate** (`tidb_query_executors`)
   that is independently versionable. This is a structural lesson worth
   internalizing before writing any code: **don't bury the executor in the
   storage server's main crate.**
6. **Per-operator parallelism via goroutines is identical to CockroachDB.**
   The "Rust storage / Go compute" split doesn't change the SQL-layer
   concurrency story at all. Any per-core executor work CockroachDB wants to
   do has no parallel in TiDB; you'd be pioneering it.
7. **TiDB can spill to disk** for HashJoin/Sort/HashAgg via
   `chunk.RowContainer`. This is something the CockroachDB SQL executor does
   too (with `diskmap`), but it's worth noting both projects converged on
   "operator-specific spillers, not a general spilling framework."
8. **There's no pre-execution sandbox or static analyzer for the operator
   tree.** TiKV trusts that TiDB built a sane plan. The deadline + memory
   quota are runtime defenses, not static checks. **In the CockroachDB v2
   architecture, you don't need a verifier if you don't use bytecode** —
   another argument against jumping to a VM.

---

## References

### Source code (HEAD as of clone)

- TiKV coprocessor v1 endpoint: `tikv/src/coprocessor/endpoint.rs`
- TiKV coprocessor v1 module docs: `tikv/src/coprocessor/mod.rs`
- TiKV batch executor framework: `tikv/components/tidb_query_executors/src/runner.rs`
- TiKV expression library: `tikv/components/tidb_query_expr/`
- TiKV aggregation library: `tikv/components/tidb_query_aggr/`
- TiKV coprocessor v2 (dylib plugins): `tikv/src/coprocessor_v2/mod.rs`,
  `tikv/components/coprocessor_plugin_api/src/plugin_api.rs`
- TiKV server config (limits): `tikv/src/server/config.rs` (search
  `end_point_*`)
- tipb DAG/Executor/Expr definitions:
  - `tipb/proto/select.proto` (DAGRequest, SelectResponse)
  - `tipb/proto/executor.proto` (Executor types, ExecType enum)
  - `tipb/proto/expression.proto` (Expr, ScalarFuncSig)
- TiDB coprocessor client: `tidb/pkg/store/copr/coprocessor.go`
- TiDB chunk format: `tidb/pkg/util/chunk/chunk.go`
- TiDB plan cache: `tidb/pkg/planner/core/plan_cache.go`

### Official documentation

- [TiKV Dev Guide — Coprocessor Intro](https://tikv.github.io/tikv-dev-guide/understanding-tikv/coprocessor/intro.html)
- [TiKV Deep Dive — Distributed SQL](https://tikv.org/deep-dive/distributed-sql/dist-sql/)
- [TiDB Dev Guide — Execution](https://pingcap.github.io/tidb-dev-guide/understand-tidb/execution.html)
- [TiDB Dev Guide — Implementation of Vectorized Execution](https://pingcap.github.io/tidb-dev-guide/understand-tidb/implementation-of-vectorized-execution.html)
- [TiDB Dev Guide — Parallel Execution Framework](https://pingcap.github.io/tidb-dev-guide/understand-tidb/parallel-execution-framework.html)
- [TiDB Dev Guide — Plan Cache](https://pingcap.github.io/tidb-dev-guide/understand-tidb/plan-cache.html)
- [TiDB Docs — SQL Prepared Plan Cache](https://docs.pingcap.com/tidb/stable/sql-prepared-plan-cache/)
- [TiDB Docs — SQL Non-Prepared Plan Cache](https://docs.pingcap.com/tidb/stable/sql-non-prepared-plan-cache/)
- [TiDB Docs — TiKV Coprocessor Configuration](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/)
- [TiDB Docs — TiFlash MPP Mode](https://docs.pingcap.com/tidb/stable/use-tiflash-mpp-mode/)
- [TiDB Docs — TiFlash Pipeline Execution Model](https://docs.pingcap.com/tidb/stable/tiflash-pipeline-model/)
- [TiKV Coprocessor v2 Rust docs](https://tikv.github.io/doc/tikv/coprocessor_v2/index.html)
- [coprocessor_plugin_api Rust docs](https://tikv.github.io/doc/coprocessor_plugin_api/)

### Blog posts and talks

- [PingCAP — 10× Performance Improvement for Expression Evaluation (Vectorization)](https://www.pingcap.com/blog/10x-performance-improvement-for-expression-evaluation-made-possible-by-vectorized-execution/)
- [PingCAP — How We Improved TPC-C by 50% and TPC-H by 100%](https://medium.com/@PingCAP/how-we-improved-tpc-c-performance-by-50-and-tpc-h-performance-by-100-31c78f66e399)
- [PingCAP — Hash Join Improvements in TiDB 8.5](https://www.pingcap.com/blog/hash-join-improvements-in-tidb-8-5-part-1/)
- [TiKV Blog — TiKV 3.0 GA](https://tikv.org/blog/tikv-3.0ga/)
- [TiKV Blog — Looking Back at LFX 2021: Coprocessor v2](https://tikv.org/blog/lfx-2021-copr-v2/)
- [Coprocessor in TiKV — andremouche blog](https://andremouche.github.io/tidb/coprocessor_in_tikv.html)
- [TiKV Coprocessor Walkthrough — xieyu blog](https://xieyu.github.io/blog/tidb/coprocessor.html)

### GitHub PRs and issues worth reading

- [TiKV PR #6375 — Allow coprocessor to use yatp](https://github.com/tikv/tikv/pull/6375)
- [TiKV PR #6401 — Replace all readpools with yatp](https://github.com/tikv/tikv/pull/6401)
- [TiKV PR #5155 — Extract `tidb_query` crate](https://github.com/tikv/tikv/pull/5155)
- [TiKV Issue #9747 — LFX: Coprocessor Plugin (v2)](https://github.com/tikv/tikv/issues/9747)
- [TiDB PR #4856 — Column-oriented memory layout](https://github.com/pingcap/tidb/pull/4856)
- [TiDB PR #5178 — Once-per-batch iteration](https://github.com/pingcap/tidb/pull/5178)
- [TiDB Issue #21186 — Leverage Apache Arrow](https://github.com/pingcap/tidb/issues/21186)

### Academic & background

- Daniel Abadi et al., "The Design and Implementation of Modern Column-Oriented
  Database Systems," 2013 (background for chunk format).
- Leis et al., "Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation
  Framework for the Many-Core Age," SIGMOD 2014 (basis for the TiFlash
  pipeline engine).
- Boncz et al., "MonetDB/X100: Hyper-Pipelining Query Execution," CIDR 2005
  (origin of the vectorized batch size constant `BATCH_MAX_SIZE`, which
  TiKV's source comment explicitly references).
