---
author: assistant (Claude); originally drafted in another session and
        validated/restructured 2026-05-13 to peer with the other notes
        in this directory
date: 2026-05-13
---

# TiDB / TiKV — research notes

Distributed SQL system from PingCAP: stateless Go SQL pods (TiDB)
talking over gRPC to a separately-deployed Rust storage tier (TiKV)
that runs an SQL-shaped operator DAG on each Range. Closest production
example we have of "compute pod talks to storage pod over RPC, with
SQL-shaped computation pushed down via a small portable program."
Worth a careful look because the pushdown wire format is not bytecode
— it is a typed protobuf operator DAG with monomorphized function IDs
— and the team has explicitly walked away from a more-flexible
plugin-based alternative.

## 1. System shape

- **TiDB** — stateless Go SQL pod. Parser, optimizer, executor; no
  persistent state. Scales horizontally for compute.
- **TiKV** — stateful Rust storage pod. Owns Ranges (default ~96 MiB),
  serves KV reads and writes plus the **coprocessor** (an SQL-ish
  evaluator scoped to one Range). Scales horizontally for storage.
- **PD** — placement driver; metadata, scheduler, timestamp oracle.
- **TiFlash** — optional columnar replica with its own MPP execution
  engine. Receives a superset of the coprocessor protobufs, runs
  joins/window/exchange/CTE that TiKV refuses.

The coprocessor is the interesting piece for our purposes: it is the
only place TiDB ships SQL-shaped computation across a process
boundary into the storage tier.

## 2. The TiKV coprocessor

### 2.1 Wire format

A coprocessor request is a gRPC `Coprocessor.Request`
(`kvproto/coprocessor.proto`); when `tp == 103` the payload is a
`tipb.DAGRequest` carrying:

- `repeated Executor executors` — a **post-order, leaf-first linear
  encoding** of an operator tree (TiKV) or a tree-shaped
  `root_executor` field (TiFlash, which needs multi-input operators).
- `repeated uint32 output_offsets` — which columns to ship back.
- `EncodeType encode_type` — `TypeDefault` (row), `TypeChunk`
  (column-major), `TypeCHBlock` (TiFlash-only).

Each `Executor` is a tagged union (TableScan, IndexScan, Selection,
Projection, Limit, TopN, Aggregation, IndexLookUp). Expressions are
`tipb.Expr` trees; each scalar function is identified by a
`ScalarFuncSig` numeric enum value. The enum has on the order of
**700 entries** — one per (function, operand-type) combination.
`PlusInt = 203`, `LTReal = 101`, `RegexpUTF8Sig = 4312`,
`JsonExtractSig = 5001`. The plan tree carries already-resolved
type-monomorphized function IDs, so TiKV does not type-check;
it dispatches on a u32.

It is **not bytecode.** No jumps, no register file, no instruction
pointer. Conceptually somewhere between an AST and bytecode: AST
verbosity, with bytecode's no-typecheck property because the planner
has already monomorphized.

### 2.2 What the coprocessor will run

From `BatchExecutorsRunner::check_supported`
(`tikv/components/tidb_query_executors/src/runner.rs`):

| Accepted on TiKV | Rejected (TiFlash-only) |
|---|---|
| TableScan, IndexScan, IndexLookUp | Join |
| Selection (WHERE), Projection | Sort |
| Limit, TopN | Window |
| Simple/Hash/Stream Aggregation | ExchangeSender / ExchangeReceiver |
| | PartitionTableScan, Expand, BroadcastQuery |
| | CTESink, CTESource |

The boundary is **per-Range streaming pipelines, one input, zero or
one output stream.** No shuffle, no build/probe, no shared state
between requests. This isn't a wire-format limitation; TiFlash uses
the same protobufs. It is a deliberate scope on what fits cheaply
into a Range-scoped pushdown engine.

Aggregation is split: the coprocessor returns *partial* aggregates
(`Partial1Mode`/`Partial2Mode`), TiDB's planner inserts a final-mode
aggregator on the SQL side. `count(*)` becomes "TiKV returns
per-region counts; TiDB sums them."

### 2.3 Sandboxing and resource control

The coprocessor never executes user code, so the threat model is
"well-formed plans that may be expensive," not "adversarial code."
Mitigations live in `tikv/src/coprocessor/endpoint.rs`:

- `end_point_memory_quota` — `max(total_mem * 0.125, 500 MiB)`,
  ref-counted per request, reject on full.
- `end_point_request_max_handle_duration` — soft deadline checked
  at every executor `next_batch()`.
- `end_point_recursion_limit = 1000` on the protobuf decoder.
- `end_point_batch_row_limit = 64` initial, doubles up to a max.
- `end_point_max_concurrency` semaphore for "heavy" requests;
  requests under `LIGHT_TASK_THRESHOLD = 5 ms` skip the semaphore.
- **yatp** scheduler — custom Rust work-stealing pool with multi-level
  feedback queues. New requests start high-priority; tasks accumulating
  CPU drop to lower priority. Migration from `tokio-threadpool` to
  yatp (PR #6375/#6401, 2020) reportedly cut p99 read latency by
  ~14% / p999 by ~20% under mixed workloads.

There is no per-request CPU quota; resource isolation is the soft
deadline + MLFQ scheduler + the newer **Resource Control / Request
Units** subsystem (TiDB 7.x) which integrates named resource groups
end-to-end via `kvproto::resource_manager::Consumption`.

### 2.4 Versioning

The single most surprising design choice. There is **no version
field in `DAGRequest`**. Compatibility is achieved by:

1. **Forward-compatible add-only enums.** New executor types are
   appended to `enum ExecType`; new functions to `enum ScalarFuncSig`.
   Old TiKV sees an `ExecType` it doesn't understand and fails the
   request — no graceful degradation at the wire layer.
2. **Function-support negotiation in the SQL planner.** TiDB's
   planner carries a per-TiKV-version pushdown matrix
   (`expression.IsPushDownExpr`); functions not on the list for the
   target version's TiKV simply don't get pushed down — they evaluate
   in TiDB instead.
3. **Mixed-version clusters work** because the planner intersects
   pushdown matrices across reachable TiKVs (resolved via `pd-client`
   at plan time) and falls back to TiDB-side eval for anything the
   minimum version doesn't support.

This is engineering pragmatism, not elegance. Cost: the planner
carries a hand-maintained per-version function-support table, and
adding a new pushdown function is a three-repo dance
(`tipb` → `tikv` → `tidb`) plus a release-gating ladder.

### 2.5 Coprocessor v2 (the dead one)

`tikv/src/coprocessor_v2/` is a separate framework that loads
arbitrary Rust dylibs via `libloading` and hands them raw bytes plus
`RawStorage`. Built in 2021 as an LFX mentorship project, with the
stated long-term goal of re-implementing v1 *as* a v2 plugin. **It
has not been touched at scale in the 4+ years since.** No WASM, no
sandbox; v2 plugins run with full process privileges.

Treat as production evidence that "open up arbitrary code execution
in your storage server" does not survive contact with operations,
even when one company controls both ends of the protocol. (Bytecode
interpretation with an opcode allowlist is a different threat model
and not affected by this lesson.)

### 2.6 Per-request overhead and cache

Realistic mental model: a few hundred microseconds of gRPC + protobuf
framing + region routing + lock check, plus the actual scan work. For
point reads, **TiDB explicitly bypasses the coprocessor** and uses
`tikvrpc.Get` against the storage read pool directly — there is no
benefit to wrapping a single-row read in DAG plumbing.

For analytical queries with stable scans, TiKV maintains a per-Range
"data version." TiDB sends `cache_if_match_version=N`; TiKV returns
just `is_cache_hit=true` with no row data if the data hasn't
changed. The TiDB-side cache holds the prior result; the round trip
is metadata-only.

A wide scan crossing 100 Regions becomes 100 gRPC RPCs; the TiDB-side
`CopIterator` parallelizes them with concurrency
`tidb_distsql_scan_concurrency` (default 15). Initial response batch
is 32 rows, doubling up to a cap. There's a dedicated "small task"
path (`smallConcPerCore=20`) for queries below `CopSmallTaskRow=32`
to amortize per-RPC fixed cost.

## 3. TiDB executor

**Volcano + intra-operator parallelism, with vectorized chunks.**

- The outer skeleton is Volcano: `Open()`/`Next()`/`Close()` on every
  operator. `Next` is **not thread-safe** — only one goroutine ever
  calls `Next` on a given operator.
- Each `Next` returns a `Chunk` of up to 1024 rows in column-major
  layout (`tidb/pkg/util/chunk/chunk.go`). Fixed-width columns inline,
  variable-width via offset arrays. The chunk size is the X100 vector
  size; comments in the chunk source cite the X100 paper directly.
- **Parallelism is internal to operators.** A HashJoin launches N
  inner workers + 1 outer worker; HashAgg launches partial + final
  worker pools. They communicate via Go channels carrying `*Chunk`
  pointers. The parent operator's `Next` just pops from the result
  channel.

There is **no bytecode VM**. Expressions are evaluated by walking
`expression.Expression` trees, with a `VecEval*` fast path that
operates column-at-a-time (~10× speedup over row-at-a-time).
Vectorizing >500 builtin functions was a multi-year community effort
using Go `text/template`-generated code (`builtin_*_vec_generated.go`).

Memory: chunks are pooled and reused across `Next` calls
(`chk.Reset()` keeps backing arrays). Per-query `memory.Tracker`
composes into per-session and per-cluster trackers. On quota hit, an
**OOMAction** chain fires: rate-limit, then spill (HashJoin/Sort/Agg
have operator-specific spillers via `chunk.RowContainer`), then
cancel. There is no arena allocator; TiDB relies on Go GC, with chunk
reuse as the main defense against allocator churn. **GC unpredictability
under load is the most-cited operational complaint about TiDB at
scale.**

### 3.1 Plan caching

Two-level cache (`pkg/planner/core/plan_cache.go`):

1. **Prepared plan cache** — SQL parsed to AST, AST parameterized,
   physical plan cached keyed by `(db_name, stmt_id, schema_version,
   sql_mode, time_zone, ...)`. Re-execute does parameter substitution
   + scan-range adjustment + validity check, skipping parse/optimize.
2. **Non-prepared plan cache** (TiDB 6.0+) — even ad-hoc SQL is
   parameterized on the fly (`SELECT * FROM t WHERE a=1` →
   `... WHERE a=?`) and looked up in the same cache. PingCAP reports
   4-15% improvement on TPC-C / Sysbench / banking workloads from
   this alone.

The cached object is the *physical plan* (`PhysicalPlan` tree), not
the executor tree, not bytecode. Building executors from the plan is
fast and per-execute. The cache is **session-local** — every session
pays a cold-start; cross-session caching has been proposed multiple
times but not shipped.

### 3.2 Concurrency model

Goroutine-per-query at the request layer, plus
goroutines-per-operator-worker inside the query. No per-core pinned
executor or async runtime. A query acquires goroutines as needed and
releases them at `Close`. The TiFlash MPP path adds something like
async pipeline scheduling, but only inside TiFlash (C++/ClickHouse-
derived).

## 4. Published quantitative outcomes

PingCAP-published, treat as upper bounds:

| Metric | Value | Source |
|---|---|---|
| Vectorized vs row expression eval | **~5.4× faster** (38461 → 7056 ns/op, 1024-row chunk) | PingCAP "10× perf" blog |
| End-to-end TPC-H (TiDB 3.0 → 4.0) | **~2× faster on average** | PingCAP "TPC-H 100%" blog |
| Chunk RPC CPU savings on TiDB side | **~50% reduction** during data transfer | Same |
| Read p99 from yatp migration | **~14% reduction** (mixed workloads) | TiKV PR #6375 |
| Read p999 from yatp migration | **~20% reduction** (mixed workloads) | TiKV PR #6375 |
| Non-prepared plan cache speedup | 4% TPC-C, 10%+ banking, 15% Sysbench RangeScan | TiDB 6.0 release notes |
| TPC-H TiDB 5.0 MPP vs Greenplum 6.15 | **2-3× typical, 8× on some** | TiDB 5.0 release notes |

What is **not** published anywhere I could find: per-coprocessor-RPC
overhead in absolute µs; network bytes saved by pushdown vs
ship-all-rows; direct apples-to-apples TiDB vs CockroachDB
throughput-per-vCPU.

## 5. What to take from this — analysis

> *Marked as analysis (assistant's reading). The §1-4 content above
> is source-derived; this section is what's worth pulling forward
> for the broader investigation.*

### What's directly informative

- **Per-Range streaming-pipeline scope.** TiKV converged on a
  deliberately narrow set of operators (TableScan, Selection,
  Projection, Limit, TopN, partial Aggregation) — joins/sort/window
  are explicitly out. This boundary fell out naturally from "what
  fits cheaply on a single Range," not from wire-format choice. Any
  CRDB equivalent for KV-side execution would land at the same
  boundary regardless of substrate.
- **Aggregation-split pattern.** Partial aggregates in storage,
  final-mode merge at the SQL layer. Ships kilobytes for queries that
  would otherwise return millions of rows. Worth adopting if any
  KV-side execution work happens.
- **Coprocessor-cache pattern.** Per-Range data version + "validate
  and skip" on the request. Returns ~zero bytes on cache hit.
  CockroachDB has range-cache and leaseholder routing for metadata
  but no equivalent data-version cache for the query result.
- **Executor library as a separate crate.** `tidb_query_executors`,
  `tidb_query_expr`, `tidb_query_aggr` were extracted out of the
  storage server early. The split made compile times tractable and
  let the executors evolve without touching the endpoint. Structural
  lesson worth borrowing on day one of any equivalent CRDB work.
- **Autoparameterization of non-prepared SQL** (TiDB 6.0+) — extends
  the plan-cache benefit from the prepared-statement path to
  ORM-generated literal SQL. Reported ~4-15% workload-level gains.
  Same idea Clustrix used at the cache key (see
  [`clustrix-xpand.md`](clustrix-xpand.md)); a separate-but-coupled
  workstream from any CRDB execution-engine investment.

### Where the analogy breaks down

- **Wire boundary.** TiKV runs the operator DAG inside the storage
  *process*. CockroachDB doesn't have a separate storage process —
  Pebble and the DistSQL processors live in the same binary on each
  node. CRDB's DistSQL flows do ship processors (with filters,
  projections, partial aggregations) to the data-holding nodes, so
  computation does run remotely; the wire boundary it crosses is
  gateway-SQL → remote-node-SQL, not SQL → storage. The KV layer
  itself does not run a pluggable SQL evaluator.
- **Versioning machinery.** TiKV's per-version pushdown matrix is
  a hand-maintained workaround for not having a protocol-versioning
  framework. CockroachDB has `pkg/clusterversion` for protocol-level
  gating, which is the right *kind* of machinery — substantially
  easier to leverage than hand-maintained tables — but applying it to
  per-function pushdown gating still requires real plumbing
  (defining gates, integrating with planner pushdown decisions,
  handling mixed-version states).
- **Per-core executor.** TiDB doesn't have one; their MPP path is
  inside TiFlash (a separate C++ engine). Any per-core executor
  work in CRDB would be pioneering rather than borrowing.
- **GC discipline.** TiDB's main known operational pain is
  GC-unpredictability at scale — a real cost of being on Go.
  Validates that allocation discipline matters for any new CRDB
  execution engine on the same runtime.

### Non-takeaways

- **The "send a small program" framing does not require bytecode.**
  TiKV's typed-protobuf operator DAG handles everything they need.
  Whether bytecode is the right substrate for CRDB is a separate
  question with its own tradeoffs (see
  [`../discussions/bytecode-vs-volcano.md`](../discussions/bytecode-vs-volcano.md));
  TiKV's choice of DAG was structurally forced by TiDB having no
  bytecode VM to reuse, not by an argument that DAG is universally
  superior.
- **Coprocessor-v2-style "load arbitrary native code in storage" is
  a dead pattern.** This says nothing about safer pushdown
  mechanisms (bytecode + opcode allowlist, sandboxed WASM).

## 6. Open questions

- **Coprocessor-cache equivalent for CRDB.** TiKV's data-version
  caching pattern looks powerful for repeated analytical queries.
  Worth a focused look at whether CRDB has anything analogous and
  what it would take to add.
- **CockroachDB admission control vs TiKV's yatp.** Yatp's MLFQ
  scheduler reportedly cut p99 by 14% / p999 by 20% on mixed
  workloads. Does CRDB's KV admission control provide similar
  tail-latency benefits, or is there headroom for similar work
  on the KV-side scheduler?
- **The autoparameterization workstream specifically.** Independent
  of any execution-engine work, would extending CRDB's plan cache
  to autoparameterize literal SQL capture a meaningful chunk of the
  Phase-1-style win on non-prepared traffic? Probably the cheapest
  large-impact follow-on to Phase 1.

## References

### Source code (TiKV / TiDB / tipb, HEAD as of 2026-04 clone)

- `tikv/src/coprocessor/endpoint.rs` — coprocessor v1 endpoint
- `tikv/components/tidb_query_executors/src/runner.rs` — batch
  executor framework, `check_supported`
- `tikv/components/tidb_query_expr/`, `tidb_query_aggr/` —
  expression and aggregation libraries
- `tikv/src/coprocessor_v2/mod.rs`,
  `tikv/components/coprocessor_plugin_api/src/plugin_api.rs` —
  the dead plugin framework
- `tikv/src/server/config.rs` — search `end_point_*` for limits
- `tipb/proto/select.proto`, `executor.proto`, `expression.proto` —
  DAGRequest, Executor, Expr, ScalarFuncSig
- `tidb/pkg/store/copr/coprocessor.go` — TiDB coprocessor client
- `tidb/pkg/util/chunk/chunk.go` — chunk format
- `tidb/pkg/planner/core/plan_cache.go` — both plan caches

### Documentation and writeups

- [TiKV Dev Guide — Coprocessor Intro](https://tikv.github.io/tikv-dev-guide/understanding-tikv/coprocessor/intro.html)
- [TiKV Deep Dive — Distributed SQL](https://tikv.org/deep-dive/distributed-sql/dist-sql/)
- [TiDB Dev Guide — Execution](https://pingcap.github.io/tidb-dev-guide/understand-tidb/execution.html)
- [TiDB Dev Guide — Vectorized Execution](https://pingcap.github.io/tidb-dev-guide/understand-tidb/implementation-of-vectorized-execution.html)
- [TiDB Dev Guide — Plan Cache](https://pingcap.github.io/tidb-dev-guide/understand-tidb/plan-cache.html)
- [TiDB Docs — Non-Prepared Plan Cache](https://docs.pingcap.com/tidb/stable/sql-non-prepared-plan-cache/)
- [PingCAP — 10× Performance Improvement (Vectorization)](https://www.pingcap.com/blog/10x-performance-improvement-for-expression-evaluation-made-possible-by-vectorized-execution/)
- [PingCAP — TPC-H 100% improvement](https://medium.com/@PingCAP/how-we-improved-tpc-c-performance-by-50-and-tpc-h-performance-by-100-31c78f66e399)
- [TiKV Blog — Looking Back at LFX 2021: Coprocessor v2](https://tikv.org/blog/lfx-2021-copr-v2/)
- [TiKV PR #6375 — yatp scheduler](https://github.com/tikv/tikv/pull/6375)
- [TiKV PR #5155 — Extract `tidb_query` crate](https://github.com/tikv/tikv/pull/5155)
- [TiKV Issue #9747 — LFX: Coprocessor Plugin (v2)](https://github.com/tikv/tikv/issues/9747)
