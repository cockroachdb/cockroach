# SQL Executor Redesign — Research and Design Notes

**Status:** Living document. Updated as investigations land.
**Started:** 2026-04-30
**Owner:** Matt White
**Branch context:** Builds on PR #168824 (point-select fast-path experiment) and the CPU-affinity follow-on in this worktree (`cpu-affinity` branch).

---

## Context and Goal

The point-select fast-path experiment (PR #168824) demonstrated that per-execute SQL pipeline overhead is the dominant cost for simple OLTP queries:

- **+47–66% per-vCPU throughput** on Linux for the fast-path-eligible query class
- **Max QPS at p99 ≤ 500µs: 30K → 68K (+124%)**
- Wins come from skipping per-execute setup: planning context construction, flow construction, DistSQL receiver init

The **strategic goal** is to generalize that win — bring per-query CPU cost down for *all* prepared statements, not just the narrow PK-lookup case the fast path handles. The product-level framing is workload economics: serve a given workload at a given p99 SLO with fewer cores. That widens the set of customer workloads CRDB can be deployed for at viable cost.

The throughput-per-vCPU framing matters because it determines which optimizations are worth pursuing:
- Wins that reduce CPU work per query (the SQL pipeline win) directly increase QPS-per-vCPU
- Wins that only reduce per-query wall time (e.g., parallelizing fan-out) only help if they let queries fit a budget that they otherwise miss; otherwise they're throughput-neutral

---

## Investigations Completed

### TiKV / TiDB Architecture (2026-04-30)

**Why this investigation:** TiDB+TiKV is CockroachDB's most direct competitor and the closest production analog to several architectural directions we'd been considering — split-binary deployment (Go SQL pod, Rust storage), shipping computation to storage nodes, and a per-engine pushdown model. Full report at `tidb_tikv_research.md` in this directory (~4850 words, ~50 references).

**Top findings that change our v2 thinking:**

1. **TiKV's coprocessor wire format is *not* bytecode. It is a typed protobuf operator DAG.** Specifically: `tipb.DAGRequest` carrying a list of `tipb.Executor` nodes (TableScan, Selection, Projection, Limit, TopN, various Aggregations — that's it) plus `tipb.Expr` trees. Each scalar function is a numeric `ScalarFuncSig` enum (~700 monomorphized entries). TiKV interprets the tree using pre-compiled Rust functions; there is no VM, no bytecode, no register file, no jumps. PingCAP went the *other direction* from "send a small program" and it's worked at scale for ~10 years.

2. **Coprocessor v2 — the actual "ship Rust dylibs to your storage server" framework — is dead.** Built in 2021 as an LFX mentorship project with the stated long-term plan of re-implementing v1 as a v2 plugin. Four years later it remains untouched and unused. Strong evidence that "open up arbitrary code execution in your storage layer" is operationally untenable even when one company controls both ends of the protocol.

3. **There is no version field in `DAGRequest`.** Compatibility is handled exclusively by the SQL planner carrying a per-TiKV-version function-pushdown matrix and silently degrading to TiDB-side eval for unsupported functions. Wire format is append-only protobuf; version negotiation lives in the planner, not in the protocol. Adding a new pushdown function is a three-repo dance (`tipb` → `tikv` → `tidb`) gated by a release.

4. **TiKV explicitly rejects joins, sort, window, exchange, CTE.** See `BatchExecutorsRunner::check_supported`. The natural shape for "what fits on one Range" is "single-input streaming pipelines, one input, zero or one output." Those operators only exist on TiFlash (their separate columnar engine). The boundary is more constrained than I'd assumed and falls out of physical constraints.

5. **TiDB has no bytecode VM and no per-core executor.** Their SQL execution layer is Volcano + intra-operator parallelism via goroutine workers, with vectorized 1024-row chunks (their `colexec` analog). This is the *same model as CockroachDB today*. The "Rust storage / Go compute" split changes nothing about SQL-layer concurrency. Per-core executor work in CRDB would be **pioneering, not copying**.

6. **Vectorized expression evaluation gave TiDB ~5.4× speedup** (38461 ns/op → 7056 ns/op for a representative function); end-to-end TPC-H improved ~2× from vectorization across versions; "Chunk RPC" (skipping a row→column re-encoding step) cut TiDB CPU by ~50% during data transfer. These are the published wins for the architectural direction TiDB went.

7. **TiDB's main known operational complaint is GC unpredictability under load.** Validates the discussion we had about Go's GC being acceptable for OLTP latency budgets *with* allocation hygiene, but a real ongoing cost at scale.

8. **Aggregation is split: partial in TiKV, final in TiDB.** Partial-mode results are tiny (count-per-region, sum-per-region), final-mode merges them. Pattern we should adopt for v2.

9. **Coprocessor cache returns ~zero bytes on hit.** TiKV maintains a "data version" per Region; TiDB sends `cache_if_match_version` and TiKV returns just `is_cache_hit=true` if data is unchanged. Pattern worth lifting.

10. **Yatp scheduler (custom Rust MLFQ work-stealing pool) cut p99 read latency by ~14%, p999 by ~20% under mixed workloads** vs. tokio-threadpool. Validates that scheduler choice matters at the storage layer; CRDB's KV admission control may have similar headroom.

11. **The executor library was extracted into separate crates (`tidb_query_executors`, `tidb_query_expr`, etc.) early in the project's life.** A structural lesson: don't bury the executor in the storage server's main package. Plan for independent versioning of the execution layer from day one.

**Implications for our architecture (substantive):**

- **For v2: keep bytecode as the wire format, but lift TiKV's scope/packaging/sandbox patterns.** TiKV chose operator DAG because TiDB had no bytecode VM to reuse — they extended their existing planner output. CockroachDB's situation is different: a v1 bytecode VM produces wire-format-shaped bytecode that can be reused for v2 with one compiler and two interpreters, vs. inventing a parallel DAG representation. The TiKV lessons that *do* transfer are: scope to single-Range streaming pipelines, split aggregations partial/final, structure the executor as a separate package from day one, sandbox by construction (forward-only flow, opcode allowlist, resource limits) rather than by isolation. The Coprocessor v2 abandonment was about loading native dylibs, not about bytecode interpretation; bytecode + opcode allowlist is structurally similar to DAG for safety purposes.

- **For v1: opcode-number stability still matters — but for different reasons than originally framed.** Originally framed as "wire-protocol forward compatibility for v2." That's still the right reason, but with cluster-version gates handling the negotiation, the discipline is "stable IDs + cluster-version-gated rollout of new opcodes," not "design for backward-incompatible opcode-set changes." TiKV's per-version function matrix is what we *don't* need because we have proper version gates already.

- **An alternative path A' worth considering: extend colexec coverage rather than build a new bytecode VM.** TiDB's wins came from vectorization, not a VM. CockroachDB has colexec already. The fast-path PR's wins were on OLTP point queries (where colexec doesn't directly apply), but a substantial fraction of "SQL pipeline overhead" might be capturable just by routing more queries through colexec instead of through the planNode tree-walker. Worth evaluating before committing to the bytecode-VM path.

- **Validates the v3 endgame partition.** TiKV doesn't do inter-storage-node coordination either; "complex" pushdowns (joins, exchanges, window) live on TiFlash with morsel-driven parallelism. The boundary between v2 (single-Range pushdown) and v3 (cross-Range coordination) is real and TiKV's v3-equivalent is a separate columnar engine, not just "more pushdown."

- **The per-core executor question stays open.** TiDB doesn't do it; nobody in this comparison set does. That's evidence it's not necessary, but also evidence it's a pioneering opportunity if the workload-economics case is strong enough. Path B's preserved option remains relevant; the bar to actually exercise it is now higher because there's no production analog to crib from.

### CPU Affinity (2026-04-26 — 2026-04-27)

**Hypothesis:** Linux kernel-level thread→core migration is costing measurable performance for CRDB's hot path. Per-thread pinning should improve throughput-per-vCPU by preserving cache locality.

**Setup:** GCE n2-custom-24 (24 vCPUs), GOMAXPROCS=24, `BenchmarkPointSelect` from `pkg/sql/tests`. Per-thread pinning via a 2s ticker that walked `/proc/self/task` and called `unix.SchedSetaffinity` round-robin across cores from `/sys/devices/system/cpu/online`. Code in `pkg/sql/tests/cpu_pin_linux_test.go` on this branch.

**Result:**

| concurrency | original PR baseline p99 | pinned p99 |
| ----------- | ------------------------ | ---------- |
| 1           | 390 µs                   | 406 µs     |
| 64          | 2,350 µs                 | 4,469 µs (~2×) |
| 128         | (n/a)                    | 11,567 µs  |

`perf stat` confirmed pinning was 100% effective: cpu-migrations dropped from O(thousands)/sec to **0/sec**, context-switches dropped to ~4/sec.

**Diagnosis:** Pinning doubled p99 at high concurrency while eliminating migration entirely. Three compounding causes:

1. Go's M:N scheduler decouples goroutines from threads. There's no stable thread→work mapping for pinning to preserve.
2. With pinned Ms, inter-goroutine handoffs serialize on the receiving goroutine's specific assigned core, rather than landing on whichever core is idle.
3. Round-robin pinning by `/proc/self/task` order assigns Go's startup-time background threads (sysmon, GC workers, finalizer) to the early cores, leaving them ~idle while benchmark workers concentrate on later cores.

**Conclusion:** Per-thread CPU pinning is **not viable as a standalone optimization** for CRDB's current architecture. The kernel scheduler's load balancing is a *feature* for handoff-heavy Go workloads, not a cost.

**What it would take to make affinity pay off:** A different architecture — per-core executor + per-core data placement + key→core routing. That's the v2/v3 work below, not a tweak.

**Pinned in memory:** `project_cpu_affinity_negative_result.md` — when affinity is proposed in future, push back unless the proposal also restructures data placement and execution model.

---

## Architecture Ladder

A three-step ladder that compounds in value but doesn't require the next step. Each is independently shippable.

### v1 — SQL-pod bytecode VM
**Scope:** Generalize the fast-path win to all prepared statements. VM lives in the SQL pod, KV access uses existing API (returns owned bytes after RPC).

**Win:** Reduces per-execute SQL pipeline overhead across the board. Foundation for everything later.

**Status:** Not started. Designed below.

### v2 — KV-side execution (bytecode, scoped to single-Range pipelines)
**Scope:** Ship bytecode programs (the same bytecode emitted by v1's compiler) to KV nodes via RPC. KV node has its own interpreter for the v2 opcode subset. Single-Range streaming pipelines only — no joins, sort, window, exchange, CTE on the storage side.

**Win:** Adds zero-copy from Pebble pages + dramatic network-bytes reduction (filter/project happen at storage, only matched columns return). Big win for read-heavy queries that scan many rows and project few. Aggregations split: partial in storage, final in SQL pod.

**On the bytecode-vs-DAG question (2026-04-30):** TiKV ships typed protobuf operator DAGs, not bytecode. They picked DAG because TiDB's planner is Volcano-based and emits operator trees natively — DAG was the natural extension of their existing compiler output. CockroachDB's situation is different: if v1 emits bytecode, that bytecode is already wire-format-shaped, and reusing it for v2 means one compiler / two interpreters / one debug surface — substantively simpler than maintaining a parallel DAG representation just for v2. The TiKV evidence that informs us is about scope (single-Range streaming pipelines) and packaging (separate executor library), not about wire format choice.

**Sandboxing in a bytecode design:** achievable by construction — forward-only flow, instruction count cap, deadline check at backward branches, statically-sized register file, opcode allowlist for the v2 subset. The Coprocessor v2 abandonment was about loading native dylibs (full process privileges), not about bytecode interpretation. Bytecode + opcode allowlist + runtime resource limits = same structural safety as DAG.

**Pattern reference (for scope/aggregation split/cache):** TiKV coprocessor v1 (`tipb.DAGRequest`), HBase coprocessors, BigQuery / Spanner predicate pushdown. See `tidb_tikv_research.md` in this directory.

**Status:** Pending v1. Specific v2 design notes captured in "v2 Design Decisions" below.

### v3 — Distributed execution
**Scope:** Full inter-storage-node coordination. Joins, aggregates, intermediate result staging. KV nodes communicate with each other to compose query fragments, not just SQL pod ↔ storage.

**Win:** Endgame. Multi-range queries become as cheap as single-range.

**Status:** Years out. Mentioned for completeness; no design work yet.

---

## v1 Design Decisions (as of 2026-04-30)

### Bytecode VM, flat dispatch

`for { switch op { ... } }`. Explicit operand stack, explicit pc, no Go-level recursion. Replaces today's planNode tree-walker, which recurses through Go's call stack. Bounded native stack per query regardless of nesting.

**Open alternative (2026-04-30): expand `colexec` coverage instead of building a new VM.** TiDB's wins came from vectorized chunk-based execution (their colexec analog), not from a bytecode VM — they don't have one. CockroachDB has colexec already; a substantial fraction of the SQL pipeline overhead the fast-path PR captured might also be capturable just by routing more queries through colexec instead of through the planNode tree-walker. The two paths address different parts of the workload: colexec is row-batch oriented (favors analytical and medium-result queries), bytecode VM favors single-row OLTP (no batch overhead for 1-row results, which is where the fast-path PR demonstrated headroom). Worth measuring before committing — specifically, what fraction of the fast-path PR's win comes from "skip per-execute setup" vs. "tighter inner-loop dispatch"? If most is the former, both colexec-extension and a VM capture it. If meaningful gain is in the latter, the VM is required for OLTP point queries that don't fit colexec's batch model.

### Path B: state-machine VM with explicit suspend/resume

The dispatch loop checks for suspend after each opcode. In v1 the dispatcher always synchronously calls the resume — opcodes that "would park" simply complete and resume immediately. Costs ~5-10% on the VM hot path vs. a pure recursive interpreter. Buys: option to add a per-core async executor later without changing the VM.

**Why path B over path A:** The throughput-per-vCPU framing makes the per-core-executor option valuable enough to keep open. Path B's overhead is the price of preserving that option. The decision is reversible only by paying the cost of rewriting the VM.

### Sync opcodes in v1, no per-core dispatcher

Goroutine-per-query is fine for v1. Per-core executor + key→core routing is a separate project, deferred. The VM design accommodates it (state-machine dispatch, explicit suspend/resume) but doesn't ship it.

### Async wrapper for fan-out KV opcodes

For multi-key, lookup-join, or batch operations, wrap the existing sync KV API in goroutine-pool wrappers that fire N requests in parallel and aggregate via a per-VM response channel. Cost: ~50-80ns per call via worker pool (vs. ~200ns per goroutine spawn). Win: cuts wall time for fan-out queries from N×RTT to ~RTT, which lets queries fit p99 budgets they'd otherwise miss.

This is *concurrency*, not affinity. Throughput-per-vCPU neutral; latency-budget positive.

### Dynamic register file, compile-time layout

Per-query layout determined when the bytecode is compiled. Each query gets exactly the registers it needs. Single allocation per query (or zero, with prepared-statement caching).

```
Layout descriptor (compile-time):
    register_count, slot_types[], slot_offsets[]
Allocation (per query, or once per prepared stmt):
    buffer = make([]byte, layout.Size)
```

### Tagged-union Value cells, fixed-size

Each register slot is a fixed-size cell that can hold any scalar (int, bool, short string, timestamp) without heap allocation. Variable-width data (long strings, byte slices, JSON, tuples) goes in a per-query arena; the register holds an arena `(offset, length, tag)` handle.

### Per-query scratch arena for intermediates

Reset per row (for streaming queries) or per query (for OLTP). Variable-width intermediate values live here. `sync.Pool`'d, size-classed.

### Caller-owned output arena (ownership inversion)

The VM does NOT own the result row buffer. pgwire (or the KV writer, or downstream operator) provides a `RowSink` and the VM appends directly into it. Eliminates the "values that need to outlive the arena" problem entirely — there's no boundary copy, the lifetime is whatever the consumer needs.

```go
type RowSink struct {
    buf       []byte
    layout    *RowLayout
    onRowDone func()
}
func (s *RowSink) ReserveRow(maxBytes int) []byte
func (s *RowSink) CommitRow(actualBytes int)
```

Pattern reference: Clustrix's input-arena-for-output-rows.

### Prepared-statement cached register file

Stash the register-file buffer + per-query scratch arena on the prepared statement object. First execute allocates; every subsequent execute is allocation-free. Matches OLTP workload shape (small set of prepared statements, very high frequency).

### Zero-allocation hot path, CI-enforced

`testing.AllocsPerRun(N, vm.Step)` returning != 0 fails CI. `gcassert:inline` and `gcassert:bce` annotations on the unsafe-layout helpers. The dispatch loop, opcode handlers, register read/write all allocate-free per call.

### Stable opcode numbers, opcode metadata

Opcode IDs are forward-compatibility identifiers from day one — never repurpose, deprecate-but-don't-delete. Each opcode carries metadata: `remoteable bool` (eligible for the v2 KV-side opcode subset), resource bounds (max instructions, max memory), capability requirements (read-only, requires-txn, etc.).

The forward-compat reasoning: if v2 ships v1's bytecode over the wire, opcode numbers become a real wire-protocol surface and need cluster-version gating for new additions. CockroachDB's existing version-gate machinery handles this cleanly — much simpler than TiKV's hand-maintained per-version function matrix, but discipline-wise it's the same shape: "this opcode is supported on storage nodes at cluster version ≥ X."

### unsafe.Pointer-backed register file (probable)

The register file is a `[]byte` with manual layout via `unsafe.Pointer` + offset arithmetic. Helpers:

```go
//gcassert:inline
//gcassert:bce
func loadInt64(buf []byte, off int) int64 {
    return *(*int64)(unsafe.Pointer(&buf[off]))
}
```

Confined to a tight low-level package with extensive tests. Reviewers approve once.

Pattern reference: Clustrix used C macros for the same purpose; never caused production problems. Discussed in conversation 2026-04-30.

### Single Go runtime

Rust/Tokio embedding (independent runtime in the same process, shared-memory channels for work dispatch) was considered. Architecturally coherent but operational cost is severe: two toolchains, two memory models, two crash dump formats, KV-back-to-Go boundary problem for any opcode that needs further fetches. Marginal CPU win (~10-20% from no-GC + zero-cost abstractions) doesn't justify the operational doubling.

If Rust ever makes sense for CRDB, it's as a separate process for the v3 storage-side execution engine — TiKV-style — not embedded in the SQL pod or KV node.

### GC: zero-alloc VM is necessary, not sufficient

Zero-alloc VM removes the VM's *contribution* to GC pressure. It does not eliminate STW pauses or concurrent-mark CPU steal caused by the rest of the process. For OLTP p99 budgets in the 100s-of-µs to ms range, with proper system-wide allocation hygiene + GOGC tuning, GC overhead can be brought to a few percent of CPU. That's acceptable for the workload-economics framing.

Aesthetic preference for "zero GC" (e.g., Rust) is real but should not drive architecture decisions for CRDB's actual SLO targets.

---

## v2 Design Decisions (drafted 2026-04-30, post-TiKV research)

These are tentative — v2 is pending v1 — but the TiKV evidence is concrete enough to write down so the v1 design doesn't paint v2 into a corner. Some of these choices lift directly from TiKV's pattern (operator scope, aggregation split, packaging discipline); others (notably wire format) are deliberately *not* what TiKV did because our v1 starting point is different.

### Wire format: bytecode, reusing v1's emission

If v1 builds a bytecode VM, the bytecode it produces *is* the v2 wire format. One compiler, two interpreters (SQL-pod and KV-side). One representation through EXPLAIN, debug dumps, profile traces, and over the wire.

**Why not TiKV's typed-DAG protobuf format:** TiDB picked DAG because their SQL planner is Volcano-based and emits operator trees natively — DAG was the natural extension of what their compiler already produced. They never had a bytecode layer. CockroachDB's situation is different: if v1 emits bytecode, inventing a parallel DAG just for v2 means two compilers, two formats, two executors — gratuitous when the existing bytecode can ship over the wire as-is. Adding a pushdown function with bytecode is one PR; with DAG it's TiKV's three-repo dance.

**Sandboxing in a bytecode design:** the things TiKV gets "for free" from DAG (no infinite loops, bounded memory, no arbitrary code) are achievable in bytecode by construction:
- Forward-only or loop-counter-bounded control flow
- Instruction count cap + wall-clock deadline check at backward branches
- Statically-sized register file from the compiled program's layout descriptor
- Opcode allowlist for the v2 subset (no opcodes that touch SQL-side state, no UDF dispatch, no recursion)

The Coprocessor v2 abandonment lesson is about *loading native code* (dylibs with full process privileges), not about bytecode interpretation. WASM, eBPF, and other constrained bytecode systems are routinely sandboxed. The lesson is "don't load native code in your storage server"; bytecode can be made structurally safe.

### Versioning via cluster-version gates + opcode-support negotiation

Bytecode opcode IDs are stable identifiers (never repurpose, deprecate-but-don't-delete). New opcodes are added in a cluster-version-gated way: the SQL planner asks "does this storage node support opcode X?" via the cluster version registry and silently degrades to SQL-pod evaluation for unsupported opcodes. CockroachDB's existing cluster-version machinery makes this much cleaner than TiKV's per-version function matrix that they had to build by hand.

Same pattern as their per-version function whitelist, just expressed through bytecode opcode IDs instead of `ScalarFuncSig` enum values, and gated by an existing CRDB mechanism instead of a hand-maintained table.

### Allowed operators on the storage side

Mirror TiKV's scope: TableScan, IndexScan, IndexLookUp, Selection (WHERE), Projection, Limit, TopN, partial Aggregation (Simple, Hash, Stream). Explicitly *not*: Join, Sort (across regions), Window, Exchange, CTE — those stay at the SQL pod.

This constraint is enforced at v2 by "the v2 opcode subset doesn't include those operations" plus a planner-side check that programs targeting v2 only use v2-eligible opcodes. Same boundary TiKV converged on after a decade; arrived at by physical constraints (single-Range streaming pipelines, one input, zero or one output) rather than wire format choice.

### Aggregation split

Partial-mode aggregations on the storage side (per-Range counts, sums, group-by hashes), final-mode merge at the SQL pod. Same pattern TiKV uses. Tiny network payloads even for full-table aggregations.

### Coprocessor cache pattern

If the storage layer can maintain a "data version" per Range (or per scanned key range), the SQL pod can send `cache_if_match_version=N` and the storage layer can return `cache_hit=true` without shipping any rows. For analytical queries that re-run over slowly-changing data, this is potentially zero-byte responses on subsequent runs.

CockroachDB's range cache + leaseholder routing handles metadata caching but there's no equivalent "validate-and-skip" pattern for the data itself. Worth considering.

### Executor library as a separate package from day one

TiKV's `tidb_query_executors`, `tidb_query_expr`, `tidb_query_aggr` crates are independently versionable from the storage server. Lesson learned the hard way after compile times grew. CockroachDB should structure the v2 executor as `pkg/sql/storageexec` (or similar) from day one — separate from `pkg/kv/kvserver` and from `pkg/sql/exec`.

### Per-Range RPC fanout, with batching for small tasks

A scan crossing N Ranges becomes N concurrent RPCs. TiKV's `CopIterator` parallelizes via a configurable concurrency setting (default 15). Initial batch is small (32 rows), doubling up to a max — minimizes latency for small results, amortizes RPC overhead for large ones. CockroachDB's DistSender already does Range-level fanout for many operations; v2 adds the "with computation pushed down" variant.

---

## Variable-Width Data Strategy

The genuine hard problem the v1 design has to handle: row widths aren't known at compile time. Three sub-questions, each answered:

**1. Where does it live?** Arena, register holds `(offset, length, tag)` handle. Small scalars inline in the Value cell; anything variable in the arena.

**2. When does the arena reset?** Per query (cached on prepared statement) for OLTP; per row for streaming; per batch for vectorized. Different reset cadences for different query classes.

**3. What about values that outlive the arena?** Nothing does, because the output ownership inversion means the result row is written into the consumer's buffer, not the VM's arena. Output crosses no boundary.

Nested variable data (ARRAY, JSONB, tuples-of-tuples) gets recursive arena layout — each nested value writes its own header (length + element count + offset table) into the arena, parent's handle points at the header, readers walk lazily. Pattern reference: PostgreSQL varlena, DuckDB nested types.

**Open question (unresolved from Clustrix experience):** Does arena growth-via-realloc become measurable cost at high QPS for unusually-wide rows? Mitigations exist (chunked arena, max-width prediction from prepared statement) but unknown if needed.

---

## Open Questions / TODO

### High priority

- [ ] **Bytecode VM vs. extending colexec for v1.** TiKV/TiDB research surfaced this as a real alternative — TiDB's wins came from vectorization, not a VM. The fast-path PR's gains are on OLTP point queries (where colexec's batch model doesn't fit) but a substantial fraction of "SQL pipeline overhead" might be capturable just by routing more queries through colexec. Specific question to answer: *what fraction of the fast-path PR's win is "skip per-execute setup" vs. "tighter inner-loop dispatch"?* If most is the former, both paths capture it; the colexec extension is dramatically less new code. If meaningful gain is in the latter, the VM is needed for OLTP point queries.
- [ ] **CockroachDB's KV admission control vs. TiKV's yatp.** Yatp's MLFQ scheduler reportedly cut p99 by 14% / p999 by 20% under mixed workloads. CockroachDB has admission control; does it provide similar tail-latency benefits, or is there headroom for similar work on the KV-side scheduler?
- [ ] **Coprocessor-cache equivalent for CockroachDB.** TiKV's "data-version-based cache hit returns no rows" pattern could be powerful for analytical queries with stable scans. Does CockroachDB have any equivalent? What would it take to build one?

### Medium priority

- [ ] **SQLite VDBE register-file layout and opcode design.** Less critical than originally framed since v2 will likely use operator DAG (not bytecode), but still relevant for v1's internal representation if we go the VM route. Concrete reference for register-file layout, tagged-union design, opcode dispatch.
- [ ] **DuckDB string heap / vector arena.** Variable-width data management at the state-of-the-art. Probably more relevant than VDBE if we end up extending colexec instead of building a VM.
- [ ] **PostgreSQL TupleTableSlot + memory contexts.** Hierarchical arena pattern with decades of hardening.

### Lower priority

- [ ] **Wasmtime / V8 embedding patterns.** Mostly closed off by TiKV's evidence on Coprocessor v2 abandonment. Re-open only if the Rust/Tokio question resurfaces with new motivation.
- [ ] **HBase coprocessor framework.** Largely subsumed by the TiKV research.

---

## Decisions Deferred

These were considered in the design discussion and consciously not pursued (yet). Included so future-readers don't re-litigate.

- **Per-core async executor (Path B's eventual payoff).** Path B preserves the option; v1 does not exercise it. Decision to ship requires a separate project of comparable size to v1, and a product story that justifies the per-vCPU efficiency win against the implementation complexity. **Updated 2026-04-30:** TiDB also doesn't do this; nobody in our comparison set does. That's evidence it's unnecessary for the workload class but also that pursuing it would be pioneering, not following an established pattern.
- **Rust/Tokio embedding.** Operational cost (two runtimes, two toolchains, crash isolation, hiring) outweighs the marginal performance gain. If pursued, the right shape is a separate process at the v3 storage layer, not embedded.
- **Borrowed-bytes-from-Pebble path.** Requires the VM to live next to storage (distributed execution engine = v3). Not viable in v1 because the SQL pod fetches over RPC; in v2's "there-and-back" model the operator DAG runs on the storage side, where bytes are local — this *is* the borrow path, just framed differently than the original "VM next to storage" version.
- **Per-thread CPU affinity (without architectural restructuring).** Empirically refuted by the affinity experiment. See `project_cpu_affinity_negative_result.md`.
- **Loading native code (Rust dylibs, etc.) in storage.** TiKV's Coprocessor v2 framework was a four-year proof that this doesn't work operationally even when one company controls both ends. Bytecode interpretation is a different threat model and is fine; loading native code with full process privileges is what's deferred indefinitely.
- **Typed protobuf operator DAG as v2 wire format (TiKV's choice).** Considered and rejected for our specific situation. TiKV picked DAG because TiDB had no bytecode VM to reuse; CRDB's v1 will produce wire-format-shaped bytecode that's reused for v2. One representation through the stack is worth more than DAG's marginal implementation simplicity.

---

## Appendix: Pinned-in-Memory Findings

These are facts from prior conversations / experiments that should remain authoritative reference points for future decisions:

- **2026-04-27**: CPU pinning hurts CRDB point-select latency. Eliminated migrations entirely (cpu-migrations: thousands/sec → 0/sec) but doubled p99 at conc=64. Architecture-level decoupling of goroutines from kernel threads means pinning has nothing to extract. Don't propose affinity as a standalone optimization.
- **2026-04-27**: Bazel's processwrapper-sandbox restricts test-binary affinity to a single core via sched_setaffinity at exec. The `tags = ["cpu:N"]` mechanism affects scheduling concurrency, not affinity; it doesn't fix this. The Go-side workaround (read /sys/devices/system/cpu/online instead of inherited mask) does.
- **2026-04-26**: The fast-path PR's TEST_CPU=24 setup is critical for getting accurate measurements; `./dev bench` defaults to GOMAXPROCS=1 via `-test.cpu`.
- **2026-04-30**: TiKV's coprocessor wire format is typed protobuf operator DAG, not bytecode (~700 monomorphized scalar function IDs). Their attempt at flexible code execution (Coprocessor v2 dylibs) was abandoned. *That said*, "TiKV chose DAG" is not a sufficient reason for CRDB to choose DAG — TiKV came from a Volcano-no-VM starting point, while CRDB's v1 will produce bytecode that can be reused as v2's wire format. The v2 design uses bytecode (one compiler, two interpreters); the lessons we lift from TiKV are scope (single-Range pipelines), packaging (separate executor library), and constructive sandboxing (forward-only flow + opcode allowlist + resource limits, equivalent to DAG's structural safety).
- **2026-04-30**: TiDB has no bytecode VM; their SQL execution wins came from vectorized 1024-row chunks (~5.4× expression eval, ~2× TPC-H, ~50% TiDB CPU savings on Chunk RPC). Same model as CockroachDB's colexec. If our v1 win is "more vectorization coverage," that's a much smaller project than building a VM.
