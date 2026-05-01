---
author: assistant (Claude)
date: 2026-04-30
---

# MonetDB/X100 — research notes

Third entry in the prior-art survey. MonetDB/X100 is the seminal vectorized
execution paper (Boncz, Zukowski, Nes, CIDR 2005); its design language
underpins every modern analytical engine and is the direct ancestor of
CRDB's own `colexec`. Worth reading carefully because the question
"extend `colexec` further vs build a new VM" is essentially a question
about how far to push the X100 model.

Primary source: ["MonetDB/X100: Hyper-Pipelining Query
Execution"](https://www.cidrdb.org/cidr2005/papers/P19.pdf), CIDR 2005.
Secondary: ["MonetDB/X100 — A DBMS in the CPU Cache"](https://ir.cwi.nl/pub/11098/11098B.pdf)
(Zukowski/Boncz/Nes/Héman, IEEE Data Eng. Bull. 2005), the companion paper
with more numbers; and Zukowski's PhD thesis Chapter 4
([pure.uva.nl/.../68048_08.pdf](https://pure.uva.nl/ws/files/4321268/68048_08.pdf)).
The paper PDFs in WebFetch don't decode cleanly (FlateDecode binary), so
this note triangulates from secondary summaries plus the published figures
where they're directly quoted; quoted phrases are marked.

## 1. Background

MonetDB before X100 was a research column-store from CWI built around
the **MIL** language and **BAT** (Binary Association Table)
abstraction. Each operator consumed and produced full columns: every
intermediate result was fully materialized in memory. This was great
for per-operator CPU efficiency (tight loops, no per-tuple dispatch),
but the intermediate state of a query could exceed RAM and the
materialization traffic became the bottleneck on TPC-H scales beyond
the memory hierarchy.

X100 was the retrofit. CIDR §1: row-at-a-time Volcano has
interpretation overhead; column-at-a-time MIL has materialization
overhead; vectorized in-cache execution is the middle ground.

Industry inheritance, briefly:

- **DuckDB** — embedded analytical DB from CWI (Mühleisen, Raasveldt);
  closest to the original spirit, default vector size 2048.
- **Velox** — Meta's C++ execution-engine library; production-grade
  vectorization with extensive encoding-aware optimizations.
- **ClickHouse** — column-store OLAP engine; calls its vectors "Blocks";
  converged on the model from a different lineage.
- **Photon** — Databricks' vectorized C++ engine; replaced their
  Spark-on-JVM execution path.
- **TiDB chunked execution** — `Chunk` retrofitted onto an existing
  Volcano operator tree.
- **CockroachDB `colexec`** — `coldata.Batch` is the vector;
  type-specialized kernels are generated at build time via
  `pkg/sql/colexec/execgen`.

## 2. Core thesis

Central argument: tuple-at-a-time Volcano spends most of its CPU time
on *interpretation overhead*, not computation. The companion Data Eng.
Bull. paper measures it concretely on TPC-H Q1: MySQL spends ~10% of
execution time on actual computation, the rest on hash-table management,
field navigation, and tuple copying; IPC ~0.7–0.8. X100 on the same
query: IPC over 2.0 and ~5× wall-clock improvement. The often-cited
point comparison: a primitive multiplication takes **2 cycles per tuple
in X100 vs 49 in MySQL** (Zukowski et al., "A DBMS in the CPU Cache").

Why vectorization helps (paper §3, §4):

- Each per-vector primitive is a tight `for (i=0; i<n; i++)` loop over
  contiguous arrays of one primitive type. The compiler can apply
  loop-pipelining, auto-vectorize to SIMD, and (with `restrict`
  pointers) prove non-aliasing.
- Per-tuple branches inside an operator become per-vector branches at
  the loop boundary, restoring branch-predictor effectiveness.
- Function-pointer dispatch happens *per vector*, not per tuple,
  amortizing across thousands of values.
- The instruction working set of one primitive fits in L1-I; the data
  vector fits in L1-D or L2.

The paper identifies regimes where vectorization *doesn't* pay,
indirectly via the vector-size sweep (Figure 3): vector size 1 recovers
Volcano's poor performance; full-column vectors recover MIL's
memory-bandwidth wall. The plateau between ~100 and ~10K tuples per
vector is where intermediates fit in L1/L2 and dispatch is amortized —
performance varies <2× across that range. The paper is TPC-H-focused
and doesn't directly quantify the OLTP-shaped failure mode; §5 below
addresses that gap.

## 3. Specific design choices

**Vector size.** The paper picks ~1024 tuples as a working default and
justifies it via the L1/L2-fit argument. The TPC-H Q1 sweep (Figure 3)
shows performance collapses on either side: at vector size 1 the engine
performs like MySQL; at 4M (whole-column) cache misses dominate.
Optimum is broad — anywhere intermediates fit in L1 and the loop body
stays in L1-I. The exact number is a tunable, not a constant of
nature. CRDB's `pkg/col/coldata/batch.go` captures this explicitly:
`DefaultColdataBatchSize = 1024` with a comment crediting the X100
paper and noting `tpchvec/bench` later confirmed 1024 (1280 was
marginally better).

**Per-vector primitives.** Each operator's per-type kernel is a separate,
type-specialized function — one for each `(operator, type)` combination,
not a polymorphic dispatch. The paper describes generating these via
**macro expansion**: a primitive like `addition` is a C macro
parameterized over its operand types, expanded into a closed set of
type-specific functions at build time. The combinatorial explosion is
real and accepted as a cost of doing business; it's what lets the inner
loop be a flat arithmetic kernel with no virtual calls. CRDB's
`execgen` is the same pattern with a Go template engine instead of cpp.

**Selection vectors.** Filter and predicate results are not expressed by
physically removing rows from the vector; they're expressed as a
**selection vector** — a small `int` (or bitmap, depending on system)
list of indices into the original vector identifying surviving rows.
Two important consequences: (1) downstream primitives don't need to
re-allocate or re-pack; they can take an optional selection-vector
parameter and skip the masked-out positions, or operate on the full
vector and let the next selection step compose. (2) The vector
allocation is fixed-size and reusable across the pipeline — no
variable-length resizing per batch.

**Pipelined execution.** Vectors flow between operators in a Volcano-shaped
control flow (`next()` returns a vector instead of a tuple), but there is
*no* full materialization between steps — vectors are sized to fit in
cache and consumed in place by the next operator. The paper calls this
"vectorized in-cache pipelining" and explicitly contrasts it with
MonetDB/MIL's column-at-a-time materialization. The buffer manager
(**ColumnBM**) supplies cache-resident column chunks to the bottom of the
pipeline; intermediate vectors live in pre-allocated scratch buffers
attached to each operator.

**Expression evaluation.** Expression trees are decomposed into a sequence
of primitive calls, each writing its output into an intermediate vector
that becomes input to the next. So `a * b + c` becomes
`mul_vec(tmp1, a, b); add_vec(out, tmp1, c)`. The intermediate `tmp1` is a
pre-allocated scratch buffer owned by the expression evaluator, sized to
the vector size — no per-evaluation allocation. Type promotion and
cross-type combinations are handled by emitting cast primitives in the
expression decomposition pass.

## 4. What derived systems took

**DuckDB** — closest descendant. Default `STANDARD_VECTOR_SIZE = 2048`
(power-of-two sweet spot, larger than X100's 1024 because L1/L2 have
grown). Type-specialized kernels via C++ templates. Selection vectors
first-class; dictionary vectors are a selection-vector pointing into a
distinct-values vector. First author Hannes Mühleisen came from CWI.
([execution-format docs](https://duckdb.org/docs/stable/internals/vector))

**Velox** — Meta's industrial generalization. Kept the vectorized
substrate but added *encoding-aware* execution: vectors carry their
physical encoding (flat, dictionary, constant, sequence/RLE,
frame-of-reference), and operators **peel** common dictionary wrappings
off their inputs to evaluate expressions on the smaller distinct-values
set. Adds **lazy vectors** (materialize on first read), **adaptive
conjunct reordering** (reorder AND/OR conjuncts by observed
short-circuit rate), and SIMD-driven filter evaluation that processes
"roughly an integer hit per CPU clock using AVX2." Selection is a
`SelectivityVector` bitmap, threaded through the entire expression
evaluator. ([VLDB 2022](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf),
[expression-eval docs](https://facebookincubator.github.io/velox/develop/expression-evaluation.html))

**ClickHouse** — independent lineage that converged on the model. The
unit is a `Block` (`IColumn` + `IDataType` + name). Functions are
*only ever* invoked on Blocks — single-row evaluation is explicitly
forbidden. Storage granules (~8192 rows) are themselves tuned to the
vectorized engine. Idiosyncrasy: ClickHouse ships **multiple compiled
kernel variants** per primitive (scalar, AVX2, AVX-512) and selects via
`cpuid` at runtime — more aggressive than X100's "let the compiler
vectorize" strategy. ([architecture
overview](https://clickhouse.com/docs/development/architecture))

**TiDB chunked execution** — TiDB's `Chunk` is the X100 vector,
retrofitted onto an existing row-at-a-time Volcano operator tree.
Per the workspace's `tidb_tikv_research.md`, this retrofit yielded
~5.4× expression-evaluation speedup, ~2× TPC-H wall-clock improvement,
and ~50% TiDB CPU savings on the Chunk RPC path. Notably, **TiDB
acknowledges vectorization doesn't help OLTP point-select shapes** and
keeps row-mode codepaths for them.

**CockroachDB `colexec`** — `coldata.Batch` is the vector; default size
1024 (with the comment in `pkg/col/coldata/batch.go` literally citing
the X100 paper). Type specialization is `pkg/sql/colexec/execgen` —
templated Go that monomorphizes operators per concrete column type at
build time, the same idea as X100 macros. Selection vectors are
present (`Batch.Selection()`) and used through the operator tree.
The colexec/rowexec split exists because, like TiDB, the OLTP
single-row case isn't competitive in the vectorized engine and routes
through the row engine instead.

## 5. The OLTP-vs-vectorized tension

> *Analysis — assistant's reading, not from the paper itself.*

The X100 model is optimized for *batch* workloads where per-vector
overhead amortizes over thousands of tuples. For OLTP point-select
shapes (1-row results from a primary key lookup), it has structural
overheads that don't go away regardless of kernel speed:

- **Vector header overhead is per-batch.** The batch object, selection
  vector, per-column null bitmap, and per-column scratch buffer are all
  paid in full for a 1-row batch.
- **Per-batch dispatch doesn't amortize over 1 row.** Moving dispatch
  from per-tuple to per-vector only helps if the vector is full.
- **Pipeline setup is per-query.** DuckDB's framing (from their VSS
  extension blog): "we generally favor spending a lot of time up front
  to optimize the query plan and pre-allocate large buffers and caches
  so that everything is as efficient as possible once we start
  executing. A lot of this work becomes unnecessary when the actual
  working set is so small."
- **Memory-traffic asymmetry.** A row engine touches one row's bytes;
  a column engine touches one column-vector per active column.

The CIDR paper does not measure or discuss this regime — its benchmark
is TPC-H. The Figure 3 vector-size sweep treats vector size 1 as "what
Volcano does," with the implicit framing that no one would actually
operate there. Derived systems have responded in three ways:

1. **Two engines** — row-mode path for OLTP, vectorized for OLAP. CRDB
   (rowexec/colexec), TiDB (legacy row + Chunk), SQL Server (row +
   batch-mode columnstore). Cost: maintaining and routing between two
   stacks.
2. **Single vectorized engine, accept the OLTP overhead** — DuckDB,
   ClickHouse, both honest about being unsuitable for true OLTP.
   ClickHouse's "row mode" for point queries isn't really a row mode;
   the sparse-index lookup still reads a whole 8192-row granule.
3. **Special-case fast paths inside the vectorized engine** — DuckDB's
   `HNSW_INDEX_JOIN` and similar operators bypass the standard pipeline
   for known-small-result shapes; structurally the same pattern as
   CRDB's `placeholder_fast_path`.

The structural observation: X100 is not a universal model. It is right
for query shapes where per-batch overhead amortizes across batch work.
Below some break-even point in result cardinality the overhead
dominates. That point is qualitatively "tens of rows" — well above
OLTP point-selects, well below analytical scans, and exactly in the
gap where mixed-workload applications live.

## 6. Open questions / followups

- The exact per-batch fixed cost for `colexec` in CRDB is the
  load-bearing number for the OLTP-vs-vectorized question and isn't
  measured here.
- Velox's encoding-aware peeling and adaptive conjunct reordering are
  the most interesting departures from pure X100 and deserve their own
  notes.
- Photon (Databricks) was mentioned but not investigated; rebuilding
  vectorized execution from JVM Spark likely produced lessons relevant
  to a colexec-vs-new-VM evaluation.
- A short side-note on the original MIL/BAT model would round out the
  picture — X100 is positioned explicitly against that extreme.
- The original 1024 default is Pentium 4-era. DuckDB's 2048 and the
  colexec-bench finding favoring 1280 both suggest the optimum drifts
  up slowly with L1 size; worth a measurement on current hardware.
