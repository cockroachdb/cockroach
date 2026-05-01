---
firsthand: Matt White (intimately familiar with the system)
analysis: assistant (Claude)
date: 2026-04-24
---

# Clustrix / MariaDB Xpand execution model — research notes

Clustrix (later MariaDB Xpand) is a distributed SQL database written in
C with a Cascades-style optimizer. Particularly interesting for our
purposes because (a) it had a real query-compilation pipeline that
ended in optional native code, and (b) it autoparameterized every
incoming query rather than only caching at PREPARE time. The
firsthand description below is from someone who worked on the system;
the analysis section that follows is mine.

## 1. System shape

- Distributed execution: query fragments shipped to nodes that hosted
  the relevant row data. **Explicitly out of scope for the initial
  CRDB engine work** — we're not proposing distributed execution as
  v1 — but worth keeping the design hospitable to it. The firsthand
  comparison: Clustrix was "crazy fast" relative to alternatives,
  and the fragment-shipping model was a load-bearing reason.
- Optimizer: Cascades-family, structurally analogous to CRDB's
  xform/memo.
- Threading: programs ran on **small-stack fibers**, not OS threads.
  This is important context for the register/allocation model below
  — fiber-local state is much cheaper than goroutine-stack-bound
  state and the system could afford a register-file-on-a-page
  approach that wouldn't trivially translate to Go's growable stacks.

## 2. Cache + dispatch interface

Every incoming query went through this interface, regardless of
whether the application explicitly prepared it:

1. **Parse + analyze + autoparameterize.** Literals embedded in the
   query string were lifted out as parameters automatically, producing
   a parameterized "prepared query tree." This tree was the cache key.
2. **Cache lookup.** On hit, the cache returned a *program number*.
3. **Execute.** The executor retrieved the cached program by number
   and invoked it with a *parameter vector* containing the relevant
   session variables and the user-supplied parameters and constants
   (the values lifted out at autoparameterize time).
4. **Cache miss.** The query was planned by the Cascades optimizer
   and the resulting plan compiled (see §3); the program was inserted
   into the cache and execution proceeded.

The internal mechanics of the cache (eviction, plan invalidation,
parameter-skew handling, etc.) are out of scope for now — we may come
back to them when the "what does CRDB cache, and how" question becomes
load-bearing.

The external interface is what matters most: **every query becomes a
`(program, parameter_vector)` pair before execution**, with a uniform
calling convention. The cache contract and the executor contract are
disjoint and can evolve independently.

The parameter vector itself was **encoded as a tuple using the same
on-disk encoding format**. Driven by the distributed nature of the
system (parameter vectors travel between nodes, see §4 below), but
useful as a single-encoding-for-everything design property in its
own right: there is no separate "parameter packing" format, no
per-call serialization choice, no caller-side encoding decisions.

There are actually **two caches**, kept separate:

- **Plan cache.** The optimizer's output. Lives only on the
  originating node (the node that received the query from the
  client). Heavyweight to populate (full Cascades planning).
- **Program cache.** The compiled artifact (bytecode, optionally
  with `VM_NATIVE` blobs). Lives on every node that may execute
  any fragment of any program. Distributable: a node receiving an
  execute request with an unknown `program_id` can request a copy
  from the originating node and cache it locally.

The split keeps optimization centralized while making execution
artifacts cheaply replicable. Caching policy was **fixed-size LRU,
plans not currently executing eligible for eviction, with
heuristics for re-planning entries**. Details deliberately tabled
at this phase of the discussion.

## 3. Compilation pipeline

On cache miss, the optimizer's output was fed through a multi-stage
compilation pipeline:

```
Cascades plan
   │
   ▼
[Query compiler] ──> graph IR (SQL-machinery nodes,
   │                            ordering edges,
   │                            column-value edges, ...)
   ▼
[Bytecode emitter] ──> bytecode program
   │
   │ (optionally)
   ▼
[Assembler] ──> x86 machine code (serial calls to VM operator
   │            implementations)
   ▼
VM_NATIVE bytecode instruction (carries the machine code blob)
   │
   ▼
[Bytecode interpreter executes the program]
```

Notes from the firsthand description:

- The graph IR carried SQL-machinery nodes connected by typed edges
  (ordering, column-value, etc.). This is more elaborated than a
  simple operator tree — closer to a dataflow graph.
- The assembler stage was deliberately simple: the emitted x86 was
  "little more than serially calling the VM operator implementations."
  The win over pure bytecode interpretation was dispatch elimination
  (no per-opcode interpreter dispatch loop), not anything sophisticated
  like register allocation or instruction scheduling. This is
  consistent with the ~15% bytecode-vs-native delta cited in
  conversation context.
- Native code was wrapped in a single bytecode instruction
  (`VM_NATIVE`) that the interpreter executed. **The interpreter was
  the entry point in both cases** — so adding native compilation
  did not require a separate execution path. This is an important
  architectural property: the bytecode VM is the substrate; native
  is an optimization layered into it.
- In theory a single program could mix bytecode and native (some
  pipelines compiled, some interpreted). In practice this never
  happened — programs were either all-bytecode or had a `VM_NATIVE`
  wrapping the whole thing. The hybrid flexibility was architectural
  capability, not a useful feature.

### VM design: register-based, compile-time-allocated

The VM was **register-based, not stack-based**. Registers were
allocated at compile time as part of program emission — the program
declares its register file shape, and the executor sets aside a page
of RAM at invocation time to back it. There was no per-execute
register allocation, no spilling, no per-instruction stack
manipulation. Some registers were typed to hold tuples directly
(useful given the tuple-encoded parameter vector and the on-disk
tuple format).

This is the cleanest possible expression of the "no runtime
allocations" goal: **all per-execute state shape is fixed at
compile time; the executor just wires a page to it**. Combined with
small-stack fibers, the entire query-context allocation problem
collapses to "set aside one page of memory per concurrent execution."

Translating to Go is non-trivial — Go's growable stacks, escape
analysis, and lack of fiber primitives all make the literal mechanism
hard — but the *design property* (compile-time register allocation,
fixed-shape per-execute scratch) is what matters and is achievable
in some form.

### Opcode granularity: deliberately mixed

The instruction set spans a wide range of granularities:

- **Fine-grained**: arithmetic (add, subtract), value comparisons,
  individual cursor operations.
- **Coarse-grained**: "get me a cursor from this LSM starting/ending
  with these keys" is a single opcode. "Iterate this cursor" is
  another.

The compiler emits at whichever level fits the operation; there is
no purity constraint that all opcodes be of similar weight. This
matters because it lets the bytecode carry both the
expression-evaluation work (where fine-grained dispatch wins from
inlining) and the storage-interaction work (where you don't want
to bytecode every byte of a KV scan, you want one opcode that
hands control to a pre-existing C scanner).

## 4. Distributed execution: continuation-passing

Out of scope for our v1 but worth recording for future reference,
because it affects what an architecturally-hospitable v1 should
look like.

Distributed execution was **continuation-passing-style**, not
DistSQL-style:

1. The executor starts the query program on the local
   (originating) node.
2. The program decides which other nodes it needs data from —
   based on which keys it is reading and where those keys live.
3. The originating program forwards a message to each chosen node
   containing `(program_id, fragment_number, parameter_vector)`.
4. The receiver looks up `program_id` in its local program cache;
   on miss, requests a copy from the originating node and caches
   it. Then it executes the named fragment with the supplied
   parameter vector.
5. As that fragment reads rows from local LSMs, it can in turn
   forward parameter vectors to further nodes (e.g. for the next
   join phase, or for an aggregation site). The chain continues
   until a final node sends results back to the originator.

The architectural property worth highlighting: **node identity is
not baked in at compile time**. The same compiled program runs on
every node; *which* node any particular fragment runs on is decided
at execution time based on the parameter vector and the data
locations. CRDB's DistSQL today bakes node IDs into the physical
plan during planning (which is one of the reasons replanning per
execute is necessary — the data layout might have changed).

Compare:

| | CRDB DistSQL | Clustrix CPS |
|---|---|---|
| Node identity in plan | Baked at planning time | Resolved at execution time |
| Replan on lease move | Required | Not required |
| Cache portability | Tied to specific layout | Program is location-agnostic |

This is forward-looking only. The point isn't to redesign DistSQL
in v1; it's to note that the engine substrate we pick should not
preclude this model later.

## 5. Comparison to Postgres + CRDB

| Property | Postgres | CRDB today | Clustrix/Xpand |
|---|---|---|---|
| Cache scope | Explicit PREPARE only | Explicit PREPARE only | **Every query (autoparameterized)** |
| Cache key | Parsed/analyzed query | Parsed/analyzed query | Parameterized query tree (post-autoparameterize) |
| Cached artifact | `Plan` tree (immutable, refcounted) | Memo / `IdealGenericPlan` | **Compiled program (bytecode, optionally native)** |
| Per-execute work past cache hit | One `ExecInitNode` pass + `PlanState` build | Exec-builder + DistSQL planning + `row.Fetcher` init + receiver + flow goroutines | **Lookup by program number; invoke with parameter vector** |
| Execution substrate | Volcano, function-pointer dispatch via `ExecProcNodeFirst` | colexec (vectorized) or rowexec (Volcano), interface dispatch | **Single VM (bytecode interpreter); native is layered into the same VM via one opcode** |
| Codegen | LLVM JIT for expression eval + tuple deforming, gated on cost | None | Bytecode → optional x86 (whole-program), no LLVM, no cost gating |
| Distribution | None | DistSQL (per-execute physical planning) | Fragment-shipping to data nodes |

The headline contrast vs CRDB is that the **per-execute work past
the cache hit collapses to "look up program; invoke with params."**
The expensive things CRDB does per execute (exec-build, DistSQL
physical planning, fetcher init) all happened *during compilation*
in Clustrix and were embedded in the cached program. Postgres
collapses some of this but not all — `ExecInitNode` still runs per
execute. Clustrix moves the line further.

## 6. What's most interesting for our work

Five things from this that change my mental model:

1. **Autoparameterization extends caching from a feature to an
   architecture.** CRDB's plan cache today only helps applications
   that use the extended pgwire prepared protocol. Most ORM-generated
   SQL doesn't (or interleaves prepares with literals). If we're
   building per-execute caching infrastructure as part of the engine
   rewrite, autoparameterization is the multiplier that turns it from
   "helps the prepared-statement path" into "helps everything."
   Worth flagging as a separate-but-coupled workstream.

2. **The "program + parameter vector" execution contract is exactly
   the thin interface a bytecode VM wants.** The cache is one side, the
   VM is the other, and they're decoupled by the program-number
   handle. Lets us evolve the bytecode (or add native, or replace
   the VM entirely) without touching the cache or the caller. This
   is architecture I'd want regardless of whether we go bytecode or
   native.

3. **Native code as one opcode inside the bytecode VM is a clean
   layering.** The VM is the substrate; native is a per-program
   optimization wrapped in a single `VM_NATIVE` instruction. Means
   you can ship the bytecode VM first, add native later, and the
   execution-engine-vs-everything-else interface never changes.
   Compare to a "we have an interpreter and we have a JIT and they're
   separate paths" world, which is more code and more bugs.

4. **Compile-time register allocation is a concrete answer to
   "no runtime allocations."** All per-execute scratch state has its
   shape fixed at compile time; the executor binds a single page of
   memory to that shape at invocation. There is no per-execute
   `make([]X, ...)`, no per-execute slice growth, no per-execute
   pool fetch — the program *declares* what it needs, and the
   executor *provides* it. Translating the literal mechanism to Go
   is hard (Go has no first-class equivalent of "set aside a page
   and treat it as a typed register file"), but the design
   *property* (compile-time-fixed scratch shape) is achievable with
   an arena-style per-program context struct. This is the most
   directly relevant insight for the Allocation Discipline section
   of DESIGN.md.

5. **Mixed-granularity opcodes are not a sign of design impurity.**
   The instruction set spans add/subtract through "open this LSM
   cursor with these bounds" through "iterate this cursor." The
   bytecode is not a uniform-cost abstraction; it is the right
   level of abstraction for whatever the operation is. CRDB will
   want this — fine-grained opcodes for expression eval, coarse
   opcodes that hand off to existing optimized scan/fetch code.
   Avoid the trap of "one opcode size for all operations."

The "hybrid programs in theory but not in practice" honesty is
worth carrying forward too: don't pay the architectural complexity
cost of mixing modes if you're not going to use it.

## 7. What we should NOT borrow as-is

- **The compilation pipeline output (whole-program native).** Clustrix
  emitted x86 directly; that's hard in Go because of the runtime
  constraints we already catalogued (GC stack maps, safepoints, no
  stable JIT escape hatch). Section 4 of DESIGN.md will work out
  what the Go-shaped equivalent is.
- **Distributed execution as a v1 commitment.** Per scope. Keep it
  on the architecture's option list, not its critical path.
- **Small-stack-fiber threading model.** Go gives us goroutines with
  growable stacks instead. The fiber model is what made Clustrix's
  register-on-a-page work *literally*; we need an equivalent design
  property without trying to recreate the fiber substrate. (Stack
  allocation via escape analysis, sync.Pool-backed contexts, or an
  explicit per-execute arena struct are all candidate Go-shaped
  paths; this is for §4/§5 of DESIGN.md.)

## 8. Open questions

Most of the original open questions were answered in the second
firsthand round (struck through below). Remaining open:

- ~~Bytecode design specifics: stack-based vs register-based? How
  large was the opcode set? Was each VM operator a single opcode
  or a sequence?~~ **Register-based with compile-time allocation;
  some registers typed for tuples; opcodes deliberately
  mixed-granularity (arithmetic up through "get LSM cursor").**
- ~~How was the parameter vector laid out? Fixed-shape struct per
  program, or a tagged-union array?~~ **Encoded as a tuple in the
  same on-disk format, driven by inter-node messaging needs.**
- How did the executor handle SQL-level concurrency (cursors,
  cross-statement state, aborts)? Was the program reentrant or
  single-shot? **Still open.**
- ~~Cache eviction and invalidation policy.~~ **Fixed-size LRU,
  plans not currently executing eligible for eviction, with
  heuristics for re-planning. Tabled at this phase.**
- ~~Did fragment-shipping produce one cached program per node or
  one per query?~~ **One program per query, replicated to nodes
  on demand via miss → request from originator. Plan cache and
  program cache are separate.**

New open questions surfaced this round:

- The compile-time register file: how was the page sized? Single
  size for all programs, or per-program? Did programs ever exceed
  a page and need spilling, or was that a hard error at compile
  time?
- How were tuple-typed registers laid out internally — column
  vector, row format, or something else? Affects how directly the
  pattern translates to a CRDB-shaped equivalent.
