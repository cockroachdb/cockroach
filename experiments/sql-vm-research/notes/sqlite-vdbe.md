---
author: assistant (Claude)
date: 2026-05-11
---

# SQLite VDBE — research notes

Fourth and final entry in the prior-art survey. SQLite is the most
direct living example of the design under investigation: a small
bytecode VM for SQL where PREPARE compiles to a program and EXECUTE
runs it. It is the existence proof that the pattern works at
production scale — the most-deployed DB engine on Earth, stable for
two decades, with the VDBE design roughly frozen since the
register-based rewrite in the SQLite 3.x line. Postgres (operator
tree, Volcano), MonetDB/X100 (vectorized, Volcano), and Clustrix
(bytecode plus optional native, fragment shipped) bracket the
surrounding design space; VDBE is the cleanest
small-bytecode-VM-for-SQL reference point on that map.

Sections 1–5 are source-derived and cited. Section 6 is analysis.

## 1. Background and history

SQLite is a single-file, in-process, embedded SQL database written
in C, public domain, with no client/server boundary.

The choice of C is deliberate. The "Why is SQLite written in C?"
page ([whyc.html](https://sqlite.org/whyc.html)) gives four
reasons: **performance** ("no other language claims to be faster
than C for general-purpose programming"); **compatibility** (nearly
every runtime can call C, so SQLite can be embedded from Java,
Swift, Python, Go, etc.); **low dependencies** (only `memcpy`,
`strcmp`, `strlen`, etc.); and **stability** (C is "old and
boring"). Java/C++ are rejected for ABI reasons; Rust/Go because a
rewrite "would probably introduce far more bugs than would be
fixed" and because safe languages complicate graceful OOM handling.
Rust is named as a possible future but not now.

**VDBE history.** The original VDBE (through SQLite 2.8.0) was
**stack-based**: three operands per opcode, most instructions
pushed/popped on an execution stack
([vdbe.html](https://sqlite.org/vdbe.html), preserved as
documentation of the 2.x machine). The switch to a **register-based**
VM shipped in the SQLite 3.x line in early 2008 (not 2004, as one
occasionally sees claimed); the 3.x machine has different opcodes
and grew to **five operands** (P1..P5).

The mailing-list thread ["Is there a design doc for the VM
re-write?"](https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg54910.html)
preserves the rationale. D. Richard Hipp's stated motivation was
that **code generation is much easier against a register machine**:
SQL operators map nearly one-to-one to parameterised opcodes whose
register operands are stable across executions of the same prepared
statement. A stack VM forces the code generator to track virtual
stack depth and emit push/pop bookkeeping; with registers, it picks
a slot at PREPARE time and the executor never thinks about shape.
Secondary: stack manipulation is a recurring source of bugs the
register model doesn't have. One consequence of having no stack:
the VM has no place to store subroutine return addresses, so VDBE
subroutines stash them in registers and are **not reentrant**.

**Stability.** The register-based VDBE has been the SQLite engine
for ~18 years and has been incrementally extended (more opcodes,
new value types, JSON support) but not fundamentally rewritten. For
comparison, Postgres has rewritten its executor at least twice in
the same period (most recently the JIT/expression-eval rework
around PG11), and MySQL has had two major executor overhauls. The
VDBE's longevity is itself a data point.

## 2. VDBE structure and opcode set

**Opcode count.** As of the current
[opcode.html](https://sqlite.org/opcode.html), there are **192
opcodes**. They are not uniform RISC-style micros; they span a wide
range of granularities.

Rough functional grouping (mine, derived from
[opcode.html](https://sqlite.org/opcode.html)):

- **Cursor lifecycle:** `OpenRead`, `OpenWrite`, `OpenEphemeral`,
  `OpenAutoindex`, `Close`.
- **Cursor positioning / iteration:** `Rewind`, `Last`, `Next`,
  `Prev`, `SeekGE`, `SeekLT`, `SeekRowid`, `IdxGT`, `IdxLT`,
  `IdxInsert`, `IdxDelete`.
- **Row data access:** `Column`, `RowData`, `Rowid`, `Insert`,
  `Delete`.
- **Arithmetic / logic / bits:** `Add`, `Subtract`, `Multiply`,
  `Divide`, `Remainder`, `And`, `Or`, `BitAnd`, `BitOr`.
- **Comparisons / branching:** `Eq`, `Ne`, `Lt`/`Le`/`Gt`/`Ge`,
  `If`, `IfNot`, `IsNull`, `Goto`, `Jump`, `Halt`.
- **Functions and aggregates:** `Function`, `PureFunc`, `AggStep`,
  `AggFinal`.
- **Transactions / savepoints / schema:** `Transaction`,
  `Savepoint`, `Checkpoint`, `CreateBtree`, `DropTable`,
  `DropIndex`, `Clear`, `Destroy`.
- **Program scaffolding:** `Init` (mandatory first instruction),
  `Halt` (closes cursors and exits), `ResultRow` (yields a row).

**Granularity.** Concrete examples:

- `OpenRead P1 P2 P3` opens a read-only B-tree cursor on the table
  whose root page is in register P3, into cursor slot P1. A single
  opcode that runs the whole cursor-open path through the pager
  and B-tree layers — far from a "micro."
- `Column P1 P2 P3` extracts the P2-th column from the record at
  cursor P1 into register P3 — parses the SQLite record format,
  handles type affinities, etc. Again: one opcode, a lot of C
  behind it.
- `Next P1 P2` advances cursor P1 and jumps to address P2 if there
  is a next row. The hot inner loop of every scan is `Next` plus
  `Column` plus filter ops.
- `ResultRow P1 P2` exposes registers P1..P1+P2-1 as a row and
  causes `sqlite3_step` to return `SQLITE_ROW`. How the VM yields.
- `Goto P2` is the unconditional branch you'd expect; `Init` is
  always at instruction 0.

So the opcode set is **deliberately mixed-granularity**: `Goto` is
a PC mutation, but `Column` is hundreds of lines of C. SQLite makes
no attempt to keep opcodes uniform in cost.

**Register file.** Per
[opcode.html](https://sqlite.org/opcode.html), registers are a flat
numbered array, **sized at PREPARE time**, **untyped at the schema
level** but **dynamically tagged** at runtime: a register can hold
NULL, a 64-bit signed integer, an IEEE double, a variable-length
string, a BLOB, or be in an "Undefined" state distinct from NULL.
Numbering starts at 0; contents clear on reset/finalize. This
manifest typing is the sharpest contrast with strictly-typed
engines (see §6).

**Instruction layout.** From
[`src/vdbe.h`](https://raw.githubusercontent.com/sqlite/sqlite/master/src/vdbe.h):

```c
struct VdbeOp {
  u8 opcode;          /* What operation to perform */
  signed char p4type; /* One of the P4_xxx constants for p4 */
  u16 p5;             /* Fifth parameter is an unsigned 16-bit int */
  int p1;             /* First operand */
  int p2;             /* Second parameter (often jump destination) */
  int p3;             /* The third parameter */
  union p4union { ... } p4;
};
```

P1/P2/P3 are 32-bit signed ints (often register numbers or branch
targets), P5 is a 16-bit flags word, P4 is a tagged union (string,
collation, function pointer, key info, etc.). Each op is ~24–32
bytes — not a tight bytecode buffer; the program lives in malloc'd
memory.

**Dispatch loop.** From
[`src/vdbe.c`](https://raw.githubusercontent.com/sqlite/sqlite/master/src/vdbe.c)
the inner loop is essentially:

```c
for(pOp=&aOp[p->pc]; 1; pOp++){
  switch( pOp->opcode ){
    case OP_Goto: { ... }
    case OP_Add:  { ... }
    /* ... 192 cases ... */
  }
}
```

A giant `for(;;) switch` with explicit `goto`s out of cases for the
hot exits (`jump_to_p2`, `jump_to_p2_and_check_for_interrupt`,
`no_mem`, `abort_due_to_error`). The file carries an explicit
comment that the style is intentional: "This code uses unstructured
'goto' statements and does not look clean. But that is not due to
sloppy coding habits. The code is written this way for performance,
to avoid having to run the interrupt and progress checks on every
opcode." Compilers turn the dense switch into a jump table; the
file is on the order of 10k lines.

## 3. PREPARE → EXECUTE wiring

The lifecycle ([c3ref/stmt.html](https://sqlite.org/c3ref/stmt.html),
[c3ref/prepare.html](https://sqlite.org/c3ref/prepare.html)):

1. **`sqlite3_prepare_v2`** — parser builds an AST, planner builds
   a plan, code generator emits a VDBE program. The `sqlite3_stmt`
   carries the bytecode and the register-file size. Per the docs:
   "Think of each SQL statement as a separate computer program. The
   original SQL text is source code. A prepared statement object
   is the compiled object code."
2. **`sqlite3_bind_*`** — writes parameter values into reserved
   register slots. No re-compilation.
3. **`sqlite3_step`** — runs the VM until it yields a row
   (`OP_ResultRow` returns `SQLITE_ROW`), finishes (`OP_Halt`
   returns `SQLITE_DONE`), or errors. Between `SQLITE_ROW`
   returns the VM is **paused mid-program**: the PC sits at the
   instruction after `ResultRow` and the next `step` resumes from
   there. No coroutines or threads — the loop is structured to
   return cleanly and re-enter.
4. **`sqlite3_reset`** — resets PC, clears register state, keeps
   the bytecode and bindings.
5. **`sqlite3_finalize`** — frees the program and registers.

Per-prepare: parsing, planning, code generation, register
allocation, P4 constants. Per-execute: bound parameters, register
live values, PC, cursor positions, transient memory.

**Schema-change handling.** From
[c3ref/prepare.html](https://sqlite.org/c3ref/prepare.html):
`sqlite3_prepare_v2` stores a copy of the original SQL inside the
statement object; if `sqlite3_step` detects that the schema has
changed under it, the engine **automatically re-prepares from the
saved SQL** and retries — up to `SQLITE_MAX_SCHEMA_RETRY` times
before surfacing the error. The legacy `sqlite3_prepare` instead
returned `SQLITE_SCHEMA` and made re-prepare the caller's problem.
The canonical mental model is therefore: SQL text is durable,
bytecode is the cache.

## 4. How rich operations integrate

The interesting question for any modern adaptation. A "pure"
bytecode VM would have one opcode per micro-op, but SQL execution
needs to call into B-tree cursors, sorters, hash tables,
transaction managers. SQLite's answer is consistent: **make the
rich operation an opcode, and let the opcode body call into the
subsystem directly**.

- **B-tree cursors.** `OpenRead`/`OpenWrite` allocate a `BtCursor`
  slot on the VM and call into `btree.c`. `Rewind`, `Next`,
  `SeekGE`, etc. are thin trampolines. `Column` decodes the current
  record. `IdxGT` compares an unpacked key against the current
  index entry. Each is essentially "call this C function with these
  register operands."
- **Sorters and ephemeral tables.** `OpenEphemeral` opens a scratch
  B-tree (in memory, spilling if needed) that doubles as the
  sorter / hash-aggregation backing store. There is no dedicated
  `SortRows` opcode — sorting is `OpenEphemeral` + `IdxInsert` +
  scan. The "rich operation" is hidden behind the cursor
  abstraction.
- **Functions.** Built-in scalar and aggregate functions are
  invoked via `OP_Function`/`OP_PureFunc`/`OP_AggStep`/`OP_AggFinal`,
  with P4 carrying a pointer to the registered `FuncDef`. UDFs
  registered via `sqlite3_create_function` look identical at the
  VDBE level — same opcode, different function pointer in P4. This
  is why custom functions impose effectively zero VDBE overhead.

The pattern is "opcode as a typed call site": each cursor
operation, function call, and transaction primitive is a single
instruction that internally does a lot of work. The VM provides
operand decoding, register-file management, and PC/branching; the
database functionality lives in C. This is the same pattern Clustrix
used (see [clustrix-xpand.md](./clustrix-xpand.md)) and appears to
be the natural shape this kind of design takes.

## 5. Concurrency, transactions, errors

SQLite's concurrency model is single-process-friendly,
multi-process-tolerant, and very different from anything CRDB
faces. The VM-level story is what matters here.

- **Locking** ([lockingv3.html](https://sqlite.org/lockingv3.html))
  — default rollback-journal mode uses five file-level lock states
  (UNLOCKED → SHARED → RESERVED → PENDING → EXCLUSIVE). One
  writer, multiple readers; a writer eventually excludes readers.
- **WAL mode** ([wal.html](https://sqlite.org/wal.html)) — writers
  append to a WAL file rather than mutating the main DB; readers
  see a consistent snapshot. "Writers do nothing that would
  interfere with the actions of readers." Still single-writer at
  any instant.
- **Transactions wrap the VM.** `OP_Transaction` opens (or re-uses)
  a transaction on the B-tree backend. `OP_Halt` with the
  appropriate flag triggers commit or rollback. SAVEPOINT is
  explicit (`OP_Savepoint`).
- **Error handling.** When an opcode errors (e.g. constraint
  violation in `OP_Insert`), it `goto`s `abort_due_to_error` in
  the dispatch loop. That path closes cursors, rolls the statement
  (and possibly the enclosing transaction) back, and returns an
  error from `sqlite3_step`. The VM never partially commits.
- **Threading**
  ([threadsafe.html](https://sqlite.org/threadsafe.html)) — three
  modes: single-thread (no mutexes), multi-thread (a given
  connection or statement may not be touched concurrently from two
  threads), serialized (a single mutex serializes all access). A
  single `sqlite3_stmt` is **never safely concurrent**; the only
  legal model is one-statement-per-thread or pool-and-serialize.
- **Reentrance.** As noted in §1, VDBE subroutines store return
  addresses in registers and are not reentrant within a program.

## 6. Comparison to CRDB context — Analysis (assistant's reading)

This section is my reading of the implications, not source-derived.

### What translates directly

- **Bytecode-as-cached-prepared-statement.** SQL → AST → optimizer
  → bytecode in a statement object → bind → step. The decoupling
  of PREPARE from EXECUTE, and invisible re-prepare on schema
  change, map onto how CRDB's session/statement machinery already
  wants to work.
- **Register-based with compile-time allocation.** Postgres does a
  slot-based equivalent per expression context; with VDBE the
  allocation is uniform across all operators rather than per-node.
  That's the structural simplification.
- **Mixed-granularity opcodes.** Cursor/scan/aggregate primitives
  alongside arithmetic and branching should be the default, not a
  compromise. No realistic CRDB VM has 30 micros for "decode a
  row."
- **EXPLAIN that yields bytecode.** SQLite's `EXPLAIN` returns the
  program listing directly. Excellent for debugging; an
  `EXPLAIN BYTECODE` view alongside the existing plan tree.

### What does not translate

- **Single-host vs distributed.** SQLite is one process and one
  file. CRDB has DistSQL, KV pushdown, multi-node coordination. A
  SQLite-style VM would cover the per-node leaf of a distributed
  plan; the inter-node fabric is out of scope for the opcode set.
- **Manifest typing vs strict typing.** SQLite registers are tagged
  unions; type coercion happens at opcode execution. CRDB has
  strict typing all the way down and a Cascades-style optimizer
  that depends on stable types for costing. A CRDB VM would have
  **typed register pools** (or typed slots with no per-value tag),
  removing a class of runtime checks but introducing an allocation
  pattern closer to Postgres's `TupleTableSlot` than SQLite's `Mem`
  cell. Much of `vdbe.c` is the coercion ladder and simply wouldn't
  exist.
- **Storage layer.** SQLite owns a B-tree and pager; many opcodes
  are typed entry points into those modules. CRDB's storage is KV
  (Pebble + Raft + MVCC). The CRDB analog of
  `OpenRead`/`Next`/`Column` would be opcodes over KV scans and
  row decoders — same shape, different implementation beneath.
- **Concurrency model.** SQLite's "one writer at a time" is
  irrelevant. CRDB's concurrency surface is intra-statement
  (parallel scans, distributed fragments) and cross-statement
  (many sessions, shared catalog). VDBE has nothing to teach here.
- **Language runtime.** SQLite is C. A CRDB VM is Go — GC'd,
  goroutine stacks (no fibers), no inline assembly without cgo, no
  computed `goto`. A `for { switch }` in Go does compile to a jump
  table for dense cases, but the interaction with escape analysis
  and the GC is more delicate than in C. The Clustrix
  small-stack-fiber observation is relevant: CRDB cannot replicate
  that cheaply.

### Opcode set sizing

SQLite's 192 opcodes cover an entire SQL surface — DML, DDL,
transactions, savepoints, virtual tables, FTS hooks, JSON,
maintenance ops. Stripping that to "what a CRDB execution VM needs"
both shrinks and expands the count:

- *Shrinks:* CRDB does not need DDL opcodes (catalog changes go
  through schema-change machinery), FTS, virtual tables, or
  SQLite-specific maintenance ops.
- *Expands:* CRDB needs distinct opcodes per typed flavor (typed
  `Add` per numeric type, or typed dispatch on a generic `Add`),
  MVCC-aware scan opcodes, AOST/historical-read variants, locking
  opcodes (`FOR UPDATE`), and arguably a separate vectorized batch
  family if `colexec`-like execution coexists.

Net feeling: **150–250 opcodes is the right ballpark**, with the
distribution skewed more toward typed arithmetic/comparison than
SQLite's. This is a guess, not a measurement.

## 7. Open questions

- **Computed-goto dispatch in Go.** Confirm whether the Go
  compiler's switch lowering is good enough to compete with C's
  `&&label` computed goto on a 200-case opcode loop, or whether a
  generated function-pointer-per-opcode dispatch table wins. The
  single biggest performance unknown.
- **Typed registers vs tagged registers.** Decide whether the
  register file is typed-per-slot (reusing the optimizer's type
  metadata, no runtime tag) or tagged-union-per-slot like SQLite.
  Affects opcode count, runtime checks, and GC pressure.
- **Vectorized opcodes coexisting with scalar.** Does `colexec`
  become an alternate opcode family, a separate VM, or fold into
  the same instruction set with batch-shaped operands?
- **Cursor abstraction over KV.** SQLite's `BtCursor` carries
  position and lock state. The CRDB equivalent over MVCC KV scans
  needs read-timestamp, intent handling, and resume keys. Worth
  sketching `OpenRead`/`Next`/`Column` concretely against a KV
  scan before committing to an opcode shape.
- **Re-prepare on schema change.** SQLite's
  re-prepare-from-saved-SQL trick is elegant. Whether CRDB's
  current statement-cache invalidation can adopt it directly, or
  whether descriptor leases interact badly, is worth a focused
  look.
- **Subroutines and reentrance.** SQLite's choice to make
  subroutines non-reentrant (return addresses in registers) is a
  defensible simplification; whether CRDB needs reentrant
  subprograms (e.g. for recursive CTEs) needs checking.
