- Feature Name: Query Compilation
- Status: draft
- Start Date: 2017-06-22
- Authors: knz
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

The RFC proposes to invest into JIT compilation of queries into a
representation suitable for fast(er) execution and more control
over memory allocations and accounting.

This proposal is fully orthogonal/complementary to the plan to use an
IR for query optimizations/transformations. We are focusing here on
what happens after a query has been fully optimized.

The strategy would be to implement in two steps, the first operating
only on expressions, which is (slightly) easier and would benefit
distributed execution immediately; then on logical plans, to
accelerate local execution and possibly teach us how to accellerate
distributed processors later.

# Motivation

- for compilation of expressions:
  - expression evaluation has a lot of Go call overhead
  - performs lots of heap allocations (GC pressure)
  - poor ability to track memory usage accurately (higher risk of OOM or query failure)
  - compilation would accelerate both local and distributed execution
  - nearly impossible to adapt current code to accelerate towards vector processing

- for compilation of query plans:
  - current interpretation scheme has a lot of Go call overhead
  - performs lots of heap allocations (GC pressure)
  - memory usage accounting is ad-hoc and hard to maintain/verify
    - it's currently difficult to decide ahead of time whether to use in-RAM or on-disk memory
	  for processing. Compilation would ease that.
  - compilation would accelerate local execution (and perhaps, distributed too)
  - nearly impossible to adapt current code to accelerate towards rowbatch-oriented processing
  - hard to adapt the current code to accelerate towards concurrent local execution

- thinking forward: a bytecode representation for expressions and query plans would make it possible
  for 3rd parties to target the CockroachDB execution engine from different query language
  - creates a smoother path to support the MySQL protocol
  - creates a strong(er) foundation to the vision "CockroachDB as a computing platform"

# Detailed design

## Compilation of expression evaluation

### Background

The current code uses naive interpretation by depth-first recursion
onto the expression AST. The tree is recursed into for every instance
of the expression to be evaluated, that is at least at every row. Each
intermediate operation allocates its result on the heap. For example, the
following code is used every time two DECIMAL values are added together:

```go
func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
	l := &left.(*DDecimal).Decimal
	r := &right.(*DDecimal).Decimal
	dd := &DDecimal{}
	_, err := ExactCtx.Add(&dd.Decimal, l, r)
	return dd, err
}
```

### Intro to compilation to stack machines

The basic idea below is a classical approach, which is both extremely
well studied, straightforward to implement and super-fast to
compile. It is not as fast at run-time as JIT compilation to native
code, but native JIT compilation is very expensive upfront and the
mechanism proposed here is probably going to get us big wins compared
to our current approach.

The approach assumes that the execution engine implements a stack
machine. A stack machine is an abstract construction consisting of two
"memories" with the structure of a stack: a code memory and a data
memory. They are called "stacks", in contrast with the more
conventional "random access" memories found in some other abstract
machines, because the data can only be accessed from one end, using
push/pop/peek operations.

(For the theoretically inclined: the usual "simple story" about stack
machine compilation assumes a data stack and a linear tape for
code. That's because the simple story usually doesn't deal with
conditionals. SQL has IF/CASE, so the code needs to live in a stack,
and conditionals instructions conditionally push/pop code).

### Execution on a stack machine

We start with execution because it is always easier to explain and
comprehend compilation if the execution model is clear beforehand.  I
will explain both the abstract model and how a emulation of it on a
conventional computer works.

- the stack machine operates in cycles.
- the implementation uses a main loop, where one iteration emulates a cycle.

- if the code stack is empty, the machine halts.
- the implementation uses a "code stack pointer" (CSP). The code stack
  is a fixed size array. The head of the stack is at the beginning of
  the array. If the CSP reaches the end of the array, there is no more
  code and the machine halts.

- at each cycle, the machine pops an instruction.
- the implementation accesses the code stack at the current CSP value,
  and increases the CSP.

- the instruction contains three things:
  - how many items from the data stack the operation will operate on. These items will get popped first.
  - which operation to perform on this data. The operation, after it completes, will determine
    how many results (usually 1) were produced; the results get added back to the data stack.
  - what happens with the code stack. Usually this part of the instruction is empty,
    which means the instruction disappears from the code stack. For conditionals, it's more
    complicated (see below).
- in an implementation, the instruction is encoded as a number or a struct that encodes the
  things explained above. In Go the operation definition would be a function pointer.

- the stack machine runs the instruction, and pushes back the results into the data stack
- in the implementation, the operation is interpreted. Note that no type switch or conditional
  is needed here (except for conditional operations, see below). There is a separate array for
  data with a separate data stack pointer (DSP). The data array, in contrast to the code array,
  has the head at the end. The DSP is decremented and incremented as needed
  to represent pops and pushes.

- once the current instruction has completed, the stack machine starts the next cycle.
- the implementation completes the current iteration and starts the next.

### Minimal set of instructions

- load constant into data stack. The implementation takes the
  corresponding constant value from a pre-computed array of constants
  (separate from the two stacks), and places it on the data stack.  
- conditional code pop: pop zero or more values from the data
  stack. Depending on some function of this data, pop zero or more
  values from the code stack and push zero or more values into the
  data stack. For example, a simple `if a then b else c` where b has 3
  instructions and c has 10 instructions would be encoded as `<code
  for a> <"if (pop 1 data) (pop 4 code)"> <code for b> <"if true (pop
  10 code)"> <code for c>` 
- any data-to-data computation useful for the language: add, substract, etc.

### Compilation to a stack machine

The task of a compiler is to transform an expression AST into a representation
into three data structures: a code stack, a data stack and a constant array.

The compiler can operate with a single depth-first traversal of the AST.
The transformation operates as follows on each tree node:

- a datum:
  - add the value into the constant array.
  - add a "load constant" instruction at the end of the code array.
- a data-to-data operation:
  - recurse into the operand sub-trees first, then
  - add the corresponding instruction at the end of the code array.
- a conditional:
  - recurse into the condition sub-tree first, then
  - for each branch sub-tree,
    - make a temporary code stack array, then
    - recurse into the branch sub-tree and let it populate the temporary code stack array.
  - use the length of each code stack sub-array to create the
    necessary conditional code pop instructions.
  - concatenate the code stack sub-arrays into the main one.

### Example

Let's watch this work, with the expression `(3 + 2) * 4`.
The tree for this is `BinOp(*, BinOp(+, Datum(3), Datum(2)), Datum(4))`

Compilation recursion:

- enter outer BinOp
- recurse into inner BinOp
- recurse into first Datum
- Datum to constant array: [3],
- Code array: [loadcst 0]
- recurse into 2nd Datum
- Datum to constant array [3 2]
- Code array: [loadcst 0, loadcst 1]
- Recursion back to inner BinOp, generate opcode:
- Code array: [loadcst 0, loadcst 1, add]
- Recursion back to outer binOp, then recurse into 3rd datum
- Datum to constant array: [3 2 4]
- Code array: [loadcst 0, loadcst 1, add, loadcst 2]
- Recursion back to outer binOp, generate opcode:
- Code array: [loadcst 0, loadcst 1, add, loadcst 2, mul]

At run-time:
Inputs:
- code stack:  [loadcst 0, loadcst 1, add, loadcst 2, mul]
- constant array: [3 2 4]
- data stack initially empty

1. loadcst 0: data stack becomes [3]
2. loadcst 1: data stack becomes [3 2]
3. add: data stack becomes [5]
4. loadcst 2: data stack becomes [5 4]
5. mul: data stack becomes [6]
6. no more instruction, the machine halts. The end result is the state of the data stack.

### Reuse for multiple evaluations, expression parameters

The same code array + constant array together can be reused multiple
times to evaluate the same expression.

Of course in SQL we have placeholders and column references
(IndexedVars) which can change for every row -- how to deal with
those?

There are two mechanisms possible, equally easy to implement:

- consider any placeholder or IndexedVar as a "constant" in the
  compilation scheme above. However, remember which positions in the
  constant array correspond to each placehoder or Indexedvar. When the
  placeholder or IndexedVar changes (different row or different
  placeholder values), replace the value in the constant array by the
  new placeholder/IndexedVar value.

- introduce a new instruction type "read placeholder" and/or "read
  IndexedVar", similar to "load constant", which take the value and
  places it into the data stack. Use them during compilation.

(Which one should be used may depend on some initial benchmarking to
check which one is faster. I have a slight preference for 3 separate
instructions, this allows to share the constant array when the same
expression is used in different spots in SQL which could run
concurrently.)

### Conditionals

SQL has two conditionals: COALESCE/IFNULL, and CASE. These are
conditional operations because some of their operands *must not be
evaluated* depending on the run-time value of some other operand.

COALESCE can be compiled as follows (this is a special case
of the general case for conditional compilation already explained above):

- compile each of the N sub-expressions in N separate code sub-arrays
- append the first code sub-array into the main code array
- compute the sum S(N-1) of the sizes of the remaining N-1 code sub-arrays
- append a conditional code pop instruction to the code array equivalent to
  `if (pop 1 data) != NULL { (push 1 data), (pop N-1+S(N-1)-1 code) }`
- append the 2nd code sub-array into the main code array
- append a conditional code pop instruction to the code array equivalent to
  `if (pop 1 data) != NULL { (push 1 data), (pop N-2+S(N-2)-1 code) }`
- do the same again for the remaining N-3 code sub-arrays. For the last one
  omit the final conditional instruction.

that's all! For example, consider `COALESCE(3+sin(x), y-42, 69)+10`.

- the compilation of the sub expressions gives the code sub-arrays:
  - [loadcst 3, loadcst x, call sin[2 args], add]
  - [loadcst y, loadcst 42, sub]
  - [loadcst 69]
- main code array grows with [loadcst 3, loadcst x, call sin[2 args], add]
- main code array grows with `if (pop 1 data) != NULL { (push 1 data), (pop 5 code) }`
- main code array grows with [loadcst y, loadcst 42, sub]
- main code array grows with `if (pop 1 data) != NULL { (push 1 data), (pop 1 code) }`
- main code array grows with [loadcst 69]
- compilation continues, to add [loadcst 10, add]

End result (code array):

```
loadcst 3
loadcst x
call sin[2 args]
add
if (pop 1 data) != NULL { (push 1 data), (pop 5 code) }
loadcst y
loadcst 42
sub
if (pop 1 data) != NULL { (push 1 data), (pop 1 code) }
loadcst 69
loadcst 10
add
```

At run-time, supposing x is NULL and y is 0:

0. code = [loadcst 3, ...] data = []
1. code = [loadcst x, ...] data = [3]
2. code = [call sin, ...], data = [3 NULL]
3. code = [add, ...], data = [3 NULL]
4. code = [if, ...], data = [NULL]
5. code = [loadcst y, ...], data = []
6. code = [loadcst 42, ...], data = [0]
7. code = [sub, ...], data = [0 42]
8. code = [if, ...], data = [-42]
9. code = [loadcst 10, ...], data = [-42]
10. code = [add], data = [-42 10]
11. code = [], data = [30]
12. machine ends

Compilation for CASE is similar and left as an exercise to the reader.

### Performance aspects

The immediate performance advantages of the approach presented so far are:

- compilation in linear time (linear in the number of nodes in the
  AST), i.e. super fast, when there are no conditional expressions
- very few branches in the execution code: much more friendly to the branch predictor
- very little call traffic: much fewer instructions to set up/tear down stack frames
- the execution engine can keep track of expression memory accurately
  - in particular the maximum datum memory usage can be computed before the
    query is ran

Possible additional performance improvements:

- the data array can contain numeric indices to separate Go slices for
  each datum type, instead of references to the Datum objects
  themselves, to enable datum reuse and further reduce heap
  allocations. This is especially interesting in the case of result
  values for individual operations, as these can then target an
  existing slice slot instead of allocating a fresh object.

- once the expression has been compiled, the execution engine can
  operate on batches of raws at a time with no change in the compiler
  -- simply change the meaning of operation instructions to operate on
  vectors of value at a time.  (this can even be done when there are
  conditional expressions using a conventional trick of vector
  processors - I can explain if there is demand, GPUs do the same)

### Advanced memory accounting

Once this machinery is in place we can move forward and estimate more
precisely a upper bound on how much memory expression evaluation can
use *as a function of the data size in the tables being read*.

In other words we can get the compiler to produce a function, and not
a single number. Given some table statistics we can then estimate the
cost dynamically.

How this works? Here's some "black magic": plug the size upper bounds
of the input of an expression (its constant array + placeholders +
column input) in the place of the inputs themselves; then change the
Go code used for the instructions generated by the compiler to change their meaning to compute memory usage instead.
For example:

- "add int,int" - becomes arg[0]
- "add decimal,decimal" - becomes max(arg[0], arg[1])
- "concat string,string" - becomes arg[0] + arg[1]

then evaluate the expression in that context. The "result" of *that*
evaluation is the upper memory bound for the evaluation on real
values.

## Compilation of query plans

The proposal here is to do something conceptually similar to
expressions above: define a general abstract machine with a simple
instruction set, transform a logical query plan (or a sub-part of a
logical query plan) into some static representation of what they do,
expressed as arrays of instructions and constants, and implement a
run-time engine that emulates the abstract machine.

### Overview / Conceptual model

The individual nodes of a logical plan, both in theoretical relational
algebra and in the volcano model, can be modeled as a streaming processor
that takes input rows from zero or more input streams and produces
output rows into one output stream.

The connections between nodes then establish how the streams are
connected; a traditional plan typically uses a tree topology, but we
will not assume a specific topology here.

What we will assume however is that whichever topology there is, it is
*static*: how the nodes are connected between themselves does not
change during query execution.

So in summary the components we are manipulating in the abstract machine are:

- plan nodes that are stream-to-stream functions (programs whose
  execution take streamed data as input and produce streamed data)
- a topology (set of edges between nodes)

A query compiler is thus an algorithm which transforms a logical query
plan to a set of node programs + constants (i.e. two arrays per node)
and a topology (one array).

The execution engine is then an algorithm which takes what the
compiler produces, and "runs" it: schedules execution of the
node-programs in some order which ensures progress on all streams. The
order is called "a schedule":

- The Volcano models tells us that if the topology is a tree, the
  scheduler can simply operate by depth-first walks of the topology:
  if the topology is represented as an array and pre-sorted, the
  schedule is simply a linear traversal;
- if the topology is more complex than that then we can either compute
  a schedule in advance in some common cases (namely, when it is known in
  advance that there is no internal buffering of rows by the nodes and
  each node can produce at most a constant of rows per set of input
  rows - that's called static dataflow scheduling in the literature);
  or we can use a classical dynamic scheduleer which keeps track of
  which streams are non-empty and which streams are full to decide
  what to do next.

### Abstract machine

The machine for plan execution is more complex than for simple
expressions.

#### Storage

It contains the following memories, where N is the
number of nodes in the logical plan and M the number of edges in the
topology:

- N code arrays (not stack any more - but the careful reader will recognize that
  the stack-based execution described above naturally extends to a code array)
- N code pointers and program status (running/waiting/stopped)
- 1 or N temporary data stacks (the data stack in the previous code), which
  is reset to empty between expression evaluations. Only 1 is needed if execution
  is entirely sequential, but the overall abstract machine naturally extends
  to concurrent execution in which case each node needs its own temp data stack
- M row buffers, each with minimum capacity of 1 row (the precise
  capacity can be even fixed to 1 in an initial, sequential
  implementation; larger capacities are only useful when considering
  processing by row batches or concurrently).
- bucket memories for each node that needs them; their number is fixed
  for each logical plan.

A "bucket memory" is the equivalent of a sorted K/V store: a data
structure which can use the key encoding of a subset of a row (or a
row prefix) as key and store one or more rows as values, and which can
be iterated on to read from the buckets in sorted order.

*Conjecture*: no additional memory is needed (conceptually) to perform
every SQL computation -- everything from sorts, unions, joins etc. can
use just the row buffers and the bucket memories.

Note: I acknowledge there is an argument to be made in the
*implementation* for different "flavors" of bucket memories: 
- with or without ordering of buckets (e.g. for JOIN),
- with or without keys (just one bucket with all rows therein, e.g. for CROSS JOIN), 
- with or without values (just the buckets, e.g. for DISTINCT),
- in-RAM or on-disk (e.g. for large buckets),

However this is an "implementation detail" which needs not be
considered at the level of the abstract machine and in the outline of
a compiler.

#### Local Execution - sequential

(probably the first to be implemented, both because it's simple and
because it can serve as baseline to measure the benefits of any other
approach.)

- We assume a sorted topology, so that if node A is a source for node B,
  it appears before B in the node array. (The assumption is realizable for tree plans)

- the machine operates in cycles.

- at each cycle, the machine iterates over the sorted topology. For each iteration (each node):
  - if the current node is in state "stopped" it is skipped
  - if it is "waiting" but none of its input streams have data, it is skipped
  - the machine progresses the node-program (by executing instructions
    from the node's code array from the current code pointer forward) until
    it either stops explicitly or encounters a blocked read or write
    operation on one of its streams (or, optionally, until the
    node-program has executed some maximum number of local
    instructions. We can use this to manage time consumption.
  - the node-status is updated (waiting/running/terminated)

- at the end of a cycle, if no node has made progress, the machine stops:
  - with a deadlock error (internal error) if there are still waiting nodes,
  - with success if all nodes have terminated.

In this algorithm since only 1 node can be evaluating expressions at a time,
a single data stack for expression evaluation is sufficient.

#### Local Execution - concurrent

Here we extend the sequential execution by creating multiple goroutines.
Each goroutine can be in charge of 1 or mode nodes. If in charge of multiple nodes,
it can use a sequential scheduler locally.

What is important is that a suitably synchronized data structure is
used for inter-goroutine streams. Go channels would work but may be
overkill, because Go channels support multiple readers and multiple
writers and thus use expensive synchronization primitives. Query
execution only uses single-reader, single-writer queues so can
probably benefit from something simpler.

#### Mixed ditributed-local execution

(To be developed further): same execution engine for individual nodes,
but the stream data structures are substituted by distsql flow
endpoints.

### Compilation - query-level

The compiler performs a walk (depth-first traveral) of the logical
plan; for each node it produces:
- the code array for that node,
- the constant array(s) for its expression(s) if any,
- how many input and output streams it has and with which schema (column types)
- the number of bucket memories it needs.

During the recursion, if we are simply planning for local execution
then the recursion code can also immediately produce the topology (the
suitably sorted array of nodes). If this is later extended to mesh
with the distsql planner, the distql planner can be smarter and decide
more complex topologies instead.

With regards to data structures, the different things enumerated above
can be stored in arrays -- no need for tree-like data structures. Each
node in the logical plan is numbered, and the node number can be used
to index plan-level arrays containing the other things as items.

### Compilation - node-level

Inside each node expression compilation can proceed as per the
previous top-level section in this RFC.

For the surrounding relational algorithm (e.g. sorting, duplicate
elimination etc) Two approaches are possible:

- fixed-function node programs with code implemented in Go
  - the Go code is then modified to use the pre-allocated data
    structures produced by the compiler: the bucket memories, the
    streams, and the row buffers.
- node programs are compiled to elementery instructions then interpreted
  by a general-purpose execution engine.
  - if going in that direction, there would still be a handful of
    primitives still implemented as blackbox Go functions, at least
    for KV access to the distributed data store and virtual tables.

The first is probably going to be used initially, as a stepping stone
and to make the work incremental: the fixed-function node programs are
simply a mash-up of the existing code for the `Start` and `Next`
methods.

The second approach can then be pushed forward from there, motivated
by two goals:
- unify the execution engine for local and distributed execution (less
  code to audit/maintain)
- present a general-purpose computing framework that can be targeted
  by non-SQL languages.

### Performance aspects

The immediate performance advantages of query plan compilation:

- compilation in linear time (linear in the number of nodes in the logical plan), i.e. super fast
- very few branches in the execution code: much more friendly to the branch predictor
- very little call traffic: much fewer instructions to set up/tear down stack frames
- very little to no DTuple (tuple array) allocations - the row buffers can be pre-allocated
  in advance because the topology is fixed
- the execution engine can keep track of memory more accurately at
  lower cost, often ahead of time (see next sub-section)

### Advanced memory accounting

For each node type we need a resource model that estimates the
resource consumption (both data stack + bucket memory) as a function
of available table statistics. Because the abstract machine model here
is *much* more simple than the current Go code, the problem becomes
tractable. (I think?)

In particular there should much less need (possibly no need at all for
many common query plans) to update memory accounts on every row
processed, as the upper bound can be then computed fully ahead of
time.

This last part in particular is crucial to automate the decision of
whether to use in-RAM or on-disk storage for bucket memories.

# Drawbacks

- Some additional complexity for new hires to understand what is going on.
  Preventative care:
  - document, document, document
  - develop a codelab soon after
  - augment CRL "suggested reading" list(s) with books on compilation

# Unresolved questions

- introducing the Apply relational operator (or equivalent) for
  correlated subqueries and recursive CTEs will need to extend the
  conceptual model slightly. I believe, but haven't completely thought
  out yet, that the topology can stay static and therefore the
  schedule + resource model can be precomputed even in presence of
  Apply.

- ???
