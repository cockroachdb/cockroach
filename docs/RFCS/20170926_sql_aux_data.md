- Feature Name: Auxiliary leaf data in SQL abstract trees
- Status: in-progress
- Start Date: 2017-09-04
- Authors: knz, jordan
- RFC PR: [#18204](https://github.com/cockroachdb/cockroach/pull/18204)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes to extract leaf data from the tree data
structures used to represent SQL syntax and logical plans, and instead:

- host the values in slices in a context data structure passed as
  argument to the functions where the values are needed/used;
- in the tree, instead store an index into that slice.

Why: this generally increases performance in several areas, and
incidentally+serendipitously removes the cause for a sore point that
prevented progress on the IR RFC (#10055).

How: this is a mechanical, easy to review code substitution in the
`sql/parser` and `sql` packages.

Impact: performance + enables further IR work.

# Motivation

tl;dr: hosting leaf data outside of the logical tree makes
things generally simpler and cleaner, which is desirable overall.

Why is this so? Granted, it is hard to recognize this to be true in
general. Concretely, there are four different sorts of leaf
data items in SQL trees, and for each of them the motivation for this
RFC can be phrased in a different way, which I detail below. However,
after reading the four motivations the reader can satisfy themselves
that the "tl'dr" summary above is adequate.

The four sorts are, in decreasing order of "how well they justify this
RFC and the corresponding changes":

- placeholders
- subqueries
- column data references ("indexed vars")
- datums

## Motivation for placeholders

Currently in CockroachDB placeholders in the input (`$1`, `$2` etc. in
prepared queries) are replaced by a `Placeholder` instance in the
tree:

```go
type Placeholder struct {
	Name string
	typeAnnotation
}
```

During type checking, the `typeAnnotation` member gets filled in with
the inferred/specified type for the placeholder. This field is
subsequently used by the `ResolvedType()` method whenever the type is
needed.

Then, when the prepared query is executed, the entire AST is
*rewritten* so that each `Placeholder` instance is replaced by the
`Datum` for the value provided by the client. Since CockroachDB avoids
in-place modifications to AST nodes, this rewrite requires
re-allocating a fresh object on the heap for every ancestor of a
placeholder up to the root of the query AST.

What's wrong with this?

- when a placeholder is used multiple times (e.g. `select $1 where x >
  $1`), the type information is stored multiple times in the AST.
- the tree rewrite at each execute of a prepared query is expensive
  and puts pressure on Go's heap allocator and GC.

## Motivation for subqueries

Subqueries in SQL can be of two sorts:

- subqueries as data sources, e.g. `select * from (select * from
  kv)` - these are simply nested selects and are treated very
  efficiently: their logical plan is simply embedded as the suitable
  operand in the enclosing query's own plan.
- subqueries as expressions. For example with `select * from kv limit
  (select v from kv where k=2)`. Here the value of the result of
  evaluating the subquery must be known before the rest of the plan
  can be executed.

This latter case is currently handled as follows:

- when a query plan is initially constructed, first all the
  expressions are traversed, and any sub-query expression is replaced
  by an instance of `sql.subquery` (or `parser.SubqueryPlaceholder`
  once #18094 is merged). *This object is embedded as leaf
  data in the expression tree!*
- when execution of a plan starts, again all expressions are
  traversed, and any sub-query is executed in turn to completion, and
  the query's results are used to *replace* the `subquery` object in
  the expression tree. Since CockroachDB avoids in-place modifications
  to AST nodes, this rewrite requires re-allocating a fresh object on
  the heap for every ancestor of a subquery up to the root of the
  query AST.
- the EXPLAIN statement must also traverse all expressions in a logical
  plan to "fish out" the logical plans of subqueries and embed them in the EXPLAIN output.

What's wrong with this?

- the various stages of subquery handling are really algorithms of the
  form "for every subquery, *no matter where it appears*, do X". It is
  silly to have to recursively traverse all expressions, *including
  those that definitely do not contain subqueries*, to apply these
  algorithms.
- the rewrite of the subquery results, when applicable, is expensive
  towards Go's heap allocator and GC.

## Motivation for column data referencs ("indexed vars")

Currently in CockroachDB an early transform called "name resolution"
will replace any column reference by name (e.g. "`k`" in `select k
from foo`) or by ordinal reference (e.g. `@1` in `select @1 from foo`)
by an instance of `IndexedVar`:

```go
type IndexedVar struct {
	Idx       int
	container IndexedVarContainer
}
```

This object contains the index of the column inside the "current data
source context" (usually: the current table; this is only more complex
with joins and UPSERT). It *also* contains a pointer to some other object, somewhere, that is able to:

- serve the type of the column (via the `ResolvedType()` method);
- serve the value of the column for the current row (via the `Eval()` method);
- serve a representation of the column reference (via the `Format()` method).

This `container` fields requires some care during transforms: if an
expression is migrated from one level of a logical plan to another (a
common occurrence during optimizations), the `container` field must be
suitably rewritten.

This happens pretty often actually (`BindIfUnbound()` and `Rebind()`). Two aspects of note:

- during initial name resolution, `BindIfUnbound()` is called, and
  this actually overwrites `container` in-place. This incidentally
  violates the rule that ASTs should be immutable once constructed!
  And has caused bugs (and probably is causing bugs, due to our
  inability to assert immutability).
- `Rebind()` is called many times for the same expression in
  moderately complex queries (most notoriously during filter
  propagation), each time requires a full traversal of the entire
  expression tree, and if an expression is found requires a
  substitution. And as you can expect if you've read the two sections
  above: since CockroachDB avoids in-place modifications to AST nodes,
  this rewrite requires re-allocating a fresh object on the heap for
  every ancestor of a subquery up to the root of the query AST.

What's wrong with this?

- `BindIfUnbound()` violates the immutability contract;
- if the same column reference is used multiple times at the same
  level of a logical plan, there are multiple redundant references
  (`container` copies) to the data source.
- `Rebind()` exerts pressure on the Go heap allocator and GC.

## Motivation for Datums

(This motivating section is a bit less evident for the casual reader
with less experience with CockroachDB's SQL codebase. It is also not
terribly important since the 3 sections above already motivate the
common underlying pattern. Feel free to skip to the next sub-section.)

CockroachDB currently defines 17 elementary Go types to hold SQL
values (`DInt`, `DString`, `DInterval`, etc.), increasing to 18
when #18171 merges, and possibly increasing further as we extend pg
compatibility. Moreover, we are eventually planning to let users
define their own value types.

It is certainly instructive to ask: "Why?"

For example, TiDB uses a single Go struct for all the values, storing
the actual value in different fields of that struct depending on the
semantic type of the SQL expression. (This is not where this RFC is
going but this observation highlights that the current approach in
CockroachDB is not trivially necessary.)

There are two motivations for using separate Go types:

- Go requires that values that are composed of a reference to
  something else (e.g. `DString`, or a slice reference as in `DByte`),
  and values that are composed of simple "value bits" (e.g. `DInt`,
  `DFloat`) are stored in variables that have distinct Go types,
  because Go's GC needs to differentiate statically value and
  reference variables.
- in many expression transformation algorithms in CockroachDB we must
  discriminate in conditionals based on the semantic type of SQL
  values. It so happens that Go is rather good at branching on type
  tags ("type switches" in Go's jargon) so this vaguely suggests
  separate Go types for each SQL value type.

What's wrong with this?

- `Datum` types are actually embedded in SQL expression trees using an
  interface reference (`Datum` and/or `Expr`). This means that even
  "value" types are never embedded as-is in the expression tree and
  must be allocated on the heap instead. Worse even, a SQL value that
  is itself a reference (e.g. `DBytes`) then requires a double
  indirection: once to retrieve the Datum object from the Expr
  reference, then another one to access the data behind that datum's
  reference. Given the ubiquity of values this merits some attention
  if a simplification is possible.
- the value types are also used to report values in *result rows*
  during plan execution, behind a `Datum` reference. In Go this means
  that each time a row tuple is constructed, Go must assemble a
  reference to the value together with the vtable pointer of the
  `Datum` interface, to construct a `Datum` reference in each position
  of the result tuple. Then whenever this value is consumed somewhere
  else (either at a different level in the logical plan or when the
  results are converted towards pgwire), Go must check that the type
  cast is valid, this constitutes run-time overhead. One should note
  here that this type dance is entirely *unnecessary*: from one row to
  the next, the type of the SQL value for a given result column is
  *always the same* (*). Storing then checking the Go type information in
  memory for every cell of every row in a plan's result column is
  horrendously redundant.

(*) Regarding the handling of NULL values: NULL is a value that
inhabits every type. That is, `NULL::int` and `NULL::string` are two
different things in SQL. Even in a column where NULL *values* can
occur, all values in the columns should still be of the same type.
This is not currently true in CockroachDB, some suggestions are made
below to arrive there.

## Synthesis: what's the underlying problematic pattern?

Discriminating different things in a tree data structure in Go using
different Go types implementing a common "node interface" is a
textbook design pattern. It is simple to understand, simple to
implement, simple to recognize, and "just works".

However it breaks down when any of the following conditions apply:

- the application domain strongly favors/recommends immutable trees,
  but the logical values in the trees require replacement between
  uses. This requires an expensive "rewrite via new allocation" traversal.
- some algorithms need to perform an action for every node in a tree of a
  specific type, without knowning in advance where these values are,
  and this types happens to be rather uncommon in trees in
  practice. This causes "work for nothing": the common case pays the
  price of full tree traversals even when the application domain tells us
  in advance that these full traversals are usually unnecessary.
- when many objects *are known to have the same type* (e.g. values for
  the same column in many rows, multiple instances of the same
  placeholders in different places of an expression) it is wasteful to
  duplicate this type information across all instances.

In CockroachDB, all three conditions apply. So there's a problem.

# Guide-level explanation

When this proposal is implemented, CockroachDB will replace the
"embedding" of special values in IR trees by a simple integer value,
to serve as index in an array of appropriate type outside of the tree.

For example, before:

```go
e := BinExpr{Left: IndexedVar{Idx:1}, Right: IndexedVar{Idx:2}, Op: Add}

// Link IndexedVars to containers.
ivarHelper := MakeIndexedVarHelper(container)
e = ivarHelper.Rebind(e)

// Use the Expr.
fmt.Println(e.String()) // shows "x + y" instead of "@1 + @2"

// This uses:
// type IndexedVar struct {Idx int; container IndexedVarContainer}
// type Expr interface { Format(buf *bytes.Buffer); String() string };
// (X).String() calls (X).Format() for every X implementing Expr;
// (iv *IndexedVar).Format() calls iv.container.FormatIndexedVar(iv.Idx).
```

After:

```go
e := BinExpr{Left: IndexedVarIdx(1), Right: IndexedVarIdx(2), Op: Add}

// Use the Expr.
fmt.Println(Format(e, container)) // shows "x + y" instead of "@1 + @2"

// This uses:
type IndexedVarIdx int
type Expr { Format(buf *bytes.Buffer, container IndexedVarContainer) ... }
func Format(e, container) {
  e.Format(buf, container)
}
func (iv IndexedVarIdx) Format(buf, container) {
  container.FormatIndexedVar(int(iv))
}
```

In general, the tree structure will store only an integer, and the
resolution of that integer to the "thing" that it logically refers to
is only performed at the point of use, not stored in the tree
directly. This enables minimal data storage in the tree and efficient
substitutions of the corresponding values without having to mutate the
tree.

# Reference-level explanation

## Detailed design

| Current code | New code | Notes |
|--------------|----------|-------|
| `type IndexedVar struct {...}` | `type IndexedVarIdx int` | |
| `type Placeholder struct {name, typ}` | `type PlaceholderIdx string` | (1) |
| `type subquery struct {...}` | `type subquery int` | |
| `type Datum interface { Expr; ... }` | `type Datum int` | (4) |
| `(NodeFormatter).Format(buf, f)` | `(NodeFormatter).Format(ctx, buf)` | (2) (3) |
| `(TypedExpr).ResolvedType()` | `(TypedExpr).ResolvedType(ctx)` | (3) |
| `(Datum).AmbiguousFormat()` | `(Datum).AmbiguousFormat(ctx)` | (3) |
| `(Datum).Prev()` | `(Datum).Prev(ctx)` | (3) |
| `(Datum).Next()` | `(Datum).Prev(ctx)` | (3) |
| `(Datum).IsMin()` | `(Datum).IsMin(ctx)` | (3) |
| `(Datum).IsMax()` | `(Datum).IsMax(ctx)` | (3) |
| `(Datum).min()` | `(Datum).min(ctx)` | (3) |
| `(Datum).max()` | `(Datum).max(ctx)` | (3) |
| `(Datum).Size()` | `(Datum).Size(ctx)` | (3) |

Notes:

1. about placeholders: CockroachDB currently identifies placeholders
   by name. This is because the Postgres protocol, in principle,
   allows placeholders with arbitrary names not just numbers. In
   practice however, we never encountered a client that does so, and
   the CockroachDB code even contains an assertion on the initial
   lexing of placeholder names to force them to be numerical. Perhaps
   it is time to drop the idea to name placeholders and instead number
   them, which will in turn make the data structure even smaller and
   more efficient to use (lookups using an array instead of a map).
   We can note here that placeholders are always "dense" in practice
   (all the placeholder between $1 and $max are used), so this
   optimization will not create memory inefficiencies.

2. the `FmtFlags` argument is replaced by a `FormattingContext` struct
   reference which contains both the formatting flags / overrides and
   (a reference to) the semantic context.

3. the new semantic context reference passed through the recursive
   interface API gives the method implementations access to the arrays
   where they can look up the values from the numeric
   Datum/subquery/indexedvar/placeholder references.

4. Regarding the handling of NULL. If we wish to keep the current
   implementation semantics which treats NULL values in trees as
   always untyped (`DNull` never has a type other than itself), we can
   use the value -1 to encode it as an integer. If we wish to change
   this and make NULL a member of every type, and have datum slots
   have a type next to a NULL value, then the index can refer to a
   value slot with no data, and we can separately introduce a bitmap
   of which datums in the value slots should be interpreted as SQL
   NULLs.

## Opportunity for logical plans

Currently in CockroachDB the logical links between stages of logical
plans are implemented using simple Go references. For example, a
`joinNode` is a struct with two members `left` and `right` each of
type `planNode`, an interface, and this can be dereferenced in memory
to get access to another Go struct.

Meanwhile, most of the "interesting" optimization algorithms for SQL
queries make use of the notion of *equivalence classes* for logical
plans: two (sub-)trees in a logical plans that are semantically
equivalent (same columns, same result rows) can be substituted for one
another "for free", and the different candidate equivalent plans
should be kept in memory side by side while the optimization
measures/decides which one to keep.

This strongly suggests to implement the link between logical plans not
as a simple Go reference to another plan, but instead as a reference
to a "equivalence class" which in data structure terms would be
something like a "set" (probably an array in practice), itself
containing references to actual trees.

The issue here is that the particular data structure(s) to be used to
represent equivalence classes may change between optimization
algorithms. We wouldn't want to change the code to traverse logical
plans and otherwise manipulate them (i.e. change the IR language that
defines logical plans) every time we consider a different way to
represent equivalence classes.

However, behold! The pattern presented in this RFC can be once again
reused here. A link to a logical sub-tree in a logical plan can be
stored as an integer. Then the algorithms that traverse the logical
plan can take a context argument, and use that context to look up
sub-tree from these integer values. This enables decoupling the
implementation of equivalence classes from the implementation of
logical plan traversals that are not particular to optimization
algorithms.

## Drawbacks

- Slight deviation from 100-level data structure textbooks. (Although
  the new proposed pattern is not really unknown to 200- or 300-level
  compiler courses).
- There's a new additional function argument to some of the expression
  methods. The potential corresponding run-time overhead is expected
  to be offset by the overall cache utilization gains of storing much
  less redundant information side-by-side in memory, together with the
  lower GC activity.

## Rationale and Alternatives

The motivation section above provides the majority of the rationale
for this change. Even if the motivation section for Datums isn't as
transparent as the three others, there is impetus to do something
about Datums suggested by the query compilation RFC (#16686).

Additionally, another motivation emerges from the IR RFC (#10055): we
need leaf data using Go types that cannot be expressed easily using
basic types (e.g. when the types belong to external Go packages, like
we do for `DDecimal`). This causes a difficulty when planning to
implement/deploy a code generator from a [simple type
language](20170517_algebraic_data_types.md): the code generator should
then be extended in several non-trival ways to support allocating,
manipulating and traversing objects at run-time which it knows little
about; the input definition language must be extended to specify these
external types; and the testing and validation story becomes much more
murky. This was a major unresolved question in the IR RFC in #10055
until this point. By subjecting all non-trivial leaf data to the
treatment advertised in this RFC, this complexity is side-stepped
entirely.

Several alternatives were considered:

- do nothing: further SQL development and run-time performance slowly
  grinds to a halt (hyperbolic perhaps, but it highlights the trend).
- rewrite CockroachDB's SQL layer in a different programming language
  which enables programmers to control allocations and avoid paying
  run-time overhead for typing when it's known to be safe. This
  alternative, if considered, would necessarily be a longer-term
  endeavour. Also even if the generated code would be overall much
  more heap-efficient with tree rewrites, it would still require
  algorithms to traverse trees even when it's not needed.
- extend the ADT RFC to support a much richer type system, and express
  all the tree data using that.  This more or less amounts to
  re-inventing a programming language, which is a lot more
  effort. Also it does not really solve the problem of redundant
  data. Also even if the generated code would be overall much more
  heap-efficient with tree rewrites, it would still require algorithms
  to traverse trees even when it's not needed.
- when expressions are analyzed, store a copy of the reference to the
  "special" nodes (placeholders/subqueries/datums/indexedvars) into
  the semantic context. This way algorithms that need to do "for every
  X, do Y" can find all the X's in the semantic context and avoid
  expression traversal. This does not eliminate information redundancy
  however, nor the overhead of type casts.

The reviewers are invited to suggest additional alternatives if they
can see any!

## Unresolved questions

None.
