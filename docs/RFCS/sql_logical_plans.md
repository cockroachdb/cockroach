- Feature Name: Data structures for SQL logical plans
- Status: draft
- Start Date: 2010-10-12
- Authors: knz, Peter Mattis
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC outlines new data structures and principles around the
handling of SQL logical plans, that both 1) lower the cost of managing
logical plans in the current architecture 2) facilitate further work
on SQL optimizations.

This RFC is motivated by a) the need to set up the scene for multiple
other RFCs to come and b) to translate into human language a [large,
interesting yet mostly uncommented repository of code that Peter
produced](https://github.com/petermattis/opttoy).

This RFC is based on the following prior work:

- [Tech note: Guiding principles for the SQL middle-end and back-end in CockroachDB](../tech-nodes/sql-principles.md)
- [Tech note / meta-RFC: Upcoming SQL changes / Evolving the SQL layer in CockroachDB](https://github.com/cockroachdb/cockroach/pull/18977)
- [RFC: SQL Logical planning](https://github.com/cockroachdb/cockroach/pull/19135) in particular the terminology
  set forth there, for example the "prep", "rewrite" and "search" phases.
- [RFC: Algebraic Data Types in Go](20170517_algebraic_data_types.md) because we're going to reuse the mechanisms there.

In the plan set forward with "upcoming SQL changes" the present RFC
corresponds to the items:

- SQL leaf data
- Introduce scoping and free variables in name resolution
- Apply relational operator
- Plan equivalency classes
- Make plans stateless / context-free

i.e. acting on this RFC will move forward on all these 5 items at
once, and complete several of them.

Finally, this RFC is not *based* on the [IR
RFC](https://github.com/cockroachdb/cockroach/pull/10055) but will
instead serve as new *prelude* to it. The text in the IR RFC that
pertains to this one will be migrated here, and the remaining text
will be later updated to reflect the choices made here.

# Motivation

The motivation for this work is to reduce various overheads of the
current technology and to align the architecture of CockroachDB with
the state-of-the-art.

# Guide-level explanation

We use a simplified mini-SQL language defined below, or µSQL, to
simplify the explanation in this section.

## Reminder/overview of the situation prior to this RFC

Prior to this RFC, CockroachDB would take as input a query like:

```sql
select v, k from (table kv) where k < 3
```

and first parse it as an abstract syntax tree (AST):

```
(Select
  (SelectClause
     Targets: [(UnresolvedName "v") (UnresolvedName "k")]
     From:    [(UnresolvedName "kv")]
     Where:   (BinExpr
                Op:   Lt
                Left:  (UnresolvedName "v")
                Right: (NumVal 3))
    ))
```

(We use S-expression syntax here to represent trees, to simplify the
authoring of the RFC. Imagine a tree data structure in memory.)

Then the construction of the planNode tree would run semantic analysis
on all the expressions to "resolve the names" and "resolve the
types". The results would be:

```
(renderNode
  renders: [(IndexedVar 1) (IndexedVar 0)]
  source:
     (filterNode
        filter: (BinExpr
                  Op:    Lt
                  Left:  (IndexedVar 0)
                  Right: (DInt 3))
        source: (scan table: "kv")))
```

Note how the planNode tree contains AST nodes from the original AST as
branches.

Then a separate phase "plan expansion" would simplify this plan, in
this case generating the span for the scan:

```
(renderNode
  renders: [(IndexedVar 1) (IndexedVar 0)]
  source: (scan
             table: "kv"
             spans: [/#-/2])
  )
```

In this vision, we have a strong conceptual separation between
"logical plan nodes" (with nodes implementing the `planNode`
interface) and "SQL expression nodes" (implementing the `parser.Expr`
interface).

## Overview of the situation after this RFC

The same query initially would be transformed to the same AST as
previously; that part doesn't change.  However the first thing
happening afterwards is the translation of that AST into a fully
independent data structure:

```
0: (project 1 [4 3])
1: (filter 2 5)
2: (scan "kv" [3 4])
3: (placeholder 1)
4: (placeholder 2)
5: (< 3 6)
6: (lit 3)
```

This data structure is an array of "classes": each line contains one
or more structural representations that all compute the same result.

Then the optimizations/rewrites would then simplify the memo in-place:

```
0: (project 2 [4 3])
1: --
2: (scan "kv" [3 4] spans: [/#-/2])
3: (placeholder 1)
4: (placeholder 2)
5: --
6: --
```

The remaining sub-sections provide just enough additional details to
clarify and motivate this example, without providing all the details
needed for a full SQL language.

## Definition of µSQL

We'll initially define µSQL as the following subset of SQL:

```
exp := 'select' sexp 'from' '(' exp ')' [ 'where' sexp ]
     | 'table' IDENT

sexp := 'exists (' exp ')'
     | LIT
     | IDENT
	 | sexp '=' sexp
	 | sexp '+' sexp
```

These are example valid queries:

```
table kv
select k from (table kv)
select k from (table kv) where k + 10 < v
select k from (table kv) where exists (select a from (table t) where a = v)
```

As you see µSQL at this stage only operates on single-column queries.
Further µSQL has just one type "integer" and uses integer as booleans
like in the C language for the purpose of deciding WHERE.

Also you notice that ̧µSQL doesn't do table aliases or prefixes, so we
can't use the same table in sub-queries and refer to the outer column
names to correlate. This is just to simplify the guide-level
explanation; µSQL does support correlation just well (see the last
example above) and there is not more complexity needed in the
implementation to support the name resolution rules of real SQL.

## Overview

Prior to this RFC, the code in CockroachDB would handle (µ)SQL as follows:

- the parser would transform an expression (`stmt`) into an AST, with
  two separate groups of AST node types: expressions (`Expr`), and
  statement (`Statement` and accompanying statement clauses).

- the planNode constructors would recurse into the statement AST node
  types to create a planNode tree; each planNode may then contain
  references to `Expr` nodes (ASTs for value expressions) mostly
  unchanged. Sub-queries in value expressions are not handled well.

This RFC proposes instead to operate as follows, at a high level:

- the parser would transform an expression into an AST, like before.
  Arguably, the rest of this RFC also enables simplifying this phase
  to use a single group of AST node types but this simplification is
  out of scope. It will be treated separately in the [IR
  RFC](https://github.com/cockroachdb/cockroach/pull/10055).

- a simple recursion would traverse the AST and create a new *fully
  independent* data structure akin to a tree conceptually, but which
  unlike planNodes and AST nodes does not from a tree in memory.

The innovation in this RFC is the second part, where we replace the
planNode hierarchy by something different.

## Representation of logical plans

A logical plan remains *conceptually* a tree. Both relational
expressions and scalar expressions are grouped together in the same
(conceptual) tree and we'll just use the word "expression" for both
henceforth.

The *semantics* of a logical plan (i.e. the meaning of its
interpretation during query execution) remains, as before, the result
of evaluating the top-level expression in the tree.

A logical plan is *represented* (in code, in memory) by:

- a single flat array of *nodes* that conceptually represent classes of expressions.
- an index into the top-level array that identifies the root expression (class).
- a class node is itself a data structure comprised of:
  - a member data structure that contains the *properties* of the class.
  - a (dynamically allocated) unordered multiset of one or more
    concrete *expression representations*.

The data structure has a principal invariant: *all the expression
representations inside a class have the same semantics.* That is, each
of the nodes in the top-level array defines an equivalency class (all
its member expression representations are semantically equivalent, and
they share the same properties).

The top-level array together with its item nodes is called a *memo*.

The new thing in this RFC is that the representation of an expression
is split (in code, in memory) into two places: one at the head of the class
node where its properties are encoded, and one in the representation
set where its structure (and, later, physical properties) is encoded.

## Expression representations

Before we look at examples we should first enumerate the possible
expression representations inside a class in µSQL:

- `(lit N)` to represent the literal value N
- `(+ x y)` where `x` and `y` are indexes into the memo.
- `(= x y)` where `x` and `y` are indexes into the memo.
- `(filter x y)` where `x` is an index into the memo that designates
  where the filter is getting data from, and `y` is an index into the
  memo that designates the predicate being computed for each row to
  decide whether the row is omitted.
- `(project x y)` where `x` is an index into the memo that designates
  where the projection is getting data from, and `y` is an index into
  the memo that designates the value being computed for each row.
- `(placeholder)`, which represents "the current row" in the
  relational context where it is evaluated. (In µSQL there is a single
  cell per row, so "the current row" is a scalar value. In the
  detailed guide below we'll have multiple numbered placeholders per
  row.)
- `(scan <name> x)` which extracts all the rows in the named table into
  the placeholder indexed by `x`.
- `(exists x)` which evaluates to non-zero iff the result of the evaluation
  of the expression indexed by `x` has more than zero rows.

### Examples

Let's take an first simple example:

```sql
select k+42 from (table kv)
```

Corresponding memo, with properties omitted:

```
root index: 0
0: (project 1 3)
1: (scan "kv" 2)
2: (placeholder)
3: (+ 2 4)
4: (lit 42)
```

We can conceptually visualize this memo as a tree by drawing the edges
between index references and the representations:

```
         (project)
          /     \
  (scan "kv")    (+)
       |        /   \
    (placehodler)  (lit 42)
```

Now, for a slightly more complex query with correlation:

```sql
select k from (table kv) where exists (select a from (table t) where a = v)
```

Memo, with properties omitted:

```
root index: 0
0: (filter 1 3)
1: (scan "kv" 2)
2: (placeholder)
3: (exists 4)
4: (filter 5 7)
5: (scan "t" 6)
6: (placeholder)
7: (= 2 6)
```

Or represented as a conceptual tree:

```
            (filter)
	         /    \
     .------'    (exists)
	 |               \
     |              (filter)
     |             /       \
 (scan "kv")   (scan "t")   '--.
	 |            |            |
 (placeholder) (placeholder)  (=)
	 \            \          /  |
	  \            '--------'   |
	   '------------------------'
```

## Early advantages

Supposing we can use a single Go type to encode all the various
expression representations (this is made possible by the [ADT
RFC](20170517_algebraic_data_types.md)), the memo can be highly
compact in memory and keep the number of allocations much lower than
currently.

### Where the magic happens, part 1: conjunctions

Suppose we extend µSQL with AND expressions and inequality
comparisons. Note how a new representation type `(and z)` appears. `z`
is an *integer set*, which we can conveniently encode as a bitmap.

For example:

```sql
select k from kv where k and k < 10 and k
```

Memo:

```
0: (project 1 3)
1: (scan "kv" 2)
2: (placeholder)
3: (and {2,4})
4: (and {2,5})
5: (< 2 6)
6: (lit 10)
```

Now here's where the magic happens: in the simplify phase, we can
always apply the rule `(and (and x) y) = (and (union x y))`. With the
integer sets represented as bitmaps, we just need to compute a bitwise
OR of the bitmaps. There is no memory traffic. And then the two
`(and)` representations in the example collapse to:

```
0: (project 1 3)
1: (scan "kv" 2)
2: (placeholder)
3: (and {2,5})
4: --
5: (< 2 6)
6: (lit 10)
```

This optimization is particularly important during predicate
push-down, as will be detailed later.

### Where the magic happens, part 2: multicolumn rel exprs

Suppose we extend µSQL to support multiple columns per relational expression.

Then:

- the representation type `(scan <name> r)` must be extended so
  `r` is an index list (to decide in which order the columns on disk are
  presented in the resulting placeholders)

- the representation type `(project x r)` is extended accordingly,
  with `r` an index list instead of a single index as previously. `x`
  is the source relational expression as previously.

Then:

```sql
select k, v from (table "kv")
```

Memo:

```
0: (project 1 [2 3])
1: (scan "kv" [2 3])
2: (placeholder @1)
3: (placeholder @2)
```

Another:

```sql
select v, k from (table "kv")
```

Memo:

```
0: (project 1 [3 2])
1: (scan "kv" [2 3])
2: (placeholder @1)
3: (placeholder @2)
```

Another:

```sql
select k+1,k+1 from (table "kv")
```

Memo:

```
0: (project 1 [4 4])
1: (scan "kv" [2 3])
2: (placeholder @1)
3: (placeholder @2)
4: (+ 2 5)
5: (lit 1)
```

See how common (sub-)expressions can be shared.

### Where the magic happens, part 3: stacked projections

Example:

```sql
select k, v from (select v, k from (table "kv"))
```

Memo:

```
0: (project 1 [3 4])
1: (project 2 [4 3])
2: (scan "kv" [3 4])
3: (placeholder @1)
4: (placeholder @2)
```

What's new?  **Every column item in an expression has a
global index in the memo, so relational expressions at different
levels can refer to the same classes in the memo without ambiguity.**

There is no need any more for the *contextual renumbering of
IndexedVar* as currently done in CockroachDB, and thus the associated
renumbering cost whenever an expression is migrated from one level to
another.

## Properties

The properties part on each node encode the following:

- the expression's type (used to make sense of the bytes in memory)
- if it is a relational expression:
  - its known orderings, as a set of lists (arrays) of memo positions (classes)
  - its key column set, as an integer set (bitmap) of memo positions (classes)
  - its column equivalency groups, as an integer set (bitmap) of memo positions (classes)
  - its *result columns* as an integer set (bitmap)
  - (later, during optimizations) its needed columns, as an integer set (bitmap) subset of the result columns
  - its *presentation order* as an integer list

### Result columns as properties are just a cache

The introduction of the result columns in the properties header of
each class is not fundamentally necessary: we could always recompute
them from the expression representation(s).

- for `(project)` and `(scan)` the result columns is the set of entries in the 2nd term in the expression representation
- for `(filter)` the result columns are the same as that of the first operand
- for each scalar expression the result column set is empty

However many optimizations benefit from having access to the result
columns in constant time. So we propose to cache it in each class.

### Presentation order as properties, and more magic

The *presentation order* in the properties header determines in which order
the result columns are presented in result rows, for relational expressions.

It is defined as follows: *the presentation order is an ordered list
of the values in the result column set*.

This is partly an optimization: without this we can use the `(project)` nodes
previously defined without loss of generality; the `(project)` and
`(scan)` nodes indicate the order explicitly via their 2nd parameter,
and `(filter)` simply preserves the order.

The reason why the presentation order is useful is to simplify
the handling of successive `(project)` nodes, and (later) the handling
of joins and UNION with mis-aligned columns.

Also, and that's where it's not strictly an optimization, "hidden"
columns can be encoded by columns present in the result columns set
but not in the presentation order list. This removes the need for the
corresponding `Hidden` boolean from the current CockroachDB source
code.

For example, whereas the explanation above was proposing:

```
select k, v from (table kv)         select v, k from (table kv)

0: (project 1 [2 3])                0: (project 1 [3 2])
1: (scan "kv" [2 3])                1: (scan "kv" [2 3])
2: (placeholder @1);k               2: (placeholder @1);k
3: (placeholder @2);v               3: (placeholder @2);v
```

What we'd really be doing looks more like this:

```
select k, v from (table kv)       select v,k from (table kv)

0: props:{                        0: props:{
    rez: {1,2,3}                      rez: {1,2,3}
    prez: [1 2]                       prez: [2 1]
   }                                 }
   reprs: (scan "kv" {1,2,3})        reprs: (scan "kv" {1,2,3})
1: reprs: (placeholder @1);k      1: reprs: (placeholder @1);k
2: reprs: (placeholder @2);v      2: reprs: (placeholder @2);v
3: reprs: (placeholder @3);rowid  3: reprs: (placeholder @3);rowid
```

i.e. we tack a simplified projection in the properties header of every
class. The dedicated `(project)` expression is then only needed when
it *computes* things using expressions that are not just placeholders.

This in turn can be used for the following optimizations:

- two stacked `(project)` nodes where the top one merely reuses the columns
  produced by the bottom one can be collapsed by simply eliminating the bottom one,
  there is no rewrite needed.
- immediate `(project)` children of a `(join)` (defined below) can be simply
  ignored/eliminated, because joins are agnostic to the presentation order
  of their operands.

### Some more examples

TODO: The following examples should be detailed here.

- multiple examples for joins
- examples for UNION
- examples for GROUP BY, ORDER BY
- examples for window functions
- examples for special columns in joins:

```sql
select k, a.k, b.k from (kv a natural inner join kv b)
-- complexity: unqualified "k" is equivalent to "a.k"

select k, a.k, b.k from (kv a natural left  join kv b)
-- complexity: unqualified "k" is coalesce(a.k, b.k)
```

Also some examples of the trivial rewrites for  decorrelation of simple correlated subqueries (pattern: filter - exists - subquery).


## Glossary

| Prior to this RFC  | After this RFC                                       |
|--------------------|------------------------------------------------------|
| Value expression   | Value or scalar expression                           |
| planNode tree      | relational expression                                |
| planNode           | expression node                                      |
| "front-end" = parsing + name resolution + type checking + normalization | "front-end" = parsing + preparation |
| N/A                | preparation or "prep" = name resolution + type checking
| "middle-end" = construction of logical plan + always-beneficial rewrites + plan expansion | "middle-end" = rewrite + search |
| N/A                | rewrite = normalization + always-beneficial rewrites |
| plan expansion     | search = enumeration of candidates                   |

# Reference-level explanation

This particular RFC proposes to:

1) keep the current AST unchanged;
2) keep the planNode data structures, to carry semantic information not encoded in the memo
   structure, to support local execution, and as intermediate representation beyond
   the memo as input to the distSQL physical planner.
3) discourage further work on optimizations that manipulate the planNode structures
   directly, and redirect effort on optimization using the memo structure instead.

The transformation steps would thus be:

```
+-------+
| parse |
+---+---+
    |
  (ast)
    |
	v
+------------------------------------------+
| decide what to do: cur code or new code? |
+---+----------------------------------+---+
    | [use new]                        | [use cur]
  (ast)                              (ast)
	|                                  |
	v                                  v
+-------+ [failure]			 +----------------------------+
| prep  +------------------->| compile using current code |
+---+---+				   	 +---------+------------------+
    |								   |
  (memo)							 (planNode)
    |								   |
	v								   |
+---------------------------------+	   |
| rewrite (mashup of pieces of our|    |
|          current expansion code)|    |
+---+-----------------------------+	   |
    |								   |
  (memo)							   |
    |								   |
	v								   |
+---------------------------------+	   |
| search (mashup of pieces of our |	   |
|         current expansion code  |	   |
|         +, incrementally, new)  |	   |
+---+-----------------------------+	   |
    |								   |
  (memo)							   |
    |								   |
	v								   |
+--------------------+				   |
| translate (compat) |				   |
+---+----------------+				   |
    |								   |
  (planNode, simplified)			   |
    |								   |
	| .--------------------------------'
	|/
	|
	|
	+
	v
+---------------------+
| distSQL plan tester |
+---+-----------------+
	|\
	| '----------------------.
	|                        |
    v                        v
+-----------------+    +-----------+
| local execution |    | distSQL   |
+-----------------+    +-----------+
```

The phase "decide what to do" would be initially simply a test
on a session variable, with a default taken from a cluster setting
`experimental.sql.memo.enabled`. This is to be eliminated
in the future when the new structure is proven to be
as general and more efficient than the current code.

Also, we foresee that the new data structures may not be able to
encode all possible ASTs currently accepted by/valid in CockroachDB.
In the case the "prep" phase fails to compile an AST into the memo,
the logical planning can fall back to the current code.

The work to eliminate the "compatibility" phase where the memo is
converted back to planNode for execution is, arguably, also necessary
for best performance but placed out of scope of this RFC and deferred
to a later phase to 1) focus the discussion 2) enable an intermediate
phase where both code paths co-exist, which will facilitate
verification.

Meanwhile, note however that the architecture presented here will enable
optimizations that are not currently feasible using the current
code. It may well be that despite this inefficiency, we can activate
this new code path for particular "known-good" queries or apps where
the overall simplification of the query plan will pay off the overhead
of this additional transform.

Finally, the work to merge local and distSQL execution engines is also
out of scope and can be (should be!) performed fully orthogonally to
the work in this RFC.

## Detailed design

Peter is working on a prototype at http://github.com/petermattis/opttoy.

More prototypes / experimentations upcoming to refine this RFC further.

### Top-level data structures

In Go:

```
type memo struct {
  root    int
  classes []class
}

type class struct {
  props properties
  exprs []Expr
}

type properties struct {
  typ  Type // probably our current `parser.Type` as-is
  relprops
}

type relprops struct {
  columns       intset       // probably some bitmap struct
  presentation  []int        // if nil, natural order of `columns`
  neededColumns intset       // to elide values at execution
  ordering      orderingInfo // from Radu
}

type Expr .... // (see below)
```

### Expression representations

The `Expr` type is to be auto-generated using a higher-level definition, using the code
already present in `pkg/sql/ir`:

```
sum Expr = 
  | Placeholder
  | Literal
  | Project
  | Filter
  | Scan
  | ...

struct Placeholder { int colnum }
struct Literal { ... TBD ... }
struct Project { int src; int[] exprs }
struct Filter  { int src; int filter }
struct Scan    { int tableID; int[] columns }
...
```

The generated code will ensure a single Go type is used "behind the
scene" and can be allocated in slices to store in the memo's classes.

Note how each of the representation types does not use direct memory
references to another node, and instead uses `int` fields to refer
(implicitly) to positions in the memo.

The full definition of the expression representation language is out
of scope for this RFC, and instead will be specified incrementally in
the [IR RFC](https://github.com/cockroachdb/cockroach/pull/10055) over
time so as to support more and more SQL ASTs until the full SQL
dialect of CockroachDB is supported.

### Algorithms for the "prep" phase

Typing of expressions in the AST does not change in this RFC.

However name resolution and typing are not any more a separate
recursion from the planNode construction. Instead name resolution,
typing and the construction of the memo happen in a single recursive
algorithm.

Name resolution further uses a *scope environment data structure*, as
in state-of-the-art compilers. The structure is a linked list of
associative arrays, where each associative array maps "known names at
that level" to positions in the memo where that name is defined.  For
details refer to the book "Modern Compiler Implementation" by A. Appel
of similar textbooks on compiler constructions.

### Algorithms for the "rewrite" phase

This corresponds to the always-beneficial transforms of the current
"plan optimization" code:

- determining needed columns.

  This is greatly simplified by the bitmaps; needed columns
  are simply a depth-first traversal of the memo
  with OR of the needed columns at each level on the returning
  edge of the traversal.

- computing orderings (column equivalency classes, key column groups, etc).

  Current code by Radu, mostly unchanged. Already uses bitmaps.
  
- predicate push-down.
  
  This is greatly simplified by the bitmaps again. 
  
  - conjunctions can be merged transparently by ORing their needed
    column sets and ORing their AND argument sets, without allocating
    new expression representations.

  - the memo index of a predicate expression at one level can be
    copied to another level transparently as long as its needed
    columns are also available in the resulting columns of the other
    level (which one can determine with a bitwise AND of the intsets),
    without any expression rewriting.

### First "search phase" implementation

Initially this would contain just a couple of rules applied
deterministically and unconditionally, exactly those dealing with
index selection in `expand_plan.go`.

### Action items - summary

The following new things should be implemented:

- new data structures for the memo, classes, expression representations
  - the properties header can reuse existing structs designed by Radu
    for planNodes
- construct a new Prep phase algorithm that merges type checking, name resolution and memo construction.
- adapt the `expandPlan` recursions to the memo
- new transform from memo to planNode tree
- new conditional / code path in `makePlan()`
  - new cluster setting `experimental.sql.memo.enabled`

## Drawbacks

None known.

## Rationale and Alternatives

TBD

## Unresolved questions

- encoding of literals
