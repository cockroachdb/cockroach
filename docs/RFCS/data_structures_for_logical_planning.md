- Feature Name: Data structures for SQL logical plans
- Status: draft
- Start Date: 2010-10-12
- Authors: knz, Peter Mattis
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC *outlines* new data structures and principles around the
handling of SQL logical plans, that both 1) lower the cost of managing
logical plans in the current architecture 2) facilitate further work
on SQL optimizations. It also proposes a *path to the introduction* of
these data structures in the current CockroachDB source base.

It is complementary to the separate [RFC: SQL Logical
planning](sql_query_planning.md) which
provides both context, additional motivations and an overarching
story. The reader is invited to look at that other RFC (as well as
other documents listed in the Context section below) for the
background story.

The new data structures are:

- *classes* to represent nodes in the SQL expression tree, where a
  single class can contain multiple alternative equivalent expressions
  that share a common set of properties, and where individual
  expressions refer to entire classes as children/siblings rather than
  other expressions.
- a *memo* data structure that stores all the classes. The name
  "memo" is used because it memoizes the results of analyzing/generating
  alternative plans for a single query.

The proposed path is to make this new code co-exist with the current
code initially, with a plan to replace the current code completely in
the longer term.

In comparison with the current code in CockroachDB, the main change is
the introduction of expression classes, so that expressions do not
form a "textbook" tree in memory any more.

The scope of this RFC is restricted to:

- give an overall idea to the human reader of how a memo works and
  motivate its choice, so as to enable more team members to
  participate in the exploratory effort.
- present a path to integrate a memo structure into the current
  CockroachDB code base.

In particular, this RFC does not intend to detail the particular
algorithms that will be used to populate the memo and search for
optimal queries, nor does it intend to fully specify the set of
logical and physical properties of a class of expressions.

# Context of this document

This RFC is based on the following prior work:

- [Tech note: The SQL layer in CockroachDB](../tech-notes/sql.md)
- [Tech note: Guiding principles for the SQL middle-end and back-end in CockroachDB](../tech-nodes/sql-principles.md)
- [Tech note / meta-RFC: Upcoming SQL changes / Evolving the SQL layer in CockroachDB](https://github.com/cockroachdb/cockroach/pull/18977)
- [RFC: SQL Logical planning](sql_query_planning.md) in particular the terminology
  set forth there, for example the "prep", "rewrite" and "search" phases.
- [RFC: Algebraic Data Types in Go](20170517_algebraic_data_types.md) because we're going to reuse the mechanisms there.

The readers are encouraged to read these prior work tech notes and
RFCs, in particular the RFC on logical planning by Peter, and make
their own experiments in code to accompany their understanding of the
concepts presented in this RFC.

As of this writing, the following code experiments can be used to
accompany the reading in this RFC:

- bare-bones, introductory code with many comments by knz: http://github.com/knz/sqlexp
- a more advanced prototype in Go by Peter: http://github.com/petermattis/opttoy

In the plan set forward with "upcoming SQL changes" the present RFC
corresponds to the items:

- SQL leaf data
- Introduce scoping and free variables in name resolution
- Apply relational operator
- Plan equivalency classes
- Make plans stateless / context-free

i.e. acting on this RFC will move forward on all these 5 items at
once, and complete all of them except the first.

Finally, this RFC is not *based* on the [IR
RFC](https://github.com/cockroachdb/cockroach/pull/10055) but will
instead serve as new *prelude* to it. The text in the IR RFC that
pertains to this one will be migrated here, and the remaining text
will be later updated to reflect the choices made here.

# Motivation

This RFC is motivated by:

1) a need for new data structures to reduce various overheads of the
   current technology, in particular the following should be
   eliminated, in decreasing order of importance:

   - the preparation of execution-related data structures in
     `planNodes` that are only useful for local execution,
     when the query will end up being distributed anyway.
   - 2-3 different tree traversals to properly
     handle name resolution and scalar subquery analysis.
   - the renumbering of IndexedVar nodes when transforming plans
   - linear/quadratic loops over the list of columns on both
     sides of a join or union to "match up" the columns
   - the use of the Go heap allocator to handle intermediate
     new nodes in a plan during transformations.

2) a desire to align the architecture of CockroachDB with the state-of-the-art,
   which will ease the adoption of algorithms and ideas from the literature.
3) a need to set up the scene for multiple other RFCs to come.
4) a need to aid the reading of various prototypes
   developed in the team to experiment with these concepts.

# Guide-level explanation

## Glossary

| Prior to this RFC  | After this RFC                                       |
|--------------------|------------------------------------------------------|
| Value expression   | Value or scalar expression                           |
| planNode tree      | relational expression                                |
| planNode           | memo-expression or just "expression"                 |
| render             | project                                              |
| expression ref     | memo index                                           |
| "front-end" = parsing + name resolution + type checking + normalization | "front-end" = parsing + preparation |
| N/A                | preparation or "prep" = name resolution + type checking
| "middle-end" = construction of logical plan + always-beneficial rewrites + plan expansion | "middle-end" = rewrite + search |
| N/A                | rewrite = normalization + always-beneficial rewrites |
| plan expansion     | search = enumeration of candidates                   |

## Example query handling, prior to this RFC

Prior to this RFC, CockroachDB would take as input a query like:

```sql
select v, k from kv where k < 3
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
branches. For example, the whole `(BinExpr ...)` sub-expression is
embedded as the `filter` member of the filterNode (with the unresolved
name replaced by an IndexedVar, as a result of name resolution).

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

In this implementation, we have a strong conceptual separation between
"logical plan nodes" (with nodes implementing the `planNode`
interface) and "SQL expression nodes" (implementing the `parser.Expr`
interface).

Also, the logical plan nodes perform double-duty as physical plan
nodes. For example, `scanNode` is populated with buffers to facilitate
physical execution, although these structures aren't necessary during
planning.

## Example query handling, after this RFC

The same query initially would be transformed to the same AST as
previously; that part doesn't change. However the first thing
happening afterwards is the translation of that AST into a fully
independent data structure [*]:

```
0: (project 1 [4 3])
1: (filter 2 5)
2: (scan "kv" [3 4])
3: (var "kv.k")
4: (var "kv.v")
5: (< 3 6)
6: (literal "3")
```

This data structure is an array of *classes*: each line contains one
or more structural representations, called memo-expressions, that all
compute the same result. Initially (during the "prep" phase), there
is just one memo-expression, as demonstrated in the example above.

Then the optimizations/rewrites would then simplify the memo, in-place
for rewrites, and adding more classes and memo-expressions per class
during search.

```
After rewrite (with predicate push-down):

0: (project 2 [4 3])
1: -- unreferenced
2: (scan "kv" [3 4])     prop: filter k < 3
3: (var "kv.k")
4: (var "kv.v")
5: -- unreferenced
6: -- unreferenced

After search:

0: (project 7 [4 3]) -- the variant (project 2 [4 3]) was eliminated
1: -- unreferenced
2: -- unreferenced
3: (var "kv.k")
4: (var "kv.v")
5: -- unreferenced
6: -- unreferenced
7: (scan "kv" [3 4] spans: [/#-/2])
```

The remaining sub-sections provide just enough additional details to
clarify and motivate this example, without providing all the details
needed for a full SQL language.

[*] Caveat: there's a discussion still open about whether we convert
from the AST directly to a memo-like structure, or whether there will
be an intermediate relational algebra tree in-between. As of this writing [this
discussion is still open in the overarching
RFC](sql_query_planning.md). Peter
suggests we may want to do this, whereas I (knz) think we don't, to
save time and memory. There is no clear evidence in favor of either at
this time.


## General principles

Prior to this RFC, the code in CockroachDB would handle SQL as follows:

- the parser would transform an expression (`stmt`) into an AST, with
  two separate groups of AST node types: expressions (`Expr`), and
  statement (`Statement` and accompanying statement clauses).

- the `planNode` constructors would recurse into the statement AST node
  types to create a planNode tree; each planNode may then contain
  references to `Expr` nodes (ASTs for value expressions) mostly
  unchanged. Sub-queries in value expressions are not handled well.

This RFC proposes instead to operate as follows, at a high level:

- the parser would transform an expression into an AST, like before.

  (Arguably, the rest of this RFC also enables simplifying this phase
  to use a single group of AST node types but this simplification is
  out of scope. It will be treated separately
  in the [IR RFC](https://github.com/cockroachdb/cockroach/pull/10055).)

- a simple recursion would traverse the AST and create a new *fully
  independent* data structure akin to a tree conceptually, but which
  unlike planNodes and AST nodes does not form a tree in memory.

The innovation in this RFC is the second part, where we replace the
planNode hierarchy by something different, a memo.

(As outlined in the strategy section below, initially this code
would exist side-by-side with the planNode hierarchy, but would
*eventually* replace it.)

## Representation of logical plans

A logical plan remains *conceptually* mostly a tree (although we'll
also keep links between variable definitions and their uses, so the
full actual structure will actually be a DAG). Both relational
expressions and scalar expressions are grouped together in the same
(conceptual) tree/graph and we'll just use the word "expression" for both
henceforth.

The *semantics* of a logical plan (i.e. the meaning of its
interpretation during query execution)
remains, as before, the result of evaluating the top-level expression.

A logical plan (or, more precisely, the set of all equivalent plans
that will be considered during optimizations) is *represented* (in
code, in memory) by:

- a single flat array of items that conceptually represent *classes* of expressions.
- an index into the top-level array that identifies the root expression (class).
- a class is itself a data structure comprised of:
  - a member data structure that contains the *logical properties* of the class.
  - a (dynamically allocated) unordered multiset of one or more
    concrete expression representations, called "memo-expressions".
    You can think about an memo-expression as a particular strategy
    to compute the results.

The data structure has a principal invariant: *all the
memo-expressions inside a class have the same semantics* with
"semantics" defined above: they compute the "same results" for the
query, where sameness is defined by what is allowable for the SQL
query text initially provided (e.g. two results are the "same" if they
differ only by their ordering and no ORDER BY clause was provided).

We attach *logical properties* to entire classes, *physical
properties* to individual memo-expressions. [Properties are defined
further in a separate RFC](sql_plan_properties.md).

The top-level array together with its item nodes is called a *memo*.

The new thing in this RFC is that the representation of an expression
is split (in code, in memory) into two places: one at the head of the class
node where its properties are encoded, and one in the representation
set where its structure (and, later, physical properties) is encoded.

## Expression representations

Before we look at examples we can enumerate the possible memo-expression
types we'll need for them:

- `(literal "N")` to represent the literal value N.
- `(+ x y)` where `x` and `y` are indexes into the memo.
- `(= x y)` where `x` and `y` are indexes into the memo.
- `(filter x y)` where `x` is an index into the memo that designates
  where the filter is getting data from, and `y` is an index into the
  memo that designates the predicate being computed for each row to
  decide whether the row is omitted.
- `(project x y)` where `x` is an index into the memo that designates
  where the projection is getting data from, and `y` is an index into
  the memo that designates the value being computed for each row.
- `(var "name")`, which represents the value in column `name` in
  the current row in the relational context where it is evaluated.
- `(scan <name> x)` which extracts all the rows in the named table into
  the var(s) indexed by `x`.
- `(exists x)` which evaluates to non-zero iff the result of the evaluation
  of the expression indexed by `x` has more than zero rows.

### Examples

Let's take an first simple example:

```sql
select x+42 from foo
```

Corresponding memo, with properties omitted:

```
root index: 0
0: (project 1 3)
1: (scan "foo" 2)
2: (var "x")
3: (+ 2 4)
4: (literal "42")
```

Remember, `(project x y)` means "return the value of the expression `y`
for every row in the result of expression `x`".

We can conceptually visualize this memo as an expression tree by
drawing the edges between index references and the representations;
there are some non-tree additional edges between variable definitions
and uses:

```
         (project)
          /     \
  (scan "foo")    (+)
       |        /   \
    (var "x")--'   (lit 42)
```



Now, for a slightly more complex query with correlation:

```sql
select x from foo where exists (select a from t where a = x)
```

Memo, with properties omitted:

```
root index: 0
0: (filter 1 3)
1: (scan "foo" 2)
2: (var "x")
3: (exists 4)
4: (filter 5 7)
5: (scan "t" 6)
6: (var "a")
7: (= 2 6)
```

Or represented as a conceptual expression tree with additional
non-tree edges between var def and use:

```
            (filter)
             /    \
     .------'    (exists)
     |               \
     |              (filter)
     |             /       \
 (scan "foo")  (scan "t")   '--.
     |            |            |
  (var "x")    (var "a")      (=)
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

Suppose we also consider AND expressions and (in)equality
comparisons. 

We'll encode a conjunction of the form `a AND b AND c AND d ...` as a
single node `(and z)` where `z` is the set of memo indexes of the
member expressions, which we can conveniently encode as a bitmap.

For example:

```sql
select x from foo where x != 0 and x < 10 and x != 0
```

Memo:

```
0: (project 1 3)
1: (scan "foo" 2)
2: (var "x")
3: (and {7,4})
4: (and {7,5})
5: (< 2 6)
6: (literal "10")
7: (!= 2 8)
8: (literal "0")
```

Now here's where the magic happens: in the simplify phase, we can
always apply the rule:

```
SQL:       (a AND b) AND c    = a AND (b AND c)    = a AND b AND c
algebraic: (and {(and {a,b}), c}) = (and {a, (and {b,c})}) = (and (union {a} {b,c}))
```

With the integer sets represented as bitmaps, we just need to
compute a bitwise OR of the bitmaps. There is no memory traffic. And
then the two `(and)` representations in the example collapse to:

```
0: (project 1 3)
1: (scan "foo" 2)
2: (var "x")
3: (and {7,5}) (and {7,4}) -- the two memo-expressions are equivalent
4: (and {7,5})
5: (< 2 6)
6: (literal "10")
7: (= 2 8)
8: (literal "0")
```

This optimization is particularly important during predicate
push-down, as will be detailed later.

### Where the magic happens, part 2: multicolumn rel exprs

Suppose we also consider multicolumn queries.

Then:

- the representation type `(scan <name> r)` must be extended so
  `r` is an index list (to decide in which order the columns on disk are
  presented in the resulting placeholders)

- the representation type `(project x r)` is extended accordingly,
  with `r` an index list instead of a single index as previously. `x`
  is the source relational expression as previously.

Then:

```sql
select k, v from kv
```

Memo:

```
0: (project 1 [2 3])
1: (scan "kv" [2 3])
2: (var "k")
3: (var "v")
```

Another:

```sql
select v, k from kv
```

Memo:

```
0: (project 1 [3 2])
1: (scan "kv" [2 3])
2: (var "k")
3: (var "v")
```

Another:

```sql
select k+1,k+1 from kv
```

Memo:

```
0: (project 1 [4 4])
1: (scan "kv" [2 3])
2: (var "k")
3: (var "v")
4: (+ 2 5)
5: (literal "1")
```

See how common (sub-)expressions can be shared.

### Where the magic happens, part 3: stacked projections

Example:

```sql
select k, v from (select v, k from kv)
```

Memo:

```
0: (project 1 [3 4])
1: (project 2 [4 3])
2: (scan "kv" [3 4])
3: (var "k")
4: (var "v")
```

What's new?  **Every sub-expression, including variables (and later
subqueries), in an expression has a global index in the memo, so
relational expressions that depend on them at different scoping levels
of the query can refer to the same classes in the memo without
ambiguity.**

There is no need any more for the *contextual renumbering of
IndexedVar* as currently done in CockroachDB, and this thus eliminates
the associated renumbering cost whenever an expression is migrated
from one level to another.

## Properties

This RFC does not propose to fix the set of logical properties that
need to be maintained. This set will likely evolve over time: the
properties that need to be defined are exactly those necessary to
support SQL optimizations and the preparation of an execution plan --
we will not want to add properties arbitrarily unless/until they are
actually needed.

[A separate RFC draws an inventory of the properties being
considered.](sql_plan_properties.md)

### Result columns as properties are just a cache

The introduction of the result columns in the properties header of
each class is not fundamentally necessary: we could always recompute
them from the expression representation(s).

For example:

- for `(project)` and `(scan)` the result columns is the set of
  entries in the 2nd term in the expression representation;
- for `(filter)` the result columns are the same as that of the first
  operand.

However many optimizations benefit from having access to the result
columns in constant time. So we propose to cache it in each class.

### Presentation order as properties, and more magic

The *presentation order* in the properties header determines in which order
the result columns are presented in result rows, for relational expressions.

It is defined as follows: *the presentation order is an ordered list
of the values in the result column set*.

This is partly an optimization: without this we can use the
`(project)` nodes previously defined without loss of generality; the
`(project)` and `(scan)` nodes indicate the order explicitly via their
2nd parameter, and `(filter)` simply preserves the order.

The reason why the presentation order is useful is to simplify
the handling of successive `(project)` nodes, and (later) the handling
of joins and UNION with mis-aligned columns.

For example, whereas the explanation above was proposing:

```
select k, v from kv       select v, k from kv

0: (project 1 [2 3])      0: (project 1 [3 2])
1: (scan "kv" [2 3])      1: (scan "kv" [2 3])
2: (var "k")              2: (var "k")
3: (var "v")              3: (var "v")
```

What we'd really be doing looks more like this:

```
select k, v from kv           select v,k from kv

0: props:                     0: props:
    outs: {1,2}                   outs: {1,2}
    cols: [1 2]                   prez: [2 1]
   reprs: (scan "kv" {1,2})      reprs: (scan "kv" {1,2})
1: reprs: (var "k")           1: reprs: (var "k")
2: reprs: (var "v")           2: reprs: (var "v")
```

i.e. we tack a simplified projection in the properties header of every
class. The dedicated `(project)` expression is then only needed when
it *computes* things using expressions that are not just placeholders.

This in turn can be used for the following optimizations:

- two stacked `(project)` nodes where the top one merely reuses the
  columns produced by the bottom one can be collapsed by simply
  eliminating the bottom one, there is no rewrite needed.
- immediate `(project)` children of a `(join)` (defined below) can be
  simply ignored/eliminated, because joins are agnostic to the
  presentation order of their operands.

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

Also some examples of the trivial rewrites for decorrelation of simple
correlated subqueries (pattern: filter - exists - subquery).

## Strategy for evolving the current code base

This RFC proposes to:

1) keep the current AST unchanged (for the time being);
2) keep the `planNode` data structures, to carry semantic information not encoded in the memo
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
+-------+ [failure]          +----------------------------+
| prep  +------------------->| compile using current code |
+---+---+                    +---------+------------------+
    |                                  |
  (memo)                             (planNode)
    |                                  |
    v                                  |
+---------------------------------+    |
| rewrite (mashup of pieces of our|    |
|          current expansion code)|    |
+---+-----------------------------+    |
    |                                  |
  (memo)                               |
    |                                  |
    v                                  |
+---------------------------------+    |
| search (mashup of pieces of our |    |
|         current expansion code  |    |
|         +, incrementally, new)  |    |
+---+-----------------------------+    |
    |                                  |
  (memo)                               |
    |                                  |
    v                                  |
+--------------------+                 |
| translate (compat) |                 |
+---+----------------+                 |
    |                                  |
  (planNode, simplified)               |
    |                                  |
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

The phase "decide what to do" would be initially a test on a
session variable, with a default taken from a cluster setting
`experimental.sql.memo.enabled`. This is to be eliminated in the
future when the new structure is proven to be as general and more
efficient than the current code.

Also, in the short term it is infeasible to support all of the
existing ASTs currently accepted by/valid in CockroachDB.  In the case
the "prep" phase fails to compile an AST into the memo, the logical
planning can fall back to the current code. (The medium to long term
goal is to completely remove the current planNode generation code
paths; this will be discussed in a subsequent RFC.)

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

# Reference-level explanation

As explained at the start of this RFC, at this point we are merely
"setting the scene" for further exploratory work. It is not the goal
of this RFC to answer all the questions necessary to achieve a
working implementation, but rather to introduce a set of concepts that
should be common to the various experiments.

In particular, this RFC proposes to commit to the following:

- using classes to identify expressions as opposed to memory references
  to a single expression structure.

- grouping multiple equivalent expressions into a class, storing its
  properties just once for the whole class.

- storing all the classes in a single data structure, called a "memo".
  (There may be one or two memos in the implementation, depending on
  whether we share one for both scalar and relational expressions, or
  separate memos. This distinction is inconsequential however to
  reason about it - two separate memos can be thought as just one with
  an extended index space.)

- using the integer index of a class in the memo as a reference to
  an expression from other classes and memo-expressions, as opposed to a
  reference in memory like in the current CockroachDB code.

- not changing the semantics of a class after it has been attributed
  an index in the memo.

- using the fact that all expressions have different indexes in the
  memo by using fast integer sets (bitmaps) to represent sets of
  expressions, columns, dependencies, etc.

In addition, this RFC proposes a concrete introduction plan for these
data structure in CockroachDB, outlined in the guide-level section
above already.

## Detailed design

Again, this RFC is not proposing to commit to particular implementations, but
rather to the concepts.

The reader can satisfy themselves that the concepts are implementable
by referring to the following prototypes, available at the time of
this writing:

- "opttoy" from Peter, at http://github.com/petermattis/opttoy

  This (Go) experiment is focused on *using* these data structures in prototypes
  of possible SQL optimizations. The emphasis is on completeness
  and analysing of which properties will be useful in CockroachDB.

- "sqlexp" from knz, at http://github.com/knz/sqlexp

  This (Python) experiment is focused on *illustrating* these data
  structures and how to organize code around them. The emphasis is on
  transparency (lots of comments, simple code) and revealing the
  essence of the ideas we'll be working with.

The reader is encourage to explore these concepts further in
additional prototypes, and modify this RFC to add links to them.

### Top-level data structures

Example representation in Go (again, this might change in the future):

```go
type memo struct {
  root    int
  classes []class
}

type class struct {
  props properties
  exprs []mexpr
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

type mexpr .... // (see below)
```

### Memo-expressions

The `mexpr` type is really a sum type. Further experiments
are needed to determine the particular definition that will
enable the least churn in Go's runtime.

One possible avenue is to use the compact IR node generator already
present in `pkg/sql/ir`, from a high-level definition like this one:

```
sum MExpr =
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

(Notice again how each of the representation types does not use direct
memory references to another node, and instead uses `int` fields to
refer (implicitly) to positions in the memo.)

The full definition of the memo-expression type is out of scope for this
RFC. This RFC merely assumes that the implementation(s) that will come
in accordance to the plans presented here will propose their own set
of memo-expression types and document them thorougly.

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
or similar textbooks on compiler construction.

The current code for type checking and name resolution can be reused
as-is; the following changes are needed to reuse them both in the
current code path and a recursive function for memo construction:

- the `SemaContext` data structure must be extended to carry a reference
  to the planner.
- `TypeCheck` on unresolved names must use the planner to resolve
  the name into an Indexed Var at that point. (currently it panics,
  asserting that there are no more unresolved names at the time of type checking)
- `TypeCheck` on unsubstituted subqueries in scalar context
  must use the planner to resolve the subquery at that point.
  (currently it panics, asserting there are no more AST subqueries
  at the time of type checking)

The latter two new code paths need not be visible in the current planNode
construction path that replaces unresolved names and sub-queries
by IndexedVars and `sql.subquery` object, respectively. However,
once we have implemented them, the current code would benefit
from that change too by avoiding a separate recursion.

### Algorithms for the "rewrite" phase

This corresponds to the always-beneficial, cost-agnostic transforms of
the current "plan optimization" code:

- determining needed columns.

  This is greatly simplified by the bitmaps; needed columns
  are simply a depth-first traversal of the memo
  with OR of the needed columns at each level on the returning
  edge of the traversal.

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

- pre-computing orderings (column equivalency classes, key column groups, etc).

  Current code by Radu, mostly unchanged. Already uses bitmaps.

  This property is also re-computed during the search phase for every
  new alternative plan enumerated. However it can be pre-populated for
  all the freshly created nodes during the rewrite phase.

Other cost-agnostic transforms may be included here, for example
decorrelation/unnesting, join elimination, apply elimination, etc. The
precise set of transforms targeted by rewrite is not in scope for this
RFC.


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

None.
