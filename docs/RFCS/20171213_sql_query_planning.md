- Feature Name: SQL Query Planning
- Status: in-progress
- Start Date: 2017-12-13
- Authors: Peter Mattis
- RFC PR: #19135
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC sketches the outlines of the high-level modules of a SQL
query planning including a full-featured optimizer.

# Motivation

SQL query planning is concerned with transforming the AST of a SQL
query into a physical query plan for execution. Naive execution of a
SQL query can be prohibitively expensive, because SQL specifies the
desired results and not how to achieve them. A given SQL query can
have thousands of alternate query plans with vastly different
execution times. The techniques used to generate and select a good
query plan involve significant engineering challenges.

This RFC is intended to provide guidance for both short term and long
term work on the SQL optimizer, and highlight areas of the current
system that will need to evolve.

# Guide-level explanation

## Overview

SQL query planning is often described in terms of 8 modules:

1. [Stats](#stats)
2. [Prep](#prep)
3. [Rewrite](#rewrite)
4. [Memo](#memo)
5. [Cost Model](#cost-model)
6. [Search](#search-aka-enumeration)
7. [Properties](#properties)
8. [Transformations](#transformations)

Note that Stats, Cost Model, Memo, Properties and Transformations
could be considered modules, while Prep, Rewrite and Search could be
considered phases, though we'll refer to all 8 uniformly as modules in
this document. Memo is a technique for compactly representing the
forest of trees generated during Search. Stats, Properties, Cost Model
and Transformations are modules that power Prep, Rewrite and Search.

```
                   SQL query text
                         |
                   +-----v-----+
                   |   Parse   |
                   +-----+-----+
                         |
                       (ast)
                         |
     +-------+     +-----v-----+  - constant folding, type checking, name resolution
     | Stats +----->   Prep    |  - computes initial properties
     +-------+     +-----+-----+  - retrieves and attaches stats
                         |        - done once per PREPARE
                      (expr)
                         |
                   +-----v-----+  - capture placeholder values / timestamps
                +-->  Rewrite  |  - cost-agnostic transformations, eg. predicate push-down
+------------+  |  +-----+-----+  - done once per EXECUTE
| Transforms +--+        |
+------------+  |     (expr)
                |        |
                +-->-----v-----+  - cost-based transformations
+------------+     |  Search   |  - finds lowest cost physical plan
| Cost Model +----->-----+-----+  - includes DistSQL physical planning
+------------+           |
                  (physical plan)
                         |
                   +-----v-----+
                   | Execution |
                   +-----------+
```


CockroachDB already has implementations of portions of these modules
except for Stats and Memo. For example, CockroachDB performs name
resolution and type checking which is part of Prep, and performs
predicate push down through joins which traditionally happens during
Rewrite. CockroachDB utilizes a primitive Cost model during index
selection (a portion of Search) to choose which index to use based on
filters and desired ordering.

In addition to the 8 modules, another aspect of the optimizer that
needs discussion is [Testing](#testing) and test infrastructure.

Lastly, a strawman [Roadmap](#roadmap) is proposed for how to break up
this work over the next several releases.

## Glossary

The following terms are introduced/defined in this RFC:

- [**algebraic equivalence**](#properties)
- [**attributes** of expressions](#properties-vs-attributes)
- [**cardinality**](#stats)
- [**decorrelating**](#rewrite), syn. "unnesting"
- [**derived** vs **required** properties](#properties)
- [**enforcer** operator for properties](#properties)
- [**equivalence class**](#memo)
- [**exploration** vs **implementation** transformations](#search)
- [**expressions** in queries](#prep)
- [**functional dependencies**](#prep)
- [**logical** vs **physical** properties](#memo)
- [**logical** vs **physical** vs **scalar** operators](#prep)
- [**memo-expressions**](#memo)
- [**operator** in query expressions](#prep)
- [**pattern** in transformations](#memo)
- [**predicate push-down**](#rewrite)
- [**prep** phase](#prep)
- **properties** of expressions [1](#memo) [2](#properties)
- [**pruning** during search](#search)
- [**query text**](#modules)
- [**rewrite** phase](#rewrite)
- [**scalar** vs **relational** properties](#properties)
- [**search** phase](#search)
- [**selectivity**](#stats)
- [**tracked** vs **computed** properties](#properties)
- [**transformation** of expressions](#rewrite)
- [**unnesting**](#rewrite), syn. "decorrelating"

## Modules

The parse phase is not discussed in this RFC. It handles the
transformation of the *SQL query text* into an abstract syntax tree
(AST).

### Prep

*Prep* (short for "prepare") is the first phase of query optimization
where the AST is transformed into a form more suitable for
optimization and annotated with information that will be used by later
phases. Prep includes resolving table and column references (i.e. name
resolution) and type checking, both of which are already performed by
CockroachDB.

During Prep, the AST is transformed from the raw output of the parser
into an expression "tree".

```go
type operator int16

type expr struct {
  op              operator
  children        []*expr
  relationalProps *relationalProps // See [relational properties](#tracked_properties)
  scalarProps     *scalarProps     // See [scalar properties](#tracked_properties)
  physicalProps   *physicalProps   // See [physical properties](#tracked_properties)
  private         interface{}
}
```

The term *"expression"* here is based on usage from literature, though
it is mildly confusing as the current SQL code uses "expression" to
refer to scalar expressions. In this document, "expression" refers to
either a relational or a scalar expression. Using a uniform node type
for expressions facilitates transforms used during the Rewrite and
Search phases of optimization.

Each expression has an *operator* and zero or more operands
(`expr.children`). Operators can be *relational* (e.g. `join`) or
*scalar* (e.g. `<`). Relational operators can be *logical* (only
specifies results) or *physical* (specifies both result and a
particular implementation).

During Prep all the columns are given a unique index (number). Column
numbering involves assigning every base column and non-trivial
projection in a query a unique query-specific index.

Giving each column a unique index allows the expression nodes
mentioned above to track input and output columns, or really any set
of columns during Prep and later phases, using a bitmap. The bitmap
representation allows fast determination of compatibility between
expression nodes and is utilized during rewrites and transformations
to determine the legality of such operations.

The Prep phase also computes *logical properties*, such as the input
and output columns of each (sub-)expression, equivalent columns,
not-null columns and functional dependencies.

The functional dependencies for an expression are constraints over one
or more sets of columns. Specific examples of functional dependencies
are the projections, where 1 or more input columns determine an output
column, and "keys" which are a set of columns where no two rows output
by the expression are equal after projection on to that set (e.g. a
unique index for a table where all of the columns are NOT
NULL). Conceptually, the functional dependencies form a graph, though
they are not represented as such in code.

### Rewrite

The second phase of query optimization is *rewrite*. The rewrite phase
performs *transformations* on the logical query tree which are always
beneficial (i.e. cost-agnostic).

A transformation transforms a (part of a) query into another. Note
that there is conceptual overlap with the Search phase which also
performs transformations on the query. Both phases employ
transformations, yet Search needs to track and cost the alternatives
while Rewrite does not. In the specific context of the rewrite phase,
transformations are commonly called *rewrites*.

During Rewrite, the previous version of an expression is
discarded. During Search, both the original and new expression are
preserved side-by-side as alternatives, see the [section
below](#search) for details.

Also note that some of the transformations performed by Rewrite need
not be performed again by Search (decorrelation is the prime
example). The vast majority of transforms performed by Search are not
used by Rewrite.

Rewrite is the phase where e.g. correlated subqueries are
*decorrelated* (synonym: *unnesting*), additional predicates are
inferred and *predicate push down* occurs, and various other
simplifications to the relational algebra tree (e.g. projection & join
elimination). As an example of predicate push down, consider the
query:

```sql
SELECT * FROM a, b USING (x) WHERE a.x < 10
```

The naive execution of this query retrieves all rows from `a` and `b`,
joins (i.e. filters) them on the variable `x`, and then filters them
again on `a.x < 10`. Predicate push down attempts to push down the
predicate `a.x < 10` below the join. This can obviously be done for
the scan from `a`:

```sql
SELECT * FROM (SELECT * FROM a WHERE a.x < 10), b USING (x)
```

Slightly more complicated, we can also generate a new predicate using
the functional dependence that `a.x = b.x` (due to the join
predicate):

```sql
SELECT * FROM
  (SELECT * FROM a WHERE a.x < 10),
  (SELECT * FROM b WHERE b.x < 10) USING (x)
```

Predicate push down is aided by predicate inference. Consider the query:

```sql
SELECT * FROM a, b USING (x)
```

Due to the join condition, we can infer the predicates `a.x IS NOT
NULL` and `b.x IS NOT NULL`:

```sql
SELECT * FROM a, b USING (x)
  WHERE a.x IS NOT NULL AND b.x IS NOT NULL
```

And predicate push down can push these predicates through the join:

```sql
SELECT * FROM
  (SELECT * FROM a WHERE a.x IS NOT NULL),
  (SELECT * FROM b WHERE b.x IS NOT NULL) USING (x)
```

### Stats

Table statistics power both the cost model and the search of alternate
query plans. A simple example of where stastistics guide the search of
alternate query plans is in join ordering:

```sql
SELECT * FROM a JOIN b
```

In the absence of other opportunities, this might be implemented as a
hash join. With a hash join, we want to load the smaller set of rows
(either from `a` or `b`) into the hash table and then query that table
while looping through the larger set of rows. How do we know whether
`a` or `b` is larger? We keep statistics about the *cardinality* of `a`
and `b`, i.e. the (approximate) number of different values.

Simple table cardinality is sufficient for the above query but fails
in other queries. Consider:

```sql
SELECT * FROM a JOIN b ON a.x = b.x WHERE a.y > 10
```

Table statistics might indicate that `a` contains 10x more data than
`b`, but the predicate `a.y > 10` is filtering a chunk of the
table. What we care about is whether the result of the scan of `a`
after filtering returns more rows than the scan of `b`. This can be
accomplished by making a determination of the *selectivity* of the
predicate `a.y > 10` (the % of rows it will filter) and then
multiplying that selectivity by the cardinality of `a`. The common
technique for estimating selectivity is to collect a histogram on
`a.y` (prior to running the query).

The collection of table statistics occurs prior to receiving the
query. As such, the statistics are necessarily out of date and may be
inaccurate. The system may bound the inaccuracy by recomputing the
stats based on how fast a table is being modified. Or the system may
notice when stat estimations are inaccurate during query execution.

[A separate RFC covers statistics collection in
CockroachDB.](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20170908_sql_optimizer_statistics.md)

### Memo

Memo is a data structure for efficiently storing a forest of query
plans. Conceptually, the memo is composed of a numbered set of
**equivalency classes** called **groups** where each group contains a
set of logically equivalent expressions. The different expressions in
a single group are called **memo-expressions** (memo-ized
expressions). While an expression node outside of the memo contains a
list of child expressions, a memo-expression contains a list of child
groups.

By definition, all the memo-expressions in a group share the same
*logical properties*, a concept explored more in depth in the [section
below](#properties). The memo-expression structure mirrors the
expression structure:

```go
type exprID int32
type groupID int32

type memoExpr struct {
  op            operator
  children      []groupID
  physicalProps *physicalProps
  private       interface{}
}

type memoGroup struct {
  exprs           []memoExpr
  relationalProps *relationalProps
  scalarProps     *scalarProps
}
```

Transformations are *not* performed directly on the memo because
transformations operate on trees while the memo models a forest of
trees. Instead, expression fragments are extracted from the memo,
transformed, and re-inserted into the memo. At first glance, this
seems onerous and inefficient, but it allows transformations to be
rewritten more naturally and the extraction of expression fragments
can be performed efficiently.

Extracting an expression fragment for transformation is performed via
a process called *binding*. Binding allows iterating over all of the
expressions matching a pattern that are rooted at a particular
memo-expression. A pattern is specified using the same expression
structure that is to be extracted, with the addition of "pattern-leaf"
and "pattern-tree" placeholders that act as wildcards:

* A **pattern leaf** matches any expression tree, with only the root
  of the tree being retained in the bound expression. It is used when
  the expression is used opaquely by the transformation. In other
  words, the transformation doesn't care what's inside the subtree. It
  is a "leaf" in the sense that it's a leaf in any binding matching a
  pattern.
* A **pattern tree** matches any expression tree and indicates that
  recursive extraction of the full subtree is required. It is
  typically used for scalar expressions when some manipulation of that
  expression is required by the transformation. Note that a pattern
  tree results in all possible subtrees being enumerated, however
  scalar expressions typically don't have many subtrees (if there are
  no subqueries, there is only one subtree). [TODO(peter): what to do
  about subqueries in a scalar context? Iterating over all of the
  subquery expressions doesn't seem right. There is a TODO in `opttoy`
  to cache scalar expressions in `memoGroup`. Need to investigate this
  further.]

To better understand the structure of the memo, consider the query:

```sql
SELECT * FROM a, b WHERE a.x = b.x
```

Converted to the expression structure which models the extended
relational algebra the query looks like:

```
inner-join [columns: a.x a.y b.x b.z]
  filters:
    eq
      inputs:
        variable (a.x)
        variable (b.x)
  inputs:
    scan [columns: a.x a.y]
    scan [columns: b.x b.z]
```

Inserting the expression tree into the memo results in:

```
6: [inner-join [1 2 5]]
5: [eq [3 4]]
4: [variable b.x]
3: [variable a.x]
2: [scan b]
1: [scan a]
```

Memo groups are numbered by when they were created and the groups are
topologically sorted for display (this is an implementation detail and
not intended to be prescriptive). In the above example, each group
contains only a single memo-expression. After performing the join
commutativity transformation, the memo would expand:

```
6: [inner-join [1 2 5]] [inner-join [2 1 5]]
5: [eq [3 4]]
4: [variable b.x]
3: [variable a.x]
2: [scan b]
1: [scan a]
```

Memo groups contain logically equivalent expressions, but two
logically equivalent expression may not be placed in the same memo
group. This occurs because determining logical equivalency of two
relational expressions is complex to perform 100% correctly. A
correctness failure (i.e. considering two expressions logically
equivalent when they are not) results in invalid transformations and
invalid plans. Placing two logically equivalent expressions in
different groups has a much gentler failure mode: the memo and search
are less efficient.

Insertion of an expression into the memo is performed by recursively
inserting all of the sub-expressions into the memo and then computing
a **fingerprint** for the memo-expression. The fingerprint for a
memo-expression is simply the expression operator and the list of
child groups. For example, in the memo examples above, the fingerprint
for the first inner-join expression is `[inner-join [1 2 5]]`. The
memo maintains a map from expression fingerprint to memo group which
allows quick determination if an expression fragment already exists in
the memo. A small amount of operator-specific normalization is
performed when computing the group fingerprint for a
memo-expression. For example, the left and right inputs of an
inner-join are output in sorted order which results in the expressions
`[inner-join [1 2 5]]` and `[inner-join [2 1 5]]` having the same
group fingerprint. The operator-specific normalization is
conservative. The common case for placing logically equivalent
expressions in the same group is adherence to the invariant that
transformed expressions are logically equivalent to their input.

```go
type memo struct {
  // Map from memo-expression "group" fingerprint to group ID.
  groupMap map[string]groupID
  groups   []memoGroup
}
```

In addition to memo expressions, memo groups also contain a map from
desired physical properties to optimization state for the group for
those properties. This state is discussed more in
[Search](#search-aka-enumeration-or-transformation).

A **location** within the memo identifies a particular memo-expression
by its group and expression number. When an expression fragment is
extracted from the memo, each `expr` is tagged with the location it
originated from in the memo. This allows subsequent reinsertion of a
transformed expression into the memo to quickly determine which groups
the expression nodes should be added to.

```go
type memoLoc struct {
  group groupID
  expr  exprID
}
...
type expr struct {
  op  operator
  loc memoLoc
  ...
}
```

The above depictions of the memo structures are simplified for
explanatory purposes. The actual structures are similar, though
optimized to reduce allocations.

### Properties

Properties are meta-information that are maintained at each node in an
expression. Properties power transformations and optimization.

#### Properties vs attributes

The term "property" encompasses information that is well-defined over
any expression in its group: a given scalar property is well-defined
for all scalar operators; a relational property is well-defined for
all relational operators. For example, "nullability" is a property
that is properly defined (and says something meaningful for) any any
scalar expression.

In contrast, some bits of information are only relevant for specific
operators. For example, the "join algorithm" is only relevant for join
operators; the "index name" is only relevant for table scan operators,
etc. This operator-specific data is called an *attribute* and is
attached to a particular memo-expression.

#### Logical vs physical properties

Logical properties are maintained for both relational and scalar
operators. A logical property refers to logical information about the
expression such as column equivalencies or functional dependencies.

Physical properties are those that exist outside of the relational
algebra such as row order and data distribution. Physical property
requirements arise from both the query itself (the non-relational
`ORDER BY` operator) and by the selection of specific implementations
during optimization (e.g. a merge-join requires the inputs to be
sorted in a particular order).

By definition, two memo-expressions in the same group have the same
logical properties and the logical properties are attached to the memo
group. The physical properties for the memo-expressions in a group may
differ. For example, a memo group containing inner-join will also have
hash-join and merge-join implementations which produce the same set of
output rows but in different orders.

#### Relational vs scalar properties

The memo contains memo-expressions with either scalar (e.g. `+`, `<`,
etc.) or relational (e.g. `join`, `project`, etc.) operators,
distinguished as scalar expressions vs relational expressions.

Scalar and relational properties are maintained in separate data
structures, but note that both scalar and relational properties are
considered *logical*.

#### Derived vs required properties

Properties can be *required* or *derived*.

A required property is one specified by the SQL query text.  For
example, a DISTINCT clause is a required property on the set of
columns of the corresponding projection -- that the tuple of columns
forms a key (unique values) in the results.

A derived property is one derived by the optimizer for an
expression based on the properties of the children nodes.

For example, in `SELECT k+1 FROM kv`, once the ordering of "k" is
known from kv's descriptor, the same ordering property can be derived
for `k+1`.

During optimization, for each node with required properties the
optimizer will look at the children node to check whether their actual
properties (which can be derived) match the requirement. If they don't
the optimizer must introduce an *enforcer* operator in the plan that
provides the required property.

For example, an `ORDER BY` clause creates a required ordering property
can cause the optimizer to add a sort node as an enforcer of that
property.

#### Tracked vs computed properties

A [tracked property](#tracked_properties) is one which is maintained
in a data structure (e.g. `relationalProps`, `scalarProps`,
`physicalProps`). A computed property is one which is computed from an
expression or an expression fragment as needed. For intermediate
nodes, all properties can be computed which makes tracked properties
akin to a cache. The decision for whether to track or compute a
property is pragmatic. Tracking a property requires overhead whether
the property is used or not, but makes accessing the property in a
transformation fast. Computing a property can be done only when the
property is used, but is not feasible if the computation requires an
entire sub-expression tree (as opposed to a fragment). [Computed
properties](#computed_properties) primarly occur for scalar properties
for which transformations often have the entire scalar expression.

#### Tracked properties

The determination of the properties to track is a key aspect of the
design of an optimizer. Track too many and adding new operators
becomes onerous and maintaining the properties through transformations
becomes expensive. Track too few and certain transformations become
difficult.

Relational properties:

* Output columns [`intset`]. The set of columns output by an
  expression. Used to determine if a predicate is compatible with an
  expression.
* Outer columns [`intset`]. The set of columns that are used by the
  operator but not defined in the underlying expression tree (i.e. not
  supplied by the inputs to the current expression). Synonym: *free
  vars*.
* Not-NULL columns [`intset`]. Column nullability is associated with
  keys which are a factor in many transformations such as join
  elimination, group-by simplification
* Keys [`[]intset`]. A set of columns for which no two rows are equal
  after projection onto that set. The simplest example of a key is the
  primary key for a table. Note that a key requires all of the columns
  in the key to be not-NULL.
* Weak keys [`[]intset`]. A set of columns where no two rows
  containing non-NULL values are equal after projection onto that
  set. A UNIQUE index on a table is a weak key and possibly a key if
  all of the columns are not-NULL. Weak keys are tracked because they
  can become keys at higher levels of a query due to null-intolerant
  predicates.
* Foreign keys [`map[intset]intset`]. A set of columns that uniquely
  identify a single row in another relation. In practice, this is a
  map from one set of columns to another set of columns.
* Equivalency groups [`[]intset`]. A set of column groups (sets) where all columns
  in a group are equal with each other.
* Constant columns [`intset`]. Columns for which we know we have a
  single value.

Scalar properties:

* Input columns [`intset`]. The set of columns used by the scalar
  expression. Used to determine if a scalar expression is compatible
  with the output columns of a relational expression.
* Defined columns [`intset`]. The set of columns defined by the scalar
  expression.

Physical properties:

* Column ordering. Specified by a top-level projection.
* Row ordering. Specified by an `ORDER BY` clause or required by a
  physical operator (e.g. merge-join). Row ordering is enforced by the
  **sort** operator.
* Rewindability. Required by multi-use CTEs. Every reference to the
  CTE in the query needs to return the same results. A read-only query
  has this property by default, though care must be taken with regards
  to the [Halloween
  Problem](https://en.wikipedia.org/wiki/Halloween_Problem) if the
  read-only query exists in the context of a DML query. A CTE
  containing a DML (such as `INSERT` or `UPDATE`) needs to have its
  results materialized in temporary storage and thus provide
  *rewindability*. This property is enforced using a **spool**
  operator.

Note that this list of properties is not exhaustive. In particular,
there are a large number of scalar properties for which it isn't clear
if the property should be tracked or computed when necessary. For
example, null-tolerance (does a predicate ever return true for a NULL
value) can be computed from a scalar expression when needed. It is an
open question as to whether it is utilized frequently enough that it
should be tracked.

Tracking is a bit more than caching of computed properties: we can't
compute certain relational properties without the entire
sub-expression. Keys are an example: if you have a deeply nested join,
in order to compute the keys after performing a join associativity
transform, you would need to have the entire expression tree. By
tracking the keys property and maintaining it at each relational
expression, we only need the fragment of the expression needed by the
transform.

### Computed properties

Computed properties are used primarily in conjunction with scalar
expressions. The properties are computed rather than tracked because
we usually have the full scalar expression vs just a fragment for
relational expressions.

Computed scalar properties:

* Injectivity. An injective expression preserves distinctness: it
  never maps distinct elements of its domain to the same element of
  its codomain. `exp(x) = e^x` is injective.
* Monotonicity. An monotonic expression preserves ordering. The
  preservation may be positive or negative (maintains order or inverts
  order) and strict or weak (maintains uniqueness or invalidates it).
  `floor(x)` is a positive-weak monotonic expression. `-x` is a
  negative-strict monotonic expression.
* Null-intolerance. A null-intolerant expression is a predicate which
  never returns `true` for a `NULL` input column. Null-intolerance is
  used to infer nullability of columns. `x = y` (where `x` and `y` are
  columns) is a null-intolerant expression.
* Contains-aggregate. Does the scalar expression contain any aggregate
  functions?
* Contains-subquery. Does the scalar expression contain any
  subqueries?

### Transformations

Transformations convert an input expression tree into zero or more
logically equivalent trees. Transformations utilize properties in
order to determine the validity of the transformation. Transforms are
configured with an expression pattern, a check method and an apply
method. The expression pattern is used to identify locations within
the full expression where the transform can be applied. The check
method performs additional checks to determine the validity of a
transformation. And the apply method applies the transformation,
generating zero or more logically equivalent expressions.

Transformations are categorized as *exploration* or
*implementation*. An exploration transformation creates a logical
expression from an existing logical expression. An implementation
transform creates a physical expression from a logical
expression. Note that both exploration and implementation transforms
take as input logical expressions.

Some examples of transformations:

* Join commutativity swaps the order of the inputs to an inner join:
  `[join a b] -> [join b a]`.
* Join associativity reorders the children of a parent and child join:
  `[join [join a b] c]` -> `[join [join a c] b]`
* Join elimination removes unnecessary joins based on projected
  columns and foreign keys.
* Distinct/group-by elimination removes unnecessary distinct/group-by
  operations based on keys.
* Decorrelation replaces correlated subqueries with semi-join,
  anti-join and apply operators.
* Scan to index scan transforms the logical scan operator into one or
  more index scans on covering indexes.
* Inner join to merge-join transforms a logical inner join operator
  into a merge-join operator.

An example transformation is join commutativity. The pattern for join
commutativity is an inner-join:

```
inner-join
 |
 +-- pattern leaf  // left input
 |
 +-- pattern leaf  // right input
 |
 +-- pattern leaf  // join condition
```

An inner-join always has 3 children: the left and right inputs and the
join condition. Join commutativity only needs to swap the left and
right inputs an this specifies pattern leaf for all 3 children.

The actual join commutativity transform is straightforward:

```go
// This is demonstration code, the real implementation will be mildly
// more complex in order to reduce heap allocations.
func (joinCommutativity) apply(e *expr) *expr {
  return &expr{
    op: innerJoinOp,
    children: []*expr{
      e.children[1],
      e.children[0],
      e.children[2],
    }
    props: e.props,
  }
}
```

Note that join commutativity is the simplest transform. More
sophisticated transforms have to perform complex checks for whether
they can be applied to an expression and for generating the resulting
transformed expression. For a slightly more complex example, join
associativity sorts the join conditions between the upper and lower
joins and checks to see if it is creating an undesirable cross-join.

Implicit in the join commutativity example above is that
transformations are written in code. An alternative is to create a
domain specific language for expressing transformations. The benefit
of such a language is the potential for more compact and expressive
transformations. The downside is the need to write a compiler for the
DSL. The current decision is to eschew a DSL for transformations as
the work involved seems strictly greater than writing transformations
in Go. In particular, a DSL would require both the author and reviewer
to learn the DSL. And a DSL doesn't necessarily ease writing a
transformation. Complex transformations may require extensions to the
DSL and the DSL compiler and thus not simplify writing the
transformation at all. In the short and medium term, the set of
transformations is expected to remain small as energies go into
fleshing out other query planning modules. The decision about a DSL
for transformations should be revisited as the transformation set
grows or in the light of experimentation with a DSL that proves its
worth.

### Cost model

The cost model takes as input a physical query plan and computes an
estimated "cost" to execute the plan. The unit of "cost" can be
arbitrary, though it is desirable if it has some real world meaning
such as expected execution time. What is required is for the costs of
different query plans to be comparable. A SQL optimizer is seeking to
find the shortest expected execution time for a query and uses cost as
a proxy for execution time.

Cost is roughly calculated by estimating how much time each node in
the expression tree will use to process all results and modelling how
data flows through the expression tree. [Table statistics](#stats) are
used to power cardinality estimates of base relations which in term
power cardinality estimates of intermediate relations. This is
accomplished by propagating histograms of column values from base
relations up through intermediate nodes (e.g. combining histograms
from the two join inputs into a single histogram). Operator-specific
computations model the network, disk and CPU costs. The cost model
should include data layout and the specific operating environment. For
example, network RTT in one cluster might be vastly different than
another.

The operator-specific computations model the work performed by the
operator. A hash-join needs to model if temporary disk will be needed
based on the estimated size of the inputs.

Because the cost for a query plan is an estimate, there is an
associated error. This error might be implicit in the cost, or could
be explicitly tracked. One advantage to explicitly tracking the
expected error is that it can allow selecting a higher cost but lower
expected error plan over a lower cost but higher expected error
plan. Where does the error come from? One source is the innate
inaccuracy of stats: selectivity estimation might be wildly off due to
an outlier value. Another source is the accumulated build up of
estimation errors the higher up in the query tree. Lastly, the cost
model is making an estimation for the execution time of an operation
such as a network RTT. This estimate can also be wildly inaccurate due
to bursts of activity.

Search finds the lowest cost plan using dynamic programming. That
imposes a restriction on the cost model: it must exhibit optimal
substructure. An optimal solution can be constructed from optimal
solutions of its subproblems.

### Search (a.k.a. Enumeration)

Search is the final phase of optimization where many alternative
logical and physical query plans are explored in order to find the
best physical query plan. The output of Search is a physical query
plan to execute. Note that in this context, a physical query plan
refers to a query plan for which the leaves of the tree are table
scans or index scans. In the long term, DistSQL planning will be
incorporated into Search, though in the short term it may be kept
separate.

In order to avoid a combinatorial explosion in the number of
expression trees, Search utilizes the Memo structure. Due to the large
number of possible plans for some queries, Search cannot explore all
of them and thus requires *pruning* heuristics. For example, Search
can cost query plans early and stop exploring a branch of plans if the
cost is greater than the current best cost so far.

Search begins with a Memo populated with the expression provided by
Rewrite. Search is modelled as a series of tasks that optimize an
expression. Conceptually, the tasks form a dependency tree very much
like the dependency tree formed by tools like make. Each task has a
count of its unfinished dependencies and a pointer to its parent
task. When a task is run it is passed its parent task and as part of
running it can add additional dependencies to its parent, thus making
the tree of dependencies dynamic. After a task is run, it decrements
its parent tasks and schedules it for execution if it was the last
dependency. Note that new tasks are only created if new expressions
were added to the memo. Search will not terminate if we continually
created new expressions via transformations, but that would also
indicate that we have an unbounded growth in expressions. In practice,
Search will have some limits on the number of steps it performs or
time it can take.

The initial task for Search is to optimize the "root" group. The tasks
described are the standard Cascades-style search tasks:

1. `OptimizeGroup(reqProps)`. Implements the group (via
   `ImplementGroup`) which generates implementations for the
   expressions in the group, then selects the plan with the least
   estimated cost. Enforcers (e.g. sort) are added as needed.

2. `ImplementGroup`. Explores the group (via `ExploreGroup`) which
   generates more logical expressions in the group and in child
   groups, then generates implementations for all of the logical
   expressions (via `ImplementGroupExpr`). `ImplementGroup` itself
   does not perform any transformations, but acts as a synchronization
   point for dependent tasks.

3. `ImplementGroupExpr`. Implements all of the child groups (via
   `ImplementGroup`), then applies any applicable implementation
   transformations (via `Transform`) to the forest of expressions
   rooted at the specified memo-expression. Example transformation:
   inner-join to merge-join and hash-join.

4. `ExploreGroup`. Explores each expression in the group (via
   `ExploreGroupExpr`). `ExploreGroup` itself does not perform any
   transformations, but acts as a synchronization point for dependent
   tasks.

5. `ExploreGroupExpr`. Explores all of the child groups (via
   `ExploreGroup`), then applies any applicable exploration
   transformations (via `Transform`) to the forest of expressions
   rooted at the specified memo-expression. Example transformations:
   join commutativity and join associativity.

6. `Transform`. Applies a transform to the forest of expressions
   rooted at a particular memo-expression. There are two flavors of
   transformation task: exploration and implementation. The primary
   difference is the state transition after the task finishes. An
   exploration transform task recursively schedules exploration of the
   group it is associated with. An implementation transform task
   schedules optimization of the group.

A search *stage* is configured by a set of exploration and
implementation transforms, and a *budget*. The budget is used to prune
branches of the search tree which appear undesirable. The initial
search stage has a limited set of exploration and implementation
transforms (perhaps 0 exploration transforms), an unlimited budget,
and aims to quickly find a workable, though possibly slow, plan. Each
subsequent stage uses the cost from the best plan of the previous
stage for pruning. [TODO(peter): my understanding of how this will
work is slightly fuzzy. My usage of the term budget might be
off. Perhaps better to describe it as "max cost".]

Full featured optimizers can contain hundreds of
transformations. Checking whether each transformation is applicable at
each node would be prohibitively expensive, so the transformations are
indexed by the root operator of their pattern. Transformations are
further categorized as exploration and implementation and divided
amongst the search stages based on generality and expected benefit.

Search is naturally parallelizable, yet exploiting that parallelism
involves synchronization overhead. Parallelization also can allow one
query to utilize more planning resources than other queries. Rather
than support parallelization of search, energy will instead be
directed at making search and transformations fast and memory
efficient.

### Testing

Historically, SQL databases have introduced subtle bugs that have
lasted for years through invalid transformations. Search should be
designed for testability. One example of this is to allow verification
that all of the alternate plans generated by Search actually produce
the same result.

In addition to testing the alternative query plans, there is utility
in generating a large number of valid SQL statements. The existing
Random Syntax Generator does one level of this by generating
syntactically valid SQL. An additional level would be to generate
semantically valid queries which might be more feasible by random
generation at the expression level.

The relational algebra expression trees should provide a textual
format to ease testing using infrastructure similar to the existing
logic tests where test files define queries and expected results.

Optimization is concerned with making queries faster and it is quite
disturbing to users when inadvertent regressions occur. A large test
suite needs to be developed over time which ensures that the addition
of new transformations or improvements to the various modules do not
cause regressions in the chosen plans.

Generating actual table data with various data distributions for
testing purposes would be both onerous and slow. Table statistics are
a key factor in the decisions performed by search. In order to
adequately test how the behavior of search changes with changing table
statistics, we need an easy mechanism for injecting fake statistics.

## Roadmap

The above outline sketches a large amount of work. How do we get there
from here? A strawman proposal divides the work into several
releases. The farther out the proposed work, the fuzzier the proposal
becomes.

### 2.0

* Stats. Stats are not dependent on other planning modules but are a
  prerequisite to cost-based transformations. Stats are only generated
  explicitly via `CREATE STATISTICS`.

* Prep. Introduce the expression tree. Construct the expression tree
  from the existing AST output by the parser. Use the AST-based type
  checking and name resolution. The existing AST-based planning code
  will be left in place and a parallel world of expression-based
  planning will be erected. The new planning code will not be used in
  this release.

* Rewrite. Predicate inference and predicate push down.

* Memo. Introduce the memo structure.

* Testing. Use ugly hacks to hook up a hobbled version of something as
  an alternate query planner. Perhaps a flag to pass queries through
  the expression format and memo and then translate them back into the
  AST in order to use the legacy planner.

### 2.1

* Stats. Automatically gather stats on PKs and index columns.

* Prep. Perform name resolution and type checking on the expression
  tree. Support non-recursive CTEs. Fall-back to legacy planning code
  for unsupported queries.

* Rewrite. Transform correlated subqueries into apply
  variants. Transform common apply variants into joins.

* Execution. Nested-loop-join, semi-join, anti-join and apply
  processors.

* Cost model. Basic cost model that is powered by stats.

* Search. Task-based single stage search. No pruning. Use existing
  DistSQL planning. Facility for time-travel debugging of the search
  process and inspecting the memo state (e.g. logical and physical
  properties). Global and per-session disablement of individual
  transforms.

* Transforms. Join elimination, distinct/group-by elimination, join
  commutativity, join associativity, index selection, and scalar
  normalization.

* Testing. Random generation of table data based on schema and query
  to exercise corner conditions. Random sampling and execution of
  alternate query plans to verify equivalence. Test suite for plan
  selection using injected stats.

### 2.2

* Stats. Support more advanced statistics (e.g. filtered statistics).

* Prep. Support 100% of queries, enabling the deletion of the legacy
  planning code.

* Cost model. Make the cost model more sophisticated by taking into
  account measurements of network bandwidth and latency. Validate cost
  model against actual queries.

* Search. Add multiple stages with pruning heuristics. Integrate
  DistSQL planning.

* Transforms. Pull group-by above a join. Push group-by below a
  join. Split group-by into local and global components. Simplify
  outer joins.

* Execution. Stream-group-by.

## Unresolved questions

* Flesh out understanding of where physical properties such as
  ordering can be imposed by the query itself. For example, a
  top-level `ORDER BY` clause definitely imposes ordering. But so does
  an `ORDER BY` clause that is the immediate sub-expression of
  `LIMIT/OFFSET`, `DISTINCT ON`, `WITH ORDINALITY`,
  `{INSERT,UPSERT,DELETE,UPDATE}` and `CREATE TABLE ... AS ...`. We
  also need to pay attention to `ORDER BY INDEX` and `ORDER BY PRIMARY
  KEY`, though those clauses likely degenerate into `ORDER
  BY`. Are there other places we need to pay attention to physical
  properties?  Are there other physical properties to capture at
  intermediate nodes?

* Which parts of query planning can be performed during PREPARE vs
  EXECUTE? Most (all?) of the transformations that are part of Rewrite
  can be performed during PREPARE. For example, predicate push-down
  and decorrelation do not require placeholder values. And some parts
  of Search, such as join enumeration, can be performed during
  PREPARE. The part that is restricted to EXECUTE are certain parts of
  index selection and thus costing of query plans.

* The performance of the query planner itself is important because
  query planning occurs for every query executed. What sorts of fast
  paths are possible for simple queries?

* Window functions.

* Describe max1row operator and why it is necessary.

## Appendix

### Expr/Memo examples

Consider the query:

```sql
SELECT v, k FROM kv WHERE k < 3
```

Building the expression tree results in:

```
project [out=(0,1)]
  columns: kv.v:1 kv.k:0
  projections:
    variable (kv.v) [in=(1)]
    variable (kv.k) [in=(0)]
  inputs:
    select [out=(0,1)]
      columns: kv.k:0* kv.v:1
      filters:
        lt [in=(0)]
          inputs:
            variable (kv.k) [in=(0)]
            const (3)
      inputs:
        scan [out=(0,1)]
          columns: kv.k:0 kv.v:1
```

Some points to notice above. The relational operators (`project`,
`select` and `scan`) track their output column set as a bitmap
(i.e. `out=(0,1)`). Scalar expressions such as `variable` and `lt`
track their required input columns. Relational operators have a slice
of children where the interpretation of the children is operator
specific. The `project` operator has 2 children: a relational input
and a list of projections. Note that the order of projections is
important and are stored using an `ordered-list` operator in the
memo. The `select` operator also has 2 children: a relational input
and a list of filters.

Inserting the expression tree into the memo results in:

```
8: [project [5 7]]
7: [ordered-list [6 2]]
6: [variable kv.v]
5: [select [1 4]]
4: [lt [2 3]]
3: [const 3]
2: [variable kv.k]
1: [scan kv]
```

Here we can see more clearly the child structure of the various
relational operators. The `select` expression in group 5 has 2
children: groups 1 and 4. Group 1 is a `scan` and group 4 is the
filter.

As another example, consider the query:

```sql
SELECT k, v FROM (SELECT v, k FROM kv)
```

Inserting into the memo we get:

```
7: [project [5 6]]
6: [ordered-list [3 2]]
5: [project [1 4]]
4: [ordered-list [2 3]]
3: [variable kv.k]
2: [variable kv.v]
1: [scan kv]
```

Notice that the variables (`kv.k` and `kv.v`) are only present once in
the memo and their groups are shared by both projection lists.
