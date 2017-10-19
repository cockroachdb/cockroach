- Feature Name: SQL Query Planning
- Status: draft
- Start Date: 2017-10-08
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

SQL query planning is often described in terms of 7 modules:

1. [Stats](#stats)
2. [Prep](#prep)
3. [Rewrite](#rewrite)
4. [Memo](#memo)
5. [Cost Model](#cost-model)
6. [Search](#search-aka-enumeration-or-transformation)
7. [Properties](#properties)

Note that Stats, Cost Model, Memo and Properties could be considered
modules, while Prep, Rewrite and Search could be considered phases,
though we'll refer to all 7 uniformly as modules in this
document. Memo is a technique for compactly representing the forest of
trees generated during Search. Stats, Properties and Cost Model are
modules that power Prep, Rewrite and Search.

```
  SQL query text
       |
       v
   .--------.
   |  Parse | - out of scope for this RFC
   '---+----'
       |
      (ast)
       |
       v
   .--------.  - includes constant folding, type checking, name resolution
   | Prep   |  - computes some initial properties
   '---+----'  - done once per PREPARE
       |
 (memo or data structure TBD)
       |
       v
   .---------. - done every EXECUTE to capture placeholder values / timestamps
   | Rewrite | - includes cost-agnostic transformations, eg. predicate push-down
   '---+-----' - computes more properties
       |
       |                .----------.
    (memo)              |  Stats   |
       |                '----+-----'
       |                     /
       v    v---------------'
   .---------.
   | Search  | - uses Cost Model
   '---+-----' - derives properties for alternate plans
       |
    (memo with physical plan)
       |
       v
 .--------------------.
 | Topology planning  | - called "distsql physical planning" previously
 | then execution     |
 '--------------------'
```


CockroachDB already has implementations of portions of these modules
except for Stats and Memo. For example, CockroachDB performs name
resolution and type checking which is part of Prep, and performs
predicate push down through joins which traditionally happens during
Rewrite. CockroachDB utilizes a primitive Cost model during index
selection (a portion of Search) to choose which index to use based on
filters and desired ordering.

In addition to the 6 modules, another aspect of the optimizer that
needs discussion is [Testing](#testing) and test infrastructure.

Lastly, a strawman [Roadmap](#roadmap) is proposed for how to break up
this work over the next several releases.

## Glossary

The following terms are introduced/defined in this RFC:

- [**algebraic equivalence**](#properties)
- [**cardinality**](#stats)
- [**condition** in transformations](#search)
- [**decorrelating**](#rewrite), syn. "unnesting"
- [**derived** vs **required** properties](#properties)
- [**enforcer** operator for properties](#properties)
- [**equivalency class**](#memo)
- [**exploration** vs **implementation** transformations](#search)
- [**expressions** in queries](#prep)
- [**functional dependencies**](#prep)
- [**logical** vs **physical** properties](#memo)
- [**logical** vs **physical** vs **scalar** operators](#prep)
- [**m-expression**](#memo)
- [**operator** in query expressions](#prep)
- [**pattern** in transformations](#search)
- [**predicate push-down**](#rewrite)
- [**prep** phase](#prep)
- **properties** of expressions [1](#memo) [2](#properties)
- [**pruning** during search](#search)
- [**query text**](#modules)
- [**rewrite** phase](#rewrite)
- [**scalar** vs **relational** properties](#properties)
- [**search** phase](#search)
- [**selectivity**](#stats)
- [**top-down** and **bottom-up** search strategies](#search)
- [**transformation** of expressions](#rewrite)
- [**unnesting**](#rewrite), syn. "decorrelating"
- [**utility** of a transformation](#search)

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

During prep, the AST is transformed from the raw output of the parser
into an expression "tree".

The term *"expression"* here is based on usage from literature, though
it is mildly confusing as the current SQL code uses "expression" to
refer to scalar expressions. In this document, "expression" refers to
either a relational or a scalar expression. Using a uniform node type
for expressions facilitates transforms used during the Rewrite and
Search phases of optimization.

Each expression has an *operator* and zero or more operands. Operators
can be *relational* (e.g. `join`) or *scalar* (e.g. `<`). Relational
operators can be *logical* (only specifies results) or *physical*
(specifies both result and a particular implementation).

During Prep all the variables are given a unique index (number).
Variable numbering involves assigning every base column and
non-trivial projection in a query with a unique index.

Giving each variable a unique index allows the expression nodes
mentioned above to track input and output variables, or really any set
of variables during Prep and later phases, using a bitmap. The bitmap
representation allows fast determination of compatibility between
expression nodes and is utilized during rewrites and transformations
to determine the legality of such operations.

The Prep phase also starts computing *logical properties*, such as the
input and output variables of each (sub-)expression and various
functional dependencies.

The *functional dependencies* of an expression in this context is the
set of input variables that are necessary and sufficient to compute
the expression's result. This will be later used to derive more
properties (e.g. ordering) by using the edges of the functional
dependency graph.

The Prep phase also determines a list of filters and projections
applied at relational nodes.

The output of Prep may be another tree data structure (separate from
the AST and the memo), although perhaps Prep can already populate the
memo. This will be determined in full by experimentation.

### Rewrite

The second phase of query optimization is *rewrite*. The rewrite phase
performs *transformations* on the logical query tree which are always
beneficial.

A transformation transforms a (part of a) query into another.
Note that there is conceptual overlap with the Search
phase which also performs transformations on the query. Both phases
employ transformations, yet Search needs to track and cost the
alternatives while Rewrite does not.
In the specific context of the rewrite phase, transformations are commonly
called *rewrites*.

During Rewrite, the previous version is discarded. During Search,
typically both original and new expression are preserved side-by-side
as alternatives, see the [section below](#search) for details.

Also note that some of the
transformations performed by Rewrite need not be performed again by
Search (decorrelation is the prime example). The vast majority of
transforms performed by Search are not used by Rewrite.

Rewrite is the phase where e.g. correlated subqueries are *decorrelated*
(synonym: *unnesting*) and *predicate push down* occurs,
and various other simplifications to the
relational algebra tree (e.g. projection & join elimination). As an example
of predicate push down, consider the query:

  `SELECT * FROM a, b USING (x) WHERE a.x < 10`

The naive execution of this query retrieves all rows from `a` and `b`,
joins (i.e. filters) them on the variable `x`, and then filters them
again on `a.x < 10`. Predicate push down attempts to push down the
predicate `a.x < 10` below the join. This can obviously be done for
the scan from `a`:

  `SELECT * FROM (SELECT * FROM a WHERE a.x < 10), b USING (x)`

Slightly more complicated, we can also generate a new predicate using
the functional dependence that `a.x = b.x` (due to the join
predicate):

```
SELECT * FROM
  (SELECT * FROM a WHERE a.x < 10),
  (SELECT * FROM b WHERE b.x < 10) USING (x)
```

### Stats

Table statistics power both the cost model and the search of alternate
query plans. A simple example of where stastistics guide the search of
alternate query plans is in join ordering:

  `SELECT * FROM a JOIN b`

In the absence of other opportunities, this might be implemented as a
hash join. With a hash join, we want to load the smaller set of rows
(either from `a` or `b`) into the hash table and then query that table
while looping through the larger set of rows. How do we know whether
`a` or `b` is larger? We keep statistics about the *cardinality* of `a`
and `b`, i.e. the (approximate) number of different values.

Simple table cardinality is sufficient for the above query but fails
in other queries. Consider:

  `SELECT * FROM a JOIN b ON a.x = b.x WHERE a.y > 10`

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
CockroachDB.](https://github.com/cockroachdb/cockroach/pull/18399)

### Memo

Memo is a data structure for maintaining a forest of query
plans. Conceptually, the memo is composed of a numbered set of
**equivalency classes** where each group is a node in the query
plan. Each equivalency class contains one or more equivalent logical
or physical expression nodes. For example, one equivalency group might
contain both "a = b + 1" and "b = a - 1" which are logically
equivalent in relational algebra.

The different nodes in a single equivalency class are called
**m-expressions** (memo(ized) expressions).

By definition, all the m-expressions in a class share the same
*logical properties*, a concept explored more in depth in the [section
below](#properties).

During search, m-expressions in the memo are walked over and
progressively transformed creating new m-expressions in order to
generate alternative plans. The memo structure avoids the
combinatorial explosion of trees be naively enumerating the variants
by having each m-expressions in the tree point to child equivalency
classes rather than directly to child m-expressions.

For example, consider the query:

  `SELECT * FROM a, b, c`

Transformed to relational algebra, this might look like:

  `(JOIN (JOIN a b) c)`

If we were naively enumerating all of the possible join orders, we
would have several other possible plans due to variations in join
ordering using commutativity and associativity:

```
(JOIN (JOIN a b) c)
(JOIN (JOIN b a) c)
(JOIN c (JOIN b a))
(JOIN c (JOIN a b))

(JOIN a (JOIN b c))
(JOIN a (JOIN c b))
(JOIN (JOIN b c) a)
(JOIN (JOIN c b) a)

(JOIN b (JOIN a c))
(JOIN b (JOIN c a))
(JOIN (JOIN a c) b)
(JOIN (JOIN c a) b)
```

With memo, these variants share m-expressions. This is done by grouping
logically equivalent nodes. A representation of the above query before
enumeration might look like:

```
4: [join 2, 3]
3: [scan c]
2: [join 0, 1]
1: [scan b]
0: [scan a]
```

The root of the query is `4`. During enumeration, transformation rules
create logically equivalent m-expressions in each equivalency group, but doing
so does not require creating new parent m-expressions. In the above example,
the expanded memo would look like:

```
6: [join 2, 3], [join 3, 2], [join 1, 5], [join 5, 1], [join 1, 4], [join 4, 1]
5: [join 0, 2], [join 2, 0]
4: [join 0, 3], [join 3, 0]
3: [scan c]
2: [join 0, 1], [join 1, 0]
1: [scan b]
0: [scan a]
```

The above query is considering join order, but such logically
equivalent m-expressions also occur for index selection.

Note that the numbering of the equivalency classes in the memo is
arbitrary, but the depth-first traversal during the construction of
the memo usually results in the highest number becoming the root of
the query.

The memo is principally used by Search. To avoid creating an
additional intermediate data structure, it may be beneficial to have
Prep populate the memo as well. This will be determined in a later
iteration.

[The memo structure is discussed further in a separate
RFC](https://github.com/cockroachdb/cockroach/pull/19220).

### Properties

#### Logical vs physical properties

We call *properties* the meta-information the Memo captures about each
sub-expression. We use properties in two ways.

1) to state that two expressions are equivalent from the relational
   point of view. That is, they produce the same results when
   run. These properties are said to be *logical*. The Memo uses this
   concept to store expressions with the same logical properties in
   the same Memo equivalence class. The second way to we use
   properties is

2) as a means to store information that can be useful during
   optimization.

Note that two m-expressions might be logically equivalent but have
different *physical properties*. For example, a table scan and an
index scan can both output the same set of columns, but provide
different orderings.

Physical properties are annotations on m-expressions; i.e.
m-expressions in the same (logical) equivalency class can have
different physical properties. We expect that only m-expressions that
correspond to physical operators carry physical properties.

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
properties (which can be derived) match the requirement. If they
don't the optimizer must introduce an *enforcer* operator in the plan
that creates the required property.

For example, an ORDER BY clause that creates a required property can
cause the optimizer to add a sort node as enforcement.

#### Relational vs scalar properties

The memo contains m-expressions with either scalar (e.g. `+`, `<`,
etc.) or relational (e.g. `join`, `project`, etc.) operators,
distinguished as scalar expressions vs relational expressions.

The sets of useful properties for scalar vs relational expression are
only partially overlapping.

Some properties are useful to both, for example variable dependencies
(the set of variables from the context necessary to compute the
result) are useful for computing (de)correlation.

Some properties are only relational, for example unicity of a column
group across all rows.

Some properties are only scalar, for example that a variable can never
be NULL.

#### Which properties will be used

The determination of the right set of properties to build and maintain
is a key aspect of the design of an optimizer. This set must
efficiently support the manipulation we want to apply to the original
query without incurring too much planning overheads.

The particular definition of the properties to be used in CockroachDB
will grow incrementally over time in parallel with the query
optimizer.

[A separate RFC will outline some properties already envisioned to be
useful initially.](https://github.com/cockroachdb/cockroach/pull/19366)

### Cost model

The cost model takes as input a logical or physical query plan and
computes an estimated "cost" to execute the plan. The unit of "cost"
can be arbitrary, though it is desirable if it has some real world
meaning such as expected execution time. What is required is for the
costs of different query plans to be comparable. A SQL optimizer is
seeking to find the shortest expected execution time for a query and
uses cost as a proxy for execution time.

Cost is roughly calculated by estimating how many rows a node in the
expression tree will process and maintaining metrics or heuristics
about how fast the the node can process rows. The row cardinality
estimate is powered by [table statistics](#stats). The processing
speed is partially dependent on data layout and the specific
environment we're operating in. For example, network RTT in one
cluster might be vastly different than another.

Because the cost for a query plan is an estimate, there is an
associated error. This error might be implicit in the cost, or could
be explicitly tracked. One advantage to explicitly tracking the
expected error is that it can allow selecting a higher cost but lower
expected error plan over a lower cost but higher expected error
plan. Where does the error come from? One source is the innate
inaccuracy of stats: selectivity estimation might be wildly off due to
an outlier value. Another source is build up of estimation errors as
the higher up in the query tree. Lastly, the cost model is making an
estimation for the execution time of an operation such as a network
RTT. This estimate can also be wildly inaccurate due to bursts of
activity.

### Search (a.k.a. Enumeration or Transformation)

Search is the final phase of optimization. The output of Rewrite is a
expression tree that represents the current logical plan for
the query. If Prep and Rewrite use the memo already, there would
be just one m-expression per class at this point.

Search is the phase of optimization where many alternative logical and
physical query plans are explored in order to find the best physical
query plan. The output of Search is a physical query plan to
execute. Note that in this context, a physical query plan refers to a
query plan for which the leaves of the tree are table scans or index
scans. DistSQL planning may be an additional step (i.e. DistSQL
physical planning would schedule processors on nodes).

In order to avoid a combinatorial explosion in the number of
expression trees, Search utilizes the Memo structure. Due to the large
number of possible plans for some queries, Search cannot explore all
of them and thus requires *pruning* heuristics. For example, Search
can cost query plans early and stop exploring a branch of plans if the
cost is greater than the current best cost so far.

Search begins with an empty Memo and the expression provided by
Rewrite. It begins enumerating alternatives, costing them and pruning
branches that are deemed not worthwhile to explore based on their
cost.

At its core, Search is iterating over m-expressions and creating new
equivalent m-expressions through transformation. The most basic
logical transformation is join order enumeration (e.g. `a JOIN b` ->
`b JOIN a`).  The transformations that enumerate alternate plans that
are *algebraically equivalent* without making implementation decisions
are commonly called *exploration* transformations.

Logical to physical transformations include enumerating the
possibilities for retrieving the necessary columns from a table,
either by a secondary index or the primary index. The logical to
physical transformation may generate glue m-expressions such as
sorting or distinct filtering.  These are called *implementation*
transformations.

The expression nodes maintained by search extend the Rewrite
expression nodes with the addition of *physical properties* such as
provided ordering. CockroachDB already does much of this physical
property maintenance (see `physicalProps`), though it might need to be
rewritten to operate on top of the Search expression nodes.

Full featured optimizers can include hundreds of
transformations. Checking whether each transformation is applicable at
each node would be prohibitevely expensive, so the transformations are
indexed in various ways. The most basic indexing is by expression
operator (e.g. `inner join`, `group by`, etc). A transformation is
composed of a `match` function (*pattern* and/or *condition*), and an
`apply` function. The `match` function provides a quick yes/no answer
as to whether the transformation can be applied to an expression and
returns an estimate of the expected benefit (called a "promise" in
some of the literature, or *utility*). Search then applies the
transformations in order of their expected benefit, generating new
expressions that are added to the memo. Note that `match` does not
need to guarantee the transformation can apply.

The basic form of Search iteration is open to debate. In a
Cascades-style optimizer, Search maintains a stack of tasks to perform
which it initializes with an Explore Group task for the root
class. Each task can generate additional tasks that are added to the
stack. The usage of an explicit stack, rather than a recursive search,
provides flexibility in the enumeration and allows Search to be
terminated when a goal criteria (such as a deadline) is reached. Note
that processing of these optimizer tasks can be done concurrently and
it might be worthwhile to parallelize the work to reduce latency of
the optimizer.

Strategies have been classified in the literature as being *top-down*
or *bottom-up*. They both use dynamic programming and memoization. The
difference is that in a bottom-up search, one enumerates all the plans
possible from the leaves of an expression up. This mostly applies to
join ordering -- but not only to it. So we'd see a loop in the number
of tables that were already joined in the enumeration step that would
go like this. The best plan for a N-way join query is achieved by
finding the best plan for any 2-way join, then adding a new join to
that to find the best plan for a 3-way join, and so on until the
number of joins a query brings. In contrast, the top-down approach is
similar to a quicksort. We'd find a "pivot" in the number of relations
and recurse into the two halves. So to find the best N-way join, we
find a relation to take out and then find the best N-1 join to join
with that relation. Note that in this explanation we’'ve considered
only left-deep plans, that is, there are no desire to execute any
joins concurrently. Alternatively, [bushy
plans](https://www.red-gate.com/simple-talk/sql/performance/join-reordering-and-bushy-plans/)
would allow for those.

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
disturbing when inadvertent regressions occur. A large test suite
needs to be developed over time which ensures that the addition of new
transformations or improvements to the various modules do not cause
regressions in the chosen plans.

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

### 1.2

* Stats. Stats are not dependent on other modules but could be
  utilized today to improve aspects of CockroachDB query
  planning. Additionally, a rough RFC exists and the work appears
  doable in the time frame of a single release.

* Prep. Introduce a new relational algebra expression tree, possibly a
  first iteration of the memo.  This paves the way for easier
  transformations during Rewrite and Search. There are significant
  engineering challenges in doing this work. The parser is likely to
  continue to use the existing AST, so the expression tree will have
  to be generated by converting the AST. In order to allow this work
  to be performed incrementally, the existing AST-based planning code
  will be left in place and a parallel world if IR-based planning will
  be erected. This is discussed separately in the [data structures
  RFC](https://github.com/cockroachdb/cockroach/pull/19220) which
  discusses this initial implementation plan further.

* Rewrite. Predicate push down and predicate propagation across joins.

### 1.3

* Prep. Support >50% of queries.

* Rewrite. Simple decorrelation of subqueries which should handle the
  majority of such queries. We will need to introduce semi-join and
  anti-join processors in order to support this.

* Memo and search. Introduce the memo structure and memo IR
  trees. Move index selection for the IR representation from the plan
  nodes to search.

### 1.4

* Prep. Support 100% of queries, enabling the deletion of the legacy
  planning code.

* Cost model: Introduce a basic cost model that is powered by stats.

* Search: Organize search using optimizer tasks.

### 1.5

* Rewrite part 2. Implement more general decorrelation of subqueries.

* Cost model part 2. Make the cost model more sophisticated by taking
  into account measurements of network bandwidth and latency.

* Search part 2. Extend the transformation set. Consider parallelizing
  search.
