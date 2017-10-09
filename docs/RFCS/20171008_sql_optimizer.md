- Feature Name: SQL Optimizer Outline
- Status: draft
- Start Date: 2017-10-08
- Authors: Peter Mattis
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC sketches the outlines of the high-level modules of a SQL
optimizer.

# Motivation

SQL optimization is concerned with transforming a SQL query into a
physical query plan. Naive execution of a SQL query can be
prohibitively expensive, because SQL specifies the desired results and
not how to achieve them. A given SQL query can have thousands of
alternate query plans with vastly different execution times. The
techniques used to generate and select a good query plan involve
significant engineering challenges.

This RFC is intended to provide guidance for both short term and long
term work on the SQL optimizer, and highlight areas of the current
system that will need to evolve.

## Overview

SQL optimization is often described in terms of 6 modules:

1. [Stats](#stats)
2. [Prep](#prep)
3. [Rewrite](#rewrite)
4. [Memo](#memo)
5. [Cost Model](#cost-model)
6. [Search](#search-aka-enumeration-or-transformation)

Note that Stats, Cost Model and Memo could be considered modules,
while Prep, Rewrite and Search could be considered phases, though
we'll refer to all 6 uniformly as modules in this document. Memo is a
technique for compactly representing the forest of trees generated
during Search. Stats and Cost Model are modules that power Prep,
Rewrite and Search.

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

### Stats

Table statistics power both the cost model and the search of alternate
query plans. A simple example of where stastistics guide the search of
alternate query plans is in join ordering:

  `SELECT * FROM a JOIN b`

In the absence of other opportunities, this might be implemented as a
hash join. With a hash join, we want to load the smaller set of rows
(either from `a` or `b`) into the hash table and then query that table
while looping through the larger set of rows. How do we know whether
`a` or `b` is larger? We keep statistics about the cardinality of `a`
and `b`.

Simple table cardinality is sufficient for the above query but fails
in other queries. Consider:

  `SELECT * FROM a JOIN b ON a.x = b.x WHERE a.y > 10`

Table statistics might indicate that `a` contains 10x more data than
`b`, but the predicate `a.y > 10` is filtering a chunk of the
table. What we care about is whether the result of the scan of `a`
after filtering returns more rows than the scan of `b`. This can be
accomplished by making a determination of the selectivity of the
predicate `a.y > 10` and then multiplying that selectivity by the
cardinality of `a`. The common technique for estimating selectivity is
to collect a histogram on `a.y` (prior to running the query).

The collection of table statistics occurs prior to receiving the
query. As such, the statistics are necessarily out of date and may be
inaccurate. The system may bound the inaccuracy by recomputing the
stats based on how fast a table is being modified. Or the system may
notice when stat estimations are inaccurate during query execution.

### Prep

Prep (short for prepare) is the first phase of query optimization
where the AST is transformed into a form more suitable for
optimization and annotated with information that will be used by later
phases. Prep includes resolving table and column references (i.e. name
resolution) and type checking, both of which are already performed by
CockroachDB.

During prep, the AST is transformed from the raw output of the parser
into an expression tree. The term "expression" here is based on usage
from literature, though it is mildly confusing as the current SQL code
uses "expression" to refer to scalar expressions. In this document,
"expression" refers to either a relational or a scalar
expression. Using a uniform node type for expressions facilitates
transforms used during the Rewrite and Search phases of
optimization. In addition to the basic information contained in the
AST, each expression node tracks logical properties, such as the input
and output variables of the node and functional dependencies, and
maintains a list of filters and projections applied at the node.

Variable numbering involves assigning every base attribute and
relational attribute in a query with a unique zero-based index. Given
each attribute a unique index allows the expression nodes mentioned
above to track input and output variables using a bitmap. The bitmap
representation allows fast determination of compatibility between
expression nodes and is utilized during rewrites and transformations
to determine the legality of such operations.

### Rewrite

The second phase of query optimization is rewrite. The rewrite phase
performs transformations on the logical query tree which are always
beneficial. This is the phase where correlated subqueries are
decorrelated and predicate push down occurs. As an example of the
latter, consider the query:

  `SELECT * FROM a, b USING (x) WHERE a.x < 10`

The naive execution of this query retrieves all rows from `a` and `b`,
joins (i.e. filters) them on the attribute `x`, and then filters them
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

### Memo

Memo is a data structure for maintaining a forest of query
plans. Conceptually, the memo is composed of a set of equivalency
groups where each group is a node in the query plan. Each equivalency
group contains one or more equivalent logical or physical nodes. For
example, one equivalancy group might contain both "a JOIN b" and "b
JOIN a" which are logically equivalent in relational algebra. During
Search, but nodes might get enumerated in order to determine in which
order to perform the join. The memo structure avoids the combinatorial
explosion of trees be naively enumerating the variants by having each
node in the tree point to child equivalency groups rather than
directly to child nodes.

Note that two nodes might be logically equivalent but have different
physical properties. For example, a table scan and an index scan can
both output the same set of columns, but provide different
orderings. Despite the different physical properties, the nodes are
placed in the same equivalency group. [TBD: I believe this description
is accurate, but lacks sufficient support. The Memo structure deserves
its own RFC before implementation.]

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

With memo, these variants share nodes. This is done by grouping
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
create logically equivalent nodes in each equivalency group, but doing
so does not require creating new parent nodes. In the above example,
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
equivalent nodes also occur for index selection.

Note that the numbering of the equivalency groups in the memo is
arbitrary, but the academic literature usually does it such that the
highest number is the root of the query.

Still TBD is when to utilize the memo structure. It is primarily
intended for search where there is a combinatorial explosion in the
number of query plans considered. But it may be useful to use it
during prep in order to avoid a conversion from the query trees used
by prep to the memo structure.

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
single expression tree that represents the current logical plan for
the query. Search is the phase of optimization where many alternative
logical and physical query plans are explored in order to find the
best physical query plan. The output of Search is a physical query
plan to execute. Note that in this context, a physical query plan
refers to a query plan for which the leaves of the tree are table
scans or index scans. DistSQL planning may be an additional step
(i.e. DistSQL physical planning would schedule processors on nodes).

In order to avoid a combinatorial explosion in the number of
expression trees, Search utilizes the Memo structure. Due to the large
number of possible plans for some queries, Search cannot explore all
of them and thus requires pruning heuristics. For example, Search can
cost query plans early and stop exploring a branch of plans if the
cost is greater than the current best cost so far.

At its core, Search is iterating over the nodes in the expression tree
and creating new equivalent nodes through transformation. The most
basic logical transformation is join order enumeration (e.g. `a JOIN
b` -> `b JOIN a`). Logical to physical transformations include
enumerating the possibilities for retrieving the necessary columns
from a table, either by a secondary index or the primary index. The
logical to physical transformation may generate glue nodes such as
sorting or distinct filtering.

The expression nodes maintained by search extend the Rewrite
expression nodes with the addition of physical properties such as
desired and provided ordering. CockroachDB already does much of this
physical property maintenance (see `physicalProps`), though it might
need to be rewritten to operate on top of the Search expression nodes.

Full featured optimizers can include hundreds of
transformations. Checking whether each transformation is applicable at
each node would be prohibitevely expensive, so the transformations are
indexed in various ways. The most basic indexing is by expression
operator (e.g. `inner join`, `group by`, etc). A transformation is
composed of a `match` function and an `apply` function. The `match`
function provides a quick yes/no answer as to whether the
transformation can be applied to an expression and returns an estimate
of the expected benefit (called a "promise" in some of the
literature). Search then applies the transformations in order of their
expected benefit, generating new expressions that are added to the
memo. Note that `match` does not need to guarantee the transformation
can apply.

The basic form of Search iteration is open to debate. In a
Cascades-style optimizer, Search maintains a stack of tasks to perform
which it initializes with an Explore Group task for the root
group. Each task can generate additional tasks that are added to the
stack. The usage of an explicit stack, rather than a recursive search,
provides flexibility in the enumeration and allows Search to be
terminated when a goal criteria (such as a deadline) is reached. Note
that processing of these optimizer tasks can be done concurrently and
it might be worthwhile to parallelize the work to reduce latency of
the optimizer.

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

* Prep. Introduce a new relational algebra expression tree (a.k.a. the
  SQL IR work). This paves the way for easier transformations during
  Rewrite and Search. There are significant engineering challenges in
  doing this work. The parser is likely to continue to use the
  existing AST, so the IR tree will have to be generated by converting
  the AST. In order to allow this work to be performed incrementally,
  the existing AST-based planning code will be left in place and a
  parallel world if IR-based planning will be erected. The output of
  IR-based planning will utilize the existing plan nodes. The
  AST-based planning code will be utilized as an escape hatch for
  queries that IR-based planning does not yet support. For the 1.2
  release, the scope would be restricted to supporting a handful of
  query types.

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

* Rewrite part 2. Implement general decorrelation of subqueries.

* Cost model part 2. Make the cost model more sophisticated by taking
  into account measurements of network bandwidth and latency.

* Search part 2. Extend the transformation set. Consider parallelizing
  search.
