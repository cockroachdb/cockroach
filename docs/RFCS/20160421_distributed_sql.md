- Feature Name: Distributing SQL queries
- Status: completed
- Start Date: 2015/02/12
- Authors: andreimatei, knz, RaduBerinde
- RFC PR: [#6067](https://github.com/cockroachdb/cockroach/pull/6067)
- Cockroach Issue:

# Table of Contents


  * [Table of Contents](#table-of-contents)
  * [Summary](#summary)
    * [Vocabulary](#vocabulary)
  * [Motivation](#motivation)
  * [Detailed design](#detailed-design)
    * [Overview](#overview)
    * [Logical model and logical plans](#logical-model-and-logical-plans)
      * [Example 1](#example-1)
      * [Example 2](#example-2)
      * [Back propagation of ordering requirements](#back-propagation-of-ordering-requirements)
      * [Example 3](#example-3)
      * [Types of aggregators](#types-of-aggregators)
    * [From logical to physical](#from-logical-to-physical)
      * [Processors](#processors)
    * [Joins](#joins)
      * [Join-by-lookup](#join-by-lookup)
      * [Stream joins](#stream-joins)
    * [Inter-stream ordering](#inter-stream-ordering)
    * [Execution infrastructure](#execution-infrastructure)
      * [Creating a local plan: the ScheduleFlows RPC](#creating-a-local-plan-the-scheduleflows-rpc)
      * [Local scheduling of flows](#local-scheduling-of-flows)
      * [Mailboxes](#mailboxes)
      * [On-the-fly flows setup](#on-the-fly-flows-setup)
    * [Retiring flows](#retiring-flows)
      * [Error handling](#error-handling)
  * [A more complex example: Daily Promotion](#a-more-complex-example-daily-promotion)
  * [Implementation strategy](#implementation-strategy)
      * [Logical planning](#logical-planning)
      * [Physical planning](#physical-planning)
      * [Processor infrastructure and implementing processors](#processor-infrastructure-and-implementing-processors)
          * [Joins](#joins-1)
      * [Scheduling](#scheduling)
      * [KV integration](#kv-integration)
    * [Implementation notes](#implementation-notes)
        * [Test infrastructure](#test-infrastructure)
        * [Visualisation/tracing](#visualisationtracing)
  * [Alternate approaches considered (and rejected)](#alternate-approaches-considered-and-rejected)
    * [More logic in the KV layer](#more-logic-in-the-kv-layer)
      * [Complexity](#complexity)
      * [Applicability](#applicability)
    * [SQL2SQL: Distributed SQL layer](#sql2sql-distributed-sql-layer)
      * [Sample high-level query flows](#sample-high-level-query-flows)
      * [Complexity](#complexity-1)
      * [Applicability](#applicability-1)
    * [Spark: Compiling SQL into a data-parallel language running on top of a distributed-execution runtime](#spark-compiling-sql-into-a-data-parallel-language-running-on-top-of-a-distributed-execution-runtime)
      * [Sample program](#sample-program)
      * [Complexity](#complexity-2)
  * [Unresolved questions](#unresolved-questions)


# Summary

In this RFC we propose a general approach for distributing SQL processing and
moving computation closer to the data. The goal is to trigger an initial
discussion and not a complete detailed design.

## Vocabulary

- KV - the KV system in cockroach, defined by its key-value, range and batch API
- k/v - a key-value pair, usually used to refer to an entry in KV
- Node - machine in the cluster
- Client / Client-side - the SQL client
- Gateway node / Gateway-side - the cluster node to which the client SQL query is delivered first
- Leader node / Leader-side - the cluster node which resolves a KV operation and
                              has local access to the respective KV data

Most of the following text reads from the gateway-side perspective, where the query parsing and planning currently runs.

# Motivation

The desired improvements are listed below.

1. Remote-side filtering

  When querying for a set of rows that match a filtering expression, we
  currently query all the keys in certain ranges and process the filters after
  receiving the data on the gateway node over the network. Instead, we want the
  filtering expression to be processed by the lease holder or remote node, saving on
  network traffic and related processing.

  The remote-side filtering does not need to support full SQL expressions - it
  can support a subset that includes common expressions (e.g. everything that
  can be translated into expressions operating on strings) with the requesting
  node applying a "final" filter.

2. Remote-side updates and deletes

  For statements like `UPDATE .. WHERE` and `DELETE .. WHERE` we currently
  perform a query, receive results at the gateway over the network, and then
  perform the update or deletion there.  This involves too many round-trips;
  instead, we want the query and updates to happen on the node which has access
  to the data.

  Again, this does not need to cover all possible SQL expressions (we can keep
  a working "slow" path for some cases). However, to cover the most important
  queries we still need more than simple filtering expressions (`UPDATE`
  commonly uses expressions and functions for calculating new values).

3. Distributed SQL operations

  Currently SQL operations are processed by the entry node and thus their
performance does not scale with the size of the cluster. We want to be able to
distribute the processing on multiple nodes (parallelization for performance).

  1. Distributed joins

    In joins, we produce results by matching the values of certain columns
    among multiple tables. One strategy for distributing this computation is
    based on hashing: `K` nodes are chosen and each of the nodes with fast
    access to the table data sends the results to the `K` nodes according to a
    hash function computed on the join columns (guaranteeing that all results
    with the same values on these columns go to the same node). Hash-joins are
    employed e.g. by F1.


    Distributed joins and remote-side filtering can be needed together:
    ```sql
    -- find all orders placed around the customer's birthday. Notice the
    -- filtering needs to happen on the results. I've complicated the filtering
    -- condition because a simple equality check could have been made part of
    -- the join.
    SELECT * FROM Customers c INNER JOIN Orders o ON c.ID = i.CustomerID
      WHERE DayOfYear(c.birthday) - DayOfYear(o.date) < 7
    ```

  2. Distributed aggregation

    When using `GROUP BY` we aggregate results according to a set of columns or
    expressions and compute a function on each group of results. A strategy
    similar to hash-joins can be employed to distribute the aggregation.

  3. Distributed sorting

    When ordering results, we want to be able to distribute the sorting effort.
    Nodes would sort their own data sets and one or more nodes would merge the
    results.

# Detailed design

## Overview

The proposed approach was originally inspired by [Sawzall][1] - a project by
Rob Pike et al.  at Google that proposes a "shell" (high-level language
interpreter) to ease the exploitation of MapReduce. Its main innovation is a
concise syntax to define “local” processes that take a piece of local data and
emit zero or more results (these get translated to Map logic); then another
syntax which takes results from the local transformations and aggregates them
in different ways (this gets translated to Reduce logic).  In a nutshell:
Sawzall = MapReduce + high-level syntax + new terminology (conveniently hiding
distributed computations behind a simple set of conceptual constructs).

We propose something somewhat similar, but targeting a different execution
model than MapReduce.

1. A predefined set of *aggregators*, performing functionality required by SQL.
   Most aggregators are configurable, but not fully programmable.
2. One special aggregator, the 'evaluator', is programmable using a very simple
   language, but is restricted to operating on one row of data at
   a time.
3. A routing of the results of an aggregator to the next aggregator in the
   query pipeline.
4. A logical model that allows for SQL to be compiled in a data-location-agnostic
   way, but that captures enough information so that we can distribute the
   computation.

Besides accumulating or aggregating data, the aggregators can feed their results
to another node or set of nodes, possibly as input for other programs. Finally,
aggregators with special functionality for batching up results and performing KV
commands are used to read data or make updates to the database.

The key idea is that we can map SQL to a well-defined logical model which we
then transform into a distributed execution plan.

[1]: http://research.google.com/archive/sawzall.html

## Logical model and logical plans

We compile SQL into a *logical plan* (similar on the surface to the current
`planNode` tree) which represents the abstract data flow through computation
stages. The logical plan is agnostic to the way data is partitioned and
distributed in the cluster; however, it contains enough information about the
structure of the planned computation to allow us to exploit data parallelism
later - in a subsequent phase, the logical plan will be converted into a
*physical plan*, which maps the abstract computation and data flow to concrete
data processors and communication channels between them.

The logical plan is made up **aggregators**. Each aggregator consumes an **input
stream** of rows (or more streams for joins, but let's leave that aside for now)
and produces an **output stream** of rows. Each row is a tuple of column values;
both the input and the output streams have a set schema.  The schema is a set of
columns and types, with each row having a datum for each column. Again, we
emphasize that the streams are a logical concept and might not map to a single
data stream in the actual computation.

We introduce the concept of **grouping** to characterize a specific aspect of
the computation that happens inside an aggregator. The groups are defined based
on a **group key**, which is a subset of the columns in the input stream schema.
The computation that happens for each group is independent of the data in the
other groups, and the aggregator emits a concatenation of the results for all
the groups. The ordering between group results in the output stream is not
fixed - some aggregators may guarantee a certain ordering, others may not.

More precisely, we can define the computation in an aggregator using a function
`agg` that takes a sequence of input rows that are in a single group (same group
key) and produces a set of output rows. The output of an aggregator is
the concatenation of the outputs of `agg` on all the groups, in some order.

The grouping characteristic will be useful when we later decide how to
distribute the computation that is represented by an aggregator: since results
for each group are independent, different groups can be processed on different
nodes. The more groups we have, the better. At one end of the spectrum there are
single-group aggregators (group key is the empty set of columns - `Group key:
[]`, meaning everything is in the same group) which cannot be distributed. At
the other end there are no-grouping aggregators which can be parallelized
arbitrarily. Note that no-grouping aggregators are different than aggregators
where the group key is the full set of columns - the latter still requires rows
that are equal to be processed on a single node (this would be useful for an
aggregator implementing `DISTINCT` for example). An aggregator with no grouping
is a special but important case in which we are not aggregating multiple pieces
of data, but we may be filtering, transforming, or reordering individual pieces
of data.

Aggregators can make use of SQL expressions, evaluating them with various inputs
as part of their work. In particular, all aggregators can optionally use an
**output filter** expression - a boolean function that is used to discard
elements that would have otherwise been part of the output stream.

(Note: the alternative of restricting use of SQL expressions to only certain
aggregators was considered; that approach makes it much harder to support outer
joins, where the `ON` expression evaluation must be part of the internal join
logic and not just a filter on the output.)

A special type of aggregator is the **evaluator** aggregator which is a
"programmable" aggregator which processes the input stream sequentially (one
element at a time), potentially emitting output elements. This is an aggregator
with no grouping (group key is the full set of columns); the processing of each
row independent. An evaluator can be used, for example, to generate new values from
arbitrary expressions (like the `a+b` in `SELECT a+b FROM ..`); or to filter
rows according to a predicate.

Special **table reader** aggregators with no inputs are used as data sources; a
table reader can be configured to output only certain columns, as needed.
A special **final** aggregator with no outputs is used for the results of the
query/statement.

Some aggregators (final, limit) have an **ordering requirement** on the input
stream (a list of columns with corresponding ascending/descending requirements).
Some aggregators (like table readers) can guarantee a certain ordering on their
output stream, called an **ordering guarantee** (same as the `orderingInfo` in
the current code). All aggregators have an associated **ordering
characterization** function `ord(input_order) -> output_order` that maps
`input_order` (an ordering guarantee on the input stream) into `output_order`
(an ordering guarantee for the output stream) - meaning that if the rows in the
input stream are ordered according to `input_order`, then the rows in the output
stream will be ordered according to `output_order`.

The ordering guarantee of the table readers along with the characterization
functions can be used to propagate ordering information across the logical plan.
When there is a mismatch (an aggregator has an ordering requirement that is not
matched by a guarantee), we insert a **sorting aggregator** - this is a
non-grouping aggregator with output schema identical to the input schema that
reorders the elements in the input stream providing a certain output order
guarantee regardless of the input ordering. We can perform optimizations wrt
sorting at the logical plan level - we could potentially put the sorting
aggregator earlier in the pipeline, or split it into multiple nodes (one of
which performs preliminary sorting in an earlier stage).

To introduce the main types of aggregators we use of a simple query.

### Example 1

```sql
TABLE Orders (OId INT PRIMARY KEY, CId INT, Value DECIMAL, Date DATE)

SELECT CID, SUM(VALUE) FROM Orders
  WHERE DATE > 2015
  GROUP BY CID
  ORDER BY 1 - SUM(Value)
```

This is a potential description of the aggregators and streams:

```
TABLE-READER src
  Table: Orders
  Table schema: Oid:INT, Cid:INT, Value:DECIMAL, Date:DATE
  Output filter: (Date > 2015)
  Output schema: Cid:INT, Value:DECIMAL
  Ordering guarantee: Oid

AGGREGATOR summer
  Input schema: Cid:INT, Value:DECIMAL
  Output schema: Cid:INT, ValueSum:DECIMAL
  Group Key: Cid
  Ordering characterization: if input ordered by Cid, output ordered by Cid

EVALUATOR sortval
  Input schema: Cid:INT, ValueSum:DECIMAL
  Output schema: SortVal:DECIMAL, Cid:INT, ValueSum:DECIMAL
  Ordering characterization:
    ValueSum -> ValueSum and -SortVal
    Cid,ValueSum -> Cid,ValueSum and Cid,-SortVal
    ValueSum,Cid -> ValueSum,Cid and -SortVal,Cid
  SQL Expressions: E(x:INT) INT = (1 - x)
  Code {
    EMIT E(ValueSum), CId, ValueSum
  }

AGGREGATOR final:
  Input schema: SortVal:DECIMAL, Cid:INT, ValueSum:DECIMAL
  Input ordering requirement: SortVal
  Group Key: []

Composition: src -> summer -> sortval -> final
```

Note that the logical description does not include sorting aggregators. This
preliminary plan will lead to a full logical plan when we propagate ordering
information. We will have to insert a sorting aggregator before `final`:
```
src -> summer -> sortval -> sort(OrderSum) -> final
```
Each arrow is a logical stream. This is the complete logical plan.

In this example we only had one option for the sorting aggregator. Let's look at
another example.


### Example 2

```sql
TABLE People (Age INT, NetWorth DECIMAL, ...)

SELECT Age, Sum(NetWorth) FROM v GROUP BY AGE ORDER BY AGE
```

Preliminary logical plan description:
```
TABLE-READER src
  Table: People
  Table schema: Age:INT, NetWorth:DECIMAL
  Output schema: Age:INT, NetWorth:DECIMAL
  Ordering guarantee: XXX  // will consider different cases later

AGGREGATOR summer
  Input schema: Age:INT, NetWorth:DECIMAL
  Output schema: Age:INT, NetWorthSum:DECIMAL
  Group Key: Age
  Ordering characterization: if input ordered by Age, output ordered by Age

AGGREGATOR final:
  Input schema: Age:INT, NetWorthSum:DECIMAL
  Input ordering requirement: Age
  Group Key: []

Composition: src -> summer -> final
```

The `summer` aggregator can perform aggregation in two ways - if the input is
not ordered by Age it will use an unordered map with one entry per `Age` and the
results will be output in arbitrary order; if the input is ordered by `Age` it can
aggregate on one age at a time and it will emit the results in age order.

Let's take two cases:

1. src is ordered by `Age` (we use a covering index on `Age`)

   In this case, when we propagate the ordering
   information we will notice that `summer` preserves ordering by age and we
   won't need to add sorting aggregators.

2. src is not ordered by anything

   In this case, summer will not have any output ordering guarantees and we will
   need to add a sorting aggregator before `final`:
   ```
   src -> summer -> sort(Age) -> final
   ```
   We could also use the fact that `summer` would preserve the order by `Age`
   and put the sorting aggregator before `summer`:
   ```
   src -> sort(Age) -> summer -> final
   ```
   We would choose between these two logical plans.

There is also the possibility that `summer` uses an ordered map, in which case
it will always output the results in age order; that would mean we are always in
case 1 above, regardless of the ordering of `src`.

### Back propagation of ordering requirements

In the previous example we saw how we could use an ordering on a table reader
stream along with an order preservation guarantee to avoid sorting. The
preliminary logical plan will try to preserve ordering as much as possible to
minimize any additional sorting.

However, in some cases preserving ordering might come with some cost; some
aggregators could be configured to either preserve ordering or not. To avoid
preserving ordering unnecessarily, after the sorting aggregators are put in
place we post-process the logical plan to relax the ordering on the streams
wherever possible. Specifically, we inspect each logical stream (in reverse
topological order) and check if removing its ordering still yields a correct
plan; this results in a back-propagation of the ordering requirements.

To recap, the logical planning has three stages:
 1. preliminary logical plan, with ordering preserved as much as possible and no
    sort nodes,
 2. order-satisfying logical plan, with sort nodes added as necessary,
 3. final logical plan, with ordering requirements relaxed where possible.

### Example 3

```sql
TABLE v (Name STRING, Age INT, Account INT)

SELECT COUNT(DISTINCT(account)) FROM v
  WHERE age > 10 and age < 30
  GROUP BY age HAVING MIN(Name) > 'k'
```

```
TABLE-READER src
  Table: v
  Table schema: Name:STRING, Age:INT, Account:INT
  Filter: (Age > 10 AND Age < 30)
  Output schema: Name:STRING, Age:INT, Account:INT
  Ordering guarantee: Name

AGGREGATOR countdistinctmin
  Input schema: Name:String, Age:INT, Account:INT
  Group Key: Age
  Group results: distinct count as AcctCount:INT
                 MIN(Name) as MinName:STRING
  Output filter: (MinName > 'k')
  Output schema: AcctCount:INT
  Ordering characterization: if input ordered by Age, output ordered by Age

AGGREGATOR final:
  Input schema: AcctCount:INT
  Input ordering requirement: none
  Group Key: []

Composition: src -> countdistinctmin -> final
```

### Types of aggregators

- `TABLE READER` is a special aggregator, with no input stream. It's configured
  with spans of a table or index and the schema that it needs to read.
  Like every other aggregator, it can be configured with a programmable output
  filter.
- `EVALUATOR` is a fully programmable no-grouping aggregator. It runs a "program"
  on each individual row. The evaluator can drop the row, or modify it
  arbitrarily.
- `JOIN` performs a join on two streams, with equality constraints between
  certain columns. The aggregator is grouped on the columns that are
  constrained to be equal. See [Stream joins](#stream-joins).
- `JOIN READER` performs point-lookups for rows with the keys indicated by the
  input stream. It can do so by performing (potentially remote) KV reads, or by
  setting up remote flows. See [Join-by-lookup](#join-by-lookup) and
  [On-the-fly flows setup](#on-the-fly-flows-setup).
- `MUTATE` performs insertions/deletions/updates to KV. See section TODO.
- `SET OPERATION` takes several inputs and performs set arithmetic on them
  (union, difference).
- `AGGREGATOR` is the one that does "aggregation" in the SQL sense. It groups
  rows and computes an aggregate for each group. The group is configured using
  the group key. `AGGREGATOR` can be configured with one or more aggregation
  functions:
  - `SUM`
  - `COUNT`
  - `COUNT DISTINCT`
  - `DISTINCT`

  `AGGREGATOR`'s output schema consists of the group key, plus a configurable
  subset of the generated aggregated values. The optional output filter has
  access to the group key and all the aggregated values (i.e. it can use even
  values that are not ultimately outputted).
- `SORT` sorts the input according to a configurable set of columns. Note that
  this is a no-grouping aggregator, hence it can be distributed arbitrarily to
  the data producers. This means, of course, that it doesn't produce a global
  ordering, instead it just guarantees an intra-stream ordering on each
  physical output streams). The global ordering, when needed, is achieved by an
  input synchronizer of a grouped processor (such as `LIMIT` or `FINAL`).
- `LIMIT` is a single-group aggregator that stops after reading so many input
  rows.
- `INTENT-COLLECTOR` is a single-group aggregator, scheduled on the gateway,
  that receives all the intents generated by a `MUTATE` and keeps track of them
  in memory until the transaction is committed.
- `FINAL` is a single-group aggregator, scheduled on the gateway, that collects
  the results of the query. This aggregator will be hooked up to the pgwire
  connection to the client.

## From logical to physical

To distribute the computation that was described in terms of aggregators and
logical streams, we use the following facts:

 - for any aggregator, groups can be partitioned into subsets and processed in
   parallel, as long as all processing for a group happens on a single node.

 - the ordering characterization of an aggregator applies to *any* input stream
   with a certain ordering; it is useful even when we have multiple parallel
   instances of computation for that logical node: if the physical input streams
   in all the parallel instances are ordered according to the logical input
   stream guarantee (in the logical plan), the physical output streams in all
   instances will have the output guarantee of the logical output stream. If at
   some later stage these streams are merged into a single stream (merge-sorted,
   i.e. with the ordering properly maintained), that physical stream will have
   the correct ordering - that of the corresponding logical stream.

 - aggregators with empty group keys (`limit`, `final`) must have their final
   processing on a single node (they can however have preliminary distributed
   stages).

So each logical aggregator can correspond to multiple distributed instances, and
each logical stream can correspond to multiple physical streams **with the same
ordering guarantees**.

We can distribute using a few simple rules:

 - table readers have multiple instances, split according to the ranges; each
   instance is processed by the raft leader of the relevant ranges and is the
   start of a physical stream.

 - streams continue in parallel through programs. When an aggregator is reached,
   the streams can be redistributed to an arbitrary number of instances using
   hashing on the group key. Aggregators with empty group keys will have a
   single physical instance, and the input streams are merged according to the
   desired ordering. As mentioned above, each physical stream will be already
   ordered (because they all correspond to an ordered logical stream).

 - sorting aggregators apply to each physical stream corresponding to the
   logical stream it is sorting. A sort aggregator by itself will *not* result
   in coalescing results into a single node. This is implicit from the fact that
   (like evaluators) it requires no grouping.

It is important to note that correctly distributing the work along range
boundaries is not necessary for correctness - if a range gets split or moved
while we are planning the query, it will not cause incorrect results. Some key
reads might be slower because they actually happen remotely, but as long as
*most of the time, most of the keys* are read locally this should not be a
problem.

Assume that we run the Example 1 query on a **Gateway** node and the table has
data that on two nodes **A** and **B** (i.e. these two nodes are masters for all
the relevant range). The logical plan is:

```
TABLE-READER src
  Table: Orders
  Table schema: Oid:INT, Cid:INT, Value:DECIMAL, Date:DATE
  Output filter: (Date > 2015)
  Output schema: Cid:INT, Value:DECIMAL
  Ordering guarantee: Oid

AGGREGATOR summer
  Input schema: Cid:INT, Value:DECIMAL
  Output schema: Cid:INT, ValueSum:DECIMAL
  Group Key: Cid
  Ordering characterization: if input ordered by Cid, output ordered by Cid

EVALUATOR sortval
  Input schema: Cid:INT, ValueSum:DECIMAL
  Output schema: SortVal:DECIMAL, Cid:INT, ValueSum:DECIMAL
  Ordering characterization: if input ordered by [Cid,]ValueSum[,Cid], output ordered by [Cid,]-ValueSum[,Cid]
  SQL Expressions: E(x:INT) INT = (1 - x)
  Code {
    EMIT E(ValueSum), CId, ValueSum
  }
```

![Logical plan](images/distributed_sql_logical_plan.png?raw=true "Logical Plan")

This logical plan above could be instantiated as the following physical plan:

![Physical plan](images/distributed_sql_physical_plan.png?raw=true "Physical Plan")

Each box in the physical plan is a *processor*:
 - `src` is a table reader and performs KV Get operations and forms rows; it is
   programmed to read the spans that belong to the respective node. It evaluates
   the `Date > 2015` filter before outputting rows.
 - `summer-stage1` is the first stage of the `summer` aggregator; its purpose is
   to do the aggregation it can do locally and distribute the partial results to
   the `summer-stage2` processes, such that all values for a certain group key
   (`CId`) reach the same process (by hashing `CId` to one of two "buckets").
 - `summer-stage2` performs the actual sum and outputs the index (`CId`) and
   corresponding sum.
 - `sortval` calculates and emits the additional `SortVal` value, along with the
   `CId` and `ValueSum`
 - `sort` sorts the stream according to `SortVal`
 - `final` merges the two input streams of data to produce the final sorted
   result.

Note that the second stage of the `summer` aggregator doesn't need to run on the
same nodes; for example, an alternate physical plan could use a single stage 2
processor:

![Alternate physical plan](images/distributed_sql_physical_plan_2.png?raw=true "Alternate physical Plan")

The processors always form a directed acyclic graph.

### Processors

Processors are generally made up of three components:

![Processor](images/distributed_sql_processor.png?raw=true "Processor")

1. The *input synchronizer* merges the input streams into a single stream of
   data. Types:
   * single-input (pass-through)
   * unsynchronized: passes rows from all input streams, arbitrarily
     interleaved.
   * ordered: the input physical streams have an ordering guarantee (namely the
     guarantee of the corresponding logical stream); the synchronizer is careful
     to interleave the streams so that the merged stream has the same guarantee.

2. The *data processor* core implements the data transformation or aggregation
   logic (and in some cases performs KV operations).

3. The *output router* splits the data processor's output to multiple streams;
   types:
   * single-output (pass-through)
   * mirror: every row is sent to all output streams
   * hashing: each row goes to a single output stream, chosen according
     to a hash function applied on certain elements of the data tuples.
   * by range: the router is configured with range information (relating to a
     certain table) and is able to send rows to the nodes that are lease holders for
     the respective ranges (useful for `JoinReader` nodes (taking index values
     to the node responsible for the PK) and `INSERT` (taking new rows to their
     lease holder-to-be)).

## Joins

### Join-by-lookup

The join-by-lookup method involves receiving data from one table and looking up
corresponding rows from another table. It is typically used for joining an index
with the table, but they can be used for any join in the right circumstances,
e.g. joining a small number of rows from one table ON the primary key of another
table. We introduce a variant of `TABLE-READER` which has an input stream. Each
element of the input stream drives a point-lookup in another table or index,
with a corresponding value in the output. Internally the aggregator performs
lookups in batches, the way we already do it today. Example:

```sql
TABLE t (k INT PRIMARY KEY, u INT, v INT, INDEX(u))
SELECT k, u, v FROM t WHERE u >= 1 AND u <= 5
```
Logical plan:
```
TABLE-READER indexsrc
Table: t@u, span /1-/6
Output schema: k:INT, u:INT
Output ordering: u

JOIN-READER pksrc
Table: t
Input schema: k:INT, u:INT
Output schema: k:INT, u:INT, v:INT
Ordering characterization: preserves any ordering on k/u

AGGREGATOR final
Input schema: k:INT, u:INT, v:INT

indexsrc -> pksrc -> final
```

Example of when this can be used for a join:
```
TABLE t1 (k INT PRIMARY KEY, v INT, INDEX(v))
TABLE t2 (k INT PRIMARY KEY, w INT)
SELECT t1.k, t1.v, t2.w FROM t1 INNER JOIN t2 ON t1.k = t2.k WHERE t1.v >= 1 AND t1.v <= 5
```

Logical plan:
```
TABLE-READER t1src
Table: t1@v, span /1-/6
Output schema: k:INT, v:INT
Output ordering: v

JOIN-READER t2src
Table: t2
Input schema: k:INT, v:INT
Output schema: k:INT, v:INT, w:INT
Ordering characterization: preserves any ordering on k

AGGREGATOR final
Input schema: k:INT, u:INT, v:INT

t1src -> t2src -> final
```

Note that `JOIN-READER` has the capability of "plumbing" through an input column
to the output (`v` in this case). In the case of index joins, this is only
useful to skip reading or decoding the values for `v`; but in the general case
it is necessary to pass through columns from the first table.

In terms of the physical implementation of `JOIN-READER`, there are two possibilities:

 1. it can perform the KV queries (in batches) from the node it is receiving the
    physical input stream from; the output stream continues on the same node.

    This is simple but involves round-trips between the node and the range
    lease holders. We will probably use this strategy for the first implementation.

 2. it can use routers-by-range to route each input to an instance of
    `JOIN-READER` on the node for the respective range of `t2`; the flow of data
    continues on that node.

    This avoids the round-trip but is problematic because we may be setting up
    flows on too many nodes (for a large table, many/all nodes in the cluster
    could hold ranges). To implement this effectively, we require the ability to
    set up the flows "lazily" (as needed), only when we actually find a row that
    needs to go through a certain flow. This strategy can be particularly useful
    when the ordering of `t1` and `t2` are correlated (e.g. t1 could be ordered
    by a date, `t2` could be ordered by an implicit primary key).


    Even with this optimization, it would be wasteful if we involve too many
    remote nodes but we only end up doing a handful of queries. We can
    investigate a hybrid approach where we batch some results and depending on
    how many we have and how many different ranges/nodes they span, we choose
    between the two strategies.


### Stream joins

The join aggregator performs a join on two streams, with equality constraints
between certain columns. The aggregator is grouped on the columns that are
constrained to be equal.

```
TABLE People (First STRING, Last STRING, Age INT)
TABLE Applications (College STRING PRIMARY KEY, First STRING, Last STRING)
SELECT College, Last, First, Age FROM People INNER JOIN Applications ON First, Last

TABLE-READER src1
Table: People
Output Schema: First:STRING, Last:STRING, Age:INT
Output Ordering: none

TABLE_READER src2
Table: Applications
Output Schema: College:STRING, First:STRING, Last:STRING
Output Ordering: none

JOIN AGGREGATOR join
Input schemas:
  1: First:STRING, Last:STRING, Age:INT
  2: College:STRING, First:STRING, Last:STRING
Output schema: First:STRING, Last:STRING, Age:INT, College:STRING
Group key: (1.First, 1.Last) = (2.First, 2.Last)  // we need to get the group key from either stream
Order characterization: no order preserved  // could also preserve the order of one of the streams

AGGREGATOR final
  Ordering requirement: none
  Input schema: First:STRING, Last:STRING, Age:INT, College:STRING
```

![Logical plan for join](images/distributed_sql_join_logical.png?raw=true "Logical plan for join")

At the heart of the physical implementation of the stream join aggregators sits
the join processor which (in general) puts all the rows from one stream in a
hash map and then processes the other stream. If both streams are ordered by the
group key, it can perform a merge-join which requires less memory.


Using the same join processor implementation, we can implement different
distribution strategies depending how we set up the physical streams and
routers:

 - the routers can distribute each row to one of multiple join processors based
   on a hash on the elements for the group key; this ensures that all elements
   in a group reach the same instance, achieving a hash-join. An example
   physical plan:

   ![Physical plan for hash join](images/distributed_sql_join_physical.png?raw=true "Physical plan for hash join")

 - the routers can *duplicate* all rows from the physical streams for one table
   and distribute copies to all processor instances; the streams for the other
   table get processed on their respective nodes. This strategy is useful when
   we are joining a big table with a small table, and can be particularly useful
   for subqueries, e.g. `SELECT ... WHERE ... AND x IN (SELECT ...)`.

   For the query above, if we expect few results from `src2`, this plan would
   be more efficient:

   ![Physical plan for dup join](images/distributed_sql_join_physical2.png?raw=true "Physical plan for dup join")

   The difference in this case is that the streams for the first table stay on
   the same node, and the routers after the `src2` table readers are configured
   to mirror the results (instead of distributing by hash in the previous case).


## Inter-stream ordering

**This is a feature that relates to implementing certain optimizations, but does
not alter the structure of logical or physical plans. It will not be part of the
initial implementation but we keep it in mind for potential use at a later
point.**

Consider this example:
```sql
TABLE t (k INT PRIMARY KEY, v INT)
SELECT k, v FROM t WHERE k + v > 10 ORDER BY k
```

This is a simple plan:

```
READER src
  Table: t
  Output filter: (k + v > 10)
  Output schema: k:INT, v:INT
  Ordering guarantee: k

AGGREGATOR final:
  Input schema: k:INT, v:INT
  Input ordering requirement: k
  Group Key: []

Composition: src -> final
```

Now let's say that the table spans two ranges on two different nodes - one range
for keys `k <= 10` and one range for keys `k > 10`. In the physical plan we
would have two streams starting from two readers; the streams get merged into a
single stream before `final`. But in this case, we know that *all* elements in
one stream are ordered before *all* elements in the other stream - we say that
we have an **inter-stream ordering**. We can be more efficient when merging
(before `final`): we simply read all elements from the first stream and then all
elements from the second stream. Moreover, we would also know that the reader
and other processors for the second stream don't need to be scheduled until the
first stream is consumed, which is useful information for scheduling the query.
In particular, this is important when we have a query with `ORDER BY` and
`LIMIT`: the limit would be represented by an aggregator with a single group,
with physical streams merging at that point; knowledge of the inter-stream
ordering would allow us to potentially satisfy the limit by only reading from
one range.

We add the concept of inter-physical-stream ordering to the logical plan - it is
a property of a logical stream (even though it refers to multiple physical
streams that could be associated with that logical stream). We annotate all
aggregators with an **inter-stream ordering characterization function** (similar
to the ordering characterization described above, which can be thought of as
"intra-stream" ordering). The inter-stream ordering function maps an input
ordering to an output ordering, with the meaning that if the physical streams
that are inputs to distributed instances of that aggregator have the
inter-stream ordering described by input, then the output streams have the given
output ordering.

Like the intra-stream ordering information, we can propagate the inter-stream
ordering information starting from the table readers onward. The streams coming
out of a table reader have an inter-stream order if the spans each reader "works
on" have a total order (this is always the case if each table reader is
associated to a separate range).

The information can be used to apply the optimization above - if a logical
stream has an appropriate associated inter-stream ordering, the merging of the
physical streams can happen by reading the streams sequentially. The same
information can be used for scheduling optimizations (such as scheduling table
readers that eventually feed into a limit sequentially instead of
concurrently).

## Execution infrastructure

Once a physical plan has been generated, the system needs to divvy it up
between the nodes and send it around for execution. Each node is responsible
for locally scheduling data processors and input synchronizers. Nodes also need
to be able to communicate with each other for connecting output routers to
input synchronizers. In particular, a streaming interface is needed for
connecting these actors. To avoid paying extra synchronization costs, the
execution environment providing all these needs to be flexible enough so that
different nodes can start executing their pieces in isolation, without any
orchestration from the gateway besides the initial request to schedule a part
of the plan.

### Creating a local plan: the `ScheduleFlows` RPC

Distributed execution starts with the gateway making a request to every node
that's supposed to execute part of the plan asking the node to schedule the
sub-plan(s) it's responsible for (modulo "on-the-fly" flows, see below). A node
might be responsible for multiple disparate pieces of the overall DAG. Let's
call each of them a *flow*. A flow is described by the sequence of physical
plan nodes in it, the connections between them (input synchronizers, output
routers) plus identifiers for the input streams of the top node in the plan and
the output streams of the (possibly multiple) bottom nodes. A node might be
responsible for multiple heterogeneous flows. More commonly, when a node is the
lease holder for multiple ranges from the same table involved in the query, it will
be responsible for a set of homogeneous flows, one per range, all starting with
a `TableReader` processor. In the beginning, we'll coalesce all these
`TableReader`s into one, configured with all the spans to be read across all
the ranges local to the node. This means that we'll lose the inter-stream
ordering (since we've turned everything into a single stream). Later on we
might move to having one `TableReader` per range, so that we can schedule
multiple of them to run in parallel.

A node therefore implements a `ScheduleFlows` RPC which takes a set of flows,
sets up the input and output mailboxes (see below), creates the local
processors and starts their execution. We might imagine at some point
implementing admission control for flows at the node boundary, in which case
the RPC response would have the option to push back on the volume of work
that's being requested.

### Local scheduling of flows

The simplest way to schedule the different processors locally on a node is
concurrently: each data processor, synchronizer and router can be run as a
goroutine, with channels between them. The channels can be buffered to
synchronize producers and consumers to a controllable degree.

### Mailboxes

Flows on different nodes communicate with each other over GRPC streams. To
allow the producer and the consumer to start at different times,
`ScheduleFlows` creates named mailboxes for all the input and output streams.
These message boxes will hold some number of tuples in an internal queue until
a GRPC stream is established for transporting them. From that moment on, GRPC
flow control is used to synchronize the producer and consumer.
A GRPC stream is established by the consumer using the `StreamMailbox` RPC,
taking a mailbox id (the same one that's been already used in the flows passed
to `ScheduleFlows`). This call might arrive to a node even before the
corresponding `ScheduleFlows` arrives. In this case, the mailbox is created on
the fly, in the hope that the `ScheduleFlows` will follow soon. If that doesn't
happen within a timeout, the mailbox is retired.
Mailboxes present a channel interface to the local processors.
If we move to a multiple `TableReader`s/flows per node, we'd still want one
single output mailbox for all the homogeneous flows (if a node has 1mil ranges,
we don't want 1mil mailboxes/streams). At that point we might want to add
tagging to the different streams entering the mailbox, so that the inter-stream
ordering property can still be used by the consumer.

A diagram of a simple query using mailboxes for its execution:
![Mailboxes](images/distributed_sql_mailboxes.png?raw=true)

### On-the-fly flows setup

In a couple of cases, we don't want all the flows to be setup from the get-go.
`PointLookup` and `Mutate` generally start on a few ranges and then send data
to arbitrary nodes. The amount of data to be sent to each node will often be
very small (e.g. `PointLookup` might perform a handful of lookups in total on
table *A*, so we don't want to set up receivers for those lookups on all nodes
containing ranges for table *A*. Instead, the physical plan will contain just
one processor, making the `PointLookup` aggregator single-stage; this node can
chose whether to perform KV operations directly to do the lookups (for ranges
with few lookups), or setup remote flows on the fly using the `ScheduleFlows`
RPC for ranges with tons of lookups. In this case, the idea is to push a bunch
of the computation to the data, so the flow passed to `ScheduleFlows` will be a
copy of the physical nodes downstream of the aggregator, including filtering
and aggregation. We imagine the processor will take the decision to move to
this heavywight process once it sees that it's batching a lot of lookups for
the same range.

## Retiring flows

Processors and mailboxes needs to be destroyed at some point. This happens
under a number of circumstances:

1. A processor retires when it receives a sentinel on all of its input streams
   and has outputted the last tuple (+ a sentinel) on all of its output
   streams.
2. A processor retires once either one of its input or output streams is
   closed. This can be used by a consumer to inform its producers that it's
   gotten all the data it needs.
3. An input mailbox retires once it has put the sentinel on the wire or once
   its GRPC stream is closed remotely.
4. An output mailbox retires once it has passed on the sentinel to the reader,
   which it does once all of its input channels are closed (remember that an
   output mailbox may receive input from many channels, one per member of a
   homogeneous flow family). It also retires if its GRPC stream is closed
   remotely.
5. `TableReader` retires once it has delivered the last tuple in its range (+ a
   sentinel).

### Error handling

At least initially, the plan is to have no error recovery (anything goes wrong
during execution, the query fails and the transaction is rolled back).
The only concern is releasing all resources taken by the plan nodes.
This can be done by propagating an error signal when any GRPC stream is
closed abruptly.
Similarly, cancelling a running query can be done by asking the `FINAL` processor
to close its input channel. This close will propagate backwards to all plan nodes.


# A more complex example: Daily Promotion

Let's draw a possible logical and physical plan for a more complex query. The
point of the query is to help with a promotion that goes out daily, targeting
customers that have spent over $1000 in the last year. We'll insert into the
`DailyPromotion` table rows representing each such customer and the sum of her
recent orders.

```SQL
TABLE DailyPromotion (
  Email TEXT,
  Name TEXT,
  OrderCount INT
)

TABLE Customers (
  CustomerID INT PRIMARY KEY,
  Email TEXT,
  Name TEXT
)

TABLE Orders (
  CustomerID INT,
  Date DATETIME,
  Value INT,

  PRIMARY KEY (CustomerID, Date),
  INDEX date (Date)
)

INSERT INTO DailyPromotion
(SELECT c.Email, c.Name, os.OrderCount FROM
      Customers AS c
    INNER JOIN
      (SELECT CustomerID, COUNT(*) as OrderCount FROM Orders
        WHERE Date >= '2015-01-01'
        GROUP BY CustomerID HAVING SUM(Value) >= 1000) AS os
    ON c.CustomerID = os.CustomerID)
```

Logical plan:

```
TABLE-READER orders-by-date
  Table: Orders@OrderByDate /2015-01-01 -
  Input schema: Date: Datetime, OrderID: INT
  Output schema: Cid:INT, Value:DECIMAL
  Output filter: None (the filter has been turned into a scan range)
  Intra-stream ordering characterization: Date
  Inter-stream ordering characterization: Date

JOIN-READER orders
  Table: Orders
  Input schema: Oid:INT, Date:DATETIME
  Output filter: None
  Output schema: Cid:INT, Date:DATETIME, Value:INT
  // TODO: The ordering characterizations aren't necessary in this example
  // and we might get better performance if we remove it and let the aggregator
  // emit results out of order. Update after the  section on backpropagation of
  // ordering requirements.
  Intra-stream ordering characterization: same as input
  Inter-stream ordering characterization: Oid

AGGREGATOR count-and-sum
  Input schema: CustomerID:INT, Value:INT
  Aggregation: SUM(Value) as sumval:INT
               COUNT(*) as OrderCount:INT
  Group key: CustomerID
  Output schema: CustomerID:INT, OrderCount:INT
  Output filter: sumval >= 1000
  Intra-stream ordering characterization: None
  Inter-stream ordering characterization: None

JOIN-READER customers
  Table: Customers
  Input schema: CustomerID:INT, OrderCount: INT
  Output schema: e-mail: TEXT, Name: TEXT, OrderCount: INT
  Output filter: None
  // TODO: The ordering characterizations aren't necessary in this example
  // and we might get better performance if we remove it and let the aggregator
  // emit results out of order. Update after the section on backpropagation of
  // ordering requirements.
  Intra-stream ordering characterization: same as input
  Inter-stream ordering characterization: same as input

INSERT inserter
  Table: DailyPromotion
  Input schema: email: TEXT, name: TEXT, OrderCount: INT
  Table schema: email: TEXT, name: TEXT, OrderCount: INT

INTENT-COLLECTOR intent-collector
  Group key: []
  Input schema: k: TEXT, v: TEXT

AGGREGATOR final:
  Input schema: rows-inserted:INT
  Aggregation: SUM(rows-inserted) as rows-inserted:INT
  Group Key: []

Composition:
order-by-date -> orders -> count-and-sum -> customers -> inserter -> intent-collector
                                                                  \-> final (sum)
```

A possible physical plan:
![Physical plan](images/distributed_sql_daily_promotion_physical_plan.png?raw=true)

# Implementation strategy

There are five streams of work. We keep in mind two initial milestones to track
the extent of progress we must achieve within each stream:
- Milestone M1: offloading filters to remote side
- Milestone M2: offloading updates to remote side

### Logical planning

Building a logical plan for a statement involves many aspects:
 - index selection
 - query optimization
 - choosing between various strategies for sorting, aggregation
 - choosing between join strategies

This is a very big area where we will see a long tail of improvements over time.
However, we can start with a basic implementation based on the existing code.
For a while we can use a hybrid approach where we implement table reading and
filtering using the new framework and make the results accessible via a
`planNode` so we can use the existing code for everything else. This would be
sufficient for M1. The next stage would be implementing the mutation aggregators
and refactoring the existing code to allow using them (enabling M2).

### Physical planning

A lot of the decisions in the physical planning stage are "forced" - table
readers are distributed according to ranges, and much of the physical planning
follows from that.

One place where physical planning involves difficult decisions is when
distributing the second stage of an aggregation or join - we could set up any
number of "buckets" (and subsequent flows) on any nodes. E.g. see the `summer`
example. Fortunately there is a simple strategy we can start with - use as many
buckets as input flows and distribute them among the same nodes. This strategy
scales well with the query size: if a query draws data from a single node, we
will do all the aggregation on that node; if a query draws data from many nodes,
we will distribute the aggregation among those nodes.

We will also support configuring things to minimize the distribution - gettting
everything back on the single gateway node as quickly as possible. This will be
useful to compare with the current "everything on the gateway" approach; it is
also a conservative step that might avoid some problems when distributing
queries between too many nodes.

A "stage 2" refinement would be detecting when a computation (and subsequent
stages) might be cheap enough to not need distribution and automatically switch
to performing the aggregation on the gateway node. Further improvements
(statistics based) can be investigated later.

We should add extended SQL syntax to allow the query author to control some of
these parameters, even if only as a development/testing facility that we don't
advertise.

### Processor infrastructure and implementing processors

This involves building the framework within which we can run processors and
implementing the various processors we need. We can start with the table readers
(enough for M1). If necessary, this work stream can advance faster than the
logical planning stream - we can build processors even if the logical plan isn't
using them yet (as long as there is a good testing framework in place); we can
also potentially use the implementations internally, hidden behind a `planNode`
interface and running non-distributed.

##### Joins

A tangential work stream is to make progress toward supporting joins (initially
non-distributed). This involves building the processor that will be at the heart
of the hash join implementation and integrating that code with the current
`planNode` tree.

### Scheduling

The problem of efficiently queuing and scheduling processors will also involve a
long tail of improvements. But we can start with a basic implementation using
simple strategies:
 - the queue ordering is by transactions; we don't order individual processors
 - limit the number of transactions for which we run processors at any one time;
   we can also limit the total number of processors running at any one time, as
   long as we allow all the processors needed for at least one txn
 - the txn queuing order is a function of the txn timestamp and its priority,
   allowing the nodes to automatically agree on the relative ordering of
   transactions, eliminating deadlock scenarios (example of a deadlock: txn A
   has some processors running on node 1, and some processors on node 2 that are
   queued behind running processors of txn B; and txn B also has some processors
   that are queued behind txn A on node 1)

We won't need anything fancier in this area to reach M1 and M2.

### KV integration

We do not propose introducing any new KV Get/Put APIs. The current APIs are to
be used; we simply rely on the fact that when run from the lease holder node they will
be faster as the work they do is local.

However, we still require some integration with the KV layer:

1. Range information lookup

   At the physical planning stage we need to break up key spans into ranges and
   determine who is the lease holder for each range. We may also use range info at the
   logical planning phase to help estimate table sizes (for index selection,
   join order, etc).  The KV layer already has a range cache that maintains this
   information, but we will need to make changes to be more aggressive in terms
   of how much information we maintain, and how we invalidate/update it.

2. Distributed reads

   There is very little in the KV layer that needs to change to allow
   distributed reads - they are currently prevented only because of a fix
   involving detecting aborted transactions (PR #5323).

3. Distributed writes

   The txn coordinator currently keeps track of all the modified keys or key
   ranges. The new sql distribution layer is designed to allow funneling the
   modified key information back to the gateway node (which acts as the txn
   coordinator). There will need to be some integration here, to allow us to
   pass this information to the KV layer. There are also likely various cases
   where checking for error cases must be relaxed.

The details of all these need to be further investigated. Only 1 and 2 are
required for M1; 3 is required for M2.

Another potential improvement is fixing the impedance mismatch between the
`TableReader`, which produces a stream, and the underlying KV range reads,
which do batch reading. Eventually we'll need a streaming reading interface for
range reads, although in the beginning we can use what we have.

## Implementation notes

Capturing various notes and suggestions.

#### Test infrastructure

Either extend the logictest framework to allow specification of additional
system attributes, or create new framework. We must have tests that can control
various settings (number of nodes, range distribution etc) and examine the
resulting query plans.

#### Visualisation/tracing

Detailed information about logical and physical plans must be available, as well
as detailed traces for all phases of queries, including execution timings,
stats, etc.

At the SQL we will have to present data, plans in textual representations. One
idea to help with visualisation is to build a small web page where we can paste
a textual representation to get a graphical display.

# Alternate approaches considered (and rejected)

We outline a few different approaches we considered but eventually decided
against.

## More logic in the KV layer

In this approach we would build more intelligence in the KV layer. It would
understand rows, and it would be able to process expressions (either SQL
expressions, or some kind of simplified language, e.g. string based).

### Complexity

Most of the complexity of this approach is around building APIs and support for
expressions. For full SQL expressions, we would need a KV-level language that
is able to read and modify SQL values without being part of the SQL layer. This
would mean a compiler able to translate SQL expressions to programs in a
KV-level VM that perform the SQL-to-bytes and bytes-to-SQL translations
explicitly (i.e. translate/migrate our data encoding routines from Go to that
KV-level VM's instructions).

### Applicability

The applicability of this technique is limited: it would work well for
filtering and possibly for remote-side updates, but it is hard to imagine
building the logic necessary for distributed SQL operations (joins,
aggregation) into the KV layer.

It seems that if we want to meet all described goals, we need to make use of a
smarter approach. With this in mind, expending any effort toward this approach
seems wasteful at this point in time. We may want to implement some of these
ideas in the future if it helps make things more efficient, but for now we
should focus on initial steps towards a more encompassing solution.


## SQL2SQL: Distributed SQL layer

In this approach we would build a distributed SQL layer, where the SQL layer of
a node can make requests to the SQL layer of any other node. The SQL layer
would "peek" into the range information in the KV layer to decide how to split
the workload so that data is processed by the respective raft range lease holders.
Achieving a correct distribution to range lease holders would not be necessary for
correctness; thus we wouldn't need to build extra coordination with the KV
layer to synchronize with range splits/merges or lease holdership changes during an
SQL operation.


### Sample high-level query flows

Sample flow of a “simple” query (select or update with filter):

| **Node A**                                 |  **Node B**  | **Node C** | **Node D** |
|--------------------------------------------|--------------|------------|------------|
| Receives statement                         |              |            |            |
| Finds that the table data spans three ranges on **B**, **C**, **D** |  |            |
| Sends scan requests to **B**, **C**, **D** |              |            |            |
|     | Starts scan (w/filtering, updates) | Starts scan (w/filtering, updates) | Starts scan (w/filtering, updates) |
|     | Sends results back to **A** | Sends results back to **A** | Sends results back to **A** |
| Aggregates and returns results.            |              |            |            |

Sample flow for a hash-join:

| **Node A**                                 |  **Node B**  | **Node C** | **Node D** |
|--------------------------------------------|--------------|------------|------------|
| Receives statement                         |              |            |            |
| Finds that the table data spans three ranges on **B**, **C**, **D** |  |            |
| Sets up 3 join buckets on B, C, D          |              |            |            |
|     | Expects join data for bucket 0 | Expects join data for bucket 1 | Expects join data for bucket 2 |
| Sends scan requests to **B**, **C**, **D** |              |            |            |
|     | Starts scan (w/ filtering). Results are sent to the three buckets in batches | Starts scan (w/ filtering) Results are sent to the three buckets in batches | Starts scan (w/ filtering). Results are sent to the three buckets in batches |
|     | Tells **A** scan is finished | Tells **A** scan is finished | Tells **A** scan is finished |
| Sends finalize requests to the buckets     |              |            |            |
|     | Sends bucket data to **A**  | Sends bucket data to **A** | Sends bucket data to **A**  |
| Returns results                            |              |            |            |

### Complexity

We would need to build new infrastructure and APIs for SQL-to-SQL. The APIs
would need to support SQL expressions, either as SQL strings (which requires
each node to re-parse expressions) or a more efficient serialization of ASTs.

The APIs also need to include information about what key ranges the request
should be restricted to (so that a node processes the keys that it is lease holder
for - or at least was, at the time when we started the operation). Since tables
can span many raft ranges, this information can include a large number of
disjoint key ranges.

The design should not be rigid on the assumption that for any key there is a
single node with "fast" access to that key. In the future we may implement
consensus algorithms like EPaxos which allow operations to happen directly on
the replicas, giving us multiple choices for how to distribute an operation.

Finally, the APIs must be designed to allow overlap between processing, network
transfer, and storage operations - it should be possible to stream results
before all of them are available (F1 goes as far as streaming results
out-of-order as they become available from storage).

### Applicability

This general approach can be used for distributed SQL operations as well as
remote-side filtering and updates. The main drawback of this approach is that it
is very general and not prescriptive on how to build reusable pieces of
functionality. It is not clear how we could break apart the work in modular
pieces, and it has the potential of evolving into a monster of unmanageable
complexity.


## Spark: Compiling SQL into a data-parallel language running on top of a distributed-execution runtime

The idea here is to introduce a new system - an execution environment for
distributed computation. The computations use a programming model like M/R, or
more pipeline stuff - Spark, or Google's [Dataflow][1] (parts of it are an
Apache project that can run on top of other execution environments - e.g.
Spark).

In these models, you think about arrays of data, or maps on which you can
operate in parallel. The storage for these is distributed. And all you do is
operation on these arrays or maps - sort them, group them by key, transform
them, filter them. You can also operate on pairs of these datasets to do joins.

These models try to have *a)* smart compilers that do symbolic execution, e.g.
fuse as many operations together as possible - `map(f, map(g, dataset)) == map(f
● g, dataset)` and *b)* dynamic runtimes. The runtimes probably look at operations
after their input have been at least partially computed and decide which nodes
participate in this current operation based on who has the input and who needs
the output. And maybe some of this work has already been done for us in one of
these open source projects.

The idea would be to compile SQL into this sort of language, considering that we
start execution with one big sorted map as a dataset, and run it.If the
execution environment is good, it takes advantage of the data topology. This is
different from "distributed sql" because *a)* the execution environment is
dynamic, so you don't need to come up with an execution plan up front that says
what node is gonna issues what command to what other node and *b)* data can be
pushed from one node to another, not just pulled.

We can start small - no distributed runtime, just filtering for `SELECTS` and
filtering with side effects for `UPDATE, DELETE, INSERT FROM SELECT`. But we
build this outside of KV; we build it on top of KV (these programs call into KV,
as opposed to KV calling a filtering callback for every k/v or row).

[1]: https://cloud.google.com/dataflow/model/programming-model

### Sample program

Here's a quick sketch of a program that does remote-side filtering and deletion
for a table with an index.

Written in a language for (what I imagine to be) Spark-like parallel operations.
The code is pretty tailored to this particular table and this particular query
(which is a good thing). The idea of the exercise is to see if it'd be feasible
at all to generate such a thing, assuming we had a way to execute it.

The language has some data types, notably maps and tuples, besides the
distributed maps that the computation is based on. It interfaces with KV through
some `builtin::` functions.

It starts with a map corresponding to the KV map, and then munges and aggregates
the keys to form a map of "rows", and then generates the delete KV commands.

The structure of the computation would stay the same and the code would be a lot
shorter if it weren't tailored to this particular table, and instead it used
generic built-in functions to encode and decode primary key keys and index keys.

```sql
TABLE t (
  id1 int
  id2 string
  a string
  b int DEFAULT NULL

  PRIMARY KEY id1, id2
  INDEX foo (id1, b)
)

DELETE FROM t WHERE id1 >= 100 AND id2 < 200 AND len(id2) == 5 and b == 77
```

```go
func runQuery() {
  // raw => Map<string,string>. The key is a primary key string - table id, id1,
  // id2, col id. This map is also sorted.
  raw = builtin::readRange("t/primary_key/100", "t/primary_key/200")

  // m1 => Map<(int, string), (int, string)>. This map is also sorted because the
  // input is sorted and the function maintains sorting.
  m1 = Map(raw, transformPK).

  // Now build something resembling SQL rows. Since m1 is sorted, ReduceByKey is
  // a simple sequential scan of m1.
  // m2 => Map<(int, string), Map<colId, val>>. These are the rows.
  m2 = ReduceByKey(m1, buildColMap)

  // afterFilter => Map<(int, string), Map<colId, val>>. Like m2, but only the rows that passed the filter
  afterFilter = Map(m2, filter)

  // now we batch up all delete commands, for the primary key (one KV command
  // per SQL column), and for the indexes (one KV command per SQL row)
  Map(afterFilter, deletePK)
  Map(afterFilter, deleteIndexFoo)

  // return the number of rows affected
  return len(afterFilter)
}

func transformPK(k, v) {
  #pragma maintainsSort  // important, keys remain sorted. So future
                         // reduce-by-key operations are efficient
  id1, id2, colId = breakPrimaryKey(k)
  return (key: {id1, id2}, value: (colId, v))
}

func breakPrimaryKey(k) {
  // remove table id and the col_id
  tableId, remaining = consumeInt(k)
  id1, remaining = consumeInt(remaining)
  id2, remaining = consumeInt(remaining)
  colId = consumeInt(remaining)
  returns (id1, id2, colId)
}

func BuildColMap(k, val) {
  colId, originalVal = val  // unpack
  a, remaining = consumeInt(originalVal)
  b, remaining = consumeString(remaining)
  // output produces a result. Can appear 0, 1 or more times.
  output (k, {'colId': colId, 'a': a, 'b': b})
}

func Filter(k, v) {
  // id1 >= 100 AND id2 < 200 AND len(id2) == 5 and b == 77
  id1, id2 = k
  if len(id2) == 5 && v.getWithDefault('b', NULL) == 77 {
    output (k, v)
  }
  // if filter doesn't pass, we don't output anything
}


func deletePK(k, v) {
  id1, id2 = k
  // delete KV row for column a
  builtIn::delete(makePK(id1, id2, 'a'))
  // delete KV row for column b, if it exists
  if v.hasKey('b') {
    builtIn::delete(makePK(id1, id2, 'b'))
  }
}

func deleteIndexFoo(k, v) {
  id1, id2 = k
  b = v.getWithDefault('b', NULL)

  builtIn::delete(makeIndex(id1, b))
}
```

### Complexity

This approach involves building the most machinery; it is probably overkill
unless we want to use that machinery in other ways than SQL.

# Unresolved questions

The question of what unresolved questions there are is, as of yet, unresolved.
