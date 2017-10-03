- Feature Name: Interleaved Table Joins
- Status: draft
- Start Date: 2017-10-03
- Authors: Richard Wu
- RFC PR: [#19028](https://github.com/cockroachdb/cockroach/pull/19028)
- Cockroach Issue: [#18948](https://github.com/cockroachdb/cockroach/issues/18948)

# Table of contents

 * [Summary](#summary)
 * [Motivation](#motivation)
 * [Guide-level explanation](#guide-level-explanation)
    * [Terminology](#terminology)
    * [CockroachDB User](#cockroachdb-user)
    * [CockroachDB Contributor](#cockroachdb-contributor)
    * [Examples](#examples)
 * [Reference-level explanation](#reference-level-explanation)
    * [Detailed design](#detailed-design)
       * [Planning implementation](#planning-implementation)
          * [Logical planning](#logical-planning)
          * [Local execution engine](#local-execution-engine)
          * [Distributed execution engine (DistSQL)](#distributed-execution-engine-distsql)
       * [Processors implementation](#processors-implementation)
          * [Reading component](#reading-component)
          * [Joining component](#joining-component)
          * [Final output](#final-output)
    * [Drawbacks](#drawbacks)
    * [Rationale and Alternatives](#rationale-and-alternatives)
       * [[1] Scanning the interleaved hierarchy](#1-scanning-the-interleaved-hierarchy)
       * [[2] Logical planning &gt; physical planning](#2-logical-planning--physical-planning)
       * [[3] Generalizing planning](#3-generalizing-planning)
          * [[3a] Common ancestor joins](#3a-common-ancestor-joins)
          * [[3b] Prefix joins](#3b-prefix-joins)
             * [Canonical prefix join example](#canonical-prefix-join-example)
             * [Approach 1 (2-pass, one scan)](#approach-1-2-pass-one-scan)
             * [Approach 2 (2-pass, least memory, O(2k) scans)](#approach-2-2-pass-least-memory-o2k-scans)
          * [[3c] Subset joins](#3c-subset-joins)
       * [[4] Multi-table joins](#4-multi-table-joins)
       * [[5] Theta Joins](#5-theta-joins)
       * [[6] Combined InterleaveReaderJoiner](#6-combined-interleavereaderjoiner)
       * [[7] Index joins](#7-index-joins)
    * [Unresolved questions](#unresolved-questions)

# Summary

We [currently permit
users](https://www.cockroachlabs.com/docs/stable/interleave-in-parent.html) to
specify if a child table should be interleaved on-disk under its specified
parent table. This feature was adopted from [Google Spanner's interleaved
tables](https://cloud.google.com/spanner/docs/schema-and-data-model#creating_interleaved_tables)
and was meant to be an optimization the user could opt in for tables that are
often queried in a parent-child relationship (i.e. one-to-one, one-to-many).
Refer to the [RFC on interleaved
tables](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160624_sql_interleaved_tables.md)
for more details.

Beyond co-locating the child's key-value (kv) pairs with the parent's kv pairs
in the underlying Key-Value (KV) storage (which provides the significant optimization
of 1 phase-commit (1PC) commits for free), we do not currently take advantage
of this fact to optimize queries.

One type of query we can optimize for interleaved tables is joins between the
parent and children tables. In the context of DistSQL, instead of spawning two
`TableReader`s, one scan for the parent and one scan for the child table for a
total of two scans, and a `Joiner` processor, we can do one scan over the
relevant interleaved rows, then perform a join on the table rows.

This optimization has significant upside on performance, potentially lowering
both the number of scans - interleaved parent and child tables are
scanned simultaneously instead of in two separate scans - and the volume of
inter-node gRPC traffic for the join - rows routed by [the hash on their
join
columns](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160421_distributed_sql.md#processors)
cross the network to their respective join processor.

This RFC identifies two implementation phases for interleaved table joins:
1. Planning interleaved table joins for a given query (Planning implementation phase)
2. Mechanism that performs the joint scan as well as the join (Processors implementation phase)

The Processors implementation phase will be carried out first in a bottom-up fashion.

# Motivation

We currently highlight [some of the
benefits](https://www.cockroachlabs.com/docs/stable/interleave-in-parent.html#benefits)
for interleaved tables such as joins, yet we do not currently execute them
differently for interleaved tables compared to non-interleaved tables. In
fact, since we do two separate table scans for the parent and child table in
an interleaved configuration, there is a performance impact to scan disjoint
kv ranges as [noted in the interleave table
docs themselves](https://www.cockroachlabs.com/docs/stable/interleave-in-parent.html#tradeoffs).

To illustrate the current behavior, let us interleave table `child` into
table `parent`:
```
CREATE DATABASE foo;

CREATE TABLE IF NOT EXISTS parent (id INT PRIMARY KEY);

CREATE TABLE IF NOT EXISTS child (id INT, parent_id INT, PRIMARY KEY (parent_id, id)) INTERLEAVE IN PARENT parent (parent_id);

EXPLAIN SELECT * FROM child JOIN parent ON child.parent_id = parent.id;
```
which results in the following logical plan
```
+-------+------+----------------+--------------------+
| Level | Type |     Field      |    Description     |
+-------+------+----------------+--------------------+
|     0 | join |                |                    |
|     0 |      | type           | inner              |
|     0 |      | equality       | (parent_id) = (id) |
|     0 |      | mergeJoinOrder | +"(parent_id=id)"  |
|     1 | scan |                |                    |
|     1 |      | table          | child@primary      |
|     1 |      | spans          | ALL                |
|     1 | scan |                |                    |
|     1 |      | table          | parent@primary     |
|     1 |      | spans          | ALL                |
+-------+------+----------------+--------------------+
```

Users expecting a performance boost for parent-child joins (even simple ones
like the above) are instead experiencing a performance hit.

# Guide-level explanation

It is worth noting how SQL rows are mapped to KV pairs, specifically how data
for a given table is represented as key-value pairs on the primary index (refer to [this blog
post](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/)).

## Terminology

- **logical plan**: what an SQL query is transformed into after parsing, type
  checking, name resolution, and query optimization. A logical plan is
  concretely represented as a (logical) plan tree (tree of `planNode`s) in
  CockroachDB.
- **physical plan**: what [the distributed execution
  engine](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160421_distributed_sql.md)
  transforms the logical plan into. It is employed to generate and schedule
  flows which are composed of processors which are the functional units that
  defer computation to the data nodes.
- **parent-child relationship**: when table `C` is interleaved into table `P`,
  then `C` is the child table of the parent table `P`. `P` and `C` have a
  parent-child relationship where `C` is a direct "descendant" of `P` (and `P`
  a direct "ancestor" of `C`).
- **interleaved table**: child table that is interleaved into its parent primary index
  via the `CREATE TABLE ... INTERLEAVE IN PARENT` rule. This is concretely an
  **interleaved primary index** (see next point).
- **interleaved index**: the primary or secondary index of a child table that
  is interleaved into the parent table's primary index. Interleaved secondary
  indexes are created via the `CREATE INDEX ... INTERLEAVE IN PARENT` rule.
- **interleave prefix**: the primary key (consisting of 1+ columns) of the
  parent table and its corresponding prefix of the interleaved index of the
  child table. In the [Motivation](#motivation) example, the interleave prefix
  is `(child.parent_id)` or `(parent.id)`. Note the prefix may or may not be a
  proper prefix (it is possible for the child table to have no additional
  primary key columns).
- **(grand-)\*children**: the set of all direct and indirect descendants of the
  specified table. That is: all tables/indexes that are interleaved together
  with the specified (ancestor) table except itself.
- **(grand-)\*parent**: similar to **(grand-)\*children** except with ancestors
  instead of descendants.
- **root table**: parent table that is __not__ interleaved into another
  table (it is the root of its interleaved hierarchy).
- **interleaved hierarchy**: root table and all its (grand-)\*children. For a
  given database, there may exist 0 or more interleaved hierarchies.
- **interleaf (pl. interleaves)**: a parent row with a primary key and all (grand-)\*children
rows with the same interleave prefix.

## CockroachDB User

From a CockroachDB user perspective, there is no apparent feature change. Join
performance will be improved for joins between interleaved tables and their
parent tables. Since we do not employ any significant query rewriting or
optimization, the implementation of how we recognize interleaved table joins in
the Planning phase will heavily dictate how we advertise this performance
improvement.

Specifically, the first iteration will permit more efficient joins between
tables with a parent-child relationship. Any table that is often joined with
a parent table (e.g. via a foreign key) should be interleaved in the the parent table.

In general, tables that are consistently subject to hierarchial querying patterns
(i.e. queried together via multi-way joins) can see improvements with interleaving.
The [current docs](https://www.cockroachlabs.com/docs/stable/interleave-in-parent.html)
do a good job highlighting the benefits and tradeoffs of interleaved tables
in the general case. See [Drawbacks](#drawbacks) for caveats on advising users
with respect to interleaved tables.

## CockroachDB Contributor

For CockroachDB contributors and developers, changes to the codebase are mapped
to their respective phases. Firstly, it is important to understand how the
interleaved indexes are mapped in storage. Refer to the [RFC on interleaved
tables](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160624_sql_interleaved_tables.md).

As for how each phase of this feature affects the codebase:
1. The Planning implementation phase will introduce changes to the logical plan
   whereby interleaved joins are acknowledged. Note choosing to introduce
   interleaved joins at the logical plan level as opposed to the physical plan
   level is further discussed in [\[2\] Logical planning > physical
   planning](#2-logical-planning--physical-planning).
    - `joinNode`s will be annotated in the logical plan if it is the root of
      a plan sub-tree, have two `scanNode` children/leaves, and:
       - Each of the `scanNode`s correspond to a scan of the parent and child
         tables
       - The `joinNode` is an **equality** join on the parent's primary key (the interleave
         prefix) (see [\[5\] Theta joins](#5-theta-joins) for inequality joins)
      In the general case, the table with the join columns could be a common
      ancestor (see [\[3a\] Common ancestor joins
      Alternatives](#3a-common-ancestor-joins)) and/or the join columns could
      be a prefix of the interleave prefix (see [\[3b\] Prefix
      joins](#3b-prefix-joins)).
    - Local execution engine will carry out the join as it would without interleaved
    joins (until we decide to push down interleaved join logic to the local nodes, see
    [\[2\] Logical planning > physical planning](#2-logical-planning--physical-planning).
    - Distributed execution engine will plan an `InterleaveReader`
      and `MergeJoiner` pair on each node (along with their routers and
      streams) that has data for the relevant span when a `joinNode` with an
      interleaved join algorithm is encountered.
2. The Processors implementation phase will introduce a new processor, the
   `InterleaveReader` processor that functionally combines two `TableReader`s
   (corresponding to each parent and child table), and plumbing to feed the
   output of `InterleaveReader` to a `MergeJoiner` (since interleave prefixes
   are defined on the parent table's primary key, there is an ordering
   gaurantee we can take advantage of):
    - The `InterleaveReader` will scan the interleaved hierarchy (scoped to the
      relevant key span if there is a filter on the primary key). It can be
      configured with the `TableDescriptor`s and `IndexDescriptor`s of the
      parent and child table such that it produces `N` output streams for each
      table (more precisely, index). `N = 2` if we want to retrieve a parent
      table's and its child table's rows. See [\[1\] Scanning the interleaved
      hierarchy](#1-scanning-the-interleaved-hierarchy) for discussion on why
      this is the case.
    - There is the caveat that each `InterleaveReader` must read full
      interleaves (and cannot partially read an interleaf): otherwise there may
      potentially be missing joined rows (see the [Reference-level explanation
      section](#Reference-level-explanation)).
    - After the rows are extracted, the joining logic is identical to that of
      `MergeJoiner`. We can thus pipe a `MergeJoiner` after every `InterleaveReader`.
    - See [Combined
      InterleaveReaderJoiner](#6-combined-interleavereaderjoiner) for a
      previous design iteration where the reader and joiner were combined into
      one processor.

Note every type of join (inner, left, right, full) can easily be supported.


## Examples

For the query described above where we do a simple join between table `parent`
and table `child` where `child` is `INTERLEAVED INTO PARENT` `parent`
```
SELECT * FROM child JOIN parent ON child.parent_id = parent.id
```

we previously had the logical plan
```
+-------+------+----------------+--------------------+
| Level | Type |     Field      |    Description     |
+-------+------+----------------+--------------------+
|     0 | join |                |                    |
|     0 |      | type           | inner              |
|     0 |      | equality       | (parent_id) = (id) |
|     0 |      | mergeJoinOrder | +"(parent_id=id)"  |
|     1 | scan |                |                    |
|     1 |      | table          | child@primary      |
|     1 |      | spans          | ALL                |
|     1 | scan |                |                    |
|     1 |      | table          | parent@primary     |
|     1 |      | spans          | ALL                |
+-------+------+----------------+--------------------+
```
which is logical plan with a `joinNode` root and two `scanNode` leaves. This
could very well be a `planNode` sub-tree in a more complex query. Regardless,
one can annotate the `joinNode` to identify the interleaved join which will
produce a query plan resembling
```
+-------+------+------------------+--------------------+
| Level | Type |     Field        |    Description     |
+-------+------+------------------+--------------------+
|     0 | join |                  |                    |
|     0 |      | type             | inner              |
|     0 |      | equality         | (parent_id) = (id) |
|     0 |      | mergeJoinOrder   | +"(parent_id=id)"  |
|     0 |      | interleavePrefix | parent_id, id      |
|     1 | scan |                  |                    |
|     1 |      | table            | child@primary      |
|     1 |      | spans            | ALL                |
|     1 | scan |                  |                    |
|     1 |      | table            | parent@primary     |
|     1 |      | spans            | ALL                |
+-------+------+------------------+--------------------+
```
The local execution engine can recognize the additional `interleavePrefix`
field once it implements interleaved join logic. The distributed
execution engine can produce a physical plan with one `InterleaveReader`
processor for the scans on `child` and `parent` that feeds into a `MergeJoiner`
on every node that contains the relevant ranges.

# Reference-level explanation

This RFC will be scoped down to:
1. joins between **two** tables in the interleaved hierarchy
2. planning for interleaved joins between parent-child tables (see future work
   on [\[3a\] Common ancestor joins](#3a-common-ancestor-joins))
3. the join columns must be exactly the interleave prefix (see future work on [\[3b\]
   Prefix joins](#3b-prefix-joins) and [\[3c\] Subset
   joins](#3c-subset-joins))

Furthermore, we will discuss the two implementation phases (Planning and
Processors) separately since they can be orthogonally implemented.

## Detailed design

### Planning implementation

There are three areas (that each vary in the amount of work required) that are
relevant to implementing the planning of interleaved table joins:
1. Logical planning & annotating `joinNode`s
2. Local execution engine (little work required)
3. Distributed execution engine & spawning appropriate flows

#### Logical planning

Annotating `joinNode`s can be accomplished after a logical plan has been
created and during optimization (since one can consider identifying interleaved
table joins an optimization; by precedent, we did this in the `optimizePlan`
stage of the planner [for merge
joins](https://github.com/cockroachdb/cockroach/pull/17214/files)). We
introduce a general `algorithmHint` field on `joinNode`s to annotate.

This can be accomplished when we `expandPlan` (within `optimizePlan`) and do type
switching on the nodes we encounter. Specifically, we'd have a helper function to
peek into the left and right sources to see if they satisfy the conditions
for an interleaved join. This is already being done for identifying merge
joins and merge join order: in fact this can be a cookie cutter derivative
of the [planning changes in the DistSQL merge join
PR](https://github.com/cockroachdb/cockroach/pull/17214/files#diff-03ffd4efde59eae13665287cc1193d9a).

The conditions for an interleaved join in this first iteration are:
1. Each of the `scanNode`s correspond to a scan of the parent and child tables
2. The `joinNode` is an **equality** join on the parent's primary key (the
   interleave prefix)

We first check if the `planNode` attach to `left` and `right` are `scanNode`s.
We then corroborate their `InterleaveDescriptor`s (nested in `IndexDescriptor` which
is nested in `TableDescriptor`). Note that `InterleaveDescriptor` does in
fact keep track of all ancestors, thus it's rather simple to do a interleaved hierarchy traversal
if we want to do [common ancestor joins](#3a-common-ancestor-joins) in the future.
If the first ancestor's (the parent) table ID of `left` or `right`
corresponds to the table ID of the other table, then we've satisfied condition #1.

Note that only a scan of the interleaved index of the child table can be
incorporated in an interleaved join. For example, if the child table's primary index
is interleaved into the parent table but the `scanNode` is a scan over one of the child's
secondary indexes, then an interleaved join is not appropriate. In the future,
we could also identify the primary index's `scanNode` under the `indexJoinNode`
and emit the child's primary index rows simultaneously. If the secondary
index is interleaved in the primary index then we can should also [optimize index joins](#7-index-joins).

We then check if the `joinPredicate` on the join nodes have equality indices
on the (exact) columns in the interleave prefix.

If both conditions are met, we set `algorithmHint` to "parent-child interleaved join".
The annotated plan tree is then sent off to the execution engine.

#### Local execution engine

Thankfully, there is not much to do here for the first iteration of this RFC!

The [idea of spawning the `InterleaveReader` in the local execution
engine](#2-logical-planning--physical-planning) is out of this RFC's scope.

#### Distributed execution engine (DistSQL)

The bulk of the logic that will setup the interleaved
join will be in
[`createPlanForJoin`](https://github.com/cockroachdb/cockroach/blob/10e3751071c3b80540486d4a2f11c2d322501d42/pkg/sql/distsql_physical_planner.go#L1851).
If the interleaved join algorithm has been selected by the logical planner, it
will skip [creating the individual
plans](https://github.com/cockroachdb/cockroach/blob/10e3751071c3b80540486d4a2f11c2d322501d42/pkg/sql/distsql_physical_planner.go#L1874L1878)
for the `left` and `right` `scanNode`s and invoking `MergePlans`. Instead, the
descriptors on the `scanNode`s  would be used to construct the
`InterleaveReader` processor (see [Processors
implementation](#processors-implementation)).  Similarly, the join
information will be forwarded to the subsequent `MergeJoiner`
processor.  What we eventually initialize into `InterleaveReaderSpec` (e.g.
`TableDescriptor`s, `IndexDescriptor`s) will depend on how we [implement the
`InterleaveReader`](#processors-implementation)).

Connecting the `InterleaveReader` to the subsequent `MergeJoiner` will be
somewhat similar to the current implementation for merge joins and
`joinNode`s in `createPlanForJoin`. The final physical plan for a three-node
cluster looks something like

![image](https://user-images.githubusercontent.com/10563314/31399738-5bbf529a-adbb-11e7-9503-bec9a3b962f4.png)

First the union of the spans from the two `scanNode`s are passed into
[`partitionSpans`](https://github.com/cockroachdb/cockroach/blob/62b7495302a8e06b4be3780e349d10d31e378bd7/pkg/sql/distsql_physical_planner.go#L488). This will
return a slice of `spanPartitions` of length `n`, which corresponds to the
number of data nodes.  We will thus eventually spawn `n` `InterleaveReader`s
and `n` `MergeJoiner`s, one for each node (`n = 3` in the diagram above).
Side note: in fact, any scan over an interleaved index will always [default to
the span of the root table in the interleaved
hierarchy](https://github.com/cockroachdb/cockroach/blob/de7337dc5ca5b4e5ee17e812c817e4bba5a449ca/pkg/sql/sqlbase/table.go#L239L243).

These spans will need to be "fixed" in order to prevent partial interleave
reads since range splits can happen in between interleaves.
If an interleaf is only partially read on a given node (for example, the parent
row is read but none of the children rows are read because they overflow into
the next span partition on a different node), then we will miss a few joined
rows. We need to first extract the parent's prefix key then call `PrefixEnd()`.
To do this, we need to figure out the cumulative. That is for a given `child` key
```
/<tableid>/<indexid>/<parent-interleave-prefix>/<interleave sentinel>/<rest-of-child-primary-key>/...
```
where the number of segments (`/.../`) for `<parent-interleave-prefix>` (i.e.
number of columns in the prefix) is `shared_prefix_len` in the child table's
`InterleaveDescriptor`.
For example, if we wanted to fix this span (generated from `partitionSpans`)
(`#` is the `<interleave sentinel>`)
```
StartKey: /parent/1/2/#/child/2
EndKey:   /parent/1/42/#/child/4
```
to
```
StartKey: /parent/1/2/#/child/2
EndKey:   /parent/1/43
```
such that we know for certain we read all `child` rows with interleave prefix
`5`, then we need to first extract `/parent/1/42` from
`/parent/1/42/#/child/4`. We can call `encoding.PeekLength()` `3` times (once
for each segment) to find the total prefix length then take the prefix of
`/parent/1/42/#/child/4` of that length.  More generally, for a descendant
`EndKey`
```
/<1st-tableid>/<1st-indexid>/<1st-index-columns>/#/<2nd-tableid>/<2nd-indexid>/2nd-index-columns/#/...
```
the number of times we call `encoding.PeekLength()` is
```
3 * count(interleave ancestors) + sum(shared_prefix_len) - 1
```
where the `- 1` is to not include the last `<interleave sentinel>`.

Instead of specifying a hash router for each `TableReader` instance to their
respective `MergeJoiner`s hashed by the equality columns, the two output
streams of the `InterleaveReader` will be mapped directly to the inputs of the
`MergeJoiner` processor. Two `OutputRouterSpec`s for `PASSTHROUGH` routers are
specified for each `InterleaveReader`. They each correspond to the output
streams for `parent` and `child` rows. This is accomplished by setting the
appropriate fields (i.e. `SourceProcessor`, `DestProcessor`, `DestInput`)
fields of `distsqlplan.Stream` and adding it to the plan. Since
`distsqlplan.Stream` current does not support multiple source outputs for a
given `SourceProcessor`, we will also need to introduce `SourceOutput int`
(similar to how `DestInput int` permits multiple input streams).

### Processors implementation

For the first iteration, we only care about equality joins on the entire
interleave prefix. If you imagine the following interleaved hierarchy
in storage (where each row represents a table row), where the primary
key of of `parent` is `pk`, the primary key of `childN` is `(pk, ckN)`,
and the interleaved index/interleave prefix is `(pk)`:
```
<pk>/.../parent_r1
  <pk>/<ck1>/.../child1_r1
    ...
  <pk>/<ck1>/.../child1_r2
  <pk>/<ck2>/.../child2_r1
  <pk>/<ck3>/.../child3_r1
  ...
<pk>/.../parent_r2
  <pk>/<ck1>/.../child1_r3
  <pk>/<ck2>/.../child2_r2
  <pk>/<ck2>/.../child2_r3
  <pk>/<ck2>/.../child2_r4
  ...
```
Let's suppose we want to join `parent` and `child2`
```
SELECT * FROM parent JOIN child2 ON parent.pk = child2.pk
```
The expected output is
```
parent_r1 U child2_r1
parent_r2 U child2_r2
parent_r2 U child2_r3
parent_r2 U child2_r4
...
```

We can scan top to bottom of the interleaved hierarchy and separate out
`parent_rN` rows and `child2_rN` rows into two streams.

#### Reading component

The implementation of `InterleaveReader` will be very similar to `tablereader.go` except
that we have branching logic to emit rows to `N` streams for `N` table/index combinations (where
`N = 2` in the simple parent-child case).

Looking through the implementation of `RowFetcher`, specifically the call chain
```
RowFetcher --> RowFetcher.NextKey --> RowFetcher.ReadIndexKey --> DecodeIndexKey (table.go)
```
it doesn't seem like `RowFetcher` is very permissive when it comes to allow
1-pass through on an interleaved hierarchy. Specfically, [`DecodeIndexKey` in
`table.go`](https://github.com/cockroachdb/cockroach/blob/de7337dc5ca5b4e5ee17e812c817e4bba5a449ca/pkg/sql/sqlbase/table.go#L778L780)
 will need to be refactored so that it returns `true` for kv pairs if
it matches either the target `parent` index or `child2` index. This is
generalized to a set membership problem with a set of `N` `(tableID, indexID)` tuples if we
emit multiple interleaved tables in one scan or to
support [multi-table joins](#4-multi-table-joins)).

Of course, `RowFetcher` and anything in between will need to be able to pass
the set of `N` `(TableID, IndexID)` tuples along so we can emit the rows we need, and thus
`InterleaveReaderSpec` will need to pass this information along.

The refactor necessary is as follows (grouped by component):
1. A new `MultiRowFetcher` that permits `n` `TableDescriptor`s,
   `IndexDescriptors` and other fields within the current `RowFetcher`. `NextRow` will
   need to be able to return the `(TableID, IndexID)` corresponding to the row that's
   retrieved.
   - Instead of trying to refactor `RowFetcher` and introducing additional
     overhead, it is prudent to separate this `MultiRowFetcher` for
     `InterleaveReader`.
2. `DecodeMultiIndexKey` in `table.go` that takes a set of `(TableID, IndexID)`
   tuples for determining whether or not the key is any of the `N` indexes we
   want.

Since `InterleaveReader` "owns" its `N` output streams (rather than letting
some `Router` message broker direct the output), a similar buffer logic will be
[required to prevent
deadlocks](https://github.com/cockroachdb/cockroach/issues/17097). The deadlock happens
when `MergeJoiner` (`streamMerger` to be precise) tries to [batch all rows from both sides with the
current join column
values](https://github.com/cockroachdb/cockroach/blob/70c96175bb5b60bf6d531ab3bcebd2bb723d0bc7/pkg/sql/distsqlrun/stream_merger.go#L37#L45).
It will try to retrieve parent rows until it sees that the values for the join
columns increase (in order to form a batch of parent rows with the same join
column values). However, this would not be possible if the next row after the
parent row is a child row since the `InterleavedReader` reads top to bottom. Thus
we would need to buffer enough child rows until `streamMerger` can get the next
parent row which has a different primary key.

```
parent_r1    <- first row in parent batch, NextRow until parent row with a different primary key
  child_r1   <- need to buffer since NextRow is called
  child_r2   <- buffered
  child_r3   <- buffered
parent_r2    <- parent row with a different primary key, finish parent batch
                and move on to child batch (child_r1 - r3 is unblocked and sent)
```

In the case of a join on the entire interleave prefix, buffering is __okay__
since there is at most 1 parent row for 0+ child rows. We would thus buffer at
most 1 child row in order to retrieve the next parent row (that has a larger
primary key, finishing off the batch). In the case where we do a [prefix
join](3b-prefix-joins), parent rows are separated by blocks of 0+ child rows,
thus we will need to buffer the child rows in order to retrieve all the parent
rows for a given batch.

This buffering logic needs to be [abstracted from
`routerBase`](https://github.com/cockroachdb/cockroach/blob/de7337dc5ca5b4e5ee17e812c817e4bba5a449ca/pkg/sql/distsqlrun/routers.go#L160L164)
and imported into `InterleaveReader`.

#### Joining component

The actual joining logic in `MergeJoiner` should be agnostic to whether the
rows came from two `TableReader`s or one `InterleaveReader`.

One of the drawbacks of delegating the joining logic to a generic `MergeJoiner`
is, in the simple case, having to buffer batches of child rows even though we
know there is only one parent row to join and we can stream these joined rows.

For example, the second batch for `parent` and `child2` looks like
```
<parent batch>  <child batch>
parent_r2       child2_r2
                child2_r3
                child2_r4
```
Instead of buffering the entire batch, we know for certainty that
we need only buffer the one parent row and stream-join the child rows.

One way we can fix this is to refactor `streamMerger` to recognize
that if either the `left` or `right` batch has one row, we can stream join
the other row.

For the scope of the first iteration, we will defer this optimization.

If we wanted to perform [multi-table joins](#4-multi-table-joins) efficiently,
we could consider an `InterleaveJoiner` (or `MultiMergeJoiner`) that can merge
`N` arbitrary batches.

## Drawbacks

Everything discussed in this PR is more efficient than the way we do joins currently.
This is because we do one scan over the interleaved hierarchy instead of two
separate scans for each the two table in the join.

In terms of future caveats, interleaved table joins are ill-advised if the majority
of queries are joins on a small subset of the interleaved hierarchy. In the scope of
this RFC (where we focus on joins between two tables in the interleaved hierarchy),
one should only have a parent and child table in a given interleaved hierarchy.

Once multi-table interleaved joins are implemented (see [\[4\] Multi-table
joins](#4-multi-table-joins)), any scan queries
(whether on individual tables or joins) should be on the majority of the tables
in the interleaved hierarchy (to optimize the amount of relevant data read when
doing [the interleaved hierarchy
scan](#1-scanning-the-interleaved-hierarchy)).

All this being said, we should be __very cautious__ prescribing usage patterns
for interleaved tables. We currently do not support interleave schema changes
(i.e. un-interleaving a table from its parent). It would be inconsistent of
us if we prescribe that interleaved tables should only be used in parent-child
relationships presently, but amend our prescription to include deeply-nested,
hierarchial relationships once we implement multi-table joins and common
ancestor joins.

## Rationale and Alternatives

### [1] Scanning the interleaved hierarchy

After discussions with @jordanlewis who has been working with indexes of interleaved
parent and child tables, a couple of key points were brought up that will dictate
how we do the reading component of `InterleaveReader`:
1. Currently, one cannot selectively scan all parent rows without scanning
   interleaved (grand-)\*children. Similarly, one cannot selectively scan
   all the child rows without scanning its (grand-)\*parents and
   (grand-)\*children.
   - For example, a query for all rows of table `parent` with primary key
     between `2` and `42` might look up the span `parent@primary /2-/43`. This
     scan at the `KVFetcher` level iterates through all `child` rows and
     [decodes their index
     keys](https://github.com/cockroachdb/cockroach/blob/62b7495302a8e06b4be3780e349d10d31e378bd7/pkg/sql/sqlbase/rowfetcher.go#L154#L155)
     with interleave prefix also between `2` and `42`. It then decides whether
     or not the `child` row is a `parent` row via a [comparison on its
     `TableID` and
     `IndexID`](https://github.com/cockroachdb/cockroach/blob/62b7495302a8e06b4be3780e349d10d31e378bd7/pkg/sql/sqlbase/table.go#L778L780).
   - One key observation by @jordanlewis is that a secondary index can
     be scanned and index joined with the primary rows of the parent and/or
     child table to obtain a scan of each table.
     - This secondary index should not be maintained automatically: a user
       should create this secondary index if need be.
     - The planner will be need to know when to use this secondary index and
       an index join to perfrom these types of scans: index selection can be
       augmented to impose a penalty for scans over parent interleaves.
     - Benchmark opportunity: secondary indexes do incur a small cost on
       `INSERT/UPDATE`s. They are also located on separate ranges (and possibly
       separate machines) which will require sending rows over the wire to the
       `Joiner`s. This is potentially worse than doing an entire hierarchy
       scan and filtering for relevant rows.
2. If a filter is given for the parent primary key, then we can scope the scan.
   This however still picks up all the (grand-)\*children rows of the parent.

### [2] Logical planning > physical planning

For planning interleaved table joins, one can either introduce the branching
point (that is, the point at which an `InterleavedRaederJoiner` is introduced)
in the physical plan (processor-level in the distributed execution engine) or
in the logical plan (which affects both execution engines).

Currently the local execution engine only scans and propagate rows in the
computed spans on the data nodes to the gateway node. Note however that these
scans occur at the leaves of plan tree (`scanNode`s are always leaves). More
precisely, the `InterleaveReader + MergeJoiner` logic described for the
distributed execution
engine can theoretically be accomplished in the local execution engine: the
local implementation spawns and runs an `InterleaveReader` processor and
`MergeJoiner` processor and returns the joined results. If we never do quite
fuse the local and distributed backends into one entity, annotating a
`joinNode` to perform an interleaved join if possible will give us the
**flexibility** to optimize both pathways.

This decision to annotate logical plans to allow for more efficient joins [has
precedent when we added ordering information for merge joins to
`joinNode`s](https://github.com/cockroachdb/cockroach/pull/17214) for the
distributed execution engine.

### [3] Generalizing planning

As noted in the [Guide-level explanation section](#Guide-level-explanation),
one can identify the plan tree pattern that has a `joinNode` as the root and
two `scanNode` leaves that correspond to the parent and child tables. For the
simple case, one can verify that the join column(s) are on the primary key of
the parent table (and a prefix of the interleaved index of the child table, but
this is mandated by `INTERLEAVE` anyways), but this can of course be
generalized.

#### [3a] Common ancestor joins

When we have a parent and direct child relationship and join, we really have a
join on a common ancestor's primary key (the parent) between the parent and the
child.  If we imagine the interleave relationships for a given database as a
forest where every top-level parent is the root of a tree (and these trees can
be arbitrarily deep), then a **common ancestor** for two nodes `A` and `B` is
defined as as node whose subtrees contain `A` and `B`.

Imagine that we wanted to perform a join between two children tables on their
parent's primary key. For example, suppose we have table `parent, child1,
child2`:
```
CREATE TABLE parent (id INT PRIMARY KEY);

CREATE TABLE child1 (id1 INT, pid INT, PRIMARY KEY (pid, id1)) INTERLEAVE IN PARENT parent (pid);

CREATE TABLE child2 (id2 INT, pid INT, PRIMARY KEY (pid, id2)) INTERLEAVE IN PARENT parent (pid);

SELECT * FROM child1 JOIN child2 ON child1.pid = child2.pid;
```
The forest representation (with one tree) looks like
```
      parent
       / \
     /     \
   /         \
child1     child2
```
It is complete and sound to scan the interleaved parts of `child1` and `child2`
and perform interleaved joins per each unique `parent` primary key. Reasoning:
all `child1` rows with interleave prefix `X` are interleaved under the `parent`
row with primary key `X` by `INTERLEAVE` constraint.  Similarly all `child2`
rows with `X` prefix are interleaved under the same `parent` row.  Therefore,
all `child1` and `child2` rows with equivalent `pid`s are nested under the same
interleave block and thus will be joined.

This concept can be aribitrarily extended to any common ancestor primary key
join so long as the join is exactly on the common ancestor's primary key. Since
the nesting depths of interleaved tables are typically no more than 1-4 (if
there is very deep nesting, then we'll have other issues to worry about), we
can simply traverse up the connected tree until we reach the root and check if
a common ancestor was encountered (traverse up instead of down from either
node since `InterleaveDescriptor` only keeps track of ancestors, not
descendants).

The use cases for a join on an ancestor more than one level higher in the
hierarchal tree is limited since it is intuitively rare for a grandchildren to
have a foreign key reference to its grandparent. Similarly, joining siblings on
their parents' key is essentially a cross product join per each unique primary
parent key, which is hard to imagine as a common query. A multi-table join
between parent-child-grandchild is conceivable (see [Multi-table
joins](#4-multi-table-joins)).

#### [3b] Prefix joins

Prefix joins involves joining on a proper prefix of the primary key of
the parent (which is a prefix of the interleave prefix).

##### Canonical prefix join example

For example suppose we table `parent` that
has primary key (PK) `(pk1, pk2, pk3)` and table `child` that has primary key
(PK) `(pk1, pk2, pk3, c_pk1)`.

The simple case we've been dealing with is a join on (exclusively) columns
`pk1`, `pk2`, and `pk3`.

It was proposed that we could possibly do a join on a prefix of the
interleave prefix. Suppose we wanted to join on `pk1` and
`pk2`. For clarity sakes, let us imagine we have the parent key schema
```
/<pk1>/<pk2>/<pk3>/...
```
and the interleaved child key schema
```
/<pk1>/<pk2>/<pk3>/<interleaved sentinel>/<c_pk1>/...
```
where the `interleaved sentinel` is `#` (in practice, it is the first byte in
`KeyEncDescending` such that it is always sorted after the parent kv pairs).
Example kv pairs (each representing an entire table row) for the parent and
child tables will be stored as (where child rows are indented)
```
...
/1/1/2/...
  /1/1/2/#/2/...
  /1/1/2/#/4/...
/1/1/3/...
  /1/1/3/#/1/...
  /1/1/3/#/3/...
  /1/1/3/#/7/...
/1/2/3/...
  /1/2/3/#/5/...
/1/2/9/...
  /1/2/9/#/5/...
  /1/2/9/#/7/...
...
```
Joining on `pk1` and `pk2` would result in the following parent-child kv pair joins
```
---------------------------------
| parent (key) | child (key)    |
---------------------------------
| /1/1/2/...   | /1/1/2/#/2/... |
| /1/1/2/...   | /1/1/2/#/4/... |
| /1/1/2/...   | /1/1/3/#/1/... |
| /1/1/2/...   | /1/1/3/#/3/... |
| /1/1/2/...   | /1/1/3/#/7/... |
| /1/1/3/...   | /1/1/2/#/2/... |
| /1/1/3/...   | /1/1/2/#/4/... |
| /1/1/3/...   | /1/1/3/#/1/... |
| /1/1/3/...   | /1/1/3/#/3/... |
| /1/1/3/...   | /1/1/3/#/7/... |
| ...          | ....           |
--------------------------------
```
and similarly for the joins where `(pk1, pk2) = (1, 2)`.

**UPDATE: Since this section was tailored to a custom `InterleaveJoiner`
instead of the current proposal of a `MergeJoiner`, the approaches discussed
here are irrelevant and are only kept for posterity should we revert back to a custom
`InterleaveJoiner`**.

Since the primary index
for the parent table is ordered, two 2-pass approaches using a merge-join
pattern e-merges here:

  TODO(richardwu): From my understanding, if one does not know the exact
  `/pk1/pk2` start and end keys (i.e. one can't tell RowFetcher/kvFetcher to scan
  the first, second, etc. /pk1/pk2/ range), then one would have to scan the
  entire hierarchy. The bases for the following approaches revolve around the
  fact that we can't do separate scans for each unique `(pk1, pk2)` prefix.

##### Approach 1 (2-pass, one scan)
This approach uses
```
O(# of parent+child rows for given (pk1, pk2) prefix)
```
memory and 1 span scan:
1. Scan top to bottom through the entire interleaved hierarchy
2. For every new `(pk1, pk2)` prefix:
   - `NextRow()` and store parent/child row in their respective buffers until
     we see a new `(pk1, pk2)` prefix
   - Perform a full (cross product) join between all parent and child rows,
     emitting newly joined rows

##### Approach 2 (2-pass, least memory, O(2k) scans)
If the memory requirements of approach 1 becomes too unwieldy, one can consider
storing either the child rows or parent rows in one buffer (deciding which will
require table statistics or a general heuristic). This requires
```
min{
  O(# of child rows for given (pk1, pk2) prefix),
  O(# of parent rows for given (pk1, pk2) prefix),
}
```
memory. WLOG, let's store the parent rows in our buffer.  **This approach
relies on the assumption that RocksDB can efficiently cache the requested
span and subsequent overlapping scans of the initial span. @petermattis notes
that RocksDB should be able to cache these large blocks of SSTables**:
1. Initialize span as the entire interleaved hierarchy
2. Scan through the rows for a given `(pk1, pk2)` prefix:
   - For every parent row, store in buffer
   - For every child row, discard
   - Once a new `(pk1, pk2)` prefix is encountered, mark the current
   `(pk1, pk2)` prefix for the next scan
3. Scan through the the current `(pk1, pk2)` span again
   - Join each encountered child row with the cached parent rows and emit
4. Begin a new scan with the next `(pk1, pk2)` prefix (go to #2)

To illustrate, let's assume that there are **three** unique `(pk1, pk2)` prefixes. This
forms three blocks of our overall span: block A, B, and C.
1. The first scan is over the entire span (A + B + C).
2. Once the first key of B is encountered, we perform a scan on block A again
   and perform our join/emit.
3. We resume scanning block B + C.
4. Once the first key of C is encountered, we perform a scan on block B again
   and perform our join/emit.
5. We resume scanning block C. We reach the end of our span scan.

For `k` unique prefixes, this requires O(2k) scans. __Assuming every scan
is retrieved from the RocksDB cache, we only do one read from disk.__

#### [3c] Subset joins

Subset joins are joins on a strict subset of the interleave prefix
columns that is not a prefix itself. For example, if `(pk1, pk2, pk3)` is the
interleave prefix, then
```
{(pk2), (pk3), (pk1, pk3), (pk2, pk3)}
```
is its corresponding set of subsets.

There is no optimized way __specific to interleaved tables__ to performing subset
joins (that I can think of). If you refer to the [Canonical prefix join
example](#canonical-prefix-join-example), a join on `(pk1, pk3)` would involve
some disjoint cross joins which is hard to optimize in a general way. We could
of course use a hybrid merge-hash join for a join on `(pk1, pk3)`: we'd perform
a hash-join (hashed on `pk3`) on all rows for each `pk1` (see [this
issue for more details](https://github.com/cockroachdb/cockroach/issues/16580)).

### [4] Multi-table joins

For a given interleaved hierarchy, we might fold in multiple joins from
more than two tables in the hierarchy. Suppose we have the hierarchy
```
customers
  orders
    items
```
We may want retrieve all `items` that `customers` have purchased joined with
their customer information (stored in `customers`). The corresponding
(contrived) query might look like
```
SELECT * FROM
  customers
  JOIN
    (SELECT
       *
     FROM
       orders JOIN items ON orders.id = items.order_id
    )
  ON customers.id = orders.cus_id
```

Ideally, this three-table join be combined into one join with one scan of the
interleaved hierarchy. `InterleaveReader` will have `N = 3` output streams (one for
each table) which feeds into `N = 3` inputs in some `InterleaveJoiner` that performs a
merge join on `N = 3` tables simultaneously (this is essentially a 3-way cross product,
which can be implemented with 3 loops nested in a hierarchy).

### [5] Theta Joins

Theta joins, or joins on columns with predicates that are not all equivalence predicates,
are a little more complicated with interleaved table joins. I haven't fully
grok-ed the implications of theta joins for interleaved tables, so I'll leave it
open for now.

### [6] Combined InterleaveReaderJoiner

After some initial discussion in the RFC PR, I came to the realization that
the goal of this RFC is to avoid scanning the entire interleaved hierarchy twice
with two independent `TableReader` processors and simply have some `InterleaveReader`
that emits both `parent` and `child` rows in the simple case. The joining logic
is identical to that of `MergeJoiner`.

In fact, the [prefix](#3b-prefix-joins) and [subset joins](#3c-subset-joins)
are general cases for `MergeJoiner` that is current being tracked by [this
issue](https://github.com/cockroachdb/cockroach/issues/16580).

### [7] Index joins

Although we [do not publicly advertise](https://www.cockroachlabs.com/docs/stable/interleave-in-parent.html)
interleaving secondary indexes under primary indexes, this is possible
with the syntax
```
CREATE INDEX secondary_idx ON foo (id, data) INTERLEAVE IN PARENT foo (id)
```
We can thus identify interleaved joins on `indexJoinNode`s (where `left` and `right`
`scanNode`s in `joinNode`s are now `index` and `table` `scanNode`s in `indexJoinNode`s)
and perform efficient index joins. The planning and processor implementation
for index joins is symmetric to that of regular joins.

## Unresolved questions

- Should we implement the planning part to take into account the general cases
  for the first iteration, or should it work with simply parent-child
    relationship? The general case may be more involved but allows for
    extensibility.
- ~~Prefix joins: approach #2 is equivalent to or strictly better than
  approach #1, under the assumption that RocksDB caches things properly. Is
  approach #1 the safer option since we won't know how RocksDB cache behaves in
  production (i.e. when it runs out of cache space, since the amount RocksDB
  will need to cache is far greater than what we need cached). The point of
  approach #2 is if RocksDB caches our blocks anyways, then we don't need to
  store as much in memory.~~ This is irrelevant now that we plan to
  use a `MergeJoiner` to perform the joining logic.
- What if the `left` and `right` nodes of the `joinNode` are not directly
  `scanNode`s, but some other "pass-through" node exists in between? I
  understand that filters are pushed down into the `scanNode`s if possible, but
  I am unfamiliar what other `planNode`s might be interplaced. This is relevant
  to how we identify interleaved table joins.  That being said, it begs the
  question: if there are nodes interplaced between the `scanNode`s and the
  `joinNode`, is there a general heuristic to push up these `scanNode`s such
  that we can use an `InterleaveReader` then apply the effects of the elided
  `planNode`s?
- How else can [multi-table joins](#4-multi-table-joins) be queried?

