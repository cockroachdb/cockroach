- Feature Name: Interleaved Table Joins
- Status: draft
- Start Date: 2017-10-03
- Authors: Richard Wu
- RFC PR: #19028
- Cockroach Issue: #18948

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
          * [[3b] Strict prefix joins](#3b-strict-prefix-joins)
             * [Canonical prefix join example](#canonical-prefix-join-example)
             * [Approach 1 (2-pass, one scan)](#approach-1-2-pass-one-scan)
             * [Approach 2 (2-pass, least memory, O(2k) scans)](#approach-2-2-pass-least-memory-o2k-scans)
          * [[3c] Subset prefix joins](#3c-subset-prefix-joins)
       * [[4] Multi-table interleaved joins](#4-multi-table-interleaved-joins)
       * [[5] Theta Joins](#5-theta-joins)
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
in the underlying RocksDB storage (which provides the significant optimization
of 1 phase-commit (1PC) commits for free), we do not currently take advantage
of this fact to optimize queries.

One type of query we can optimize for interleaved tables are joins between the
parent and children tables. In the context of DistSQL, instead of spawning two
`TableReader`s that scan twice, once for the parent and once for the child
table, and a `Joiner` processor, we can do one scan over the relevant
interleaved range as well as performing the join in tandem.

This optimization has significant upside on performance, potentially
lowering both the number of RocksDB scans and the volume of inter-node
gRPC traffic for the join.

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

It is worth noting how tables are represented in RocksDB, specifically
how table data is in fact stored in the primary index key-value pairs (refer to
[this blog post](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/)).

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
- **interleaved table**: child table that is interleaved into its parent table
  via the `CREATE TABLE ... INTERLEAVE IN PARENT` rule. This is concretely an
  **interleaved primary index** (see next point).
- **interleaved index**: the primary or secondary index of a child table that
  is interleaved into the parent table's primary index. Interleaved secondary
  indexes are created via the `CREATE INDEX ... INTERLEAVE IN PARENT` rule.
- **interleave prefix**: the primary key (consisting of 1+ columns) of the
  parent table and its corresponding prefix of the interleaved index of the
  child table. In the [Motivation](#motivation) example, the interleave prefix
  is `(child.parent_id)` or `(parent.id)`. Note the prefix may or may not be a
  strict prefix (it is possible for the child table to have no additional
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

## CockroachDB User

From a CockroachDB user perspective, there is no apparent feature change. Join
performance will be improved for joins between interleaved tables and their
parent tables. Since we do not employ any significant query rewriting or
optimization, the implementation of how we recognize interleaved table joins in
the Planning phase will heavily dictate how we advertise this performance
improvement.

## CockroachDB Contributor

For CockroachDB contributors and developers, changes to the codebase are mapped
to their respective phases. Firstly, it is important to understand how the
interleaved indexes are stored in RocksDB. Refer to the [RFC on interleaved
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
      be a strict prefix of the interleave prefix (see [\[3b\] Strict prefix
      joins](#3b-strict-prefix-joins)).
    - Local execution engine will carry out the join as it would without interleaved
    joins (until we decide to push down interleaved join logic to the local nodes, see
    [\[2\] Logical planning > physical planning](#2-logical-planning--physical-planning).
    - Distributed execution engine will physically plan an
      `InterleavedReaderJoiner` processor (along with associated helpers and
      routers) when a `joinNode` with interleaved join annotations is
      encountered.
2. The Processors implementation phase will introduce a set of processors,
   namely the `InterleavedReaderJoiner` processor that functionally combines two `TableReader`s
   (corresponding to each parent and child table) and a `Joiner` (more
   specifically a `MergeJoiner` since interleaved prefixes are defined with the
   parent table's __ordered__ primary key).
    - The "reading" component of `InterleavedReaderJoiner` will scan the
      interleaved hierarchy (scoped to the relevant key range if there is a filter
      on the primary key), filtering out the relevant parent and child rows.
      See [\[1\] Scanning the interleaved
      hierarchy](#1-scanning-the-interleaved-hierarchy) for discussion
      on why this is the case. The reading component works in tandem with the
      joining component.
    - The "joining" component joins each parent row (with a unique primary key) to
      each of 0+ child rows (with a common parent prefix). This can be accomplished
      in-memory rather efficiently and pushed/streamed out.
  Note every type of join (inner, left, right, outer) can be easily supported
  (see the [Reference-level explanation
  section](#Reference-level-explanation)).


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
|     0 |      | interleavedPrefix| parent_id, id      |
|     1 | scan |                  |                    |
|     1 |      | table            | child@primary      |
|     1 |      | spans            | ALL                |
|     1 | scan |                  |                    |
|     1 |      | table            | parent@primary     |
|     1 |      | spans            | ALL                |
+-------+------+------------------+--------------------+
```
The local execution engine can recognize the additional `interleavedPrefix`
field once it implements interleaved join logic whereas the distributed
execution engine can produce a physical plan with an `InterleavedReaderJoiner`
processor for `child` and `parent`.

# Reference-level explanation

This RFC will be scoped down to:
1. joins between **two** tables in the interleaved hierarchy
2. planning for interleaved joins between parent-child tables (see future work
   on [\[3a\] Common ancestor joins](#3a-common-ancestor-joins))
3. the join columns must be exactly the interleave prefix (see [\[3b\] Strict
   prefix joins](#3b-strict-prefix-joins) and future work on [\[3c\] Subset
   prefix joins](#3c-subset-prefix-joins))

Furthermore, we will discuss the two implementation phases (Planning and
Processors) separately since they can for the most part be orthogonally
implemented.

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
table joins an optimization; by precedence, we did this in the `optimizePlan` stage
of the planner [for merge joins](https://github.com/cockroachdb/cockroach/pull/17214/files)).

This can be accomplished when we `expandPlan` (within `optimizePlan`) and do a
switch on the nodes we encounter. Specifically, we'd have a helper function to
  peek into the left and right sources to see if they satisfy the conditions
  for an interleaved join. This is already being done for identifying merge
    joins and merge join order: in fact this can be a cookie cutter derivation
    of the [planning changes in merge join
    PR](https://github.com/cockroachdb/cockroach/pull/17214/files#diff-03ffd4efde59eae13665287cc1193d9a).

The conditions for an interleaved join we will check for in this first
iteration are:
1. Each of the `scanNode`s correspond to a scan of the parent and child tables
2. The `joinNode` is an **equality** join on the parent's primary key (the
   interleave prefix)

We first check if the `planNode` attach to `left` and `right` are `scanNode`s.
We then corroborate their `InterleaveDescriptor`s (nested in `IndexDescriptor` which
is nested within `TableDescriptor`). Note that `InterleaveDescriptor` does in
fact keep track of all ancestors, thus it's rather simple to do a interleaved hierarchy traversal
if we want to do [common ancestor joins](#3a-common-ancestor-joins) in the future.
If the direct parent (for the first itereation) table ID of `left` or `right`
corresponds to the table ID of the other table, then we've satisfied condition #1.

We then check if the `joinPredicate` on the join nodes have equality indices
on the (exact) columns in the interleave prefix.

The annotated plan tree is then sent off to the execution engine.

#### Local execution engine

Thankfully, not much to do here for the first iteration of this RFC!

The [idea of spawning the `InterleavedReaderJoiner` in the local execution
engine](#2-logical-planning--physical-planning) is out of this RFC's scope.

#### Distributed execution engine (DistSQL)

The bulk of the logic that will handle `joinNode`s annotated with interleaved
table join information will be in
[`createPlanForJoin`](https://github.com/cockroachdb/cockroach/blob/10e3751071c3b80540486d4a2f11c2d322501d42/pkg/sql/distsql_physical_planner.go#L1851).
If the annotation for interleaved joins exist on the join node, it will skip
[creating the individual
plans](https://github.com/cockroachdb/cockroach/blob/10e3751071c3b80540486d4a2f11c2d322501d42/pkg/sql/distsql_physical_planner.go#L1874L1878)
for the `left` and `right` `scanNode`s and `MergePlans`. Instead, they would
  be used to construct the `InterleavedReaderJoiner` processor (see [Processors
  implementation](#processors-implementation)).  Similarly, the join
  information will be similarly forwarded to a separate helper method for
  creating the `InterleavedReaderJoiner`. What we eventually
  initialize into `InterleavedReaderJoinerSpec` will depend on how we [implement
  our `InterleavedReaderJoiner`](#processors-implementation)).
  Connecting the `InterleavedReaderJoiner`
  processor will be similar to how the `HashJoiner` processor is connected.

### Processors implementation

For the first iteration, we only care about equality joins on the entire
interleave prefix. If you imagine the following interleaved hierarchy
stored in RocksDB (where each row represents a table row), where the primary
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

This is accomplished with a simple and efficient algorithm:
1. Scan the entire span covering the interleaved hierarchy
2. Ignore all rows that do not belong to `parent` or `child2`:
  a. If `parent` row: store it in memory
  b. If `child2` row: join it with the current `parent` row
  c. Emit the joined row

`InterleavedReaderJoiner` is thus broken down into two components, "reading"
and "joining".

#### Reading component

The implementation will be very similar to `tablereader.go`. There are one of two options here:
1. Refactor `tableReader` so it understands when to emit every row from the
   entire interleaved hierarchy. We can then pipe an additional processor
   `InterleavedJoiner` after it to do #2 of the algorithm (caveat, see next
   point).
2. Create a new processor `InterleavedReaderJoiner` and copy most of the logic
   from `tableReader` (for now). This is what I lean towards since if we plan
   to do 2 passes for [strict prefix joins](#3b-strict-prefix-joins) or any
   other additional logic with the rows, we should really extract this out into
   a separate processor.

With option 2 in mind, we follow the implementation of `tableReader` for inspiration.

Looking through the implementation of `RowFetcher`, specifically the call chain
```
RowFetcher --> RowFetcher.NextKey --> RowFetcher.ReadIndexKey --> DecodeIndexKey (table.go)
```
it doesn't seem like `RowFetcher` is very permissive when it comes to allow
1-pass through on an interleaved hierarchy. Specfically, `DecodeIndexKey` in
`table.go` will need to be refactored so that it returns `true` for kv pairs if
it matches either the target `parent` index or `child2` index (in fact, this
needs to be further generalized to `N` indices from `M` tables if we want to
support [multi-table interleaved joins](#4-multi-table-interleaved-joins)). Of course,
`RowFetcher` and anything in between will need to be able to pass the index and table
descriptors along so we can emit the rows we need, and thus `InterleavedReaderJoinerSpec`
will need to pass this information along.

#### Joining component

The above covers #1 and part of #2 (ignoring the rows) of the algorithm. #2a
is relatively straightforward.  #2b can be accomplished with a `joinerBase`
and invoking `render` on the stored `parent` row and the streamed `child` rows.

### Final output

There shouldn't be any problem treating the output of the `InterleavedReaderJoiner`
as if it was coming out of a `MergeJoiner`. In fact, the streams of the `InterleavedReaderJoiner`
are ordered because of the primary key join. This is however not the case with
[subset prefix joins](#3c-subset-prefix-joins).

## Drawbacks

Everything discussed in this PR is more efficient than the way we do joins currently.
This is because we do one scan over the interleaved hierarchy instead of two
separate scans for each the two table in the join.

In terms of future caveats, interleaved table joins are ill-advised if the majority
of queries are joins on a small subset of the interleaved hierarchy. In the scope of
this RFC (where we focus on joins between two tables in the interleaved hierarchy),
one should only have a parent and child table in a given interleaved hierarchy.

Once multi-table interleaved joins are implemented (see [\[4\] Multi-table
interleaved joins](#4-multi-table-interleaved-joins)), any scan queries
(whether on individual tables or joins) should be on the majority of the tables
in the interleaved hierarchy (to optimize the amount of relevant data read when
doing [the interleaved hierarchy
scan](#1-scanning-the-interleaved-hierarchy)).

All this being said, we should be __very cautious__ prescribing usage patterns
for interleaved tables. We currently do not support interleave schema changes
(?) and it would be inconsistent of us if we prescribe that interleaved
tables should only be used in parent-child relationships presently, but amend
our prescription to include deeply-nested, hierarchial relationships once we
implement, for instance, multi-table joins and common ancestor joins.

## Rationale and Alternatives

### [1] Scanning the interleaved hierarchy

After discussions with @jordanlewis who has been working with indexes of interleaved
parent and child tables, a couple of key points were brought up that will dictate
how we do the "reading" component of `InterleavedReaderJoiner`:
1. Currently, one cannot selectively scan all parent rows without scanning
   interleaved (grand-)\*children. Similarly, one cannot selectively scan
   all the child rows without scanning its (grand-)\*parents and
   (grand-)\*children.
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
precisely, the `InterleaveReaderJoiner` logic described for the distributed
execution engine can theoretically be accomplished in the local execution
engine: the local implementation spawns and runs an `InterleavedReaderJoiner`
processor and returns the results. If we never do quite fuse the local and
distributed backends into one entity, annotating a `joinNode` to perform an
interleaved join if possible will give us the **flexibility** to optimize both
pathways.

This decision to annotate logical plans to allow for more efficient joins [has
precedence when we added ordering information for merge joins to
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
parent's primary key, then we can use the `InterleaveReaderJoiner` logic.  For
example, suppose we have table `parent, child1, child2`:
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
hierarchal tree is limited since it is intuitively rare for a grandchildren
to have a foreign key reference to its grandparent. Similarly, joining
siblings on their parents' key is essentially a cross product join per each
unique primary parent key, which is hard to imagine as a common query.

#### [3b] Strict prefix joins

Strict prefix joins involves joining on a strict prefix of the primary key of
the parent (which is a strict prefix of both parent and child).

##### Canonical prefix join example

For example suppose we table `parent` that
has primary key (PK) `(pk1, pk2, pk3)` and table `child` that has primary key
(PK) `(pk1, pk2, pk3, c_pk1)`.

The simple case we've been dealing with is a join on (exclusively) columns
`pk1`, `pk2`, and `pk3`.

It was proposed that we could possibly do a join on a strict prefix of the
interleave prefix. Suppose we wanted to join on `pk1` and
`pk2`. For clarity sakes, let us imagine we have the parent key schema
```
/<pk1>/<pk2>/<pk3>/...
```
and the interleaved child key schema
```
/<pk1>/<pk2>/<pk3>/<interleaved sentinel>/<c_pk1>/...
```
where the `interleaved sentinel` is `$` (in practice, it is the first byte in
`KeyEncDescending` such that it is always sorted after the parent kv pairs).
Example kv pairs for the parent and child tables will be stored in RocksDB as
(where child kv pairs are indented)
```
...
/1/1/2/...
  /1/1/2/$/2/...
  /1/1/2/$/4/...
/1/1/3/...
  /1/1/3/$/1/...
  /1/1/3/$/3/...
  /1/1/3/$/7/...
/1/2/3/...
  /1/2/3/$/5/...
/1/2/9/...
  /1/2/9/$/5/...
  /1/2/9/$/7/...
...
```
Joining on `pk1` and `pk2` would result in the following parent-child kv pair joins
```
---------------------------------
| parent (key) | child (key)    |
---------------------------------
| /1/1/2/...   | /1/1/2/$/2/... |
| /1/1/2/...   | /1/1/2/$/4/... |
| /1/1/2/...   | /1/1/3/$/1/... |
| /1/1/2/...   | /1/1/3/$/3/... |
| /1/1/2/...   | /1/1/3/$/7/... |
| /1/1/3/...   | /1/1/2/$/2/... |
| /1/1/3/...   | /1/1/2/$/4/... |
| /1/1/3/...   | /1/1/3/$/1/... |
| /1/1/3/...   | /1/1/3/$/3/... |
| /1/1/3/...   | /1/1/3/$/7/... |
| ...          | ....           |
--------------------------------
```
and similarly for the joins where `(pk1, pk2) = (1, 2)`. Since the primary index
for the parent table is ordered, two 2-pass approaches using a merge-join
pattern e-merges here:

  TODO(richardwu): From my understanding, if one does not know the exact
  `/pk1/pk2` start and end keys (i.e. one can't tell RowFetcher/RocksDB to scan
  the first, second, etc. /pk1/pk2/ range), then one would have to scan the
  entire hierarchy. The bases for the following approaches revolve around the
  fact that we can't do separate scans for each unique `(pk1, pk2)` prefix.

##### Approach 1 (2-pass, one scan)
This approach uses
```
O(# of parent+child rows for given (pk1, pk2) prefix)
```
memory and 1 `RowFetcher`/RocksDB range scan:
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
2. Once the first key of B is encountered, we perform a scan on block A again and perform our join/emit.
3. We resume scanning block B + C.
4. Once the first key of C is encountered, we perform a scan on block B again and perform our join/emit.
5. We resume scanning block C. We reach the end of our range scan.

For `k` unique strict prefixes, this requires O(2k) scans. __Assuming every scan
is retrieved from the RocksDB cache, we only do one read from disk.__

#### [3c] Subset prefix joins

Subset prefixes are joins on a strict subset of the interleave prefix
columns that are not strict prefixes. For example, if `(pk1, pk2, pk3)` is the
interleave prefix, then
```
{(pk2), (pk3), (pk1, pk3), (pk2, pk3)}
```
is its corresponding set of strict subsets.

There is no optimized way of performing subset prefix joins (that I can think
of). If you refer to the [Canonical prefix join example](#canonical-prefix-join-example),
a join on `(pk1, pk3)` would involve some disjoint cross joins which is hard to optimize
in a general way.

Of course with any joins between two tables in a interleaved hierarchy, instead
of executing two separate `TableReader` processors and a `Joiner`, one can use
the reading component `InterleavedReaderJoiner` to read all rows in the first
pass and store them in memory, then perform `X` join. This is better than the
traditional approach where we do two full scans of the interleaved hierarchy
(see [\[1\] Scanning the entire interleaved
hierarchy](#1-scanning-the-interleaved-hierarchy)).

### [4] Multi-table interleaved joins

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
       orders.customer_id AS cus_id,
       items.*
     FROM
       orders JOIN items ON orders.id = items.order_id
    )
  ON customers.id = cus_id
```
(This is contrived since if `items` is `INTERLEAVED IN orders`, then it must also
contain `customer_id`).

Ideally, these two joins can be combined into one join with one scan of the
interleaved hierarchy.

### [5] Theta Joins

Theta joins, or joins on columns with predicates that are not all equivalence predicates,
are a little more complicated with interleaved table joins. I haven't fully
grok-ed the implications of theta joins for interleaved tables, so I'll leave it
open for now.

## Unresolved questions

- Should we implement the planning part to take into account the general cases
  for the first iteration, or should it work with simply parent-child
    relationship? The general case may be more involved but allows for
    extensibility.
- Strict prefix joins: approach #2 is equivalent to or strictly better than
  approach #1, under the assumption that RocksDB caches things properly. Is
  approach #1 the safer option since we won't know how RocksDB cache behaves in
  production (i.e. when it runs out of cache space, since the amount RocksDB
  will need to cache is far greater than what we need cached). The point of
  approach #2 is if RocksDB caches our blocks anyways, then we don't need to
  store as much in memory.
- What if the `left` and `right` nodes of the `joinNode` are not directly
  `scanNode`s, but some other "pass-through" node exists in between? I
  understand that filters are pushed down into the `scanNode`s if possible, but
  I am unfamiliar what other `planNode`s might be interplaced. This is relevant
  to how we identify interleaved table joins.  That being said, it begs the
  question: if there are nodes interplaced between the `scanNode`s and the
  `joinNode`, is there a general heuristic to push up these `scanNode`s such
  that we can use `InterleavedReaderJoiner` then apply the interplaced
  `planNode`s?
- How else can "multi-table" joins be queried (see [\[4\] Multi-table
  interleaved joins](#4-multi-table-interleaved-joins))?

