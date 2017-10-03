- Feature Name: Interleaved Table Joins
- Status: draft
- Start Date: 2017-10-03
- Authors: Richard Wu
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #18948

# Summary

We [currently permit users](https://www.cockroachlabs.com/docs/stable/interleave-in-parent.html)
to specify if a child table should be interleaved on-disk under its specified
parent table. This feature was adopted from [Google Spanner's interleaved tables](https://cloud.google.com/spanner/docs/schema-and-data-model#creating_interleaved_tables)
and was meant to be an optimization the user could opt in for tables that
are often queried in a parent-child relationship (i.e. one-to-one, one-to-many).
Refer to the [RFC on interleaved tables](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160624_sql_interleaved_tables.md) for more details.

Beyond co-locating the child's key-value (kv) pairs with the parent's kv pairs
in the underlying RocksDB storage, we do not currently take advantage of this
fact to optimize queries.

One type of query we can optimize for interleaved tables are joins between
the parent and children tables. In the context of DistSQL, instead of spawning
two `TableReader`s that scan twice, once for the parent and once for the child table,
and a `Joiner` processor, we can do one scan over the relevant interleaved
range as well as performing the join within one processor.

The performance impact can be significant considering we reduce the number
of gRPC streams between nodes and RocksDB scans.

This RFC identifies two phases of interleaved table joins:
1. Mechanism that performs the joint scan as well as the join (Processor phase)
2. Planning interleaved table joins for a given query (Planning phase)

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

## Terminology

- **logical plan**: what an SQL query is transformed into after parsing, type
  checking, name resolution, etc. A logical plan is concretely represented as
  an AST in CockroachDB.
- **physical plan**: what
  [DistSQL](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160421_distributed_sql.md)
  transforms the logical plan (AST) into. It is composed of flows which is
  composed of processors which are the functional units that do local
  processing.
- **interleaved index**: the primary or secondary index of a child table that
  is interleaved into a parent table. Interleaved secondary indexes are created
  via the `CREATE INDEX ... INTERLEAVE IN PARENT` rule.
- **interleaved table**: child table that is interleaved into its parent table
  via the `CREATE TABLE ... INTERLEAVE IN PARENT` rule. This is equivalent to
  an **interleaved primary index**.
- **interleave prefix**: the primary key (consisting of 1+ columns) of the
  parent table and its corresponding prefix of the interleaved index of the
  child table. In the [Motivation](#Motivation) example, the interleave prefix
  is `parent_id` or `parent.id`. Note the prefix may or may not be a strict
  prefix (it is possible for the child table to have no additional primary key
  columns).
- **(grand-)*children**: Kleene star expression for any subset of the set
  `{children, grand-children, grand-grand-children, ...}`
- **top-level table**: parent table that is __not__ interleaved into another
  table (it is the root in its interleaved tree) and **all its interleaved
  (grand-)*children.**.

## CockroachDB User

From a CockroachDB user perspective, there is no apparent feature change. Join
performance will be improved for interleaved tables. Since we do not employ
any significant query rewriting or optimization, the implementation of
how we recognize interleaved table joins in the Planning phase will
heavily dictate how we advertise this performance improvement.

## CockroachDB Contributor

For CockroachDB contributors and developers, changes to the codebase are mapped
to their respective phases. Firstly, it is important to understand how the
interleaved indexes are stored in RocksDB. Refer to the [RFC on interleaved
tables](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160624_sql_interleaved_tables.md).

As for how each phase of this feature affects the codebase:
1. The Processor phase will introduce an additional processor,
   `InterleavedReaderJoiner`, that functionally combines two `TableReader`s
   (correspond to each parent and child tables) and a `Joiner` (more
   specifically a `MergeJoiner` since interleave prefixes are defined with the
   parent table's ordered primary key).
    - The "reading" component of `InterleavedReaderJoiner` will scan the entire
      top-level table of the interleave relationship, filtering out the relevant
      parent and child rows. See [1] of [Rationale and
      Alternatives](#Rationale-and-Alternatives) for discussion on why this is the
      case. The reading component works in tandem with the joining component.
    - The "joining" component joins each parent row (with a unique primary key) to
      each of 0+ child rows (with a common parent prefix). This can be accomplished
      in-memory rather efficiently and streamed to the attached `RowReceiver`
      (TODO(richardwu): Is this always an outbox? I'm guessing we might need to
      attach a router (hash or pass-through) depending on the plan).
2. The Planning phase will introduce changes to the logical planning phase as
   well as branching logic in both ClassicSQL™ and DistSQL™. Note choosing to
   introduce interleaved joins at the logical level as opposed to the physical
   level is further discussed in [2] of [Rationale and
   Alternatives](#Rationale-and-Alternatives).
    - An `interleaveJoinNode` will replace sub-ASTs that consists of a `joinNode`
      root and two `scanNode` leaves where:
      - Each of the `scanNode`s correspond to a scan of the parent and child tables
      - The `joinNode` joins on the parent's primary key (or the interleave prefix)
  In the general case this could be any strict, ancestor prefix see [3] in
  [Rationale and Alternatives](#Rationale-and-Alternatives). (TODO(richardwu):
  will this always be a leaf-AST since `scanNode`s have no input and thus should
  always be leaves?)
    - ClassicSQL will treat `interleaveJoinNode` as the elided sub-AST with a
      `joinNode` root and two `scanNode`s.
    - DistSQL will physically plan an `InterleavedReaderJoiner` processor (along
      with associated helpers and routers) when an `interleaveJoinNode` is
      encountered.

Note every type of join (inner, left, right, outer) can be easily supported (see the
[Reference-level explanation section](#Reference-level-explanation)).

## Examples

For the query described above where we do a simple join between table `parent`
and table `child` where `child` is `INTERLEAVED INTO PARENT `parent`
```
SELECT * FROM child JOIN parent ON child.parent_id = parent.id
```

we previously had the logical plan:
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
which is an AST with a `joinNode` root and two `scanNode` leaves. This could
very well be a sub-AST in a more complex query. Regardless, one can transform
this logical plan into
```
+-------+-----------------+----------------+--------------------+
| Level | Type            |     Field      |    Description     |
+-------+-----------------+----------------+--------------------+
|     0 | interleavedJoin |                |                    |
|     0 |                 | childTable     | child@primary      |
|     0 |                 | parentTable    | parent@primary     |
|     0 |                 | type           | inner              |
|     0 |                 | prefix         | (parent_id)        |
+-------+-----------------+----------------+--------------------+
```

# Reference-level explanation

1. Do we use the same logic already present in the joiners (the actual
   amalgamating part)?

2. Full interleave prefixes, partial interleave prefixes

3. Secondary indexes work too!

4. How to scope the spans so they only retrieve:
  - the parent rows - use `interleaveSentinel`
  - the specific child rows (and not any other siblings or (grand-)*children)


5. Joins between siblings, (grand-)*children and parent (alternative +
unresolved question)
  - E.g. sibling1 JOIN sibling2 ON sibling1.parent_id = sibling2.parent_id
  - This could arbitrary be extended to any tables that have joins on a
    **common ancestor primary key prefix** (mind-blown) <---- key observation

6. Table will be ordered locally? since primary key of parent is ordered

7. Caveat: Can interleave rows be inserted with a foreign key to the parent
without the parent key actually existing? (see the RFC for interleaved tables)

This is the technical portion of the RFC. Explain the design in sufficient detail that:

(You may replace the section title if the intent stays clear.)

- Its interaction with other features is clear.
- It is reasonably clear how the feature would be implemented.
- Corner cases are dissected by example.

The section should return to the examples given in the previous
section, and explain more fully how the detailed proposal makes those
examples work.

## Detailed design

What / how.

Outline both "how it works" and "what needs to be changed and in which order to get there."

Describe the overview of the design, and then explain each part of the
implementation in enough detail that reviewers will be able to
identify any missing pieces. Make sure to call out interactions with
other active RFCs.

## Drawbacks

TODO(richardwu)

## Rationale and Alternatives

### [1] Scanning the entire top-level table

After discussions with @jordanlewis who has been working with indexes of interleaved
parent and child tables, a couple of key points were brought up that dictates
how we do the "reading" component of `InterleavedReaderJoiner`:
1. Currently, one cannot selectively scan all parent rows without scanning
   interleaved (grand-)*children. Similarly, one cannot selectively scan
   all the child rows without scanning its (grand-)*parents and
   (grand-)*children.
   - One suggestion by @jordanlewis is to maintain a secondary index that can
     be scanned and index joined with the primary rows of the parent and/or
     child table.  This defeats the purpose of the one-pass interleaved scan
     and join but could very well be more efficient.
     - Aside: this secondary index scan-join for interleaved tables is probably
       more efficient if we want to scan all rows for a given table in an
       interleaved relationship in a non-joining query, but the planner will
       need to know when to use it.

### [2] Logical planning > physical (DistSQL) planning

For planning interleaved table joins, one can either introduce the branching point
(that is, the point at which a special `InterleaveReadingJoiningThing` is used instead)
in the physical plan (processor-level in DistSQL) or in the logical plan (which affects
both ClassicSQL and DistSQL).

Currently ClassicSQL only scans and propogate rows in the computed spans on the data nodes
to the gateway node . Note however that `scanNode`s are
always leaves in our AST ((TODO(richardwu): are these assumptions correct?) and thus any
modification to the logic associated with these `scanNode`s can be pushed down to the local nodes. More precisely,
the `InterleaveReaderJoiner` logic described for DistSQL can theoretically be accomplished in ClassicSQL without
a huge refactoring. If we never do quite combine ClassicSQL and DistSQL into one entity,
producing a logical `interleaveJoinNode` will give us the **flexibility** to optimize both pathways.

## [3] Identifying logical interleave joins

As noted in the [Guide-level explanation section](#Guide-level-explanation), one can
identify the sub-AST that has a `joinNode` as the root and two `scanNode` leaves that correspond
to the parent and child tables. For the simple case, one can verify that the join column(s)
are on the primary key of the parent table (and a prefix of the interleaved index of the child table, but this is mandated
by interleaving anyways).

There are two general cases:

### [3a] Strict prefix joins

Joining on a strict prefix of the primary key of the parent (which is a strict prefix of both parent and child).
For example suppose we table `parent` that has primary key (PK) `(pk1, pk2, pk3)`
and table `child` that has primary key (PK) `(pk1, pk2, pk3, c_pk1)`.

The simple case we've been dealing with is a join on (exclusively) columns `pk1`, `pk2`, and `pk3`.

It was proposed that we could possibly do a join on a strict prefix of this common interleave prefix. For example, suppose we wanted to join on `pk1` and `pk2`.


### [3b] Common ancestor joins

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
and perfrom interleaved joins per each unique `parent` primary key. Reasoning:
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
a common ancestor was encountered.

## Unresolved questions

- Should we implement the planning part to take into account the general cases
  for the first iteration, or should it work with simply parent-child
    relationship? The general case may be more involved but allows for
    extensibility.

- What parts of the design do you expect to resolve through the RFC
  process before this gets merged?
- What parts of the design do you expect to resolve through the
  implementation of this feature before stabilization?
- What related issues do you consider out of scope for this RFC that
  could be addressed in the future independently of the solution that
  comes out of this RFC?
