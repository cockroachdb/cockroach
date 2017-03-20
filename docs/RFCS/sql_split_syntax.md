- Feature Name: SQL Syntax for splitting tables
- Status: draft/in-progress/completed/rejected/obsolete
- Start Date: 2017-03-14
- Authors: Radu Berinde
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #13665

# Summary

This RFC proposes new syntax for introducing table splits and relocating ranges via SQL.

Specifically, we want to be able to:
 - introduce range splits at specific points in a table or index;
 - trigger a "scattering" of ranges in a table or index (or in an area of a
   table/index);
 - relocate specific ranges to a specified set of replicas.

# Motivation

The main motivation is to allow setting up tests, benchmarks, and reproducible
testbeds, especially for DistSQL. One set of some sample tests that we want to
be able to set up directly from logic tests are in
[TestDistSQLPlanner](https://github.com/cockroachdb/cockroach/blob/cc5ba57/pkg/sql/distsql_physical_planner_test.go#L75).
These tests need to split tables in a specific way, and they need to reprogram
the replicas and range leaders of each split.

The secondary motivation is for restore (as in backup/restore), which needs to
introduce a specific set of key splits. Currently this is done by custom code
which works as follows: the keys are sorted; a split is introduced on the middle
key, then the splits to the left and respectively to the right are processed
recursively (in parallel). We want the new syntax to implement this algorithm so
that backup/restore can use it (assuming they can switch to using split point
specified via column values as opposed to keys).

We currently have the `ALTER TABLE/INDEX SPLIT AT` statement which allows
introducing a table or index split at a specific tuple of column values. The
main drawback is that it cannot be used *programmatically*: each split must be
in its own statement; we cannot generate (e.g. via a `SELECT`) a set of values
where to split. We want to write tests like the one linked above without
hardcoding each split in the test file; and we want to be able to easily change
the test table sizes. In addition, the `SPLIT AT` statements don't support
control of replication.

# Detailed design

### 1. `ALTER TABLE/INDEX SPLIT AT` enhancement ###

The existing `SPLIT AT` syntax is changed: instead of taking a tuple of column
values for a single split, `SPLIT AT` takes an arbitrary select clause where
each row contains the primary key values; a split is issued for each row.

```sql
ALTER TABLE <table> SPLIT AT <select_statement>
ALTER INDEX <table>@index SPLIT AT <select_statement>
```

Existing uses of `ALTER TABLE SPLIT AT (pk1,pk2,...)` are changed to `ALTER
TABLE SPLIT AT VALUES (pk1,pk2,...)` (and similarly for `ALTER INDEX`);
examples:


| Old                                    | New                                           |
|----------------------------------------|-----------------------------------------------|
| `ALTER TABLE d.t SPLIT AT ('c', 3)`    | `ALTER TABLE d.t SPLIT AT VALUES ('c', 3)`    |
| `ALTER INDEX d.t@s_idx SPLIT AT ('f')` | `ALTER INDEX d.t@s_idx SPLIT AT VALUES ('f')` |

The new syntax allows introducing multiple split points, as well as calculated
or table-driven split points; for example:
```sql
ALTER TABLE t1 SPLIT AT VALUES ('c', 3), ('d', 4), ('e', 5)
ALTER INDEX t2@idx SPLIT AT SELECT i*10 FROM GENERATE_SERIES(1, 5) AS g(i)
ALTER TABLE t3 SPLIT AT SELECT pk1, pk2 FROM d.t WHERE split_here
```

The `SPLIT AT` statement is also extended to support providing only a *prefix*
of the primary key or index values. Example:
```sql
CREATE TABLE t (k1, k2, k3, v INT, PRIMARY KEY (k1, k2, k3))

-- Introduce a split at at /t/primary/1
ALTER TABLE t SPLIT AT VALUES (1)

-- Introduce a split at at /t/primary/1/2
ALTER TABLE t SPLIT AT VALUES (1,2)

-- Introduce a split at at /t/primary/1/2/3
ALTER TABLE t SPLIT AT VALUES (1,2,3)
```

Note that the statement returns only after the splits have been preformed.

Implementation note: the `SPLIT AT` implementation can issue the splits in any
order; for example, if there are many splits, it is advantageous to sort the
split points, split at the middle point, then recursively process the left and
right sides (in parallel).

##### Return values #####

`ALTER TABLE/INDEX SPLIT AT` currently returns a row with two columns: the key
in raw and pretty-printed form. The same schema is kept; a row for each split is
output.

If a split already exists, `SPLIT AT` currently returns an error.

TBD: what is the correct behavior with multiple split points? Ignore this error?

### 2. `ALTER TABLE/INDEX SCATTER` ###

A new pair of statements similar to `SPLIT AT` are introduced. Each has two
forms. The first form causes all the ranges for that table or index to be
"scattered": for each range, a new random set of stores are chosen for replicas
(in accordance with the zone config).

```sql
ALTER TABLE <table> SCATTER
ALTER INDEX <table>@index SCATTER
```

The second form allows only a specific area of a table or index to be scattered.
The area is specified using a pair of tuples (primary key or index column
values) for the start and end of the area. The values are interpreted similar to
`SPLIT AT`, and similarly a prefix of the primary key or index columns can be
specified:

```sql
ALTER TABLE <table> SCATTER (startPK1, startPK2, ...) (endPK1, endPK2, ...)
ALTER INDEX <table>@index SCATTER (startCol1, startCol2, ...) (endCol1, endCol2, ...)
```

Note that no new split points are introduced by `SCATTER`.

Examples:
```sql
ALTER TABLE t SCATTER
ALTER TABLE t SCATTER (1,1) (1,2)
ALTER INDEX t@idx SCATTER (1) (2)
```

The statement returns only after the relocations are complete.

### 3. `ALTER TABLE/INDEX TESTING_RELOCATE` ###

The `TESTING_RELOCATE` statements can be used to relocate specific ranges to
specific stores. This is very low-level functionality and is intended to be used
sparingly, mainly for setting up tests which benefit from a predetermined data
distribution. The rebalancing queues should be stopped in order to make the
relocations "stick".

```sql
ALTER TABLE <table> TESTING_RELOCATE <select_statement>
ALTER INDEX <table>@<index> TESTING_RELOCATE <select_statement>
```

`TESTING_RELOCATE` takes a select statement with the following result schema:
 - the first column is the relocation information: an array of integers, where
   each integer is a store ID. This indicates the set of replicas for the range;
   the first replica in the array will be the new lease owner.

 - the rest of the columns indicate a point in the table as a list of primary
   key or index columns (similar to `SPLIT AT`). The range that contains this
   point is relocated according to the first column. Note that, just like `SPLIT
   AT`, a row with this particular set of values doesn't have to exist in the
   table.

Examples:
```sql
CREATE TABLE t (k1, k2, k3, v INT, PRIMARY KEY (k1, k2, k3))

-- Move the range containing /t/primary/1/2/3 to store 1:
ALTER TABLE t TESTING_RELOCATE VALUES (ARRAY[1], 1, 2, 3)

-- Move the range containing /t/primary/1/2 to stores 5,6,7 (with 5 as lease owner):
ALTER TABLE t TESTING_RELOCATE VALUES (ARRAY[5,6,7], 1, 2)

-- Move even k1s to store 1, odd k1s to store 2:
ALTER TABLE t TESTING_RELOCATE SELECT ARRAY[1+i%2], i FROM GENERATE_SERIES(1, 10) AS g(i)
```

The statement returns only after the relocations are complete.

# Drawbacks

# Alternatives

An alternative considered was to create statements that operate on *keys* (key
prefixes) and provide functions that generate key prefixes from column values.
This was deemed as too low-level for the SQL interface.

Another alternative considered was to also use a function for the splitting
functionality, e.g. `SELECT split_at(..)`. The problem is that it forces the
splits to happen sequentially; we cannot implement the algorithm mentioned above
that parallelizes the splits. One way around this would be to introduce `split_at`
as an *aggregation* function (akin to `sum`).

# Unresolved questions
