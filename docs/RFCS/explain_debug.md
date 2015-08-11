- Feature Name: explain_debug
- Status: draft
- Start Date: 2015-08-10
- RFC PR:
- Cockroach Issue: #2047

# Summary

Add support for an `EXPLAIN DEBUG` statement. The `EXPLAIN DEBUG` statement
will cause simple (single table or index) select statements to display the
underlying key-value pairs that were accessed:

* the first column will contain the index of the row the key maps to.
* the second column will contain a pretty-printed version of the key in the
  underlying key-value store.
* the third column will contain a pretty-printed version of the value.
* the fourth column will contain a boolean indicating whether the row would
  have been included in the normal output or not.

As an example, consider the following table:

```sql
CREATE TABLE foo (
  a INT PRIMARY KEY,
  b TEXT,
  c BOOL,
  CONSTRAINT bar UNIQUE (b)
)
```

We'll populate the table with a couple of rows:

```sql
INSERT INTO foo VALUES (1, 'one', true), (2, 'two', false), (3, 'three', true)
```

`EXPLAIN DEBUG` can be used to show the key-value pairs accessed during the
query:

```sql
EXPLAIN DEBUG SELECT * FROM foo WHERE a >= 2;
----
RowIdx  Key               Value   Output
0       /foo/primary/1/a  1       false
0       /foo/primary/1/b  "one"   false
0       /foo/primary/1/c  true    false
1       /foo/primary/2/a  2       true
1       /foo/primary/2/b  "two"   true
1       /foo/primary/2/c  false   true
2       /foo/primary/3/a  3       true
2       /foo/primary/3/b  "three" true
2       /foo/primary/3/c  true    true
```

The entire table has been scanned because we currently don't use the
where-clause to restrict the scan. Similarly, we can show the key-value pairs
in the index `bar`:

```sql
EXPLAIN DEBUG SELECT * FROM foo.bar WHERE b = 'two';
----
RowIdx  Key               Value   Output
0       /foo/bar/"one"    NULL    false
1       /foo/bar/"two"    NULL    true
2       /foo/bar/"three"  NULL    false
```

# Motivation

Filtering of rows using SQL where-clauses makes it difficult to determine which
key-value pairs were scanned by a query. The `EXPLAIN` statement can be used to
provide database specific stats regarding the execution of the query. The
`EXPLAIN DEBUG` statement will be useful during testing for determining whether
and how an index was accessed.

Listing all of the primary key-value pairs for a table can be performed by:

```sql
  EXPLAIN DEBUG SELECT * FROM table
```

If #2046 is accepted, listing all of a secondary index key-value pairs can be
performed by:

```sql
  EXPLAIN DEBUG SELECT * FROM table.index
```

# Detailed design

A key will be pretty-printed using `/` as a field separator:

* Each key will be prefixed with `/TableName/IndexName`.
* The field of each key will be printed according to its type:
  - Integer fields will be printed in decimal.
  - Floating point fields will be printed using Go's `%g` formatting flag.
  - String and blob fields will be printed using Go's `%q` formatting flag.
* The column ID portion of the key (if present) will be printed as
  `ColumnName`.

The `EXPLAIN DEBUG` statement will initially not support `ORDER BY` or `GROUP
BY` clauses. It is intended to affect the output of simple scans and as such
will affect the output of `scanNode`.

# Drawbacks

* The `EXPLAIN DEBUG` statement will be visible to end-users. It might be
  burdensome to explain and/or support it. We'll probably want to state
  initially that support may be removed at any time and the format may change
  at any time.

# Alternatives

* We can add alternative hooks that are not exposed via SQL to see which
  key-value pairs are accessed during query processing. For example, we could
  set a flag in `planner` and then create a test harness which parses SQL
  statements and uses a planner to execute them.

# Unresolved questions
