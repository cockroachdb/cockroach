- Feature Name: explain
- Status: draft
- Start Date: 2015-08-10
- RFC PR:
- Cockroach Issue:

# Summary

Add support for an `EXPLAIN DEBUG` statement. The `EXPLAIN DEBUG` statement
will cause simple (single table or index) select statements to display the
underlying key-value pairs that were accessed:

* the first column will contain the index of the row the key maps to.
* the second column contain will contain a pretty-printed version of the key in
  the underlying key-value store.
* the third column will contain a boolean indicating whether the row was
  filtered by the where-clause or not.

# Motivation

Filtering of rows using SQL where-clauses makes it difficult to determine which
key-value pairs were scanned by a query. The `EXPLAIN` statement can be used to
provide database specific stats regarding the execution of the query. The
`EXPLAIN DEBUG` output will be useful during testing for determining whether
and how an index was accessed.

Listing all of the primary key-value pairs for a table can be performed by:

```sql
  EXPLAIN SELECT * FROM table
```

Listing all of a secondary indexes key-value pairs can be performed by:

```sql
  EXPLAIN SELECT * FROM table.index
```

# Detailed design

A key will be pretty-printed using `/` as a field separator:

* Each key will be prefixed with `/TableName/IndexName`.
* The field of each key will be printed according to its type:
  - Integer fields will be printed in decimal.
  - Floating point fields will be printed using Go's '%g' formatting flag.
  - String and blob fields will be printed using Go's `%q` formatting flag.
* The column ID portion of the key (if present) will be printed as
  `ColumnName`.

# Drawbacks

# Alternatives

* We can add alternative hooks that are not exposed via SQL to see which
  key-value pairs are accessed during query processing. For example, we could
  set a flag in `planner` and then create a test harness which parses SQL
  statements and uses a planner to execute them.

# Unresolved questions
