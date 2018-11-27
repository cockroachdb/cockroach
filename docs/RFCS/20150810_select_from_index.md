- Feature Name: select_from_index
- Status: completed
- Start Date: 2015-08-10
- RFC PR: [#2046](https://github.com/cockroachdb/cockroach/pull/2046)
- Cockroach Issue:

# Summary

Extend the SELECT syntax to allow an optional index to be specified in table
references. The current table reference syntax is `table` or
`database.table`. This syntax would be extended to allow `table@index` and
`database.table@index`.

Only the column names that are part of the index will be made available to the
rest of the query. For example, consider the following table:

```sql
CREATE TABLE test (
  a INT PRIMARY KEY,
  b TEXT,
  c BOOLEAN,
  CONSTRAINT foo INDEX (b, c)
)
```

The new syntax can be used to restrict the query to only using the index `foo`:

```sql
SELECT * FROM test@foo
```

It will be an error to access columns that are not part of the index:

```sql
SELECT a FROM test@foo WHERE a > 2
```

# Motivation

As indexes are optional in SQL, it is difficult to know when an index is used
to satisfy a query which in turn makes it difficult to use the existing logic
test infrastructure to test correctness of index scans. Providing for
specification of an index to read from will make testing of index scans easier
and meshes with our desire to allow explicitness in queries.

Note that this proposal is essentially a very strict form of an index hint or
forced index hint functionality. It is possible we'll adjust the semantics in
the future.

# Detailed design

The grammar will be extended to allow an `@index` suffix to
table names. `planner.Select` will be enhanced to select the index to use
(if specified). `scanNode` will be enhanced to understand scanning from
secondary indexes. This latter work is required to support secondary index
scans.

# Drawbacks

* Providing an SQL extension inhibits portability. It is expected this
  functionality will primarily be used for testing. If necessary, we can
  restrict this functionality to only be available under a flag.

* There are likely unforeseen interactions with joins.

# Alternatives

* Testing of index scans can use a different test harness than the existing SQL
  logic test infrastructure.

* Instead of adding a new delimiter for index specification, we could overload
  the dot-separated notation for qualified names. For example, `table.index` or
  `database.table.index`. This would remove the need to adjust the grammar but
  would add ambiguity as to whether the first element in a qualified name is a
  table or a database.

# Unresolved questions
