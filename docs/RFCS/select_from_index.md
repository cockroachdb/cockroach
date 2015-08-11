- Feature Name: select_from_index
- Status: draft
- Start Date: 2015-08-10
- RFC PR:
- Cockroach Issue: #2046

# Summary

Extend the SELECT syntax to allow an optional index to be specified in
table references. The current table reference syntax is `table` or
`database.table`. This syntax would be extended to allow `table.index`
and `database.table.index`.

# Motivation

As indexes are optional in SQL, it is difficult to know when an index
is used to satisfy a query which in turn makes it difficult to use the
existing logic test infrastructure to test correctness of index
scans. Providing for specification of an index to read from will make
testing of index scans easier and meshes with our desire to allow
explicitness in queries.

# Detailed design

`QualifiedNames` (which are used for table names) already allow an
arbitrary number of dot-separated elements. Table descriptor lookup in
`planner.getTableDesc` will be extended so that failure to lookup a
database descriptor would fall-back to assuming the `table.index`
syntax had been used.

`planner.Select` will be enhanced to use the normalized
`QualifiedName` returned by `planner.getTableDesc` to select the index
to use (if specified). `scanNode` will be enhanced to understand
scanning from secondary indexes. This latter work will is required to
support secondary index scans.

# Drawbacks

* Providing an SQL extension inhibits portability. It is expected this
  functionality will primarily be used for testing. If necessary, we
  can restrict this functionality to only be available under a flag.

* There are likely unforeseen interactions with joins.

# Alternatives

* Testing of index scans can use a different test harness than the
  existing SQL logic test infrastructure.

* Instead of overloading the dot-separated notation for qualified
  names, we could introduce new syntax. For example, `table@index` or
  `table:index`. This would remove the ambiguity of whether the first
  element in a qualified name is a table or a database.

# Unresolved questions
