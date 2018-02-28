- Feature Name: Computed Columns
- Status: draft
- Start Date: 2017-12-14
- Authors: Daniel Harrison, Justin Jaffray
- RFC PR:
- Cockroach Issue:

# Summary

Computed columns allow users to create columns which are not directly writable,
but are instead pure functions of other column values in that row. This is
supported in several major databases, including MySQL, Oracle, and SQL Server.
This RFC covers both the version of computed columns that are physically stored
alongside other columns (referred to here as *stored*) and computed
columns which are only computed when read (*virtual*).

# Motivation

Computed columns allow storing data derived from other columns without putting
the burden of keeping that data updated on the client. See [Guide Level
Explanation: Partitioning](#partitioning) and [Guide Level Explanation: JSON](#json) for the two
motivating examples.


# Guide Level Explanation

The grammar for a computed column is `column_name <type> AS <expr>
[VIRTUAL | STORED]`, where `<expr>` is a pure function of non-computed columns in the
same table. `STORED` indicates a stored computed column and is required if that
column appears in the primary key. The default value is `STORED`. What syntax
other databases use is listed at the bottom of this document.

## Partitioning

[Partitioning] requires that partitions are defined using columns that are a
prefix of the primary key. In the case of geo-partitioning, some applications
will want to collapse the number of possible values in this column, to make
certain classes of queries more performant (see [Query planning changes in the
RFC] for details). For example, if a `users` table has a `country` and `state`
column, then a CockroachDB operator could make a stored computed column
`locality` with a reduced domain for use in partitioning.

```sql
CREATE TABLE users (
  locality STRING AS (CASE
    WHEN country IN ('ca', 'mx', 'us') THEN 'north_america'
    WHEN country IN ('au', 'nz') THEN 'australia'
  END) STORED,
  id SERIAL,
  name STRING,
  country STRING,
  ...
  PRIMARY KEY (locality, id)
) PARTITION BY LIST (locality) (
  PARTITION north_america VALUES IN ('north_america'),
  PARTITION australia VALUES IN ('australia')
)
```

## JSON

When the primary source of truth for a table lives in a JSON blob, it can be
desirable to put an index on a particular field of a JSON document. In
particular, computed columns allow for the following use-case: a two column
table, with a primary key column and a payload column, whose primary key is
computed as some field from the payload column. This alleviates the need for
the client to manually separate their JSON blobs from their primary keys.

```sql
CREATE TABLE documents (
  id STRING PRIMARY KEY AS (payload->>'id') STORED,
  payload JSONB
)
```

## Notes
- For simplicity, we disallow computed columns from referencing other computed
  columns. This avoids having to ensure that there are no cycles and also
  guarantees that we don’t need to be concerned with the order in which we
  evaluate computed columns. This restriction can be revisited later if
  customers request it.
- The expression provided following the `AS` is attempted to be inferred as the
  listed type, based on the types of the other columns in the table, erroring
  if the type check fails. This is as opposed to implicitly attempting to cast
  the resulting expression to the given type. Casts must be done explicitly
  with the `::` operator.
- Computed columns behave like any other column, with the exception that they
  cannot be written to directly. Performing an `INSERT` or `UPDATE` which
  specifies a computed column is an error unless the expression being inserted
  is `DEFAULT`.
- The expression defining the column must be pure. In practice this just means
  that it can’t use any builtins marked as `impure`, such as `now()` or
  `nextval()`.
- Computed columns are mutually exclusive with `DEFAULT`.
- `CHECK` constraints are run after computed columns are resolved.

# Reference-level explanation
## Detailed Design

A `computed` field is added to `ColumnDescriptor` as follows:

```protobuf
message ColumnDescriptor {
  ...
  message Computed {
    expr string = 1;
    bool stored = 2;
    repeated ColumnId dependencies = 3;
  }
  Computed computed = 10;
}
```

Stored computed columns are always read exactly like any other column.
When a column that a stored computed column depends on is updated, then
it too must be updated. This requires fetching all columns that it depends on.
`table{Inserter,Updater,Upserter}` already have a mechanism for this
(`requestedCols`) that we can hook into.

Virtual computed columns are never written. When a virtual
computed column is read, the columns it depends on must also be fetched.
`scanNode` and `tableReader` will have to be updated.

In addition, users are stopped from dropping a column if it’s referenced by a
computed column. Computed column expressions for dependents of a column are
kept up to date if columns are renamed (this is similar to what is done for
`CHECK` constraints).

## Modification to `information_schema`

As computed columns are not a feature in Postgres, let alone a standard SQL
feature, we will match [MySQL's extension] to `information_schema`, which is to
add an extra `TEXT` column `generation_expression` to the
`information_schema.columns` table which is empty for non-computed columns and
contains the expression used to compute the column otherwise.

# Drawbacks
- As Postgres does not currently support this feature, by committing to
  particular semantics for it, we risk diverging from theirs if they choose to
  support it in the future. One mitigating factor is that the syntax between
  the existing implementations may vary, but the behavior is largely the same,
  so if we end up in this situation, it’s likely that we can simply alias
  Postgres’s syntax.

# Alternatives
- MySQL allows computed columns to reference other computed columns, provided
  that the column being referenced appears before the referencing column in the
  `CREATE TABLE` statement. Given that there’s a clear path forward to this
  alternative if we want to do this in the future, something like this is
  out-of-scope for this initial RFC.
- There isn’t a standard syntax for this feature. All of MySQL, SQL Server, and
  Oracle implement it slightly differently. Postgres does not have this
  feature.
- Computed indexes can also solve many of the same problems as computed
  columns. We consider them out-of-scope for this project but they could this
  work could be expanded on to support them in the future. Many of the benefits
  can be had by creating the index on a virtual computed column,
  without the index selection complications.


| Database               | Virtual                                                         | Stored                                                                    |
| ---------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------- |
| CockroachDB (Proposed) | `inventory_value INT AS (qty_available * unit_price) VIRTUAL`   | `inventory_value INT AS (qty_available * unit_price) STORED`              |
| MySQL                  | `inventory_value INT AS (qty_available * unit_price) [VIRTUAL]` | `inventory_value INT AS (qty_available * unit_price) <STORED\|PERSISTED>` |
| SQL Server             | `inventory_value AS qty_available * unit_price`                 | `inventory_value AS qty_available * unit_price PERSISTED`                 |
| Oracle                 | `inventory_value COMPUTED BY qty_available * unit_price`        | `inventory_value AUTOMATIC INSERT AS qty_available * unit_price`          |

# Unresolved Questions
[Partioning]: https://github.com/cockroachdb/cockroach/blob/aa61db043e9c54c0b83a405cd76ce0ec7cc6a35d/docs/RFCS/20170921_sql_partitioning.md
[Query planning changes in the RFC]: https://github.com/cockroachdb/cockroach/blob/aa61db043e9c54c0b83a405cd76ce0ec7cc6a35d/docs/RFCS/20170921_sql_partitioning.md#query-planning-changes
[MySQL's extension]: https://dev.mysql.com/doc/refman/5.7/en/columns-table.html
