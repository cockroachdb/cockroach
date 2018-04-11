- Feature Name: Alter column type
- Status: draft
- Start Date: 2018-04-11
- Authors: Bob Vawter
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issues:
  [#9851](https://github.com/cockroachdb/cockroach/issues/9851)
  [#5950](https://github.com/cockroachdb/cockroach/issues/5950)

[TOC levels=1-3 markdown]: #

# Table of Contents
- [Summary](#summary)
- [Motivation](#motivation)
- [Guide-level explanation](#guide-level-explanation)
    - [Conversion classes](#conversion-classes)
        - [Trivial](#trivial)
        - [Validated](#validated)
        - [Cast](#cast)
        - [Assisted](#assisted)
    - [Indexed columns](#indexed-columns)
    - [Two-phase change](#two-phase-change)
    - [Key constraints](#key-constraints)
        - [Interleaved tables](#interleaved-tables)
- [Reference-level explanation](#reference-level-explanation)
    - [Process](#process)
    - [Failure modes](#failure-modes)
    - [Job control](#job-control)
    - [Allowable conversions](#allowable-conversions)
        - [Array conversions](#array-conversions)
    - [Drawbacks](#drawbacks)
    - [Rationale and Alternatives](#rationale-and-alternatives)
    - [Unresolved questions](#unresolved-questions)


# Summary

Users should be able to execute DDL statements to alter the type of
existing columns, without resorting to a copy-and-rename approach. These
kinds of statements are often used by ORM or other SQL tools or by users
during their prototyping phase.

This RFC describes a staged approach to enabling column-type changes by
grouping type conversions by implementation complexity.

The implementation described here supports keeping the table online,
with some reasonable restrictions applied while the schema change is in
progress. Changes to primary-key columns are explicitly out of scope for
this RFC.

# Motivation

Schema evolution is an important part of any database with a non-trivial
lifetime. While it is possible to effectively change a column's type
using a copy-and-rename approach, users expect to be able to perform
in-place changes to a column's type. Users should also have an easy way
to take advantage of new features in CockroachDB, such as the `INET` or
`JSONB` types.

# Guide-level explanation

The SQL syntax for this feature is borrowed from [PostgreSQL's `ALTER
TABLE` syntax](https://www.postgresql.org/docs/9.3/static/sql-altertable.html),
with one CockroachDB-specific extension to manage online schema changes.

```
ALTER TABLE table_name
  ALTER [COLUMN] column_name
  [SET DATA] TYPE data_type
  [COLLATE collation]
  [USING expression]
  [DEFERRED | COMPLETE | ABORT]
```

Column-type changes borrow from the
[F1 schema-change process](https://www.cockroachlabs.com/blog/how-online-schema-changes-are-possible-in-cockroachdb/)
to allow a column-type change to occur without taking the affected
table/column offline. In general, we assume that a column-type change
will not be an instantaneous process and could involve a non-trivial
amount of wall time in order to complete. While a column-type change is
in progress, reads from that column will return the original type, while
any writes must be compatible with both the current and future schema.
Once the type-change work has been completed, the new type becomes
effective and the old data can be retired. We allow only a single
column-type change to be in progress for any given table at a time.

## Conversion classes

Certain classes of type conversions lend themselves to faster or
less-complicated approaches and are described herein. A full table of
type conversions is [described later](#allowable-conversions) in this
RFC.

### Trivial

Trivial conversions are those that involve no conversion of on-disk data
and which cannot create any conflict between existing data and the
updated schema, such as converting `STRING -> BYTES` or `STRING(10) ->
STRING(25)`. These enable a fast-path conversion by simply updating the
table descriptor.

### Validated

Validated conversions involve no conversion of on-disk data, but do
require that all existing data is validated against the proposed schema
change before completion. This would include conversions such as
`STRING(100) -> STRING(10)` or `BYTES -> STRING`. These have a cost
somewhere between trivial and general conversions, since on-disk data
does not need to be rewritten, only revalidated.

### Cast

Cast conversions involve applying a built-in cast behavior to the
existing column data requiring the on-disk KV data to be rewritten, such
as `UUID -> STRING`.

### Assisted

Assisted conversions involve applying a `USING expression` to the
existing column data, such as `FLOAT -> INT USING ceil(column_name)`.

## Indexed columns

Columns that are part of an index are supported, however columns that
are part of a primary key are explicitly out of scope for this change.

## Two-phase change

Given that a type-change process may take an arbitrary and unpredictable
amount of time to complete, we support an optional two-phase approach to
making the schema change visible to SQL clients. This allows the user to
choose exactly when the change to the table schema will become visible,
allowing application downtime to be minimized in non-trivial-change
cases.

The `ALTER TABLE` command is first run with the `DEFERRED` option, which
begins the type-conversion in the background. At any point in time, but
generally after the job has completed, the user may issue the same
`ALTER TABLE` command, replacing `DEFERRED` with `COMPLETE` to allow the
type change to become visible once the job has completed. The `ABORT`
option allows an ongoing type-conversion operation to be canceled.

```
-- Convert a STRING to the 2.0 INET type.
-- This command will return a row containing a job id.
ALTER TABLE rpc_logs
  ALTER COLUMN caller_ip
  SET DATA TYPE INET
  DEFERRED

-- The application can continue reading and writing this column as STRING.

-- Wait for the job to finish, stop the application, swap the column type.
-- This command would block, returning any output from the conversion job.
-- Any reads that happen-after this command will see the new type, while any
-- reads that happen-before this command will see the old type.
ALTER TABLE rpc_logs
  ALTER COLUMN caller_ip
  SET DATA TYPE INET
  COMPLETE

-- Restart new version of the application that expects the INET type.
```

Electing not to use the `DEFERRED` option is equivalent to running a
`DEFERRED` followed immediately by a `COMPLETE`. We will refer to this
as "auto-complete" mode.

The `COMPLETE` option requires the entire, original `ALTER TABLE`
command as a sanity-check. If the type conversion generated any output,
it will be emitted by this command. If the type conversion has not yet
completed, this command will block until it has. It is not an error to
run `COMPLETE` before the conversion job has finished; it is simply a
signal to indicate that the SQL consumers are prepared to handle the new
column type.

## Key constraints

Columns that are the target of a primary or foreign key constraint will
not be supported under this RFC. It could be possible to support foreign
keys in the future by cascading the type change, but this would greatly
increase the amount of coordination required to effect the total schema
change.

### Interleaved tables

Changing the types of primary keys is not within the scope of this RFC,
so this feature does not interact with interleaved tables. When changes
to primary keys are supported, a change to the PK of a parent table
would have to be cascaded to the corresponding PK prefix column of any
interleaved descendant tables.

# Reference-level explanation

## Process

Conceptually, a column type change can be thought of as creating a
shadow column of the new type, which captures writes to the original,
source, column while a backfill process is run. Once the backfill is
complete, the shadow column is swapped for the original, which is then
retired. The shadow column will be anonymous and generally invisible to
the user until it is swapped in. Shadow columns will be created in the
same column family as the source column.

This list describes the general type-change case, not all of these steps
are necessarily executed for trivial or validated changes.

* `ALTER TABLE ... COLUMN ... TYPE` is run, resulting in a new shadow
  column being created in `DELETE_ONLY` mode. The shadow column has a
  new column id and a reference to the source column
  * If there are indexes that refer to the column, additional
    shadow-indexes are created.
* The shadow column and indexes are upgraded to `WRITE_ONLY` mode.
* Nodes that write to the source column will synchronously apply the
  requested type change and duplicate the write to the shadow column and
  indexes.
* A backfill process is distributed across all nodes in the cluster
  which have data for the table. This backfill will populate the shadow
  column and shadow indexes, inserting data with the source MVCC
  timestamp. The backfilled sstables will be inserted into the LSM such
  that any shadowed write will have priority.
* The following steps will only occur if the change is in auto-complete
  mode or when the user issues the `COMPLETE` command.
* Once the backfill is complete, the `WRITE_ONLY` restriction is removed
  from the shadow column and shadow indexes. Any code that resolves a
  column or index name to an id should be prepared to look for a shadow
  that has been activated.
* The table metadata is updated, changing the column name and indexes to
  point at the shadow data, making the original column or indexes
  non-live.
* The old column and indexes are retired in the same manner as dropping
  a column, allowing any active readers to complete their transaction.

To summarize:
* Column and index writers should be prepared to duplicate their writes
  when a writable shadow exists for their target column or index.
* Code that resolves a column or index name for reading should be
  prepared to look for an activated shadow.
* It must be possible to insert bulk, backfilled data into the LSM such
  that the bulk data will be shadowed by any ad-hoc writes that occurred
  during the backfilling process.

## Failure modes

Changing a column type is a low-risk activity, at least from the
perspective of data safety, because the process can be aborted at any
time up until the shadow column and indexes are promoted to `READ` mode.
All validation or conversion of existing will have been performed before
the new type becomes visible to SQL consumers, while any writes
performed during the conversion process must conform to both the old and
new column types.

The most likely cause for type-change failure is the presence of
existing data which cannot be coerced into the desired type. In this
case, the type change should abort and a sample of non-conforming rows /
values should be returned to the user.

Type changes which require extensive rewriting of data will temporarily
increase the amount of disk space required for the new sstables until
the old data can be retired. The amount of extra disk space required
could be estimated from existing table stats.

In the case where a column-type conversion (e.g. `STRING -> UUID`) is in
process and a user attempts to insert data that is not compatible with
the target type, the `INSERT` should fail with an error message such as
`column user_id is being converted from STRING to UUID. The value
'foobar' cannot be converted to UUID.`

## Job control

The type change will be modeled as a bulk-IO job, allowing it to be
paused and resumed. In addition, controls for data throughput can be
added to avoid IO starvation. Non-trivial conversions will be able to
operate on a range-by-range basis, which can be checkpointed.

## Allowable conversions

In general, scalar types can be converted per the existing [cast and
conversion rules](https://www.cockroachlabs.com/docs/stable/data-types.html)
outlined in the existing documentation.

In certain cases where a type conversion is not currently allowed by a
cast, it will be marked as `(extension)`.

| From                | To                      | Notes               |
|:--------------------|:------------------------|:--------------------|
| `BOOL`              |                         |                     |
|                     | `DECIMAL`               |                     |
|                     | `FLOAT`                 |                     |
|                     | `INT`                   |                     |
|                     | `STRING`                |                     |
| `BYTES`             |                         |                     |
|                     | `JSONB`                 | Cast (extension)    |
|                     | `STRING`                | Validated           |
|                     | `UUID`                  | Cast (extension)    |
| `DATE`              |                         |                     |
|                     | `DECIMAL`               |                     |
|                     | `INT`                   |                     |
|                     | `FLOAT`                 |                     |
|                     | `TIMESTAMP`             |                     |
|                     | `STRING`                |                     |
| `DECIMAL(p,s)`      |                         |                     |
|                     | `BOOL`                  |                     |
|                     | `DECIMAL(p[-1], s[-1])` | Validated           |
|                     | `DECIMAL(p[+1], s[+1])` | Trivial             |
|                     | `FLOAT`                 |                     |
|                     | `INT`                   |                     |
|                     | `STRING`                |                     |
| `FLOAT` & friends   |                         |                     |
|                     | `BOOL`                  |                     |
|                     | `DECIMAL`               |                     |
|                     | `INT`                   |                     |
|                     | `STRING`                |                     |
|                     | `FLOAT4`                | Trivial / validated |
|                     | `FLOAT8`                | Trivial / validated |
| `INET`              |                         |                     |
|                     | `STRING`                |                     |
| `INT` & friends     |                         |                     |
|                     | `BOOL`                  |                     |
|                     | `DATE`                  |                     |
|                     | `DECIMAL`               |                     |
|                     | `FLOAT`                 |                     |
|                     | `INTERVAL`              |                     |
|                     | `STRING`                |                     |
|                     | `TIMESTAMP`             |                     |
|                     | `INT4`                  | Trivial / validated |
|                     | `INT2`                  | Trivial / validated |
|                     | `BIT(n)`                | Trivial / validated |
|                     | `INTERVAL`              |                     |
|                     | `DECIMAL`               |                     |
|                     | `FLOAT`                 |                     |
|                     | `INT`                   |                     |
|                     | `STRING`                |                     |
|                     | `TIME`                  |                     |
| `STRING(n)`         |                         |                     |
|                     | `BOOL`                  |                     |
|                     | `BYTES`                 | Trivial             |
|                     | `DATE`                  |                     |
|                     | `DECIMAL`               |                     |
|                     | `FLOAT`                 |                     |
|                     | `INET`                  |                     |
|                     | `INT`                   |                     |
|                     | `INTERVAL`              |                     |
|                     | `JSONB`                 | Cast (extension)    |
|                     | `STRING(n-1)`           | Validated           |
|                     | `STRING(n+1)`           | Trivial             |
|                     | `TIME`                  |                     |
|                     | `TIMESTAMP`             |                     |
| `STRING COLLATE ab` |                         |                     |
|                     | `STRING COLLATE cd`     | Trivial             |
| `TIME`              |                         |                     |
|                     | `INTERVAL`              |                     |
|                     | `STRING`                |                     |
| `TIMESTAMP`         |                         |                     |
|                     | `INT`                   |                     |
|                     | `DECIMAL`               |                     |
|                     | `FLOAT`                 |                     |
|                     | `TIME`                  |                     |
|                     | `DATE`                  |                     |
|                     | `STRING`                |                     |
| `UUID`              |                         |                     |
|                     | `BYTES`                 | Trivial             |
|                     | `STRING`                | Cast (extension)    |

### Array conversions

The element type of an array can be converted, based on the conversion
rules above. That is, `STRING[]` could be converted to `DATE[]`.
Additionally, it is possible to convert any type to an array of a
convertable type, e.g. `STRING -> DATE[]`.

## Drawbacks

Strictly speaking, support for changing a column's type is unnecessary,
since a copy-and-rename operation can be performed or users can write
their own migration code. This is, however, a sub-optimal user
experience and will diminish compatibility with ORM or other tooling
that expects to be able to manipulate column definitions at runtime.

This proposal will require a new cluster version and the feature can
only be enabled once all nodes have been upgraded.

This proposal increases the complexity of the write path and causes a 2X
write amplification while an alteration is in progress. Readers should
be less affected, save that they will need to change how they resolve
column names to the underlying ids when a shadow column or index exists
that is readable.


## Rationale and Alternatives

The reason for calling out the various cases of type conversions is that
they lend themselves to a phased implementation approach:
* Trivial introduces the new SQL syntax and plumbing for the final stage
  of modifying a table descriptor.
* Validated adds a background job, related plumbing, and makes the
  two-phase change (`DEFERRED` and `COMPLETE`) actually do something.
* The general case can then be solved for, by extending the alteration
  job to perform the cast or assisted conversion and bulk-import
  operation, as well as making targeted changes to readers and writers
  of SQL/KV data.

Solving for the general case does have the potential to become highly
disruptive, depending on how the existing SQL/KV code is structured, and
possibly adding complexity to too many different parts of the codebase.
It also assumes that it would be straightforward to allow the
bulk-converted data to be easily masked by ad-hoc writes that occurred
during the conversion process. The benefit to solving for the the
general case is that it will make users feel that they have the freedom
to evolve their schemas and it will make it easier for users to adopt
new CockroachDB data types (e.g. `INET`, `JSONB`) without having to
manage their own dual-write migration.

The two-phase schema change approach is introduced in this RFC because
of the length of time it might take to complete a type conversion and
the desire to keep existing SQL consumers functional throughout the
backfilling process. The cost of supporting this is small; the
conversion process would follow the same steps with or without the
two-phase approach. The great benefit in this is that a user can choose
the exact time to swap from one version of their schema to another,
without requiring them to support the data type changing out at an
unpredictable time.

## Unresolved questions

* Can we easily merge bulk and ad-hoc writes with `AddSSTable` or
  similar?
* How does swapping out the active indexes affect query planning and
  execution?
