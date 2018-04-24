- Feature Name: Alter column type
- Status: In-Progress
- Start Date: 2018-04-11
- Authors: Bob Vawter
- RFC PR: [#24703](https://github.com/cockroachdb/cockroach/pull/24703)
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
    - [Two-phase change](#two-phase-change)
    - [Key constraints](#key-constraints)
        - [Interleaved tables](#interleaved-tables)
    - [Indexed columns](#indexed-columns)
    - [Views](#views)
- [Reference-level explanation](#reference-level-explanation)
    - [Process](#process)
    - [Failure modes](#failure-modes)
    - [Job control](#job-control)
    - [Allowable conversions](#allowable-conversions)
        - [Array conversions](#array-conversions)
    - [Drawbacks](#drawbacks)
    - [Rationale and Alternatives](#rationale-and-alternatives)


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
TABLE` syntax](https://www.postgresql.org/docs/9.3/static/sql-altertable.html).

```
ALTER TABLE table_name
  ALTER [COLUMN] column_name
  [SET DATA] TYPE data_type
  [COLLATE collation]
  [USING expression]
```

In general, we assume that a column-type change will not be an
instantaneous process and could involve a non-trivial amount of wall
time in order to complete. Column-type changes build on top of existing
infrastructure to allow a column-type change to occur without taking the
affected table/column offline. While a column-type change is in
progress, reads from that column will return the original type, while
any writes must be compatible with both the current and future schema.
Once the type-change work has been completed, the new type becomes
effective and the old data can be retired. We allow only a single
column-type change to be in progress for any given table at a time;
multiple type changes will be queued and executed sequentially.

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
`STRING(100) -> STRING(10)` or `BYTES -> STRING`.

In cases where existing data conflicts with the requested type, the type
conversion will fail, rather than attempting to automatically coerce
non-conforming data. In the example of `STRING(100) -> STRING(10)`
above, the user would have the option of using an assisted cast
(described below) to achieve the desired result e.g. `USING
substring(my_string_column, 1, 10)`.

These have a cost somewhere between trivial and general conversions,
since on-disk data does not need to be rewritten, only revalidated.

### Cast

Cast conversions involve applying a built-in cast behavior to the
existing column data requiring the on-disk KV data to be rewritten, such
as `UUID -> STRING`.

### Assisted

Assisted conversions involve applying a `USING expression` to the
existing column data, such as `FLOAT -> INT USING ceil(column_name)`.
The allowable expressions are the same as those used in
[computed columns](https://www.cockroachlabs.com/docs/stable/computed-columns.html#creation),
namely pure functions using the data available in a single row.

## Two-phase change

An `ALTER ... COLUMN TYPE` command is implemented in two phases:
1. Add a new, hidden, computed column to the table that effects the
   conversion.
2. Atomically remove the computed nature from the new column, swap it
   for the original, and drop the original column.

Given that backfilling the computed column may take an arbitrary and
unpredictable amount of time, we will document and support a "manual"
version of the above, allowing a user to control exactly when a type
change becomes visible to SQL clients.

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

## Indexed columns

Columns that are part of an index are supported, however columns that
are part of a primary key are explicitly out of scope for this change.

## Views

Columns that are depended upon by a `CREATE VIEW` are out of scope for
this RFC. A future change to support views should be possible in the
same manner as supporting columns that are the target of a foreign-key
constraint.

# Reference-level explanation

## Process

Conceptually, a column type change can be thought of as creating an
anonymous, "shadow" column of the new type, which captures writes to the
original, source, column while a backfill process is run. Once the
backfill is complete, the shadow column is swapped for the original,
which is then retired. The shadow column will be generally invisible to
the user until it is swapped in. Shadow columns will be created in the
same column family as the source column.

This list describes the general type-change case, not all of these steps
are necessarily executed for trivial or validated changes.

* `ALTER ... COLUMN TYPE` is run, resulting in a new shadow column being
  created. The shadow column will be created as a computed column, in
  the source column's family, with the expression provided by the user
  via `USING expression` or inferred for cast-type conversions.
  * If there are indexes that refer to the column, additional
    shadow-indexes are created.
* The shadow column and indexes are backfilled using existing
  functionality for creating
  [new computed columns](https://github.com/cockroachdb/cockroach/issues/22652)
  and indexes.
* Nodes that write to the source column will synchronously apply the
  requested type change and duplicate the write to the shadow column and
  indexes.
  * If the new value that is written is incompatible with the requested
    conversion (e.g. `'foo'` -> `INET`), an error will be returned to
    the caller.
* Once the backfill is complete, several actions happen atomically:
  * The computed nature is removed from the shadow column.
  * The hidden flag is removed from the shadow column.
  * The source column is dropped.
  * The shadow column is renamed.
  * Any indexes on the original column are dropped.

## Failure modes

Changing a column type is a low-risk activity, at least from the
perspective of data safety, because the process can be aborted at any
time up until the shadow column and indexes are made live. All
validation or conversion of existing will have been performed before the
new type becomes visible to SQL consumers, while any writes performed
during the conversion process must conform to both the old and new
column types.

The most likely cause for type-change failure is the presence of
existing data which cannot be coerced into the desired type. In this
case, the type change will abort and a sample of non-conforming rows /
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

Unless otherwise noted in the Notes field below, a type conversion can
be assumed to be a [Cast conversion](#cast). In certain cases where a
type conversion is not currently allowed by a cast, it will be marked as
`Cast (extension)`. If a `USING expression` is present in the `ALTER
COLUMN` change, an [Assisted conversion](#assisted) will be performed.

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
|                     | `INT`                   |                     |
|                     | `INT2`                  | Trivial / validated |
|                     | `INT4`                  | Trivial / validated |
|                     | `INTERVAL`              |                     |
|                     | `STRING`                |                     |
|                     | `TIME`                  |                     |
|                     | `TIMESTAMP`             |                     |
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
|                     | `STRING COLLATE cd`     |                     |
| `TIME(TZ)`          |                         |                     |
|                     | `INTERVAL`              |                     |
|                     | `STRING`                |                     |
|                     | `TIME(TZ)`              | Trivial             |
| `TIMESTAMP(TZ)`     |                         |                     |
|                     | `INT`                   |                     |
|                     | `DECIMAL`               |                     |
|                     | `FLOAT`                 |                     |
|                     | `TIME`                  |                     |
|                     | `DATE`                  |                     |
|                     | `STRING`                |                     |
|                     | `TIMESTAMP(TZ)`         | Trivial             |
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
experience and diminishes compatibility with ORM or other tooling that
expects to be able to manipulate column definitions at runtime, e.g.
[Django](https://github.com/cockroachdb/cockroach/issues/5950),
[Grafana](https://github.com/grafana/grafana/issues/8900).

This proposal will require a new cluster version and the feature can
only be enabled once all nodes have been upgraded.

This proposal increases the complexity of the write path and causes a 2X
write amplification while an alteration is in progress. Readers should
be unaffected, as they will continue to interact with the existing,
source column and indexes.


## Rationale and Alternatives

Primary-key columns are explicitly out of scope in this RFC because
changing a primary key (without type conversion) is undergoing active
research. Once type changes have been implemented, changing the type of
a primary key column is equivalent to changing the primary key to
include a shadow column created by a type-change operation.
Additionally, the change to the primary key type would have to be
cascaded to any foreign key constraints, leading to a significant jump
in complexity for the initial implementation.

The reason for calling out the various cases of type conversions is that
they lend themselves to a phased implementation approach:
* Trivial introduces the new SQL syntax and plumbing for the final stage
  of modifying a table descriptor.
* Validated adds a background job, related plumbing, and makes the
  two-phase change actually do something.
* The general case can then be solved for, by extending the alteration
  job to perform the cast or assisted conversion and bulk-import
  operation, as well as making targeted changes to readers and writers
  of SQL/KV data.

The two-phase schema change approach is highlighted in this RFC because
of the length of time it might take to complete a type conversion and
the desire to keep existing SQL consumers functional throughout the
backfilling process. The great benefit in this is that a user can choose
the exact time to swap from one version of their schema to another,
without requiring them to support the data type changing out at an
unpredictable time.
