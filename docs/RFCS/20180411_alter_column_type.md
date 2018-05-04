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
        - [Manual preparation](#manual-preparation)
    - [Column structural dependencies](#column-structural-dependencies)
        - [Indexed columns](#indexed-columns)
        - [Other structural dependencies](#other-structural-dependencies)
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
[EXPLAIN ( STEPS )]
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
effective and the old data can be retired.

We allow only a single column-type change to be in progress for any
given table at a time; multiple type changes will be queued and executed
sequentially. However, nothing in this design necessarily precludes
simultaneous execution of multiple type changes. The use of
[manual preparation](#manual-preparation) for a type change can also
make that change nearly instantaneous.

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

An `ALTER ... COLUMN TYPE` command is essentially a macro around many
different "micro" SQL DDL statements, separated in two phases:
1. Add a new, hidden, computed column to the table that effects the
   conversion. Any structural dependencies (e.g. indexes) on the source
   column are also duplicated using the new, hidden column.
2. Atomically remove the computed nature from the new column, swap it
   for the original, and drop the original column.

### Manual preparation

Given that backfilling the computed column and duplicating any other
structural dependencies may take an arbitrary and unpredictable amount
of time, we want to allow the user to complete as much preparatory work
as possible before issuing the `ALTER ... COLUMN TYPE` so that it
completes as quickly as possible. This will allow the user to minimize
application downtime in cases where the application cannot support the
effects of the `ALTER ... COLUMN TYPE` becoming visible at an arbitrary
time.

To support this, we support `EXPLAIN`ing the steps required to complete
the schema change. This will execute the planning phase of the schema
change and return the results to the user. The result of this command
will contain the following information:
* A description of work to be performed.
* A suggested SQL statement to execute to prepare the schema.
* If the planner detects that the step has already been prepared, an
  explanation of what will happen once the `ALTER ... COLUMN TYPE` is
  executed.

For instance:

`EXPLAIN(STEPS) ALTER TABLE a ALTER COLUMN b TYPE INT`

| description            | prepared | command                                                      |
|:-----------------------|:---------|:-------------------------------------------------------------|
| create computed column |          | `ALTER TABLE a ADD COLUMN b_alter_int COMPUTED USING b::INT` |
| clone index a.b        |          | `CREATE INDEX b_alter_int ON a (b_alter_int)`                |

This will allow a DBA to run the necessary preparatory commands in
advance of rolling out a new version of any application that depends on
the updated schema. An automated tool with specific knowledge of
CockroachDB can consume this output to automate schema changes while
also minimizing downtime. Once all of the returned steps are ready, the
user can assume that the `ALTER ... COLUMN TYPE` command should complete
instantaneously.

Suppose the user has manually created the computed column, per the
suggested SQL command. Re-running the `EXPLAIN` would result in the
following:

| description               | prepared                            | command                                       |
|:--------------------------|:------------------------------------|:----------------------------------------------|
| create destination column | found b_alter_int; will rename to b |                                               |
| clone index a.b           |                                     | `CREATE INDEX b_alter_int ON a (b_alter_int)` |


## Column structural dependencies

### Indexed columns

Columns that are part of an index are supported, however columns that
are part of a primary key are explicitly out of scope for this change.
We support indexes by duplicating every existing index that includes the
source column. The source column will be replaced by the shadow column
in the shadow index's definition. Once the schema change is complete,
the shadow index will be renamed to the original index name to preserve
compatibility with queries that use named indexes.

### Other structural dependencies

In general, columns that are the target of a structure dependency other
than indexes (e.g. views, primary keys, foreign keys, interleaved table
prefixes) are out of scope for this RFC. Attempting to change a column
that is a target of a structural dependency will return an error to the
client explaining the limitation. Future revisions to this work can
gradually lift the constraints after deciding on the semantics of how
type changes should be cascaded.

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
  * For every index that refers to the column, an additional shadow
    index is created that substitutes the shadow column.
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
  * Shadow indexes are renamed to replace the original indexes.

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
'foobar' cannot be converted to UUID.` If it is impractical to
differentiate an error produced by the computed, shadow column when a
schema change is ongoing, the error message returned for computation
failures will be generalized to ensure that the user isn't left confused
about why a value could not be inserted.

## Job control

The type change will be modeled as a bulk-IO job (e.g. `IMPORT CSV`),
allowing it to be paused and resumed and to have a place to store
ongoing state and progress information. In general, we expect that the
implementation of other DDL statements should become more job-like in
the future, so the implementation should attempt to produce reusable
components and abstractions where it makes sense to.

The job will consist of:
* A planning phase that, given an existing table schema, will determine
  all necessary steps to perform.
* A de-duplication phase which will apply heuristics to determine if
  there are existing schema elements that satisfy portions of the plan.
* A "macro" phase which executes the necessary remaining steps.
* A finalization phase which can revert partial changes if a step fails
  (e.g. a `UNIQUE` shadow index detects duplicate values in the
  converted shadow column).

As the job progresses through the various steps, it will update a
progress entry, allowing it to be visualized like other jobs.

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

Any permutation of an array conversion is acceptable: `T -> T[]`,
`T[] -> T`, `T -> S[]`, or `T[] -> S[]`. In all cases, an explicit
`USING expression` is required, since there is no one-size-fits-all
approach to effecting the conversion.

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
