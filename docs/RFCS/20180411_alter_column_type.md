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
        - [Compatibility and upgrade path](#compatibility-and-upgrade-path)
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

The current `TableDescriptor.Mutations` collection is somewhat
overloaded: it acts as a collection of not-public columns that
require special handling in the write path and as storage for managing
the state of the column operation.  As part of this change, a new
approach for handling multi-step table schema mutations will be added.

The state-management role of `TableDescriptor.Mutations` will be retired
in favor of a new `TableDescriptor.Actions` collection that the
schema-change code will operate on.  The existing use of
`TableDescriptor.Mutations` for not-public columns will be retained to
minimize impact on the SQL write path.

Conceptually, an `TableAction` represents a function which operates on a
`TableDescriptor` to produce a derived `TableDescriptor`, dependencies
on other actions, and any necessary state to drive the
transformation.  Actions have a monotonically increasing id number,
which is assigned using a `TableDescriptor.NextActionID` field.

Each table will have an "action loop", driven by a table-specific,
schema-change leaseholder.  The action loop performs the following:
* Identify disjoint graphs of actions to perform.
  * These action graphs can be thought of as transactions on the
    `TableDescriptor`.  Either all actions in a graph eventually
    complete successfully, or all of them are reverted.
  * When a graph has reached a terminal state, all of its associated
    actions are retired from the `TableDescriptor`.
* Randomly select an action whose dependencies have been
  successfully completed.
  * A `TableAction` can be marked as having been definitively completed
    by the schema-changer code, or by invoking a
    `TableAction.IsComplete()` function in cases where the table's
    schema-change coordinator node has restarted.
  * Executing actions or action graphs in parallel is currently out of
    scope for this RFC, but could represent a significant wall-time win
    in cases where a user is making several independent changes to a
    table schema.
* `Resume()` the action by:
  * Applying the action's function to the active `TableDescriptor` to
    produce the next `TableDescriptor` to publish when the action
    completes.  This allows for continuous revalidation of whether
    or not the action is still valid.
  * Marking the `TableAction` as in-progress, persisting any updates
    made to the `TableAction` by the `Resume()` function, and the
    original `TableDescriptor` contents.
  * Waiting for the success or failure of any long-running background
    processes necessary to effect the action (e.g. index backfills).
  * Marking the `TableAction` as being definitively completed.
* In the event that an action graph fails, a `TableAction.Rollback()`
   function will be invoked that is given the original and proposed
   `TableDescriptor` values to undo the change.
   * Simply replacing the active `TableDescriptor` with the stored,
     original `TableDescriptor` would work, but would limit future
     opportunities for parallel schema-change execution.

With this low-level action loop in mind, any long-running or multi-step
DDL statement that operates on a `TableDescriptor` can be expanded into
some number of `TableAction` entries which are executed in sequence.
The user's DDL statement can also be associated with a system job which
reflects the execution state of the associated action graph.  This will
allow long-running DDL statements to provide status feedback to the user
and offer a hook for canceling their execution.

Thus, an `ALTER COLUMN TYPE` command would expand to the following
collection of actions:
* Add the shadow column
* Add shadow indexes
  * These depend on the add-column `TableAction`
* Finalize the `ALTER COLUMN TYPE`
  * This represents a point-of-no-return for the schema change and
    is guaranteed to operate last by depending on all previous actions.

### Compatibility and upgrade path

Attempting to make a mixed-version cluster operate with and without
`TableAction`-aware nodes would be exceeding complicated, so for the
purposes of implementing this RFC, we will maintain the existing code
path in parallel with the proposed changes.  `ALTER COLUMN TYPE` will
be available only via the new `TableAction` code path.

A new `TableDescriptor.ClusterVersion` field will be added that the
schema-change code uses in conjunction with the global cluster version
to enable future migrations of `TableDescriptor` data.  This
`ClusterVersion` field will be set to enable the new `TableAction` path
once the cluster's version has been upgraded to an action-aware value
and there are no `TableMutation` entries still in flight.

This scheme can also be extended in the future to enable multi-table
schema changes by allowing `TableAction` dependencies to reference
actions in other `TableDescriptor` instances.

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
