- Feature Name: Invisible Index
- Status: in-progress
- Start Date: 2022-06-28
- Authors: Wenyi Hu
- RFC PR: https://github.com/cockroachdb/cockroach/pull/83531
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/72576,
  https://github.com/cockroachdb/cockroach/issues/82363

# Summary

An invisible index is an index that is maintained up to date but is ignored by
the optimizer unless explicitly selected with [index
hinting](https://www.cockroachlabs.com/docs/v22.1/table-expressions#force-index-selection).

The main purpose of this RFC is to introduce the feature, document
implementation decisions, and the technical design.

# Motivation: use cases

### 1. Debugging without the expense of dropping and then recreating the index.
If an application suddenly starts using an index unexpectedly and is causing
lots of performance issues, you can change the index to be invisible as a short
term solution. In a new session, you can investigate what the problem might be
by turning `optimizer_use_invisible_indexes` on or with force indexes. Once the
issue has been solved, the index can be made visible again.

### 2. Roll out new indexes with more confidence.
When you create a new index, all the queries are able to use it which could have
an immediate effect on the application. Currently, some users with large
production scales are concerned about the impact of introducing new indexes and
potentially affecting their applications significantly.

With this feature, you can introduce the index as invisible first. In a new
session, you could give a workout and observe the impact of the new index by
turning `optimizer_use_invisible_indexes` on or with force indexes.

Note that this allows us to see the impact more safely, but the maintenance cost
associated with an index during inserts, upserts, updates, delete is still
needed.

### 3. Drop indexes with less risk.
A question that comes up frequently about indexes is whether an index is
actually useful for queries or if it is just sitting around and wasting
maintenance cost and storage.

Currently, the only way you can test this is by dropping the index and then
recreating it if the index turns out to be useful. However, when the table gets
large, recreating the index can become really expensive.

With invisible indexes, you can mark the index as invisible first, wait for a
few weeks to measure the impact, and then drop the index if no drop in
performance is observed. If the index turns out to be needed, you can easily
change the index back to visible without the cost of rebuilding an index.

Invisible indexes reduce the risk associated with dropping an index but not with
no risks. Firstly, just because an index is not used during this observation
period, this does not mean it will not be used in the future. Secondly,
invisible indexes are still used behind the scene by the optimizer for any
constraint check and maintenance purposes (more details below). In that case,
you cannot expect the database to behave in the exactly same way as dropping an
index.

### 4. Make indexes only available to specific queries.
If you want to make an index available only for specific queries, if only a
certain part of your application requires an index, or if you know a single
query is causing problems and creating an index would help, you can use an
invisible index and make this index available only to queries you want. This
leaves the rest of your application unaffected.

### 5. Index foreign key columns on a child table.
As mentioned earlier, invisible indexes are actually used for any constraint
check or maintenance purposes. This unlocks another use case. Invisible indexes
on foreign key columns can still be used to prevent full table locking and
performance issues.

When a parent table performs update or delete operations, the optimizer must
perform a FK check on the child table to ensure that there are no rows in the
child table referencing the columns being deleted or updated. If there are such
rows, this operation will fail. If the foreig key column on the child table is
not indexed, this check would require a full table level lock. This means that
if there is another operation on any rows in the child table, the operation will
still hang. On occasions, this could even lead to deadlocks.

However, having an index on foreign key columns, regardless of its visibility,
will only lead to a row level lock. This means that the operation will no longer
hang if there is another operation on an unrelated row in the child table. But
it will still hang if the operation is performed on the same row. This is one of
the cases where people should be cautious. Dropping such invisible indexes will
lead to a full table lock which is very different from marking an index
invisible.



# Implementation Decisions
Please let me know if you have any questions or different opinions on these
decisions. It should be relatively easy to make a change. Any feedback would be
greatly appreciated : )

### Conclusion:
- Users can create an invisible index or alter an existing index to be invisible.
- Indexes are visible by default.
- Primary indexes are not allowed to be invisible.
- Constraints cannot be created with invisible indexes. Creating a unique or a foreign key constraint with invisible indexes is not supported.
- Partial invisible indexes or inverted invisible indexes are both supported. The behavior is as expected.
- Queries can be instructed to use invisible indexes explicitly through force index or index hinting.
- Recreating an invisible index will make the index visible by default.
- Session variable, `optimizer_use_invisible_indexes`, can be set to true to use invisible indexes or false to ignore invisible indexes. By default, `optimizer_use_invisible_indexes` is set to false.

The following points are where you should be cautious; making an index invisible is not exactly the same as dropping the index.
- Force index or index hinting with invisible index is allowed and will override the invisible index feature.
  - If the index is dropped, this will throw an error.
- Invisible indexes will be considered as visible while policing unique or foreign key constraints. We will temporarily disable the invisible index feature during any constraint check.
  - If the index is dropped, the constraint check could lead to a full table scan.
- Log warning messages on occasions where users cannot expect the invisible index to be equivalent to dropping an index. It will be impossible to predict if users create a child table on an unique invisible index later on. The best we could do is to do some checks when users create a child table or alter a unique index to be invisible. Alternatively, we can always log this message whenever a unique invisible index has been created.
  - force index with invisible index
  - having a foreign key constraint referencing a unique invisible index
    - having a unique invisible index in general (warn if you create a child table referencing it, warn if you are using insert on conflict with it, delete on cascade)

### 1. What types of indexes can be invisible?
In [MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html), all
indexes other than primary keys explicitly or implicitly can be invisible. In
MySQL, a table with no explicit primary key will use the first unique index on
NOT NULL columns as an implicit primary key. Implicit and explicit primary
indexes are both not allowed to be invisible.

In CockroachDB, a table with no explicit primary key creates a new rowid column,
and creating a unique index on a null column will not change the primary key.
Primary indexes cannot be invisible. Unique indexes can be invisible.

### 2. Are constraints allowed to be created with invisible indexes?
In MySQL, only indexes can be invisible.

In CockroachDB, only indexes can be invisible as well. Creating a unique
constraint or a foreign key constraint with invisible indexes does not make
sense and will not be supported.

One might think creating unique constraints with invisible indexes is similar to
creating unique constraints without indexes (which is something we are currently
supporting). But they have very different semantic meanings. Firstly, creating a
constraint without index was not user-friendly and was created for multi-tenant
testing purposes. Secondly, creating a constraint with an invisible index is
still an index but just ignored by the optimizer.

This leads to an issue with the parser. Creating an invisible unique index
inside a `CREATE TABLE` definition is supported by the grammar rule, but the
parser will throw an error. This is because the parser is doing a round trip in
`pkg/testutils/sqlutils/pretty.go`. In `sql.y`, creating an unique index in a
`CREATE TABLE` statement returns a new structure
`tree.UniqueConstraintTableDef`. However, creating a unique constraint with a
not visible index is not supported by the parser. When the parser does a round
trip for the following statement, it formats it to a pretty statement using the
unique constraint definition. But the parser does not support unique constraints
with not visible index syntax. So it will fail while parsing the pretty
statement. Since `logictest` also performs a roundtrip check in
`pkg/sql/logictest/logic.go`, `logictest` would also fail. But creating an
unique index inside a `CREATE TABLE` definition will still work in a cluster.
This is a known issue. See more details in
https://github.com/cockroachdb/cockroach/pull/65825. The current solution for
this issue is to not create any tests in parser or in logictest. We will test
invisible unique indexes with `CREATE UNIQUE INDEX` and hope the behavior will
be the same if we create the index in a `CREATE TABLE` definition.

### 3. Should Force Index with invisible index be allowed?
In MySQL, [errors
occur](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html) when
queries include hints that refer to an invisible index. Instead, MySQL supports
a system variable that enables control over optimizer behavior. The
[use_invisble_indexes](https://dev.mysql.com/doc/refman/8.0/en/switchable-optimizations.html#optflag_use-invisible-indexes)
flag of this system variable controls whether the optimizer uses invisible
indexes for query plan. Users can set this flag for a single query.

In Oracle, a query can be modified to explicitly use the invisible index with a
force [index hint](http://www.dba-oracle.com/t_11g_new_index_features.htm).

In CockroachDB, using force index or index hinting with invisible index will log
a warning message. Giving the option to force the use of an invisible index
would give more flexibility since the fallback could be a full table scan.

### 4. Are invisible indexes still maintained and up-to-date?
In MySQL, index visibility does not affect index maintenance. Indexes are
maintained up to date with insert, delete, upsert, and update.

Same for CRDB. Just like any other indexes, an invisible index consumes
maintenance cost and resources.

### 5. Are unique constraints with invisible indexes still in effect?
In [MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html),
constraints are still in effect.

Same for CRDB. Regardless of index visibility, unique indexes still prevent
checks for duplicate values when inserting or updating data. Foreign key
constraints on invisible unique indexes still enforce referential integrity
which make sure that if there is a value in a child table referencing a value in
the parent table, then this value in the parent table must exist.

### 6. Scope of Invisibility: to what extent should the optimizer ignore invisible indexes? Should constraint check use or ignore invisible indexes?
Consider the following situation. Creating a child table requires a parent table
to have a unique index on the FK columns. What happens if the unique index is
invisible here? What happens if this unique index is changed to invisible after
the child table has been created? Consider another case. What happens if INSERT
ON CONFLICT is performed on an invisible unique index?

The first option would be to ignore the invisible index completely. However,
this means that when there is an insert operation on the child table, this
operation could require a full table scan to police the foreign key check. The
same situation applies if a parent table performs a delete or update operation
or if a child table performs an insert, upsert, or update operation. This would
not only lead to performance issues; having a unique constraint was necessary to
create the child table or to perform INSERT ON CONFLICT. If the index becomes
invisible, does it really make sense to allow these operations? Overall, this
option does not seem to work.

The second option would be to throw an error when an operation requires a
constraint check using invisible indexes. The justification behind this option
would be if someone wants to test the impact of dropping an index, this would be
the expected behavior. If someone wants to drop an index, it doesn’t make sense
to perform INSERT ON CONFLICT. However, this makes this feature much more
limited. As described above in the motivation section, there are other use cases
other than testing the impact of dropping an index. For example, this feature is
also helpful for a staged rollout of a new index or with debugging. Users might
want to use an invisible unique index with INSERT ON CONFLICT and then observe
the impact of this new index by altering the session variable to use invisible
indexes.

The only option left with us is to allow the optimizer to still use invisible
indexes while policing foreign key constraints or unique constraints. This
obviously has some drawbacks; users can no longer expect dropping an index to
behave exactly the same as marking an index as invisible. But we will try our
best to document this well and log messages on occasions where they cannot
expect the same behavior. On the bright side, this should be the more
standardized way based on MySQL and Oracle.

**Conclusion**

Regardless of index visibility, the optimizer will temporarily
disable the invisible index feature during any unique or foreign key constraint
check.

### 7. How to observe the impact on invisible indexes?
- SQL statements or queries will have different execution plans. You can see this using EXPLAIN.
- Queries will have different performance. (Check with SQL Observability)

# Technical Design
## 1. How does the optimizer support this feature?
As discussed above, to fully support the invisible index feature, we need to
ignore the invisible index unless it is used for constraint check or used during
force index.

Firstly, let’s ignore the part where we need to disable the invisible index
feature and focus on how the optimizer will ignore invisible indexes in general.

While the optimizer optimizes the plan using `xform.Optimizer`, the optimizer
will explore every possible query plan by using transformation rules to generate
equivalent memo groups. While constructing the memo groups, the optimizer will
enumerate indexes on a given Scan operator’s table using `ForEach ` under
	`pkg/sql/opt/xform/scan_index_iter.go`. This is where we can hide the index
away from the optimizer. While enumerating every index of the Scan operator’s
table, the optimizer will check if the index is invisible and ignore if it is.
Then, the optimizer is effectively ignoring the invisible index by not
generating any Scan operator for the invisible index.

Secondly, let’s think about what happens when force index is used with invisible
index. Force index will override the invisible index feature. Regardless of
index visibility, any indexes other than the force index will be ignored.

Thirdly, let’s think about how to disable invisible index features during
constraint check. When we are constructing a memo group for any `ScanOp` on a
given table in `buildScan` under `pkg/sql/opt/optbuilder/select.go`, we can add
a flag to `ScanPrivate` to indicate if this Scan expression is for a constraint
check. When the factory constructs the Scan expression, this flag will pass
along and ultimately to the optimizer. When the optimizer enumerates indexes on
a given Scan operator under `pkg/sql/opt/xform/scan_index_iter.go`, the
optimizer can then check if the scan is built for constraint check before
ignoring the invisible index.

Now let’s talk about which flag we should pass along whe `buildScan` is called.
There are several cases where foreign key constraints or unique constraint
checks are needed.

### Foreign key constraint check will be needed:
  - When a parent table performs an `UPDATE` or `DELETE` operation, FK check on the child table is needed.
  - When a child table performs an `INSERT`, `UPSERT`,or `UPDATE` operation, FK check on the parent table is needed.
  - There may be different foreign key actions `[UPDATE | DELETE] [ON CASCADE | SET DEFAULT | SET NULL | NO ACTION | RESTRICT| ON CONSTRAINT]`.
### Unique constraint check will be needed:
  - When `INSERT [ON CONFLICT DO NOTHING | DO UPDATE SET | ON CONSTRAINT | DISTINCT ON]`
  - When `UPSERT`, `UPDATE`

BuildScan is called in the following functions.
1. `func (cb *onDeleteFastCascadeBuilder) Build`: called by `planCascade` in `fk_cascade.go` for ON DELETE CASCADE
  - disable invisible index
2. `func (b *Builder) buildDeleteCascadeMutationInput`: called by `onDeleteSetBuilder` and `onDeleteCascadeBuilder` in `fk_cascade.go` for ON DELETE CASCADE, ON DELETE SET DEFAULT, ON DELETE SET NULL
  - disable invisible index
3. `func (b *Builder) buildUpdateCascadeMutationInput`: called by `onUpdateCascadeBuilder` in `fk_cascade.go` for ON UPDATE CASCADE, SET DEFAULT, SET NULL
  - disable invisible index
4. `func (mb *mutationBuilder) buildInputForDelete`: called by `buildInputForDelete` in `mutation_builder.go` which builds a memo group for DeleteOp
  - enable invisible index
5. `func (mb *mutationBuilder) buildInputForUpdate`: called by `buildInputForUpdate` in `mutation_builder.go` which builds a memo group for UpdateOp
  - enable invisible index
6. `func (mb *mutationBuilder) buildAntiJoinForDoNothingArbiter`: called by `buildInputForDoNothing` in `mutation_builder_arbiter.go` for INSERT ON CONFLICT DO NOTHING
  - disable invisible index
7. `func (mb *mutationBuilder) buildLeftJoinForUpsertArbiter`: called by `buildInputForUpsert` in `mutation_builder_arbiter.go` for UPSERT, INSERT..ON CONFLICT..DO UPDATE
  - disable invisible index
8. `func (h *arbiterPredicateHelper) tableScope`: eventually called by `findArbiters` in `mutation_builder_arbiter.go` for INSERT ON CONFLICT DO NOTHING, UPSERT, INSERT ON CONFLICT DO UPDATE SET
  - disable invisible index
9. `func (h *fkCheckHelper) buildOtherTableScan`: called by `buildDeletionCheck`, `buildInsertionCheck` in `mutation_builder_fk.go`
  - disable invisible index
10. `func (h *uniqueCheckHelper) buildTableScan`: called by `buildUniqueChecksForInsert`, `buildUniqueChecksForUpdate`, `buildUniqueChecksForUpsert` in `mutation_builder_unique.go` to enforce UNIQUE WITHOUT INDEX constraints
  - disable invisible index
11. `func (b *Builder) buildDataSource`: called by `buildJoin`, `buildFromWithLateral`, `buildFromTablesRightDeep` (called by buildInputForUpdate, buildSelectClause) in `select.go`
  - enable invisible index
12. `func (b *Builder) buildScanFromTableRef`: called by `buildDataSource` in `select.go` which adds support for numeric references in queries
  - enable invisible index

## 2. Test Cases
The invisible index test cases need to exercise these dimensions and try to call every function that is passing a flag to `buildScan`.

#### I. `pkg/sql/opt/xform/testdata/rules/not_visible_index`
- Under normal conditions, the invisible index feature should be enabled. 
- These test cases under the `opt` folder should check the basic functionality of the invisible index feature assuming the flag has been passed correctly. 
- Check normal invisible indexes that do not involve any FK or unique constraint checks.
  - Select
  - Insert, Update, Delete, Upsert (to do)
  - Force Index
  - Partial Index
  - Inverted Index & Partial Inverted Index
  - Non Null constraint check
    - *This should cover 4 buildInputForDelete, 5 buildInputForUpdate, 11 buildDataSource, 12 buildScanFromTableRef.*

#### II. `pkg/sql/opt/exec/execbuilder/testdata/not_visible_index`
- These logic tests under the `execbuilder` folder should check the following:
  - Cases where we need to log warning messages when dropping an index is not equivalent to marking an index invisible. 
    - Whenever a unique invisible index is created in CREATE INDEX or CREATE TABLE
    - Whenever a invisible index is created on a child table in CREATE INDEX or CREATE TABLE
  - Show indexes
  - System descriptors 
  - Check some basic test cases using EXPLAIN

#### III. Dogfooding already existing test cases. 
- Instead of creating new test cases, we will check the scan flag in the new output of the existing test cases to make sure that the flag has been passed correctly.
- The invisible index feature should be disabled during any unique constraint check.
  - Check when a table with unique indexes performs INSERT ON CONFLICT, DO NOTHING, DO UPDATE SET, ON CONSTRAINT.
    - *This should cover 6 buildAntiJoinForDoNothingArbiter, 7 buildLeftJoinForUpsertArbiter, 8 tableScope, 10 buildTableScan.*
- The invisible index feature should be disabled during any FK check.
  - Check when a parent table performs update or delete. 
    - *This should cover 4 buildInputForDelete, 5 buildInputForUpdate, 9 buildOtherTableScan.*
  - Check when a child table performs insert, upsert, or update. 
    - *This should cover 9 buildOtherTableScan.*
  - Check when a parent table performs update or delete with different FK actions. ON DELETE DO NOTHING, ON DELETE CASCADE, ON DELETE SET DEFAULT, ON DELETE SET NULL, and same for ON UPDATE.
    - *This should cover 1 onDeleteFastCascadeBuilder, 2 buildDeleteCascadeMutationInput, 3 buildUpdateCascadeMutationInput, 4 buildInputForDelete, 5 buildInputForUpdate.*
  - Check when a child table performs INSERT ON CONFLICT, DO NOTHING, DO UPDATE SET, ON CONSTRAINT.
    - *This should cover 6 buildAntiJoinForDoNothingArbiter, 7 buildLeftJoinForUpsertArbiter, 8 tableScope, 9 buildOtherTableScan, 10 buildTableScan.*

# Syntax
Invisible index feature is introducing four new user facing syntaxes. Since PostgreSQL does not support
the invisible index feature yet, we will use MySQL and Oracle as a reference for the standardized syntax. CRDB is already supporting a similar feature, invisible column feature.

### a. CREATE INDEX, CREATE TABLE, ALTER INDEX statements
When users are creating a new invisible index with `CREATE TABLE`, `CREATE INDEX` or altering an index to be invisible with `ALTER INDEX`, they will need to specify whether the index is invisible. The two options that we have discussed are `NOT VISIBLE`, `INVISIBLE`. MySQL and Oracle both support `INVISIBLE` instead. Invisible column feature is using `NOT VISIBLE`.

- **Conclusion**: We have decided that being consistent with the invisible column feature is more important. If you are wondering about why the Invisible column feature chose `NOT VISIBLE` over `INVISIBLE`, please look at this PR https://github.com/cockroachdb/cockroach/pull/26644 for more information.

#### Create Index Statements
```sql
CREATE [UNIQUE | INVERTED] INDEX [CONCURRENTLY] [IF NOT EXISTS] [<idxname>]
      ON <tablename> ( <colname> [ASC | DESC] [, ...] )
      [USING HASH] [STORING ( <colnames...> )]
      [PARTITION BY <partition params>]
      [WITH <storage_parameter_list>] [WHERE <where_conds...>]
      [VISIBLE | NOT VISIBLE]
```
- Example

```sql
CREATE INDEX a ON b.c (d) VISIBLE
CREATE INDEX a ON b.c (d) NOT VISIBLE

CREATE INDEX a ON b (c) WITH (fillfactor = 100, y_bounds = 50) VISIBLE
CREATE INDEX a ON b (c) WITH (fillfactor = 100, y_bounds = 50) NOT VISIBLE

CREATE INDEX geom_idx ON t USING GIST(geom) VISIBLE
CREATE INDEX geom_idx ON t USING GIST(geom) NOT VISIBLE

CREATE UNIQUE INDEX IF NOT EXISTS a ON b (c) WHERE d > 3 VISIBLE
CREATE UNIQUE INDEX IF NOT EXISTS a ON b (c) WHERE d > 3 NOT VISIBLE
```

#### Create Table Statements
```sql
CREATE [[GLOBAL | LOCAL] {TEMPORARY | TEMP}] TABLE [IF NOT EXISTS] <tablename> [table_element_list] [<on_commit>]
```

```sql
table_element_list: index_def
[UNIQUE | INVERTED] INDEX [<name>] ( <colname> [ASC | DESC] [, ...]
      [USING HASH] [{STORING | INCLUDE | COVERING} ( <colnames...> )]
      [PARTITION BY <partition params>]
      [WITH <storage_parameter_list>] [WHERE <where_conds...>]
      [VISIBLE | NOT VISIBLE]
```

- Example:
```sql
CREATE TABLE a (b INT8, c STRING, INDEX (b ASC, c DESC) STORING (c) VISIBLE)
CREATE TABLE a (b INT8, c STRING, INDEX (b ASC, c DESC) STORING (c) NOT VISIBLE)

CREATE TABLE a (b INT, UNIQUE INDEX foo (b) WHERE c > 3 VISIBLE)
CREATE TABLE a (b INT, UNIQUE INDEX foo (b) WHERE c > 3 NOT VISIBLE)
```

#### ALTER INDEX Statements
```sql
ALTER INDEX [IF EXISTS] <idxname> [VISIBLE | NOT VISIBLE]
```

```sql
ALTER INDEX a@b VISIBLE
ALTER INDEX a@b NOT VISIBLE
```

### b. SHOW INDEX Statements
The second user-facing syntax is related to SQL statements like SHOW INDEX. The three options are `is_hidden`, `visible`, or `is_visible`. MySQL is using [`visible`](https://dev.mysql.com/doc/refman/8.0/en/show-index.html). Invisible column feature is using `is_hidden` for [`SHOW COLUMNS`](https://www.cockroachlabs.com/docs/stable/show-columns.html).

- **Conclusion**: we have decided that it is more important to stay consistent with the first user-facing syntax and also with MySQL —use visible.

A new column needs to be added to the output of following SQL statements:
```sql
SHOW INDEX FROM (table_name)
SHOW INDEXES FROM(table_name)
SHOW KEYS FROM (table_name)

SHOW INDEX FROM DATABASE(database_name)
SHOW INDEXES FROM DATABASE (database_name)
SHOW KEYS FROM DATABASE (database_name)
```

```
table_name  index_name  non_unique  seq_in_index  column_name  direction  storing  implicit  visible
```

### c. Tables that store indexes information
The third user-facing syntax is with `crdb_internal.table_indexes` and `information_schema.statistics`. Invisible column feature uses `hidden` for `table_columns`. MySQL uses `is_visible`. Oracle uses `visibility`.
- **Conclusion**: we have decided that being consistent with the second-user facing syntax and with MySQL is more important --- use `is_visible`.

`crdb_internal.table_indexes`
```
descriptor_id descriptor_name index_id index_name index_type is_unique is_inverted is_sharded ***is_visible*** shard_bucket_count created_at                                                                                              
```
`information_schema.statistics`
```
table_catalog table_schema table_name non_unique index_schema index_name seq_in_index column_name COLLATION cardinality direction storing implicit ***is_visible***
```

We are also introducing another field in the index descriptor (just for internal use). The options are `Hidden`, `Invisible`, or `NotVisible`. The invisible column feature is using `Hidden` in the column descriptor. Using visible or visibility would be odd as well since the default boolean value is false (by default, index should be visible).

- **Conclusion**: we have decided to use `NotVisible`. Since we chose `visible` for all invisible index features above, choosing `NotVisible` or `Invisible` here is more consistent. `NotVisible` is preferred here because we are trying to stay away from the keyword `Invisible` to avoid confusion for the first user-facing syntax.

For more context about how this conclusion was drawn, please see https://github.com/cockroachdb/cockroach/pull/83388 and this RFC PR’s discussion thread.

# Fine-Grained Control of Index Visibility
As of now, the plan is to introduce the general feature of invisible index
first. The design and implementation details for fine-grained control of index
visibility will be added later on.

Later on, we want to extend this feature and allow a more fine-grained control
of index visibility by introducing the following two features.

1. Indexes are not restricted to just being visible or invisible; users can
   experiment with different levels of visibility. In other words, instead of
   using a boolean invisible flag, users can set a float invisible flag between
   0.0 and 1.0. The index would be made invisible only to a corresponding
   fraction of queries. Related:
   https://github.com/cockroachdb/cockroach/issues/72576#issuecomment-1034301996

2. Different sessions of a certain user or an application can set different
   index visibility for indexes. Related:
   https://github.com/cockroachdb/cockroach/issues/82363

Another option regarding scope of invisibility is to support two types of
indexes. Invisible indexes would stay the same as described above. We can also
consider supporting another type of index where the optimizer ignores
completely, and the purpose of this index could be specific to mimic the same
behavior as dropping an index.
