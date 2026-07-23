- Feature Name: Invisible Index
- Status: in-progress
- Start Date: 2022-06-28
- Authors: Wenyi Hu
- RFC PR: https://github.com/cockroachdb/cockroach/pull/83531
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/72576,
  https://github.com/cockroachdb/cockroach/issues/82363

# Summary

An invisible index is an index that is maintained up-to-date but is ignored by
the optimizer unless explicitly selected with [index
hinting](https://www.cockroachlabs.com/docs/v22.1/table-expressions#force-index-selection)
or for constraint purposes.

The main purpose of this RFC is to introduce the feature, document
implementation decisions, and propose a technical design.

# Motivation: use cases

### 1. Roll out new indexes with more confidence.
When you create a new index, all queries are able to pick it which could have
an immediate effect on the workload. Currently, some users with large
production scales are concerned about the impact of introducing new indexes and
potentially affecting their applications significantly.

With invisible indexes, you can introduce the index as invisible first. In a new
session, you could give a workout and observe the impact of the new index by
turning `optimizer_use_not_visible_indexes` on or with index hinting. If this
index does turn out to be useful, you can then change this index to be visible
in your database.

Note that this allows us to see the impact more safely; the maintenance cost
associated with an index during inserts, upserts, updates, or delete is still
needed.

### 2. Drop indexes with less risk.
A question that comes up frequently about indexes is whether an index is
actually useful for queries or if it is just sitting around and wasting
maintenance cost. Currently, the only way you can test this is by dropping the
index and then recreating it if the index turns out to be useful. However, when
the table gets large, recreating the index can become really expensive.

With invisible indexes, you can mark the index as invisible first, wait for a
few weeks to measure the impact, and then drop the index if no drop in
performance is observed. If the index turns out to be needed, you can easily
change the index back to visible without the cost of rebuilding an index.

Note that using an invisible index reduces the risk associated with dropping the
index but not with no risks. First, just because an index is not used during
this observation period, this does not mean it will not be used in the future.
Second, invisible indexes are still used behind the scene by the optimizer for
any constraint check and maintenance purposes (more details below). In that
case, you cannot expect the database to behave in the exact same way as dropping
an index.

### 3. Debugging.
If queries suddenly start using an index unexpectedly and is causing performance
issues, you can change the index to be invisible as a short term solution. You
can then investigate what the problem might be using
`optimizer_use_not_visible_indexes` or index hinting in a new session. Once the
issue has been solved, the index can be made visible again.

### 4. Make indexes only available to specific queries.
If you know certain queries have problems and creating an index would help, you
can use invisible index and make this index available only to queries you want
with index hinting. In this way you can leave the rest of your application
unaffected.

# Implementation Decisions
### Conclusion:
- Users can create an invisible index or change an existing index to be invisible.
- By default, indexes are visible.
- Primary indexes cannot be invisible.
- Constraints cannot be created with invisible indexes. Creating unique or foreign key constraints with invisible indexes is not supported.
- Partial invisible indexes or inverted invisible indexes are both supported. The behavior is as expected.
- Queries can be instructed to use invisible indexes explicitly through [index
  hinting](https://www.cockroachlabs.com/docs/v22.1/table-expressions#force-index-selection).
- Session variable, `optimizer_use_not_visible_indexes`, can be set to true to tell the optimizer to treat invisible indexes as they are visible. By default, `optimizer_use_not_visible_indexes` is set to false.

The following points are where things might be unexpected as making an index invisible is not exactly the same as dropping the index.
- Force index or index hinting with invisible index is allowed and will override the invisible index feature.
  - If the index is dropped instead, this will throw an error.
- Invisible indexes will be treated as visible while policing unique or foreign key constraints. In other words, we will temporarily disable the invisible index feature during any constraint check.
  - If the index is dropped, the query plan for constraint check could be different and lead to a full table scan.

### 1. What types of indexes can be invisible?
Primary indexes cannot be invisible. Any secondary indexes including unique
indexes can be invisible.

In [MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html), all
indexes other than primary keys can be invisible. In MySQL, a table with no
explicit primary key will use the first unique index on NOT NULL columns as an
implicit primary key. Implicit and explicit primary indexes are both not allowed
to be invisible. However, a table with no explicit primary key creates a new
rowid column in CRDB, and creating a unique index on a null column will not
change the primary key.

### 2. Can constraints be invisible? Can constraints be created with invisible indexes? 
No. Constraints cannot be invisible, and they cannot be created with invisible
indexes. Having invisible constraint means that the constraint can be on and off
at different times. Since constraint is an insert-time enforcement, allowing
invisible constraint could lead to corrupted indexes. 

One might think creating unique constraints with invisible indexes is similar to
creating unique constraints without indexes (which is something CRDB is
currently supporting). But they have very different semantic meanings. First,
creating a constraint without index is not user-friendly and was created for
multi-tenant testing purposes. Second, creating a constraint with an invisible
index is still an index but just ignored by the optimizer.

Overall, only indexes can be invisible. Creating a unique constraint or a
foreign key constraint with invisible indexes will not be supported. This leads
to an issue with the parser. This behavior aligns with MySQL.

This leads to another issue in the parser; creating an invisible unique index
inside a `CREATE TABLE` definition is supported by the grammar rule, but the
parser will throw an error. This is because the parser is doing a round trip in
`pkg/testutils/sqlutils/pretty.go`. In sql.y, creating a unique index in a
`CREATE TABLE` statement returns a new structure
`tree.UniqueConstraintTableDef`. However, creating unique constraints with not
visible indexes is not supported by the parser. When the parser does a round
trip for the following statement, it formats it to a pretty statement using the
unique constraint definition. But the parser does not support unique constraint
with not visible index syntax. So it will fail while parsing the pretty
statement. Since logictest also performs a roundtrip check in
`pkg/sql/logictest/logic.go`, logictest would also fail. But creating a unique
index inside a `CREATE TABLE` definition will still work in a cluster. This is a
known issue. See more details in
https://github.com/cockroachdb/cockroach/pull/65825.

### 3. Should Force Index with invisible index be allowed?
Using index hinting with invisible indexes is allowed and is part of the feature
design. Although this may lead to different behavior with the index being
dropped, this offers more flexibility with the feature. For example, the
fallback of some queries might be a full table scan and may be too expensive.

In MySQL, index hinting with invisible indexes
[errors](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html).
Instead, MySQL supports index hinting with the session variable
[optimizer_use_not_visible_indexes](https://dev.mysql.com/doc/refman/8.0/en/switchable-optimizations.html#optflag_use-invisible-indexes).
Users can instruct queries to use invisible indexes by setting this session
variable to true for only specific queries.

### 4. Are invisible indexes still maintained and up-to-date?
Yes. Just like any other indexes, an invisible index consumes maintenance cost
and resources. Regardless of visibility, indexes are maintained up-to-date with
insert, delete, upsert, and update.

This behavior aligns with [MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html).

### 5. Are unique constraints with invisible indexes still in effect?
Regardless of index visibility, unique indexes still prevent checks for
duplicate values when inserting or updating data. Creating a foreign key
constraint requires unique indexes or constraints on the parent table. Foreign
key constraints are still enforced even if the unique indexes on the parent
table become invisible; if a column in a child table is referencing another
column in the parent table, then this value in the parent table must exist.

This behavior aligns with [MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html).

### 6. Scope of Invisibility: to what extent should the optimizer ignore invisible indexes? Should constraint check use or ignore invisible indexes?
Consider the following situation. Creating a child table requires a parent table
to have a unique index on the FK columns. What happens if the unique index is
invisible here? What happens if this unique index is changed to invisible after
the child table has been created? Consider another case. What happens if INSERT
ON CONFLICT is performed on an invisible unique index?

The first option would be to ignore the invisible index completely. However,
this means that when insert on the child table may require a full table scan to
police the foreign key check. The same situation applies if a parent table
performs delete or update, or if a child table performs insert, upsert, or
update. This would not only lead to performance issues; having a unique
constraint was necessary to create the child table or to perform INSERT ON
CONFLICT. If the index becomes invisible, does it really make sense to allow
these operations? Overall, this option is not viable.

The second option would be to throw an error when an operation requires a
constraint check using invisible indexes. The justification behind this option
would be if someone wants to test the impact of dropping an index, this would be
the expected behavior. 

However, if someone wants to drop an index, it does not make sense if they still
want to have a foreign key constraint on it or to perform `INSERT ON CONFLICT`.
In addition, this makes this feature much more limited. As described above in
the motivation section, there are other use cases other than testing the impact
of dropping an index. For example, this feature is also helpful for a staged
rollout of a new index. Throwing an error with `INSERT ON CONFLICT` could lead
to confusion.

The only option left with us is to allow the optimizer to still use invisible
indexes while policing foreign key constraints or unique constraints. This
obviously has some drawbacks; users can no longer expect dropping an index to
behave exactly the same as marking an index as invisible. But we will try our
best to document this well and log messages on occasions where they cannot
expect the same behavior. On the bright side, this should be the more
standardized way based on MySQL and Oracle.

We should log warning messages on occasions where users cannot expect the invisible index to be equivalent to dropping an index.
We will log this message:
- if users are changing an existing visible index to invisible or if users are dropping an invisible index
- if this invisible index may be used to police constraint check 
  - when this invisible index is unique 
  - or when this invisible index is on a child table, and the first column stored by the index is part of the FK constraint.

**Conclusion**

The optimizer will treat all invisible indexes as they are visible for any
unique or foreign key constraint purposes.

### 7. How to observe the impact on invisible indexes?
- SQL statements or queries will have different execution plans. 
  - You can see this using `EXPLAIN`. 
  - If you want to know whether invisible indexes are used for constraint check,
    you can use `EXPLAIN (VERBOSE)` and check if `disabled not visible index
    feature` is set as part of the scan flags.
- Queries or workload will have different performance.

# Technical Design
## 1. How does the optimizer support this feature?
As discussed above, to fully support the invisible index feature, we need to
ignore the invisible index unless it is used for constraint check or used during
force index.

First, let’s ignore the part where we need to disable the invisible index
feature and focus on how the optimizer will ignore invisible indexes in general.

During exploration, the optimizer will explore every possible query plan using
transformation rules. While constructing equivalent memo groups, the optimizer
will enumerate indexes on a given Scan operator’s table using `ForEach` under
`pkg/sql/opt/xform/scan_index_iter.go`. This is where we can hide the index away
from the optimizer. While enumerating every index, the optimizer can check if
the index is invisible and ignore if it is. The optimizer can effectively ignore
the invisible index by blocking the creation of query plans with invisible
indexes.

Second, let’s think about what happens when force index is used with invisible
index. Force index will override the invisible index feature. We will just need
to check if the flag for force index is set before ignoring invisible indexes
during exploration.

Third, let’s think about how to disable invisible index features during
constraint check. During Optbuild, we are constructing scan expression on a
given table using `buildScan` under `pkg/sql/opt/optbuilder/select.go`. We can
add a flag to `ScanPrivate` to indicate if this Scan expression was built for a
constraint check. When the factory constructs the scan expression, this flag
will be passed along as a scan operator property.

When the optimizer enumerates indexes on a given Scan operator under
`pkg/sql/opt/xform/scan_index_iter.go`, the optimizer can then check if the scan
is built for constraint check before ignoring the invisible index.

### Foreign key constraint check will be needed:
  - When a parent table performs an `UPDATE` or `DELETE` operation, FK check on the child table is needed.
  - When a child table performs an `INSERT`, `UPSERT`,or `UPDATE` operation, FK check on the parent table is needed.
  - There may be different foreign key actions `[UPDATE | DELETE] [ON CASCADE | SET DEFAULT | SET NULL | NO ACTION | RESTRICT| ON CONSTRAINT]`.
### Unique constraint check will be needed:
  - When `INSERT [ON CONFLICT DO NOTHING | DO UPDATE SET | ON CONSTRAINT | DISTINCT ON]`
  - When `UPSERT`, `UPDATE`

## 2. Syntax
### a. CREATE INDEX, CREATE TABLE, ALTER INDEX statements
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
A new column needs to be added to the output of `crdb_internal.table_indexes` and `information_schema.statistics`.
`crdb_internal.table_indexes`
```
descriptor_id descriptor_name index_id index_name index_type is_unique is_inverted is_sharded ***is_visible*** shard_bucket_count created_at                                                                                              
```

`information_schema.statistics`
```
table_catalog table_schema table_name non_unique index_schema index_name seq_in_index column_name COLLATION cardinality direction storing implicit ***is_visible***
```

## d. Alternative Syntax Considered 
### a. CREATE INDEX, CREATE TABLE, ALTER INDEX statements
Invisible index feature is introducing four new user facing syntax. Since
PostgreSQL does not support the invisible index feature yet, we will use MySQL
and Oracle as a reference for the standardized syntax.

The two options that we have discussed are `NOT VISIBLE` and `INVISIBLE`. 

- Reason why `NOT VISIBLE` is good: CRDB currently supports a similar feature,
invisible column feature. And invisible column feature is using `NOT VISIBLE`
for its syntax. If you are wondering about why the invisible column feature chose
`NOT VISIBLE` over `INVISIBLE`, please look at this PR
https://github.com/cockroachdb/cockroach/pull/26644 for more information.
- Reason why `INVISIBLE` is good: MySQL and Oracle both support `INVISIBLE`. 

**Conclusion**: we have decided that being consistent internally with what CRDB
already has is more important than being consistent with other database engines.

There has been discussion about supporting INVISIBLE as an alias. But this could
lead to more issues: 
1. If we support INVISIBLE as an alias for invisible index
feature, we would have to support INVISIBLE as an alias for the invisible column
feature as well. There are some technical issues in the grammar to do that. 
2. If users are migrating from other database to CRDB, they would need to rewrite
their SQL anyway. 
3. This might lead to confusion when user tries to create invisible columns or
   indexes. Overall, supporting `INVISIBLE` as an alias doesn't seem to provide
   a large benefit.


### b. SHOW INDEX Statements
The three options are `is_hidden`, `is_visible`, and `visible`. 

- Reason why `is_hidden` is good: invisible column feature is using `is_hidden` for [`SHOW COLUMNS`](https://www.cockroachlabs.com/docs/stable/show-columns.html).
- Reason why `visible` is good: this is more consistent what we chose with the first syntax --- VISIBLE | NOT VISIBLE. MySQL is also using [`visible`](https://dev.mysql.com/doc/refman/8.0/en/show-index.html).
- Reason why `is_visible` is bad: less consistent with other columns in [`SHOW INDEX`](https://www.cockroachlabs.com/docs/stable/show-index.html) such as `storing, implicit, non_unique`.

**Conclusion**: `visible` it is more important to stay consistent with the first user-facing syntax.

### c. Tables that store indexes information: `crdb_internal.table_indexes` and `information_schema.statistics`
The three options are `is_hidden`, `is_visible`, and `visible`. 

- Reason why `hidden` is good: Invisible column feature uses `hidden` for `table_columns`.
- Reason why `is_visible` is good: MySQL uses `is_visible`. Also, this is more consistent with other columns in `table_indexes`, such as is_unique, is_inverted, is_sharded.
- Reason why `visibility` is good: Oracle uses `visibility`.

- **Conclusion**: `is_visible` it is more important to stay consistent with the second-user facing syntax.

### d. Index Descriptor

We are also introducing another field in the index descriptor (just for internal
use). The options are `Hidden`, `Invisible`, or `NotVisible`. The invisible
column feature is using `Hidden` in the column descriptor. Using visible or
visibility would be odd as well since the default boolean value is false (by
default, index should be visible).

- **Conclusion**: `NotVisible`. Since we chose `visible` for all invisible index features above, choosing `NotVisible` or `Invisible` here is more consistent. `NotVisible` is preferred here because we are trying to stay away from the keyword `Invisible` to avoid confusion for the first user-facing syntax.

For more context on how this conclusion was drawn, please see https://github.com/cockroachdb/cockroach/pull/83388 and this RFC PR’s discussion

# Fine Grained Control of Index Visibility
As of now, the plan is to introduce the general feature of invisible index
first. The design and implementation details for fine-grained control of index
visibility will be discussed in the future.

Later on, we want to extend this feature and allow a more fine-grained control
of index visibility by introducing the following two features.

1. Indexes are not restricted to just being visible or invisible; users can
   experiment with different levels of visibility. In other words, instead of
   using a boolean invisible flag, users can set a float invisible flag between
   0.0 and 100.0. The index would be made invisible only to a corresponding
   fraction of queries. Related:
   https://github.com/cockroachdb/cockroach/issues/72576#issuecomment-1034301996

2. Different sessions of users can set different index visibility.
   Related: https://github.com/cockroachdb/cockroach/issues/82363

3. We can consider introducing another session variable or another type of
    indexes that provides the exact same behaviour as dropping an index.
