- Feature Name: Invisible Index
- Status: draft
- Start Date: 2022-06-28
- Authors: Wenyi Hu
- RFC PR: (TODO (wenyihu6): link PR later)
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/72576,
  https://github.com/cockroachdb/cockroach/issues/82363

# Summary

This new feature introduces the option to make an index become invisible. An
invisible index is an index that is up-to-date but is ignored by the optimizer
unless explicitly specified with [index
hinting](https://www.cockroachlabs.com/docs/v22.1/table-expressions#force-index-selection).
Users can create an index as invisible or alter an index to be invisible after
its initialization. As for now, primary indexes cannot be invisible. But unique 
indexes can still be invisible. Specifically, the unique constraint still prevents 
insertion of duplicates into a column regardless of whether the inedx is invisible.
But the index will be ignored by the optimizer for queries.

The main purpose of this RFC is to introduce the feature, to document different 
choices of potential SQL syntaxes, and ultimately to justify the decision.

# Motivation

Currently, users are not able to observe the impact of removing an index without
risking the cost of rebuilding the index. This new feature would allow users to
validate whether an index should be dropped by changing it to invisible first.
If a drop in query performance is observed, the index can be quickly toggled
back to visible without rebuilding the index.

Similarly, this new feature would also allow users to roll out new indexes with
more confidence. Currently, some users with large production scales are
concerned about the impact of introducing new indexes and potentially affecting
their applications significantly. With this feature, users can create new
indexes and easily toggle it back to invisible without the cost of dropping the
index.

This new feature would also be useful if we want to set an index to be visible
only to specific queries. By using index hinting, users can force an invisible
index to be visible to parts of their applications without affecting the rest of
the application.

# Technical design

## SQL Syntax

This following section will discuss different SQL syntax choices. PostgreSQL
does not support invisible indexes yet. We will be using MySQL and Oracle SQL as
a reference for the standardized way to support invisible index syntax. The
points below outline different choices and their use examples.

Just for reference, the following section shows how SQL syntax now looks like.
The parts surrounded by *** [] ***  propose different options that we can consider.

[//]: # (CREATE INDEX)
1. `CREATE INDEX`
- Create an invisible index by using `CREATE INDEX` in index definition.
- Create an unique invisible index by using `CREATE UNIQUE INDEX` in index definition.
```sql
CREATE [UNIQUE | INVERTED] INDEX [CONCURRENTLY] [IF NOT EXISTS] [<idxname>]
        ON <tablename> ( <colname> [ASC | DESC] [, ...] )
        [USING HASH] [STORING ( <colnames...> )]
        [PARTITION BY <partition params>]
        [WITH <storage_parameter_list>] [WHERE <where_conds...>]
        *** [INVISIBLE | NOT VISIBLE | VISIBLE | HIDDEN] *** 
```

```sql
CREATE INDEX a ON b.c (d) VISIBLE
CREATE INDEX a ON b.c (d) INVISIBLE
CREATE INDEX a ON b.c (d) HIDDEN
CREATE INDEX a ON b.c (d) NOT VISIBLE

CREATE INDEX a ON b (c) WITH (fillfactor = 100, y_bounds = 50) VISIBLE
CREATE INDEX a ON b (c) WITH (fillfactor = 100, y_bounds = 50) INVISIBLE
CREATE INDEX a ON b (c) WITH (fillfactor = 100, y_bounds = 50) HIDDEN
CREATE INDEX a ON b (c) WITH (fillfactor = 100, y_bounds = 50) NOT VISIBLE

CREATE INDEX geom_idx ON t USING GIST(geom) WITH (s2_max_cells = 20, s2_max_level = 12, s2_level_mod = 3) HIDDEN
CREATE INDEX geom_idx ON t USING GIST(geom) WITH (s2_max_cells = 20, s2_max_level = 12, s2_level_mod = 3) INVISIBLE
CREATE INDEX geom_idx ON t USING GIST(geom) WITH (s2_max_cells = 20, s2_max_level = 12, s2_level_mod = 3) NOT VISIBLE

CREATE UNIQUE INDEX IF NOT EXISTS a ON b (c) WHERE d > 3 HIDDEN
CREATE UNIQUE INDEX IF NOT EXISTS a ON b (c) WHERE d > 3 INVISIBLE
CREATE UNIQUE INDEX IF NOT EXISTS a ON b (c) WHERE d > 3 NOT VISIBLE
```
[//]: # (CREATE TABLE)
2. `CREATE TABLE`
- Create an invisible index by adding an index in a `CREATE TABLE...(INDEX)` definition.
- Create an unique invisible index by adding an index in a `CREATE TABLE...(UNIQUE INDEX)` definition.
- Create an unique invisible index by adding an unique constraint within the table constraint definition of a `CREATE TABLE ...(CONSTRAINT ...)`
- Create an unique invisible index by adding an unique constraint within the column definition of a `CREATE TABLE ...(UNIQUE...)` definition.

```sql
 CREATE [[GLOBAL | LOCAL] {TEMPORARY | TEMP}] TABLE [IF NOT EXISTS] <tablename> [table_element_list] [<on_commit>]
```

<blockquote><details>
    <summary>table_element_list</summary>

  <details>
    <summary>Index Definition</summary>

```sql
[UNIQUE | INVERTED] INDEX [<name>] ( <colname> [ASC | DESC] [, ...] 
        [USING HASH] [{STORING | INCLUDE | COVERING} ( <colnames...> )]
        [PARTITION BY <partition params>]
        [WITH <storage_parameter_list>] [WHERE <where_conds...>] 
        *** [INVISIBLE | NOT VISIBLE | VISIBLE | HIDDEN] *** 
``` 

```sql
CREATE TABLE a (b INT8, c STRING, INDEX (b ASC, c DESC) STORING (c) INVISIBLE)
CREATE TABLE a (b INT8, c STRING, INDEX (b ASC, c DESC) STORING (c) NOT VISIBLE)

CREATE TABLE a (b INT, UNIQUE INDEX foo (b) WHERE c > 3 INVISIBLE)
CREATE TABLE a (b INT, UNIQUE INDEX foo (b) WHERE c > 3 NOT VISIBLE)
```

</details>

  <details>
    <summary>Column Constraint Definition</summary>

```sql
[CONSTRAINT <constraintname>] 
          { NULL | NOT NULL | NOT VISIBLE | 
           UNIQUE [WITHOUT INDEX | *** WITH INVISIBLE INDEX  | WITH NOT VISIBLE INDEX ***]
           PRIMARY KEY *** [WITH INVISIBLE INDEX | WITH NOT VISIBLE INDEX | WITHOUT VISIBLE INDEX] *** | CHECK (<expr>) | DEFAULT <expr> | ON UPDATE <expr> | GENERATED { ALWAYS | BY DEFAULT } 
           AS IDENTITY [( <opt_sequence_option_list> )] }
-- Note: primary index cannot be invisible. In this case, the rule is introduced only to throw a semantic error later on.
```

```sql
CREATE TABLE a (b INT8 CONSTRAINT c UNIQUE WITHOUT INDEX)
CREATE TABLE a (b INT8 CONSTRAINT c UNIQUE WITH INVISIBLE INDEX)
CREATE TABLE a (b INT8 CONSTRAINT c UNIQUE WITH HIDDEN INDEX)
CREATE TABLE a (b INT8 CONSTRAINT c UNIQUE WITHOUT VISIBLE INDEX)
CREATE TABLE a (b INT8 CONSTRAINT c UNIQUE WITH NOT VISIBLE INDEX)

CREATE TABLE a (b INT8 CONSTRAINT c PRIMARY KEY INVISIBLE)  --/ semantic error
CREATE TABLE a (b INT8 CONSTRAINT c PRIMARY KEY NOT VISIBLE)  -- semantic error
CREATE TABLE a (b INT8 CONSTRAINT c PRIMARY KEY HIDDEN)  -- semantic error
```
  </details>

  <details>
    <summary>Table Constraint Definition</summary>

```sql
    UNIQUE [WITHOUT INDEX | *** WITH INVISIBLE INDEX  | WITH NOT VISIBLE INDEX ***] ( <colnames...> ) [{STORING | INCLUDE | COVERING} ( <colnames...> )]
    PRIMARY KEY ( <colnames...> ) [USING HASH] *** [WITH INVISIBLE INDEX | WITH NOT VISIBLE INDEX] ***  -- Note: primary index cannot be invisible. In this case, the rule is introduced only to throw a semantic error later on.
```
```sql
CREATE TABLE a (b INT8, c STRING, CONSTRAINT d UNIQUE WITH INVISIBLE INDEX (b, c))
CREATE TABLE a (b INT8, c STRING, CONSTRAINT d UNIQUE WITH HIDDEN INDEX (b, c))
CREATE TABLE a (b INT8, c STRING, CONSTRAINT d UNIQUE WITH NOT VISIBLE INDEX (b, c))
CREATE TABLE a (b INT8, c STRING, CONSTRAINT d UNIQUE WITHOUT VISIBLE INDEX (b, c))

CREATE TABLE a (b INT8, c STRING, CONSTRAINT d UNIQUE (b) INVISIBLE INDEX)
CREATE TABLE a (b INT8, c STRING, CONSTRAINT d UNIQUE (b) HIDDEN INDEX)
CREATE TABLE a (b INT8, c STRING, CONSTRAINT d UNIQUE (b) NOT VISIBLE INDEX)

CREATE TABLE a (b INT8, c STRING, PRIMARY KEY (b, c, "0") INVISIBLE) -- semantic error
CREATE TABLE a (b INT8, c STRING, PRIMARY KEY (b, c, "0") HIDDEN) -- semantic error
CREATE TABLE a (b INT8, c STRING, PRIMARY KEY (b, c, "0") NOT VISIBLE) -- semantic error

```
  </details>

  </blockquote></details>

[//]: # (Alter Table)
3. `Alter Table`
- Create an unique invisible index by adding unique constraint within the table constraint definition of an `ALTER TABLE <name> ADD CONSTRAINT ...`
- Create an unique invisible index by adding unique constraint within the column definition of `ALTER TABLE <name> ADD <coldef>`, `ALTER TABLE <name> ADD IF NOT EXISTS <coldef>`, `ALTER TABLE <name> ADD COLUMN <coldef>`, `ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef>`.

```sql
   ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS] <colname> <type> [<constraint...>]
   ALTER TABLE ... ADD <constraint>
   ALTER TABLE ... ALTER PRIMARY KEY USING INDEX <name> -- Note: primary index cannot be invisible. In this case, the rule is introduced only to throw a semantic error later on.
```

<blockquote><details>
<summary>constraint</summary>

```sql
[CONSTRAINT <constraintname>] {NULL | NOT NULL | UNIQUE [WITHOUT INDEX | *** WITH INVISIBLE INDEX  | WITH NOT VISIBLE INDEX *** ]| PRIMARY KEY [*** WITH INVISIBLE INDEX  | WITH NOT VISIBLE INDEX *** ]| CHECK (<expr>) | DEFAULT <expr>}
```

```sql
ALTER TABLE a ADD CONSTRAINT a_idx UNIQUE WITH INVISIBLE INDEX (a)
ALTER TABLE a ADD CONSTRAINT a_idx UNIQUE WITH HIDDEN INDEX (a)
ALTER TABLE a ADD CONSTRAINT a_idx UNIQUE WITHOUT VISIBLE INDEX (a)
ALTER TABLE a ADD CONSTRAINT a_idx UNIQUE WITH NOT VISIBLE INDEX (a)

ALTER TABLE IF EXISTS a ADD COLUMN b INT8 UNIQUE WITH INVISIBLE INDEX, ADD CONSTRAINT a_no_idx UNIQUE WITH INVISIBLE INDEX (a)
ALTER TABLE IF EXISTS a ADD COLUMN b INT8 UNIQUE WITH HIDDEN INDEX, ADD CONSTRAINT a_no_idx UNIQUE WITH HIDDEN INDEX (a)
ALTER TABLE IF EXISTS a ADD COLUMN b INT8 UNIQUE WITH NOT VISIBLE INDEX, ADD CONSTRAINT a_no_idx UNIQUE WITH NOT VISIBLE INDEX (a)
ALTER TABLE IF EXISTS a ADD COLUMN b INT8 UNIQUE WITHOUT VISIBLE INDEX, ADD CONSTRAINT a_no_idx UNIQUE WITHOUT VISIBLE INDEX (a)
```
</blockquote></details>

[//]: # (Alter Index)
4. `Alter Index`
```sql
ALTER INDEX [IF EXISTS] <idxname> [INVISIBLE | NOT VISIBLE | HIDDEN]
```

```sql
ALTER INDEX a@b INVSIBLE
ALTER INDEX a@b NOT VISIBLE
ALTER INDEX a@b HIDDEN
```

[//]: # (Show Constraint)
5. `Show Constraint`
```sql
SHOW CONSTRAINT FROM table_name with_comment
```

```sql
table_name            constraint_name             constraint_type  details(*** add invisible index details ***)      validated
unique_without_index  my_partial_unique_f         UNIQUE           UNIQUE WITH INVISIBLE INDEX (f) WHERE (f > 0)     true
unique_without_index  my_partial_unique_f         UNIQUE           UNIQUE WITH HIDDEN INDEX (f) WHERE (f > 0)        true
unique_without_index  my_partial_unique_f         UNIQUE           UNIQUE WITH NOT VISIBLE INDEX (f) WHERE (f > 0)   true
```

[//]: # (Show Index)
6. A new column needs to be added to `crdb_internal.table_indexes`.
```
descriptor_id  descriptor_name  index_id  index_name  index_type  is_unique  is_inverted  is_sharded  ***is_hidden***       shard_bucket_count created_at
                                                                                                      ***is_invisible*** 
                                                                                                      ***is_not_invisible*** 
                                                                                                      ***visibility*** 
```

7. A new column needs to be added to the output of following SQL statements:
```sql
SHOW INDEX FROM (table_name) 
SHOW INDEXES FROM(table_name) 
SHOW KEYS FROM (table_name) 

SHOW INDEX FROM DATABASE(database_name) 
SHOW INDEXES FROM DATABASE (database_name) 
SHOW KEYS FROM DATABASE (database_name) 
```

```
table_name  index_name  non_unique  seq_in_index  column_name  direction  storing  implicit ***is_hidden***
                                                                                            ***is_invisible***
                                                                                            ***is_not_invisible*** 
                                                                                            ***visibility***
```

7. Note that `CREATE CONSTRAINT` and `ALTER CONSTRAINT` are both not supported by the parser.

### Discussion 
CockroachDB currently supports invisible column feature. For this
feature, `NOT VISIBLE` is used for its SQL statement, and `is_hidden` is used
for the new column added to `SHOW INDEX`. It would be nice to stay consistent.
But [MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html) and
[Oracle](https://oracle-base.com/articles/11g/invisible-indexes-11gr1) both
support `INVISIBLE` for invisible indexes. [MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-columns.html), [Oracle](https://oracle-base.com/articles/12c/invisible-columns-12cr1) 
also use `INVISIBLE` for the invisible columns.
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html) use
`is_visible` for the new column added to `SHOW INDEX`. 
[Oracle](http://www.dba-oracle.com/t_11g_new_index_features.htm) uses
`VISIBILITY` for the new column added to `SHOW INDEX`.

I was wondering why `NOT VISIBLE` was chosen for the invisible column feature.
I tried changing it to `INVISIBLE` in `sql.y`, and it caused conflicts in the
grammar. I'm not sure if this was the reason why we chose `NOT VISIBLE`.
PostgreSQL currently doesn't support invisible index or invisible column
feature. We can also try supporting both `NOT VISIBLE` and `INVISIBLE`, but more
work would be needed to find a grammar rule that allows both.

### Update on Discussion
Invisible index feature is introducing three new user facing syntaxes. 
When users are creating a new invisible index with `CREATE TABLE`, `CREATE
INDEX`, or `ALTER INDEX`, they will need to specify whether the index is
invisible. The two options that we have discussed are `NOT VISIBLE`,
`INVISIBLE`.
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html) and
[Oracle](https://oracle-base.com/articles/11g/invisible-indexes-11gr1) both
support `INVISIBLE` instead. Invisible column feature is using `NOT VISIBLE`.
But we have decided that being consistent with the invisible column feature is
more important.

The second user-facing syntax is related to SQL statements like`SHOW INDEX`. The
three options are `is_hidden`, `visible`, or `is_visible`.
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/show-index.html) is using
`visible`. Invisible column feature is using
[`is_hidden`](https://www.cockroachlabs.com/docs/stable/show-columns.html). And
now we want to decide if it is more important to stay consistent with the
invisible column feature and use `is_hidden` or to stay consistent with the
first user-facing syntax and use `is_visible` or `visible`.

The third user-facing syntax is with `crdb_internal.table_indexes`. Invisible
column feature is using `hidden` in `table_columns`. MySQL uses
[`INFORMATION_SCHEMA.STATISTICS`](https://dev.mysql.com/doc/refman/8.0/en/information-schema-statistics-table.html)
and has `is_visible`. Oracle uses
[`all_indexes`](https://docs.oracle.com/cd/E18283_01/server.112/e17110/statviews_1106.htm)
and has `visibility.`

We are also introducing another field in the index descriptor (just for internal
use). The options are `hidden`, `invisible`. The invisible column feature is
using `hidden` in column descriptor.  If we decide to choose `visible` for all
invisible index syntax mentioned above, we can use `invisible` here to be more
consistent. My slight preference is to use either hidden or invisible (not
visible) for the index descriptor since the default behavior for a new index is
visible, and the default boolean value is false.

Another thing to note is that MySQL introduces a new column in
`INFORMATION_SCHEMA.STATISTICS`. We are now introducing a new column in 
`crdb_internal.table_indexes`, but we can add a new column to
`INFORMATION_SCHEMA.STATISTICS` as well.

### Technical Design

Constraint check with this invisible index feature adds another level of
complexity to consider. To illustrate, consider when there is a parent table and
a child table. A unique index or a unique constraint is required on the parent
table column in order to create a child table referencing the column. When user
performs an insert on the child table, the optimizer will choose this unique
index to perform a FK check. However, user can now mark an unique index on the
parent table as invisible. The question now is whether the optimizer should
still ignore this invisible unique index or not. If not, the optimizer would
have to perform a full table scan for an INSERT statement. The same question
applies when we try to perform an unique constraint check. If yes, the optimizer
is using an invisible index.

The decision is to temporarily disable invisible index feature during any
constraint checks. 

This has some benefits. 
1. This is the standardized way in other SQL engines, such as MySQL. 
2. Creating a child table requires an unique constraint on the parent table.
 Marking this unique index as invisible doesn't really make sense. If user tries
 to do that, we should still use this index for constraint check. This almost
 never causes any performance drops.

This also has some drawbacks. Users can no longer mark an index as invisible and
believe that this is equivalent to dropping an index.

During constraint check, we still want to use the invisible index.

Concerns are mainly about insert, update, delete, upsert, and especially when
they are used with FK parent and child tables. When we are performing insert to
the child table, a constraint check on the parent table will take place. And an
invisible unique index will cause some concerns -> potential full table scan.
This is a FK check. When we are performing delete or update on the parent table,
a constraint check on the child table will take place. And an invisible
secondary or unique index on the FK column will cause some concerns -> also
potential full table scan.  This is a constraint check. To solve this problem,
we can consider having a flag whenever we are performing some constraint check.
This is a problem even if we are just supporting invisible non unique indexes.
When we are inserting or updating on conflict in just one table, this is not a
constraint check during EXPLAIN. Would require some other flags.

We could consider use this as a general rule: When users are trying to change a
constraint that is necessary to perform certain actions, we should either
disallow this by giving an error or we just use the index as they are visible
and give a reminder that we are using the index. Special case for FK secondary
index on child table as it was not necessary, but we will still use the index
for constraint check. For example, a unique constraint was necessary to perform
INSERT ON CONFLICT. If the user tries to make that unique constraint invisible,
we will either give an error or give some hint to tell the user that we will be
treating this constraint as visible if they try to perform INSERT ON CONFLICT
later on. Similarly for FK constraints, we can give an error or a hint when they
are trying to mark the unique index on parent table or index on FK in child
table invisible.



To disable invisible index during any constraint check...
Whenever we are calling buildScan in `pkg/sql/opt/optbuilder/select.go`, we will pass in a boolean flag to indicate if the invisible index feature should be disabled during the scan.
Inside buildScan, it will pass in this flag to scanPrivate.Flags for ForEachStartingAfter under `pkg/sql/opt/xform/scan_index_iter.go`

This following section summarizes which part of optimizer disables invisible index feature and which part enables. 
BuildScan is called in the following functions. 
1. `func (cb *onDeleteFastCascadeBuilder) Build`: called by planCascade for ON DELETE CASCADE disable invisible index
2. `func (b *Builder) buildDeleteCascadeMutationInput`: called by onDeleteSetBuilder and also by onDeleteCascadeBuilder for ON DELETE CASCADE, ON DELETE SET DEFAULT, ON DELETE SET NULL, disable invisible index
3. `func (b *Builder) buildUpdateCascadeMutationInput`: called by planCascade for ON UPDATE CASCADE, SET DEFAULT, SET NULL, disable invisible index
4. `func (mb *mutationBuilder) buildInputForDelete`: called by buildInputForDelete which builds a memo group for DeleteOp, enable invisible index
5. `func (mb *mutationBuilder) buildInputForUpdate`: called by buildInputForUpdate which builds a memo group for UpdateOp, enable invisible index
6. `func (mb *mutationBuilder) buildAntiJoinForDoNothingArbiter`: called by buildInsertForNothing INSERT ON CONFLICT DO NOTHING, disable invisible index
7. `func (mb *mutationBuilder) buildLeftJoinForUpsertArbiter`: called by buildInputForUpsert INSERT, UPSERT ON CONFLICT, DO UPDATE SET, disable invisible index
8. `func (h *arbiterPredicateHelper) tableScope`: called by findArbiters INSERT ON CONFLICT DO NOTHING, UPSERT, INSERT ON CONFLICT DO UPDATE SET, disable invisible index
9. `func (h *fkCheckHelper) buildOtherTableScan`: called by buildFKChecksAndCascadeForDelete, buildFKChecksForUpdate, buildFKChecksForUpsert, buildFKChecksForInsert, disable invisible index
10. `func (h *uniqueCheckHelper) buildTableScan`: called by buildUniqueChecksForInsert, buildUniqueChecksForUpdate, buildUniqueChecksForUpsert, disable invisible index
11. `func (b *Builder) buildDataSource`: called by buildJoin, buildInputForUpdate, buildSelectClause, enable invisible index
12. `func (b *Builder) buildScanFromTableRef`: called by buildDataSource

## Later Discussion: Fine-Grained Control of Index Visibility
Later on, we want to extend this feature and allow a more fine-grained control
of index visibility by introducing the following two features.

As of now, the plan is to introduce the general feature of invisible index
first. The design and implementation details for fine-grained control of index
visibility will be added later on.

1. Indexes are not restricted to just being visible or invisible; users can experiment
   with different levels of visibility. In other words, instead of using a boolean
   invisible flag, users can set a float invisible flag between 0.0 and 1.0. The
   index would be made invisible only to a corresponding fraction of queries. 
   Related: https://github.com/cockroachdb/cockroach/issues/72576#issuecomment-1034301996

2. Different sessions of a certain user or application can set different index
   visibilities for indexes.
   Related: https://github.com/cockroachdb/cockroach/issues/82363
