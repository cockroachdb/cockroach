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
- Create an unique invisible index by using `CREATE UNIQUE INDEX` in index definition.
```
CREATE [UNIQUE | INVERTED] INDEX [CONCURRENTLY] [IF NOT EXISTS] [<idxname>]
        ON <tablename> ( <colname> [ASC | DESC] [, ...] )
        [USING HASH] [STORING ( <colnames...> )]
        [PARTITION BY <partition params>]
        [WITH <storage_parameter_list>] [WHERE <where_conds...>]
        *** [INVISIBLE | NOT VISIBLE | VISIBLE | HIDDEN] *** 
```

[//]: # (CREATE TABLE)
2. `CREATE TABLE`
- Create an invisible index by adding an index in a `CREATE TABLE...(UNIQUE INDEX)` definition.
- Create an unique invisible index by adding an unique constraint within the table constraint definition of a `CREATE TABLE ...(CONSTRAINT ...)`
- Create an unique invisible index by adding an unique constraint within the column definition of a `CREATE TABLE ...(UNIQUE...)` definition.

```
 CREATE [[GLOBAL | LOCAL] {TEMPORARY | TEMP}] TABLE [IF NOT EXISTS] <tablename> [table_element_list] [<on_commit>]
```

<blockquote><details>
    <summary>table_element_list</summary>

  <details>
    <summary>Index Definition</summary>

```
[UNIQUE | INVERTED] INDEX [<name>] ( <colname> [ASC | DESC] [, ...] 
        [USING HASH] [{STORING | INCLUDE | COVERING} ( <colnames...> )]
        [PARTITION BY <partition params>]
        [WITH <storage_parameter_list>] [WHERE <where_conds...>] *** [INVISIBLE | NOT VISIBLE | VISIBLE | HIDDEN] *** 
``` 
</details>

  <details>
    <summary>Column Constraint Definition</summary>

    [CONSTRAINT <constraintname>] 
          { NULL | NOT NULL | NOT VISIBLE | 
           UNIQUE [WITHOUT INDEX | *** WITH INVISIBLE INDEX  | WITH NOT VISIBLE INDEX ***]
           PRIMARY KEY *** [WITH INVISIBLE INDEX | WITH NOT VISIBLE INDEX] *** | CHECK (<expr>) | DEFAULT <expr> | ON UPDATE <expr> | GENERATED { ALWAYS | BY DEFAULT } 
           AS IDENTITY [( <opt_sequence_option_list> )] }
    // Note: primary index cannot be invisible. In this case, the rule is introduced only to throw a semantic error later on.
  </details>

  <details>
    <summary>Table Constraint Definition</summary>

    UNIQUE [WITHOUT INDEX | *** WITH INVISIBLE INDEX  | WITH NOT VISIBLE INDEX ***] ( <colnames...> ) [{STORING | INCLUDE | COVERING} ( <colnames...> )]
    PRIMARY KEY ( <colnames...> ) [USING HASH] *** [WITH INVISIBLE INDEX | WITH NOT VISIBLE INDEX] ***  // Note: primary index cannot be invisible. In this case, the rule is introduced only to throw a semantic error later on.
  </details>

  </blockquote></details>

[//]: # (Alter Table)
3. `Alter Table`
- Create an unique invisible index by adding unique constraint within the table constraint definition of an `ALTER TABLE <name> ADD CONSTRAINT ...`
- Create an unique invisible index by adding unique constraint within the column definition of `ALTER TABLE <name> ADD <coldef>`, `ALTER TABLE <name> ADD IF NOT EXISTS <coldef>`, `ALTER TABLE <name> ADD COLUMN <coldef>`, `ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef>`.

```
   ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS] <colname> <type> [<constraint...>]
   ALTER TABLE ... ADD <constraint>
   ALTER TABLE ... ALTER PRIMARY KEY USING INDEX <name> // Note: primary index cannot be invisible. In this case, the rule is introduced only to throw a semantic error later on.
```

<blockquote><details>
<summary>constraint</summary>

```
[CONSTRAINT <constraintname>] {NULL | NOT NULL | UNIQUE [WITHOUT INDEX | *** WITH INVISIBLE INDEX  | WITH NOT VISIBLE INDEX *** ]| PRIMARY KEY [*** WITH INVISIBLE INDEX  | WITH NOT VISIBLE INDEX *** ]| CHECK (<expr>) | DEFAULT <expr>}
```
</blockquote></details>

[//]: # (Alter Index)
4. `Alter Index`
```
ALTER INDEX [IF EXISTS] <idxname> [INVISIBLE | NOT VISIBLE]
```

[//]: # (Show Constraint)
5. `Show Constraint`
```
SHOW CONSTRAINT FROM table_name with_comment
```
```
table_name  constraint_name  constraint_type  details(*** add invisible index details ***)  validated
```

[//]: # (Show Index)
6. A new column needs to be added to `crdb_internal.table_indexes`.
```
descriptor_id  descriptor_name  index_id  index_name  index_type  is_unique  is_inverted  is_sharded *** is_hidden | is_invisible | is_not_visible *** shard_bucket_count created_at
```

7. A new column needs to be added to the output of following SQL statements:
``` 
SHOW INDEX FROM (table_name) 
SHOW INDEXES FROM(table_name) 
SHOW KEYS FROM (table_name) 
```
```
table_name  index_name  non_unique  seq_in_index  column_name  direction  storing  implicit *** is_hidden | is_invisible | is_not_visible ***  
```

```
SHOW INDEX FROM DATABASE(database_name) 
SHOW INDEXES FROM DATABASE (database_name) 
SHOW KEYS FROM DATABASE (database_name) 
```
```
table_name  index_name  non_unique  seq_in_index  column_name  direction  storing  implicit *** is_hidden | is_invisible | is_not_visible ***  
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