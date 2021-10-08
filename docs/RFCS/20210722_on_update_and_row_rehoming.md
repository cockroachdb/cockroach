- Feature Name: ON UPDATE expressions and REGIONAL BY ROW rehoming
- Status: in-progress
- Start Date: 2021-07-22
- Authors: Peyton Walters
- RFC PR: [#67969](https://github.com/cockroachdb/cockroach/pull/67969)
- Cockroach Issue: [#28281](https://github.com/cockroachdb/cockroach/issues/28281), [#65532](https://github.com/cockroachdb/cockroach/issues/65532)

# Summary

This RFC proposes the addition of ON UPDATE expressions to allow the user to
recalculate column expressions on UPDATE/UPSERT.

This RFC also proposes optionally using an ON UPDATE expression to re-home rows
in REGIONAL BY ROW tables on UPDATE/UPSERT, allowing for rows to be rehomed
post-insert.

# Motivation

In release 21.1, we added REGIONAL BY ROW tables, allowing users to tag rows
with a region identifier in order to home rows in the same table to different
regions. With this addition, we added a default clause to the region column in
case the region is not specified on INSERT, defaulting to the user’s gateway
region and falling back to the user’s primary region in cases where the gateway
region is not a region in the database.

This design presents challenges in the case where the user performs an INSERT in
a region that is not their “home”. For example, if an American user was to
INSERT their data in Europe then travel back to America, their data would stay
homed in Europe, defeating the purpose of this optimization.

This design proposes the addition of an ON UPDATE expression to allow users to
specify an expression to be applied on UPDATE/UPSERT. This extension will then
be used to recalculate `crdb_region` from user’s gateway region on UPDATE/UPSERT
with the idea that continuing to home rows post-insert will lead to more
efficient row placement.

# Technical design

Note: In this RFC, the term UPDATE is used to refer to behavior that should
happen on both UPDATE and UPSERT.

## Update Mechanism

To support rehoming, a "recalculate on UPDATE" mechanism will be implemented.
This mechanism will allow users to mark a column with an expression to be
recalculated whenever a row is modified. This mechanism can be thought of as
similar to DEFAULT expressions in that if a column is not explicitly set in an
UPDATE, it will be filled with the value calculated from the column's ON UPDATE
expression.

Note that this ON UPDATE expression is distinct from a column's DEFAULT
expression i.e., a user can set different DEFAULT and ON UPDATE expressions. It
is also not required that a DEFAULT be specified in order to use ON UPDATE; they
are separate constructs.

### When does ON UPDATE execute?

| Update mechanism         | ON UPDATE applied (Proposal) | Postgres | MySQL |
|--------------------------|:-----------------:|:-------:|:--------:|
| UPDATE                   |         ✅         |✅|✅|
| UPSERT                   |         ✅         |✅|✅|
| ON UPDATE CASCADE        |         ✅         |✅|❌|
| Backfilling a new column |         ❌         |❌|❌|

#### UPDATE

UPDATE statements will apply ON UPDATE expressions when an UPDATE is issued that
does not modify a column with an ON UPDATE. For example, in a table `t(p int, b
int)` where `b` has `ON UPDATE 100` if an UPDATE is issued that only touches
`p`, `b` will be set to 100.

#### UPSERT

UPSERT statements follow the UPDATE rules if the row to be UPSERT already
exists. If the UPSERT creates a new row, however, an ON UPDATE will not be
applied.

#### ON UPDATE CASCADE

If a row in a table with an ON UPDATE column is modified by an ON UPDATE CASCADE
operation, the ON UPDATE expression will be applied. For example, in a table
`t(p int, b int, c int)`, `c` is a foreign key with ON UPDATE CASCADE, and `b` has
ON UPDATE, the ON UPDATE for `b` will be applied when `c` gets a cascade.

This decision is made primarily in order to be semantically consistent with ON
UPDATE in Postgres triggers; Postgres ON UPDATE triggers fire when a row is
modified via an ON UPDATE CASCADE. This decision will become particularly
relevant if triggers are implemented in Cockroach in the future.

This behavior can also be motivated via the row rehoming use case. If a REGIONAL
BY ROW table `rt` has a foreign key onto another RBR table `ft`, we'd like rows
in `rt` to be in the same region as the rows they reference in `ft`. Therefore,
an update to a key in `ft` should require that the region for referencing rows
in `rt` be recalculated, leading to rows in `rt` being in the same region as the
rows they reference in `ft`. See the rehoming section below for more details on
how ON UPDATE will be used in REGIONAL BY ROW tables.

#### Backfills

In the case of a backfill resulting from adding a column, however, the ON UPDATE
clause will not be re-evaluated. Applying the ON UPDATE to all rows is likely
unexpected behavior, particularly when the ON UPDATE captures something unique
about each row (region, last modified timestamp, etc.).

### Same-column foreign key constraints

If a foreign key column has both an ON UPDATE expression and a foreign key ON
UPDATE reference action, it's unclear whether the ON UPDATE should apply after
the reference action since its use would wipe any changes from the reference
action. Because of this, we restrict ON UPDATE to be used only on columns which
do not have a foreign key with an ON UPDATE action.

### Syntax

Since this addition is a qualification on a column, it can be specified at
create time with the following syntax:

```
<column_name> <column_type> <other_qualifications> ON UPDATE <update_expr>
```

Note: In earlier versions of this RFC, the syntax `ON UPDATE SET` was used. Upon
further review, it was realized that it is impossible to disambiguate an ON
UPDATE SET expression from an `ON UPDATE SET [NULL | DEFAULT]` on a foreign key
constraint. For example, the following statement would have been ambiguous:

```sql
CREATE TABLE example(
    p INT,
    j INT REFERENCES other(j) ON UPDATE SET NULL
)
```

The ON UPDATE expression must be standalone i.e., it cannot reference other
columns. The primary use of the expression is to capture context-specific
information like gateway region, current time, current user. The purpose is
_not_ to calculate information based on other columns - for this use case,
computed columns are a better fit. Additionally, just like DEFAULT statements,
volatile expressions like `nextval()` are permitted.

Concretely, creating a table with a `quantity_on_hand` column that applies a
value of 50 ON UPDATE would look like:

```sql
CREATE TABLE inventories (
  product_id        INT,
  warehouse_id      INT,
  quantity_on_hand  INT ON UPDATE 50,
  PRIMARY KEY (product_id, warehouse_id)
);
```

This syntax also applies to `ALTER TABLE ALTER COLUMN`:

```sql
-- Modifying an existing ON UPDATE expression or adding a new one
ALTER TABLE <table_name> ALTER COLUMN <column_name> SET ON UPDATE <update_expr>
-- Dropping an ON UPDATE expression
ALTER TABLE <table_name> ALTER COLUMN <column_name> DROP ON UPDATE
```

Concretely with our inventories example, we have:

```sql
-- Modifying the existing ON UPDATE expression to 50
ALTER TABLE inventories ALTER COLUMN quantity_on_hand SET ON UPDATE 50
-- Dropping an ON UPDATE expression
ALTER TABLE inventories ALTER COLUMN quantity_on_hand DROP ON UPDATE
```

#### Example Usage

```sql
CREATE TABLE inventories (
    product_id        INT,
    warehouse_id      INT,
    quantity_on_hand  INT,
    PRIMARY KEY (product_id, warehouse_id)
  );
INSERT INTO inventories (product_id, warehouse_id, quantity_on_hand) VALUES (1, 1, 1);
SELECT quantity_on_hand FROM inventories;
> 1
UPDATE inventories SET product_id = 2 WHERE warehouse_id = 1;
SELECT quantity_on_hand FROM inventories;
> 1
ALTER TABLE inventories ALTER COLUMN quantity_on_hand SET ON UPDATE 50;
UPDATE inventories SET product_id = 3 WHERE warehouse_id = 1;
SELECT quantity_on_hand FROM inventories;
> 50
UPDATE inventories SET quantity_on_hand = 100;
-- how to UPDATE keeping current value for quantity
UPDATE inventories i SET (product_id, quantity_on_hand) = (4, i.quantity_on_hand);
SELECT quantity_on_hand FROM inventories;
> 100
```

### Storage

The ON UPDATE expression will be added to the column descriptor that it's
applied to as a string field. The existing expression serializing code will be
used to serialize the ON UPDATE expression to the string and to deserialize it
to an expression.

## Row Rehoming

To implement row rehoming, an ON UPDATE expression will be added to the
crdb_region column in REGIONAL BY ROW tables. This will make it so that when an
UPDATE occurs that does not touch `crdb_region`, the `gateway_region()` default
clause will be evaluated and applied.

This has the potential to cause thrashing if UPDATEs happen from many different
regions, so this behavior will be gated behind a cluster setting,
`sql.defaults.auto_rehome.enabled`.

If the user wishes to apply this behavior to just one of their tables as opposed
to applying a cluster-wide setting, they can create the REGIONAL BY ROW table
and alter its `crdb_region` column with an ON UPDATE expression , or they can specify
ON UPDATE on their `crdb_internal_region` column and specify the column with
REGIONAL BY ROW AS.

Example of creating an auto-rehoming table:

```sql
CREATE TABLE test (
  p int,
  region crdb_internal_region
    DEFAULT default_to_database_primary_region(gateway_region())
    SET ON UPDATE default_to_database_primary_region(gateway_region())
)
  LOCALITY REGIONAL BY ROW AS region;
```

## Drawbacks

The primary drawback of this option for rehoming is that ON UPDATE expressions
cannot reference any other columns, so there is no way to implement any
heuristic for choosing when to rehome a row. For example, a heuristic like “only
rehome after 5 sequential UPDATEs” would be impossible to implement. Depending
on the client workload, a heuristic like this may decrease thrashing,
particularly if the vast majority of UPDATEs come from a single region but some
occasionally come from other regions.

Another drawback of modifying a row's region is that the region is part of the table's primary key,
so modifying the region is a primary key modification. This means that in the case
of a table with a column family with an indexed column, the whole family will have to be
fetched for every UPDATE. Non-regional example (note that we must do a full scan after modifying `k`):

```
statement ok
CREATE TABLE t (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX a_idx (a),
  INDEX b_idx (b),
  FAMILY (k, a),
  FAMILY (b)
)

statement ok
INSERT INTO t VALUES (10, 20, 30)

# We do not need to scan the column family with b because b_idx will never
# change.
query T kvtrace
UPDATE t SET a = 220 WHERE k = 10
----
Scan /Table/53/1/10/0
Put /Table/53/1/10/0 -> /TUPLE/2:2:Int/220
Del /Table/53/2/20/10/0
CPut /Table/53/2/220/10/0 -> /BYTES/ (expecting does not exist)

# We must scan the column family with b because b_idx could change, if the PK
# changes.
query T kvtrace
UPDATE t SET a = 222, k = 10 WHERE k = 10
----
Scan /Table/53/1/10{-/#}
Scan /Table/53/1/10/{1/1-#}
Put /Table/53/1/10/0 -> /TUPLE/2:2:Int/222
Del /Table/53/2/220/10/0
CPut /Table/53/2/222/10/0 -> /BYTES/ (expecting does not exist)
```

## Rationale and Alternatives

### ON UPDATE

It's worth noting that the ON UPDATE syntax is heavily inspired by [MySQL's ON
UPDATE
CURRENT_TIMESTAMP](https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html)
syntax. In terms of functionality, however, this proposal is distinct in that it
accepts arbitrary expressions while the only MySQL usage of `ON UPDATE` is `ON
UPDATE CURRENT_TIMESTAMP`. This proposal proposes a more general feature so that
more workloads can be enabled. Because of the [special syntax
form](https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html) for
`current_timestamp()`, `ON UPDATE CURRENT_TIMESTAMP` is valid usage under
this proposal.

This proposal is also distinct from the MySQL implementation in its interaction
with ON UPDATE CASCADE. MySQL does not re-evaluate ON UPDATE when a row is
modified via CASCADE. In this case, it makes sense to deviate from the MySQL
behavior in order to be semantically consistent with Postgres; In particular if
triggers are implemented in the future, having different behavior for ON UPDATE
in triggers vs in qualifications is likely surprising behavior.

### Rehoming Alternatives

#### Alternative 1: Modify UPDATEs to include a computed column

In this solution, when an UPDATE comes in and is being built, it is modified
according to the following logic:

1. If the table being modified is a RBR table with a DEFAULT clause, continue
1. If the UPDATE does not touch the `crdb_region` column, continue
1. Add a computed column for `crdb_region` using the same logic as is used in
the DEFAULT clause for the column i.e. default to the gateway region and fall
back to the primary region

This approach leverages our existing defaulting logic and computed column
infrastructure, so it has the advantage of being fairly easy to implement.
Unfortunately, it requires custom logic in the optimizer which is undesirable as
the optimizer is designed to be as general as possible. See notes on the
prototype for this approach:

https://github.com/cockroachdb/cockroach/pull/66604#pullrequestreview-688564136

#### Alternative 2: Wait for triggers

#### Trigger Value

Triggers are a traditional SQL solution to this problem, and they offer
functionality that ON UPDATE expressions cannot. If triggers were used, row
rehoming could be implemented simply by adding a defaulting trigger to the
`crdb_region` column.

Triggers also have the advantage of being able to execute logic based on the
past and current state of the row in question. This ability opens the door for
us to define a heuristic for choosing when to rehome a row, and it also lets
users define their own heuristics if they prefer.

#### ON UPDATE value

ON UPDATE expressions are valuable over triggers for two primary reasons:

1. User Experience

    Triggers are a complex concept and frequently a high amount of boilerplate
    to achieve a simple goal like ON UPDATE. ON UPDATE expressions provide a
    simpler path for users who don't need the custom logic that triggers
    provide.
   
2. Performance

    Because triggers take row state as a parameter and execute custom logic as a
    result, they must be run outside the context of the update, leading to extra
    operations required outside the UPDATE. With ON UPDATE expressions, however,
    expressions can be evaluated within the context of the UPDATE, keeping 1PC
    UPDATE performance.

#### Takeaway

Triggers are not currently implemented, and their implementation burden is much
higher than that of ON UPDATE expressions, so this proposal is a better
candidate for the short term. Once triggers are implemented, however, rehoming
should be revisited and potentially reimplemented.

# Explain it to folk outside of your team

See motivation and syntax sections for a summary of why we're doing this and
how.

# Unresolved questions

- Is one-shot rehoming good enough? Access patterns where data is frequently
updated from different locations may result in thrashing, which would
significantly decrease performance on the thrashed row. This is probably just
something we'll have to test out.
