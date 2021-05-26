- Feature Name: Expression Indexes
- Status: in-progress
- Start Date: 2021-06-15
- Authors: Marcus Gartner
- RFC PR: [#65756](https://github.com/cockroachdb/cockroach/pull/65756)
- Cockroach Issue: [#9682](https://github.com/cockroachdb/cockroach/issues/9682)

# Summary

This RFC proposes a design for supporting expression indexes, also known as
"functional indexes", "computed indexes", and "calculated indexes" in other
relational databases.

An expression index allows a user to index an expression or multiple
expressions. For example:

```sql
-- Create an expression index.
CREATE INDEX lower_name_idx ON users (lower(name));

-- This query can scan lower_user_idx.
SELECT * FROM users WHERE lower(name) = 'marcus';
```

# Motivation

Expression indexes are a common feature of relational databases, including
Postgres. A typical use case is creating an expression index that allows for
efficient case-insensitive searches on a `STRING` column.

# Technical design

Expression indexes are syntactic sugar. Creating one is functionally equivalent
to creating an index on a virtual computed column. This design focuses on the
behavior of this abstraction.

This design omits details about maintaining the state of expression indexes and
using them to service a query. To the optimizer and execution engine, an
expression index is the same as an index on a virtual column, which has been
supported since 21.1.0.

### Requirements

This design in influenced by the primary criteria:

* Users should see expressions, not names of virtual columns, when expression
  indexes are displayed, e.g., in `SHOW CREATE TABLE`.
* `DROP INDEX` should drop virtual columns that were created by an expression
  index.
* Executing the output of `SHOW CREATE TABLE` with a table containing an
  expression index should create an identical table.

### Inaccessible columns

In order to satisfy these requirements, a new boolean `Inaccessible` field will
be added to column descriptors. An inaccessible column:

1. Is not shown to users when inspecting tables via `SHOW CREATE TABLE` or when
querying `pg_catalog` tables.
2. Cannot be manually created by a user.
3. Cannot be referenced in computed column expressions, indexes, partial index
predicates, foreign keys, or `CHECK` constraints.
4. Cannot be referenced in queries.
5. Does not appear in star `*` expansion.

### Creating an expression index

An expression index is created in a `CREATE TABLE` or `CREATE INDEX` statement.
It can index a single expression, multiple expressions, or a combination of
expressions and columns. Expression indexes can be `UNIQUE` or `INVERTED`, but a
`PRIMARY` index cannot include expressions.

```sql
CREATE TABLE users (
    k INT PRIMARY KEY,
    name STRING,
    games_played INT,
    games_won INT,
    ranking INT,
    profile JSON,
    INDEX users_games_lost_idx ((games_played - games_won))
);

CREATE UNIQUE INDEX ON users (lower(name));

CREATE INDEX ON users (games_won, (games_played - games_won), lower(name));

CREATE INVERTED INDEX ON users ((profile->'bio'));
```

Index expressions in `CREATE TABLE` and `CREATE INDEX` statements must be
wrapped in parentheses, unless they are a single function expression, as shown
above. This matches Postgres's behavior. It may be possible to lift the
requirement for extra parentheses, which can be explored after the initial
implementation is complete.

When an expression index is created, an _inaccessible_ virtual column is
created to represent each expression in the index. This column is included in
the index descriptor's list of indexed columns. The column name starts with
`crdb_internal_idx_expr`, with an incrementing integer appended to prevent
conflicts when there is more than one expression index column in the table.

If the user does not specify an index name, we follow Postgres's index naming
conventions which include `_expr[N]` for each indexed expression.

```sql
-- Creates an index named t_expr_idx.
CREATE INDEX ON t ((a + b));

-- Creates an index named t_expr_expr1_idx.
CREATE INDEX ON t ((a + b), (a + 10));

-- Creates an index named t_expr_expr1_b_expr2_idx.
CREATE INDEX ON t ((a + 1), (a + 2), b, (a + 3));
```

### Dropping an expression index

When an index is dropped, any inaccessible indexed columns are dropped. This is
safe because it is guaranteed that an inaccessible column is not referenced
elsewhere in the table's descriptor.

The inaccessibility of a column allows us to differentiate between a virtual
column created for an expression based index (which we want to drop) and a
virtual column created manually by a user (which we do not want to drop).

For example:

```sql
CREATE TABLE t (
    k INT PRIMARY KEY,
    a INT,
    v INT AS (a + 10) VIRTUAL,
    INDEX v_idx (v),
    INDEX expr_idx ((a + 100))
);

-- Dropping expr_idx drops the inaccessible crdb_internal_idx_expr column.
DROP INDEX expr_idx;

-- Dropping v_idx does not drop v.
DROP INDEX v_idx;

SELECT create_statement FROM [SHOW CREATE TABLE t];
create_statement
------------------------------------------------------
CREATE TABLE public.t (
    k INT8 NOT NULL,
    a INT8 NULL,
    v INT8 NULL AS (a + 10:::INT8) VIRTUAL,
    CONSTRAINT "primary" PRIMARY KEY (k ASC)
);
```

### Displaying an expression index

When displaying an expression index to a user, any inaccessible virtual computed
columns in the index are displayed as the expression they represent, rather than
being displayed as their column name.

The inaccessibility of a column is again used to differentiate between a virtual
column created for an expression index and a virtual column created manually.
For the latter, we show the column name, not the expression when displaying the
index.

For example:

```sql
CREATE TABLE t (
    k INT PRIMARY KEY,
    a INT,
    v INT AS (a + 10) VIRTUAL,
    INDEX v_idx (v),
    INDEX expr_idx ((a + 100))
);

SELECT create_statement FROM [SHOW CREATE TABLE t];
create_statement
------------------------------------------------------
CREATE TABLE public.t (
    k INT8 NOT NULL,
    a INT8 NULL,
    v INT8 NULL AS (a + 10:::INT8) VIRTUAL,
    CONSTRAINT "primary" PRIMARY KEY (k ASC),
    INDEX v_idx (v ASC),
    INDEX expr_idx ((a + 100:::INT8) ASC)
);
```

### Unique expression indexes

To fully support unique expression indexes, `INSERT ... ON CONFLICT` statements
will allow specifying conflict expressions, in addition to columns. Indexes
with matching expressions are chosen as arbiter indexes. For example:

```sql
CREATE TABLE t (
    k INT PRIMARY KEY,
    a INT,
    UNIQUE INDEX t_a_plus_10_key ((a + 10))
);

EXPLAIN INSERT INTO t VALUES (1, 10) ON CONFLICT ((a + 10)) DO NOTHING;
                       info
------------------------------------------------------
  distribution: local
  vectorized: true

  • insert
    │ into: t(k, a)
    │ auto commit
    │ arbiter indexes: t_a_plus_10_key
    │
    ...
```

## Alternatives considered

Multiple alternatives were explored, including some where the `Inaccessible`
column field were not necessary. Ultimately, these designs either failed to meet
the primary requirements, or did not closely match the behavior of Postgres.

For example, using a hidden (`NOT VISIBLE`), virtual computed columns was
considered. However, these columns are included in the output of `SHOW CREATE
TABLE`, which makes it hard to design logic that creates a table identical to
the output of this statement. Naive approaches create duplicate virtual columns
(one for the displayed column and another for the expression index). Crafty but
brittle solutions rely on reusing existing virtual columns when creating an
expression index, or hard-coded edge cases for specific column name prefixes.
