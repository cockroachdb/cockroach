- Feature Name: Expression Indexes
- Status: draft
- Start Date: 2021-05-26
- Authors: Marcus Gartner
- RFC PR: TODO
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

# Terminology

These existing concepts appear throughout the document.

- **virtual [computed] column:** A column based on an expression that is
computed at query time. Its value is not stored in the primary index. A
secondary index can index a virtual column.
  
- **hidden column:** A column that does not appear in `*` expansion. Hidden
columns can be automatically created by the database, for example `rowid`. They
can also be created by a user with the `NOT VISIBLE` option. They are displayed
in `SHOW CREATE TABLE` with the `NOT VISIBLE` designation.

# Technical design

Expression indexes are syntactic sugar. Creating one is equivalent to creating
an index on a virtual computed column. This design focuses on the behavior of
this abstraction. The design covers the three major interactions with expression
indexes: creating them, dropping them, and displaying them.

This design omits details about maintaining the state of expression indexes and
using them to service a query. To the optimizer and execution engine, an
expression index is the same as an index on a virtual column. Virtual columns
have been supported since 21.1.0, so no execution or optimizer changes are
required.

## Requirements

The design was chosen because it satisfies these criteria:

**A. Users should see expressions, not names of virtual columns, when expression
indexes are displayed, e.g., in `SHOW CREATE TABLE`.**

**B. `DROP INDEX` should drop virtual columns that were created by an expression
index, when possible.**

**C. Executing the output of `SHOW CREATE TABLE` should create an identical
table.**

### Creating an expression index

An expression index is created in a `CREATE TABLE` or `CREATE INDEX` statement.
It can index a single expression, multiple expressions, or a combination of
expressions and columns.

```sql
CREATE TABLE t (
    k INT PRIMARY KEY,
    a INT,
    b INT,
    c STRING,
    INDEX ((a + 10)),
    UNIQUE INDEX ((a + b))
);

CREATE INDEX i ON t (lower(c), a);
```

A hidden virtual column represents each expression in an expression index. This
column is included in the index descriptor's list of indexed columns. The column
name is prefixed with `crdb_idx_expr`, denoting that the column was created for
an expression index. This prefix is critical for satisfying **A**, **B**, and
**C**.

When an expression index is created, we attempt to use an existing column if
possible. An existing column can be used if:

1. It has a name with the prefix `crdb_idx_expr`.
2. It is a hidden virtual column.
3. It has a matching expression.

If no such column exists, a new hidden virtual column is created.

### Dropping an expression index

When an expression index is dropped, the hidden virtual column(s) that represent
its expression(s) are dropped, if possible.

Notice that an index descriptor includes no information to denote whether or not
it is an expression index. An expression index loses this identity as soon as
the AST representing it is converted into an index descriptor. This poses two
challenges:

1. How can we drop virtual columns used by an expression index if we cannot
determine an index is expression-based.
2. How can we differentiate an indexed virtual column created for an expression
index with one created manually by a user. We want to drop only the former, not
the latter.

To address these challenges, when dropping any index, an indexed column is
dropped if:

1. It has a name with the prefix `crdb_idx_expr`.
2. It is a hidden virtual column.
3. It is not referenced by another index, partial index predicate, constraint,
or computed column expression.

### Displaying an expression index

When displaying an expression index to a user, the expressions it indexes are
displayed, not the names of virtual columns that represent the expression.

As mentioned in the section above, nothing in an index descriptor denotes that
it is an expression index. This creates challenges in displaying expression
indexes similar to the challenges in dropping them.

To address these challenges, when displaying any index, an indexed virtual
column's expression is displayed rather than its name if:

1. It has a name with the prefix `crdb_idx_expr`.
2. It is a hidden virtual column.

When the table containing an expression index is displayed, the hidden virtual
column(s) that the index relies on is displayed as `NOT VISIBLE`. This is
standard for all hidden columns.

The `crdb_idx_expr` column name prefix communicates that a column originated
from an expression index. This is critical for satisfying **C**. It must be
communicated that a column was created for an expression index to avoid creating
a duplicate column when the output of `SHOW CREATE TABLE` is executed. The
output of `SHOW CREATE TABLE` cannot communicate arbitrary information about
columns; it must be valid SQL.

## Putting it all together

The following example shows the outcome of creating, dropping, and displaying
expression indexes.

```sql
CREATE TABLE t (
    k INT PRIMARY KEY,
    a INT,
    b INT,
    INDEX t_a_plus_b_idx ((a + b))
);

SELECT create_statement FROM [SHOW CREATE TABLE t];
                  create_statement
------------------------------------------------------
CREATE TABLE public.t (
    k INT8 NOT NULL,
    a INT8 NULL,
    b INT8 NULL,
    crdb_idx_expr INT8 NOT VISIBLE NULL AS (a + b) VIRTUAL,
    CONSTRAINT "primary" PRIMARY KEY (k ASC),
    INDEX t_a_plus_b_idx ((a + b) ASC)
);

DROP INDEX t_a_plus_b_idx;

SELECT create_statement FROM [SHOW CREATE TABLE t];
                  create_statement
------------------------------------------------------
CREATE TABLE public.t (
    k INT8 NOT NULL,
    a INT8 NULL,
    b INT8 NULL,
    CONSTRAINT "primary" PRIMARY KEY (k ASC)
);
```

The following example shows the behavior when executing the output of `SHOW
CREATE TABLE` of a table with an expression index. Notice that an identical
table is created. The `crdb_idx_expr` hidden virtual column is not duplicated
because the expression index uses it rather than creating a new column.

```sql
CREATE TABLE public.t (
    k INT8 NOT NULL,
    a INT8 NULL,
    b INT8 NULL,
    crdb_idx_expr INT8 NOT VISIBLE NULL AS (a + b) VIRTUAL,
    CONSTRAINT "primary" PRIMARY KEY (k ASC),
    INDEX t_a_plus_b_idx ((a + b) ASC)
);

SELECT create_statement FROM [SHOW CREATE TABLE t];
                  create_statement
------------------------------------------------------
CREATE TABLE public.t (
    k INT8 NOT NULL,
    a INT8 NULL,
    b INT8 NULL,
    crdb_idx_expr INT8 NOT VISIBLE NULL AS (a + b) VIRTUAL,
    CONSTRAINT "primary" PRIMARY KEY (k ASC),
    INDEX t_a_plus_b_idx ((a + b) ASC)
);
```

## Drawbacks

### Relying on the name of virtual columns is brittle

A user could create a hidden virtual column with a name prefixed with
`crdb_idx_expr`. They could then create an index on this column. If the index
was dropped, the manually created virtual column would be dropped too, because
it would be indistinguishable from a `crdb_idx_expr` hidden virtual column
created for an expression index.

This is acceptable because it is unlikely that a user will create a hidden
virtual column with the prefix `crdb_idx_expr`.

## Alternatives

The following alternatives aim to avoid the drawback of relying on the
`crdb_idx_expr` column name prefix. They either fail in meeting **A**, **B**,
and **C**, or significantly increase the complexity of the implementation.

### Minimalist

- Do not attempt to reuse existing hidden virtual columns when an expression
index is created.
- Do not drop virtual columns used by an expression index when dropping the
index.
- Show users an expression rather than the name of a virtual column if the
column is hidden.

This alternative satisfies **A** and would be the most simple to implement. It
also avoids the drawback of relying on a column name prefix. However, it
explicitly fails to meet criterion **B**. It also fails to meet criterion **C**
because executing the output of `SHOW CREATE TABLE` would create a table with an
additional column.

### Expression tracking in index descriptors

- Reuse an existing hidden virtual column with a matching expression
(non-matching name prefix is ok) when an expression index is created.
- Keep track of which columns were originally created for expression indexes in
index descriptors.
- Drop these columns when dropping the index if they are not referenced
elsewhere.
- Show users an expression rather than the name of a virtual column for these
columns.

This alternative satisfies criteria **A** and **B**. It also avoids the drawback
of relying on a column name prefix. However it is more complex to implement
because it requires changes to index descriptors.

It also fails to meet **C** in a subtle way. Executing the output of `SHOW
CREATE TABLE` would first create the hidden virtual column. Then the expression
index's descriptor would be created. It would rely on the hidden virtual column
but not track it as originally created for the expression index, because the
column already exists. The resulting table descriptor would not be identical to
the original. Dropping the expression index in the original would drop the
hidden virtual column. In the copied table, dropping the expression index would
*not* drop the hidden virtual column.

### Invisible column

- Create an "invisible" column classification.
- Create an invisible virtual column to represent expressions in expression
indexes.
- Drop any invisible virtual column used by an index when dropping the index.
- Do not allow any indexes, partial index predicates, constraints, or computed
column expressions to reference an invisible column.
- Never show users invisible column names (with the exception of `EXPLAIN`).
- Show users expressions of invisible virtual columns when displaying an index.

This alternative satisfies criteria **A**, **B**, and **C**. It also avoids the
drawback of relying on a column name prefix. Unfortunately, it is significantly
more complex to implement. The invisible column classification also disregards
the desire to make schemas more transparent by showing users hidden columns.
