- Feature Name: Partial Indexes
- Status: completed
- Start Date: 2020-05-07
- Authors: mgartner
- RFC PR: [#48557](https://github.com/cockroachdb/cockroach/pull/48557)
- Cockroach Issue: [#9683](https://github.com/cockroachdb/cockroach/issues/9683)

# Summary

This RFC proposes the addition of partial indexes to CockroachDB. A partial
index is an index with a boolean predicate expression that only indexes rows in
which the predicate evaluates to true.

Partial indexes are a common feature in RDBMSs. They can be beneficial in
multiple use-cases. Partial indexes can:

- Improve read performance, like normal indexes, without overhead for writes
  that don't match the predicate expression.
- Scan fewer rows than normal indexes on the same columns due to the partial
  index indexing a subset of rows.
- Reduce the total size of the set of indexes required to satisfy queries, by
  both reducing the number of rows indexed, and reducing the number of columns
  indexed.
- Ensure uniqueness on a subset of rows in a table via `CREATE UNIQUE INDEX
  ...`.

# Guide-level Explanation

## Usage

Partial indexes are created by including a _predicate expression_ via `WHERE
<predicate>` in a `CREATE INDEX` statement. For example:

```sql
CREATE INDEX popular_products ON products (price) WHERE units_sold > 1000
```

The `popular_products` index only indexes rows where the `units_sold` column has
a value greater than `1000`.

Partial indexes can only be used to satisfy a query that has a _filter
expression_, `WHERE <filter>`, that implies the predicate expression. For
example, consider the following queries:

```sql
SELECT max(price) FROM products

SELECT max(price) FROM products WHERE review_count > 100

SELECT max(price) FROM products WHERE units_sold > 500

SELECT max(price) FROM products WHERE units_sold > 1500
```

Only the last query can utilize the partial index `popular_products`. Its filter
expression, `units_sold > 1500`, _implies_ the predicate expression,
`units_sold > 1000`. Every value for `units_sold` that is greater than `1500` is
also greater than `1000`. Stated differently, the predicate expression
_contains_ the filter expression.

Note that CRDB, like Postgres, will perform a best-effort attempt to prove that
a query filter expression implies a partial index predicate. It is not
guaranteed to prove implication of arbitrarily complex expressions.

## Valid Predicate Expressions

There are some notable restrictions that are enforced on partial index
predicates.

1. They must result in a boolean.
2. They can only refer to columns in the table being indexed.
3. Functions used within predicates must be immutable. For example, `now()` is
   not allowed because its result depends on more than its arguments.

## Index Hints

Unlike full indexes, a partial index cannot be used to satisfy all queries.
Queries that don't imply the partial index predicate may need to return rows
that do not exist in the partial index.

Therefore, hinting a partial index, with `table@index`, will behave as follows:

* If the query filter expression can be proved to imply the partial index
  predicate, the partial index will be used in the query plan.
* If not, an error will be returned.

# Reference-level Explanation

This design covers 5 major aspects of implementing partial indexes: parsing,
testing predicate implication, generating partial index scans, statistics, and
mutation.

## Parsing

The parser will be updated to support new syntax for creating partial indexes.
Below are examples of statements that will be supported.

```sql
-- CREATE INDEX
CREATE INDEX ON a (b) WHERE c > 3
CREATE UNIQUE INDEX ON a (b) WHERE c > 3
CREATE INVERTED INDEX ON a (b) WHERE c > 3

-- CREATE TABLE ... INDEX (not supported by Postgres)
CREATE TABLE a (b INT, INDEX (b) WHERE b > 3)
CREATE TABLE a (b INT, UNIQUE INDEX (b) WHERE b > 3)
CREATE TABLE a (b INT, INVERTED INDEX (b) WHERE b > 3)

--- CREATE TABLE ... CONSTRAINT (not supported by Postgres)
CREATE TABLE a (b INT, CONSTRAINT c UNIQUE (b) WHERE b > 3)
```

In general, the partial index predicate will be the last optional term for
statements that create indexes. For example, below is an overview of the syntax
supported for `CREATE INDEX`.

```
CREATE [UNIQUE] [INVERTED] INDEX [name]
   ON tbl (cols...)
   [STORING ( ... )]
   [INTERLEAVE ...]
   [PARTITION BY ...]
   [WHERE ...]
```

Note that the `WHERE predicate` modifier will not be allowed in column
qualifiers. For example, the statement below will **NOT** be supported:

```sql
CREATE TABLE a (k INT PRIMARY KEY, b INT UNIQUE WHERE k = 0)
```

In order to ensure that predicates are valid (e.g., they result in booleans and
contain no impure functions), we will use the same logic that validates `CHECK`
constraints, `sqlbase.SanitizeVarFreeExpr`. The restrictions for `CHECK`
constraints and partial index predicates are the same.

## Testing Predicate Implication

In order to use a partial index to satisfy a query, the filter expression of the
query must _imply_ that the partial index predicate is true. If the predicate is
not provably true, the rows to be returned may not exist in the partial index,
and it cannot be used. Note that other indexes, partial or not, could still be
used to satisfy the query.

### Exact matches

First, we will check if any conjuncted-expression in the filter is an exact
match to the predicate.

For example, consider the filter expression `a > 10 AND b < 100` and the partial
index predicate `b < 100`. The second conjuncted expression in the filter, `b <
100`, is an exact match to the predicate `b < 100`. Therefore this filter
implies this predicate.

We can test for pointer equality to check if the conjuncted-expressions are an
exact match. The `interner` ensures that identical expressions have the same
memory address.

### Non-exact matches

There are cases when an expression implies a predicate, but is not an exact
match.

For example, `a > 10` implies `a > 0` because all values for `a` that satisfy
`a > 10` also satisfy `a > 0`.

Constraints and constraint sets can be leveraged to help perform implication
checks. However, they are not a full solution. Constraint sets cannot represent
a disjunction with different columns on each side.

Consider the following example:

```sql
CREATE TABLE products (id INT PRIMARY KEY, price INT, units_sold INT, review_count INT)
CREATE INDEX popular_prds ON t (price) WHERE units_sold > 1000 OR review_count > 100
```

No constraint can be created for the top-level predicate expression of
`popular_prds`.

Therefore, constraints alone cannot help us determine that `popular_prds` can be
scanned to satisfy any of the below queries:

```sql
SELECT COUNT(id) FROM products WHERE units_sold > 1500 AND price > 100

SELECT COUNT(id) FROM products WHERE review_count > 200 AND price < 100

SELECT COUNT(id) FROM products WHERE (units_sold > 1000 OR review_count > 200) AND price < 100
```

In order to accommodate for such expressions, we must walk the filter and
expression trees. At each predicate expression node, we will check if it is
implied by the filter expression node.

Postgres's [predtest library](https://github.com/postgres/postgres/blob/c9d29775195922136c09cc980bb1b7091bf3d859/src/backend/optimizer/util/predtest.c#L251-L287)
uses this method to determine if a partial index can be used to satisfy a query.
The logic Postgres uses for testing implication of conjunctions, disjunctions,
and "atoms" (anything that is not an `AND` or `OR`) is as follows:

    ("=>" means "implies")

    atom A => atom B if:          B contains A
    atom A => AND-expr B if:      A => each of B's children
    atom A => OR-expr B if:       A => any of B's children

    AND-expr A => atom B if:      any of A's children => B
    AND-expr A => AND-expr B if:  A => each of B's children
    AND-expr A => OR-expr B if:   A => any of B's children OR
                                    any of A's children => B

    OR-expr A => atom B if:       each of A's children => B
    OR-expr A => AND-expr B if:   A => each of B's children
    OR-expr A => OR-expr B if:    each of A's children => any of B's children

Because atoms will not contain any `AND` or `OR` expressions, we can generate a
`constraint.Span` for each of them in order to check for containment. There may
be edge-cases which cannot be handled by `constraint.Span`, such as `IS NULL`
expressions or multi-column values, like tuples.

At a high-level, to test whether or not `atom A => atom B`, we can perform the
following tests, in order:

1. If the atoms are equal (pointer equality), then `A => B`.
2. If the column referenced in `A` is not the column referenced in `B`, then `A`
   does not imply `B`.
3. If the `constraint.Span` of `A` is contained by the `constraint.Span` of `B`,
   then `A => B`.

There may be special considerations required for handling multi-column atoms,
such as `(a, b) > (1, 2)`. Multi-column spans should be helpful in proving
containment, though it should be noted that Postgres only supports simple
multiple column implications.

The time complexity of this check is `O(P * F)`, where `P` is the number of
nodes in the predicate expression and `F` is the number of nodes in the filter
expression.

## Generating Partial Index Scans

We will consider utilizing partial indexes for both unconstrained and
constrained scans. Therefore, we'll need to modify both the `GenerateIndexScans`
and `GenerateConstrainedScans` exploration rules (or make new, similar rules).

In addition, we'll need to update exploration rules for zig-zag joins and
inverted index scans.

We'll remove redundant filters from the expression when generating a scan over a
partial index. For example:

```sql
CREATE TABLE products (id INT PRIMARY KEY, price INT, units_sold INT, units_in_stock INT)
CREATE INDEX idx1 ON products (price) WHERE units_sold > 1000

SELECT * FROM products WHERE price > 20 AND units_sold > 1000 AND units_in_stock > 0
```

When generating the constrained scan over `idx1`, the `units_sold > 1000` filter
can be removed from the outer `Select`, such that only the `units_in_stock > 0`
filter remains.

Only conjuncted filter expressions that _exactly_ match the
predicate expression can be removed. For example, a filter expression
`units_sold > 1200` could not be removed. This filter would remain and be
applied after the scan in order to remove any rows returned by the scan with
`units_sold` between `1000` and `1200`.

## Statistics

The statistics builder must take into account the predicate expression, in
addition to the filter expression, when generating statistics for a partial
index scan. This is because the number of rows examined via a partial index scan
is dependent on the predicate expression.

For example, consider the following table, indexes, and query:

```sql
CREATE TABLE products (id INT PRIMARY KEY, price INT, units_sold INT, type TEXT)
CREATE INDEX idx1 ON products (price) WHERE units_sold > 1000
CREATE INDEX idx2 ON products (price) WHERE units_sold > 1000 AND type = 'toy'

SELECT COUNT(*) FROM products WHERE units_sold > 1000 AND type = 'toy' AND price > 20
```

A scan on `idx1` will scan `[/1001 - ]`. A scan on on `idx2` will have the same
scan, `[/1001 - ]`, but will examine fewer rows - only those where `type = 'toy'`.
Therefore, the optimizer cannot rely solely on the scan constraints to determine
the number of rows returned from scanning a partial index. It must also take
into account the selectivity of the predicate to correctly determine that
scanning `idx2` is a lower-cost plan than scanning `idx1`.


["The Case For Partial Indexes"](https://dsf.berkeley.edu/papers/ERL-M89-17.pdf)
details how to estimate the number of rows examined by a partial index scan,
based on the selectivity of the filter and predicate expressions. This can be
used as a starting point for making adjustments to the statistics builder.

The statistics builder must account for the special case when predicate
expressions include columns that are indexed. For example, consider the
following table, index, and query.

```sql
CREATE TABLE products (id INT PRIMARY KEY, units_sold INT)
CREATE INDEX idx ON products (units_sold) WHERE units_sold > 100

SELECT COUNT(*) FROM products WHERE units_sold > 200
```

In this case, the constraint spans of the predicate and query filter are
`[/101 - ]` and `[/201 - ]`, respectively. Because the constraints apply to the
same column, the selectivity of each is _not_ independent. The number of rows
examined is approximately the total number of rows in the table, multiplied by
the selectivity of `[/201 - ]`. It would be inaccurate to instead estimate the
number of rows examined as the total number of rows in the table, multiplied by
the selectivity of `[/201 - ]` and the selectivity of `[/101 - ]`.

## Mutation

Partial indexes only index rows that satisfy the partial index's predicate
expression. In order to maintain this property, `INSERT`s, `UPDATE`s, and
`DELETE`s to a table must update the partial index in the event that they change
the candidacy of a row. Partial indexes must also be updated if an `UPDATE`d row
matches the predicate both before and after the update, and the value of the
indexed columns change.

In order for the execution engine to determine when a partial index needs to be
updated, the optimizer will project boolean columns that represent whether or not
partial indexes will be updated. This will operate similarly to `CHECK`
constraint verification.

### Insert

If the row being inserted satisfies the predicate, write to the partial index.

### Delete

If the row being deleted satisfies the predicate, delete it from the partial
index.

### Updates

Updates will require two columns to be projected for each partial index. The
first is true if the old version of the row is in the index and needs to be
deleted. The second is true if the new version of the row needs to be written to
the index.

Consider the following table of possibilities, where:

 * `r` is the version of the row before the update
 * `r'` is the version of the row after the update
 * `pred_match(r)` is `true` when `r` matches the partial index predicate

|                Case                  | Delete `r` from index | Insert `r'` to index |
| ------------------------------------ | --------------------- | -------------------- |
| `pred_match(r) AND !pred_match(r')`  | `True`                | `False`              |
| `!pred_match(r) AND pred_match(r')`  | `False`               | `True`               |
| `pred_match(r) AND pred_match(r')`*  | `True`                | `True`               |
| `!pred_match(r) AND !pred_match(r')` | `False`               | `False`              |


*Note that in the case that the row was already in the partial index and will
remain in the partial index after the update, the index only needs to be updated
(delete `r` and insert `r'`) if the value of the indexed columns changes. If the
value of the indexed columns is not changing, there is no need to update the
index.

### Primary Key Changes

If a primary key change occurs, the partial index will need to be rewritten so
that the values in the index store the new primary key. This is similar to other
secondary indexes, only that the added complexity of checking if rows belong in
the partial index must be considered.

# Alternatives considered

## Disallow `OR` operators in partial index predicates

**This alternative is not being considered because it would make CRDB partial
indexes incompatible with Postgres's partial indexes.**

Testing for predicate implication could be simplified by disallowing `OR`
operators in partial index predicates. A predicate expression without `OR` can
always be represented by a constraint. Therefore, to test if a filter implies
the predicate, we simply check if any of the filter's constraints contain the
predicate constraint. Walking the expression trees would not be required.

[SQL Server imposes this limitation for its form of partial
indexes](https://docs.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql?view=sql-server-ver15).
Such an expression could always be represented by a constraint. Therefore, to
test if a filter implies the predicate, we simply check if any of the filter's
constraints contain the predicate constraint.

Note that the `IN` operator would still be allowed, which provides a form of
disjunction. The `IN` operator can easily be supported because it represents a
disjunction on only one column, which a constraint _can_ represent.

# Work Items

Below is a list of the steps (PRs) to implement partial indexes, roughly
ordered.

- [X] Add partial index predicate to internal index data structures, add parser
  support for `WHERE <predicate>`, add a cluster flag for gating this
  defaulted to "off"
- [X] Add simple equality implication check to optimizer when generating index
  scans, in GenerateIndexScans.
- [X] Same, for GenerateConstrainedScans.
- [X] Add support for updating partial indexes on inserts.
- [X] Add support for updating partial indexes on deletes.
- [X] Add support for updating partial indexes on updates and upserts.
- [X] Add support for backfilling partial indexes.
- [X] Update the statistics builder to account for the selectivity of the partial index
  predicate.
- [X] Add more advanced implication logic for filter and predicate expressions.
- [ ] Add support in other index exploration rules:
  - [ ] GenerateInvertedIndexScans
  - [ ] GenerateZigZagJoin
  - [ ] GenerateInvertedIndexZigZagJoin
- [X] Add support for partitioned partial indexes
- [ ] Add support for using partial indexes in Lookup Joins
- [ ] Consider using partial indexes for auto-generated indexes used for foreign
  keys.
- [X] Add support for `ON CONFLICT WHERE [index_predicate] DO ...` for identifying
  conflict behavior for unique partial indexes.
  - More info in the [Postgres
    docs](https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT)
    and [this blog
    post](https://medium.com/@betakuang/why-postgresqls-on-conflict-cannot-find-my-partial-unique-index-552327b85e1)

# Resources

- [Postgres partial indexes documentation](https://www.postgresql.org/docs/current/indexes-partial.html)
- [Postgres CREATE INDEX documentation](https://www.postgresql.org/docs/12/sql-createindex.html)
- [Postgres predicate test source code](https://github.com/postgres/postgres/blob/master/src/backend/optimizer/util/predtest.c)
- ["The Case For Partial Indexes", Michael Stonebraker](https://dsf.berkeley.edu/papers/ERL-M89-17.pdf)
- [Use the Index Luke - Partial Indexes](https://use-the-index-luke.com/sql/where-clause/partial-and-filtered-indexes)
