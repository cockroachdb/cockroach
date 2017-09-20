- Feature Name: SQL consistency checks
- Status: draft
- Start Date: 2017-09-20
- Authors: Joey Pereira
- RFC PR:
- Cockroach Issue: [#10425](https://github.com/cockroachdb/cockroach/issues/10425)

# Summary

This RFC outlines the steps to create SQL consistency checks that will ensure
the consistency of table data, including foreign keys and indexes. These checks
will be query statements, runnable through scheduled jobs. This is needed to
catch errors earlier on and guarantee they are caught in the first place. The
aspirational outcome is where these checks will be run after each transaction,
furthering our ability to correlate the error to the action that caused it.

# Motivation

To this point, we don't have ways to reliably check the consistency of table
data, and it's only detected on failures that often occur on access. There have
been various consistency errors that have popped up, appearing as missing index
entries ([#18533][]) and unchecked foreign keys ([#17626][], [#17690][]).
There's more data related to table consistency that can go awry, as mentioned in
[#10425][]. Currently, aspects that can be checked for consistency include:
- Associated index keys
- Dangling index references (i.e. missing row but index entry retained)
- `CHECK` constraints
- `NOT NULL` constraints
- `UNIQUE` constraints
- `FOREIGN KEY` constraints
- Primary index data is organized into column families as per the
  TableDescriptor (e.g. there is no extraneous column data in a family and there
  are no extraneous families, see [column families][column families] for how
  they work).

By having consistency checks run periodically we can understand if and when
errors are occurring. As a user of cockroach, I will know that the underlying
data is correct.

Furthermore, an aspirational goal of the consistency checking is to be able to
conditionally run the checks after transactions to be able to narrow down an
inconsistency to a specific set of operations. This would be a huge asset for
tracing these issues as they come up. Without this level of detail, we're left
in the dark with only knowing the error occurs.

[#18533]: (https://github.com/cockroachdb/cockroach/issues/18533)
[#17626]: (https://github.com/cockroachdb/cockroach/issues/17626)
[#17690]: (https://github.com/cockroachdb/cockroach/issues/17690)
[#10425]: (https://github.com/cockroachdb/cockroach/issues/10425)
[column families]: (https://www.cockroachlabs.com/docs/stable/column-families.html)

# Guide-level explanation

These consistency checks help evaluate the validity of various constraints that
take place at both the SQL level and also internally, for things such as
indexes. If a check happens to fail we ... (TODO: what is a user actionable
alert we can do?). These checks will present as jobs which will be viewable in
the Admin UI. The same functionality will also be made to operate on an
individual table and put behind a SQL command such as `CHECK TABLE <table>`.

(COMMENT: Should this be renamed to SQL constraint checks? Indexes fall under
consistency and not quite constraint, otherwise it's all constraints)

These checks will be of two types:
- Done through a bulk operation, e.g. a simple SQL query
- Done as an incremental process, e.g. only checking a set of recently updated
  rows

Bulk operations are perfectly suitable for a check that is done periodically,
such as once every 24 hours in order to ensure an operating database is still
valid. For example, when checking for index size:
```sql
SELECT count(*) FROM table@index
# Assert this equals
SELECT count(*) FROM table
```

On the other hand, an incremental check is more desirable for frequent checks,
such as after every transaction. These will be where we might end up checking
only the set of rows that were modified in a transaction, in an attempt to still
do a consistency check frequently but without the cost. What this may look like
for foreign keys would be
```sql
# ON ROW ADD/UPDATE
SELECT count(*)
FROM table t
JOIN fk_table ft
  ON t.fk_key = ft.key
WHERE t.id = ANY(<RELEVANT ROWS>)
# Assert this equals
SELECT count(*) FROM <RELEVANT ROWS>
```

That's not to say that even if incremental checks are present, bulk checks may
be wanted as a secondary check and seems more practical as a user of
CockroachDB. Incremental checks can serve better utility as checks run
conditionally during debug builds and long-term testing.

# Detailed design

We will discuss how each correctness check will be implemented, how all of the
checks are run and then where their results are fed.

Generally, the idea behind the incremental checks will be to do some minimal
check on only changed data without having to do a full database check. This is
largely only possible via change data capture. Unless an incremental version is
specified, the incremental check is equivalent with the exception of filtering
rows. A comment on performance would be that using change data capture, we could
run the check on the modified rows that we recorded only on a smaller set
interval of time, instead of after every transaction. That way we don't do a
check for each transaction.

TODO: Unless there is a way to know which rows were added, deleted, or updated
we cannot do incremental checks without change data capture. It may be possible
to do this by modifying transaction sessions to retain state for later doing a
check, but is likely not worth the effort.

## Executing correctness checks
Knowing which checks to run is only requires pulling table data out, which is
possible through SQL:
```sql
SHOW DATABASES
SHOW TABLES
SHOW CONSTRAINTS FROM <table>
SHOW INDEXES FROM <table>
```

These will be executed on a configurable interval to run as jobs which will
gather the data on what needs to be checked and then run the checks. TODO: Minor
implementation detail, would these checks be run as one job or several?

## Failing constraints

TODO: What does it look like to fail a constraint? What do we do with the
failure? Do we show it in the dashboard? Report it in sentry? Email the user and
us?
```go
type ConstraintCheckFailure struct {
  // The type of constraint. For example, CHECK, PRIMARY KEY, INDEX.
  Constraint ConstraintType
  // Table of the failed constraint or index.
  Table string
  // Name of the constraint or index.
  ConstraintName string
  // Columns involved in the constraint or index.
  Columns []string
  // RowID which failed.
  RowID int64
  // TODO: How do we express the difference between a missing index and
  // dangling index reference?
  // These seem like they will be different ConstraintTypes.
}
```

## Index correctness

There are three possibilities for what can happen to indexes.
- An index is not made
- An index is not dropped when a row is dropped, and we get a dangling reference
- An index is created more than one.
- TODO: Can indexes get updated when a row is updated? How would this impact the
  index?

Naively, we can test index correctness via. testing `SELECT COUNT(*) FROM
table@index` and comparing that against the table size. Unfortunately, this
isn't completely correct. Under the scenario when one index is missing and there
is either a duplicate index entry or a dangling index entry, this will be a
false positive result.

In order to successfully complete this check then, a pointwise comparison will
need to be done on the indexes. As per suggestion from Jordan, we may require a
new DistSQL processor to accomplish an efficient pointwise comparison, but what
this would look like would simply be

```sql
SELECT count(*)
FROM table t
JOIN table@index i
  ON t.id = i.id
```

I believe we can find any violating entries with
```sql
SELECT t.id as t_id, i.id as i_id
FROM table t
FULL JOIN table@index i
  ON t.id = i.id
WHERE
  t.id IS NULL OR i.id IS NULL
```

## Check constraints

TODO: Can we just retrieve the constraint expression via. `details` field of
`SHOW CONSTRAINTS FOR [TABLE]`?

Checking this constraint can be done using `SELECT id FROM table WHERE
!(<CHECK>)`.

## Not null constraints

Checking this constraint can be done using `SELECT id FROM table WHERE col IS
NULL `.

## Unique constraints

TODO: Would an existing uniqueness index influence the result of `DISTINCT`?
Checking this constraint can be done using `SELECT COUNT(DISTINCT(<col>)) FROM
table`.

We can find any rows that violate this using
```sql
SELECT <col>, ARRAY_AGG(id) as rows
FROM table
GROUP BY <col>
WHERE ARRAY_LENGTH(rows, 1)
```

## Foreign key constraints

Checking this constraint can be done using 
```sql
SELECT t.id
FROM table t
LEFT JOIN fk_table ft
  ON t.fk_key = ft.key
WHERE ft.key = NULL
```

## Rationale and Alternatives

As table metadata needed is available in SQL, all of these constraints are
expressed easily as only SQL statements. The only concerns with expressing them
as SQL will be if the expressions get optimized into something which doesn't
check what we want. Additionally, if the checks may not be as optimized as
desired if they are implemented at just the SQL level (but this is good
motivation to improve SQL performance if that's a problem!)

As an alternative, we could build these checks directly into Cockroach at a
lower level than SQL expressions. Since many of these constraints involve SQL
concepts and metadata, I don't think it is even viable to even create some of
these below SQL without having code touching into the SQL layer and doing so
would be more effort.

Additionally, an alternative for incremental checks would be to build a system
to run the checks shortly after 

An alternative for building the incremental checks would be to possibly run a
small the check after each transaction. In order to do so though, we first need
to know the rows that have been modified. As a result, without change data
capture we will not have incremental checks we will only be able to detect the
presence of a failure but not the source of it.

TODO: As of now, is that possible? My guess is no, which places this as likely
too high effort to immediately worth it, and is dependant on if we suddenly
discover several consistency failures.

## Unresolved questions

- As I'm not familiar with how jobs work in CockroachDB, I don't know if they
  are the most appropriate approach to making these checks.
- Checking if column families are organized correctly is a property of the
  underlying KV encoding, and as such is not accessible through plain SQL. It
  also appears to be less vital for correctness checking. For these reasons, it
  has been omitted.
- Can index backfilling be problematic for the index checks? Is it possible to
  detect if that is happening and to defer checking index consistency?
- What can we provide as actionable to a user who has encountered a check
  failure? Can send an alert via Sentry or other means?
