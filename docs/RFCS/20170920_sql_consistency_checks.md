- Feature Name: Table consistency checks
- Status: draft
- Start Date: 2017-09-20
- Authors: Joey Pereira
- RFC PR: [#18675](https://github.com/cockroachdb/cockroach/issues/18675)
- Cockroach Issue: [#10425](https://github.com/cockroachdb/cockroach/issues/10425)

# Summary

A set of routines and a SQL statement to invoke them, that will check
the consistency of the table data, including schemas, foreign keys, and
indexes.

These checks will be accessible through SQL query queries. If
a user wants to run them on a regular schedule, they can use an external
process to do so. The checks may possibly be used in logictests to
further check correctness.
 
# Motivation

Until this point, in CockroachDB, there aren't ways to reliably check
the consistency of table data, and it's only detected on failures that
often occur on access. There have been various consistency errors that
have popped up, appearing as mismatched index entries ([#18705][],
[#18533][]) and unchecked foreign keys ([#17626][], [#17690][]). A
significant factor causing a rise in these types of errors is new
features that manipulates tables outside of transactions (schema
changes, backup, CSV import).

In summary, aspects that can be checked include:
- Secondary indexes have entries for all rows in the table
- Dangling index references (i.e. missing row but index entry retained)
- SQL constraints (`CHECK`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY`)
- Indexes using [STORING][]. Wrong copies of data in the index (either
  in the index key or the data payload)
- Tables using [FAMILY][]. Primary index data is organized into column
  families as per the TableDescriptor (e.g. there is no extraneous
  column data in a family and there are no extraneous families)
- Invalid data encodings. Data can't be read from the bytes, or doesn't
  match expected types.
- Non-canonical encodings (where we can decode the data, but if we
  re-encode it we get a different value.)
- [Composite encodings][] are valid. This includes `DECIMAL` and
  collated strings types
- Key encoding correctly reflects the ordering of the decoded values

Schemas also can be checked for the following:
- Invalid parent ID for interleaved tables
- Invalid parent ID in general (db doesn't exist anymore)
- Invalid index definitions (wrong columns)
- Invalid view descriptors: invalid column types; invalid SQL; invalid
  number of columns.
- Invalid column families

[#17626]: https://github.com/cockroachdb/cockroach/issues/17626
[#17690]: https://github.com/cockroachdb/cockroach/issues/17690
[#18533]: https://github.com/cockroachdb/cockroach/issues/18533
[#18705]: https://github.com/cockroachdb/cockroach/issues/18705
[STORING]: https://www.cockroachlabs.com/docs/stable/create-index.html#store-columns
[FAMILY]: https://www.cockroachlabs.com/docs/stable/column-families.html
[Composite encodings]: https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md#composite-encoding

# How to use the checks

The checks are separated into a few commands in order to split the
purpose of them, alongside ones that may require more work than others
(and will take longer, such as indexes).
- Checking a secondary index. This checks that the secondary index
  contains the correct data, and exists no index entries are missing.
- Checking an SQL constraint. This checks the validity of a table
  constraint.
- Checking the table data. This checks that there are no errors in the
  stored table data, such as encoding errors. i.e. all checks mentioned
  in "motiviation" except the above two.
- Checking the database data. An alias for checking all of the tables.

In addition, there will be an option to run all of the checks.

They can be manually invoked via the CLI, with the following statements.
```sql
# Checks a secondary index, making sure that all the data stored in it is correct.
CHECK TABLE <table> INDEX <index_name>;

# Checks a specific table constraint against all rows.
CHECK TABLE <table> CONSTRAINT <constraint_name>;

# Checks the underlying table data is correct.
CHECK TABLE <table> VALIDITY;

# Checks all of the tables in the database are correct.
CHECK DATABASE <database> VALIDITY;

# Does a complete check, including the underlying data, SQL constraints, and indexes.
CHECK TABLE <table>;
CHECK DATABASE <database>;
```

Execution of the check statements will block until the check finishes,
and rows will be returned for each detected failure. The rows for the
check failures will also persist to a system table with a date of
detection.

For example, if a user wants to check the consistency of encoding errors,
```sql
CHECK TABLE mytable VALIDTY;
---
CheckFailure | Database     | Table     | ConstraintName | Columns     | KeyPrefix        | PKeyID
'ENCODING'   | 'mydatabase' | 'mytable' | NULL           | {'acolumn'} | '/1/mytable/...' | 1

```

# Detailed design

## Terminology

The **physical table** is the underlying KV pairs behind tables. This is
where we find primary and secondary indexes (as distinct keys),
groupings of certain data (column families), how index data is organized
(index storing conditions), and the encoding of the data.

On the other hand, the **logical table** is the SQL representation of
the data. This is where we think about data as rows and columns. It's
also where we have table schemas and any corresponding constraints
(`UNIQUE`, `CHECK`, `NOT NULL`, `FOREIGN KEY`).

There are two main types of implementations of the checks that will be
discussed in this RFC. The first is using higher level processes to
execute the check on the logical table[0] -- hence we will call them
**Logical checks**. These may involve regular SQL plans, or other higher
level functions that operate on rows. For checks involving the physical
table, these need to be done through operating on the KV pairs directly
-- so we will call these **KV checks**. These checks will be done
through a custom distSQL processor that will scan through KV spans and
check them.

## Implementations of checks

As mentioned earlier, all of the checks are invokable by any SQL client, including the CLI shell.

Checks on the table schema can be implemented in an already existing
`TableDescriptor.Validate`. The new capabilities are:
- Check for invalid parent ID for interleaved tables
- Checks for an invalid parent ID in general (db doesn't exist anymore)
- Check for invalid index definitions (wrong columns)
- Check for invalid view descriptors: invalid column types; invalid SQL;
  invalid number of columns.
- Check for invalid column families

SQL constraints are checks on the logical table (`CHECK`, `NOT NULL`,
`UNIQUE`, `FOREIGN KEY`).

The rest of the checks are on the physical table.
- Check for indexes using [STORING][] have the correct data
- Tables using [FAMILY][] have the data organized into families
  correctly
- Invalid data encodings
- Non-canonical encodings (i.e. data round-trips)
- [Composite encodings][] are valid
- Key encoding correctly reflects the ordering of the decoded values
- Secondary indexes have entries for all rows in the table
- Dangling index references (i.e. missing row but index entry retained)

All physical table checks will be implemented with a KV check. All
logical table checks can be done with other processes, with the
exception of the `NOT NULL` constraint, which can only be checked with a
KV check.

### KV checks (excluding indexes)

KV checks will be implemented using distSQL. In order to do so we will
create a new distSQL processor, `TableChecker`. `TableChecker` is
similar to `TableReader`, but it differs by doing additional checks
while scanning through KV pairs and returning a list of check failures
intead of data rows. These check failure rows will then be collected and
presented upon completion, as a new distSQL schema.

During execution of the `CHECK` plan, we will construct and execute a
distSQL physical plan, similar to how the `SchemaChanger` does vi a.
`CreateBackfiller`. This plan will be made to consist of `TableChecker`
processors processing all the key spans relevant to the check.

The underlying processing that happens behind both `TableReader` and
`TableChecker` is the `RowFetcher`. Inside the current `RowFetcher` the
method `processKv` is responsible for encoding specific work, which is
precisely where the checks will be added.

Inside `processKV`, we can do the following checks in the key iterator:
- Check composite encodings are correct and round-trip
- Check key encodings correctly reflect ordering of decoded values
- Check the KV pair is well-formed (everything is present, no extra
  data, checksum matches)
- For primary keys, verify that the column family 0 (sentinel) is
  present

While iterating through keys some may be interleaved KV pairs from other
tables. These can be checked that they do refer to a table that should
be present.

Execution of KV checks can be throttled by providing `KVFetcher` with a
batch size and then sleeping the between processing batches.

### Index checking

Secondary indexes need to be checked that no entries are missing for the
row data. Index references may also point to an invalid row, i.e. they
are dangling.

Secondary indexes present a complexity that we don't have with other KV
checks: we need to reference other KV pairs which may not exist on the
key span we're searching.

The approach for doing this check in the efficiently is not yet
determined. See unresolved questions.

### SQL constraints (`CHECK`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY`)

It should be noted that `CHECK` and `FOREIGN KEY` constraints can
already be checked with  `ALTER TABLE ... VALIDATE CONSTRAINT`, but
there may be issues with attempting to do a full check, as
indicated by [#17690][].

Given that those two constraint checks are already implemented, and they
involve more complex work, they will not be implemented as KV checks
unless necessary as judged by performance. `CHECK` involves an SQL
expression which is non-trivial to execute against KV pairs and
`FOREIGN KEY` faces the same problem as secondary index checks -- having
to do lots of RPCs or load the all the column data into memory.

Checking `NOT NULL` is relatively simple to implement as a KV check
through `processKv` mentioned above and is requires since a node panics
on encountering a `NULL` value in a non-nullable column (see
`RowFetcher.finalizeRow`).

Because `UNIQUE` involves the entire column data, there is no simple way
to implement it. NB: I haven't put too much thought into this check, as
it does not seem to be a priority. It may be possible to run this check
through a distSQL plan that semantically represents:
```sql
# Get distinct value count
SELECT DISTINCT(col) FROM table
# Get total value count
SELECT COUNT(col) FROM table WHERE NOT NULL
# Assert they are equal
```

Alternatively, to retrieve any violations we could use a plan that
semantically represents:
```sql
SELECT col, array_agg(row_id)
FROM table
GROUP BY col
WHERE array_length(row_id) > 2
```

[#17690]: https://github.com/cockroachdb/cockroach/issues/17690

## What happens when a check fails

When a `CHECK` statement is run, once it finishes running a return a row
for each failure that has been detected. Rows will have the same data as
found in the struct below.

A prospective model is be the following
```go
type ConstraintCheckFailure struct {
  // The type of check failure. For example, CHECK, PRIMARY KEY,
  // MISSING_SECONDARY_INDEX, ENCODING, DANGLING_SECONDARY_INDEX, ...
  CheckFailure CheckFailureType
  // Database of the failed check.
  Database string
  // Table of the failed constraint.
  Table string
  // Name of the constraint involved, if it's an SQL constraint check.
  ConstraintName string
  // Columns involved in the check failure.
  Columns []string
  // KeyPrefix is the internal prefix for the KV pair that failed.
  KeyPrefix string
  // PKeyID is the primary key ID in string representation, if at all
  // retrievable.
  PKeyID string
}
```

# Alternatives

## SQL statements to execute the check

Here are a list of how other databases provide a interfaces for checking
the consistency of data.

### Microsoft

Microsoft SQL Server has a command `DBCC`. Documentation can be found
[here][MS DBCC]. They have dozens sub-commands which they put into 4
categories.

| Command category   | What it performs                                                   |
|---------------|---------------------------------------------------------------------------------------------------------|
| Maintenance   | Maintenance tasks on a database, index, or filegroup.                                                   |
| Miscellaneous | Miscellaneous tasks such as enabling trace flags or removing a DLL from memory.                         |
| Informational | Tasks that gather and display various types of information.                                             |
| Validation    | Validation operations on a database, table, index, catalog, filegroup, or allocation of database pages. |

One of the closest commands to ours, `DBCC CHECKTABLE` is under
"Validation". It has a few options you can provide such as
`PHYSICAL_ONLY`, which checks "physical structure of the page, record
headers and the physical structure of B-trees.", `DATA_PURITY` which
"check the table for column values that are not valid or out-of-range.
For example ... detects columns with date and time values that are
larger than or less than the acceptable range."

They also have a `DBCC CHECKDB` which does the same and takes the same
options on a database level.

The last relevant one for us is `DBCC CHECKCONSTRAINTS` which can take a
constraint name or `ALL_CONSTRANTS`. This is more similar to our `ALTER
TABLE ... VALIDATE` then the check command, as the purpose is to check a
disabled constraint.

[MS DBCC]: https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-transact-sql

### Oracle
Oracle has a `VALIDATE` command that is used for several different
things, such as validating the database or a backup. Documentation can
be found [here][Oracle]. This command specifically runs to check
"physical corruption only". In summary, the command also has an option
to check for logical corruption. In both cases, failures are logged and
can be viewed with a `LIST FAILURE` command.

> Use the VALIDATE command to check for corrupt blocks and missing files, or to determine whether a backup set can be restored.

> If VALIDATE detects a problem during validation, then RMAN displays it and triggers execution of a failure assessment. If a failure is detected, then RMAN logs it into the Automated Diagnostic Repository. You can use LIST FAILURE to view the failures.



In particular there is an option which instead makes the database to
check logical corruption, `CHECK LOGICAL`:
>  Tests data and index blocks in the files that pass physical corruption checks for logical corruption, for example, corruption of a row piece or index entry. If RMAN finds logical corruption, then it logs the block in the alert log and server session trace file.

[Oracle]: http://docs.oracle.com/cd/B28359_01/backup.111/b28273/rcmsynta053.htm#RCMRF162

### MySQL

MySQL has an operation `CHECK TABLE` that is very similar to what ours
is functionally. Documentation can be found [here][MySQL Check]. They
also have `ANALYZE`, `REPAIR`, and `OPTIMIZE` queries. (`REPAIR` seems
to not do much except for with specific setups.)
```
CHECK TABLE tbl_name [, tbl_name] ... [option] ...
option = {
    FOR UPGRADE
  | QUICK
  | FAST
  | MEDIUM
  | EXTENDED
  | CHANGED
}
```
Where any of the options can be used together. Yes, this is actually
very confusing because they each have their own distinct meaning, so
you can `CHECK TABLE ... QUICK FAST`.

| Option   | Meaing                                                                                                                                                        |
|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| QUICK    | Do not scan the rows to check for incorrect links.                                                                                                            |
| FAST     | Check only tables that have not been closed properly.                                                                                                         |
| CHANGED  | Check only tables that have been changed since the last check or that have                                                                                    |
| MEDIUM   | Scan rows to verify that deleted links are valid. This also calculates a key checksum for the rows and verifies this with a calculated checksum for the keys. |
| EXTENDED | Do a full key lookup for all keys for each row. This ensures that the table is 100% consistent, but takes a long time.

[MySQL Check]: https://dev.mysql.com/doc/refman/5.7/en/check-table.html

## Queue-based checks

Using queue you have access to the KV interface but are bound to only a
single replica and this was brought up as a problem for some checks (is
it?). Using a queue you would find the same KV pair processing as it is
being implemented in `processKv`.

## Change data capture and per-transaction checks

Using a stream of data it is possible to make incremental checks that
check only the set of data that has changed. This isn't as thorough as
other checks but this opens the possibility for checking more
frequently.

# Future work

These are ideas that are not the primary focus for the RFC and are
explicitly deemed out of scope, but are future additions that have been
discussed or would be good improvements for the checks.

## Checking system tables

Outside of checking the user data is consistent, largely testing the
correctness of the SQL layer, system tables can also be checked. A few
system table checks include:
- Invalid table ID / missing namespace entry
- Duplicate table IDs in namespace table

## Scheduled checks

If checks are run automatically in any manner brings up problems for how
we prevent these checks from impacting foreground traffic. The checks
can be run manually at the discretion of a DBA as desired, while fully
understanding the impact.

If run automatically the checks could be run as a job so that it will
appear in the admin UI and can also be canceled/paused/resumed from
there.

To my current knowledge, a simple goroutine run with a timer to initiate
the job will suffice. Alternatively, it was suggested to create a `CRON`
for CockroachDB in order to manage scheduled jobs, which is deserving of
it's own RFC.

## Error reporting

Reporting goes in tandem with scheduled checks as it only becomes
relevant when we don't have a foreground process to receive errors at.
If we add a scheduled job to do checks or do them through other
background processes we need a way to alert the user about failures.

There are several different ways this could be done.
- Store the failure into a table for display in admin. This can possibly
  include suggestions of how to repair the failure.
- Send a sentry report of the failure.
- Do other user-notifying actions, e.g. email them.

## Attempting to repair the data

Repairing the failing data is left as future work, because of the large
scope of the problem. This section is mentioned as it was discussed in
order to give guidance to the problem.

There are 3 categories for the repair action we can take.
- Automatically repair the error
- Prompt a manual process to repair the error
- Do nothing -- not repairable, but we can alert the user

In the case of both manual repair and where no repair is possible, we
may need to do more than just let the user know their data is bad. Will
this failed data cause further issues? Can we quarantine it at all?

Overall, due to the complexity of what could have happened to the data
there may be no automatic repair actions that can be taken. In some
cases we may want to quarantine row or KV data to prevent further
errors.

### Missing secondary indexes

Two cases could have happened:
- We failed to create the index entry (on row creation, or update)
- We failed to remove the primary key during deletion

For the first case of what happened, what we would want to do is to
create the index entry. Because of the second case though we can't just
do automatic repair here as the row could be expected to not exist.

### Dangling index reference

In this case, as there is no primary data found, the possibilities for
what could have happened here:
- The secondary index entry was not deleted with the row.
- The primary key changed and the reference did not.
- The secondary index key changed but the entry wasn't updated.
- The primary data may have been erreonously deleted and secondary
  indexes retained.


For the cases 2 and 3, the secondary index existence check will check
a problem if a new entry was not made. Therefore it's the best action is
to also delete the entry.

The last case is a little more complicated. If all the secondary indexes
have an entry for the ghost row but it's not in the table, it may have
been erroneously deleted from the table. Because of this, we may not
want to delete the dangling entry but instead attempt to preserve all of
them in a quarantine for 1) further investigation by an operator -
perhaps there's enough data to reconstruct the row and 2) helping us
investigate what went wrong.

### SQL constraints (`CHECK`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY`)

These constraints correspond to user-level data and because of this,
there is no best action to take. Instead what we can do is present
several different actions the user can take, including row deletion or
replacing a value.

### Encoding or other KV problems

If there are any problems on the KV layer, there is not much we can do
to repair the data. If the broken data is the value, and the column
happens to be a secondary or primary index, we can reconstruct the value
from the data in the index key. The same applies in the opposite
direction.

If we can't do either, the best thing we can do is report as much of the
data as possible and attempt to quarantine it.

# Unresolved questions

- What SQL queries will be available? What kind of options do we want to
  provide? Throttling? Conditionally run different checks?

- Can work from RowFetcher be re-used? It's large, and has lots of
  intricate details about KV encoding, which we need to handle.
  Otherwise we're adding a duplicate of every KV encoding detail that
  needs to be changed in tandem.

- Can the results from this RFC benefit `FSCK` for backups?

- Can index backfilling be problematic for the index checks? Is it
  possible to detect if that is happening then defer checking index
  consistency? We shouldn't have read capabilities yet if it's being
  backfilled, so we should able to respect the capability.

- What can we provide as actionable to a user who has encountered a
  check failure? Can send an alert via Sentry or other means? Should we
  store them in a system table to be accessed later?

- Do these check queries run in the foreground, or in the background as
  a job? Can they be paused/resumed/cancelled?

- While it's possible to throttle how fast we scan during KV checks
  through distSQL, it may not be possible to throttle other checks. Is
  this a concern?

- While it may be possible to check SQL constraints with KV checks, it's
  far easier to implement them outside of KV checks due to their nature
  of involving SQL concepts. It's TBD during implementation whether it
  will be worth it to make these KV checks. This would mean to do these we need to do extra passes over data. (Exception is `NOT NULL`, only checkable in a KV check.)

- How will we implement a check of the `UNIQUE` SQL constraint? How much
  of a priority is this? We can sort of do this through a set of SQL queries.

- How will we do index checks efficiently? One strategy is to iterate
  through all of the table data and re-generate the index data. Then,
  after doing the re-generation we scan through the secondary indexex
  and compare it against our the "expected" re-generated index.

  In practice what this may involve is re-generating parts of the index
  in all distSQL nodes as we scan through primary data. We will then
  aggregating it, then split the re-generated parts to go to nodes which
  scan the respective secondary index. We may end up doing two passes on
  all secondary data, but we can also optimize this to retain all checks
  on the secondary data KV pairs to this part of the distSQL pipeline.

  An alternative solution is while scanning through primary row data,
  batch RPCs to fetch the corresponding secondary indexes and compare
  it. We will also need to scan the secondary indexes if we want to find
  any dangling references.

  While the first idea will not be performant due to large amounts of RPC,
  it wil be far easier to implement and will be a first step to creating
  it. It's TBD whether the second idea will be needed, but it also faces
  problems with the complexity of making a temporary index whether on disk
  or in-memory.
