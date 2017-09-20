- Feature Name: SQL consistency check interface
- Status: draft
- Start Date: 2017-09-20
- Authors: Joey Pereira
- RFC PR: [#18675](https://github.com/cockroachdb/cockroach/issues/18675)
- Cockroach Issue: [#10425](https://github.com/cockroachdb/cockroach/issues/10425)

# Summary

This RFC outlines the interface for a set of SQL statement that will
check the validity of the table data, including schemas, foreign
keys, indexes, and encoding.

These checks will be accessible through SQL query queries. If
a user wants to run them on a regular schedule, they can use an external
process to do so. The checks may possibly be used in logictests to
further check correctness.

# Motivation

Until this point, in CockroachDB, there aren't ways to reliably check
the consistency of table data. Often, errors are only detected on
problems that occur on access. There have been various consistency
errors that have popped up, appearing as mismatched index entries
([#18705][], [#18533][]) and unchecked foreign keys ([#17626][],
[#17690][]). A significant factor causing a rise in these types of
errors are new features that manipulate tables outside of transactions
(schema changes, backup, CSV import).

In summary, aspects that can be checked include:
- Secondary indexes have entries for all rows in the table
- Dangling index references (i.e. missing row but index entry retained)
- SQL constraints (`CHECK`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY`)
- Indexes using [STORING][]. Wrong copies of data in the index (either
  in the index key or the data payload)
- Tables using [FAMILY][]. Primary index data is organized into column
  families as per the TableDescriptor (e.g. there is no extraneous
  column data in a family and there are no extraneous families)
- Invalid data encodings. Data can't be read from the bytes or doesn't
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

What follows is a detailed user-level documentation of the feature.

The `SCRUB` command is for checking the validity of different apects of
CockroachDB. Optionally, `SCRUB` will also repair any errors if
possible.

The `SCRUB` statements are as follows:

| STATEMENT         | Purpose                                                                                       |
|-------------------|-----------------------------------------------------------------------------------------------|
| SCRUB TABLE       | Run either all checks or the provided checks on a specific table.                             |
| SCRUB DATABASE    | Run either all checks or the provided checks on a specific database.                          |
| SCRUB INDEX       | Scan through the provided secondary indexes to check if any entries are missing or incorrect. |
| SCRUB CONSTRAINTS | Force constraint validation checks on the provided constraints.                               |

## Output

If 1 or more rows are returned when the `SCRUB` command completes the an
error found in the data. Each row output refers to a distinct error. A
UUID associated with each run of check can be used to look up failures
in the system table `system.check_errors`. When running `SCRUB`, the
UUID can be found the event log along with any rows returned. 

The return has the following schema:
| Column           | Type     | Description                                                                                                                               |
|------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| job_uuid         | `UUID`     | A UUID for the execution of a check. The job_uuid can be used to later look up failures from the system table.                            |
| check_failure    | `STRING`   | Type of check failure. See the checks below for the possible types returned.                                                              |
| database         |  `STRING`  | Database containing the error.                                                                                                            |
| table            | `STRING`   | Table containing the error.                                                                                                               |
|  constraint_name | `STRING`   | Name of the constraint that has the error if a constraint is involved. This will be the name of the index if an index is involved. |
| columns          | `STRING[]` | List of all the columns involved in the failure. If the failure is a constraint or index, it may be multiple columns.                     |
| key_prefix       | `STRING`   | An internal prefix for the KV pair with the error.                                                                                        |
| pkey_id          | `STRING`   | Primary key ID of the row with the error, casted to a string. If no primary key is involved, or retrievable, will be `NULL`.              |
| repaired         | `BOOL`     | Whether or not the row has been automatically repaired.              |

## Examples

To run all checks on a table, use the following statement. Note that
`job_uuid` has been truncated for brievity.
```sql
CHECK TABLE mytable
```
| job_uuid      | check_failure | database     | table     | constraint_name | columns     | key_prefix       | pkey_id | repaired |
|---------------|---------------|--------------|-----------|-----------------|-------------|------------------|---------|---------|
| '63616665...' | 'ENCODING'    | 'mydatabase' | 'mytable' | NULL            | {'acolumn'} | '/1/mytable/123' | 123     | FALSE |
| '63616665...' | 'CONSTRAINT'  | 'mydatabase' | 'mytable' | 'acol_not_null' | {'acolumn'} | '/1/mytable/1'   | 1       | FALSE |

The following command also outputs the same rows as found above, and can
be done anytime after the check has returned.
```sql
SELECT * FROM system.check_errors WHERE job_uuid = '63616665...';
```

## `CHECK TABLE` and `CHECK DATABASE` commands

The `CHECK TABLE` and `CHECK DATABASE` statements run a check on the
provided table(s)/database(s) to detect any errors in the data. Options
can also be provided for checking only specific aspects. By default, all
checks are run.

The syntax for the statements is as follows:

```sql
CHECK TABLE <table>... [WITH <option>...]
CHECK DATABASE <database>... [WITH <option>...]
```

| Option      | Associated sub-command | Meaning                                                                                      |
|-------------|----------------|------------------------------------------------------------------------------|
| INDEX       | `CHECK INDEX` | Scan through all secondary index entries, checking if there are any missing or incorrect.    |
| CONSTRAINTS | `CHECK CONSTRAINTS` | Force constraint validation checks on all SQL constraints in the table(s) or database(s).    |
| PHYSICAL    | N/A | Scan all the rows to make sure that the data encoding and organization of data is correct.   |
| SYSTEM    | N/A | Checks all of the system tables associated with the table(s) or database(s).   |

For options associated with other sub-commands, see the sub-command
section for more details on them, including what they check and the
their output.

`check_failure` types associated with a `SYSTEM` check are as follows:
| check_failure | Meaning                                                                                                                                              |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| INTERNAL  | Something happened to a system table... Oops. (TODO: No internal check errors have been considered yet) |

 `check_failure` types associated a `PHYSICAL` check are as follows:
| check_failure | Meaning                                                                                                                                              |
|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| ENCODING   | One of the following has happened: unexpected data was present in a KV pair, a column value failed a re-encoding test, an invalid KV pair was found. |

## `CHECK CONSTRAINT` command

Checks the integrity of the specified SQL constraints. You can specify a
list of either tables or specific constraints to check. Any tables will
have all of their constraints checked.

The syntax for the statements is as follows:
```sql
CHECK CONSTRAINTS (<table>|<constraint>)...
```

Rows returned by `CHECK CONSTRAINT` will always have `constraint_name`
and `columns` present. `columns` will list all of the columns involved
in the constraint. If the constraint is `CHECK`, then the list of
columns will be empty. `check_failure` types associated with `CHECK
CONSTRAINT` checks are as follows:
| check_failure | Meaning                             |
|--------------|-------------------------------------|
| CONSTRAINT   | A row was violating the constraint. |
## `CHECK INDEX` command

Checks the integrity of the specified secondary indexes. You can specify
a list of either tables or specific indexes to check. Any tables will
have all of their indexes checked.

The syntax for the statements is as follows:
```sql
CHECK INDEX (<table>|<indexes>)...
```

Rows returned by `CHECK INDEX` will always have `constraint_name` and
`columns` present. `constraint_name` will be the name of the index and
`columns` will list all of the columns involved in the index.
`check_failure` types associated with `CHECK INDEX` checks are as
follows:

| check_failure             | Meaning                                                                             |
|--------------------------|-------------------------------------------------------------------------------------|
| MISSING_SECONDARY_INDEX  | A secondary index entry was missing for a row.                                      |
| DANGLING_SECONDARY_INDEX | A secondary index entry was found where the primary row data referenced is missing. |

## Repair process

Due to all the possible causes of error, the only action of repair for
certain errors may cause data loss. `REPAIR` will only do repairs with
no guaranteed data loss, while `REPAIR_ALLOW_DATA_LOSS` is available fix
problems that may result in data loss in addition to those fixed by
`REPAIR`.

Repairing can be enabled by providing the option to the regular `SCRUB`
statements. For example, to repair all errors found while checking a
table for any constraint violations or system errors:
```sql
SCRUB TABLE mytable WITH CONSTRAINT, SYSTEM, REPAIR
```

A list of `check_failure` types can be provided to conditionally only
repair the specified types of failures. For example, to only repair
encoding:
```sql
SCRUB TABLE mytable WITH CONSTRAINT, PHYSICAL, REPAIR(ENCODING)
```

If the `check_failure` type provided is not being checked by the
statement, and error will be thrown.
```sql
SCRUB TABLE mytable WITH CONSTRAINT, REPAIR_ALLOW_DATA_LOSS(ENCODING)
error: "attempting to repair error type that is not being checked: ENCODING"
```

With `REPAIR`, the following errors will be fixed:
- `MISSING_SECONDARY_INDEX`

In order to fix certain errors, operations that cause data loss may have
to be done. There is a `REPAIR_ALLOW_DATA_LOSS` option which fixes a few
more errors in addition to that of `REPAIR`, including:
- `ENCODING`
- `DANGLING_SECONDARY_INDEX`

Note that every repair will be logged with the entity of the data being
changed. Even in the event of data loss repairs, all data will be logged
before removal.

It is recommended when running a secondary index check to also provide
the option `REPAIR_ALLOW_DATA_LOSS(DANGLING_SECONDARY_INDEX)` along with
`REPAIR`. While there is a small change data loss may occur, any data
being removed will be logged. If both `DANGLING_SECONDARY_INDEX` and
`MISSING_SECONDARY_INDEX` are not both repaired this will problems when
using indexes.

## General guideline for new checks

When adding a check, you should be considering two questions:
1) Does this check involve only one particular database or table?

   If so, the check should exist as an option to `CHECK TABLE` and/or
   `CHECK DATABASE`.
2) Does this check involve an element that can be referenced that isn't
   a database or table? For example, indexes and constraints have unique
   names.

   If so, the check should exist as it's own sub-command which is able
   to check only a provided set of elements. For example, the `CHECK
   INDEX` or `CHECK CONSTRAINT` sub-commands behave this way.

# Alternatives

## SQL interfaces for running checks

There are limitless names you could give the check command. Scrub was
derived from it's use in [memory and filesystems][Data scrubbing].
Considering the same statement will be used for both checking and
repair, the name is a verb that reasonably fits both.

Fun fact: ZFS' use of scrubbing is distinct from the traditional fsck,
as it doesn't require unmounting a disk and taking it offline to do
error correction. You could interpret that the distinction was
intentional to change the perception of requiring downtime for error
correction. [Source][Oracle ZFS]

An alternative suggestion for concise verbs to express the statement was
`CHECK ...` for running checks, and `CHECK AND CORRECT ...` for running
repairs alongside the checks.

Use of an acronym such as `DBCC` has it's use -- if people are coming
from MSSQL, they'll recognize the command and it's capabilities. To the
unsuspecting person, it's less clear what the command may do.

Below is list of how other databases provide interfaces for checking
the consistency of data.

[Data scrubbing]: https://en.wikipedia.org/wiki/Data_scrubbing
[Oracle ZFS]: https://docs.oracle.com/cd/E23823_01/html/819-5461/gbbwa.html

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



In particular, there is an option which instead makes the database to
check logical corruption, `CHECK LOGICAL`:
>  Tests data and index blocks in the files that pass physical corruption checks for logical corruption, for example, corruption of a row piece or index entry. If RMAN finds logical corruption, then it logs the block in the alert log and server session trace file.

[Oracle]: http://docs.oracle.com/cd/B28359_01/backup.111/b28273/rcmsynta053.htm#RCMRF162

### MySQL

MySQL has an operation `CHECK TABLE` that is very similar to ours
functionally. Documentation can be found [here][MySQL Check]. They also
have `ANALYZE`, `REPAIR`, and `OPTIMIZE` queries. (`REPAIR` seems
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

| Option   | Meaning                                                                                                                                                        |
|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| QUICK    | Do not scan the rows to check for incorrect links.                                                                                                            |
| FAST     | Check only tables that have not been closed properly.                                                                                                         |
| CHANGED  | Check only tables that have been changed since the last check or that have                                                                                    |
| MEDIUM   | Scan rows to verify that deleted links are valid. This also calculates a key checksum for the rows and verifies this with a calculated checksum for the keys. |
| EXTENDED | Do a full key lookup for all keys for each row. This ensures that the table is 100% consistent, but takes a long time.

[MySQL Check]: https://dev.mysql.com/doc/refman/5.7/en/check-table.html

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
its own RFC.

## Error reporting

Reporting goes in tandem with scheduled checks as it only becomes
relevant when we don't have a foreground process to receive errors at.
If we add a scheduled job to do checks or do them through other
background processes we need a way to alert the user about failures.

For all approaches, whether the command runs in the foreground or
starts a background process, we can:
- Assign each check job a UUID
- Add any errors encountered into a recovery table with the job UUID
- Add an entry into the event log with job UUID, so the recovery entries
  can be looked up.

In addition, a few things we may want to consider:
- Store the failure into a table for display in admin. This can possibly
  include suggestions of how to repair the failure.
- Send a sentry report of the failure.
- Do other user-notifying actions, e.g. email them.

## Attempting to repair the data

Repairing the failing data is left as future work, because of the large
scope of the problem. It also fits in the context of a separate SQL
statement (or sub-command of this one) which explicitly _repairs_ any
errors caught by this check. This section is mentioned as it was
discussed in order to give guidance to the problem.

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
- The primary data may have been erroneously deleted and secondary
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
  provide? How do we split them, or do we make redundant checks between
  table and specific checks? e.g. `CHECK TABLE table WITH PHYSICAL` vs
  `CHECK PHYSICAL table`?

- What can we provide any actionable feedback to a user who has
  encountered a check failure? Can send an alert via Sentry or other
  means? Do we want anything more than just event log entries?

- Do these check queries run in the foreground, or in the background as
  a job? Can they be paused/resumed/cancelled?

- While it may be possible to throttle how fast we scan during KV checks
  (the `PHYSICAL` check), it may not be possible to throttle other
  checks. Do we provide throttling mechanisms to only that one command?

- All `PHYSICAL` check failures are bucketed into encoding. I'm not too
  sure what/how data about the failure can be presented in a meaningful
  way. This information is only helpful to us as far as I know. Maybe
  "The KV pair contained the bytes 0xABFF12 : 0xFFEE and the column
  'name' couldn't decode from the value at byte indexes 2-4"
