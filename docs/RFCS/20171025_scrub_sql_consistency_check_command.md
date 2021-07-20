- Feature Name: SCRUB -- SQL consistency check command
- Status: in-progress
- Start Date: 2017-09-20
- Authors: Joey Pereira
- RFC PR: [#18675](https://github.com/cockroachdb/cockroach/issues/18675)
- Cockroach Issue: [#10425](https://github.com/cockroachdb/cockroach/issues/10425)

# Summary

This RFC outlines the interface for a set of SQL statements that will
check the validity of the table data, including schemas, foreign
keys, indexes, and encoding.

These checks will be accessible through SQL queries. If a user wants to
run them on a regular schedule, they can use an external process to do
so. The checks may possibly be used in logictests to further check
correctness.

# Motivation

Until this point, in CockroachDB, there weren't ways to reliably check
the consistency of table data. Often, errors are only detected on
problems that occur on access. There have been various consistency
errors that have popped up during development, appearing as mismatched
index entries ([#18705], [#18533]) and unchecked foreign keys
([#17626], [#17690]). A significant factor causing a rise in these
types of errors are new features that manipulate tables outside of
transactions (schema changes, backup, CSV import).

In summary, aspects that can be checked include:
- Secondary indexes have entries for all rows in the table
- Dangling index references (i.e. missing row but index entry retained)
- SQL constraints (`CHECK`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY`)
- Indexes using [STORING]. Wrong copies of data in the index (either
  in the index key or the data payload)
- Tables using [FAMILY]. Primary index data is organized into column
  families as per the TableDescriptor (e.g. there is no extraneous
  column data in a family and there are no extraneous families)
- Invalid data encodings. Data can't be read from the bytes or doesn't
  match expected types.
- Non-canonical encodings (where we can decode the data, but if we
  re-encode it we get a different value.)
- [Composite encodings] are valid. This includes `DECIMAL` and
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


# How to use SCRUB

What follows is a detailed user-level documentation of the feature.

The `SCRUB` command is for checking the validity of different aspects of
CockroachDB. Optionally, `SCRUB` will also repair any errors if
possible. In order to run `SCRUB`, the user needs to have root
permissions.

When a `SCRUB` is run, a job is made that will be visible through the
Admin UI and the jobs table (`SHOW JOBS`). The `SCRUB` job is
cancelable.

The `SCRUB` statements are as follows:

| STATEMENT         | Purpose                                                                                       |
|-------------------|-----------------------------------------------------------------------------------------------|
| SCRUB TABLE       | Run either all checks or the provided checks on a specific table.                             |
| SCRUB DATABASE    | Run either all checks or the provided checks on a specific database.                          |
| SCRUB ALL         | Run either all checks or the provided checks on all databases.                              |

## Output

If 1 or more rows are returned when the `SCRUB` command completes the
error found in the data. Each row output refers to a distinct error.
When running `SCRUB`, the UUID can be found the event log along with any
rows returned. The UUID associated with an execution of `SCRUB` can be
used to look up failures in the system table `system.scrub_errors`.

Additionally, any errors found will be reported to Cockroach Labs
through Sentry but the with details stripped to not expose any of the
data.

Both the rows returned and the system table have the following schema:

| Column           | Type     | Description                                                                                                                               |
|------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| job_uuid         | `UUID`     | A UUID for the execution of a scrub. The job_uuid can be used to later lookup failures from the scrub errors system table.                            |
| error_type       | `STRING`   | Type of error. See the "Error types" section for the possible types.                                                                       |
| database         |  `STRING`  | Database containing the error.                                                                                                            |
| table            | `STRING`   | Table containing the error.                                                                                                               |
| primary_key      | `STRING`   | Primary key of the row with the error, cast to a string. If no primary key is involved, or retrievable, this will be `NULL`.              |
| timestamp        | `TIMESTAMP` | The timestamp associated with the data that has the error. If present, this will be the last time the data was updated.                     |
| repaired         | `BOOL`     | Whether or not the row has been automatically repaired.              |
| details          | `STRING`   | A free-form JSON string with additional details about the error. This also includes internal details, such as the raw key, raw secondary key, and table ID.           |


## Command usage

The `SCRUB TABLE`, `SCRUB DATABASE`, and `SCRUB ALL` statements run a
scrub on the target (either all databases or the specified
table(s)/databases(s)) to detect any errors in the data. Options can
also be provided for checking only specific aspects. By default, all
checks are run.

The syntax for the statements is as follows:

```sql
SCRUB TABLE <table>... [AS OF SYSTEM TIME <expr>] [WITH OPTIONS <option>...]
SCRUB DATABASE <database>... [AS OF SYSTEM TIME <expr>] [WITH OPTIONS <option>...]
```

Note that the options `INDEX` and `CONSTRAINT` are not permitted with
`SCRUB ALL` as the specified indexes or constraints may be ambiguous.

| Option      | Meaning                                                                                      | Options |
|-------------|------------------------------------------------------------------------------|----|
| INDEX       | Scan through all secondary index entries, checking if there are any missing or incorrect.    | Either `INDEX ALL` can be used to check all indexes, or checks can be restricted only to specified indexes, e.g. `INDEX (<index_name>...)` |
| CONSTRAINT  | Force constraint validation checks on all SQL constraints in the table(s) or database(s).    | Either `CONSTRAINT ALL` can be used to check all Constraints, or the check can be restricted only to specified constraints, e.g. `CONSTRAINT (<constraint_name>...)` |
| PHYSICAL    | Scan all the rows to make sure that the data encoding and organization of data is correct.   | N/A |
| SYSTEM      | Checks all of the system tables associated with the table(s) or database(s).   | N/A |

## Examples

To run all checks on a table, use the following statement. Note that
`job_uuid` has been truncated for brevity.

```sql
SCRUB TABLE mytable
```

| job_uuid      | error_type | database     | table     | constraint      | columns     |  pkey_id | timestamp | repaired | details |
|---------------|---------------|--------------|-----------|-----------------|-------------|-------------------|---------|-----------|---------|
| '63616665...' | 'invalid_encoding'    | 'mydatabase' | 'mytable' | NULL            | {'acolumn'} | 123     | '2016-03-26 10:10:10' |FALSE | '{"key": }' |
| '63616665...' | 'constraint'  | 'mydatabase' | 'mytable' | 'acol_not_null' | {'acolumn'} | 1       | '2016-03-26 10:10:10' | FALSE | '{}' |

The following command also outputs the same rows as found above, and can
be done anytime after the check has returned. Note that this will return additional columns.

```sql
SELECT * FROM system.check_errors WHERE job_uuid = '63616665...';
```

To run a physical check and an index check on only the indexes
`name_idx` and `other_idx`:

```sql
SCRUB DATABASE current_db WITH OPTIONS PHYSICAL, INDEX (name_idx, other_idx)
```

To run all checks on a database with data as of a system time:

```sql
SCRUB DATABASE current_db AS OF SYSTEM TIME '2017-11-13'
```

## As of system time

Scrub also supports the `AS OF SYSTEM TIME <expr>` clause in order to
run all checks on historical values. For example, this is useful for
checking indexes backfilled by schema changes if schema changes do
backfill historical values in order to verify the integrity (they
currently do _not_ backfill historical values).

## Error types

The following table has all of the possible `error_type`s found during
checking. It is sorted by the corresponding check options that may
return the error.

| Failure type               | Corresponding checks | Description                                                                                             |
|----------------------------|----------------------|---------------------------------------------------------------------------------------------------------|
| constraint                 | CONSTRAINT           | A row was violating an SQL constraint.                                                                  |
| missing_secondary_index    | INDEX                | A secondary index entry was missing for a row.                                                          |
| dangling_secondary_index   | INDEX                | A secondary index entry was found where the primary row data referenced is missing.                     |
| invalid_composite_encoding | PHYSICAL             | Data was found with an invalid composite encoding.                                                      |
| invalid_encoding           | PHYSICAL             | Data was found with an invalid encoding, and could not be decoded.                                      |
| noncanonical_encoding      | PHYSICAL             | Data was found with a non-canonical encoding.                                                           |
| internal                   | SYSTEM               | Something happened to a system table... Oops. (TODO: No internal check errors have been considered yet) |


Errors with a failure type `constraint` will include the keys
`constraint` and `columns` in the `details` JSON column. `columns` will
list all of the columns involved in the constraint. If the SQL
constraint being checked is a `CHECK` constraint, then the list of
columns will be empty.

Errors with the failure type `MISSING_SECONDARY_INDEX` or
`DANGLING_SECONDARY_INDEX` check will include the keys `index` and
`columns` in the `details` JSON column. `index` will be the name of the
index and `columns` will list all of the columns involved in the index.

# Alternatives

## SQL interfaces for running checks

There are limitless names you could give the check command. Scrub was
derived from its use in [memory and filesystems][Data scrubbing].
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

Use of an acronym such as `DBCC` has value -- if people are coming
from MSSQL, they'll recognize the command and its capabilities. To the
unsuspecting person though, it is less clear what the command may do.

Below is list of how other databases provide interfaces for checking
the consistency of data.

[Data scrubbing]: https://en.wikipedia.org/wiki/Data_scrubbing
[Oracle ZFS]: https://docs.oracle.com/cd/E23823_01/html/819-5461/gbbwa.html

### Microsoft

Microsoft SQL Server has a command `DBCC`. Documentation can be found
[here][MS DBCC]. They have dozens of sub-commands which they put into 4
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

## Cleaning up the `system.scrub_errors` table

As with `system.jobs`, we will need to clean up the `scrub_errors`
table. This is currently an unresolved question/future work also in
the [system jobs RFC].

[system jobs RFC]: https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20170215_system_jobs.md#unresolved-questions-and-future-work

## Checking system tables

Outside of checking the user data is consistent, largely testing the
correctness of the SQL layer, system tables can also be checked. A few
system table checks include:
- Invalid table ID / missing namespace entry
- Duplicate table IDs in namespace table
- Whether `system.zones` has a default entry, i.e. an entry for
  RootNamespaceId
- Whether every `system.zones` entry corresponds to a descriptor and
  namespace. (We should really just put a foreign key on the table, but
  that might be hard.)
- Similarily, check `system.lease` entries refer to valid descriptor and
  namespace.
- Whether users mentioned in descriptor privileges actually appear in
  `system.users`

## Scheduled checks

If checks are run automatically in any manner this brings up problems
for how we prevent these checks from impacting foreground traffic. The
checks can be run manually at the discretion of a DBA as desired, while
fully understanding the impact.

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

## Repairing errors

See the [Repairing SQL Errors RFC](repairing_sql_errors.md)

# Unresolved questions
