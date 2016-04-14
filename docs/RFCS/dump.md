- Feature Name: SQL Backup
- Status: draft
- Start Date: 2016-04-14
- Authors: Matt Jibson
- RFC PR: #6058


# Summary

In order to support backing up SQL data, we need a program that can recreate
structure and data. Add a `dump` subcommand to the CLI that produces a `.sql`
file that is suitable for being read by the `cockroach sql` shell.

# Motivation

We need a way to backup and restore SQL data for various reasons (just to
have backups, some kinds of version migrations or updates). Doing this at
any level below the SQL level (i.e., copying files, KV pairs, raft logs
(is this even a thing?)) may be faster in some cases, but will also exclude
some upgrades from being performed at all. For example, the recent decimal
sort encoding bug could only be fixed by re-encoding the decimal value,
which requires the SQL layer to re-encode it.

# Detailed design

The proposal is to add a `dump` subcommand to the CLI that can dump a
specified table, database, or entire cluster. This command will connect to
the SQL endpoint and produce structure and data SQL statements.

## Structured Statements

Structure statements (`CREATE TABLE`) will be produced by a new `SHOW CREATE
TABLE` statement. MySQL supports a `SHOW CREATE TABLE` statement. Postgres
uses `pg_dump --schema-only` which is an executable that queries the `pg_*`
tables and makes a `CREATE` statement from those. We have to do this work
somewhere, so we might as well expose this as a new statement that other
users and programs can use. Along with this, `SHOW CREATE DATABASE`, `SHOW
CREATE INDEX` and others as needed will also be added.

## Data Statements

Data statements (`INSERT`) will be produced using a `SELECT *`. Since a
table could be very large, we will page over the data using the last fetched
primary key as the starting point:

```
SELECT * FROM t LIMIT 1000
	Fetches keys 1 - 1000
SELECT * FROM t WHERE t.PK > 1000 LIMIT 1000
	Fetches keys 1001 - 2000
…
```

TODO: do these SELECTs need ORDER BY? If a SELECT always returns data (when
there is no ORDER BY clause) in the order of the PK, then I think we can get
by without it. Although maybe it’s safest to always specify ORDER BY PK,
since the query planner knows that the PK is indexed so it doesn’t have
to perform a sort on the entire table each time.

TODO: Is this any different than using LIMIT … OFFSET? Assuming that we issue
these SELECTs from the same transaction timestamp (and thus no data is added
or removed from the table), is there any reason to use OFFSET over the PK?

The results of these SELECT statements will be formatted into INSERT
statements.

TODO: One INSERT per row? INSERTs with multiple rows? How many rows per
INSERT? Profiling this and determining an optimum number may be better than
guessing. For now I think using the same number of rows as per SELECT LIMIT
per INSERT is a good choice.

## Transactions

Since we need the data to not change during the multiple SELECTs, all this
work (structure + data) must be done in the same transaction, or at least at
the same timestamp. Issuing a `BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT`
will do this for a specific connection.

Since large backups will take more time, they have a higher chance of losing
their connection for whatever reason, and thus ends the transaction. We thus
need a way to resume a dump from the last place and time that it started. This
could be achieved using a time travel query (#5963) which discusses many
possible solutions.

TODO: is SNAPSHOT the best isolation level for this? Or do the time travel
queries fix any time-related problems and we don’t have to use transactions
at all? We want something that will allow us to read data that won’t change
and that also won’t abort any other write transactions. I’m not overly
familiar with our isolation levels so I need some confirmation here.

# Alternatives

Instead of connecting to the SQL endpoint, we could connect directly to the
KV layer and perform all the same work. This would require more work since
we'd be re-implementing a lot of stuff that SQL does for us, but it *may*
increase performance because we can skip some things SQL does that we don't
care about. However, these are just possible performance gains for more work,
so this is likely a bad idea to start with, and may not have any actual
performance gains even if it were done.

Both Postgres and MySQL have machinery to have the server write a file from
a SELECT: `SELECT * FROM t INTO OUTFILE`, `COPY t TO ‘filename’`. These
seem not appropriate for Cockroach since the performance benefits of these
statements is that the network does not have to be traversed when dumping the
data. But in Cockroach’s case the data is already on various nodes over
the network, so the benefit of writing to a file on a specific server is
greatly decreased. These seem like optimizations for a future version which
can determine which ranges live on which nodes and request that those nodes
write a SQL dump file to their own disks, which operators then aggregate
and archive elsewhere.

# Unresolved questions

User accounts and privilege grants. Do we want them always, never, or
sometimes? Our current GRANT system can grant privileges to users that don't
exist, so it may make sense to always create GRANT statements for any level
of dump (table, database, cluster).
