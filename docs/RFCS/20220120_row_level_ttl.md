# Row Level TTL
* Feature Name: Row-level TTL
* Status: in-progress
* Start Date: 2021-12-14
* Authors: Oliver Tan
* RFC PR: [#75189](#75189)
* Cockroach Issue: [#20239](#20239)

# Summary
Row-level "time to live" (TTL) is a mechanism in which rows from a table
automatically get deleted once the row surpasses an expiration time (the "TTL").
This has been a [feature commonly asked for](#20239).

This RFC proposes a CockroachDB level mechanism to support row-level TTL, where
rows will be deleted after a certain period of time. As a further extension in a
later release, rows rows will be automatically hidden after they've expired
their TTL and before they've been physically deleted.

The following declarative syntaxes will initialize a table with row-level TTL:
```sql
-- Using a database-managed column for TTL expiration.
CREATE TABLE tbl (
   id INT PRIMARY KEY,
   text TEXT
) WITH (ttl_expire_after = '5 minutes')
-- Using custom logic for TTL expiration.
CREATE TABLE tbl (
   id INT PRIMARY KEY,
   text TEXT,
   expiration TIMESTAMPTZ,
   should_delete BOOL,
) WITH (ttl = 'on', ttl_expiration_expression = 'if(should_delete, expiration, NULL)');
```

By implementing row-level TTL, we are saving developers from writing a complex
scheduled job and additional application logic to handle this functionality. It
also brings us to parity with other database systems which support this feature.

**Note**: this does NOT cover partition level deletes, where deletes can be
"accelerated" by deleting entire "partitions" at a time.

# Motivation
Today, developers who need row-level TTL need to roll out their own mechanism
for deleting rows as well as adding application logic to filter out the expired
rows.

The deletion job itself can get complex to write. We have a [guide](cockroach TTL advice)
which was difficult to perfect - when we implemented it ourselves,
we found there were multiple issues related to performance. Developers have to
implement and manage several knobs to balance deletion time and performance on
foreground traffic (traffic from the application):
* how to shard out the delete & run the deletes in parallel
* how many rows to SELECT and DELETE at once
* a rate limit to control the rate of SELECTs and DELETEs

Furthermore, developers require productionizing the jobs themselves, adding more
moving pieces in their production environment.

Having a row-level TTL is sought often enough that warrants being a feature
supported by CockroachDB natively, avoiding the complexity of another moving
part and complex logic from developers and making data easy.

# Technical design

At a high level, a user specifies a TTL on the table level syntax. A background
deletion job is scheduled to delete any rows that have expired the TTL using the
SQL delete clause.

## Syntax and Table Metadata

### Table Creation
TTLs are defined on a per-table basis. Developers can create a table with a TTL
using the following syntax, extending the `storage_parameter` syntax in
PostgreSQL.

To create a table with TTL with an automatically managed column to expire
rows, a user can use:
```sql
CREATE TABLE tbl (
  id   INT PRIMARY KEY,
  text TEXT
) WITH (ttl_expire_after = '5 minutes')
```

This automatically creates a repeating scheduled job for the given table, as
well as adding the HIDDEN column `crdb_internal_expiration` to symbolize the
TTL and implicitly adds the `ttl` and `ttl_automatic_column` parameters:

```sql
CREATE TABLE tbl (
   id INT PRIMARY KEY,
   text TEXT,
   crdb_internal_expiration TIMESTAMPTZ
      NOT VISIBLE
      NOT NULL
      DEFAULT current_timestamp() + '5 minutes'
      ON UPDATE current_timestamp() + '5 minutes'
) WITH (ttl = 'on', ttl_automatic_column = 'on', ttl_expire_after = '5 minutes')
```

Users can also opt to use their own expression which evaluates to a NULLABLE
TIMESTAMPTZ to signify expiration:

```sql
CREATE TABLE tbl (
   id INT PRIMARY KEY,
   text TEXT,
   expiration TIMESTAMPTZ,
   should_delete BOOL,
) WITH (ttl_expiration_expression = 'if(should_delete, expiration, NULL)');
```

Which similarly implicitly adds storage parameters:
```sql
CREATE TABLE tbl (
   id INT PRIMARY KEY,
   text TEXT,
   expiration TIMESTAMPTZ,
   should_delete BOOL,
) WITH (ttl = 'on', ttl_expiration_expression = 'if(should_delete, expiration, NULL)');
```

Note that `ttl_expiration_expression` or `ttl_expire_after` is required for TTL
to be enabled, and that both may be specified:
```sql
CREATE TABLE tbl (
   id INT PRIMARY KEY,
   text TEXT,
   should_delete BOOL,
) WITH (ttl = 'on', ttl_expiration_expression = 'if(should_delete, crdb_internal_expiration, NULL)', ttl_expire_after = '10 mins');
```

TTL metadata is stored on the TableDescriptor:
```protobuf
message TableDescriptor {
  message RowLevelTTL {
    // DurationExpr is the automatically assigned interval for when the TTL should apply to a row.
    optional string duration_expr = 1 [(gogoproto.nullable)=false];
    // DeletionCron is the cron-syntax scheduling of the deletion job.
    optional string deletion_cron = 2 [(gogoproto.nullable)=false];
    // DeletionPause is true if the TTL job should not run.
    // Intended to be a temporary pause.
    optional bool deletion_pause = 3 [(gogoproto.nullable)=false];
    // DeleteBatchSize is the number of rows to delete in each batch.
    optional int64 delete_batch_size = 4;
    // SelectBatchSize is the number of rows to select at a time.
    optional int64 select_batch_size = 5;
    // MaximumRowsDeletedPerSecond controls the amount of rows to delete per second.
    // At zero, it will not impose any limit.
    optional int64 max_rows_deleted_per_second = 6;
    // RangeConcurrency controls the amount of ranges to delete at a time.
    // Defaults to 0 (number of CPU cores).
    optional int64 range_concurrency = 7;
  }

  // ...
  optional RowLevelTTL row_level_ttl = 47 [(gogoproto.customname)="RowLevelTTL"];
  // ...
}
```

### Storage Parameters for Controlling Delete Rate
As part of the `(option = value, …)` storage parameter syntax, we will support
the following options to control the TTL job:

Option | Description
--- | ---
`ttl` | Automatically set option. Signifies if a TTL is active.  Not used for the job.
`ttl_automatic_column` | Automatically set option if automatic connection is enabled. Not used for the job.
`ttl_expire_after` | When a TTL would expire. Accepts any interval. Defaults to ''30 days''. Minimum of `'5 minutes'`.
`ttl_expiration_expression` | If set, uses the expression specified as the TTL expiration. Defaults to just using the `crdb_internal_expiration` column.
`ttl_select_batch_size` | How many rows to fetch from the range that have expired at a given time. Defaults to 500. Must be at least `1`.
`ttl_delete_batch_size` | How many rows to delete at a time. Defaults to 100. Must be at least `1`.
`ttl_range_concurrency` | How many concurrent ranges are being worked on at a time. Defaults to `cpu_core_count`. Must be at least `1`.
`ttl_admission_control_priority` | Priority of the admission control to use when deleting. Set to a high amount if deletion traffic is the priority.
`ttl_select_as_of_system_time` | AS OF SYSTEM TIME to use for the SELECT clause. Defaults to `-30s`. Proposed as a hidden option, as I'm not sure anyone will actually need it.
`ttl_maximum_rows_deleted_per_second` | Maximum number of rows to be deleted per second (acts as the rate limit). Defaults to 0 (signifying none).
`ttl_pause` | Pauses the TTL job from executing.
`ttl_job_cron` | Frequency the job runs, specified using the CRON syntax.

### Applying or Altering TTL for a table
TTL can be configured using `ALTER TABLE`:

```sql
-- Adding TTL for the first time with an automatic column, or changing the automatic TTL expiration.
ALTER TABLE tbl SET (ttl_expire_after = '5 minutes');
-- Adding TTL for the first time with an expiration expression, or changing the expiration expression.
ALTER TABLE tbl SET (ttl_expiration_expression = '...');
-- Altering the TTL options.
ALTER TABLE tbl SET (ttl_select_batch_size=200);
-- Restore default option for the TTL, reusing the PostgreSQL RESET syntax.
ALTER TABLE tbl RESET (ttl_select_batch_size);
```

Note initially any options applied to the TTL in regards to the deletion job
will not apply whilst the deletion job is running; the user must
restart the job for the settings to take effect. A HINT will be displayed to the
user if this is required.

### Converting between `ttl_automatic_column` and `ttl_expiration_expression`

Users can convert from using the automatic column to the expiration expression
by re-using the `SET` syntax:

```sql
ALTER TABLE tbl SET (ttl_expiration_expression = 'other_column');
-- Resetting the ttl_automatic_column will drop the TTL column.
-- This step is optional in case the automatic column is still used.
ALTER TABLE tbl RESET (ttl_automatic_column);
```

To go the other way:
```sql
ALTER TABLE tbl SET (ttl_expire_after = '10 minutes');
ALTER TABLE tbl RESET (ttl_expiration_expression);
```

### Dropping TTL for a table
TTL can be dropped using `ALTER TABLE`:

```sql
ALTER TABLE tbl RESET (ttl)
```

The RESET will also drop any created automatic TTL column.

### Limitations on the `crdb_internal_expiration` column
* The column can be used in indexes, constraints and primary keys.
* The ON UPDATE and ON DELETE for `crdb_internal_expiration` may be not overridden.
* The column cannot be re-named.

## The Deletion Job
Row deletion is handled by a scheduled job in the jobs framework. The scheduled
job gets created (or modified) by the syntax detailed in the previous section -
there is one scheduled job per table with TTL.

The deletes issued by the job are a SQL-based delete, meaning they will show up
on changefeeds as a regular delete by the root user (we may choose to create a
new user for this in the future). Using SQL based deletes has the nice property
of handling deletes on secondary index entries automatically, and can form the
basis of correctly handling foreign keys or triggers in the future.

### Job Algorithm

On each job invocation, the deletion job performs the following:
* Establish a "start time".
* Paginate over all ranges for the table. Allocate these to a "range worker pool".
* Then, for each range worker performs the following:
  * Traverse the range in order of PRIMARY KEY:
    * Issue a SQL SELECT AS OF SYSTEM TIME '-30s' query on the PRIMARY KEYs up to
      select_batch_size entries in which the TTL column is less than the established
      start time (note we need to establish the start time before we run or the job
      could potentially never finish). The SELECT is separate from the DELETE clause
      to avoid the expensive costs associated with a write transaction on the entire
      range.
    * Issue a SQL DELETE up to delete_batch_size entries from the table using the
      PRIMARY KEYs selected above. Since the row may be updated between the above
      SELECT and this DELETE, we also add a clause here to ensure the row has
      truly expired TTL when deleting.

Example SELECT query:
```sql
SELECT pk_col_1, pk_col_2 FROM tbl
AS OF SYSTEM TIME '-30s'
WHERE crdb_internal_expiration >= '2022-01-02 01:02:03'
ORDER BY pk_col_1, pk_col_2
```

Example DELETE query:
```sql
DELETE FROM tbl
WHERE (pk_col_1, pk_col_2) IN  ((1, 2), (3, 4), (5, 6))
AND crdb_internal_expiration >= '2022-01-02 01:02:03'
```

### Controlling Delete Rate
Whilst our aim is for foreground traffic to be unaffected whilst deletion rate
manages to clean up "enough", we expect that there is still tinkering developers
must make to ensure a rate for their workload.

In addition to the options listed above for controlling delete rate, there
are additional knobs a user can use to control how effective the deletion
performs:
* how often the deletion job runs (controls amount of "junk" data left)
* table GC time (when tombstones are removed and space is therefore reclaimed)
* the size of the ranges on the table, which has knock on effects for
  `ttl_range_concurrency`.

### Admission Control
To ensure the deletion job does not affect foreground traffic, we plan on using
[admission control](admission control) on a SQL transaction at a low value
(`-100`). This leaves room for lower values in future.

From previous experimentation, this largely regulates the amount of knob
controls users may need to configure, making this a "configure once, walk away"
process which does not need heavy monitoring.

### Transaction Priority
We will also run the DELETE queries under a lower transaction priority so that
any contending transaction would be able to abort the DELETE and force it to
retry instead of waiting on its locks.

### Rate Limiting
In an ideal world, admission control could reject traffic and force a backoff
and retry if it detects heavy load and rate limiting is not needed. However,
we will introduce a rate limiter option just in case admission control is not
adequate, especially in its infancy of being exposed. We can revisit this at a
later point.

#### A note on delete_batch_size
Deleting rows on tables with secondary indexes on a table lays more intents,
hence causing higher contention. In this case, there is an argument to delete
less rows at a time (or indeed, just one row at a time!) to avoid this. However,
testing showed deletes could never catch up in this case and realistically a
very small number of rows can be deleted in this way. "100" was set as the
balance for now.

## Filtering out Expired Rows
Rows that have expired their TTL can be optionally removed from all SQL query
results. Work for this is required at the optimizer layer. However, this is not
planned to be implemented for the first iteration of TTL.

## Foreign Keys to/from TTL Tables
To avoid additional complexity in the initial implementation, foreign keys (FK)
to and from TTL tables will not be permitted due to complexities with the
implementation which are complex to handle, for example:
* When having a non-TTL table with a FK dependent on a TTL table with an
  `ON UPDATE/DELETE CASCADE`, the non-TTL table need to hide any rows which
  are linked to an expired TTL row.
* When having a TTL table with an FK dependent on a non-TTL table,
  `ON DELETE RESTRICT` should only block a delete on the non-TTL table
  if the row has expired.

## Introspection
The TTL definition for the table will appear in `SHOW CREATE TABLE`. The options
for the TTL job on the table can be found on the `pg_class` table under
`reloptions`.

## Observability
As mentioned above, we expect users may need to tinker with the deletion job to
ensure foreground traffic is not affected.

We will also expose a number of metrics on the job to help gauge deletion
progress which is exposed as a prometheus metric with a `tablename` label.

The following can be monitored using metrics graphs on the DB console:
* selection and deletion rate (`sql.ttl.select_rate`, `sql.ttl.delete_rate`)
* selection and deletion latency (`sql.ttl.select_latency`, `sql.ttl.delete_latency`)

In the future, we may also introduce a mechanism to monitor "lag rate" by
exposing a denormalized table metadata field or metric similar to
`estimated_row_count` called `estimated_expired_row_count`, which counts the
number of rows which are estimated to not have expired TTL. Users will be able
to poll this to determine the effectiveness of the TTL deletion job. This can be
handled by the automatic statistics job.

## Alternatives

### KV Approach to Row Expiration
There was a major competitor to the "SQL-based deletion approach" - having the
KV be the layer responsible for TTL. The existing GC job would be responsible
for automatically clearing these rows. The KV layer would not return any row
which has expired from the TTL, in theory also removing contention from intent
resolution thus reducing foreground performance issues.

The main downside of this approach is that is it not "SQL aware", resulting in
a few problems:
* Secondary index entries would need some reconciliation to be garbage 
  collected. As KV is not aware which rows in the secondary index need to be
  deleted from the primary index, this would represent blocking TTL tables
  allowing row-level TTL.
  * We can make all of these changes atomically without needing to coordinate
    with concurrent transactions. If we did filtering during query execution
    and we made a change to the semantics of what was filtered, we might need
    to be careful about how we roll out those versions to users. Namely, we
    cannot start deleting data until nobody thinks that data might be live.
    This toolkit exists if we chose to go to it.
* If we wanted CDC to work, tombstones would need to be written by the KV GC
  process. This adds further complexity to CDC.

As row-level TTL is a "SQL level" feature, it makes sense that something in the
SQL layer would be most appropriate to handle it. See [comparison
doc](comparison doc) for other observations.

### Alternative TTL columns
Another proposal for TTL columns was to have two columns:
* a `last_updated` column which is the last update timestamp of the column.
* a `ttl` column which is the interval since `last_updated` before the row
  gets deleted.

An advantage here is that it is easy to specify an "override" - bump the
`ttl` interval value to be some high number, and `last_updated` still gets
updated.

We opted against this as we felt it was less obvious than the absolute
timestamp long term vision.

### Alternative TTL SQL syntax
An alternative SQL syntax for TTL would be to make TTL its own clause in a
CREATE TABLE / ALTER TABLE.

The syntax would look as follows:

```sql
-- creating a table with TTL
CREATE TABLE tbl (
  id INT PRIMARY KEY,
  text TEXT
) TTL;
-- creating a table with TTL options
CREATE TABLE tbl (
  id INT PRIMARY KEY,
  text TEXT
) TTL (ttl_expire_after = '5 mins', delete_batch_size = 1, job_cron = '* * 1 * *');
-- adding a new TTL
ALTER TABLE ttl SET TTL; -- starts a TTL with a default of 30 days.
ALTER TABLE ttl SET TTL (ttl_expire_after = '5 mins', delete_batch_size = 1, job_cron = '* * 1 * *');
-- dropping a TTL
ALTER TABLE tbl DROP TTL;
```

The main attraction of this is that it is clear TTL is being added or dropped.
However, we have a mild preference for re-using the `storage_parameter` `WITH`
syntax from PostgreSQL, and that's what we ended up going with.

### Improved ON UPDATE expression for `crdb_internal_expiration`

In some cases, a user may wish to override TTL for a specific row. We currently
have a workaround for this by using the  `ttl_expiration_expression`.
Another way to get around this would be to update the `ON UPDATE` clause
for `crdb_internal_expiration` to be:

```sql
CASE WHEN crdb_internal_expiration IS NULL
-- NULL means the row should never be TTL'd
THEN NULL
-- otherwise, take whatever is larger: the user set crdb_internal_expiration
-- value or an update TTL based on the current_timestamp.
ELSE max(crdb_internal_expiration, current_timestamp() + 'table ttl value')
```

However, due to the limitation that `ON UPDATE` cannot reference a table
expression, this cannot yet be implemented. We may choose to use
this as a later default when we implement [triggers](#28296).

## Future Improvements

### CDC DELETE annotations
We may determine in future that DELETEs issued by a TTL job are a "special type"
of delete that users can process.

### Secondary index deletion improvements
We can improve deletion speed on secondary indexes if we split the deletes for
secondary indexes and the primary index. This way, we have a lot more
flexibility in terms of which writes we batch.

What this allows us to do is throw thousands of rows in a big buffer, decompose
their individual writes, bucket them by range, and send a batch per range. This
has two benefits, both of which are very significant:
* we can easily build up large  batches of 100s of writes for each range,
  which will stay batched all the way through Raft and down to Pebble. 
* these batches don’t run a distributed transaction protocol, so they hit the
  1PC fast path instead of writing intents, running a 2-phase commit protocol,
  then cleaning up those intents.

This is predicated on filtering out expired rows working as otherwise users
could miss entries when querying the secondary index as opposed to the primary.

### Improve the deletion loopl
We can speed up the deletion by using indexes if one was created on the TTL
column for the table.

## Open Questions
N/A

[#20239]: https://github.com/cockroachdb/cockroach/issues/20239
[#75189]: https://github.com/cockroachdb/cockroach/pull/75189
[#28296]: https://github.com/cockroachdb/cockroach/issues/28296
[cockroach TTL advice]: https://www.cockroachlabs.com/docs/stable/bulk-delete-data.html
[admission control]: https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md
[comparison doc]: https://docs.google.com/document/d/1HkFg3S-k3s2PahPRQhTgUkCR4WIAtjkSNVylarMC-gY/edit#heading=h.o6cn5faoiokv
