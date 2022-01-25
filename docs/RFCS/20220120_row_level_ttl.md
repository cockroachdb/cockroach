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
CREATE TABLE tbl (
   id INT PRIMARY KEY,
   text TEXT
) WITH (expire_after = '5 minutes')
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

The deletion job itself can get complex to write. We have a [guide](cockroach
TTL advice) which was difficult to perfect - when we implemented it ourselves,
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
a SQL delete clause.

## Syntax and Table Metadata

### Table Creation
TTLs are defined on a per-table basis. Developers can create a table with a TTL
using the following syntax, extending the `storage_parameter` syntax in
PostgreSQL:

```sql
CREATE TABLE tbl (
  id   INT PRIMARY KEY,
  text TEXT
) WITH (expire_after = '5 minutes')
```

This automatically creates a repeating scheduled job for the given table, as
well as adding the HIDDEN column `crdb_internal_expiration` to symbolize the
TTL:

```sql
CREATE TABLE tbl (
   id INT PRIMARY KEY,
   text TEXT,
   crdb_internal_expiration TIMESTAMPTZ
      NOT VISIBLE
      NOT NULL
      DEFAULT current_timestamp() + '5 minutes'
      ON UPDATE current_timestamp() + '5 minutes'
) WITH (expire_after = '5 minutes')
```

A user can override the crdb_internal_expiration expression by setting a custom
option for the column to represent as a TTL:

```sql
CREATE TABLE tbl (
   id   INT PRIMARY KEY,
   text TEXT,
   other_ts_field TIMESTAMPTZ
) WITH (expire_after = '5 minutes', ttl_expiration_column='other_ts_field')
```

TTL metadata is stored on the TableDescriptor:
```protobuf
message TableDescriptor {
  message RowLevelTTL {
    // DurationExpr is the automatically assigned interval for when the TTL should apply to a row.
    optional string duration_expr = 1 [(gogoproto.nullable)=false];
    // DeletionCron is the cron-syntax scheduling of the deletion job.
    optional string deletion_cron = 2 [(gogoproto.nullable)=false];
    // DeletionPause is true if the TTL job should
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

### Applying or Altering TTL for a table
TTL can be configured using `ALTER TABLE`:

```sql
-- adding or changing TTL on a table
ALTER TABLE tbl SET (expire_after = '5 minutes');
-- adding TTL with other options
ALTER TABLE tbl SET (expire_after = '5 minutes', ttl_expiration_column='other_ts_field');
-- restore default option for the TTL, reusing the PostgreSQL RESET syntax.
ALTER TABLE tbl RESET (ttl_expiration_column);
```

Note initially any options applied to the TTL in regards to the deletion job
will not apply whilst the deletion job is running; the user must
restart the job for the settings to take effect. A HINT will be displayed to the
user if this is required.

### Dropping TTL for a table
TTL can be dropped using `ALTER TABLE`:

```sql
ALTER TABLE tbl RESET (ttl_expiration_column)
```

The DROP will drop the TTL column if the column was not overriden by
the user with `ttl_expiration_column`. Other `ttl_`-prefixed settings will also
be dropped.

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
      could potentially never finish).
    * Issue a SQL DELETE up to delete_batch_size entries from the table using the
      PRIMARY KEYs selected above.

### Admission Control
To ensure the deletion job does not affect foreground traffic, we plan on using
[admission control](admission control) on a SQL transaction at a low value
(`-200`). This leaves room for lower values in future.

From previous experimentation, this largely regulates the amount of knob
controls users may need to configure, making this a "configure once, walk away"
process which does not need heavy monitoring.

### Rate Limiting
In an ideal world, admission control could reject traffic and force a backoff
and retry if it detects heavy load and rate limiting is not needed. However,
we will introduce a rate limiter option just in case admission control is not
adequate, especially in its infancy of being exposed. We can revisit this at a
later point.

### Controlling Delete Rate
Whilst our aim is for foreground traffic to be unaffected whilst deletion rate
manages to clean up "enough", we expect that there is still tinkering developers
must make to ensure a rate for their workload.

As such, users have knobs that control the delete rate, including:
* how often the deletion job runs (controls amount of "junk" data left)
* table GC time (when tombstones are removed and space is therefore reclaimed)
* the size of the ranges on the table, which has knock on effects for
  `ttl_range_concurrency`.

As part of the `(option = value, …)` syntax, users can also control the
following on the deletion job itself:

Option | Description
--- | ---
`expire_after` | When a TTL would expire. Accepts any interval. Defaults to ''30 days''. Minimum of `'5 minutes'`.
`ttl_select_batch_size` | How many rows to fetch from the range that have expired at a given time. Defaults to 500. Must be at least `1`.
`ttl_delete_batch_size` | How many rows to delete at a time. Defaults to 100. Must be at least `1`.
`ttl_range_concurrency` | How many concurrent ranges are being worked on at a time. Defaults to `cpu_core_count`. Must be at least `1`.
`ttl_admission_control_priority` | Priority of the admission control to use when deleting. Set to a high amount if deletion traffic is the priority.
`ttl_select_as_of_system_time` | AS OF SYSTEM TIME to use for the SELECT clause. Defaults to `-30s`. Proposed as a hidden option, as I'm not sure anyone will actually need it.
`ttl_maximum_rows_deleted_per_second` | Maximum number of rows to be deleted per second (acts as the rate limit). Defaults to 0 (signifying none).
`ttl_pause` | Pauses the TTL job from executing. Existing jobs will need to be paused using CANCEL JOB.
`ttl_job_cron` | Frequency the job runs, specified using the CRON syntax.

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
planned to be implemented for the first release of TTL.

## Overriding the default value for the `crdb_internal_expiration` column

In an ideal world, the user can simply update the `crdb_internal_expiration`
value to "override" the TTL. However, with our current defaults, this value
would get overridden if the row is updated due to the `ON UPDATE` clause. The
`ON UPDATE` should ideally be:

```sql
CASE WHEN crdb_internal_expiration IS NULL
-- NULL means the row should never be TTL'd
THEN NULL
-- otherwise, take whatever's larger: the user set crdb_internal_expiration
-- value or an update TTL based on the current_timestamp.
ELSE max(crdb_internal_expiration, current_timestamp() + 'table ttl value')
```

However, due to the limitation that `ON UPDATE` cannot reference a table
expression, this cannot yet be implemented. This can change when we implement
[triggers](https://github.com/cockroachdb/cockroach/issues/28296).

A workaround may be planned after the first iteration, but it is not deemed
a blocking feature for the first iteration of TTL.

## Foreign Keys
To avoid additional complexity in the initial implementation, foreign keys to or
from TTL tables will not be permitted. More thought has to be put on ON
DELETE/ON UPDATE CASCADEs before we can look at allowing this functionality.

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
) TTL (expire_after = '5 mins', delete_batch_size = 1, job_cron = '* * 1 * *');
-- adding a new TTL
ALTER TABLE ttl SET TTL; -- starts a TTL with a default of 30 days.
ALTER TABLE ttl SET TTL (expire_after = '5 mins', delete_batch_size = 1, job_cron = '* * 1 * *');
-- dropping a TTL
ALTER TABLE tbl DROP TTL;
```

The main attraction of this is that it is clear TTL is being added or dropped.
However, we have a mild preference for re-using the `storage_parameter` `WITH`
syntax from PostgreSQL, and that's what we ended up going with.

## Future Improvements
* Special annotation on TTL deletes for CDC: we may determine in future that
  DELETEs issued by a TTL job are a "special type" of delete that users can
  process.
* Secondary index deletion improvements: in the future, we can remove the
  performance hit when deleting secondary indexes by doing a delete on the
  secondary index entry before the delete on the primary index entry. This is
  predicated on filtering out expired rows working.
* Speed up the deletion by using indexes if one was created on the TTL column
  for the table.

## Open Questions
N/A

[#20239]: https://github.com/cockroachdb/cockroach/issues/20239
[#75189]: https://github.com/cockroachdb/cockroach/pull/75189
[cockroach TTL advice]: https://www.cockroachlabs.com/docs/stable/bulk-delete-data.html
[admission control]: https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/admission_control.md
[comparison doc]: https://docs.google.com/document/d/1HkFg3S-k3s2PahPRQhTgUkCR4WIAtjkSNVylarMC-gY/edit#heading=h.o6cn5faoiokv
