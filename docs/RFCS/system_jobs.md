- Feature Name: System Jobs
- Status: draft
- Start Date: 2017-02-13
- Authors: Nikhil Benesch
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#12555]

# Summary

Add a system table to track the progress of backups, restores, and schema
changes.

# Motivation

When performing a schema change, only one bit of progress information is
available: whether the change has completed, indicated by whether the query has
returned. Similarly, when performing a backup or restore, status is only
reported after the backup or restore has completed. Given that a full backup of
a 2TB database takes on the order of several hours to complete, the lack of
progress information is a serious pain point for users.

Additionally, if the leader of a backup or restore job fails, the backup or
restore will abort, even if the individual workers in the job were otherwise
successful.

This RFC proposes a new system table, `system.jobs`, that tracks the status of
these long-running backup, restore, and schema change "jobs." This table will
directly expose the desired progress information via `SELECT` queries over the
table, enable an API endpoint to expose the same progress information in the
admin UI, and enable an internal daemon that periodically adopts and resumes
orphaned jobs.

# Detailed design

## Prior art

Adding a `system.jobs` table has been proposed and unsuccessfully implemented
several times. [@vivekmenezes]'s initial attempt in [#7037] was abandoned since,
at the time, there was no way to safely add a new system table to an existing
cluster. Several months later, after [@a-robinson] and [@danhhz] built the
necessary cluster migration framework, @a-robinson submitted a migration to add
a `system.jobs` table in [#11722] as an example of using the new framework. His
PR was rejected because the table's schema hadn't been fully considered. This
RFC describes a revised `system.jobs` schema for us to thoroughly vet before we
proceed with a final implementation.

## The proposal

Within this RFC, the term "job" or "long-running job" refers only to backups,
restores, and schema changes. Other types of long-running queries, like slow
`SELECT` statements, are explicitly out of scope. A "job-creating query," then,
is any query of one of the following types:

- `ALTER TABLE`, which creates a schema change job
- `BACKUP`, which creates a backup job
- `RESTORE`, which creates a restore job

To track the progress of these jobs, the following table will be injected into
the `system` database using the [cluster migration framework]:

```sql
CREATE TABLE system.jobs (
    id               INT DEFAULT unique_rowid(),
    status           STRING NOT NULL,
    percentCompleted DECIMAL(5, 2),
    lastUpdated      TIMESTAMP DEFAULT now(),
    payload          BYTES,
    PRIMARY KEY (status, id)
)
```

Each job is identified by a unique `id`, which is assigned when the job is
created. Currently, this ID serves only to identify the job to the user, but
future SQL commands to e.g. abort running jobs will need this ID to
unambiguously specify the target job.

The `status` column reprents a state machine with `pending`, `running`,
`succeeded`, and `failed` states. Jobs are created in the `pending` state when
the job-creating query is accepted, move to the `running` state once work on the
job has actually begun, then move to a final state of `succeeded` or `failed`.
Once a job has entered a final state, it becomes immutable. The `pending` state
warrants additional explanation: it's used to track jobs that are enqueued but
not currently performing work. Schema changes, for example will sit in the
`pending` state until all prior schema change jobs have completed.

The `percentCompleted` field, as expected, starts as `NULL` and is periodically
updated from 0 to 100 while the job is `running`. Jobs in the `succeeded` state
will always have a `percentCompleted` of 100, while jobs in the `failed` state
may have any `percentCompleted` value. This value is stored as a decimal with a
scale of two to allow tracking sub-percent progress on large jobs.

The `lastUpdated` field is updated to the current time whenever any change is
made to the record.

We want to avoid future schema changes to `system.jobs` if at all possible.
Every schema change requires a cluster migration, and every cluster migration
introduces node startup time overhead, plus risk and complexity. To that end,
fields that are not fundamental to the schema are stashed in the `payload`
column, which stores a protobuf that can be evolved per the usual protobuf
forwards-compatibility support. A potential structure of the `payload` follows.

```protobuf
message BackupJobPayload {
    string to = 1;
}

message RestoreJobPayload {
    string from = 1;
}

message SchemaChangeJobPayload {
    uint32 mutation_id = 1;
}

message JobPayload {
    string query = 1;
    string creator = 2;
    int64 created = 3;
    int64 started = 4;
    repeated uint32 target_id = 5;
    string error = 6;
    oneof details {
        BackupJobPayload backup_details = 7;
        RestoreJobPayload restore_details = 8;
        SchemaChangeJobPayload schema_change_details = 9;
    }
}
```

The `query` field stores the raw text of the job-creating query for display in
the UI.

The `creator` field records the user who launched the job. It's not clear
whether this field is necessary; see [Unresolved
questions](#unresolved-questions) below.

Next up are two fields to track the timing of the job. The `created` field
tracks when the job-creating query was executed. The `started` field tracks when
the job switches from `pending` to `running`. These fields, combined with the
`lastUpdated` field in the table itself, allow us to compute the duration of the
job (`lastUpdated` - `started`) and the delay in starting the job (`started` -
`created`). Every job that is `pending` will have a null `started` column, while
every job that is not `pending` will have a non-null `started` column. It would
be possible to instead use the same status for both `pending` and `running`,
allowing the presence of the `started` column to distinguish between the two
states, but the design as presented is more obvious.

The repeated `target_id` field stores the IDs of the databases or tables
affected by the job. For backups and restores, the IDs of any tables targeted
will have an entry. For schema migrations, the ID of the one database (`ALTER
DATABASE...`) or table (`ALTER TABLE...`) under modification will be stored.
Future long-running jobs which don't operate on databases or tables can simply
leave this field empty.

The `error` field stores the reason for failure, if any. This is the same error
message that is reported to the user through the normal query failure path, but
is recorded in the table for posterity.

Finally, the type of job can be determined by reflection on the `details` oneof,
which stores additional details relevant to a specific job type. The
`SchemaJobPayload`, for example, stores the ID of the underlying mutation.

Which fields belong in the table proper and which fields belong in the protobuf
are not entirely clear. See [Alternatives](#alternatives) and [Unresolved
questions](#unresolved-questions).

## Expected queries

To help evaluate which fields belong in the `payload` protobuf, following is a
selection of SQL queries expected to be run against the `system.jobs` table.
Most of these queries will be executed by the database internals, though some
are expected to be run manually by users monitoring job progress.

To create a new job:

```sql
-- {} is imaginary syntax for a protobuf literal.
INSERT
  INTO system.jobs (jobType, status, payload)
  VALUES ('backup', 'pending', {
    query = 'BACKUP foodb TO barstorage',
    creator = 'root',
    created = now(),
    backup_details = {
      to = 'barstorage',
      targets = ['foodb.tbla', 'foodb.tblb']
    }
  })
  RETURNING id
```

To mark a job as running:

```sql
-- {...old, col = 'new-value' } is imaginary syntax for a protobuf literal that
-- has the same values as old, except col is updated to 'new-value'.
UPDATE system.jobs
  SET status = 'running',
      percentCompleted = 0,
      lastUpdated = now(),
      payload = {...payload, started = now()}
```

To update the status of a running job:

```sql
UPDATE system.jobs
  SET percentCompleted = 24.42,
      lastUpdated = now()
```

To mark a job as successful:

```sql
UPDATE system.jobs
  SET percentCompleted = 100,
      status = 'succeeded',
      lastUpdated = now()
```

To mark a job as failed:

```sql
UPDATE system.jobs
  SET status = 'failed',
      lastUpdated = now(),
      payload = {...payload, error = 's3.aws.amazon.com: host unreachable'}
```

To find queued or running jobs (e.g., for the default "System jobs" admin view):

```sql
SELECT * FROM system.jobs WHERE status IN ('pending', 'running');
```

In the above query, the admin interface will need to extract the `created` field
from the `payload` protobuf in from each record in order to sort the records
sensibly. It is currently impossible to do the sorting in the SQL CLI directly.
(Similarly, it is not possible to limit the query to only jobs of a certain type
without decoding the protobuf.)

Under the proposed design, though, it's possible to retrieve the crucial
progress information for a job with a known ID from the SQL CLI:

```sql
SELECT status, percentCompleted FROM system.jobs WHERE id = ?;
```

We currently have a customer requesting the ability to do at least the above
from SQL. Additionally, support for reaching into protobuf columns from a SQL
query is planned.

## Minor considerations

- Cloud storage credentials can be passed in the query for `BACKUP` and
  `RESTORE` commands. Care will be taken to strip those credentials from the
  query text before the text is inserted into the `system.jobs` table.

- Unlike past proposals, the schema intentionally lacks a `name` or
  `description` column. As it stands, none of the job-creating queries allow the
  user to specify such a name or description.

# Drawbacks

Requiring the job leader to periodically issue `UPDATE system.jobs SET
percentCompleted = ...` queries to update the progress of running jobs is
somewhat unsatisfying. One wishes to be able to conjure the `percentCompleted`
column only when the record is read, but this design introduces significant
implementation complexity.

# Alternatives

## Wider protobufs

To further minimize the chances that we'll need to modify the `system.jobs`
schema, we could instead stuff all the data into the `payload` protobuf:

```sql
CREATE TABLE system.jobs (
    id      INT PRIMARY KEY,
    jobType STRING NOT NULL,
    payload BYTES,
)
```

This allows for complete flexibility in adjusting the schema, but prevents
essentially all useful SQL queries over the table until protobuf columns are
natively supported.

## Narrow protobufs

We could also allow all data to be filtered by widening the `system.jobs` table
to include some (or all) of the fields proposed to be stored in the `payload`
protobuf. Following is an example where all but the job-specific fields are
pulled out of `payload`.

```sql
CREATE TABLE system.jobs (
    id                INT DEFAULT unique_rowid() PRIMARY KEY,
    status            STRING NOT NULL,
    percentCompleted  DECIMAL(5, 2),
    payload           BYTES,
    query             STRING,
    creator           STRING,
    created           TIMESTAMP NOT NULL,
    started           TIMESTAMP,
    lastUpdated       TIMESTAMP NOT NULL,
    error             STRING,
    payload           STRING,
    INDEX status_idx  (status)
)
```

The `payload` type then simplifies to the below definition, where the
job-specific message types are defined as above.

```protobuf
message JobPayload {
    oneof details {
        BackupJobPayload backup_details = 1;
        RestoreJobPayload restore_details = 2;
        SchemaChangeJobPayload schema_change_details = 3;
    }
}
```

This alternative poses a significant risk if we need to adjust the schema, but
unlocks or simplifies some useful SQL queries. Retrieving the second page of
completed backup jobs can be expressed very naturally, for example:

```sql
SELECT * FROM system.jobs
  WHERE jobType = 'backup'
    AND status IN ('succeeded', 'failed')
    AND created < ?lastSeenCreated
  ORDER BY created
  LIMIT 50
```

Additionally, none of the `UPDATE` queries in the [Expected
queries](#expected-queries) will need to modify the protobuf in this alternative.

## Alternative indexing scheme

The proposed schema creates a primary index on `(status, id)`. This speeds up
admin UI queries to display queued or running jobs by filtering out a
potentially unbounded number of completed jobs. This does, however, mean queries
for a specific job ID (`SELECT status, percentCompleted WHERE jobId = ?`)
executed by e.g. the user from the SQL CLI can't use the index. We could instead
have a primary index on ID to speed up this use case and install a secondary
index on status for the admin UI's use case.

```sql
CREATE TABLE system.jobs (
    id               INT DEFAULT unique_rowid() PRIMARY KEY,
--- ...
    INDEX status_idx (status)
)
```

# Unresolved questions

First, two questions about the alternatives discussed above, which have broad
design implications:

- How aggressive should we be about putting things in the protobuf? That is,
  should we favor either the "Wider protobufs" or "Narrower protobufs" described
  above over the current middle-of-the road approach? See the [expected SQL
  queries](#expected-queries) above for guidance.

- Which indexing scheme should we use?

Second, several specific questions about fields in the schema:

- Should the `id` column have type `UUID DEFAULT uuid_v4()` instead of `INT DEFAULT
  unique_rowid()`? The `eventlog` and `rangelog` tables use the former style, while
  all other system tables use the latter.

- Is `DECIMAL(5, 2)` the right choice of data type for the `percentCompleted`
  column? Other options considered were `FLOAT` and `INT`.

- Is `creator` a useful field?

- None of the existing system tables have indices. Is this an accident
  of fate or are indices on system tables a bad idea?

Finally, one forward-looking question:

- How does a user get a job ID via SQL? Job-creating queries currently block
  until the job completes; this behavior is consistent with e.g. `ALTER...`
  queries in other databases. This means, however, that a job-creating query
  cannot return a job ID immediately. Users will need to search the
  `system.jobs` table manually for the record that matches the query they ran.

The answer to this question is unlikely to influence the design of the schema
itself. How we communicate the job ID to the user seems orthogonal to how we
keep track of job progress.


[#7037]: https://github.com/cockroachdb/cockroach/pull/7073
[#11722]: https://github.com/cockroachdb/cockroach/pull/11722
[#12555]: https://github.com/cockroachdb/cockroach/issues/12555
[@a-robinson]: https://github.com/a-robinson
[@danhhz]: https://github.com/danhhz
[@vivekmenezes]: https://github.com/vivekmenezes
[cluster migration framework]: https://github.com/cockroachdb/cockroach/pull/11658
[eventType]:https://github.com/cockroachdb/cockroach/blob/f7d5b1a5d0b8043083d943a533e20fc591641684/pkg/sql/sqlbase/system.go#L311