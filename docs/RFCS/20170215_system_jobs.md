- Feature Name: System Jobs
- Status: completed
- Start Date: 2017-02-13
- Authors: Nikhil Benesch
- RFC PR: [#13656]
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

Additionally, while each node runs a schema change daemon that can restart
pending schema changes if the coordinating node dies, the same is not true for
backups and restores. If the coordinating node for a backup or restore job dies, the
job will abort, even if the individual workers were otherwise successful.

This RFC proposes a new system table, `system.jobs`, that tracks the status of
these long-running backup, restore, and schema change "jobs." This table will
directly expose the desired progress information via `SELECT` queries over the
table, enable an API endpoint to expose the same progress information in the
admin UI, and enable an internal daemon that periodically adopts and resumes all
types of orphaned jobs. The table will also serve as a convenient place for
schema changes to store checkpoint state; currently, checkpoints are stored on
the affected table descriptor, which must be gossiped on every write.

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
    id      INT DEFAULT unique_rowid() PRIMARY KEY,
    status  STRING NOT NULL,
    created TIMESTAMP NOT NULL DEFAULT now(),
    payload BYTES,
    INDEX   (status, created)
)
```

Each job is identified by a unique `id`, which is assigned when the job is
created. Currently, this ID serves only to identify the job to the user, but
future SQL commands to e.g. abort running jobs will need this ID to
unambiguously specify the target job.

The `status` column represents a state machine with `pending`, `running`,
`succeeded`, and `failed` states. Jobs are created in the `pending` state when
the job-creating query is accepted, move to the `running` state once work on the
job has actually begun, then move to a final state of `succeeded` or `failed`.
The `pending` state warrants additional explanation: it's used to track jobs
that are enqueued but not currently performing work. Schema changes, for
example, will sit in the `pending` state until all prior schema change jobs have
completed.

The `created` field, unsurprisingly, is set to the current timestamp at the time
the record is created.

The admin UI job status page is expected to display jobs ordered first by their
status, then by their creation time. To make this query efficient, the table has
a secondary index on `status, created`.

We want to avoid future schema changes to `system.jobs` if at all possible.
Every schema change requires a cluster migration, and every cluster migration
introduces node startup time overhead, plus some risk and complexity. To that
end, any field not required by an index is stashed in the `payload` column,
which stores a protobuf that can be evolved per the standard protobuf
forwards-compatibility support. The proposed message definition for the
`payload` follows.

```protobuf
message BackupJobPayload {
    // Intentionally unspecified.
}

message RestoreJobPayload {
    // Intentionally unspecified.
}

message SchemaChangeJobPayload {
    uint32 mutation_id = 1;
    repeated roachpb.Span resume_spans = 2;
}

message JobLease {
    uint32 node_id = 1;
    int64 expires = 2;
}

message JobPayload {
    string description = 1;
    string creator = 2;
    int64 started = 4;
    int64 finished = 5;
    int64 modified = 6;
    repeated uint32 descriptor_ids = 7;
    float fraction_completed = 8;
    string error = 9;
    oneof details {
        BackupJobPayload backup_details = 10;
        RestoreJobPayload restore_details = 11;
        SchemaChangeJobPayload schema_change_details = 12;
    }
    JobLease lease = 13;
}
```

The `description` field stores the text of the job-creating query for display in
the UI. Schema changes will store the query verbatim; backups and restores,
which may have sensitive cloud storage credentials specified in the query, will
store a sanitized version of the query.

The `creator` field records the user who launched the job.

Next up are four fields to track the timing of the job. The `created` field
tracks when the job record is created, the `started` field tracks when the job
switches from `pending` to `running`, and the `finished` field tracks when the
job switches from `running` to its final state of `succeeded` or `failed`. The
`modified` field is updated whenever the job is updated and can be used to
detect when a job has stalled.

The repeated `descriptor_id` field stores the IDs of the databases or tables
affected by the job. For backups and restores, the IDs of any tables targeted
will have an entry. For schema migrations, the ID of the one database (`ALTER
DATABASE...`) or table (`ALTER TABLE...`) under modification will be stored.
Future long-running jobs which don't operate on databases or tables can simply
leave this field empty.

The `fraction_completed` field is periodically updated from 0.0 to 1.0 while the
job is `running`. Jobs in the `succeeded` state will always have a
`fraction_completed` of 1.0, while jobs in the `failed` state may have any
`fraction_completed` value. This value is stored as a float instead of an
integer to avoid needing to choose a fixed denominator for the fraction (e.g.
100 or 1000).

The `error` field stores the reason for failure, if any. This is the same error
message that is reported to the user through the normal query failure path, but
is recorded in the table for posterity.

The type of job can be determined by reflection on the `details` oneof, which
stores additional details relevant to a specific job type. The
`SchemaJobPayload`, for example, stores the ID of the underlying mutation and
checkpoint status to resume an in-progress backfill if the original coordinator
dies. `BackupJobPayload` and `RestoreJobPayload` are currently empty and exist
only to allow reflection on the `details` oneof.

Finally, the `lease` field tracks whether the job has a live coordinator. The
field stores the node ID of the current coordinator in `lease.node_id` and when
their lease expires in `lease.expires`. Each node will run a daemon to scan for
running jobs whose leases have expired and attempt to become the new
coordinator. (See the next section for a proposed lease acquisition scheme.)
Schema changes have an existing daemon that does exactly this, but the daemon
currently stores the lease information on the table descriptor. The daemon will
be adjusted to store lease information here instead and extended to support
backup and restore jobs.

Several alternative divisions of fields between the schema and the protobuf were
considered; see [Alternatives](#alternatives) for more discussion.

## Expected queries

To help evaluate the schema design, a selection of SQL queries expected to be
run against the `system.jobs` table follows. Most of these queries will be
executed by the database internals, though some are expected to be run manually
by users monitoring job progress.

To create a new job:

```sql
-- {} is imaginary syntax for a protobuf literal.
INSERT
    INTO system.jobs (status, payload)
    VALUES ('pending', {
        description = 'BACKUP foodb TO barstorage',
        creator = 'root',
        modified = now(),
        descriptor_ids = [50],
        backup_details = {}
    })
RETURNING id
```

To mark a job as running:

```sql
-- {...old, col = 'new-value' } is imaginary syntax for a protobuf literal that
-- has the same values as old, except col is updated to 'new-value'.
UPDATE system.jobs
    SET status = 'running',
        payload = {
            ...payload,
            started = now(),
            modified = now(),
            fraction_completed = 0.0,
            lease = { node_id = 1, expires = now() + JobLeaseDuration}
        }
    WHERE id = ?
```

To update the status of a running job:

```sql
UPDATE system.jobs
    SET payload = {...payload, modified = now(), fraction_completed = 0.2442}
    WHERE id = ?
```

To take over an expired lease:

```go
func maybeAcquireAbandonedJob() (int, JobPayload) {
    jobs = db.Query("SELECT id, payload FROM system.jobs WHERE status = 'running'")
    for _, job := range jobs {
        payload := decode(job.payload)
        if payload.lease.expires.Add(MaxClockOffset).Before(time.Now()) {
            payload.lease = &JobLease{NodeID: NODE-ID, Expires: time.Now().Add(JobLeaseDuration)}
            if db.Exec(
                "UPDATE payload = ? WHERE id = ? AND payload = ?",
                encode(payload), job.ID, job.payload,
            ).RowsAffected() == 1 {
                // Acquired the lease on this job.
                return job.id, payload
            }
            // Another node got the lease. Try the next job.
        }
        // This job still has an active lease. Try the next job.
    }
    return nil, nil
}
```

To mark a job as successful:

```sql
UPDATE system.jobs
    SET status = 'succeeded'
        payload = {...payload, modified = now()}
    WHERE id = ?
```

To mark a job as failed:

```sql
UPDATE system.jobs
    SET status = 'failed',
        payload = {...payload, modified = now(), error = 's3.aws.amazon.com: host unreachable'}
    WHERE id = ?
```

To find queued or running jobs (e.g., for the default "System jobs" admin view):

```sql
SELECT * FROM system.jobs WHERE status IN ('pending', 'running') ORDER BY created;
```

To get the status of a specific job (e.g., a user in the SQL CLI):

```sql
SELECT status FROM system.jobs WHERE id = ?;
```

# Drawbacks

- Requiring the job leader to periodically issue `UPDATE system.jobs SET payload
  = {...payload, fraction_completed = ?}` queries to update the progress of
  running jobs is somewhat unsatisfying. One wishes to be able to conjure the
  `fraction_completed` column only when the record is read, but this design
  would introduce significant implementation complexity.

- Users cannot retrieve fields stored in the protobuf from SQL directly, but
  several fields that might be useful to users, like `fraction_completed` and
  `creator`, are stored within the protobuf. We can solve this by introducing a
  special syntax, like `SHOW JOBS`, if the need arises. Additionally, support
  for reaching into protobuf columns from a SQL query is planned.

  Note that at least one current customer has requested the ability to query job
  status from SQL directly. Even without a `SHOW JOBS` command, basic status
  information (i.e., `pending`, `running`, `succeeded`, or `failed`) is
  available directly through SQL under this proposal.

# Alternatives

## Wider protobufs

To further minimize the chances that we'll need to modify the `system.jobs`
schema, we could instead stuff all the data into the `payload` protobuf:

```sql
CREATE TABLE system.jobs (
    id      INT DEFAULT unique_rowid() PRIMARY KEY,
    payload BYTES,
)
```

This allows for complete flexibility in adjusting the schema, but prevents
essentially all useful SQL queries and indices over the table until protobuf
columns are natively supported.

## Narrow protobufs

We could also allow all data to be filtered by widening the `system.jobs` table
to include some (or all) of the fields proposed to be stored in the `payload`
protobuf. Following is an example where all but the job-specific fields are
pulled out of `payload`.

```sql
CREATE TABLE system.jobs (
    id                INT DEFAULT unique_rowid() PRIMARY KEY,
    status            STRING NOT NULL,
    description       STRING NOT NULL,
    creator           STRING NOT NULL,
    nodeID            INT,
    created           TIMESTAMP NOT NULL DEFAULT now(),
    started           TIMESTAMP,
    finished          TIMESTAMP,
    modified          TIMESTAMP NOT NULL DEFAULT now(),
    descriptors       INT[],
    fractionCompleted FLOAT,
    INDEX (status, created)
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
unlocks or simplifies some useful SQL queries. Additionally, none of the
`UPDATE` queries in the [Expected queries](#expected-queries) would need to
modify the protobuf in this alternative.

## Tracking job update history

Also considered was a schema capable of recording every change made to a job.
Each job, then, would consist of a collection of records in the `system.jobs`
table, one row per update. The table schema would include a timestamp on
every row, and the primary key would expand to `id, timestamp`:

```sql
CREATE TABLE system.jobs (
    id        INT DEFAULT unique_rowid(),
    timestamp TIMESTAMP NOT NULL DEFAULT now()
    status    STRING NOT NULL,
    payload   BYTES,
    PRIMARY KEY (id, timestamp)
)
```

The `created`, `started`, and `finished` fields could then be derived from the
`timestamp` of the first record in the new state, so the protobuf would simplify
to:

```protobuf
message JobPayload {
    string description = 1;
    string creator = 2;
    float fraction_completed = ?;
    uint32 node_id = 2;
    repeated uint32 descriptor_id = 6;
    string error = 7;
    oneof details {
        BackupJobPayload backup_details = 8;
        RestoreJobPayload restore_details = 9;
        SchemaChangeJobPayload schema_change_details = 10;
    }
}
```

The first entry into the table for a given job would include the immutable facts
about the job, like the `description` and the `creator`. Future updates to a job
would only include the updated fields in the protobuf. A running job would
update `fraction_completed` in the usual case, for example, and would update
`node_id` if the coordinating node changed.

Protobufs elide omitted fields, so the space requirement of such a scheme is a
modest several dozen kilobytes per job, assuming each job is updated several
thousand times. Unfortunately, this design drastically complicates the queries
necessary to retrieve information from the table. For example, the admin UI
would need something like the following to display the list of in-progress and
running jobs:

```sql
SELECT
    latest.id AS id,
    latest.status AS status,
    latest.timestamp AS updated,
    latest.payload AS latestPayload,
    initial.payload AS initialPayload,
    initial.timestamp AS created
FROM (
    SELECT jobs.id, jobs.timestamp, jobs.status, jobs.payload
    FROM (SELECT id, max(timestamp) as timestamp FROM jobs GROUP BY id) AS latest
    JOIN jobs ON jobs.id = latest.id AND jobs.timestamp = latest.timestamp
) AS latest
JOIN jobs AS initial ON initial.id = latest.id AND initial.status = 'pending'
WHERE latest.status IN ('pending', 'running')
ORDER BY initial.timestamp
```

The above query could be simplified if we instead reproduced the entire record
with every update, but that would significantly increase the space requirement.
In short, this alternative either complicates implementation or incurs
significant space overhead for no clear win, since we don't currently have a
specific compelling use case for a full update history.

# Unresolved questions and future work

- How does a user get a job ID via SQL? Job-creating queries currently block
  until the job completes; this behavior is consistent with e.g. `ALTER...`
  queries in other databases. This means, however, that a job-creating query
  cannot return a job ID immediately. Users will need to search the
  `system.jobs` table manually for the record that matches the query they ran.
  The answer to this question is unlikely to influence the design of the schema
  itself, since how we communicate the job ID to the user is orthogonal to how
  we keep track of job progress.

- All system log tables, including `system.jobs`, will eventually need garbage
  collection to prune old entries from the table, likely with a
  user-configurable timeframe.

[#7037]: https://github.com/cockroachdb/cockroach/pull/7073
[#11722]: https://github.com/cockroachdb/cockroach/pull/11722
[#12555]: https://github.com/cockroachdb/cockroach/issues/12555
[#13656]: https://github.com/cockroachdb/cockroach/pull/13656
[@a-robinson]: https://github.com/a-robinson
[@danhhz]: https://github.com/danhhz
[@vivekmenezes]: https://github.com/vivekmenezes
[cluster migration framework]: https://github.com/cockroachdb/cockroach/pull/11658
