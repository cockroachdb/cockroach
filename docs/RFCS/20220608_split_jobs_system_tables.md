- Feature Name: Splitting Up the Jobs System Tables
- Status: draft
- Start Date: 2022-06-08
- Authors: David Taylor, Steven Danna, Yevgeniy Miretskiy
- RFC PR: #82638
- Cockroach Issue: #122687 and others.

## Summary

The background job execution framework within CockroachDB currently stores its state and the state
of the jobs it runs in columns of the `system.jobs` table. 

This design has led to numerous operational problems in customer clusters, mostly stemming from
transaction contention and MVCC garbage accumulation in this single table that is serving multiple
needs. Replacing this single table with separate tables, each serving a more narrowly scoped
purpose, should significantly improve both the performance and reliability of the jobs system,
remove some scale limits, and add new capabilities.

## Motivation

Currently the jobs API allows using transactions to create and interact with a job, including when
the job itself wants to interact with its persisted job state. The transactional aspect of the Jobs
API is vital to some clients of the jobs system, such as schema changes which depend on
transactional job creation. However this choice has also introduced problems. The jobs system itself
relies on scanning the jobs table to show what jobs are currently running, to find jobs which need
to start running, and to find those which are running but need to be paused or cancelled. Further,
it is critically important that an operator facing a production incident be able to inspect what
jobs are running and pause or cancel one if it is suspected of being the cause of problems. However
today, those actions can be blocked by any job that has a slow or frequent transaction that has
locked one or more rows in the jobs table. 

The jobs system and its associated operator-facing tools should be resilient to the behaviour of any
individual job, but it is not today, and this has caused and continues to cause numerous production
issues.

To avoid such contention related issues, as well as other issues such as excessive MVCC garbage,
many jobs end up electing to store less state or updating it less often than they would like. Such
jobs may lose progress if paused or restarted, or are forced to hold more state in memory instead,
adding scale limits or risking OOMs. 

In addition to contention issues, if a job adds to much information to its current state record, or
updates it too often, the MVCC garbage associated with those revisions to its row in the jobs table
can cause the table to become unavailable because CockroachDB's KV layer is not permitted to split
between revisions of a single SQL row. Packing state, progress, and control all into one row leads
to larger rows, increasing the risk of this type of failure. While this is partially mitigated today
via careful organisation into column families, the large serialised protobuf messages favoured by
jobs to represent progress or arguments still often result in large job rows, which are typically
edited every 30-60sec to persist progress. Jobs should be able to store all the state that is useful
to them without running into limitations of the job system. They should not need to discard useful
state for fear of compromising the job control system itself, and the associated APIs offered to
jobs should make it easier to write jobs that do what their authors want to do without worrying
about adversely affecting the jobs system.

## Goals

1. The jobs system should allow operators to inspect and control the set of jobs, regardless of how
   transactions are used to create jobs or update the persisted state of any one job.
2. Jobs should be able to be created in a transaction, with typical transactional semantics, i.e. a
   job is only created and starts running if the transaction commits and minimal effect if it rolls
   back.
3. Jobs should be able to store as much persisted state as they want, and update it as much as they
   want, without negatively affecting other jobs or the jobs system health.
4. Operators should be able to inspect the recent history of a job's status or progress.

## Design Summary

Most of the problems with the current jobs system arise from the fact that different users, with
different needs, are sharing the single system.jobs table. To resolve these issues we propose
breaking this single table up into multiple tables that each serve specific, narrower purposes.

The current system.jobs table will be split into five separate tables: 

1. `system.jobs` tracks the set of jobs, what step they need to take and coordinates which
   nodes will do so. I
2. `system.job_progress` and `system.job_status` record human-consumed progress and
   status changes
3. `system.job_info` is used by the implementation of individual jobs to record and retrieve their
   state and arguments
4. `system.job_intake` is used to sidestep creation transaction contention in the control table.

Each of these four tables serves a specific purpose and is described in more detail below, but the 
two most important aspects of this design are:

a) **the API will not allow transactional interactions with system.jobs which could block**, and

b) **there may be many rows per job in job_info. **

This change highly severable, allowing it to be implemented gradually: The transition to storing 
extended mutable jobs-type-specific information in job_info will take write load and some contention
off the control table with or without the implementation of the status tables or intake table; Each 
step is expected yield its own marginal returns. The introduction of the intake queue is explicitly 
left as future work at this time.

## Detailed Design

### system.jobs

#### Description

The system.jobs table stores the core state of the jobs system; it defines what jobs are
currently running, have run, need to start running or need to stop running. 

This table is _exclusively_ used by the jobs system itself; no user-controlled or job-controlled
transactions are allowed in this table. This avoids a misbehaving job or user from blocking the jobs
system and its ability to continue to manage other jobs.

Job-controlled transactions have historically shown two main forms of misbehaving in this table:
taking too long to commit updates, or committing large updates too often, with both causing
contention and the latter causing accumulation of mvcc garbage. Both of these are mitigated by
providing jobs a separate table described below in which to read and write its own information --
using transactions that block for as long as it wishes and spread across many small rows if needed
-- leaving this control table to be only those fields needed by the job control system itself.

Thus this table only contains fields common to all jobs, not job specific protos or arguments. It
specifies that a particular job ID, with a particular type, needs to be started, stopped, etc. The
jobs system then uses that information to find the relevant implementing code -- i.e. the resumer
registered for that type -- and call the relevant method -- e.g. Resume, OnFailOrCancel, etc --
informing it of the ID it should be resuming/cancelling/etc. The job’s implementation of those
methods are then responsible for retrieving any job-specific details, such as which files it is
importing or what tables it is backing up to where, by looking for the applicable rows it or its
planning placed in the job_info table.

While this table specifies the _type_ of each job, it does not store any job-type specific
information; i.e. while it indicates that job 123 is a BACKUP job and is ready to run, it does _not_
indicate what job 123 is backing up or how much it has completed; that information must be stored
elsewhere, as it is often determined or mutated using explicit transactions. 

#### Schema
This is an existing table that already has some number of legacy columns. For the purposes of this
document, it is the existing jobs table, extended with human-readable description, summary, owner
and various timestamps that appear in SHOW.

#### Job Control Statements and Their Transactions

User-facing job control statements will result in transactions in this table, such as to update a
running job's status to pause-requested. This matches the current behavior with respect to writing a
pause-request to the current system.jobs, however the goal of eliminating all blocking transactions
from the new control table suggests that such control statements should be updated to no longer be
allowed in explicit, user-owned transactions. In many ways, this added restriction should have been
there already: running `PAUSE JOB X` inside a transaction can be confusing, because it does not
actually pause the running job until it is committed, at which time a pause-request is written, then
at some later time observed, and only then does the job itself move to paused. However if adding
this new restriction proves unpalatable, options include either simply allowing these blocking
transactions and the fact they block the jobs system --  if a user has indicated they will pause a
job they need to either commit that fact or abort it before we can answer if the job is paused or
not -- or we can introduce additional queued operation tables similar to the intake table described
below. 


###### Upgrade Path for system.jobs
New columns are added and we dual-write the legacy fields and the new ones until the upgrade is 
complete.

---

### system.job_progress + system.job_status

#### Description

The job_progress and system.job_statuss tables store human-readable information about
a job's current and recent status and progress.

These tables are not read by the jobs control system at any point, and should not be read by the job
itself either; they are intended solely for human-consumption via SHOW JOBS and the DB Console. They
are separate from the control row so that the control row does not need to be updated as the job 
executes and updates its progress or status information.

Both tables have many rows per job: Each time a job updates its persisted status or progress it will
insert a new row into the respective table with the new value, and inspection tools will scan for
the latest row per job to find its current status/progress. 

Both tables have the same primary key: job ID plus row creation time. However they are separate as
their payloads are both of very different size -- a small number versus a large string -- and are
updated at very different frequencies; a completion percentage or timestamp likely updates
continuously for the entirely life of a job while execution status may change a fixed number of
times per execution phase.

#### Schema

```
    CREATE TABLE system.job_progress (
    	job_id INT NOT NULL,
    	written TIMESTAMPTZ,
     	fraction FLOAT, # null if job does not have a defined completion percentage
    	resolved DECIMAL, # set only for jobs that advance in time e.g. cdc or replication
    	PRIMARY KEY (job_id, last_updated DESC)
    )
    CREATE TABLE system.job_status (
    	job_id INT NOT NULL,
    	written TIMESTAMPTZ,
    	kind STRING, # running_status, state_change, etc.
    	message STRING, # human-readable information, like "waiting for leases"
    	PRIMARY KEY (job_id, last_updated DESC)
    )
```



#### SHOW JOBS

The job control table, along with these tables plus the intake table, defines the set of jobs the
system is running, will run or has has failed or creased running, along with human-readable
information about them, such as their description, percentage complete, or recent status messages;
taken together, these are what an operator is looking for when they ask "what jobs are my cluster running?
How complete are the job I was running? Did they fail and if so why?". This interaction is fulfilled
by SHOW JOBS and the jobs page on the DB console. Both of these are powered by an internal virtual
table crdb_internal.jobs, and will continue to be going forward, however this table will now be
reading columns only from the control table and the status tables (plus the intake table). Rendering
the virtual table will _not_ require decoding large proto payloads, that also contain large amounts
of job-specific state, nor will it block on transactions that are manipulating that state, as the
virtual table will not depend on the info table, described below, where that state now resides and
is manipulated.

In contrast, job-specific detail inspection, such as SHOW BACKUP JOB or similar code called from the
per-job DB console page, will delegate to job-specific code that can in turn determine which keys
from the info table it should fetch and render, to show more detailed, job-type-specific
information. However for the common inspection tools of SHOW JOBS and the DB Console that show an
overview of many different types of jobs at once will exclusively be iterating over these tables
where non-control transactions are forbidden and all fields are native SQL columns, not large nested
protocol buffer messages, reduce the risk of SHOW JOBSs hanging or consuming excessive memory.

SHOW JOBS will additionally need to union rows in the intake table described below, using SKIP
LOCKED, as they are committed runnable jobs that just have not yet been moved into the control
table.


#### Alternatives considered


###### Status and progress columns in a single row that are updated in place

If updates to human-readable progress and status information were done as in-place revisions rather
than inserting new rows and optionally deleting older rows for that job, this could either be a
single status table, or just columns in the control table, using separate column families. However
this has two drawbacks: first, run-away updates to status can produce an excessive number of
revisions to a single row. Of course, with row-per-revision such cases produce excessive rows
instead, but a key difference between these is that a KV range is allowed to split between rows but
not between revisions of a row. 

Making the system resilient to accidental excessive revisions is one motivation for essentially
re-implementing MVCC on top of SQL rows. This concern, while certainly motivated by past experience
with jobs producing excessive MVCC garbage to due to frequent update to persisted progress, may be
mitigated here thanks to the rows being quite limited in size: unlike the current jobs table where
the fraction complete can share a column family with a larger encoded proto of persisted job state,
these rows would be very small. Additionally rate-limiting updates have mostly proven successful in
avoiding this class of issue. 

But another, and perhaps stronger motivation for this additional layer of MVCC is making history
accessible to the jobs inspection tools: what rate has progress advanced in the last 5 or 10
minutes? What was the previous status message and when did it change? The underlying KV MVCC support
does not currently allow such inspection by SQL users, and even if it did, would be tightly coupled
to its retention configuration. If we want to offer status history inspection, keeping revisions to
status as separate rows seems like a prerequisite.

When writing a new row for every update, we have to write a _whole_ new row to change any field of 
that row. This is unfortunate if we have a large (1kb+) string message that changes rarely in the 
same row as a small, often updated completed fraction. In the updated-in-place table, these could be
placed in separate column families, however in the new-row-per-update table, that is not an option.
Instead, we split these two fields -- the rarely changed, large message and the often changed, small
float -- into separate tables.

---

### system.job_info

#### Description

The system.job_info table is used by individual jobs to store their arguments, and progress
information and any other mutable state. 

This table stores many rows per job ID, allowing breaking down a large amount of state into many,
separately updatable records. It is up to the individual job’s implementation to choose what they
store and how they store it, including how they pick keys under which to store the individual
fragments of information they need to persist. For example, the planning phase of an IMPORT job
might store an info row "inputs" specifying the URLs it will later read from; during execution it
might read that row back to then go open those files and begin reading, then update
"progress/&lt;fileID>" rows for each file as it processes. 

Jobs can interact with their rows in this table using transactions without fear of negatively
affecting other jobs as no part of the jobs control or inspection system relies on scanning this
table directly, although some detailed inspection tools may delegate to per-job-type code that does
fetch and process information from this table to render results. 

While no scans will be happening on this table by any common code of code belonging to another job,
it is still hard to debug issues that occur in the current jobs system that arise from excessive row
sizes; Helpers for writing info -- which is often serialized protobuf and can thus sometimes become 
large  if some field or nested field is unexpectedly large -- should reject writes of rows that are
too large (e.g. >32mb) to prevent a problematic row from entering this table in the first place and
provide an immediate error message that directly identifies the source of the large attempted write.

#### Schema

```
    CREATE TABLE system.job_info (
    	job_id INT NOT NULL,
    	info_id STRING NOT NULL,
    	written TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    	value BYTES NOT NULL,
    	PRIMARY KEY (job_id, info_id, written DESC)
    )
```

#### Timestamped Keys (MVCC-over-SQL)

Similar to the job status tables, the info table uses a new row for each revision of a given named
info key, rather than allowing updates to an underlying row in-place.

This is motivated by past experience with jobs the jobs table becoming unavailable after a job's
repeated updates to a single larger row generated excessive MVCC garbage.The KV layer is unable to
split between revisions to a single SQL row, so large rows revised in-place can easily result in
over-full range that eventually becomes unavailable: a 500kb row can only be revised 1024 times
before the MVCC garbage exceeds the 512mb range-size threshold, which would take under nine hours at
just two updates a minute. While one goal of this info table is to provide an easier mechanism for
jobs to easily store a larger number of smaller rows instead of updating a single large payload,
given that jobs are likely to start to store encoded proto messages in rows in this table, it should
be expected that those could become large; again, past experience says that a typically small error
message may unexpectedly be an entire http response payload, or a normally small map may become
giant, and such cases should degrade gracefully rather than make the jobs tables unavailable.

Thus, like the status tables above, updates to keys in this info table will instead be an insert of
the new info paired with deletion of any older revision of it. This case is actually simpler than
the status case as no history is required and the addition of MVCC-like behaviour is solely for
making past revisions splittable, so each insert can delete all prior revisions.

#### Mutual Exclusion of Updates

Updates to system.job_info should, by default, only be allowed from processors that were started as
part of the currently running instance of the job. The API for updating the system.job_info,
system.job_progress, and system.job_status tables will require the current
claim_session_id for the job.  This session ID will be passed from the job system to the job
implementation’s relevant methods (e.g. Resume). Jobs that wish to update job info from multiple
nodes will need to pass the session ID to the relevant processors on those nodes.

Updates will be allowed so long as the session specified by the session ID is still alive:

```
INSERT INTO system.jobs_info SELECT $1, $3, NOW(), $4 WHERE crdb_internal.sql_liveness_is_alive($2) IS TRUE
```

Since jobs are only eligible for resumption once the existing session ID returns false from
crdb_internal.sql_liveness_is_alive, such a condition should ensure that we are not updating the
job_info  table while another instance of the job is running without going to the jobs_control
table.

##### Alternatives options for Update Exclusion 

We could allow a limited violation of our invariant and do a join on the system.jobs_control table:

```
INSERT INTO system.jobs_info SELECT id, $3, NOW(), $4 FROM system.jobs_control WHERE id = $2 AND claim_session_id = $2;
```

While this would be touching the jobs_control table, it would be a point read on a single row and
would not contend with scans of the jobs table. If testing proves contention-free at scale, we may
prefer this implementation.

We also considered using the session_id to look up the current expiration of the session, and
setting a transaction deadline of our insert to the expiration of the session since we know the
session is valid through that expiration. This is what the span configuration reconciler currently
does. 


#### Upgrade Path for system.jobs-stored Job Info

In the current unified job control/info/etc system  jobs system table, "Payload"/"Details" and
"Progress" are monolithic protos, stored in columns along side control information. For the
migration, calls to functions that set them, such as SetDetails(), can start to dual-write, updating
both the current column as well as writing to a well known key in the new info table, e.g.
"legacy_details".  Using cluster versions, once this is happening on all nodes, a migration job can
then read all current jobs from the existing table to populate these rows in the info table instead.
Once that migration has finished, the post-migration cluster version can be used by calls to get the
details or progress to load from the info table instead. Existing job implementations do not need to
actively change to use the new per-info read and write methods unless they want to take advantage of
being able to track multiple separate pieces of info.

---

## Future work: Contentionless Job Creation

Note: This section is speculation as to the nature of future work with the
goal of eliminating all user-controlled transactions from the primary job table.
The approach outlined in this section requires further review and testing via
a prototype and load generator. Initial implementation work is not expected to 
include the ingestion queue described here, and will continue to allow user-
controlled transactions related to job creation to write and hold locks directly
in the core jobs table. 

### system.job_intake

#### Description

The job_intake table is how new jobs _enter_ the jobs system. 

Job creators can use a transaction of their own, that can block on other operations, to create new
jobs in this intake table, while keeping that transaction's locks _out_ of the core system.jobs
table, where it would interfere with core jobs system operations.

Once a job is committed to this intake table, it is then found by the jobs system (using SKIP LOCKED
and/or via rangefeeds so that still uncommitted jobs do not block this intake process), copied into
the job control table and deleted from this intake table. 

The jobs system can use SKIP LOCKED in this way on this table to safely find just new jobs, since
jobs are never modified after being committed in this table, the only rows skipped by such a SKIP
LOCKED query are only new row or those being processed. This differs from the core job control table
where jobs are modified in-place, and are thus locked during modification, meaning skipping locked
rows would skip existing rows incorrectly. Thus this separate table, where jobs are only created and
then immediately moved after they're committed, is uniquely safe to read with SKIP LOCKED for this
specific purpose.

#### Schema

```
CREATE TABLE system.job_intake (
	id INT PRIMARY KEY,
	job_type		STRING, 
	description 	STRING, 		# human-readable description
	created 		TIMESTAMPTZ,
	created_by_type	STRING,
	created_by_id	INT
)
```



#### Job Ingestion

The simplest mechanism to intake jobs would be a loop, running on every node, which simply reads and
deletes incoming jobs from intake and writes them to the job control table, along the lines of:

```
BEGIN;
DELETE FROM system.job_intake
WHERE id IN (
    SELECT * FROM system.job_intake
    FOR UPDATE SKIP LOCKED LIMIT 16;
) RETURNING *;

INSERT INTO system.jobs VALUES (...), (...),(...);
COMMIT;
```

However, in the steady state, the job system can have an established rangefeed on the
system.job_intake table to immediately see newly committed rows instead of polling in a loop.  Since
rangefeeds only receive a row once it is committed, they provide semantics similar to SKIP LOCKED
which we need -- seeing committed rows while not blocking on uncommitted rows -- and would thus
allow us to avoid polling the table:

```
BEGIN;
DELETE FROM system.job_intake WHERE id = <ID from rangefeed event> RETURNING …;
INSERT INTO system.jobs VALUES (...);
COMMIT;
```

The rangefeed's initial scan could also provide an alternative to the skip-locked query for bringing
in existing rows during startup. 

To avoid unnecessary duplication of work in large clusters, we may need to restrict the job
ingestion process to a subset of nodes in the cluster or introduce delays or jitter and then use a
signal from the job creator to its local job registry to avoid that delay or jitter in the default
case where the creating node does not immediately crash.

Additionally, it is possible this intake process could be fused with the adoption process to
optimize it further however this is left to follow-up work.
