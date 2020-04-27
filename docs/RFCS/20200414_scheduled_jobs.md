 Feature Name: Scheduled Jobs
- Status: WIP -- not ready for review.
- Start Date: 2020-04-14
- Authors: Yevgeniy Miretkiy, David Taylor
- RFC PR: 48131
- Cockroach Issue: 47223


# Summary

Scheduled jobs adds an ability to execute scheduled, periodic jobs to CockroachDB.

# Motivation

Database administrators have a need to execute certain jobs periodically or at fixed times.
  * Run daily or hourly backups
  * Run import job daily (for example to add data to development database) 
  or refresh externally provided datasets
  * Produce monthly reports
  * Export data to reporting or analytics pipelines

Users have also expressed desire to execute a one-off job 
(for example: administrator creates a temporary database for a user for 1 week;
one week from now -- drop this database).

Of course these types of activities are already possible by using external systems.

However, the external-cron solution has few deficiencies:
  * Our "make easy" ethos means wherever possible we bundle dependencies 
    (config, orchestration, etc) into the system.  Ideally no separate services 
    should be required to operate CockroachDB, including with our recommended
    cadence of periodic backups. 
  * It is hard to maintain external cron mechanism particularly in an HA deployment or 
    in a shared environment (i.e. cockroach cloud, or on-prem shared database 
    where the administrator may want to control how often each database gets backed up)
  * It is more difficult to have the external system take into account
    the current conditions of the database (load for example) when 
    scheduling jobs.

# Detailed design

## System Requirements

The primary motivation for adding scheduled jobs system is to support the important
case of _making backups easy_.  While backups are the primary
driver, we want the scheduled jobs to be flexible in order to support other use cases.

The scheduled jobs goals are:
* Support running full and periodic backups in an easy and intuitive way.
* Support flexible execution policies:
    * Periodic execution
    * Configurable error handling (e.g. start next run if previous failed or not)
* Support user/role delegation when running jobs and tasks (V2 probably)
* Scheduled jobs must be resilient in a distributed environment
    * Ensure the system spawns one instance of the task
    * Must be resilient to node restarts/crashes
* Be scalable (particularly for virtualized cloud environment)

The non-goals are:
* Scheduled jobs cannot provide guarantees that the job will run at 
exactly the time specified in the job spec, though the system will try to do so
if it's possible.
* The scheduled jobs system is not responsible for actually _executing_ the job itself. 
That is still handled like any other job by our jobs system. 
It is responsible for pushing work into that system at the designated time and 
with the configured recurrence policy.


## Scheduled Job
A scheduled job is a task that runs at some time in the future.  The task may be periodic, 
or it may be a one-off task.  The task itself is defined as a “job type” and 
an opaque “job details”, which is an argument interpreted by the task runner for this type.
The task runner itself is something which plans and creates a "job" (entry in system.jobs) 
for execution by the existing jobs framework.

The initial implementation will provide one task runner type which evaluates SQL statements.
We will only support SQL statements that can be executed by the system jobs framework.
Currently, cockroach supports limited set of such statements (`BACKUP`, `RESTORE`, etc), 
but we plan on evaluating and possibly adding support for running any sql statement, 
wrapped as a system job, in future work.

## Scheduled Job Representation

The system stores scheduled job related information in a new `system.scheduled_jobs` table.
In addition, we add a new indexed column to the `system.jobs` table to correlate
system jobs to their schedule.  

First, we'll define various protocol messages used to represent cron job,
then, we'll describe the changes to the [system](system-database-changes) database
in more detail. 

#### Scheduled Job Protocol Messages

`ScheduledJobArguments` protocol message describes the job that needs
to be executed:

```proto
message ScheduledJobArguments {
  string sql = 1;
}
```

Currently, the job is simply an SQL statement to execute (to be extended
in future versions)

The `ScheduleDetails` describes how to schedule this job:
```proto
message ScheduleDetails {
  // WaitBehavior describes how to handle previously  started
  // jobs that have not completed yet.
  enum WaitBehavior {
    // Wait for the previous run to complete
    // before starting the next one.
    WAIT = 0;
    // Do not wait for the previous run to complete.
    NO_WAIT = 1;
    // If the previous run is still running, skip this run
    // and advance schedule to the next recurrence.
    SKIP = 2;
  }

  // ErrorHandlingBehavior describes how to handle failed job runs.
  enum ErrorHandlingBehavior {
    // By default, failed jobs will run again, based on their schedule.
    RETRY_SCHED = 0;
    // Retry failed jobs soon.
    RETRY_SOON = 1;
    // Stop running this schedule
    PAUSE_SCHED = 2;
  }

  // How to handle running jobs.
  WaitBehavior wait = 1;

  // How to handle failed jobs.
  ErrorHandlingBehavior on_error = 2;
}
```

The default scheduling policy is:
  * Wait for the outstanding job to finish before starting the next one
    (`wait == WaitBehavior.WAIT`)
  * If there were any errors in the execution, we continue attempting
  to execute scheduled job (`on_error == RETRY_SCHED`)

In the subsequent iterations of this system, we can extend `ScheduleDetails`
protocol messages to support more use cases.  Some future enhancements might include:
  * Do not spawn new job if more than N instances of this job still running
    (instead of restricting to 1)
  * max_retries: Pause scheduled job if more than X runs resulted in an error.
  * Add execution time limits (kill a job if it runs longer than X) 

Job execution is configured via `ExecutorArguments` protocol message:
```proto
message ExecutorArguments {
  google.protobuf.Any args = 1
}
```

`ExecutorArguments` is a protocol message containing opaque `args` which are
arguments specific to different executor implementations.

We plan on supporting multiple types of executors for running periodic jobs.

The initial implementation will come with just one executor, `inline`, 
which just runs the SQL statement in an InternalExecutor 
in the same transaction used to read/update schedule.

Since `inline` executor would not be suitable for long-running queries, a follow-up can
add additional executor types:
  * SqlJob executor: create a job to asynchronously execute the SQL statement arguments.
  * backup executor: special purpose backup executor.

Finally, whenever we perform changes to the scheduled job (i.e. pause it), we want
to record this information for debugging purposes in the following protocol message:

```proto
// ScheduleChangeInfo describes the reasons for schedule changes.
message ScheduleChangeInfo {
  message Change {
    google.protobuf.Timestamp time = 1;
    string reason = 2;
  }
  repeated Change changes = 1  [(gogoproto.nullable) = false];
}
```

#### System database changes.
Scheduled jobs stores job definitions in the `system.scheduled_jobs` table:
```sql
CREATE TABLE system.scheduled_jobs (
    sched_id          INT DEFAULT unique_rowid() PRIMARY KEY,
    job_name          STRING NOT NULL,
    created           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    owner             STRING,
    next_run          TIMESTAMPTZ,
    schedule_expr     STRING,
    schedule_details  BYTES,
    job_args          BYTES NOT NULL,
    executor_type     STRING NOT NULL,
    executor_args     BYTES,
    schedule_changes  BYTES,

    INDEX             (next_run),
    FAMILY "sched"    (sched_id, next_run),
)
```

* `sched_id` is a unique id for this scheduled job
* `job_name` is a descriptive name  (used when showing jobs in the UI)
* `created` is the scheduled job creation timestamp
* `owner` is the user to execute the job as; this might have to be postponed
to V2;
* `next_run` is the next scheduled run of this job.
* `schedule_expr` is the job schedule specified in crontab format; may be empty
* `schedule_details` is a serialized `ScheduleDetails` protocol message.
* `job_args` is a serialized `ScheduledJobArguments` protocol message
If next_run is `NULL`, then the job does not run (it's paused).
* `executor_type` is a string identifying executor to use when executing this schedule
* `executor_args` is a serialized `ExecutorArguments` protocol message
* `schedule_changes` is a serialized `ScheduleChangeInfo` protocol message.

Scheduled jobs system uses the `crontab` format to specify job schedule.  We believe that, even
though this format is not the easiest to use/parse, the crontab format is, nonetheless,
well-established and very flexible.  We will attempt to use existing crontab parsing
packages if possible.

The initial version of scheduled jobs will use UTC timezone for all the schedules.

Scheduled jobs system uses `system.jobs` table as a table of record for the initiated scheduled
job runs.  We will need to change the schema for the `system.jobs` table as follows:

```sql
ALTER TABLE system.jobs ADD COLUMN sched_id INT;
CREATE INDEX sched_idx ON system.jobs(sched_id) STORING(status);
```

The `sched_id` indexed column references the schedule ID in the `system.scheduled_jobs`.
We do not add `FOREIGN KEY` constraint (we want to keep historical records,
even if we delete schedule)

#### Protocol Messages vs Columns: side note
It would be nice to put schedule in its own protocol message so that we may extend
it in the future (e.g. add TZ).
However, the use of protocol buffers directly in the database proved to be difficult
from the UI perspective (the operator may want to view the schedule) as well as
debug-ability.  If the native protobuf support improves, we may replace some
of the `system.scheduled_jobs` fields with protocol messages.
See [#47534](https://github.com/cockroachdb/cockroach/issues/47534)

## Scheduled Job Execution
Each node in the cluster runs a scheduled job execution daemon.
At a high level, the cron daemon periodically performs the following steps:
  1. Starts transaction
  1. Polls `system.scheduled_jobs` to find eligible scheduled job
  1. Executes the job
      * Job execution means that we *queue* the job to be executed by the system jobs.
      * Job execution is done by the job executor configured for this scheduled
      * Job execution is subject to the cluster conditions (e.g. cluster settings);
      * Future versions will also take into consideration cluster wide conditions 
      (such as load), and may decide to reschedule the job instead of executing it.
  1. Updates the `next_run` according to the cron experession, setting it to the next computed time,
   or null if there is no next run.
  1. Commits transaction 

The daemon will apply a small amount of jitter to it's timing to reduce the
probability of the conflicting transactions.

The above steps run inside a single transaction to ensure that
only one node starts running a job. Any errors encountered during
execution are handled based on the `ScheduleDetails` configuration.

The important thing to keep in mind is that since we plan on executing
inside the transaction (at least for the initial version), the actual
statement must complete fairly quickly.  To that end, we plan on 
modifying relevant statements (`BACKUP`, `IMPORT`, etc) to add a variant of these
statements to only queue a job in their transaction 
([#47539)[https://github.com/cockroachdb/cockroach/issues/47539])


#### Scheduled Job Executors

Once determination is made to execute the job, the actual execution
is controlled by an implementation of the `ScheduledJobExecutor` interface.

```go
type ScheduledJobExecutor interface {
  ExecuteJob(...args elided...) (int64, error)
  NotifyJobCompletion(...args elided...) error
}
```

`ExecuteJob`, as the name implies, executes the job (the arguments to this method
are elided, but will include `ExecutorArguments`, `ScheduledJobArguments`, context,
transaction objects, etc)

As mentioned above, initially, we plan on supporting just one executor type.
However, the system is open for new executor types to be added.
Each executor must be registered via registration hook:

```go
func RegisterScheduledJobExecutor(name string, ex ScheduledJobExecutor) error
```

The executors are identified by name, which is stored in the 
`system.scheduled_jobs.executor_type` column.

Recall, the scheduled job framework is responsible for spawning the
system job to do the actual work.  The scheduler needs to know
about the status of the completed jobs.

Depending on the schedule configuration, failed runs may pause the scheduled job.
The scheduled job may also want to persist some state based on the results
of the job (e.g. save the last full backup location in a table)

We will also modify the jobs system to inform scheduled jobs whenever the scheduled
job terminates:

```go
func NotifyJobCompletion(ctx, md *JobMetadata, txn *Txn) error
``` 

The `jobspb.JobMetadata` contains all the relevant job information:
job ID, schedule ID, job completion status, as well as opaque payload,
describing the job itself.  The notification hook is also given a
transaction, so that the changes performed by the schedule notification
hook happens atomically with the job status update mutation.
This hook will lookup the job, and invoke `ScheduledJobExecutor`
to notify it of the job completion.


#### Polling
Our initial MVP version will use a simple polling mechanism where each node picks 
the next runnable job, and attempts to execute it.

Inside the transaction, we select the next eligible job to run.

```sql
SELECT 
  C.job, C.schedule, C.schedule_details, C.user,
  (SELECT count(*) 
   FROM system.jobs J
   WHERE J.sched_id = C.sched_id AND job_status NOT IN ('failed', 'succeeded')
  ) AS num_running,
FROM system.scheduled_jobs C
WHERE next_run < current_timestamp()
```

If `num_running > 0` and policy setting `skip_wait == false`, then we need to
wait for the previous invocation to complete.  To do this, we simply advance
the `next_run` time by some amount and commit transaction.

Once job execution policy constraints have been satisfied, 
we can compute the next time this job is supposed to run.
If the schedule is set, then compute the next_run based on the schedule.
If not, this is a one-off job, set `next_run` to `NULL` (and update
`change_info` to indicate that the schedule completed).

Finally, execute the statement and commit the transaction.

We have multiple venues on improving scalability and performance in the future releases,
driven by the customer needs (Cockroach Cloud, etc): 
  * Nodes may pick a random runnable job (instead of the oldest), to reduce contention
  * If polling becomes problematic (scale), we can explore using another mechanism 
  (leasing/locks) to coordinate which node has exclusive dispatch responsibility,
   and then allow that node to load the schedule into a more efficient structure 
   like a timer ring that is populated via polling or a rangefeed.
  * We can add a coordinator job responsible for finding lots of work to do and 
    spawning these jobs
  * Loading some (or all) scheduled state in memory to avoid rescans

#### Pause/Resume
Pausing the schedule involves setting `next_run` to NULL (and updating
change info appropriately, e.g. reason="paused by an operator")

Un-pausing the schedule involves computing the `next_run` time based on the `schedule`
(and recording the change info)

#### One-Off Jobs
It is possible to use scheduled jobs to execute a one-off job.  A one-off job is represented
by having an entry in the `system.scheduled_jobs` table that has a `next_run` set, but does not
have a `schedule_expr`.

## Monitoring 

Since scheduled jobs is responsible for running unattended jobs, it is import that
we provide visibility into the health of those jobs.

The full details of the monitoring integration are outside the scope
of this document, however, at the very least, we will export the following
metrics:
   * number of paused scheduled jobs
   * number of jobs started
   
## CLI / Admin UI

Interaction with the scheduled job system will require CLI and UI support, as well as changes
to the existing statements.

The actual details of those changes are outside the scope of this RFC, but
at the very least, we will need the support for the following:
  * Create/Modify/Delete scheduled job
  * Pause/Unpause scheduled job
  * Show backup history created by particular schedule

Most of the above can be done by regular SQL statements; however, 
we think that we would need to have a UI with more polish, e.g:
   * CREATE FULL CLUSTER BACKUP WITH weekly SCHEDULE

These statements are syntactic sugar.  We may want to disable
direct access to the underlying `system.scheduled_jobs` table, and only provide
accessors via those "higher level" statements (this is similar how `jobs` uses
virtual stables/show statements)
 
## Future work

* Support user/role delegation: run job as another user
* Support job prioritization
* Support scheduling virtualization 
* Define a new runnable job that wrap and arbitrary block of SQL statements, 
  so that the scheduled job system could then create such jobs, effectively allowing 
  the execution of any sql statement as a scheduled job
* Save terminal job state information into “history” table upon job completion.

