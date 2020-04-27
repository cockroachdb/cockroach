 Feature Name: Scheduled Jobs
- Status: Ready for review.
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
* Support running different types of work:
    * Backup tasks
    * Import tasks
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

A scheduled job is a task that runs at some time in the future.  
The task may be periodic, or it may be a one-off task.  

The task itself is treated as a black box: an opaque protocol message which is interpreted
by the task runner -- an executor -- for this particular task.

The task runner itself is something which plans and executes the task -- for example,
a task runner may create  a "job" record (entry in system.jobs) for execution by the existing 
jobs framework.

The initial implementation will provide one task runner type which evaluates SQL statements.
We will only support SQL statements that can be executed by the system jobs framework.
Currently, cockroach supports limited set of such statements (`BACKUP`, `RESTORE`, etc), 
but we plan on evaluating and possibly adding support for running any sql statement, 
wrapped as a system job, in future work.

### SQL Interface with Scheduled Jobs

It should be possible to interact with the scheduled job system via SQL.

We plan on *eventually* supporting the following statements:
  * PAUSE/RESUME SCHEDULE `id`: pauses/resumes specified schedule.
  * PAUSE JOBS FOR SCHEDULE: pause any jobs that were created by this schedule that are running.
  * SHOW SCHEDULES

These statements are a syntactic sugar ("SHOW SCHEDULES" is more/less identical to "SELECT *"). 
We will most likely defer work on these statements until later versions of this system.

Also, note that the actual creation of the scheduled job record is outside the scope of this
RFC. This RFC describes a low level scheduling sub-system.  

We plan on implementing other, higher level abstractions on top of scheduled jobs. 
For example, a "backup schedules system" will concern itself with all aspects of periodic backup
schedules.  That system will address its SQL/UI needs 
(for example, "CREATE PERIODIC BACKUP ON foo RUNNING@daily") and those higher level statements
will be responsible for creating appropriate scheduled jobs records.

We may want to disable direct access (even for root/super users) to the underlying 
`system.scheduled_jobs` table, and only provide accessors via those "higher level" statements 
(this is similar how `jobs` uses virtual stables/show statements)

## Scheduled Job Representation

The system stores scheduled job related information in a new `system.scheduled_jobs` table.
In addition, we add a new indexed column to the `system.jobs` table to correlate
system jobs to their schedule.

#### `system.scheduled_jobs` Table
Scheduled jobs stores job definitions in the `system.scheduled_jobs` table:

```sql
CREATE TABLE system.scheduled_jobs (
    schedule_id       INT DEFAULT unique_rowid() PRIMARY KEY,
    schedule_name     STRING NOT NULL,
    created           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    owner             STRING,
    next_run          TIMESTAMPTZ,
    schedule_expr     STRING,
    schedule_details  BYTES,
    executor_type     STRING NOT NULL,
    execution_args    BYTES NOT NULL,
    schedule_changes  BYTES,

    INDEX             (next_run),
    FAMILY "sched"    (sched_id, next_run),
    FAMILY "other"    (... all other fields...)
)
```

* `schedule_id` is a unique id for this scheduled job
* `schedule_name` is a descriptive name  (used when showing this schedule in the UI)
* `created` is the scheduled job creation timestamp
* `owner` is the user who owns (created) this schedule (and in v1, who to execute the job as)
* `next_run` is the next scheduled run of this job.
If next_run is `NULL`, then the job does not run (it's paused).
* `schedule_expr` is the job schedule specified in crontab format; may be empty
* `schedule_details` is a serialized `ScheduleDetails` protocol message.
* `executor_type` is a name identifying executor to use when executing this schedule
* `execution_args` is a serialized `ExecutionArguments` protocol message
* `schedule_changes` is a serialized `ScheduleChangeInfo` protocol message.

Scheduled jobs system uses the `crontab` format to specify job schedule.  We believe that, even
though this format is not the easiest to use/parse, the crontab format is, nonetheless,
well-established and very flexible.  We will attempt to use existing crontab parsing
packages if possible.

The initial version of scheduled jobs will use UTC timezone for all the schedules.

### Modifications to `system.jobs` Table

Scheduled jobs system uses `system.jobs` table as a table of record for the initiated scheduled
job runs.  We will need to change the schema for the `system.jobs` table as follows:

```sql
ALTER TABLE system.jobs ADD COLUMN schedule_id INT;
CREATE INDEX sched_idx ON system.jobs(schedule_id) STORING(status);
```

The `schedule_id` indexed column references the schedule ID in the `system.scheduled_jobs`.
We do not add `FOREIGN KEY` constraint (we want to keep historical records,
even if we delete schedule)
This additonal column and index will enable use to efficiently implement SQL statements
related to scheduled jobs (PAUSE JOBS FOR SCHEDULE 123)

### Scheduled Job Protocol Messages

The `ScheduleDetails` describes how to schedule the job:
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

  // How to handle running jobs started by this schedule.
  WaitBehavior wait = 1;

  // How to handle failed jobs started by this schedule.
  ErrorHandlingBehavior on_error = 2;
}
```

The default scheduling policy is:
  * Wait for the outstanding job to finish before starting the next one
    (`wait == WaitBehavior.WAIT`)
  * If there were any errors in the execution, we continue attempting
  to execute scheduled job (`on_error == RETRY_SCHED`)

Note: `ScheduleDetails` does not include information on when to run the schedule.
This data is available in the `system.scheduled_jobs.schedule_expr`.

In the subsequent iterations of this system, we can extend `ScheduleDetails`
protocol messages to support more use cases.  Some future enhancements might include:
  * Do not spawn new job if more than N instances of this job still running
    (instead of restricting to 1)
  * max_retries: Pause scheduled job if more than X runs resulted in an error.
  * Add execution time limits (kill a job if it runs longer than X) 
  * Skip execution if we are too far behind (for example, if the cluster was down,
    we may want to skip all "old" runs and reschedule them to continue based on their
    schedule settings.

Scheduled job system treats work it needs to execute as a black box.
`ExecutionArguments` describes the work to be executed:

```proto
message ExecutorArguments {
  google.protobuf.Any args = 1
}
```
 
`ExecutorArguments` is a protocol message containing opaque `args` which are
arguments specific to different executor implementations (see #scheduled-job-executors
below).


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

#### Protocol Messages vs Columns: side note
It would be nice to put schedule in its own protocol message so that we may extend
it in the future (e.g. add TZ).
However, the use of protocol buffers directly in the database proved to be difficult
from the UI perspective (the operator may want to view the schedule) as well as
debug-ability.  If the native protobuf support improves, we may replace some
of the `system.scheduled_jobs` fields with protocol messages.
See [#47534](https://github.com/cockroachdb/cockroach/issues/47534)

## Scheduled Job Daemon
Each node in the cluster runs a scheduled job execution daemon responsible
for finding and executing periodic jobs (see #scheduled-job-execution below).

Each node in the cluster will wait some time (between 2-5 minutes) before performing
initial scan to let the cluster initialize itself after a restart.

The scheduling daemon is controlled via server settings:
  * `server.job_scheduler.enable` the main knob to enable/disable scheduling daemon
  * `server.job_scheduler.pace` how often to scan `system.scheduled_jobs` table; Default 1 min.
    * Daemon will apply a small amount of jitter to this setting (10-20%).
  * `server.job_scheduler.max_started_jobs_per_iteration`  maximum number of schedules daemon
  will execute per iteration.  Default: 10 (note: this is per node in the cluster)

The `server.job_scheduler.max_started_jobs_per_iteration` (in conjunction with 
`server.job_scheduler.pace`) acts as a safety valve to avoid starting too many jobs 
at the same time -- we want to smooth out the number of jobs we start over time.  
In addition, the default schedule setting prevents starting new instance of the job 
if the previous one has not completed yet; this effectively puts the limit of how many jobs 
can be started by scheduled job system.

We will rely on `system.jobs` to properly manage the load and not to start job execution 
if doing so would be detrimental to cluster health.  The `system.jobs` system may, for example,
decide to limit the number of jobs that can be started by any particular user.  As mentioned
above, the default scheduling policy will prevent more jobs from being scheduled.

The load balancing in `system.jobs` is has not been implemented yet 
([#48825](https://github.com/cockroachdb/cockroach/issues/48825)).
 

#### Scheduled Job Execution
At a high level, the scheduled job daemon periodically performs the following steps:
  1. Starts transaction
  1. Polls `system.scheduled_jobs` to find eligible scheduled job
  1. Updates the `next_run` according to the cron experession, setting it to the next computed time,
     or null if there is no next run.
     * This update acquires intent-based lock on this row and, in effect, exclusively
     locks this row against other dispatchers
  1. Executes the job
      * Job execution means that we *queue* the job to be executed by the system jobs.
      * Job execution is done by the job executor configured for this schedule
      * Job execution is subject to the cluster conditions (e.g. cluster settings);
      * Future versions will also take into consideration cluster wide conditions 
      (such as load), and may decide to reschedule the job instead of executing it.

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

We plan on supporting multiple types of executors for running periodic jobs.

The initial implementation will come with just one executor, `inline`, 
which just runs the SQL statement in an InternalExecutor 
in the same transaction used to read/update schedule.

Since `inline` executor would not be suitable for long-running queries, a follow-up can
add additional executor types:
  * SqlJob executor: create a job to asynchronously execute the SQL statement arguments.
  * backup executor: special purpose backup executor.

The `ScheduledJobExecutor` is defined as follows:
```go
type ScheduledJobExecutor interface {
  ExecuteJob(context, ExecutionArguments) error
  NotifyJobTermination(...args elided...) error
}
```

`ExecuteJob` as the name implies, executes the job.

As mentioned above, initially, we plan on supporting just one executor type.
However, the system is open for new executor types to be added.
Each executor must be registered via registration hook:

`NotifyJobTermination` is invoked whenever job completes.  The executor
has a chance to examine job termination status and take action appropriate
for this executor.

```go
type ScheduledJobExecutorFactory = func(...) (ScheduledJobExecutor, error)

func RegisterScheduledJobExecutorFactory(name string, fact ScheduledJobExecutorFactory) error
```

The executors are identified by name, which is stored in the 
`system.scheduled_jobs.executor_type` column.

Recall, the scheduled job framework is responsible for spawning the
system job to do the actual work.  The scheduler needs to know
about the status of the completed jobs.

Depending on the schedule configuration, failed `system.jobs` runs may pause the scheduled job.
In addition, if the executor fails to execute the job and `ExecuteJob` returns an error,
then this error is treated the same.

`NotifyJobTermination` may also be used to persist some state based on the results
of the job (e.g. save the last full backup location in a table)

```go
func NotifyJobTermination(ctx, md *JobMetadata, txn *Txn) error
``` 

The `jobspb.JobMetadata` contains all the relevant job information:
job ID, schedule ID, job completion status, as well as opaque payload,
describing the job itself.  The notification hook is also given a
transaction, so that the changes performed by the schedule notification
hook happens atomically with the job status update mutation.
This hook will lookup the job, and invoke `ScheduledJobExecutor`
to notify it of the job completion, since handling of job termination
(failure or success) may be specific to each type of executor (for example, backup
specific executor may want to record some history data in its own table).


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
   like a timer wheel that is populated via polling or a rangefeed.
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

## Monitoring and Logging

Since scheduled jobs is responsible for running unattended jobs, it is import that
we provide visibility into the health of those jobs.

The schedule changes are reflected in the `system.scheduled_jobs.schedule_changes`
column.  However, once the work is handed off to the system jobs framework, the
logging and visibility into the actual job will be the responsibility of that system
(see [#47212](https://github.com/cockroachdb/cockroach/issues/47212)).

The full details of the monitoring integration are outside the scope
of this document, however, at the very least, we will export the following
metrics:
   * number of paused scheduled jobs
   * number of jobs started
   
## Future work

* Support user/role delegation: run job as another user
* Support job prioritization
* Support scheduling virtualization 
* Define a new runnable job that wrap and arbitrary block of SQL statements, 
  so that the scheduled job system could then create such jobs, effectively allowing 
  the execution of any sql statement as a scheduled job
* Save terminal job state information into “history” table upon job completion.

