- Feature Name: Per-session CPU usage and other metrics
- Status: draft
- Start Date: 2021-02-08
- Authors: knz
- RFC PR: [#60589](https://github.com/cockroachdb/cockroach/pull/60589)
- Cockroach Issue: [#59998](https://github.com/cockroachdb/cockroach/issues/59998)


# Summary

This RFC proposes to use custom extensions to the Go runtime to track
CPU usage, as well as other metrics, per SQL session -- including all
goroutines that do work on behalf of a SQL session, and without
instrumenting the code manually inside CockroachDB.

In a later stage, this work will also enable constraining
CPU usage and other resources according to configurable quota.

# Motivation

Today there is an issue when CPU usage is high, it is very difficult
to track down which SQL session or query is responsible for that load.

In turn the difficulty comes from the Go runtime which does not provide
an abstraction to extract metrics per group of goroutines performing
related work on behalf of a common client-defined query or session.

# Technical design

In a nutshell, the proposed change adds new APIs to Go's `runtime` package to
create a new “task group” abstraction, which captures counters and other
control mechanisms shared by a collection of goroutines and is inherited
automatically by all new goroutines indirectly spawned.

Once this API is in place inside Go, we can extend CockroachDB as follows:

- whenever a SQL session is created, a new task group is created for its
  goroutine and its children goroutines
- distsql execution on other nodes also sets up a task group whenever
  one or more flows are set up on behalf of a query started on a different node,
  to connect all the flows for that queries “under” a common, separate task group.
- as the distsql processors advance or complete work, the current metrics
  of the task group are pushed back to the gateway to update common counters
  for the session.
- on the gateway, the vtable `crdb_internal.node_sessions` and other related
  facilities report, for each session/query, the additional metrics from the task group.
- SQL job execution also creates task groups, one per job, in a way
  that can be inspected separately (e.g. via `crdb_internal.jobs`? TBD)

A preliminary version of the proposed Go runtime changes is available
in this branch:

https://github.com/cockroachdb/go/commits/crdb-fixes

And an example application inside CockroachDB, to measure CPU scheduling ticks
separately for each SQL session, is available here:

https://github.com/cockroachdb/cockroach/pull/60588

## Go runtime extension

### Task group abstraction

The center of the proposed approach is the definition of a new “task
group” abstraction inside the Go runtime / scheduler, in addition to
the existing G / M / P abstractions.

(The reader of this RFC is expected to have some familiarity with the
G / M / P abstractions already. Refer to the Go team's own
documentation and the top comment inside the `runtime/proc.go` file in
the Go distribution.)

A task group is a collection of zero or more goroutines.

Each goroutine is associated to exactly one task group.

There is a “default” (“global”? “common”?) task group defined
initially for all goroutines spawned from the main/initial goroutine.

Each new goroutine (started with the `go` keyword) inherits the task
group of its parent.

A `runtime.SetInternalTaskGroup()` API enables Go applications to override the
task group of the current goroutine. After this has been overridden,
all further goroutines spawned from that point inherit the new task
group. (But all previously spawned goroutines retain their previously
defined task group.)

The Go runtime scheduler is aware of the task groups and is able to
update / maintain runtime counters and other metrics at the level
of the task group, not just the goroutine.

### Internal vs external state for task groups

A task group is represented at runtime by two different pieces of
state inside every goroutine:

- the internal task group struct has a definition known only to the
  `runtime` package, and cannot be read / changed by Go applications.

  This is defined as the (opaque) type `runtime.InternalTaskGroup`.
  It is also the state defined via the main new API `SetInternalTaskGroup()`
  outlined above.

  This is the struct used inside the Go scheduler and other runtime
  subsystems (e.g. the heap GC in the future).

  There are public accessors in the `runtime` package to retrieve
  the values stored inside the internal task group struct.

  For example, `GetInternalTaskGroupSchedTicks(TaskGroup) uint64` retrieves
  the number of scheduling ticks incurred by the given task group.

- for applications that wish to attach additional state to a task group,
  that can be left opaque to the go runtime, another API is provided:

  ```go
  // GetLogicalTaskGroupID retrieves the current goroutine's task group
  // ID. This is inherited from the goroutine's parent Top-level
  // goroutine are assigned their own ID as group ID.
  func GetLogicalTaskGroupID() int64

  // SetGoroutineGroupID sets the current goroutine's task group ID.
  // This value is inherited to children goroutines.
  func SetLogicalTaskGroupID(groupid int64)
  ```

  These IDs can then be used in application code to serve as key
  in a Go map or other similar structure to attach additional
  state to a task group.

## Tracked metrics and state

- The internal task group struct will be responsible exclusively to track
  metrics that cannot be tracked by application code (or not effectively).

  Examples:

  - CPU usage
  - current total RAM usage
  - number of discrete heap allocations

- The logical task group ID can be used to attach identification
  labels or descriptions, or metrics / counters that are updated at
  application level. Example valuable attachments:

  - task group label as a string,
  - current SQL statement AST (or maybe some other aspects of the SQL session)
  - owner / user
  - `application_name`

  All these additional fields are going to make it easier to understand
  who / what is responsible for resource usage.

  The reason why we prefer an ID-to-state mapping at the application
  level is that we wish to avoid having to update the Go runtime every
  time we want to attach new state to task groups. The internal task
  group struct should remain restricted to those metrics / variables
  that must absolutely be known inside the `runtime` package, in
  particular because they are accessed/updated inside the Go scheduler
  and other sub-systems.

## Implementation of the task group abstraction

Implementation-wise, we extend the `g` struct inside the go runtime
with two fields `taskGroupCtx` (for the internal task group)
and `taskGroupId` (for the logical task group ID).

Both are inherited when creating a new goroutine. When a goroutine is
created without a parent, they are initialized as follows:

- `taskGroupCtx` is set to `defaultTaskGroupCtx`, the default/common
  task group.
- `taskGroupId` is set to the goroutine's ID (this default may change, unsure).

These defaults are not too important/interesting since we will ensure
that the task group is overridden every time we create a new SQL
session or job.

## Example metrics: CPU scheduler ticks

Context: The Go scheduler is preemptive and runs goroutines in segments of time
called "ticks". The specific time length of a tick is variable but is
bounded to a maximum, which is quite small (there are more than 10 per
second, TBD how much precisely). For CPU-heavy loads every tick tends
to use its fully allowed length before control is switched over to a
different goroutine, to ensure fair scheduling.

In order to estimate CPU usage for different SQL queries/sessions, and
generally identify which SQL session(s)/query(ies) is/are responsible
for peaks in CPU usage, we need to collect stats about scheduler
ticks.

However we cannot / do not want to do so separately per goroutine,
since a single SQL session/query may be served by many goroutines over
time. Instead, we wish to collect this metric across all goroutines
that participate in a SQL query/session.

For this we can use the new task group abstraction.

The RFC proposes to extend the internal task group struct with a new
`schedticks` field, incremented upon every scheduler tick for the
current task group.

Example diff to add this increment, in `runtime/proc.go`:

```go
func execute(gp *g, inheritTime bool) {
   ...
    if gp.taskGroupCtx != nil {
        atomic.Xadd64(&gp.taskGroupCtx.schedtick, 1)
    }
```

Then we can retrieve the scheduling ticks for a given task group, in
`runtime/task_group.go`:

```go
// GetInternalTaskGroupSchedTicks retrieves the number of scheduler ticks for
// all goroutines in the given task group.
func GetInternalTaskGroupSchedTicks(taskGroup TaskGroup) uint64 {
    tg := (*t)(taskGroup)
    return atomic.Load64(&tg.schedtick)
}
```

And then we can connect this inside SQL, in `sql/conn_executor.go`:

```go
func (ex *connExecutor) run(...) {
    ...
    ex.taskGroup = runtime.SetTaskGroup()
}

...

func (ex *connExecutor) serialize() serverpb.Session {
    ...
    return serverpb.Session{
         ...
        // Experimental
        SchedTicks: runtime.GetTaskGroupSchedTicks(ex.taskGroup),
    }
}
```

And then, to make it observable for SQL DBAs, in `sql/crdb_internal.go`:

```go
func populateSessionsTable(...) {
   ...
   ... addRow(
            ...
            tree.NewDInt(tree.DInt(session.SchedTicks)),
   )
   ...
}
```

Example usage:

```
root@:26257/defaultdb> select node_id, client_address, last_active_Query, sched_ticks 
                       from crdb_internal.node_sessions;

  node_id | client_address | last_active_query | sched_ticks
----------+----------------+-------------------+--------------
        1 | [::1]:27580    | SHOW database     |         972
        1 | [::1]:27577    | SHOW database     |        6675
(2 rows)
```

This shows 2 distinct SQL sessions (there are two interactive terminals
running a SQL shell connected to this node). The `sched_ticks` column
reports that one session has used 972 ticks and the other has used 6675 ticks.

The value of sched ticks can be confirmed to increase in relation to
actual CPU usage. We can compare how the ticks are incremented for several operations:

| Operation                                           | Tick increment                              |
|-----------------------------------------------------|---------------------------------------------|
| `pg_sleep(0)`                                       | 20                                          |
| `pg_sleep(1)`                                       | 20 (sleeping time does not incur CPU usage) |
| `select count(*) from generate_series(1,10000000)`  | 280                                         |
| `select count(*) from generate_series(1,100000000)` | 2800  (ten times more than previous)        |

As this confirms, idle sessions don't increase their `sched_ticks`, whereas busy sessions increment
it in proportion to the work actually done.

(NB: the Go changes outlined above do not include the aggregation of
sched ticks across multiple nodes when queries are distributed. We'd
need to think further about how to reason about CPU usage on multiple
nodes, and whether we even want this to be aggregated, or whether we
want the CPU ticks to be presented side-by-side for separate nodes.)

PR where this change has been prototyped: https://github.com/cockroachdb/cockroach/pull/60589

## Example metrics: heap usage

The basic concept here is to force the Go heap allocator to use
different heaps for different task groups.

To achieve this we change the heap allocator structures to attach
a task group to "pages" (blocks) of system memory that are used
to carve heap allocations from.

When a heap allocation is performed, the RAM usage counter on the task
group is incremented; when the GC liberates all the allocations on the
page/block it decreases the task group's RAM usage by the size of the
page.

This is a bit more complicated to get right, because we probably
want to avoid fragmentation for "small objects" that are only allocated
once per SQL session. How to deal with those?

There are multiple schemes we can envision:

- separate the abstraction of "task group" into sub-containers for
  heap usage, such that the small allocations are "billed" against the
  common/default task group while "large" allocations remain billed
  per task group.

  This can be considered acceptable if we have confidence that the
  number of small allocations inside each SQL session/query remains
  small (i.e. most problematic heap usage is incurred by larger
  allocations).

- track the task group for every object inside the page, or for groups
  of objects of the same type (e.g. slices).

  This will yield more precise / complete heap usage metrics but incur
  higher RAM usage overall due to the additional task group tracking.

Which scheme to use needs to be guided by further experiments.

## Later benefit: resource control (not just measurement)

Once these facilites are in place, we can then extend the task group
abstraction inside the go runtime, with a per-group configurable
maximum value, such that an exception or other control mechanism is
issued whenever the go runtime notices the metric is exceeding the
configured budget.



## Drawbacks

Maintaining a custom runtime extension in the Go project really means
creating a new project maintained by Cockroach Labs, which will
responsible for importing all the changes made upstream by the Go
team. This is additional effort, also requiring a skill set that we
have not yet been hiring for inside the CockroachDB and Cockroach
Cloud projets.

That will in turn incur working hours for the unforeseeable future.

We can mitigate that risk either:

- by setting up a communication forum
  with the Go team to demonstrate the runtime extensions we are
  working with and advocate for their inclusion in the mainstream
  Go distribution.

- by developing the runtime extensions not as a repository fork, but
  instead as a patchset, and create automation which will
  automatically detect changes to upstream Go and apply the patches
  automatically every time a new Go release is made.  (This is the
  approach taken by the `redact` package to override the `fmt`
  behavior, see
  [here](https://github.com/cockroachdb/redact/blob/master/internal/README.md#refreshing-the-sources).)

## Rationale and Alternatives

The main alternative for the approach taken here is to manually
instrument every single entry point in Go where "CPU time" or "memory
usage" can be incurred. This has been attempted in the past and was
largely unsuccessful.

# Explain it to someone else

Magic!

# Unresolved questions

- How to ensure that CockroachDB can build and run with a standard
  Go distribution not equipped with the proposed API? What are the
  exact build steps to make this possible?

  (We accept that the resource monitoring benefits would not be available
  when the custom extension is not used.)

- Do we want to extend the approach here to also measure resource
  usage incurred by KV-level goroutines? How would that work?
