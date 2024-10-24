- Feature Name: Task groups in Go: per-session resource usage tracking (CPU)
- Status: draft
- Start Date: 2021-02-08
- Authors: knz, tbg, peter
- RFC PR: [#60589](https://github.com/cockroachdb/cockroach/pull/60589)
- Cockroach Issue: [#59998](https://github.com/cockroachdb/cockroach/issues/59998)


# Summary

This RFC proposes to use custom extensions to the Go runtime to track
CPU usage, as well as other CPU-related metrics, per SQL session --
including all goroutines that do work on behalf of a SQL session, and
without instrumenting the code manually inside CockroachDB.

In a later stage, this work will also enable constraining
CPU usage per SQL session or query according to configurable quota.

Note: there is a [separate
RFC](20210215_task_group_resource_tracking_ram.md) for observing and
restricting RAM usage.

Note: this work is valuable even if we consider splitting the SQL
processing into different processes (e.g. one process for SQL, one for
KV; or one process per SQL tenant; or separate process for SQL
gateways and distSQL processors). Even in a multi-process
architecture, we are unlikely to create one separate process per SQL
session. The proposal in this RFC provides fine-grained resource
consumption separation between SQL sessions, including separate
sessions for the same SQL tenant.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Motivation](#motivation)
- [Technical design](#technical-design)
    - [Proof of concept: CPU scheduler ticks](#proof-of-concept-cpu-scheduler-ticks)
        - [Precise CPU time / utilization](#precise-cpu-time--utilization)
    - [Later benefit: resource control (not just measurement)](#later-benefit-resource-control-not-just-measurement)
    - [Related work](#related-work)
        - [Tailscale's custom Go patches](#tailscales-custom-go-patches)
        - [Related work in the Go project](#related-work-in-the-go-project)
    - [Rationale and Alternatives](#rationale-and-alternatives)
        - [Manual instrumentation](#manual-instrumentation)
        - [PProf-based CPU reporting](#pprof-based-cpu-reporting)
        - [Taskgroup-based CPU usage sampling](#taskgroup-based-cpu-usage-sampling)
- [Explain it to someone else](#explain-it-to-someone-else)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->


# Motivation

Today there is an issue when CPU usage is high, it is difficult
to track down which SQL session or query is responsible for that load.

In turn the difficulty comes from the Go runtime which does not provide
an abstraction to extract metrics per group of goroutines performing
related work on behalf of a common client-defined query or session.

Even though we are also considering process-level separation (e.g. one
separate SQL process per tenant), users still wish to know which
queries use the most CPU within the same CockroachDB process.

# Technical design

In a nutshell, the proposed change adds new APIs to Go's `runtime` package to
create a new “task group” abstraction, which captures counters and other
control mechanisms shared by a collection of goroutines and is inherited
automatically by all new goroutines indirectly spawned.

See the [companion mini-RFC](20210215_task_groups.md) for details
about the new abstraction.

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

And an example application inside CockroachDB, to measure CPU
scheduling ticks and absolute run time in nanoseconds, separately for
each SQL session, is available here:

https://github.com/cockroachdb/cockroach/pull/60588

## Proof of concept: CPU scheduler ticks

**NB: this section is just proof of concept. See section immediately below
for a better production-level approach.**

Context: The Go scheduler is preemptive and runs goroutines in
segments of time called "ticks" (at most 10ms per tick).

In order to estimate CPU usage for different SQL queries/sessions, and
generally identify which SQL session(s)/query(ies) is/are responsible
for peaks in CPU usage, we can collect stats about scheduler
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
func initTaskGroupMetrics() {
  ...
  taskGroupMetrics.metrics = map[string]func(tg *t, out *metricValue){
		"/taskgroup/sched/ticks:ticks": func(tg *t, out *metricValue) {
			out.kind = metricKindUint64
			out.scalar = atomic.Load64(&tg.schedtick)
		},
        ...
}

//go:linkname readTaskGroupMetrics runtime/metrics.runtime_readTaskGroupMetrics
func readTaskGroupMetrics(taskGroup InternalTaskGroup, samplesp unsafe.Pointer, len int, cap int) {
	sl := slice{samplesp, len, cap}
	samples := *(*[]metricSample)(unsafe.Pointer(&sl))
	...
 	for i := range samples {
		sample := &samples[i]
		compute, ok := taskGroupMetrics.metrics[sample.name]
		compute((*t)(taskGroup), &sample.value)
    }
}
```

Then we expose the metrics via the new `metrics` package, in
`runtime/metrics/task_group.go`:


```go
// Implemented in the runtime.
func runtime_readTaskGroupMetrics(runtime.InternalTaskGroup, unsafe.Pointer, int, int)

// ReadTaskGroup populates each Value field in the given slice of metric samples.
// The metrics are read for the specified task group only.
//
// See the documentation of Read() for details.
func ReadTaskGroup(taskGroup runtime.InternalTaskGroup, m []Sample) {
	runtime_readTaskGroupMetrics(taskGroup, unsafe.Pointer(&m[0]), len(m), cap(m))
}
```

(NB: This is the API pattern promoted by [Go 1.16's new `metrics` package](https://tip.golang.org/pkg/runtime/metrics/).)




And then we can connect this inside SQL via the [`go-plus`
library](https://github.com/cockroachdb/go-plus), in
`sql/conn_executor.go`:

```go
func (ex *connExecutor) run(...) {
    ...
    ex.taskGroup = taskgroup.SetTaskGroup()
}

...

func (ex *connExecutor) serialize() serverpb.Session {
    ...
	taskGroupMetrics := []metrics.Sample{
		{Name: "/taskgroup/sched/ticks:ticks"}, ...}
	metrics.ReadTaskGroup(ex.taskGroup, taskGroupMetrics)
	var schedTicks uint64
	if taskGroupMetrics[0].Value.Kind() != metrics.KindBad {
		schedTicks = taskGroupMetrics[0].Value.Uint64()
	}

    return serverpb.Session{
         ...
        // Experimental
        SchedTicks: schedTicks,
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

**Why this approach is unsuitable to measure CPU time:**

It is possible for ticks to be shorter than the preemption interval,
for example when a Goroutine yields CPU to grab a lock currently held
elswehere. So a goroutine that often swaps in and out of CPU usage
because its progress is blocked by another goroutine will appear as if
it is using a lot of CPU time, even though it is merely yielding CPU
more often.

Generally, *ticks* are not a very precise measurement of CPU time.

### Precise CPU time / utilization

We reuse the same approach as the previous section; however instead of
ticks we measure actual CPU time, not ticks.

We achieve this using the internal (non-exported) runtime function
`nanotime()` or `walltime()`.  These functions report the current time
at nanosecond-precision. They have negligible cost, because they
merely read a value from RAM via the [process's vDSO
interface](https://man7.org/linux/man-pages/man7/vdso.7.html).

This works as follows:

- we add a new field `lastSchedTime` in the `g` struct.
- we add a new field `nanos` in the `taskGroupCtx`

- upon leaving the processor (in the `dropg()` function):
  - we take `lastSchedtime` from the current/past `g` (indicates the start time of the goroutine we're scheduling away from)
  - we measure the current time with `nanotime()` or `walltime()`.
  - we add the delta between current time and `lastSchedTime` to `taskGroupCtx.nanos`

- upon entering the processor (in the `execute()` function):
  - we store the current time in `lastSchedTime` of the new `g`.

On CPU architectures that provide a hardware timestamp counter (TSC)
that is synchronized across all CPU cores, where the TSC is available
without special system privileges, and where the TSC unit has a clear
relationship to real time, we can also build a custom assembly-level
function that retrieves the TSC and uses that instead. This may yield
even better performance since it does not need to synchronize the vDSO
timestamp across all core caches.

Example usage:

```
root@:26257/defaultdb> select node_id, client_address, last_active_Query, cpu_time
                       from crdb_internal.node_sessions;
  node_id | client_address | last_active_query |    cpu_time
----------+----------------+-------------------+------------------
        1 | [::1]:58417    | SHOW database     | 00:00:00.003878
        1 | [::1]:58420    | SHOW database     | 00:00:00.00287
(2 rows)
```

This shows 2 distinct SQL sessions (there are two interactive terminals
running a SQL shell connected to this node). The `cpu_time` column
reports that one session has used 388ms of CPU time and the other has used 892ms.

The CPU time column can be confirmed to increase in relation to
actual CPU usage. For example:

| Operation                                           | CPU time increment                              |
|-----------------------------------------------------|-------------------------------------------------|
| `pg_sleep(0)`                                       | ~900µs                                          |
| `pg_sleep(1)`                                       | ~900µs (sleeping time does not incur CPU usage) |
| `select count(*) from generate_series(1,10000000)`  | ~1.24s                                          |
| `select count(*) from generate_series(1,100000000)` | ~12.4s  (ten times more than previous)          |

As this confirms, idle sessions don't increase their CPU time, whereas busy sessions increment it.

(NB: Again, the Go changes outlined above do not include the aggregation of
CPU time across multiple nodes when queries are distributed.)

PR where this change has been prototyped: https://github.com/cockroachdb/cockroach/pull/60588


## Later benefit: resource control (not just measurement)

Once these facilites are in place, we can then extend the task group
abstraction inside the go runtime, with a per-group configurable
maximum value, such that an exception or other control mechanism is
issued whenever the go runtime notices the metric is exceeding the
configured budget.

## Related work

### Related work in the Go project

- [runtime: GC pacer problems meta-issue](https://github.com/golang/go/issues/42430)

  An inventory of the following known problems:

  - Idle GC interferes with auto-scaling systems - the idle GC
    utilizes idle CPU cycles, which prevents orchestration tools from
    reducing hardware CPU allocation to an idle server.

  - Assist credit system issues - certain goroutines are forcefully
    slowed down when memory pressure is high, even if they do not
    participate in memory usage.

  - High GOGC values have some phase change issues - if a server's
    behavior changes suddenly, e.g. a single query comes in with more
    memory usage than usual, the GC will not notice this on time.

  - Support for minimum and maximum heaps - it is not possible to
    configure soft minimum and maximum limits for heap usage or to use
    a ballast allocation to recover from memory exhaustion.

  - Failure to amortize GC costs for small heaps - a program which has
    a majority of its allocations in global variables will confuse the
    GC and cause it to consume too much CPU.

- [proposal: runtime: add per-goroutine CPU stats](https://github.com/golang/go/issues/41554)

  This is an issue filed by a member of our team (Alfonso).
  It proposes to accumulate CPU time per goroutine.

  Recent discussion on the issue cross-referenced this PR.

  Main problem with the proposal: measuring at the level of individual
  goroutines is too fine, there may be multiple goroutines for a
  single query. Also it makes it impossible to collect cumulative
  stats when sub-goroutines terminate.

## Rationale and Alternatives

There are two main alternatives to the work considered here:

- manual instrumentation everywhere.
- separate processes.
- pprof-based CPU reporting.
- another taskgroup-based approach to CPU reporting, using a sampling approach.

They are outlined below.

### Manual instrumentation

The idea is to manually instrument every single entry point in Go
where "CPU time"  can be incurred.

For CPU usage, we can add an "increment CPU time" call into every function
that performs work on behalf of a SQL query or session. This also needs
to be called inside every loop, once per iteration.

Problems with this approach:

- engineers will forget to add the CPU accounting.
- the accounting will make the code harder to read and to maintain.
- the tracking will be imprecise and it will be impossible to distinguish
  a function that performs a lot of work without a loop, from one
  that is simply waiting.

### Separate processes

We could spawn more CockroachDB processes, with fewer work done per
process.

We already do this for multi-tenant, with one process per SQL tenant.
One known drawback is that a process crash kills all that tenant's
connections to the server.

We can further refine by creating more processes. There are two approaches:

- Create one sub-process per session. That is more or less what pg does.

  - One drawback is that we must be very aggressive with connection
    pooling, to avoid keeping/starting processes
    for idle sessions.

- Create sub-processes for the distSQL execution. That would be a new idea.

  - One drawback is that we would need cross-process IPCs to flow data
    back and forth, as well as table descriptors and other metadata
    used for exec processors. This may have a performance overhead.

  - Another drawback is that we have many queries that operate on the
    gateway node directly. This makes the CPU usage for multiple
    sessions confusing to monitor on the gateway node.

Finally, something not discussed so far but must be mentioned for any
process-based solutions: we still also need to think about CPU
accounting and budgeting:

- separate processes give us CPU accounting "for free" thanks to OS metrics.

- CPU budgeting would then come in either of three variants:

  - no specific budgets: the OS scheduler is responsible for
    fairness. This might be OK but we'd need a lot of benchmarking to
    know for sure.
  - fixed CPU limits: like above, we'd need to find out what is a good limit.
  - relative CPU limits: we can group processes on Linux, and assign
    scheduling priorities to groups. Within a group processes are
    scheduled fairly. This would enable us to give priority to certain
    SQL sessions, e.g. admin or internal sessions.

### PProf-based CPU reporting

The Go [`pprof`](https://golang.org/pkg/runtime/pprof/) package is
integrated with the Go runtime system to provide a breakdown of CPU
and RAM usage when *profiling* a process.

Here is how this works:

1. the program sets “profiling labels” at the top of a goroutine spawn
   tree using `pprof.SetGoroutineLabels()`. The Go runtime system
   has special support for labels and ensures they are inherited by any
   goroutine spawned from this root.
   (This is the `labels` field in the `g` struct, see `src/runtime/runtime2.go`)

   Also, gRPC propagates profiling labels along RPC calls so they are
   also inherited on remote nodes automatically.

2. to start a CPU usage report, the program requests `StartCPUProfile`
   on every server process simultaneously. An output file / buffer
   must be passed as argument. This instructs the Go runtime to start
   collecting stats.

   The Go runtime profiles CPU execution by interrupting execution at
   a configurable frequency (configurable via `SetCPUProfileRate`, for
   example 100 times per second). Every time execution is interrupted,
   it inspects which goroutines are currently running, and
   tallies them as "active" as per their profiling labels.

   The profiling reports are written to the output buffer specified at
   the beginning.

3. to end the profiling report, the program requests `StopCPUProfile`.

An example implementation of this approach was prototyped here:
https://github.com/cockroachdb/cockroach/pull/60508

**Considerations**

- CPU Profiling operates by suspending execution of, and taking snapshots
  of goroutines' stacks at a defined frequency, which comes at a
  performance cost. The lower the frequency, the smaller the cost, but
  there is a corresponding loss of fidelity. There is skepticism
  whether the approach can achieve a useful trade-off between the two
  dimensions that would ultimately be preferable to modifying the
  runtime directly.

  For reference a (non-scientific) experiment observed a 6% perf hit
  on the kv100 workload.

- DataDog built an entire product around regular CPU profiles
  collected for [15s every
  minute](https://github.com/DataDog/dd-trace-go/blob/8026b33c428aea01d992707b1951d5e5559f4ed3/profiler/options.go#L40-L44)
  at 100hz (the default frequency). They don't provide performance
  numbers though, so the above question remains.

- Profiler labels are not yet implemented for use in heap profiles,
  though this is something the Go community
  [wants](https://github.com/golang/go/issues/23458#issuecomment-377973247)
  (i.e. it will either happen or they will be open to us making it
  happen without a need to fork)

- the pprof *label* abstraction is too heavyweight for use for heap usage
  and there is mounting consensus in the go team that the API needs to change
  before we can do something about heap usage. [This issue comment](https://github.com/golang/go/issues/23458#issuecomment-377973247)
  and [this one](https://github.com/golang/go/issues/23458#issuecomment-620237813) discuss this further.
  This uncertainty about API definitions may delay the work further
  on the Go team's side.

- relying on profiling can only make resource consumption
  *observable*, not *controllable*. Reigning in resource usage is only
  possible in a reactive way (i.e. eg. by terminating queries that are
  above budget) while modifying the runtime directly could in
  principle allow us to establish hard bounds.

- a pprof-based approach avoids maintaining a permanent fork of the Go runtime.


### Taskgroup-based CPU usage sampling

Let's assume we annotate goroutines with a task group ID, as outlined
in the main proposal.

An alternative approach to CPU accounting is to not track CPU time at
every scheduling event. Instead, we can use a *sampling* approach as
follows:

- at a periodic interval (e.g. 10 times per second), iterate over all
  Ps, for each P look at the associated M, what goroutine it is
  running (`curg`) and then peek at which task group the goroutine is associated.
- increment a counter in the task group.

This is conceptually similar to the pprof-based approach, except that
we are not interested in a full stack trace per goroutine. So we do
not need to interrupt the goroutine execution to observe the goroutine
state.

This sampling approach is likely lighterweight than the precise
counters proposed in the main text of the RFC above.

However, sampling in general does not offer us a path to *control* CPU
usage, for example via a conditional against a configurable budget in
the scheduling code.

# Explain it to someone else

We enhanced CockroachDB's ability to track CPU resource usage at
the level of SQL queries and session.

We achieved this by implementing custom changes to the runtime system
of the Go language. This means that a custom Go runtime is now needed
to build CockroachDB.

It is still possible to build CockroachDB without these custom
patches, however that will prevent CockroachDB from performing precise
resource tracking and cause certain features to become disabled.


# Unresolved questions

- Do we want to extend the approach here to also measure resource
  usage incurred by KV-level goroutines? How would that work?
