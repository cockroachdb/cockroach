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

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Motivation](#motivation)
- [Technical design](#technical-design)
    - [Go runtime extension](#go-runtime-extension)
        - [Task group abstraction](#task-group-abstraction)
        - [Internal vs external state for task groups](#internal-vs-external-state-for-task-groups)
        - [Tracked metrics and state](#tracked-metrics-and-state)
        - [Implementation of the task group abstraction](#implementation-of-the-task-group-abstraction)
    - [Example metrics](#example-metrics)
        - [Proof of concept: CPU scheduler ticks](#proof-of-concept-cpu-scheduler-ticks)
        - [Precise CPU time / utilization](#precise-cpu-time--utilization)
        - [Heap usage](#heap-usage)
            - [Context: how the Go heap allocator works](#context-how-the-go-heap-allocator-works)
            - [Tweaking the Go heap allocator for precise measurement](#tweaking-the-go-heap-allocator-for-precise-measurement)
            - [Possible future enhancement: custom allocators](#possible-future-enhancement-custom-allocators)
    - [Later benefit: resource control (not just measurement)](#later-benefit-resource-control-not-just-measurement)
    - [Drawbacks](#drawbacks)
    - [Related work](#related-work)
        - [Tailscale's custom Go patches](#tailscales-custom-go-patches)
        - [The `gotcha` package](#the-gotcha-package)
        - [Related work in the Go project](#related-work-in-the-go-project)
    - [Rationale and Alternatives](#rationale-and-alternatives)
        - [Manual instrumentation](#manual-instrumentation)
        - [PProf-based CPU reporting](#pprof-based-cpu-reporting)
- [Explain it to someone else](#explain-it-to-someone-else)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->


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

  (Note: the specific naming and interface of these APIs is left to be
  improved through iteration and experimentation. We should aim to
  follow the example set by [Go 1.16's new `metrics` package](https://tip.golang.org/pkg/runtime/metrics/).)

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

### Tracked metrics and state

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

  To achieve this, we can use a key-value storage facility as a
  package inside CockroachDB, keyed by the task group. We can also look
  at the KV storage abilities of `context.Context` for inspiration.

### Implementation of the task group abstraction

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

## Example metrics

### Proof of concept: CPU scheduler ticks

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


And then we can connect this inside SQL, in `sql/conn_executor.go`:

```go
func (ex *connExecutor) run(...) {
    ...
    ex.taskGroup = runtime.SetTaskGroup()
}

...

func (ex *connExecutor) serialize() serverpb.Session {
    ...
	taskGroupMetrics := []metrics.Sample{
		{Name: "/taskgroup/sched/ticks:ticks"}, ...}
	metrics.ReadTaskGroup(ex.taskGroup, taskGroupMetrics)

    return serverpb.Session{
         ...
        // Experimental
        SchedTicks: taskGroupMetrics[0].Value.Uint64(),
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

PR where this change has been prototyped: https://github.com/cockroachdb/cockroach/pull/60589

### Heap usage

#### Context: how the Go heap allocator works

The Go heap allocator is explained in a comment at the top of
the file `src/runtime/malloc.go` and `mcache.go`.

In a nutshell, the allocator works differently for small and large
objects.

For small objects it tries to allocate locally to the current thread
(one `mcache` per P) and if that fails grab memory globally.

Of note, small allocations in the `mcache` are grouped together
(including from separate goroutines running on the same P) so that
multiple allocated objects appear as one from the GC's perspective.

Large objects are allocated globally in any case.

The allocation unit is the page (8KB), however pages are allocated
from the OS in batches of 64MB called “arenas”. Each arena has
metadata that indicates which parts of it contain pointers and which
do not; this metadata is needed during GC to decide whether to
interpret data as reference or an integer.

All the arena metadata is linked together in a single “arena map” so
that the GC can find metadata for every location in the process'
address space.

The GC is asynchronous and runs in a different goroutine than where
allocations were made.

#### Tweaking the Go heap allocator for precise measurement

We tweak the allocator as follows:

- large allocations are tagged with a pointer to their
  `taskGroupCtx`. We increment the allocation tally in `taskGroupCtx`
  when the allocation occurs. When the GC releases the object, its
  size is subtracted from the tally in the linked `taskGroupCtx`.

  This is the simple case.

- for small allocations, we do as follows:

  - we change the mapping of P to `mcache`.  Instead of one
    `mcache` per P, we use one `mcache` per P per `taskGroupCtx` --
    i.e. we implement a separate small heap per task group.

  - when an allocation elects to use a small heap, instead of
    retrieving the mcache via `getg().p.mcache`, we use
    `getg().taskGroupCtx.mcache[getg().p.id]`.

    (This is simple+safe because the P IDs are allocated from 0
    to GOMAXPROCS-1, and we can reallocate the mcache arrays
    everytime GOMAXPROCS is overridden.)

  - every page allocated by a `mcache` is tagged with a pointer
    to its owner `taskGroupCtx`. This tells the GC that
    every object “under” this page “belongs” to that
    task group. This is correct because the mapping established
    above forces this ownership.

  - we increment the allocation tally each time a small allocation
    occurs.

  - like for large allocations above, everytime the GC deallocates
    a full page of small objects it decreases the tally in the
    linked `taskGroupCtx`.

#### Possible future enhancement: custom allocators

(NB: this section is out of scope for this RFC but is included
as a teaser for later opportunities.)

This is an idea proposed by Andrei M: instead of a fixed
mapping from task group to P to `mcache`, we could even customize
the allocation algorithm(s) per task group.

That is, we can define a custom `malloc` function pointer per task
group, and have the runtime-level `mallogc` function call the current
task group's `malloc` function if defined.

This would make it possible for applications to implement custom
allocation policies for different objects, including slab allocations,
separate accounting for different types, etc.

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

Additionally, we can also consider that other companies even smaller
than Cockroach Labs have invested in a custom Go fork before us (see example below).

## Related work

### Tailscale's custom Go patches

As per [this Twitter
message](https://twitter.com/bradfitz/status/1360293997038637058) from
Brad Fitzpatrick, Tailscale is using a custom Go fork to fix issues
with the networking code and perhaps other patches.

That tweet in turn refers to example use [here](https://github.com/tailscale/tailscale/commit/88586ec4a43542b758d6f4e15990573970fb4e8a).

These patches do not relate to resource consumption, however it is
interesting to see that Tailscale found it a worthwhile investment
manage a custom fork of Go.

For reference, Tailscale was founded in 2019 and has a smaller
engineering team than Cockroach Labs at the time of this writing.

### The `gotcha` package

Kostiantyn Masliuk created the
[`gotcha`](https://github.com/1pkg/gotcha) package which overrides the
Go heap allocator function `mallocgc` at runtime (i.e. does not need a
custom Go build), to provide statistics at the level of individual
goroutines.

Explanatory article: [Let's trace goroutine allocated
memory](https://1pkg.github.io/posts/lets_trace_goroutine_allocated_memory/)

The way this works is that it catches all calls to `mallocgc`, then
retrieves a struct from the goroutine's local storage (provided by
separate packages [`golocal`](github.com/1pkg/golocal) and
[`gls`](github.com/modern-go/gls)), then increments counters in that
struct with the requested object size.

Deallocations are not tracked, so this only provides **total
cumulative allocated size**, not *current* allocated size.

Also the data is hard to retrieve/interpret once the goroutines have
terminated.

Also the way `mallocgc` is overridden is extremely unsafe,
platform-specific and version-specific (to one version of Go).

### Related work in the Go project

- [Proposal: add a way for applications to respond to GC
  backpressure](https://github.com/golang/go/issues/29696).

  The person who posted the issue remarked that without GC
  backpressure, applications cannot respond to memory exhaustion.

  The discussion then went on and focused on a solution that posts GC
  events on a Go channel. The assumption is that a single listener in
  the application code would be able to respond to GC pressure events.

  **This approach is flawed** for CockroachDB's needed because it does
  not make it possible to stop or suspend individual SQL queries when
  memory exhaustion is reached.

  It also does not make it possible to set different memory budgets
  for different SQL sessions or applications.

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



## Rationale and Alternatives

There are two main alternatives to the work considered here:

- manual instrumentation everywhere.
- pprof-based CPU and memory reporting.

Both are outlined below.

### Manual instrumentation

The idea is to manually instrument every single entry point in Go
where "CPU time" or "memory usage" can be incurred.

- For memory: extend use of the existing `mon.BytesMonitor` to every
  heap allocation performed on behalf of a SQL query or session. Track
  down the end of the lifetime for every allocated object, to perform
  the corresponding `Close` / `Shrink` calls.

  Problems with this approach:

  - a `Shrink` call does not guarantee the memory is free for another
    task to take, as described in the problem statement at the top:
    until the GC runs, it is still possible for the process to crash
    even though the bytes monitor count memory as “available.”
  - engineers will forget to add the memory accounting.
  - tracking the exact lifetimes will add complexity to the programming process.
  - the accounting will make the code harder to read and to maintain.

- For CPU usage: add an "increment CPU time" call into every function
  that performs work on behalf of a SQL query or session. This also needs
  to be called inside every loop, once per iteration.

  Problems with this approach:

  - engineers will forget to add the CPU accounting.
  - the accounting will make the code harder to read and to maintain.
  - the tracking will be imprecise and it will be impossible to distinguish
    a function that performs a lot of work without a loop, from one
    that is simply waiting.

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

**Why this approach is unsuitable:**

- The profiling causes frequent interruptions of the goroutines, which
  impairs performance. It also trashes CPU caches and generally slows
  down processing for memory-heavy workloads.

- It is only able to *observe* CPU usage. It cannot stop tasks from
  consuming more CPU.

- It only measures CPU usage during the goroutine interrupts. To get a
  more precise measurement requires increasing the profiling
  frequency, which impairs performance more.

- The CPU usage output is formatted for consumption by human eyes. It
  is difficult and expensive to re-format so that it can be inspected
  using an API.

# Explain it to someone else

We enhanced CockroachDB's ability to track hardware resource usage at
the level of SQL queries and session.

We achieved this by implementing custom changes to the runtime system
of the Go language. This means that a custom Go runtime is now needed
to build CockroachDB.

It is still possible to build CockroachDB without these custom
patches, however that will prevent CockroachDB from performing precise
resource tracking and cause certain features to become disabled.


# Unresolved questions

- How to ensure that CockroachDB can build and run with a standard
  Go distribution not equipped with the proposed API? What are the
  exact build steps to make this possible?

  (We accept that the resource monitoring benefits would not be available
  when the custom extension is not used.)

- Do we want to extend the approach here to also measure resource
  usage incurred by KV-level goroutines? How would that work?
