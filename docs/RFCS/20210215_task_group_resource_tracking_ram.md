- Feature Name: Task groups in Go: per-session resource usage tracking (RAM)
- Status: draft
- Start Date: 2021-02-08
- Authors: knz, tbg, peter
- RFC PR: [#60589](https://github.com/cockroachdb/cockroach/pull/60589)
- Cockroach Issue: [#59998](https://github.com/cockroachdb/cockroach/issues/59998)


# Summary

This RFC proposes to use custom extensions to the Go runtime to track
RAM usage, as well as other RAM-related metrics, per SQL session -- including all
goroutines that do work on behalf of a SQL session, and without
instrumenting the code manually inside CockroachDB.

In a later stage, this work will also enable constraining
CPU usage and other resources according to configurable quota.

Note: there is a [separate
RFC](20210215_task_group_resource_tracking_cpu.md) for observing and
restricting CPU usage.

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
    - [Context: how the Go heap allocator works](#context-how-the-go-heap-allocator-works)
    - [Tweaking the Go heap allocator for precise measurement](#tweaking-the-go-heap-allocator-for-precise-measurement)
    - [Proof of concept: tracking large heap allocs precisely](#proof-of-concept-tracking-large-heap-allocs-precisely)
    - [How to account for shared data structures (e.g. caches)](#how-to-account-for-shared-data-structures-eg-caches)
    - [Possible future enhancement: custom allocators](#possible-future-enhancement-custom-allocators)
    - [Later benefit: resource control (not just measurement)](#later-benefit-resource-control-not-just-measurement)
    - [Related work](#related-work)
        - [The `gotcha` package](#the-gotcha-package)
        - [Related work in the Go project](#related-work-in-the-go-project)
    - [Rationale and Alternatives](#rationale-and-alternatives)
        - [Manual instrumentation](#manual-instrumentation)
        - [Separate processes](#separate-processes)
- [Explain it to someone else](#explain-it-to-someone-else)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->


# Motivation

Today there is an issue when RAM usage is high, it is  difficult
to track down which SQL session or query is responsible for that load.

In turn the difficulty comes from the Go runtime which does not provide
an abstraction to extract metrics per group of goroutines performing
related work on behalf of a common client-defined query or session.

Even though we are also considering process-level separation (e.g. one
separate SQL process per tenant), users still wish to know which
queries use the most RAM within the same CockroachDB process.

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

And an example application inside CockroachDB, to measure large heap
allocations (>32KB) separately for each SQL session, is available
here:

https://github.com/cockroachdb/cockroach/pull/60588

## Context: how the Go heap allocator works

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

## Tweaking the Go heap allocator for precise measurement

We can tweak the allocator as follows:

- the `mspan` for large allocations are tagged with a pointer to their
  `taskGroupCtx`. We increment the allocation tally in `taskGroupCtx`
  when the allocation occurs. When the GC releases the object, its
  size is subtracted from the tally in the linked `taskGroupCtx`.

  This is the simple case. See proof of concept below.

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

**Concern with the proposal above**

The small heap is defined as an array of `numSpanClasses` *size
classes* (default 134), with each span defined as a linked list of
pages to serve allocations of that size.

Creating P times `numSpanClasses` a linked list of 8KB pages, *per
task group*, would incur a potential cost of 1MB per core per task
group, e.g. 16MB per task group on a 16-core machine. If we have one
task group per SQL session, that may be too expensive. For reference,
today an idle SQL session costs less than 100KB.

**Other alternate approaches**

- have small heaps (mcaches) shared more, for example across multiple
  SQL sessions. Only keep the larger allocs separate and billed to
  individual task groups. This would make sense if we assume that the
  bulk of allocations comes from larger objects.

  Implementation-wise that would need a separate abstraction from the
  task group identified here, something like "bags of goroutines that
  share a small cache".

- we could have a separate implementation of small heap dedicated to
  individual SQL queries, that would do stack allocation
  (i.e. everything goes to a single linked list of pages, all tagged
  by the task group). This is OK-ish because we know everything gets
  deallocated in bulk at the end.

In any case this section needs further experiments to narrow down the
most promising approach.

## Proof of concept: tracking large heap allocs precisely

For large allocations, the go heap allocator dedicates entires
`mspans` (the internal unit of accounting) to each allocation.
Because the `mspans` are dedicated, we can do precise accounting as
described below.

NB: An allocation is considered “large” when it is 32KiB or larger
(see `_MaxSmallSize` in `sizeclasses.go`).

How to achieve this:

1. add a new `largeHeapUse` counter in the task group struct.
2. in `(*mcache).allocLarge()`, increment the task group counter
   by the size of the allocation if a task group is defined.
   Also, store a pointer to the *counter* (not the task group - this is not needed)
   inside the `mspan`.
3. in `(*mheap).freeSpanLocked()`, if a counter pointer is present in the
   `mspan`, decrease it by the size of the allocation.

We place the increase in `allocLarge()` and not `allocSpan()` because the latter
is also used for prepopulating mspans in the small heap and this proof of
concept should be limited to large allocations (see discussion above about
the complexities of the small heap).

When this is achieved, we can expose the new counter via the `metrics` API,
for example in crdb's `conn_executor.go`:

```go
	taskGroupMetrics := []metrics.Sample{
	    // ...
		{Name: "/taskgroup/heap/largeHeapUsage:bytes"},
	}
	metrics.ReadTaskGroup(ex.taskGroup, taskGroupMetrics)
```

This is demonstrated in the custom go runtime at https://github.com/cockroachdb/go/commits/crdb-fixes
and the PR at https://github.com/cockroachdb/cockroach/pull/60588

## How to account for shared data structures (e.g. caches)

Some data structures in RAM are populated by one SQL session but the
data remains for use by other SQL sessions. For example the SQL role
membership cache, the descriptor collection, etc. all are "shared resources".

If we account for these allocations as "belonging" to one task group,
that will creates the illusion of imbalance and possibly trigger
assertions that no data leaks from a SQL session when it terminates.

How to deal with this?

We can use a task group for the cache itself. Then we can have the
goroutine temporarily swap in the cache task group for that
allocation, then swap it out (and restore the original goroutine's
task group) after the allocation completes.

This can be done in the crdb code, there is no extra Go runtime
customization needed.

## Possible future enhancement: custom allocators

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

## Related work

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

They are outlined below.

### Manual instrumentation

The idea is to manually instrument every single entry point in Go
where  "memory usage" can be incurred.

For memory, extend use of the existing `mon.BytesMonitor` to every
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
    gateway node directly. These can also cause OOM problems, and
    would not be protected by distsql-specific forks.

Meanwhile, regardless of the approach considered, we have a discussion
about memory budgets. When using multiple processes, do we set limits
and how?

- fixed limits will cause fragmentation.
- fixed limits raise the question: what is a good limit? We do not have a model for that.
- we could also operate without limits. In that case we rely on the
  system OOM killer to only select "offending" processes, and we will
  need to be careful to "protect" the gateway/master processes.

# Explain it to someone else

We enhanced CockroachDB's ability to track RAM resource usage at
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
