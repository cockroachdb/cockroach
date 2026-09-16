- Feature Name: Task groups in Go
- Status: draft
- Start Date: 2021-02-08
- Authors: knz, tbg, peter
- RFC PR: [#60589](https://github.com/cockroachdb/cockroach/pull/60589)
- Cockroach Issue: [#59998](https://github.com/cockroachdb/cockroach/issues/59998)

# Summary

This mini-RFC introduces several extensions to the low-level
concepts inside the Go language, including a notion of “task group”,
by means of a customization to the Go runtime system.

The proposal creates an API which is going to be available for both
customized and non-customized (“vanilla”) Go runtimes. With a vanilla
runtime, the API will provide  no-op behavior. This retains the ability
for community members to produce custom CockroachDB builds without
the hassle of setting up a custom Go installation.


This mini-RFC is a companion to the two other RFCs that propose to
expose (and later control) CPU and RAM usage per SQL session.
Refer to these two other texts for a wider context.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Motivation](#motivation)
- [Technical design](#technical-design)
    - [Overview](#overview)
    - [Proof of concept](#proof-of-concept)
    - [Inheritable Goroutine IDs](#inheritable-goroutine-ids)
    - [Task group abstraction](#task-group-abstraction)
        - [API](#api)
        - [State associated with task groups](#state-associated-with-task-groups)
    - [Implementation of the IGID and task group abstractions](#implementation-of-the-igid-and-task-group-abstractions)
    - [Other runtime customizations](#other-runtime-customizations)
        - [Goroutine observability](#goroutine-observability)
        - [Manual control over the Go scheduler](#manual-control-over-the-go-scheduler)
        - [Choose panic behavior on fault](#choose-panic-behavior-on-fault)
        - [Choose output for runtime errors](#choose-output-for-runtime-errors)
- [Related work](#related-work)

<!-- markdown-toc end -->


# Motivation

There are several objectives wrt resource usage observability and
control which cannot be achieved using the standard APIs from the Go
runtime. We wish to enhance CockroachDB in that direction.

In the past, we were reluctant to mandate custom Go runtimes because
Cockroach Labs was heavily reliant on bottom-up adopters using the
source repository directly. Since the start of CockroachdB's history
however, we now know that the majority of users download pre-built
binaries (either standalone or via Docker container images). It has
thus become reasonable for Cockroach Labs to standardize a set of Go
patches that would be automatically applied in the release process to
produce “official” binaries.

# Technical design

In a nutshell, the proposed change adds new APIs to Go's `runtime` package to
create new abstractions, including new APIs inside the `runtime` package.

When doing so however, we need to be careful to ensure that Go programs
like CockroachDB can continue to build using non-customized rutime systems.

To achieve this, we also provide a Go library shim called `go-plus`, which
exposes the same API to Go programs regardless of whether the Go runtime
is customized or not.

Internally, the `go-plus` library switches behavior based on a (new) build
tag `goplus`. The customized go compiler/runtime defines this tag implicitly
and automatically, whereas a vanilla compiler would not.

## Overview


At the time of this writing, we envision two API layers:

- **proc** provides Go programs with finer grain control over the Go
  runtime system. This makes it possible to build more "advanced"
  concurrency data structures such as a custom sync.Pool
  implementation.

  It also provides a new runtime feature called "Inheritable Goroutine ID" (IGID),
  further described below.

  API doc link: pkg.go.dev/github.com/cockroachdb/go-plus/proc

- **taskgroup** provides Go programs with a new runtime abstraction
  called “task group”, to provide aggregate accounting of resource
  usage to fleets of related goroutines. This is also further detailed below.

  API doc link: pkg.go.dev/github.com/cockroachdb/go-plus/taskgroup

## Proof of concept

The proposed Go runtime extensions are available on a branch `crdb-fixes`
on a fork of the Go runtime at
https://github.com/cockroachdb/go/commits/crdb-fixes

The shim adapter library `go-plus` is available at
https://github.com/cockroachdb/go-plus

The auto-generated Go documentation for the extension is available here:

https://pkg.go.dev/github.com/cockroachdb/go-plus/proc

https://pkg.go.dev/github.com/cockroachdb/go-plus/taskgroup

## Inheritable Goroutine IDs

One of the proposed abstractions is the Inheritable Goroutine ID, a
64-bit integer which, once set on a goroutine, is automatically
inherited by all goroutines spawned from it.

This makes it possible to associate e.g. a group of goroutines
to a SQL query or session ID and report it in logs and traces.

This clarifies when looking at goroutine dumps how the goroutines are
related to each other.

Relevant APIs in the `proc` sub-package of `go-plus`:

```go
package proc

// GetIGID retrieves the inheritable goroutine ID (IGID).
// This is inherited from the goroutine's parent.
// Top-level goroutines are assigned their own ID as group ID.
//
// Returns 0 if the extension is not supported.
func GetIGID() int64


// SetIGID sets the current goroutine's inheritable
// goroutine ID.
// This value is inherited to children goroutines.
//
// A possible good value to use is the value of GetGID(),
// to obtain a behavior similar to process group IDs on unix.
//
// No-op if the extension is not supported.
func SetIGID(igid int64)
```

## Task group abstraction

This proposes a new “task group” abstraction inside the Go runtime /
scheduler, in addition to the existing G / M / P abstractions.

(The reader of this RFC is expected to have some familiarity with the
G / M / P abstractions already. Refer to the Go team's own
documentation and the top comment inside the `runtime/proc.go` file in
the Go distribution.)

A task group is a collection of zero or more goroutines.

Each goroutine is associated to exactly one task group.

There is a “default” task group defined initially for all goroutines
spawned from the main/initial goroutine.

Each new goroutine (started with the `go` keyword) inherits the task
group of its parent.

A `taskgroup.SetTaskGroup()` API enables Go applications to override the
task group of the current goroutine. After this has been overridden,
all further goroutines spawned from that point inherit the new task
group. (But all previously spawned goroutines retain their previously
defined task group.)

The Go runtime scheduler is aware of the task groups and is able to
update / maintain runtime counters and other metrics at the level
of the task group, not just the goroutine.

### API

```
package taskgroup
// T represents a task group inside the Go runtime.
//
// T values are reference-like, are comparable and can be nil, however
// no guarantee is provided about their concrete type.
type T

// SetTaskGroup creates a new task group and attaches it to the
// current goroutine. It is inherited by future children goroutines.
// Top-level goroutines that have not been set a task group
// share a global (default) task group.
//
// If the extension is not supported, the operation is a no-op
// and returns a nil value.
func SetTaskGroup() T

// ReadTaskGroupMetrics reads the runtime metrics for the specified
// task group. This is similar to the Go standard metrics.Read() but
// reads metrics scoped to just one task group.
//
// The following metric names are supported:
//
//   /taskgroup/sched/ticks:ticks
//   /taskgroup/sched/cputime:nanoseconds
//   /taskgroup/heap/largeHeapUsage:bytes
//
// If the extension is not supported, the metric value will be
// initialized with metric.KindBad.
//
// The reason why tg metrics are served by a dedicated function is
// that this function only locks the data structures for the specified
// task group, not the entire runtime. This makes it generally cheaper
// to use in a concurrent environment than metrics.Read().
func ReadTaskGroupMetrics(taskGroup T, m []metrics.Sample)
```

### State associated with task groups

There is an (opaque) type `taskgroup.T`.  It is also the state
produced via the main new API `taskgroup.SetTaskGroup()` outlined
above.

This is the struct used inside the Go scheduler and other runtime
subsystems (e.g. the heap GC in the future).

This struct is associated with various counters and other metrics
that are updated by the Go scheduler.

At this time, the following are maintained separately per task group:

- CPU scheduling ticks.
- CPU effective runtime nanoseconds.
- number of discrete heap allocations (cumulative).
- currently allocated size of large (>32KB) heap allocations.

The metrics per task group can be extracted using the
new `taskgroup.GetTaskGroupMetrics()` API.

## Implementation of the IGID and task group abstractions

Implementation-wise, we extend the `g` struct inside the go runtime
with two fields `taskGroupCtx` (for the internal task group)
and `groupId` (for the IGID).

Both are inherited when creating a new goroutine. When a goroutine is
created without a parent, they are initialized as follows:

- `taskGroupCtx` is set to `defaultTaskGroupCtx`, the default/common
  task group.
- `groupId` is set to the goroutine's ID (this default may change, unsure).

These defaults are not too important/interesting since we will ensure
that the task group is overridden every time we create a new SQL
session or job.

## Other runtime customizations

### Goroutine observability

```go
package proc

// GetGID retrieves the goroutine (G's) ID.
//
// This works even when the extension is not supported,
// by relying on github.com/petermattis/goid.
func GetGID() int64

// GetGoroutineStackSize retrieves an approximation of the current goroutine (G)'s
// stack size.
//
// Returns 0 if the extension is not supported.
func GetGoroutineStackSize() uintptr
```

### Manual control over the Go scheduler

```go
package proc

// GetPID retrieves the P (virtual CPU) ID of the current goroutine.
// These are by design set between 0 and gomaxprocs - 1.
// This can be used to create data structures with CPU affinity.
//
// Note: because of preemption, there is no guarantee that the
// goroutine remains on the same P after the call to GetPID()
// completes. See Pin/UnpinGoroutine().
//
// Returns 0 if the extension is not supported.
func GetPID()

// GetOSThreadID retrieves the OS-level thread ID, which can be used to e.g.
// set scheduling policies or retrieve CPU usage statistics.
//
// Note: because of preemption, there is no guarantee that the current
// goroutine remains on the same OS thread after a call to this
// function completes. See Pin/UnpinGoroutine.
//
// Returns 0 if the extension is not supported.
func GetOSThreadID() uint64

// PinGoroutine pins the current G to its P and disables preemption.
// The caller is responsible for calling Unpin. The GC is not running
// between Pin and Unpin.
// Returns the ID of the P that the G has been pinned to.
//
// Returns -1 if the extension is not supported.
func PinGoroutine()

// UnpinGoroutine unpints the current G from its P.
// The caller is responsible for ensuring that PinGoroutine()
// has been called first. The behavior is undefined
// otherwise.
//
// No-op if the extension is not supported.
func UnpinGoroutine()
```

### Choose panic behavior on fault

This enables telling the goroutine to throw panic objects instead of
stopping the entire program:

```go
package proc

// SetDefaultPanicOnFault sets the default value of the "panic on
// fault" that is otherwise customizable with debug.SetPanicOnFault.
//
// The default value is used for "top level" goroutines that are
// started during the init process / runtime. The flag is inherited to
// children goroutines.
// When not customized, the go runtime uses "false" as default.
//
// Returns false if the extension is not supported.
func SetDefaultPanicOnFault(enabled bool)

// GetDefaultPanicOnFault retrieves the default value of the "panic on
// fault" flag.
//
// Returns false if the extension is not supported.
func GetDefaultPanicOnFault()
```

### Choose output for runtime errors

```go
package proc

// GetExternalErrorFd retrieves the file descriptor used to emit
// panic messages and other internal errors by the go runtime.
//
// Returns -1 if the extension is not supported.
func GetExternalErrorFd()

// SetExternalErrorFd changes the file descriptor used to emit panic
// messages and other internal errors by the go runtime.
//
// No-op if the extension is not supported.
func SetExternalErrorFd(fd int)
```

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
than Cockroach Labs have invested in a custom Go fork before us.

# Related work

Some related work is described below, as well as in the two companion
RFCs on CPU and RAM usage.

However, all this related work suffers from the same limitation: **they
assume that the desired observability granularity is one goroutine**.

In CockroachDB, the unit of processing is a group of goroutines
performing work on behalf of a SQL query (or, inside KV, on behalf of
a range queue, or on behalf of a KV client).

Hence the need for an abtraction that groups goroutines together.

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

Some additional points:

1. the Tailscale team attempts to upstream everything. Most of their
    fork is "this will be in the next Go version but I wanted it now",
    so their fork is correspondingly small.
2. It is unlikely that what we are proposing in the RFC will ever be
   upstreamed. Our fork will possibly be much larger than theirs.

Also, Tailscale has a smaller engineering team, but they have more Go core expertise.

# Rationale and Alternatives

See discussion in related RFCs on CPU and RAM usage tracking.

# Unresolved questions

None known.
