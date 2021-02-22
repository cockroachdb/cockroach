This prototype demonstrates how, with a modest technical investment, we can
improve make the CPU utilization observable on a granular level (SQL sessions,
statements, etc) using Goroutine labels and the Go CPU profiler

[Go profiler labels]: https://rakyll.org/profiler-labels/

Before we dive into what the prototype does, here's a status quo. We are already supporting profiler labels to some degree, though in an unpolished form that is hard to capitalize on. Concretely, we set profiler labels here:

https://github.com/cockroachdb/cockroach/blob/401e9d4b9da1aa074a8083922f13c3d4bcaf4d41/pkg/sql/conn_executor_exec.go#L105-L121

This snippet also shows that we have infrastructure that tells us when to
enable Goroutine labels, so that we only do so if the process is actively being
traced (with labels requested). This is relevant because Goroutine labels are
far from free. The implementation in the runtime is not very allocation
efficient: you need to wrap a map in a Context, and the runtime makes an
additional copy of this map internally. The internal copy cannot be avoided,
but many of the surrounding allocations could be with appropriate pooling and
perhaps a custom impl of `context.Context` that we use for the sole purpose of
passing the map into the runtime. Either way, the take-away is that getting the
overhead down will be important if we are to rely on this mechanism more, as it
would be a shame to make the system a lot slower when trying to find out why it
is so slow in the first place.
To summarize the status quo, we are also already taking label-enabled profiles
of all nodes in the cluster in debug.zip, and also add a script to show a breakdown
by tags. As far as I know, these have never really been used.

The goal:

Given a cluster in which (one or many) nodes experience high CPU usage, a TSE/SRE
can easily identify

1. whether the high CPU usage is primarily driven by query load, and if so,
2. which user/session/query is responsible for that load, so that they can
3. alert the user and/or throttle or blocklist it (not included in the prototype)

Stretch goal (not included but could be worth thinking about)

- reduce the overhead of the proposed mechanism so that we can periodically enable
  it (at low profiling frequency), and
- also offer CPU breakdown per query/txn. Now imagine that we also persist historical
  statement/txn stats (maybe just store it before the hourly reset), it seems pretty
  powerful.

What this prototype does concretely is focus only on 1) and 2) with no attempt to
optimize performance.

- Try to ensure we always allocate all proper labels to everything. In the process of that:
- Strip out the "only if requested" logic, to simplify things
- Add a dummy marker label in cli/start which allows us to look at a profile and figure out
  if there's any work done on behalf of the dummy marker, which would indicate that we were
  not properly labelling everywhere.
- hoist the tracing in connExecutor.execStmt into the connExecutor main loop. I did this
  because I realized that significant chunks of CPU time are spent outside of `execStmt`,
  and most of the work in the prototype was to arrive at a system that captured enough of
  the work so that we could comfortably "deduce" that a query is responsible for overload.
- write gRPC interceptors that propagate labels across RPC boundaries. This is mostly for
  distributed SQL flows: if the profile stopped at the gateway, but the CPU was spent on
  another node, it wouldn't be associated to the query. Note that all nodes need to actively
  profile (or at least attach goroutine labels) at around the same time, or the links will
  be lost.
- disable the "local node" fastpath for KV RPCs because it's ad-hoc and
  bypasses the interceptors; it would need a more holistic solution before shipping.

Learnings from the prototype:

- Go intentionally made the labels API such that you couldn't derive Goroutine-local storage from it. On top of the allocation inefficiency, we also need to always keep the `context.Context` and the actual labels in sync manually (as the gRPC interceptors can only get the labels from there; they can't read them off the goroutine). Labels automatically (via the runtime) get propagated to child goroutines, but we often don't do the same thing for `Context` - because doing so also propagates the cancellation. The prime example of this is `stopper.RunAsyncTask`, where we often derive from `context.Background` for the task. We need to change the Stopper to automatically put the labels of the parent goroutine into the children it starts, which is not too difficult. Next, this project would lend further weight to [ban the Go keyword]; the Stopper should be our authority here.
- In a fully loaded system, the contributions from "our" goroutines aren't usually 100%. There is one clear outlier which we can fix: pebble. It has its own goroutine label, which is fine, but we should massage it so that it fits in with whatever labelling categories we want to productionize here (if any). Since pebble is third party, likely we'll just want it to give us a hook to determine the labels rather than hard-coding its own. But even pebble aside, runtime-internal goroutines tend to eat a good chunk of CPU when busy, for example for GC scans. This raises the interesting question about 1) at the top of this message - given the CPU time associated to our labels, how can we determine if it's enough to claim that it's overloading the machine? Clearly if a 30s sample displays only 1s of total CPU, the system is not busy. A 30s sample on a 4vcpu machine could top out at around 120s total. If we account for 60s of that, the system seems very busy, but we only account for 50%. What does that mean? So there will need to be some guidance around this to help SRE/TSE make judgment calls. I think what it will boil down to is: if the total samples account for close to `num_vcpus * profile_duration`, then the system is pretty much overloaded (they should also see that from the runtime metrics). Since we promise that we are putting labels on all moving parts, we can assume that the seconds accrued by those labels are "100% of the productive work" and that this is the whole category to look at. But this needs to be verified experimentally. It could be possible for some SQL query to be low-CPU but extremely alloc heavy, so that it would drive the garbage collector nuts.
- The goroutine profile also uses labels, so it can be used to check whether we're labelling all of the goroutines, simply by restricting to goroutines which don't have a certain label that we expect to be present on all of our processes.
- It's possible to grab the profile in the process and to then evaluate it, so it shouldn't be too hard to have a button in the UI that populates the stmt table with a CPU percentage breakdown.
- We need a little bit of convention around these labels, i.e. we need to wrap the labels API into our custom thing that is more strongly typed.

[ban the Go keyword]: https://github.com/cockroachdb/cockroach/issues/58164
