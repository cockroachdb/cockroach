# Admission Control

Author: Sumeer Bhola


## Goals

Admission control for a system decides when work submitted to that
system begins executing, and is useful when there is some resource
(e.g. CPU) that is saturated. The high-level goals of admission
control in CockroachDB are to control resource overload such that it
(a) does not degrade throughput or cause node failures, (b) achieve
differentiation between work with different levels of importance
submitted by users, and (c) allow for load-balancing among data
replicas (when possible).

For CockroachDB Serverless, where the shared cluster only runs the KV
layer and below, admission control also encompasses achieving fairness
across tenants (fairness is defined as equally allocating resources,
e.g. CPU, across tenants that are competing for resources).
Multi-tenant isolation for the shared cluster is included in the scope
of admission control since many of the queuing and re-ordering
mechanisms for work prioritization overlap with those for inter-tenant
isolation.

Even though we do not discuss per-tenant SQL nodes in the remainder of
this document, the overload control mechanisms discussed below could
potentially be applied in that context, though the code for it is
incomplete. The scope of admission control excludes per-tenant cost
controls, since per-tenant cost is not a system resource.

The current implementation focuses on node-level admission control,
for CPU and storage IO (specifically writes) as the bottleneck
resources. The focus on node-level admission control is based on the
observation that large scale systems may be provisioned adequately at
the aggregate level, but since CockroachDB has stateful nodes,
individual node hotspots can develop that can last for some time
(until rebalancing). Such hotspots should not cause failures or
degrade service for important work (or unfairly for tenants that are
not responsible for the hotspot).

Specifically, for CPU the goal is to shift queueing from inside the
goroutine scheduler, where there is no differentiation, into various
admission queues, where we can differentiate. This must be done while
allowing the system to have high peak CPU utilization. For storage IO,
the goal is to prevent the log-structured merge (LSM) tree based
storage layer (Pebble) from getting into a situation with high read
amplification due to many files/sub-levels in level 0, which slows
down reads. This needs to be done while maintaining the ability to
absorb bursts of writes. Both CPU overload and high read amplification
in the LSM are areas where we have seen problems in real CockroachDB
clusters.

The notable omission here is memory as a bottleneck resource for
admission control. The difficulty is that memory is non-preemptible
(mostly, ignoring disk spilling), and is used in various layers of the
system, and so slowing down certain activities (like KV processing)
may make things worse by causing the SQL layer to hold onto memory it
has already allocated. We want forward progress so that memory can be
released, and we do not know what should make progress to release the
most memory. We also currently do not have predictions for how much
memory a SQL query will consume, so we cannot make reasonable
reservations to make up for the non-preemptibility.

## High-level Approach


### Ordering Tuple

Admission queues use the tuple **(tenant, priority, transaction start
time)**, to order items that are waiting to be admitted. There is
coarse-grained fair sharing across tenants (for the multi-tenant
shared cluster). Priority is used within a tenant, and allows for
starvation, in that if higher priority work is always consuming all
resources, the lower priority work would wait forever. The transaction
start time is used within a priority, and gives preference to earlier
transactions. We currently do not have a way for end-users to assign
priority to their SQL transactions. We also currently do not support
multi-tenancy in dedicated clusters, even though many such customers
would like to distinguish between different internal tenants. Both
these limitations could be addressed by adding the requisite
plumbing/integration code.


### Possible solution for CPU Resource with scheduler change

Let us consider the case of CPU as a bottleneck resource. If we had
the ability to change the goroutine scheduler we could associate the
above ordering tuple with each goroutine and could allocate the CPU
(P) slots to the runnable goroutine that should be next according to
that tuple. Such a scheme does not need to make any guesses about
whether some admitted work is currently doing useful work or blocked
on IO, since there is visibility into that state inside the
scheduler. And if we are concerned about starting too much work, and
not finishing already started work, one could add a **started**
boolean as the first element of the tuple and first give preference to
goroutines that had already started doing some work (there is a draft
Cockroach Labs [internal
doc](https://docs.google.com/document/d/18S4uE8O1nRxULhSg9Z1Zt4jUPBiLJgMh7X1I6shsbug/edit#heading=h.ssc9exx0epqo)
that provides more details). However, we currently do not have the
ability to make such scheduler changes. So we resort to more indirect
control as outlined in the next section.

### Admission for CPU Resource

#### Kinds of Work and Queues

There are various kinds of work that consume CPU, and we add admission
interception points in various places where we expect the
post-admission CPU consumption to be significant and (somewhat)
bounded. Note that there is still extreme heterogeneity in work size,
that we are not aware of at admission time, and we do not know the
CPU/IO ratio for a work unit. Specifically, the interception points
are:

- **KV**: KV work admission, specifically the
  `roachpb.InternalServer.Batch` API implemented by
  [`Node`](https://github.com/cockroachdb/cockroach/blob/d10b3a5badf25c9e19ca84037f2426b03196b2ac/pkg/server/node.go#L938). This
  includes work submitted by SQL, and internal operations like
  garbage collection and node heartbeats.

- **SQL-KV**: Admission of SQL processing for a response provided by
  KV. For example, consider a distributed SQL scan of a large table
  that is being executed at N nodes, where the SQL layer is issuing
  local requests to the KV layer. The response from KV is subject to
  admission control at each node, before processing by the SQL layer.

- **SQL-SQL**: Distributed SQL runs as a two-level tree where the
  lower level sends back responses to the root for further
  processing. Admission control applies to the response processing
  at the root.

Each of these kinds of work has its own admission queue. Work is
queued until admitted or the work deadline is exceeded. Currently
there is no early rejection when encountering long queues. Under
aggressive user-specified deadlines throughput may collapse because
everything exceeds the deadline after doing part of the work. This is
no different than what will likely happen without admission control,
due to undifferentiated queueing inside the goroutine scheduler, but
we note that this is a behavior that is not currently improved by
admission control.

The above list of kinds are ordered from lower level to higher level,
and also serves as a hard-wired ordering from most important to least
important. The high importance of KV work reduces the likelihood that
non-SQL KV work will be starved. SQL-KV (response) work is prioritized
over SQL-SQL since the former includes leaf DistSQL processing and we
would like to release memory used up in RPC responses at lower layers
of RPC tree. We expect that if SQL-SQL (response) work is delayed, it
will eventually reduce new work being issued, which is a desirable
form of natural backpressure. Note that this hard prioritization
across kinds of work is orthogonal to the priority specified in the
ordering tuple, and would ideally not be needed (one reason for
introducing it is due to our inability to change the goroutine
scheduler).

Consider the example of a lower priority long-running OLAP query
competing with higher priority small OLTP queries in a single node
setting. Say the OLAP query starts first and uses up all the CPU
resource such that the OLTP queries queue up in the KV work
queue. When the OLAP query's KV work completes, it will queue up for
SQL-KV work, which will not start because the OLTP queries are now
using up all available CPU for KV work. When this OLTP KV work
completes, their SQL-KV work will queue up. The queue for SQL-KV will
first admit those for the higher priority OLTP queries. This will
prevent or slow down admission of further work by the OLAP query.

One weakness of this prioritization across kinds of work is that it
can result in priority inversion: lower importance KV work, not
derived from SQL, like GC of MVCC versions, will happen before
user-facing SQL-KV work. This is because the backpressure via SQL-SQL,
mentioned earlier, does not apply to work generated from within the KV
layer. This could be addressed by introducing a **KV-background** work
kind and placing it last in the above ordering.

#### Slots and Tokens

The above kinds of work behave differently in whether we know a work
unit is completed or not. For KV work we know when the admitted work
completes, but this is not possible to know for SQL-KV and SQL-SQL
work because of the way the execution code is structured. Knowing
about completion is advantageous since it allows for better control
over resource consumption, sine we do not know how big each work unit
actually is.

We model these two different situations with different ways of
granting admission: for KV work we grant a slot that is occupied while
the work executes and becomes available when the work completes, and
for SQL-KV and SQL-SQL we grant a token that is consumed. The slot
terminology is akin to a scheduler, where a scheduling slot must be
free for a thread to run. But unlike a scheduler, we do not have
visibility into the fact that work execution may be blocked on IO. So
a slot can also be viewed as a limit on concurrency of ongoing
work. The token terminology is inspired by token buckets. Unlike a
token bucket, which shapes the rate, the current implementation limits
burstiness and does not do rate shaping -- this is because it is hard
to predict what rate is appropriate given the difference in sizes of
the work.

#### Slot Adjustment for KV

The current implementation makes no dynamic adjustments to token burst
sizes since the lack of a completion indicator and heterogeneity in
size makes it hard to figure out how to adjust these tokens. In
contrast, the slots that limit KV work concurrency are adjusted. And
because KV work must be admitted (and have no waiting requests) before
admission of SQL-KV and SQL-SQL work, the slot adjustment also
throttles the latter kinds of work.

We monitor the following state of the goroutine scheduler: the number
of processors, and the number of goroutines that are runnable (i.e.,
they are ready to run, but not running). The latter represents
queueing inside the goroutine scheduler, since these goroutines are
waiting to be scheduled.  KV work concurrency slots are adjusted by
gradually decreasing or increasing the total slots (additive
decrements or increments), when the runnable count, normalized by the
number of processors, is too high or too low. The adjustment also
takes into account current usage of slots. The exact heuristic can be
found in `admission.kvSlotAdjuster`. It monitors the runnable count at
1ms intervals. The motivation for this high frequency is that sudden
shifts in CPU/IO ratio or lock contention can cause the current slot
count to be inadequate, while leaving the CPU underutilized, which is
undesirable.


#### Instantaneous CPU feedback and limiting bursts

Tokens are granted (up to the burst size) for SQL-KV when the KV work
queue is empty and the CPU is not overloaded. For SQL-SQL the
additional requirement is that the SQL-KV work queue must also be
empty. It turns out that using the runnable goroutine count at 1ms
intervals, as a signal for CPU load, is insufficient time granularity
to properly control token grants. So we use two instantaneous
indicators:

- CPU is considered overloaded if all the KV slots are utilized.

- Tokens are not directly granted to waiting requests up to the burst
  size. Instead we setup a "grant chaining" system where the goroutine
  that is granted a token has to run and grant the next token. This
  gives instantaneous feedback into the overload state.  In an
  experiment, using such grant chains reduced burstiness of grants by
  5x and shifted ~2s of latency (at p99) from the goroutine scheduler
  into admission control (which is desirable since the latter is where
  we can differentiate between work).

### Admission for IO Resource

KV work that involves writing to a store is additionally subject to a
per-store admission queue. Admission in this queue uses tokens
(discussed below), and happens before the KV work is subject to the KV
CPU admission queue (which uses slots), so that we do not have a
situation where a KV slot is taken up by work that is now waiting for
IO admission.

KV work completion is not a good indicator of write work being
complete in the LSM tree. This is because flushes of memtables, and
compactions of sstables, which are the costly side-effect of writes,
happen later. We use "IO tokens" to constrain how many KV work items
are admitted. There is no limit on tokens when the LSM is healthy. Bad
health is indicated using thresholds on two level 0 metrics: the
sub-level count and file count. We do not consider other metrics for
health (compaction backlog, high compaction scores on other levels
etc.) since we are primarily concerned with increasing
read-amplification, which is what will impact user-facing traffic. It
is acceptable for the LSM to deteriorate in terms of compaction scores
etc. as long as the read-amplification does not explode, because
absorbing write bursts is important, and write bursts are often
followed by long enough intervals of low activity that restore the LSM
compaction scores to good health.

When the LSM is considered overloaded, tokens are calculated by
estimating the average bytes added per KV work, and using the outgoing
compaction bytes to estimate how many KV work items it is acceptable
to admit. This detailed logic can be found in
`admission.ioLoadListener` which generates a new token estimate every
15s. The 15s duration is based on experimental observations of
compaction durations in level 0 when the number of sub-levels
increases beyond the overload threshold. We want a duration that is
larger than most compactions, but not too large (for
responsiveness). These tokens are given out in 1s intervals (for
smoothing). The code has comments with experimental details that
guided some of the settings.

### Priority adjustments and Bypassing admission control

KV work that is not directly issued by SQL is never queued, though it
does consume a slot, which means the available slots can become
negative. This is a simple hack to prevent distributed deadlock that
can happen if we queue KV work issued due to other KV work. This will
need to be changed in the future to also queue low priority KV
operations (e.g. GC can be considered lower priority than user facing
work).

Transactions that are holding locks, or have ongoing requests to
acquire locks, have their subsequent work requests bumped to higher
priority. This is a crude way to limit priority inversion where a
transaction holding locks could be waiting in an admission queue while
admitted requests are waiting in the lock table queues for this
transaction to make progress and release locks. Such prioritization
can also fare better than a system with no admission control, since
work from transactions holding locks will get prioritized, versus no
prioritization in the goroutine scheduler. A TPCC run with 3000
warehouses showed 2x reduction in lock waiters and 10+% improvement in
transaction throughput with this priority adjustment compared to no
priority adjustment. See
https://github.com/cockroachdb/cockroach/pull/69337#issue-978534871
for comparison graphs.

### Tuning Knobs

Enabling admission control is done via cluster settings. It is
currently disabled by default. There are three boolean settings,
`admission.kv.enabled`, `admission.sql_kv_response.enabled`,
`admission.sql_sql_response.enabled` and we have only experimented
with all of them turned on.

There are also some advanced tuning cluster settings, that adjust the
CPU overload threshold and the level 0 store overload thresholds.

### Results

There are certain TPCC-bench and KV roachtests running regularly with
admission control enabled (they have "admission" in the test name
suffix). The TPCC performance is roughly equivalent to admission
control disabled. Certain KV roachtests that used to overload IO, but
were not running long enough to show the bad effects, are worse with
admission control enabled when comparing the mean throughput. However
the runs with admission control enabled are arguably better since they
do not have a steadily worsening throughput over the course of the
experimental run.

For some graphs showing before and after effects of enabling admission control see:

- CPU overload:
  https://github.com/cockroachdb/cockroach/pull/65614#issue-651424608
  and
  https://github.com/cockroachdb/cockroach/pull/66891#issue-930351128

- IO overload:
  https://github.com/cockroachdb/cockroach/pull/65850#issue-656777155
  and
  https://github.com/cockroachdb/cockroach/pull/69311#issue-978297918
