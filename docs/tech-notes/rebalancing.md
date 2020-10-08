# Rebalancing

**Last update:** January 2019

**Original author:** Alex Robinson

This document serves as an end-to-end description of the current state of range
and lease rebalancing in Cockroach as of v2.1. The target reader is anyone who
is interested in better understanding how and why replica and lease placement
decisions get made. Little detailed knowledge of core should be needed.

The most complete documentation, of course, is in the code, tests, and the
surrounding comments, but those are necessarily split across several files and
are much tougher to piece together into a logical understanding. That scattered
knowledge is centralized here, without excessive detail that is likely to become
stale.

## Table of Contents

* [Overview](#overview)
* [Considerations](#considerations)
* [Implementation](#implementation)
  * [Replicate Queue](#replicate-queue)
    * [Choosing an action](#choosing-an-action)
    * [Picking an up-replication target](#picking-an-up-replication-target)
    * [Picking a down-replication target](#picking-a-down-replication-target)
    * [Picking a rebalance target](#picking-a-rebalance-target)
    * [Per-replica / expressive constraints](#per-replica--expressive-constraints)
    * [Lease transfer decisions](#lease-transfer-decisions)
  * [Store Rebalancer](#store-rebalancer)
  * [Other details](#other-details)
* [Known issues](#known-issues)

## Overview

Cockroach maintains multiple replicas of each range of data for fault tolerance.
It also maintains a single leaseholder for each range to optimize the
performance of reads and help maintain correctness invariants. The locations of
these replicas and leaseholders are hugely important to the fault tolerance and
performance of the system as a whole, and so Cockroach contains a bunch of logic
that proactively tries to ensure a reasonably optimal distribution of replicas
and leases throughout the cluster.

## Considerations

There are a number of factors that need to be considered when making placement
decisions. For replicas, this includes:

* User-specified constraints in zone configs. These obviously need to be respected.
* Disk fullness. Moving replicas to a store that's out (or nearly out) of disk
  space is clearly a bad idea. Moving replicas away from a store that's nearly
  out of disk space is often a good idea, but not always.
* Diversity of localities. If we put all of the replicas for a range in just one
  or two localities, then a single locality (datacenter, rack, region, etc)
  failure will cause data unavailability / loss. We should try to spread
  replicas as widely as possible for maximal fault tolerance.
* Number of ranges on each store.
* Load on each node. Uneven distribution of load can cause bottlenecks that
  seriously affect the overall throughput of the cluster (e.g. [#26059]).
* Amount of data on each store. We don't want one disk in a cluster to fill up
  long before the others, and it's also valuable for recovery time to be
  roughly the same after any given node failure, which isn't the case if one
  node has significantly more data on it than others.
* Number of writes on each store. Disks have a limited amount of IOPs and
  bandwidth, so bottlenecks can be a problem here as well, at least
  hypothetically.

We currently don't directly use the last two factors, instead hoping that
balancing the overall load and number of ranges are good enough proxies for the
number of writes and the amount of data, respectively. We previously tried to
integrate these factors into decisions, but allocation decisions became quite
complex and the approach ran into a number of issues ([#17979]), so it has since
been removed in favor of the `StoreRebalancer`'s pure QPS-based rebalancing.

For lease rebalancing, the considerations include:

* Lease count on each node. Balancing this should roughly balance out the amount
  of load on each node, assuming a uniform distribution.
* QPS on each node. It turns out that not all workloads have a uniform
  distribution.
* Locality of data access. If most requests to a range are coming from the other
  side of the world, maybe we should move the lease closer to them.

Note that there is a built-in conflict here -- moving leases closer to where
requests are coming from may require imbalancing the lease count, or causing the
nodes in those localities to have more load than other nodes. Getting this right
can be a tough balancing act, and it's hard to ever be fully confident that
you've gotten it right because there are almost certainly workloads out there
that won't be handled optimally by whatever decision-making logic you implement.

## Implementation

Historically, all rebalancing has been handled by the `ReplicateQueue`. As of
v2.1, there's also a separate component called the `StoreRebalancer` which
focuses specifically on the problem of balancing the QPS on each store. QPS is
used here essentially as a proxy for the (CPU/network/disk) load on each node.
It's not a perfect proxy in general, but it seems to work well in benchmarks and
tests.

### Replicate Queue

The `ReplicateQueue` is one of our handful of replica queues which periodically
iterate over all the replicas in each store. Replicas are queued by the
`replicaScanner` on each store, which simply scans over all replicas at a
configurable pace and runs them through each of the replica queues. Replicas
are also sometimes manually queued in the `ReplicateQueue` upon certain
triggers, as will be explained later.

Upon being asked to operate on a replica, the `ReplicateQueue` must:

1. Decide whether the replica's range needs any replica/lease placement changes
2. Decide exactly what change to make
3. Make the change
4. Repeat until no more changes are needed

The main interesting bit here, of course, is how the decisions are made, which
is described in detail in the subsections below. The only other points of note
are:

* The `ReplicateQueue` only acts on ranges for which the local store is the
  current leaseholder.
* Only one goroutine is doing all this processing, including the actual sending
  of snapshots. This is desirable because it keeps snapshots from overloading
  the network and seriously impacting user traffic, but it also has downsides.
  In particular, if any part of the processing of a replica gets stalled
  (sending a snapshot being the most likely slow part, but IIRC we also had
  occasional problems with a lease transfer blocking in the past), then it will
  take a long time for the replicate queue to get through all of its store's
  replicas. There's a hard limit of 60 seconds processing time per replica, but
  even this means that up-replication from a node failure can take a
  surprisingly long time in some pathological cases, whether due to a bug or
  just due to large replicas and low bandwidth between nodes.
* The 60 second deadline per replica means that sufficiently low snapshot
  bandwidth or sufficiently large replicas can make some ranges impossible to
  up-replicate or rebalance, because their snapshots can't complete in time
  and just get canceled on every attempt. This shouldn't happen with the
  default settings of `kv.snapshot_rebalance.max_rate`,
  `kv.snapshot_recovery.max_rate`, and `ZoneConfig.RangeMaxBytes`, but
  modifications to one or more of them can put a cluster in danger.
* We limit lease transfers away from each node to one per second. This is a
  very long-standing policy that hasn't been reconsidered in a long time, but
  it has minimal known downsides ([#19355]) that qps-based lease rebalancing
  mostly obviates.
* If a node needs to be up-replicated but there are no available matching nodes,
  or if a range needs to be processed but doesn't have a quorum of live
  replicas (i.e. it's an "unavailable" range), the replica will be put in
  purgatory to be re-processed when new nodes become live.

##### Choosing an action

First, we must decide what action to do - up-replicate, down-replicate, or
consider a rebalance. This decision is quite simple and can be easily
understood from the code. Essentially we just have to compare the number of
desired replicas from the applicable `ZoneConfig` to the number of non-dead,
non-decommissioning replicas from the range. There's a bit of extra logic needed
to dynamically adjust the desired number of replicas when it's greater than the
number of nodes in the cluster ([#27349], [#30441], [#32949], [#34126]), but
that's about it.

##### Picking an up-replication target

Picking an up-replication target is relatively straightforward. We can just
iterate over all live stores in the cluster, evaluating them on each of the
[considerations](#Considerations) in order, choosing one of the best results. We
will never, ever choose a store that doesn't meet the `ZoneConfig` constraints,
has an overfull disk, or is on the same node as another store that already
contains a replica for the range. After that, we will first prefer maximizing
diversity before considering factors such as the range count on each store. We
notably do not consider the QPS on each store here -- it's only taken into
account by the `StoreRebalancer`, never by the `ReplicateQueue`.

Rather than always choosing the best result, if there are two similarly good
options we will choose randomly between them. See
https://brooker.co.za/blog/2012/01/17/two-random.html or
https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf for details on
why this behavior is preferable.

##### Picking a down-replication target

Picking a replica to remove from an over-replicated range is also quite
straightforward. We just iterate over each replica's store, grading it on the
same [considerations](#Considerations) as always, choosing one of the two
worst-scoring stores. The only real exception is if one of the replicas is dead;
in such cases, we'll always remove the dead store(s) first. Note that as of
[#28875] we don't remove replicas from dead stores until we have allocated a
replacement replica on a different store. This makes certain data loss scenarios
less likely (see [#25392] for details).

If the algorithm chooses to remove the local replica, the replica must first
transfer the lease away before it can be removed. Note that while the new
leaseholder's replicate queue will examine the range shortly after acquiring the
lease, it's possible for the new leaseholder to make a different decision. This
isn't a real problem, but it does mean that removing oneself involves more work
and less certainty than removing any of the other replicas.

##### Picking a rebalance target

Deciding when to rebalance is when things start to get legitimately tricky, and
is what much of the allocator code is devoted to. This makes intuitive sense if
you consider that when adding or removing replica you both

1. know that you need to take action - unless all the options are truly terrible
   you should pick one of them.
2. only have to consider each store with respect to the set of existing
   replicas' stores. For adding a replica, this is roughly linear with respect
   to the number of live stores in the cluster. For removing a replica, it's
   linear with respect to the number of replicas in the range.

However, when rebalancing, you have to decide whether taking action is actually
desirable. And in practice, you want a bias against action, since there's a real
cost to moving data around, and we don't want to do so unless there's a
correspondingly real benefit. Also, the problem isn't linear any more - it's
roughly O(m*n) when there are m replicas in the range and n live stores in the cluster,
because we have to choose both the replica to be removed and the replica to add.
This is particularly an issue for diversity score calculations and per-replica /
expressive zone config constraints. For example, if you have the following
stores:

StoreID | Locality              | Range Count
--------|-----------------------|------------
s1      | region=west,zone=a    | 10
s2      | region=west,zone=b    | 10
s3      | region=central,zone=a | 100
s4      | region=east,zone=a    | 100

And a range has replicas on s1, s3, and s4, then going purely by range count it
would be great to take a replica off of range s3 or s4, which are both
relatively overfull. It would also be great to add a replica to s2, which is
relatively underfull. However, replacing s3 or s4 with s2 would hurt the range's
diversity, which we never choose to do without the user telling us to.

You can probably imagine that as the number of stores grows, doing all the
pairwise comparisons could become quite a bit of work. To optimize these
calculations, we group stores that share the same locality and the same
node/store attributes (a mostly-forgotten feature, but one that still needs to
be accounted for when considering `ZoneConfig` constraints). We can do all
constraint and diversity-scoring calculations just once for each group, and also
pair each group up against the only existing replicas that it could legally
replace without hurting diversity or violating constraints. We then only have to
do range count comparisons within these "comparable" classes of stores.

At the end, we can determine which (added replica, removed replica) pairs are
the largest improvement and choose from amongst them. As one last precautionary
step, we then simulate the down-replication logic on the set of replicas that
will result from adding the new replica. If the simulation finds that we would
remove the replica that was just added, we choose not to make that change. This
avoids thrashing, and is needed because we can't atomically add a member to the
raft group at the same time that we remove one. It's possible that this isn't
necessary right now, since the rebalancing code has been significantly improved
since it was added, but at the very least it's a nice fail-safe against future
mistakes.

##### Per-replica / expressive constraints

We support two high-level types of constraints -- those which apply to all
replicas in a range, and those which are scoped to only apply to a particular
number of the replicas in a range (publicly referred to as [per-replica
constraints]). The latter option adds a good deal of subtlety to all allocator
decisions -- up-replication, down-replication, and especially rebalancing.

In order to satisfy the requirements, we had to split up constraint checking
into separate functions that work differently for adding, removing, and
rebalancing. We also had to add an internal concept of whether a replica is
"necessary" for meeting the required constraints, in addition to the existing
concept of whether or not the replica is valid. A replica is "necessary" if the
per-replica constraints wouldn't be satisfied if the replica weren't part of a
range.

For more details on the design of the feature, see the discussion on [#19985].
For the implementation, see [#22819].


#### Lease transfer decisions

For the most part, deciding whether to transfer a lease is a fairly
straightforward decision based on whether the current leaseholder node is in a
draining state and on the lease counts on all the stores holding replicas for a
range. The more complex logic is related to the follow-the-workload
functionality that kicks in if-and-only-if the various nodes holding replicas
are in different localities. The logic involved here is better explained in the
[original RFC](../RFCS/20170125_leaseholder_locality.md) than I could do in less
space here. The logic has not meaningfully changed since the original
design/implementation.

### Store Rebalancer

As of v2.1, Cockroach also includes a separate control loop on each store called
the `StoreRebalancer`. The `StoreRebalancer` exists because we found in [#26059]
that an uneven balance of load on each node was causing serious performance
problems when attempting to run TPC-C at large scale without using partitioning.
Ensuring that each laod had a more even balance of work to do was experimentally
found to allow significantly higher and smoother performance.

The `StoreRebalancer` takes a somewhat different approach to rebalancing,
though. While the `ReplicateQueue` iterates over each replica one at a time,
deciding whether the replica would be better off somewhere else, the
`StoreRebalancer` looks at the overall amount of load (`BatchRequest` QPS
specifically, although it could in theory consider other factors) on each store
and attempts to take action if the local store is overloaded relative to the
other stores in the cluster. This difference is important -- our previous
attempt to rebalance based on load was integrated into the replicate queue, and
it didn't work very well for at least three different reasons:

1. We bit off more than we could chew, trying to rebalance on too many different
   factors at once - range count, keys written per second, and disk space used.
2. Keys written per second was the wrong metric, at least for TPC-C. Experimentation
   showed that the number of `BatchRequest`s being handled by a store per second
   were much more strongly correlated with a load imbalance than keys written
   per second.
3. Most importantly, the replicate queue only looks at one replica at a time. It
   may see that the load on each store is uneven, but it doesn't have a good way
   of knowing whether the replica in question would be a good one to move to try
   to event things out (if a particular range is relatively low in the metric
   we want to even out, it's intuitively a bad one to move). We did start
   gossiping quantiles in order to help determine which quantile a range fell in
   and thus whether it would be a good one to move, but this was still pretty
   imprecise.

The `StoreRebalancer` solves all these problems. It only focuses on QPS, and by
focusing on the store-level imbalance first and picking ranges to rebalance
later, it can choose ranges that are specifically high in QPS in order to have
the biggest influence on store-level balance with the smallest disruption on
range count (which the `ReplicateQueue` is still responsible for attempting to
even out). Ranges to rebalance are efficiently chosen because we have started
tracking a priority queue of the hottest ranges by QPS on each store. This queue
gets repopulated once a minute, when the existing loop that iterates over all
replicas to compute store-level stats does its thing. This list of hot ranges
can have other uses as well, such as powering debug endpoints for the admin UI
([#33336]).

Interpreting the exact details of how things work from the code should be pretty
straightforward; we attempt to move leases to resolve imbalances first, and only
resort to moving replicas around if moving leases was insufficient to resolve
the imbalance. There are some controls in place to avoid rebalancing when QPS is
too low to matter, or to avoid messing with a range that's so hot that it
constitutes the majority of a node's qps, or to not bother moving ranges with
too few qps to really matter, or a few other such things.

The `StoreRebalancer` can be controlled by a cluster setting that either fully
turns it off, enables just lease rebalancing, or enables both lease and replica
rebalancing, which is the default.

For more details, see the original prototype ([#26608]) or the final
implementation ([#28340], [#28852]).

### Other details

Before removing a replica or transferring a lease, we need to take the raft
status of the various existing replicas into account. This is important to avoid
temporary unavailability.

For example, if you transfer the lease for a range to a replica that is way
behind in processing its raft log, it will take some time before that replica
gets around to processing the command which transferred the lease to it, and it
won't be able to serve any requests until it does so.

Or when considering which replica to remove from a range, we must take care not
to remove a replica that is critical for the range's quorum. If only 3 replicas
out of 5 are caught up with the raft leader's state, we can't remove any of
those 3, but can safely remove either of the other 2.

Note that it's possible that the raft state of the underlying replicas changes
between when we do this check and when the actual transfer/removal takes place,
so it isn't a foolproof protection, but the window of risk is very small and we
haven't noticed it being a problem in practice.

## Known issues

* Rebalancing isn't atomic, meaning that adding a new replica and removing the
  replica it replaces is done as two separate steps rather than just one. This
  leaves room for locality failures between the two steps to cause
  unavailability ([#12768]). For example, if a range has replicas in localities
  `a`, `b`, and `c`, and wants to rebalance to a different store in `a`, there
  will be a short period of time in which 2 of the range's 4 replicas are in
  `a`.  If `a` goes down before one of them is removed, the range will be
  without a quorum until `a` comes back up.
* Rebalancing doesn't work well with multiple stores per node because we want to
  avoid ever putting multiple replicas of the same range on the same node
  ([#6782]). This has never been a deal breaker for anyone AFAIK, but occasionally
  annoys a user or two.
* `RelocateRange` is flaky in v2.2-alpha versions because we now immediately put a
  range through the replicate queue when a new lease is acquired on it ([#31287]).
  It may fail to complete its desired changes successfully due to racing with
  changes proposed by the new leaseholder.
* `RelocateRange` (and consequently the `StoreRebalancer` as a whole) doesn't
  populate any useful information into the `system.rangelog` table, which has
  traditionally been the best way to debug rebalancing decisions after the
  fact (#34130).

[#6782]: https://github.com/cockroachdb/cockroach/issues/6782
[#12768]: https://github.com/cockroachdb/cockroach/issues/12768
[#17979]: https://github.com/cockroachdb/cockroach/issues/17979
[#19355]: https://github.com/cockroachdb/cockroach/issues/19355
[#19985]: https://github.com/cockroachdb/cockroach/issues/19985
[#22819]: https://github.com/cockroachdb/cockroach/pulls/22819
[#25392]: https://github.com/cockroachdb/cockroach/issues/25392
[#26059]: https://github.com/cockroachdb/cockroach/issues/26059
[#26608]: https://github.com/cockroachdb/cockroach/pull/26608
[#27349]: https://github.com/cockroachdb/cockroach/pull/27349
[#28340]: https://github.com/cockroachdb/cockroach/pull/28340
[#28852]: https://github.com/cockroachdb/cockroach/pull/28852
[#28875]: https://github.com/cockroachdb/cockroach/pull/28875
[#30441]: https://github.com/cockroachdb/cockroach/pull/30441
[#31287]: https://github.com/cockroachdb/cockroach/issues/31287
[#32949]: https://github.com/cockroachdb/cockroach/pull/32949
[#33336]: https://github.com/cockroachdb/cockroach/pull/33336
[#34126]: https://github.com/cockroachdb/cockroach/pull/34126
[#34130]: https://github.com/cockroachdb/cockroach/issues/34130
[per-replica constraints]: https://www.cockroachlabs.com/docs/stable/configure-replication-zones.html#scope-of-constraints
