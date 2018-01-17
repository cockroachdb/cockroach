- Feature Name: Rebalancing plans for 1.1
- Status: in-progress
- Start Date: 2017-06-02
- Authors: Alex Robinson
- RFC PR: [#16296](https://github.com/cockroachdb/cockroach/pull/16296)
- Cockroach Issue:
  - [#12996](https://github.com/cockroachdb/cockroach/issues/12996)
  - [#15988](https://github.com/cockroachdb/cockroach/issues/15988)
  - [#17979](https://github.com/cockroachdb/cockroach/issues/17979)

# Summary

Lay out plans for which rebalancing improvements to make (or not make) in the
1.1 release and designs for how to implement them.

# Background / Motivation

We’ve made a couple of efforts over the past year to improve the balance of
[replicas](20160503_rebalancing_v2.md) and
[leases](20170125_leaseholder_locality.md) across a
cluster, but our balancing algorithms still don’t take into account everything
that a user might care about balancing within their cluster. This document puts
forth plans for what we’ll work on with respect to rebalancing during the 1.1
release cycle. In particular, four different improvements have been proposed.

## Balancing disk capacity, not just number of ranges ("size-based rebalancing")

Our existing rebalancing heuristics only consider the number of ranges on each
node, not the amount of bytes, effectively assuming that all ranges are the same
size. This is a flawed assumption -- a large number of empty ranges can be
created when a user drops/truncates a table or runs a restore from backup that
fails to finish. Not considering the size of the ranges in rebalancing can lead
to some nodes containing far more data than others.

## Balancing request load, not just number of ranges ("load-based rebalancing")

Similarly, the rebalancing heuristics do not consider the amount of load on each
node when making placement decisions. While this works great for some of our
load generators (e.g. kv), it can cause problems with others like ycsb and with
many real-world workloads if many of the most popular ranges end up on the same
node. When deciding whether to move a given range, we should consider how much
load is on that range and on each of the candidate nodes.

## Moving replicas closer to where their load is coming from ("load-based replica locality")

For the 1.0 release, [we added lease transfer
heuristics](20170125_leaseholder_locality.md) that move leases closer to the
where requests are coming from in high-latency environments. It’s easy to
imagine a similar heuristic for moving ranges -- if a lot of requests for a
range are coming from a locality that doesn’t have a replica of the range, then
we should add a replica there. That will then enable the lease-transferring
heuristics to transfer the lease there if appropriate, reducing the latency to
access the range.

## Splitting ranges based on load ("load-based splitting")

A single hot range can become a bottleneck. We currently only split ranges when
they hit a size threshold, meaning that all of a cluster’s load could be to a
single range and we wouldn’t do anything about it, even if there are other nodes
in the cluster (that don’t contain the hot range) that are idle. While splitting
decisions may seem somewhat separate from rebalancing decisions, in some
situations splitting a hot range would allow us to more evenly distribute the
load across the cluster by rebalancing one of the halves. 

This is so important for performance that we already support manually
introducing range splits, but an automated approach would be more appropriate as
a permanent solution.

# Detailed Design

## Balancing based on multiple factors

Currently when we’re scoring a potential replica rebalance, we only have to
consider the relevant zone config settings and the number of replicas on each
store. This allows us to effectively treat all replicas as if they’re exactly
the same. Adding in factors like the size of the range and the number
of QPS to a range invalidates that assumption, and forces us to consider how a
replica differs from the typical replica on both dimensions. For example, if
node 1 has fewer replicas than node 2 but more bytes stored on it, then we might
be willing to move a big replica from node 1 to 2 or a small replica from node 2
to 1, but wouldn’t want the inverses.

Thus, in addition to knowing the size or QPS of the particular range
we’re considering rebalancing, we’ll also want to know some idea of the
distribution of size or QPS per range for the replicas in a store. This will
mean periodically iterating over all the replicas in a store to aggregate
statistics so that we can know whether a range is larger/smaller than others or
has more/less QPS than others. Specifically, we'll try computing a few
percentiles to help pick out the true outliers that would have the greatest
effect on the cluster's balance.

We can them compute rebalance scores by considering the percentiles of a
replica and under/over-fullness of stores amongst all the considered dimensions.
We will prefer moving away replicas at high percentiles from stores that are
overfull for that dimension toward stores that are less full for the dimension
(and vice versa for low percentiles and underful stores under the expectation
that the removed replicas can be replaced by higher percentile replicas). The
extremeness of a given percentile and under/over-fullness will increase the
weight we give to that dimension. These heuristics will allow us to combine
the different dimensions into a single final score, and should be covered by a
large number of test cases to ensure stability in different scenarios.

## Size-based rebalancing

Taking size into account seems like the simplest modification of our existing
rebalancing logic, but even so there are a variety of available approaches:

1. We already gossip each store’s total disk capacity and unused disk capacity.
   We could start trying to balance unused disk capacity across all the nodes of
   the cluster. That would mean that in the case of heterogeneous disk sizes,
   nodes with smaller disks might not get much (if any) data rebalanced to them
   if the cluster doesn’t have much data.

2. We could try to balance used disk capacity (i.e. total - unused). In
   heterogeneous clusters, this would mean that some nodes would fill up way
   before others (and potentially way before the cluster fills up as a whole).
   Situations in which some nodes but not others are full are not regularly
   tested yet, so we may have to start if we go this way.

3. We could try to balance fraction of the disk used. This is the happy
   compromise between the previous two options -- it will put data onto nodes
   with smaller disks right from the beginning (albeit less data), and it
   shouldn’t often lead to smaller nodes filling up way before others.

The first option most directly parallels our existing logic that only attempts
to balance the number of replicas without considering the size of each node’s
disk, but the third option appears best overall. It’s likely that we’ll want to
change the replica logic as part of this work to take disk size into account,
such that we’ll balance replicas per GB of disk rather than absolute number of
replicas.

## Load-based rebalancing

As part of our [leaseholder locality](20170125_leaseholder_locality.md) work, we
started tracking how many requests each range’s leaseholder receives. This gives
us a QPS number for each leaseholder replica, but no data for replicas that
aren’t leaseholders. If we left things this way, our replica rebalancing would
suddenly take a dependency on the cluster’s current distribution of
leaseholders, which is a scary thought given that leaseholder rebalancing
conceptually already depends on replica rebalancing (because it can only balance
leases to where the replicas are). As a result, I think we’ll want to start
tracking the number of applied commands on each replica instead of relying on
the existing leaseholder QPS.

Once we have that per-replica QPS, though, we can aggregate it at the store
level and start including it in the store’s capacity gossip messages to use it
in balancing much like disk space.

## Load-based replica locality

This is where things get tricky -- while the above goals are about bringing the
cluster into greater balance, trying to move replicas toward the load is likely
to reduce the balance within the cluster. Reducing the thrashing involved in the
leaseholder locality project was quite a lot of work and still isn’t resilient
to certain configurations. When we’re talking about moving replicas rather than
just transferring leases, the cost of thrashing skyrockets because snapshots
consume a lot of disk/network bandwidth.

This also conflicts with one of our design goals from [the original rebalancing
RFC](20150819_stateless_replica_relocation.md), which is that the decision to
make any individual operation should be stateless. Because the counts of
requests by locality are only tracked on the leaseholder, these types of
decisions are inherently stateful, so we should tread into making them with
caution.

In the interest of not creating problem cases for users, I’d suggest pushing
this back until we have known demand for it. Custom zone configs paired with
leaseholder locality already do a pretty good job of enabling low-latency access
to data.

## Load-based splitting

Load-based splitting is conceptually pretty simple, but will likely produce
some edge cases in practice. Consider a few representative examples:

1. A range gets a lot of requests for single keys, evenly distributed over the
   range. Splitting will help a lot.

2. A range gets a lot of requests for just a couple of individual keys (and
   the hot requests don't touch multiple hot keys in the same query, a la case
   4). Splitting will help if and only if the split is between the hot keys.

3. A range gets a lot of requests for just a single key. Splitting won’t help at
   all.

4. A range gets a lot of scan requests or other requests that touch multiple
   keys. Splitting could actually make things worse by flipping an operation
   from a single-range operation into a multi-range one.

Given these possibilities, it’s clear that we’re going to need more granular
information than how many requests a range is receiving in order to decide
whether to split a range. What we really need is something that will keep track
of the hottest keys (or key spans) in the hottest ranges. This is basically a
streaming top-k problem, and there are plenty of algorithms that have been
written about that should work for us given that we only need approximate
results.

It’s also worth noting that we’ll only need such stats for ranges that have a
high enough QPS to justify splitting. Thus, our approach will look something
like:

1. Track the QPS to each leaseholder (which we’re already doing as of
   [#13426](https://github.com/cockroachdb/cockroach/pull/13426)).

2. If a given range’s QPS is abnormally high (by virtue of comparing to the
   other ranges), start recording the approximate top-k key spans.
   Correspondingly, if a range's QPS drops down and we had been tracking its
   top-k key spans, we should notice this and stop.

3. Periodically check the top key spans for these top ranges and determine if
   splitting would allow for better distributing the load without making too
   many more multi-range operations. Picking a split point and determining
   whether it'd be beneficial to split there could be done by sorting the top
   key spans and, between each of them, comparing how many requests would be
   to spans that are to the left of, the right of, or overlapping that
   possible split point.

4. If a good split point was found, do the split.

5. Sit back and let load-based rebalancing do its thing.

This will take a bit of work to finish, and isn’t critical for 1.1, but
would be a nice addition and comes with much less downside risk than something
like load-based replica locality. We’ll try to get to it if we have the time,
otherwise can implement it for 1.2.

### Alternatives

The approximate top-k approach to determining split points is fairly precise,
but also adds some fairly complex to the hot code path for serving requests to
replicas. A simple alternative would be for us to do the following for each hot
range:

1. Pick a possible split point (the mid-point of the range to start with).

1. For each incoming request to the hot replica, record whether the request is
   to the left side, the right side, or both.

1. After a while, examine the results. If most of the requests touched both
   sides, abandon trying to split the range. If most of the requests were split
   pretty evenly between left and right, make the split at the tested key. If
   the results were pretty uneven, try moving the possible split point in the
   direction that received more requests and try again, a la binary search.
   After O(log n) possible split points, we'll either find a decent split point
   may determine that there isn't an equitable split point (because the
   requests are mostly to a single key).

In fact, even if we do use a top-k approach, testing out the split point like
this before making the split might still be smart to ensure that all of the
spans that weren't included in the top-k aren't touching both sides of the
split.

Finally, the simplest alternative of all (proposed by bdarnell on #16296) is
to not do load-based splitting at all, and instead just split more eagerly for
tables with a small number of ranges (where "small" could reasonabl be defined
as "less than the number of nodes in the cluster"). This wouldn't help with
steady state load at all, but it would help with the arguably more common
scenario of a "big bang" of data growth when a service launches or during a
bulk load of data.

### Drawbacks

Splitting ranges based on load could, for certain request patterns, lead to a
large build-up of small ranges that don't receive traffic anymore. For example,
if a table's primary keys are ordered by timestamp, and newer rows are more
popular than old rows, it's very possible that newer parts of the table could
get split based on load but then remain small forever even though they don't
receive much traffic anymore.

This won't cripple the cluster, but is less than ideal. Merge support is being
tracked in [#2433](https://github.com/cockroachdb/cockroach/issues/2433).
