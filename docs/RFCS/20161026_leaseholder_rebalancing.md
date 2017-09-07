- Feature Name: Leaseholder Rebalancing
- Status: completed
- Start Date: 2016-10-26
- Authors: Peter Mattis
- RFC PR: [#10262](https://github.com/cockroachdb/cockroach/pull/10262)
- Cockroach Issue: [#9462](https://github.com/cockroachdb/cockroach/issues/9462) [#9435](https://github.com/cockroachdb/cockroach/issues/9435)

# Summary

Periodically rebalance range leaseholders in order to distribute the
per-leaseholder work.

# Motivation

The primary goal of ensuring leaseholders are distributed throughout a
cluster is to avoid scenarios in which a node is unable to rebalance
replicas away because of the restriction that we refuse to rebalance a
replica which holds a range lease. This restriction is present in
order to prevent an availability hiccup on the range when the
leaseholder is removed from it.

It is interesting to note a problematic behavior of the current
system. The current leaseholder will extend its lease as long as it is
receiving operations for a range. And when a range is split, the lease
for the left-hand side of the split is cloned and given to the
right-hand side of the split. The combined effect is that a newly
created cluster that has continuous load applied against it will see a
single node slurp up all of the range leases which causes a severe
replica imbalance (since we can't rebalance away from the leaseholder)
as well as a performance bottleneck. We actually see increased
performance by periodically killing nodes in the cluster.

The second goal is to more evenly distributed load in a cluster. The
leaseholder for a range has extra duties when compared to a follower:
it performs all reads for a range and proposes almost all
writes. [Proposer evaluated KV](20160420_proposer_evaluated_kv.md) will
reduce the cost of write KV operations on followers exacerbating the
difference between leaseholders and followers. These extra duties
impose additional load on the leaseholder making it desirable to
spread that load throughout a cluster in order to improve performance.

The last goal is to place the leaseholder for a range near the gateway
node that is accessing the range in order to minimize network RTT. As
an obvious example: it is preferable for the leaseholder to be in the
same datacenter as the gateway node. Note that there is usually more
than one gateway node accessing a range and there will be common
workloads where the traffic from gateway nodes is not coming from a
single locality, making it impossible to satisfy this goal. In general,
we'd like to minimize the aggregate RTT for accessing the range which
makes the mixture of reads and writes important (reads only need a
round-trip from the gateway to the leaseholder while writes need a
round-trip to the leaseholder and from the leaseholder to a quorom of
followers). Also, this goal is at odds with the second goal of
distributing load throughout a cluster and we'll need to be careful
with heuristics here. It may be beneficial to place the leaseholder in
the same datacenter as the gateways accessing the range, but doing so
can lower total throughput depending on the workload and if the
latencies between datacenters are small.

# Detailed design

This RFC is intended to address the first two goals and punt on the
last one (load-based leaseholder placement). Note that addressing the
second goal of evenly distributing leaseholders across a cluster will
also address the first goal of the inability to rebalance a replica
away from the leaseholder as we'll always have sufficient
non-leaseholder replicas in order to perform rebalancing.

Leaseholder rebalancing will be performed using a similar mechanism to
replica rebalancing. The periodically gossiped `StoreCapacity` proto
will be extended with a `lease_count` field. We will also reuse the
overfull/underfull classification used for replica rebalancing where
overfull indicates a store that has x% more leases than the average
and underfull indicates a store that has x% fewer leases than the
average. Note that the average is computed using the candidate stores,
not all stores in the cluster. Currently, `replicateQueue` has the
following logic:

1. If range has dead replicas, remove them.
2. If range is under-replicated, add a replica.
3. If range is over-replicated, remove a replica.
4. If the range needs rebalancing, add a replica.

The proposal is to add the following logic (after the above replica
rebalancing logic):

5. If the leaseholder is on an overfull store transfer the lease to
the least loaded follower less than the mean.
6. If the leaseholder store has a leaseholder count above the mean and
one of the followers has an underfull leaseholder count transfer the
lease to the least loaded follower.

# Testing

Individual rebalancing heuristics can be unit tested, but seeing how
those heuristics interact with a real cluster can often reveal
surprising behavior. We have an existing allocation simulation
framework, but it has seen infrequent use. As an alternative, the
`zerosum` tool has been useful in examining rebalancing heuristics. We
propose to fork `zerosum` and create a new `allocsim` tool which will
create a local N-node cluster with controls for generating load and
using smaller range sizes to test various rebalancing scenarios.

# Future Directions

We eventually need to provide load-based leaseholder placement, both
to place leaseholders close to gateway nodes and to more accurately
balance load across a cluster. Balancing load by balancing replica
counts or leaseholder counts does not capture differences in per-range
activity. For example, one table might be significantly more active
than others in the system making it desirable to distribute the ranges
in that table more evenly.

At a high-level, expected load on a node is proportional to the number
of replicas/leaseholders on the node. A more accurate approximation is
that it is proportional to the number of bytes on the node (though
this can be thwarted by an administrator who recognizes a particular
table has higher load and thus sets the target range size
smaller). Rather than balancing replica/leaseholder counts we could
balance based on the range size (i.e. the "used-bytes" metric).

The second idea is to account for actual load on ranges. The simple
approach to doing this is to maintain an exponentially decaying stat
of operations per range and to multiply this metric by the range size
giving us a range momentum metric. We then balance the range momentum
metric across nodes. There are difficulties to making this work well
with the primary one being that load (and thus momentum) can change
rapidly and we want to avoid the system being overly sensitive to such
changes. Transferring leaseholders is relatively inexpensive, but not
free. Rebalancing a range is fairly heavyweight and can impose a
systemic drain on system resources if done too frequently.

Range momentum by itself does not aid in load-based leaseholder
placement. For that we'll need to pass additional information in each
`BatchRequest` indicating the locality of the originator of the
request and to maintain per-range metrics about how much load a range
is seeing from each locality. The rebalancer would then attempt to
place leases such that they are spread within the localities they
receiving load from, modulo their other placement constraints
(i.e. diversity).

# Drawbacks

* The proposed leaseholder rebalancing mechanisms require a transfer
  lease operation. We have such an operation for use in testing but it
  isn't ready for use in production (yet). This should be rectified
  soon.

# Alternatives

* Rather than placing the leaseholder rebalancing burden on
  `replicateQueue`, we could perform rebalancing when leases are
  acquired/extended. This would work with the current expiration-based
  leases, but not with [epoch-based](20160210_range_leases.md) leases.

* The overfull/underfull heuristics for leaseholder rebalancing
  mirrors the heuristics for replica rebalancing. For leaseholder
  rebalancing we could consider other heuristics. For example, we
  could periodically randomly transfer leases. We have some
  experimental evidence that this is better than the status quo due to
  tests which periodically restart nodes and thus cause the leases on
  that node to be redistributed in the cluster. The downside to this
  approach is that it isn't clear how to extend it to support more
  sophisticated decisions such as load-based leaseholder
  rebalancing. Random transfers also have the disadvantage of causing
  minor availability disruptions. The system should be able to reach
  an equilibrium in which lease transfers are rare.

* Another signal that could be used in conjunction with the proposed
  overfull/underfull heuristic is the time since the lease was last
  transferred. If we disallow frequent transfers we can prevent
  thrashing and enforce an upper bound on the rate of transfer-related
  "hiccups". The time since last lease transfer can help us choose
  which lease to transfer from an overfull store. This signal will be
  explored if testing shows thrashing is a problem.

* A simple mechanism for avoiding thrashing (moving leases back and
  forth) is to use a larger value for the overfull/underfull
  threshold. This satisfies the primary goal for the RFC at the
  expense of the second goal of balancing leases for improved
  performance.

# Unresolved questions
