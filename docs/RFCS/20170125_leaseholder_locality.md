- Feature Name: Leaseholder Locality ("Leases follow the workload")
- Status: completed
- Start Date: 2017-01-25
- Authors: Alex Robinson
- RFC PR: [#13233](https://github.com/cockroachdb/cockroach/pull/13233)
- Cockroach Issue:
  - [#13232](https://github.com/cockroachdb/cockroach/issues/13232)

# Summary

Enable range leaseholders to move closer to their clients, which reduces the
latency of KV requests when replicas are far apart.

# Motivation

The primary motivation for moving leaseholders around based on where their
requests are coming from is to reduce the latency of those requests.  When a
request for a given key is received by a gateway node, the node must forward the
request to the leaseholder for the key's range. This isn't a big deal if all a
cluster's nodes are nearby each other in the network, but if the leaseholder
happens to be halfway around the world from where the request originated, the
network round trip time that gets added to the request latency by this can be
quite high.

This affects both reads and writes. For reads, getting the request to and from
the leaseholder dominates the request latency in widely distributed clusters
since the leaseholder can serve reads without talking to other replicas. For
writes, even though raft commands will incur an additional round trip between
replicas, removing the round trip to the leaseholder could nearly halve the
total request latency.

While there are typically multiple gateway nodes accessing a range and that they
won't necessarily all be from the same locality, our goal is to minimize the
aggregate RTT cost of accessing a given range by properly placing its
leaseholder.

We believe there are usage patterns that would benefit greatly from better
leaseholder placement. Consider a system that spans datacenters all around the
world. When it's daytime in the Asia/Australia, the datacenter(s) there will be
receiving most of the requests. As time passes, more of the requests will start
to originate from Europe, and later on from the Americas. If the leaseholder for
a range is always in an Asian data center, then the latency of accessing that
range will be much worse when most of its requests come from elsewhere. This is
where the idea of the lease "following the workload" or "following the sun" comes
from.

Finally, it's worth noting that this goal is somewhat at odds with our desire
to distribute load evenly throughout a cluster (e.g. via [range
rebalancing](20160503_rebalancing_v2.md) and [leaseholder
rebalancing](20161026_leaseholder_rebalancing.md). In fact, this goal was
specifically pushed off when the initial form of leaseholder rebalancing was
designed and implemented.  Placing the leaseholders near their most common
gateways may lower total throughput if it maxes out what the local machines can
do while leaving the machines in other datacenters underutilized, particularly
if the latency between datacenters is small. There is a fine balance to be kept
to avoid minimizing latency at the expense of too much throughput and vice
versa.

# Detailed design

Given that a lease transfer mechanism already exists, the remaining difficulty
lies in deciding when to transfer leases. We [already have
logic](20161026_leaseholder_rebalancing.md) that decides when to transfer leases
with the goal of ensuring each node has approximately the same number of leases.
Anything we add to make leases follow the workload will need to play nicely with
that existing goal.

## Tracking request origins

In order to have any idea where to rebalance leases to, we first need to track
where the requests for a given range are coming from. To that end, we can add
request origin information to the `Header` that's included in all
`BatchRequest`s.

When tracking the origin of requests, we don't just care about about the
individual node that a request came from, but its locality. To understand this,
consider a cluster with nodes 1-3 in datacenter 1, nodes 4-6 in datacenter 2,
and nodes 6-9 in datacenter 3. Range x in this cluster has replicas on nodes 1,
4, and 7, and the current leaseholder is node 1. If the leaseholder is receiving
a lot of its requests from nodes 8 and 9, then we may want to move the lease to
node 7 even if node 7 itself isn't sending the range much traffic, because node
7 shares a locality with the origin of the requests.

Luckily, each node already has a `NodeDescriptor` for every other node in the
cluster, and the `NodeDescriptor` proto contains its node's locality
information. Thus, all we need to add to the `BatchRequest` `Header` proto is a
`gateway_node_id` field that gets filled in by the client kv transport. While
the client transport will typically fill this field in with its own ID, it can
also be spoofed when appropriate, such as by DistSQL when a node that wasn't
actually the gateway for a given request makes KV requests on behalf of the real
gateway.

### Alternatives for tracking request origins

Given that we know the IP address of each node in the cluster, we could
potentially try to skip adding the `gateway_node_id` field to each
`BatchRequest` and just rely on the source IP address. That would work in most
cases, but could break down when nodes are communicating with each other via a
load balancer or proxy, without saving much (adding an int to each
`BatchRequest` should have a negligible effect on request size).

We could alternatively include all the locality tags from the source node in
each request, which would eliminate the need to look up the locality of each
node when making decisions based on the collected data. However, this requires
much more data to be sent over the wire and stored per-range in the system,
effectively duplicating the work already done by the gossiping and storage of
node descriptors. It would be reasonable to take this approach -- it would save
an integer's worth of work in the case that the nodes in a cluster don't have
any locality labels -- but it doesn't simplify enough to make up for its added
cost.

## Recording request origins

Each leaseholder replica will maintain a map from locality to the number of
requests received from that locality. Ideally, the request counts would decay
exponentially over time. If this proves too difficult to implement efficiently
(e.g. in a cluster with requests coming from tens of localities), we could opt
for swapping out the map with a new map periodically, such as when we examine it
to decide whether to transfer the replica's lease.

If we go with the latter approach we should also maintain the time that we last
cleared out the map of request counts so that we can determine the rate of
requests to the range. Understanding the load on each range will help us
prioritize them for transfer (and help us to balance the load on each node, even
if that isn't the immediate goal of this work).

For the purposes of optimizing lease locality, we'll count each BatchRequest as
a single request. For tracking the load on each replica, we'll likely want to
measure something a little fancier, such as the number of non-noop subrequests
in the BatchRequest or the amount of KVs scanned by the request. These would be
better estimates of load, but for locality we want to focus on the number of
requests suffering from the large network RTT.

## Making lease transfer decisions

Much of the infrastructure needed for making decisions about when to transfer
leases was [already added](https://github.com/cockroachdb/cockroach/pull/10464)
as part of the initial lease balancing work, so all we need to do is start using
request locality as an additional input to the decision-making process. The
difficult problems that we really need to solve for are:

* Prioritizing transferring leases for the replicas that are receiving the
  greatest number of requests from distant localities.
* Avoiding thrashing if the origins of requests change frequently.
* Finding the right balance between keeping a similar number of leases on each
  node or moving all the leases to where the most traffic is.

### Prioritizing ranges with the most cross-DC traffic

If a cluster really is spread over datacenters around the world, it's likely
that most of a cluster's ranges will be getting most of their traffic from the
same locality. If this is the case, the most bang-for-the-buck would come from
moving the leases for ranges that are receiving the most requests and whose
request distributions are most skewed toward the given location. To that end, we
may want to periodically record stats about the rate of requests to the ranges
on a store and how many of those requests are from distant localities.  These
would be much like counts we calculate of the number of ranges and leases on a
store. However, unlike those stats, we may not want to start gossiping these as
part of the store descriptor until we have a concrete use for them.  That will
likely come once we start getting smarter about determining just how much load a
node can handle.

Given these stats, we can make decisions per-replica by only transferring leases
for replicas whose requests skew more strongly from distant localities.

Of course, there is a risk that moving all the hottest ranges to the same
locality will could have a worse effect on throughput if those nodes get
overloaded. We'll have to test the heuristics carefully to tune them and
determine when this could become a problem. It's very possible that we'll have
to expose a tuning knob for this to give users more control over the tradeoff.

### Avoiding thrashing

In order to avoid thrashing of leases, we can partially reuse the mechanisms
already in place for avoiding lease thrashing with respect to leaseholder
balance, particularly the rate-limiting of lease transfers
([#11729](https://github.com/cockroachdb/cockroach/pull/11729)).

Additionally, we can learn from both lease and replica rebalancing that there
needs to be a wide range of configurations in which no action is taken -- e.g.
a node with 4% more ranges than the mean won't bother transferring a range to a
node with 4% fewer ranges than the mean. We'll need a cushion in our heuristic
such that we only transfer leases away if there's a significant difference in
the number of requests coming from the different localities.

Along the same lines, we shouldn't make hasty decisions. Measurements of request
distributions are less reliable if we only make them over short time windows.
We'll need to pick a reasonable minimum duration of time to measure before
transferring leases. This will likely be in the tens of minutes -- enough time
to get good data, but not so long that traffic patterns are likely to change
drastically in shorter time periods. An alternative would be to factor in the
number of requests, since a 5 minute measurement with a million samples is much
more trustworthy than a 20 minute measurement with a thousand samples.

### What to do if nodes don't have locality information

If nodes don't have any locality information attached to them, we lose most of
our ability to determine where nodes are located with respect to each other.
While we should make an effort to encourage all multi-datacenter deployments to
specify locality information (and can automate the assignment of locality info
in cloud environments), there will certainly be some deployments without it. We
have a few options in such cases:

* Fall back to operating exactly the same as today, not doing any load-based
  lease placement. We'll still be able to use the per-replica request counts
  once we start factoring load into balancing decisions.
* Fall back to a very limited version of load-based lease placement where we
  only move a lease based on request locality if a node holding one of the other
  replicas is forwarding the vast majority of traffic to the range.
* Calculate (and gossip) estimates of the latency between all pairs of nodes and
  use that to guess at the localities of nodes for the sake of rebalancing.

The last option can be put off to future optimization efforts (if ever). The
second option may be beneficial if a user sets up a multi-datacenter deployment
without locality info, but in the more common case of low-latency, single-DC
deployments it'd likely just make the lease balancing worse for no real latency
gain. We'll stick with the first option for now.

### Reconciling lease locality with lease balance

As mentioned above, our goals in this RFC come into fairly direct conflict with
the goals of the [lease rebalancing RFC](20161026_leaseholder_rebalancing.md).
If all the requests are coming from a single locality, it would be ideal from a
latency perspective for all the leases to be there as well, so long as that
wouldn't overload the nodes with too much work. However, it's possible that
putting all the leases into that locality will overload the nodes such that
overall cluster throughput (and even latency) is worse than it would be if the
leases were properly balanced.

Ideally, we would have some understanding of how fully utilized each node is in
terms of throughput. That measurement could be gossiped in the same way as each
node's storage utilization and used in allocation decisions. It's not totally
clear what metric to use for this, though, so for now we will leave it out of
allocation decisions (suggestions welcome -- perhaps a rather naive utilization
measurement can go a long way here). We could easily add a measure of how much
load a node has on it, but not of how close to fully utilized it is.

Similarly, it'd be useful to have an estimate of the RTT between localities when
making allocation decisions. While we don't currently have this (as far as I'm
aware), measuring it wouldn't be very hard, so I suggest that we do so and use
it to help tune lease allocations. We'll have to make sure that this can't lead
to too much thrashing if different nodes come up with different estimates, but
re-measuring this periodically will help fight such issues and also have the
nice benefit that the cluster will react accordingly if the RTT between
localities changes.

Thus, our heuristic for deciding when to transfer a lease will look something
like this:

* If the cluster doesn't have locality information, fall back to the existing
  behavior. The existing behavior is to transfer the lease if the current
  leaseholder node is considered overfull (based on the `rebalanceThreshold`
  constant, which is currently 5% of the mean number of leases) or if it has
  more than the mean number of leases and another replica is underfull.
  * Note that we can start using the new information on how many requests each
    replica has been serving lately to get an idea of how many requests each
    node is serving and use that rather than just the number of leaseholders.
    That work is somewhat orthogonal to optimizing leaseholder locality, but is
    worth noting and working on soon.
* If the cluster does have locality information, then measure the RTT between
  localities. As the latency between localities increases, raise a new
  `interLocalityRebalanceThreshold` proportionately. This will affect the
  underfull/overfull calculations for leases when comparing replicas in
  different localities, but the normal `rebalanceThreshold` would still be used
  when comparing replicas within the same locality. The exact numbers for this
  can be worked out in testing, but it will make lease balancing less and less
  important until we eventually don't consider it at all for replicas in
  different localities. If it's legal to make a cross-locality transfer based on
  the nodes in question and the current `interLocalityRebalanceThreshold`, then
  we can begin considering whether it's worth it to make a transfer based on the
  distributon of requests to the replica.

# Testing

Much like how `allocsim` has been useful for testing the lease rebalancing
heuristics, it would be very nice to have a repeatable tool for testing this as
well. However, whereas the only real variable when testing lease rebalancing was
the relevant code and the only real output was the number of leases held by each
node, we now have multiple inputs (the code, the latency between nodes, the
locality labels) and multiple outputs (the number of leases held by each node,
the distribution of request latency, and the request throughput) to consider.

In order to better test this new functionality, I propose:

* Adding a testing knob to simulate additional latency between nodes.
* Extending `allocsim` to be able to set up different locality configurations
  with the new latency knob.
* Extending `allocsim` to send load against the specific nodes/localities and
  measure the resulting throughput and latency.

The output of allocsim will enable us to better understand how different
heuristics perform on different cluster configurations. We may be able to find a
heuristic that performs reasonably well across a variety of clusters, or may
find that we have to introduce a tuning knob of some sort to shift the balance
one way or another. Either way, it's tough for us to know exactly what'll work
without a testing tool like this, so we'll rely heavily on it when tuning the
heuristic.

# Future Directions

* It would be helpful to expose information about balancing decisions in the UI
  so that users and developers can better understand what's happening in their
  clusters, which will be particularly important when bad decisions are made,
  causing performance dips.

* There could be situations in which the locality that's generating most of the
  requests to a range doesn't have a local replica for that range. While ideally
  cluster admins would construct `ZoneConfig` settings that make sense for their
  environments and workloads, we could potentially benefit from taking
  per-locality load into account when making replica placement decisions (not
  just lease placement decisions).

* As mentioned above, we should start using the recent request counts being
  added as part of this work to improve the existing replica and lease placement
  heuristics. This should be fairly straightforward, it'll just require some
  benchmarking. There's more discussion of this in the [future directions
  section of the leaseholder rebalancing
  RFC](20161026_leaseholder_rebalancing.md#future-directions).

* As mentioned above, it would be very beneficial if we came up with some way of
  measuring the true load on each node, either as a collection of measurements
  or as some combination of CPU utilization, memory pressure, and disk I/O
  utilization. This would make improving locality and balancing load
  significantly easier and more effective. Ideally we could even differentiate
  between load caused by being a leaseholder and load caused by being a follower
  replica.

* It's possible that this work will actually be harmful in certain odd cluster
  configurations. For example, if a range has two replicas in different parts of
  Europe and only a single replica in Australia, then writes will perform better
  if the lease is in Europe even if most of its requests are coming from
  Australia. This is because raft commands will commit much more quickly if
  proposed from one of the replicas in Europe. We may want to take into account
  the latency between the localities of all the different replicas to avoid such
  problems, but such cases aren't critical since they're not recommended
  configurations to begin with.

# Drawbacks

* This approach does not have any true safeguards against overloading nodes in
  localities where most requests are coming from. We will prefer moving load
  there as long as the inter-locality RTT is high enough regardless of how much
  load the nodes can handle. At a high level, this is an existing issue of our
  system, but intentionally imbalancing the leases will make the risk much worse
  (until we start factoring in some measure of load percentage when making
  decisions). We're going to need some sort of flow control mechanism regardless
  of this change, and once we have it we can use it to help with these
  decisions.
* Relying on measurements of latency from each node to each other locality may
  lead to unexpected thrashing if nodes get drastically different measurements.
  Taking multiple measurements over time should help with this, but it's
  conceivable that certain networks could exhibit a persistent difference.
* Unless we decide to do a lot more work determining and gossiping latency
  information between all nodes, these optimizations won't be used if cluster
  admins don't add locality info to the nodes.

# Alternatives

* As mentioned above, we could track the origin of each request by sending the
  locality labels along with each request rather than just the source node ID,
  but that would add a lot of duplicated info along with every request.
* Rather than considering it future work, it'd be beneficial if we could make
  allocation decisions based on the actual load on each node. Skipping right to
  that solution would be great (suggestions appreciated!).
* We may find it more effective to measure something other than number of
  requests handled by a replica. For example, perhaps time spent processing
  requests for the replica or bytes returned in response to requests to the
  replica would be more accurate measurements of the load on the replica. These
  are things that we could potentially experiment with using allocsim to see if
  they provide better balance.

# Unresolved questions
