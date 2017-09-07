- Feature name: Rebalancing V2
- Status: completed
- Start date: 2016-04-20
- Last revised: 2016-05-03
- Authors: Bram Gruneir & Cuong Do
- RFC PR: [#6484](https://github.com/cockroachdb/cockroach/pull/6484)
- Cockroach Issue:

# Table of Contents

- [Table of Contents](#table-of-contents)
- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Metrics for evaluating rebalancing](#metrics-for-evaluating-rebalancing)
- [Detailed Design](#detailed-design)
  - [Store: Add the ability to reserve a replica](#store-add-the-ability-to-reserve-a-replica)
  - [Protos: add a timestamp to *StoreDescriptor* and reservations to *StoreCapacity*](#protos-add-a-timestamp-to-storedescriptor-and-reservations-to-storecapacity)
  - [RPC: ReserveReplica](#rpc-reservereplica)
  - [Update *StorePool/Allocator* to call *ReserveReplica*](#update-storepoolallocator-to-call-reservereplica)
- [Drawbacks](#drawbacks)
  - [Too many requests](#too-many-requests)
- [Alternate Allocation Strategies](#alternate-allocation-strategies)
  - [Other enhancements to distributed allocation](#other-enhancements-to-distributed-allocation)
  - [Centralized allocation strategy](#centralized-allocation-strategy)
    - [Allocator lease acquisition](#allocator-lease-acquisition)
    - [Allocator lease renewal](#allocator-lease-renewal)
    - [Updating the allocator’s *StoreDescriptors*](#updating-the-allocators-storedescriptors)
    - [Centralized decision-making](#centralized-decision-making)
    - [Failure modes for allocation lease holders](#failure-modes-for-allocation-lease-holders)
    - [Conclusion](#conclusion)
  - [CopySets](#copysets)
  - [CopySets emulation](#copysets-emulation)
- [Allocation Heuristic Features](#allocation-heuristic-features)
- [Testing Scenarios](#testing-scenarios)
  - [Simulator](#simulator)
- [Unresolved Questions](#unresolved-questions)
  - [Centralized vs Decentralized](#centralized-vs-decentralized)

# Summary

Rebalancing is the redistribution of replicas to optimize for a chosen set of heuristics. Currently,
each range lease holder runs a distributed algorithm that spreads replicas as evenly as possible across
the stores in a cluster. We artificially limit the rate at which each node may move replicas to
avoid the excessive thrashing of replicas that results from making independent rebalancing decisions
based on outdated information (gossiped `StoreDescriptor`s that are up to a minute old).

As detailed later in this document, we’ve weighed decentralized against centralized allocation
algorithms, as well as different heuristics for evaluating whether replicas are properly balanced.
For V2 of our replica allocator, we are adding a replica reservation step to the distributed
allocator and intelligently increasing the frequency at which we gossip `StoreDescriptor`s.
These modifications will significantly reduce the time required to rebalance small-to-medium-sized
clusters while avoiding the waste of resources and degradation in performance caused by excessive
thrashing of replicas.

We’re specifically not addressing load-based rebalancing or heterogeneous stores/nodes in V2.
Moreover, we are not addressing the potential for data unavailability when more than one node goes
offline. These are important problems that will be addressed in V3 or later.

# Motivation

To allocate replicas for ranges, we currently rely on distributed
[stateless replica relocation](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20150819_stateless_replica_relocation.md).

Each range lease holder is responsible for replica allocation decisions (adding and removing replicas)
for its respective range. This is a good, simple start. However, it is particularly susceptible to
thrashing. Because different range lease holders distribute replicas independently, they don't necessarily
converge on a desirable distribution of replicas within a reasonable number of replica allocations.

A number of factors contribute to this thrashing. First, there is a lack of updated information on
the current state of all stores so replication decisions typically rely on outdated data. The
current state of the cluster is retrieved from gossiped store descriptors. However, store
descriptors are only gossiped at an interval of once every minute. So if a store is suddenly
overloaded with replicas, it may take up to one minute (plus gossip network propagation time) for
its updated descriptor to reach various range lease holders.

Secondly, until recently, our replica allocator had no limits for how fast rebalancing could occur.
Combined with lack of data and since there is no coordination between replica allocators, an
over-allocation to the new store is likely to occur.

For example, consider the following scenario where we have 3 perfectly balanced stores:

![Thrashing 1](images/rebalancing-v2-thrashing1.png?raw=true "Thrashing 1")

Let's add an empty store:

![Thrashing 2](images/rebalancing-v2-thrashing2.png?raw=true "Thrashing 2")

As soon as that store is seen by each range’s allocator, the following occurs:

![Thrashing 3](images/rebalancing-v2-thrashing3.png?raw=true "Thrashing 3")

This over-rebalancing continues for many cycles, often resulting in tens of thousands of replicas
adds and removes for clusters with miniscule data.

As a stopgap measure, a
[recent change](https://github.com/cockroachdb/cockroach/commit/c4273b9ef7f418cab2ac30a10a8707c1601e5e99)
has added a minimum delay of 65 seconds between rebalance attempts for each node, to reduce
thrashing. This works well for small clusters with little data. However, this severely slows down
the process of rebalancing many replicas in an imbalanced cluster.

# Goals

Rebalancing is a topic of ongoing research and investigation, in this section, goals for a second
version of rebalancing are presented. In the section that immediately follows it, a collection of
future goals are presented for potential post V2 work.

Relative priorities are debatable and depend on deployment specifics, so the following are listed
in no particular order:

- Minimizes thrashing.

  Thrashing, which can occur when a node is added or removed can quickly bring a cluster to a near
  halt due to the number replicas being moved between nodes which results in requests queuing up
  waiting to be serviced.

- Is performant in clusters with up to 32 nodes.

  The choice of 32 nodes here matches our OKRs. This limit is to make testing more tractable.
  Performance will be measured using the metrics described in the Metrics section below.

- Is performant in a dynamic cluster. A dynamic cluster is one in which nodes can be added and
  removed arbitrarily.

  While this may seem like an obvious goal, we should ensure that equilibrium is reached quickly
  in cases when one or more nodes are added and removed at once. It should be noted that only
  a single node can be removed at a time but any number of nodes can be added.

- Handles outages of any number of nodes gracefully, as long as quorum is maintained.

  This is the classic repair scenario.

- Don’t overfill stores.

  Overly full disks tend to perform worse. But this is also to ensure we don’t overly fill a new
  store when it’s added. Using the example from the motivation section above, if the new store
  could only hold 100 replicas it would be catastrophic for the cluster.

# Non-Goals

There are a number of interesting further investigations on improving rebalancing and how it can
impact overall cluster and perhaps individual operation performance. We list them here for
potential future work on what we are calling post V2 rebalancing. Again, these are not ordered by
priority:

- Is performant in heterogeneous clusters.

  Clusters with different sized stores with different CPUs and lagging nodes.

- Is performant in large clusters (>>32 nodes).

  Further work past our arbitrary limit of 32 nodes.

- Replicas are moved to where there is demand for them.

  Experiment to see if this would be useful. There may be performance gains on keeping replicas of
  single tables together on the same set of stores.

- Globally distributed data.

  How should the replicas be organized when there are potentially long round trips between
  datacenters?

- Optimizes data transfer based on network topology.

  Examples: ping times between replicas, network distance, rack and datacenter awareness.

- Decrease chance of data unavailability as nodes go down.

  See the discussion on CopySets below.

- Distribute "hot" keys and ranges evenly through the cluster.

  This would greatly help to distribute load and make the cluster more robust and be able to handle
  a larger number of queries with lower latency.

- Defragment. Co-locate tables that are too big for a single range to the same set of stores.

  This could speed up all large table queries.

# Metrics for evaluating rebalancing

As with any system, a set of evaluation criteria are required to ensure that changes improve the
cluster. We propose the following criteria. It should be noted that most changes may positively
impact one and negatively affect the others:

- Distribution of data across stores. Measured using percentage of store capacity available.
- Speed at which perturbed systems reach equilibrium. Measured using the number of rebalances
  until and the total time until the cluster is stable.

For post V2 consideration:

- User latency. Rebalancing should never affect user query latency, but too many rebalances may do
  just that. Measured using latencies of user queries.
- Distribution of load across stores. Measured using CPU usage and network traffic.

# Detailed Design

The current distributed allocator cannot rebalance quickly because of the >= 65 second rebalancing
backoff. Because removing that backoff would cause excessive allocation thrashing, the `Allocator`
has to be modified to make faster progress while minimizing thrashing.

To reduce thrashing, we are introducing the concept of reserved replicas. Before allocating a new
replica, an allocator will first reserve the space for the new replica. This will be accomplished
by adding a new RPC `ReserveReplica` that requests to reserve space for the new replica on one
store. Once received, the store can reply with either a yes or a no. When it replies with a
`reserved`, the space for said replica is reserved for a predetermined amount of time. If no
replica appears within that time, it is no longer reserved. (It should be noted that the size of a
replica depends on the split size based on the table and or zone. This should be taken into
consideration.)

Each `ReserveReplica` response contains the latest `StoreDescriptor`s from the node with a
node-local timestamp. The caller can use these to update its cached copy of the `StoreDescriptor`.

When it replies with a `not reserved`, it also includes possible reasons as to why for debugging
purposes. These reasons can include:

1. Too full in terms of absolute free disk space (this includes all reserved replica spots)
1. Overloaded (once we define what that term specifically means)
1. Too many current reservations (throttling factor will be determined experimentally)

Any other error, including networking errors can be considered a `not reserved` response for the
purposes of allocation. When a `not reserved` is received, that response is cached in the store
pool until the next `StoreDescriptor` update. We avoid issuing further `ReserveReplica` calls to
that store until the next `StoreDescriptor` update.

The next subsections contain all of the major tasks that will be required to complete this feature
and further details about each.

## Store: Add the ability to reserve a replica

To prevent a store from being overwhelmed and overloaded, the concept of a reserved replica will be
added to a store. A reserved replica reserves a full replica’s amount of space for a predetermined
amount of time (typically for a `storeGossipInterval`) and reserves it for the expected incoming
replica for a specific RangeID. If the replica for the range is not added within the reservation
timeframe, the reservation is removed and the space becomes free again.

If a new replica arrives and there is no reservation, the store will still create the new replica
and this will not cancel any pre-existing reservations. The gating of when to allow a new
reservation is decided in the `ReserveReplica` RPC and is not part of adding a replica in the
store.

Optionally, the ability to control the amount of currently available space on a store might be used
to slow down a cluster from suddenly jumping on a new node when one becomes available. By
pre-reserving (for a non existing store) all or most of the new store’s capacity and staggering
the timeouts, it may prevent all replicas from suddenly being interested in adding themselves to
the store. This will require some testing to determine if it will be beneficial.

## Protos: add a timestamp to *StoreDescriptor* and reservations to *StoreCapacity*

By adding a timestamp to the `StoreDescriptor` proto, it enables the ability to quickly pick the
most recent `StoreDescriptor`. This timestamp is local to the store that generated the
`StoreDescriptor`. The main use case for this is when calling `ReserveReplica`, regardless of the
response being a `reserved` or a `not reserved`, it will also return updated `StoreDescriptor`s for
all the stores on the node. These updated descriptors will be used to update the cached
`StoreDescriptor` in the `StorePool` of the node calling `ReserveReplica`. There may be a small
race with these descriptors and new ones that are arriving via gossip. A timestamp fixes this
problem. Any subsequent calls to the allocator will have a fresher sense of the cluster. Note that
it may be possible to skip the addition of the timestamp by returning a gossip `Info` from the
`reserveReplica` RPC.

By adding a `reservedSpace` value to capacity, it allows more insight into how the total capacity
of a store is used and be able to make better decisions around it. Also, by adding
`activeReservations` an allocator will be able to choose rebalancing targets that are not
overwhelmed with requests.

## RPC: ReserveReplica

By adding `ReserveReplica` RPC to a node, it will enable a range to reserve a replica on a node
before calling `changeReplica` and adding the replica directly. Because no node will ever have the
most up to date information about another one, the response will always include an updated
`StoreDescriptor` for the store in which a reservation is requested. It should be noted that this
is a new type of RPC in which it addresses a store and not a node or range.

The request will include:

- `StoreID` of the store in which to reserve the replica space
- `RangeID` of the requesting range. Consider repurposing a `ReplicaDescriptor` here.
- All other parameters that are required by the allocator, such as required attributes.

The response will include:

- `StoreDescriptor`s The most up to date store descriptors for all stores in the node. Note that
  there may be a requirement to limit the number of times `engine.Capacity` is called as this is
  doing a physical walk of the hard drive. Consider wrapping the descriptor in a gossip `Info`.
- `Status` An ENUM or boolean value that indicates if a replica has been reserved or not. Usually
  this will be either a `reserved` or `not reserved`.

When determining if a store should reserve a new replica based on a request, it should first check
some basic conditions:

- Is the store being decommissioned?
- Are there too many reserved replicas?
- Is there enough free (non-reserved) space available on the store?

Typically the response will be a yes. A response of `not reserved` will only occur when the store
is being overloaded or is close to being overly full. Even when a reservation has been made, there
is no guarantee that the store calling will still fill the reservation. It only means that that
space has been reserved.

## Update *StorePool/Allocator* to call *ReserveReplica*

When trying to choose a new store to allocate replica a range to, after sorting all the available
ranges and ruling out the ones with incorrect attributes. The allocator picks the top store to
locate the new replica based on the heuristic discussed at the end of the of this document. It then
calls `ReserveReplica` on that node.

After each call to `ReserveReplica`, the `StorePool` on a node will update its cached
`StoreDescriptor`s. (Consider reusing or extending some of the gossip primitives as this could be
partially considered a forced gossip update.)

On a `not reserved` response, add a note that the store refused and so that it will not be
considered for new allocations for a short period (perhaps 1 second).

On a `reserved` response the replica will issue a `replicaChangeRequest` to add the chosen store.

# Drawbacks

## Too many requests

When a new node joins the cluster and it's gossiped `StoreDescriptor` makes its way to all stores
that could stand to have some ranges rebalanced, it may create too much network traffic calling the
`ReserveReplica` RPC. To ensure this doesn't happen, the RPC should be extremely quick to respond
and require very little processing on the receiving store's side, especially in the case that it is
a rejection.

# Alternate Allocation Strategies

This sections contains a collection of other techniques and strategies that were considered. Some
of these enhancements may still be included in V2.

## Other enhancements to distributed allocation

Here is a small collection to tweaks that could be used to alter how a distributed allocation could
work. These are not being implemented now, but could be considered as alternatives if the
`ReserveReplica` strategy doesn’t solve all issues.

- Make the gossiping of `StoreDescriptor`s event driven. Anytime a snapshot is applied or a
  replica is garbage collected. If no event occurs, gossip the `storeDescriptor` every
  `gossipStoresInterval`.

  This could reduce the time it takes for the updated descriptor to make its way to all other
  nodes.

- Decrease the `gossipStoresInterval` to 10 seconds so `StoreDescriptor`s are fresher.

  This adds a lot of churn to the gossiped descriptors so the increased network traffic might
  outweigh the benefits of faster rebalancing.

- Move from using gossiped `StoreDescriptor`s (updated every 60 seconds) to gossiped
 `StoreStatuses` (written every 10 seconds).

  This would require gossiping which would incur the same problem as decreasing the
  `gossipStoresInterval`.

- Based on the latest Store Descriptors, determine how many stores would likely rebalance in the
  next 10 seconds. Then, each of those stores rebalances with probability
  `1/(# of candidate stores)`. For example, suppose that we're balancing by replica count. Two
  stores have 100 replicas, and one store has 0 replicas. So, there are 2 stores that are good
  candidates to move replicas from. Each of those 2 stores would have a `1/2` probability of
  starting a rebalance. We could speed this up by doing this virtual coin toss `N` times, where `N`
  is the total number of replicas we'd like to move to the destination store.

  This might be a useful option if there is still too much thrashing when a new node is added.

- Don't try to rebalance any other replicas on a store until the previous `ChangeReplicas` call has
  finished and the snapshot has been applied.

  This limits each store to performing a single `ChangeReplica`/Snapshot at a time. It would limit
  thrashing but also greatly increase the time it takes to reach equilibrium.

## Centralized allocation strategy

One way to avoid the thrashing caused by multiple independently acting allocators is to centralize
all replica allocation. In this section, a possible centralized allocation strategy is described in
detail.

### Allocator lease acquisition

Every second, each node checks whether there’s an allocation lease holder
("allocator") through a `Get(globalAllocatorKey)`. If that returns no data, the
node tries to become the allocator lease holder using a `CPut` for that key. In
pseudo-code:

``` pseudocode
    every 60 seconds:
      result := Get(globalAllocatorKey)
      if result != nil {
        // do nothing
        return
      }
      err := CPut(globalAllocatorKey, nodeID+"-”+expireTime, nil)
      if err != nil {
        // Some other node became the allocator.
        return
      }
      // This node is now the allocator.
```

### Allocator lease renewal

Near the end of the allocation lease, the current allocator does the following:

``` golang
    err := CPut(globalAllocatorKey,
      nodeID+"-”+newExpireTime,
      nodeID+"-”+oldExpireTime)
    if err != nil {
      // Re-election failed. Step down as allocator lease holder.
      return err
    }
    // **Re-election succeeded**. We’re still the allocation lease holder.
```

For example, if the allocation lease term is 60 seconds, the current allocation lease holder could
 try to renew its lease 55 seconds into its term.

We may want to enforce artificial allocator lease term limits to more regularly
exercise the lease acquisition code.

### Updating the allocator’s *StoreDescriptors*

An allocation lease holder needs recent store information to make effective allocation decisions.

This could be achieved using either of two different mechanisms: decreasing the
interval for gossiping `StoreDescriptor`s from 60 seconds to a lower value, (perhaps 10 seconds) or
by writing the descriptors to a system keyspace and retrieving them, possibly using inconsistent
reads, (also every 10 seconds or so). Also, using `StoreStatus`es instead of descriptors is also an
option. Recall that `StoreDescriptor` updates are frequent and the allocation lease holder is the only
node making rebalancing decisions. So, the allocation lease holder could use the latest gossiped
`StoreDescriptor`s and its knowledge of the replica allocation decisions made since the last
`StoreDescriptor` gossip to derive the current state of replica allocation in the cluster.

### Centralized decision-making

Pseudo-code for centralized rebalancing:

``` pseudocode
    for _, rangeDesc := range GetAllRangeDescriptorsForCluster() {
      makeAllocationDecision(rangeDesc, allStoreDescriptors)
    }
```

The `StoreDescriptor`s are discussed in the previous section. `GetAllRangeDescriptorsForCluster`
warrants specific attention: it needs to retrieve a potentially large number of range descriptors.
For example, suppose that a cluster is storing 100 TiB of de-duplicated data. That is a minimum of
16,384,000 ranges each with an associated `RangeDescriptor`. Requiring the scanning, sorting and
decision-making based on this large of a collection could be a performance problem. There are
clearly methods which may solve some of these bottlenecking problems. Ideas include only looking to
move ranges from high to low loads or using a "power of two" technique to randomly pick two stores
when looking for a rebalance target.

### Failure modes for allocation lease holders

1. Poor network connectivity.
1. Leader node goes down.
1. Overloaded allocator node. This may be unlikely to cause problems that
   extend beyond one term. An overloaded allocator node probably
   wouldn’t complete its allocator lease renewal KV transaction before its term
   ends.

The likely failure modes can largely be alleviated by using short allocation lease terms.

### Conclusion

***Advantages***

- Less thrashing and no herding, since the allocator will know not to overload a new node.
  Distributed, independently acting allocators can make decisions that run counter to the others’
  decisions.
- Easier to debug, as there is only one place that performs the rebalancing.
- Easier to work with a CopySet style algorithm (see below for a discussion on CopySets).

***Disadvantages***

- When making rebalancing decisions, there is a lack of information that must be overcome.
  Specifically, the lack of `RangeDescriptor`s that are required when actually making the final
  decision. These are too numerous to be gossiped and must be stored and retrieved from the db
  directly. On the other hand, in a decentralized system, all `RangeDescriptor`s are already
  available directly in memory in the store.
- When dealing with a cluster that use attributes, the central allocator will have to handle all
  rebalancing decisions by either using a full knowledge of a cluster or by using subsets of the
  cluster based on combinations of all available attributes.
- As the cluster grows, there may be performance issues that arise on the central allocator. Some
  ways to alleviate this would be to ensure that the centralized allocator itself is located on the
  same node in which all required data exists (be it tables and indexes which might be required).
- If we use `CPuts` for allocator election, the range that contains the leader key becomes a single
  point of failure for the cluster. This could be alleviated by making the allocation lease holder the
  same as the range lease holder of the range holding the `StoreStatus` protos.
- More internal work needs to be done to support a centralized system. Be it via an election or
  using the range lease holder of a specific key.

***Verdict***

The main issue that causes the thrashing and overloading of stores is lack of current information.
A big part of that is the lack of knowledge about allocation decisions that are occurring while
making other decision. A centralized allocator would fix those issues. However, there are
implementation and performance issues that may arise from moving to a central allocator. Be it the
potential requirement to iterate over a set or all of the `RangeDescriptors`, dealing with
performance concerns of having all rebalancing decisions made in an expedient manner, or cases in
which the centralized allocator itself is faulty in some way, make the centralized solution less
appealing.

## CopySets

[https://www.usenix.org/system/files/conference/atc13/atc13-cidon.pdf](https://www.usenix.org/system/files/conference/atc13/atc13-cidon.pdf)

By using an algorithm to determine the best CopySets for each type of configuration (ignoring
overlap), we can limit the locations of all replicas and as the shape of the cluster changes, it
can adapt appropriately.

***Advantages***

- Greatly reduces the chance of data availability when >1 nodes die.
- No central controller/lease holder
- No fighting with all ranges when a new node joins or one is lost.
- It will take a bit of time for all nodes to receive the updated gossiped network topology, so this
  might be a good way to gate the changes.
- While there is greater complexity in the algorithm for determining the CopySets themselves, the
  allocator becomes extremely simple.

***Disadvantages***

- When a new node joins, it could be a number of replicas need to move, all at once, depending on
  how the algorithm is setup. So some artificial limiting may be required on a new node being
  added or one being removed.
- Heterogeneous environments in which stores differ in sizes makes the CopySet algorithm also
  extremely problematic.
- In dynamic environments, ones in which nodes are added and removed, the CopySet algorithm will
  lead to potential store rot.

***Verdict***

While the advantages of CopySets are clear, its disadvantages are too numerous. The CopySet
algorithms only works well in a static (no new nodes added or removed) and homogenous (all stores
are the same size) setup. Trying to work around these limitations leads one into a rabbit hole.
Here is a list of considered ways to shoehorn the algorithm to our system:

- For the dynamic cluster - recalculate the CopySets each time a node is added and removed and then
  move all misplaced replicas
- For heterogeneous stores - split all store space into blocks (of around 100 or 1000 replicas) and
  run the algorithm against that
- For zones and different replication factors - have a collection of CopySets, one for each
  combination, with overlap
- For the overlaps created by the zones fix - make CopySets that contain more than the number of
  replicas, so make the CopySet fit to 4 instead of 3, and rebalance amongst the 4 stores

Each of these "solutions" adds more complexity and takes away from the original benefit of using
the CopySet algorithm in the first place.

## CopySets emulation

As an alternative to implementing the CopySets algorithm directly, add a secondary tier of
rebalancing that adds an affinity for co-locating replicas on the same set of stores. This can be
done by simply applying a small weight to having all replicas co-located. Note that this should
not interrupt the need for real rebalancing, but all other features being equal, choose a store
with the most other replicas in common.

Testing will be required to see if this has the desired effect.

***Advantages***

- A weaker constraint than straight CopySets. CopySets prescribe exactly where each replica should
  go, while this method will let that happen organically.
- Easy to add to our current balancing heuristic.
- Reduces the chance of data loss when more than one node dies.

***Disadvantages***

- May cause more thrashing and more rebalances before equilibrium is set.
- It will never be as efficient as straight CopySets
- There is a chance that the cluster gets into a less desirable state if not done carefully.

***Verdict***

If done well, this could greatly reduce the risk of data loss when more than one node dies. This
should be in serious consideration for rebalancing V3.

# Allocation Heuristic Features

Currently, the allocator makes replica counts converge on the cluster mean range count. This
effectively reduces the standard deviation of replica counts across stores. Stores that are too
full (95% used capacity) do not receive new replicas.

Possible changes for V2:

- **Mean vs. median**
  It is possible that converging on the mean has undesirable consequences under certain scenarios.
  We may want to converge on the median instead. Care should be taken that whatever is chosen works
  well for small *and* large clusters.

For future consideration (post v2):

- **Store capacity available**
  Care must be taken. Using free disk space is problematic, because for nearly empty clusters, the
  OS install dominates disk usage. This will be one of the first aspects to look at in post V2
  work.

- **Node load**
  We will likely want to move replicas off overloaded nodes for some definition of "load."

- **Node health**
  If a node is serving requests slowly for a sustained period, we should rebalance away from that
  node. This is related but not identical to load.

# Testing Scenarios

The chosen allocation strategy should perform well in the following scenarios:

For V2:

1. Small (3 node) cluster
1. Medium (32 node) cluster
1. Bringing up new nodes in a staggered fashion
1. Bringing up new nodes all at once
1. Removing nodes, one at a time.
1. Removing and bringing a node back up after different timeouts.
1. Cluster with overloaded stores (i.e. hotspots)
1. Nearly full stores
1. Node permanently going down
1. Network slowness
1. Changing the attribute labels of a store

For future consideration (post v2):

1. Large (100+ node) cluster
1. Very large (1000+ node) cluster
1. Stores with different capacities
1. Heterogeneous nodes (CPU)
1. Replication factor > 3 (some basic testing will be done, but it won’t be concentrated on)
1. Geographically distributed cluster

It will take many iterations to arrive at a replication strategy that works for all of these cases.
These will be incorporated into unit and acceptance tests as applicable.

## Simulator

To aid in testing, the rebalancing simulator will be update to speed up testing. Some of these
updates include:

- Being able to take a running cluster and output the current and all previous states so that the
  simulator can emulate it.
- Convert the custom allocator input formats to protos.
- Update the simulator based on changes proposed in this RFC. (i.e. add replica reservations).
- Add a collection of more accurate metrics.

# Unresolved Questions

## Centralized vs Decentralized

Both approaches are clearly viable solutions with advantages and drawbacks. Is one option
objectively better than the other? It might be worthwhile to test the performance of both a central
and decentralized rebalancing scheme in different configurations under different loads. One option
would be to update the simulator to be able to test both, but that would not be an ideal
environment. How much time will this take and can it be done quickly?
