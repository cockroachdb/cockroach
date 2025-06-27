- Feature Name: Range leaseholder cache invalidation
- Status: draft
- Start Date: 2020-08-10
- Authors: knz, nathan, andrei
- RFC PR: [#52593](https://github.com/cockroachdb/cockroach/pull/52593)
- Cockroach Issue: [This github card](https://github.com/cockroachdb/cockroach/projects/19#card-36490737),
  [#50199](https://github.com/cockroachdb/cockroach/issues/50199)

# Summary

This RFC propose an improvement to the range cache to reduce the
unavailability and performance irregularities that occur when nodes
are restarted, migrated or simply become unavailable.

To understand the proposal, consider the following scenario:

- consider 3 nodes n1, n2, n3;
- n1 has a lease initially;
- n3 knows about n1's lease via its cache;
- then n1's lease is transfered to n2.

In that scenario, *today* node n3 does not get informed proactively
that the lease has been transferred to n2. Instead, upon the first
next request for the range, n3 will first attempt to send the request
to n1. Then it expects n1 to *redirect* the request to n2. If n1
happens to still be running, this works; however, if n1 is down (either
planned or unplanned), the request fails to reach n1, then
n3 starts trying every other replica and discover the lease on n2.

This stall and subsequent discovery process has been observed to
cause measurable latency blips and other unexpected behavior, which
this RFC offers to adddress.

The selected proposal combines two mechanisms, denoted as [solutions
4&6 below](#summary-of-solutions-and-choice): for background lease
transfers, it introduces low-traffic gossiping of new leaseholders;
and for node shutdowns/restarts it makes range caches use liveness
records.

# Motivation

## Scenarios

This RFC looks at three scenarios:

**Scenario A.** Nodes get shutdown gracefully. In this case, there is a possibility
for the node being shut down to "announce" this to the rest of the
cluster.

Today the mechanism for graceful shutdown moves all the leases from
the node being shut down to other nodes. Both the stopped node and
the other nodes with the transferred leases have up-to-date
knowledge.

(These would be n1 and n2 in the example at the beginning.)

However, 3rd party nodes that don't receive leases do not get
informed of this process. So they may retain an outdated cache entry
that points to the node being shut down. As long as the node is
down (e.g. during a restart) any further queries to ranges
previously leased to that node will experience transient failures.

(This would be n3 in the example at the beginning.)

**Scenario B.** Nodes disappear from the cluster ungracefully -- either because the
process crashes or a network partition. In this case, the remainder
of the cluster must discover this fact. We consider the case
where the remainder of the cluster votes for a lease to appear
elsewhere. The replicas involved in the lease vote know
about the new leaseholder.

(In the example at the beginning, we can assume replication factor 1;
then n2 could self elect to be leaseholder for the range when n1 dies.)

However, 3rd party nodes will not be informed of the new lease
election. So they may retain an outadated cache entry, like above,
with similar consequences.

Worse even, if there is a network partition, the attempt to connect to
the now-unreachable node may encounter a much higher delay (TCP
timeout, 30s by default—note that our existing RPC heartbeat timeout,
which drops unhealthy connection, only kicks in after a connection has
been established. For brand new connections, the timeout can be
longer.).

(This would be n3 in the example at the beginning.)

**Scenario C.** Leases get transferred from one node to another
as part of the background rebalancing that is continuously happening
throughout the cluster, or in response to zone config changes.

(In the example at the beginning, a lease gets “naturally” transferred
from n1 to n2.)

In this case, when a lease gets transferred from one to another node,
the other nodes are not notified. So they may retain an outadated
cache entry, like above, with similar consequences.

(In the example at the beginning, n3 does nopt know of the transfer
between n1 and n3.)

**All three scenarios** have the following in common:

- range lease transferred from n1 to n2.
- n1 becomes unreachable.
- n3 then mistakenly tries
  to reach out to n1 which it believes is still leaseholder.

In this context we want to:

1. reduce tail latencies by reducing mis-routing
2. help with the pathological behaviors in cases when a node is
   unresponsive and other nodes contact it even though it lost its leases
   (e.g. #37906).

# Background

## Gossip

The gossip subsystems propagates K/V pairs.

Each pair has a "time to live" (TTL) after which it disappears from
the gossip network.

Nodes can "subscribe" to particular key prefixes to receive a
notification (a code callback) every time a new value is received for
a particular key.

**All K/V data in gossip is copied on every node in the network.** (In
a lazy manner, but still: every node has a copy of all the data under
TTL.)

## Draining

A graceful node shutdown proceeds as follows (simplified):

1. stops accepting new leases.
2. stop SQL clients and distsql flows.
3. marks node as draining in its liveness record.
4. transfers leases to other nodes.
5. process terminates.

## Liveness updates

Remember from above [draining nodes update their liveness
  records](#draining). Liveness record updates are automatically
gossipped.

Separately, every time a node notices that another node has
disappeared (when it looks the node up, and notices that there was no
recent heartbeat to the liveness record), it marks it as unavailable
(bumps its epoch). This also gets automatically gossipped.

## General CockroachDB design principles

“Don't pay for what you don't use”

We try as much as possible to not store data on a node that is not
necessary for requests served by this node. In particular, we avoid
O(N) storage requirements on every node, where N is the total
number of ranges in the cluster.

“Caches don't have a minimum size requirement”

It should be possible to size caches up and down (e.g. down to obey
memory limit constraints) and still benefit from them. A cache that
only works if its size is larger than some number defined cluster-wide
is undesirable.

In the remainder of the RFC, the “cache” in this principle designates
the gossip K/V space as a whole. It means we don't want to mandate
that gossip grows to arbitrarily large number of entries to make
the solutions work.

## Multi-tenant CockroachDB

Multi-tenant splits the system in 3 components:

1. SQL proxy
2. SQL tenant servers
3. KV servers

Each SQL tenant server hosts a `DistSender` which is responsible for
routing requests to ranges and nodes. Each DistSender has a range cache.

However, only the KV servers are connected to each other and partake in
gossip. SQL tenant servers each are connected to exactly 1 KV server
and do not have access to gossip.

# Guide-level explanation

All the technical solutions that were considered here are invisible to
end-users of CockroachDB.

For the CockroachDB eng teams, see [the summary section](#summary-of-solutions-and-choice) below.

# Reference-level explanation

The following solutions were considered:

[1. Cluster-wide invalidation via RPC](#1-cluster-wide-invalidation-via-RPC)

[2. Gossiping lease transfers](#2-gossiping-lease-transfers)

[3. Gossiping lease acquisitions](#3-gossiping-lease-acquisitions)

[4. Liveness-based cache invalidation with poison](#4-liveness-based-cache-invalidation-with-poison)

[5. Liveness-based cache invalidation with bypass](#5-liveness-based-cache-invalidation-with-bypass)

[6. Best-effort gossiping of lease acquisitions](#6-best-effort-gossiping-of-lease-acquisitons)

[7. Extra drain delay](#7-extra-drain-delay)

All these solutions were proposed in the context of single-tenant CockroachDB.
For each of them, there is a trivial extension to make it work in the multi-tenant case, explained in a sub-section.

[Summary of solutions and choice](#summary-of-solutions-and-choice)

## 1. Cluster-wide invalidation via RPC

Outline:

- between steps 4 and 5 of the [draining process](#draining), the node
  shutting down calls a new RPC `InvalidateNode()` on every other node
  in the cluster to announce itself as now-unavailable.
- upon receiving `InvalidateNode()`, each other node removes any
  range entry for that node from their cache.
- upon the next first request, the nodes would go through
  the discovery process to gain knowledge of the new leaseholder.

**Pros:**

- Addesses [scenarios A and C](#scenarios).
- Simple to explain/understand, simple to implement.
- It doesn't rely on gossip, so assuming the cluster is otherwise
  healthy it will reliably invalidate/update all the caches as a
  synchronous part of the draining process.

**Cons:**

- Does not address [scenario B](#scenarios).
- A cluster-wide RPC will not propagate to nodes that are briefly
  unavailable. When these nodes come back, their cache is still
  outdated. So even [scenarios A and C](#scenarios) are only partially
  covered.
- This still depends on the lease discovery mechanism. If
  the discovery re-attempts to use the node that just went down,
  that will block/fail. If the discovery takes a while, some
  client requests will block/fail.

  This discovery process could be skipped if the `InvalidateNode()`
  request included the new leaseholders (as in the "drain record"
  proposed for option 2 below). OTOH, this would mean pushing a lot of
  data out of the draining node in a less network-efficient way than
  gossip (but more memory-efficient).

Note from Peter:

> I think it is worthwhile to quantify how much data this would
> be. Assuming 100k ranges per node and 1/3 a node is leaseholder for
> 1/3 of those replicas, we'd need to send out leaseholder info for
> 30k replicas. What is the minimal info we can send about a new
> leaseholder for a range? We need the range ID (8 bytes) and the new
> leaseholder replica ID (4 bytes)? Maybe something about the epoch of
> the new leaseholder node. So ballpark 512KB - 1MB. That is large,
> but not horrifically so. The bigger problem I see is that we'd have
> to send this to every node which doesn't scale well to large cluster
> sizes.
>
> Update: I bet a clever encoding could reduce the space overhead
> further. For example, rather than a list of invalidations, we could
> maintain a map from nodeID to a list of new leaseholder info. The new
> leaseholder info itself could be sorted by range ID and the range IDs
> delta encoded. I didn't put much thought into this, but it seems like
> we could get a fairly compact representation with some elbow grease.

### Multi-tenant variant

- The KV servers receive the `InvalidateNode()` RPC from other KV servers.
- The KV servers *forward* `InvalidateNode()` RPCs to SQL tenants,
  where the range caches are stored.

## 2. Gossiping lease transfers

Outline:

- at the end of step 4 of the [draining process](#draining), the node
  shutting down emits a gossip "drain record" keyed on its own node ID,
  with a payload containing the list of all range IDs it had
  a lease for, and for each of them the destination of the lease.
- upon receiving a drain record, every other node updates the
  entries in their lease cache to point to the new node.

**Pros:**

- Addresses [scenarios A and C](#scenarios).
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
  (i.e. eliminates one con of solution 1 above)
- By informing the other nodes of the new leaseholder,
  this solution accelerates/removes the need for the
  lease discovery mechanism.
  (i.e. eliminates the other cons of solution 1 above)

**Cons:**

- Does not address [scenario B](#scenarios).
- Each "drain record" in gossip has N entries, where N is the previous
  number of leases held in that node. This number can be arbitrarily
  large, up to the total number of ranges in the cluster.
  This violates the [design principles](#general-cockroachdb-design-principles).

  To quantify this with an example: cluster has 500k ranges, As all N
  nodes in the cluster periodically get restarted, we get N entries
  with all leases spread evenly (or not) across N entries. So N
  doesn't matter, and we have 1 integer per lease left as overall cost
  **That's less than 2MB of data total gossip storage** with 500K ranges.

  We can partially alleviate this cost by populating the lease expiry
  time as the value of [gossip TTL](#gossip). This reduces
  the space complexity from O(#ranges) to O(#active ranges).

  Note from Ben:

  > Choosing a timeout here is tricky (there's no particular reason
  > for it to be related to the lease expiry time). Too long and you
  > increase the gossip memory requirements. Too short and it may
  > expire before getting propagated to all nodes and you're back to
  > dealing with stale caches. (and because it's all on one gossip
  > key, expiration is all or nothing. It may even be possible that
  > large gossip values are more likely to experience delays
  > propagating through the system).

- Due to the eventual consistency of gossip, there's an potentially
  long amount of time between the moment a node terminates ([step 5 of
  drain](#draining)) and the moment other nodes learn of the drain
  record. During that time, requests can be mis-routed.

  This last cons can be alleviated by extending step 5 of the draining
  process by a "sleep period" to let gossip propagate, calibrated to
  the standard cross-cluster gossip delivery latency (for reference,
  that's around 10-20s for up to 100 geo-distributed nodes in normal
  operation).

### Multi-tenant variant

- The KV servers receive the "drain records" over gossip from other KV servers.
- The KV servers *forward* the drain records to SQL tenants,
  where the range caches are stored.

## 3. Gossiping lease acquisitions

Outline:

- each node acquiring a lease (either because it was given to it
  during another's node drain, or it is voted to be when a node goes
  AWOL) gossips this fact in a "lease record", keyed by the range ID
  with the lease as payload.
- upon receiving "lease records", other nodes update their cache
  with the gossiped lease, if it's newer than what they know.

This approach was prototyped here: https://github.com/cockroachdb/cockroach/pull/52572

**Pros:**

- Addresses both [scenarios A and C](#scenarios).
- Partially addresses [scenario B](#scenarios): every node
  that *already* has a cached entry for a range will gain updates with this solution.
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
  (i.e. eliminates one con of solution 1 above)
- By informing the other nodes of the new leaseholder,
  this solution accelerates/removes the need for the
  lease discovery mechanism.
  (i.e. eliminates the other cons of solution 1 above)

**Cons:**

- Only partially addresses [scenario B](#scenarios):
  if a node does not yet have a cache entry when it receives the gossip update,
  then it won't "learn" anything. So the next first request to the range
  will go through discovery and risk hitting an unavailable node.

  (Note that we do not want extend this approach to *create* cache
  entries if they did not exist already, lest the range cache blow up
  with one entry per range overall, not just entries for those ranges
  active on the node.)

- There is one "lease record" in gossip per range.
  This violates the [design principles](#general-cockroachdb-design-principles).

  To quantify this with an example: cluster has 500k ranges.  Each
  lease record is ~100 bytes (as per Andrei).  So we're looking at
  **50MB of data in gossip.** This is large but not outrageously so --
  with 100MB ethernet, that's less than 500ms to move from one node to
  another.  As per [Peter's comments
  above](#1-cluster-wide-invalidation-via-rpc), we can use clever
  compression schemes to reduce this amount of data further.

  We can also partially alleviate this cost by populating the lease expiry
  time as the value of [gossip TTL](#gossip). This reduces
  the space complexity from O(#ranges) to O(#active ranges).

  Remark from Peter:

  > Another thought is that we could batch the gossiped leaseholder
  > updates from a node. For example, when a node acquires a lease it
  > gossips that it is the leaseholder and starts a timer for X
  > seconds. Any new lease acquisitions while the timer is running get
  > bundled together and are only sent when the timer expires.

  Remark from Andrei:

  > Besides the TTL, the other big possible mitigation here is various
  > rate limits - like a rate limit per node dictating how many
  > acquisitions per second it can add to gossip, and maybe also one
  > in gossip about how many of these updates it forwards every second
  > (like, once that is reached, a node could pretend that the TTL is
  > hit for everything else it receives). I don't know how feasible
  > the latter is.

  Peter's reaction to this last remark:

  > I'd be very wary of adding additional smarts to gossip itself. The
  > first rule of using gossip: do not modify gossip.

  Remark from Ben:

  > There is some room for cleverness here — we could choose not to
  > gossip *every* lease acquisition (and similarly, we could use a
  > rather short TTL and not worry if some records get dropped before
  > propagating). Maybe have a rate limit and prioritize those ranges
  > with higher traffic, for example.
  >
  > With a short enough TTL, we can think of the gossip costs here
  > primarily in terms of the communications cost instead of the
  > memory size.

  NB: the above remarks do not take into account scenarios A and B,
  when a large number of leases get transferred at once and it's likely
  important that every other node takes note of all of it.

- Like above, there's a delay between the moment a node terminates
  ([step 5 of drain](#draining)) and the moment other nodes learn of
  the new lease records. During that time, requests can be mis-routed.

  Like above, this last cons can be alleviated by extending step 5 of
  the draining process by a "sleep period".

### Multi-tenant variant

- The KV servers receive the "lease records" over gossip from other KV servers.
- The KV servers *forward* the lease records to SQL tenants,
  where the range caches are stored.

## 4. Liveness-based cache invalidation with poison

Outline:

- [liveness updates](#liveness-updates) are gossiped as usual.
- upon receiving a liveness update that indicates that another node is
  not live, each node updates its range cache as follows:

  - invalidate all leases pointing to the now-unavailable node, ie.
    remove the lease from the cache.
  - mark that store/node ID as "not preferred" for the subsequent lease
    discovery. This *“poisons”* that store/node ID.

- upon a cache miss, the range cache will still do lease discovery,
  but it will skip over "not preferred" nodes/stores in the first
  round.

  Only if a lease is not discovered during the first round, are
  poisoned nodes added to the lookup in the second and next rounds.

**Pros:**

- Addresses both [scenarios A and B](#scenarios).
- Addresses scenario B where solution 3 above does not: the
  liveness information is also taken into account during
  cache misses.
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
  (i.e. eliminates one con of solution 1 above)
- Does not add new data in gossip. Does not violate the [design
  principles](#general-cockroachdb-design-principles).

**Cons:**

- Does not address [scenario C](#scenarios): if there is no node
  liveness changes, other nodes are not informed of lease transfers.
- Added complexity in the range cache to mark node/store IDs as
  poisoned.
- Added complexity in the lease discovery process.
- There is still a need for lease discovery; there is a chance
  that the first next replica contacted during discovery
  will take a while to respond.
- Like above, there's a delay between the moment a node terminates
  ([step 5 of drain](#draining)) and the moment other nodes learn of
  the new lease records. During that time, requests can be mis-routed.

  Like above, this last cons can be alleviated by extending step 5 of
  the draining process by a "sleep period".

### Multi-tenant variant

- The KV servers receive the liveness updates over gossip from other
  KV servers.
- The KV servers *forward* the liveness updates to SQL tenants,
  where the range caches are stored.

## 5. Liveness-based cache invalidation with bypass

Outline:

- [liveness updates](#liveness-updates) are gossiped as usual.
- nothing particular happens upon receiving a liveness update
  (other than updating the liveness cache as usual).
- upon a cache lookup, the range cache consults liveness.

  If the entry in the cache point to a node for which liveness
  currently says the node is not live, the cache entry is ignored (is
  *“bypassed”*), and another live replica is tried. If that replica
  has a lease, then it will serve the request; otherwise it will
  redirect to the updated lease and that will also refresh the cache.

**Pros:**

- Addresses both [scenarios A and B](#scenarios).
- Addresses scenario B where solution 3 above does not: the
  liveness information is also taken into account during
  cache misses.
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
  (i.e. eliminates one con of solution 1 above)
- Does not add new data in gossip. Does not violate the [design
  principles](#general-cockroachdb-design-principles).
- No new data stored in the range cache to mark nodes
  as poisoned.

**Cons:**

- Does not address [scenario C](#scenarios): if there is no node
  liveness changes, other nodes are not informed of lease transfers.
- Some complexity in cache lookups, to consult liveness
  upon both cache hits and misses.
- Added complexity during the lease discovery process.
- Like above, there's a delay between the moment a node terminates
  ([step 5 of drain](#draining)) and the moment other nodes learn of
  the new lease records. During that time, requests can be mis-routed.

  Like above, this last cons can be alleviated by extending step 5 of
  the draining process by a "sleep period".


### Multi-tenant variant

- The KV servers receive the liveness updates over gossip from other
  KV servers.
- The KV servers *forward* the liveness updates to SQL tenants,
  where the range caches are stored.

## 6. Best-effort gossiping of lease acquisitions

Outline:

- same principle as [solution 3
  above](#3-gossiping-lease-acquisitions), however the TTL on gossip
  entries is set to a short value.

**Pros:**

- Addresses [scenario C](#scenarios).
- Partially addresses [scenario A](#scenarios).
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
- By informing the other nodes of the new leaseholder,
  this solution accelerates/removes the need for the
  lease discovery mechanism.

**Cons:**

- Does not fully address [scenario A](#scenarios): the gossip records
  may not propagate throughout the entire cluster, and during a node
  shutdown (when many leases are transferred in a short time) some
  lease transfer notifications may be lost.
- Might cause a gossip storm during node restarts.

  Remark from Ben about this:

  > There is some room for cleverness here — we could choose not to
  > gossip *every* lease acquisition (and similarly, we could use a
  > rather short TTL and not worry if some records get dropped before
  > propagating). Maybe have a rate limit and prioritize those ranges
  > with higher traffic, for example.
  >
  > With a short enough TTL, we can think of the gossip costs here
  > primarily in terms of the communications cost instead of the
  > memory size.

- Like above, there's a delay between the moment a node terminates
  ([step 5 of drain](#draining)) and the moment other nodes learn of
  the new lease records. During that time, requests can be mis-routed.

  Like above, this last cons can be alleviated by extending step 5 of
  the draining process by a "sleep period".

## 7. Extra drain delay

Outline:

- an additional idle sleep is added between [steps 4 and 5 of the
  draining process](#draining).
- during that time, other nodes that are currently using ranges
  whose lease just left the node shutting down, get a chance
  to see their requests re-routed with a NotLeastHolder error
  through the node being shut down.
- when the node finally terminates its process, hopefully
  most other nodes that currently serve queries against
  those ranges have updated lease information.

**Pros:**

- Possibly addresses [scenario A](#scenarios).
- Extremely modest implementation effort.

**Cons:**

- Unclear whether it actually addresses [scenario A](#scenarios). This
  needs to be determined via experimentation.
- Does not address [scenarios B & C](#scenarios)

## Summary of solutions and choice

| Solution                                                                         | Addresses [scenario A](#scenarios) (restarts) | Addresses [scenario B](#scenarios) (node down) | Addresses [scenario C](#scenarios) (rebalance) | New data in gossip                             | New logic in cache         |
|----------------------------------------------------------------------------------|-----------------------------------------------|------------------------------------------------|------------------------------------------------|------------------------------------------------|----------------------------|
| [1](#1-cluster-wide-invalidation-via-RPC) (cluster-wide RPC)                     | partially                                     | no                                             | partially                                      | N/A                                            | no                         |
| [2](#2-gossiping-lease-transfers) (gossip transfers)                             | yes                                           | no                                             | yes                                            | sizeof(int) x #ranges  (=2MB at 500K ranges)   | no                         |
| [3](#3-gossiping-lease-acquisitions) (gossip *all* acquisitions)                 | yes                                           | partially                                      | yes                                            | sizeof(lease) x #ranges (=50MB at 500K ranges) | no                         |
| [4](#4-liveness-based-cache-invalidation-with-poison) (liveness poison)          | yes                                           | yes                                            | no                                             | N/A                                            | Poisoned node/store IDs    |
| [5](#5-liveness-based-cache-invalidation-with-bypass) (liveness bypass)          | yes                                           | yes                                            | no                                             | N/A                                            | Liveness bypass in lookups |
| [6](#6-best-effort-gossiping-of-lease-acquisitions) (gossip *some* acquisitions) | partially                                     | no                                             | yes                                            | sizeof(lease) x #ranges / saving-factor        | no                         |
| [7](#7-extra-drain-delay) (extra drain delay)                                    | yes (likely)                                  | no                                             | no                                             | N/A                                            | no                         |

Solution 1 is undesirable because it does't address [scenario
A&C fully](#scenarios): the synchronous RPC will fail to update
nodes that are temporarily unreachable.

Solution 2 is undesirable because it does not address [scenario
B](#scenarios) and has a potentially unbounded gossip storage footprint
that goes against the [design
principles](#general-cockroachdb-design-principles). (Mitigating factor: the actual size in practice is limited, see table above.)

Solution 3 is desirable because it addresses [all 3
scenarios](#scenarios). However:

- it is suspicious because of the gossip storage requirement that goes
  against the [design
  principles](#general-cockroachdb-design-principles). (Mitigating
  factor: the actual size in practice is limited, see table above.)
- it does not address scenario B fully, because we don't add cache
  entries for downed nodes, we just update existing entries.

Solutions 4 & 5 satisfy both [scenarios A & B](#scenarios) without
incurring extra gossip cost:

- However, they may require one additional cross-node hop for every
  range with outdated cache information, to discover the new lease. In
  contrast, solutions 2 & 3 & 6 pre-populate the caches with the
  location of the new lease.
- Additionally, solutions 4 & 5 do not satisfy scenario C, whereas
  solutions 3 & 6 do.

Solution 5 is simpler to implement than 4 but may incur lock
contention around the liveness cache in Go.

Solution 7 has been proposed much later during the RFC process, while
discussing solutions 1-6. It might provide an adequate answer to
[scenario A](#scenarios), to be combined with solution 6 to address
[scenario C](#scenarios).

Comment from Andrei, about the relative desirability of solutions 3 (perhaps 6)
and 4+5:

>  So, whereas I think Nathan sees more gossipping to
>  be a risk to stability, I see it as aiming to help stability. It
>  sucks for a node to lose its leases in whatever ways (e.g. say it
>  shed them away through load balancing, or its so overloaded that
>  it's failing to heartbeat its node liveness) but then that has no
>  effect because nobody can find out about where the new leases
>  are. For each individual situation where something like this
>  happens there'll be something else to fix, but still I see people
>  being up to date with lease locations as a stabilizing factor.

Other comment from Andrei, about the future utility of solutions 3 & 6:

>  I think we'd benefit from caches being kept up to date under all
>  conditions (well, maybe not under extreme conditions where there's
>  too many updates to propagate), and I'm also interested in
>  expanding a gossiping mechanism that I hope we introduce here to
>  also deal with range splits/merges. Range descriptor changes are
>  even more disruptive to traffic than lease changes, so I want to be
>  proactive about them too.

Comment from Ben, about combining solutions:

> We should make use of the gossiped liveness information [in any
> case] to either proactively invalidate the lease cache or bypass
> entries pointing to down nodes.
>
> [i.e. apply solutions 4&5 regardless of whichever solution we
> want for scenario C]
>
> It's not out of the question to gossip lease acquisitions with a
> short TTL as a best-effort supplement to pre-populate caches. It
> sounds potentially expensive so we'd need to do a lot of testing,
> but I think if this were available as an option I'd turn it on.
>
> [i.e. apply solution 6 for scenario C, with the understanding that
> scenarios A and B are covered by solutions 4&5]

More comment from Ben:

> I would eliminate solution 2 because it does not address scenario B
> at all, and does not seem substantially better than solution 1 for
> scenario A. I don't like the fact that solution 1 only addresses
> scenario A, but I could be persuaded to choose it if gossip turns
> out to be too slow or unreliable for one of the gossip-based
> solutions to work.

> Scenario C is also important, but any solution that addresses
> scenario C carries more risk since it puts a lot of stress on the
> gossip system (or requires a new non-gossip system for propagating
> this information). I think it's likely that we'd need to put some
> sort of opt-in/opt-out switch in place for this system, and I
> wouldn't that to also give up our solution for scenario A, hence the
> recommendation for a combination something like 4/5+3. (this also
> lets us split the work across multiple releases if need be)

### Selection

Some more experimentation is needed:

- check if the NotLeaseHolder error during an additional drain wait
  (solution 7) is sufficient to significantly alleviate scenario A. In
  this case, we'll do this first to score a "win" for v20.2. The
  implementation of solution 4 or 5 can then be delayed until later.

- if the first experiment fails, measure the cost of adding more data
  to gossip. Depending on how well gossip deals with augmented data,
  this may make solution 3 acceptable for all three scenarios.

- if gossip struggles, we will want to use a combination of solution 6
  (scenario C) with 4 or 5 (scenarios A&B).
  
  - For scenario A: start with solution 5, and if profiling reveals
    lock contention, look at 4 to address scenarios A&B (the most
    important for v20.2).

  - For scnario C: We'll use the result of the experiment above to
    select a suitable TTL parameter of solution 6.

## Unresolved questions

TBD
