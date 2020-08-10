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
n3 starts trying every other node and discover the lease on n2.

This stall and subsequent discovery process have been observed to
cause measurable latency blips and other unexpected behavior, which
this RFC offers to adddress.

To address this, multiple proposals have been considered (detailed
below).

The selected proposal uses the existing liveness records, and tweaks
the graceful shutdown process ("drain") to give some time for the
liveness record to propagate to other nodes.

# Motivation

## Scenarios

This RFC looks at two scenarios:

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

However, 3rd party nodes that were not in the replica group will
not be informed of the new lease election. So they may retain
an outadated cache entry, like above, with similar consequences.

Worse even, if there is a network partition, the attempt to
connect to the now-unreachable node may encounter a much higher delay
(TCP timeout, 30s by default).

(This would be n3 in the example at the beginning.)

**Both scenarios** have the following in common:

- range lease transferred from n1 to n2.
- n1 becomes unreachable.
- n3 is not part of the replication group, and mistakenly tries
  to reach out to n1 which it believes is still leaseholder.

We want to avoid this mistaken routing and associated delays.

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

For the CockroachDB eng teams, the "bird eye's view" of the selected
solution is like this:

- Already true previously: when a node shuts down, or when a node is
  found dead by a lease transfer, its liveness record is updated in
  persistent KV.
- **New:** each range cache subscribes to liveness updates.
  When a node liveness record is updated, range caches "invalidate"
  any entries related to that node in the cache. This way
  a node that's not currently available is not considered
  to route range requests by DistSender.
- **New:** draining nodes now wait for a bit before terminating the
  process, to give a chance for other nodes in the cluster to update
  their caches.

# Reference-level explanation

The following solutions were considered:

[1. Cluster-wide invalidation via RPC](#1-cluster-wide-invalidation-via-RPC)

[2. Gossiping lease transfers](#2-gossiping-lease-transfers)

[3. Gossiping lease acquisitions](#3-gossiping-lease-acquisitions)

[4. Liveness-based cache invalidation with poison](#4-liveness-based-cache-invalidation-with-poison)

[5. Liveness-based cache invalidation with bypass](#4-liveness-based-cache-invalidation-with-bypass)

All these solutions were proposed in the context of single-tenant CockroachDB.
For each of them, there is a trivial extension to make it work in the multi-tenant case, explained in a sub-section.

[Summary of solutions and choice](#summary-of-solutions-and-choice)

## 1. Cluster-wide invalidation via RPC

Outline:

- between steps 4 and 5 of the [draining process](#draining), the node
  shutting down calls a new RPC `InvalidateNode()` on every other node
  in the cluster to announce itself as now-unavailable.
- upon receiving `InvalidateNode()`, each other node removes any
  range entry for that node from their lease cache.
- upon the next first request, the nodes would go through
  the discovery process to gain knowledge of the new leaseholder.

**Pros:**

- Addesses [scenario A](#scenarios).
- Simple to explain/understand, simple to implement.

**Cons:**

- Does not address [scenario B](#scenarios).
- A cluster-wide RPC will not propagate to nodes that are briefly
  unavailable. When these nodes come back, their cache is still
  outdated. So even [scenario A](#scenarios) is partially covered.
- This still depends on the lease discovery mechanism. If
  the discovery re-attempts to use the node that just went down,
  that will block/fail. If the discovery takes a while, some
  client requests will block/fail.

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

- Addresses [scenario A](#scenarios).
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
  (i.e. eliminates one cons of solution 1 above)
- By informing the other nodes of the new leaseholder,
  this solution accelerates/removes the need for the
  lease discovery mechanism.
  (i.e. eliminates the other cons of solution 1 above)

**Cons:**

- Does not address [scenario B](#scenarios).
- Each "drain record" in gossip has N entries, where N is the previous
  number of leases in that node. This number can be arbitrarily
  large, up to the total number of ranges in the cluster.
  This violates the [design principles](#general-cockroachdb-design-principles).

  We can partially alleviate this cost by populating the lease expiry
  time as the value of [gossip TTL](#gossip). This reduces
  the space complexity from O(#ranges) to O(#active ranges).

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

- Addresses both [scenarios A and B](#scenarios).
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
  (i.e. eliminates one cons of solution 1 above)
- By informing the other nodes of the new leaseholder,
  this solution accelerates/removes the need for the
  lease discovery mechanism.
  (i.e. eliminates the other cons of solution 1 above)

**Cons:**

- There is one "lease record" in gossip per range.
  This violates the [design principles](#general-cockroachdb-design-principles).

  We can partially alleviate this cost by populating the lease expiry
  time as the value of [gossip TTL](#gossip). This reduces
  the space complexity from O(#ranges) to O(#active ranges).

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
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
  (i.e. eliminates one cons of solution 1 above)
- Does not add new data in gossip. Does not violate the [design
  principles](#general-cockroachdb-design-principles).

**Cons:**

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
- By using gossip, the data will eventually reach every other
  node even those that were temporarily unavailable.
  (i.e. eliminates one cons of solution 1 above)
- Does not add new data in gossip. Does not violate the [design
  principles](#general-cockroachdb-design-principles).
- No new data stored in the range cache to mark nodes
  as poisoned.

**Cons:**

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

## Summary of solutions and choice

| Solution                                              | Addresses scenario A | Addresses scenario B | New data in gossip | New logic in cache         |
|-------------------------------------------------------|----------------------|----------------------|--------------------|----------------------------|
| [1](#1-cluster-wide-invalidation-via-RPC)             | yes                  | no                   | N/A                | no                         |
| [2](#2-gossiping-lease-transfers)                     | yes                  | no                   | O(#leases)         | no                         |
| [3](#3-gossiping-lease-acquisitions)                  | yes                  | yes                  | O(#ranges)         | no                         |
| [4](#4-liveness-based-cache-invalidation-with-poison) | yes                  | yes                  | N/A                | Poisoned node/store IDs    |
| [5](#4-liveness-based-cache-invalidation-with-bypass) | yes                  | yes                  | N/A                | Liveness bypass in lookups |

Solutions 1 & 2 are undesirable becasue they don't address [scenario
B](#scenarios).

Solution 3 is undesirable because of the gossip storage requirement
that goes against the [design
principles](#general-cockroachdb-design-principles).

Solutions 4 & 5 satisfy both scenarios without incurring extra gossip
cost. However, they may require one additional cross-node hop for
every range with outdated cache information, to discover the new
lease. In contrast, solutions 2 & 3 pre-populate the caches with the
location of the new lease.

Solution 5 is simpler to implement than 4 but may incur lock
contention around the liveness cache in Go.

## Unresolved questions

TBD
