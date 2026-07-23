- Feature Name: replica_tombstone
- Status: completed
- Start Date: 2015-07-29
- RFC PR: [#1865](https://github.com/cockroachdb/cockroach/pull/1865)
- Cockroach Issue: [#768](https://github.com/cockroachdb/cockroach/issues/768),
                   [#1878](https://github.com/cockroachdb/cockroach/issues/1878),
                   [#2798](https://github.com/cockroachdb/cockroach/pull/2798)

# Summary

Add a **Replica ID** to each replica in a range descriptor, and pass
this replica ID in all raft messages. When a replica is removed from a
node, record a tombstone with the replica ID, and reject any later
messages referring to that replica.

# Motivation

Replica removal is necessarily an asynchronous process -- the node
holding the removed replica may be down at the time of its removal,
and so any recovering node may have some replicas that should have
been removed but have not yet been garbage-collected. These nodes may
try to send raft messages that could disrupt the legitimate members of
the group. Even worse, if there has been enough turnover in the
membership of a group, a quorum of removed replicas may manage to
elect a lease holder among themselves.

We have an additional complication compared to vanilla raft because we
allow node IDs to be reused (this is necessary for coalesced
heartbeats. We may remove a replica from a node and then re-add it
later with the same node ID, so we must be able to distinguish
messages from an earlier epoch of the range.

Here is a scenario that can lead to split-brain in the current system:

1. A range R has replicas on nodes A, B, and C; C is down.
2. Nodes A and B execute a `ChangeReplicas` transactions to remove
   node C. Several more `ChangeReplicas` transactions follow, adding
   nodes D, E, and F and removing A and B.
3. Nodes A and B garbage-collect their copies of the range.
4. Node C comes back up. When it doesn't hear from the lease holder of
   range R, it starts an election.
5. Nodes A and B see that node C has a more advanced log position for
   range R than they do (since they have nothing), so they vote for it.
   C becomes lease holder and sends snapshots to A and B.
6. There are now two "live" versions of the range. Clients
   (`DistSenders`) whose range descriptor cache is out of date may
   talk to the ABC group instead of the correct DEF group.

The problems caused by removing replicas are also discussed in   [#768](https://github.com/cockroachdb/cockroach/issues/768).

# Detailed design

A **Replica ID** is generated with every `ChangeReplicas` call and
stored in the `Replica` message of the range descriptor. Replica IDs
are monotonically increasing within a range and never reused. They are
generated using a `next_replica_id` field which will be added to the
`RangeDescriptor` (alternative generation strategies include using the
raft log position or database timestamp of the `EndTransaction` call
that commits the membership change, but this information is less
accessible at the point where it would be needed).

The `ReplicaID` replaces the current `RaftNodeID` (which is
constructed from the `NodeID` and `StoreID`). Raft only knows about
replica IDs, which are never reused, so we don't have to worry about
certain problems that can aries when node IDs are reused (such as vote
changes or log regression. `MultiRaft` is responsible for mapping
`ReplicaIDs` to node and store IDs (which it must do to coalesce
heartbeats). This is done with a new method on the
`WriteableGroupStorage` interface, along with an in-memory cache. Note
the node and store IDs for a given replica never change once that
replica is created, so we don't need to worry about synchronizing or
invalidating entries in this cache.

The raft transport will send the node, store, and replica ID of both
the sender and receiver with every `RaftMessageRequest`. This is how
the `Store` will learn of the replica ID for new ranges (the sender's
IDs must be inserted in the replica ID cache, so we can route
responses that do not yet appear in any range descriptor we have
stored). The `Store` will drop incoming messages with a replica ID
that is less than the last known one for the range (in order to
minimize disruption from out-of-date servers).

The `DistSender` will also send the replica ID in the header of every
request, and the receiver will reject requests with incorrect replica
IDs (This will be rare since a range will normally be absent from a
node for a time before being re-replicated).

When a replica is garbage-collected, we write a **tombstone**
containing the replica ID, so that the store can continue to drop
out-of-date raft messages even after the GC.

In the scenario above, with this change node C would send node A and
B's replica IDs in its vote requests. They would see that they have a
tombstone for that replica and not recreate it.

# Drawbacks

Tombstones must be long-lived, since a node may come back online after
a lengthy delay. We cannot garbage-collect tombstones unless we also
guarantee that no node will come back after a period of time longer
than the tombstone GC time.

# Alternatives

If we had an explicit replica-creation RPC instead of creating them
automatically on the first sighting of a raft message, this problem
may go away. However, doing so would be tricky: this RPC would need to
be retried in certain circumstances, and it is difficult to
distinguish cases where a retry is necessary from cases that lead to
the problem discussed here.

# Unresolved questions

* How long must we wait before garbage-collecting tombstones? Can we
  do it at all?
