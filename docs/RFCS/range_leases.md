- Feature Name: Node-level mechanism for refreshing range leases
- Status: draft
- Start Date: 2016-02-10
- Authors: Ben Darnell, Spencer Kimball
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue:


# Summary

This is a proposal to replace the current per-range lease mechanism with
a coarser-grained per-node lease in order to minimize range lease renewal
traffic. In the new system, a range lease will have two components:
a short-lived node lease (managed by the node) and a range lease of indefinite
duration (managed by the range – as it is currently). Only the node-level lease
will need to be automatically refreshed.


# Motivation

All active ranges require a range lease, which is currently stored in
the raft log. These leases have a short duration (currently 1 second)
in order to be responsive to failures. Since they are stored in the
raft log, they must be managed independently for each range and cannot
be coalesced as is possible for heartbeats. This results in a very
large amount of traffic to renew leases on ranges.

A motivating example is a table with 1,000 ranges experience heavy
read traffic. If the primary key for the table is chosen such that
load is evenly distributed, then read traffic will likely keep each
range active. The range lease must be renewed constantly in order to
serve consistent reads. For 1,000 ranges, that would require 1,000
Raft commits per second. This seems untenable even for this simple
example, especially in light of a read-only workload.

An insight which suggests possible alternatives is that renewing 1,000
range leases is duplicating a lot of work in a system which has only
a small number of nodes. In particular, we mostly care about node
liveness. Currently, each replica holding range leases must
individually renew. What if we could have the node renew for all of
its replicas holding range leases at once?


# Detailed design

We introduce a new node lease table at the beginning of the keyspace
(not an actual SQL table; it will need to be accessed with lower-level
APIs). This table is special in several respects: it is gossipped, and
leases within its keyspace (and all ranges that precede it, including
meta1 and meta2) use the current, per-range lease mechanism to
avoid circular dependencies. This table maps node IDs to an epoch
counter and a lease expiration timestamp.

The range lease is moved from a special raft command (which
writes to a range-local, non-transactional range lease key) to a
transactional range-local key (similar to the range descriptor). The
range lease identifies the node that holds the lease and its
epoch counter. It has a start timestamp but does not include an
expiration time. The lease record is always updated in a distributed
transaction with the node lease record to ensure that the epoch
counter is consistent and the start time is greater than the prior
lease holder’s node lease expiration (plus the maximum clock offset).

Each node periodically performs a conditional put to its node lease to
increase the expiration timestamp and ensure that the epoch has not
changed. If the epoch does change, *all* of the range leases held by
this node are revoked. A node *must not* propose any commands with a
timestamp greater than the latest expiration timestamp it has written
to the lease table.

A range lease is valid for as long as the node’s lease has the same
epoch. If a node is down (and its node lease has expired), another
node may revoke its lease(s) by incrementing the epoch. Once this is
done the old lease is invalidated and a new node may claim the lease.

A node may give up its own lease without incrementing the epoch
counter by means of a conditional put to set the leaseholder to
zero. This is necessary in the case of rebalancing when the node that
holds the lease is being removed. Leases can be transferred using a
similar mechanism, for example to respond to changing geographic
traffic patterns.

A replica claims the range lease by executing a transaction which
reads the replica’s node lease epoch and then does a conditional put
on the range-local range lease. The transaction record will be local
to the range lease record, so intents will always be cleaned on
commit or abort. There are never intents on the node lease because
they’re only updated via a conditional put. Nodes either renew based
on their last read value, or revoke another node’s lease based on the
last gossiped value. The conditional put either succeeds or fails, but
is never written as part of a transaction.

At the raft level, each command currently contains the node ID that
held the lease at the time the command was proposed. This will be
extended to include the epoch of that node’s lease. Commands are
applied or rejected based on their position in the raft log: if the
node ID and epoch match the last committed lease, the command will be
applied; otherwise it will be rejected.


# Drawbacks

The greatest drawback is relying on the availability of the node lease
table. This presents a single point of failure which is not as severe
in the current system. Even though the first range is crucial to
addressing data in the system, those reads can be inconsistent and
meta1 records change slowly, so availability is likely to be good even
in the event the first range can’t reach consensus. A reasonable
solution is to increase the number of replicas in the zones including
the node lease table – something that is generally considered sound
practice in any case.

Another drawback is the concentration of write traffic to the node
lease table. This could be mitigated by splitting the node lease table
at arbitrary resolutions, perhaps even so there’s a single node lease
per range. This is unlikely to be much of a problem unless the number
of nodes in the system is significant.


# Alternatives

The cost of the current system of per-range lease renewals could be
reduced easily by changing some constants (increasing range sizes and
lease durations), although the gains would be much less than what is
being proposed here and read-heavy workloads would still spend much of
their time on lease updates.

If we used copysets, there may be an opportunity to maintain lease holder
leases at the granularity of copysets.


# Unresolved questions

Should we have a general purpose lease protobuf to describe both?
Single lease for first range leases using current system and all other
range leases using the proposed system.

How does this mechanism inform future designs to incorporate quorum
leases?
