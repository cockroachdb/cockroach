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
the raft log. These leases have a moderate duration (currently 9
seconds) in order to be responsive to failures. Since they are stored
in the raft log, they must be managed independently for each range and
cannot be coalesced as is possible for heartbeats. If a range is
active the lease is renewed before it expires (currently, after 7.2
seconds). This results in a significant amount of traffic to renew
leases on ranges.

A motivating example is a table with 10,000 ranges experience heavy
read traffic. If the primary key for the table is chosen such that
load is evenly distributed, then read traffic will likely keep each
range active. The range lease must be renewed in order to serve
consistent reads. For 10,000 ranges, that would require 1,388 Raft
commits per second. This imposes a drain on system resources that
grows with the dataset size.

An insight which suggests possible alternatives is that renewing
10,000 range leases is duplicating a lot of work in a system which has
only a small number of nodes. In particular, we mostly care about node
liveness. Currently, each replica holding range leases must
individually renew. What if we could have the node renew for all of
its replicas holding range leases at once?


# Detailed design

We introduce a new node lease table at the beginning of the keyspace
(not an actual SQL table; it will need to be accessed with lower-level
APIs). This table is special in several respects: it is gossiped, and
leases within its keyspace (and all ranges that precede it, including
meta1 and meta2) use the current, per-range lease mechanism to avoid
circular dependencies. This table maps node IDs to an epoch counter,
and an expiration timestamp.

## Liveness table

Column|     Description
NodeID|     node identifier
Epoch|      monotonically increasing liveness epoch
Expiration| timestamp at which the liveness record expires

Each node periodically performs a conditional put to its node lease to
increase the expiration timestamp and ensure that the epoch has not
changed. If the epoch does change, *all* of the range leases held by
this node are revoked. A node *must not* propose any commands with a
timestamp greater than its expiration timestamp modulo the maximum
clock offset.

A range lease is valid for as long as the node’s lease has the same
epoch. If a node is down (and its node lease has expired), another
node may revoke its lease(s) by incrementing the node lease
epoch. Once this is done the old range lease is invalidated and a new
node may claim the range lease.

A node can transfer a range lease it owns without incrementing the
epoch counter by means of a conditional put to the range lease record
to set the new leaseholder or else set the leaseholder to 0. This is
necessary in the case of rebalancing when the node that holds the
range lease is being removed. `AdminTransferLease` will be enhanced to
perform transfers correctly using node lease style range leases.

Nodes which propose or transfer an epoch-based range lease must
themselves be live according to the liveness table. Keep in mind that
a node considers itself live according to whether it has successfully
written a recent liveness record which proves its liveness measured
by current time vs the record's expiration module the maximum clock
offset.

To propose an epoch-based range lease, the existing lease must either
be a traditional, expiration-based lease, with the proposer being the
leaseholder or the lease being expired, -or- be an epoch-based lease
where the proposer is the leaseholder or the leaseholder is 0, or have
an old epoch. Other replicas in the same range will always accept a
range lease request where the epoch is being incremented -- that is,
they defer to the veracity of the proposer's outlook on the liveness
table. They do not consult their outlook on the liveness table and can
even be disconnected from gossip.

[NB: previously this RFC recommended a distributed transaction to
update the range lease record. See note in "Alternatives" below for
details on why that's unnecessary.]

In addition to nodes updating their own liveness entry with ongoing
updates via conditional puts, non-leaseholder nodes may increment
the epoch of a node which has failed to update its heartbeat in time
to keep it younger than the threshold liveness duration.

At the raft level, each command currently contains the node ID that
held the lease at the time the command was proposed. This will be
extended to include the epoch of that node’s lease. Commands are
applied or rejected based on their position in the raft log: if the
node ID and epoch match the last committed lease, the command will be
applied; otherwise it will be rejected.


# Performance implications

We expect traffic proportional to the number of nodes in the system.
With 1,000 nodes and a 9s liveness duration threshold, we expect every
node to do a conditional put to update the heartbeat timestamp every
7.2s. That would correspond to ~140 reqs/second, a not-unreasonable
load for this function.

We still require the traditional expiration-based range leases for any
ranges located at or before the liveness table's range. This might be
problematic in the case of meta2 address record ranges, which are
expected to proliferate in a large cluster. This lease traffic could
be obviated if we moved the node liveness table to the very start of
the keyspace, but the historical apportionment of that keyspace makes
such a change difficult.


# Drawbacks

The greatest drawback is relying on the availability of the node lease
table. This presents a single point of failure which is not as severe
in the current system. Even though the first range is crucial to
addressing data in the system, those reads can be inconsistent and
meta1 records change slowly, so availability is likely to be good even
in the event the first range can’t reach consensus. A reasonable
solution is to increase the number of replicas in the zones including
the node lease table - something that is generally considered sound
practice in any case. [NB: we also rely on the availability of various
system tables. For example, if the `system.lease` table is unavailable
we won't be able to serve any SQL traffic].

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

## Use of distributed txn for updating liveness records

The original proposal mentioned: "The range lease record is always
updated in a distributed transaction with the -node lease record to
ensure that the epoch counter is consistent and -the start time is
greater than the prior range lease holder’s node-lease expiration
(plus the maximum clock offset)."

This has been abandoned mostly out of a desire to avoid changing the
nature of the range lease record and the range lease raft command. To
see why it's not necessary, consider a range lease being updated out
of sync with the node liveness table. That would mean either that the
epoch being incremented is older than the epoch in the liveness table
or else at a timestamp which has already expired. It's not possible to
update to a later epoch or newer timestamp than what's in the liveness
table because epochs are taken directly from the liveness table and
are incremented monotonically; timestamps are proposed only within the
bounds by which a node has successfully heartbeat the liveness table.

In the event of an earlier timestamp or epoch, the proposer would
succeed at the range lease, but then fail immediately on attempting to
use the range lease, as it could not possibly still have an HLC clock
time corresponding to the now-old epoch at which it acquired the lease


# Unresolved questions

Should we have a general purpose lease protobuf to describe both?
Single lease for first range leases using current system and all other
range leases using the proposed system.

How does this mechanism inform future designs to incorporate quorum
leases?

TODO(peter): What is the motivation for gossiping the node lease
table? Gossiping means the node's will have out of date info for the
table.
