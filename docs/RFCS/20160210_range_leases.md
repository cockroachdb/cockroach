- Feature Name: Node-level mechanism for refreshing range leases
- Status: completed
- Start Date: 2016-02-10
- Authors: Ben Darnell, Spencer Kimball
- RFC PR: [#4288](https://github.com/cockroachdb/cockroach/pull/4288)
- Cockroach Issue: [#315](https://github.com/cockroachdb/cockroach/issues/315),
                   [#6107](https://github.com/cockroachdb/cockroach/issues/6107)

# Summary

This is a proposal to replace the current per-range lease mechanism with
a coarser-grained per-node lease in order to minimize range lease renewal
traffic. In the new system, a range lease will have two components:
a short-lived node lease (managed by the node) and a range lease of indefinite
duration (managed by the range – as it is currently). Only the node-level lease
will need to be automatically refreshed.


# Motivation

All active ranges require a range lease, which is currently updated
via Raft and persisted in the range-local keyspace. Range leases have
a moderate duration (currently 9 seconds) in order to be responsive to
failures. Since they are stored through Raft, they must be maintained
independently for each range and cannot be coalesced as is possible
for heartbeats. If a range is active the lease is renewed before it
expires (currently, after 7.2 seconds). This can result in a
significant amount of traffic to renew leases on ranges.

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

We introduce a new node liveness system KV range at the beginning of
the keyspace (not an actual SQL table; it will need to be accessed
with lower-level APIs). This table is special in several respects: it
is gossiped, and leases within its keyspace (and all ranges that
precede it, including meta1 and meta2) use the current, per-range
lease mechanism to avoid circular dependencies. This table maps node
IDs to an epoch counter, and an expiration timestamp.

## Node liveness proto

| Column     | Description |
| ---------- | ----------- |
| NodeID     | node identifier |
| Epoch      | monotonically increasing liveness epoch |
| Expiration | timestamp at which the liveness record expires |

The node liveness system KV range stores supports a new type of range
lease, referred to hereafter as an "epoch-based" range
lease. Epoch-based range leases specify an epoch in addition to the
owner, instead of using a timestamp expiration. The lease is valid for
as long as the epoch for the lease holder is valid according to the
node liveness table. To hold a valid epoch-based range lease to
execute a batch command, a node must be the owner of the lease, the
lease epoch must match the node's liveness epoch, and the node's
liveness expiration must be at least the maximum clock offset further
in the future than the command timestamp. If any of these conditions
are not true, commands are rejected before being executed (in the case
of read-only commands) or being proposed to raft (in the case of
read-write commands).

Expiration-based range leases were previously verified when applying
the raft command by checking the command timestamp against the lease's
expiration. Epoch-based range leases cannot be independently verified
in the same way by each Raft applier, as they rely on state which may
or may not be available (i.e. slow or broken gossip connecton at an
applier). Instead of checking lease parameters both upstream and
downstream of Raft, this new design accommodates both lease types by
checking lease parameters upstream and then verifying that the lease
**has not changed** downstream. The proposer includes its lease with
Raft commands as `OriginLease`. At command-apply time, each node
verifies that the lease in the FSM is equal to the lease verified
upstream of Raft by the proposer.

To see why lease verification downstream of Raft is required,
consider the following example:

- replica 1 receives a client request for a write
- replica 1 checks the lease; the write is permitted
- replica 1 proposes the command
- time passes, replica 2 commits a new lease
- the command applies on replica 1
- replica 2 serves anomalous reads which don't see the write
- the command applies on replica 2

Each node periodically heartbeats its liveness record, which is
implemented as a conditional put which increases the expiration
timestamp and ensures that the epoch has not changed. If the epoch
does change, *all* of the range leases held by this node are
revoked. A node can only execute commands (propose writes to Raft or
serve reads) if it's the range `LeaseHolder`, the range lease epoch is
equal to the node's liveness epoch, and the command timestamp is less
than the node's liveness expiration minus the maximum clock offset.

A range lease is valid for as long as the node’s lease has the same
epoch. If a node is down (and its node liveness has expired), another
node may revoke its lease(s) by incrementing the non-live node's
liveness epoch. Once this is done the old range lease is invalidated
and a new node may claim the range lease. A range lease can move from
node A to node B only after node A's liveness record has expired and
its epoch has been incremented.

A node can transfer a range lease it owns without incrementing the
epoch counter by means of a conditional put to the range lease record
to set the new `LeaseHolder` or else set the `LeaseHolder` to 0. This is
necessary in the case of rebalancing when the node that holds the
range lease is being removed. `AdminTransferLease` will be enhanced to
perform transfers correctly using epoch-based range leases.

An existing lease which uses the traditional, expiration-based
mechanism may be upgraded to an epoch-based lease if the proposer
is the `LeaseHolder` or the lease is expired.

An existing lease which uses the epoch-based mechanism may be acquired
if the `LeaseHolder` is set to 0 or the proposer is incrementing the
epoch. Replicas in the same range will always accept a range lease
request where the epoch is being incremented -- that is, they defer to
the veracity of the proposer's outlook on the liveness record. They do
not consult their outlook on liveness and can even be disconnected
from gossip and still proceed.

[NB: previously this RFC recommended a distributed transaction to
update the range lease record. See note in "Alternatives" below for
details on why that's unnecessary.]

At the raft level, each command currently contains the node ID that
held the lease at the time the command was proposed. This will be
extended to include the epoch of that node’s lease. Commands are
applied or rejected based on their position in the raft log: if the
node ID and epoch match the last committed lease, the command will be
applied; otherwise it will be rejected.

Node liveness records are gossiped by the range lease holder for the
range which contains it. Gossip is used in order to minimize fanout
and make distribution efficient. The best-effort nature of gossip is
acceptable here because timely delivery of node liveness updates are
not required for system correctness. Any node which fails to receive
liveness updates will simply resort to a conditional put to increment
a seemingly not-live node's liveness epoch. The conditional put will
fail because the expected value is out of date and the correct liveness
record is returned to the caller.


# Performance implications

We expect traffic proportional to the number of nodes in the system.
With 1,000 nodes and a 3s liveness duration threshold, we expect every
node to do a conditional put to update the expiration timestamp every
2.4s. That would correspond to ~417 reqs/second, a not-unreasonable
load for this function. By contrast, using expiration-based leases in
a cluster with 1,000 nodes and 10,000 ranges / node, we'd expect to
see (10,000 ranges * 1,000 nodes / 3 replicas-per-range / 2.4s)
~= 1.39M reqs / second.

We still require the traditional expiration-based range leases for any
ranges located at or before the range containing liveness records. This
might be problematic in the case of meta2 address record ranges, which
are expected to proliferate in a large cluster. This lease traffic
could be obviated if we moved the node liveness records to the very
start of the keyspace, but the historical apportionment of that
keyspace makes such a change difficult. A rough calculation puts the
number of meta2 ranges at between 10 and 50 for a 10M range cluster,
so this seems safe to ignore for the conceivable future.


# Drawbacks

The greatest drawback is relying on the availability of the range
containing the node liveness records. This presents a single point of
failure which is not as severe in the current system. Even though the
first range is crucial to addressing data in the system, those reads
can be inconsistent and meta1 records change slowly, so availability
is likely to be good even in the event the first range can’t reach
consensus. A reasonable solution is to increase the number of replicas
in the zones containing the node liveness records - something that is
generally considered sound practice in any case. [NB: we also rely on
the availability of various system data. For example, if the
`system.lease` info isn't available we won't be able to serve any SQL
traffic].

Another drawback is the concentration of write traffic to the node
liveness records. This could be mitigated by splitting the node
liveness range at arbitrary resolutions, perhaps even so there’s a
single node liveness record per range. This is unlikely to be much of
a problem unless the number of nodes in the system is significant.


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
updated in a distributed transaction with the node liveness record to
ensure that the epoch counter is consistent and the start time is
greater than the prior range lease holder’s node liveness expiration
(plus the maximum clock offset)."

This has been abandoned mostly out of a desire to avoid changing the
nature of the range lease record and the range lease raft command. To
see why it's not necessary, consider a range lease being updated out
of sync with the node liveness records. That would mean either that
the epoch being incremented is older than the epoch in the liveness
record or else at a timestamp which has already expired. It's not
possible to update to a later epoch or newer timestamp than what's in
the liveness record because epochs are taken directly from the source
and are incremented monotonically; timestamps are proposed only within
the bounds by which a node has successfully heartbeat the liveness
record.

In the event of an earlier timestamp or epoch, the proposer would
succeed at the range lease, but then fail immediately on attempting to
use the range lease, as it could not possibly still have an HLC clock
time corresponding to the now-old epoch at which it acquired the lease.


# Unresolved questions

Should we have a general purpose lease protobuf to describe both?
Single lease for first range leases using current system and all other
range leases using the proposed system.

How does this mechanism inform future designs to incorporate quorum
leases?
