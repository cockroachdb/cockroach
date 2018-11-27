- Feature Name: Quiesce Ranges
- Status: obsolete
- Start Date: 2016-06-24
- Authors: David Taylor, Daniel Harrison, Tobias Schottdorf
- RFC PR: #8811
- Cockroach Issue: [#357](https://github.com/cockroachdb/cockroach/issues/357)


# Summary
Replicas of inactive ranges could potentially shutdown their raft group to save
cpu, memory, and network traffic.

# Motivation
As clusters grow to potentially very large numbers of ranges, if access patterns
are such that some of those ranges see no traffic, maintaining the raft groups
and other state for them wastes substantial resources for little benefit.

Coalesced raft heartbeats potentially mitigate _some_ of this waste, in terms of
number of network requests, but after unpacking still require processing and
thus do not eliminate the marginal cost of maintaining inactive ranges.

Detecting inactive ranges and quiescing them -- shutting down their raft group
and possibly flagging them to skip other bookkeeping and maintenance -- could
offer substantial savings for clusters with very large numbers of ranges.

Additionally, some operations may be able to use the fact a range is in a
quiesced state. For example, some possible implementations of bulk ingestion of
*new* ranges could be simplified (by being able to assume the raft state is
frozen).

# Detailed design
Replicas of a quiescent range do not maintain a raft group.

The most significant challenge is coordinating the transition from active to
quiesced: a leader choosing to not maintain a quiesced raft group is easily
mistaken for failing to maintain an active one.

To enter the quiesced state, the leader sends a quiesce command to all
followers. Upon receipt, followers stop expecting heartbeats for the remainder
of the term. A raft group is quiesced only for one term.

Once followers are no longer expecting heartbeats, the leader can stop sending
them. If, however, it does this before a follower has received and processed the
quiesce command, that follower will assume the leader is lost and call a new
election, waking the group back up. To prevent this, the leader should continue
sending heartbeats for some period after the command is issued -- specifically
either until all followers acknowledge it or until the election timeout has
elapsed (the election timeout is, by definition, the amount of time after which
a node is assumed to be unreachable).

If a follower is unreachable for more than the election timeout and and thus is
unaware the group has quiesced, it will attempt to call an election and wake the
range back up. This however would happen anyway -- since it was unreachable for
more than the election timeout, it was going to call the election anyway, thus
ending the term that quiesced.

Once quiesced, any replica asked to propose a command should restart raft and
trigger an election to be able to acquire the lease.

A node that is partitioned or killed when a range quiesces will be unaware of
that change when it returns, and may initiate a new election causing the range
to return to actively maintaining raft until it quiesces again.

# Drawbacks
The largest drawback is reasoning about and maintaining the additional
complexity that this introduces.

As mentioned above, this only benefits clusters where access patterns are such
that large numbers of ranges see no activity.

This almost certainly needs to be implemented at least partially upstream in
raft -- inspecting the follower state to determine when it is safe to stop
heartbeating involves inspecting internal raft state. Upstream raft may not see
as much value in incorporating this as the majority of users likely have only a
single raft group, so they would see little benefit.

Additionally, the first request to a range that has quiesced will see a
non-trivial latency penalty, as it will need to start the raft group back up
before it can proceed (though a in-place revival by the leader discussed below
could mitigate this).

Killed or partitioned nodes will spuriously wake groups that had quiesced in
their absence, but as the range will still have not seen recent activity, it can
re-quiesce shortly thereafter. Ccombined with the fact that this should be some
what rare in normal operation, the expected cost of spurious wakeups should be
small (and no worse than if the ranges didn't quiesce at all).

# Alternatives
Rather than quiescing the remainder of the current term, one could potentially
call an election with an extra quiesced flag. A follower that votes for that
would not expect heartbeats from that leader for that term. This seems to
involve strictly more modification to raft, and it harder to reason about as it
spans multiple terms and must thus consider multiple leader scenarios.

As a follow-up optimization, the former leader of a quiesced range could
optimistically attempt to revive the quiesced group *in-place*, without starting
a new term, by resuming heartbeats and proposing commands. Any other node
wishing to revive the group would still need to initiate an election for a new
term, as would the former leader if it failed to get a majority of ACKs on the
first command it proposed after waking. This can be done purely as an
optimization at a later time.
