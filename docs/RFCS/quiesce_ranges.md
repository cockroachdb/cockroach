- Feature Name: Quiesce Ranges
- Status: draft
- Start Date: 2016-06-24
- Authors: David Taylor, Daniel Harrison, Tobias Schottdorf
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#357](https://github.com/cockroachdb/cockroach/issues/357)


# Summary
Replicas of inactive ranges could potentially shutdown their raft group to save
cpu, memory, and network traffic.

# Motivation
As clusters grow to potentially very large numbers of ranges, if access patterns
are such that some of those ranges see no traffic, maintaining the raft groups
and other state for them potentially wastes substantial resources for little
benefit.

Coalesced raft heartbeats potentially mitigate _some_ of this waste, in terms of
number of network requests, but after unpacking still require processing and
thus do not eliminate the marginal cost of maintaining inactive ranges.

Detecting inactive ranges and quiescing them -- shutting down their raft group
and possibly flagging them to skip other bookkeeping and maintenance -- could
offer substantial savings for clusters with very large numbers of ranges.

Additionally, some operations may be able to use the fact a range is in a
quiesced state. For example, some possible implementations of bulk ingestion of
*new* ranges could be simplified (by being able assume the raft state is
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

Once quiesced, any replica receiving a request should restart raft and trigger
an election to be able to acquire the lease. Theortically, the lease holder
could serve reads for the remainder of its lease but if a request is received,
it is reasonable to resume raft immediately as the range is no longer inactive
and thus should no longer be quiesced.

# Drawbacks
The largest drawback is reasoning about and maintaining the additional
complexity that this introduces.

This almost certainly needs to be implemented at least partially upstream in
raft -- inspecting the follower state to determine when it is safe to stop
heartbeating involves inspecting internal raft state.

Additionally, the first request to a range that has quiesced will see a
non-trivial latency penalty, as it will need to start the raft group back up
before it can proceed.

# Alternatives
Rather than quiescing the remainder of the current term, one could potentially
call an election with an extra quiesced flag. A follower that votes for that
would not expect heartbeats from that leader for that term. This seems to
involve strictly more modification to raft, and it harder to reason about as it
spans multiple terms and must thus consider multiple leader scenarios.
