# Follower reads are consistent reads at historical timestamps from follower replicas

Original author: Spencer Kimball

## Introduction

Follower reads make the non-leader replicas in a range suitable
sources for historical reads. The key enabling technology is the
propagation of a **closed timestamp** from the Raft leader to
followers. The closed timestamp (CT) is more than just a timestamp. It
is a set of conditions, that if met, guarantee that a follower replica
has all state necessary to satisfy reads at or before the CT.

Follower reads can increase read throughput by allowing every replica
to serve read requests. For geo-distributed clusters, this can
translate to significant latency improvements. Follower reads are not
intended to serve consistent reads at the current timestamp, and so
are suitable only for workloads which can tolerate a slightly stale
version of the data (on the order of seconds). Note, however, that
the historical timestamp chosen to do follower reads will reveal a
consistent view of the database at that timestamp.

This tech note is motivated by follower reads, but the CT mechanism
has far reaching consequences for database functionality.

- Because it records successively higher timestamps at which a
  replica's contents are completely valid, it can be used to implement
  disaster recovery for a cluster by rebuilding total cluster state at
  the minimum of surviving replicas' CTs.
- It serves as a checkpoint for change data capture consumers,
  informing them when it is safe to consider all data to have been
  received at or before the CT from a source replica.

## Follower reads

A more extended description of CT follows, but let's start with how CT
can be used to implement follower reads, which is straightforward.  At
the distributed sender, if a read is for a historical timestamp
earlier than the current time less a target duration (equal to the
target closed timestamp interval plus the raft heartbeat interval), it
is sent to the nearest replica (as measured by health, latency, and
locality), instead of to the leaseholder.

When a read is handled by a replica, and it is not the leaseholder, it
is checked against the CT conditions and serviced locally if they are
met. This avoids forwarding to the leaseholder and avoids updating the
timestamp cache. Note that closed timestamps and follower reads are
supported only on ranges with epoch-based leases.

Easy, right? The hard part is the closed timestamp. Read on.

## Closed Timestamp: overview

A closed timestamp (CT) is a set of conditions, which if true,
guarantee that a replica has all data necessary to serve reads at a
timestamp. Information about CTs is sent with coalesced heartbeats.
- **Min log index**: the minimum Raft log index must have been
  committed on the follower. This value is transmitted, per-range,
  with Raft heartbeats.
- **Closed timestamp**: the timestamp, before which reads may be
  served by the replica if it has committed at least up to the min log
  index. This value typically trails the clock of the originating
  store by at least a constant target duration. This value is
  transmitted with coalesced Raft heartbeats, for an entire store, on
  behalf of all ranges for which it is the leader / leaseholder.
- **Liveness epoch**: the liveness epoch is transmitted for the entire
  store in the top-level coalesced heartbeat. When the liveness epoch
  changes, all prior information held on behalf of the sender is
  discarded as no longer trustable. While the liveness epoch was
  incremented, another replica likely held the lease and so we can no
  longer safely apply new store-wide closed timestamps to formerly
  quiesced ranges.
- **Unquiesced ranges**: a set of range IDs which were previously
  quiesced but have been unquiesced on the sender, since the last
  heartbeat. Unquiesced ranges can't rely on the store-wide closed
  timestamp sent with coalesced heartbeats, and may instead only use
  the closed timestamp in conjunction with explicit min log indexes
  sent per-range. A range in the unquiesced set may not have
  associated per-range info in the same coalesced heartbeat. This is
  because Raft schedules heartbeats independently of the first command
  sent after a range is unquiesced.

  The unquiesced ranges set is necessary, despite the likelihood that
  a per-range heartbeat will be received for each of the ranges in the
  same coalesced heartbeat, because when a range is unquiesced, the
  exact timing for when a per-range heartbeat is scheduled and
  coalesced is independent of when the next coalesced heartbeat goes
  out.
- **Sequence**: a sequence number incremented and sent with successive
  coalesced heartbeats. This allows the recipient to realize when a
  heartbeat was missed, and to discard information it holds about
  quiesced ranges.

When it propagates a CT, a Raft leader / leaseholder is making the
following guarantee: *Every Raft command proposed after the min log
index will be at a later timestamp than the closed timestamp.*

CTs are propagated via Raft heartbeats, which are sent by the Raft
leader to followers when a range is active (not quiesced). CTs,
specifically, are sent *only* when the Raft leader is also the range
leaseholder. This is necessary because the leaseholder makes the
command proposal, which is where a replica guarantees the command is
forwarded to at least the min proposal timestamp. In the interests of
building the explanation in pieces, let's first not consider how CTs
work when ranges are quiesced. Instead, let's simplify by allowing CTs
to be valid only when explicitly received on a heartbeat for a
range. Let's look next at the stated guarantee.

## Maintaining the stated guarantee

*Every Raft command proposed after the min log index will be at a
later timestamp than the closed timestamp.*

Each `Store` object maintains a **min proposal timestamp** (MPT),
which is linked to Raft proposals in order to provide successively
higher closed timestamps to send with heartbeats. The min proposal
timestamp, as the name suggests, maintains a low water timestamp for
command proposals. Similar to the timestamp cache, when commands are
evaluated on the write path, their timestamp is forwarded to be at
least the MPT.

The MPT is a slightly tricky object. It consists of a reader/writer
mutex and two timestamps (`cur` and `last`), each with a ref count.
The `cur` timestamp is the value of the min proposal timestamp used to
forward command timestamps, while `last` is the value of the min
proposal timestamp sent as the store-wide closed timestamp on
coalesced heartbeats. This dichotomy here allows us to pin a value for
the MPT when a command first begins execution in
`Replica.tryExecuteWriteBatch`, until after it is proposed. The goal
is to avoid holding a reader lock during command evaluation and Raft
proposal.

When the MPT is accessed in `Replica.tryExecuteWriteBatch`, the `cur`
timestamp is returned and its ref count is incremented. After command
proposal, a cleanup function is invoked which decrements either the
`cur` or `last` ref count, depending on which timestamp was originally
returned (note that if you start with `cur=t1`, the MPT may move to
`cur=t2, last=t1` while you are proposing your command). The MPT is
also accessed when sending CTs with Raft heartbeats. This happens just
once every time heartbeats are sent from a node to peer nodes in the
`Store.coalescedHeartbeatsLoop`.

As long as the `last` ref count is non-zero, the `last` timestamp is
returned for use with CTs. This ensures that while any commands may
still be being proposed to Raft using the `last` timestamp as the low
water mark, no CTs will be sent with a higher closed timestamp. If the
`last` ref count is zero, then the `last` timestamp is returned for
the current round of heartbeats, while the `cur` timestamp and ref
count are transferred to `last`. A new `cur` timestamp is set to
`hlc.Clock.Now() - ClosedTimestampInterval` with ref count 0.

The MPT specifies timestamps `cur=C` and `last=L`, where `C > L`.  On
each successive round of heartbeats, a node sends CTs with timestamps
set to `L(1) <= L(2) <= L(3) <= ... <= L(N)`, and enforces that all
commands proposed between heartbeats have command timestamps forwarded
to at least `C(1) <= C(2) <= C(3) <= ... <= C(N)`. Because all
commands proposed between heartbeats `K` and `K+1` will have at least
timestamp `C(K)`, and `C(K) > L(K)`, then a heartbeat for any range
will report a min log index at which that command and all subsequent
commands must have a timestamp greater than `L(k)`, which proves the
stated guarantee.

### What about leadership / leaseholdership changes and node restarts?

When leadership / leaseholdership migrates between nodes, the
timestamp cache takes precedence over the MPT and prevents any command
proposals earlier than the new leader node's `hlc.Clock.Now()` plus
the max clock offset. This allows the MPT used by two successive
nodes, and by extension the CT, to actually regress, while still
maintaining the critical guarantee that *every Raft command proposed
after the min log index will be at a later timestamp than the closed
timestamp*. Note that the guarantee itself says nothing about how the
MPT or CT vary as leadership / leaseholdership changes.

## Quiescence

Things get more interesting when a range quiesces. Replicas of
quiesced ranges no longer receive heartbeats. However, if a replica is
quiesced, we can continue to rely on the most recent *store-wide* CT
timestamp supplied in coalesced heartbeats, so long as the liveness
epoch (which continues to be reported with heartbeats) remains
stable. In order to do this, a node must guarantee the following
contract: once a min log index is sent for a range, the sender (while
the liveness epoch remains the same) must on subsequent heartbeats
either:
- Return a new min log index for a range if not quiesced.
- Return the range ID if the range has become unquiesced.
- Return nothing. If the range is quiesced, then the store-wide CT can
  be used for the range in conjunction with the last min log index. If
  the range is unquiesced, the last-received per-range heartbeat's min
  log index must be used in conjunction with the CT returned with that
  same heartbeat.

Before a command is proposed, the range is unquiesced if necessary. In
the event an unquiesce is necessary, the range ID is added to a map
which parallels the per-range heartbeat maps, which are populated
pending the next coalesced heartbeat. This guarantees that the next
heartbeat received by followers will specify the IDs of any unquiesced
ranges, preventing the use of an advanced store-wide closed timestamp,
where the range is in fact active and may have proposed new commands.

### Details

Nodes maintain a map from node/store ID to a CT, which contains the
store-wide closed timestamp, liveness epoch, and a set of quiesced
range IDs. This is updated on receipt of coalesced heartbeats. If the
liveness epoch changes on successive heartbeats, the CT struct is
reset. Each heartbeat for a range updates the replica's `r.mu.closed`
struct, which keeps track of the closed timestamp and min log index
pair. It also contains a `confTS` confirmed timestamp, which is set
to the closed timestamp once the committed log index is at least
equal to the min log index.

When a read is serviced by a follower, that replica first checks if
the read timestamp is at or before the confirmed timestamp. If so, it
can service the read and proceeds with no further checks. If not, it
checks whether the read is at or before the last-received closed
timestamp. If so, and the min log index has been committed, the
read is serviced. If not, then **if the range is quiesced** and the
range lease is valid and matches the leaseholder's last reported
liveness epoch, the *store-wide* CT can be used as long as the min log
index has been committed.

This mechanism requires that once a range is quiesced (this is learned
by the follower on Raft heartbeats), the next heartbeat after
unquiescing must inform the follower. A sequence number is sent with
heartbeats to prevent a missed heartbeat from allowing a follower to
use the store-wide CT when it is in fact not quiesced. If the leader /
leaseholder restarts or loses its lease, its liveness epoch will be
incremented, preventing the use of stale min log indexes with newer
instances of the same leader / leaseholder store.

Note that an important property of the implementation for closed
timestamps is that **all** information about them is transmitted via
Raft heartbeats. If a heartbeat is missed or mis-ordered, then the use
of store-wide advancing closed timestamps is halted. If heartbeats are
delayed by arbitrary amounts of time, the followers will still be able
to use the last store-wide closed timestamp for quiesced ranges and
any per-range closed timestamps which were previously transmitted, but
that information will simply become increasingly stale, not incorrect.

On splits, a node's closed timestamp information is kept current for
the LHS, and copied to the RHS of the split. The confirmed closed
timestamp and closed timestamp are simply copied, while the min log
index is set to 0. Splits guarantee exclusion on commands to the
range, and the RHS will have an empty Raft log when the split is
finalized.

### State transitions

Raft leadership can change while leaseholdership remains stable, and
vice versa. The table below lists state transitions and explains how
the stated guarantees are maintained.

| Scenario | Range State | Explanation |
| -------- | ----------- | ----------- |
| Lose leadership | Range unquiesced | On loss of leadership, heartbeats stop coming from the old leader and start coming from the new leader. However, without the lease, the new leader sends no new min log indexes with per-range heartbeats, so the CT is no longer advanced for unquiesced ranges. |
| Lose leadership | Range quiesced | Heartbeats stop coming from the old leader, but its advancing store-wide CT can continue being used as long as its lease remains valid. |
| Lose leaseholdership | Range unquiesced | On loss of leaseholdership, heartbeats continue, but without new min log indexes, so the CT is no longer advanced. |
| Lose leaseholdership | Range quiesced | If the lease was transferred away, that would have required the range to be unquiesced. Instead, loss of leaseholdership requires that the liveness epoch of the prior leaseholder was incremented and the lease captured. The incremented epoch will clear the CT map entry, preventing further use of the store-wide CT. |
| Lose / regain leaseholdership | Range quiesced | If the L/LH loses the lease, the liveness epoch will have been incremented, preventing the use of new store-wide CT with old min log index. Note that follower replicas check the lease is valid before using a putative leaseholder's advancing store-wide closed timestamp. |
| Missed unquiesce | Range quiesced | Range replica is partitioned and misses unquiesce, L/LH proposes new commands and re-quiesces. Replica becomes unpartitioned and receives its first heartbeat from L/LH from the period of the second quiescence. This later heartbeat will have a sequence number with a gap, which will clear the quiesced set and prevent the advanced store-wide CT from being used. |
| Lost heartbeat | Range unquiesced | The min log index will not be advanced. When the next heartbeat arrives, it will specify the same or greater min log index and a new closed timestamp. |
| Lost heartbeat | Range quiesced | Whether or not a range has unquiesced, the follower will assume it has if it notices from the heartbeat sequence number that a heartbeat was missed. This causes the quiesced map to be discarded on the follower, which prevents the follower from using the advancing store-wide closed timestamp of the leaseholder until the follower receives a new per-range heartbeat that re-quiesces it. A follower which doesn't have enough information to service a follower read will unquiesce and wake the leader to make sure this happens. |
| L/LH restart | Range unquiesced | The lease will have to be renewed with a new liveness epoch, successive heartbeats will convey updated min log index. |
| L/LH restart | Range quiesced | On lease renewal, the range will unquiesce, and successive heartbeats will convey updated min log index. |
