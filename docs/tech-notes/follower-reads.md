# Follower reads are consistent reads at historical timestamps from follower replicas

Original author: Spencer Kimball

## Introduction

Follower reads make the non-leader replicas in a range suitable
sources for historical reads. The key enabling technology is the
propagation of a **max safe timestamp** from the Raft leader to
followers. The max safe timestamp (MST) is more than just a
timestamp. It is a set of conditions, that if met, guarantee that
a follower replica has all state necessary to satisfy reads at or
before the MST.

Follower reads can increase read throughput by allowing every replica
to serve read requests. For geo-distributed clusters, this can
translate to significant latency improvements. Follower reads are not
intended to serve consistent reads at the current timestamp, and so
are suitable only for workloads which can tolerate a slightly stale
version of the data (on the order of seconds). Note, however, that
the historical timestamp chosen to do follower reads will reveal a
consistent view of the database at that timestamp.

This tech note is motivated by follower reads, but the MST mechanism
has far reaching consequences for database functionality.

- Because it records successively higher timestamps at which a
  replica's contents are completely valid, it can be used to implement
  disaster recovery for a cluster by rebuilding total cluster state at
  the minimum of surviving replicas' MSTs.
- It serves as a checkpoint for change data capture consumers,
  informing them when it is safe to consider all data to have been
  received at or before the MST from a source replica.

## Follower reads

A more extended description of MST follows, but let's start with how
MST can be used to implement follower reads, which is straightforward.
At the distributed sender, if a read is for a historical timestamp
earlier than the current time less a target max transaction age, it is
sent to the nearest replica (as measured by latency and locality),
instead of to the leaseholder.

When a read is handled by a replica, and it is not the leaseholder, it
is checked against the MST conditions and serviced locally if they are
met. This avoids forwarding to the leaseholder and avoid updating the
timestamp cache.

Easy, right? The hard part is the max safe timestamp. Read on.

## Max Safe Timestamp: overview

A max safe timestamp (MST) is a set of conditions, which if true,
guarantee that a replica has all data necessary to serve reads at a
timestamp. Information about MSTs is sent with coalesced heartbeats.
- **Min safe log index**: the minimum Raft log index must have been
  committed on the follower. This value is transmitted, per-range,
  with Raft heartbeats.
- **Max safe timestamp**: the timestamp, before which reads may be
  served by the replica if it has committed at least up to the min
  safe log index. This value will be in the past by at least a
  constant target duration. This value is transmitted with coalesced
  Raft heartbeats, for an entire store, on behalf of all ranges for
  which it is the leader / leaseholder.
- **Liveness epoch**: the liveness epoch is transmitted for the entire
  store in the top-level coalesced heartbeat. When the liveness epoch
  changes, all prior information held on behalf of the sender is
  discarded as no longer trustable.
- **Unquiesced ranges**: a set of range IDs which were previously
  quiesced but have been unquiesced on the sender, since the last
  heartbeat. Unquiesced ranges can't rely on the store-wide max safe
  timestamp sent with coalesced heartbeats, and may instead only use
  the max safe timsestamp in conjunction with explicit min safe log
  indexes sent per-range.
- **Max safe sequence**: a sequence number incremented and sent with
  successive coalesced heartbeats. This allows the recipient to
  realize when a heartbeat was missed, and to discard information
  it holds about quiesced ranges.

When it propagates a MST, a Raft leader / leaseholder is making the
following guarantee: *Every Raft command proposed after the min safe
log index will be at a later timestamp than the max safe timestamp.*

MSTs are propagated via Raft heartbeats, which are sent by the Raft
leader to followers when a range is active (not quiesced). MSTs,
specifically, are sent *only* when the Raft leader is also the range
leaseholder. In the interests of building the explanation in pieces,
let's first not consider how MSTs work when ranges are
quiesced. Instead, let's simplify by allowing MSTs to be valid only
when explicitly received on a heartbeat for a range. Let's look next
at the stated guarantee.

## Maintaining the stated guarantee

*Every Raft command proposed after the min safe log index will be at a
later timestamp than the max safe timestamp.*

Each `Store` object maintains a **min proposal timestamp** (MPT),
which is linked to Raft proposals in order to provide successively
higher max safe timestamps to send with heartbeats. The min proposal
timestamp, as the name suggests, maintains a low water timestamp for
command proposals. Similar to the timestamp cache, when commands are
evaluated on the write path, their timestamp is forwarded to be at
least the MPT.

The MPT is a slightly tricky object. It consists of a reader/writer
mutex and two timestamps (`cur` and `last`), each with a ref count.
One purpose of this structure is to pin a value for the MPT when a
command first begins execution in `Replica.tryExecuteWriteBatch`,
until after it is proposed. The goal is to avoid holding a reader
lock during command evaluation and Raft proposal.

When the MPT is accessed in `Replica.tryExecuteWriteBatch`, the `cur`
timestamp is returned and its ref count is incremented. After command
proposal, a cleanup function is invoked which decrements either the
`cur` or `last` ref count, depending on which timestamp was originally
returned. The MPT is also accessed when sending MSTs with Raft
heartbeats. This happens just once every time heartbeats are sent from
a node to peer nodes in the `Store.coalescedHeartbeatsLoop`.

As long as the `last` ref count is non-zero, the `last` timestamp is
returned for use with MSTs. This ensures that while any commands may
still be being proposed to Raft using the `last` timestamp as the low
water mark, no MSTs will be sent with a higher safe timestamp. If the
`last` ref count is zero, then the `last` timestamp is returned for
the current round of heartbeats, while the `cur` timestamp and ref
count are transferred to `last`. A new `cur` timestamp is set to
`hlc.Clock.Now() - MaxSafeTimestampInterval` with ref count 0.

The MPT specifies timestamps `cur=C` and `last=L`, where `C > L`.  On
each successive round of heartbeats, a node sends MSTs with timestamps
set to `L(1) <= L(2) <= L(3) <= ... <= L(N)`, and enforces that all
commands proposed between heartbeats have command timestamps forwarded
to at least `C(1) <= C(2) <= C(3) <= ... <= C(N)`. Because all
commands proposed between heartbeats `K` and `K+1` will have at least
timestamp `C(K)`, and `C(K) > L(K)`, then a heartbeat for any range
will report a min safe log index at which that command and all
subsequent commands must have a timestamp greater than `L(k)`, which
proves the stated guarantee.

### What about leadership / leaseholdership changes and node restarts?

When leadership / leaseholdership migrates between nodes, the
timestamp cache takes precedence over the MPT and prevents any command
proposals earlier than the new leader node's `hlc.Clock.Now()` plus
the max clock offset. This allows the MPT used by two successive
nodes, and by extension the MST, to actually regress, while still
maintaining the critical guarantee that *every Raft command proposed
after the min safe log index will be at a later timestamp than the max
safe timestamp*. Note that the guarantee itself says nothing about how
the MPT or MST vary as leadership / leaseholdership changes.

## Quiescence

Things get more interesting when a range quiesces. Replicas of
quiesced ranges no longer receive heartbeats. However, if a replica is
quiesced, we can continue to rely on the most recent *store-wide* MST
timestamp supplied in coalesced heartbeats, so long as the liveness
epoch (which continues to be reported with heartbeats) remains
stable. In order to do this, a node must guarantee the following
contract: once a min safe log index is sent for a range, the sender
(while the liveness epoch remains the same) must on subsequent
heartbeats either:
- Return a new min safe log index for a range if not quiesced.
- Return the range ID if the range has become unquiesced.
- Return nothing. If the range is quiesced, then the store-wide MST
  can be used for the range in conjunction with the last min safe log
  index. If the range in unquiesced, the last-received per-range MST
  must be used in conjunction with the last min safe log index.

### Details

Nodes maintain a map from node/store ID to a MST, which contains the
store-wide max safe timestamp, liveness epoch, and a set of quiesced
range IDs. This is updated on receipt of coalesced heartbeats. If the
liveness epoch changes on successive heartbeats, the MST struct is
reset. Each heartbeat for a range updates the replica's `r.mu.mst`
struct, which keeps track of the max safe timestamp, min safe log index
pair.

When a read is serviced by a follower, that replica first checks if
the last-received MST for the range covers the read timestamp. If it
does, and the min safe log index has been committed, the read is
serviced. If not, then **if the range is quiesced** and the range
lease is valid and matches the leaseholder's last reported liveness
epoch, the *store-wide* MST can be used as long as the min safe log
index has been committed.

This mechanism requires that once a range is quiesced (this is learned
by the follower on Raft heartbeats), the next heartbeat after
unquiescing must inform the follower. A sequence number is sent with
heartbeats to prevent a missed heartbeat from allowing a follower to
use the store-wide MST when it is in fact not quiesced. If the leader
/ leaseholder restarts or loses its lease, its liveness epoch will be
incremented, preventing the use of stale min safe log indexes with
newer instances of the same leader / leaseholder store.

### State transitions

Raft leadership can change while leaseholdership remains stable, and
vice versa. The table below lists state transitions and explains how
the stated guarantees are maintained.

| Scenario | Range State | Explanation |
| -------- | ----------- | ----------- |
| Lose leadership | Range unquiesced | On loss of leadership, heartbeats stop coming from the old leader and start coming from the new leader. However, without the lease, the new leader sends no new min log index, so the MST is no longer advanced. |
| Lose leadership | Range quiesced | Heartbeats stop coming from the old leader, but its advancing store-wide MST can continue being used as long as its lease remains valid. |
| Lose leaseholdership | Range unquiesced | On loss of leaseholdership, heartbeats continue, but without new min safe log indexes, so the MST is no longer advanced. |
| Lose leaseholdership | Range quiesced | If the lease was transferred away, that would have required the range to be unquiesced. Instead, loss of leaseholdership requires that the liveness epoch of the prior leaseholder was incremented and the lease captured. The incremented epoch will will clear the MST map entry, preventing further use of the store-wide MST. |
| Lose / regain leaseholdership | Range quiesced | If the L/LH loses the lease, the liveness epoch will have been incremented, preventing the use of new store-wide MST with old min safe log index. |
| Missed unquiesce | Range quiesced | Range replica is partitioned and misses unquiesce, L/LH proposes new commands and re-quiesces. Replica becomes unpartitioned and receives its first heartbeat from L/LH from the period of the second quiescence. This later heartbeat will necessarily include the range ID as part of the unquiesced set, which will prevent the advanced store-wide MST from being used. |
| Lost heartbeat | Range unquiesced | The min safe log index will not be advanced. When the next heartbeat arrives, it will specify the same or greater min safe log index and a new max safe timestamp. |
| Lost heartbeat | Range quiesced | Whether or not a range has unquiesced, the follower will assume it has if it notices from the heartbeat sequence number that a heartbeat was missed. This causes the quiesced map to be discarded on the follower. |
| L/LH restart | Range unquiesced | The lease will have to be renewed with a new liveness epoch, successive heartbeats will convey updated min safe log index. |
| L/LH restart | Range quiesced | On lease renewal, the range will unquiesce, and successive heartbeats will convey updated min safe log index. |
