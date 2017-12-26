# Follower reads are consistent reads at historical timestamps from follower replicas

Original author: Spencer Kimball

## Introduction

Follower reads make the non-leader replicas in a range suitable
sources for historical reads. The key enabling technology is the
propagation of a **closed timestamp heartbeats** from the range
leaseholder to followers. The closed timestamp heartbeat (CT
heartbeat) is more than just a timestamp. It is a set of conditions,
that if met, guarantee that a follower replica has all state necessary
to satisfy reads at or before the CT.

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
  disaster recovery for a cluster by rebuilding a consistent (though
  possibly regressed) cluster state at the minimum of the surviving
  replicas' CTs.
- It serves as a checkpoint for change data capture consumers,
  informing them when it is safe to consider all data to have been
  received at or before the CT from a source replica.

## Follower reads

A more extended description of CT follows, but let's start with how CT
can be used to implement follower reads, which is straightforward. At
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

## Closed Timestamp Heartbeats: overview

A closed timestamp heartbeat (CT heartbeat) is sent along with
coalesced Raft heartbeats to allow replicas to serve consistent
historical reads. The following information is added to coalesced
heartbeats, which are exchanged between stores located on different
nodes:

- **Closed timestamp (CT)**: the timestamp at or before which replicas
  can serve reads if the other conditions below hold. This value
  typically trails the clock of the originating store by at least a
  constant target duration. Each closed timestamp heartbeat contains
  one such timestamp on behalf of all ranges for which the originating
  store believes it holds the lease.
- **Ranges map**: map from range ID to information about non-quiesced
  ranges active during the period between this and the prior coalesced
  heartbeat. For each range, the following information is provided:
  - **Min lease applied index (MLAI)**: if the follower's
    `LeaseAppliedIndex` is greater than or equal to the MLAI, the
    follower may serve reads at timestamps less than or equal to the CT.
  - **Quiesced**: a boolean that, if true, announces that the range is
    now quiesced and should be treated as such by the recipient in
    future coalesced heartbeats until notified otherwise. (If false,
    the range is treated as non-quiesced).

  **Note**: for quiesced ranges, there is no per-range information in
  the heartbeat (the point of quiescence is to avoid unnecessary
  communication). Instead, the origin store:
  - promises that no leaseholder replica will propose a command between
    any two heartbeats without also including the range ID in the ranges
    map as unquiesced, and
  - allows the follower to verify whether the origin store's CT
    heartbeat remains valid for a still-quiescent range using the
    heartbeat **sequence** and **liveness epoch**. See below for details.
- **Sequence**: a sequence number incremented and sent with successive
  coalesced heartbeats. This is used to guarantee that the leaseholder
  includes any newly unquiesced replica in the next outgoing CT
  heartbeat. This sequence number allows the recipient to detect a
  missed heartbeat, and on so doing, it must assume that all replicas
  are unquiesced.
- **Liveness epoch**: the origin store's reported liveness epoch, used
  by the recipient to verify that information from previous CT
  heartbeats about range quiescence remains valid. Followers must
  check whether the lease for the replica is that of the originating
  store at the given epoch and confirm that it is live. If the liveness
  epoch advances, all information from prior CT heartbeats is discarded.

Note that Raft plays no fundamental role in this mechanism and thus,
it is irrelevant whether the Raft leader and the leaseholder are
colocated (though in the common case they are). Coalesced heartbeats
are employed for CT heartbeat transport because they are convenient,
and for easy access to the quiescent state (which in itself is not a
Raft concept but an auxiliary mechanism added by us), and similarly
the lease applied index is not a Raft term: in fact, it is our
stand-in for reasoning about Raft log positions, which are notoriously
difficult to work with. For example, if a command gets initially
proposed at a log index N, it could theoretically still apply at a
higher log index if the proposing leader steps down and another leader
takes over but puts the command in a higher slot. The leadership could
even be won back by the original node after. The lease applied index
prevents these scenarios.

## Constructing the CT heartbeat

In the interests of building the explanation in pieces, let's first not
consider how CTs work when ranges are quiesced. Instead, let's simplify by
allowing CTs to be valid only when explicitly received on a heartbeat for a
range.

### Non-quiesced ranges

In a CT heartbeat, the origin store makes the following guarantee
for a given range it holds the lease for:

*Every Raft command proposed after the min lease applied index (MLAI)
will be at a later timestamp than the closed timestamp (CT).*

The MLAI which is reported with CT heartbeats is constructed for each
range by reporting `Replica.mu.lastAssignedLeaseIndex`, which is
guaranteed to be greater than or equal to any lease index which was
assigned as the `MaxLeaseIndex` on Raft commands proposed by the
leaseholder before the prior heartbeat was sent. More on this below.

The leaseholder wants to
- maintain an answer to the question: What's the largest timestamp for
  which there's no command in flight and never again will be?
- have that answer increase over time, roughly staying within the
  target duration of the node's clock.

The object in charge of this is the per-`Store` **min proposal
timestamp** (MPT) which is linked to command proposals in order to
provide successively higher closed timestamps to send with CT
heartbeats. The min proposal timestamp, as the name suggests,
maintains a low water timestamp for command proposals. Similar to the
timestamp cache, when commands are evaluated on the write path, their
timestamps are forwarded to be at least the MPT.

The MPT is a slightly tricky object. It consists of two timestamps
`last` and `cur` with associated ref counts (and a reader/writer mutex
we'll ignore here and for which care is taken that it is not held over
long operations such as command evaluation or proposal).

`last` is the value of the min proposal timestamp sent as the
store-wide closed timestamp in the most recent coalesced heartbeat,
and `cur` is the timestamp below which new proposals are not accepted;
both are updated for each constructed coalesced heartbeat.

The ref count for `last` counts the commands which are in process for
timestamps forwarded to at least `last`, while that for `cur` covers
commands forwarded to at least `cur`. Note that the MPT guarantees
that the set of commands with timestamps within the interval `[0,
last)` is always empty.

Note that a command is "in process" while it is being evaluated (into
a proposal) and proposed. Once it is proposed" (as in "handed to
Raft"), it's not "in process" any more for the purposes of the MPT
(though, of course, it will first have to clear Raft until it actually
applies and becomes visible).

Let's walk through an example of how the MPT works. Initially, `last`
and `cur` demarcate some time interval. Three commands arrive; `cur`
picks up a refcount of three (new commands are forced above `cur`, though
in this case they were there to begin with):

```
             0        3
           last      cur    commands
             |        |        /\   \_______
             |        |       /  \          |
             v        v       v  v          v
------------------------------x--x----------x------------> time
```

Next, it's time to construct a coalesced heartbeat. Since `last` has a
refcount of zero, we know that nothing is in progress for timestamps
`[last, cur)` and we can advance `last` to `cur`, and move `cur` to
`now-target duration`. Note that `cur` now has a new refcount of zero,
while `last` picked up `cur`s previous refcount, three. This
demonstrates the "morally" in the intervals assigned to `cur` and
`last`: one of the commands is in `[cur, ∞)` and yet `last` is now in
charge of tracking it. This is common in practice since `cur`
typically trails the node's clock by seconds.

```
                      3                   0
                     last    commands    cur
                      |        /\   \_____|__
                      |       /  \        | |
                      v       v  v        v v
------------------------------x--x----------x------------> time
```

Two of the commands get proposed, decrementing `last`s
refcount. Additionally, two new commands arrive at timestamps below
`cur`. As before, `cur` picks up a refcount of two, but additionally
the commands' timestamps are forwarded to `cur`. These new commands
get proposed quickly (so they don't show up again) and `cur`s refcount
will drop back to zero.

```
                      1     in-flight      2
                    last     command      cur
                      |         \          |
                      |          \         |
                      v          v         v
---------------------------------x-----------------------> time
                                           ʌ
                                           |
            _______________________________/
           |   forwarding    |
           |                 |
       new command         new command
     (finishes quickly) (finishes quickly)
```

The remaining command sticks around. This is unfortunate; it's time
for another coalesced heartbeat, but we can't send a higher `last`
than before and must stick to the same one.

```
                  (blocked)             (blocked)
                      1     in-flight      0
                    last     command      cur
                      |         \          |
                      |          \         |
                      v          v         v
---------------------------------x-----------------------> time
```

Finally the command gets proposed. A new command comes in at some
reasonable timestamp and `cur` picks up a ref, but that doesn't bother
us.
```
                      0                    1
                    last                  cur     in-flight
                      |                    |      proposal
                      |                    |        |
                      v                    v        v
----------------------------------------------------x----> time
```

Time for the next coalesced heartbeat. We can finally move `last` to
`cur` (picking up its refcount) and `cur` to `now-target duration`
with a zero refcount, concluding the example.
```
                                           1               0
                                         last             cur ---···
                                           |
                                           |
                                           v
----------------------------------------------------x----> time
```

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
each successive heartbeat `1, 2, 3, ..., N`, a node sends CTs with
timestamp equal to `L(1) <= L(2) <= L(3) <= ... <= L(N)`, and enforces
that all commands proposed between heartbeats have command timestamps
forwarded to at least `C(1) <= C(2) <= C(3) <= ... <= C(N)`. Because
all commands proposed between heartbeats `K` and `K+1` will have at
least timestamp `C(K)`, and `C(K) > L(K)`, then a CT heartbeat for any
range will report a MLAI at which that command and all subsequent
commands must have a timestamp greater than `L(k)`, which proves the
stated guarantee.

#### Timestamp forwarding and intents

We forward commands' timestamps in order to guarantee that they don't
produce visible data at timestamps below the CT. A case in which that
is less obvious is that of an intent.

To see this, consider that a transaction has two relevant timestamps:
`OrigTimestamp` (also known as its read timestamp) and `Timestamp`
(also known as its commit timestamp). while the timestamp we forward
is `Timestamp`, the transaction internally will in fact attempt to
write at OrigTimestamp (but relies on moving these intents to their
actual timestamp later, when they are resolved). This prevents certain
anomalies, particularly with `SNAPSHOT` isolation.

Naively, this violates the guarantee: we promise that no more data will
appear below a certain timestamp, but then it does. Note however that
this data isn't visible at timestamps below the commit timestamp (which
was forwarded): to read the value, the intent has to be resolved first,
which implies that it will move at least to `Timestamp` in the progress,
restoring the guarantee required.

Similarly, this does not impede the usefulness of the CT mechanism for
recovery: the restored consistent state may contain intents that belong
to transactions that started after the CT; however, they will simply be
aborted.

#### What about leaseholdership changes and node restarts?

When leaseholdership migrates between nodes, the timestamp cache takes
precedence over the MPT and prevents any command proposals earlier
than the new leader node's `hlc.Clock.Now()` plus the max clock
offset. This allows the MPT used by two successive nodes, and by
extension the CT, to actually regress, while still maintaining the
critical guarantee that *every Raft command proposed after the MLAI
will be at a later timestamp than the closed timestamp*. Note that the
guarantee itself says nothing about how the MPT or CT vary as
leaseholdership changes.

### Quiescence

Things get more interesting when a range quiesces. Replicas of
quiesced ranges no longer receive heartbeats. However, if a replica is
quiesced, we can continue to rely on the most recent *store-wide* CT
timestamp supplied in coalesced heartbeats, so long as the liveness
epoch (which continues to be reported with heartbeats) remains
stable. In order to do this, a node must guarantee the following
contract: once a MLAI is sent for a range, the sender (while the
liveness epoch remains the same) must on subsequent heartbeats either:
- Return a new MLAI for a range if unquiesced / has unquiesced.
- Return a new MLAI for a range if it has quiesced since last heartbeat.
- Return nothing, if the range was and remains quiesced. In this case,
  the store-wide CT can be used for the range in conjunction with the
  last known MLAI. If the range is unquiesced, the last-received
  per-range heartbeat's MLAI must be used in conjunction with the CT
  returned with that same heartbeat.

Before a command is proposed, the range is unquiesced if necessary.
If unquiesced, the range ID is added to the ranges map with the
quiesced bool set to false and the MLAI for the range set to the last
assigned lease index. The ranges map is sent with the next coalesced
heartbeat. This guarantees that the next heartbeat received by
followers will specify the IDs of any unquiesced ranges, preventing
the use of an advanced store-wide closed timestamp, unless the range
has met the requirements of the advanced MLAI.

### Details

Nodes maintain a map from node ID/store ID to the store-wide closed
timestamp, liveness epoch, and a set of quiesced range IDs. This is
updated on receipt of coalesced heartbeats. If the liveness epoch
changes or the sequence number skips an increment on successive
heartbeats, the CT struct is reset. Each range in the CT heartbeat for
which an updated MLAI is specified, updates the associated replica's
`r.mu.closed` struct, which keeps track of the closed timestamp and
MLAI pair. It also contains a `confTS` confirmed timestamp, which is
set to the closed timestamp once the replica's `LeaseAppliedIndex` is
at least equal to the MLAI.

When a read is serviced by a follower, that replica first checks if
the read timestamp is at or before the confirmed timestamp. If so, it
can service the read and proceeds with no further checks. If not, it
checks whether the read is at or before the last-received closed
timestamp. If so, and the `LeaseAppliedIndex >= MLAI`, the read is
serviced. If not, then **if the range is quiesced** and the range
lease is valid and matches the leaseholder's last reported liveness
epoch, the *store-wide* CT can be used as long as `LeaseAppliedIndex >= MLAI`.

This mechanism requires that once a range is quiesced (learned via CT
heartbeats for this case, not via Raft heartbeats), the next heartbeat
after unquiescing must inform the follower. A sequence number is sent
with heartbeats to prevent a missed heartbeat from allowing a follower
to use the store-wide CT when it is in fact not quiesced. If the
leaseholder restarts or loses its lease, its liveness epoch will be
incremented, preventing the use of stale MLAIs with newer instances of
the same leaseholder store.

Note that an important property of the implementation for closed
timestamps is that **all** information about them is transmitted via
coalesced heartbeats. If a heartbeat is missed or mis-ordered, then
the use of store-wide advancing closed timestamps is halted. If
heartbeats are delayed by arbitrary amounts of time, the followers
will still be able to use the last store-wide closed timestamp for
quiesced ranges and any per-range closed timestamps which were
previously transmitted. That information will simply become
increasingly stale, not incorrect.

TODO(spencer): still need to investigate exactly what happens with the
`LeaseAppliedIndex` to the RHS of a split.

On splits, a node's closed timestamp information is kept current for
the LHS, and copied to the RHS of the split. The confirmed closed
timestamp and closed timestamp are simply copied, while the MLAI is
set to 0. Splits guarantee exclusion on commands to the range, and the
RHS will have an empty Raft log when the split is finalized.

### State transitions

| Scenario | Range State | Explanation |
| -------- | ----------- | ----------- |
| Lose leadership | Range unquiesced | On loss of leadership, heartbeats stop coming from the old leader and start coming from the new leader. However, CT heartbeats are always sent from the leaseholder, so are unaffected. |
| Lose leadership | Range quiesced | Heartbeats keep coming from the same store because it remains leaseholder; its advancing store-wide CT can continue being used as long as its lease remains valid. |
| Lose leaseholdership | Range unquiesced | On loss of leaseholdership, heartbeats cease, so the CT is no longer advanced. |
| Lose leaseholdership | Range quiesced | If the lease was transferred away, that would have required the range to be unquiesced. Instead, loss of leaseholdership requires that the liveness epoch of the prior leaseholder was incremented and the lease captured. The incremented epoch will clear the CT map entry, preventing further use of the store-wide CT. |
| Lose / regain leaseholdership | Range quiesced | If the leaseholder loses the lease, the liveness epoch will have been incremented, preventing the use of new store-wide CT with old MLAI. Note that follower replicas check the lease is valid before using a putative leaseholder's advancing store-wide closed timestamp. |
| Missed unquiesce | Range quiesced | Range replica is partitioned and misses unquiesce, leaseholder proposes new commands and re-quiesces. Replica becomes unpartitioned and receives its first heartbeat from leaseholder from the period of the second quiescence. This later heartbeat will have a sequence number with a gap, which will clear the quiesced set and prevent the advanced store-wide CT from being used. |
| Lost heartbeat | Range unquiesced | The MLAI will not be advanced. When the next heartbeat arrives, it will specify the same or greater MLAI and a new closed timestamp. |
| Lost heartbeat | Range quiesced | Whether or not a range has unquiesced, the follower will assume it has if it notices from the heartbeat sequence number that a heartbeat was missed. This causes the quiesced map to be discarded on the follower, which prevents the follower from using the advancing store-wide closed timestamp of the leaseholder until the follower receives a new per-range heartbeat that re-quiesces it. A follower which doesn't have enough information to service a follower read will unquiesce and wake the leader to make sure this happens. |
| Leaseholder restart | Range unquiesced | The lease will have to be renewed with a new liveness epoch, successive heartbeats will convey updated MLAI. |
| Leaseholder restart | Range quiesced | On lease renewal, the range will unquiesce, and successive heartbeats will convey updated MLAI. |
