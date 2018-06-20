- Feature Name: Follower Reads
- Status: draft
- Start Date: 2018-06-03
- Authors: Spencer Kimball, Tobias Schottdorf
- RFC PR: #21056
- Cockroach Issue: #16593

# Summary

Follower reads are consistent reads at historical timestamps from follower
replicas. They make the non-leader replicas in a range suitable sources for
historical reads.

The key enabling technology is the propagation of **closed timestamp
heartbeats** from the range leaseholder to followers. The closed timestamp
heartbeat (CT heartbeat) is more than just a timestamp. It is a set of
conditions, that if met, guarantee that a follower replica has all state
necessary to satisfy reads at or before the CT.


# Motivation

Consistent historical reads are useful for analytics queries and in particular
allow such queries to be carried out more efficiently and, with appropriate
configuration, away from foreground traffic. But historical reads are also key
to a proposal for [reference-like
tables](https://github.com/cockroachdb/cockroach/issues/26301) aimed at cutting
down on foreign key check latencies particularly in geo-distributed clusters;
they help recover a reasonably recent consistent snapshot of a cluster after a
loss of quorum; and they are one of the ingredients for [Change Data
Capture](https://github.com/cockroachdb/cockroach/pull/25229).


# Guide-level explanation

The closed timestamp mechanism provides to each non-leaseholder replica a
regular stream of information originating at the lease holder about which
requests it is allowed to serve.

At a high level, the "stream of information" contains what one might intuitively
expect: If a leaseholder accepts a write at timestamp T into its Raft log at
position P, then a follower can't serve a historical read at timestamps `S >= T`
until it has applied log position `P`, or it will fail to see the write.
Similarly, it can't serve any read at timestamp `S` at all unless it is promised
that future writes at or below timestamp `S` are impossible.

In the proposal, we're thus going to deal with "log positions" and timestamps.
For technical reasons, the "log position" is not the Raft log position but the
Lease Applied Index, a concept introduced by us on top of Raft to handle
Raft-internal reproposals and reorderings. Ultimately, what we're after is a
promise of the form "no more proposals writing to timestamps below T are going
to apply after log index I". This guarantee is tricky to extract from the Raft
log index since proposing a command at log index I does not restrict it from
showing up at higher log indices later, especially in leader-not-leaseholder
situations. The Lease Applied Index was introduced precisely to have better
control, and allows us to make the above promise.

Now let's talk about timestamps. As we've seen above, a follower needs to know
that a given timestamp is "safe" (in parlance of this RFC, "closed") to serve
reads for; there must not be some in-flight write that would invalidate a
follower read retroactively. Hence, the lease holder must maintain a mechanism
that prevents proposals at non- recent timestamps, and regularly communicates a
safe timestamp to the followers.

Since a node can house on the order of 50000 replicas, it's untenable to
establish direct communication between all leaseholders and followers. Instead,
communication is *coalesced* between stores, very similar to Raft heartbeats.
However, this is not enough: we expect that a significant fraction of ranges is
idle at any given point in time, and considerable work has been put into making
these ranges "quiet" (or, as we call it in code, quiescent) at the Raft level. A
key element of this proposal (and one of its ingredients that is tricky to get
right) is making sure that quiescent ranges too can serve reads from followers,
without requiring individual communication.

Fundamentally this is achieved is by notifying the followers as a range goes
quiescent, after which they can serve reads at non-recent timestamps. The tricky
part is making sure that ranges cannot unquiesce without the followers noticing
on time. To understand exactly how that is achieved, we refer to the reference
section below.

# Reference-level explanation

A more extended description of CT (closed timestamps) follows, but let's start
with how CT can be used to implement follower reads, which is conceptually
straightforward. At the distributed sender, if a read is for a historical
timestamp earlier than the current time less a target duration (which adds
comfortable padding to when followers are ideally able to serve these reads), it
is sent to the nearest replica (as measured by health, latency, locality, and
perhaps a jitter), instead of to the leaseholder.

When a read is handled by a replica, and it is not the leaseholder, it
is checked against the CT conditions and serviced locally if they are
met. This avoids forwarding to the leaseholder and avoids updating the
timestamp cache. Note that closed timestamps and follower reads are
supported only on ranges with epoch-based leases, and that follower reads
do not check for clock uncertainty restarts.

If the replica cannot serve the follower read or intents needed to be resolved,
it returns a suitable error and the request (for the given range) is retried at
the leaseholder. This makes sure that a lagging follower does not add repeated
failed attempts to a request.

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
    TODO(tschottdorf): can we untangle this from the Raft notion of quiescence?

  **Note**: for quiesced ranges, there is no per-range information in
  the heartbeat (the point of quiescence is to avoid unnecessary
  communication). Instead, the origin store:
  - promises that no leaseholder replica will propose a command
    between any two heartbeats without also including the range ID in
    the ranges map, and
  - allows the follower to verify whether the origin store's CT
    heartbeat remains valid for a still-quiescent range using the
    heartbeat **sequence** and **liveness epoch**. See below for details.
- **Sequence**: a sequence number incremented and sent with successive
  coalesced heartbeats. This is used to guarantee that the leaseholder
  includes any active replica in the next outgoing CT heartbeat. This
  sequence number allows the recipient to detect a missed heartbeat,
  and on so doing, it must assume that all formerly quiesced ranges
  have become active again. (TODO: insert footnote about mitigations)
- **Liveness epoch**: the origin store's reported liveness epoch, used
  by the recipient to verify that information from previous CT
  heartbeats about range quiescence remains valid. Followers must
  check whether the lease for the replica is that of the originating
  store at the given epoch and confirm that it is live. If the
  liveness epoch advances, the recipient must assume that all quiescent
  ranges have unquiesced, perhaps under a new lease holder.
  heartbeats is discarded.

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

Quiesced replicas which do not serve any reads before the liveness
epoch is incremented cannot forward their CTs to the last known closed
timestamp from the leaseholder. This is because we can't assume ranges
that we previously believed were quiescent actually had valid leases
(and actually were quiescent) at the time of the previous heartbeat.
We can't use that value after the fact.

## Constructing the CT heartbeat

In the interests of building the explanation in pieces, let's first
not consider how CTs work when ranges are quiesced. Instead, let's
simplify by allowing CTs to be valid only when explicitly received on
a heartbeat for a range.

### Unquiesced ranges

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

The object in charge of this is the per-`Store` **min proposal timestamp** (MPT)
which is linked to command proposals in order to provide successively higher
closed timestamps to send with CT heartbeats. The min proposal timestamp, as the
name suggests, maintains a low water timestamp for command proposals
(initialized to the start of the lease). Similar to the timestamp cache, when
commands are evaluated on the write path, their timestamps are forwarded to be
at least the MPT.

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

Naively, this violates the guarantee: we promise that no more data will appear
below a certain timestamp. Note however that this data isn't visible at
timestamps below the commit timestamp (which was forwarded): to read the value,
the intent has to be resolved first, which implies that it will move at least to
`Timestamp` in the progress, restoring the guarantee required.

Similarly, this does not impede the usefulness of the CT mechanism for
recovery: the restored consistent state may contain intents that belong
to transactions that started after the CT; however, they will simply be
aborted.

Note that for the CDC use case, this closed timestamp mechanism is a necessary,
but not sufficient, solution. In particular, a CDC consumer must find (or track)
and resolve all intents at timestamps below a given closed timestamp first.

#### What about leaseholdership changes and node restarts?

When a node restarts, it waits out the clock offset and is forced to request a
new lease. Thus, to guarantee correctness of the MPT mechanism, it is sufficient
to consider the case in which a lease changes hands (whether voluntarily or
not). Such transfers necessarily imply that the expiration (or transfer
timestamp) of the old lease precedes the start of the new one, and so the MPT
of the new leaseholder is strictly larger than that of the old one.

MPT used by two successive nodes, and by extension the CT,
monotonically increase. It's easy to see how this alone maintains the
critical guarantee that *every Raft command proposed after the MLAI
will be at a later timestamp than the closed timestamp*.

### Quiescence

Quiescence is a concept we bolted on top of Raft. Briefly put, whenever the
lease holder (and simultaneously Raft leader) finds that there are no in-flight
proposals and that everything is fully replicated, it instructs its followers
(and itself) to quiesce, that is, go dormant without expecting to be heartbeat.

This is an important performance optimization that the CT mechanism would
partially undo if it didn't correspondingly optimize for this case.

Ideally, we want to make it such that the most recent *store-wide* CT timestamp
supplied in coalesced heartbeats is valid for all quiescent ranges, so long as
the liveness epoch (which continues to be reported with heartbeats) remains
stable.

In order to do this, a node must guarantee the following contract: once a MLAI
is sent for a range, the sender (while the liveness epoch remains the same) must
on subsequent heartbeats either:
- Return a new MLAI for a range if unquiesced and was active since the
  last heartbeat.
- Return a new MLAI for a range if it has quiesced since last heartbeat.
- Return nothing, if the range was and remains quiesced, or is
  unquiesced but inactive. If quiesced, the store-wide CT can be used
  for the range in conjunction with the last known MLAI. If unquiesced,
  only the last reported CT & MLAI can be used.

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

On splits, a node's closed timestamp information is kept current for
the LHS, and copied to the RHS of the split. The confirmed closed
timestamp and closed timestamp are simply copied, while the MLAI is
set to 0. Splits guarantee exclusion on commands to the range, and the
RHS will have an empty Raft log when the split is finalized.

### State transitions

| Scenario | Range State | Explanation |
| -------- | ----------- | ----------- |
| Transfer lease | Range unquiesced | On transfer of lease, the range must be unquiesced, and the leader must be the leaseholder. Heartbeats will cease from the original leaseholder and start from the new leaseholder. |
| Transfer lease | Range quiesced | Quiesced ranges must unquiesce in order to propose a lease transfer. See above. |
| Lose leaseholdership | Range unquiesced | On loss of leaseholdership, heartbeats cease, so the CT is no longer advanced. |
| Lose leaseholdership | Range quiesced | If the lease was transferred away, that would have required the range to be unquiesced. Instead, loss of leaseholdership requires that the liveness epoch of the prior leaseholder was incremented and the lease captured. The incremented epoch will clear the CT map entry, preventing further use of the store-wide CT. |
| Lose / regain leaseholdership | Range quiesced | If the leaseholder loses the lease, the liveness epoch will have been incremented, preventing the use of new store-wide CT with old MLAI. Note that follower replicas check the lease is valid before using a putative leaseholder's advancing store-wide closed timestamp. |
| Missed unquiesce | Range quiesced | Range replica is partitioned and misses unquiesce, leaseholder proposes new commands and re-quiesces. Replica becomes unpartitioned and receives its first heartbeat from leaseholder from the period of the second quiescence. This later heartbeat will have a sequence number with a gap, which will clear the quiesced set and prevent the advanced store-wide CT from being used. |
| Lost heartbeat | Range unquiesced | The MLAI will not be advanced. When the next heartbeat arrives, it will specify the same or greater MLAI and a new closed timestamp. |
| Lost heartbeat | Range quiesced | Whether or not a range has unquiesced, the follower will assume it has if it notices from the heartbeat sequence number that a heartbeat was missed. This causes the quiesced map to be discarded on the follower, which prevents the follower from using the advancing store-wide closed timestamp of the leaseholder until the follower receives a new per-range heartbeat that re-quiesces it. A follower which doesn't have enough information to service a follower read will unquiesce and wake the leader to make sure this happens. |
| Leaseholder restart | Range unquiesced | The lease will have to be renewed with a new liveness epoch, successive heartbeats will convey updated MLAI. |
| Leaseholder restart | Range quiesced | On lease renewal, the range will unquiesce, and successive heartbeats will convey updated MLAI. |


## Rationale and Alternatives

What's special about this design is that it preserves quiescent ranges. This is
deemed necessary and was not the case for the alternative discussion, in which
Raft proposals were used to advance the safe timestamp. At the time of writing,
this design appears to be the sane solution with the given boundary conditions.

## Unresolved questions

### Disentangling CT from coalesced heartbeats

Ben points out that not piggybacking on coalesced heartbeats may add clarity to
the design.

See [here on Reviewable](https://reviewable.io/reviews/cockroachdb/cockroach/21056#-L896rgu0RpL-o19TmT9)

### Clarifying quiescence

The interaction between quiescence and automatic extension of the follower read
timestamp is difficult to encapsulate in code, and may result in tricky
correctness bugs.

If this were to be disentangled, even if only in the explanation, much would be
gained. The follower reads version of "quiesced" and its Raft predecessor are
not the same.

### Protecting against missed heartbeats

After a missed heartbeat, the recipient node must currently assume that all
quiesced ranges have unquiesced. This often means that a significant number of
ranges will refuse follower reads for the duration of a heartbeat, and that all
of the quiescent ranges will wake up when they receive a request. We don't
expect heartbeats to be missed in regular operations, but we do need to limit
the impact this would have somewhat. The most straightforward way to achieve
this is to be able to "recover" the missed heartbeat. For instance, nodes could
re-send failed heartbeats a given number of times; or they could merge a missed
heartbeat into the next one to the same node (so that receiving the next
heartbeat establishes information parity despite having missed the previous
one). This is not implemented in the initial (experimental) version of follower
reads.

### Configurability

For now, the min proposal timestamp roughly trails real time by five seconds.
This can be made configurable, for example via a cluster setting or, if more
granularity is required, via zone configs.

Transactions which exceed the lag are usually forced to restart, though this
will often happen through a refresh (which is comparatively cheap, though it
needs to be tested).
