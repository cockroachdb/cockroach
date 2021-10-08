- Feature Name: Follower Reads
- Status: completed
- Start Date: 2018-06-03
- Authors: Spencer Kimball, Tobias Schottdorf
- RFC PR: #21056
- Cockroach Issue: #16593

# Summary

Follower reads are consistent reads at historical timestamps from follower
replicas. They make the non-leader replicas in a range suitable sources for
historical reads. Historical reads include both `AS OF SYSTEM TIME` queries
as well as transactions with a read timestamp sufficiently in the past (for
example long-running analytics queries).

The key enabling technology is the exchange of **closed timestamp updates**
between stores. A closed timestamp update (*CT update*) is a store-wide
timestamp together with (sparse) per-range information on the Raft progress.
Follower replicas use local state built up from successive received CT updates
to ascertain that they have all the state necessary to serve consistent reads
at and below the leaseholder store's closed timestamp.

Follower reads are only possible for epoch-based leases, which includes all user
ranges but excludes some system ranges (such as the addressing metadata ranges).
In what follows all mentioned leases are epoch-based.

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

Fundamentally, the idea is that we already keep multiple consistent copies of
all data via replication, and that we want to utilize all of the copies to
serve reads. Morally speaking, a read which only cares to access data that was
written at some timestamp well in the past *should* be servable from all
replicas (assuming normal operation), as replication typically catches up all
the followers quickly, and most writes happen at "newer" timestamps. Clearly
neither of these two properties are guaranteed though, so replicas have to be
provided with a a way of deciding whether a given read request can be served
consistently.

The closed timestamp mechanism provides between each pair of stores a regular
(on the order of seconds) exchange of information to that end. At a high level,
these updates contain what one might intuitively expect:

A follower trying to serve a read needs to know that a given timestamp is "safe"
(in parlance of this RFC, "closed") to serve reads for; there must not be some
in-flight or future write that would invalidate a follower read retroactively.
Each store maintains a data structure, the **min proposal tracker** (*MPT*)
described later, to establish this timestamp.

Similarly, if a range's leaseholder commits a write into its Raft log at index
`P` before announcing a *closed timestamp*, then the follower must wait until it
has caught up to that index `P` before serving reads at the closed timestamp. To
provide this information, each store also includes with each closed timestamp an
updated minimum log index that the follower must reach before "activating" the
associated closed timestamp on that replica.

Providing the information only when there has been write activity on a given
range since the last closed timestamp is key to performance, as a store can
house upwards of 50000 replicas, and including information about every single
one of them in each update is prohibitive due to the overhead of visiting them.

This is similar to *range quiescence*, which avoids Raft heartbeats between
inactive ranges. It's worth pointing out that quiescent ranges are able to serve
follower reads, and that there is no architectural connection between follower
reads in quiescence, though a range that is quiescent is typically one that
requires no per-range CT update.

As we've seen above, this RFC deals in "log positions" (and closed timestamps).
For technical reasons, the "log position" is not the Raft log position but the
**Lease Applied Index**, a concept introduced by us on top of Raft to handle
Raft-internal reproposals and reorderings. Ultimately, what we're after is a
promise of the form

> no more proposals writing to timestamps less than or equal to  `T` are going
to apply after log index `I`.

This guarantee is tricky to extract from the Raft log index since proposing a
command at log index `I` does not restrict it from showing up at higher log
indices later, especially in leader-not-leaseholder situations. The *Lease
Applied Index* was introduced precisely to have better control, and allows us to
make the above promise.

# Reference-level explanation

This section will focus on the technical details of the closed timestamp
mechanism, with an emphasis on correctness.

A closed timestamp update contains the following information (sent by an origin `Store`):

- the **liveness epoch** (of the origin `Store`)
- a **closed timestamp** (`hlc.Timestamp`, typically trails "real time" by at least a constant target duration)
- a **sequence number** (to allow discarding built-up state on missed updates)
- a map from `RangeID` to **minimum lease applied index** (*MLAI*) that specifies
  the updates to the recipient's map accumulated from all previous updates.

The accumulated per-range state together with the closed timestamp serve as a
guarantee of the form

> Every Raft command proposed after the min lease applied index (MLAI)
will be ahead of the closed timestamp (CT).

Each store starts out with an empty state for each peer store and epoch, and
merges the *MLAI* updates into the state (overwriting existing *MLAI*s).
Whenever the sequence number received in an update from a peer store displays a
gap, the state for that peer store is reset, and the current update merged into
the empty state: this means that all information regarding ranges not explicitly
mentioned in the current update is lost. Similarly, if the epoch changes, the
state for any prior epoch is discarded and the update applied to an empty state
for the new epoch.

At a high level, the design splits into three parts:

1. How are the outgoing updates assembled? This will mainly live in the Replica write
path: whenever something is proposed to Raft, action needs to be taken to
reflect this proposal in the next CT update.
2. How are the received updates used and which reads can be served? This lives
mostly in the read path.
3. How are reads routed to eligible follower replicas? This lives both in
`DistSender` and the DistSQL physical planner.

We will talk about how they are used first, as that is the most natural
starting point for correctness.

To serve a read request at timestamp `T` via follower reads, a replica

1. looks up the lease, noting the store (and thus node) and epoch it belongs to.
1. looks up the CT state known for this node and epoch.
1. checks whether the read timestamp `T` is less than or equal to the closed timestamp.
1. checks whether its *Lease Applied Index* matches or exceeds the *MLAI* for the range (in the absence of an *MLAI*, this check fails by default).

If the checks succeed, the follower serves the read (without an update to the
timestamp cache necessary). If they don't, a `NotLeaseholderError` is returned.

Note that if the read fails because no *MLAI* is known for that range, there
needs to be some proactive action to prompt re-sending of the *MLAI*. This is
because without write activity on the range (which is not necessarily going to
happen any time soon) the origin store will not send an update. Strategies to
mitigate this are discussed in a dedicated section below.

## Implied guarantees

Implicitly, a received update represents the following essential promises:

- the origin node was, at any point in time, live for the given epoch and closed
  timestamp. Concretely, this means that the origin node had a liveness update (for
  the epoch) with the closed timestamp falling *before* the stasis period.

  This guarantees that no other node could forcibly take over the lease at a
  timestamp less than or equal to the closed timestamp, and consequently for any
  lease (as seen on a follower) for that origin store and epoch the origin store
  knows about all relevant Raft proposals that need to be applied before serving
  follower reads.

  In other words, **the ranges map in the update is authoritative** as long as:
- the *MLAI map* contains an update for any range for which a command has been
  proposed since the last update.

  This guarantee is hopefully not a surprise, but implicit in this is the
  requirement that any relevant write actually increments the lease applied
  index. Luckily, all commands do, except for lease requests (not transfers --
  see below for those), which don't mutate user-visible state.
- the origin store won't (ever) initiate a lease transfer that would allow
  another node to write at or below the closed timestamp. In other words, in the
  case of a lease transfer the next lease would start at a timestamp greater than
  the closed timestamp. This is likely impossible in practice since the transfer
  timestamps and proposed closed timestamps are taken from the same hybrid logical
  clock, but an explicit safeguard will be added just in case.

  If this rule were broken, another lease holder could propose commands that
  violate the closed timestamp sent by the original node (and a lagging follower
  would continue seeing the old lease and convince itself that it was fine to
  serve reads).

  Lease transfers also require an update in the *MLAI map*; they need to
  essentially force the follower to see the new lease before they serve further
  follower reads (at which point they will turn to the new leaseholder's store
  for guidance). Nothing special is required to get this behavior; a lease
  transfer requires a valid *Lease Applied Index*, so the same mechanism that
  forces followers to catch up on the Raft log for writes also makes them
  observe the new lease. This requires that we wait until reaching the MLAI
  for a closed timestamp until we decide which node's state to query.

  Note that a node restart implies a change in the liveness epoch, which in
  turn invalidates all of the information sent before the restart.

## Recovering from missed updates

To regain a fully populated *MLAI* map when first receiving updates (or after
resetting the state for a peer node), there are two strategies:

1. special case sequence number zero so that it includes an *MLAI* for all
   ranges for which the lease is held. When an update is missed, the recipient
   notifies the sender and it resets its sequence number to zero (thus sending
   a full update next).
2. ask for updates on individual ranges whenever a follower read request fails
   because of a missing *MLAI*.

We opt to implement both strategies, with the first doing the bulk of the work.
The first strategy is worthwhile because

1. the payload is essentially two varints for each range, amounting to no more than
   20 bytes on the wire, adding up to a 1mb payload at 50000 leaseholder replicas
   (but likely much less in practice).
   Even with 10x as many, a rare enough 10mb payload seems unproblematic,
   especially since it can be streamed.
2. without an eager catch-up, followers will have to warm up "on demand" but the
   routing layer has no insight into this process and will blindly route reads
   to followers, which makes for a poor experience after a node restart.

But this strategy can miss necessary updates as leases get transferred to
otherwise inactive ranges. To guard against these rare cases, the second
strategy serves as a fallback: recipients of updates can specify ranges they
would like to receive an MLAI for in the next update. They do this when they
observe a range state that suggests that an update has been missed, in
particular when a replica has no known MLAI stored for the (non-recent) lease.

## Constructing outgoing updates

To get in the right mindset of this, consider the simplified situation of a
`Store` without any pending or (near) future write activity, that is, there are
(and will be) no in-flight Raft proposals. Now, we want to send an initial CT
update to another store. This means two things:

1. the need to "close" a timestamp, i.e. preventing any future write activity visible
   at this timestamp, for any write proposed by this store as a leaseholder (for the
   current epoch).
2. Tracking an *MLAI* for each replica (for which the lease for the epoch is held).

The first requirement is roughly equivalent to bumping the low water mark of
the timestamp cache to one logical tick above the desired closed timestamp
(though doing that in practice would perform poorly).

The second one is also straightforward: simply read the *Lease Applied Index* for
each replica; since nothing is in-flight, that's all the followers need to know
about.

In reality, there will sometimes be ongoing writes on a replica for which we want
to obtain an *MLAI*, and so 1) and 2) get more complicated.

Instead of adjusting the timestamp cache, we introduce a dedicated data
structure, the **minimum proposal tracker** (*MPT*), which tracks (at coarse
granularity) the timestamps for which proposals are still ongoing. In
particular, it can decide when it is safe to close out a higher timestamp than
before. This replaces 1), but retrieving an *MLAI* is also less straightforward
than before.

Assume the replica shows a *Lease Applied Index* of 12, but three proposals are
in-flight whereas another two have acquired latches and are still evaluating.
Presumably the in-flight proposals were assigned to *Lease Applied Indexes* 13
through 15, and the ones being evaluated will receive 15 and 16 (depending on
the order in which they enter Raft). This is where the *MPT*'s second function
comes in: it tracks writes until they are assigned a (provisional) *Lease
Applied Index*, and makes sure that an authoritative *MLAI* delta is returned
with each closed timestamp. This delta is *authoritative* in the sense that it
will reflect the largest **proposed** *MLAI* seen relevant to the newly closed
timestamp (relative to the previous one).

Consequently when we say that a proposal is tracked, we're talking about the
interval between determining the request timestamp (which is after acquiring
latches) and determining the proposal's *Lease Applied Index*.

It's natural to ask whether there may be "false positives", i.e. whether a
command proposed for some *Lease Applied Index* may never actually return from
Raft with a corresponding update to the range state. The response is that this
isn't possible: a command proposed to Raft is either retried until's clear that
the desired *Lease Applied Index* has already been surpassed (in which case
there is no problem) or the leaseholder process exits (in which case there will
be a new leaseholder and previous in-flight commands that never made it into the
log are irrelevant).

The naive approach of tracking the maximum assigned lease applied index is
problematic. To see this, consider the relevant example of a store that wants to
close out a timestamp around five seconds in the past, but which has high write
throughput on some range. Tracking the maximum proposed lease applied index
until we close out the timestamp `now()-5s` means that a follower won't be able
to serve reads until it has caught up on the last five seconds as well, even
though they are likely not relevant to the reads it wants to serve. This
motivates the precise form of the *MPT*, which has two adjacent "buckets" that
it moves forward in time: one tracking proposals relevant to the next closed
timestamp, and one with proposals relevant for the one after that.

The MPT consists of the previously emitted closed timestamp (zero initially) and
a prospective next closed timestamp aptly named `next` (always strictly larger
than `closed`) at or below which new writes are not accepted. It also contains
two ref counts and *MLAI* maps associated to below and above `next`,
respectively.

Its API is roughly the following:

```go
// t := NewTracker()

// In Replica write path:
waitCmdQueue(ba)
applyTimestampCache(ba)
ts, done:= t.Track(ba.Timestamp)
ba.ForwardTimestamp(ts)
proposal := evaluate(ba)
proposal.LeaseAppliedIndex = <X>
done(proposal.LeaseAppliedIndex)
propose(proposal)

// In periodic store-level loop:
closedTS, mlaiMap := t.CloseWithNext(clock.Now()-TargetDuration)
sendUpdateToPeers(closedTS, mlaiMap)
```

Note that by using this API for *any* proposal it is guaranteed that we produce
all the updates promised to consumers of the CT updates. A few redundant pieces
of information may be sent (i.e. for lease requests triggering on a follower
range) but these are infrequent and cause no harm.

In what follows we'll to through an example, which for simplicity assumes that
all writes relate to the same range (thus reducing the *MLAI* maps to scalars).
The state of the *MPT* is laid out as in the diagram below. You see a previously
closed timestamp as well as a prospective next closed timestamp. There are three
proposals tracked at timestamps strictly less than `next`, and one proposal at
`next` or higher. Additionally, for proposals strictly less than `next`, the
*MLAI* `8` was recorded while that for the other side is `17`.

```
   closed           next
      |            @8 | @17
      |            #3 | #1
      |               |
      v               v
---------------------------------------------------------> time
```

Let's walk through an example of how the MPT works. For ease of illustration, we
restrict to activity on a single replica (which avoids having a *map* of
*MLAI*s; now it's just one). Initially, `closed` and `next` demarcate some time
interval. Three commands arrive; `next`'s right side picks up a refcount of three
(new commands are forced above `next`, though in this case they were there to begin
with):

```
          closed    next    commands
             |     @0 | @0     /\   \_______
             |     #0 | #3    /  \          |
             v        v       v  v          v
------------------------------x--x----------x------------> time
```

Next, it's time to construct a CT update. Since `next`'s left has a refcount of
zero, we know that nothing is in progress for timestamps below `next`, which
will now officially become a closed timestamp. To do so, `next` is returned to
the client along with the *MLAI* for its left (there is none this time around).
Additionally, the data structure is set up for the next iteration: `closed` is
forwarded to `next`, and `next` forwarded to a suitable timestamp some constant
target duration away from the current time. The commands previously tracked
ahead of `next` are now on its left. Note that even though one of the commands
has a timestamp ahead of `next`, it is now tracked to its left. This is fine; it
just means that we're taking a command into account earlier than required for
correctness.

```
                                         next
                                       @0 | @0
                    closed   commands  #3 | #0
                      |        /\   \_____|__
                      |       /  \        | |
                      v       v  v        v v
------------------------------x--x----------x------------> time
```

Two of the commands get proposed (at *LAI*s, say, 10 and 11), decrementing
the left refcount and adding an *MLAI* entry of 11 (the max of the two) to it.
Additionally, two new commands arrive, this time at timestamps below `next`.
 These commands are forced above `next` first, so the refcount goes to the right.
These new commands get proposed quickly (so they don't show
up again) and the right refcount will drop back to zero (though it will retain the
max *MLAI* seen, likely 13).

```
                            in-flight
                   closed    command     next
                      |         \       @11| @0
                      |          \      #1 | #2
                      v          v         v
---------------------------------x-----------------------> time
                                           ʌ
                                           |
            _______________________________/
           |   forwarding    |
           |                 |
       new command         new command
   (finishes quickly @13) (finishes quickly @12)
```

The remaining command sticks around in the evaluation phase. This is
unfortunate; it's time for another CT update, but we can't send a higher closed
timestamp than before (and must stick to the same one with an empty *MLAI* map)

```
                  (blocked)             (blocked)
                            in-flight
                   closed    command     next
                      |         \       @11| @13
                      |          \      #1 | #0
                      v          v         v
---------------------------------x-----------------------> time
```

Finally the command gets proposed at LAI 14. A new command comes in at some
reasonable timestamp and the right side picks up a ref. Note the resulting
odd-looking situation in which the left is @14 and the right @13 (this is fine;
the client tracks the maximum seen):

```
                   closed                next     in-flight
                      |                 @14| @13  proposal
                      |                 #0 | #1     |
                      v                    v        v
----------------------------------------------------x----> time
```

Time for the next CT update. We can finally close `next` (emitting @14) and move
it to `now-target duration`, moving the right side refcount and *MLAI* to the
left in the process.

```
                                         closed   in-flight  @13| @0
                                           |      proposal   #1 | #0
                                           |        |     _____/
                                           |        |    /
                                           v        v   v
----------------------------------------------------x----> time
```

## Initial catch-up

The main mechanism for propagating *MLAI*s is triggered by proposals. When an
initial update is created, valid *MLAI*s have to be obtained for all ranges for
which followers are supposed to be able to serve reads. This raises two practical
questions: for which replicas should an *MLAI* be produced, and how to produce one.

We create an *MLAI* for all ranges for which (at the time of checking) the
current state indicates that the lease is held by the local store (this can have
both false positives and false negatives but a missed follower read will trigger
a proactive upgrade for the range it occurred on).

The initial catch-up is simple: before closing a timestamp (via the MPT), iterate
through all ranges and (if they show the store as holding the lease) feed the
MPT a proposal that lets it know the most recent *Lease Applied Index* on that
replica:

```go
_, done:= t.Track(hlc.Timestamp{})
repl.mu.Lock()
lai := repl.mu.lastAssignedLeaseIndex
repl.mu.Unlock()
done(lai)
```

This can race with other proposals, but the MPT will track the maximum seen.

## Timestamp forwarding and intents

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
recovery: the restored consistent state may contain intents. But the
restored consistent state also allows resolving all of the intents in
the same way, since what matters is the transaction record. The result
will be that the intents are simply dropped, unless there is a committed
transaction record, in which case they will commit.

Note that for the CDC use case, this closed timestamp mechanism is a necessary,
but not sufficient, solution. In particular, a CDC consumer must find (or track)
and resolve all intents at timestamps below a given closed timestamp first.

## Splits/Merges

No action is necessary for splits: the leaseholders of the LHS and RHS are
colocated and so share the same closed timestamp mechanisms. For convenience an
update for the RHS is added to the next round of outgoing updates, otherwise
follower reads for the RHS would cut out for a moment.

Merges are more interesting since the leaseholders of the RHS and the LHS are
not necessarily colocated. If the RHS's store has closed a higher timestamp, say
1000, while the LHS's store is only at 500, after the merge commands might be
accepted on the combined range under the closed timestamp 500 that violate the
closed timestamp 1000. To counteract this, the `Subsume` operation
returns the closed timestamp on the origin store and the merging replica
takes it into account. Initially, the split trigger will populate the
timestamp cache for the right side of the merge; if this has too big an impact
on the timestamp cache (especially as merges are rolled out, we might merge
away large swaths of empty ranges), we can also store the timestamp on the
replica and use it to forward proposals manually.

## Routing layer

This RFC proposes a somewhat simplistic implementation at the routing layer: At
`DistSender` and its DistSQL counterpart, if a read is for a timestamp earlier
than the current time less a target duration (which adds comfortable padding to
when followers are ideally able to serve these reads), it is sent to the nearest
replica (as measured by health, latency, locality, and perhaps a jitter),
instead of to the leaseholder.

When a read is handled by a replica not equipped to serve it via a regular or
follower read, a `NotLeaseHolderError` is returned and future requests for that
same (chunk of) batch will make no attempt to use follower reads; this avoids
getting stuck in an endless loop when followers lag significantly. Similarly,
follower reads are never attempted for ranges known not to use epoch based
leases.

## Further work

While the design outlined so far should give a reasonably performant baseline,
it has several shortcomings that will need to be addressed in follow-up work:

### Lagging followers

Assume that timestamps are closed at a 5s target duration every second, and
that the last proposal taken into account for each closed timestamp finishes
evaluating just before the timestamp is closed out. In that case, the *MLAI*
check on the followers is more likely to fail for a short moment until the Raft
log has caught up with the very recent proposal; if the catch-up takes longer
than the interval at which the timestamps are closed out, no follower read will
ever be possible. A similar scenario applies to followers far removed from the
usual commit quorum or lagging for any other reason. This should be fairly
rare, but seems important enough to be tackled in follow-up work.

The fundamental problem here is that older closed timestamps are discarded when
a new one is received, resulting in the follower never catching up to the current
closed timestamp. If it remembered the previous CT updates, it could at least
serve reads for that timestamp. This calls for a mechanism that holds on to
previous *CT*s and *MLAI*s so that reads further in the past can be served.
This won't be implemented initially to keep the complexity in the first version
to a minimum.

One way to address the problem is the following: On receipt of a CT update, copy
the CT and MLAI into the range state if the Raft log has caught up to the MLAI
(keeping the most recently overwritten value around to serve reads for). This
means that the replica will always have a valid CT during normal operation,
though one that lags the received updates (various variations on this theme
exist). However, note the strong connection to the following section:

### Recovery from insufficient quorum

As mentioned in the initial paragraphs, follower reads can help recover a
recent consistent state of an unavailable cluster, by determining the maximum
timestamp at which every range has a surviving replica that can serve a
follower read (if all replicas of a range are lost, there is obviously no hope
of consistent recovery).
At this timestamp, a consistent read of the entire keyspace (excluding
expiration-based ranges) can be carried out and used to construct a backup.
Note that if expiration-based replicas persisted the last lease they held, the
timestamp could be lowered to the minimum over all surviving expiration based
replicas' last leases, for a consistent (but less recent) read of the *whole*
keyspace.

For maximum generality, it is desirable to in principle be able to recover
without relying on in-memory state, so that a termination of the running
process does not bar a subsequent recovery.

Naively this can be achieved by persisting all received *CT* updates (with
some eviction policy that rolls up old updates into a more recent initial
state), though the eventual implementation may opt to persist at the Replica
level instead (where updates caught up to can more easily be pruned).

### Range feeds

[Range feeds] are a range-level mechanism to stream updates to an upstream
Change Data Capture processor. Range feeds will rely on closed timestamps and
will want to relay them to an upstream consumer as soon as possible.  This
suggests a reactive mechanism that notifies the replicas with an active Range
feed on receipt of a CT update; given a registry of such replicas, this is easy
to add.

### `AS OF SYSTEM TIME RECENT`;

With the advent of closed timestamps, we can also simplify `AS OF SYSTEM TIME`
by allowing users to let the server chose a reasonable "recent" timestamp in
the past for which reads can be distributed better. Note that, other than
what was requested in [this issue][autoaost], there is no guarantee about
blocking on conflicting writers. However, since a transaction that has
`PENDING` status with a timestamp that has since been closed out is likely
to have to restart (or ideally refresh) anyway, we could consider allowing it
to be pushed.

## Rationale and Alternatives

This design appears to be the sane solution given boundary conditions.

## Unresolved questions

### Configurability

For now, the min proposal timestamp roughly trails real time by five seconds.
This can be made configurable, for example via a cluster setting or, if more
granularity is required, via zone configs (which in turn requires being able to
retrieve the history of the settings value or a mechanism that smears out the
change over some period of time, to avoid failed follower reads).

Transactions which exceed the lag are usually forced to restart, though this
will often happen through a refresh (which is comparatively cheap, though it
needs to be tested).

[RangeFeed]: https://github.com/cockroachdb/cockroach/pull/26782
[autoaost]: https://github.com/cockroachdb/cockroach/issues/25405
