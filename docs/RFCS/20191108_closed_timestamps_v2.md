- Feature Name: Log-Driven Closed Timestamps
- Status: implemented
- Start Date: 2020-11-13
- Authors: Andrei Matei, Nathan VanBenschoten
- RFC PR: [56675](https://github.com/cockroachdb/cockroach/pull/56675)
- Cockroach Issue: None

# Summary

This RFC discusses changing the way in which closed timestamps are published
(i.e. communicated from leaseholders to followers). It argues for replacing the
current system, where closed timestamps are tracked per store and communicated
out of band from replication, and migrating to a hybrid scheme where, for
“active” ranges, closed timestamps are piggy-backed on Raft commands, and
"inactive" ranges continue to use a separate (but simpler) out-of-band/coalesced
mechanism.

This RFC builds on, and contrasts itself, with the current system. Familiarity
with [the status
quo](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180603_follower_reads.md)
is assumed.

# Motivation

The Non-blocking Transactions project requires us to have at least two
categories of ranges with different policies for closing timestamps. The current
system does not allow this flexibility. Even regardless of the new need, having
a per-range closed timestamp was already something we've repeatedly wanted. The
existing system has also proven to be subtle and hard to understand; this
proposal seems significantly simpler which would be a big win in itself.

## Simplicity

- The existing closed timestamp subsystem is too hard to understand. This
proposal will make it more straightforward by inverting the relationship between
closed timestamps and log entries. Instead of closed timestamp information
pointing to log entries, log entries will contain closedts information.
- Closed timestamps will no longer be tied to liveness epochs. Currently,
followers need to reconcile the closed timestamps they receive with the current
lease (i.e. does the sender still own the lease for each of the ranges it's
publishing closed timestamps for?); a closed timestamp only makes sense when
coupled with a particular lease. This proposal gets rid of that; closed
timestamps would simply be associated with log positions. The association of
closed timestamps to leases has resulted in [a subtle bug](https://github.com/cockroachdb/cockroach/issues/48553).
    - This is allowed by basing everything off of applied state (i.e. commands
    that the leaseholder knows have applied / will apply) because command
    application already takes into account lease validity using lease sequences.
- The Raft log becomes the transport for closed timestamps on active ranges, eliminating
the need for a separate transport. The separate transport remains for inactive
(inactive is pretty similar to quiesced) ranges, but it will be significantly
simpler because:
    - The log positions communicated by the side-transport always refer to
    applied state, not prospective state. The fact that currently they might
    refer prospective state (from the perspective of the follower) is a
    significant source of complexity.
    - The side-transport only deals with ranges without in-flight proposals.
    - The side-transport would effectively increase the closed timestamp of the
    last applied Raft entry without going through consensus.
    - The entries coming from the side-transport don't need to be stored
    separately upon reception, eliminating the need for
    `pkg/kv/kvserver/closedts/storage`.
- No ["Lagging followers"
problem](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180603_follower_reads.md#lagging-followers).
Closed-timestamps are immediately actionable by followers receiving them
(because they refer to state the followers have just applied, not to prospective
state).
- The proposal avoids closed timestamp stalls when MLAI is never reached due to
failed proposal and then quiescence.
- [Disabling closed timestamps during
merges](https://github.com/cockroachdb/cockroach/pull/50265) becomes much
simpler. The current solution is right on the line between really clever and
horrible - because updates for many ranges are bundled together, we rely on
publishing a funky closed timestamp with a LAI that can only be reached if the
merge fails.
- Propagating the closed timestamps to the RHS on splits becomes easier.
Currently, this happens below Raft - each replica independently figures out an
`initialMaxClosed` value suitable for the new range. With this proposal, the
RHS's initial closed will simply be the closed ts carried by the command
corresponding to the `EndTxn` with the `SplitTrigger`.
- We can stop resetting the timestamp cache when a new lease is applied, and
instead simply rely on the closed timestamp check. Currently, we need both; the
closed timestamp check is not sufficient because the lease's start timestamp is
closed asynchronously wrt applying the lease. In this proposal, leases will
naturally carry (implicitly) their start as a closed timestamp.
  

## Flexibility

- The proposal makes the closed timestamp entirely a per-range attribute,
eliminating the confusing relationship between the closed timestamp and the
store owning the lease for a range. Each range evolves independently,
eliminating cross-range dependencies (i.e. a request that's slow to evaluate no
longer holds up the closing of timestamps for other ranges) and letting us set
different policies for different ranges. In particular, for the Non-blocking
Transactions project, we'll have ranges with a policy of closing out *future*
timestamps.
- Ranges with expiration-based leases can now start to use closed timestamps
too. Currently, only epoch-based leases are compatible with closed timestamps,
because a closed timestamp is tied to a lease epoch.
- Closed timestamps will become durable and won't disappear across node
restarts, because they'll be part of the Range Applied State key.
    - This is important for [recovery from insufficient
    quorum](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180603_follower_reads.md#recovery-from-insufficient-quorum).
    With the existing mechanism, it's unclear how a consistent closed timestamp
    can be determined across multiple ranges. With the new proposal, it becomes
    trivial.
- The proposal will make it easier for follower reads to wait when they can't be
immediately satisfied, instead of being rejected and forwarded to the
leaseholder immediately. We don't currently have a way for a follower read that
fails due to the follower's closed timestamp to queue up and wait for the
follower to catch up. This would be difficult to implement today because of how
closed timestamp entries are only understandable in the context of a lease.
Keeping these separate and having a clear, centralized location where a
replica's closed timestamp increases would make this kind of queue much easier
to build.

# Design

A concept fundamental to this proposal is that closed timestamps become
associated with Raft commands and their LAIs. Each command carries a closed
timestamp that says: whoever applies this command *c1* with closed ts *ts1* is
free to serve reads at timestamps <= *ts1* on the respective range because,
given that *c1* applied, all further applying commands perform writes at
timestamps > *ts1*.

Closed timestamps carried by commands that ultimately are rejected are
inconsequential. This is a subtle, but important difference from the existing
infrastructure. Currently, an update says something like "if a follower gets
this update (epoch,MLAI), and if, at the time when it gets it, the sender is
still the leaseholder, then the receiver will be free to use this closed
timestamp as soon as replication catches up to MLAI". The current proposal does
away with the lease part - the receiver no longer concerns itself with the lease
status. Referencing leases in the current system is necessary because, at the
time when the provider sends a closed timestamp update, that update references a
command that may or may not ultimately apply. If it doesn't apply, then the
epoch is used by the receiver to discern that the update is not actionable. In
this proposal, leases no longer play a role because updates refer to committed
commands, not to uncertain commands.


### Splits and Merges

The Right-Hand Side (RHS) of a split will start with the closed timestamps carried
by the command that creates the range (i.e. the `EndTxn` with the `SplitTrigger`).
We've considered choosing an initial closed timestamp for the RHS during
evaluation and putting it in the `SplitTrigger`. The problem with that is that
it'd allow the closed timestamp to regress for the RHS once the split is
applied, which would allow follower reads to use inconsistent snapshots across
ranges. Consider:
1. Say we're splitting the range `a-c` into `a-b` and `b-c`. The `AdminSplit` is
   evaluated. The `SplitTrigger` is computed to contain a closed ts = `10`.
2. After the `AdminSplit` is evaluated, but before the corresponding command
   (`c1`) is sequenced by the proposal buffer, another request evaluates and its
   proposal gets sequenced with a closed ts of `20`.
3. `c1` applies. Then the split applies on some, but not, all replicas. So now,
   the replicas that have applied the split have a closed ts of `20` for `a-b` and
   a closed ts of `10` for `b-c`. The replicas that have not applied it have a
   closed ts of `20` for `a-c`.
4. A transaction now writes at ts `15` to keys `b` and, say, `x`. Say that the
   follower who hasn't yet applied the split continues to not apply it for a while,
   and thus also doesn't apply the write to `b`.
5. Now a read is performed across `b` and `x` @ `16`. The read on `b` can be
   served by a non-split follower because it's below (what it believes to be) the
   relevant closed timestamp. This means that the read might fail to see the write in question,
   which is no good - in particular because the read could see the write on `x`.
   
For merges, the closed timestamp after the merge is applied will be the one of
the LHS. This technically allows for a regression in the RHS's closed timestamp
(i.e. the RHS might have been frozen at a time when its closed timestamps is 20
and the merge is completed when the closed ts of the LHS is 10), but this will
be inconsequential because writes to the old RHS keyrange between 10 and 20 will
be dissalowed by the timestamp cache: we'll initialize the timestamp cache of
the RHS keyrange to the freeze time (currently we only muck with the RHS's
timestamp cache if the leases for the LHS and RHS were owned by different
stores).

### LAIs vs. Raft log indexes

Like the current implementation, this proposal would operate in terms of LAIs,
not of Raft log indexes. Each command will carry a closed timestamp,
guaranteeing that further *applied* commands write at higher timestamps. We
can't perfectly control the order in which commands are applied to the log
because proposing at index *i* does not mean that the entry cannot be appended
to the log at a higher position in situations where the leaseholder (i.e. the
proposer) is different from the Raft leader. This kind of reordering of commands
in the log means that, even though the proposer increases the closed timestamps
monotonically, the log entries aren't always monotonic. However, applying
commands still have monotonic closed timestamps because reodered commands don't
apply (courtesy of the LAI protection).

The fact that a log entry's closed timestamp is only valid if the entry passes
the LAI check indicates that closed timestamps conceptually live in the lease
domain, not in the log domain, and so we think of closed timestamps being
associated with a LAI, not with a log index position. Another consideration is
the fact that there are Raft entries that don't correspond to evaluated requests
(things like leader elections and quiescence messages). These entries won't
carry a closed timestamp.

The LAI vs log index distinction is inconsequential in the case of entries
carrying their own closed timestamp, but plays a role in the design of the
side-transport: that transport needs to identify commands somehow, and it will
do so by LAI.

We'll now first describe how closed timestamps are propagated, and then we'll
discuss how timestamps are closed.

## Closed timestamp transport and storage

For "active" ranges, closed timestamps will be carried by Raft commands. Each
proposal carries the range's closed timestamp (at the time of the proposal). If
the proposal is accepted, it will apply on each replica. At application time,
each replica will thus become aware of a new closed timestamp. The one exception
are lease request (including lease transfers): these proposals will not carry a
closed timestamp explicitly. Instead, the lease start time will act as the
closed timestamp associated with that proposal. This allows us to get rid of the
reset of the tscache that we currently perform when a new lease is applied;
writes can simply check against the closed timestamp.

One thing to note is that closed ts are monotonic below Raft, so when applying
commands a replica can blindly overwrite its known closed timestamp. Another
thing to note is that, while a range is active, the close-frequency parameter of
closed timestamps (i.e. the `kv.closed_timestamp.close_fraction` cluster
setting) doesn't matter. Closed timestamp publishing is more continuous, rather
than the discreet increments dictated by the current, periodic closing. The
periodic closing will still happen for inactive ranges, though.

Each replica will write the closed timestamp it knows about in a new field part
of the `RangeStateAppliedKey` - the range-local key maintaining the applied LAI
and the range stats, among others. This key is already being written to on every
command application (because of the stats). Snapshots naturally include the
`RangeStateAppliedKey`, so new replicas will be initialized with a closed
timestamp. Splits will initialize the RHS by propagating the RHS's closed
timestamp through the `SplitTrigger` structure (which btw will be a lot cleaner
than the way in which we currently prevent closed ts regressions on the RHS - a
below-Raft mechanism).

We've talked about "active/inactive" ranges, but haven't defined them. A range
is inactive at a point in time if there's no inflight write requests for it
(i.e. no writes being evaluated at the moment and no proposals in-flight). The
leaseholder can verify this condition. If there's no lease, then no replica can
verify the condition by itself, which will result in the range not advancing its
closed timestamp (that's also the current situation); the next request will
cause a lease acquisition, at which point timestamps will start being closed
again. When a leaseholder detects a range to be inactive, it can choose to bump
its closed timestamp however it wants as long as it stays below the lease expiration
period (as long as it promises to not accept new writes below it). Each node
will periodically scan its ranges for inactive ones (in reality, it will only
consider ranges that have been inactive for a little while) and, when detecting
a new one, it will move it over to the set of inactive ranges for which closed
timestamps are published periodically through a side-transport (see below). An
exception is a subsumed range, which cannot have its closed timestamp advanced
(because that might cause an effecting closed ts regression once the LHS of the
respective merge takes over the key space).

When the leaseholder for an inactive range starts evaluating a write request,
the range becomes active again, and so goes back to having its closed timestamps
transported by Raft proposals.

The inactivity notion is similar to quiescence, but otherwise distinct from it
(there's no reason to tie the two). Quiescence is initiated on each range's Raft
ticker (ticking every 200ms); we may or may not reuse this for detecting
inactivity. 200ms might prove too high of a duration for non-blocking ranges,
where this duration needs to be factored in when deciding how far in future a
non-blocking txn writes.

### Side-transport for inactive ranges

As hinted above, each node will maintain a set of inactive ranges for which
timestamps are to be closed periodically and communicated to followers through a
dedicated streaming RPC. Each node will have a background goroutine scanning for
newly-inactive ranges to add to the set, and ranges for which the lease is not
owned any more to remove from the set (updates for a range are only sent for
timestamps at which the sender owns a valid lease). Writes remove ranges from
the set, as do lease expirations. Periodically, the node closes new timestamps
for all the inactive ranges. The exact timestamp that is closed depends on each
individual range's policy, but all ranges with the same policy are bumped to the
same timestamp (for compression purposes, see below). For "normal" ranges, that
timestamp will be `now() - kv.closed_timestamp.target_duration`.

Once closed, these timestamps need to be communicated to the followers of the
respective ranges, and associated with a Raft entries. We don't want to
communicate them using the normal mechanism (Raft messages) because we don't
want to send messages on otherwise inactive ranges - in particular, such
messages would unquiesce the range - thus defeating the point of quiescence. So,
we'll use another mechanism: a streaming RPC between each provider node and
subscriber node. This stream would carry compressed closed timestamp updates for
the sender's set of inactive ranges, grouped by the ranges' closed ts policy. In
effect, each node will broadcast information about all of its inactive ranges to
every other node (thus every recipient will only be interested in some of the
updates - namely the ones for which it has a replica). The sender doesn't try to
customize messages for each recipient.

The first message on each stream will contain full information about the groups:
```json
groups: [
  {
    group id:  g1,
    members: [{range id:  1, log index:  1}, {range id:  2, log index:  2}], 
    timestamp: 100,
  },
  {
    group id:  g2,
    members: [{range id:  3, log index:  3}, {range id:  4, log index:  4}], 
    timestamp: 200,
  },
  ...
]
```
Inactive ranges are grouped by their closed ts policy and the members of each
group are specified. For each range, the last LAI (as of the time when the
update is sent) is specified - that's the entry that the closed timestamp refers
to. The receiver of this update will bump the closed timestamp associated with
that LAI to the respective group's new closed timestamp.

After the initial message on a stream, following messages are deltas of the form:
```json
groups: [
  {
    group id:  g1,
    removals: [{range id:  1, log index:  1}],
    additions: [{range id:  5, log index:  5}],
    timestamp: 200,
  },
  ...
]
```

The protocol takes advantage of the fact that the streaming transport doesn't
lose messages.

It may seem that this side-transport is pretty similar to the existing
transport. The fact that it only applies to inactive ranges results in a big
simplification for the receiver: the receiver doesn't need to be particularly
concerned that it hasn't yet applied the log position referenced by the update -
and thus doesn't need to worry that overwriting the `<lai,closed ts>` pair that it
is currently using for the range with this newer info will result in the
follower not being to serve follower-reads at any timestamp until replication
catches up. The current implementation has to maintain a history of closed ts
updates per range for this reason, and all that can go away.

The recipient of an update will iterate through all the update's ranges and
increment the respective's replica's (if any) closed timestamp. A case to
consider is when the follower hasn't yet applied the LAI referenced by the
update; this is hopefully uncommon, since updates refer to inactive ranges so
replication should generally be complete. When an update with a not-yet-applied
LAI is received, the replica's closed timestamp cannot simply be bumped, so
we'll just ignore such an update (beyond still processing the respective range's
addition to an inactive group). An alternative that was considered is to have
the replica store this prospective update in a dedicated structure, which could
be consulted on command application; when the expected LAI finally applies, the
command application could also bump the closed timestamp and clear the
provisional update structure. We opted against this complication as it would
only provide momentary improvements: without doing it, it takes an extra message
from the side transport after the range's replication catches up to bump the
closed timestamp.

Note that this scheme switches the tradeoff from the current
implementation: currently, closed timestamp updates on the follower side are
lazy; replicas are not directly updated in any way, instead letting reads
consult a separate structure for figuring out what's closed. The current
proposal makes updates more expensive but reads cheaper.

## Closing of timestamps

We've talked about how closed timestamps are communicated by a leaseholder to
followers. Now we'll describe how a timestamp becomes closed. The scheme is
pretty similar to the existing one, but simpler because LAI don't need to be
tracked anymore, and because all the tracking of requests can be done per-range
(instead of per-store). Another difference is that the existing scheme relies on
a timer closing out timestamps, where this proposal relies just on the flow of
requests to do it (for active ranges).

### Theoretical considerations

We *can* close a timestamp as soon as there's no in-flight evaluation of a
request at a lower timestamp. As soon as we close a timestamp, any write at a
lower timestamp needs to be bumped. So, we don't want to be too aggressive in
closing timestamps; we want to always trail current time some (this doesn't talk
about leading closed timestamps for non-blocking ranges) such that there's some
slack for recent transactions to come and perform their writes without
interference.

The scheme that directly encodes what we just said would be tracking in-flight
evaluations in a min-heap, ordered by their evaluation timestamp. As soon as an
evaluation is done, we could take it out of the heap and we could close the
remaining minimum (if it is below the desired slack). Actually maintaining this
heap seems pretty expensive, though. To make it cheaper, we could start
compressing the requests into fewer heap entries: we could discretize the
timestamps into windows of, say, 10ms. Each window could be represented by the
heap by one element, with a ref count. If we'd maintain these buckets in the
simplest way possible - a circular buffer - then their count would still be
unbounded as it depends on the evaluation time of the slowest request. Going a
step further would be placing a limit on the number of buckets and, once the
limit is reached, just have new requests join the last bucket. What should this
limit be? Well, one doesn't really work, and the next smallest number is two.
So, at the limit, we could maintain just two buckets.

The current implementation uses two buckets, and the narrative above is a
post-rationalization of how we got it. The current proposal keeps a two-buckets
scheme in place.

#### Analysis

Let's analyze how the two-buckets scheme behaves vs. the optimal approach of a
min-heap. What we want to know is 1) what is the rate that the tracker advances,
and 2) what is the lag that it imposes. Let's say that each write takes time *L*
to evaluate. The worst case is when a new request joins
*cur* immediately before it shifts. So then *prev* takes *L* to shift again, as it
needs to wait for the entire duration of the newly added request's evaluation.
So the rate of the tracker is *1/L*.  
Then there's the lag. We pick a timestamp that we want to close when
initializing *cur*. We then need to wait for one shift for this to be added to log
entries as the closed timestamp. This is then the active closed timestamp for *L*,
so the worst-case lag is *2L*.

If we compare that to the min-heap, get a rate of ∞ (period of 0) -
the closed timestamp can advance as fast as proposals happen. However, we do get
a lag of *L*, because we need to wait for the earliest entry to finish evaluating
before the closed timestamp can progress.

So the two-bucket approach leads to twice the lag of the "optimal" min-heap
approach.

### How the tracker will work

Each range will have a *tracker* which keeps track of what requests are
currently evaluating - which indirectly means tracking what requests have to
finish evaluating for various timestamps to be closed. The tracker maintains an
ordered set of *buckets* of requests in the process of evaluating. Each bucket
has a timestamp and a reference count associated with it; every request in the
bucket evaluated (or is evaluating) as a higher timestamp than the bucket's
timestamp. The timestamp of bucket *i* is larger than that of bucket *i-1*. The
buckets group requests into "epochs", each epoch being defined by the smallest
timestamp allowed for a request to evaluate at (a request's write timestamp
needs to be strictly greater than its bucket's timestamp). At any point in time
only the last two buckets can be non-empty (and so the implementation will just
maintain two buckets and continuously swap them) - we'll call the last bucket
*cur* and the previous one *prev*. We'll say that we "increment" *prev* and
*cur* to mean that we move the *prev* pointer to the next bucket, and we
similarly create a new bucket and move the *cur* pointer to it.

The tracker will logically be part of the `ProposalBuffer` - it will be
protected by the buffer's peculiar rwlock. Insertions into the buckets will need
the read lock (similar to insertions into the buffer), and the flushing of the
buffer (done under a write lock) removes the flushed requests from their
buckets. Integrating the tracker with the `ProposalBuffer` helps to ensure that
the closed timestamps carried by successive commands are monotonic. Requests
need to fix the closed timestamp that their proposal will carry either before,
or otherwise atomically with, exiting their bucket. It'd be incorrect to allow a
request to exit its bucket and fix its proposal's timestamp later, because we'd
run the risk of assigning a closed ts that's too high (as it possibly wouldn't
take into account other requests with higher timestamps that have also exited
the bucket after it).

As opposed to the current implementation, the buckets and their timestamps don't
directly related to the current closed timestamp. The range's closed timestamp
(meaning, the highest timestamp that was attached to a proposal) will be
maintained separately by the tracker. This allows the command corresponding to
the last requests that leaves the bucket (i.e. commands that leave both buckets
empty when they exit) to carry a timestamp relative to the flush time, not one
relative to when the request started evaluating. Any new request exiting the
buckets, though, will carry a closed ts that's higher or equal to prev*'s
timestamp.

### Rules for maintaining the buckets

When a request arrives, it enters *cur*. If *cur*'s timestamp is not set, it is
set now to `now() - kv.closed_timestamp.target_duration` (or according to
whatever the range's policy is). If *prev* is found to be empty, both *prev* and
*cur* are incremented - after the increment, the request finds itself in *prev*,
and *cur* is a new, empty bucket.

When creating a new bucket (by incrementing *cur*), that bucket's timestamp is
not set immediately. Instead, it is set by the first request to enter it. Notice
that a bucket's timestamp does not depend on the request creating the bucket, so
timestamps for buckets are monotonically increasing.

When a requests exits its bucket (i.e. at buffer flush time), the logic depends
on what bucket it's exiting. A request that's not the last one in its bucket
doesn't do anything special. When the request is the last one:

1. If exiting *prev*, then *prev* and *cur* are incremented. This way, a new bucket is created 
and the next arriving request will get to set its timestamp. 
   
2. If exiting *cur*, then *cur*'s timestamp is reset to the uninitilized state.
This lets the next request set a new, higher one. Note that we can't necessarily
increment cur* and *prev* because *prev* might not be empty.


### Rules for assigning a proposal's closed ts

Again, the closed ts that a proposal carries is determined at flush time - after all the
requests sequenced in the log before it have exited the tracker, and before any request sequenced
after it has exited.

1. If both buckets are empty, the timestamp is `now() -
kv.closed_timestamp.target_duration`. Notice how the timestamps of the buckets
don't matter, and the request's evaluation write timestamp (evaluation
timestamp) also doesn't matter. Indeed, the closed timestamp carrier by a
proposal can be higher than its write timestamp (such a case simply means that a
proposal is writing at, say, 10, and is also promising that future proposals
will only write above, say, 20).

2. If *prev* is empty, then *cur*'s ts is used. *cur*'s timestamp must be set, otherwise
we would have been in case 1.

3. Otherwise (i.e. if *prev* is not empty), *prev*'s timestamp is used.

Lease requests are special as they don't carry a closed timestamp. As explained
before, the lease start time acts as the proposal's closed ts. Such requests are
not tracked by the tracker.

#### Some diagrams:

In the beginning, there are two empty buckets:

```
|               |               |
|prev;ts=<nil>  |cur;ts=<nil>   |
---------------------------------
```

The first request (`r1`) comes and enters *cur*. Since *cur*'s ts is not set,
it gets sets now to `now()-5s = 10(say)` (say that the
range's target is a trail of 5s). Once the bucket timestamp is set, the request bumps
its own timestamp to its buckets timestamp. The request then notices that *prev*
is empty, so it shifts *cur* over (thus now finding itself in *prev*). Requests
from now on will carry a closed ts >= 10.

```
|refcnt: 1 (r1) |               |
|prev;ts=10     |cur;ts=<nil>   |
---------------------------------
```

While `r1` is evaluating, 3 other requests come. They all enter *cur*. The first
one among them sets *cur*'s timestamp to, say, 15.


```
|refcnt: 1 (r1) |refcnt:3    |
|prev;ts=10     |cur;ts=15   |
---------------------------------
```

Let's say two of these requests finish evaluating while `r1`'s eval is still in
progress. They exit *cur* and they carry prev*'s `ts=10`). Then let's say `r1`
finishes evaluating and exits (also carrying `10`). Nothing more happens, even
though *prev* is now empty; if *cur* were empty, *prev* and *cur* would now be
incremented.

```
|refcnt: 0      |refcnt:1 (r2) |
|prev;ts=10     |cur;ts=15     |
---------------------------------
```

Now say the last request out of *cur* (call it `r2`) exits the bucket. Since
both buckets are empty, it will carry `now - 5s`. We know that this is >= 15
because 15 was `now - 5s` at some point in the past. Since *cur* is empty, its
timestamp gets reset.

```
|refcnt: 0      |refcnt:0     |
|prev;ts=10     |cur;ts=<nil> |
---------------------------------
```

At this point the range is technically inactive. If enough time passes, the
background process in charge of publishing updates for inactive ranges using the
side transport will pick it up. But let's say another request comes in quickly.
When the next request comes in, though, the same thing will happen as when the
very first request came: it'll enter *cur*, find *prev* to be empty, shift over
*cur*->*prev* and create a new bucket.

## Migration

The existing closed timestamp tracker and transport will be left in place for a
release, working in parallel with the new mechanisms. Followers will only
activate closed timestamps communicated by the new mechanism after a cluster
version check.
