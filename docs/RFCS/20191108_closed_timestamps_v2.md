- Feature Name: Log-Driven Closed Timestamps
- Status: draft
- Start Date: 2020-11-13
- Authors: Andrei Matei, Nathan VanBenschoten
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

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
    last applies Raft entry without going through consensus.
    - The entries coming from the side-transport don't need to be written to any storage.
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

# Design

A concept fundamental to this proposal is that closed timestamps become
associated to Raft log positions. Each Raft command carries a closed timestamp
that says: whoever applies this command *c1* with closed ts *ts1* is free to
serve reads at timestamps <= *ts* on the respective range. Closed timestamps
carried by commands that ultimately are rejected are inconsequential. This is a
subtle, but important different from the existing infrastructure. Currently, an
update says something like "if a follower gets this update (epoch,MLAI), and if,
at the time when it gets it, the sender is still the leaseholder, then the
receiver will be free to use this closed timestamp as soon as replication
catches up to MLAI". The current proposal does away with the lease part - the
receiver no longer concerns itself with the lease status. Referencing leases in
the current system is necessary because, at the time when the provider sends a
closed timestamp update, that update references a command that may or may not
ultimately apply. If it doesn't apply, then the epoch is used by the receiver to
discern that the update is not actionable. In this proposal, leases no longer
play a role because updates refer to committed commands, not to uncertain
commands.

We'll first describe how closed timestamps are propagated, and then we'll
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
of the `RangeStateAppliedKey` - the range-local key maintaining the range stats,
among others. This key is already being written to on every command application
(because of the stats). Snapshots naturally include the `RangeStateAppliedKey`,
so new replicas will be initialized with a closed timestamp.

We've talked about "active/inactive" ranges, but haven't defined them. A range
is inactive at a point in time if there's no inflight write requests for it
(i.e. no writes being evaluated at the moment). The leaseholder can verify this
condition. If there's no lease, then no replica can verify the condition by
itself, which will result in the range not advancing its closed timestamp
(that's also the current situation (right?)). When a leaseholder detects a range
to be inactive, it can choose bump its closed timestamp however it wants (as
long as it promises to not accept new writes below it). Each node will
periodically scan its ranges for inactive ones (in reality, it will only
consider ranges that have been inactive for a little while) and, when detecting
a new one, it will move it over to the set of inactive ranges for which closed
timestamps are published periodically through a side-transport (see below).

When the leaseholder for an inactive range starts evaluating a write request,
the range becomes active again, and so goes back to having its closed timestamps
transported by Raft proposals.

The inactivity notion is similar to quiescence, but otherwise distinct from it
(there's no reason to tie the two). In practice, the process that detects ranges
eligible for quiescing will also become responsible for detecting inactive
ranges.

### Side-transport for inactive ranges

As hinted above, each node will maintain a set of inactive ranges for which
timestamps are to be closed periodically (according to
`kv.closed_timestamp.close_fraction`) and communicated to followers through a
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
respective ranges, and associated with a Raft log position. We don't want to
communicate them using the normal mechanism (Raft messages) because we don't
want to send messages on otherwise inactive ranges - in particular, such
messages would unquiesce the range - thus defeating the point of quiescence. So,
we'll use another mechanism: a streaming RPC between each provider node and
subscriber node. This stream would carry compressed closed timestamp updates for
the sender's set of inactive ranges, grouped by the ranges' closed ts policy.
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
Inactive ranges are grouped by their closed ts policy and the members of
each group are specified. For each range, the last log position (as of the time
when the update is sent) is specified - that's the position that the closed
timestamp refers to. The receiver of this update will bump the closed timestamp
associated with that log position to the respective group's new closed
timestamp.

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
and thus doesn't need to worry that overwriting the <lai,closed ts> pair that it
is currently using for the range with this newer info will result in the
follower not being to serve follower-reads at any timestamp until replication
catches up. The current implementation has to maintain a history of closed ts
updates per range for this reason, and all that can go away.

## Closing of timestamps

We've talked about how closed timestamps are communicated by a leaseholder to
followers. Now we'll describe how a timestamp becomes closed. The scheme is
pretty similar to the existing one, but simpler because LAI don't need to be
tracked anymore, and because all the tracking of requests can be done per-range
(instead of per-store). Another difference is that the existing scheme relies on
a timer closing out timestamps, where this proposal relies just on the flow of
requests to do it (for active ranges).

Each range will have a *tracker* which keeps track of what requests have to
finish evaluating for various timestamps to be closed. It maintains an ordered
set of "buckets" of requests in the process of evaluating. Each bucket has a
timestamp and a reference count associated with it; every request in the bucket
evaluated as a higher timestamp than the bucket's timestamp. The timestamp of
bucket *i* is larger than that of bucket i-1*. The buckets group requests into
"epochs", each epoch being defined by the smallest timestamp allowed for a
request to evaluate at. At any point in time only the last two buckets are
active (and so the implementation can just maintain two buckets and continuously
swap them) - we'll call the last bucket cur* and the previous one *prev*. The
range's closed timestamp is always prev*'s timestamp - so every proposal carries
*prev*'s timestamp, regardless of whether the proposal comes from a request in
*prev* or in *cur*. When a request arrives, it enters *cur*. A request that
finds *prev* to be empty increments *prev* and *cur*. This is the moment when
the range's closed timestamp moves forward - because now the timestamp that used
to be associated with *cur* becomes associated with *prev* and so it is closed.
This rule means that the first first request on a range will join *cur* but then
immediately find itself in *prev* - which makes bootstrap work. When creating a
new bucket, that bucket's timestamp is not set immediately. Instead, it is set
by the first request to enter it. That request sets at `now() -
kv.closed_timestamp.target_duration`, or according to whatever the range's
policy is.
 
In order to keep the closed timestamps carried by successive commands monotonic,
requests fix the closed timestamp that their proposal will carry before exiting
their bucket. So, after evaluation is done, the sequence is:
1. Read *prev*'s closed timestamp.
2. Enter the `ProposalBuffer` and get sequenced with the other proposals.
3. Exit your bucket (i.e. decrement the bucket's refcount and, if 0, increment
*prev* and *cur*).

Lease requests are special as in they don't carry a closed timestamp. As
explained before, the lease start time acts that proposal's closed ts.

Some diagrams:

In the beginning, there are two empty buckets:

```
|               |               |
|prev;ts=<nil>  |cur;ts=<nil>   |
---------------------------------
```

The first request (`r1`) comes and enters *cur*. Since it's the first request to
ever enter *cur*, it sets its timestamp to `now()-5s = 10(say)` (say that the
range's target is a trail of 5s). Once that timestamp is set, the request bumps
its own timestamp to its buckets timestamp. The request then notices that *prev*
is empty so it shifts *cur* over (thus now finding itself in *prev*). `ts=10` is
now closed, since it's *prev*'s timestamp.

```
|refcnt: 1 (r1) |               |
|prev;ts=10     |cur;ts=<nil>   |
---------------------------------
```

While 3 other requests come. They all enter *cur*. The first one among them sets
*cur*'s timestamp to, say, 15.


```
|refcnt: 1 (r1) |refcnt:3    |
|prev;ts=10     |cur;ts=15   |
---------------------------------
```

Let's say two of these requests finish evaluating while `r1`'s eval is still in
progress. They exit *cur* and they carry the ranges closed timestamp at the time
of their exit (i.e. *prev*'s `ts=10`). Then let's say `r1` finishes evaluating
and exits. Nothing more happens, even though prev* is now empty.

```
|refcnt: 0      |refcnt:1    |
|prev;ts=10     |cur;ts=15   |
---------------------------------
```

Now say the last request out of *cur* (call it `r2`) exits the bucket. It still
carries a `closedts=10`. Nothing else happens.

```
|refcnt: 0      |refcnt:0    |
|prev;ts=10     |cur;ts=15   |
---------------------------------
```

At this point the range is technically inactive. If enough time passes, the
background process in charge of publishing updates for inactive ranges using the
side transport will pick it up. But let's say another request comes in quickly.
When the next request comes in, though, the same thing will happen as when the
very first request came: it'll enter *cur*, find *prev* to be empty, shift over
*cur*->*prev* and create a new bucket. The range's closed timestamp is now `15`,
and that's what every proposal will carry from now on.

Notice that the scheme is technically suboptimal. `r2` could have carried a
`closedts=15` - in other words we could have shifted the buckets also when the
last request out of *prev* exited. The logic presented seems to be the minimal
one that's sufficient for keeping closed timestamps moving forward.

## Migration

!!! both systems run at the same time for a release
