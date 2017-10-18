- Feature Name: Max safe timestamp
- Status: draft
- Start Date: 2017-10-11
- Authors: Alex Robinson, Nathan VanBenschoten
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#2656](https://github.com/cockroachdb/cockroach/issues/2656),
                   [#6130](https://github.com/cockroachdb/cockroach/issues/6130),
                   [#9712](https://github.com/cockroachdb/cockroach/issues/9712),
                   [#16593](https://github.com/cockroachdb/cockroach/issues/16593),
                   [#16838](https://github.com/cockroachdb/cockroach/pull/16838),
                   [#17535](https://github.com/cockroachdb/cockroach/pull/17535)

# Summary

Propose mechanisms for tracking what has come to be known as the "max safe
timestamp" for a range, which is the maximum timestamp at which a range
guarantees no more writes will be allowed to occur. While the thinking to this
point has been that follower reads and change feeds (i.e. CDC) could share an
approach to tracking the max safe timestamp, we believe that the different needs
of the two features makes partially decoupling them advantageous.

# Motivation

The motivation is best understood by reading through the RFCs for
[CDC](https://github.com/cockroachdb/cockroach/pull/17535) and [Change
Feeds](https://github.com/cockroachdb/cockroach/pull/16838) as well as the
[issue for follower
reads](https://github.com/cockroachdb/cockroach/issues/16593). To summarize,
non-leaseholder replicas need to know the range of timestamps at which it is
safe to serve reads without coordinating with the leaseholder, and change feeds
require what are known as "close notifications", which indicate no new writes
will be allowed to happen at timestamps earlier than some "closed" timestamp.

# Guide-level explanation

TODO: Initial version of this RFC is just for reaching consensus on rough ideas

# Reference-level explanation

TODO: Initial version of this RFC is just for reaching consensus on rough ideas

## Detailed design

### Goals

* Enable follower reads and change feeds.
* Don't affect the performance of clusters that aren't using the above features.
* Don't affect the performance of ranges that aren't using the above features.
* Minimize the performance impact on writes to ranges with active follower reads
  or changes feeds. A small performance hit caused by increased resource usage
  is to be expected, but there should be no blocking or artificial delays
  introduced.
* Allow users to customize the trade-off between allowing older writes and
  allowing more recent follower reads / close notifications.

### Follower reads

The basic approach to implementing follower reads was outlined [in a comment on
the
issue](https://github.com/cockroachdb/cockroach/issues/16593#issuecomment-309549461).
The high-level breakdown of how it works is:

* All replicas track the highest write timestamp they have appended to their
  raft log, which we'll call the `max_write_timestamp`.
* The leaseholder will also track the `max_proposed_write_timestamp`.
* The leaseholder promises not to propose a write with timestamp less than
  `max_proposed_write_timestamp - max_write_age`, where `max_write_age` is a
  configurable setting that will limit how old writes can be. The default
  value for `max_write_age` will be in the tens of seconds, but we expect that
  setting it down to just a few seconds will be common if we can support it.
  * Making this safe will require taking these timestamps into account when
    proposing or re-proposing commands.
  * Making this safe may also require the leaseholder (in rare cases) to propose
    empty commands to make sure a write which has been proposed but has not yet
    committed will fail the `LeaseAppliedIndex` / `MaxLeaseIndex` check if they
    do eventually commit. This would be to prevent a write from getting
    arbitrarily delayed between the leaseholder proposing the command and the
    command actually making it into the raft log. A long enough delay before the
    command makes it into the raft log could cause a violation of the
    `max_proposed_write_timestamp` promise, for example if the delay is longer
    than the range's configured `max_write_age`. Proposing empty commands would
    increase the raft log's index such that the delayed command would fail its
    `LeaseAppliedIndex` check, preventing it from applying with a timestamp
    older than `max_write_timestamp - max_write_age`.
* Followers can serve reads at times less than their `max_write_timestamp -
  max_write_age`.
  * This may mean the some followers can serve reads that others
    can't, but all such reads will be correct as long as the leaseholder doesn't
    break its promise about not proposing old writes.
  * Intents encountered by the followers can be resolved as necessary. This will
    add some additional latency to such reads, but maintains correctness. Any
    intents left by transactions that are still pending should have their
    transactions aborted at this point.
* If a follower receives a stale read request that it can't serve, it has to
  forward it to the leaseholder via a NotLeaseHolderError.
* To make all of this usable, the `DistSender` will have to be instrumented to
  decide which replica is closest, decide whether a follower may be able to
  handle the request, and forward such requests to the leaseholder if the
  follower can't.
* The cluster's `maxOffset` doesn't need to be considered. During steady-state
  operation under a single range lease, all timestamps for writes are getting
  verified by the same leaseholder. When the lease changes hands, the new
  leaseholder can base its new `max_proposed_write_timestamp` on when the
  previous lease expired, as described in more detail below.

There are still a couple big questions left.

As long as new writes are happening on the range, all the replicas will
naturally have their `max_write_timestamp` kept up-to-date by the timestamps on
the newly proposed writes. However, if no write has been proposed for a while,
the followers' `max_write_timestamp` will fall far behind the actual current
time, limiting the usefulness of follower reads in inactive ranges. This
suggests that we need periodic "heartbeats" of some sort to keep followers
up-to-date with respect to which timestamps are safe to read. There are two
options here:

1. Have the leaseholder periodically propose an empty command to raft with the
   current timestamp if no other writes have come through lately.
2. Use a higher-level mechanism, such as attaching a new field to raft
   heartbeats or piggybacking on quiescence to convey this information.

Option 2 is lower overhead but riskier, since there's no guarantee that the new
leaseholder after a lease change will know about the increased
`max_write_timestamp`, but it seems as though a new leaseholder could just be
extra cautious when it takes over the lease. The previous leaseholder couldn't
have proposed a write with a timestamp greater than its lease expiration, so new
leaseholders can start out by setting `max_proposed_write_timestamp` to the
previous lease's expiration timestamp.

The next bigger question is whether this extra work to keep
`max_write_timestamp` up to date should always be enabled by default. There are
a few options here:

1. Always enabled. Leaseholders always have to do the work of keeping
   `max_write_timestamp` up-to-date on followers.
2. Only enabled when a user explicitly sets a `max_write_age` via zone config.
3. Only enabled when a leaseholder has reason to believe that followers are
   receiving read requests at old timestamps.
4. A combination of 2 and 3.

Option 1 is a bit of a non-starter due to the extra work required, since we
shouldn't be slowing down users' clusters for features that they aren't using.
It also would prevent ranges from [quiescing](20160824_quiesce_ranges.md).
Option 2 is good because it gets explicit user consent, but bad because it means
that users have to know about the feature before they're able to benefit from
it. We'll at least want an option to configure `max_write_age`, though, since
there could be workloads out there with really long-running writes that work
today but wouldn't work with a small default `max_write_age`.  Heuristics aren't
fun, but option 3 seems best as long as we can reasonably decide when to start,
when to stop, and how frequently to update `max_write_timestamp` in the absence
of writes. Instrumenting the `DistSender` to indicate that a stale read was
rejected by a follower would be a good way of notifying the leaseholder to
start, but deciding when to stop may just be a manner of timing out every so
often.

There are more questions that could be asked about follower reads (e.g. defining
a new syntax for stale reads at a system-chosen timestamp), but those can be
deferred to a follow-up PR or a later extension to this one.

### Change feeds

Unlike for follower reads, change feeds / CDC need to care very dearly about
unresolved intents. A change feed can't publish a write to any listeners until
it knows that the write's transaction has committed, meaning that it publishes
new writes as the intents are resolved.

Follower reads can simply resolve an intent when it encounters one, whereas
change feeds need to make promises to clients in the form of "close
notifications" that no new writes will come in at an old timestamps. The
leaseholder can avoid proposing new writes at old timestamps using the same
`max_proposed_write_timestamp - max_write_age` approach as for follower reads,
but any existing intents on the range are also potential writes. An unresolved
intent could become a valid write that the leaseholder doesn't learn about until
any arbitrary later time unless it proactively tracks intents and resolves them.

Here's a summary of our thoughts:

* We can reuse the `max_write_timestamp` tracking from follower reads, but more
  logic is needed to ensure no old intents are left unresolved.
* We have to track unresolved intents somewhere - scanning the entire range to
  check for intents every time we want to send a close notification is going to
  be prohibitively expensive unless close notifications can be very infrequent.
  * Tracking intents  will still be expensive, so we should only do so for
    ranges that have an active change feed listening to them.
* Where should we track intents?
  * In-memory on all replicas: makes failover easy if a change feed needs to
    switch which one it's talking to (e.g. because a node went down), but will
    cost us a lot of memory usage. Benchmarking may demonstrate this is
    feasible, but until then it seems too expensive.
  * On-disk: we considered some linked list approaches where pointers are stored
    as part of each intent to track the intents in a range, but couldn't come up
    with a scheme that would be reasonably efficient to keep up-to-date.  If
    possible, this would get around the need to track things in memory, but
    doesn't seem very promising from an implementation perspective.
  * In-memory on only the replica the change feed is attached to: minimizes
    resources consumed (no disk I/O, just memory on one node), but failover to a
    different replica will require work to reconstruct the in-memory data
    structure. This appears to be the way to go (more detail below).
* On lease transfer, the change feeds RFC already proposes a form of ["resume
  tokens"](https://github.com/cockroachdb/cockroach/pull/16838/files#diff-6cc9e2a4b26d4ee01d624c7015fc5220R242).
  These are meant to allow a new leaseholder to resume the change feed by
  finding the corresponding point in its raft log and catching up by reading
  from there. We believe the same can be done for intent-tracking -- if the
  resume token also includes a reference to the point in the raft log
  corresponding to the last "close notification", we know that there are no
  intents remaining from before that point. The new leaseholder can scan on from
  that point to reconstruct the in-memory intent-tracking data structure.
  * In the case that the raft log has been truncated, a full range scan will be
    required, as is already the case for the original resume token idea.
* Given the above information, the replica serving the change feed (presumably
  the leaseholder, but I do think we could support followers as well) can emit a
  value as soon as its intent is resolved, and can emit a close notification for
  any time less than `max_write_timestamp - max_write_age` and earlier than the
  oldest remaining intent.
  * The replica serving the change feed also has the right to proactively
    resolve any unresolved intents older than `max_write_timestamp -
    max_write_age` in order to enable the sending of a close notification.
* Like for follower reads, we won't enable the requisite tracking of intents
  unless there is a change feed attached to the range. If there isn't, there is
  no reason to incur the cost of doing so.

Thus, the max-safe timestamp needed by change feeds is largely additive on top
of the max-safe timestamp used by follower reads, but with the optional
optimization of not needing to send `max_write_timestamp` updates to followers
if change feeds only attach to the leaseholder.

## Drawbacks

TODO: Initial version of this RFC is just for reaching consensus on rough ideas

## Rationale and Alternatives

### Use RocksDB time-bound iterators to find recent intents on-demand

One alternative to consider for the tracking of intents is to rely on RocksDB's
`TimeBoundIterator` to only scan over recently-written SSTables. This feature
allows you to provide a range of MVCC timestamps and only read through the
SSTables containing entries at those timestamps. This would allow us to just
maintain the timestamp of the oldest intent and periodically do a
`TimeBoundIterator` scan over all MVCC keys written after that timestamp,
updating what is considered the oldest intent appropriately. This would
alleviate the need for extra in-memory tracking and for any sort of coordination
required to update the in-memory data structure on writes.

The downside, though, is that it necessarily operates on all the ranges in a
store, even if change feeds are only active on a small subset of the ranges.
Thus, it makes the performance impact of change feeds scale with the writes on
the entire cluster rather than with the writes on the tables or ranges being
subscribed to, which could be problematic for many deployments. Tuning how
frequently we ran such scans would also be tricky, given the need to balance the
timeliness of close notifications with the cost of all the iteration.

### Replicate the TimestampCache through Raft

This alternative is only half serious, but it serves as a nice generalization of
"max safe timestamp" that provides more insight into the concept. Instead of
proposing dummy write commands to update the `max_write_timestamp` through Raft,
the leaseholder could instead periodically bump its `TimestampCache` low water
mark and send the entire structure through Raft. Followers could then maintain a
copy of this cache themselves and serve any read requests that they observe
where the timestamp is equal to or less than the minimum timestamp for all spans
in the request within the `TimestampCache`.

The benefit of this approach over replicating `max_write_timestamp` is that
follower reads would not always need to trail the current time by more than
`max_write_age`. If the `TimestampCache` is already updated for a read over a
certain span on the leaseholder, it is guaranteed that no writes will take place
under this timestamp within this span. This means that for certain spans of keys
that already have an updated timestamp cache value on the leaseholder, followers
could read locally at that updated time once the update is replicated through
Raft. This would allow them to read locally for timestamps much closer to the
present time.

We can equate "max safe timestamp" to the low water mark of the `TimestampCache`
in this alternative. The two ideas are analogous if we imagine that the low
water mark of the cache is set to `max_write_timestamp-max_write_age` and that
the cache maintains no timestamp intervals other than this low water mark.

This has an interesting extension, where followers could proactively request
`TimestampCache` updates, on-demand. They could then wait for the replicated
cache update so they could field reads for certain spans locally. While this
would still incur a round-trip, it could be used to read up-to-date information
from followers when it's apparent that the read will create a very large result
set and shipping this result over the network will be much more expensive than
shipping the bookkeeping required to allow a follower read. The origins of this
idea came from benefit 4 in the original comment of [this forum
post](https://forum.cockroachlabs.com/t/why-do-we-keep-read-commands-in-the-command-queue/360).

Or course, shipping the `TimestampCache` through Raft would probably be too
expensive to be feasible.

### Replicate the TimestampCache through Raft

This alternative is only half serious, but it serves as a nice generalization of
"max safe timestamp" that provides more insight into the concept. Instead of
proposing dummy write commands to update the `max_write_timestamp` through Raft,
the leaseholder could instead periodically bump its `TimestampCache` low water
mark and send all `TimestampCache` entries from its store that overlap with its
key span through Raft. Followers could then maintain a copy of this cache
themselves and serve any read requests that they receive where the timestamp is
equal to or less than the minimum timestamp for all spans in the request within
the `TimestampCache`.

The benefit of this approach over replicating `max_write_timestamp` is that
follower reads would not always need to trail the current time by more than
`max_write_age`. If the `TimestampCache` is already updated for a read over a
certain span on the leaseholder, it is guaranteed that no writes will take place
under this timestamp within this span. This means that for certain spans of keys
that already have an updated timestamp cache value on the leaseholder, followers
could read locally at that updated time once the update is replicated through
Raft. This would allow them to read locally for timestamps much closer to the
present time.

We can equate "max safe timestamp" to the low water mark of the `TimestampCache`
in this alternative. The two ideas are analogous if we imagine that the low
water mark of the cache is set to `max_write_timestamp-max_write_age` and that
the cache maintains no timestamp intervals other than this low water mark.

This has an interesting extension, where followers could proactively request
`TimestampCache` updates, on-demand. They could then wait for the replicated
cache update so they could field reads for certain spans locally. While this
would still incur a round-trip, it could be used to read up-to-date information
from followers when it's apparent that the read will create a very large result
set and shipping this result over the network will be much more expensive than
shipping the bookkeeping required to allow a follower read. The origins of this
idea came from benefit 4 in the original comment of [this forum
post](https://forum.cockroachlabs.com/t/why-do-we-keep-read-commands-in-the-command-queue/360).

Or course, shipping the `TimestampCache` through Raft would probably be too
expensive to be feasible.

## Unresolved questions

- We could use some eyes on the proposal/re-proposal question. Re-proposals got
  brought up as a major roadblock on the original follower read issue, but we
  aren't sure why the logic there can't just be changed to consider timestamps
  when deciding whether to re-propose. Also, the method of proposing empty
  commands to time out delayed proposals could benefit from review.
- The user interface for follower reads still needs to be thought out, which can
  either be done in a separate RFC or a follow-on here.
