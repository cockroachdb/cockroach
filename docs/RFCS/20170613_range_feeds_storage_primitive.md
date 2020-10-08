- Feature Name: RangeFeed: A Storage-Level Primitive for Change Feeds
- Status: draft
- Start Date: 2017-06-13
- Authors: Arjun Narayan, Tobias Schottdorf, Nathan VanBenschoten
- RFC PR: #16838
- Cockroach Issue: #9712, #6130

# Summary

This RFC proposes a mechanism for subscribing to changes to a set of key ranges
starting at an initial timestamp.

It is for use by Cockroach-internal higher-level systems such as distributed
SQL and change data capture. ["Change Data Capture (CDC)"][cdc] is a "sister RFC"
detailing these higher-level systems.

We propose to add a basic building block for range feeds: a command `RangeFeed`
served by `*Replica` which, given an HLC timestamp and a set of key spans
contained in the Replica returns a stream-based connection that

1. eagerly delivers updates that affect any of the given key spans, and
1. periodically delivers checkpoints, i.e. tuples `(timestamp, key_range)` where
   receiving `(ts1, [a,b))` guarantees that no future update affecting `[a,b)`
   will be sent for a timestamp less than `ts1` (see Semantics section below for
   details).

If the provided timestamp is too far in the past, or if the Replica can't serve
the RangeFeed, a descriptive error is returned which enables the caller to
retry.

The design overlaps with incremental Backup/Restore and in particular [follower
reads][followerreads], but also has relevance for [streaming SQL
results][streamsql].

The design touches on the higher-level primitives that abstract away the Range
level, but only enough to motivate the Replica-level primitive itself.

# Motivation

Many databases have various features for propagating database updates to
external sinks eagerly. Currently, however, the only way to get data out of
CockroachDB is via SQL `SELECT` queries, `./cockroach dump`, or Backups
(enterprise only). Furthermore, `SELECT` queries at the SQL layer do not support
incremental reads, so polling through SQL is inefficient for data that is not
strictly ordered. Anecdotally, range feeds are one of the more frequently
requested features for CockroachDB.

Our motivating use cases are:

- wait for updates on individual rows or small spans in a table. For example, if
  a branch is pushed while its diff is viewed on Github, a notification will pop
  up in the browser to alert the user that the diff they're viewing has changed.
  This kind of functionality should be easy to achieve when using CockroachDB.
  Individual developers often ask for this, and it's one of the RethinkDB/etcd
  features folks really like.
- stream updates to a table or database into an external system, for example
  Kafka, with at-least-once semantics. This is also known as Change Data Capture
  (CDC). This is something companies tend to ask about in the context of moving
  data into an analytics platform or other auxiliary systems (cold storage,
  legacy caches, or even another CockroachDB cluster).
- implement efficient incrementally updated materialized views. This is
  something we think everyone wants, but does not dare to ask.

The above use cases were key in informing the design of the basic building block
presented here: We

- initiate range feeds using a HLC timestamp since that is the right primitive
  for connecting the "initial state" (think `SELECT * FROM ...` or `INSERT ...
  RETURNING CURRENT_TIMESTAMP()`) to the stream of updates.
- chose to operate on a set of key ranges since that captures both collections
  of individual keys, sets of tables, or whole databases (CDC).
- require checkpoint notifications because often, a higher-level system needs to
  buffer updates until it is knows that older data won't change any more; a
  simple example is wanting to output updates in timestamp-sorted order. More
  generally, checkpoint notifications enable check pointing for the case in
  which a range feed disconnects (which would not be uncommon with large key
  spans).
- emit checkpoint notifications with attached key ranges since that is a natural
  consequence of the Range-based sharding in CockroachDB, and fine-grained
  information is always preferrable. When not necessary, the key range can be
  processed away by an intermediate stage that tracks the minimum checkpointed
  timestamp over all tracked key spans and emits that (with a global key range)
  whenever it changes.
- make the checkpoint notification threshold configurable via `ZoneConfig`s
  (though this is really a requirement we impose on [16593][followerreads]).
  Fast checkpoints (which allow consumers to operate more efficiently)
  correspond to disabling long transactions, which we must not impose globally.
- aim to serve range feeds from follower replicas (hence the connection to
  [16593][followerreads]).
- make the protocol efficient enough to operate on whole databases in practice.

Note that the consumers of this primitive are always going to be
CockroachDB-internal subsystems that consume raw data (similar to a stream of
`WriteBatch`). We propose here a design for the core layer primitive, and the
higher-level subsystems are out of scope.

# Guide-level explanation

This section is incomplete.

# Reference-level explanation

## Detailed design

### Types of events

The events emitted are simple:

- `StreamState`: lets the client know that initial catch-up
  has finished (which gives more predictable ordering semantics),
- `Value(key, value, timestamp)`: `key` was set to `value` at `timestamp`, and in particular
- `Value(key, <nil>, timestamp)`: `key` was deleted at `timestamp`, and
- `Checkpoint(startKey, endKey, timestamp)`: no more events will be emitted for
  `[startKey, endKey)` at timestamps below the event's. This event type will
  only be received after the `StreamState` notification, though that is an
  implementation detail the client does not need to know about.

### Stream semantics

In the presence of reconnections (which are expected), the system provides
at-least-once semantics: The caller must handle duplicates which can occur as it
reconnects to streams using the latest known checkpoint notification timestamp
("resolved timestamp") as its new base timestamp, as in the following example:

1. `ts=120` checkpoints
1. receive `key=x` changes to `a` at `ts=123`
1. disconnect; reconnect with base timestamp `ts=120`
1. receive `key=x` changes to `a` at `ts=123`.

A stream will never emit events for timestamps below the stream's base
timestamp.

In what follows, we discuss a single open stream.

#### Value() timestamps and SteadyState notification

Before the `SteadyState` notification, clients may not make any assumptions
about the event timestamp ordering (because the catch-up and regular
notifications are interleaved in the stream). However, once `SteadyState` has
been achieved, updates for *individual* keys arrive in ascending timestamp
order:

  1. receive `key=x` changes to `a` at `ts=123`
  1. receive `key=x` changes to `b` at `ts=119`
  1. receive `key=y` changes to `0` at `ts=117`
  1. receive `SteadyState`
  1. receive `key=x` changes to `c` at `ts=124`
  1. receive `key=y` changes to `1` at `ts=118`
  1. receive `key=x` changes to `3` at `ts=125`
  1. receive `key=y` changes to `2` at `ts=124`

#### Checkpoint() notification

An event `Checkpoint(k1, k2, ts)` represents the promise that no more `Value()`
notifications with timestamps less than `ts` are going to be emitted, i.e. the
following is illegal:

1. `[a, z)` checkpoints at `ts=100`
1. `key=x` gets deleted at `ts=90`

For simplicity, the stream does not commit to strictly ascending checkpoint
notifications. That is, though it wouldn't be expected in practice, callers
should ignore checkpoints that have no effect (due to having received a
"newer" checkpoint earlier). Similarly, checkpoint notifications may be
received for timestamps below the base timestamp.

Furthermore, the system guarantees that any `Value()` events is eventually
followed by a `Checkpoint()` event.

#### Wire format

The exact format in which events are reported are TBD. RangeFeed processing will
require that Raft log be decoded and sanitized in order to properly track
intents, filter based on key ranges, and remove range-local updates. Because we
will have already incurred this decoding and filtering cost, it is likely that
the wire format will be a protobuf union type that closely resembles the events
presented in [Types of events](#types-of-events).

### Replica-level

Replicas (whether they're lease holder or not) accept a top-level `RangeFeed`
RPC which contains a base HLC timestamp and (for the sake of presentation) one
key range for which updates are to be delivered. The `RangeFeed` command first
grabs `raftMu`, opens a RocksDB snapshot, registers itself with the raft
processing goroutine and releases `raftMu` again. 

By registering itself, it receives notifications for all future updates
which apply on the Replica, and sends them on the stream to the caller
(after suitably sanitizing to account only for the updates which are relevant
to the span the caller has supplied). This means that the `RangeFeed` is driven
off the Raft log itself. This poses a few challenges, described in
[The Resolved Timestamp](#resolved-timestamps) and
[Decoding WriteBatches](#decoding-writebatches).

The remaining difficulty is that additionally, we must retrieve all updates made
at or after the given base HLC timestamp, and this is what the engine snapshot
is for. We invoke a new MVCC operation (specified below) that, given a base
timestamp and a set of key ranges, synthesizes RangeFeed notifications from the
snapshot (which is possible assuming that the base timestamp is a valid read
timestamp, i.e. does not violate the GCThreshold or the like).

Once these synthesized events have been fed to the client, we begin emitting
checkpoint notifications. Checkpoints are per-Range, so all the key ranges
(which are contained in the range) will be affected equally.

When the range splits or merges, of if the Replica gets removed, the stream
terminates in an orderly fashion. The caller (who has knowledge of Replica
distribution) will retry accordingly. For example, when the range splits, the
caller will open two individual RangeFeed streams to Replicas on both sides of
the post-split Ranges, using the highest resolved timestamp it received before
the stream disconnected. If the Replica gets removed, it will simply reconnect
to another Replica, again using its most recently received resolved timestamp.
Duplication of values streamed to clients after its reconnect can be reduced by
strategically checkpointing just before the stream gets closed.

Initially, streams may also disconnect if they find themselves unable to keep up
with write traffic on the Range. Later, we can consider triggering an early
split and/or adding backpressure, but this is out of scope for now.

#### Resolved Timestamps

The timestamp provided by a `Checkpoint()` notification is called a "resolved
timestamp". As discussed above, the receipt of one of these checkpoints with an
associated resolved timestamp indicates to a downstream receiver that all
subsequent `Value()` notifications will have timestamps strictly greater than
the resolved timestamp.

Because the `RangeFeed` is driven off the Raft log, we have an opportunity to
reuse the "closed timestamp" proposed in the [follower reads RFC][followerreads],
which is incremented during Raft log application, as a basis for this "resolved
timestamp". In fact, the semantics of the resolved timestamp line up very
closely with those of the closed timestamp. However, they are not the same.
Fundamentally, the closed timestamp restricts the state of a Range such that no
"visible" data mutations are permitted at earlier timestamps. On the other hand,
resolved timestamps restrict the state of a RangeFeed such that no `Value()`
notifications are permitted at earlier timestamps. The key difference here is
that data mutations are allowed beneath a closed timestamp as long as they are
not externally "visible". This is not true of the resolved timestamp because the
`RangeFeed` is driven directly off the state of a Range through its Raft log
updates. As such, all external changes to a Range beneath a given timestamp,
"visible" or not, must be made to the Range before its resolved timestamp can be
advanced to that timestamp.

This distinction becomes interesting when considering unresolved transaction
intents because these intents can undergo changes at timestamps earlier than a
Range's closed timestamp. This is possible because intent resolution of an
intent which is part of a committed or aborted transaction does not change the
visible state of a range. In effect, this means that a range can "close"
timestamp t2 before resolving an intent for a transaction at timestamp t1, where
t1 < t2. This phenomena is explored in the [follower reads RFC's][followerreads]
`Timestamp forwarding and intents` section. 

Because we intend to drive `Value()` notifications for transactional changes off
of intent resolution of individual keys, this effect prevents us from using a
Range's closed timestamp as its resolved timestamp directly. Instead, a Range's
resolved timestamp will always be the minimum of its closed timestamp and the
`Timestamp` of the earliest unresolved intent that exists within its key bounds.
This poses two additional concerns on top of the need for a functioning closed
timestamp: tracking unresolved intents and proactively pushing or resolving
unresolved intents to ensure that the resolved timestamp continues to make
forward progress.

##### Tracking Unresolved Intents (UnresolvedIntentQueue)

Each replica (follower or leaseholder) with at least one active RangeFeed will
track all unresolved intents that exist within its key bounds. It will do so by
maintaining a priority queue that tracks all transactions with unresolved
intents on the Range called the `UnresolvedIntentQueue`. Each item in the queue
will contain the following information:
- txnID
- txn key
- max Timestamp seen for this txn (min Timestamp that it can commit at)
- refcount of unresolved intents for txn on this range

Each item in the queue will maintain a priority based on its Timestamp, such
that the item with the earliest timestamp will rise to the head of the queue.

Because the queue contains a reference count of each unresolved intent for the
txn on the range, its size will be O(active txns on range) instead of O(active
intents on range). This also means that the queue does not need to track the
specific keys of each intent. This is critical as it should prevent the queue's
memory footprint from growing large enough to need to spill to disk.

Still, it's conceivable that the queue could grow large enough that it needs to
spill to disk. In this case, we'll either use a temp storage engine to house the
queue or break the rangefeed and force consumers to reconnect. The
representation should be fairly straightforward, and will be organized into a
two-level structure. Primarily, we'll maintain `<timestamp/txnID>` keys pointing
to values with the associated `txn key` and `intent refcount`. This will allow
us to use the RocksDB engine's internal sorting to quickly find the oldest
transaction. On the side, we'll also need `<txnID>` keys pointing to `timestamp`
values. This will allow us to forward a transaction's timestamp given its ID and
a new timestamp. It's unclear how critical this disk spilling will be. The first
iteration of the `UnresolvedIntentQueue` will just throw an error that shuts
down the RangeFeed if it grows too large.

The queue will be initially built using the RocksDB snapshot captured under lock
when the first RangeFeed connected. It will then be incrementally maintained
(txns added and deleted, refcount adjusted, Timestamp forwarded) as Raft log
entries indicate that intents are written and resolved. Care will be needed if
we allow Raft entries to be processed concurrently with the initial scan. For
instance, we'll need to allow refcounts to go negative at least until the catch
up scan has completed.

Whenever an intent is resolved, its transaction's corresponding unresolved
intent reference count is decremented. When a transaction's ref count reaches 0,
it is removed from the queue. When the queue's highest priority txn (the txn
with the earliest Timestamp) is removed or has its timestamp forwarded, the
replica's `resolvedTS` is advanced to `min(closedTS, newTopTxn.Timestamp)`. If
this causes the `resolvedTS` to move, it will trigger a `Checkpoint()`
notification.

When a range splits or merges, the `UnresolvedIntentQueue` will be cleared at
the same time that all `RangeFeeds` are terminated. It will be re-built when a
RangeFeed connects to the new range. Importantly, it should be relatively cheap
to re-build using a `TimeBoundIterator` because this iterator can start
iterating from the previously established resolved timestamp.

This is probably a good enough reason to periodically persist the resolved
timestamp in a range-id local key on the range itself.

##### Ensuring Progress

On its own, this queue would not be enough to ensure that the resolved timestamp
continues to make forward progress. This is because transactions may be
abandoned before or after committing but before cleaning up all of their
intents. It's also possible that a transaction is not abandoned but is simply
waiting for a significant amount of time while continuing to heartbeat its
transaction record. Either way, if no other transaction stumbles upon the
intents, they will never be pushed or resolved. This is a problem because the
oldest unresolved intent on a range will prevent the range's resolved timestamp
from moving forward. To ensure that these old intents don't hold up the resolved
timestamp, we need some forcing function to push the intents to a higher
timestamp. 

Luckily, we already know how to do this using `PushTxnRequest`s. A new policy
will be introduced to push any transactions that have a sufficiently old
timestamp in the `UnresolvedIntentQueue` using a high-priority `PushTxnRequest`.
It is likely that this age threshold will be similar to the follower reads
closed timestamp interval because no transactions will be able to commit before
this interval anyway. Whatever we decide, we will want to jitter this slightly
to prevent all replicas in a Range from attempting to push abandoned or
long-running transactions at the same time.

The push will tell us one of three things:
- the txn is still PENDING but has been pushed to a larger timestamp. We can
  forward the timestamp in the `UnresolvedIntentQueue` and update the resolved
  timestamp accordingly.
- the txn is ABORTED. We can remove the transaction's record from the
  `UnresolvedIntentQueue` and update the resolved timestamp accordingly. Even
  though the intents won't be cleaned up, we know that they will never be
  committed.
- the txn is COMMITTED and the txn record holds the authoritative list of
  all of its intents. We can resolve whichever are present on our range,
  which will in turn allow us to remove the transaction's record from the
  `UnresolvedIntentQueue` after we see the intent resolution pop out of the
  raft log.

Note that none of this requires that we maintain a record of the intents for a
given transaction in the `UnresolvedIntentQueue`. These intents will already be
present in a COMMITTED transaction record and we can ignore them for an ABORTED
transaction (where the transaction record may be missing intents). However, it
might be best to attempt to resolve sufficiently old intents when we run into
them while constructing the `UnresolvedIntentQueue`, so that we don't continue
to run into them later. This will be less of a problem if we persist the
resolved timestamp and use a `TimeBoundIterator` to skip over the abandoned
intents when re-building the `UnresolvedIntentQueue`.

#### The Difficulty With Decoding WriteBatches

The heart of this proposal originally relied on the ability to decode
`WriteBatch`es present in Raft entries and accurately determine their intended
effect on values, intents, and other MVCC-related state. To do this, it was
important to understand what entries a WriteBatch will contain for a given MVCC
operations. Below is a flow diagram of all mutating MVCC operations as of commit
396ea7e.

```
- MVCCPut
    - inline (never transactional)
        + write meta key w/ inline value
    - not inline
        - transactional
            - no
                + write new version
            - yes
                - intent exists with older timestamp
                    + clear old version
                    + write meta
                    + write new version
                - else
                    + write meta
                    + write new version
- MVCCDelete
    - inline (never transactional)
        + clear meta key
    - not inline
        + same as MVCCPut but with empty value and Deleted=true in meta
- MVCCDeleteRange
    + calls MVCCDelete for each key in range
- MVCCMerge
    + write merge record (never transactional)
- MVCCResolveWriteIntent
    - commit
        + clear meta
        - if timestamp of intent is incorrect
            + clear old version key
            + write new version key
    - abort
        + clear version key
        + clear meta key
    - push
        + write meta with larger timestamp
        + clear old version key
        + write new version key
- MVCCGarbageCollect
    + clear each meta key
    + clear each version
```

The process of decoding `WriteBatch`es will consist of taking a list of RocksDB
batch entries and reverse engineering the collective higher level operations it
is performing. For instance, a `WriteBatch` that includes a write to a meta and
version key for the same logical key will be interpreted as an intent write.
Likewise, the deletion of a meta key will be interpreted as a successful intent
resolution.

This raises some serious concerns. To start, the diagram shows how complicated
this decoding process will be. Not only are there a large number of state
transitions here that we'll have to pattern match against, but its not clear
whether this decoding will even be un-ambiguous without additional engine reads.
That said, the process of decoding the WriteBatch itself is not insurmountable
by any means. The bigger concern is that this is creating a very substantial
below-Raft dependency on the implementation details of our above-Raft
MVCC-layer. We're going to have to be very careful about making any changes to
MVCC once we introduce this dependency, and there may be serious consequences to
this. Issues like these were what prompted the proposer-evaluated kv refactor,
and they should not be taken lightly.

#### Logical MVCC Operations

Because of this, we instead propose an alternative to decoding the `WriteBatch`
in a Raft command directly. Instead, we propose the introduction of a logical
list of higher-level MVCC operations to each Raft command. These higher-level
operations will not be bound to the semantics of the physical operations
described in the `WriteBatch`, and as such will not restrict changes to them
in the future. Instead, the operations will describe the changes of the batch
at the MVCC level. This avoids the previous issue and allows for much easier
decoding of the operations downstream of Raft.

To start, the following field (alternate name "LogicalOps") will be added to the
`RaftCommand` proto message:

```
repeated MVCCLogicalOp mvcc_ops;
```

The definition of `MVCCLogicalOp` will be something like:
```
message Bytes {
    option (gogoproto.onlyone) = true;

    bytes inline = 1;

    message Pointer {
        int32 offset = 1;
        int32 len = 2;
    }
    Pointer pointer = 2;
}

message MVCCWriteValueOp {
    bytes key = 1;
    util.hlc.Timestamp timestamp = 2;
    bytes value = 3;
}

message MVCCWriteIntentOp {
    bytes txn_id = 1;
    bytes txn_key = 2;
    util.hlc.Timestamp timestamp = 3;
}

message MVCCUpdateIntentOp {
    bytes txn_id = 1;
    util.hlc.Timestamp timestamp = 2;
}

message MVCCCommitIntentOp {
    bytes txn_id = 1;
    bytes key = 2;
    util.hlc.Timestamp timestamp = 3;
    bytes value = 4;
}

message MVCCAbortIntentOp {
    bytes txn_id = 1;
}

message MVCCLogicalOp {
    option (gogoproto.onlyone) = true;

    MVCCWriteValueOp   write_value   = 1;
    MVCCWriteIntentOp  write_intent  = 2;
    MVCCUpdateIntentOp update_intent = 3;
    MVCCCommitIntentOp commit_intent = 4;
    MVCCAbortIntentOp  abort_intent  = 5;
}
```

Notably, all byte slice values will optionally point into the WriteBatch itself,
which will help limit the write amplification caused by replicating both physical
and logical operations. This optimization can be introduced gradually. We could
also explore compression techniques, which could result in similar deduplication.

Above Raft, the log of MVCCOps will be constructed side-by-side with the
`WriteBatch` as each Request is processed within a BatchRequest.

Once the Raft entry is applied below Raft and its MVCC ops are split out, they
will then each be run through the following logic:
```
for op in mvccOps {
    match op {
        WriteValue(key, ts, val) => SendValueToMatchingFeeds(key, ts, val),
        WriteIntent(txnID, ts)   => {
            UnresolvedIntentQueue[txnID].refcount++
            UnresolvedIntentQueue[txnID].Timestamp.Forward(ts)
        }, 
        UpdateIntent(txnID, ts) => UnresolvedIntentQueue[txnID].Timestamp.Forward(ts),
        CommitIntent(txnID, key, ts) => {
            UnresolvedIntentQueue[txnID].refcount--
            // It's unfortunate that we'll need to perform an engine lookup
            // for this, but it's not a huge deal. The value should almost
            // certainly be in RocksDB's memtable or block cache, so there's
            // not much of a reason for any extra layer of caching.
            SendValueToMatchingFeeds(key, MVCCGet(key, ts), ts)
        },
        AbortIntent    => UnresolvedIntentQueue[txnID].refcount--,
    }
}
```

### MVCC-level

We add an MVCC command that, given a snapshot, a key span and a base timestamp,
synthesizes events for all newer-than-base-timestamp versions of keys contained
in the span.

Note that data is laid out in timestamp-descending per-key order, which is the
opposite of the naively expected order. However, during the catch-up phase, the
stream may emit events in any order, and so there is no problem. A reverse scan
could be used instead and would result in forward order, albeit perhaps at a
slight performance cost and little added benefit.

Note: There is significant overlap with `NewMVCCIncrementalIterator`, which is
used in incremental Backup/Restore.

### DistSender-level

The design is hand-wavy in this section because it overlaps with [follower
reads][followerreads] significantly. When reads can be served from followers,
`DistSender` or the distributed SQL framework need to be able to leverage this
fact. That means that they need to be able to (mostly correctly) guess which, if
any, follower Replica is able to serve reads at a given timestamp. Reads can be
served from a Replica if it has been notified of that fact, i.e. if a higher
checkpoint exists.

Assuming such a mechanism in place, whether in DistSender or distributed SQL, it
should be straightforward to add an entity which abstracts away the Range level.
For example, given the key span of a whole table, that entity splits it along
range boundaries, and opens individual `RangeFeed` streams to suitable members
of each range. When individual streams disconnect (for instance due to a split)
new streams are initiated (using the last known closed timestamp for the lost
stream).

### Implementation suggestions

There is a straightforward path to implement small bits and pieces of this
functionality without embarking on an overwhelming endeavour all at once:

First, implement the basic `RangeFeed` command, but without the base timestamp
or checkpoints. That is, once registered with `Raft`, updates are streamed, but
there is no information on which updates were missed and what timestamps to
expect. In turn, the MVCC work is not necessary yet, and neither are [follower
reads][followerreads].

Next, add an experimental `./cockroach debug rangefeed <key>` command which
launches a single `RangeFeed` request through `DistSender`. It immediately
prints results to the command line without buffering and simply retries the
`RangeFeed` command until the client disconnects. It may thus both produce
duplicates and miss updates.

This is a toy implementation that already makes for a good demo and allows a
similar toy implementation that uses [streaming SQL][streamsql] once it's
available to explore the SQL API associated to this feature.

A logical next step is adding the MVCC work to also emit the events from the
snapshot. That with some simple buffering at the consumer gives at-least-once
delivery.

Once follower reads are available, it should be straightforward to send
checkpoints from `RangeFeed`.

At that point, core-level work should be nearly complete and the focus shifts to
implementing distributed SQL processors which provide real changefeeds by
implementing the routing layer, buffering based on checkpoints, and to expose
that through SQL in a full-fledged API. This should allow watching simple
`SELECT` statements (anything that boils down to simple table-readers with not
too much aggregation going on), and then, of course, materialized views.

## Concerns

### Performance

Performance concerns exist mostly in two areas: keeping the follower reads
active (i.e. proactively closing out timestamps) and the MVCC scan to recover
the base timestamp. There are also concerns about the memory and CPU costs
associated to having many (smaller) watchers.

We defer discussion of the former into its own RFC. The concern here is that
when one is receiving events from, say, a giant database which is mostly
inactive, we don't want to force continuous Range activity  even if there aren't
any writes on almost all of the Ranges. Naively, this is necessary to be able to
checkpoint timestamps.

The second concern is the case in which there is a high rate of `RangeFeed`
requests with a base timestamp and large key ranges, so that a lot of work is
spent on scanning data from snapshots which is not relevant to the feed. This
shouldn't be an issue for small (think single-key) watchers, but a large feed in
a busy reconnect loop could inflict serious damage to the system, especially if
it uses a low base timestamp (we may want to impose limits here) and thus has to
transfer significant amounts of data before switching to streaming mode.

Change Data Capture-type change feeds are likely to be singletons and
long-lived. Thus, it's possible that all that's needed here are good diagnostic
tools and the option to avoid the snapshot catch-up operation (when missing a
few updates is OK).

### Ranged updates

Note that the existing single-key events immediately make the notification for,
say, `DeleteRange(a,z)` very expensive since that would have to report each
individual deletion. This is similar to the problem of large Raft proposals due
to proposer-evaluated KV for these operations.

Pending the [Revert RFC][revertrfc], we may be able to track range deletions
more efficiently as well, but a straightforward general approach is to attach to
such proposals enough information about the original batch to synthesize a
ranged event downstream of Raft.

## Drawbacks

This section is incomplete.

## Rationale and Alternatives

This design aims at killing all birds with one stone: short-lived watchers,
materialized views, change data capture. It's worth discussing whether there are
specialized solutions for, say, CDC, that are strictly superior and worth having
two separate subsystems, or that may even replace this proposed one.

## Unresolved Questions

We may have to group emitted events by transaction ID. This is a tough problem
to solve as the transaction ID is not stored on disk and changing that is too
expensive.

### Licensing

We will have to figure out what's CCL and what's OSS. Intuitively CDC sounds
like it could be an enterprise feature and single-column watches should be OSS.
However, there's a lot in between, and the primitive presented here is shared
between all of them.


[followerreads]: https://github.com/cockroachdb/cockroach/pull/26362
[revertrfc]: https://github.com/cockroachdb/cockroach/pull/16294
[streamsql]: https://github.com/cockroachdb/cockroach/pull/16626
[cdc]: https://github.com/cockroachdb/cockroach/pull/25229
