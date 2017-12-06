- Feature Name: Storage-Level Primitive for Change Feeds
- Status: draft
- Start Date: 2017-06-13
- Authors: Arjun Narayan and Tobias Schottdorf
- RFC PR: #16838
- Cockroach Issue: #9712, #6130

# Summary

This RFC proposes a mechanism for subscribing to changes to a set of key ranges
starting at an initial timestamp.

It is for use by Cockroach-internal higher-level systems such as distributed
SQL, change data capture, or a Kafka producer endpoint. A "sister RFC"
detailing these higher-level systems will be prepared by @arjunravinarayan.

We propose to add a basic building block for change feeds: a command
`ChangeFeed` served by `*Replica` which, given a (sufficiently recent) HLC
timestamp and a set of key spans contained in the Replica returns a stream-based
connection that

1. eagerly delivers updates that affect any of the given key spans, and
1. periodically delivers checkpoints, i.e. tuples `(timestamp, key_range)` where
   receiving `(ts1, [a,b))` guarantees that no future update affecting `[a,b)`
   will be sent for a timestamp less than `ts1` (see Semantics section below for
   details).

If the provided timestamp is too far in the past, or if the Replica can't serve
the ChangeFeed, a descriptive error is returned which enables the caller to
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
strictly ordered. Anecdotally, change feeds are one of the more frequently
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

- initiate change feeds using a HLC timestamp since that is the right primitive
  for connecting the "initial state" (think `SELECT * FROM ...` or `INSERT ...
  RETURNING CURRENT_TIMESTAMP()`) to the stream of updates.
- chose to operate on a set of key ranges since that captures both collections
  of individual keys, sets of tables, or whole databases (CDC).
- require checkpoint notifications because often, a higher-level system needs to
  buffer updates until is knows that older data won't change any more; a simple
  example is wanting to output updates in timestamp-sorted order. More
  generally, checkpoint notifications enable check pointing for the case in which a
  change feed disconnects (which would not be uncommon with large key spans).
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
- aim to serve change feeds from follower replicas (hence the connection to
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
as its new base timestamp, as in the following example:

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
notifications with timestamps less than or equal to `ts` are going to be
emitted, i.e. the following is illegal:

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

The exact format in which events are reported are TBD. Throughout this document,
we assume that we are passing along `WriteBatch`es for change events, though
that is unlikely to be the final format: Typical consumers will live in the SQL
subsystem.

### Replica-level

Replicas (whether they're lease holder or not) accept a top-level `ChangeFeed`
RPC which contains a base HLC timestamp and (for the sake of presentation) one
key range for which updates are to be delivered. The `ChangeFeed` command first
grabs `raftMu`, opens a RocksDB snapshot, registers itself with the raft
processing goroutine and releases `raftMu` again. By registering itself, it
receives all future `WriteBatch`es which apply on the Replica, and sends them on
the stream to the caller (after suitably sanitizing to account only for the
updates which are relevant to the span the caller has supplied).

The remaining difficulty is that additionally, we must retrieve all updates made
at or after the given base HLC timestamp, and this is what the engine snapshot
is for. We invoke a new MVCC operation (specified below) that, given a base
timestamp and a set of key ranges, synthesizes ChangeFeed notifications from the
snapshot (which is possible assuming that the base timestamp is a valid read
timestamp, i.e. does not violate the GCThreshold or the like).

Once these synthesized events have been fed to the client, we begin emitting
checkpoint notifications. We assume that these notifications are driven by
[follower reads][followerreads] and can be observed periodically, so that we are
really only relaying them to the stream. Checkpoints are per-Range, so all the
key ranges (which are contained in the range) will be affected equally.

When the range splits or merges, of if the Replica gets removed, the stream
terminates in an orderly fashion. The caller (who has knowledge of Replica
distribution) will retry accordingly. For example, when the range splits, the
caller will open two individual ChangeFeed streams to Replicas on both sides of
the post-split Ranges, using the highest checkpoint notification timestamp it
received before the stream disconnected. If the Replica gets removed, it will
simply reconnect to another Replica, again using its most recently received
checkpoint. Duplication of values streamed to clients after its reconnect can be
reduced by strategically checkpointing just before the stream gets closed.

Initially, streams may also disconnect if they find themselves unable to keep up
with write traffic on the Range. Later, we can consider triggering an early
split and/or adding backpressure, but this is out of scope for now.

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
range boundaries, and opens individual `ChangeFeed` streams to suitable members
of each range. When individual streams disconnect (for instance due to a split)
new streams are initiated (using the last known closed timestamp for the lost
stream).

### Implementation suggestions

There is a straightforward path to implement small bits and pieces of this
functionality without embarking on an overwhelming endeavour all at once:

First, implement the basic `ChangeFeed` command, but without the base timestamp
or checkpoints. That is, once registered with `Raft`, updates are streamed, but
there is no information on which updates were missed and what timestamps to
expect. In turn, the MVCC work is not necessary yet, and neither are [follower
reads][followerreads].

Next, add an experimental `./cockroach debug changefeed <key>` command which
launches a single `ChangeFeed` request through `DistSender`. It immediately
prints results to the command line without buffering and simply retries the
`ChangeFeed` command until the client disconnects. It may thus both produce
duplicates and miss updates.

This is a toy implementation that already makes for a good demo and allows a
similar toy implementation that uses [streaming SQL][streamsql] once it's
available to explore the SQL API associated to this feature.

A logical next step is adding the MVCC work to also emit the events from the
snapshot. That with some simple buffering at the consumer gives at-least-once
delivery.

Once follower reads are available, it should be straightforward to send
checkpoints from `ChangeFeed`.

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

The second concern is the case in which there is a high rate of `ChangeFeed`
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


[followerreads]: https://github.com/cockroachdb/cockroach/issues/16593
[revertrfc]: https://github.com/cockroachdb/cockroach/pull/16294
[streamsql]: https://github.com/cockroachdb/cockroach/pull/16626
