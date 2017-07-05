- Feature Name: Change Data Capture
- Status: draft
- Start Date: 2017-08-07
- Author: Arjun Narayan
- RFC PR: #17535
- Cockroach Issue: #9712, #6130, #16838

# Summary

This RFC proposes a distributed architecture for Change Data Capture
(CDC) in CockroachDB, as an Apache Kafka publisher, providing both
at-least-once or at-most-once delivery semantics (depending on user
choice). This RFC addresses several concerns:

1) The semantics of the client-facing CDC primitive.

2) its implementation as a distributed system building upon the core
single-range `ChangeFeed` primitive described in #16838. ChangeFeeds
are a replica-level system, watching a single contiguous keyspan from
a single key range, and are not fault tolerant. They provide at most
once delivery semantics.

On top of this primitive, this RFC builds a fault-tolerant,
distributed primitive for Change Data Capture, at the level of entire
tables, or databases, which we call `ChangeAggregator`. The main
concerns in designing the `ChangeAggregator` are:

3) aggregating individual `ChangeFeed` streams from many ranges,
surviving various chaos events such as leadership changes, range
rebalancing, and lagging replicas.

4) Supporting resumability and cancellability of CDC feeds

# Motivation

See the motivations section in #16838 for a general motivation for
this feature.

# Background

There are, broadly, two ways clients want to capture changes: by
transaction or by row. The former streams transactions that touched a
watched-set (i.e. a table or a database), the second streams
row-by-row updates of the watched-set. The former has the advantage of
clumping together related changes, the latter makes filtering on
specific rows easier. We can support both with a common internal
format:

```
message CDC_message {
   HLC_timestamp timestamp
   UUID transaction_id
   Database_row old_row
   Database_row new_row
}
```

An insertion has an empty `old_row` field, a deletion has an empty
`new_row` field, and an update has both fields non-empty. This format
is strongly influenced by [the RethinkDB change
feed](https://rethinkdb.com/docs/changefeeds/ruby/#basic-usage)
design.

We can also stream changes by transaction as well, although we focus
on the row-by-row streaming paradigm in this document without loss of
generality: If we assume that we will build a CDC that streams these
row-by-row updates in `HLC_timestamp` order, we can simply buffer
messages and batch them by `transaction_id` order to provide the
alternate design.

Finally, we can also stream transactions _out of order_, at the
consumer's request.

# Desiderata

As CDC is an XL project, we first outline some desiderata, and then
scope these into various iterations. Do note that many of these are
not in scope for 1.2, but we do want to at least think about them here
so that we do not build a solution that is not eventually extensible
to the ideal change data capture solution.

* A CDC feed should be registerable against all writes to a single
  database or a single table. The change feed should persist across
  chaos, rolling cluster upgrades, and large tables/databases split
  across many ranges, delivering at-least-once or at-most-once message
  semantics, at the users choice. The change feed should be
  registerable as an Apache Kafka publisher.

* An important consideration in implementing change feeds is
  preserving the ordering semantics provided by MVCC timestamps: we
  wish for the feed as a whole to maintain the same ordering as
  transaction timestamp ordering.

* A change feed should explicitly flag if it can no longer provide
  exactly-once delivery mechanics. This is as close to exactly-once
  delivery as we can get: "exactly once or explicit error
  state". Optionally (at the clients choice) a change feed can
  continue, but we want to be explicit about when we degrade into that
  guarantee.

* It should be possible to create change feeds transactionally with
  other read statements: for instance, a common use case is to perform
  a full table read along with registering a change feed for all
  subsequent changes to that table: this way, the reader/consumer can
  build some initial mutable state from the initial read, and then
  mutate their state machine with each change notification, and be
  sure that they have not missed any message in between the read and
  the first message received on the feed, nor will they receive
  changes that are already accounted for in that `SELECT`.

* A change feed should gracefully partition when the rate of changes
  are too large for a single processor to handle into multiple Kafka
  "topics".

* Finally, a design for change feeds should be forward compatible with
  feeding changes _into_ incrementally updated materialized views
  using the timely+differential dataflow design.

Supporting filters and more complex transformations on the result of
Change Data Capture is explicitly out of scope. The intention is that
those transformations will be provided by incrementally updated
materialized views, which will eventually also reuse the CDC
infrastructure to provide feeds _out_ of the materialized views.

This would be efficient even in the case of very sparse filters, as
the physical planning process can place a materialized view filter
processor on the same machine as the change feed aggregator,
minimizing the amount of data that is streamed across machines.

# Document Overview

We begin by covering some building blocks that we will use. Then we
consider some [challenges](#challenges). We then cover a
(#detailed-design). In the end, we look at some [background and
related work](#background), followed by some appendices that cover
known topics but that are useful material for readers unfamiliar with
the details of some parts of CockroachDB.

# Building blocks

## Replica `ChangeFeed`

We assume that there exists a replica level `ChangeFeed` primitive,
which works as follows: we "open" a long-lived ChangeFeed to a
leaseholder replica by providing it a keyspan that is a subset of its
keyspace and a starting HLC timestamp that is newer than the current
Timestamp minus the MVCC GC duration.

This primitive then streams us:

* row-by-row changes, along with the HLC timestamp and transaction
  UUID of the change:
    ValueChange<RangeID, key, newValue, timestamp, UUID>

* periodic `checkpoint notifications` parameterized by an HLC
  timestamp `t`, which are a promise that all updates for this watched
  keyspace `<=t` have been sent (i.e. the min-safe timestamp watermark
  has passed this HLC timestamp):
    Checkpoint<startKey, endKey, timestamp>

If we are not consuming our changes fast enough, or otherwise are
clogging up the GRPC stream, the `ChangeFeed` may terminate the
feed. The `ChangeFeed` is also terminated if the replica loses the
lease, gets relocated, or is otherwise interrupted in any fashion.

## Minimum safe timestamp watermarks

Replicas need to be able to "close" a timestamp watermark,
guaranteeing that forever after, no write event will happen at a
timestamp below that watermark.  Otherwise `ChangeFeeds` from multiple
replicas cannot be ordered: a given transaction at `T_1` might be
preceded by some other transaction at an earlier transaction
timestamp, but which only gets committed later.

## `ChangeAggregator`

The key function of the ChangeAggregator is to take individual
`ChangeFeed` streams from replicas, and aggregate them into a single
stream of change events on a larger watched set, such as a table or
database. This is required because naively just interleaving streams
from various replicas would result in changes streaming out of
transaction timestamp order.

I propose that we write a `DistSQL` processor named
`ChangeAggregator`, which calls the `DistSQL` span resolver, finds the
ranges that it is watching, and sets up `ChangeFeed`s on each of
them. It then receives the stream of changes, buffering them in sorted
`HLC_timestamp` order. It also receives watermark updates from each
`ChangeFeed`. It only relays a message at time `T_1` out when every
replica has raised the watermarked past `T_1`.

The `ChangeAggregator` requires some message from each `ChangeFeed` at
regular intervals (either a write update, a watermark raise, or a
heartbeat). Otherwise it presumes that `ChangeFeed` dead, and attempts
to recreate it. Since a `ChangeAggregator` requires unanimous
watermark raises, it cannot proceed if a single `ChangeFeed` has
failed. This might create large buffers, so a `ChangeAggregator` will
use temporary storage to store its buffer of messages (a facility we
already have in DistSQL).

## SpanResolver

We also rely on the DistSQL SpanResolver to find us the leaseholder
for a keyspan, and to divide up a large keyspan into many smaller
spans, giving us `NodeID`s for each span.

## System.jobs table and Job Resumability

We use the System.jobs table to keep track of whether the
`ChangeAggregator` is alive. This part is Work in progress, as it is
not clear to me how much work is required for the `ChangeAggregator`
to durably commit the timestamp of the latest message it sent out, and
for the job system to recreate the `ChangeAggregator` if it failed.

## Timelystamp

The `ChangeFeed` RFC briefly introduced the notion of timestamps with
checkpoints. We formally define them here. In order to avoid confusion
with HLC Timestamps and to make their provenance
from [Naiad](http://sigops.org/sosp/sosp13/papers/p439-murray.pdf)'s
"Timely Dataflow" we call them "Timelystamps".

In general, a Timely Dataflow Timelystamp is a k-dimensional
generalization of a timestamp, but over here, we only require two
dimensional Timelystamps, where one dimension is the regular integer
HLC timestamp, and the second dimension is a Boolean (checkpointed or
not). The full k-dimensional Timelystamps in the presence of cyclic
dataflow graphs will be fully fleshed out in a future Materialized
Views RFC, but the properties relevant to CDCs are as follows:

1. Each individual change event is sent along with a 2-d Timelystamp,
   that has the proto: `message Timelystamp {HLC_timestamp timestamp,
   bool checkpointed}`

2. When a sender outputs Timelystamp `T=<t,true>`, it guarantees that
   it will never ever send another Timelystamp `T'=<t',_>`
   where `t' <= t`.

5. Checkpoint at `t_2` implies `forall t_1`, checkpoint `t_1` where
   `t_1 < t_2`.

6. A sender should eventually checkpoint every Timelystamp it sends.

The Naiad paper uses the terminology "Notify" to indicate a timestamp
checkpoint, and gives an inductive definition (defining a k+1
dimension output timestamp assuming a k dimensional input timestamp),
so it's rather unhelpful as a reference.

## Fault tolerant DistSQL processors.

Unlike regular DistSQL processors, which are spun up with flows
already assigned, a fault tolerant DistSQL processor is able to spin
up its subtree plan on the fly. Thus, if you spin up the final
downstream processor, it can recursively spin up dependent processors.

We need fault tolerant processors for CDCs because CDCs are long lived
and almost certainly going to encounter a dead processor, and we need
the CDC to resume without throwing everything away and starting from
scratch, as we do with regular DistSQL queries, only retrying at the
executor level. This is not something that is in scope for 1.2.

## System.jobs table and CDC Resumability

We use the System.jobs table to keep track of CDC endpoints, and use
the job resumability system to spin up `ChangeAggregator`s. The scope
of this work is currently unknown, and may not fall into 1.2.

The ChangeAggregator needs to save the HLC timestamp of the last
message it sent. It can either do so before sending it to the Kafka
stream (giving an at-most-once delivery guarantee) or after sending it
(giving an at-least-once delivery guarantee).

# Open questions

Is there a way to query a Kafka producer to find out the last durably
committed HLC timestamp? Since Kafka maintains a sliding window of
messages, this seems theoretically possible. If so, it would be
possible for a recovered CDC to deduplicate, and recover exactly-once
delivery as long as recovery happens before the sliding window grows
too large and messages are dropped.

# Scope, and ordering of implementation

As these are ambitious design goals, we structure the implementation
in several stages:

0. Build the `ChangeFeed` primitive at the replica level, allowing for
a stream of change events reflecting writes to that replica. The
`ChangeFeed` dies if the replica is in any way disturbed (leadership
lost, rebalanced, etc).

1. Tracking (at the replica layer) of a "min-safe timestamp"
watermark, which allows a `ChangeFeed` to guarantee that no writes
will ever occur before the watermark. This is required for in-order
Change Data Capture, but out-of-order change data capture can proceed
without it.

2. A stub `ChangeAggregator` that strips the internal information out
of the incoming `ChangeFeed` stream, stores everything in-memory, and
pushes as a Kafka producer to a Kafka cluster.

3. A basic CDC against an entire table, by calling the `DistSQL`
`SpanResolver` sets up multiple `ChangeFeed`s to cover the whole
table, and uses external storage to buffer messages that are not yet
past the replica watermark.

4. Transactionally creating CDCs.

5. Resumability/cancellation of CDCs, persistence and recovery
throughout cluster chaos.

6. Performance testing of CDC under full write load (e.g. a change
feed on a table under YCSB write-only workload).

7. Fault tolerance and resumability.

Stage 0 and 1 can proceed in parallel. 2 depends on 0, and 3 depends
on 2, but cannot deliver in-order messages until 1 is finished. 4
seems easy and just requires threading timestamps.

5 is an unknown (comments welcome!), as it's unclear how much of the
work done by the Bulk I/O team on resumability and jobs tables can be
reused. 7 is not scoped out in this RFC.

# Related Work

Change feeds are related to, but not the same
as [Database Triggers](https://www.postgresql.org/docs/9.1/static/sql-createtrigger.html).

Database triggers are arbitrary stored procedures that "trigger" upon
the execution of some commands (e.g. writes to a specific database
table). However, triggers do not usually give any consistency
guarantees (a crash after commit, but before a trigger has run, for
example, could result in the trigger never running), or atomicity
guarantees. Triggers thus typically provide "at most once" semantics.

In contrast, change feeds typically provide "at least once"
semantics. This requires that change feeds publish an ordered stream
of updates, that feeds into a queue with a sliding window (since
subscribers might lag the publisher). Importantly, publishers must be
resilient to crashes (up to a reasonable downtime), and be able to
recover where they left off, ensuring that no updates are missed.

"Exactly once" delivery is impossible for a plain message queue, but
recoverable with deduplication at the consumer level with a space
overhead. Exactly once message application is required to maintain
correctness on incrementally updating materialized views, and thus,
some space overhead is unavoidable. The key to a good design remains
in minimizing this space overhead (and gracefully decommissioning the
ChangeFeed/materialized views if the overhead grows too large and we
can no longer deliver the semantics, so we never "lie", but gracefully
deliver the bad news that we can no longer tell the truth).

Change feeds are typically propagated through Streaming Frameworks
like [Apache Kafka](https://kafka.apache.org)
and
[Google Cloud PubSub](https://cloud.google.com/pubsub/docs/overview),
or to simple Message Queues like [RabbitMQ](https://www.rabbitmq.com/)
and [Apache ActiveMQ](http://activemq.apache.org/). These frameworks
typically maintain a sliding window of ordered messages: a producer
writes to the tail of the window, and a consumer reads from the head
of the window. These message queues can mediate between two different
systems that read and write at different rates (for instance, a
 OLAP warehouse that reads from the ChangeFeed will have high
latency on absorbing changes due to its columnar storage, but can
accumulate a buffer of changes, and load them once. Meanwhile, the
OLTP database feeding into the message queue might write at a more
constant rate, and would not want to block its transaction throughput
on the warehouse having ingested the changes).

[RethinkDB change feeds](https://rethinkdb.com/docs/changefeeds/ruby/)
were a much appreciated feature by the industry community. RethinkDB
change feeds returned a `cursor`, which was possibly blocking. A
cursor could be consumed, which would deliver updates, blocking when
there were no new updates remaining. Change feeds in RethinkDB could
be configured to `filter` only a subset of changes, rather than all
updates to a table/database. The API design in this RFC is heavily
influenced by RethinkDB's change feed API.

The [Kafka Producer
API](http://docs.confluent.io/current/clients/confluent-kafka-go/index.html#hdr-Producer)
works as follows: producers produce "messages", which are sent
asynchronously. A stream of acknowledgment "events" are sent back. It
is the producers responsibility to resend messages that are not
acknowledged, but it is acceptable for a producer to send a given
message multiple times. Kafka nodes maintain a "sliding window" of
messages, and consumers read them with a cursor that blocks once it
reaches the head of the stream.

TODO(arjun): Will a Kafka stream deduplicate messages that are
repeated, but which are within the sliding window of messages?

# Appendix A: MVCC Refresher

As a quick refresher on CockroachDB's MVCC model, Consider how a
multi-key transaction executes:

1. A write transaction `TR_1` is initiated, which writes three keys:
   `A=a`, `B=b`, and `C=c`. These keys reside on three different ranges,
   which we denote `R_A`, `R_B`, and `R_C` respectively. The
   transaction is assigned a timestamp `t_1`. One of the keys is
   chosen as the leader for this transaction, lets say `A`.

2. The writes are first written down as _intents_, as follows: The
   following three keys are written:
    `A=a intent for TR_1 at t_1 LEADER`
    `B=b intent for TR_1 at t_1 leader on A`
    `C=c intent for TR_1 at t_1 leader on A`.

3. Each of `R_B` and `R_C` does the following: it sends an `ABORT` (if
   it couldn't successfully write the intent key) or `STAGE` (if it
   did) back to `R_A`.

4. While these intents are "live", `B` and `C` cannot provide reads to
   other transactions for `B` and `C` at `t>=t_1`. They have to relay
   these transactions through `R_A`, as the leader intent on `A` is
   the final arbiter of whether the write happens or not.

5. `R_A` waits until it receives unanimous consent. If it receives a
   single abort, it atomically deletes the intent key. It then sends
   asynchronous potentially-lossy cancellations to `B` and `C`, to
   clean up their intents. If these cancellations fail to send, then
   some future reader that performs step 4 will find no leader intent,
   and presume the intent did not succeed, relaying back to `B` or `C`
   to clean up their intent.

6. If `R_A` receives unanimous consent, it atomically deletes the
   intent key and writes value `A=a at t_1`. It then sends
   asynchronous commits to `B` and `C`, to clean up their intents. If
   they receive their intents, they remove their intents, writing `B=b
   at t_1` and `C=c at t_1`.

Do note that if these messages in step 5 and 6 do not make it due to a
crash at A, this is not a consistency violation: Reads on `B` and `C`
will have to route through `A` as the intent marking the leader at `A`
will remain, and will stall until they find the updated successful
write on `A`, or find that the intent has disappeared.  Thus, while
there is a "best-effort" attempt to eagerly resolve the intent on all
ranges, in the worst case they are only resolved lazily.

Therefore, the final step is not transactionally consistent. We cannot
use those cleanups to trigger change feeds, as they may not happen
until much later (e.g. in the case of a crash immediately after `TR_1`
atomically commits at `A`, before the cleanup messages are sent, after
`A` recovers, the soft state of the cleanup messages is not recovered,
and is only lazily evaluated when `B` or `C` are read). Using these
intent resolutions would result in out-of-order message delivery.

Using the atomic commit on `R_A` as the trigger poses a different
problem: now, an update to a key can come from _any other range on
which a multikey transaction could legally take place._ While in SQL
this is any other table in the same database, in practice in
CockroachDB this means effectively any other range. Thus every range
(or rather, every store, since much of this work can be aggregated at
the store level) must be aware of every ChangeFeed on the database,
since it might have to push a change from a transaction commit that
involves a write on a "watched" key.

A compromise is for ChangeFeeds to only depend on those ranges that
hold keys that the ChangeFeed depends on. However, a range (say `R_B`)
is responsible for registering dependencies on other ranges when
there is an intent created, and that intent lives on another range
(say `R_A`). It then must periodically poll `R_A` to find if the
intent has been resolved (one way or another) before it can "close"
the HLC timestamp and update its safe watermark.
