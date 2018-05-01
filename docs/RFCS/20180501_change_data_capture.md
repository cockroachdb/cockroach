- Feature Name: Change Data Capture (CDC)
- Status: draft
- Start Date: 2018-05-01
- Authors: Arjun Narayan, Daniel Harrison, Nathan VanBenschoten, Tobias Schottdorf
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#2656], [#6130]


# Summary

Change Data Capture (CDC) provides efficient, distributed, row-level change
subscriptions.


# Motivation

CockroachDB is an excellent system of record, but no technology exists in a
vacuum. Users would like to keep their data mirrored in full-text indexes,
analytics engines, and big data pipelines, among others. A pull model can
currently be used to do this, but it’s inefficient, overly manual, and doesn’t
scale; the push model described in this RFC addresses these limitations.

Anecdotally, CDC is one of the more frequently requested features for
CockroachDB.


# Guide-level explanation

The core primitive of CDC is the `CHANGEFEED`. Changefeeds target a whitelist of
databases, tables, partitions, rows, or a combination of these, called the
"watched rows". Every change to a watched row is emitted as a record in a
configurable format (initially JSON or Avro) to a configurable sink (initially
Kafka). Changefeeds can be paused, unpaused, and cancelled.

Changefeeds scale to any size CockroachDB cluster and are designed to impact
production traffic as little as possible.

By default, when a new changefeed is created, an initial timestamp is chosen and
the current value of each watched row as of that timestamp is emitted (similar
to `SELECT ... AS OF SYSTEM TIME`). After this "initial scan", updates to
watched rows are emitted. The `AS OF SYSTEM TIME <timestamp>` syntax can be used
to start a new changefeed that skips the initial scan and only emits all changes
after the user-given timestamp.

Kafka was chosen as our initial sink because of customer demand and its status
as the clear frontrunner in the category. Most of these customers also use the
associated Confluent Platform ecosystem and Avro format; our changefeeds are
designed to integrate well with them. Other sinks will be added as demand
dictates, but the details of this are out of scope. The rest of this document is
written with Kafka specifics, like topics and partitions, to make it easier to
understand, but these same processes will work for other sinks.


## Row Ordering

Each emitted record contains the data of a changed row along with the timestamp
associated with the transaction that updated the row. Rows are sharded between
Kafka partitions by the row’s primary key. Tables with more than one [column
family] will require some buffering and are more expensive in a changefeed. If a
row is modified more than once in the same transaction, only the last will be
emitted.

Once a row has been emitted with some timestamp, no _previously unseen_ versions
of that row will be emitted with a lower timestamp. In the common case, each
version of a row will be emitted once, but some (infrequent) conditions will
cause them to be repeated, giving our changefeeds an at-least-once delivery
guarantee. See [Full-text index example: Elasticsearch]’s "external versions"
for one way to be resilient to these repetitions.

Many of the sinks we could support have limited exactly-once guarantees on the
consumer side, so there's not much to be gained by offering exactly-once
delivery on the producer side, when that's even possible. Kafka, however, does
have the necessary primitives for us to build an option to provide [exactly-once
delivery] at the cost of performance and scalability.

Cross-row and cross-table order guarantees are not given.

These particular ordering and delivery guarantees were selected to allow simple
cases to be easy and low-latency (after the initial catch-up and in the absence
of rebalancing, perhaps single or double digit milliseconds).


## Cross-Row and Cross-Table Ordering

Some users require stronger order guarantees, so we provide the pieces necessary
to reconstruct them. (See [Transaction grouped changes] for why CockroachDB
doesn’t do this ordering itself.)

As mentioned above, each emitted record contains the timestamp of the
transaction that updated the row. This can be used with the `timestamp_closures`
option, which causes the changefeed to periodically emit an in-stream timestamp
close notifications on every kafka partition. The timestamp included in this
record is a guarantee that no previously unseen version of _any_ row will be
emitted to that partition afterward will have a higher timestamp.

Together, these can be used to give strong ordering and global consistency
guarantees by buffering records in between timestamp closures. See [Data
warehouse example: Amazon Redshift] below for a concrete example of how this
works.


## Syntax Examples

Creating a changefeed is accomplished via `CREATE CHANGEFEED`:

```sql
CREATE CHANGEFEED <name> FOR TABLE tweets EMIT TO 'kafka://host:port' WITH <...>
CREATE CHANGEFEED <name> FOR TABLE tweets VALUES FROM (1) TO (2) EMIT TO <...>
CREATE CHANGEFEED <name> FOR TABLE tweets PARTITION north_america EMIT TO <...>
CREATE CHANGEFEED <name> FOR DATABASE db EMIT TO <...>
```

- EMIT TO `<sink>` is a URI that contains all the configuration information
  needed to connect to a given sink. Any necessary configuration is done through
  query parameters. This is similar to how the URIs work in
  BACKUP/RESTORE/IMPORT.
  - `kafka://host:port` Kafka is used as the sink. The bootstrap server is
    specified as the host and port.
    - `?kafka_topic_prefix=<...>` A string to prepend to the topic names used by
      this changefeed.
    - `?kafka_schema_topic=<...>` A Kafka topic to emit all schema changes to.
    - `?confluent_schema_registry=<address>` The address of a schema registry
      instance. Only allowed with the avro format. When unspecified, no schema
      registry is used.
  - `experimental_table:///` A [SQL table sink] in the same CockroachDB cluster.
    - TODO options
- `WITH <...>`
  - `WITH envelope=<...>` Kafka records have a key and a value. The key is
    always set to the primary key of the changed row and the value’s contents
    are controlled with this option.
    - `envelope='none'` [DEFAULT] The new values in the row (or empty for a
      deletion). This works with Kafka log compaction. The system may internally
      need to read additional column families.
    - `envelope='key_only'` The value is always empty. No additional reads are
      ever needed.
    - `envelope='diff'` The value is a composite with the old row value and the
      new row value. The old row value is empty for INSERTs and the new row
      value is empty for DELETEs. The system will internally need additional
      reads.
  - `WITH format=<...>`
    - `format='json'` [DEFAULT] The record key is a serialized JSON array. The
      record value is a serialized JSON object mapping column names to column
      values.
    - `format='avro'` The record key is an [Avro] array, serialized in the
      binary format. The record value is an [Avro] record, mapping column names
      to column values, serialized in the binary format.

See [other CDC syntaxes] for alternatives that were considered.


## Altering the changefeed

`PAUSE CHANGEFEED <name>` and `RESUME CHANGEFEED <name>` can be used to pause
and unpause changefeeds.

`DROP CHANGEFEED <name>` can be used to stop a changefeed.

`SHOW CREATE CHANGEFEED <name>` works similarly to the other `SHOW CREATE`
command and can be used view the changefeed definition.

Tthe following `ALTER CHANGEFEED` commands can be used to change the
configuration. Only paused changefeed jobs can be altered, so the user may have
to `PAUSE CHANGEFEED` and `RESUME CHANGEFEED` as necessary.

`ALTER CHANGEFEED <name> EMIT FROM SYSTEM TIME <timestamp>` can be used to
manually adjust the highwater mark forward or back. When resumed, the changefeed
will emit any changes after the given timestamp, but will not emit the initial
catch-up. To do this, the changefeed must be cancelled and replaced with a new
one.

`ALTER CHANGEFEED <name> EMIT TO <...>` can be used to migrate Kafka clusters.

`ALTER CHANGEFEED <name> SET key=<...>` can be used to change the `WITH`
options.


## Full-text index example: Elasticsearch

A user creates a json changefeed with no envelope and
`kafka_topic_prefix=crdb_`. A kafka consumer tails every topic starting with
`crdb_`, writing them to Elasticsearch. The "mapping" is either derived
automatically from the json or manually set beforehand by the user. The
`topic+partition+offset` tuple is used as Elasticsearch’s external version;
because we guarantee that keys are written to the changefeed in order, this is
sufficient to prevent clobbering. The Confluent [ElasticsearchSinkConnector] can
be configured exactly with this logic, so the user doesn’t need to write any
code.


## Data warehouse example: Amazon Redshift

TODO: This could use more detail.

A user creates an avro changefeed with no envelope. A kafka consumer group tails
the resulting topics by repeating the following steps in a loop.

1. A timestamp T in the future is picked.
2. The consumers consume from Kafka, writing any records with CockroachDB
   timestamps before T to an S3 folder. Any records with timestamps after T are
   written to the next folder.
3. Once _every_ partition has received a close notification with a timestamp
   greater than T, then no more records with timestamps less than T will ever
   come.
4. The data in the T bucket is loaded via Redshift bulk load. It is
   transactionally consistent across the entire CockroachDB cluster.
5. Repeat


## Failures

If Kafka is unavailable or underprovisioned the changefeed will buffer records
as necessary. Buffering is limited by the `--max-changefeed-memory` and
`--max-disk-changefeed-storage` flags. As with any changefeed, degraded sinks
minimally affect foreground traffic. Further, one table failing will not affect
other tables, even ones watched by the same changefeed.

It is crucial that Kafka recovers before the watched rows (and the system
tables) exit their respective [garbage collection] TTL (default 25 hours) with
enough time for the changefeed to catch up. This is similar to the restriction
on [incremental backups]. A metric is exported to monitor how close to the GC
TTL each changefeed’s most behind timestamp is. Production users should monitor
and alert on this metric.

If the sink is down for too long, some data may be lost and the changefeed will
be marked as failed. User intervention is required. Either a new changefeed can
be created with the existing Kafka topics and an appropriate `AS OF SYSTEM TIME`
(which will leave a hole of missing data) or a new changefeed can be restarted
with an empty sink (a clean slate).

If Kafka recovery won't happen in time, the relevant garbage collection TTLs can
be manually increased by the user. This affects disk usage and query
performance, so the system will not do it automatically. Similarly, additional
partitions are added by the user to underprovisioned sinks because the
key-to-partition sharding would change. This would break any consumers that
assume all keys are in the same partition, a common assumption.

Records are first buffered in memory, but this is limited to at most
`--max-changefeed-memory` bytes across all changefeeds on a node. If this limit
is reached, records are then spilled to disk, up to the
`--max-disk-changefeed-storage` flag. If this limit is also reached, the
changefeed will enter a "stalled" state and will tear down its internal
connections to reduce load on the cluster. When the backlog clears out, the
changefeed will restart from where it left off.


## AdminUI

A new page is used in the AdminUI to show the status of changefeeds, distinct
from the "Jobs" page because changefeeds don't have a finite lifespan.
Incrementally updated materialized views, for example, would also appear on this
page. Similarly, a counterpart to `SHOW JOBS` is introduced.


# Reference-level explanation

Each changefeed is structured as a single coordinator which sets up a
long-running [DistSQL] flow with a new processor described below.

Each changefeed is given an entry in the [system jobs], which provides this
fault-resistent coordinator. Briefly, this works by a time based lease, which
must be continually extended; if it is not, a new coordinator is started. Some
failure scenarios result in a split-brain coordinator, but our ordering and
duplicate delivery guarantees were chosen to account for this.

In addition to the 0-100% progress tracking, the `system.jobs` tracking is
extended with an option to show the minimum of all closed timestamps.


## DistSQL flow

```proto
// ChangeAggregatorSpec is the specification for a processor that watches for
// changes in a set of spans. Each span may cross multiple ranges.
message ChangeAggregatorSpec {
  message Watch {
    util.hlc.Timestamp timestamp = 1;
    roachpb.Span span = 2;
  }
  repeated Watch watches = 1;
  bool initial_scan = 2;
}
```

Each flow is made of a set of `ChangeAggregator` processors. Progress
information in the form of `(span, timestamp)` pairs is passed back to the job
coordinator via DistSQL metadata. (TODO: Is the metadata the right place to do
this?).

DistSQL is currently optimized for short-running queries and doesn’t yet have a
facility for resilience to processor errors. Instead, it shuts down the whole
flow. This is unfortunate for CDC, but failures should be relatively infrequent
and the amount of work duplicated when the flow is restarted should be
relatively small, so it’s not as bad as it first sounds. The largest impact is
on tail latencies. If/when DistSQL's capabilities change, this will be
revisited.

`ChangeAggregator`s are scheduled such that each is responsible for data local
to the node it’s running on. If the data it’s watching moves, the local data
will become remote, which will continue to work, but will use bandwidth and add
a network hop to latency. DistSQL doesn’t yet have a mechanism for moving or
reconfiguring processors in a running flow, but leaving this state indefinitely
is unfortunate. Instead, the job coordinator will periodically run some
heuristics and when it’s advantageous, stop and restart the flow such that all
processors will be doing local watches again. If the heuristics are not
aggressive enough, the user always has an escape hatch of manually pausing and
resuming the job. This restarting of the flow is costly in proportion to the
size of the changefeed and will affect tail latencies, so something should be
done here in future work.

GDPR will also require that processors be scheduled in a certain region, once
that's supported by DistSQL.


## ChangeAggregator processor

A `ChangeAggregator` is responsible for watching for changes in a set of
disjoint spans, buffering during the initial catch up, translating kvs into sql
rows, and emitting to a sink. Additionally, it bookkeeps and emits the timestamp
close notifications described in [Cross-Row and Cross-Table Ordering].

If the `initial_scan` option is set, then the current value of every watched row
at `timestamp` is emitted. `ChangeAggregator` then proceeds to set up
ChangeFeeds.

### ChangeFeed

The [ChangeFeed] command sets up a stream of all changes to keys in a given
span. The changes are returned exactly as they are [proposed and sent through
raft]: in RocksDB WriteBatch format, possibly with unrelated data (parts of the
range’s keyspace not being watched by the `ChangeFeed` and range-local keys).

Passing the raft proposal through unchanged minimizes cpu load on the replica
serving the `ChangeFeed`. This is not initially critical, since we’ll just be
scheduling the processors on the same node, but gives us flexibility later to
make changefeeds extremely low-impact on production traffic. (Note that a
`ChangeFeed` can be thought of as a slightly lighter weight additional Raft
follower.) Using the same format as proposer-evaluated kv also lesses the chance
that we’ll have tricky rpc migration issues in the future.

The amount of unrelated data returned will be low if most or all of the range’s
keyspace is being watched, but in the worst case (changes touching many keys in
the range), may be high if only a small subset is watched. This means table and
partition watches will be efficient, but the efficiency of single-row watches
will depend on the workload. We may support single-row watches, but are not
optimizing for them yet, so this is okay.

`ChangeFeed`s are run on the leaseholder for now. It should be possible to run
`ChangeFeed`s on followers in the future, which becomes especially important
when ranges are geographically distributed. However, there are complications to
work through. Importantly, this follower could be the slowest one, increasing
commit-to-kafka changefeed latency. We also don’t currently have common code for
geography-aware scheduling; this would have to be built.

A new `ChangeFeed` first registers itself to emit any relevant raft commands as
they commit. These are sent to the (ordered) rpc stream in the same order they
are in the raft log. These are buffered by `ChangeAggregator`.

After the `ChangeFeed` is set up, the `ChangeAggregator` uses `ExportRequest` to
catch up on any changes between `watch.timestamp` and some timestamp after the
raft hook was registered. These partially overlap the `ChangeFeed` output and
must be deduplicated with it. `ExportRequest` already uses the time-bounded
iterator to minimize the data read, but support for `READ_UNCOMMITTED` is added
so we don't need to block on intent resolution. The potential move to larger
range sizes may dictate that this becomes a streaming process so we don't have
to hold everything in memory, but details of that are out of scope.

There is an ongoing discussion about how `ChangeFeed` will handle splits,
merges, leaseholder moves, node failures, etc. The tradeoffs will be worked out
in an update to the `ChangeFeed` RFC and are out of scope here. However, it's
important to note that the `ChangeFeed` will occasionally need to disconnect as
part of normal operation (e.g. if it receives a snapshot) and so the
reconnection should be handled as cheaply as possible, likely by using closed
timestamps as lower bounds on the catch up scans.


### Intent tracking

Once the catch up scans finish, the buffered data is processed. Single range
fastpath transations and committed intents are passed on to the next stage.
Aborted intents and unrelated data are filtered. This is guaranteed to emit
individual keys in mvcc timestamp order. Opened intents are tracked as follows.

The `ChangeFeed` also returns in-stream, "[follower read] notifications", which
are a guarantee that no new intents or transactions will be written below a
timestamp. Notably, this does not imply that all the intents below it have been
resolved. An in-memory structure is bootstraped with all open intents returned
by the `ExportRequest` catch up scans. As intents are resolved and follower read
notifications arrive, `min(earliest unresolved intent, latest follower read
notification)` will advance. These advances correspond to our close notification
guarantees and are passed to later stages.


### KV to row conversion

`TableDescriptor`s are required to interpret kvs as sql rows. The correctness of
our online schema changes depends on each query holding a "lease" for the
TableDescriptor that is valid for the timestamp it’s executing at. In contrast
to queries, which execute with a distinct timestamp, changefeeds operate
continuously.

Rows are stored using one kv per [column family]. For tables with one column
family, which is the common case and the default if the user doesn’t specify,
each entry in the `ChangeFeed`'s WriteBatch is the entire data for the row. For
tables with more than one column family, a followup kv scan will be needed to
reconstruct the row. A similar fetch will be required if the user requests a
changefeed with both the old and new values. `ChangeFeed` could instead send
this along when necessary, eliminating a network hop and the cost of serving a
request, but it’s preferable to avoid pushing knowledge of column families and
sql rows down to kv.


### Sink emitter

The sink emitter receives an ordered stream of sql rows, schema changes, and
close notifications. The sql rows are sent to the relevant kafka topic, sharded
into partitions by primary key. When a schema registry is provided, the schema
changes are forwarded.

Close notifications are forwarded when requested by the user. We guarantee that
no records with a timestamp less than the close notification will be after it,
so progress must be synchronously written to the job entry before emitting a
close notification.

For easy development and testing, a 1 partition topic will be auto-created on
demand when missing. For production deployments, the user should set the Kafka
option to disallow auto-creation of topics and manually create the topic with
the correct number of partitions. A user may manually add partitions at any time
and this transparently works, though the row to partition mapping will change
and consumers are responsible for handling this. Kafka does not support removing
partitions.


### Buffering

For simplicity and to even out cpu utilization, there is backpressure from Kafka
all the way back to the buffer between `ChangeFeed` and the intent tracker. The
buffer starts in-memory but spills to disk if it exceeds its budget. If the disk
buffer also exceeds its budget, the `ChangeFeed`s are temporarily shut down
until the emitting process has caught up.


## SQL Table Sink

```sql
CREATE TABLE <name> (
  table STRING,
  message_id INT,
  key BYTES,
  value BYTES,
  PRIMARY KEY (table, key, message_id)
)
```

The SQL Table Sink is initially only for internal testing. A single table is
created and contains all updates for a changefeed, even if that changefeed is
watching multiple tables. The `key` and `value` are encoded using the `format`
specified by the changefeed. `message_id`s are only comparable between rows with
equal `key`s.


## Truncate

For efficiency, in CockroachDB and in other databases, truncate is implemented
as a schema change instead of a large number of row deletions. This has
consequences for changefeeds. Consider the [Elasticsearch example] above. It’s
not using schema changes for anything, so rows in a truncated table would
continue to exist in Elasticsearch after truncate.

Postgres’s logical decoding is not aware of truncate or schema changes. We adopt
the SQL Server behavior, which prevents a table with an active changefeed from
being truncated.


## Permissions

Changefeeds will initially be admin-only. Eventually we should allow non-admin
users to create changefeeds, and admins should be able to revoke those users'
permissions (breaking/cancelling the feeds).



# Drawbacks

## Lots of configuration options

There are a number of options described above for format, envelope, sink
configuration, schema topics, etc. This is a lot, but they all seem necessary.
Simple cases like mirroring into Elasticsearch should work without the overhead
of avro, schema registries, and message envelopes. At the same time, we don’t
want to limit power by not having the ability for users to subscribe to the diff
of a changed row. Connectors in the Confluent platform and other CDC
implementations also seem to have a large number of configuration options, so
maybe this is just unavoidable.


## No transaction grouped changes

One could easily imagine that a user would want to subscribe to a totally
ordered feed of all transactions that happen in the database. However, unlike
sharded PostgreSQL (for example), CockroachDB supports transactions touching any
part of the cluster. Which means that in the general case, there is no way to
horizontally divide up the transaction log of the cluster such that each piece
is independent. Our only options are (a) a single transaction log, which limits
scalability, (b) encoding the transaction dependency graph and respecting it
when replaying transactions, which is complex, or (c) taking advantage of
special cases (such and such tables are never used in the same transactions as
these other ones), which will not always work.

As a result, we’ve decided to give CockroachDB users the information necessary
to reconstruct transactions, but will not immediately be building anything to do
it in the general case.


# Rationale and alternatives

## Alternative sinks

Our customers have overwhelmingly asked for Kafka as the sink they want for CDC
changes, so it will be the first we support but Kafka won’t work for everyone.

Some changefeeds will be low-throughput and for this a streaming SQL connection
(with the same wire format as PostgreSQL’s `LISTEN`) would be sufficient and
much simpler operationally. This lends itself well to single-row watches (in the
style of RethinkDB).

Other users will want their changefeeds to emit to a cloud-hosted pubsub.

Kafka is not the only sink we’ll need, but to limit scope these other sinks not
be in the initial version.


## Write a Kafka connector

The Confluent platform ships with a [JDBC source connector], so we’re done,
right? It works by periodically scanning an incrementing id column or a last
updated timestamp column or both. The id column only works with append-only
tables and the last updated timestamp column must be maintained correctly by the
user. It works well for prototyping, but ultimately we feel that latency,
cluster load, and developer burden means that this doesn’t fit with our mission
to make data easy.

We could also write our own Confluent Platform connector. While Kafka will be
our initial sink, we don’t want to tie ourselves to it too closely. It’s also
not clear how we could make this work with end-to-end push to keep latencies and
performance impact low.


## Exactly-once delivery

TODO: Description of kafka transactions and how they could be used for this.


# Unresolved Questions

- Flow control/throttling of the ChangeFeed streams
- Lots of small changefeeds will require some coalescing
- A column whitelist, which could be useful for GDPR compliance or performance
- Backup/restore of changefeeds
- Details of recovering a failed changefeed using `revision_history` backups


# Appendix

## Appendix: Kafka background

[Kafka] is used for all sorts of things, including as a pub/sub. The fundamental
abstraction is a named _topic_, which is a stream of _records_.

A topic is subdivided into a number of (essentially totally independant)
_partitions_. Each partition is a distributed, replicated, ordered, immutable
sequence of records that is continually appended to. Records are a timestamped
key/value byte pair. Keys are not required. If the timestamp is not specified,
it will be assigned by Kafka. Once appended to a partition, an _offset_ is given
to the record, so topic+partition+offset will uniquely identify a record.

The number of partitions in a topic can be increased, but never decreased. There
are no ordering guarantees across partitions.

Any producers for a topic may append new records to any partition, by default
hash of key or round-robin if no key is specified. Two records sent from a
producer to the same partition are guaranteed to retain their order in the log,
but there are no ordering guarantees across-producers.

Any number of _consumer groups_ may tail a topic. A consumer group is made up of
independent consumer processes. Each partition in a topic is assigned to exactly
one consumer in the group. By default, the consumer works beginning to end and
Kafka keeps track of the current position in each partition, handling consumer
failures as needed, but consumers may seek to positions at will.

Old log entries are cleaned up according to time/space/etc policies.


## Appendix: Avro background

Avro is a serialization format, similar to protocol buffers. Notably, schemas
are designed to be serialized and stored alongside the serialized records. The
spec defines clear "[schema resolution]" rules for asserting whether a
progression of schemas is forward or backward compatible and for reading data
written with an older or newer version of the schema. The [Confluent Schema
Registry] integrates Kafka and Avro and allows consumers to declare which schema
evolutions are supported.


## Appendix: Other CDC syntaxes

Whenever possible, we reuse PostgreSQL syntax in an attempt to minimize user
friction. For CDC, there are two potentially relevant feature: logical decoding
and logical replication.

Logical decoding allows a dynamically loaded plugin to access the WAL. This is
how most Postgres CDC works. For example, [Debezium] uses logical decoding to
hook PostgreSQL up to Kafka. Unfortunately, the syntax is tightly tied to
PostgreSQL's notion of replication slots, which don't map cleanly onto
CockroachDB's distributed ranges and replicas.

Logical replication is used to stream changes from one PostgreSQL instance to
another to keep them in sync. Its syntax is in terms of producers and
subscribers. The biggest reason to have drop-in compatibility is so tooling,
such as ORMs, will work out of the box, but it's unlikely that ORMs would be
creating changefeeds. Additionally, our model of push to a sink is different
enough from a subscriber pulling from a producer that trying to reuse syntax is
likely to be more confusing than helpful. This syntax could always be added as
an alias if we build support for CockroachDB to CockroachDB replication.



[#2656]: https://github.com/cockroachdb/cockroach/issues/2656
[#6130]: https://github.com/cockroachdb/cockroach/issues/6130
[avro]: #appendix-avro-background
[changefeed]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20170613_change_feeds_storage_primitive.md
[column family]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20151214_sql_column_families.md
[column family]: https://www.cockroachlabs.com/docs/stable/column-families.html
[confluent schema registry]: https://docs.confluent.io/current/schema-registry/docs/index.html
[cross-row and cross-table ordering]: #cross-row-and-cross-table-ordering
[data warehouse example: amazon redshift]: #data-warehouse-example-amazon-redshift
[debezium]: http://debezium.io
[distsql]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20160421_distributed_sql.md
[elasticsearch-example]: #fulltext-index-example-elasticsearch
[elasticsearchsinkconnector]: https://docs.confluent.io/current/connect/connect-elasticsearch/docs/elasticsearch_connector.html
[exactly-once delivery]: #exactly-once-delivery
[full-text index example: elasticsearch]: #full-text-index-example-elasticsearch
[garbage collection]: https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html#garbage-collection
[incremental backups]: https://www.cockroachlabs.com/docs/stable/backup.html
[jdbc source connector]: https://docs.confluent.io/current/connect/connect-jdbc/docs/source_connector.html
[kafka]: https://kafka.apache.org/intro
[other cdc syntaxes]: #appendix-other-cdc-syntaxes
[proposed and sent through raft]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20160420_proposer_evaluated_kv.md
[schema resolution]: http://avro.apache.org/docs/current/spec.html#Schema+Resolution
[sql table sink]: #sql-table-sink
[system jobs]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20170215_system_jobs.md
[transaction grouped changes]: #no-transaction-grouped-changes
[follower read]: https://github.com/cockroachdb/cockroach/pull/19222
