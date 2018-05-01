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
“watched rows”. Every change to a watched row is emitted as a record in a
configurable format (initially JSON or Avro) to a configurable sink (initially
Kafka). Changefeeds are [long running jobs] and can be paused, unpaused, and
cancelled.

Changefeeds scale to any size CockroachDB cluster. Production traffic is
impacted as little as possible.

By default, when a new changefeed is created, an initial “catch-up” (TODO better
jargon for this) scan is run that emits the current value of each watched row
before emitting any updates to that row (TODO jargon for this phase). The `AS OF
SYSTEM TIME <timestamp>` syntax can be used to start a new changefeed that skips
this catch-up scan and only emits all changes after the given timestamp.

Kafka was chosen as our initial sink because of customer demand and its status
as the clear frontrunner in the category. Most of these customers also use the
associated Confluent Platform ecosystem and Avro format; our changefeeds are
designed to integrate well with them. Other sinks will be added as demand
dictates, but the details of this are out of scope. The rest of this document is
written with Kafka specifics, like topics and partitions, to make it easier to
understand, but these same processes will work for other sinks.


## Row Ordering

Changes to a row are always emitted in order and rows are sharded between Kafka
partitions by the row’s primary key. This allows simple cases to be easy and
low-latency (after the initial catch-up and in the absence of rebalancing,
perhaps single digit milliseconds). It also means Kafka’s log compaction feature
will work as expected.

While changes to a row are emitted in order, we offer “at least once” semantics,
which means that for a given row, the feed may emit sequences like V1, V2, V3,
V2, V3, V4 (note the repeated “V2 V3” subsequence). See [Full-text index
example: Elasticsearch]’s “external versions” for one way to be resilient to
this.

Cross-row or cross-table order guarantees are not given.


## Cross-Row and Cross-Table Ordering
Some users require stronger order guarantees, so we provide timestamps on
records and periodic “timestamp close” notifications, which can be used to
reconstruct orderings. (See [Transaction grouped changes] for why CockroachDB
doesn’t do this ordering itself.)

Each emitted record contains the transaction timestamp the most recent change
to that row committed at. This can be used with the `timestamp_closures` option,
which causes the changefeed to periodically emit timestamp close notifications
on every kafka partition. The timestamp included in this record is a guarantee
that every record emitted to that partition afterward will have a higher
timestamp.

Combined, these mean a CDC consumer can buffer records until timestamp closures
to give strong ordering and global consistency guarantees. See [Data warehouse
example: Amazon Redshift] below for a concrete example of how this works.


## Syntax Examples

TODO: This syntax is a placeholder, I'd love to hear your thoughts.

```sql
CREATE CHANGEFEED EMIT TABLE tweets
    WITH sink=kafka, kafka_bootstrap_server=<address>
CREATE CHANGEFEED EMIT TABLE tweets VALUES FROM (1) TO (2) WITH <...>
CREATE CHANGEFEED EMIT DATABASE db TO KAFKA WITH <...>
CREATE CHANGEFEED EMIT db1.t1, db2.* TO KAFKA WITH <...>
```

TODO: Should we allow changefeeds to be named? Tables and indexes are named, but
system jobs are currently not. It may be a nice shorthand for the job id in
PAUSE/RESUME JOB, etc.


## Options

- `WITH column_whitelist=<...>` Comma-separated list of which columns to include
  in emitted records. If unspecified, all columns are emitted.
- `WITH envelope=<...>` Kafka records have a key and a value. The key is always
  set to the primary key of the changed row and the value’s contents are
  controlled with this option.
  - `envelope=none` [DEFAULT] The new values in the row (or empty for a
    deletion). This works with Kafka log compaction. The system may internally
    need to read additional column families.
  - `envelope=key_only` The value is always empty. No additional reads are ever needed.
  - `envelope=diff` The value is a composite with the old row value and the new
    row value. The old row value is empty for INSERTs and the new row value is
    empty for DELETEs. The system will internally need additional reads.
- `WITH format=<...>`
  - `format=json` [DEFAULT] The record key is a serialized JSON array. The
    record value is a serialized JSON object mapping column names to column
    values.
  - `format=avro` The record key is an Avro array, serialized in the binary
    format. The record value is an Avro record, mapping column names to column
    values, serialized in the binary format.
- `WITH sink=kafka` Kafka is currently the only sink option.
- `WITH kafka_bootstrap_servers=<address>` The bootstrap address of the Kafka
  cluster to use.
- `WITH kafka_topic_prefix=<...>` A string to prepend to the names of the topics
  used by this changefeed.
- `WITH kafka_schema_topic=<...>` A Kafka topic to emit all schema changes to.
- `WITH confluent_schema_registry=<address>` The address of a schema registry
  instance. Only allowed with `format=avro`. When unspecified, no schema
  registry is used.


## Full-text index example: Elasticsearch

A user creates a json changefeed with no envelope and kafka_topic_prefix set to
`crdb_`. A kafka consumer tails every topic starting with `crdb_`, writing them
to Elasticsearch. The “mapping” is either derived automatically from the json or
manually set beforehand by the user. The `topic+partition+offset` tuple is used
as Elasticsearch’s external version; because we guarantee that keys are written
to the changefeed in order, this is sufficient to prevent clobbering. The
Confluent [ElasticsearchSinkConnector] can be configured exactly with this
logic, so the user doesn’t need to write any code.


## Data warehouse example: Amazon Redshift

TODO: This could use more detail.

A user creates an avro changefeed with no envelope. A kafka consumer group tails
the resulting topics by repeating the following steps in a loop.

1. A timestamp T in the future is picked.
2. The consumers consume from Kafka, writing any records with CockroachDB
   timestamps before T to an S3 folder. Any records with timestamps after T
   are written to the next folder.
3. Once _every_ partition has received a close notification with a timestamp
   greater than T, then no more records with timestamps less than T will ever
   come.
4. The data in the T bucket is loaded via Redshift bulk load. It is
   transactionally consistent across the entire CockroachDB cluster.
5. Repeat

​​
## Altering the changefeed

Only paused changefeed jobs may be altered, so the user runs `PAUSE JOB
<jobid>`. (TODO: Should we error or pause the job for them if they try to alter a
running job?). Then the following `ALTER CHANGEFEED <jobid>`  commands may be
used to change the configuration. After any alterations, the changefeed must be
unpaused with `RESUME JOB <jobid>` to restart the changefeed.

`ALTER CHANGEFEED <jobid> EMIT FROM SYSTEM TIME <timestamp>` can be used to
manually adjust the highwater mark forward or back. When resumed, the changefeed
will emit any changes after the given timestamp, but will not emit the initial
catch-up. To do this, the changefeed must be cancelled and replaced with a new
one.

`ALTER CHANGEFEED <jobid> SET kafka_bootstrap_server=<address>` can be used to
migrate Kafka clusters.


## Geographically distributed changefeed

A user has geo-partitioned their data for GDPR compliance and cannot let EU data
out of the EU. One changefeed is created for EU partitions, pointing at a Kafka
cluster located in the EU and another changefeed is created for the remaining
partitions. Alternately, one changefeed is created for everything, but the
`column_whitelist=<...>` option is used to whitelist the columns that don’t
contain protected data.


## Failures

If Kafka is unavailable or partially unavailable for a short time, the
changefeed will buffer records until it’s back up, spilling to disk if
necessary. One table failing will not affect other tables.

If this buffer gets larger than the `TODO` cluster setting, the changefeed will
enter a “stalled” state and will tear down its internal connections to reduce
load on the cluster, polling to discover when the sink recovers. When the sink
becomes available again, the changefeed will restart from where it left off.

It is crucial that Kafka recovers before the watched rows exit their respective
[garbage collection] windows (default 25 hours) with enough time for the
changefeed to catch up. This is similar to the restriction on [incremental
backups]. The user can temporarily raise the garbage collection config, at the
cost of additional disk usage, but the system will not do this automatically.

If the sink is down for too long, some data may be lost and the changefeed will
be marked as failed. User intervention is required. Either a new changefeed can
be created with the existing topics and an appropriate `AS OF SYSTEM TIME`
(which will leave a hole of missing data) or a new changefeed can be restarted
with an empty sink (a clean slate). (TODO: It's also technically possible to
recover the relevant data from `revision_history` backups that covers the time
and keys.)

If the sink is underprovisioned and cannot keep up, the changefeed will
eventually fall behind by more than the gc threshold and will fail. Additional
partitions could be created automatically, but are not because the
key-to-partition sharding would change and this would break any consumers that
assume all keys are in the same partition, a common assumption. Instead a metric
is exported to monitor how close to the gc threshold each changefeed’s most
behind timestamp is. Production users should monitor and alert on this metric.


# Reference-level explanation

Each changefeed is a [system job]. When started or unpaused, the job coordinator
sets up a long-running [DistSQL] flow with some new processors described below.
The current 0-100% progress tracking doesn’t match an continuous stream of
updates. It will be replaced by the minimum of all closed timestamps.


## DistSQL flow

Each flow is made of a set of ChangeAggregator processors, each feeding 1:1 into
a KafkaEmitter processor. Each ChangeAggregator is responsible for watching for
changes in a set of disjoint spans, buffering during the initial catch up,
translating kvs into sql rows, and passing along timestamp close notifications.
The KafkaEmitter is responsible for formatting its inputs according to the
configured changefeed options. Progress information in the form of `(span,
timestamp)` pairs is passed back to the coordinator via DistSQL metadata. (TODO:
Is the metadata the right place to do this?).

DistSQL is currently optimized for short-running queries and doesn’t yet have a
facility for resilience to processor errors. Instead, it shuts down the whole
flow. This is unfortunate for CDC, but failures should be relatively infrequent
and the amount of work duplicated when the flow is restarted should be
relatively small, so it’s not as bad as it first sounds. If/when DistSQL's
capabilities change, this will be revisited.

ChangeAggregators are scheduled such that each is responsible for spans local to
the node it’s running on, with all its output going to a colocated KafkaEmitter.
If the data it’s watching moves, the local ChangeFeed will become a remote one,
using bandwidth and adding a network hop to changefeed latency. DistSQL doesn’t
yet have a mechanism for moving or reconfiguring processors in a running flow,
but leaving this state indefinitely is unfortunate. Instead, the job coordinator
will periodically run some heuristics and when it’s advantageous, stop and
restart the flow such that all processors will be doing local watches again. If
the heuristics are not aggressive enough, the user always has an escape hatch of
manually pausing and resuming the job. This restarting of the flow is costly in
proportion to the size of the changefeed and will affect tail latencies, so
something should be done here in future work.


## ChangeAggregatorProcessor

```proto
// ChangeAggregatorSpec is the specification for a processor that watches for
// changes in a set of spans. Each span may cross mutiple ranges.
message ChangeAggregatorSpec {
  message Watch {
    util.hlc.Timestamp timestamp = 1;
    roachpb.Span span = 2;
  }
  repeated Watch watches = 1;
  bool incremental = 2;
}
```

The [ChangeFeed] command sets up a stream of all changes to keys in a given
span. The changes are returned exactly as they are [proposed and sent through
raft]: in RocksDB WriteBatch format, possibly with unrelated data (parts of the
range’s keyspace not being watched by the ChangeFeed and range-local keys).

Passing the raft proposal through unchanged minimizes cpu load on the replica
serving the ChangeFeed. This is not initially critical, since we’ll just be
scheduling the processors on the same node, but gives us flexibility later to
make changefeeds extremely low-impact on production traffic. (Note that a
ChangeFeed can be thought of as a slightly lighter weight additional replica.)
Using the same format as proposer-evaluated kv also lesses the chance that we’ll
have tricky rpc migration issues in the future.

The amount of unrelated data returned will be low if most or all of the range’s
keyspace is being watched, but in the worst case (changes touching many keys in
the range), may be high if only a small subset is watched. This means table and
partition watches will be efficient, but the efficiency of single-row watches
will depend on the workload. We may support single-row watches, but are not
optimizing for them yet, so this is okay.

ChangeAggregator runs ChangeFeeds on the leaseholder for now. It should be
possible to run ChangeFeeds on followers in the future, which becomes especially
important when ranges are geographically distributed. However, there are
complications to work through that are out of scope for now. Importantly, this
follower could be the slowest one, increasing commit-to-kafka changefeed
latency. We also don’t currently have common code for geography-aware
scheduling; this would have to be built.

A ChangeFeed is started at a timestamp, likely at least a small amount in the
past. It first registers itself to emit any relevant raft commands as they
commit. These are sent to the (ordered) rpc stream in the same order they are in
the raft log. Second, the ChangeFeed starts a concurrent process to scan and
emit any data in the range between the ChangeFeed timestamp and the timestamp at
which the raft hook was registered. A `StreamState` message is sent over the rpc
stream once this process finishes.

Before `StreamState`, keys can be emitted out of order, so the ChangeAggregator
must buffer them (TODO mem vs disk). After `StreamState`, individual keys are
guaranteed to be emitted in timestamp order, which means no buffering is needed
and ChangeAggregator can immediately emit them as rows.

After `StreamState`, the ChangeFeed will also begin emitting timestamp close
notifications, which are messages that indicate that no kvs with a lower
timestamp (TODO what’s the < vs <= here?) than the one in the close
notification. These are passed on by ChangeAggregator to be used by downstream
consumers.

TODO: There is an ongoing discussion about how ChangeFeed will handle splits,
merges, leaseholder moves, node failures, etc. The tradeoffs will be worked out
in an update to the ChangeFeed RFC and are out of scope here.


### KV to row conversion

TableDescriptors are required to interpret kvs as sql rows. The correctness of
our online schema changes depends on each query holding a “lease” for the
TableDescriptor that is valid for the timestamp it’s executing at. In contrast
to queries, which execute with a distinct timestamp, changefeeds operate
continuously. TODO: How should table leasing work here?

Rows are stored using one kv per [column family]. For tables with one column
family, which is the common case and the default if the user doesn’t specify,
each entry in the ChangeFeed’s WriteBatch is the entire data for the row. For
tables with more than one column family, a followup kv scan will be needed to
reconstruct the row. A similar fetch will be required if the user requests a
changefeed with both the old and new values. ChangeFeed could instead send this
along when necessary, eliminating a network hop and the cost of serving a
request, but it’s preferable to avoid pushing knowledge of column families and
sql rows down to kv.


## KafkaEmitter

KafkaEmitter receives an ordered stream of sql rows, schema changes, and close
notifications. The sql rows are sent to the relevant kafka topic, sharded into
partitions by primary key. When a schema registry is provided, the schema
changes are forwarded. Close notifications are forwarded when requested by the
user.

For easy development and testing, a 1 partition topic will be auto-created on
demand when missing. For production deployments, the user should set the Kafka
option to disallow auto-creation of topics and manually create the topic with
the correct number of partitions. TODO: I think this is how everyone else does
it but should double check.


## Truncate

For efficiency, in CockroachDB and in other databases, truncate is implemented
as a schema change instead of a large number of row deletions. This has
consequences for changefeeds. Consider the [Elasticsearch example] above. It’s
not using schema changes for anything, so rows in a truncated table would
continue to exist in Elasticsearch after truncate.

TODO: It’s not clear what to do here. It would be nice if this “just worked” in
development clusters. Postgres’s logical decoding is not aware of truncate or
schema changes. SQL Server prevents a table with an active changefeed from being
truncated. Oracle GoldenGate operates as SQL statements and forwards the
truncate as a statement. The Debezium MySQL connector stores truncates in the
schema change topic, if it’s used.


## Permissions

To avoid ambiguity and surprise if SELECT permissions are revoked from a user
that created an active changefeed, changefeeds may only be created by admin.


# Drawbacks

## Lots of configuration options

There are a number of options described above for format, envelope, sink
configuration, column whitelisting, schema topics, etc. This is a lot, but they
all seem necessary. Simple cases like mirroring into Elasticsearch should work
without the overhead of avro, schema registries, and message envelopes. At the
same time, we don’t want to limit power by not having the ability for users to
subscribe to the diff of a changed row. Connectors in the Confluent platform and
other CDC implementations also seem to have a large number of configuration
options, so maybe this is just unavoidable.


## Transaction grouped changes

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

Other users will be running in the cloud and want their changefeeds to emit to a
cloud-hosted pubsub.

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


## Exactly once delivery

It might be possible to could offer exactly once delivery by reading Kafka on
restarts to find the last committed values. However, this is exceedingly
expensive in the worst cases.


# Unresolved Questions

- What happens if a row is changed twice in the same transaction?
- Flow control/throttling of the ChangeFeed streams
- Lots of small changefeeds will require some coalescing


# Appendix: Kafka background

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

# Appendix: Background on other CDC impls

TODO

[#2656]: https://github.com/cockroachdb/cockroach/issues/2656
[#6130]: https://github.com/cockroachdb/cockroach/issues/6130
[changefeed]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20170613_change_feeds_storage_primitive.md
[column family]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20151214_sql_column_families.md
[data warehouse example: amazon redshift]: #data-warehouse-example-amazon-redshift
[distsql]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20160421_distributed_sql.md
[elasticsearch-example]: #fulltext-index-example-elasticsearch
[elasticsearchsinkconnector]: https://docs.confluent.io/current/connect/connect-elasticsearch/docs/elasticsearch_connector.html
[full-text index example: elasticsearch]: #fulltext-index-example-elasticsearch
[garbage collection]: https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html#garbage-collection
[incremental backups]: https://www.cockroachlabs.com/docs/stable/backup.html
[jdbc source connector]: https://docs.confluent.io/current/connect/connect-jdbc/docs/source_connector.html
[kafka]: https://kafka.apache.org/intro
[long running jobs]: https://www.cockroachlabs.com/docs/stable/show-jobs.html
[proposed and sent through raft]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20160420_proposer_evaluated_kv.md
[system job]: https://github.com/cockroachdb/cockroach/blob/381e4dafa596c5f3621a48fcb5fce1f62b18c186/docs/RFCS/20170215_system_jobs.md
[transaction grouped changes]: #transaction-grouped-changes
