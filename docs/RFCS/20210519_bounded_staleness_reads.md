- Feature Name: Bounded Staleness Reads
- Status: in-progress
- Start Date: 2021-05-19
- Authors: Nathan VanBenschoten, Rebecca Taft, Oliver Tan
- RFC PR: #66020.
- Cockroach Issue: #25405.

# Summary

Bounded staleness reads are a form of historical read-only queries that use a
dynamic, system-determined timestamp, subject to a user-provided staleness
bound, to read from nearby replicas while minimizing data staleness. They
provide a new way to perform follower reads off local replicas to minimize query
latency in multi-region clusters.

Bounded staleness reads complement CockroachDB's existing mechanism for
performing follower reads, which was originally proposed in [this RFC](20180603_follower_reads.md)
and later adopted in [this RFC](20181227_follower_reads_implementation.md).
This original form of follower reads is more precisely classified as an
exact staleness read, meaning that the read occurs at a statically chosen
timestamp, regardless of the state of the system.

Exact staleness and bounded staleness reads can exist side-by-side, as there are
trade-offs between the two in terms of cost, staleness, and applicability. In
general, bounded staleness reads are more powerful because they minimize
staleness while being tolerant to variable replication lag, but they come at the
expense of being more costly and usable in fewer places.

Bounded staleness queries are limited in use to single-statement read-only
queries, and only a subset of read-only queries at that. They will be accessed
similarly to exact bounded staleness reads - through a pair of new functions
that can be passed to an `AS OF SYSTEM TIME` clause:
- `SELECT ... FROM ... AS OF SYSTEM TIME with_min_timestamp(TIMESTAMP)`
- `SELECT ... FROM ... AS OF SYSTEM TIME with_max_staleness(INTERVAL)`

The approach discussed in this RFC has a prototype in
https://github.com/cockroachdb/cockroach/pull/62239 which, while not identical
to what is proposed here, is similar and demonstrates the high-level changes
that are needed to support bounded staleness reads.

# Motivation

Bounded staleness reads are useful for [all the same reasons](20181227_follower_reads_implementation.md#motivation)
as exact staleness reads. Chiefly, they allow for low-latency reads
off follower replicas (voting or non-voting) in multi-region clusters.

However, there are a few key areas where the two forms of stale reads differ,
and these differences are what motivate the introduction of bounded staleness
reads.

The primary benefit of bounded staleness reads over exact staleness reads is
**availability**. Here, the term availability means two different but related
things. First, bounded staleness reads improve the availability of serving a
read with low-latency from a local replica instead of serving it with
high-latency from the leaseholder. Second, bounded staleness reads improve the
availability of serving a read at all in the presence of a network partition or
other failure event that prevents the SQL gateway from communicating with the
leaseholder. This increase to availability is because bounded staleness reads do
not require users (or the `follower_read_timestamp` builtin function) to guess
at the maximum timestamp that can be served locally and risk being wrong.

The secondary benefit of bounded staleness reads over exact staleness reads is
that they **minimize staleness** when possible. Whereas with exact staleness
reads, which must conservatively pick a historical query timestamp in advance
which provides a sufficiently high probability of being served locally and
without blocking on conflicting writes, with bounded staleness reads, the query
timestamp is chosen dynamically and the least stale timestamp that can be served
locally without blocking is used.

# Technical design

Bounded staleness reads will be exposed through a pair of new functions that can
be passed to an `AS OF SYSTEM TIME` clause:
- `SELECT ... FROM ... AS OF SYSTEM TIME with_min_timestamp(TIMESTAMP)`
- `SELECT ... FROM ... AS OF SYSTEM TIME with_max_staleness(INTERVAL)`

`with_min_timestamp(TIMESTAMP)` defines a minimum timestamp to perform the
bounded staleness read at. The actual timestamp of the read may be equal to or
later than the provided timestamp, but can not be before the provided timestamp.
This is useful to request a read from nearby followers, if possible, while
enforcing causality between an operation at some point in time and any dependent
reads.

`with_max_staleness(INTERVAL)` defines a maximum staleness interval to perform
the bounded staleness read with. The timestamp of the read may be less stale,
but can be at most this stale with respect to the current time. This is useful
to request a read from nearby followers, if possible, while placing some limit
on how stale results can be. `with_max_staleness(INTERVAL)` is syntactic sugar
on top of `with_min_timestamp(now() - INTERVAL)`.

## Semantics

### Limitations

Bounded staleness reads will be limited to single-statement ("implicit"),
read-only transactions. This means that explicit transactions, those surrounded
by a `BEGIN` and `COMMIT` statement, can not use bounded staleness reads. The
reason for this limitation is that the selection of the transaction timestamp
for a bounded staleness transaction requires knowledge of which rows will be
read, so the timestamp selection cannot be performed before the SQL gateway has
seen the entire transaction.

Additionally, the initial implementations of bounded staleness reads will be
limited to a subset of all read-only query plans. This limitation will be lifted
incrementally over three distinct stages.
- **Stage 1 - single-scan, single-range queries**
- **Stage 2 - single-scan, multi-range queries**
- **Stage 3 - multi-scan, multi-range queries**

We define a **single-scan** query as one that contains only a single `ScanExpr`,
and by extension, only a single `TableReader`, since we're not using DistSQL, as
mentioned below.

We define a **single-range** query as one that touches data on only a single KV
range. Because range splits can be added between any two rows in a table in
response to size and load factors, this effectively limits queries to those that
touch a single SQL row. To avoid user surprises, we eliminate dynamic factors by
limiting support in stage 1 to queries that are guaranteed to touch a single
row, and, of those, only queries that use a covering index so that they do not
require an index join.

Bounded staleness reads will also be initially prevented from using DistSQL.
This is not a fundamental limitation, but it makes this proposal significantly
simpler and will not come into play until we reach Stage 2. This limitation can
be later lifted if we find it sufficiently important to remove.

### Guarantees

The high-level goal of bounded staleness reads is that, when possible given
their staleness bound, they be served by the replica closest to the gateway
without "blocking". In this context, blocking refers to waiting on a range's
closed timestamp or waiting on conflicting transactions, both of which may
require coordination with the range's leaseholder.

Bounded staleness reads strive to provide this guarantee. They make the claim
that if the closest replica (by latency) to the gateway is reachable, has a
closed timestamp at or above the staleness bound, and has no outstanding intents
at or below the staleness bound and conflicting with the read, the read will be
served without blocking.

Formally, we define the _local_resolved_timestamp_ as a function of a gateway
node and a set of key spans as follows:
```python
relevant_ranges(keys) = filter(lambda range: range.contains(keys), all_ranges)
closest_replica(gw, range) = min(range.repls, lambda repl: latency(gw, repl))
closest_replicas(gw, keys) = map(lambda range: closest_replica(gw, range), relevant_ranges(keys))

min_closed_timestamp(gw, keys) = min(closest_replicas(gw, keys),  lambda repl: repl.closed_timestamp)

min_intent_timestamp_on_repl(repl) = min(repl.intents, lambda intent: intent.timestamp)
min_intent_timestamp(gw, keys) = min(closest_replicas(gw, keys),  min_intent_timestamp_on_repl)

local_resolved_timestamp(gw, keys) = min(min_closed_timestamp(gw, keys), min_intent_timestamp(gw, keys)-1) 
```
Next, we define the _local_resolved_timestamp_ for a given bounded staleness
read query as follows:
```python
maximal_query_footprint(read) = union(read.all_possible_scans_known_at_query_planning_time)
local_resolved_timestamp(read) = local_resolved_timestamp(read.gateway, maximal_query_footprint(read))
```
We then guarantee that if the _local_resolved_timestamp_ of a given bounded
staleness read is greater than or equal to its staleness bound, the read will be
served from the closest replica(s) without blocking.
```python
is_local_and_non_blocking(read) = local_resolved_timestamp(read) >= read.min_timestamp
```

This guarantee is subject to a limitation discussed in [Schema Unavailability](#schema-unavailability).

### Non-Guarantees

#### Minimizing staleness

Bounded staleness reads make **no** hard guarantee about minimizing staleness.
In other words, they do not guarantee that if the _local_resolved_timestamp_ is
greater than the read's staleness bound at the time of the query, the read will
be performed at the _local_resolved_timestamp_. This non-guarantee permits
partial resolved timestamp information to be cached on a gateway for improved
query efficiency, at the expense of freshness.

However, in practice, this property does hold if the bounded staleness read is
able to exercise the [server-side negotiation fast-path](#server-side-negotiation-fast-path),
which is limited to single-range reads.

#### Stronger consistency when possible 

Bounded staleness reads are **not** promoted to strong consistency reads when
the leaseholder for all keys ranges that they touch are in the same region. This
is a guarantee present in [Azure Cosmos DB](#microsoft-azure-cosmos-db) but not
in [Google Spanner](#google-spanner). Strong reads, even from the leaseholder of
a range may require blocking on conflicting transactions, so making this
guarantee would compromise the "non-blocking guarantee" we intend to make here.

## KV implementation

This section discusses the implementation of bounded staleness reads from the
perspective of the KV layer.

### External Interface

At a high-level, the KV client (`kv.DB` and `kv.Txn`) will expose two ways to
interact with bounded staleness reads.

First, it will expose a `NegotiateAndSend` method on a `kv.Txn`. This method
accepts a read-only `BatchRequest` with a `min_timestamp_bound` set in its
`Header`. It will issue the `BatchRequest` in the context of a previously unused
`kv.Txn` while providing the [bounded staleness guarantee](#guarantees).

Second, it will provide access to a `BoundedStalenessNegotiator` instance
through a new `Negtiator` method on a `kv.DB`, which is capable of determining
the `local_resolved_timestamp` for a set of spans.

Together, the additions to the API will look like:
```go
// NegotiateAndSend is a specialized version of Send that is capable of
// orchestrating a bounded-staleness read through the transaction, given a
// read-only BatchRequest with a min_timestamp_bound set in its Header.
//
// The transaction must not have been used before. If the call returns
// successfully, the transaction will have been given a fixed timestamp.
func (*Txn) NegotiateAndSend(
	context.Context, roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error)


// BoundedStalenessNegotiator provides the local resolved timestamp for a
// collection of key spans.
type BoundedStalenessNegotiator interface {
    LocalResolvedTimestamp(
    	ctx context.Context, minTSBound hlc.Timestamp, spans []roachpb.Span,
    ) (hlc.Timestamp, error)
}

// Negotiator provides access to the DB's BoundedStalenessNegotiator.
func (*DB) Negotiator() BoundedStalenessNegotiator
```

### Protobuf API Changes

To support the bounded staleness implementation, we will make three changes to
the KV Protobuf API.

First, we will introduce a new `ReadConsistencyType` called `BOUNDED_STALENESS`.
This `ReadConsistencyType` will return `false` from `RequiresReadLease`, meaning
that requests with this consistency level set will be routed to the nearest
replica for a range by the `DistSender`.

Second, we will introduce a new field on the `BatchRequest` `Header` struct
called `min_timestamp_bound`. This field can only be set when the header's
`ReadConsistency` is set to `BOUNDED_STALENESS` and when the `timestamp` field
is not set.

Third, we will introduce a new `GetResolvedTimestamp` request and response pair.
The intention is for this request type to be sent in `BatchRequests` with an
`INCONSISTENT` consistency level so that it routes to the nearest replica, skips
leaseholder checks, and does not wait in the replica's concurrency manager for
latches. During evaluation, it computes the resolved timestamp over its key span
on the replica that it is evaluating on. This means taking the minimum of the
replica's closed timestamp and the timestamp before any locks in its key span.
The `GetResolvedTimestamp` response type will implement the `combinable`
interface and will take the minimum of any two responses.

#### GetResolvedTimestamp efficiency

`GetResolvedTimestamp` will initially be fairly expensive due to its scan for
intents/locks. This is because intents can currently be interleaved in a range's
data, so scanning for the resolved timestamp of a range is an O(num_keys_in_range)
operation. In the near future, we expect to write a migration to eliminate
interleaved intents and pull all intents into the lock-table as part of
addressing [#41720](https://github.com/cockroachdb/cockroach/issues/41720). When
we do that, it will be possible to make `GetResolvedTimestamp` more efficient,
reducing it to an O(num_locks_in_range) operation.

Once this is possible, we will be able to be more aggressive with our use of
`GetResolvedTimestamp`. This is a soft blocker for progressing beyond [step
1](#limitations) in the implementation of bounded staleness reads, because we
deem the cost of the O(num_keys_in_range) operation to be too expensive to
expose to users.

If even an O(num_locks_in_range) operation is too expensive, even with the
[client-side caching](#implementation-of-boundedstalenessnegotiator) in the
`BoundedStalenessNegotiator`, we will need to explore methods for efficiently
tracking a range's resolved timestamp. We currently do this in a rangefeed's
[`resolvedTimestamp`](https://github.com/cockroachdb/cockroach/blob/6b10baca35f4afa88469044f28e8ebc81e46c15a/pkg/kv/kvserver/rangefeed/resolved_timestamp.go)
tracker, but this is not currently available on replicas without an active
rangefeed (which comes with other costs).

#### Abandoned intents

If we did nothing special, `GetResolvedTimestamp` would find that abandoned
intents in its keyspace result in indefinite resolved timestamp stalls because
successive `GetResolvedTimestamp` requests would continue to find the same
abandoned intents. This is the reason why rangefeed's periodically push all
intents on their range.

We could do something similar here, but there is an easier solution to dealing
with abandoned intents. `GetResolvedTimestamp` will attach any intents that it
finds that are below the range's closed timestamp to its
`LocalResult.EncounteredIntents` set. This will cause the range to
asynchronously push and resolve the intents, which will eventually lead to the
cleanup of any abandoned intents, ensuring progress of a range's resolved
timestamp.

### Implementation of Txn.NegotiateAndSend

`(*Txn).NegotiateAndSend` is responsible for orchestrating bounded-staleness
reads, given a read-only `BatchRequest`. It does so by breaking `BatchRequests`
apart into two phases - _negotiation_ and _execution_.

The negotiation phase of a bounded-staleness read determines the timestamp to
perform the read using the `GetResolvedTimestamp` requests. This is coordinated
by the `BoundedStalenessNegotiator`. If the negotiator finds that the local
resolved timestamp across the read's spans is above its `min_timestamp_bound`,
it chooses this timestamp to perform the scan. Otherwise, it chooses the
`min_timestamp_bound` itself to perform the scan. It then calls
`SetFixedTimestamp` on the `Txn` receiver with the negotiated timestamp.

The execution phase of a bounded-staleness read uses the negotiated read
timestamp from the previous phase to execute the read. It clears the request's
`min_timestamp_bound` field and sets the request's `timestamp` field. It then
issues the request through its `Txn`.

#### BatchRequest preconditions

A BatchRequest will be rejected by the `(*Txn).NegotiateAndSend` unless it meets
all the following conditions:
- `Header.Timestamp` is empty
- `Header.MinTimestampBound` is not empty
- `Header.Txn` is set to nil
- `Header.ReadConsistency` is set to `BOUNDED_STALENESS`
- `ReadOnly()` returns true
- `IsLocking()` returns false
- `Txn` has not been used before
- `Txn` does not already have a fixed timestamp

#### Server-side negotiation fast-path

For single-range bounded-staleness reads, we introduce a fast-path to avoid two
rounds of communication. If the entire batch lands on a single range, we
negotiate its timestamp on the server. We do this by catching BatchRequests with
`MinTimestampBound` fields set in `Store.Send` (before the call to
`SetActiveTimestamp`) and issue a `GetResolvedTimestamp` to the targeted range.
If this succeeds, we clear the batch's `MinTimestampBound` and set its
`Timestamp` to the negotiated timestamp. We then evaluate the `BatchRequest`
with the expectation that it cannot block.

This fast-path provides two benefits:
1. it avoids two network hops in the common-case where a bounded staleness read
   is targeting a single range. This will be an important performance optimization
   for single-row point lookups.
2. it provides stronger guarantees around minimizing staleness during bounded
   staleness reads. This was explicitly [not a guarantee that we want to make](#minimizing-staleness)
   across the board, but it is a useful property to have where it is possible
   to provide cheaply.

Note that for [step 1](#limitations) in the implementation of bounded staleness
reads, all reads that are permitted by higher-level query plan checks will hit 
this fast-path.

##### DistSender single-range check

To allow the `(*Txn).NegotiateAndSend` to optimistically hit the server-side
negotiation fast-path, we will augment `DistSender` to reject cross-range
batches that have a `ReadConsistency` level of `BOUNDED_STALENESS` and a
non-empty `MinTimestampBound` using a structured error. This is analogous to the
current use of `errNo1PCTxn`.

For the remainder of this RFC, we will refer to this error as
`errNoCrossRangeServerSideNegotiation`.

#### Implementation of BoundedStalenessNegotiator

The `BoundedStalenessNegotiator` is responsible for determining the local
resolved timestamp over a set of key spans, given some minimum timestamp bound.
It does so using `GetResolvedTimestamp` requests. However, to avoid the cost of
sending `GetResolvedTimestamp` requests on every cross-range bounded staleness
read, the `BoundedStalenessNegotiator` aggressively caches resolved timestamp
spans in an LRU interval cache.

Calls into the `BoundedStalenessNegotiator` provide a minimum timestamp bound
and the spans the caller intends to read from. Any requested span that is
contained by a cache entry's span which has a resolved timeststamp equal to or
greater than the minimum timestamp bound skips the `GetResolvedTimestamp`
request and uses the cached span directly. Otherwise, the negotiator issues a
`GetResolvedTimestamp` request over the span and uses the result to update its
cache. Identical, concurrent `GetResolvedTimestamp` requests are coalesced using
a `singleflight` group.

Once a resolved timestamp has been established for each span, the minimum is
chosen and compared against the original timestamp bound. The maximum of these
two timestamps is then as the result of negotiation:
```
max(minTimestampBound, min(overlappingResolvedTimestamp))
``` 

Readers may notice that there is no asynchronous process that refreshes the
`BoundedStalenessNegotiator`'s cache to keep it fresh. We only refresh the cache
in response to negotiation requests that are not satisfied with the existing
contents of the cache. This means that multi-range reads with large staleness
bounds may get progressively more stale until they no longer satisfy timestamp
bounds. This is the flexibility provided by [not making a hard guarantee about
minimizing staleness](#minimizing-staleness).

We may reconsider this decision in the future in response to customer feedback,
opting to introduce some form of asynchronous refresh mechanism. Alternatively,
we may also consider adding a TTL to these cache entries in addition to the LRU
eviction policy so that even without an asynchronous refresh mechanism, we can
still place some kind of staleness bound (for lack of a better word) on resolved
timestamp entries in the cache. This would, in turn, allow us to guarantee that
a multi-range bounded staleness read can only lag behind its
_local_resolved_timestamp_ for some configured threshold.

The `BoundedStalenessNegotiator` is exposed by the `kv` package so that it can
be used in the most general form of bounded-staleness reads, [step 3](#limitations),
where queries can issue multiple scans. In such cases, negotiation will take
place across multiple scans before execution has started. This precludes the use
of the server-side negotiation fast-path, but this is probably not a problem,
because a complex query that performs multiple scans will almost always be
touching multiple ranges.

#### Control logic

Incorporating the server-side negotiation fast-path, the control logic in
`(*Txn).NegotiateAndSend` will look something like:
```
check BatchRequest preconditions
attempt to send immediately
- if success, return
- if error (not errNoCrossRangeServerSideNegotiation), return
- else if errNoCrossRangeServerSideNegotiation, continue
use BoundedStalenessNegotiator to negotiate timestamp
- check cache for each span
- any span not in cache or too stale, send GetResolvedTimestamp req
-- populate cache with responses
- compute local_resolved_timestamp 
given local_resolved_timestamp from negotiator
- if local_resolved_timestamp >= min_timestamp_bound
-- set txn fixed timestamp to local_resolved_timestamp
- else
-- set txn fixed timestamp to min_timestamp_bound
clear min_timestamp_bound
send batch through txn
```

## SQL implementation

This section discusses the implementation of bounded staleness reads from the
perspective of the SQL layer, which uses `(*Txn).NegotiateAndSend` and
`(*DB).Negotiator` to navigate the process of evaluating a bounded staleness
read.

### SQL Parser

The change will introduce two new SQL builtin functions: 
- `with_min_timestamp(TIMESTAMP) -> TIMESTAMP`
- `with_max_staleness(INTERVAL) -> INTERVAL`

These functions will need special casing in `tree.EvalAsOfTimestamp`.

As with all AS OF SYSTEM TIME queries, those that specify one of the two bounded
timestamp functions will need to be read-only. However, unlike all other permitted
arguments to AS OF SYSTEM TIME, these two functions will not be supported in the
explicit transaction form of AS OF SYSTEM TIME (`BEGIN AS OF SYSTEM TIME ...`,
`SET TRANSACTION AS OF SYSTEM TIME`, etc.). These two functions will only be
permitted in implicit transactions.

When an implicit transaction is run with one of these two functions, the
transaction's `min_timestamp_bound` will be computed and retained for planning
and execution.

### SQL Execution

SQL execution is in charge of using the additions to the KV API to execute a
bounded staleness read. It does so in one of two ways. Either it passes the
entire statement's batch of KV gets/scans through `(*kv.Txn).NegotiateAndSend`
all at once along with the `min_timestamp_bound`, or it uses the
`kv.BoundedStalenessNegotiator` returned from `(*kv.DB).Negotiator` and the
`min_timestamp_bound` to negotiate a query timestamp ahead of time and then runs
the statement's batches with this timestamp.

The approach chosen depends on whether the query contains a single `ScanExpr`,
and by extension, a single `TableReader`. If so, the `kv.Txn` will be handed the
`BatchRequest` with the statement's `min_timestamp_bound` and the responsibility
for negotiating the query timestamp through its new `NegotiateAndSend` method,
which enables the [server-side negotiation fast-path](#server-side-negotiation-fast-path). 
This will be the only approach supported in step 1 and step 2 of the
implementation.

Once we reach step 3, we will add support for multi-scan queries and DistSQL.
This requires SQL Execution to use the `BoundedStalenessNegotiator` and the
`min_timestamp_bound` to negotiate the query timestamp ahead of evaluation. The
query timestamp can then be fixed on the query's `kv.Txn` using
`(*kv.Txn).SetFixedTimestamp` Since the query timestamp has already been
negotiated, the BatchRequests passed to `(*kv.Txn).Send` will have an empty
`min_timestamp_bound` field, but will still have its `ReadConsistency` set to
`BOUNDED_STALENESS`.

#### Table Schemas 

When a bounded staleness read is planned, it will use the most recent table
descriptors it has available, like any other strongly consistent read. The
validity window of these descriptors, `[modification_time, present)`, will act
as additional constraints on the maximum staleness of the query. In practice,
this means that the `min_timestamp_bound` of a query will be forwarded by the
modification time of the most recent version of each of the table descriptors
that it uses.

There are downsides to this simple approach, which are discussed in the [Schema
Unavailability](#schema-unavailability) section.

#### Observability into query timestamp

We propose providing observability into a bounded-staleness read's timestamp
through use of the `cluster_logical_timestamp`. Specifically, because the
process of timestamp negotiation (lazy or eager) sets a fixed timestamp on the
query's `kv.Txn`, this method will just work. However, it will only work if
called after timestamp negotiation. Otherwise, the query will return an error.

This means that the way to access the dynamic timestamp of a bounded staleness
read looks something like:
```sql
SELECT *, cluster_logical_timestamp() FROM t AS OF SYSTEM TIME with_max_staleness('10s') WHERE id = 1;
```

#### DistSQL

DistSQL will be disabled for the initial implementation of this project. For
more, see the corresponding discussion in the [limitations section](#limitations).

### SQL Optimizer

The optimizer will be responsible for enforcing that bounded staleness can only
be used in specific cases. This allows the project to incrementally add support
for more and more complex queries in the [progression outlined above](#limitations).

To start (for step 1), we will limit the use of bounded staleness to implicit
transactions that touch a single range. From the perspective of the optimizer,
this means that at most a single row can be accessed by a single scan, and no
index or lookup joins are allowed.

The optimizer will attempt to block transactions that don’t meet the
requirements in three different places: (1) in optbuilder, to catch invalid
queries that definitely don’t meet the requirements, (2) in
`TryPlaceholderFastPath`, to catch some additional invalid queries at the prepare
phase, and (3) in the execbuilder, to block all invalid queries that weren’t
caught in previous steps.

#### Checks in optbuilder

As part of semantic analysis, the optbuilder will be able to run certain
best-effort checks to see if a query might satisfy the requirements for bounded
staleness. For example, if a query joins multiple tables, it definitely cannot
use bounded staleness (in step 1 or 2). Although in theory a join could be
eliminated by a transformation rule, to start we will err on the side of
allowing fewer queries rather than more queries. If there is a need to support
more queries, these checks can easily be relaxed later. If any of these checks
fail, the optbuilder will return an error to the client.

#### Checks in TryPlaceholderFastPath

TryPlaceholderFastPath is used to allow certain simple prepared queries with
placeholders to be fully optimized at the prepare stage. These simple queries
are largely the same ones that would be able to use bounded staleness, so it
makes sense to add checks here to catch problems at prepare time. In addition to
the checks already included in TryPlaceholderFastPath, we’ll need to add an
additional check (in step 1) that the resulting PlaceholderScanExpr scans at
most one row.

#### Checks in execbuilder

The execbuilder will catch all remaining invalid queries that weren’t caught in
previous phases of optimization. One way to make this work is to treat bounded
staleness as a “hint” that the optimizer must respect. The optimizer already
makes use of hints to disallow certain types of plans, so we can reuse some of
that infrastructure here to avoid building plans that won’t be allowed for
bounded staleness. For example, in step 1 we can use existing hints to disallow
lookup and index joins. Similar to TryPlaceholderFastPath, we can also add an
explicit check in the execbuilder that the ScanExpr scans at most one row.

The execbuilder also knows whether or not a statement is part of an implicit
transaction (allowAutoCommit is true for implicit transactions). The execbuilder
should return an error if a statement using bounded staleness is not part of an
implicit transaction.

#### TimestampNegotiation operator

Once we reach stage 3, we will need to perform an eager timestamp negotiation
phase before query execution for multi-scan statements. This will be modeled as
a new `TimestampNegotiation` (name pending) operator that sits at the top of a
query plan, similar to a CTE (i.e., WITH expression). The operator will be
configured with the query's maximal set of spans and will use the `DB`'s
`BoundedStalenessNegotiator` to configure the transaction timestamp before
allowing the rest of the query to run.

## Drawbacks

### System-Range Unavailability

The current proposal provides a method for reliably accessing data in a table
whose data ranges are unavailable. However, it still relies on consistent access
to system ranges. This is true both at the KV layer (e.g. range addressing may
be needed for routing) and at the SQL layer (e.g. table descriptors may be
needed for query processing, permission data may be needed for authorization).

This dramatically restricts the scope of the proposal, but it also limits its
utility. As proposed, bounded staleness reads are not a catch-all solution for
stale reads during any form of unavailability. Instead, bounded staleness reads,
insofar as they relate to read availability, are a solution to provide improved
availability when table data is unavailable but system data is available. We
would expect such scenarios either because system ranges are given a higher
replication factor than table data (the default) and are therefore more
resilient, or because users in a multi-region database have configured table
data to have ZONE survivability (the default) while their system data continues
to replicate across zones (the default).

We will have to document this subtlety carefully and make it clear when and how
bounded-staleness reads can be used as an availability mechanism.

In the context of this limitation and the next, "unavailability" means that
statements will hang until either connectivity is restored or a statement
timeout is reached.

### Schema Unavailability

The current proposal does not do anything particularly sophisticated around
schema changes. Instead, it proposes that we forward a bounded staleness read's
minimum timestamp to the maximum last modification time of all relevant table
descriptors. While simple, this does lead to a period of compromised
availability after a schema change. This means that during periods of strong
read unavailability (e.g. gateway partitioned from leaseholder), a schema change
can lead to bounded staleness unavailability because it places a floor on
staleness.

It is open for discussion whether we want to do anything more sophisticated here,
like introduce a retry loop that steps back across schema versions until it finds
a version whose implied key spans are all sufficiently resolved.

## Alternatives

### SQL-level Asynchronous Replication

There are a few competing proposals that detail lifting asynchronous replication
up to the level of SQL triggers or changefeeds. The proposals then suggest creating
per-region "derived tables" that are updated asynchronously in response to changes
to a parent table. https://github.com/cockroachdb/cockroach/pull/62120 is one such
alternative.

The compelling part about these proposals is that they avoid the need to serve
stale reads from the perspective of the KV layer, opting instead for
"consistent" reads on these region-local derived tables. However, this is also
their eventual downfall. In introducing a higher-level form of asynchronous
replication through triggers or changefeeds, they haven't actually solved the
problems of consistency, they have effectively just promoted concerns about
consistency to a different level — one which is even less able to speak
cohesively about the topic. At least in KV, we have existing notions of ranges
with a leader, voting followers, non-voting followers, replication backpressure,
and a closed timestamp.

This proposal keeps asynchronous replication in KV and then provides a way to
query a consistent prefix of this data.

### Range-level Resolved Timestamp Tracking

Much of the complexity around the `GetResolvedTimestamp` and the
`BoundedStalenessNegotiator` falls out of the cost of computing the resolved
timestamp for a range. This is also why we [don't feel comfortable](#getresolvedtimestamp-efficiency)
exposing anything more than single-row bounded staleness reads until we remove
interleaved intents and reduce the cost of computing a range's resolved timestamp
from O(num_keys_in_range) to O(num_locks_in_range).

If we were to maintain a Range-level resolved timestamp in the similar way to
how we maintain a Range-level closed timestamp then determining the resolved
timestamp for a range would be a constant-time operation. So the evaluation of
`GetResolvedTimestamp` on a single range would be a constant-time operation.
This would make parts of this design easier. For instance, it would reduce the
importance of aggressive caching in the `BoundedStalenessNegotiator`. However,
it also wouldn't allow us to skip any of this design, so we do not consider
the resolution of this alternative to be a design blocker.

Maintaining this resolved timestamp state would also come in handy in various
other projects, such as [recovery from insufficient quorum](https://github.com/cockroachdb/cockroach/issues/17186).

To this point, we have never bottomed out on a single design for how to
introduce such tracking in a manner that is cheap enough to tolerate.
Explorations of design alternatives is a good candidate for a separate RFC.

### Tracking Resolved Timestamp in RangeCache

Related but not identical to the previous option.

Instead of adding a new interval cache in the
[`BoundedStalenessNegotiator`](#implementation-of-boundedstalenessnegotiator),
we could store resolved timestamp state in the `RangeCache`. The `RangeCache`
currently stores a set of Range's `RangeDescriptor`, `Lease`, and
`ClosedTimestampPolicy`, all keyed by the Range's span of keys. It also has a
reasonable cache eviction policy that limits its memory footprint.

The reason this is related to the previous option is because it would require
`GetResolvedTimestamp` requests to grab an entire Range's resolved timestamp at
a time. These requests would not be able to update the cache if they only
grabbed the resolved timestamp over a portion of the Range, because the minimum
granularity of resolved timestamp tracking would be at the level of a Range's
key bounds. So we would either need to make `GetResolvedTimestamp` constant-time
(see above), or we would need to be ok with coalesced `GetResolvedTimestamp`
requests sent by the `BoundedStalenessNegotiator` potentially computing the
resolved timestamp over a larger span than strictly necessary, which only
seems feasible if we address the first part of [GetResolvedTimestamp
efficiency](#getresolvedtimestamp-efficiency).

# Unresolved questions

## Should we introduce exact staleness timestamp bound functions?

Currently, exact staleness reads are accessed by passing a `TIMESTAMP` or
negative `INTERVAL` directly to an `AS OF SYSTEM TIME` clause.

For instance, an exact staleness read at a specific timestamp looks like:
```sql
SELECT * FROM t AS OF SYSTEM TIME '2021-01-02 03:04:05'
```

Similarly, an exact staleness read with a specific amount of staleness looks
like:
```sql
SELECT * FROM t AS OF SYSTEM TIME '-30s'
```

We should keep these two forms of exact staleness reads.

However, now that we are introducing builtin functions for bounded staleness
reads, we may want to introduce corresponding builtin functions for exact
staleness reads as well. These have the benefit of being more explicit.

These would look like:
```sql
SELECT * FROM t AS OF SYSTEM TIME with_timestamp('2021-01-02 03:04:05')
```
and
```sql
SELECT * FROM t AS OF SYSTEM TIME with_exact_staleness('30s')
```

Resolution: yes, but not in the initial implementation.

## Should we change the implementation of follower_read_timestamp()

The follower_read_timestamp() builtin function is currently a helper function to
access an exact staleness read with a query timestamp that we expect to be
sufficiently stale such that it can be served by follower replicas. All of the
[downsides](#motivation) of exact staleness reads apply. Chiefly, this timestamp
is a guess and it could be wrong under regimes with unexpectedly high
replication lag. However, all of the upsides of exact staleness reads also
apply. Chiefly, that they are cheap and broadly applicable.

Do we change the implementation of `follower_read_timestamp()` to use bounded
staleness instead of exact staleness? The answer is "probably not", because of
the various [limitations](#limitations) placed on bounded staleness reads.

Resolution: no, this would be a breaking change.

# Licensing

Bounded staleness reads will be licensed under the CCL and will require a valid
enterprise license to use. This parallels exact staleness reads, which are
allowed under the BSL but will not result in follower reads unless run under the
CCL with a valid enterprise license.

# Prior art

## Google Spanner

- [Spanner: Google's Globally Distributed Database](https://research.google/pubs/pub44915/)
- [Spanner, TrueTime and the CAP Theorem](https://research.google/pubs/pub45855/)
- [Cloud Spanner Docs: Timestamp bounds](https://cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness)
- [Cloud Spanner Docs: Reads](https://cloud.google.com/spanner/docs/reads#choosing_timestamp_bounds)
- [Cloud Spanner Docs: Life of Cloud Spanner Reads & Writes](https://cloud.google.com/spanner/docs/whitepapers/life-of-reads-and-writes)
- [Zanzibar: Google’s Consistent, Global Authorization System](https://research.google/pubs/pub48190/)

Spanner is a scalable, globally distributed database developed by Google that
shares many architectural design points with CockroachDB. Notably, Spanner, like
CockroachDB, is a partitioned consensus system that uses locking and 2-phase
commit to enforce serializability and clock synchronization to enforce
consistency. Both systems use quorum replication within partitions, use
multi-version concurrency-control, and provide lock-free consistent reads.

For all of these reasons, the fact that Spanner provides bounded staleness reads
is of significant interest to this proposal. It is illustrative to explore why
and how Spanner has implemented the feature.

### Internal Spanner (circa 2013)

Spanner exposes two forms of reads-only transactions - strongly consistent
read-only transactions and snapshot reads. Both forms of reads are lock-free.

Strongly consistent read-only transactions behave as expected. The only
meaningful deviation from how CockroachDB handles them today is that while the
read does always need to touch the leader(s) of the data being read to determine
the timestamp to read at, it doesn't necessarily need to read that data from the
leader. Instead, it can take the timestamp provided by the leader and then wait
on a follower replica for its safe time (synonymous with CockroachDB's resolved
timestamp) to exceed the read timestamp. This is a fascinating form of load
balancing, but is largely unrelated to this proposal, as this approach provides
little latency or availability benefit.

Snapshot reads differ from read-only transactions in that they expose weaker
consistency guarantees in exchange for often being able to be served directly
from follower replicas without a hop to a leader replica to gather a read
timestamp. Spanner exposes two forms of snapshots reads. A client can either
specify a timestamp for a snapshot read, or provide an upper bound on the
desired timestamp’s staleness and let Spanner choose a timestamp. In either
case, the execution of a snapshot read proceeds at any replica that is
sufficiently up-to-date.

### Cloud Spanner

Cloud Spanner, in an effort to make this all more understandable, has renamed
some of these concepts. However, the ideas here remain mostly the same.

Cloud Spanner offers two forms of read:
- A _strong read_ is a read at a current timestamp and is guaranteed to see all
  data that has been committed up until the start of this read. Cloud Spanner
  defaults to using strong reads to serve read requests.
- A _stale read_ is read at a timestamp in the past. If your application is
  latency sensitive but tolerant of stale data, then stale reads can provide
  performance benefits.

Stale reads are then broken into two categories:
- _Bounded staleness:_ read a version of the data that's no staler than a bound.
- _Exact staleness:_ read the version of the data at an exact timestamp, e.g. a
  point in time in the past, though you can specify a timestamp for a time that
  hasn't passed yet. (If you specify a timestamp in the future, Cloud Spanner will
  wait for that timestamp before serving the read.)

To choose the type of read to use, various Spanner [client drivers](https://pkg.go.dev/cloud.google.com/go/spanner)
allow users [to configure](https://pkg.go.dev/cloud.google.com/go/spanner#ReadOnlyTransaction.WithTimestampBound)
a per-request [timestamp bound](https://pkg.go.dev/cloud.google.com/go/spanner#TimestampBound).

#### Bounded Staleness Contract

Cloud Spanner's documentation says the following about bounded staleness reads:

> Bounded staleness modes allow Cloud Spanner to pick the read timestamp, subject to a user-provided staleness bound. Cloud Spanner chooses the newest timestamp within the staleness bound that allows execution of the reads at the closest available replica without blocking.

> All rows yielded are consistent with each other: if any part of the read observes a transaction, all parts of the read see the transaction. Boundedly stale reads are not repeatable: two stale reads, even if they use the same staleness bound, can execute at different timestamps and thus return inconsistent results.

> Boundedly stale reads execute in two phases. The first phase negotiates a timestamp among all replicas needed to serve the read. In the second phase, reads are executed at the negotiated timestamp.

> As a result of this two-phase execution, bounded staleness reads are usually a little slower than comparable exact staleness reads. However, they are typically able to return fresher results, and are more likely to execute at the closest replica.

> Because the timestamp negotiation requires up-front knowledge of which rows will be read, it can only be used with single-use reads and single-use read-only transactions.

It also says the following about exact staleness reads, in relation to bounded
staleness reads:

> These modes do not require a "negotiation phase" to pick a timestamp. As a result, they execute slightly faster than the equivalent boundedly stale concurrency modes. On the other hand, boundedly stale reads usually return fresher results.

#### Bounded Staleness API

Cloud Spanner exposes the bounded staleness timestamp bound through two interfaces:

```go
// MaxStaleness returns a TimestampBound that will perform reads and queries at
// a time chosen to be at most "d" stale.
func MaxStaleness(d time.Duration) TimestampBound

// MinReadTimestamp returns a TimestampBound that bound that will perform reads
// and queries at a time chosen to be at least "t".
func MinReadTimestamp(t time.Time) TimestampBound
```

### Impact on Availability

There is very little discussion of the impact of stale reads on availability.
This simply does not seem to be a motivating use case for stale reads.

The only mention I was able to find about this topic is in Eric Brewer's
retrospective about [Spanner, TrueTime, and the CAP Theorem](https://research.google/pubs/pub45855/),
where he mentions:

> Snapshot reads are thus a little more robust to partitions. In particular, a
> snapshot read will work if:
>
> 1. There is at least one replica for each group on the initiating side of the partition, and
> 2. The timestamp is in the past for those replicas.
>
> The latter might not be true if the leader is stalled due to a partition, and
> that could last as long as the partition lasts, since it might not be possible
> to elect a new leader on this side of the partition. During a partition, it is
> likely that reads at timestamps prior to the start of the partition will succeed
> on both sides of the partition, as any reachable replica that has the data
> suffices.

### Uses of Bounded Staleness

One of the best showcases of effective use of Spanner's snapshot reads with
bounded staleness is in Google's [Zanzibar paper](https://research.google/pubs/pub48190/).
Zanzibar is a globally distributed authorization system built for storing and
evaluating access control lists. The system makes heavy use of bounded staleness
to provide global low-latency read access while respecting causal ordering
between ACL and content updates.

Zanzibar presents a host of interesting ideas around scaling a global system
built on Spanner, along with descriptions of the kinds of tricks such a system
can play to sustain high throughput and minimize latency while providing
meaningful consistency guarantees.

## Microsoft Azure Cosmos DB

- [Consistency levels in Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/consistency-levels#bounded-staleness-consistency)
- [Azure Cosmos DB - Tunable Consistency Models](https://www.youtube.com/watch?v=-4FsGysVD14)

Azure Cosmos DB offers five well-defined consistency levels, which dictate the
behavior of reads and writes. From strongest to weakest, the levels are:
* Strong
* Bounded staleness
* Session
* Consistent prefix
* Eventual

Each level provides availability and performance tradeoffs. The default read and
write consistency level differs depending on the API in use (MongoDB, Apache
Cassandra, Gremlin, or Azure Table storage), but this default can be overridden.

In bounded staleness consistency, the reads are guaranteed to honor the
"consistent-prefix" guarantee. The reads might lag behind writes by at most "K"
versions (that is, "updates") of an item or by "T" time interval, whichever is
reached first.

Cosmos DB differentiates the consistency guarantees of bounded staleness reads
based on whether the partition being read has its leader local or remote to the
client. For local bounded staleness reads, the consistency guarantee is promoted
to Strong consistency. For remote bounded staleness reads, the consistency
guarantee is demoted to Consistent Prefix consistency.

Because Cosmos DB provides tunable consistency levels for both reads and writes,
it is able to tailor its write path to its desired read consistency level. This
leads to an interesting trade-off and design decision. The primary reason to use
bounded staleness consistency over strong consistency is to reduce write
latency, as the weaker consistency level allows replication between regions to
occur asynchronously instead of synchronously. This allows for low-latency
writes while offering total global order outside of a "staleness window". As the
staleness window approaches for either time or updates, whichever is closer, the
service will throttle new writes to allow replication to catch up and honor the
consistency guarantee. This means that instead of losing read availability or
increasing read latency if replication lag grows due to heavy write traffic,
writes will be throttled to keep replication lag below the staleness threshold.

There's not a lot more to learn from Cosmos DB regarding bounded staleness reads
as it relates to this proposal. However, there are some interesting ideas in
Cosmos DB about tunable consistency levels and the latency, throughput, and
availability properties that fall out which are worth reading about if one gets
the chance.

## Probabilistic Bounded Staleness

- http://pbs.cs.berkeley.edu/

Not directly related. Discusses probabilistic bounds that can be placed on the
recency of data returned under eventually consistency with unbounded staleness.

"It turns out that, in practice, and in the average case, eventually consistent
data stores often deliver consistent data."
