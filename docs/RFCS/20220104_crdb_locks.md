# KV Lock Observability RFC

* Feature Name: `crdb_locks`
* Status: accepted
* Start Date: 2022-01-04
* Authors: Alex Sarkesian
* RFC PR: [#75541](https://github.com/cockroachdb/cockroach/pull/75541)
* Cockroach Issue: [#67589](https://github.com/cockroachdb/cockroach/issues/67589)

[TOC levels=1-3 markdown]: #

# Table of Contents
- [Summary](#summary)
- [Terminology](#terminology)
- [Motivation](#motivation)
- [Limitations](#limitations)
- [Technical Design](#technical-design)
  * [Virtual Table](#virtual-table)
  * [KV API](#kv-api)
    + [Fault Tolerance](#fault-tolerance)
    + [Performance Considerations](#performance-considerations)
  * [Concurrency Control](#concurrency-control)
  * [PostgreSQL Compatibility](#postgresql-compatibility)
- [Alternatives](#alternatives)
- [Use Cases](#use-cases)
  * [Use Case Matrix](#use-case-matrix)
  * [Examples](#examples)
- [Open Questions](#open-questions)
- [Future Work](#future-work)

# Summary

This design doc RFC proposes the implementation of a virtual `crdb_locks` table 
to enable observability of a point-in-time view of lock holders within a given 
cluster.  Such a view would be able to show which transactions are holding 
locks on which spans across the ranges of a cluster, as well as which 
transactions may be waiting on a given lock.  This information, in conjunction 
with other virtual tables, would allow a user to identify the individual 
transactions and queries causing other transactions to wait on a lock at a 
given point in time.  The `crdb_locks` virtual table would provide a 
client-level view into the Lock Table of each KV range’s Concurrency Manager.  This 
also means that the view will only incorporate locks managed by the Lock 
Table, and not request-level latches or replicated locks that are not 
represented in the in-memory Lock Table.

# Terminology

| Term                     | Explanation |
| ------------------------ | ----------- |
| Sequencing               | Processing a request through Concurrency Control.       |
| Transaction              | A potentially long-lived operation containing multiple requests that commits atomically.  Transactions can hold locks, and release them on commit or abort.        |
| Request                  | In Concurrency Control, a set of KV operations (i.e. `kv.Batch`) potentially belonging to a transaction that, upon sequencing, can take latches, locks, or wait in lock wait queues.        |
| Latch                    | A request-level, in-memory mechanism for mutual exclusion over key spans between requests.  They are only held over the span of a single request, and are dropped once the request completes (or enters a lock wait queue).        |
| Lock                     | A transaction-level mechanism with varying durability for mutual exclusion on a single key between transactions.  Obtained via sequencing a request, and held for the lifetime of a transaction.  They may or may not be tracked in the in-memory lock table.        |
| Lock Table (“LT”)        | An in-memory, btree-based structure tracking a subset of locks, and the associated wait queues for each lock, across a given range.  Locks in the LT may or may not be held, but “empty” locks (without holders or wait queues) are GC’d periodically.  Replicated locks that do not have wait queues are typically not tracked in the LT.        |
| Replicated Lock          | A durable lock represented in persistent storage by what is commonly known as an “intent” (`TxnMeta`).  This type of lock may or may not be tracked in the in-memory lock table - as mentioned above, a replicated lock is tracked in the lock table if there are wait queues made up of other transactions waiting to acquire this lock.         |
| Unreplicated Lock        | A lock that is only represented in the lock table, and will not persist in case of node failure or lease transfer.  Used in `[Get/Scan]ForUpdate`.        |
| Replicated Lock Keyspace | The separate keyspace in which replicated locks, commonly known as “separated intents”, are stored; in the Local keyspace of a range.  This is entirely separate from the Lock Table.  Example intent key in the replicated lock keyspace: `/Local/Lock/Intent/Table/56/1/1169/5/3054/0`.        |
| Lock Strength            | CRDB only supports Exclusive Locks for read & read/write transactions (despite enums including Shared and Upgrade types).        |
| Lock Wait Queue          | A queue of requests (which may or may not be transactional) waiting on a single key lock.  This queue forms when a request is sequenced that attempts to obtain a lock that is already held.        |

# Motivation

As a developer, database administrator, or other CockroachDB user, it is 
extremely useful to be able to obtain a point-in-time view of locks currently 
held, as well as identify which connections and/or transactions are holding 
these locks.  Ideally, a user would be able to visualize which statements or 
transactions are blocking others, and for how long the waiting transactions 
have been waiting.

While we provide some mechanisms to visualize what operations are in progress 
in a cluster, particularly the `crdb_internal.cluster_[queries, transactions, sessions]` 
virtual tables, these do not allow for a user to investigate and pinpoint what 
is currently causing contention.  The same goes for the Contention Events 
features, which are focused on showing historical contention (for requests that 
have already completed) over time, aggregated across queries and sessions, 
rather than on current lock contention.  A feature to view point-in-time lock 
state and contention is therefore currently missing, and has been 
[requested in the past](https://github.com/cockroachdb/cockroach/issues/67589).

There are a number of use cases for visualizing contention in a running 
database or cluster, as elaborated below in [Use Cases](#use-cases).  These 
use cases exist for both engineers and TSEs as well as users and administrators 
of CockroachDB, and while ``crdb_locks`` is intended for use by any type of 
user looking to investigate contention among lockholders in a cluster, it is 
one of several tools that can be used to investigate contention, and how it 
fits in with other tools is described further below in [Alternatives](#alternatives).  The 
particular use case that this feature targets especially is viewing 
point-in-time, live contention at the SQL user level, and as such can provide a 
“first line” tool to investigate issues on a running cluster.

A comparable feature that exists in PostgreSQL is known as [`pg_locks`](https://www.postgresql.org/docs/current/view-pg-locks.html), 
and one that exists in Sybase is known as [`sp_locks`](https://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.dc36273.1570/html/sprocs/X34215.htm).

```
postgres=# select locktype, relation::regclass, transactionid, mode, pid, granted from pg_locks order by pid;
   locktype    |     relation      | transactionid |       mode       |  pid  | granted
---------------+-------------------+---------------+------------------+-------+---------
 relation      | student           |               | AccessShareLock  | 30622 | t
 relation      | student           |               | RowExclusiveLock | 30622 | t
 virtualxid    |                   |               | ExclusiveLock    | 30622 | t
 relation      | student           |               | SIReadLock       | 30622 | t
 transactionid |                   |           502 | ExclusiveLock    | 30622 | t
 transactionid |                   |           502 | ShareLock        | 30626 | f
 relation      | student           |               | SIReadLock       | 30626 | t
 relation      | student           |               | AccessShareLock  | 30626 | t
 relation      | student           |               | RowExclusiveLock | 30626 | t
 virtualxid    |                   |               | ExclusiveLock    | 30626 | t
 transactionid |                   |           503 | ExclusiveLock    | 30626 | t
 ...
```

# Limitations

Currently, CockroachDB’s concurrency control mechanisms, particularly the Lock 
Table, only track locks which are currently held by transactional requests that 
also have other operations (transactional and non-transactional) waiting to 
acquire those locks.  This means that if we are relying on the Lock Table as 
the source of data, as the Technical Design below specifies, we will have a few 
limitations that should be noted, despite the fact that these limitations may 
be acceptable given the use case of this feature.

1. **Inability to display replicated locks without waiters**.  Replicated locks 
(i.e. write intents) that do not have other operations waiting to acquire them 
are not tracked in the lock table, despite their being persistent in the 
storage engine.  Given that this feature is intended to visualize contention, 
this may be of minimal concern.
2. **Only read-write transactional lockholders can be displayed.**  While 
transactional read-only requests or non-transactional requests can wait for 
locks, only transactional read-write requests (including 
`GetForUpdate/ScanForUpdate`) can obtain them.  This means that transactional 
read-only requests and non-transactional requests will only show up as waiters.
3. **Only locks & lock waiters will be displayed, not latches (or latch waiters).**  This 
is something of a design decision rather than a limitation.  Latches are 
short-lived and only held as long as a single request, rather than for the life 
of a transaction, and thus will be less useful in visualizing contention.
4. **We do not track the queries or statements that obtain locks.**  This is a 
limitation in usage due to the current monitoring capabilities of CockroachDB.  While 
we can visualize the queries that are currently waiting on a lock by 
joining with internal tables such as `crdb_internal.cluster_queries`, since we 
do not keep track of the statement history in an active transaction, we will not 
know which statement in the transaction was the one that originally obtained 
the lock.  For example, imagine an open transaction with 1k+ statements thus 
far, with `UPDATE item SET i_name = 'Blue Suede Shoes' WHERE i_id = 124;` as 
the first statement.  While other transactions that attempt to obtain the lock 
on this key will have waiting queries visible in `cluster_queries`, we will not 
be able to show that this `UPDATE` statement was the one that caused its 
transaction to obtain the lock.
5. **We do not track lock acquisition or lock wait start times.**  While 
this is a current limitation, given that there has already been some [planned work](https://github.com/cockroachdb/cockroach/issues/67619)
around this, we could consider it in the scope of this work to include both of 
these (and thus remove this limitation).

# Technical Design

The `crdb_internal.crdb_locks` table will be implemented as a [virtual schema table](https://github.com/cockroachdb/cockroach/blob/2b7ba8c72bd5031a5ae28945b5906ef0d407e6be/pkg/sql/crdb_internal.go) 
at the SQL level, and will be populated by making KV requests across the ranges 
in the cluster.  Each KV request will be evaluated using the corresponding 
range’s Concurrency Manager, which will populate the response.  These combined 
responses will be used as necessary to populate the `crdb_locks` table.

## Virtual Table

Schema for **`crdb_internal.crdb_locks`**:


```
CREATE TABLE crdb_internal.crdb_locks (
  range_id INT,           -- the ID of the range that contains the lock
  table_id INT,           -- the id of the table to which the range with this lock belongs
  database_name STRING,   -- the name of the individual database
  schema_name STRING,     -- the name of the schema
  table_name STRING,      -- the name of the table
  index_name STRING,      -- the name of the index
  lock_key BYTES,         -- the key this lock protects access to
  lock_key_pretty STRING, -- the pretty-printed key this lock protects access to
  txn_id UUID,            -- the unique ID of the transaction holding the lock (NULL if not held)
  ts TIMESTAMP,           -- the timestamp at which the lock is to be held at
  lock_strength STRING,   -- the type of lock [SHARED, UPGRADE, EXCLUSIVE] (note that only EXCLUSIVE locks currently supported, NULL if not held)
  durability STRING,      -- the durability of the lock [REPLICATED, UNREPLICATED] (NULL if not held)
  granted BOOL,           -- represents if this transaction is holding the lock or waiting on the lock
  contended BOOL,         -- represents if this lock has active waiters
);
```


This table will be populated by issuing a KV API request for all the contended 
locks across the cluster.  If possible, it would be advantageous for 
performance to incorporate filters on `range_id`s or smaller key spans, in 
order to limit the RPCs necessary.

Note that this table will not show latches or latch contention, nor will it 
show replicated locks that are uncontended, as they are not tracked by the lock 
table.  We will also not display empty locks from the lock table (i.e. those 
that are not held and do not have any other requests waiting).  The virtual 
table should support the ability to display all locks or to only display 
**contended locks**, that is, locks with readers/writers in the wait queue, so 
that we can avoid displaying every locked key held by a transaction.  See the 
Note on Keys in [Concurrency Control](#concurrency-control) for more information.

It is also important to note that we may be unable to show the time at 
which a transaction waiting on a lock started waiting.  See Note on Start Wait 
Time in [Concurrency Control](#concurrency-control) below for more information.

## KV API

The KV API will be implemented with a new KV request known as `QueryLocksRequest`.


```
proto


message QueryLocksRequest {
  RequestHeader header = 1 [(gogoproto.nullable) = false, (gogoproto.embed) = true];
  SpanScope span_scope = 2; // [GLOBAL, LOCAL]
}

message LockWaiter {
  TxnMeta waiter_txn_meta = 1;
  bool active_waiter = 2;
  SpanAccess access = 3; // [ReadOnly, ReadWrite]
  // Potentially added fields: start_wait
  // Potentially unnecessary fields: wait_kind, spans, seq_num
}

message LockStateInfo {
  int64 range_id = 2 [(gogoproto.customname) = "RangeID",
      (gogoproto.casttype) = "github.com/cockroachdb/cockroach/pkg/roachpb.RangeID"];

  Key locked_key = 1 [(gogoproto.nullable) = false];
  SpanScope span_scope = 2; // [GLOBAL, LOCAL]
  Strength lock_strength = 3;
  Durability durability = 4;
  bool lock_held = 5;
  TxnMeta lockholder_txn_meta = 6;

  repeated LockWaiter lock_waiters = 7;
}

message QueryLocksResponse {
  ResponseHeader header = 1 [(gogoproto.nullable) = false, (gogoproto.embed) = true];

  repeated LockStateInfo locks = 2;
}
```


The `QueryLocksRequest` will be issued using the KV client for a key span 
representing a single tenant (or perhaps database) in the cluster, such that 
the `DistSender` will issue a KV RPC to the leaseholder of each range, and 
return the response values to be used to populate the rows of the virtual table.

The `DistSender` will handle any instances of range unavailability or lease 
transfer between replicas entirely within the KV client layer, as with other KV requests.


### Fault Tolerance

The design described above means that we will have an all-or-nothing approach 
to responding to this query: either we can get information from all ranges, or 
if there are any unavailable ranges, the request will fail.  These semantics 
are on par with those of a large scan, or of querying `crdb_internal.ranges`, 
and are the default fault tolerance of `DistSender` and the KV client layer as 
a whole.  If necessary, it could be possible to iterate through `[/Meta2, /Meta2Max)` 
to look up the range descriptors for all ranges, issue discrete KV 
`QueryLocksRequest`s for the key span of each range, and then handle each 
response (or error) as necessary.  This would have two downsides however: 
primarily, the distinct semantics of this means that we would need to implement 
such error handling (and potentially a custom mechanism to display which ranges 
had errors) at the SQL level, which doesn’t exist today.  Secondarily, and 
perhaps less critically, this would also require an additional scan over all of 
`Meta2`.  Thus, the default fault tolerance provided by the KV client layer is 
deemed acceptable for an initial implementation.

### Performance Considerations

While we can restrict the requests for an individual query to a single (active) 
database in the cluster, it will be necessary for the KV API request to be made 
on every range in the keyspan.  While this should not cause contention with 
other operations (as we will not hold any locks over the entirety of evaluation), 
a request over the entire keyspan of a single database or cluster may 
nevertheless be a fairly large operation, requiring pagination/byte limits as 
necessary.

## Concurrency Control

At a functional level within the KV leaseholder replica of the range, the 
`QueryLocksRequest` will be sequenced as a normal request by the Concurrency 
Manager (without requesting any latches or locks), and proceed to command 
evaluation.  This is preferred over the Concurrency Manager intercepting the 
request upon sequencing as we can thus ensure that we have checked that this 
replica is the leaseholder for the range.  Once at the evaluation stage, we can 
access the Concurrency Manager to grab the mutex for the Lock Table `btree` for 
each key scope (Global keys, Local keys), clone a snapshot of the 
(copy-on-write) btree, and then release the mutex 
[(similarly to how this is done for collection of Lock Table metrics)](https://github.com/cockroachdb/cockroach/blob/55720d0a4c0dd8030cd19645461226d173eefc2e/pkg/kv/kvserver/concurrency/lock_table.go#L2682).  We 
can then iterate through all of the locks in the tree, populating the fields 
necessary from the `lockState` object as well as each lock’s `lockWaitQueue`.

**Note on Keys**: Unfortunately, since each lock state (i.e. for a single key) 
in the Lock Table only maintains minimal information about the transaction the 
lock is held by, we are unable to track the span requested by the transaction 
and will need to use the individual key.  This is because the `lockState` 
object does not track the `lockTableGuard`, which maintains the full 
[`SpanSet` the request is asking for](https://github.com/cockroachdb/cockroach/blob/55720d0a4c0dd8030cd19645461226d173eefc2e/pkg/kv/kvserver/concurrency/lock_table.go#L343).  To 
avoid returning all of the keys that are contended, we may consider only 
including keys that have readers/writers in the queues.  For instance, consider 
if `txn1` has `Put` operations on keys `[a, d]` and `txn2` has `Put` operations 
on keys `[b, e]`; while keys `b, c, d` will appear in the lock table held by 
`txn1`, the requests in `txn2` will only be waiting in at most 1 queue (i.e. 
`b`), and thus we can represent the two transactions as contending (for the 
moment) on `b`.  [See example here](https://gist.github.com/AlexTalks/1227156558f853bdf7c91a7d7c359b97).

**Note on Start Wait Time**: As the time that a transaction begins waiting on a 
lock is only tracked [within the waiting Goroutine for the purposes of Contention Events](https://github.com/cockroachdb/cockroach/blob/52c2df4fb00d943328dc59df9dc210a40a556ab2/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L165), 
and not maintained within the lock wait queue itself, we will need to modify 
this if we want to be able to display the time spent waiting in our virtual 
table view.  As this would be a highly useful feature, it is likely worth the 
time needed to make this change, but it does not exist at the moment.

**Note on Lock Aquisition Time**: Similar to the above, we do not currently 
track the time a lockholder acquires a lock.  This could be resolved by 
incorporating the [planned work to track this](https://github.com/cockroachdb/cockroach/issues/67619) 
into the scope of this project.

One last point worth noting is that while non-transactional lock holders will 
not show up in the lock table, they _can_ show up in lock wait queues (as 
“blocked” by other transactions).  This also applies to transactional read-only 
requests, with the exception of `GetForUpdate`/`ScanForUpdate` requests (which 
acquire unreplicated locks).

## PostgreSQL Compatibility

While we can implement a PostgreSQL compatibility layer and populate the virtual `pg_locks` table in response to queries, this may not be of much use for two reasons:

1. The basic mechanisms of our concurrency control implementation differs from [that of PostgreSQL](https://momjian.us/main/writings/pgsql/locking.pdf).  
Concepts such as table-level (`relation`) locks, user locks and several others 
do not apply to CockroachDB, while CockroachDB-specific concepts such as 
ranges, lock spans, and more do not apply to the paradigm layed out in 
`pg_locks`.
2. Without implementation of `pg_stat_activity` ([used in conjunction with `pg_locks`](https://wiki.postgresql.org/wiki/Lock_Monitoring)), 
which CockroachDB does not currently support, the use case for `pg_locks` 
alone, given some of the above-mentioned limitations, may not be strong.

For these reasons, a PostgreSQL compatibility layer is deemed out of scope for 
an initial implementation, though may be revisited at a later date.  If we were 
to implement a PostgreSQL-compatible mechanism to populate the (currently 
unimplemented) `pg_locks` table, [following their specification](https://www.postgresql.org/docs/current/view-pg-locks.html), 
our implementation could map as follows:

| `pg_locks` Column         | Values      | CRDB Mapping |
| ------------------------- | ----------- | ------------ |
| `locktype text`           | relation, tuple, transactionid, [more](https://www.postgresql.org/docs/current/monitoring-stats.html#WAIT-EVENT-LOCK-TABLE)  | `transactionid`                  |
| `database oid`            |                                             | `dbOid(db.GetID())`              |
| `relation oid`            |                                             | `tableOid(descriptor.GetID())`   |
| `page int4`               |                                             | `null` (does not apply)          |
| `tuple int2`              |                                             | `null` (does not apply)          |
| `virtualxid text`         |                                             | `null` (does not apply)          |
| `transactionid xid`       |                                             | `txn.UUID()` converted to `xid` (`int32`) |         
| `classid oid`             |                                             | `null` (does not apply)          |
| `objid oid`               |                                             | `null` (does not apply)          |
| `objsubid int2`           |                                             | `null` (does not apply)          |
| `virtualtransaction text` |                                             | `null` (does not apply)          |
| `pid int4`                |                                             | `null` (does not apply)          |
| `mode text`               | AccessShareLock, ExclusiveLock, RowExclusiveLock, SIReadLock, [more](https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-TABLES)  | `ExclusiveLock`, `ShareLock` (if waiter)       |
| `granted bool`            |                                             | `lockState.holder.locked` (or false if a member of `lockWaitQueue`)       |
| `fastpath bool`           |                                             | `false` (does not apply)         |
| `waitstart timestamptz`   |                                             | start time in lock table waiters |

# Alternatives

The biggest alternative solutions to observing contention is to utilize the 
Active Tracing Spans Registry and the [Contention Events framework](https://www.cockroachlabs.com/docs/v21.2/performance-best-practices-overview#understanding-and-avoiding-transaction-contention).  The 
Active Tracing Spans Registry, for which there is 
[currently ongoing work to visualize with a UI](https://github.com/cockroachdb/cockroach/pull/74318), 
would be useful to show traces for currently active operations, including those 
contending on locks, but does not specifically map to the use case a virtual 
table like ``crdb_locks`` would provide.  That said, it will likely be worth it 
to coordinate these efforts, as they can work together to better enable 
CockroachDB users and developers.  The Contention Events framework 
(i.e.`crdb_internal.cluster_contended_*` tables), for which there is 
[also planned/ongoing work](https://github.com/cockroachdb/cockroach/pull/71965), 
is [extremely useful](https://www.cockroachlabs.com/blog/what-is-database-contention/) 
for diagnosing contention in a cluster over time, but as Contention Events are 
only [emitted in tracing spans](https://github.com/cockroachdb/cockroach/blob/52c2df4fb00d943328dc59df9dc210a40a556ab2/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L894) 
_after_ a transaction has finished waiting on a lock, it does not provide 
insight into what is _currently_ blocking a particular transaction.

Given that the Active Tracing Spans Registry is also intended to visualize what 
transactions are actively running (and potentially holding locks), albeit in a 
much more engineer-focused, in-depth manner, it could be theoretically possible 
to implement something like `crdb_locks` using it as infrastructure.  At this 
point in time, however, this may not be the best approach, especially as it 
would likely require more complexity to narrow down the data in the Active 
Tracing Spans into a view like `crdb_locks`, it would be additional indirection 
rather than interfacing with the Lock Table directly, and additionally there 
are currently limitations that restrict viewing the Active Tracing Spans to a 
single node rather than cluster-wide. 

It may be also worth noting that while serializability failures can occur and 
are a common class of failures, in CRDB, they are distinct from (albeit 
potentially caused by) lock contention.  These failures, which are 
`TransactionRetryError`s with reason `RETRY_SERIALIZABLE`, 
can include some information elaborating on what caused the serializability 
failure, but do not include any information on the contention history of the 
transaction.  Nonetheless, [improving these error messages is a goal](https://github.com/cockroachdb/cockroach/issues/41057).  Lock 
contention, on the other hand, should not result in a user-visible error unless 
the client sets a timeout on a given query.  If the user does not specify a 
timeout, a query waiting on a lock could effectively be stalled indefinitely.  When 
timeouts are set, however, in the case of timeout errors we also do not surface 
information about the contention to the user, and as such this error messaging 
cannot be used to investigate lock contention.


# Use Cases

The primary use case targeted is for SQL users - particularly CockroachDB users 
and admins - to be able to visualize live, point-in-time contention across a 
given cluster or a single database in particular, though there are of course 
other users that can take take advantage of this feature.  CockroachDB 
engineers, users or developers attempting to understand the concurrency model, 
TSEs, SREs and others all may find the virtual table to be a useful view into 
what is happening on a given cluster, or what transactions may be blocking 
other operations at any given time.  This can even be especially useful for 
someone with knowledge of CRDB’s internals as a first-line tool to obtain a 
quick glance at what locks are held currently before moving onto some of the 
other tools mentioned above for a deeper investigation.

## Use Case Matrix

|                         | Historical View                         | Live View              |
| ----------------------- | --------------------------------------- | ---------------------- |
| **For Engineers/TSEs**  | Jaeger/etc, Splunk (potentially)        | Active Tracing Spans   |
| **For Users/DB Admins** | Contention Events (via SQL, Dashboards) | `crdb_locks` (via SQL) |

## Examples

To visualize the transactions holding locks with basic information about the client session:
```
SELECT 
  l.database_name,
  l.table_name,
  l.range_id,
  l.lock_key_pretty,
  l.txn_id,
  l.granted,
  s.node_id,
  s.user_name,
  s.client_address
FROM crdb_internal.crdb_locks l
JOIN crdb_internal.cluster_transactions t ON l.txn_id = t.id
JOIN crdb_internal.cluster_sessions s ON t.session_id = s.session_id
WHERE l.granted = true;
```
```
  database_name | table_name | range_id |  lock_key_pretty  |                txn_id                | granted | node_id | user_name | client_address
----------------+------------+----------+-------------------+--------------------------------------+---------+---------+-----------+------------------
  tpcc          | item       |       72 | /Table/62/1/135/0 | ba7c4940-96a5-4064-b650-ed2b7191ab5a |  true   |       2 | root      | 127.0.0.1:59033
```

To visualize transactions which are blocking other transactions:
```
SELECT 
  lh.database_name,
  lh.table_name,
  lh.range_id,
  lh.lock_key_pretty,
  lh.txn_id AS lock_holder,
  lw.txn_id AS lock_waiter
FROM crdb_internal.crdb_locks lh
JOIN crdb_internal.crdb_locks lw ON lh.lock_key = lw.lock_key
WHERE lh.granted = true AND lh.txn_id IS DISTINCT FROM lw.txn_id;
```
```
  database_name | table_name | range_id |  lock_key_pretty  |             lock_holder              |             lock_waiter
----------------+------------+----------+-------------------+--------------------------------------+---------------------------------------
  tpcc          | item       |       72 | /Table/62/1/325/0 | ba7c4940-96a5-4064-b650-ed2b7191ab5a | 55e58b37-e5aa-4e56-a4a6-13ca72dca30b
```

To include a display of the queries that are waiting on a lock:
```
SELECT 
  lh.database_name,
  lh.table_name,
  lh.range_id,
  lh.lock_key_pretty,
  q.query as waiting_query
FROM crdb_internal.crdb_locks lh
JOIN crdb_internal.crdb_locks lw ON lh.lock_key = lw.lock_key
JOIN crdb_internal.cluster_queries q ON lw.txn_id = q.txn_id
WHERE lh.granted = true AND lh.txn_id IS DISTINCT FROM lw.txn_id;
```
```
  database_name | table_name | range_id |  lock_key_pretty  |            waiting_query
----------------+------------+----------+-------------------+--------------------------------------
  tpcc          | item       |       72 | /Table/62/1/325/0 | SELECT * FROM item WHERE i_id = 325
```

To display the number of waiting transactions on a single lock:
```
SELECT 
  l.database_name,
  l.table_name,
  l.range_id,
  l.lock_key_pretty,
  COUNT(*) AS waiter_count
FROM crdb_internal.crdb_locks l
WHERE l.granted=false
GROUP BY l.database_name, l.table_name, l.range_id, l.lock_key_pretty;
```
```
  database_name | table_name | range_id |  lock_key_pretty  | waiter_count
----------------+------------+----------+-------------------+---------------
  tpcc          | item       |       72 | /Table/62/1/325/0 |            1
```

# Open Questions

* Special considerations for tenant SQL pods in Serverless

# Future Work

* Incorporating replicated locks not managed by the Lock Table
* Incorporating contention within the Latch Manager.
* Implementing as part of the information schema and/or with additional SQL syntax such as `SHOW LOCKS`
* Push-down filters for particular ranges, client sessions, etc.
* Observability in Dashboards
