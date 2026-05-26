# The observability layer in CockroachDB

Last update: May 2023

This document provides an architectural overview of the observability layer in
CockroachDB. 

Original author: j82w

Table of contents:

- [SQL Activity](#overview-of-tables-for-sql-activity-page)
- [Contention Overview](#contention-overview)

## Overview of tables for SQL Activity page

### Design version < 22.2.8

1. UI will automatically call CockroachDB endpoint when users went to the page.
1. The gateway node sends the query to the system statistics table and in memory crdb_internal table.
1. The gateway node joins all the results and return all of them to frontend.
1. Frontend would auto-refresh the results, and do all the sorting in the browser.

### Design version == 22.2.8

[99044](https://github.com/cockroachdb/cockroach/pull/99044) SQL Activity page now display only persisted stats
when selecting to view fingerprints. This means data just recently executed
might take up to 10min to show on the Console.

1. UI will automatically call CockroachDB endpoint when users went to the page.
1. The gateway node sends the query to only the system statistics table. Avoiding the in memory join overhead.
1. Frontend would auto-refresh the results, and do all the sorting in the browser.


### Design version >= 22.2.9

master [98815](https://github.com/cockroachdb/cockroach/pull/98815) & 22.2 backport [99403](https://github.com/cockroachdb/cockroach/pull/99403)  SQL Activity page adds new search criteria which requires a limit and sort to be specified.

1. User must specify which column to sort by and click the apple button for the UI to call the http endpoint on CockroachDB.
1. The gateway node sends the query to only the system statistics table. Avoiding the in memory join overhead.
1. The UI limit the results to the top 100 queries by default with max of 500.
1. User must do a manual refresh to get new results.

### Design version >= 23.1

[98885](https://github.com/cockroachdb/cockroach/pull/98885) & [100807](https://github.com/cockroachdb/cockroach/pull/100807) Add new system activity tables and update job

1. User must specify which column to sort by and click the apple button for the UI to call the http endpoint on CockroachDB.
1. The gateway node sends the query to the system activity table. If no results are found it falls back to the system statistics table.
1. The UI limit the results to the top 100 queries by default with max of 500.
1. User must do a manual refresh to get new results.

### Performance improvements
The results below were calculated using a test cluster with 9 nodes and 100k-115k rows on our statistics tables, making a request to return the results for the past 1h.

| Version information    | Latency to load SQL Activity page |
|------------------------|-----------------------------|
| Before any change      | 1.7 minutes                 |
| Changes on 22.1 & 22.2 | 9.9 seconds                 |
| Changes on 23.1        | ~500ms                      |

The performance gains are from:
1. Avoiding the fan-out to all nodes.
1. Avoiding the virtual table which filters and limits do not get pushed down to.
1. Limiting the results to the top 100 by default.
1. In 23.1 using new activity table which caches the top 500 by the 6 most popular columns.



### crdb_internal statistics table
- [crdb_internal.cluster_statement_statistics](https://github.com/cockroachdb/cockroach/blob/e0c35b4b4388ec4efc1711815f8b666e0d148d8c/pkg/sql/crdb_internal.go#LL6254C1-L6254C56)
- [crdb_internal.cluster_transaction_statistics](https://github.com/cockroachdb/cockroach/blob/e0c35b4b4388ec4efc1711815f8b666e0d148d8c/pkg/sql/crdb_internal.go#L6658)

1. Contains the in memory statistics that have not been flushed to the system statistics tables
1. Expensive operation
   1. GRPC fan-out to all nodes. 
   1. Filters are not pushed down. All the rows are returned to gateway node, a virtual table is created, and then filters are applied.

### Statistics tables
- system.statement_statistics [PRIMARY KEY (aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name, node_id)
  USING HASH WITH (bucket_count=8)](https://github.com/cockroachdb/cockroach/blob/8f10f78aed7606edd454e886183bf22c74a3153e/pkg/sql/catalog/systemschema/system.go#LL552C26-L553C39)
- system.transaction_statistics [PRIMARY KEY (aggregated_ts, fingerprint_id, app_name, node_id)](https://github.com/cockroachdb/cockroach/blob/8f10f78aed7606edd454e886183bf22c74a3153e/pkg/sql/catalog/systemschema/system.go#LL606C26-L606C88)
  
1. Each node has background [go routine](https://github.com/cockroachdb/cockroach/blob/8f10f78aed7606edd454e886183bf22c74a3153e/pkg/sql/sqlstats/persistedsqlstats/provider.go#L115) that collects stats and [writes](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlstats/persistedsqlstats/flush.go) it to statistics table every 10 minutes.
1. Statistics tables are aggregated by 1 hour and there is a row per a node to avoid contention.
1. Separate [SQL compaction job](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sqlstats/persistedsqlstats/compaction_scheduling.go) runs to delete the rows when table grows above the default [1 million rows](https://github.com/celiala/cockroach/blob/6cdaf96d8ec6eb7636551a9f1ea7fc06d80e5f1d/pkg/sql/sqlstats/persistedsqlstats/cluster_settings.go#L57).

### Activity tables 
- system.statement_activity [PRIMARY KEY (aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name)](https://github.com/cockroachdb/cockroach/blob/8f10f78aed7606edd454e886183bf22c74a3153e/pkg/sql/catalog/systemschema/system.go#L654)
- system.transaction_activity [PRIMARY KEY (aggregated_ts, fingerprint_id, app_name)](https://github.com/cockroachdb/cockroach/blob/8f10f78aed7606edd454e886183bf22c74a3153e/pkg/sql/catalog/systemschema/system.go#LL701C26-L701C80)

1. Created to improve the performance of the ui pages. 
   1. UI groups by fingerprint. Have to aggregate all the rows from based on node id.
   1. Users focus on top queries. Computing top queries is expensive and slow.
1. Activity tables have top 500 based on 6 different properties. 
   1. Limited to 6 properties because there is currently 19 properties total. Calculating and storing all 19 properties is to expensive.
1. Activity tables are updated via the [sql_activity_update_job](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/sql_activity_update_job.go)
   1. Job coordinates running it on a single node.
   1. Provide fault tolerance if a node fails.
1. Activity update job is triggered when the local node [statistics flush is done](https://github.com/cockroachdb/cockroach/blob/8f10f78aed7606edd454e886183bf22c74a3153e/pkg/sql/sql_activity_update_job.go#L99).
   1. This guarantees no contention with the local node flush which has the overhead of doing the activity update job. Other nodes can have contention with activity update job.
1. Activity table does not have node id as a primary key which aligns with UI and reduces cardinality. 
1. Activity update job has 2 queries.
   1. Number of rows are less than max top * number of columns to cache.
      1. The [query](https://github.com/cockroachdb/cockroach/blob/e0c35b4b4388ec4efc1711815f8b666e0d148d8c/pkg/sql/sql_activity_update_job.go#L275) is a more efficient query by avoiding doing the top part of the query which is not necessary since we know there are less rows.
   1. Number of rows are greater than max top * number of columns to cache.
      1. The [query](https://github.com/cockroachdb/cockroach/blob/e0c35b4b4388ec4efc1711815f8b666e0d148d8c/pkg/sql/sql_activity_update_job.go#L432) has to do the necessary sort and limits to get the top of each column.

```mermaid
sequenceDiagram
    box less than max top * num columns cached
    participant sqlActivityUpdater.transferAllStats
    end
    box greater than max top * num columns cached
    participant sqlActivityUpdater.transferTopStats
    end
    PersistedSQLStats->>sqlActivityUpdater: Statistics flush is done via channel
    sqlActivityUpdater->>sqlActivityUpdater.compactActivityTables: Removes rows if limit is hit
    sqlActivityUpdater->>sqlActivityUpdater.transferAllStats: if less than 3000 rows
    sqlActivityUpdater->>sqlActivityUpdater.transferTopStats: if greater than 300 rows
```


## Contention Overview

### Code:

- sql/contention
- kv/kvserver/concurrency/

### Three main components

- TxnIdCache
- EventStore
- Resolver

### Configs:

1. sql.contention.event_store.resolution_interval: 30 sec
1. sql.contention.event_store.duration_threshold: default 0;

### Workflow

```mermaid
sequenceDiagram
   title KV layer creates trace
   SQL layer->>KV layer: Initial call to KV layer
   KV layer->>lock_table_waiter: Transaction hit contention
   lock_table_waiter->>contentionEventTracer.notify: verify it's new lock
   contentionEventTracer.notify->>contentionEventTracer.emit: Adds ContentionEvent to trace span
   KV layer->>SQL layer: Return the results of the query
   SQL layer->>KV layer: if tracing is enabled then network call to get trace
   KV layer->>SQL layer: returns traces
```

<br>

```mermaid
sequenceDiagram
   title Transaction id cache
   connExecutor.recordTransactionStart->>txnidcache.Record: Add to cache with default fingerprint id
   connExecutor.recordTransactionFinish->>txnidcache.Record: Replace the cache with actual fingerprint id
```

<br>

```mermaid
sequenceDiagram
   title Contention events insert process
   executor_statement_metrics->>contention.registry: AddContentionEvent(ExtendedContentionEvent)
   contention.registry->>event_store: addEvent(ExtendedContentionEvent)
   event_store->>ConcurrentBufferGuard: buffer with eventBatch with 64 events
   ConcurrentBufferGuard->>eventBatchChan: Flush to batch to channel when full
```

<br>

```mermaid
sequenceDiagram
   title Background task 'contention-event-intake'
   event_store.eventBatchChan->>resolver.enqueue: Append to unresolvedEvents
   event_store.eventBatchChan->>event_store.upsertBatch: Add to unordered cache (blockingTxnId, waitingTxnId, WaitingStmtId)
```

<br>

```mermaid
sequenceDiagram
   title Background task 'contention-event-resolver' 30s with jitter
   event_store.flushAndResolve->>resolver.dequeue: Append to unresolvedEvents
   resolver.dequeue->>resolver.resolveLocked: Batch by CoordinatorNodeID
   resolver.resolveLocked->>RemoteNode(RPCRequest): Batch blocking txn ids
   RemoteNode(RPCRequest)->>txnidcache.Lookup: Lookup the id to get fingerprint
   RemoteNode(RPCRequest)->>resolver.resolveLocked: Return blocking txn id & fingerprint results
   resolver.resolveLocked->>LocalNode(RPCRequest): Batch waiting txn ids
   LocalNode(RPCRequest)->>txnidcache.Lookup: Lookup the id to get fingerprint
   LocalNode(RPCRequest)->>resolver.resolveLocked: Return waiting txn id & fingerprint results
   resolver.resolveLocked->>resolver.resolveLocked: Move resolved events to resolved queue
   resolver.dequeue->>event_store.flushAndResolve: Return all resolved txn fingerprints
   event_store.flushAndResolve->>event_store.upsertBatch: Replace existing unresolved with resolved events
```
