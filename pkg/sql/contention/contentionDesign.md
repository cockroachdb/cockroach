# Contention overview

## Code:

- sql/contention
- kv/kvserver/concurrency/

## Three main components

- TxnIdCache
- EventStore
- Resolver

## Configs:

1. sql.contention.event_store.resolution_interval: 30 sec
1. sql.contention.event_store.duration_threshold: default 0;

## Workflow

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
<br>
<br>
<br>
<br>

```mermaid
sequenceDiagram
   title Transaction id cache
   connExecutor.recordTransactionStart->>txnidcache.Record: Add to cache with default fingerprint id
   connExecutor.recordTransactionFinish->>txnidcache.Record: Replace the cache with actual fingerprint id
```

<br>
<br>
<br>
<br>
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
<br>
<br>
<br>
<br>

```mermaid
sequenceDiagram
   title Background task 'contention-event-intake'
   event_store.eventBatchChan->>resolver.enqueue: Append to unresolvedEvents
   event_store.eventBatchChan->>event_store.upsertBatch: Add to unordered cache (blockingTxnId, waitingTxnId, WaitingStmtId)
```

<br>
<br>
<br>
<br>
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
