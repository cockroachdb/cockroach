# DB.Put() Detailed Flow Analysis

## Overview

This document provides a comprehensive walkthrough of `DB.Put()` in CockroachDB, from the initial call through transaction creation, coordination, and eventual commit. The flow demonstrates how a simple key-value operation transforms into a distributed transaction with multiple layers of sophistication.

## 1. Entry Point: DB.Put()

The journey begins at `/pkg/kv/db.go:427`:

```go
func (db *DB) Put(ctx context.Context, key, value interface{}) error {
    b := &Batch{}
    b.Put(key, value)
    return getOneErr(db.Run(ctx, b), b)
}
```

**What happens:**
1. Creates a new `Batch` object
2. Adds the Put operation to the batch
3. Executes the batch via `db.Run()`
4. Extracts and returns any error

## 2. Batch Execution: DB.Run()

Located at `/pkg/kv/db.go:1000`:

```go
func (db *DB) Run(ctx context.Context, b *Batch) error {
    if err := b.validate(); err != nil {
        return err
    }
    return sendAndFill(ctx, db.send, b)
}
```

**What happens:**
1. Validates the batch
2. Calls `sendAndFill()` which converts the batch to a `BatchRequest`
3. Delegates to `db.send()`

## 3. Non-Transactional Send: DB.send()

Located at `/pkg/kv/db.go:1171`:

```go
func (db *DB) send(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
    return db.sendUsingSender(ctx, ba, db.NonTransactionalSender())
}
```

**What happens:**
1. Uses the `NonTransactionalSender()` which is actually a `CrossRangeTxnWrapperSender`
2. This wrapper handles the case where non-transactional operations might span ranges

## 4. Implicit Transaction Creation: CrossRangeTxnWrapperSender

Located at `/pkg/kv/db.go:217`:

```go
func (s *CrossRangeTxnWrapperSender) Send(
    ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
    if ba.Txn != nil {
        log.KvExec.Fatalf(ctx, "CrossRangeTxnWrapperSender can't handle transactional requests")
    }

    // First attempt: send without a transaction
    br, pErr := s.wrapped.Send(ctx, ba)
    if _, ok := pErr.GetDetail().(*kvpb.OpRequiresTxnError); !ok {
        return br, pErr
    }

    // If operation requires a transaction (e.g., spans ranges), wrap in transaction
    err := s.db.Txn(ctx, func(ctx context.Context, txn *Txn) error {
        txn.SetDebugName("auto-wrap")
        b := txn.NewBatch()
        b.Header = ba.Header
        for _, arg := range ba.Requests {
            req := arg.GetInner().ShallowCopy()
            b.AddRawRequest(req)
        }
        err := txn.CommitInBatch(ctx, b)
        br = b.RawResponse()
        return err
    })
    // ...
}
```

**What happens:**
1. First attempts to send without a transaction
2. If the operation requires a transaction (cross-range operations), creates an implicit transaction
3. Re-executes the batch within the transaction context

## 5. Transaction Creation and Initialization

When a transaction is needed, `db.Txn()` creates a transaction coordinator:

Located at `/pkg/kv/db.go:1051`:

```go
func (db *DB) Txn(ctx context.Context, retryable func(context.Context, *Txn) error) error {
    nodeID, _ := db.ctx.NodeID.OptionalNodeID()
    txn := NewTxnWithAdmissionControl(ctx, db, nodeID, source, priority)
    txn.SetDebugName("unnamed")
    return runTxn(ctx, txn, retryable)
}
```

This creates a `TxnCoordSender` via `/pkg/kv/kvclient/kvcoord/txn_coord_sender.go:236`:

```go
func newRootTxnCoordSender(
    tcf *TxnCoordSenderFactory, txn *roachpb.Transaction, pri roachpb.UserPriority,
) kv.TxnSender {
    tcs := &TxnCoordSender{
        typ: kv.RootTxn,
        TxnCoordSenderFactory: tcf,
    }
    tcs.mu.txnState = txnPending
    // ... interceptor initialization ...
}
```

## 6. Interceptor Stack Assembly

The `TxnCoordSender` builds an interceptor stack (`/pkg/kv/kvclient/kvcoord/txn_coord_sender.go:289`):

```go
tcs.interceptorAlloc.arr = [...]txnInterceptor{
    &tcs.interceptorAlloc.txnHeartbeater,      // Keeps transaction alive
    &tcs.interceptorAlloc.txnSeqNumAllocator,  // Manages sequence numbers
    &tcs.interceptorAlloc.txnWriteBuffer,      // Buffers writes
    &tcs.interceptorAlloc.txnPipeliner,        // Pipelines operations
    &tcs.interceptorAlloc.txnCommitter,        // Handles commit protocol
    &tcs.interceptorAlloc.txnSpanRefresher,    // Refreshes read spans
    &tcs.interceptorAlloc.txnMetricRecorder,   // Records metrics
}
```

Each interceptor serves a specific purpose in the transaction lifecycle.

## 7. Request Processing Through TxnCoordSender

Located at `/pkg/kv/kvclient/kvcoord/txn_coord_sender.go:510`:

```go
func (tc *TxnCoordSender) Send(
    ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
    tc.mu.Lock()
    defer tc.mu.Unlock()
    tc.mu.active = true

    // Validation checks
    if pErr := tc.maybeRejectClientLocked(ctx, ba); pErr != nil {
        return nil, pErr
    }

    // Clone transaction proto
    ba.Txn = tc.mu.txn.Clone()

    // Send through interceptor stack
    br, pErr := tc.interceptorStack[0].SendLocked(ctx, ba)

    // Update transaction state based on response
    pErr = tc.updateStateLocked(ctx, ba, br, pErr)

    // Handle EndTxn requests (commit/rollback)
    if req, ok := ba.GetArg(kvpb.EndTxn); ok {
        et := req.(*kvpb.EndTxnRequest)
        if et.Commit {
            if pErr == nil {
                tc.finalizeAndCleanupTxnLocked(ctx)
                tc.maybeCommitWait(ctx, false)
            }
        } else {
            tc.finalizeAndCleanupTxnLocked(ctx)
        }
    }
    // ...
}
```

## 8. Interceptor Chain Processing

Each interceptor processes the request in sequence:

### 8.1 TxnHeartbeater
- Starts heartbeat loop for long-running transactions
- Prevents transaction timeout
- Only active on root transactions

### 8.2 TxnSeqNumAllocator
- Assigns sequence numbers to each operation
- Ensures operation ordering within transaction
- Critical for MVCC correctness

### 8.3 TxnWriteBuffer
- Buffers writes locally before sending
- Implements write pipelining optimization
- Reduces network roundtrips

### 8.4 TxnPipeliner
- Converts writes to async intents
- Tracks in-flight writes
- Enables parallel write execution

### 8.5 TxnCommitter
- Implements parallel commit optimization
- Manages staging status
- Coordinates distributed commit

### 8.6 TxnSpanRefresher
- Tracks read spans
- Attempts to refresh read timestamp on conflicts
- Avoids unnecessary transaction restarts

### 8.7 TxnMetricRecorder
- Records transaction metrics
- Tracks latencies and retries
- Bottom of stack to observe all transformations

## 9. Distributed Execution

After passing through interceptors, the request reaches the `DistSender`:

1. **Range Resolution**: Determines which range(s) contain the key
2. **Leaseholder Routing**: Routes request to leaseholder replica
3. **MVCC Write**: Performs actual write at storage layer
4. **Intent Recording**: Creates write intent (provisional value)

## 10. Commit Protocol

For a simple Put, the commit happens implicitly:

### 10.1 Single-Range Fast Path (1PC)
If the Put affects only one range:
```go
// Detected in TxnCoordSender.Send()
if ba.IsSingleEndTxnRequest() && !tc.hasAcquiredLocksOrBufferedWritesLocked() {
    return nil, tc.finalizeNonLockingTxnLocked(ctx, ba)
}
```
- Combines write and commit in single RPC
- No separate commit phase needed
- Optimal performance path

### 10.2 Multi-Range Path (2PC)
If the Put might affect multiple ranges:

**Phase 1: Staging**
- Write intents with STAGING status
- Record all intent locations
- Prepare transaction record

**Phase 2: Commit**
- Update transaction record to COMMITTED
- Asynchronously resolve intents to committed values
- Clean up transaction state

### 10.3 Parallel Commit Optimization
When enabled, allows commit to proceed without waiting for all intent resolutions:
- Write staging timestamp
- Record in-flight writes
- Allow reads to determine commit status implicitly

## 11. Response Path

The response travels back through the interceptor stack in reverse:

1. **TxnMetricRecorder**: Records completion metrics
2. **TxnSpanRefresher**: May retry if timestamp can be advanced
3. **TxnCommitter**: Handles commit confirmation
4. **TxnPipeliner**: Tracks completed writes
5. **TxnWriteBuffer**: Clears buffered writes
6. **TxnSeqNumAllocator**: Updates sequence counter
7. **TxnHeartbeater**: Stops heartbeat if transaction complete

## 12. Error Handling and Retries

The system handles various error conditions:

### 12.1 Retriable Errors
- **WriteIntentError**: Conflict with another transaction
- **TransactionRetryError**: Timestamp pushed, can retry
- **RangeKeyMismatchError**: Range split during operation

Handled by automatic retry loop:
```go
func runTxn(ctx context.Context, txn *Txn, retryable func(context.Context, *Txn) error) error {
    for {
        err := retryable(ctx, txn)
        if err == nil {
            return nil
        }
        if !txn.IsRetryableError(err) {
            return err
        }
        txn.PrepareForRetry(ctx)
    }
}
```

### 12.2 Non-Retriable Errors
- **TransactionAbortedError**: Transaction already aborted
- **IntentMissingError**: Expected intent not found
- Various data corruption errors

These terminate the transaction immediately.

## Key Optimizations

1. **Write Pipelining**: Writes proceed without waiting for confirmation
2. **Parallel Commits**: Commit without waiting for all intents resolved
3. **Read Refresh**: Advance read timestamp to avoid restarts
4. **1PC Fast Path**: Single-range transactions skip staging phase
5. **Intent Resolution**: Asynchronous cleanup after commit

## Summary

A simple `DB.Put()` call involves:
1. Creating an implicit transaction if needed
2. Building a sophisticated interceptor stack
3. Coordinating distributed execution
4. Managing MVCC intents and timestamps
5. Implementing optimized commit protocols
6. Handling various failure scenarios with automatic retries

This architecture provides:
- **Strong consistency**: Serializable isolation
- **High performance**: Multiple optimizations
- **Fault tolerance**: Automatic retry and recovery
- **Distributed coordination**: Seamless multi-range operations

The complexity is hidden behind a simple API, but understanding the flow helps in debugging, performance tuning, and understanding system behavior.