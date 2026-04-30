# Life of a Deadlock

**Generated:** 2026-02-02
**Repository:** https://github.com/cockroachdb/cockroach
**Commit:** 3fc8163287d19e8b3f3b791e3c9badcd474608db
**Branch:** master

## Introduction

This document traces how CockroachDB detects and resolves deadlocks between transactions. A deadlock occurs when two or more transactions form a circular dependency where each transaction is waiting for locks held by another transaction in the cycle. CockroachDB uses a distributed deadlock detection algorithm that runs asynchronously across multiple nodes to identify these cycles and break them by aborting one of the transactions.

This document covers three main aspects:
1. **Transaction Locks**: How transactions acquire and hold locks on keys
2. **Deadlock Detection**: How the system identifies dependency cycles between transactions
3. **Deadlock Resolution**: How deadlocks are broken by force-pushing transactions

## Overview: The Concurrency Manager

CockroachDB's concurrency control is managed by the concurrency manager in [`pkg/kv/kvserver/concurrency/concurrency_control.go`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/concurrency_control.go#L27-L145). This component coordinates three key subsystems:

1. **Latch Manager**: Provides in-memory, short-lived exclusive access to keys during request execution
2. **Lock Table**: Tracks transaction locks (write intents) and manages wait queues
3. **Transaction Wait Queue**: Handles pushing of transactions and deadlock detection

The architecture follows this flow:

```
Request arrives
    ↓
Acquire latches (latchManager)
    ↓
Check lock table for conflicts (lockTable)
    ↓
If conflict found → Enter lock wait queue
    ↓                Drop latches
    ↓                Wait for lock holder
    ↓
Push lock holder (txnWaitQueue)
    ↓
Detect deadlock cycles
    ↓
Break deadlock by force abort
```

## Phase 1: Lock Acquisition and Conflicts

### How Transactions Acquire Locks

When a transaction writes to a key, it creates a **write intent** - a provisional value that acts as an exclusive lock on that key. These intents are stored directly in the MVCC keyspace alongside regular values.

**Entry Point:** [`concurrency_control.go:158`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/concurrency_control.go#L158) - `SequenceReq`

The request sequencing begins at [`Manager.SequenceReq()`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/concurrency_control.go#L188):

```go
// SequenceReq acquires latches, checks for locks, and queues behind and/or
// pushes other transactions to resolve any conflicts.
SequenceReq(context.Context, *Guard, Request, RequestEvalKind) (*Guard, Response, *Error)
```

The sequencing process:

1. **Acquire latches** from the latch manager - these are held throughout request execution
2. **Scan the lock table** for conflicting locks using the lock table guard
3. **If conflicts exist**, drop latches and enter the lock's wait queue
4. **If no conflicts**, proceed to evaluate the request while holding latches

### Lock Table Structure

The lock table is implemented in [`pkg/kv/kvserver/concurrency/lock_table.go`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/lock_table.go). It maintains an in-memory map of keys to their lock holders and associated wait queues.

**Data Structure:** [`lockTableGuardImpl`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/lock_table.go#L397)

```go
type lockTableGuardImpl struct {
    // The transaction associated with this guard
    // The keys this guard needs to access
    // Current state: waiting, waitFor, doneWaiting
}
```

When a request encounters a conflicting lock:
- It enters the lock's **wait queue** behind other waiters
- The wait queue is FIFO - first request to encounter the lock is at the head
- Only the **head of the queue** actively pushes the lock holder
- Other waiters passively wait for updates from the head

### Example: Creating a Deadlock

From [`pkg/sql/tests/deadlock_test.go:88-123`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/sql/tests/deadlock_test.go#L88-L123):

```go
// Transaction 1 writes to key1
tx1.Exec("UPDATE test1 SET age = 1 WHERE id = 1")  // Acquires lock on key1

// Transaction 2 writes to key2
tx2.Exec("UPDATE test2 SET age = 2 WHERE id = 1")  // Acquires lock on key2

// Transaction 2 tries to write to key1 (blocked by tx1's lock)
go tx2.Exec("UPDATE test1 SET age = 2 WHERE id = 1")

// Transaction 1 tries to write to key2 (blocked by tx2's lock)
tx1.Exec("UPDATE test2 SET age = 1 WHERE id = 1")  // DEADLOCK!
```

At this point:
- **tx1** holds lock on `test1/id=1`, waiting for lock on `test2/id=1` (held by tx2)
- **tx2** holds lock on `test2/id=1`, waiting for lock on `test1/id=1` (held by tx1)
- This forms a **dependency cycle**: tx1 → tx2 → tx1

## Phase 2: Waiting on Locks and Initiating Pushes

### Lock Table Waiter

When a request encounters a lock conflict, it enters [`lockTableWaiterImpl.WaitOn()`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L112-L115):

```go
func (w *lockTableWaiterImpl) WaitOn(
    ctx context.Context, req Request, guard lockTableGuard,
) *Error
```

This method implements a **state machine** that handles different waiting states:

1. **`waitFor`**: Waiting on another transaction's lock
2. **`waitElsewhere`**: Lock holder is on a different range
3. **`doneWaiting`**: Conflict resolved, can proceed

### Delayed Push for Deadlock Detection

From [`lock_table_waiter.go:35-75`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L35-L75), transactions wait before pushing:

```go
// LockTableDeadlockOrLivenessDetectionPushDelay sets the delay before pushing
// in order to detect dependency cycles between transactions or coordinator
// failure of conflicting transactions.
var LockTableDeadlockOrLivenessDetectionPushDelay = settings.RegisterDurationSetting(
    settings.SystemOnly,
    "kv.lock_table.deadlock_detection_push_delay",
    "the delay before pushing in order to detect dependency cycles between transactions",
    100*time.Millisecond,  // Default: 100ms
)
```

**Why the delay?** To avoid unnecessary push traffic when transactions are actively making progress. The delay optimizes for the common case where there is no deadlock or coordinator failure.

From [`lock_table_waiter.go:154-200`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L154-L200), four types of pushes are considered:

1. **Deadlock/Liveness Push**: After delay, push to detect cycles or failed coordinators
2. **Timeout Push**: If `lock_timeout` is set, push after the timeout expires
3. **Priority Push**: Immediate push if priorities allow (min vs max priority)
4. **Wait Policy Push**: Immediate push if request has `WaitPolicy_Error`

The **session-level setting** `deadlock_timeout` can override the cluster setting per transaction.

### Entering the Transaction Wait Queue

When a push is initiated, the request enters the **transaction wait queue** on the range that holds the pushee's transaction record.

From [`pkg/kv/kvserver/txnwait/queue.go:275-296`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L275-L296):

```go
// Queue enqueues PushTxn requests which are waiting on extant txns
// with conflicting intents to abort or commit.
//
// When a write intent is encountered, the command which encountered it (called
// the "pusher" here) initiates a PushTxn request to determine the disposition
// of the intent's transaction (called the "pushee" here).
type Queue struct {
    mu struct {
        txns    map[uuid.UUID]*pendingTxn      // Transaction being pushed
        queries map[uuid.UUID]*waitingQueries  // QueryTxn requests
    }
}
```

## Phase 3: Deadlock Detection Algorithm

### Transitive Dependency Tracking

The key to deadlock detection is tracking the **transitive set of dependencies** for each waiting transaction.

From [`queue.go:187-199`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L187-L199):

```go
// A waitingPush represents a PushTxn command that is waiting on the
// pushee transaction to commit or abort. It maintains a transitive
// set of all txns which are waiting on this txn in order to detect
// dependency cycles.
type waitingPush struct {
    req *kvpb.PushTxnRequest
    pending chan *roachpb.Transaction
    mu struct {
        dependents map[uuid.UUID]struct{} // transitive set of txns waiting on this txn
    }
}
```

Each `waitingPush` maintains a `dependents` set containing all transaction IDs that are transitively waiting on the pushee.

### Querying the Pusher Transaction

From [`queue.go:947-1035`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L947-L1035), the system continuously queries the pusher transaction:

```go
// startQueryPusherTxn starts a goroutine to send QueryTxn requests to
// fetch updates to the pusher's own transaction until the context is
// done or an error occurs while querying.
func (q *Queue) startQueryPusherTxn(
    ctx context.Context, push *waitingPush, readyCh <-chan struct{},
) (<-chan *roachpb.Transaction, <-chan *kvpb.Error)
```

The query mechanism:

1. **Send `QueryTxn` RPC** to the pusher's transaction record range
2. **Receive updated transaction** with current dependents
3. **Accumulate dependents** into the `waitingPush.mu.dependents` map
4. **Propagate updates** to other waiters through channels

From [`queue.go:1003-1013`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L1003-L1013):

```go
// Update the pending pusher's set of dependents. These accumulate
// and are used to propagate the transitive set of dependencies for
// distributed deadlock detection.
push.mu.Lock()
if push.mu.dependents == nil {
    push.mu.dependents = map[uuid.UUID]struct{}{}
}
for _, txnID := range waitingTxns {
    push.mu.dependents[txnID] = struct{}{}
}
push.mu.Unlock()
```

### Cycle Detection

From [`queue.go:809-854`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L809-L854):

```go
// Check for dependency cycle to find and break deadlocks.
push.mu.Lock()
_, haveDependency := push.mu.dependents[req.PusheeTxn.ID]
// ... log dependents
push.mu.Unlock()

if haveDependency {
    // Break the deadlock if the pusher has higher priority.
    p1, p2 := pusheePriority, pusherPriority
    if p1 < p2 || (p1 == p2 && bytes.Compare(req.PusheeTxn.ID.GetBytes(), req.PusherTxn.ID.GetBytes()) < 0) {
        log.VEventf(ctx, level,
            "%s breaking deadlock by force push of %s; dependencies=%s",
            req.PusherTxn.ID.Short(),
            req.PusheeTxn.ID.Short(),
            dependents,
        )
        metrics.DeadlocksTotal.Inc(1)
        return q.forcePushAbort(ctx, req)
    }
}
```

**Cycle detection logic:**
1. Check if the **pushee's transaction ID** appears in the pusher's **dependents set**
2. If yes, a cycle exists: pusher → ... → pushee → pusher
3. Determine which transaction to abort based on **priority**
4. If priorities are equal, use **transaction ID ordering** for determinism

### Example: Detecting the Cycle

In our deadlock scenario:

```
Initial state:
- tx1 (pusher) waiting on tx2 (pushee)
- tx2 (pusher) waiting on tx1 (pushee)

QueryTxn propagation:
1. tx1's pusher goroutine queries tx1's record
   → Discovers tx1 is waiting on tx2
   → Adds tx2 to tx1's dependents: {tx2}

2. tx2's pusher goroutine queries tx2's record
   → Discovers tx2 is waiting on tx1
   → Adds tx1 to tx2's dependents: {tx1}

3. QueryTxn returns updated dependents
   → tx1's push sees: "tx2 is waiting on {tx1}"
   → Adds tx1 to tx1's transitive dependents: {tx2, tx1}

Cycle detected!
   → tx1.dependents contains tx2 (the pushee)
   → This means: tx1 → tx2 → tx1 (cycle)
```

## Phase 4: Breaking the Deadlock

### Priority-Based Abort

From [`queue.go:835-854`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L835-L854):

The deadlock is broken using **priority comparison**:

1. **Higher priority wins**: If pusher priority > pushee priority, abort the pushee
2. **Equal priority**: Use transaction ID byte comparison for deterministic choice
3. **Lower priority loses**: Pusher must wait (but won't abort - other transaction will break the cycle)

This ensures:
- **Exactly one transaction is aborted** per cycle
- **Deterministic resolution** across all nodes in the cluster
- **No cascading aborts** due to consistent priority rules

### Force Push Abort

From [`queue.go:1088-1107`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L1088-L1107):

```go
// forcePushAbort upgrades the PushTxn request to a "forced" push abort, which
// overrides the normal expiration and priority checks to ensure that it aborts
// the pushee. This mechanism can be used to break deadlocks between conflicting
// transactions.
func (q *Queue) forcePushAbort(
    ctx context.Context, req *kvpb.PushTxnRequest,
) (*kvpb.PushTxnResponse, *kvpb.Error) {
    log.VEventf(ctx, 1, "force pushing %v to break deadlock", req.PusheeTxn.ID)
    forcePush := *req
    forcePush.Force = true        // Override normal checks
    forcePush.PushType = kvpb.PUSH_ABORT  // Change to abort push
    // ... send PushTxn RPC
}
```

**Force push mechanics:**
1. Set `Force = true` to bypass liveness and priority checks
2. Set `PushType = PUSH_ABORT` to abort the transaction
3. Send the `PushTxn` RPC to the pushee's transaction record
4. The transaction record is updated to `ABORTED` status
5. All intents held by the aborted transaction are eligible for cleanup

From [`queue.go:65-81`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L65-L81):

```go
// ShouldPushImmediately returns whether the PushTxn request should proceed
// without queueing. This is true for pushes which are neither ABORT nor
// TIMESTAMP, but also for ABORT and TIMESTAMP pushes with WaitPolicy_Error or
// where the pusher has priority.
func ShouldPushImmediately(
    req *kvpb.PushTxnRequest, pusheeStatus roachpb.TransactionStatus, wp lock.WaitPolicy,
) bool {
    if req.Force || wp == lock.WaitPolicy_Error {
        return true  // Force pushes bypass the queue
    }
    // ... priority checks
}
```

### Metrics and Observability

The deadlock counter is incremented at [`queue.go:852`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L852):

```go
metrics.DeadlocksTotal.Inc(1)
```

This metric is exposed as `txnwaitqueue.deadlocks_total` and can be monitored via:
- Prometheus metrics endpoint
- DB Console's metrics dashboards
- SQL: `SELECT * FROM crdb_internal.node_metrics WHERE name = 'txnwaitqueue.deadlocks_total'`

## Phase 5: Transaction Cleanup and Retry

### Notifying Waiters

From [`queue.go:476-513`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go#L476-L513):

```go
// UpdateTxn is invoked to update a transaction's status after a successful
// PushTxn or EndTxn command. It unblocks all pending waiters.
func (q *Queue) UpdateTxn(ctx context.Context, txn *roachpb.Transaction) {
    // ... remove from queue
    pending, ok := q.mu.txns[txn.ID]
    waitingPushes := pending.takeWaitingPushes()

    // Send on pending waiter channels outside of the mutex lock.
    for e := waitingPushes.Front(); e != nil; e = e.Next() {
        push := e.Value.(*waitingPush)
        push.pending <- txn  // Notify all waiters
    }
}
```

When a transaction is aborted:
1. Its entry is removed from the transaction wait queue
2. All requests waiting in its lock wait queues are notified
3. The aborted transaction receives a `TransactionRetryWithProtoRefreshError`
4. Intent cleanup begins asynchronously

### Error Propagation

From the test [`deadlock_test.go:133-141`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/sql/tests/deadlock_test.go#L133-L141):

```go
// At this point, the deadlock condition is resolved, and one of the two
// transactions received a TransactionRetry error.
if tx1Err != nil {
    require.NoError(t, tx2Err)
    require.Contains(t, tx1Err.Error(), "TransactionRetryWithProtoRefreshError")
} else if tx2Err != nil {
    require.NoError(t, tx1Err)
    require.Contains(t, tx2Err.Error(), "TransactionRetryWithProtoRefreshError")
}
```

**Client-side handling:**
- The aborted transaction receives a retry error
- The client can retry the transaction from the beginning
- CockroachDB's automatic retry logic may handle this transparently
- The winning transaction proceeds normally

## Data Flow Summary

Here's the complete flow of a deadlock from creation to resolution:

```
1. Lock Acquisition
   ├─ tx1: UPDATE test1 ... → Creates intent on key1
   └─ tx2: UPDATE test2 ... → Creates intent on key2

2. Conflict Detection
   ├─ tx1: UPDATE test2 ... → Encounters tx2's intent on key2
   │   └─ Enter lock wait queue for key2
   └─ tx2: UPDATE test1 ... → Encounters tx1's intent on key1
       └─ Enter lock wait queue for key1

3. Delayed Push Initiation (after 100ms default)
   ├─ tx1: Start push of tx2
   │   └─ Enter txnWaitQueue on tx2's record range
   └─ tx2: Start push of tx1
       └─ Enter txnWaitQueue on tx1's record range

4. Dependency Propagation
   ├─ QueryTxn(tx1) → Returns: tx1 waiting on {tx2}
   ├─ QueryTxn(tx2) → Returns: tx2 waiting on {tx1}
   ├─ tx1's push accumulates: dependents = {tx2}
   ├─ tx2's push accumulates: dependents = {tx1}
   ├─ Next QueryTxn(tx1) → Returns: tx1 waiting on {tx2}, tx2 waiting on {tx1}
   └─ tx1's push accumulates: dependents = {tx2, tx1}  ← CYCLE DETECTED

5. Deadlock Breaking
   ├─ Compare priorities: pusherPri vs pusheePri
   ├─ If equal, compare tx IDs: tx1.ID vs tx2.ID
   ├─ Winner: Force push loser
   └─ Loser: Transaction ABORTED

6. Cleanup and Notification
   ├─ UpdateTxn(aborted_tx) called
   ├─ Remove from txnWaitQueue
   ├─ Notify all waiters in lock wait queues
   ├─ Intent cleanup begins
   └─ Winner proceeds, loser receives retry error
```

## Configuration and Tuning

### Cluster Settings

From [`lock_table_waiter.go:38-75`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L38-L75):

```sql
-- Adjust deadlock detection delay (default: 100ms)
SET CLUSTER SETTING kv.lock_table.deadlock_detection_push_delay = '50ms';
```

**Trade-offs:**
- **Lower value**: Faster deadlock detection, more push traffic
- **Higher value**: Less push overhead, slower deadlock detection

### Session Settings

Per-transaction deadlock timeout:

```sql
-- Override cluster setting for this transaction
SET deadlock_timeout = 50;  -- milliseconds

BEGIN;
  -- Transaction operations with 50ms deadlock detection delay
COMMIT;
```

### Monitoring Deadlocks

```sql
-- View deadlock count
SELECT * FROM crdb_internal.node_metrics
WHERE name = 'txnwaitqueue.deadlocks_total';

-- View current push wait times
SELECT * FROM crdb_internal.node_metrics
WHERE name LIKE 'txnwaitqueue.pusher%';
```

## Key Takeaways

1. **Distributed Detection**: Deadlock detection is fully distributed - no centralized deadlock detector
2. **Transitive Dependencies**: Each transaction tracks what it's waiting on and who's waiting on it
3. **Priority-Based Resolution**: Deadlocks are broken deterministically using transaction priorities
4. **Delayed Pushing**: 100ms default delay optimizes for the common case (no deadlock)
5. **Asynchronous Propagation**: Dependency information flows through QueryTxn RPCs
6. **FIFO Fairness**: Lock wait queues are FIFO; only the head actively pushes
7. **Automatic Retry**: Clients see `TransactionRetryWithProtoRefreshError` and can retry

## Related Documentation

- RFC: [SELECT FOR UPDATE](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/docs/RFCS/20171024_select_for_update.md) - Initial design for locking reads
- Tech Note: [Transaction Coordinator Sender](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/docs/tech-notes/txn_coord_sender.md) - Client-side transaction coordination
- Metrics: [txnwaitqueue metrics](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/metrics.go) - All deadlock-related metrics

## Files Analyzed

This documentation was created by analyzing the following key files:

| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| Concurrency Manager | [`pkg/kv/kvserver/concurrency/concurrency_control.go`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/concurrency_control.go) | 1-200 | Top-level request sequencing |
| Transaction Wait Queue | [`pkg/kv/kvserver/txnwait/queue.go`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/txnwait/queue.go) | 1-1122 | Deadlock detection and resolution |
| Lock Table Waiter | [`pkg/kv/kvserver/concurrency/lock_table_waiter.go`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/lock_table_waiter.go) | 1-200 | Lock waiting and push initiation |
| Lock Table | [`pkg/kv/kvserver/concurrency/lock_table.go`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/kv/kvserver/concurrency/lock_table.go) | 397-682 | Lock tracking and wait queues |
| Deadlock Test | [`pkg/sql/tests/deadlock_test.go`](https://github.com/cockroachdb/cockroach/blob/3fc8163287d19e8b3f3b791e3c9badcd474608db/pkg/sql/tests/deadlock_test.go) | 1-149 | Integration test demonstrating deadlocks |
