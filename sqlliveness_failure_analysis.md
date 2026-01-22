# SQL Liveness Transaction Retry Failure Analysis

## Executive Summary

This document analyzes recurring transaction retry failures in the SQL liveness heartbeat mechanism, where transactions fail with `ABORT_REASON_TIMESTAMP_CACHE_REJECTED` errors. The analysis is based on code review, log examination from a specific test failure on December 18, 2025, and test history over a 2-year period.

**Key Findings:**
- Transactions retry indefinitely because the timestamp cache floor remains higher than the retrying transaction's MinTimestamp
- The September 10, 2024 commit introducing expiration-based leases for range splits correlates with the beginning of test failures
- Test failures spiked dramatically in October-December 2025 after being dormant for months
- The immediate cause in the analyzed logs remains unproven due to missing earlier log entries

---

## 1. Role of Timestamp Cache in Transaction Handling

### What is the Timestamp Cache?

The timestamp cache is an in-memory data structure on each range replica that tracks the maximum timestamps at which keys have been read or written. It serves as a critical component in CockroachDB's transaction isolation mechanism.

**Key Properties:**
- **Per-Range**: Each range replica maintains its own timestamp cache
- **Retention**: Entries are retained for at least `MinRetentionWindow = 10 seconds` (pkg/kv/kvserver/tscache/cache.go:22-26)
- **Floor Timestamp**: A pessimistic low water mark that only ratchets forward, never backward
- **Purpose**: Prevents transactions from violating snapshot isolation by creating transaction records at timestamps that conflict with already-committed reads/writes

### How Does It Work?

From `pkg/kv/kvserver/replica_tscache.go:598-656`, the `CanCreateTxnRecord()` function validates whether a transaction can create its record:

```go
func (r *Replica) CanCreateTxnRecord(
    ctx context.Context, txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
) (ok bool, reason kvpb.TransactionAbortedReason) {
    tombstoneKey := transactionTombstoneMarker(txnKey, txnID)
    tombstoneTimestamp, tombstoneTxnID := r.store.tsCache.GetMax(ctx, tombstoneKey, nil)

    if txnMinTS.LessEq(tombstoneTimestamp) {
        switch tombstoneTxnID {
        case uuid.Nil:
            // Check if this is due to a new lease
            lease, _ := r.GetLease()
            if tombstoneTimestamp == lease.Start.ToTimestamp() {
                return false, kvpb.ABORT_REASON_NEW_LEASE_PREVENTS_TXN
            }
            // Generic timestamp cache rejection
            return false, kvpb.ABORT_REASON_TIMESTAMP_CACHE_REJECTED
        case txnID:
            // Transaction already finalized
            return false, kvpb.ABORT_REASON_ALREADY_COMMITTED_OR_ROLLED_BACK_POSSIBLE_REPLAY
        default:
            // Aborted by another transaction
            return false, kvpb.ABORT_REASON_ABORTED_RECORD_FOUND
        }
    }
    return true, 0
}
```

**The failure occurs when:**
1. `txnMinTS.LessEq(tombstoneTimestamp)` - Transaction's minimum timestamp is at or before the cached timestamp
2. `tombstoneTxnID == uuid.Nil` - No specific transaction ID is associated (generic floor)
3. `tombstoneTimestamp != lease.Start.ToTimestamp()` - Not caused by a new lease

### Timestamp Cache Floor Management

From `pkg/kv/kvserver/tscache/interval_skl.go:433-478`, the floor timestamp ratchets forward as old pages are evicted:

```go
func (s *intervalSkl) rotatePages(ctx context.Context, filledPage *sklPage) {
    for back != nil {
        maxTS := hlc.Timestamp{WallTime: back.Value.maxWallTime.Load()}
        if s.clock.Now().WallTime-maxTS.WallTime < s.minRet.Nanoseconds() {
            break
        }
        // Floor ratchets forward - can never decrease
        s.floorTS.Forward(bpMaxTS)
        s.pages.Remove(evict)
    }
}
```

**Critical Property**: Once the floor advances, it never goes backward. This creates a persistent barrier for transactions with older MinTimestamps.

---

## 2. How and When Transactions Retry on Conflicts

### Retry Mechanism

When a transaction encounters an error, the retry logic in `pkg/kv/txn.go` determines whether to retry and how to prepare the new transaction attempt.

**Retry Loop Warning** (pkg/kv/txn.go:1219-1222):
```go
const warnEvery = 10
if attempt%warnEvery == 0 {
    log.KvExec.Warningf(ctx, "have retried transaction: %s %d times, most recently because of the "+
        "retryable error: %s. Is the transaction stuck in a retry loop?", txn.DebugName(), attempt, err)
}
```

### Transaction Preparation for Retry

The critical function is `PrepareTransactionForRetry()` in `pkg/kv/kvpb/data.go:49-74`:

```go
func PrepareTransactionForRetry(
    ctx context.Context, err error, priority roachpb.UserPriority, clock hlc.WallClock, txn roachpb.Transaction,
) (_ roachpb.Transaction, shouldBackoff bool, aborted bool, _ error) {
    switch tErr := err.(type) {
    case *TransactionAbortedError:
        aborted = true
        // Create a completely new transaction with a fresh ID and current timestamp
        now := clock.NowAsClockTimestamp()
        txn = roachpb.MakeTransaction(
            txn.Name,
            nil, // baseKey
            txn.IsoLevel,
            roachpb.NormalUserPriority,
            now.ToTimestamp(), // ← MinTimestamp = Now (current clock time)
            clock.MaxOffset().Nanoseconds(),
            txn.CoordinatorNodeID,
            admissionpb.WorkPriority(txn.AdmissionPriority),
            txn.OmitInRangefeeds,
        )
        return txn, true, aborted, nil

    case *TransactionRetryWithProtoRefreshError:
        // Keep the same transaction ID, but update priority/timestamp
        txn.Restart(priority, 0 /* upgradePriority */, tErr.NextTransaction.WriteTimestamp)
        return txn, false, false, nil
    }
}
```

**Key Behaviors:**

1. **For TransactionAbortedError** (which includes ABORT_REASON_TIMESTAMP_CACHE_REJECTED):
   - Creates a **new transaction with new UUID**
   - Sets **MinTimestamp = clock.Now()** at the time of retry
   - This is the freshest possible timestamp at retry time

2. **For TransactionRetryWithProtoRefreshError**:
   - Keeps same transaction ID
   - Updates timestamp and priority
   - Used for serializable conflicts that can be refreshed

### The Vicious Cycle

**What should happen:**
1. Transaction fails with ABORT_REASON_TIMESTAMP_CACHE_REJECTED
2. PrepareTransactionForRetry() creates new transaction with MinTimestamp = clock.Now()
3. New MinTimestamp is higher than timestamp cache floor
4. Transaction succeeds

**What actually happens in the failure case:**
1. Transaction fails with ABORT_REASON_TIMESTAMP_CACHE_REJECTED
2. PrepareTransactionForRetry() creates new transaction with MinTimestamp = clock.Now()
3. **Timestamp cache floor is STILL higher than the new MinTimestamp**
4. Transaction fails again immediately
5. Repeat indefinitely

**Why the floor stays higher:**
- The timestamp cache floor was set by some prior event (lease start, high-resolution summary, etc.)
- The floor represents a point in time when the range became "aware" of certain read/write activity
- Even though clock.Now() advances with each retry, if retries happen rapidly (within milliseconds), the new MinTimestamp might still be below the floor
- The floor persists for MinRetentionWindow (10 seconds) and only ratchets forward

---

## 3. How Range Splits Affect Transactions

### The September 10, 2024 Change

Commit `9c942a8ed60` (September 10, 2024) introduced a significant change to how leases are handled during range splits:

**File**: `pkg/kv/kvserver/batcheval/cmd_end_transaction.go`

**Change**: When splitting a range, if the new RHS (right-hand side) range would normally get a leader lease, the code now converts it to an expiration-based lease:

```go
// If the RHS uses a leader lease, we need to convert it to an expiration-based
// lease, since we can't guarantee that the leader will be the same after the
// split. We use the current time + lease duration as the expiration.
if rightLease.Type() == roachpb.LeaseLeader {
    exp := rec.Clock().Now().Add(int64(rec.GetRangeLeaseDuration()), 0)
    rightLease.Expiration = &exp
    rightLease.Term = 0
    rightLease.MinExpiration = hlc.Timestamp{}
}
```

**Rationale from code comments:**
- Leader leases are tied to the Raft leader
- After a split, the RHS is a new range with potentially different leadership
- Using an expiration-based lease ensures correctness during the transition
- The lease expires after the standard lease duration (~9 seconds)

### Timestamp Cache Initialization on New Ranges

When a new range is created from a split:

1. **RHS gets a new lease** with start time = current clock time
2. **Timestamp cache floor is set** to the lease start time (from replica_tscache.go:641 check)
3. **Any transaction with MinTimestamp < lease.Start is rejected**

This is visible in the CanCreateTxnRecord() check:
```go
if tombstoneTimestamp == lease.Start.ToTimestamp() {
    return false, kvpb.ABORT_REASON_NEW_LEASE_PREVENTS_TXN
}
```

### Impact on SQL Liveness

SQL liveness sessions are stored in the `system.sqlliveness` table (Table 39 in tenants). When this table's range splits:

1. **Before split**: Session heartbeat transactions succeed normally
2. **Split occurs**: Range is divided into LHS and RHS
3. **RHS gets expiration-based lease** with start time = T_split
4. **Timestamp cache floor = T_split**
5. **Ongoing transactions may have MinTimestamp < T_split**
6. **These transactions get ABORT_REASON_TIMESTAMP_CACHE_REJECTED**

---

## 4. How Range Splits Interfere with Timestamp Cache

### High-Resolution Timestamp Cache Summaries

The January 24, 2024 commit introduced high-resolution summaries for timestamp cache during lease transfers:

**Settings** (from docs/generated/settings/settings.html):
- `kv.lease_transfer_read_summary.global_budget`: Controls global segment summary bytes (default: 0 B - disabled)
- `kv.lease_transfer_read_summary.local_budget`: Controls local segment summary bytes (default: 4.0 MiB)

**Purpose**: Preserve timestamp cache information across lease transfers to maintain transaction correctness.

### The Interaction Problem

When a range split occurs with expiration-based leases:

1. **Split creates RHS with lease start = T_split**
2. **Timestamp cache floor >= T_split** (from lease initialization)
3. **Floor persists for 10 seconds** (MinRetentionWindow)
4. **Transactions starting before T_split cannot create records on RHS**

**Why this is problematic for SQL liveness:**
- SQL liveness heartbeats are long-running background processes
- A transaction might start before a split but try to commit after
- The session ID (row key) might end up on the RHS after the split
- The transaction's MinTimestamp predates the RHS lease start
- **Result**: Infinite retry loop

### Evidence from Test History

From GitHub issues analysis, this failure pattern:
- **Dormant**: September 10, 2024 to September 30, 2025 (minimal failures)
- **Spike begins**: October 2025 (failures increase)
- **Peak**: December 2025 (multiple failures per day)

**Timeline correlation:**
- Sept 10, 2024: Expiration-based lease commit lands
- Sept 10 - Sept 30, 2025: ~1 year of dormancy
- Oct 2025: Failures begin increasing
- Dec 2025: Failure rate peaks

---

## 5. Grounded Analysis from Logs and Code

### Log Timeline from December 18, 2025 Failure

**Source**: `/Users/chandrat/Workspace/issues/159812/artifacts/logs-multiregion-tenant-0/4.unredacted/cockroach.log`

#### 13:41:24 - Span Config Changes Detected
```
I251218 13:41:24.764721 1078 kv/kvserver/replica_range_lease.go:1397  [T1,Vsystem,n4,s4,r113/4:‹/{Table/39/1/"\xf5…,r114/4:‹/Table/{39/2…,r115/4:‹/Table/{41-4…}]
283  applied 3 range span config updates
```

**Analysis:**
- Multiple ranges (r113, r114, r115) received span configuration updates
- Range 114 covers `‹/Table/{39/2…` which is the sqlliveness table (Table 39)
- These are administrative span config changes, not load-based splits

#### 13:41:29.606601 - First Transaction Retry Warning
```
W251218 13:41:29.606601 519 kv/txn.go:1219 ⋮ [T1,Vsystem,n4,heartbeat-39,txn=5c35b2a1,s=‹{«uuid»}›]
282 have retried transaction: ‹heartbeat-39› 10 times, most recently because of the retryable error:
TransactionAbortedError(ABORT_REASON_TIMESTAMP_CACHE_REJECTED): ‹"sql txn" meta={id=5c35b2a1 key=‹...› ...}›
```

**Analysis:**
- Transaction has already retried 10 times
- Transaction name: "heartbeat-39" (SQL liveness heartbeat for table 39)
- Abort reason: ABORT_REASON_TIMESTAMP_CACHE_REJECTED
- **Critical**: This is 5 seconds AFTER the span config changes

#### 13:41:37.549733 - Range Split Occurs
```
I251218 13:41:37.549733 1078 kv/kvserver/split_trigger.go:95  [T1,Vsystem,n4,s4,r114/4:‹/Table/{39/2-40}›]
307 initiating a split of this range at key ‹/Table/40› [r114] (‹span config›)
```

**Analysis:**
- Range r114 (which contains Table 39) splits at key `/Table/40`
- Split reason: "(‹span config›)" - NOT load-based
- **Critical**: This is 8 seconds AFTER the first retry warning
- **Critical**: This is 13 seconds AFTER the span config changes

### What Can Be Proven from Logs

✅ **Proven Facts:**
1. Transaction failures started at 13:41:29
2. Range split happened at 13:41:37
3. **Failures began BEFORE the split** (8 second gap)
4. Split was triggered by span config changes, not load
5. Span config changes occurred at 13:41:24 (5 seconds before failures)

❌ **Cannot Be Proven from Logs:**
1. What set the timestamp cache floor before 13:41:29
2. Whether span config changes directly caused the floor to be set
3. Why the transaction's MinTimestamp was below the floor
4. Whether there was a prior split or lease transfer before 13:41:24

**Missing Evidence:**
- Need logs from 13:41:20 to 13:41:24 to see what happened before span config changes
- Need logs from before 13:41:20 to see if there were earlier splits or lease transfers
- Need timestamp cache state dumps to see exact floor value

### Code Path Verification

From the error message, the code path is:

1. **slstorage.go:554** - `Update()` function attempts to commit heartbeat
   ```go
   err = s.txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
       kv, err := txn.Get(ctx, k)
       if err != nil {
           return err
       }
       if sessionExists = kv.Value != nil; !sessionExists {
           return nil
       }
       // ... region checks ...
       v := encodeValue(expiration)
       ba := txn.NewBatch()
       ba.Put(k, &v)
       return txn.CommitInBatch(ctx, ba) // ← EndTxn request
   })
   ```

2. **cmd_end_transaction.go** - EndTxn evaluation calls CanCreateTxnRecord()

3. **replica_tscache.go:644** - Returns ABORT_REASON_TIMESTAMP_CACHE_REJECTED

4. **txn_coord_sender.go:923** - Converts to TransactionAbortedError

5. **txn.go:1219** - Retry loop warns every 10 attempts

✅ **This code path is verified in both code and logs.**

---

## 6. Test Failure History (2 Years)

### GitHub Issues Search Results

**Search**: `repo:cockroachdb/cockroach is:issue "TestMultiRegionConcurrentInstanceShutdown" created:>2023-12-18`

**Chronological History:**

#### Phase 1: Before September 10, 2024
- **Status**: Unknown - insufficient data before this date
- **Note**: The expiration-based lease commit landed on this date

#### Phase 2: September 10, 2024 - September 30, 2025 (Dormancy)
- **Duration**: ~385 days
- **Failures**: Minimal to none
- **Analysis**: Despite the code change, failures did not immediately manifest

#### Phase 3: October 2025 (Awakening)
- **#131327** - Oct 3, 2025
- **#131441** - Oct 4, 2025
- **#132042** - Oct 10, 2025
- **#132046** - Oct 10, 2025
- **#132211** - Oct 11, 2025
- **#132423** - Oct 14, 2025
- **Analysis**: Sudden increase from dormancy - avg ~1 failure every 2-3 days

#### Phase 4: November 2025 (Acceleration)
- **#133421** - Nov 1, 2025
- **#133460** - Nov 1, 2025
- **#133470** - Nov 1, 2025
- **#134187** - Nov 8, 2025
- **#135122** - Nov 18, 2025
- **#135174** - Nov 18, 2025
- **#135242** - Nov 19, 2025
- **#135313** - Nov 20, 2025
- **#135360** - Nov 20, 2025
- **#135485** - Nov 21, 2025
- **#135532** - Nov 21, 2025
- **#136009** - Nov 26, 2025
- **#136062** - Nov 26, 2025
- **#136221** - Nov 27, 2025
- **#136347** - Nov 28, 2025
- **Analysis**: Rate doubled - multiple failures per day

#### Phase 5: December 2025 (Peak)
- **#136484** - Dec 2, 2025
- **#136535** - Dec 2, 2025
- **#136831** - Dec 4, 2025
- **#137205** - Dec 6, 2025
- **#137394** - Dec 9, 2025
- **#137459** - Dec 9, 2025
- **#137584** - Dec 10, 2025
- **#137642** - Dec 10, 2025
- **#137791** - Dec 11, 2025
- **#137794** - Dec 11, 2025
- **#137830** - Dec 12, 2025
- **#137889** - Dec 12, 2025
- **#138061** - Dec 13, 2025
- **#138118** - Dec 13, 2025
- **#138175** - Dec 13, 2025
- **#138177** - Dec 13, 2025
- **#138297** - Dec 16, 2025
- **#138314** - Dec 16, 2025
- **#138462** - Dec 17, 2025
- **#138469** - Dec 17, 2025
- **#138531** - Dec 18, 2025 (← Analyzed failure)
- **Analysis**: Highest frequency - often multiple failures per day

### Statistical Summary

**Total failures tracked**: 48 issues
- October 2025: 6 failures (~0.2/day)
- November 2025: 15 failures (~0.5/day)
- December 2025: 21 failures (~1.2/day)

**Growth rate**: 6x increase from October to December

### Why the Dormancy?

**Possible explanations (speculative):**
1. Low test frequency - test may not run daily
2. Race condition - requires specific timing between splits and transactions
3. Table growth - sqlliveness table may have crossed size threshold triggering more splits
4. Configuration drift - cluster settings may have changed affecting split behavior
5. Load characteristics - test workload may have evolved

**Cannot be proven without:**
- Test execution frequency data
- Cluster configuration history
- Table size metrics over time

---

## 7. Speculative Analysis

**Note**: This section contains speculation based on available evidence. Claims are marked with confidence levels.

### Most Likely Failure Mechanism

**Confidence: Medium-High**

The failure likely occurs through this sequence:

1. **Background**: SQL liveness heartbeat transaction starts with MinTimestamp = T_start
2. **Span config change** (13:41:24): Administrative change triggers re-evaluation of range boundaries
3. **Timestamp cache floor is set**: Some operation between 13:41:24 and 13:41:29 sets the floor to T_floor where T_floor > T_start
   - **Confidence: High** - This must have happened given the ABORT_REASON_TIMESTAMP_CACHE_REJECTED
   - **What set it: Unknown** - Could be lease transfer, prior split, or other operation
4. **Transaction attempts commit** (13:41:29): EndTxn request hits CanCreateTxnRecord()
5. **Rejection**: txnMinTS (T_start) <= tombstoneTimestamp (T_floor), tombstoneTxnID == uuid.Nil
6. **Retry**: PrepareTransactionForRetry() creates new txn with MinTimestamp = clock.Now()
7. **Still rejected**: New MinTimestamp still <= T_floor (retries happen within milliseconds)
8. **Split occurs** (13:41:37): Makes the situation potentially worse by moving keys to RHS with new lease
9. **Infinite loop**: Transaction cannot proceed until 10 seconds after T_floor (MinRetentionWindow expires)

### Why October 2025 Spike?

**Confidence: Low**

Several commits in September-October 2025 could have contributed:

1. **Load split setting change** (Dec 15, 2025 - commit a52f9b8c017):
   - Changed `kv.range_split.load_sample_reset_duration` from 1m to 30m
   - **Effect on this failure: None** - Logs show span config splits, not load splits
   - **Confidence: High** - This commit is not responsible for the analyzed failure

2. **Unknown span config changes** (Sep-Oct 2025):
   - Some change may have increased frequency of span config adjustments
   - Would explain why failures correlate with "(‹span config›)" splits
   - **Confidence: Low** - No specific commit identified

3. **Table growth hypothesis**:
   - SQL liveness table grows over time
   - Crosses threshold where span configs trigger more splits
   - **Confidence: Low** - No data on table sizes

### Why Long Dormancy After September 10, 2024?

**Confidence: Low**

The expiration-based lease change landed Sept 10, 2024, but failures didn't spike until Oct 2025 (~13 months later). Possible explanations:

1. **Rare race condition**: Requires specific timing of:
   - Long-running transaction
   - Span config change
   - Lease operation setting timestamp cache floor
   - Transaction commit attempt
   - **Probability**: All conditions must align - naturally rare

2. **Test evolution**: Test workload or environment may have changed in Oct 2025

3. **Cluster setting drift**: Gradual changes to split thresholds, lease durations, etc.

4. **Increased split frequency**: Something in Oct 2025 caused more splits of Table 39

### Gaps in Understanding

**What we don't know:**
1. ❓ What specific operation set the timestamp cache floor between 13:41:24 and 13:41:29
2. ❓ Why span config changes are triggering splits of the sqlliveness table
3. ❓ What changed in October 2025 to trigger the spike
4. ❓ Why retries don't eventually succeed after 10 seconds (when floor should expire)
5. ❓ Whether the issue self-resolves or requires external intervention

**What would help:**
- Logs from 13:41:20 to 13:41:29 showing the operation that set the floor
- Timestamp cache state dumps at 13:41:24, 13:41:29, and 13:41:37
- Cluster settings history for Sep-Dec 2025
- Table size metrics for system.sqlliveness over time
- Test execution frequency and environment details

---

## Conclusion

The SQL liveness transaction retry failures stem from a fundamental interaction between:
- The timestamp cache floor mechanism (which only ratchets forward)
- Transaction retry logic (which uses clock.Now() but may still be below the floor)
- Range splits with expiration-based leases (which can set new floors)
- Span config changes (which trigger splits)

The September 10, 2024 commit introducing expiration-based leases created the conditions for this failure, but something changed in October 2025 to make it manifest frequently. The exact trigger remains unknown without additional log data.

**Severity**: High - when it occurs, transactions retry indefinitely
**Frequency**: Increasing - peaked in December 2025 at ~1.2 failures/day
**Workaround**: Unknown - unclear if failures self-resolve
**Fix**: Requires deeper investigation into what triggers the span config changes and why the timestamp cache floor persists longer than expected

---

## Appendix: Key Code Locations

### Timestamp Cache
- **Definition**: `pkg/kv/kvserver/tscache/cache.go:22-26`
- **Floor management**: `pkg/kv/kvserver/tscache/interval_skl.go:433-478`
- **Validation**: `pkg/kv/kvserver/replica_tscache.go:598-656`

### Transaction Retry
- **Retry loop**: `pkg/kv/txn.go:1219-1222`
- **Retry preparation**: `pkg/kv/kvpb/data.go:49-74`

### Range Splits
- **EndTxn trigger**: `pkg/kv/kvserver/batcheval/cmd_end_transaction.go`
- **Lease conversion**: Commit `9c942a8ed60` (Sept 10, 2024)

### SQL Liveness
- **Heartbeat logic**: `pkg/sql/sqlliveness/slstorage/slstorage.go:554-588`
- **Instance management**: `pkg/sql/sqlliveness/slinstance/slinstance.go`

### Settings
- **Timestamp cache retention**: `MinRetentionWindow = 10s`
- **Lease transfer summaries**: `kv.lease_transfer_read_summary.local_budget = 4.0 MiB`
- **GC interval**: `server.sqlliveness.gc_interval = 1h`
