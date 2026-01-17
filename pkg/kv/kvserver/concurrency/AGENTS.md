## At a glance

- Components and key entry points (paths are relative):
  - `pkg/kv/kvserver/concurrency/concurrency_control.go`: public interfaces (`Manager`, `RequestSequencer`, `LockManager`, `TransactionManager`, `RangeStateListener`, `MetricExporter`).
  - `pkg/kv/kvserver/concurrency/concurrency_manager.go`: implementation and request sequencing loop (`SequenceReq`, `FinishReq`, `HandleLockConflictError`, `OnLockAcquired`, `OnLockUpdated`, `QueryLockTableState`, range event handlers).
  - `pkg/kv/kvserver/concurrency/latch_manager.go`: per-range latches; serialize conflicting in-flight requests.
  - `pkg/kv/kvserver/concurrency/lock_table.go`: in-memory lock table, per-key queues, fairness, memory limits, metrics.
  - `pkg/kv/kvserver/concurrency/lock_table_waiter.go`: wait, push, deadlock/liveness detection, lock-timeouts, contention tracing.
  - `pkg/kv/kvserver/concurrency/lock/locking.proto` (+ `locking.go`): lock strengths, durability, wait policies, compatibility matrix.
  - `pkg/kv/kvserver/replica_read.go` and `replica_proposal.go`: surface `AcquiredLocks` and call `OnLockAcquired`/`OnLockUpdated`; handle `MissingLocks`.
  - `pkg/kv/kvserver/batcheval/cmd_query_locks.go`: `QueryLocks` for `crdb_internal.locks`.
  - `pkg/kv/kvserver/batcheval/cmd_lease.go` and `cmd_subsume.go`: unreplicated lock reliability across lease transfer/merge.
  - `pkg/kv/kvserver/batcheval/cmd_flush_lock_table.go`: manual flush of unreplicated locks to replicated locks.
  - Protos: `pkg/kv/kvpb/api.proto` (Batch header hints), `pkg/roachpb/data.proto` (`LockAcquisition`, `LockUpdate`, `LockStateInfo`).
- Owners: KV (concurrency)
- See also: `pkg/kv/kvserver/AGENTS.md`, `pkg/kv/kvclient/kvcoord/AGENTS.md`
- Control flow (range-local):

  ```text
  client batch → Replica → concurrency.Manager
    1) acquire latches (spanlatch.Manager)
    2) scan lock table → enqueue on per-key wait-queues if conflict
       - drop latches while waiting, stay at head on re-acquire
       - head-of-queue pushes holder (txnWaitQueue on holder’s txn record)
    3) no conflicts → evaluate
    4) evaluation updates lock table via OnLockAcquired/OnLockUpdated
  ```

- Tiny example — NOWAIT vs SKIP LOCKED:
  - `SELECT ... FOR UPDATE NOWAIT` errors immediately on a conflicting lock (`WriteIntentError`).
  - `SELECT ... FOR UPDATE SKIP LOCKED` returns only unlocked keys and skips conflicting ones.
- Invariants (selected):
  - Latches serialize evaluation of conflicting in-flight requests; they are always held while scanning the lock table.
  - Lock strengths and compatibility are as in `lock/locking.proto` (see Deep dive §2). Non-locking reads never block on intents; optionally block on Exclusive locks per setting below.
  - Wait-queues ensure FIFO fairness across conflicting requests using per-request sequence numbers; head-of-queue retains position across latch re-acquisition.
  - Deadlock avoidance: head-of-queue pushes after delay or immediately by priority/wait policy.
  - Under normal operation, replicated locks (intents) are never lost; unreplicated locks are best-effort but can be preserved across splits/merges/lease transfers when reliability settings are enabled.

## Deep dive

1) Architecture & control flow
- Latches (`spanlatch.Manager`) gate evaluation based on declared spans and access method. Most requests acquire latches; some only wait on latches without acquiring.
- Lock table (`lock_table.go`) tracks per-key state: current holder(s), queued readers/writers, and per-request sequence numbers for fairness. Sequencing: acquire latches → `ScanAndEnqueue` → if wait needed, drop latches and wait; otherwise evaluate.
- Wait: `lock_table_waiter.go` subscribes to lock state changes and decides when to push conflicting txns for deadlock/liveness detection or respecting wait policy / lock timeout.
- Pushing and txn wait queue: head-of-queue pushes the holder; push RPC enters the holder’s `txnwait.Queue` on the txn record’s range.
- Evaluation: on success, read-only locking operations surface `AcquiredLocks` that the replica feeds into `OnLockAcquired` (unreplicated), while write intents are integrated via `OnLockUpdated` and discovery (`HandleLockConflictError`) when seen during evaluation under latches.

2) Data model & protos
- `lock/locking.proto`
  - Strength: `None`, `Shared`, `Update`, `Exclusive`, `Intent` (increasing strength).
  - Durability: `Unreplicated` (in-memory, fast, best-effort), `Replicated` (persisted quorum-wide; never lost while range is available).
  - WaitPolicy: `Block` (default), `Error` (nowait: return `WriteIntentError` if holder active), `SkipLocked` (skip conflicting keys while scanning).
  - Compatibility: see matrix in proto; key points:
    - Non-locking reads vs intents conflict only if read ts ≥ intent ts.
    - Non-locking reads vs Exclusive locks conflict only if both txns are serializable, read ts ≥ lock ts, and setting `kv.lock.exclusive_locks_block_non_locking_reads.enabled` is true.
- `roachpb.LockAcquisition` (acquire), `roachpb.LockUpdate` (update/release), `roachpb.LockStateInfo` (observability).
- Batch header hint: `kvpb.BatchRequest.has_buffered_all_preceding_writes` allows the server to skip AbortSpan checks while preserving read-your-own-writes (see §6).

3) Concurrency & resource controls
- Latches: serialized by `Acquire` / `WaitUntilAcquired`; held while scanning the lock table; dropped while waiting in lock queues, then re-acquired while retaining queue head position.
- Lock table fairness: per-request `seqNum` across a request’s lifetime partially orders queues; prevents deadlocks from lock table reordering while allowing out-of-order progress when no contention.
- Memory/limits:
  - Soft cap on tracked keys (default ≈ 10k). When exceeded, lock table may clear state and issue `waitElsewhere` states; correctness is preserved via latches and txn pushing.
  - `kv.lock_table.maximum_lock_wait_queue_length`: cap queue length; exceeding requests error with reason `LOCK_WAIT_QUEUE_MAX_LENGTH_EXCEEDED`.
  - `kv.lock_table.discovered_locks_threshold_for_consulting_finalized_txn_cache`: large discoveries consult txn-status cache and avoid populating the lock table with resolvable locks.
  - `kv.lock_table.batch_pushed_lock_resolution.enabled`: allows non-locking readers to defer/batch intent resolution of already-pushed intents.

4) Failure modes & recovery
- Replicated locks (intents) are durable and observed during evaluation; if missing in lock table they are integrated via `HandleLockConflictError`, which enqueues the waiter behind the discovered lock while holding the sequencing position.
- Unreplicated locks (best-effort):
  - Lease transfer: by default, lease transfers opportunistically flush unreplicated locks to replicated locks during evaluation (`OnRangeLeaseTransferEval` → `MVCCAcquireLock`), bounded by `kv.lock_table.unreplicated_lock_reliability.max_flush_size`.
  - Range merge: during `Subsume`, optionally flush to replicated (`OnRangeSubsumeEval`) when `kv.lock_table.unreplicated_lock_reliability.merge.enabled` is true.
  - Split: `OnRangeSplit` moves RHS keys’ unreplicated locks into the RHS lock table; RHS acquires them as unreplicated. If disabled, both lock and txn queues are cleared.
  - Lease update (losing lease) and snapshots: lock table is cleared; correctness maintained because evaluation will rediscover intents, and unreplicated locks are best-effort.
  - Manual recovery: `FlushLockTable` command can upgrade all unreplicated locks in a span to replicated locks.
- When `QueryIntent` reports missing locks (e.g., stale best-effort lock), `OnLockMissing` marks them ineligible for later export/flush.

5) Observability & triage
- Metrics (`Manager.LockTableMetrics()`; also exposed via replica/store metrics):
  - Counts: locks, held locks, locks with queues; aggregate hold/wait durations.
  - Top-K: by waiters, hold duration, max waiter duration.
- Introspection:
  - `SELECT * FROM crdb_internal.locks` filters/paginates across ranges via `QueryLocks` and includes holder, durability, strength, waiters, durations.
  - Per-request tracing: contention events and tags emitted by `lock_table_waiter` (trace tags: `locks`, `wait_key`, `holder_txn`, `wait_start`, total `wait`). Use to pinpoint who/where a request is stuck.
  - String dumps for tests: `TestingLockTableString()`; `verifiable_lock_table.go` verifies invariants in test builds.
- Triage recipes:
  - Stuck wait: inspect request trace for `locks` tag; identify holder and key; check `crdb_internal.locks` and any wait-queue length caps; verify deadlock pushing occurred (trace shows PushTxn).
  - Starvation: confirm head-of-queue retention across retries; check for excessive `waitElsewhere` (memory pressure) and `maximum_lock_wait_queue_length` rejections.
  - Lost unreplicated locks post-split/lease transfer: verify associated settings are enabled; examine lease/merge/split events; consider running `FlushLockTable` for hot spans.
  - Aborts/read-your-own-writes: if server aborted due to AbortSpan, ensure the client uses write buffering so `HasBufferedAllPrecedingWrites` is set where appropriate.

6) Interoperability seams
- KV client/txn coordination: cooperates with client-side buffering and locking semantics; server may skip AbortSpan checks when clients attest to buffered writes.
- Txn wait queue: coordinates cross-range pushes and deadlock detection using per-transaction queues.
- Intent resolution: integrates with intent resolution services post-push and during evaluation.

7) Mixed-version / upgrades
- Request/response fields: `has_buffered_all_preceding_writes` is ignored by older servers and must be set compatibly by clients.
- Feature toggles can be enabled per-cluster and may change semantics:
  - Unreplicated lock reliability gates (split/lease transfer/merge) default to enabled in recent versions; verify during rolling upgrades.
  - `kv.lock.exclusive_locks_block_non_locking_reads.enabled` affects read vs Exclusive lock interaction.
  - `FlushLockTable` exists only in newer versions.

8) Configuration & feature gates (selected)
- `kv.lock_table.maximum_lock_wait_queue_length` (int): cap per-key waiters (QoS guardrail).
- `kv.lock_table.deadlock_detection_push_delay` (duration): delay before pushing for deadlock/liveness; overridden by per-batch `deadlock_timeout`.
- `kv.lock_table.batch_pushed_lock_resolution.enabled` (bool): batch resolution of pushed intents by non-locking readers.
- `kv.lock_table.unreplicated_lock_reliability.split.enabled` (bool)
- `kv.lock_table.unreplicated_lock_reliability.lease_transfer.enabled` (bool)
- `kv.lock_table.unreplicated_lock_reliability.merge.enabled` (bool)
- `kv.lock_table.unreplicated_lock_reliability.max_flush_size` (bytes): guardrail for durability upgrades.
- `kv.lock.exclusive_locks_block_non_locking_reads.enabled` (bool): whether non-locking reads block on Exclusive locks at serializable.

9) Edge cases & gotchas
- `SkipLocked` range reads: scan skips locked keys; validation and optimistic-conflict checking is restricted to returned point keys (`replica_read.go:collectSpansRead`).
- Non-existent keys: locking GET/DEL may still acquire a lock on a missing key to protect gaps (`LockNonExistentKeys = true`).
- Memory pressure: lock table may shed queue/lock state. Requests in `waitElsewhere` should push and/or rely on latches to preserve safety; fairness may degrade transiently.
- Savepoints/rollbacks: sequence numbers (and ignored ranges) on `LockAcquisition`/`LockUpdate` determine lock promotion/rollback across unreplicated→replicated transitions.
- Missing best-effort locks: `OnLockMissing` prevents later durability upgrade of locks that were reported missing by `QueryIntent` during read evaluation.

10) Tiny worked examples
- Locking read (point): txn T scans lock table under latches; no conflict → evaluate READ; returns `AcquiredLocks` (unreplicated Exclusive). Replica calls `OnLockAcquired`. Later write W conflicts: on sequencing, W enters wait-queue, pushes T after delay or immediately if `Error`/priority.
- Skip locked scan: range SCAN with `SkipLocked` skips locked keys at evaluation; on re-sequencing, latches cover only the concrete keys returned; no waiting occurs for skipped keys.

11) References
 - See also: `pkg/kv/kvclient/kvcoord/AGENTS_INTERCEPTORS.md`

12) Glossary
- Latch: in-memory per-range span lock; serializes evaluation of conflicting in-flight requests.
- Lock table: in-memory per-range structure tracking per-key locks, queues, and fairness sequencing.
- Lock (unreplicated): in-memory key lock (typically Exclusive) acquired by locking reads; best-effort durability.
- Intent (replicated lock): MVCC intent; durable and replicated; discovered during evaluation if absent in lock table.
- Wait policy: `Block`, `Error` (nowait), `SkipLocked`.
- Wait-queue: per-key queues in the lock table with fairness via per-request sequence numbers.
- Txn wait queue: per-txn queue on txn record’s range; used for pushing and deadlock detection.
- Push: `PushTxn` RPC to advance or abort a txn to break conflicts or detect deadlocks/coordinator failure.
- AbortSpan: per-range structure used to quickly detect aborted txns; bypassed when `HasBufferedAllPrecedingWrites` is set.
- Reliability upgrade: converting unreplicated locks to replicated locks during lease transfer/merge/explicit flush.

13) Search keys & synonyms
 
Omitted sections: Background jobs & schedules
- search: "lock table", "unreplicated locks", "SkipLocked", "nowait", "WaitPolicy_Error", "deadlock detection", "txnWaitQueue", "OnLockAcquired", "HandleLockConflictError", "QueryLocks", "FlushLockTable", "ExclusiveLocksBlockNonLockingReads", "maximum_lock_wait_queue_length".
