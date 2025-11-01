## KV Transaction Interceptors (client-side) – domain context

### At a glance

- **Component index**
  - `TxnCoordSender` (`pkg/kv/kvclient/kvcoord/txn_coord_sender.go`): client-side transaction coordinator; composes the interceptor stack and owns the txn state machine.
  - `txnHeartbeater` (`txn_interceptor_heartbeater.go`): starts/controls heartbeat loop once the txn acquires locks; aborts asynchronously on detected aborts.
  - `txnSeqNumAllocator` (`txn_interceptor_seq_num_allocator.go`): allocates sequence numbers; provides read-own-writes or stepping semantics.
  - `txnWriteBuffer` (`txn_interceptor_write_buffer.go`): buffers writes until flush/commit; transforms reads/locking requests; serves read-your-own-writes.
  - `txnPipeliner` (`txn_interceptor_pipeliner.go`): async-consensus pipelining; tracks in-flight writes; manages lock footprint.
  - `txnCommitter` (`txn_interceptor_committer.go`): commit/rollback orchestration, parallel/staging commit, EndTxn elision for RO.
  - `txnSpanRefresher` (`txn_interceptor_span_refresher.go`): collects read spans; preemptive/reactive refresh and auto-retries.
  - `txnMetricRecorder` (`txn_interceptor_metric_recorder.go`): per-txn metrics and timers.
  - `txnLockGatekeeper` (`txn_lock_gatekeeper.go`): bottom of stack; enforces one-in-flight (root), unlocks while sending, re-locks on response.
- **Ownership & boundaries**
  - Owners: KV (kvcoord/transactions)
  - Boundaries: client-side only; speaks `kvpb`/`roachpb` to `kvserver`; no direct storage/raft usage; routes via `DistSender`.
- **Stack order (root txn)**
  - Heartbeater → SeqNumAllocator → WriteBuffer → Pipeliner → Committer → SpanRefresher → MetricRecorder → Gatekeeper → DistSender

```
Client → TxnCoordSender.SendLocked
  ├─ Heartbeater
  ├─ SeqNumAllocator
  ├─ WriteBuffer
  ├─ Pipeliner
  ├─ Committer
  ├─ SpanRefresher
  ├─ MetricRecorder
  └─ LockGatekeeper → DistSender (RPC)
```

- **Critical invariants**
  - All interceptor `SendLocked` executions run while holding `TxnCoordSender` mutex; the lock is released only by `txnLockGatekeeper` during RPCs and re-acquired on reply.
  - Sequence numbers establish idempotence and ordering: writes get monotonically increasing `Sequence`; reads run at `readSeq` (read-own-writes) or under stepping; MVCC honors seq-aware semantics.
  - Write buffering invariant: if buffering is enabled, all writes are buffered until the first flush; once flushed, buffering is disabled for the remainder of the txn epoch.
  - Pipeliner tracks in-flight writes precisely; lock footprint tracking may be condensed but remains a safe superset.
  - Span refresher may re-send batches; interceptors below it must be robust to re-issues and STAGING statuses.
  - Mixed-version safety: new request fields added by interceptors (e.g., `ExpectExclusionSince`) must remain no-ops on older nodes (protobuf unknown fields ignored) or be feature-gated via settings.

### Deep dive

1) Architecture & control flow
- `TxnCoordSender` builds and links the interceptor chain once per txn attempt. For root txns the full chain is used; for leaf txns a reduced chain is used (SeqNumAllocator, WriteBuffer, Pipeliner, optional SpanRefresher), and heartbeats/committer/metrics are omitted. The gatekeeper is always at the bottom.
- `Send` path: the coordinator copies the current `Txn` proto into the batch, then calls the top interceptor’s `SendLocked`. The gatekeeper unlocks before RPC and re-locks on response. After the chain returns, `TxnCoordSender` updates its state machine (`updateStateLocked`), handles retryable errors, and finalizes on EndTxn.
- Savepoints: `TxnCoordSender` delegates create/rollback/release to each interceptor to restore internal state. Rollbacks add ignored seqnum ranges and may step the write seq (`txn_coord_sender_savepoints.go`).

2) Data model & protos
- Reads, writes, locks:
  - Locking reads and writes carry `KeyLockingStrength` and `KeyLockingDurability` (see `pkg/kv/kvpb/`).
  - Pipeliner attaches `InFlightWrites` and `LockSpans` to `EndTxn` (parallel commit bookkeeping) and tracks them locally as `inFlightWriteSet` and a condensable lock footprint.
  - Write buffer records per-key buffered values (with seq) and optional durable lock acquisitions; on flush it synthesizes Put/Delete requests ordered by sequence and may include `ExpectExclusionSince` timestamps for exclusion.
  - Span refresher collects `refreshFootprint` (condensable span set) and can emit `Refresh`/`RefreshRange` requests.

3) Concurrency & resource controls
- Gatekeeper disallows concurrent root txn requests (except Heartbeat) to avoid races with refresh and restarts; leaf txns allow concurrency (`allowConcurrentRequests=true`).
- Pipeliner resource controls:
  - `kv.transaction.max_intents_bytes` (TrackedWritesMaxSize): memory budget for lock footprint and in-flight writes. On overflow, spans are condensed; optionally txns are rejected (`kv.transaction.reject_over_max_intents_budget.enabled`) or by count (`kv.transaction.max_intents_and_locks`).
  - Async consensus enablement is conditional on request mix, batch size (`kv.transaction.write_pipelining.max_batch_size`), memory budget, and settings for ranged writes and locking reads.
- Write buffer resource control:
  - `kv.transaction.write_buffering.max_buffer_size` (0 = unbounded) triggers a flush when the estimated buffer size would exceed the limit.
  - Transformed locking scans may clamp `MaxSpanRequestKeys` to avoid unbounded buffer growth.
  - DDL-triggered mid-txn disables buffering (see SQL integration) and forces a flush.

4) Failure modes & recovery
- Retryable errors: `TxnCoordSender` converts to `TransactionRetryWithProtoRefreshError`, bumps epoch or read timestamp, and resets interceptors (`epochBumpedLocked`).
- Ambiguity: DistSender returns AmbiguousResultErrors; higher layers decide. Commit ambiguity is handled by committer/heartbeater + recovery.
- Parallel commit:
  - If `EndTxn(commit)` batch returns STAGING: committer checks implicit commit condition (all intents ≤ staging ts). If not satisfied, it re-issues a standalone `EndTxn` (explicit commit). If satisfied, it asynchronously makes the commit explicit.
- Span refresh:
  - Preemptive refresh before EndTxn if write ts > read ts and refresh cost is free or commit would fail; otherwise reactive refresh on serializable retry reasons. On success, the batch is re-sent with bumped read ts.
- Savepoints and leaf vs root:
  - Leaf txns on retryable errors move to txnError and must surface error to root; no auto-retry on leaves.
  - Savepoint rollback restores per-interceptor state; for locking activity, ignored seq ranges are added and write seq stepped to separate future operations.

5) Observability & triage
- Metrics (`pkg/kv/kvclient/kvcoord/txn_metrics.go`):
  - Commit/abort counts, 1PC, parallel commits and auto-retries; client/server refresh success/fail; condensed intent spans; in-flight locks over budget; write buffer enabled/disabled/memory limit; restart reason counters; rollback failures; commit wait counts.
- Tracing/logging:
  - Interceptors emit `log.VEvent(f)` details, e.g., pipeliner chain/condense decisions, write buffer “flushing buffer…”/transformations, span refresher refresh attempts and outcomes, commit explicitization.
- Testing knobs (`testing_knobs.go`):
  - Max refresh attempts, conditional condense of refresh spans, commit-wait hook, anchor key randomization disable, injected retry filters.
- Triage recipes:
  - Unexpected flushing: enable verbose tracing, look for write buffer “flushing buffer” event and reason (size, DeleteRange, DDL, SkipLocked); verify `kv.transaction.write_buffering.max_buffer_size` and transform settings; check whether `DisablePipelining` or buffering toggles were used by client.
  - Ordering bugs (reads missing buffered/pipelined writes): verify seq allocation in traces; check write buffer `maybeServeRead` path and that transformed locking reads use `Unreplicated` durability; in pipeliner, confirm QueryIntent chaining and error index adjustments.
  - Excess memory/condensing: watch `txn.condensed_intent_spans[_gauge]` and `txn.inflight_locks_over_tracking_budget`; optionally reproduce by lowering `kv.transaction.max_intents_bytes` and inspecting pipeliner logs that condense spans.
  - Refresh loops: enable tracing; confirm preemptive vs reactive refresh, split-EndTxn retry path; adjust `MaxTxnRefreshSpansBytes`, `keep_refresh_spans_on_savepoint_rollback.enabled` for behavior differences.
  - Parallel commit anomalies: check EndTxn response status (STAGING vs COMMITTED) and implicit commit check; look for follow-up single EndTxn and “making txn commit explicit” events.

6) Interoperability
- Server-side locking and concurrency manager: `pkg/kv/kvserver/concurrency/AGENTS.md` governs lock table semantics, wait-queues, and durable vs unreplicated locks; client-side locking reads must match server expectations.
- Protos and types: `pkg/kv/kvpb/` and `pkg/roachpb/` define request fields used by interceptors: `InFlightWrites`, `LockSpans`, `KeyLockingStrength/Durability`, `Refresh*`, `ExpectExclusionSince`, `AdmissionHeader`, etc.
- DistSender: transport ordering and batch splitting; ambiguity handling; ranges and leaseholder routing.

7) Mixed-version / upgrades
- Client-set fields must be backward-safe:
  - New request fields like `ExpectExclusionSince` are ignored by older nodes (protobuf unknown fields). Behavior enhancement only on upgraded nodes.
  - Transformations from replicated to unreplicated locking reads are safe across versions; locking semantics remain compatible.
- Feature flags are settings-gated so rollouts can stage behavior: pipelined ranged writes, pipelined locking reads, parallel commits, write buffering and its transformations.

8) Configuration & feature gates (cluster settings)
- Pipelining
  - `kv.transaction.write_pipelining.enabled`
  - `kv.transaction.write_pipelining.ranged_writes.enabled`
  - `kv.transaction.write_pipelining.locking_reads.enabled`
  - `kv.transaction.write_pipelining.max_batch_size`
  - `kv.transaction.max_intents_bytes`, `kv.transaction.reject_over_max_intents_budget.enabled`, `kv.transaction.max_intents_and_locks`
- Parallel commit
  - `kv.transaction.parallel_commits.enabled`
- Span refresh
  - `kv.transaction.max_refresh_spans_bytes`
  - `kv.transaction.keep_refresh_spans_on_savepoint_rollback.enabled`
- Write buffering
  - `kv.transaction.write_buffering.enabled`
  - `kv.transaction.write_buffering.max_buffer_size`
  - `kv.transaction.write_buffering.transformations.scans.enabled`
  - `kv.transaction.write_buffering.transformations.get.enabled`
- Heartbeater
  - `kv.transaction.randomized_anchor_key.enabled`

9) Edge cases & gotchas
- Leaf txns: cannot send locking requests (enforced by `maybeRejectIncompatibleRequest`); write buffer is still used to serve read-your-own-writes that were shipped from root.
- 1PC disabling: if any replicated locks are acquired, the committer disables 1PC path even for otherwise-eligible batches.
- DeleteRange: write buffer forces a flush and disables further buffering (large mutations); locking scans with large result sets will cap `MaxSpanRequestKeys` when transformed.
- Savepoint rollback after errors is limited: cannot rollback after most non-ConditionFailed/LockConflict errors due to ambiguity concerns.

10) Examples
- Write buffering and locking read transform
  - Put/Delete buffered; `MustAcquireExclusiveLock` on Put/Delete injects a pre-locking Get if needed. Locking Get/Scan with replicated durability are transformed to unreplicated; the durable lock acquisition is recorded in the buffer; reads may be served from buffer while also acquiring locks.
- Parallel commit
  - EndTxn(commit=true) plus writes + QueryIntent: txn enters STAGING; if all writes ≤ staging ts, it’s implicitly committed; otherwise a follow-up EndTxn commits explicitly.

11) References
- See also: `pkg/kv/kvserver/concurrency/AGENTS.md`
- Client-side
  - `pkg/kv/kvclient/kvcoord/txn_coord_sender.go`
  - `txn_interceptor_*.go` files in this directory
- Server-side
  - Locks and concurrency: `pkg/kv/kvserver/concurrency/AGENTS.md`
  - Protos: `pkg/kv/kvpb/`, `pkg/roachpb/`

12) Glossary
- In-flight writes: intent point writes that were issued with async consensus and not yet proven; must be proven before commit (QueryIntent) or folded into lock spans.
- Lock footprint: upper bound of spans where txn holds or held locks; attached to EndTxn for cleanup.
- Parallel (staging) commit: commit path where EndTxn writes a STAGING record while writes are still proving; becomes committed once implicit condition is satisfied or via explicit finalize.
- Refresh: revalidation of reads so txn can bump its read timestamp instead of epoch restart.
- Read-own-writes: client reads sees its own buffered/pipelined writes using seq numbers and local buffering.

13) Search keys & synonyms
- TxnCoordSender, DistSender, txnHeartbeater, txnPipeliner, txnWriteBuffer, txnCommitter, txnSpanRefresher
- HasBufferedAllPrecedingWrites, InFlightWrites, LockSpans, ExpectExclusionSince, QueryIntent, DeferCommitWait
- parallel commit, STAGING, implicit commit, explicit commit
- span refresher, tryRefreshTxnSpans, TransactionRetryWithProtoRefreshError
- ReadWithinUncertaintyInterval, UncertaintyInterval, CommitWait
- write buffering, read-your-own-writes, sequence numbers, ignored sequence ranges

_Note: Omitted intentionally: Background jobs & schedules_
