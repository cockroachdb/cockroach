## At a glance
- **Component index**
  - Txn coordination: `TxnCoordSender` and interceptor stack (`txn_coord_sender.go` and `txn_interceptor_*.go`)
  - Range routing: `DistSender` (`dist_sender.go`) + `rangecache.RangeCache` (`pkg/kv/kvclient/rangecache/`)
  - Protos and errors: `pkg/kv/kvpb/`, `pkg/roachpb/`
  - Server peer: `pkg/kv/kvserver/` (Replica eval, closed timestamps, uncertainty)
- **Ownership & boundaries**
  - Owners: KV (kvcoord)
  - kvcoord owns client-side txn lifecycle, retries/refresh, pipelining, parallel commits, routing, observability
  - kvcoord must not embed storage/server semantics; it speaks via `kvpb` RPCs and consumes `roachpb` types
  - Closed timestamp evaluation, latches/locks, and uncertainty computation live in `kvserver`
- **See also**: `pkg/kv/kvserver/AGENTS.md`, `pkg/kv/kvserver/concurrency/AGENTS.md`, `pkg/sql/AGENTS.md`
- **Key entry points & types**
  - `txn_coord_sender.go` (`TxnCoordSender`, `DeferCommitWait`, `maybeCommitWait`)
  - `txn_interceptor_heartbeater.go` (txn record heartbeat lifecycle)
  - `txn_interceptor_seq_num_allocator.go` (per-request sequence numbers)
  - `txn_interceptor_write_buffer.go` (write buffering; read-your-writes)
  - `txn_interceptor_pipeliner.go` (in-flight write pipelining; chaining)
  - `txn_interceptor_committer.go` (parallel commits; implicit/explicit commit)
  - `txn_interceptor_span_refresher.go` (refreshes; server-side and client-side)
  - `dist_sender.go` (divide-and-send across ranges; follower-read routing; ambiguous triage)
  - `rangecache/range_cache.go` (descriptor lookup and coalescing)
- **Critical invariants**
  - All KV ops in a txn carry monotonically increasing sequence numbers; replays are idempotent per sequence
  - `EndTxn` is last in batch; parallel commit only when all other ops are compatible and declared in-flight
  - Txn timestamps only move forward; commit timestamp equals final write timestamp; commit-wait enforces real-time ordering
  - Retries must preserve intent idempotency and refresh read spans before stepping read ts
  - Uncertainty restarts must move above the uncertain value and below local/global limits as applicable
- **Request/data flow**
  - Read/Write path (root txn)
    - SQL → kv.Txn → `TxnCoordSender` → [interceptors] → `DistSender` → ranges
```
SQL → TxnCoordSender → Heartbeater → SeqNum → WriteBuffer → Pipeliner → Committer → SpanRefresher → DistSender → Range(s)
```

## Deep dive
1) Architecture & control flow
- **Sender stack (root)** in `txn_coord_sender.go` builds this order:
  - `txnHeartbeater` → `txnSeqNumAllocator` → `txnWriteBuffer` → `txnPipeliner` → `txnCommitter` → `txnSpanRefresher` → `txnMetricRecorder` → gatekeeper
  - Gatekeeper enforces one in-flight request for roots; leaf txns allow concurrency but omit heartbeats/commit logic.
- **DistSender** splits a `BatchRequest` across range boundaries using `RangeIterator` + `RangeCache`, then
  - serializes or parallelizes sub-batches (subject to limits), preserves request order for writes,
  - retries with descriptor refresh on routing errors, and synthesizes resume spans on limits.

- **Parallel commit protocol (summary)**
  - Attach in-flight writes (new writes in batch + QueryIntent) to `EndTxn` and issue in parallel when all requests are compatible.
  - If `EndTxn` returns STAGING and all writes succeeded at or below staging ts, the txn is implicitly committed; coordinator asynchronously makes it explicit (COMMITTED).
  - If any write succeeded above staging ts, perform an auto-retry: re-issue `EndTxn` alone with in-flight writes moved to `LockSpans`.
  - DistSender splits off parallel-commit batches (`divideAndSendParallelCommit`) and handles intent-missing races via `QueryTxn` with ambiguity classification.

2) Data model & protos
- `roachpb.Transaction`: IDs, epochs, read/write timestamps, observed timestamps, status (PENDING/STAGING/PREPARED/COMMITTED/ABORTED)
- `kvpb.Error` detail types drive retry logic (e.g., `TransactionRetryWithProtoRefreshError`, `ReadWithinUncertaintyIntervalError`, `WriteTooOldError`, `AmbiguousResultError`).
- Range metadata and closed-timestamp policy: `roachpb.RangeDescriptor`, `roachpb.RangeClosedTimestampPolicy`, `kvpb.ClientRangeInfo`.
  - Uncertainty intervals: server computes global/local limits per request; on uncertainty, retries step just above the value ts within limits.

3) Concurrency & resource controls
- Gatekeeper ensures synchronous client protocol for roots (no concurrent requests except permitted cases), avoiding races in commit/abort.
- Seq numbers guarantee per-request idempotency; DistSender safe to retry non-commit ops.
- Pipelining chains reads to see pipelined writes; `txnWriteBuffer` supports read-your-writes and flush points.

4) Failure modes & recovery
- See also: `pkg/kv/kvclient/kvcoord/AGENTS_INTERCEPTORS.md` for detailed refresh/auto-retry behavior.
- Retriable errors are turned into either server-side refresh (if `CanForwardReadTimestamp` and no reads yet) or client-side refresh (span refresher) before escalating to epoch bump.
- Refresh logic:
  - Preemptive if write ts > read ts and free/no spans or EndTxn would certainly fail;
  - Reactive on `WriteTooOld`, `ReadWithinUncertaintyInterval`, etc., using `kvpb.TransactionRefreshTimestamp` and `tryRefreshTxnSpans`.
  - If refresh fails, return `RETRY_SERIALIZABLE` or propagate original retry.
- Ambiguous results (network/timeout near commit): if a commit may have succeeded, DistSender returns `AmbiguousResultError`; SQL maps to StatementCompletionUnknown.
- Aborts: `TransactionAbortedError` takes precedence when merging errors; coordinator blocks further requests except rollback.
 - Deadlocks: detected in server-side concurrency control; surfaced as retryable errors to kvcoord and handled via the standard refresh/retry or epoch bump paths.

5) Observability & triage
- Metrics: `pkg/kv/kvclient/kvcoord/txn_metrics.go` (commit/abort counts, parallel commits, retries/refreshes, condensed spans).
- Tracing: interceptors emit events for pipelining, refresh attempts/outcomes, and commit explicitization; look for “making txn commit explicit”.
- Ambiguity near commit: propagate `AmbiguousResultError` and rely on higher layers (SQL StatementCompletionUnknown) rather than client-side status resolution.
- See also: `pkg/kv/kvclient/kvcoord/AGENTS_INTERCEPTORS.md` for detailed interceptor metrics and tracing hints.

6) Interoperability
- Client/SQL: executes under `kv.Txn`; higher layers map ambiguous commit errors to SQL StatementCompletionUnknown semantics.
- KV server: correctness relies on server-side latches/locks, closed timestamps, and uncertainty evaluation; client must not assume server semantics beyond `kvpb`/`roachpb` contracts.
- Settings and feature gates flow from `pkg/settings` and `pkg/clusterversion`; unknown client hints must be safely ignored by servers.

7) Mixed-version / upgrades
- Parallel-commit participation, write-buffer hints, or follower-read routing must remain safe when some nodes don’t recognize newest flags: defaults and server-side guards ensure safety (e.g., EndTxn falls back to explicit commit; unknown client hints are ignored). Don’t assume cluster-wide feature presence unless gated by a cluster setting/version.

8) Configuration & feature gates
- `kv.transaction.parallel_commits.enabled` (default true)
- Follower reads enablement and enterprise gating live in CCL; DistSender uses injected `CanSendToFollower`.
- Refresh footprint budget: `kv.transaction.max_refresh_spans_bytes`
- Commit-wait linearizability (per-Txn option) and `DeferCommitWait` callback for external waiting.

9) Edge cases & gotchas
- Follower reads: historical reads may route to followers when eligible; server validates against closed timestamps.
- EndTxn must be terminal in batch; leaf txns cannot issue locking requests.
- After an ambiguity during commit, kvcoord must not “helpfully” resolve status; instead propagate ambiguity and rely on higher layers.
- On retries, split EndTxn from writes and disable parallel commit to avoid reordering relative to prior partial successes.
- Refresh spans may be condensed or invalidated on budget overflow; once invalidated, only epoch restart can proceed.

10) Examples
- Retry/refresh loop (simplified):
```
send batch → pErr?
  no → success; if commit → maybeCommitWait
  yes → if refreshable (WTO/uncertainty/push): try refresh spans and retry
         else if retryable: return retry with next txn proto
         else if with commit and cannot rule out success: AmbiguousResultError
         else: return error
```

11) References
- See also: `pkg/kv/kvserver/AGENTS.md`

12) Glossary
- In-flight write: pipelined write not yet proven durable; tracked by `InFlightWrites` on EndTxn
- Implicit commit: STAGING + all in-flight writes at/below staging ts
- Refresh: revalidation of read spans to bump read ts safely
- Uncertainty interval: [read ts, (local|global) limit]; defines safe read window
- Commit-wait: client sleep to enforce real-time/monotonic reads guarantees
- Ambiguous result: success unknown; treat commit as possibly applied
- Parallel commit: protocol issuing EndTxn with in-flight writes for implicit commit
- Follower read: serving consistent historical reads from non-leaseholders subject to closed timestamps

13) Search keys & synonyms
- TxnCoordSender, DistSender, RangeDescriptorCache, parallel commits, pipeliner, span refresher, ReadWithinUncertaintyInterval, ambiguous, commit-wait, follower reads, closed timestamps
Omitted sections: Background jobs & schedules