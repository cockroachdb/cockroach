# Finding Investigation Protocols

When the user asks for "details on #N", launch a `general-purpose` subagent with
an investigation brief. Do NOT perform the drill-down in the main conversation —
this preserves the main context window.

## Investigation Brief Template

Construct this from the synthesis data and send it to the subagent:

```
You are investigating a specific health finding on CockroachDB cluster
"<name>" from <finding_start> to <finding_end>.

## Investigation Brief

FINDING: #N | <severity> | <metric> | <time_range> | <value> | <description>

OPERATIONS IN WINDOW (±15 min of finding):
  <op-name> | <start> | <cleanup_end> | <recovery_end>
  (or "none" if no operations overlap)

RELATED FINDINGS FROM ANALYSIS:
  <other findings that occurred within 5 min of this finding>

## Investigation Protocol

Follow this protocol IN ORDER. You may run Python/bash scripts autonomously
to parse results — do not ask the user for permission.

### Phase 1: Re-query the specific metric (per-node breakdown)

Query the finding's metric with "by {host}" at the finding's specific time
range (±5 min padding):
  roachdev datadog metrics query \
    "avg:cockroachdb.<metric>{cluster:<name>} by {host}" \
    --from "<finding_start - 5min>" --to "<finding_end + 5min>"

Report per-node values with timestamps. Identify which nodes are affected.

### Phase 2: Follow the causal chain

Based on the finding type, query the CAUSAL CHAIN metrics to determine root
cause. Run these as a single batched query where possible.

<INSERT CAUSAL CHAIN FROM PROTOCOL TABLE>

### Phase 3: Search logs in the finding's time window

Search for logs that explain the finding:
  roachdev datadog logs search "cluster:<name> <log_search_terms>" \
    --from "<finding_start - 2min>" --to "<finding_end + 2min>" \
    --storage flex

<INSERT LOG SEARCH TERMS FROM PROTOCOL TABLE>

### Phase 4: Cross-reference with operations

If operations overlap with the finding, search for operation confirmation
logs in the time window to verify the operation actually caused the finding:
  roachdev datadog logs search "cluster:<name> <op_confirmation_terms>" \
    --from "<op_start - 1min>" --to "<op_cleanup + 1min>" \
    --storage flex

### Phase 5: Synthesize and report

Produce a structured report:

HYPOTHESIS: <one-line root cause hypothesis>
CONFIDENCE: <low | moderate | high>
  - low: single signal, could be coincidence or metric noise
  - moderate: 2+ correlated signals pointing to same cause
  - high: metric + log + operation timeline all converge

EVIDENCE:
  1. <metric evidence with per-node values and timestamps>
  2. <log evidence with excerpts>
  3. <operation correlation evidence>

CAUSAL CHAIN: <what caused what, e.g., "license-throttle → SQL throttling
  on all nodes → sql.failure spike → recovered on license restore">

WHAT WOULD CONFIRM/DENY THIS:
  <what additional evidence would raise or lower confidence>

SUGGESTED ACTION:
  <what to do — "no action needed", "investigate further", "file a bug", etc.>

VERIFICATION:
  <Datadog UI link scoped to finding's time range and host>
  <SQL query or cockroach command the user can run to verify, if applicable>

If the investigation is INCONCLUSIVE (low confidence after all phases),
recommend a debug.zip:
  To investigate further, generate a debug.zip covering this time range:
    cockroach debug zip debug-finding-N.zip \
      --host=<affected_node> \
      --from='<finding_start>' --to='<finding_end>'
```

## Protocol Table

Use this table to populate Phase 2 (causal chain metrics) and Phase 3
(log search terms) in the investigation brief.

| Finding Type | Causal Chain Metrics (Phase 2) | Log Search Terms (Phase 3) |
|---|---|---|
| **sql.failure spike** | `sql.service.latency by {host}`, `txn.restarts`, `sys.cpu.combined.percent.normalized by {host}`, `admission.io.overload by {host}` | `"throttling" OR "license" OR "admission"`, `"error" OR "failed"` |
| **sql.service.latency spike** | `txn.restarts`, `sql.failure`, `disk.iopsinprogress by {host}`, `storage.wal.fsync.latency by {host}`, `admission.io.overload by {host}`, `sys.cpu.combined.percent.normalized by {host}` | `"slow proposal" OR "circuit breaker" OR "disk stall"`, `"contention" OR "lock wait"` |
| **goroutine explosion / AC death spiral** | `sys.goroutines by {host}`, `sys.runnable.goroutines.per_cpu by {host}`, `admission.granter.slots_exhausted_duration{name=kv} by {host}`, `rpc.method.get.recv by {host}`, `sys.cpu.combined.percent.normalized by {host}` | `"disk slowness detected" OR "syncdata"`, `"store liveness withdrawal"`, `"AdmitKVWork"`, `severity:fatal` |
| **kv.prober failures** | `livenodes`, `heartbeatlatency by {host}`, `ranges.underreplicated`, `sys.uptime by {host}` | `"CockroachDB node starting"`, `severity:fatal` |
| **ranges.unavailable** | `heartbeatlatency by {host}`, `heartbeatfailures by {host}`, `rpc.connection.unhealthy by {host}`, `sys.uptime by {host}`, `requests.slow.raft` | `"replica unavailable" OR "not leaseholder"`, `"node drain" OR "connection refused"` |
| **ranges.underreplicated** | `sys.uptime by {host}`, `capacity.available by {host}`, `rpc.connection.unhealthy by {host}` | `"snapshot" OR "up-replication"`, `"node dead" OR "store dead"` |
| **circuit_breaker spike** | `ranges.unavailable`, `heartbeatlatency by {host}`, `requests.slow.raft`, `rpc.connection.unhealthy by {host}` | `"breaker" OR "tripped"`, `"replica unavailable"` |
| **closed_timestamp spike** | `sys.uptime by {host}`, `heartbeatlatency by {host}`, `requests.slow.raft` | `"closed timestamp regression"`, `severity:fatal` |
| **changefeed.commit.latency spike** | `changefeed.max.behind.nanos`, `changefeed.currently_running`, `changefeed.currently_paused`, `jobs.changefeed.protected_age_sec`, `changefeed.backfill_count` | `"changefeed" OR "slow consumer"`, `"pausing" OR "resumed"` |
| **changefeed.max.behind.nanos growing** | `changefeed.commit.latency`, `changefeed.error.retries`, `sys.cgo.allocbytes by {host}`, `changefeed.backfill_count` | `"slow consumer" OR "catchup scan"`, `"changefeed" AND "error"` |
| **changefeed dropped (currently_running decrease)** | `changefeed.failures`, `changefeed.currently_paused`, `jobs.changefeed.protected_age_sec` | `"changefeed" AND ("failed" OR "canceled" OR "paused")` |
| **protected_age_sec growing** | `changefeed.currently_running`, `changefeed.currently_paused`, `changefeed.max.behind.nanos`, `ranges` | `"protected timestamp" OR "GC threshold"`, `"changefeed" OR "backup"` |
| **l0-sublevels high** | `admission.io.overload by {host}`, `admission.granter.io_tokens_exhausted_duration by {host}`, `storage.write.stalls by {host}`, `rocksdb.read.amplification by {host}` | `"compaction"`, `"io_load_listener"`, `"write stall"` |
| **storage.write.stalls** | `disk.iopsinprogress by {host}`, `storage.wal.fsync.latency by {host}`, `admission.io.overload by {host}`, `rocksdb.read.amplification by {host}`, `storage.l0-sublevels by {host}` | `"write stall" OR "disk stall"`, `"compaction"` |
| **wal.fsync.latency spike** | `disk.iopsinprogress by {host}`, `storage.write.stalls by {host}`, `admission.io.overload by {host}`, `storage.l0-sublevels by {host}` | `"disk stall" OR "disk slowness detected" OR "syncdata"`, `"store liveness withdrawal"` |
| **CPU sustained high / hot node** | `sys.goroutines by {host}`, `rpc.method.get.recv by {host}`, `disk.iopsinprogress by {host}`, `admission.granter.slots_exhausted_duration{name=kv} by {host}` | `"compaction" OR "snapshot"`, `"admission control"`, `"AdmitKVWork"` |
| **sys.rss increasing (memory leak)** | `sys.cgo.allocbytes by {host}`, `sys.cpu.combined.percent.normalized by {host}`, `changefeed.max.behind.nanos` | `"out of memory" OR "oom"`, `"allocator" OR "catchup scan"` |
| **capacity.used divergence** | `capacity.available by {host}`, `capacity by {host}`, `capacity.used by {host}`, `ranges by {host}` | `"disk full" OR "no space"`, `"temp" OR "orphan"` |
| **range count growth** | `queue.range_merge.process.success`, `capacity.used by {host}`, `queue.gc.info.transactionresolvefailed` | `"merge queue" OR "GC"`, `"MVCC" OR "protected timestamp"` |
| **node restart (uptime reset)** | `sys.rss by {host}` (before restart), `disk.iopsinprogress by {host}`, `storage.wal.fsync.latency by {host}`, `livenodes` | `"CockroachDB node starting"`, `severity:fatal`, `"panic"`, `"oom"` |
| **rpc.connection.unhealthy** | `heartbeatlatency by {host}`, `heartbeatfailures by {host}`, `sys.uptime by {host}` | `"connection refused" OR "connection reset"`, `"node drain"` |
| **schema_change stuck/paused** | `sql.service.latency by {host}`, `txn.restarts`, `jobs.schema_change.currently_running` | `"schema change" AND ("reverting" OR "failed" OR "stuck")`, `"command is too large"` |
| **backup failure** | `heartbeatfailures by {host}`, `requests.slow.raft`, `disk.iopsinprogress by {host}` | `"backup" AND ("failed" OR "error")`, `"result is ambiguous"` |
| **restore/import failure or validation error** | `jobs.restore.currently_running`, `jobs.restore.currently_paused`, `capacity.used by {host}`, `storage.wal.fsync.latency by {host}`, `disk.iopsinprogress by {host}` | `"restore" AND ("failed" OR "error" OR "row_count_mismatch")`, `"inspect" OR "import validation"`, `"result is ambiguous" OR "AddSSTable"` |
