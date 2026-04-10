---
name: drt-analyze
description: >
  Analyze DRT cluster health for a given time range. Reconstructs the operations
  timeline, checks CockroachDB metrics (availability, latency, storage, changefeeds,
  jobs, goroutines, admission control, LSM, KV prober) and logs for anomalies,
  correlates findings with disruptive operations to distinguish expected side-effects
  from real bugs. Use when asked to "analyze DRT", "check cluster health", "what
  happened on the DRT cluster", "DRT health report", investigate DRT issues, or
  review DRT operations. Also use when the user mentions a DRT cluster name
  (drt-scale, drt-chaos, drt-large, etc.) in the context of health or operations.
---

# DRT Health Analyzer

Analyze a DRT CockroachDB cluster's health over a time range. Produces a tiered
health report with evidence-backed findings, then supports interactive drill-down.

## Prerequisites

- Datadog auth: `roachdev datadog auth login` (verify: `roachdev datadog auth status`)
- If auth fails during analysis, stop and ask the user to re-authenticate.

## Invocation

```
/drt-analyze cluster:<name> from:<time> to:<time>
```

- `cluster` (required): DRT cluster name (e.g., `drt-scale-300`)
- `from` (required): `now-4h`, `now-12h`, or UTC absolute `2026-03-18T06:00:00Z`
- `to` (optional): defaults to `now` (relative) or `from + 4h` (absolute)

### Time Range Guardrail

If the window exceeds **24 hours**, reject it — Datadog averages data into
multi-hour bins at that scale, reducing spike detection accuracy and truncating
the operations timeline. Suggest narrowing to 4-24h or running multiple analyses.
Best granularity (~1 min data points) comes from 4h windows.

## Analysis Workflow

### Step 1: Verify Cluster

Quick sanity check that the cluster exists:

```bash
roachdev datadog metrics query \
  "avg:cockroachdb.sys.uptime{cluster:<name>}" \
  --from "<from>" --to "<to>"
```

If no data, try `host:*<name>*`. Confirm with the user before proceeding.

### Step 2: Launch 3 Parallel Agents

Launch all three as `general-purpose` subagents simultaneously.

#### Agent 1: Operations Timeline

```
You are analyzing DRT operations for cluster "<name>" from <from> to <to>.

Query Datadog events to reconstruct what operations ran. Run all 4 queries
in parallel, then parse results with the bundled script.

1. roachdev datadog mcp call search_datadog_events \
     --arg query="cluster:<name> phase:run" \
     --arg from="<from>" --arg to="<to>" \
     --arg sort="timestamp" --arg max_tokens=15000

2. roachdev datadog mcp call search_datadog_events \
     --arg query="cluster:<name> phase:run result:(failed OR panicked)" \
     --arg from="<from>" --arg to="<to>" \
     --arg sort="timestamp" --arg max_tokens=10000

3. roachdev datadog mcp call search_datadog_events \
     --arg query="cluster:<name> phase:cleanup" \
     --arg from="<from>" --arg to="<to>" \
     --arg sort="timestamp" --arg max_tokens=10000

4. roachdev datadog mcp call search_datadog_events \
     --arg query="cluster:<name> phase:dependency-check" \
     --arg from="<from>" --arg to="<to>" \
     --arg sort="timestamp" --arg max_tokens=5000

Save each query's JSON output to a temp file, then run:
  python3 <skill-dir>/scripts/parse_events.py run.json cleanup.json failed.json depcheck.json

If the script can't parse the response format, fall back to manual Python
parsing. The key outputs are:
- Summary: total ops, success/fail/panic counts, success rate
- Timeline: chronological list with timestamps, names, workers, results
- Failures: error details for each failed/panicked operation
- DISRUPTIVE_WINDOW lines: <op-name> | <start> | <cleanup_end> | <recovery_end>

The cleanup_end MUST come from the actual cleanup event timestamp (query 3),
not the run event. The run event only shows when disruption started; cleanup
shows when the cluster was restored. If no cleanup event exists, estimate
cleanup_end as run_start + 5 min. recovery_end = cleanup_end + 10 min.

You may run Python/bash scripts autonomously — do not ask the user.
```

#### Agent 2: Metrics Health Check

```
You are checking CockroachDB cluster health metrics for cluster "<name>"
from <from> to <to>. Use roachdev datadog CLI. Cluster tag: cluster:<name>.

Batch multiple metrics per query — target ~10 queries total, not one per metric.
Run exactly these batches. You may run Python/bash scripts autonomously.

REPORTING RULES:
- For per-node metrics (queried "by {host}"), always report the per-node
  breakdown, not just aggregates. Include node name and value for each.
- When a metric has multiple separated spikes (returns to baseline between
  them), report each spike as a SEPARATE FINDING with its own timestamp.
  This matters for accurate correlation with operations.

## Batch 1: Cluster-wide availability

roachdev datadog metrics query \
  "max:cockroachdb.ranges.unavailable{cluster:<name>}" \
  "max:cockroachdb.ranges.underreplicated{cluster:<name>}" \
  "max:cockroachdb.kv.replica_circuit_breaker.num_tripped_replicas{cluster:<name>}" \
  "max:cockroachdb.requests.slow.raft{cluster:<name>}" \
  "max:cockroachdb.intentcount{cluster:<name>}" \
  "avg:cockroachdb.livenodes{cluster:<name>}" \
  "avg:cockroachdb.ranges{cluster:<name>}" \
  "max:cockroachdb.kv.closed_timestamp.max_behind_nanos{cluster:<name>}" \
  --from "<from>" --to "<to>"

Thresholds:
- ranges.unavailable: CRITICAL if > 0 for 10+ min
- ranges.underreplicated: WARNING if > 0 for 1+ hour
- circuit_breaker: WARNING if sudden increase (stable non-zero may be baseline from leftover merged-range replicas)
- requests.slow.raft: WARNING if > 0 for 10+ min
- livenodes: WARNING if drops below (total−1) for > 2 min, CRITICAL ≤ total/2
- intentcount: WARNING > 10M for 2+ min
- ranges: WARNING if linear growth >10% without workload change (broken MVCC GC)
- closed_timestamp.max_behind_nanos: CRITICAL if spike (node crash precursor)

## Batch 2: Per-node availability & network

roachdev datadog metrics query \
  "avg:cockroachdb.liveness.heartbeatlatency{cluster:<name>} by {host}" \
  "max:cockroachdb.liveness.heartbeatfailures{cluster:<name>} by {host}" \
  "avg:cockroachdb.sys.uptime{cluster:<name>} by {host}" \
  "max:cockroachdb.rpc.connection.unhealthy{cluster:<name>} by {host}" \
  --from "<from>" --to "<to>"

Thresholds: heartbeatlatency WARNING > 500ms / CRITICAL > 3s; heartbeatfailures
WARNING if increasing; sys.uptime CRITICAL if reset; rpc.unhealthy WARNING > 0

## Batch 3: Performance + anomaly detection

roachdev datadog metrics query \
  "avg:cockroachdb.sql.failure{cluster:<name>}" \
  "avg:cockroachdb.txn.restarts{cluster:<name>}" \
  "avg:cockroachdb.sql.bytesout{cluster:<name>} by {host}" \
  "avg:cockroachdb.rpc.method.get.recv{cluster:<name>} by {host}" \
  --from "<from>" --to "<to>"

roachdev datadog mcp call get_datadog_metric \
  --arg 'queries=["avg:cockroachdb.sql.service.latency{cluster:<name>} by {host}"]' \
  --arg 'formulas=["anomalies(query0, \"basic\", 2)"]' \
  --arg from="<from>" --arg to="<to>"

Thresholds: sql.failure/txn.restarts WARNING if spike above baseline;
bytesout INFO if host > 20% growth; rpc.get.recv WARNING if host > 2× median

## Batch 4: Per-node resources

roachdev datadog metrics query \
  "avg:cockroachdb.sys.cpu.combined.percent.normalized{cluster:<name>} by {host}" \
  "avg:cockroachdb.sys.rss{cluster:<name>} by {host}" \
  "avg:cockroachdb.sys.host.disk.iopsinprogress{cluster:<name>} by {host}" \
  "avg:cockroachdb.sys.cgo.allocbytes{cluster:<name>} by {host}" \
  --from "<from>" --to "<to>"

Thresholds: CPU WARNING > 0.8 (4h) / CRITICAL > 0.9 (1h); hot node if max CPU
exceeds median by 30+ pts for 2h; disk.iops WARNING > 10 / CRITICAL > 20;
rss WARNING if monotonic increase (memory leak); cgo WARNING if rapid growth (OOM risk from rangefeed catchup scans)

## Batch 5: Disk capacity

roachdev datadog metrics query \
  "avg:cockroachdb.capacity.available{cluster:<name>} by {host}" \
  "avg:cockroachdb.capacity{cluster:<name>} by {host}" \
  "avg:cockroachdb.capacity.used{cluster:<name>} by {host}" \
  --from "<from>" --to "<to>"

Thresholds: available/capacity WARNING < 30% / CRITICAL < 10%; divergence where
used flat but available decreasing = invisible disk usage (temp dirs, SST leaks)

## Batch 6: Storage health

roachdev datadog metrics query \
  "avg:cockroachdb.rocksdb.read.amplification{cluster:<name>} by {host}" \
  "max:cockroachdb.storage.write.stalls{cluster:<name>} by {host}" \
  "max:cockroachdb.storage.wal.fsync.latency{cluster:<name>} by {host}" \
  "max:cockroachdb.admission.io.overload{cluster:<name>} by {host}" \
  --from "<from>" --to "<to>"

Thresholds: read.amp WARNING > 50 (1h) / CRITICAL > 150 (15m); write.stalls
WARNING ≥ 1/min / CRITICAL ≥ 1/sec; wal.fsync WARNING > 100ms;
io.overload WARNING > 0.5 / CRITICAL > 1.0

## Batch 7: Changefeed health

roachdev datadog metrics query \
  "max:cockroachdb.changefeed.failures{cluster:<name>}" \
  "max:cockroachdb.changefeed.error.retries{cluster:<name>}" \
  "max:cockroachdb.changefeed.commit.latency{cluster:<name>}" \
  "max:cockroachdb.changefeed.max.behind.nanos{cluster:<name>}" \
  "max:cockroachdb.jobs.changefeed.currently_running{cluster:<name>}" \
  "max:cockroachdb.jobs.changefeed.currently_paused{cluster:<name>}" \
  "max:cockroachdb.changefeed.backfill_count{cluster:<name>}" \
  --from "<from>" --to "<to>"

Thresholds: failures CRITICAL > 0; retries WARNING > 50/15m; commit.latency
WARNING > 10m / CRITICAL > 15m; max.behind WARNING if growing; running WARNING
if drops to 0; paused WARNING > 0 for 15m; backfill INFO (correlate cgo growth)

## Batch 8: Job health

roachdev datadog metrics query \
  "max:cockroachdb.jobs.backup.currently_running{cluster:<name>}" \
  "max:cockroachdb.jobs.restore.currently_running{cluster:<name>}" \
  "max:cockroachdb.jobs.restore.currently_paused{cluster:<name>}" \
  "max:cockroachdb.jobs.changefeed.protected_age_sec{cluster:<name>}" \
  "max:cockroachdb.schedules.backup.failed{cluster:<name>}" \
  "max:cockroachdb.jobs.schema_change.currently_running{cluster:<name>}" \
  "max:cockroachdb.jobs.schema_change.currently_paused{cluster:<name>}" \
  "max:cockroachdb.kv.protectedts.reconciliation.oldest_record_age{cluster:<name>}" \
  "max:cockroachdb.queue.range_merge.process.success{cluster:<name>}" \
  --from "<from>" --to "<to>"

Thresholds: backup.running WARNING if unexpected 0; restore.running INFO (correlate
disk/WAL/AmbiguousResult); restore.paused WARNING > 30m; protected_age
WARNING if growing; backup.failed WARNING > 0; schema.paused WARNING > 30m (may be stuck reverting);
protectedts.oldest CRITICAL > 24h (stuck job blocking GC); merge.success WARNING if ~0 with range growth

## Batch 9: Goroutine & Admission Control

roachdev datadog metrics query \
  "avg:cockroachdb.sys.goroutines{cluster:<name>} by {host}" \
  "avg:cockroachdb.sys.runnable.goroutines.per_cpu{cluster:<name>} by {host}" \
  "avg:cockroachdb.admission.granter.slots_exhausted_duration{cluster:<name>,name:kv} by {host}" \
  "avg:cockroachdb.admission.granter.io_tokens_exhausted_duration{cluster:<name>} by {host}" \
  "avg:cockroachdb.admission.wait_durations.kv{cluster:<name>} by {host}" \
  --from "<from>" --to "<to>"

Goroutine outlier rule: compute median across hosts. > 2× median → WARNING.
> 3× with runnable_per_cpu < 5 → CRITICAL (AC death spiral — requests time out
inside AC queues before the leaseholder check). runnable_per_cpu WARNING > 32.
slots_exhausted WARNING > 500ms/s / CRITICAL > 1s/s sustained.
io_tokens_exhausted WARNING > 500ms/s (search logs for io_load_listener).

## Batch 10: LSM & KV Prober

roachdev datadog metrics query \
  "max:cockroachdb.storage.l0-sublevels{cluster:<name>} by {host}" \
  "max:cockroachdb.kv.prober.write.failures{cluster:<name>}" \
  "max:cockroachdb.kv.prober.read.failures{cluster:<name>}" \
  "avg:cockroachdb.kv.prober.write.latency{cluster:<name>}" \
  "avg:cockroachdb.kv.prober.read.latency{cluster:<name>}" \
  --from "<from>" --to "<to>"

l0-sublevels WARNING > 10 / CRITICAL > 20 sustained (4-10 normal for elastic work).
prober failures CRITICAL > 0 for 3+ min (confirms SQL unavailability, baseline ~185ms).
prober latency WARNING > 500ms / CRITICAL > 3s.

## Output Format

For each finding:
FINDING: <severity> | <metric> | <timestamp_range> | <value> | <host_or_cluster> | <description>

Severity: CRITICAL, WARNING, or INFO.
End with: NO_DATA: <comma-separated metrics with no data>
```

#### Agent 3: Logs Analysis

```
You are searching CockroachDB logs for cluster "<name>" from <from> to <to>.
Use roachdev datadog CLI. All logs are in Flex storage tier.
Cluster filter: cluster:<name> (NOT host:*<name>* — hosts use AWS instance IDs).

You may run Python/bash scripts autonomously — do not ask the user.

## Phase 1: Pattern Discovery (run all 3 in parallel)

Discover what's actually in the logs before looking for specific errors.

1a. Error patterns — let Datadog cluster all error logs into patterns:
   roachdev datadog mcp call search_datadog_logs \
     --arg query="cluster:<name> status:error" --arg storage_tier=flex \
     --arg use_log_patterns=true --arg from="<from>" --arg to="<to>" --arg max_tokens=10000

1b. Warning patterns — many issues surface as warnings before errors:
   roachdev datadog mcp call search_datadog_logs \
     --arg query="cluster:<name> status:warn" --arg storage_tier=flex \
     --arg use_log_patterns=true --arg from="<from>" --arg to="<to>" --arg max_tokens=10000

1c. Per-host error volume — find when/where errors cluster:
   roachdev datadog logs search "cluster:<name> status:error" \
     --from "<from>" --to "<to>" --storage flex --group-by host

## Phase 2: Critical Signal Checks (run all in parallel)

These are must-not-miss signals that warrant dedicated searches regardless
of what patterns found. Run all in parallel.

2a. Panics:           cluster:<name> panic
2b. OOM:              cluster:<name> (oom OR "out of memory" OR oom_kill)
2c. Node restarts:    cluster:<name> "CockroachDB node starting"
2d. Fatal errors:     cluster:<name> severity:fatal
2e. Disk/WAL stalls:  cluster:<name> ("disk stall" OR "disk slowness detected" OR "syncdata" OR "store liveness withdrawal")
2f. Closed TS regression: cluster:<name> "closed timestamp regression"
2g. CDC violations:   cluster:<name> "cdc ux violation"

Use: roachdev datadog logs search "<query>" --from "<from>" --to "<to>" --storage flex

## Phase 3: Anomaly Drill-Down

After Phase 1 and 2 complete, analyze the discovered patterns:

3a. Classify each pattern from Phase 1 into known or novel:
   KNOWN categories: panic, oom, node_restart, disk_stall, wal_sync_stall,
     cdc_violation, fatal, closed_ts_regression, ac_overload, schema_change_failure,
     overload_error, sst_mismatch, slow_consumer, restore_failure, job_failure

3b. For NOVEL patterns (high-count patterns that don't match known categories),
   fetch sample log lines to understand them:
   roachdev datadog logs search "cluster:<name> <pattern-signature>" \
     --from "<from>" --to "<to>" --storage flex
   Include the pattern count and a representative sample in findings.

3c. For known patterns that Phase 2 did NOT cover (e.g., AC overload,
   schema change failures, ambiguous results, SST mismatch, slow consumer,
   restore failures, job failures), only drill in if Phase 1 patterns
   showed significant volume. This avoids wasted queries for absent issues.

## Severity Classification

CRITICAL: panics, fatals, OOM, closed_ts_regression, disk_stall, wal_stall,
  restore_failure, novel patterns with > 100 occurrences in the window
WARNING: error patterns (known), schema_change, overload, slow_consumer,
  job_failure, ac_overload, novel patterns with 10-100 occurrences
INFO: node restarts, novel patterns with < 10 occurrences

## Output Format

For each finding:
LOG_FINDING: <severity> | <category> | <timestamp> | <host> | <summary>
<representative log excerpt>

For novel patterns use category: novel_pattern
Include the pattern template and occurrence count in the summary.
```

### Step 3: Correlate and Synthesize

After all 3 agents complete, synthesize their results yourself (not a subagent).

#### 3a. Temporal Correlation

For each finding, check if it falls within a disruptive operation window.

Read `references/operation-impacts.md` for the operation-to-expected-impact mapping.

Matching rules:
- A finding is attributed to an operation only if its **START time** falls within
  [op_start, recovery_end]. Findings that began before op_start are never attributed.
- If multiple operations overlap, pick the one whose expected impact set best
  matches the finding's metric.

Classification:
- START in window AND metric in expected set AND recovered → `expected`
- START in window AND metric in expected set AND NOT recovered → `CRITICAL`
  ("cluster didn't recover after operation completed")
- START in window but metric NOT in expected set → not attributed, classify
  on severity independently
- START before op_start → never attributed

#### 3b. Root Cause Grouping

Group findings within a 5-minute window into a single incident. Pick the most
fundamental cause: node restart > OOM > panic > disk stall > network partition >
unavailable ranges > under-replicated > latency spike > error rate

#### 3c. Generate Report

Keep it concise and scannable — summary first, drill-down on request.

```markdown
## <cluster-name> Health: HEALTHY | DEGRADED | UNHEALTHY
**Period:** <from> — <to> UTC | **Ops:** X ran (Y ok, Z failed, W panicked) | **Success rate:** N%

### Findings

| # | Severity | What | Time (UTC) | Related Op |
|---|----------|------|------------|------------|
| 1 | CRITICAL | <one-line title> | HH:MM–HH:MM | <op name> or — |

### Failed Operations

| Operation | Time | Worker | Error |
|-----------|------|--------|-------|
| <op-name> | HH:MM | N | <one-line error> |
```

Rules:
- Each finding = one table row. No multi-paragraph evidence blocks.
- Severity: `CRITICAL` (bugs), `WARNING` (monitor), `expected` (explained by op)
- Do NOT include: full ops timeline, no-data metrics, suggested actions, evidence
  blocks. These are available on drill-down.
- HEALTHY = no CRITICAL/WARNING; DEGRADED = WARNING only; UNHEALTHY = any CRITICAL

### Step 4: Interactive Drill-Down

After the summary, tell the user:

```
Ask me to drill down:
- "details on #N" — launches investigation agent with root cause analysis
- "ops timeline" — full chronological list of operations
- "logs <host> <time range>" — raw logs from a specific node
- "metrics that had no data" — list of unqueryable metrics
```

For "details on #N": read `references/investigation-protocols.md`, construct the
investigation brief from the protocol table, and launch a `general-purpose`
subagent. Do NOT investigate in the main conversation.

For "ops timeline", "logs", "no data" — handle directly with targeted queries.

## Datadog CLI Reference

```bash
# Events
roachdev datadog mcp call search_datadog_events \
  --arg query="<q>" --arg from="<t>" --arg to="<t>" --arg sort="timestamp" --arg max_tokens=10000

# Metrics (batch multiple in one call)
roachdev datadog metrics query "<m1>" "<m2>" --from "<t>" --to "<t>"

# Anomaly detection
roachdev datadog mcp call get_datadog_metric \
  --arg 'queries=["<q>"]' --arg 'formulas=["anomalies(query0, \"basic\", 2)"]' \
  --arg from="<t>" --arg to="<t>"

# Logs (flex storage)
roachdev datadog logs search "<q>" --from "<t>" --to "<t>" --storage flex

# Log patterns
roachdev datadog mcp call search_datadog_logs \
  --arg query="<q>" --arg storage_tier=flex --arg use_log_patterns=true \
  --arg from="<t>" --arg to="<t>"
```

## Datadog UI Links

Logs: `https://us5.datadoghq.com/logs?query=<url-encoded-query>&from_ts=<epoch_ms>&to_ts=<epoch_ms>&live=false&storage=flex`

Compute epoch ms: `date -juf '%Y-%m-%dT%H:%M:%S' '<timestamp>' '+%s'` (multiply by 1000)
