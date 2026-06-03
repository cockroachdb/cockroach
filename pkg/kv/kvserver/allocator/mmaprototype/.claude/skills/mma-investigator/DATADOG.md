# Datadog Guide for MMA Investigation

Query templates and Datadog-specific tips for investigating MMA behavior.
Replace `{cluster}` with the actual cluster name/tag and adjust time ranges as
needed.

Use the built-in `datadog` skill for guidance on Datadog MCP tool usage, and
prefer the MCP tools for both metrics and logs.

MMA-specific tips:
- **Metric prefix.** All CockroachDB metrics in Datadog use the `cockroachdb.`
  prefix (e.g. `cockroachdb.mma.store.cpu.utilization`, not
  `mma.store.cpu.utilization`).
- **Log storage tier.** Query the **Flex tier** for logs
  (`storage_tier: "flex"` or `"flex_and_indexes"`); most CockroachDB logs are
  only in Flex storage.

## Reference Dashboard

The team uses the **MMA Enriched** dashboard to monitor MMA behavior:

- **Dashboard ID:** `a7p-9t8-pyf`
- **Template variables:** `cluster`, `node_id`, `store`, `upload_id`
- **Link template:**
  ```
  https://us5.datadoghq.com/dashboard/a7p-9t8-pyf/mma-enriched?tpl_var_cluster%5B0%5D={cluster}&from_ts={from_ms}&to_ts={to_ms}&live=false
  ```

When presenting findings, always link to this dashboard filtered to the
cluster and time window under investigation. Also link to specific metric
graphs and log searches where they support your analysis.

## Metric Queries

Use the Datadog MCP `get_datadog_metric` tool for timeseries data. All metric
queries support `from`/`to` parameters for time scoping.

**Reading the procession.** To see a metric's evolution across the window without
dumping raw CSV, call `get_datadog_metric` with `raw_data=false` — it returns
~20 buckets of min/max/avg per series. Query `by {node_id}` (or `by {store}`) so
you can compare nodes; a bucket whose `max` ≫ `avg`, or a level shift across
buckets, is the signal to zoom in on. For sub-unit metrics (e.g.
`*.percent_normalized`, range 0–1), the 20-bucket avgs can round to 0/1 and hide
a real swing (e.g. 0.40→0.62) — scale up, or re-query a narrower window with
float formatting. Confirm the cluster tag resolves first
(`crl-prod-<id>` for Cloud — see Troubleshooting Missing Data).

### 1. Resource Balance Across Stores (Start Here)

These are the primary metrics for assessing cluster balance — the same metrics
used in the MMA Enriched dashboard.

**CPU balance:**
```
# CPU load per node (nanos/sec of CPU attributed to KV work)
avg:cockroachdb.rebalancing.cpunanospersecond{cluster:{cluster}} by {node_id}

# Host (whole-machine) CPU per node — the primary physical CPU signal
avg:cockroachdb.sys.cpu.host.combined.percent_normalized{cluster:{cluster}} by {node_id}

# CRDB-process CPU per node (Datadog uses the underscore form: percent_normalized)
avg:cockroachdb.sys.cpu.combined.percent_normalized{cluster:{cluster}} by {node_id}

# System CPU per store (weighted, used in dashboard)
sum:cockroachdb.sys.cpu.combined.percent_normalized{cluster:{cluster}} by {node_id,store}.weighted()

# MMA's view of CPU utilization per store
avg:cockroachdb.mma.store.cpu.utilization{cluster:{cluster}} by {store}

# Per-replica CPU distribution (p90, p95, p99 — identifies hot replicas)
p90:cockroachdb.rebalancing.replicas.cpunanospersecond{cluster:{cluster}} by {instance}
p99:cockroachdb.rebalancing.replicas.cpunanospersecond{cluster:{cluster}} by {instance}

# Cluster-wide CPU mean (single line for reference)
avg:cockroachdb.rebalancing.cpunanospersecond{cluster:{cluster}} by {cluster}
```

**Write bandwidth balance:**
```
# Write bandwidth per node/store (weighted — what MMA sees)
sum:cockroachdb.rebalancing.writebytespersecond{cluster:{cluster}} by {node_id,store}.weighted()

# Write bandwidth per node (aggregate view)
sum:cockroachdb.rebalancing.writebytespersecond{cluster:{cluster}} by {node_id}.weighted()

# Host-level disk write bytes (physical I/O, includes write amp)
avg:cockroachdb.sys.host.disk.write.bytes{cluster:{cluster}} by {node_id}.as_rate()

# Storage engine disk writes per store (physical I/O by store)
sum:cockroachdb.storage.disk.write.bytes{cluster:{cluster}} by {node_id,store}.as_rate()
```

**Disk balance:**
```
# Disk capacity used per node
avg:cockroachdb.capacity.used{cluster:{cluster}} by {node_id}

# Disk capacity available per node
avg:cockroachdb.capacity.available{cluster:{cluster}} by {node_id}

# Disk capacity available per store
avg:cockroachdb.capacity.available{cluster:{cluster}} by {store}

# Total disk capacity per node
avg:cockroachdb.capacity.total{cluster:{cluster}} by {node_id}
```

**Replica and lease distribution:**
```
# Replica count per store
sum:cockroachdb.replicas.total{cluster:{cluster}} by {instance}

# Leaseholder count per store
sum:cockroachdb.replicas.leaseholders{cluster:{cluster}} by {instance}

# Leaseholder count per node
avg:cockroachdb.replicas.leaseholders{cluster:{cluster}} by {node_id}

# Leader count per node
avg:cockroachdb.replicas.leaders{cluster:{cluster}} by {node_id}
```

**Query load:**
```
# QPS per node
avg:cockroachdb.rebalancing.queriespersecond{cluster:{cluster}} by {node_id}

# Cluster-wide QPS mean
avg:cockroachdb.rebalancing.queriespersecond{cluster:{cluster}} by {cluster}

# Read bandwidth per node
avg:cockroachdb.rebalancing.readbytespersecond{cluster:{cluster}} by {node_id}
```

### 2. MMA Rebalancing Activity

**MMA-initiated operations:**
```
# Replica move outcomes
sum:cockroachdb.mma.change.rebalance.replica.success{cluster:{cluster}} by {node_id,store}.as_rate()
sum:cockroachdb.mma.change.rebalance.replica.failure{cluster:{cluster}} by {node_id,store}.as_rate()

# Lease transfer outcomes
sum:cockroachdb.mma.change.rebalance.lease.success{cluster:{cluster}} by {node_id,store}.as_rate()
sum:cockroachdb.mma.change.rebalance.lease.failure{cluster:{cluster}} by {node_id,store}.as_rate()
```

**External (non-MMA) operations registered with MMA:**
```
sum:cockroachdb.mma.change.external.replica.success{cluster:{cluster}} by {node_id,store}.as_rate()
sum:cockroachdb.mma.change.external.replica.failure{cluster:{cluster}} by {node_id,store}.as_rate()
sum:cockroachdb.mma.change.external.lease.success{cluster:{cluster}} by {node_id,store}.as_rate()
sum:cockroachdb.mma.change.external.lease.failure{cluster:{cluster}} by {node_id,store}.as_rate()
```

**Overloaded store tracking:**
```
# By duration bucket — indicates how long stores stay overloaded
sum:cockroachdb.mma.overloaded_store.lease_grace.success{cluster:{cluster}} by {node_id,store}
sum:cockroachdb.mma.overloaded_store.lease_grace.failure{cluster:{cluster}} by {node_id,store}
sum:cockroachdb.mma.overloaded_store.short_dur.success{cluster:{cluster}} by {node_id,store}
sum:cockroachdb.mma.overloaded_store.short_dur.failure{cluster:{cluster}} by {node_id,store}
sum:cockroachdb.mma.overloaded_store.medium_dur.success{cluster:{cluster}} by {node_id,store}
sum:cockroachdb.mma.overloaded_store.medium_dur.failure{cluster:{cluster}} by {node_id,store}
sum:cockroachdb.mma.overloaded_store.long_dur.success{cluster:{cluster}} by {node_id,store}
sum:cockroachdb.mma.overloaded_store.long_dur.failure{cluster:{cluster}} by {node_id,store}
```

Duration buckets indicate desperation level:
- `lease_grace` — remote store still in lease shedding grace period (~2 min)
- `short_dur` — recently overloaded, conservative candidate selection
- `medium_dur` — overloaded ~5+ min, relaxed candidate selection
- `long_dur` — overloaded ~8+ min, most aggressive candidate selection

**Rebalancing rates (general — includes both MMA and non-MMA):**
```
# Lease transfer rate per node
sum:cockroachdb.rebalancing.lease.transfers{cluster:{cluster}} by {node_id}.as_rate()

# Range rebalance rate per node
avg:cockroachdb.rebalancing.range.rebalances{cluster:{cluster}} by {node_id}.as_rate()

# Snapshot data movement (indicates replica moves in progress)
avg:cockroachdb.range.snapshots.sent_bytes{cluster:{cluster}} by {node_id}.as_rate()
sum:cockroachdb.range.snapshots.rebalancing.rcvd_bytes{cluster:{cluster}} by {cluster}.as_rate()
```

### 3. Other Rebalancing Components

These help distinguish MMA activity from other rebalancing sources.

```
# Replicate queue activity
avg:cockroachdb.queue.replicate.process.success{cluster:{cluster}} by {node_id}.as_rate()
avg:cockroachdb.queue.replicate.process.failure{cluster:{cluster}} by {node_id}.as_rate()
avg:cockroachdb.queue.replicate.pending{cluster:{cluster}} by {node_id}

# Replicate queue operation types
sum:cockroachdb.queue.replicate.addreplica{cluster:{cluster}} by {node_id}.as_rate()
sum:cockroachdb.queue.replicate.removereplica{cluster:{cluster}} by {node_id}.as_rate()
sum:cockroachdb.queue.replicate.rebalancereplica{cluster:{cluster}} by {node_id}.as_rate()
sum:cockroachdb.queue.replicate.transferlease{cluster:{cluster}} by {node_id}.as_rate()

# Lease preference health
avg:cockroachdb.leases.preferences.violating{cluster:{cluster}} by {node_id}
avg:cockroachdb.leases.preferences.less_preferred{cluster:{cluster}} by {node_id}

# Lease transfer errors
avg:cockroachdb.leases.transfers.error{cluster:{cluster}} by {node_id}.as_rate()

# Follow-the-workload lease transfers
avg:cockroachdb.kv.allocator.load_based_lease_transfers.follow_the_workload{cluster:{cluster}} by {node_id}.as_rate()
```

### 4. Cluster and System Health

```
# Range health
avg:cockroachdb.ranges.underreplicated{cluster:{cluster}} by {node_id}
avg:cockroachdb.ranges.overreplicated{cluster:{cluster}} by {node_id}
avg:cockroachdb.ranges.unavailable{cluster:{cluster}} by {node_id}

# Node liveness
sum:cockroachdb.liveness.livenodes{cluster:{cluster}} by {node_id,store}

# LSM health
avg:cockroachdb.storage.l0_sublevels{cluster:{cluster}} by {node_id}

# IO admission control
avg:cockroachdb.admission.io.overload{cluster:{cluster}} by {node_id}

# Disk latency
p99:cockroachdb.storage.wal.fsync.latency{cluster:{cluster}} by {instance}

# Query latency
p99.9:cockroachdb.sql.service.latency{cluster:{cluster}} by {node_id}
p99.9:cockroachdb.exec.latency{cluster:{cluster}} by {node_id}
```

### 5. MMA Operational Health

```
# Operations dropped (state inconsistency)
sum:cockroachdb.mma.dropped{cluster:{cluster}} by {node_id,store}.as_rate()

# External operation registration
sum:cockroachdb.mma.external.registration.success{cluster:{cluster}} by {node_id,store}.as_rate()
sum:cockroachdb.mma.external.registration.failure{cluster:{cluster}} by {node_id,store}.as_rate()

# Span config normalization issues
sum:cockroachdb.mma.span_config.normalization.error{cluster:{cluster}} by {node_id,store}
max:cockroachdb.mma.span_config.normalization.soft_error{cluster:{cluster}} by {node_id}
```

## Log Queries

Use the Datadog MCP `search_datadog_logs` tool. **Always set
`storage_tier: "flex"`** — most logs are in Flex storage.

### Scope by attribute, not free text

MMA logs carry structured attributes; scope with these (plus `cluster:` and the
time window) rather than grepping raw text. Confirmed on a CC cluster:

```
# Tightest: the MMA algorithm logs only (pass summary, load summaries,
# candidate evaluation, results). @file is the source path.
cluster:{cluster} @file:*mmaprototype*

# Broader: the whole KV distribution layer (MMA + replicate/lease queue +
# replica-change enactment). Use when you also want surrounding queue activity.
cluster:{cluster} @channel:KV_DISTRIBUTION

# All lines within ONE rebalanceStores pass share an mmaid tag. It is a nested
# tag: query as @tags.mmaid (NOT @mmaid). @n / @s give node / store.
cluster:{cluster} @tags.mmaid:{N}
```

Do **not** filter on `status:error` to find MMA problems — in Cloud that is
dominated by log-sink/telemetry noise (`fluentSink … connection refused`,
`fluent-bit`), not CRDB. MMA has no dedicated error log; failures appear as
`result(failed): …` reasons inside the reports below.

### The three Infof tiers (what survives in production)

Detailed MMA logs sit at `VEvent` level 2/3 and are suppressed in prod; only
these promoted tiers are reliably present (see the `Logging` section of the
package `CLAUDE.md`):

```
# 1. Per-pass summary — every pass, per local store. The backbone.
cluster:{cluster} @file:*mmaprototype* "rebalancing pass summary"

# 2. Outer-loop narrative — promoted ~every 10 min.
cluster:{cluster} "cluster means:"
cluster:{cluster} "evaluating s"            # per-store sls / nls / worst dim
cluster:{cluster} "adding overloaded store"

# 3. Per-store detailed burst — promoted ~every 30 min for a store continuously
#    overloaded >=30 min. The full shedding attempt for that store.
cluster:{cluster} "start processing shedding store s{N}"
```

### The richest line: per-store-per-dimension load summary

```
# Exact classification plus the numbers (incl. the capacity-model output), e.g.:
#   load summary for dim=CPURate (s37): overloadSlow, reason: load is >10% above
#   mean [load= meanLoad= fractionUsed= meanUtil= capacity=]
cluster:{cluster} "load summary for dim"
```

### Overload state transitions

```
cluster:{cluster} "overload-start"
cluster:{cluster} "overload-end"
cluster:{cluster} "overload-continued"
```

### Candidate evaluation and outcomes (inside a detailed burst)

```
cluster:{cluster} "considering lease-transfer"
cluster:{cluster} "considering replica-transfer"
cluster:{cluster} "result(success)"     # actual movement, with resulting loads
cluster:{cluster} "result(failed)"      # carries the reason, e.g. no-cand-load
cluster:{cluster} "in lease shedding grace period"
cluster:{cluster} "reached max lease transfer count"
cluster:{cluster} "reached pending decrease threshold"
```

### Tracing a specific pass

```
cluster:{cluster} @tags.mmaid:{N}
```

## SQL Log Analytics

Use the Datadog MCP `analyze_datadog_logs` tool for aggregation. Always set
`storage_tier: "flex"`. Keep filters narrow to avoid Flex tier timeouts.

```sql
-- Count rebalancing passes over time
SELECT date_trunc('hour', timestamp) as hour, count(*)
FROM logs
WHERE message LIKE '%rebalanceStores begins%'
GROUP BY date_trunc('hour', timestamp)
ORDER BY date_trunc('hour', timestamp)

-- Count successes vs failures
SELECT
  CASE
    WHEN message LIKE '%result(success)%' THEN 'success'
    WHEN message LIKE '%result(failed)%' THEN 'failure'
  END as outcome,
  count(*)
FROM logs
WHERE message LIKE '%result(%'
GROUP BY CASE
    WHEN message LIKE '%result(success)%' THEN 'success'
    WHEN message LIKE '%result(failed)%' THEN 'failure'
  END
```

Note: set the `filter` parameter to `cluster:{cluster}` (optionally with
`@file:*mmaprototype*`) to scope these queries.

## Troubleshooting Missing Data

If metrics or logs return empty/zero results where you'd expect data, check
these common causes before concluding the data doesn't exist:

1. **Missing `cockroachdb.` prefix on metrics.** All CockroachDB metrics in
   Datadog are prefixed with `cockroachdb.` (e.g. `cockroachdb.mma.store.cpu.utilization`,
   not `mma.store.cpu.utilization`). This is the most common cause of
   all-zero metric results.
2. **Wrong storage tier for logs.** Most CockroachDB logs are only in
   Flex storage. If `search_datadog_logs` returns nothing, make sure you're
   using `storage_tier: "flex_and_indexes"`.
3. **Incorrect tag names or values.** Verify tag names with the dashboard or
   `get_datadog_metric_context`. Common pitfalls:
   - The cluster name should be in `cluster`, or sometimes a substring of `hostname`
   - For CockroachDB Cloud clusters the `cluster` tag is `crl-prod-<id>` (e.g.
     `crl-prod-38z`), not the bare `<id>` from a debug-zip directory name or a
     store locality. Querying the bare id returns no data. Fastest tag check: run
     a normal scoped query (`roachdev datadog metrics query
     "avg:cockroachdb.<metric>{cluster:crl-prod-<id>}"`) — if it returns data the
     tag is right. (The `get_datadog_metric` "metadata mode" via the `roachdev …
     mcp call` path still demands a `queries=[…]` arg, so it is not a convenient
     tag-only probe.)
   - `store` vs `store_id` (check which tag key the metric actually uses)
   - `node_id` vs `instance`
4. **Time range mismatch.** Double-check that `from` and `to` match the
   investigation window. ISO 8601 timestamps must include timezone (use `Z`
   for UTC).
5. **Aggregation hiding signal.** A `sum` or `avg` across all stores may wash
   out per-store spikes. Try grouping by `store` or `node_id` to see
   individual series.
6. **Metric not yet emitted.** Some MMA metrics (e.g. `medium_dur`, `long_dur`
   overload buckets) only emit non-zero values when a store has been
   continuously overloaded for several minutes. Zero values may be correct.

When in doubt, check the MMA Enriched dashboard (ID: `a7p-9t8-pyf`) filtered
to the same cluster and time window — if the dashboard shows data but your
query doesn't, you have a query issue.

## Metric Discovery

If the predefined queries don't cover your case:

```
# Find all MMA metrics
name_filter: "cockroachdb.mma.*"

# Find rebalancing metrics
name_filter: "cockroachdb.rebalancing.*"

# Find queue metrics
name_filter: "cockroachdb.queue.*"
```

Use `get_datadog_metric` (metadata mode) with `include_tag_values: true` to
discover available tag values for scoping.
