# Datadog Query Templates for MMA Investigation

Pre-built query templates for investigating MMA behavior. Replace `{cluster}`
with the actual cluster name/tag and adjust time ranges as needed.

Use the built-in `datadog` skill for reading from to Datadog.

**Important:** All CockroachDB metrics in Datadog use the `cockroachdb.`
prefix (e.g. `cockroachdb.mma.store.cpu.utilization`).

## Reference Dashboard

The team uses the **MMA Enriched** dashboard to monitor MMA behavior:

- **Dashboard ID:** `a7p-9t8-pyf`
- **Template variables:** `cluster`, `node_id`, `store`, `upload_id`
- **Link template:**
  ```
  https://us5.datadoghq.com/dashboard/a7p-9t8-pyf/mma-enriched?tpl_var_cluster%5B0%5D={cluster}&from_ts={from_ms}&to_ts={to_ms}&live=false
  ```

When presenting findings, always link to this dashboard filtered to the
cluster and time window under investigation.

## Metric Queries

Use the Datadog MCP `get_datadog_metric` tool for timeseries data. All metric
queries support `from`/`to` parameters for time scoping.

### 1. Resource Balance Across Stores (Start Here)

These are the primary metrics for assessing cluster balance — the same metrics
used in the MMA Enriched dashboard.

**CPU balance:**
```
# CPU load per node (nanos/sec of CPU attributed to KV work)
avg:cockroachdb.rebalancing.cpunanospersecond{cluster:{cluster}} by {node_id}

# System CPU utilization per node (normalized %)
avg:cockroachdb.sys.cpu.combined.percent.normalized{cluster:{cluster}} by {node_id}

# System CPU per store (weighted, used in dashboard)
sum:cockroachdb.sys.cpu.combined.percent.normalized{cluster:{cluster}} by {node_id,store}.weighted()

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

### General MMA Log Search

```
# All MMA rebalancing logs for a cluster
host:{host} "rebalanceStores begins"

# Rebalancing pass summaries (Infof level — always visible)
host:{host} "rebalancing pass"
```

Note: filter by `host:` (the specific host name, e.g. `wenyi-skew-0006`) or
`service:` depending on how the cluster's logs are tagged. Try both if one
returns no results.

### Overload State Transitions

```
host:{host} "overload-start"
host:{host} "overload-end"
host:{host} "overload-continued"
host:{host} "was added to shedding store list"
host:{host} "skipping overloaded store"
```

### Candidate Evaluation and Outcomes

```
host:{host} "considering lease-transfer"
host:{host} "considering replica-transfer"
host:{host} "no candidates found"
host:{host} "no suitable target found"
host:{host} "result(success)"
host:{host} "result(failed)"
```

### Grace Periods and Limits

```
host:{host} "in lease shedding grace period"
host:{host} "reached max range move count"
host:{host} "reached pending decrease threshold"
host:{host} "too soon after failed change"
```

### Tracing a Specific Pass

```
host:{host} mmaid={N}
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

Note: set the `filter` parameter to `host:{host}` to scope these queries.

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
