# Lease Preference Spike Experiment

**TLDR:** We reproduced the DistSender concurrency → scheduler latency spike
described in [#162386](https://github.com/cockroachdb/cockroach/issues/162386).
Spanning *scans* can't trigger it (the SQL layer sets `TargetBytes`, forcing
sequential dispatch), but **batched multi-row upserts** with uniformly random
keys do the trick: each 1000-row upsert fans out across ~1000 ranges in
parallel through DistSender. At just 10 ops/sec on a 3-node cluster with 5000
splits and all leases pinned to n1, the leaseholder's p99 scheduler latency
jumps from ~70k ns to **~3ms** and pmax hits **21ms**. The goroutine burst
happens on the *leaseholder* (n1), not the gateway (n3) — and it's driven by
**intent resolution**, not the write proposals themselves. After each 1000-key
transaction commits, the `IntentResolver`'s `RequestBatcher` spawns ~235
goroutines to resolve write intents across ranges, cascading into ~305
runnable goroutines within ±1ms.

---

This experiment sets up a 3-node CockroachDB cluster with a 4th node running
workload, pins all leaseholders to node 1 via zone config, and then queries
Datadog for CPU metrics to observe the effect.

## Prerequisites

- `roachprod` available on PATH
- `DD_API_KEY` and `DD_APP_KEY` environment variables set
- `DD_SITE` set to `us5.datadoghq.com` (note: must be full domain, not just `us5`)
- `pup` (Datadog CLI): `go install github.com/DataDog/pup@latest`

## Setup

```bash
# Helper to avoid repeating the cluster name everywhere.
function c() {
  echo tobias-spike
}

# Create a 4-node cluster (3 CRDB nodes + 1 workload node).
roachprod create -n 4 $(c) --gce-machine-type n2-standard-16

# Wire up Datadog telemetry on the CRDB nodes.
roachprod opentelemetry-start $(c):1-3 --datadog-api-key=$DD_API_KEY
roachprod fluent-bit-start $(c):1-3 --datadog-api-key=$DD_API_KEY

# Stage and start CockroachDB on nodes 1-3.
roachprod stage $(c) cockroach
roachprod start $(c):1-3
```

## Configure lease preferences

```bash
# Create the database and pin leases to node1.
roachprod sql $(c):1 -- -e "
CREATE DATABASE IF NOT EXISTS kv;
ALTER DATABASE kv CONFIGURE ZONE USING constraints = COPY FROM PARENT, lease_preferences = '[[+node1]]';
"
```

## Initialize workload

```bash
# Init the kv workload with 5000 splits from the workload node.
roachprod run $(c):4 -- ./cockroach workload init kv --splits 5000 {pgurl:1}
```

## Verify lease placement

```bash
# Confirm all leases landed on node 1.
roachprod sql $(c):1 -- -e "
SELECT lease_holder, count(*) AS num_ranges
FROM [SHOW RANGES FROM DATABASE kv WITH DETAILS]
GROUP BY lease_holder
ORDER BY lease_holder;
"
```

## Run spanning queries

The goal is to show that even a modest rate of fan-out queries can spike p99
scheduler latencies above the 1ms threshold on the leaseholder node.

We use node 3 as the gateway so that all fan-out requests arrive at node 1
(the leaseholder) over the network. `--concurrency` needs to match `--max-rate`
because each spanning query takes ~1s to complete (fans out across all 5000
ranges), so a single worker can only sustain 1 op/sec.

```bash
# 15 spanning queries/sec through n3 for 5 minutes.
roachprod run $(c):4 -- ./cockroach workload run kv \
  --read-percent=0 \
  --span-percent=100 \
  --span-limit=0 \
  --concurrency=15 \
  --max-rate=15 \
  --duration=5m \
  '{pgurl:3}'
```

Note: `--read-percent=0` because read + span + del must sum to 100; setting
span-percent=100 means all operations are spanning queries.

## Observe scheduler latency

Two metrics to watch:

1. **Gauge (sampled every ~15s, can miss spikes):**
   `cockroachdb.admission.scheduler_latency_listener.p99_nanos`
   Internally updates every 100ms with the p99 over that window, but because
   Datadog only scrapes the gauge every ~15s it can easily miss short spikes.

2. **Histogram (the real thing):**
   `cockroachdb.go.scheduler_latency` — query both p99 and pmax via Datadog's
   aggregation prefixes.

```bash
# Gauge: p99 scheduler latency listener (may miss spikes).
DD_SITE=us5.datadoghq.com pup metrics query \
  --query="max:cockroachdb.admission.scheduler_latency_listener.p99_nanos{cluster:tobias-spike} by {host}" \
  --from="10m" --to="now" --output=table

# Histogram p99: go scheduler latency.
DD_SITE=us5.datadoghq.com pup metrics query \
  --query="p99:cockroachdb.go.scheduler_latency{cluster:tobias-spike} by {host}" \
  --from="10m" --to="now" --output=table

# Histogram pmax: go scheduler latency — catches worst-case spikes.
DD_SITE=us5.datadoghq.com pup metrics query \
  --query="max:cockroachdb.go.scheduler_latency{cluster:tobias-spike} by {host}" \
  --from="10m" --to="now" --output=table
```

### Results

**Baseline (no workload):** All nodes show p99 ~57k ns and pmax ~160-230k ns.

**At 1 span/sec (concurrency=1, max-rate=1, gateway=n2+n3):**
- Histogram p99: leaseholder (n1) ~90-120k ns; gateways ~80-95k ns.
- Histogram pmax: leaseholder spikes to ~400-917k ns; gateways stay ~160-230k ns.
- Not enough to consistently cross 1ms.

**At ~13 span/sec (concurrency=15, max-rate=15, gateway=n3 only):**
- Gauge p99: leaseholder ~70-105k ns (misses the real spikes as expected).
- Histogram p99: leaseholder ~90-120k ns; gateway (n3) ~80-105k ns; idle node (n2) ~43-47k ns.
- Histogram pmax: **leaseholder hits 1,310,720 ns (1.3ms)** — crosses the 1ms
  threshold. Gateway (n3) regularly hits 524-786k ns. Idle node (n2) stays at
  ~163k ns.

Key takeaway: just ~13 fan-out queries/sec across 5000 ranges is enough to push
the leaseholder's pmax scheduler latency above 1ms on n2-standard-16 machines.

## Query Datadog CPU metrics

```bash
# Query avg CPU user time per host for the last 10 minutes.
DD_SITE=us5.datadoghq.com pup metrics query \
  --query="avg:system.cpu.user{cluster:tobias-spike} by {host}" \
  --from="10m" --to="now" --output=table
```

## Execution trace & schedstat

Capture a Go execution trace from the leaseholder while the workload is running:

```bash
curl -sSL "http://tobias-spike-0001.roachprod.crdb.io:26258/debug/pprof/trace?seconds=10" \
  -o /tmp/trace-tobias-spike-n1.out

# Analyze scheduling latency from the trace.
schedstat /tmp/trace-tobias-spike-n1.out

# Or view in the Go trace viewer.
go tool trace /tmp/trace-tobias-spike-n1.out
```

### schedstat results (at 13 span/sec)

```
Trace duration: 10000.7ms

--- Scheduling Latency (runnable → running) ---
Events: 1672197
  min: 1ns         p50: 2.0µs       p90: 32.4µs
  avg: 9.7µs       p99: 75.9µs      max: 540.1µs

--- Anomalies (p99 > 1ms per 100ms) ---
No anomalies detected.
```

The trace-level scheduling latency (p99=75.9us, max=540us) is well below 1ms. The
worst delays (~500us) involve bursts of ~50 goroutines becoming runnable within
±1ms, mostly from gRPC stream handling (`(*recvBuffer).put` waking up stream
readers). On 16 vCPUs the scheduler absorbs these comfortably.

The 1.3ms spikes seen in the Datadog histogram pmax but not in the 10s trace are
likely transient events (GC pauses, burst overlap) that a short trace window
didn't capture.

## Analysis: why DistSender doesn't parallelize these scans

The spanning query (`SELECT count(v) FROM [SELECT v FROM kv]`) produces a full
table `Scan /Table/106/{1-2}`. A statement bundle shows the DistSender sends
**5001 sequential single-Scan batches** (one per range), not a parallel fan-out:

```
[/cockroach.roachpb.Internal/Batch: {count: 5001, duration 1.4s}]
```

The plan confirms `distribution: local` — no DistSQL parallelism.

### Root cause: `TargetBytes` disables parallel dispatch

Each sub-batch has `target_bytes: 10485760` (10MB), set by the SQL layer's
`colbatchscan` to bound memory usage per KV fetch. This triggers the sequential
path in DistSender:

```go
// dist_sender.go:1949
canParallelize := !ba.MightStopEarly()

// batch.go:963-965
func (ba *BatchRequest) MightStopEarly() bool {
    h := ba.Header
    return h.MaxSpanRequestKeys != 0 || h.TargetBytes != 0 || h.ReturnElasticCPUResumeSpans
}
```

Since `TargetBytes != 0`, `MightStopEarly()` returns `true`, so
`canParallelize = false`. The DistSender iterates ranges sequentially,
decrementing the remaining byte budget after each response and stopping early
once exhausted.

### Implication for this experiment

The workload is **not** causing a goroutine burst per query. Each of the 13
concurrent queries iterates through 5001 ranges serially (~1ms per range,
~5s total per query). The scheduling latency impact comes from sustaining 13
concurrent serial scans, not from sudden parallel fan-out.

To produce true parallel fan-out across all ranges you would need:
- A DistSQL plan that distributes scan work across multiple processors/nodes, or
- A batch request without `TargetBytes`/`MaxSpanRequestKeys` (e.g. a
  `DeleteRange` or an `AdminScatter`), or
- A custom workload that sends explicit multi-range batch requests.

## Attempts to produce parallel fan-out via SQL

### Attempt 1: `DELETE ... WHERE k IN (SELECT unique_rowid() FROM generate_series(1, 1000))`

The idea: 1000 random-key point deletes in a single statement should produce
a single KV batch with 1000 `DeleteRequest`s, which DistSender would
parallelize (no `TargetBytes`/`MaxSpanRequestKeys`).

**What actually happens:** The optimizer produces a **lookup join** that does
1000 individual KV Gets (one per key), not a single batch:

```
└── • lookup join (inner)
        table: kv@kv_pkey
        equality: (unique_rowid) = (k)
        parallel
```

The `parallel` annotation means the lookup joiner parallelizes with limited
concurrency, but each lookup is a separate KV batch with a single Get. The
query takes ~7s (1000 round-trips at ~7ms each with some parallelism).
DistSender never sees a fat multi-range batch to fan out.

### Attempt 2: 1000 individual `DELETE FROM kv WHERE k = <key>` in one txn

The idea: many individual DELETE statements in an explicit `BEGIN/COMMIT`
block; the transaction's write pipeline would dispatch them in parallel.

**What actually happens:** Each DELETE is a separate SQL statement that
produces its own single-point KV batch. The write pipeline overlaps Raft
replication but doesn't batch them into a single DistSender dispatch. The
1000 DELETEs complete in ~6s — similar to the subquery approach, just with
pipelining overlap instead of lookup join parallelism. Still no goroutine
burst at the DistSender level.

### Why SQL can't easily produce this

The SQL layer always mediates KV access through patterns that either:
- Set `TargetBytes`/`MaxSpanRequestKeys` (scans), disabling parallel dispatch
- Use lookup joins (point lookups), which issue individual KV batches
- Execute statements one at a time (multi-statement txns)

To get true DistSender-level parallel fan-out, we likely need a custom Go
program that builds a single `roachpb.BatchRequest` containing many
`GetRequest`s or `ScanRequest`s (without byte/key limits) and sends it
directly through a KV client.

## Batched upserts: reproducing the spike

The `kv` workload's `--batch` flag controls how many rows are inserted per SQL
statement. With `--batch=1000`, each operation becomes a single multi-row
`UPSERT` with 1000 uniformly random keys spread across the 5001 ranges. Unlike
scans (which set `TargetBytes`), writes don't trigger `MightStopEarly()`, so
DistSender parallelizes the fan-out across all touched ranges.

```bash
# 10 batched upserts/sec (1000 rows each) through n3 for 5 minutes.
roachprod run $(c):4 -- ./cockroach workload run kv \
  --read-percent=0 \
  --batch=1000 \
  --concurrency=10 \
  --max-rate=10 \
  --duration=5m \
  '{pgurl:3}'
```

Workload latency: p50 ~19ms, p95 ~22ms per 1000-row upsert at steady 10 ops/sec.

### Scheduler latency results (batch=1000, 10 ops/sec)

**p99 scheduler latency (nanoseconds):**

| Host              | Baseline (pre-workload) | Under workload          |
|-------------------|-------------------------|-------------------------|
| n1 (leaseholder)  | ~70-75k ns              | **~2.7-3.0M ns (3ms)**  |
| n2 (idle)         | ~93-97k ns              | ~425-453k ns            |
| n3 (gateway)      | ~91-97k ns              | ~425-453k ns            |

**pmax scheduler latency (nanoseconds):**

| Host              | Baseline                | Under workload            |
|-------------------|-------------------------|---------------------------|
| n1 (leaseholder)  | ~163-393k ns            | **~12.6-21.0M ns (21ms)**|
| n2 (idle)         | ~163-262k ns            | ~10.5-14.7M ns            |
| n3 (gateway)      | ~163-656k ns            | ~6.3-10.5M ns             |

At just 10 multi-range upserts/sec, the leaseholder's p99 scheduler latency
crosses **3ms** — well past the 1ms threshold — and pmax hits **21ms**.

### Execution traces (batch=1000, 10 ops/sec)

Traces and schedstat output are in
[`scripts/experiment-batch1000-upsert-10qps/`](experiment-batch1000-upsert-10qps/).

```bash
# Captured 10s execution traces from n1 and n3 while the workload was running.
curl -sSL "http://tobias-spike-0001.roachprod.crdb.io:26258/debug/pprof/trace?seconds=10" \
  -o scripts/experiment-batch1000-upsert-10qps/n1.bin
curl -sSL "http://tobias-spike-0003.roachprod.crdb.io:26258/debug/pprof/trace?seconds=10" \
  -o scripts/experiment-batch1000-upsert-10qps/n3.bin

schedstat scripts/experiment-batch1000-upsert-10qps/n1.bin
schedstat scripts/experiment-batch1000-upsert-10qps/n3.bin
```

**n1 (leaseholder) — schedstat summary:**

```
Trace duration: 10000.8ms

--- Scheduling Latency (runnable → running) ---
Events: 1557821
  min: 1ns         p50: 41.5µs      p90: 1.14ms
  avg: 350.3µs     p99: 3.21ms      max: 21.76ms

--- Latency Spikes (p99 > 1ms per 100ms) ---
101 window(s) above threshold (showing top 5)
  [1] t=9900ms  p99=15.94ms  max=21.76ms  14755 events
  [2] t=7700ms  p99=4.73ms   max=6.18ms   16930 events
  [3] t=200ms   p99=4.65ms   max=5.71ms   14891 events
  [4] t=10000ms p99=4.64ms   max=4.79ms   207 events
  [5] t=7000ms  p99=4.64ms   max=6.39ms   15733 events

--- Runnable Spikes (>80 runnable per 100ms) ---
101 window(s) above threshold (showing top 5)
  [6] t=9300ms  peak 804 runnable
  [7] t=7700ms  peak 763 runnable
  [8] t=4700ms  peak 761 runnable
  [9] t=8200ms  peak 749 runnable
  [10] t=9900ms peak 720 runnable
```

Every 100ms window has elevated scheduling latency *and* elevated runnable
goroutine counts. The worst latency spike (21.76ms) involves a burst of
**305 goroutines** becoming runnable within ±1ms — 235 created by
`(*RequestBatcher).sendBatch`, plus 45 raft scheduler workers woken by
`(*Cond).Signal`. The longest single run blocking the queue:
`registryRecorder.record` holding a CPU for **14ms**.

The `RequestBatcher` here is the `IntentResolver`'s intent resolution batcher
(`irBatcher`), not the write path itself. The `RequestBatcher` library
(in `pkg/internal/client/requestbatcher`) is used for three purposes: (1)
intent resolution, (2) txn heartbeating, and (3) txn record GC. After each
1000-key upsert commits, the transaction needs to resolve its write intents
across ~1000 ranges. The `IntentResolver` queues these into `irBatcher`, which
groups them by range and fires off batches — each `sendBatch` spawns a
goroutine. So the goroutine storm is from the **cleanup phase** (intent
resolution) after commit, not the initial write proposals.

The runnable spike data reveals additional contention sources:
- **`(*tokenCounter).adjust`** (spike [9], t=8200ms): G906 unlocks
  `tokenCounterMu` and wakes **368 goroutines** blocked on
  `(*tokenCounterMu).RLock`, on top of raft scheduler and intent resolution
  activity.
- **Block cache contention** (spike [10], t=9900ms): 401 preempted goroutines
  with `(*shard).getWithReadEntry` blocked on an `RWMutex.RLock`, unblocked
  in a burst when G278027 calls `(*shard).set`.

**n3 (gateway) — schedstat summary:**

```
Trace duration: 10001.1ms

--- Scheduling Latency (runnable → running) ---
Events: 1554412
  min: 1ns         p50: 21.4µs      p90: 169.7µs
  avg: 63.5µs      p99: 521.8µs     max: 9.44ms

--- Latency Spikes (p99 > 1ms per 100ms) ---
1 window(s) above threshold

--- Runnable Spikes (>80 runnable per 100ms) ---
100 window(s) above threshold (showing top 5)
  [2] t=4200ms  peak 542 runnable
  [3] t=9800ms  peak 237 runnable
  [4] t=6700ms  peak 221 runnable
  [5] t=9700ms  peak 219 runnable
  [6] t=1800ms  peak 190 runnable
```

Only **1 latency spike** but **100 runnable spike windows** — the scheduler is
busy but on 16 CPUs it mostly keeps up. The DistSender fan-out is visible at
t=6700ms (spike [4]): **352 goroutines created by
`(*DistSender).sendPartialBatchAsync`** with G1012 (`(*http2Client).handleData`)
waking 315 gRPC stream readers when responses arrive. Despite the fan-out, n3
absorbs it without sustained latency spikes. The raft scheduler on n3 also sees
load (n3 holds replicas) with `HandleRaftRequest` and `enqueueRaftUpdateCheck`
as heavy unblockers.

### Key insight: the spike is on the leaseholder, not the gateway

The gateway (n3) runs the DistSender that parallelizes 1000 point writes
across ~1000 ranges. This does create runnable goroutine bursts (peak 542,
including 352 from `sendPartialBatchAsync`), but the 16 CPUs absorb them —
only 1 latency spike in 10s.

The leaseholder (n1) receives all ~1000 requests, processes the writes through
Raft, and then — after each transaction commits — the `IntentResolver`'s
`RequestBatcher` spawns ~235 goroutines to resolve write intents across all
the touched ranges. These intent resolution requests in turn wake raft
scheduler workers via `(*Cond).Signal`, producing cascading bursts of
**700-800 runnable goroutines** that overwhelm 16 CPUs. Additional contention
from `tokenCounter` lock release (368 goroutines) and block cache shard locks
(401 preempted goroutines) amplifies the effect.

The key distinction: the goroutine storm is not from the initial write
fan-out, but from the **post-commit intent resolution cleanup**. Each
committed transaction leaves behind write intents on ~1000 ranges that must
be resolved, and the `IntentResolver` does this with unbounded parallelism
via `RequestBatcher.sendBatch`.
