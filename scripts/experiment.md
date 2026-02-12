# Lease Preference Spike Experiment

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
