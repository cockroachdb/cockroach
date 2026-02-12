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

## Query Datadog CPU metrics

```bash
# Query avg CPU user time per host for the last 10 minutes.
DD_SITE=us5.datadoghq.com pup metrics query \
  --query="avg:system.cpu.user{cluster:tobias-spike} by {host}" \
  --from="10m" --to="now" --output=table
```
