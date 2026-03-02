# MMA Reference Guide

This document provides architectural context for investigating MMA (Multi-Metric
Allocator) behavior on CockroachDB clusters. It covers big-picture concepts and
points to source code for implementation details. Since MMA is under active
development, **always read the source code for exact thresholds, constants, and
algorithmic details** rather than relying on hard-coded values in this document.

All source paths are relative to the CockroachDB repository root. The MMA
prototype lives under `pkg/kv/kvserver/allocator/mmaprototype/`.

## CockroachDB Rebalancing System Overview

MMA is one of several components that affect replica and lease placement.
Understanding MMA behavior requires understanding how it fits alongside the
others. Several of these components run simultaneously and independently, which
means rebalancing activity you observe may not be caused by MMA.

### Component Summary

| Component | Role |
|-----------|------|
| **Replica Scanner** | Periodically scans each store's replicas and enqueues them to registered queues. Active in both SMA and MMA modes. |
| **Replicate Queue** | Handles under/over-replicated ranges, dead/decommissioning replicas, range count convergence, diversity/constraint satisfaction, full disk shedding. Active in both modes. |
| **Lease Queue** | Handles invalid leases, lease count convergence, lease preferences from span configs, IO overload. Active in both modes. |
| **Allocator** | Stateless decision-making library. Does not initiate rebalancing. Called by the store rebalancer, replicate queue, and lease queue. |
| **Store Rebalancer (SMA)** | Load-based, single metric. **Disabled when MMA is enabled.** Only acts when local store exceeds cluster mean by a threshold. Attempts lease transfers first, then replicas. |
| **MMA** | Multi-metric. Replaces the Store Rebalancer. Computes `storeLoadSummary` per store, identifies overloaded stores, attempts lease transfers then replica moves. |

### What MMA Replaces vs What Still Runs

When MMA is enabled:
- The **Store Rebalancer** (SMA's load-based component) is **disabled**.
- The **Replicate Queue** and **Lease Queue** still run independently. They
  handle count-based convergence, constraint satisfaction, IO overload, lease
  preferences, etc. — concerns orthogonal to MMA's load-based rebalancing.
- The **Replica Scanner** still runs and enqueues to those queues.

This means observed replica/lease movement may come from MMA or from the queues.
Use MMA metrics (`mma.change.rebalance.*`) vs queue metrics to distinguish.

### Enabling MMA

MMA is controlled by the cluster setting `kv.allocator.load_based_rebalancing`:
- `off` — no load-based rebalancing
- `leases` — SMA: lease-only rebalancing
- `leases and replicas` — SMA: lease and replica rebalancing (default)
- `multi-metric only` — MMA enabled, replaces SMA store rebalancer
- `multi-metric and count` — MMA enabled, also does count-based convergence

See: `pkg/kv/kvserver/kvserverbase/base.go` for the setting definition.

## MMA Architecture

### Key Types and Data Flow

```
StoreLoadMsg (via gossip)         StoreLeaseholderMsg (from local store)
        |                                     |
        v                                     v
  ProcessStoreLoadMsg()              ComputeChanges()
        |                                     |
        v                                     v
  allocatorState.cs (clusterState)  ------>  rebalanceStores()
                                              |
                                              v
                                       []ExternalRangeChange
                                              |
                                              v
                                    mmaStoreRebalancer applies changes
                                              |
                                              v
                                    AdjustPendingChangeDisposition() (feedback)
```

- **`allocatorState`** — Top-level struct, holds a mutex and the `clusterState`.
  All `Allocator` interface methods acquire this mutex.
  See: `pkg/kv/kvserver/allocator/mmaprototype/allocator_state.go`

- **`clusterState`** — Holds per-store state (`storeState`), per-range state
  (`rangeState`), per-node state, and the `meansMemo` for caching load means.
  See: `pkg/kv/kvserver/allocator/mmaprototype/cluster_state.go`

- **`rebalanceEnv`** — Created fresh for each `rebalanceStores()` invocation.
  Holds the `clusterState`, limits, thresholds, and accumulated changes.
  See: `pkg/kv/kvserver/allocator/mmaprototype/cluster_state_rebalance_stores.go`

- **`mmaStoreRebalancer`** — Integration layer in `kvserver`. Runs a periodic
  loop, calls `ComputeChanges()`, and applies the returned changes.
  See: `pkg/kv/kvserver/mma_store_rebalancer.go`

- **`AllocatorSync`** — Coordinates between MMA and the replicate/lease queues
  to prevent conflicting concurrent decisions.
  See: `pkg/kv/kvserver/mmaintegration/allocator_sync.go`

### The Rebalancing Loop

`mmaStoreRebalancer.run()` ticks on a jittered interval controlled by
`kv.allocator.load_based_rebalance.interval`. On each tick:

1. Calls `rebalance()` with `periodicCall=true`.
2. If changes were made, immediately calls `rebalance()` again with
   `periodicCall=false` (tight loop to drain pending work).
3. Stops when no changes are computed.

Each `rebalance()` call invokes `ComputeChanges()` on the allocator, which
calls `rebalanceStores()`.

## Load Dimensions

MMA tracks multiple load dimensions per store. Each dimension has a load value
and optionally a capacity, allowing MMA to compute utilization.

See: `pkg/kv/kvserver/allocator/mmaprototype/load.go` for `LoadDimension`,
`LoadVector`, capacity handling, and related types.

### Utilization vs Equal-Load Balancing

- When **capacity is known** (e.g. CPU, disk), MMA balances on **utilization**
  (load/capacity). This handles heterogeneous clusters correctly — all stores
  should run at similar utilization regardless of size.
- When **capacity is unknown**, MMA balances towards **equal absolute load**
  across stores.
- Capacity-weighted mean: computed as `sum(load)/sum(capacity)`, not the
  average of individual utilizations.

### Secondary Dimensions (Not Yet Active)

`LeaseCount` and `ReplicaCount` are defined as `SecondaryLoadDimension` but are
not yet hooked up to the main rebalancing loop. The plan is for MMA to
eventually replace the replicate and lease queues' count-based convergence.

## Store Load Classification

Each store's load is classified per dimension and then aggregated into a
`storeLoadSummary`. This is the key data structure for understanding MMA's
decisions.

### Load Summary Levels

The `loadSummary` enum classifies each store's load on a spectrum from
underloaded to urgently overloaded. The classification considers both
distance from the cluster mean and absolute utilization (when capacity is
known). Some dimensions apply additional caps to avoid thrashing when
utilization is low.

See: `loadSummaryForDimension()` in `load.go` for the exact thresholds and
classification logic.

### storeLoadSummary

The `storeLoadSummary` struct (in `store_load_summary.go`) aggregates:
- `sls` — overall store load summary (worst across all dimensions)
- `nls` — node-level load summary (CPU only, since CPU is shared across stores)
- `worstDim` — which dimension drove the overall `sls`
- `dimSummary[dim]` — per-dimension breakdown
- `maxFractionPendingIncrease/Decrease` — pending change fraction

## The Rebalancing Algorithm

`rebalanceStores()` in `cluster_state_rebalance_stores.go` is the core
algorithm. Read the source and its comments for exact details. The high-level
phases are:

### Phase 1: Identify Overloaded Stores

Compute cluster means, classify each store via `storeLoadSummary`, and
identify shedding candidates — stores that are overloaded and not already
at their pending-change limits.

### Phase 2: Track Overload Duration

Each store tracks how long it has been continuously overloaded, with a grace
period before the overload timer resets. The duration determines how
aggressively MMA picks targets (see ignore levels below).

See: `allocator_state.go` for the overload tracking constants.

### Phase 3: Sort and Process Shedding Stores

Shedding stores are sorted by severity (local store first, then by overload
level). For each shedding store, MMA attempts to shed load.

### Phase 4: Lease Transfers (Local Store Only)

If the local store is CPU-overloaded, attempt lease transfers first. Leases
can only be transferred from the local store (it holds them). If any leases
were transferred, skip replica moves for this store (wait for the next tick
to see the effect).

### Phase 5: Replica Moves

For each range in the store's top-K list (sorted by load on the worst
dimension), evaluate candidate target stores using constraint analysis,
pre-means filtering, post-means filtering, and diversity/lease-preference
ranking.

MMA limits the number of replica moves and lease transfers per invocation.
See: `newRebalanceEnv()` in `cluster_state_rebalance_stores.go` for the
per-invocation limits and other constants.

### Ignore Levels (Desperation)

As overload duration increases, MMA becomes more willing to accept suboptimal
targets. The ignore level progresses through stages that increasingly relax
which candidate stores are acceptable.

See: `allocator_state.go` for the ignore level definitions and the duration
thresholds that trigger each level.

### Remote Store Lease Shedding Grace Period

For remote stores that are CPU-overloaded, MMA waits a grace period before
shedding replicas. This gives the remote store's own leaseholder a chance to
shed leases first (which is cheaper than moving replicas).

See: `remoteStoreLeaseSheddingGraceDuration` in `allocator_state.go`.

## Candidate Filtering

### Pre-Means Filtering

Excludes stores before computing the mean for the candidate set:
- Stores with non-OK disposition (draining, dead, suspect, decommissioning,
  high disk utilization)
- Rationale: these stores can't accept work and shouldn't skew the mean

### Post-Means Filtering

Excludes candidates after computing the mean:
- Stores already hosting a replica of the range
- The shedding store itself
- Stores whose load summary (relative to the candidate-set mean) is worse than
  the source store's, to prevent thrashing
- Stores with too much pending inflight work

### Diversity and Lease Preferences

After filtering, candidates are further ranked by:
- **Diversity score** — prefer targets that improve locality diversity
- **Lease preference** — when moving a leaseholder, prefer targets matching
  the range's lease preference configuration

See: `sortTargetCandidateSetAndPick()` in `cluster_state_rebalance_stores.go`.

## Disk Utilization

Disk utilization gets special treatment. Stores above configurable thresholds
are given dispositions that cause them to actively shed replicas or refuse new
ones. When a store's disk utilization exceeds the shedding threshold, ByteSize
becomes the priority dimension for the top-K range selection regardless of
other load dimensions.

See: `highDiskSpaceUtilization()` in `load.go` and `updateStoreStatuses()`
in `cluster_state.go` for threshold logic. The thresholds are configured via
the cluster settings `kv.allocator.max_disk_utilization_threshold` and
`kv.allocator.rebalance_to_max_disk_utilization_threshold`.

## Pending Changes and Rate Limiting

MMA tracks pending changes to avoid piling up inaccurate estimates. It
enforces per-invocation limits on replica moves and lease transfers, a
pending-fraction threshold beyond which no further shedding occurs, a delay
after failed changes before retrying the same range, and a grace period
after a store drops below overload before resetting overload tracking.

See: `newRebalanceEnv()` in `cluster_state_rebalance_stores.go` for the
constant definitions.

## Logging

MMA logs on the `KvDistribution` channel. Most detailed logs are at verbosity
level 2 (`VEventf(ctx, 2, ...)`); candidate-level detail at level 3.

### Key Log Patterns

See `cluster_state_rebalance_stores.go` and `mma_store_rebalancer.go` for the
exact format strings. The important patterns are:

- **Pass start**: `"rebalanceStores begins"`
- **Cluster means**: `"cluster means: (stores-load ...) ..."`
- **Store evaluation**: `"evaluating s%d: node load %s, store load %s, worst dim %s"`
- **Overload transitions**: `"overload-start"`, `"overload-end"`, `"overload-continued"`
- **Shedding store added**: `"store s%v was added to shedding store list"`
- **Skipped (pending)**: `"skipping overloaded store s%d ..."`
- **Top-K ranges**: `"top-K[%s] ranges for s%d ..."`
- **Candidate evaluation**: `"considering replica-transfer r%v from s%v ..."`
- **Failures**: `"result(failed): no candidates found ..."`, `"result(failed): no suitable target ..."`
- **Successes**: `"result(success): ..."`
- **Pass summary**: logged at `Infof` level with shed successes, failures by
  reason, and skipped stores.

The `mmaid` tag is incremented per `rebalanceStores()` call and added to the
context, so all log messages within a single pass share the same `mmaid` value.

## Key Source Files

| File | Description |
|------|-------------|
| `pkg/kv/kvserver/allocator/mmaprototype/doc.go` | Package documentation, links to origin PR |
| `pkg/kv/kvserver/allocator/mmaprototype/allocator.go` | `Allocator` interface definition |
| `pkg/kv/kvserver/allocator/mmaprototype/allocator_state.go` | Top-level state, mutex, `ComputeChanges`, ignore levels and grace period constants |
| `pkg/kv/kvserver/allocator/mmaprototype/cluster_state.go` | Per-store/range/node state, constraint matching, disposition management |
| `pkg/kv/kvserver/allocator/mmaprototype/cluster_state_rebalance_stores.go` | **Core rebalancing algorithm**: `rebalanceStores()`, candidate filtering, lease and replica shedding |
| `pkg/kv/kvserver/allocator/mmaprototype/load.go` | `LoadDimension`, `LoadVector`, `loadSummary` enum, `loadSummaryForDimension()`, means computation, disk utilization checks |
| `pkg/kv/kvserver/allocator/mmaprototype/store_load_summary.go` | `storeLoadSummary` struct |
| `pkg/kv/kvserver/allocator/mmaprototype/mma_metrics.go` | All MMA metric definitions |
| `pkg/kv/kvserver/allocator/mmaprototype/top_k_replicas.go` | Top-K range selection for shedding |
| `pkg/kv/kvserver/allocator/mmaprototype/constraint.go` | Constraint analysis and normalization |
| `pkg/kv/kvserver/allocator/mmaprototype/rebalance_advisor.go` | `MMARebalanceAdvisor` for conflict detection with SMA |
| `pkg/kv/kvserver/allocator/mmaprototype/store_status.go` | Store disposition and health status types |
| `pkg/kv/kvserver/mma_store_rebalancer.go` | Integration: periodic loop, applies changes |
| `pkg/kv/kvserver/mmaintegration/allocator_sync.go` | Coordination between MMA and replicate/lease queues |
| `pkg/kv/kvserver/mmaintegration/store_load_msg.go` | Building `StoreLoadMsg` from store state |
| `pkg/kv/kvserver/mmaintegration/store_status.go` | Building store status updates for MMA |
| `pkg/kv/kvserver/kvserverbase/base.go` | Cluster setting `kv.allocator.load_based_rebalancing` |

## Common Investigation Scenarios

### "Stores are imbalanced on CPU but MMA isn't doing anything"

Check in order:
1. Is MMA enabled? (`kv.allocator.load_based_rebalancing` must be `multi-metric*`)
2. Does MMA see the imbalance? Check `mma.store.cpu.utilization` metrics.
3. Is the overloaded store classified as `overloadSlow` or higher? Check logs
   for `"evaluating s%d"` to see the `storeLoadSummary`.
4. Is the store stuck at pending threshold? Check for `"skipping overloaded store"` logs.
5. Are there viable targets? Check for `"no candidates found"` or
   `"no suitable target"` in logs.
6. Is the store in grace period? Check for `"in lease shedding grace period"`.

### "MMA is rebalancing but the imbalance isn't improving"

Check:
1. Is MMA succeeding? Check `mma.change.rebalance.*.success` vs `*.failure`.
2. Is something else counteracting MMA? Check replicate queue and lease queue
   activity.
3. Is a single hot range causing the imbalance? If so, MMA won't move it if
   it would just shift the problem (candidate filtering prevents this).
4. Are pending change limits preventing more aggressive action? Check the
   `mma.overloaded_store.*` duration buckets — if stores stay in
   `lease_grace` or `short_dur`, MMA is being conservative.

### "Unexpected replica/lease movement"

1. Distinguish MMA moves from queue moves using metrics.
2. Check MMA logs for the specific range movement.
3. Check if the store was classified as overloaded on a dimension you didn't
   expect (e.g., WriteBandwidth might trigger shedding even if CPU looks fine).
