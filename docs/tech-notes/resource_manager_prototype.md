# Resource Manager Prototype: Implementation Design

Author: Wenyi Hu
Date: 2026-03-15

## Overview

This document describes the implementation of the Resource Manager (RM)
prototype on the `rm` branch. The RM extends CockroachDB's CPU Time Token
(CTT) admission control to support N configurable resource groups, each
with a minimum CPU guarantee and optional full-utilization access.

The prototype builds on the existing CTT infrastructure in
`pkg/util/admission/`. It collapses the 2-queue, 4-bucket Serverless
design into a 1-queue, 2-bucket design where resource groups appear as
tenants with configurable weights and burst limits. It then adds a
two-level scheduling hierarchy (resource group -> tenant) for correct
inter-group and intra-group fairness.

## Motivation

The existing CTT system was built for Serverless, where there are exactly
two classes of work: system tenant and app tenant. Priority is hardcoded
via separate WorkQueues and separate utilization targets (95% for system,
80% for app). This design cannot express N resource groups with
arbitrary CPU shares.

The Resource Manager design (from the internal design doc "CPU Time Token
based Admission Control") calls for:

- N resource groups defined via SQL DDL (`CREATE RESOURCE GROUP`)
- Each group has a minimum CPU share (`CPU_MIN`) and optional
  `FULLY_UTILIZE` flag
- All groups share a single WorkQueue with weighted fair sharing
- Two global token buckets: 75% (noBurst) and 100% (canBurst)
- Groups that stay below their CPU_MIN qualify for burst (access to the
  100% bucket); groups that exceed it are limited to the 75% bucket
- `FULLY_UTILIZE` groups always qualify for burst

## Design

### Architecture Change: 2 Queues -> 1 Queue

**Before (Serverless)**:

```
cpuTimeTokenGrantCoordinator
+-- WorkQueue[systemTenant]  (tier 0, targets: 95%/100%)
|    +-- childGranter(tier=0) -> cpuTimeTokenGranter
+-- WorkQueue[appTenant]     (tier 1, targets: 80%/85%)
|    +-- childGranter(tier=1) -> cpuTimeTokenGranter
+-- cpuTimeTokenGranter
     +-- buckets[2 tiers][2 burst quals] = 4 buckets
```

**After (RM)**:

```
cpuTimeTokenGrantCoordinator
+-- WorkQueue (single, all tenants)
     +-- cpuTimeTokenGranter (implements granter directly)
          +-- buckets[2 burst quals] = 2 buckets
               canBurst: 100% CPU
               noBurst:   75% CPU
```

Key simplifications:
- `resourceTier` type removed entirely
- `cpuTimeTokenChildGranter` removed -- the granter implements `granter`
  directly with a single `requester`
- All type aliases (`rates`, `capacities`, `minimums`, `tokenCounts`,
  `targetUtilizations`) collapsed from `[numResourceTiers][numBurstQualifications]`
  to `[numBurstQualifications]`

### Two-Level Scheduling Hierarchy

The WorkQueue uses a two-level heap for scheduling:

```
resourceGroupHeap (outer)         -- inter-group fairness
+-- resourceGroupInfo (rg1)
|    +-- tenantHeap (inner)       -- intra-group fairness
|    |    +-- tenantInfo (t1)
|    |    +-- tenantInfo (t2)
|    +-- cpuTimeBurstBucket       -- shared burst bucket
|    +-- weight (CPU_MIN)
|    +-- used (aggregate)
+-- resourceGroupInfo (rg2)
     +-- tenantHeap (inner)
     |    +-- tenantInfo (t3)
     +-- cpuTimeBurstBucket
     +-- weight (CPU_MIN)
     +-- used (aggregate)
```

**Outer `resourceGroupHeap`**: Orders resource groups by:
1. Burst qualification (canBurst before noBurst) -- from the group's
   shared `cpuTimeBurstBucket`
2. `group.used / group.weight` (lowest ratio first) -- inter-group
   weighted fair sharing, where weight = CPU_MIN

**Inner `tenantHeap`** (within each resource group): Orders tenants by:
1. `tenant.used / tenant.weight` (lowest ratio first) -- intra-group
   weighted fair sharing, where weight = replica count

Burst qualification is NOT considered at the inner level. It is strictly
a per-group property determined by the group's shared burst bucket.

**Default behavior**: Each tenant is its own resource group (tenantID ==
resourceGroupID). In this degenerate case, the inner heap has exactly one
tenant, so the two-level hierarchy behaves identically to a flat heap.

**Why two levels matter**: Without per-group burst buckets, multiple
tenants in the same resource group would each get independent burst
qualification. A group with 3 tenants each using 5% CPU would have all 3
qualify as canBurst individually, even though the group collectively uses
15% -- exceeding its CPU_MIN of 10%. The granter cannot fix this because
it only caps total CPU utilization, not per-group fairness. The burst
bucket is the only mechanism for inter-group priority enforcement.

#### Granting Flow

```
granted():
  1. Pop top resource group from outer resourceGroupHeap
  2. Pop top tenant from that group's inner tenantHeap
  3. Pop highest-priority work item from tenant's waitingWorkHeap
  4. adjustTenantAndGroupUsedLocked(tenant, rg, count):
     a. tenant.used += count    -> fix inner tenantHeap
     b. rg.used += count        -> fix outer resourceGroupHeap
     c. rg.cpuTimeBurstBucket.adjust(-count)
  5. Remove tenant/group from heaps if no more waiting work
```

#### Admit Fast Path

The fast path allows immediate admission without queuing when:
- The resourceGroupHeap is empty (no backlog), OR
- The requesting group is not in the heap AND it qualifies as canBurst
  while the top-of-heap group is noBurst

This ensures canBurst work is never blocked behind noBurst work.

#### Weight Separation

The two levels use different weight sources:

| Level | Weight represents | Set by | Source |
|-------|------------------|--------|--------|
| Outer (resource group) | CPU_MIN | `SetResourceGroupWeights` | `SetResourceGroupConfig` (SQL DDL) |
| Inner (tenant) | Replica count | `SetTenantWeights` | `GetTenantWeights` periodic ticker |

`SetResourceGroupConfig` does NOT call `SetTenantWeights`. This is
intentional: the inter-group weight (CPU_MIN) and intra-group weight
(replica count) serve different purposes and should not be conflated.

### Cluster Settings

Two per-tier settings replaced with one:

| Before | After |
|--------|-------|
| `admission.cpu_time_tokens.target_util.app_tenant` = 0.80 | `admission.cpu_time_tokens.target_util` = 0.75 |
| `admission.cpu_time_tokens.target_util.system_tenant` = 0.95 | (removed) |
| `admission.cpu_time_tokens.target_util.burst_delta` = 0.05 | `admission.cpu_time_tokens.target_util.burst_delta` = 0.25 |

The 75% noBurst target and 25% burst delta yield:
- noBurst bucket: 75% of CPU capacity
- canBurst bucket: 100% of CPU capacity

The 75% value is chosen to keep goroutine scheduling latency low (the
design doc notes this is configurable and conservative).

### Resource Group Configuration

Each resource group is configured with two properties:

- **Weight** (`uint32`): Equal to CPU_MIN. Controls proportional fair
  sharing via `used/weight` in the outer resourceGroupHeap.
- **BurstLimitFrac** (`float64`): Controls burst qualification.
  - `>= 1.0`: FULLY_UTILIZE -- always qualifies for canBurst
  - `< 1.0`: Qualifies for canBurst only when burst bucket tokens
    exceed `burstLimitFrac x capacity`

Configuration API:

```go
type ResourceGroupConfig struct {
    Weight         uint32
    BurstLimitFrac float64
}

func (coord *CPUGrantCoordinators) SetResourceGroupConfig(
    config map[uint64]ResourceGroupConfig,
)
```

This calls `SetResourceGroupWeights` and `SetBurstLimits` internally.
It does NOT call `SetTenantWeights` -- tenant weights (replica counts)
are maintained separately by the existing periodic ticker.

For the prototype, two resource groups are hardcoded at init:
- System tenant (ID=1): weight=9, burstLimitFrac=1.0 (FULLY_UTILIZE)
- All others: weight=1 (default), burstLimitFrac=1.0 (default,
  FULLY_UTILIZE -- unconfigured groups are not restricted)

### How Fair Sharing Works

The `resourceGroupHeap` orders resource groups by:
1. **Burst qualification** (canBurst before noBurst)
2. **`rg.used / rg.weight`** (lowest ratio first)

Within each group, the `tenantHeap` orders tenants by:
1. **`tenant.used / tenant.weight`** (lowest ratio first)

When `granted()` is called, the top resource group and top tenant within
that group get the next grant. Over time, resource groups converge toward
CPU consumption proportional to their weights (CPU_MIN values), and
tenants within a group converge proportional to their replica counts.

Example with 3 resource groups:

```
CREATE RESOURCE GROUP online_rg CPU_MIN=160 FULLY_UTILIZE
CREATE RESOURCE GROUP batch_rg CPU_MIN=20
CREATE RESOURCE GROUP support_rg CPU_MIN=20

Total weight = 200
online_rg share = 160/200 = 80%
batch_rg share  = 20/200  = 10%
support_rg share = 20/200 = 10%
```

### How Burst Qualification Works

Each resource group has a `cpuTimeBurstBucket` -- a token bucket that
tracks whether the group is a light or heavy CPU user relative to its
CPU_MIN.

**Refill**: Every 1ms, each group's burst bucket is refilled at a rate
proportional to its `burstLimitFrac`:

```
scaledRefill = canBurstAllocation x burstLimitFrac
```

For a group with burstLimitFrac=0.1 (CPU_MIN=10%) on an 8-vCPU machine:
```
canBurstRate = 1.0 x 8e9 = 8e9 tokens/sec
scaledRefill = 8e9 / 1000 x 0.1 = 800K tokens per tick
```

**Drain**: When any tenant in the group has work admitted, tokens are
deducted from the group's burst bucket proportional to the CPU estimate.

**Break-even**: The group's burst bucket drains when the group's
aggregate CPU consumption exceeds its refill rate:

```
drain_per_tick = group_CPU_usage% x cpuCapacity / 1000
refill_per_tick = canBurstRate / 1000 x burstLimitFrac

Break-even when drain = refill:
  group_CPU_usage% x cpuCapacity = canBurstRate x burstLimitFrac
  group_CPU_usage% = burstLimitFrac x (canBurstRate / cpuCapacity)
  group_CPU_usage% = burstLimitFrac x 1.0
  group_CPU_usage% = burstLimitFrac = CPU_MIN%
```

So a group with CPU_MIN=10% loses burst qualification when its aggregate
CPU usage across all tenants exceeds 10%.

**Why per-group burst matters**: On master (without resource groups), the
burst bucket is somewhat loose because the granter enforces the hard CPU
cap. The burst bucket only affects queue priority ordering. Even if a
tenant incorrectly gets canBurst, total CPU is still capped.

With resource groups, the burst qualification becomes the primary
mechanism for inter-group fairness. The granter has no concept of groups
-- it only has two global token buckets shared by all groups. If a group
incorrectly qualifies as canBurst, its work jumps ahead of other groups
in the queue, and the granter cannot correct the imbalance. The burst
bucket is the only place where per-group CPU_MIN enforcement happens.

**Qualification check**:

```go
func (m *cpuTimeBurstBucket) burstQualification() burstQualification {
    if m.burstLimitFrac >= 1.0 {
        return canBurst  // FULLY_UTILIZE
    }
    if m.tokens > (m.capacity*9)/10 {
        return canBurst  // bucket >90% full = using less than CPU_MIN
    }
    return noBurst       // bucket depleted = using more than CPU_MIN
}
```

Note: the threshold is `>90% of capacity` (same as the original master
code), NOT `>burstLimitFrac x capacity`. The per-group break-even point
is controlled by the **scaled capacity and refill rate**, not by the
threshold check. Using `burstLimitFrac x capacity` would double-apply
the scaling (since capacity is already scaled by `burstLimitFrac` in
`refillBurstBuckets`), making it too easy for small groups to qualify.

### Design Doc Scenarios

**Scenario 1**: OPG=45%, BPG=15%, SCG=15%. Sum=75%.

All groups using less than 75% total. noBurst bucket has capacity.
BPG and SCG exceed their 10% CPU_MIN -> noBurst, but noBurst bucket
is not exhausted, so they're admitted. Node=75%.

**Scenario 2**: OPG=65%, BPG=15%, SCG=15%. Sum=95%.

BPG/SCG exceed 10% CPU_MIN -> lose canBurst -> noBurst -> throttled.
Usage drops below 10% -> regain canBurst -> admitted from 100% bucket.
They oscillate around 10% CPU_MIN. OPG uses 65% from 100% bucket.
Node=65%+10%+10%=85%.

**Scenario 3**: OPG=5%, BPG=50%, SCG=70%. Sum=125%.

OPG uses 5%, always canBurst. BPG/SCG are noBurst (exceed 10%).
noBurst bucket = 75%. OPG drains 5% from it, leaving 70%.
BPG/SCG fair-share 70% by weight (20:20) = 35% each. Node=75%.

### Granter Bucket Mechanics

Two global token buckets, shared by all tenants:

```
canBurst: refilled at 100% x cpuCapacity / multiplier
noBurst:  refilled at 75%  x cpuCapacity / multiplier
```

**Admission**: A request checks `buckets[qual].tokens > 0` for the
group's burst qualification. If positive, admitted. On admission,
tokens are deducted from **both** buckets.

**Deduct-all design**: This is intentional -- both buckets represent
total node CPU. When any tenant uses CPU, it reduces the budget for
everyone. The per-group differentiation comes from which bucket
is checked (canBurst vs noBurst), not from separate budgets.

**Minimums**: Each bucket has a floor to prevent unbounded token debt:

```
canBurst minimum: 0
noBurst minimum:  noBurstRate - canBurstRate (always negative)
```

This ensures canBurst recovers to positive before noBurst during
overload recovery, preserving the priority hierarchy.

### Refill Pipeline

Unchanged from the existing CTT design:

```
Every 1ms:
  cpuTimeTokenFiller
    -> cpuTimeTokenAllocator.allocateTokens()
       -> cpuTimeTokenGranter.refill()        (2 global buckets)
       -> WorkQueue.refillBurstBuckets()       (N per-group burst buckets)

Every 1s:
  cpuTimeTokenAllocator.resetInterval()
    -> cpuTimeTokenLinearModel.fit()           (recompute multiplier)
    -> apply delta to global buckets
    -> apply delta to per-group burst buckets
```

The linear model, token-to-CPU multiplier, asymmetric smoothing, and
low-CPU recovery logic are all unchanged.

### Data Structures

#### resourceGroupInfo

```go
type resourceGroupInfo struct {
    id                 uint64
    weight             uint32              // CPU_MIN, for outer heap
    used               uint64              // aggregate across all tenants
    cpuTimeBurstBucket cpuTimeBurstBucket  // shared burst bucket
    tenantHeap         tenantHeap          // inner heap (intra-group)
    tenants            map[uint64]*tenantInfo
    heapIndex          int                 // position in outer heap
}
```

#### WorkQueue.mu additions

```go
mu struct {
    resourceGroupHeap      resourceGroupHeap       // outer heap
    resourceGroups         map[uint64]*resourceGroupInfo
    tenantToResourceGroup  map[uint64]uint64        // nil = default mapping
}
```

#### tenantInfo changes

- `cpuTimeBurstBucket` removed (moved to `resourceGroupInfo`)
- `resourceGroupID uint64` added
- `tenantHeap.Less()` simplified to `used/weight` only (no burst check)

### Usage Tracking

When work is admitted, `adjustTenantAndGroupUsedLocked` updates both:

1. `tenant.used += delta` -- for intra-group fairness in the inner heap
2. `rg.used += delta` -- for inter-group fairness in the outer heap
3. `rg.cpuTimeBurstBucket.adjust(-delta)` -- tracks group CPU budget

Both are reset every ~1s by `gcTenantsResetUsedAndUpdateEstimators`.

When `AdmittedWorkDone` is called (with measured CPU time), the delta
between predicted and actual CPU is applied to both levels, correcting
the earlier estimate.

### Known Limitations / Open Design Questions

**Deduct-all + fast path fairness gap**: When a FULLY_UTILIZE group
(always canBurst) uses the fast path, its admissions drain the noBurst
bucket via deduct-all without any weight constraint. For example, if
OPG uses 65% CPU via the fast path, it drains the noBurst bucket by
65%, leaving only 10% (75% - 65%) for BPG + SCG combined -- they each
get ~5%, not their CPU_MIN of 10%. The design doc's example 2 expects
BPG and SCG to each get 10%, but the deduct-all mechanism produces
~5% each. The `resourceGroupHeap` weight-based fair sharing only applies
on the slow path (queued requests); the fast path bypasses it entirely.

Potential fixes to discuss with Sumeer:
1. Weight-constrain the fast path -- don't let a group consume more
   than its weight-proportional share of noBurst via fast path
2. Don't deduct canBurst admissions from noBurst -- but this breaks
   the "buckets represent the same physical CPU" invariant
3. Per-group admission budgets -- fundamentally different mechanism

**No scheduling latency feedback in CTT**: The pure token-based system
has no direct signal for goroutine scheduling pressure. Slots had
`isOverloaded` (usedSlots >= totalSlots), elastic CPU has
`schedulerLatencyListener`. CTT relies on the utilization target being
conservative (75%) and the multiplier indirectly catching increased
runtime overhead. The design doc suggests using runnable goroutine
count to throttle token distribution but this is not implemented.

**Elastic work not integrated**: The design doc mentions elastic work
sharing the same queue with 5% weight and 5% burst qualification limit.
This is not implemented in the prototype.

**SQL DDL not wired**: `CREATE RESOURCE GROUP` DDL is not implemented.
The prototype hardcodes two resource groups. Production wiring will
translate SQL DDL into `SetResourceGroupConfig` calls.

**Tenant-to-resource-group mapping**: `SetTenantToResourceGroupMapping`
exists but is not wired to any production code path. Currently each
tenant defaults to its own resource group. Production will need a
mechanism (likely SQL metadata) to assign tenants to resource groups.

## Files Changed

| File | Change |
|------|--------|
| `cpu_time_token_granter.go` | Removed `resourceTier`, `cpuTimeTokenChildGranter`; collapsed to 2 buckets; `cpuTimeTokenGranter` implements `granter` directly |
| `cpu_time_token_filler.go` | All type aliases collapsed to 1D; single queue; single utilization target; removed per-tier settings |
| `cpu_time_token_grant_coordinator.go` | Single WorkQueue; removed childGranters; added `ResourceGroupConfig`, `SetResourceGroupConfig` (sets resource group weights + burst limits, does NOT set tenant weights); hardcoded 2 groups at init |
| `cpu_time_token_metrics.go` | Collapsed from `numResourceTiers x numBurstQualifications` to `numBurstQualifications` counters |
| `cpu_time_token_burst.go` | Added `burstLimitFrac`; `burstQualification()` uses `>= 1.0` for FULLY_UTILIZE, `>90% of capacity` for others; per-group break-even achieved via scaled capacity/refill, not threshold |
| `work_queue.go` | Added `resourceGroupInfo`, `resourceGroupHeap` (two-level hierarchy); burst bucket moved from `tenantInfo` to `resourceGroupInfo`; `tenantHeap.Less` simplified to `used/weight` only; added `SetResourceGroupWeights`, `SetTenantToResourceGroupMapping`, `SetBurstLimits`; `Admit`, `granted`, `hasWaitingRequests` updated for two-level scheduling; `refillBurstBuckets` iterates resource groups; `gcTenantsResetUsedAndUpdateEstimators` resets both levels |

## Future Work

1. **SQL DDL wiring**: `CREATE RESOURCE GROUP` -> `SetResourceGroupConfig`
2. **Elastic work integration**: Add elastic work to the same queue with
   5% weight and 5% burst qualification limit
3. **Tenant-to-group mapping**: Wire `SetTenantToResourceGroupMapping`
   to a production code path so multiple tenants can share a resource
   group
4. **Testing**: Comprehensive tests verifying the design doc's scenarios
   with multiple resource groups under various load patterns, including
   multi-tenant resource groups
5. **Per-group utilization targets**: The design doc's section 4.1
   describes resource groups with levels that define a priority ordering.
   The current prototype uses burst qualification for priority, not
   per-group utilization targets.
