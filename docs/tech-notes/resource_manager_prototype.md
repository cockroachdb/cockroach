# Resource Manager Prototype: Implementation Design

Author: Wenyi Hu
Date: 2026-03-14

## Overview

This document describes the implementation of the Resource Manager (RM)
prototype on the `rm` branch. The RM extends CockroachDB's CPU Time Token
(CTT) admission control to support N configurable resource groups, each
with a minimum CPU guarantee and optional full-utilization access.

The prototype builds on the existing CTT infrastructure in
`pkg/util/admission/`. It collapses the 2-queue, 4-bucket Serverless
design into a 1-queue, 2-bucket design where resource groups appear as
tenants with configurable weights and burst limits.

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

### Architecture Change: 2 Queues → 1 Queue

**Before (Serverless)**:

```
cpuTimeTokenGrantCoordinator
├── WorkQueue[systemTenant]  (tier 0, targets: 95%/100%)
│    └── childGranter(tier=0) → cpuTimeTokenGranter
├── WorkQueue[appTenant]     (tier 1, targets: 80%/85%)
│    └── childGranter(tier=1) → cpuTimeTokenGranter
└── cpuTimeTokenGranter
     └── buckets[2 tiers][2 burst quals] = 4 buckets
```

**After (RM)**:

```
cpuTimeTokenGrantCoordinator
└── WorkQueue (single, all tenants)
     └── cpuTimeTokenGranter (implements granter directly)
          └── buckets[2 burst quals] = 2 buckets
               canBurst: 100% CPU
               noBurst:   75% CPU
```

Key simplifications:
- `resourceTier` type removed entirely
- `cpuTimeTokenChildGranter` removed — the granter implements `granter`
  directly with a single `requester`
- All type aliases (`rates`, `capacities`, `minimums`, `tokenCounts`,
  `targetUtilizations`) collapsed from `[numResourceTiers][numBurstQualifications]`
  to `[numBurstQualifications]`

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

Each resource group maps to a tenant in the WorkQueue with two properties:

- **Weight** (`uint32`): Equal to CPU_MIN. Controls proportional fair
  sharing via `used/weight` in the tenant heap.
- **BurstLimitFrac** (`float64`): Controls burst qualification.
  - `>= 1.0`: FULLY_UTILIZE — always qualifies for canBurst
  - `< 1.0`: Qualifies for canBurst only when burst bucket tokens
    exceed `burstLimitFrac × capacity`

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

This calls `SetTenantWeights` and `SetBurstLimits` internally.

For the prototype, two resource groups are hardcoded at init:
- System tenant (ID=1): weight=9, burstLimitFrac=1.0 (FULLY_UTILIZE)
- All others: weight=1 (default), burstLimitFrac=0.25 (default)

### How Fair Sharing Works

The `tenantHeap` orders tenants by:
1. **Burst qualification** (canBurst before noBurst)
2. **`used/weight`** (lowest ratio first)

When `granted()` is called, the tenant at the top of the heap (lowest
burst qual, then lowest used/weight) gets the next grant. Over time,
all tenants converge toward CPU consumption proportional to their
weights.

Example with 3 resource groups:

```
CREATE RESOURCE GROUP online_rg Weight_CPU=160 FULLY_UTILIZE
CREATE RESOURCE GROUP batch_rg Weight_CPU=20
CREATE RESOURCE GROUP support_rg Weight_CPU=20

Total weight = 200
online_rg share = 160/200 = 80%
batch_rg share  = 20/200  = 10%
support_rg share = 20/200 = 10%
```

### How Burst Qualification Works

Each tenant has a `cpuTimeBurstBucket` — a small per-tenant token bucket
that tracks whether the tenant is a light or heavy CPU user.

**Refill**: Every 1ms, each tenant's burst bucket is refilled at a rate
proportional to its `burstLimitFrac`:

```
scaledRefill = canBurstAllocation × burstLimitFrac
```

For a tenant with burstLimitFrac=0.1 (CPU_MIN=10%) on an 8-vCPU machine:
```
canBurstRate = 1.0 × 8e9 = 8e9 tokens/sec
scaledRefill = 8e9 / 1000 × 0.1 = 800K tokens per tick
```

**Drain**: When the tenant's work is admitted, tokens are deducted from
its burst bucket proportional to the CPU estimate.

**Break-even**: The tenant's burst bucket drains when its CPU consumption
exceeds its refill rate:

```
drain_per_tick = CPU_usage% × cpuCapacity / 1000
refill_per_tick = canBurstRate / 1000 × burstLimitFrac

Break-even when drain = refill:
  CPU_usage% × cpuCapacity = canBurstRate × burstLimitFrac
  CPU_usage% = burstLimitFrac × (canBurstRate / cpuCapacity)
  CPU_usage% = burstLimitFrac × 1.0  (since canBurstRate = cpuCapacity)
  CPU_usage% = burstLimitFrac = CPU_MIN%
```

So a tenant with CPU_MIN=10% loses burst qualification at exactly 10%
CPU usage. This is the calibration that makes the design doc's scenarios
work correctly.

**Qualification check**:

```go
func (m *cpuTimeBurstBucket) burstQualification() burstQualification {
    if m.burstLimitFrac >= 1.0 {
        return canBurst  // FULLY_UTILIZE
    }
    if m.capacity > 0 && m.tokens > int64(m.burstLimitFrac*float64(m.capacity)) {
        return canBurst  // using less than CPU_MIN
    }
    return noBurst       // using more than CPU_MIN
}
```

### Design Doc Scenarios

**Scenario 1**: OPG=45%, BPG=15%, SCG=15%. Sum=75%.

All groups using less than 75% total. noBurst bucket has capacity.
BPG and SCG exceed their 10% CPU_MIN → noBurst, but noBurst bucket
is not exhausted, so they're admitted. Node=75%.

**Scenario 2**: OPG=65%, BPG=15%, SCG=15%. Sum=95%.

BPG/SCG exceed 10% CPU_MIN → lose canBurst → noBurst → throttled.
Usage drops below 10% → regain canBurst → admitted from 100% bucket.
They oscillate around 10% CPU_MIN. OPG uses 65% from 100% bucket.
Node=65%+10%+10%=85%.

**Scenario 3**: OPG=5%, BPG=50%, SCG=70%. Sum=125%.

OPG uses 5%, always canBurst. BPG/SCG are noBurst (exceed 10%).
noBurst bucket = 75%. OPG drains 5% from it, leaving 70%.
BPG/SCG fair-share 70% by weight (20:20) = 35% each. Node=75%.

### Granter Bucket Mechanics

Two global token buckets, shared by all tenants:

```
canBurst: refilled at 100% × cpuCapacity / multiplier
noBurst:  refilled at 75%  × cpuCapacity / multiplier
```

**Admission**: A request checks `buckets[qual].tokens > 0` for the
tenant's burst qualification. If positive, admitted. On admission,
tokens are deducted from **both** buckets.

**Deduct-all design**: This is intentional — both buckets represent
total node CPU. When any tenant uses CPU, it reduces the budget for
everyone. The per-tenant differentiation comes from which bucket
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
    → cpuTimeTokenAllocator.allocateTokens()
       → cpuTimeTokenGranter.refill()        (2 global buckets)
       → WorkQueue.refillBurstBuckets()       (N per-tenant burst buckets)

Every 1s:
  cpuTimeTokenAllocator.resetInterval()
    → cpuTimeTokenLinearModel.fit()           (recompute multiplier)
    → apply delta to global buckets
    → apply delta to per-tenant burst buckets
```

The linear model, token-to-CPU multiplier, asymmetric smoothing, and
low-CPU recovery logic are all unchanged.

### Known Limitations

**Deduct-all approximation**: When a FULLY_UTILIZE group consumes a
large fraction of CPU (e.g., 65%), it drains the noBurst bucket,
reducing the budget available to non-FULLY_UTILIZE groups. The design
doc's scenarios describe the desired steady-state behavior, which is
achieved through oscillation: non-FULLY_UTILIZE groups lose canBurst
when above CPU_MIN, get throttled, drop below CPU_MIN, regain canBurst,
and settle at their CPU_MIN. The approximation is acceptable because:
1. The oscillation converges within a few cycles (< 1 second)
2. The weights ensure proportional sharing within each bucket class
3. The burst bucket's per-tenant refill rate is calibrated to CPU_MIN

**Elastic work not integrated**: The design doc mentions elastic work
sharing the same queue with 5% weight and 5% burst qualification limit.
This is not implemented in the prototype.

**SQL DDL not wired**: `CREATE RESOURCE GROUP` DDL is not implemented.
The prototype hardcodes two resource groups. Production wiring will
translate SQL DDL into `SetResourceGroupConfig` calls.

## Files Changed

| File | Change |
|------|--------|
| `cpu_time_token_granter.go` | Removed `resourceTier`, `cpuTimeTokenChildGranter`; collapsed to 2 buckets; `cpuTimeTokenGranter` implements `granter` directly |
| `cpu_time_token_filler.go` | All type aliases collapsed to 1D; single queue; single utilization target; removed per-tier settings |
| `cpu_time_token_grant_coordinator.go` | Single WorkQueue; removed childGranters; added `ResourceGroupConfig`, `SetResourceGroupConfig`; hardcoded 2 groups at init |
| `cpu_time_token_metrics.go` | Collapsed from `numResourceTiers × numBurstQualifications` to `numBurstQualifications` counters |
| `cpu_time_token_burst.go` | Added `burstLimitFrac`; `burstQualification()` uses it as threshold |
| `work_queue.go` | Added `burstLimits` map, `SetBurstLimits`, `getBurstLimitFracLocked`, `defaultBurstLimitFrac`; per-tenant burst bucket refill scaling; scaled capacity at tenant creation |

## Future Work

1. **SQL DDL wiring**: `CREATE RESOURCE GROUP` → `SetResourceGroupConfig`
2. **Elastic work integration**: Add elastic work to the same queue with
   5% weight and 5% burst qualification limit
3. **Per-group utilization targets**: The design doc's section 4.1
   describes resource groups with levels that define a priority ordering.
   The current prototype uses burst qualification for priority, not
   per-group utilization targets.
4. **Testing**: Comprehensive tests verifying the design doc's scenarios
   with multiple resource groups under various load patterns
