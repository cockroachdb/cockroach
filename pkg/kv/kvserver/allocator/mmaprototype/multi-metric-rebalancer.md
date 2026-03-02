# Absorbing Replicate Queue & Lease Queue into MMA

## Context

MMA (multi-metric allocator, `pkg/kv/kvserver/allocator/mmaprototype/`) already handles
load balancing when the `kv.allocator.load_based_rebalancing` cluster setting is set to
one of the `multi-metric` modes. In that mode it replaces the store rebalancer
(`pkg/kv/kvserver/store_rebalancer.go`). However, the replicate queue and lease queue
still run independently and handle repair actions, constraint enforcement, lease
preferences, and other non-load-balancing duties. The goal is to absorb all of these
responsibilities into MMA.

---

## Functional Comparison Table

### Legend

- **Repair**: Fixing under/over-replication, dead/decommissioning replicas
- **Rebalance**: Optimizing placement for load, diversity, or count balance
- **Lease Mgmt**: Lease transfers for preferences, load, or correctness

| Function | Replicate Queue | Lease Queue | Store Rebalancer | MMA (26.2) |
|---|---|---|---|---|
| **REPAIR ACTIONS** | | | | |
| Add voter (under-replicated) | Yes (pri 10000) | — | — | No |
| Add non-voter (under-replicated) | Yes (pri 600) | — | — | No |
| Replace dead voter | Yes (pri 12000) | — | — | No |
| Replace dead non-voter | Yes (pri 700) | — | — | No |
| Remove dead voter (over-repl) | Yes (pri 1000) | — | — | No |
| Remove dead non-voter (over-repl) | Yes (pri 400) | — | — | No |
| Replace decommissioning voter | Yes (pri 5000) | — | — | No |
| Replace decommissioning non-voter | Yes (pri 500) | — | — | No |
| Remove decommissioning voter | Yes (pri 900) | — | — | No |
| Remove decommissioning non-voter | Yes (pri 300) | — | — | No |
| Remove over-replicated voter | Yes (pri 800) | — | — | No |
| Remove over-replicated non-voter | Yes (pri 200) | — | — | No |
| Finalize atomic replication change | Yes (pri 12002) | — | — | No |
| Remove stuck learner | Yes (pri 12001) | — | — | No |
| Range unavailable (no quorum) | Detected, no action | — | — | No |
| **CONSTRAINT ENFORCEMENT** | | | | |
| Voter constraint satisfaction | Via rebalance swaps | — | — | Yes (constraint matching) |
| Non-voter constraint satisfaction | Via rebalance swaps | — | — | Yes (constraint matching) |
| Lease preference enforcement | — | Yes (pri 300) | — | Partial (lease preferences in rebalance) |
| **LOAD REBALANCING** | | | | |
| Replica rebalance (count-based) | Yes (pri 0, `ConsiderRebalance`) | — | — | No (separate concern) |
| Replica rebalance (load-based) | — | — | Yes (phase 2) | Yes (replica moves, max 1/pass) |
| Lease transfer (load-based) | — | — | Yes (phase 1) | Yes (lease transfers, max 8/pass) |
| Lease transfer (count-based) | — | Yes (pri 0) | — | No |
| Lease transfer (access locality) | — | Yes (pri 100) | — | No (being deprecated, #153866) |
| **LEASE CORRECTNESS** | | | | |
| Lease type correction | — | Yes (enqueue) | — | No |
| Invalid lease recovery | — | Yes (enqueue) | — | No |
| IO overload lease shedding | — | Yes (pri 200) | — | Yes (via store disposition: Shedding) |
| **RATE LIMITING / ANTI-THRASH** | | | | |
| Min lease transfer interval | — | Yes (1s default) | — | No (pending-change fraction limit) |
| Min IO overload shed interval | — | Yes (30s default) | — | No |
| Priority inversion detection | Yes (requeue) | — | — | No |
| Snapshot retry backoff | Yes (50ms-1s, 5 retries) | — | — | No |
| **QUEUE INFRASTRUCTURE** | | | | |
| Purgatory (retry on transient err) | Yes (1min interval) | Yes (10s interval) | — | No |
| Gossip/liveness update channel | Yes (retrigger purgatory) | No | — | No |
| Span config update enqueue | Yes (setting-gated) | — | — | No |
| Problem range re-enqueue | Yes (setting-gated, disabled) | — | — | No |
| Queue max size control | Yes (setting) | No | — | N/A |
| **STORE POOL / STATE UPDATES** | | | | |
| Pre-register change with MMA | Via AllocatorSync | Via AllocatorSync | Via AllocatorSync | N/A (is the receiver) |
| Post-apply store pool update | Via AllocatorSync | Via AllocatorSync | Via AllocatorSync | Tracks pending changes internally |
| Conflict checking with MMA | Via MMARebalanceAdvisor | — | — | Yes (advisor veto) |

---

## Component Summaries

### Replicate Queue (`replicate_queue.go`)

**Primary role**: Ensure every range has the correct number of replicas on appropriate
stores. Handles all repair actions (under/over-replication, dead/decommissioning nodes,
learner cleanup, atomic replication finalization) and count-based rebalancing.

**Key characteristics**:
- Processes one replica at a time, sequentially
- Uses `plan.ReplicaPlanner` to determine action and operation
- Priority-ordered: repair actions (pri 200-12002) >> rebalance (pri 0)
- Replace-before-remove pattern for dead/decommissioning replicas
- Retries snapshot failures with backoff (50ms-1s, 5 retries)
- Purgatory for transient errors (decommission, quorum issues)
- Priority inversion detection: requeues if enqueue priority was repair but processing
  priority drops to rebalance

**Interactions with MMA**: All changes go through `AllocatorSync.NonMMAPreChangeReplicas()`
/ `NonMMAPreTransferLease()` → `PostApply()`. MMA is informed but does not control these
changes.

### Lease Queue (`lease_queue.go`)

**Primary role**: Ensure leases are on the right stores — honoring lease preferences,
shedding from IO-overloaded stores, balancing lease counts, and correcting lease types.

**Key characteristics**:
- Processes one replica at a time
- Uses `plan.LeasePlanner` to determine if transfer needed
- Rate-limited: `MinLeaseTransferInterval` (1s default) for rebalancing transfers
- Bypasses rate limit for preference violations and IO overload
- Only produces `AllocationTransferLeaseOp` or no-op
- Purgatory for preference violations with no suitable target (10s retry)

**Interactions with MMA**: Changes go through `AllocatorSync.NonMMAPreTransferLease()` →
`PostApply()`.

### Store Rebalancer (`store_rebalancer.go`)

**Primary role**: Balance load across stores by transferring leases and relocating
replicas from overfull stores. Operates at store level rather than per-range.

**Key characteristics**:
- Runs in its own goroutine on a timer (jittered interval)
- Two phases: (1) lease transfers from hot ranges, (2) replica relocations
- Only acts when local store exceeds overfull threshold
- Skips ranges below min load fraction (0.5% for leases, 2% for replicas)
- Disabled when MMA mode is active (`LoadBasedRebalancingModeIsMMA()`)
- Uses `RangeRebalancer` interface (implemented by replicate queue) for applying changes

**Interactions with MMA**: Disabled when MMA is active. Changes go through replicate
queue's `TransferLease()` and `RelocateRange()`.

### Multi-Metric Allocator (`allocator/mmaprototype/`)

**Primary role**: Load balancing across multiple dimensions (CPU, write bandwidth, byte
size). Generates lease transfers and replica moves to balance overloaded stores.

**Key characteristics**:
- Tracks full cluster state: stores, nodes, ranges, pending changes
- Constraint matching via posting-list index (fast store↔constraint lookups)
- Rebalancing: max 1 replica move + max 8 lease transfers per `ComputeChanges()` call
- Pending change tracking with fraction-based pile-up prevention (10% threshold)
- Store status model: Health (OK/Unhealthy/Dead) × Disposition (OK/Refusing/Shedding)
- MMARebalanceAdvisor: veto mechanism for legacy allocator's rebalance candidates
- 60s retry delay after failed changes
- Leaseholder-driven: only rebalances ranges where local store holds lease

**What MMA does NOT do today**:
- Repair actions (add/remove for under/over-replication)
- Dead/decommissioning replica handling
- Learner cleanup
- Atomic replication finalization
- Lease preference enforcement (partial — uses preferences in rebalancing decisions)
- Lease type correction
- Count-based rebalancing
- Purgatory / retry mechanisms for transient failures
- Priority-based processing ordering

---

## Key Cluster Settings

| Setting | Default | Controls |
|---|---|---|
| `kv.allocator.load_based_rebalancing` | `leases and replicas` | Which rebalancing mode is active |
| `kv.replicate_queue.enabled` | `true` | Enable/disable replicate queue |
| `kv.lease_queue.enabled` | `true` | Enable/disable lease queue |
| `kv.allocator.min_lease_transfer_interval` | `1s` | Rate limit on lease transfers for rebalancing |
| `kv.allocator.min_io_overload_lease_shed_interval` | `30s` | Rate limit on IO overload lease shedding |
| `kv.enqueue_in_replicate_queue_on_span_config_update.enabled` | `true` | Enqueue on config changes |
| `kv.enqueue_in_replicate_queue_on_problem.interval` | `0` (disabled) | Proactive problem range enqueue |
| `kv.replicate_queue.max_size` | `MaxInt64` | Max replicate queue size |
| `kv.priority_inversion_requeue_replicate_queue.enabled` | `true` | Priority inversion requeue |

## MMA Rebalancing Modes

| Mode | Value | Store Rebalancer | Replicate Queue Rebalancing | MMA Rebalancing |
|---|---|---|---|---|
| `off` | 0 | Disabled | Count-based only | Disabled |
| `leases` | 1 | Lease transfers only | Count-based only | Disabled |
| `leases and replicas` | 2 | Both | Count-based only | Disabled |
| `multi-metric only` | 3 | Disabled | Count-based only | Active |
| `multi-metric and count` | 4 | Disabled | Count-based only | Active |

Note: In all modes, the replicate queue's **repair actions** always run regardless of
rebalancing mode. Only the `ConsiderRebalance` action is affected.

---

## Key Files

| File | Package | Role |
|---|---|---|
| `pkg/kv/kvserver/replicate_queue.go` | kvserver | Replicate queue implementation |
| `pkg/kv/kvserver/lease_queue.go` | kvserver | Lease queue implementation |
| `pkg/kv/kvserver/store_rebalancer.go` | kvserver | Store-level load rebalancer |
| `pkg/kv/kvserver/allocator/plan/replicate.go` | plan | Replica change planner |
| `pkg/kv/kvserver/allocator/plan/lease.go` | plan | Lease transfer planner |
| `pkg/kv/kvserver/allocator/plan/op.go` | plan | Operation types (Noop, TransferLease, ChangeReplicas, FinalizeAtomic) |
| `pkg/kv/kvserver/allocator/allocatorimpl/allocator.go` | allocatorimpl | Core allocator: ComputeAction, RebalanceTarget, TransferLeaseTarget |
| `pkg/kv/kvserver/mmaintegration/` | mmaintegration | AllocatorSync bridge between legacy allocator and MMA |
| `pkg/kv/kvserver/allocator/mmaprototype/allocator.go` | mmaprototype | MMA allocator interface |
| `pkg/kv/kvserver/allocator/mmaprototype/allocator_state.go` | mmaprototype | MMA allocator implementation |
| `pkg/kv/kvserver/allocator/mmaprototype/cluster_state.go` | mmaprototype | MMA cluster state tracking |
| `pkg/kv/kvserver/allocator/mmaprototype/cluster_state_rebalance_stores.go` | mmaprototype | MMA rebalancing logic |
| `pkg/kv/kvserver/allocator/mmaprototype/constraint_matcher.go` | mmaprototype | MMA constraint matching |
| `pkg/kv/kvserver/allocator/mmaprototype/rebalance_advisor.go` | mmaprototype | MMA veto mechanism for legacy allocator |
| `pkg/kv/kvserver/kvserverbase/base.go` | kvserverbase | Cluster settings for queue/rebalancer control |
