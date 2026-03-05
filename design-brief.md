# MMA Repair Pipeline: Architecture Brief

This document describes how the MMA repair prototype works. It assumes
familiarity with the existing MMA (load-based rebalancing) and the SMA
(single-metric allocator, i.e. the replicate queue, lease queue, and legacy
store rebalancer).

## Background: the two-system problem

Today, replica placement is split across two independent systems:

- **MMA** handles load-based rebalancing. It runs in a single loop on each
  store, receives `StoreLeaseholderMsg` updates with full range state, maintains
  an in-memory `clusterState`, and emits `ExternalRangeChange` actions (replica
  moves, lease transfers). It tracks pending changes to avoid conflicting
  decisions.
- **The replicate and lease queues (SMA)** handle repair: upreplication,
  dead/decommissioning node replacement, constraint enforcement, lease
  preferences, IO overload reactions, follow-the-workload, etc. They operate
  per-range through the queue mechanism.

These two systems make decisions independently and can conflict. The allocator
sync layer exists to coordinate them, but fundamentally we are maintaining two
competing allocators.

## What changes: one loop for everything

The repair prototype extends MMA to handle repair in the same loop that already
handles rebalancing. When the new mode is active:

1. The `mmaStoreRebalancer` ticks periodically (as today).
2. On each tick, it builds a `StoreLeaseholderMsg` from the local store (as
   today).
3. It calls `ComputeChanges` with `IncludeRepair: true`.
4. Inside `ComputeChanges`, **`repair()` runs first**, then
   `rebalanceStores()`. Repair has strict priority: its pending changes prevent
   the rebalancer from touching the same ranges.
5. The replicate and lease queues are completely disabled (`shouldQueue` and
   `process` return immediately).

The result: a single code path, a single state model, a single set of pending
changes.

## State model: `clusterState` tracks everything

MMA's `clusterState` already tracks, per range:

- Replica placement (which store, which type, who is leaseholder)
- Span config (num_replicas, constraints, voter constraints, lease preferences)
- Pending changes (in-flight operations, with success/failure tracking)
- Per-store load, capacity, health, and disposition

The repair pipeline adds one field per range: `repairAction`. This is an enum
value, eagerly computed, that answers: "what repair does this range need right
now?"

### Eager indexing

The `repairAction` is recomputed at every trigger point:

- When a `StoreLeaseholderMsg` arrives (replicas or config changed)
- When a store's health/disposition status changes
- When a pending change is added, enacted, or undone

Ranges that need repair (action is not `NoRepairNeeded` or `RepairPending`) are
indexed in `clusterState.repairRanges`, a map from `RepairAction` to the set of
range IDs needing that action. This avoids scanning all ranges on each tick.

### Pending change suppression

If a range has any pending change (repair or rebalance), its repair action is
set to `RepairPending` and it is excluded from repair consideration. This
prevents pile-up: only one operation at a time per range. If a pending change
gets stuck, the existing GC mechanism cleans it up and the range becomes
eligible for repair again.

## The RepairAction enum

The enum has 12 actionable values plus three terminal states, ordered by
priority. The ordering is ported from the SMA's `AllocatorAction` enum with
some simplifications. It determines both which problem to fix first for a
single range and which ranges get repaired first when there are many.

```
HIGH PRIORITY (incomplete state)
  FinalizeAtomicReplicationChange
  RemoveLearner

VOTER COUNT
  AddVoter
  ReplaceDeadVoter
  ReplaceDecommissioningVoter
  RemoveVoter

NON-VOTER COUNT
  AddNonVoter
  ReplaceDeadNonVoter
  ReplaceDecommissioningNonVoter
  RemoveNonVoter

CONSTRAINT PLACEMENT
  SwapVoterForConstraints
  SwapNonVoterForConstraints

TERMINAL
  RepairSkipped      (can't act: no config, lost quorum, etc.)
  RepairPending      (already being worked on)
  NoRepairNeeded     (healthy)
```

### No numerical scoring

The SMA uses a complex numerical scoring hierarchy (the "scorer") to rank
candidate stores. This scoring is famously opaque — it's difficult to predict
what the allocator will do in a given situation.

The repair pipeline deliberately avoids numerical ranking. The decision tree in
`computeRepairAction` is a straightforward if/else cascade: count replicas,
classify by store health, compare against the span config. The result is one of
the enum values above. There is no scoring, no weighting, no priority numbers.

For candidate selection within an action (e.g. "which store should we add a
voter to?"), the pipeline uses constraint satisfaction and locality diversity.
Ties are broken randomly. It does not (initially) consider load when selecting
repair targets — a range that needs upreplication gets a new replica on the
store with best diversity, not the least-loaded store. This matches current SMA
behavior and defers load-aware repair to future work.

## The repair loop

On each tick, `repair()` iterates the `repairRanges` index in priority order
(lower enum value = higher priority). For each range needing repair:

1. Skip if the local store is not the leaseholder (only leaseholders propose
   changes, matching SMA behavior).
2. Dispatch to the action-specific repair function (e.g. `repairAddVoter`).
3. The repair function selects targets using constraint satisfaction and
   diversity scoring, then records a pending change.

The prototype uses a simple greedy approach: process all ranges at each
priority level before moving to the next. In production, we would shape this
to avoid starvation — e.g. cheap operations like finalizing joint configs can
be batched aggressively, but shouldn't block upreplication indefinitely. The
enum ordering establishes relative priority, but the scheduling policy on top
of it has considerable flexibility and can be tuned as needed.

## How repair actions work

Each action follows the same pattern:

1. **Classify replicas.** Count voters, non-voters, learners. Identify dead and
   decommissioning replicas by checking store status.
2. **Select target.** For additions: find stores satisfying constraints, rank by
   locality diversity relative to existing replicas, break ties randomly. For
   removals: prefer dead > decommissioning > healthy stores, then pick worst
   diversity among the selected tier.
3. **Record pending change.** Call `addPendingRangeChange` which updates the
   cluster state and moves the range to `RepairPending`.

Special cases:

- **AddVoter** first checks whether a non-voter can be promoted to voter
  (cheaper than adding a new replica from scratch).
- **Quorum loss** (alive voters < quorum) causes the range to be skipped — no
  repair is possible without quorum.
- **Constraint swaps** are paired remove+add operations: remove the
  constraint-violating replica and add one on a conforming store.

## Integration with production code

The integration is minimal and behind a cluster setting:

- **`LBRebalancingMultiMetricRepairAndRebalance`**: a new value for the existing
  `kv.allocator.load_based_rebalancing` setting. This activates repair in
  `ComputeChanges` and disables the replicate/lease queues. The setting name
  (`load_based_rebalancing`) originally implied rebalancing only, but adding
  repair as a mode value is pragmatic — it's the same control surface operators
  already use. If the naming feels misleading long-term, we can migrate it, but
  it shouldn't block the work.
- **Queue disabling**: `shouldQueue` and `process` on both the replicate and
  lease queues return immediately when the setting is active.
- **`ForceReplicationScanAndProcess`**: routes to
  `mmaStoreRebalancer.rebalanceUntilStable()` when the setting is active. This
  is needed for `WaitForFullReplication` in tests.
- **Leave-joint handling**: MMA can now emit leave-joint changes
  (`FinalizeAtomicReplicationChange`), which are routed through
  `maybeLeaveAtomicChangeReplicas` rather than the normal
  `changeReplicasImpl` path.

## What stays the same

- The `StoreLeaseholderMsg` / `StoreLoadMsg` data flow is unchanged.
- Pending change tracking is unchanged — repair uses the same mechanism as
  rebalancing.
- The `AllocatorSync` coordination layer is unchanged.
- Rebalancing logic is completely unaffected.

## Scope and non-goals

The repair pipeline aims for parity with the SMA's core repair functionality,
not full feature parity with every behavior in the replicate and lease queues.
Specifically:

- **Count-based rebalancing** (harmonizing replica/lease counts across stores)
  is a separate workstream, not part of this prototype.
- **IO overload reactions** in the lease queue are not ported. Many of these
  have been mitigated at other layers. If specific behaviors turn out to be
  needed, they would be rebuilt in MMA rather than ported from the SMA.
- **Follow-the-workload** would go away entirely. Its heuristics are subsumed
  by MMA's load-based lease placement.
- **Load-aware repair target selection** is deferred. Initially, repair picks
  targets by constraint satisfaction and diversity only — matching the SMA.
  Load-aware selection is a future improvement that would be unique to MMA.
