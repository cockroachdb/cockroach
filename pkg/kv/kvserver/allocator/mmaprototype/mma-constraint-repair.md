# MMA Constraint Repair

## Goal

Teach MMA to repair constraint violations, absorbing this responsibility from the
replicate queue. Today, MMA skips non-conformant ranges during load rebalancing. Instead,
it should detect and fix them.

## Constraint Analysis: Eager Evaluation with Caching

Today, constraint analysis (`ensureAnalyzedConstraints()`) is deferred until rebalancing
time and only runs for ranges considered for load rebalancing (top-K by load on the local
store). This means constraint violations on ranges not in the top-K — or ranges that are
under-replicated — may never be discovered.

Instead, we compute constraints eagerly in `processRangeMsg()`, right after updating
replicas and normalizing the span config. The analysis is cached in
`rangeState.constraints` and only recomputed when replicas or config actually change.

### Why eager evaluation is cheap

- The `processRangeMsg` fast path (lines 1467-1500 in `cluster_state.go`) already detects
  the common case where replicas haven't changed and returns early. Most ranges won't need
  reanalysis on each `StoreLeaseholderMsg` cycle.
- `ensureAnalyzedConstraints()` returns immediately when the cache is valid (line 804 in
  `allocator_state.go`).
- The constraint matcher uses posting-list indices, so store-constraint lookups are O(1).
- `clearAnalyzedConstraints()` is already called when replicas or config change, so the
  cache invalidation logic is already in place.

### What the analysis tells us

The `rangeAnalyzedConstraints` object can detect:

- **Under-replication**: `notEnoughVoters()`, `notEnoughNonVoters()` — fewer replicas than
  the span config requires.
- **Over-replication**: more replicas than needed (detectable from replica counts vs
  `numNeededReplicas`).
- **Constraint violations**: `expectNoUnsatisfied()` → `constraintCount()` returns
  `under`/`match`/`over` counts per constraint conjunction. `under > 0` means a constraint
  has too few replicas satisfying it; `over > 0` means too many.
- **Voter constraint violations**: `expectMatchedVoterConstraints()` — same as above but
  for voter-specific constraints.
- **Lease preference violations**: `leaseholderPreferenceIndex` vs the preferred index.

### Repair set

After eager analysis, ranges with any violation are added to a repair set. Ranges that
become conformant are removed. At `ComputeChanges` time, the repair set is processed
before (or with higher priority than) load rebalancing.

## Repair Action Ordering

The legacy allocator (`ComputeAction`) uses numerical priorities with small adjustments
(quorum gap for `AddVoter`, parity for `RemoveVoter`, etc.) that serve double duty as
both per-range action selection and cross-range queue ordering. MMA doesn't use a priority
queue, so we replace this with a plain enum whose order determines:

1. If a range has multiple problems, which one to fix first.
2. If we can only emit N changes per pass, which ranges' repairs take precedence.

```go
type RepairAction int

const (
    // Highest priority: clean up incomplete state.
    FinalizeAtomicReplicationChange RepairAction = iota
    RemoveLearner

    // Voter count repair: add/replace before remove.
    AddVoter
    ReplaceDeadVoter
    ReplaceDecommissioningVoter
    RemoveVoter // candidate selection prefers dead > decommissioning > healthy

    // Non-voter count repair: add/replace before remove.
    AddNonVoter
    ReplaceDeadNonVoter
    ReplaceDecommissioningNonVoter
    RemoveNonVoter // candidate selection prefers dead > decommissioning > healthy

    // Constraint repair: correct replica count but wrong placement.
    SwapVoterForConstraints
    SwapNonVoterForConstraints

    // No repair needed.
    NoRepairNeeded
)
```

### Changes from legacy priorities

- **No numerical priorities or adjustments.** The legacy allocator adjusted `AddVoter`
  priority by the quorum gap and `RemoveVoter` by parity (even replica counts are more
  fragile). We drop these. The enum order is sufficient.
- **Finer granularity via enum splitting if needed.** If we later want to distinguish
  near-quorum-loss voter additions from less critical ones, we add a separate enum value
  (e.g., `AddVoterCritical` before `AddVoter`) rather than a numerical tweak.
- **`RangeUnavailable` is not an action.** It's a condition (no quorum) that prevents any
  action. MMA should detect this and skip the range, not represent it as a repair action.
- **Remove actions are simplified.** The legacy allocator had separate `RemoveDead*`,
  `RemoveDecommissioning*`, and `Remove*` actions. We collapse these into a single
  `Remove*` per replica type, with candidate selection preferring dead > decommissioning >
  healthy replicas.
- **Constraint swaps are explicit actions.** The legacy allocator handled constraint
  violations implicitly through `ConsiderRebalance`. We make them explicit enum values
  (`SwapVoterForConstraints`, `SwapNonVoterForConstraints`), ordered after count repairs.
  This matches the decision tree already outlined in the MMA constraint code
  (`constraint.go:1845`): fix replica counts first, then fix placement.

## Repair Operations

MMA already produces the operations needed for repair: add replica, remove replica, and
replace (add + remove as a pair). These are the same primitives used for load rebalancing,
represented as pending changes. No new operation types are needed.

## Interaction with Pending Changes

A range with any pending change is skipped for repair. This avoids piling up multiple
concurrent changes on the same range and lets the in-flight operation complete before
reassessing the range's state. Once the pending change resolves (success or failure), the
range is re-evaluated at the next `processRangeMsg` and re-added to the repair set if
still non-conformant.

## Interaction with Store Health and Disposition

### Adding replicas (target selection)

The target store must have `ReplicaDispositionOK`, which implies `HealthOK` (only
`storepool.StoreStatusAvailable` maps to this disposition — see
`mmaintegration/store_status.go`). This single check filters out dead, unknown,
unhealthy, decommissioning, draining, and IO-overloaded stores.

### Removing replicas (candidate preference)

When choosing which replica to remove, prefer stores in this order:

1. `HealthDead` — replica is already lost, removing it is free.
2. `HealthUnknown` — no recent gossip; may be dead, shouldn't block progress.
3. `HealthUnhealthy` — store is responding but degraded.
4. `ReplicaDispositionShedding` — store is actively draining replicas
   (decommissioning or IO-overloaded).
5. `ReplicaDispositionRefusing` — store is not accepting new replicas but not
   actively shedding (e.g., nearly full disk).
6. Any remaining replica — healthy stores, chosen by constraint/diversity fit.

Within each bucket, further tiebreaking can use constraint satisfaction and diversity
(prefer removing replicas that are less useful for constraint satisfaction or that
contribute least to diversity).
