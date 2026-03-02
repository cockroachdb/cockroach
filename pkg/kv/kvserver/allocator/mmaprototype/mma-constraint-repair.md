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

## TODO

- Define prioritization within the repair set (under-replication > constraint violation >
  over-replication, etc. — mirroring the replicate queue's priority hierarchy).
- Define what repair operations MMA needs to produce (add replica, remove replica,
  replace replica via add+remove).
- Define how repair interacts with pending changes (avoid piling up repairs on the same
  range).
- Define how repair interacts with store health/disposition (don't place replicas on dead
  or shedding stores).
