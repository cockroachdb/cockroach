# MMA Repair Pipeline: Productionization Plan

This document describes how to split the prototype PR (#164658, branch `mmra`)
into a series of reviewable pull requests for merging into `master`.

The prototype has 63 commits, ~5500 lines added across 71 files. Nobody can
review that in one pass. The plan below breaks it into 6 PRs, each 200–500
lines of real code (excluding testdata), reviewable in a single sitting.

Design docs (`multi-metric-rebalancer.md`, `mma-constraint-repair.md`,
`refactor.md`, `todo.md`, `asim_tests_todo.md`, `plan.md`, `CLAUDE.md`) are
prototyping artifacts and will NOT be checked in.

---

## PR 1: Foundation — [#165413](https://github.com/cockroachdb/cockroach/pull/165413) ✅

**Commits:**
1. `allocator/mmaprototype: move constraint helpers to production code` —
   moved `voterConstraintCount`, `constraintCount`, `constraintsForAddingVoter`,
   `candidatesToConvertFromNonVoterToVoter`, and related methods from
   `constraint_unused_test.go` to `constraint.go`.
2. `allocator/mmaprototype: add RepairAction enum and decision tree` —
   `RepairAction` enum (14 action values + `RepairSkipped`, `RepairPending`,
   `NoRepairNeeded`, `numRepairActions`), `computeRepairAction()` decision tree,
   `go:generate stringer`, `SafeFormat`.
3. `allocator/mmaprototype: wire repair index into cluster state` —
   `repairRanges` index on `clusterState`, `updateRepairAction()` called from
   `processRangeMsg`, `pendingChangeEnacted`, `undoPendingChange`,
   `addPendingRangeChange`, `updateStoreStatuses`, range removal. DSL commands
   `repair-needed` and `repair` (stub). Six testdata files covering tracking
   scenarios.

**Files touched:**
`cluster_state.go`, `cluster_state_repair.go`, `cluster_state_test.go`,
`constraint.go`, `constraint_unused_test.go`, `allocator_state.go`,
`repairaction_string.go`, `BUILD.bazel`, `stringer.bzl`, testdata files.

---

## PR 2: AddVoter + repair orchestration — [#165423](https://github.com/cockroachdb/cockroach/pull/165423) ✅

**Commits:**
1. `allocator/mmaprototype: add repair orchestration loop` —
   `repair()` method on `rebalanceEnv` iterating `repairRanges` by priority,
   filtering to leaseholder ranges, dispatching to per-action functions (all
   actions except `AddVoter` hit default "not yet implemented" log).
   `IncludeRepair` on `ChangeOptions`; `ComputeChanges` calls `repair()` before
   `rebalanceStores()` when set. `originMMARepair` `ChangeOrigin` enum value.
   `AdjustPendingChangeDisposition` updated to handle `originMMARepair`
   (temporarily uses rebalance counters). DSL `repair` command wired up.
2. `allocator/mmaprototype: implement repairAddVoter with non-voter promotion` —
   full `repairAddVoter()` with two code paths: (a) promote existing non-voter
   to voter via `promoteNonVoterToVoter()` using `MakeReplicaTypeChange`, (b)
   add new voter on constraint-satisfying, diversity-maximizing store. Helpers:
   `enactRepair`, `filterAddCandidates`, `pickStoreByDiversity` (with reservoir
   sampling for tie-breaking), `replicaStateForStore`, `diversityScorer` type,
   `isLeaseholderOnStore`. Two testdata files: `repair_add_voter`,
   `repair_promote_nonvoter`.
3. `allocator/mmaprototype: lift mmaid tagging to ComputeChanges` —
   `mmaid` increment and logtag moved from `rebalanceStores()`/`repair()` into
   `ComputeChanges` so both phases share the same mmaid context.

**Files touched:**
`cluster_state_repair.go`, `allocator.go`, `allocator_state.go`,
`cluster_state.go`, `cluster_state_rebalance_stores.go`,
`cluster_state_test.go`, `mma_metrics.go`, `repairaction_string.go`,
testdata files.

---

## PR 3: Production integration + metrics — [#165526](https://github.com/cockroachdb/cockroach/pull/165526) ✅

**Content:**
- `LBRebalancingMultiMetricRepairAndRebalance` mode added to
  `LoadBasedRebalancingMode` cluster setting in `kvserverbase/base.go`.
  `LoadBasedRebalancingModeIsMMA` updated to include the new mode. New helper
  `LoadBasedRebalancingModeIsMMARepairAndRebalance`.
- Replicate queue `shouldQueue`/`process` return immediately when mode active.
- Lease queue `shouldQueue`/`process` return immediately when mode active.
- `CountBasedRebalancingDisabled()` returns `true` for new mode.
- `rebalanceUntilStable()` extracted from `mmaStoreRebalancer.run()` — loops
  calling `rebalance()` until no changes are computed.
- `ForceReplicationScanAndProcess` delegates to
  `mmaStoreRebalancer.rebalanceUntilStable()` when mode active (fixes
  `WaitForFullReplication` in tests).
- `IsLeaveJoint()` on `ExternalRangeChange`; leave-joint changes routed through
  `maybeLeaveAtomicChangeReplicas` instead of `changeReplicasImpl`.
- Repair metrics in `mma_metrics.go`: `RepairReplicaChange{Success,Failure}`,
  `RepairLeaseChange{Success,Failure}` counters. Replaces the temporary
  rebalance-counter routing from PR 2.
- `originMMARepair` case in `AdjustPendingChangeDisposition` routes to
  dedicated repair metrics.
- `TestMMAUpreplication` integration test: 3-node cluster, scratch range goes
  from 1→3 voters entirely through MMA repair.
- ASIM wiring: `IncludeRepair` set in ASIM's `mmaStoreRebalancer` based on
  mode check, queue `Tick()` methods check enabled flags, "mma-repair" config
  added to datadriven simulation tests.

**Files touched:**
`kvserverbase/base.go`, `replicate_queue.go`, `lease_queue.go`,
`allocator/allocatorimpl/allocator.go`, `mma_store_rebalancer.go`,
`queue_helpers_testutil.go`, `client_mma_test.go`, `mma_metrics.go`,
`allocator_state.go`, `range_change.go`,
`asim/queue/replicate_queue.go`, `asim/queue/lease_queue.go`,
`asim/state/impl.go`, `asim/mmaintegration/mma_store_rebalancer.go`,
`asim/tests/datadriven_simulation_test.go`.

**Review focus:** Is this production integration safe? Are queue disabling
semantics correct? Is the `IsLeaveJoint` routing correct? Does
`ForceReplicationScanAndProcess` delegation break any existing tests?

**~LOC:** 400–500

---

## PR 4: Count-based repair actions

**Content:**
- `repairRemoveVoter()` — removes over-replicated voters, respecting
  leaseholder exclusion and diversity scoring.
- `repairAddNonVoter()` — adds non-voters when under-replicated.
- `repairRemoveNonVoter()` — removes over-replicated non-voters.
- `repairRemoveLearner()` — cleans up orphaned learners.
- `repairFinalizeAtomicReplicationChange()` — emits leave-joint change.
- Helper extractions that emerge from the patterns: `pickBestRemovalCandidate`,
  `excludeLeaseholder`.
- DSL tests for each action.
- ASIM tests for each action.

**Files touched:**
`cluster_state_repair.go`, `cluster_state_test.go`, testdata files,
ASIM testdata files.

**Review focus:** These follow the same pattern as AddVoter. Review velocity
should be high. Key things: is leaseholder exclusion correct in RemoveVoter? Is
the over-replication detection right? Are the helper extractions clean?

**~LOC:** 400–500

---

## PR 5: Dead/decommissioning replacement

**Content:**
- `repairReplaceDeadVoter()` — replaces dead voter with new voter on a live
  store, respecting diversity.
- `repairReplaceDeadNonVoter()` — same for non-voters.
- `repairReplaceDecommissioningVoter()` — replaces decommissioning voter.
- `repairReplaceDecommissioningNonVoter()` — same for non-voters.
- `repairReplaceReplica()` unified helper that merges the four Replace functions
  (remove dead/decom replica + add replacement in one operation).
- DSL tests for all four actions.
- ASIM tests for all four actions + `repair_range_unavailable` scenario.

**Files touched:**
`cluster_state_repair.go`, testdata files, ASIM testdata files.

**Review focus:** Is quorum-loss detection correct (off-by-one risk)? Is the
replacement store selection right (diversity + constraint satisfaction)? Does the
unified `repairReplaceReplica` helper correctly handle all four cases? Is the
unavailable-range skipping logic sound?

**~LOC:** 300–400

---

## PR 6: Constraint swaps

**Content:**
- `repairSwapVoterForConstraints()` — removes a voter violating constraints,
  adds a voter on a constraint-satisfying store.
- `repairSwapNonVoterForConstraints()` — same for non-voters.
- Constraint swap helpers moved from test file to production code.
- DSL tests for both actions + constraint interaction scenarios.
- ASIM tests for both actions.

**Files touched:**
`cluster_state_repair.go`, `constraint.go`, testdata files, ASIM testdata
files.

**Review focus:** Is constraint violation detection correct? Is the "which
replica to remove" logic right (pick the one with worst constraint fit)? Are
swap semantics correct (does a swap temporarily violate quorum or
constraints)?

**~LOC:** 200–300

---

## Ordering and dependencies

```
PR 1 (foundation) ──→ PR 2 (AddVoter + orchestration) ──→ PR 3 (production integration)
                                                                       │
                       ┌───────────────────────────────────────────────┘
                       ↓
                  PR 4 (count-based) ──→ PR 5 (dead/decom) ──→ PR 6 (constraint swaps)
```

PRs 4–6 are strictly sequential (later PRs reuse helpers from earlier ones).
PR 3 could technically go after PR 4–6, but doing it early surfaces production
integration questions before investing in all 12 actions.

## Notes for reviewers

- **Testdata files** (DSL golden outputs, ASIM golden outputs) are large and
  mechanical. Focus review on the test setup and configs, not the golden text.
- **Pending-change suppression** is an intentional design choice: any pending
  change on a range suppresses further repair for that range. This prevents
  pile-up but could delay repairs if changes get stuck. The GC mechanism
  handles stuck changes.
- **The `computeRepairAction` decision tree** (~200 lines) determines priority
  ordering for all actions. This is the most important piece to get right and
  should be scrutinized carefully in PR 1.
