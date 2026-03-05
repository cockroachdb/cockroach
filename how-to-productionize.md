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

## PR 1: Foundation

**Content:**
- `RepairAction` enum (all 14 values + `RepairPending`, `RepairSkipped`,
  `NoRepairNeeded`) with priority ordering.
- `computeRepairAction()` skeleton — the decision tree that maps range state to
  a repair action. Returns stubs / `NoRepairNeeded` for all actions initially.
- `repairRanges` index on `clusterState` + `updateRepairAction()` maintenance
  (called from `processRangeMsg`, `pendingChangeEnacted`, `undoPendingChange`,
  `addPendingRangeChange`, `updateStoreStatuses`, range removal).
- DSL commands (`repair-needed`, `repair`) as stubs in `cluster_state_test.go`.
- Constraint methods moved from test file to production code (`constraint.go`):
  these are a prerequisite for multiple repair actions later.
- Non-voter constraint helpers moved to production code.

**Files touched:**
`cluster_state.go`, `cluster_state_repair.go` (enum + skeleton only),
`cluster_state_test.go`, `constraint.go`, `constraint_unused_test.go`.

**Review focus:** Does this action space make sense? Is the priority ordering
right? Is the indexing/maintenance approach correct? Are the constraint method
signatures production-ready?

**~LOC:** 300–400

---

## PR 2: AddVoter + repair orchestration

**Content:**
- `repair()` method on `rebalanceEnv` — iterates `repairRanges` by priority,
  calls per-action repair functions, emits pending changes.
- `IncludeRepair` in `ChangeOptions`; `ComputeChanges` calls `repair()` before
  `rebalanceStores()` when set.
- Full `repairAddVoter()` implementation, including non-voter-to-voter
  promotion path.
- `pickStoreByDiversity` generalized to accept a `diversityScorer` function
  parameter (needed by AddVoter and reused later by removal/swap).
- Random tiebreaking in diversity picker.
- `RepairPending` state and pending-change suppression logic.
- DSL tests for `repair_add_voter`, `repair_promote_nonvoter`.
- One simple ASIM upreplication test to prove the wiring works end-to-end.

**Files touched:**
`cluster_state_repair.go`, `allocator.go`, `allocator_state.go`,
`cluster_state_test.go`, testdata files, ASIM test files.

**Review focus:** Is the orchestration right? Currently greedy on highest
priority — is that correct, or should it be weighted/round-robin? How does
pending-change suppression work (any pending change on a range → skip repair)?
Is AddVoter correct? Is the diversity picker generalization clean?

**~LOC:** 400–500

---

## PR 3: Production integration + metrics

**Content:**
- `LBRebalancingMultiMetricRepairAndRebalance` mode added to
  `LoadBasedRebalancingMode` cluster setting.
- Setting validation guarded by `buildutil.IsCrdbTest` so this mode cannot be
  enabled outside of tests.
- Replicate queue `shouldQueue`/`process` return immediately when mode active.
- Lease queue `shouldQueue`/`process` return immediately when mode active.
- `rebalanceUntilStable()` extracted from `mmaStoreRebalancer.run()`.
- `ForceReplicationScanAndProcess` delegates to
  `mmaStoreRebalancer.rebalanceUntilStable()` when mode active
  (fixes `WaitForFullReplication` in tests).
- `IsLeaveJoint()` on `ExternalRangeChange`; leave-joint changes routed through
  `maybeLeaveAtomicChangeReplicas` instead of `changeReplicasImpl`.
- Repair metrics (`mma_metrics.go`): success/failure counters for repair lease
  and replica changes.
- `originMMARepair` tracking in `AdjustPendingChangeDisposition`.
- `TestMMAUpreplication` integration test: end-to-end upreplication from 1→3
  voters under MMA.

**Files touched:**
`kvserverbase/base.go`, `replicate_queue.go`, `lease_queue.go`,
`mma_store_rebalancer.go`, `queue_helpers_testutil.go`, `client_mma_test.go`,
`mma_metrics.go`, `allocator_state.go`, `range_change.go`.

**Review focus:** Is this production integration safe? Are queue disabling
semantics correct? Is the `buildutil.IsCrdbTest` guard sufficient? Is the
`IsLeaveJoint` routing correct? Does `ForceReplicationScanAndProcess` delegation
break any existing tests?

**~LOC:** 300–400

---

## PR 4: Count-based repair actions

**Content:**
- `repairRemoveVoter()` — removes over-replicated voters, respecting
  leaseholder exclusion and diversity scoring.
- `repairAddNonVoter()` — adds non-voters when under-replicated.
- `repairRemoveNonVoter()` — removes over-replicated non-voters.
- `repairRemoveLearner()` — cleans up orphaned learners.
- `repairFinalizeAtomicReplicationChange()` — emits leave-joint change.
- Helper extractions that emerge from the patterns: `enactRepair`,
  `filterAddCandidates`, `pickBestRemovalCandidate`, `excludeLeaseholder`,
  `replicaStateForStore`.
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
- `repairReplace()` unified helper that merges the four Replace functions
  (remove dead/decom replica + add replacement in one operation).
- Quorum-loss detection: if `aliveVoters < quorum`, return `RepairSkipped`.
- DSL tests for all four actions.
- ASIM tests for all four actions + `repair_range_unavailable` scenario.

**Files touched:**
`cluster_state_repair.go`, testdata files, ASIM testdata files.

**Review focus:** Is quorum-loss detection correct (off-by-one risk)? Is the
replacement store selection right (diversity + constraint satisfaction)? Does the
unified `repairReplace` helper correctly handle all four cases? Is the
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