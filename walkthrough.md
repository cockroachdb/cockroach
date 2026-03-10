# MMA Repair Pipeline Walkthrough

*2026-03-10T14:33:07Z by Showboat dev*
<!-- showboat-id: 4ef43b28-0ee3-4732-bc27-9f4cdaaecaba -->

## Overview

This PR adds a **repair pipeline** to the Multi-Metric Allocator (MMA). Before this PR,
MMA only handled **rebalancing** -- moving replicas between stores to balance load.
Repair (upreplication, removing dead replicas, constraint satisfaction, etc.) was still
handled by the legacy **replicate queue** and **lease queue** in kvserver.

After this PR, MMA can operate in a new mode --
`LBRebalancingMultiMetricRepairAndRebalance` -- where it handles **both** repair and
rebalancing. In this mode, the replicate queue and lease queue are completely disabled
(`shouldQueue` returns false, `process` is a no-op). MMA becomes the sole decision-maker
for all replica placement.

### Architecture at a glance

The repair pipeline lives inside `mmaprototype.clusterState` and is invoked from
`ComputeChanges()` before rebalancing. The flow is:

1. **Mode gating**: The cluster setting `kv.allocator.load_based_rebalancing` has a new
   value `multi-metric repair and rebalance`. When active, it sets `IncludeRepair: true`
   on `ChangeOptions` passed to `ComputeChanges`.

2. **Queue disabling**: `replicateQueue.shouldQueue` and `.process` both short-circuit
   with no-ops when this mode is active.

3. **Repair-before-rebalance**: Inside `ComputeChanges`, if `IncludeRepair` is true,
   `repair()` runs first. Its pending changes prevent the subsequent `rebalanceStores()`
   from touching the same ranges.

4. **Change application**: Repair produces `ExternalRangeChange` values that flow
   through the same `mmaStoreRebalancer.applyChange()` path as rebalancing changes,
   with a new code path for leave-joint changes (`IsLeaveJoint()`).

5. **ForceReplicationScanAndProcess**: In test mode, this now routes through
   `mmaStoreRebalancer.rebalanceUntilStable()` instead of the replicate queue when
   MMA repair mode is active.

## 1. The New Mode: `LBRebalancingMultiMetricRepairAndRebalance`

The cluster setting enum gets a new value. When set, MMA owns everything.

```bash
sed -n '175,179p' pkg/kv/kvserver/kvserverbase/base.go
```

```output
	// LBRebalancingMultiMetricRepairAndRebalance means that MMA handles both
	// rebalancing AND repair; the replicate and lease queues are completely
	// disabled. This is the mode under which MMA is solely responsible for all
	// replica placement decisions.
	LBRebalancingMultiMetricRepairAndRebalance
```

And a helper predicate that callers check:

```bash
sed -n '143,148p' pkg/kv/kvserver/kvserverbase/base.go
```

```output
// LoadBasedRebalancingModeIsMMARepairAndRebalance returns true if MMA handles
// both rebalancing and repair, with the replicate and lease queues completely
// disabled.
var LoadBasedRebalancingModeIsMMARepairAndRebalance = func(sv *settings.Values) bool {
	return LoadBasedRebalancingMode.Get(sv) == LBRebalancingMultiMetricRepairAndRebalance
}
```

## 2. Disabling the Replicate Queue

When MMA repair mode is active, the replicate queue completely yields. Both `shouldQueue`
and `process` short-circuit:

```bash
sed -n '685,691p' pkg/kv/kvserver/replicate_queue.go
```

```output
func (rq *replicateQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, confReader spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	if kvserverbase.LoadBasedRebalancingModeIsMMARepairAndRebalance(
		&rq.store.cfg.Settings.SV) {
		return false, 0
	}
```

Similarly, `ForceReplicationScanAndProcess` (used in tests to drive deterministic
replica placement) routes through MMA instead of the replicate queue:

```bash
sed -n '58,68p' pkg/kv/kvserver/queue_helpers_testutil.go
```

```output
// ForceReplicationScanAndProcess iterates over all ranges and
// enqueues any that need to be replicated. When MMA repair-and-rebalance
// mode is active, it delegates to the MMA rebalancer instead of the
// replicate queue (whose shouldQueue/process are no-ops in that mode).
func (s *Store) ForceReplicationScanAndProcess() error {
	if kvserverbase.LoadBasedRebalancingModeIsMMARepairAndRebalance(&s.cfg.Settings.SV) {
		s.mmaStoreRebalancer.rebalanceUntilStable(context.TODO())
		return nil
	}
	return forceScanAndProcess(context.TODO(), s, s.replicateQueue.baseQueue)
}
```

## 3. The `IncludeRepair` Flag and the `ComputeChanges` Entry Point

The `ChangeOptions` struct gets a new `IncludeRepair` field:

```bash
sed -n '16,32p' pkg/kv/kvserver/allocator/mmaprototype/allocator.go
```

```output
type ChangeOptions struct {
	LocalStoreID roachpb.StoreID
	// DryRun tells the allocator not to update its internal state with the
	// proposed pending changes.
	DryRun bool
	// PeriodicCall is only used for observability, for deciding when to update
	// gauges and log a summary of what happened in the rebalancing pass. We
	// expect that when this is true, more significant changes may be produced,
	// and the subsequent calls in a tight loop will have diminishing returns
	// (due to pending changes from a load perspective), and so choose not to
	// update gauges and logs in those subsequent calls.
	PeriodicCall bool
	// IncludeRepair tells ComputeChanges to run repair() before
	// rebalanceStores(). Repair is higher priority; its pending changes
	// prevent the rebalancer from touching the same ranges.
	IncludeRepair bool
}
```

`ComputeChanges` is the main entry point. When `IncludeRepair` is true, it calls
`repair()` before `rebalanceStores()`. The key insight: repair changes are recorded
as pending, which prevents the subsequent rebalancer from touching those same ranges.

```bash
sed -n '342,365p' pkg/kv/kvserver/allocator/mmaprototype/allocator_state.go
```

```output
// ComputeChanges implements the Allocator interface.
func (a *allocatorState) ComputeChanges(
	ctx context.Context, msg *StoreLeaseholderMsg, opts ChangeOptions,
) []ExternalRangeChange {
	a.mu.Lock()
	defer a.mu.Unlock()
	if msg.StoreID != opts.LocalStoreID {
		panic(fmt.Sprintf("ComputeChanges: expected StoreID %d, got %d", opts.LocalStoreID, msg.StoreID))
	}
	if opts.DryRun {
		panic(errors.AssertionFailedf("unsupported dry-run mode"))
	}
	rangeOperationMetrics := a.getCounterMetricsForLocalStoreLocked(ctx, opts.LocalStoreID)
	a.cs.processStoreLeaseholderMsg(ctx, msg, rangeOperationMetrics)
	var passObs *rebalancingPassMetricsAndLogger
	if opts.PeriodicCall {
		passObs = a.preparePassMetricsAndLoggerLocked(ctx, opts.LocalStoreID)
	}
	re := newRebalanceEnv(a.cs, a.rand, a.diversityScoringMemo, a.cs.ts.Now(), passObs)
	if opts.IncludeRepair {
		re.repair(ctx, opts.LocalStoreID)
	}
	return re.rebalanceStores(ctx, opts.LocalStoreID)
}
```

The caller -- `mmaStoreRebalancer.rebalance()` -- sets `IncludeRepair` based on the
mode setting:

```bash
sed -n '149,153p' pkg/kv/kvserver/mma_store_rebalancer.go
```

```output
	changes := m.mma.ComputeChanges(ctx, &storeLeaseholderMsg, mmaprototype.ChangeOptions{
		LocalStoreID:  m.store.StoreID(),
		PeriodicCall:  periodicCall,
		IncludeRepair: kvserverbase.LoadBasedRebalancingModeIsMMARepairAndRebalance(&m.st.SV),
	})
```

## 4. The RepairAction Enum and Eagerly-Computed Repair State

The heart of the repair pipeline is the `RepairAction` enum. Each range tracks which
repair action (if any) it needs. The enum values are ordered by **priority** -- lower
values are higher priority. This ordering determines both which action to fix first
when a range has multiple problems, and which ranges get attention first when the
number of changes per pass is limited.

```bash
sed -n '20,87p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// RepairAction represents a repair action needed for a range. The enum values
// are ordered by priority: lower values are higher priority. This ordering
// determines both which action to fix first when a range has multiple problems,
// and which ranges' repairs take precedence when the number of changes per pass
// is limited.
//
// The zero value is intentionally invalid (not a valid action), so that
// uninitialized fields are caught rather than silently treated as the highest
// priority action.
type RepairAction int

const (
	// FinalizeAtomicReplicationChange indicates the range is in a joint
	// configuration (has VOTER_INCOMING, VOTER_DEMOTING_LEARNER, or
	// VOTER_DEMOTING_NON_VOTER replicas) that needs to be finalized.
	FinalizeAtomicReplicationChange RepairAction = iota + 1
	// RemoveLearner indicates the range has a stuck LEARNER replica that
	// should be removed.
	RemoveLearner

	// AddVoter indicates the range has fewer voters than the config requires.
	AddVoter
	// ReplaceDeadVoter indicates a voter is on a dead store and should be
	// replaced (voter count matches config).
	ReplaceDeadVoter
	// ReplaceDecommissioningVoter indicates a voter is on a decommissioning
	// (or shedding) store and should be replaced (voter count matches config).
	ReplaceDecommissioningVoter
	// RemoveVoter indicates the range has more voters than the config
	// requires. Candidate selection prefers dead > decommissioning > healthy.
	RemoveVoter

	// AddNonVoter indicates the range has fewer non-voters than the config
	// requires.
	AddNonVoter
	// ReplaceDeadNonVoter indicates a non-voter is on a dead store and should
	// be replaced (non-voter count matches config).
	ReplaceDeadNonVoter
	// ReplaceDecommissioningNonVoter indicates a non-voter is on a
	// decommissioning (or shedding) store and should be replaced (non-voter
	// count matches config).
	ReplaceDecommissioningNonVoter
	// RemoveNonVoter indicates the range has more non-voters than the config
	// requires. Candidate selection prefers dead > decommissioning > healthy.
	RemoveNonVoter

	// SwapVoterForConstraints indicates the voter count is correct but a voter
	// is placed on a store that doesn't satisfy a voter constraint. A swap
	// (remove + add) is needed.
	SwapVoterForConstraints
	// SwapNonVoterForConstraints indicates the non-voter count is correct but
	// a non-voter (or the overall set) doesn't satisfy a placement constraint.
	// A swap is needed.
	SwapNonVoterForConstraints

	// RepairSkipped indicates that repair is not being attempted for this
	// range, either because we lack the information to determine what's needed
	// (e.g. nil config, failed constraint analysis) or because we know repair
	// is impossible right now (e.g. loss of quorum).
	RepairSkipped

	// RepairPending indicates the range needs repair but already has pending
	// changes in flight. No further repair is attempted until those complete.
	RepairPending

	// NoRepairNeeded indicates the range is healthy and conformant.
	NoRepairNeeded
)
```

Each `rangeState` eagerly maintains its `repairAction`. The `clusterState` maintains a
**reverse index** (`repairRanges`) mapping `RepairAction -> set of RangeIDs`. This
avoids scanning all ranges during repair -- the index is incrementally maintained:

```bash
sed -n '1313,1316p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state.go
```

```output
	// repairRanges indexes ranges by their RepairAction. Ranges with
	// NoRepairNeeded or RepairPending are NOT stored. This allows repair()
	// to iterate ranges needing repair without scanning all ranges.
	repairRanges map[RepairAction]map[roachpb.RangeID]struct{}
```

The index is updated by `updateRepairAction()`, which recomputes the action and
adjusts the index whenever something changes (replica state, config, pending changes,
store status):

```bash
sed -n '259,290p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// updateRepairAction recomputes the repair action for a range and updates the
// repairRanges index. This should be called at every trigger point where the
// range's repair status may have changed: after processRangeMsg, store status
// changes, pending change add/undo/enact, and range GC.
func (cs *clusterState) updateRepairAction(
	ctx context.Context, rangeID roachpb.RangeID, rs *rangeState,
) {
	oldAction := rs.repairAction
	newAction := cs.computeRepairAction(ctx, rs)
	if oldAction == newAction {
		return
	}
	// Remove from old bucket.
	if oldAction != NoRepairNeeded && oldAction != RepairPending && oldAction != 0 {
		if m, ok := cs.repairRanges[oldAction]; ok {
			delete(m, rangeID)
			if len(m) == 0 {
				delete(cs.repairRanges, oldAction)
			}
		}
	}
	// Add to new bucket.
	if newAction != NoRepairNeeded && newAction != RepairPending {
		m, ok := cs.repairRanges[newAction]
		if !ok {
			m = map[roachpb.RangeID]struct{}{}
			cs.repairRanges[newAction] = m
		}
		m[rangeID] = struct{}{}
	}
	rs.repairAction = newAction
}
```

## 5. `computeRepairAction`: The Decision Function

`computeRepairAction` is the pure decision function that determines what repair (if
any) a range needs. It follows a strict priority-ordered evaluation:

1. **Nil config** -> `RepairSkipped` (can't reason about the range)
2. **Pending changes** -> `RepairPending` (already being worked on)
3. **Joint config** -> `FinalizeAtomicReplicationChange`
4. **Stuck learners** -> `RemoveLearner`
5. **Quorum check** -> `RepairSkipped` if lost (can't make changes anyway)
6. **Voter count** -> `AddVoter` / `RemoveVoter` / `ReplaceDeadVoter` / `ReplaceDecommissioningVoter`
7. **Non-voter count** -> Same pattern for non-voters
8. **Constraint satisfaction** -> `SwapVoterForConstraints` / `SwapNonVoterForConstraints`
9. **All good** -> `NoRepairNeeded`

```bash
sed -n '121,257p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// computeRepairAction determines the highest-priority repair action needed for
// the given range. It examines replicas, store statuses, and constraint
// satisfaction. Returns NoRepairNeeded if the range is healthy and conformant,
// or RepairSkipped if we can't determine what's needed or can't act (e.g. loss
// of quorum, nil config).
func (cs *clusterState) computeRepairAction(ctx context.Context, rs *rangeState) RepairAction {
	// Step 1: Invalid config — skip.
	if rs.conf == nil {
		return RepairSkipped
	}
	// Step 2: Pending changes — being worked on already.
	if len(rs.pendingChanges) > 0 {
		return RepairPending
	}

	// Step 3: Scan replicas and classify.
	var (
		numVoters      int
		numNonVoters   int
		numLearners    int
		hasJointConfig bool

		deadVoters     int
		decomVoters    int
		deadNonVoters  int
		decomNonVoters int
	)
	for _, repl := range rs.replicas {
		typ := repl.ReplicaType.ReplicaType
		switch {
		case typ == roachpb.VOTER_INCOMING ||
			typ == roachpb.VOTER_DEMOTING_LEARNER ||
			typ == roachpb.VOTER_DEMOTING_NON_VOTER:
			hasJointConfig = true
			// VOTER_INCOMING counts as a voter for quorum purposes.
			// VOTER_DEMOTING_* are leaving voters but still count until
			// finalization. We count them here but the joint config check (step 4)
			// takes priority.
			if typ == roachpb.VOTER_INCOMING {
				numVoters++
			} else if typ == roachpb.VOTER_DEMOTING_NON_VOTER {
				numNonVoters++
			}
		case typ == roachpb.LEARNER:
			numLearners++
		case isVoter(typ):
			numVoters++
			ss := cs.stores[repl.StoreID]
			if ss != nil {
				if ss.status.Health == HealthDead {
					deadVoters++
				} else if ss.status.Disposition.Replica == ReplicaDispositionShedding {
					decomVoters++
				}
			}
		case isNonVoter(typ):
			numNonVoters++
			ss := cs.stores[repl.StoreID]
			if ss != nil {
				if ss.status.Health == HealthDead {
					deadNonVoters++
				} else if ss.status.Disposition.Replica == ReplicaDispositionShedding {
					decomNonVoters++
				}
			}
		}
	}

	// Step 4: Joint config / learner checks (highest priority).
	if hasJointConfig {
		return FinalizeAtomicReplicationChange
	}
	if numLearners > 0 {
		return RemoveLearner
	}

	// Step 5: Quorum check — must happen before count-based voter checks.
	// Decommissioning voters are still alive for quorum purposes.
	aliveVoters := numVoters - deadVoters
	quorum := numVoters/2 + 1
	if aliveVoters < quorum {
		return RepairSkipped
	}

	// Step 6: Voter count checks.
	desiredVoters := int(rs.conf.numVoters)
	if numVoters < desiredVoters {
		return AddVoter
	}
	if numVoters > desiredVoters {
		// Over-replicated: RemoveVoter with candidate selection preferring
		// dead > decommissioning > healthy.
		return RemoveVoter
	}
	// Voter count matches config — check for dead/decommissioning replicas
	// that need replacement.
	if deadVoters > 0 {
		return ReplaceDeadVoter
	}
	if decomVoters > 0 {
		return ReplaceDecommissioningVoter
	}

	// Step 7: Non-voter count checks.
	desiredNonVoters := int(rs.conf.numReplicas) - desiredVoters
	if numNonVoters < desiredNonVoters {
		return AddNonVoter
	}
	if numNonVoters > desiredNonVoters {
		// Over-replicated: RemoveNonVoter with candidate selection preferring
		// dead > decommissioning > healthy.
		return RemoveNonVoter
	}
	// Non-voter count matches config — check for dead/decommissioning
	// replicas that need replacement.
	if deadNonVoters > 0 {
		return ReplaceDeadNonVoter
	}
	if decomNonVoters > 0 {
		return ReplaceDecommissioningNonVoter
	}

	// Step 8: Constraint swap checks — counts are correct, check placement.
	cs.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		return RepairSkipped
	}
	if under, _, _ := rs.constraints.voterConstraintCount(); under > 0 {
		return SwapVoterForConstraints
	}
	if under, _, _ := rs.constraints.constraintCount(); under > 0 {
		return SwapNonVoterForConstraints
	}

	// Step 9: Everything is fine.
	return NoRepairNeeded
}
```

## 6. The `repair()` Method: Orchestrating Repairs

`repair()` is the top-level method called from `ComputeChanges`. It iterates over
repair actions in priority order (leveraging the `repairRanges` index), dispatching
to action-specific handlers. Only ranges where the local store is leaseholder are
considered, matching the replicate queue's behavior.

```bash
sed -n '305,370p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// repair examines the ranges on the local store and proposes changes to bring
// them into compliance with their span configs. For example, it adds replicas
// when under-replicated, removes replicas when over-replicated, replaces dead
// or decommissioning replicas, and finalizes atomic replication changes.
//
// Only ranges where localStoreID is the leaseholder are considered for repair,
// matching how the replicate queue works: only the leaseholder proposes changes.
func (re *rebalanceEnv) repair(
	ctx context.Context, localStoreID roachpb.StoreID,
) []ExternalRangeChange {
	re.mmaid++
	ctx = logtags.AddTag(ctx, "mmaid", re.mmaid)

	// Iterate repair actions in priority order (lower enum = higher priority).
	for action := FinalizeAtomicReplicationChange; action < NoRepairNeeded; action++ {
		ranges := re.repairRanges[action]
		if len(ranges) == 0 {
			continue
		}
		// Sort range IDs for deterministic iteration order.
		ids := make([]roachpb.RangeID, 0, len(ranges))
		for rid := range ranges {
			ids = append(ids, rid)
		}
		slices.Sort(ids)

		for _, rangeID := range ids {
			rs := re.ranges[rangeID]
			// Only repair ranges where localStoreID is the leaseholder.
			if !isLeaseholderOnStore(rs, localStoreID) {
				continue
			}

			switch action {
			case ReplaceDeadVoter:
				re.repairReplaceDeadVoter(ctx, localStoreID, rangeID, rs)
			case ReplaceDecommissioningVoter:
				re.repairReplaceDecommissioningVoter(ctx, localStoreID, rangeID, rs)
			case AddVoter:
				re.repairAddVoter(ctx, localStoreID, rangeID, rs)
			case RemoveVoter:
				re.repairRemoveVoter(ctx, localStoreID, rangeID, rs)
			case AddNonVoter:
				re.repairAddNonVoter(ctx, localStoreID, rangeID, rs)
			case ReplaceDeadNonVoter:
				re.repairReplaceDeadNonVoter(ctx, localStoreID, rangeID, rs)
			case ReplaceDecommissioningNonVoter:
				re.repairReplaceDecommissioningNonVoter(ctx, localStoreID, rangeID, rs)
			case RemoveNonVoter:
				re.repairRemoveNonVoter(ctx, localStoreID, rangeID, rs)
			case RemoveLearner:
				re.repairRemoveLearner(ctx, localStoreID, rangeID, rs)
			case SwapVoterForConstraints:
				re.repairSwapVoterForConstraints(ctx, localStoreID, rangeID, rs)
			case SwapNonVoterForConstraints:
				re.repairSwapNonVoterForConstraints(ctx, localStoreID, rangeID, rs)
			case FinalizeAtomicReplicationChange:
				re.repairFinalizeAtomicReplicationChange(ctx, localStoreID, rangeID, rs)
			default:
				log.KvDistribution.Infof(ctx,
					"repair action %s for r%d not yet implemented", action, rangeID)
			}
		}
	}
	return re.changes
}
```

## 7. Detailed Walkthrough: `repairAddVoter` (Representative Example)

All repair actions follow the same pattern. Let's trace `repairAddVoter` as a
representative example. This is the action that fires when a range has fewer voters
than its span config requires (upreplication).

The flow is:

1. **Ensure constraint analysis** -- lazily compute `rangeAnalyzedConstraints`
2. **Try promotion first** -- if there's already a non-voter in the right place, promote it
3. **Find a new store** -- use constraint matching to get candidate stores
4. **Filter candidates** -- remove stores that are dead, already have a replica, on the same node, etc.
5. **Pick by diversity** -- select the most diverse store (using locality tiers)
6. **Create pending change** -- build a `PendingRangeChange` and pre-check it
7. **Enact** -- record as pending and append to the changes list

```bash
sed -n '465,535p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// repairAddVoter attempts to add a voter to an under-replicated range.
// It follows the decision tree from constraint.go: first try to promote a
// non-voter, then find a new store to add a voter.
func (re *rebalanceEnv) repairAddVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: constraint analysis failed", rangeID)
		return
	}

	// Step 1: Try to promote a non-voter to voter.
	promoteCands, err := rs.constraints.candidatesToConvertFromNonVoterToVoter()
	if err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: %v", rangeID, err)
		return
	}
	if len(promoteCands) > 0 {
		re.promoteNonVoterToVoter(ctx, localStoreID, rangeID, rs, promoteCands)
		return
	}

	// Step 2: Find a new store to add a voter.
	constrDisj, err := rs.constraints.constraintsForAddingVoter()
	if err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: %v", rangeID, err)
		return
	}

	// Get candidate stores satisfying constraints. For nil constraints (no
	// constraints configured), constrainStoresForExpr returns all stores.
	var candidateStores storeSet
	re.constraintMatcher.constrainStoresForExpr(constrDisj, &candidateStores)

	validCandidates := re.filterAddCandidates(ctx, rs, candidateStores, 0)
	if len(validCandidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: no valid target stores", rangeID)
		return
	}

	// Pick the target with the best voter diversity score.
	bestStoreID := re.pickStoreByDiversity(
		validCandidates, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)

	// Create the pending change.
	targetSS := re.stores[bestStoreID]
	addTarget := roachpb.ReplicationTarget{
		NodeID:  targetSS.NodeID,
		StoreID: bestStoreID,
	}
	addIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
	}
	addChange := MakeAddReplicaChange(rangeID, rs.load, addIDAndType, addTarget)
	rangeChange := MakePendingRangeChange(rangeID, []ReplicaChange{addChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: pre-check failed: %v", rangeID, err)
		return
	}
	re.enactRepair(ctx, localStoreID, rangeChange)
	log.KvDistribution.Infof(ctx,
		"result(success): AddVoter repair for r%v, adding voter on s%v",
		rangeID, bestStoreID)
}
```

### Key helpers used by `repairAddVoter` (and all other repair actions)

**`filterAddCandidates`** removes stores that are dead/draining/IO-overloaded, already
hosting a replica, or on the same node as an existing replica:

```bash
sed -n '426,463p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// filterAddCandidates filters candidateStores down to stores that are ready
// (not dead/draining/IO-overloaded) and not already hosting a replica for the
// range at the node level. excludeStoreID, if non-zero, is excluded from the
// existing-replica set (used when a replica on that store is being
// concurrently removed as part of the same change).
func (re *rebalanceEnv) filterAddCandidates(
	ctx context.Context, rs *rangeState, candidateStores storeSet, excludeStoreID roachpb.StoreID,
) storeSet {
	var existingReplicas storeSet
	existingNodes := make(map[roachpb.NodeID]struct{})
	for _, repl := range rs.replicas {
		if repl.StoreID == excludeStoreID {
			continue
		}
		existingReplicas.insert(repl.StoreID)
		ss := re.stores[repl.StoreID]
		if ss != nil {
			existingNodes[ss.NodeID] = struct{}{}
		}
	}
	candidateStores = retainReadyReplicaTargetStoresOnly(
		ctx, candidateStores, re.stores, existingReplicas)
	var valid storeSet
	for _, storeID := range candidateStores {
		if existingReplicas.contains(storeID) {
			continue
		}
		ss := re.stores[storeID]
		if ss == nil {
			continue
		}
		if _, ok := existingNodes[ss.NodeID]; ok {
			continue
		}
		valid = append(valid, storeID)
	}
	return valid
}
```

**`pickStoreByDiversity`** selects the most diverse store using locality tiers, with
reservoir sampling for tie-breaking:

```bash
sed -n '537,581p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// diversityScorer computes a diversity score for a candidate store's locality
// relative to existing replica localities. Both getScoreChangeForNewReplica
// (for additions) and getScoreChangeForReplicaRemoval (for removals) have this
// signature. In both cases, higher scores are better: for additions, a high
// score means the candidate is diverse; for removals, a high (least negative)
// score means the candidate is the most redundant.
type diversityScorer func(erl *existingReplicaLocalities, lt localityTiers) float64

// pickStoreByDiversity selects the store from candidates that maximizes the
// given diversity scorer. When multiple candidates are tied for the best score,
// one is chosen uniformly at random via reservoir sampling to avoid
// systematically favoring any particular store. Returns 0 if no valid candidate
// is found (e.g. all candidates have nil storeState).
//
// The localityTiers parameter determines which replicas' localities are used
// for diversity scoring: voterLocalityTiers for voter operations,
// replicaLocalityTiers for non-voter operations.
func (re *rebalanceEnv) pickStoreByDiversity(
	candidates []roachpb.StoreID, localityTiers replicasLocalityTiers, scorer diversityScorer,
) roachpb.StoreID {
	localities := re.dsm.getExistingReplicaLocalities(localityTiers)
	bestStoreID := roachpb.StoreID(0)
	bestScore := math.Inf(-1)
	tieCount := 0
	for _, storeID := range candidates {
		ss := re.stores[storeID]
		if ss == nil {
			continue
		}
		score := scorer(localities, ss.localityTiers)
		if score > bestScore && !diversityScoresAlmostEqual(score, bestScore) {
			// Strictly better — reset.
			bestScore = score
			bestStoreID = storeID
			tieCount = 1
		} else if diversityScoresAlmostEqual(score, bestScore) {
			// Tied — reservoir sampling: replace with probability 1/n.
			tieCount++
			if re.rng.Intn(tieCount) == 0 {
				bestStoreID = storeID
			}
		}
	}
	return bestStoreID
}
```

**`enactRepair`** records the change as pending and appends it to the external changes
list. This is the shared "commit" point for all repair actions:

```bash
sed -n '415,424p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// enactRepair records a repair change as pending and appends it to the changes
// list for external delivery. The caller is responsible for logging the success
// message.
func (re *rebalanceEnv) enactRepair(
	ctx context.Context, localStoreID roachpb.StoreID, rangeChange PendingRangeChange,
) {
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
}
```

## 8. The Replace Pattern: `replaceReplicaSpec` and `repairReplaceReplica`

Four repair actions share the same structure: replace a dead or decommissioning
voter/non-voter. Rather than duplicating the logic, a `replaceReplicaSpec` struct
parameterizes the shared `repairReplaceReplica` method:

```bash
sed -n '1048,1073p' pkg/kv/kvserver/allocator/mmaprototype/cluster_state_repair.go
```

```output
// replaceReplicaSpec parametrizes the shared repairReplaceReplica method for the
// four replace-replica repair actions (dead/decommissioning × voter/non-voter).
type replaceReplicaSpec struct {
	actionName      redact.SafeString
	replicaKindName redact.SafeString // "voter" or "non-voter", for log messages
	isTargetKind    func(roachpb.ReplicaType) bool
	isTargetStore   func(*storeState) bool
	excludeLH       bool
	localityTiers   func(*rangeAnalyzedConstraints) replicasLocalityTiers
	addReplicaType  roachpb.ReplicaType
}

func (re *rebalanceEnv) repairReplaceDeadVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.repairReplaceReplica(ctx, localStoreID, rangeID, rs, replaceReplicaSpec{
		actionName:      "ReplaceDeadVoter",
		replicaKindName: "voter",
		isTargetKind:    isVoter,
		isTargetStore: func(ss *storeState) bool {
			return ss.status.Health == HealthDead
		},
		excludeLH:      true,
		localityTiers:  func(ca *rangeAnalyzedConstraints) replicasLocalityTiers { return ca.voterLocalityTiers },
		addReplicaType: roachpb.VOTER_FULL,
	})
```

## 9. Change Application: `mmaStoreRebalancer.applyChange`

Repair changes flow through the same application path as rebalancing changes.
The key addition is the `IsLeaveJoint()` branch, which routes leave-joint changes
through `maybeLeaveAtomicChangeReplicas` instead of `changeReplicasImpl` (because
the kvserver uses a fundamentally different mechanism to leave joint configs):

```bash
sed -n '165,193p' pkg/kv/kvserver/mma_store_rebalancer.go
```

```output
// applyChange safely applies a single change to the store. It handles the case
// where the replica might not exist and provides proper error handling.
func (m *mmaStoreRebalancer) applyChange(
	ctx context.Context, change mmaprototype.ExternalRangeChange,
) error {
	repl := m.store.GetReplicaIfExists(change.RangeID)
	if repl == nil {
		m.as.MarkChangeAsFailed(ctx, change)
		return errors.Errorf("replica not found for range %d", change.RangeID)
	}
	changeID := m.as.MMAPreApply(ctx, repl.RangeUsageInfo(), change)
	var err error
	switch {
	case change.IsPureTransferLease():
		err = m.applyLeaseTransfer(ctx, repl, change)
	case change.IsLeaveJoint():
		// Leave-joint changes finalize joint configs and must bypass
		// ReplicationChanges() / changeReplicasImpl (see IsLeaveJoint doc).
		_, err = repl.maybeLeaveAtomicChangeReplicas(ctx, repl.Desc())
	case change.IsChangeReplicas():
		err = m.applyReplicaChanges(ctx, repl, change)
	default:
		return errors.Errorf("unknown change type for range %d", change.RangeID)
	}
	// Inform allocator sync that the change has been applied which applies
	// changes to store pool and inform mma.
	m.as.PostApply(ctx, changeID, err == nil /*success*/)
	return err
}
```

## 10. `IsLeaveJoint`: A New Change Category

`IsLeaveJoint()` is a new method on `ExternalRangeChange` that detects when a change
exclusively finalizes a joint configuration. This is needed because the kvserver uses
a fundamentally different code path to leave joint configs (no `ReplicationChanges`
are generated -- it's done via `execChangeReplicasTxn` with nil internal changes):

```bash
sed -n '129,153p' pkg/kv/kvserver/allocator/mmaprototype/range_change.go
```

```output
// IsLeaveJoint returns true if this change exclusively finalizes a joint
// configuration — every change transitions a replica from a joint-config type
// (VOTER_INCOMING, VOTER_DEMOTING_LEARNER, VOTER_DEMOTING_NON_VOTER) to its
// finalized form.
//
// These changes cannot be expressed as kvpb.ReplicationChanges because the
// production code uses a fundamentally different mechanism to leave joint
// configs (execChangeReplicasTxn with nil internal changes, triggered via
// maybeLeaveAtomicChangeReplicas). Callers must detect this case and route
// through the leave-joint path directly.
func (rc *ExternalRangeChange) IsLeaveJoint() bool {
	if len(rc.Changes) == 0 {
		return false
	}
	for _, c := range rc.Changes {
		switch c.Prev.ReplicaType.ReplicaType {
		case roachpb.VOTER_INCOMING, roachpb.VOTER_DEMOTING_LEARNER,
			roachpb.VOTER_DEMOTING_NON_VOTER:
			// Joint-config type being finalized.
		default:
			return false
		}
	}
	return true
}
```

## 11. Repair Metrics

The PR adds `originMMARepair` as a new `ChangeOrigin`, alongside the existing
`OriginExternal` and `originMMARebalance`. This enables separate metric tracking
for repair operations:

```bash
sed -n '21,27p' pkg/kv/kvserver/allocator/mmaprototype/mma_metrics.go
```

```output
type ChangeOrigin uint8

const (
	OriginExternal ChangeOrigin = iota
	originMMARebalance
	originMMARepair
)
```

This produces metrics like `mma.change.repair.replica.success` and
`mma.change.repair.replica.failure`:

```bash
sed -n '199,215p' pkg/kv/kvserver/allocator/mmaprototype/mma_metrics.go
```

```output
	metaRepairReplicaChangeSuccess = metric.Metadata{
		Name:        "mma.change.repair.replica.success",
		Help:        "Number of successful MMA-initiated repair operations that change replicas",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "repair", metric.LabelType, "replica", metric.LabelResult, "success"),
	}
	metaRepairReplicaChangeFailure = metric.Metadata{
		Name:        "mma.change.repair.replica.failure",
		Help:        "Number of failed MMA-initiated repair operations that change replicas",
		Measurement: "Range Change",
		Unit:        metric.Unit_COUNT,
		LabeledName: "mma.change",
		StaticLabels: metric.MakeLabelPairs(
			metric.LabelOrigin, "repair", metric.LabelType, "replica", metric.LabelResult, "failure"),
```

## 12. The `rebalanceUntilStable` Loop

Both the periodic timer and `ForceReplicationScanAndProcess` use
`rebalanceUntilStable`, which keeps calling `rebalance()` (which calls
`ComputeChanges` + applies changes) until no more changes are produced:

```bash
sed -n '120,163p' pkg/kv/kvserver/mma_store_rebalancer.go
```

```output
// rebalanceUntilStable keeps rebalancing until no changes are computed. This is
// used both by the periodic run loop and by ForceReplicationScanAndProcess to
// drive MMA repair deterministically in tests.
func (m *mmaStoreRebalancer) rebalanceUntilStable(ctx context.Context) {
	periodicCall := true
	for {
		if !m.rebalance(ctx, periodicCall) {
			return
		}
		periodicCall = false
	}
}

// rebalance computes the changes using the mma allocator and applies the
// changes to the store. It returns true if any changes were computed as a
// signal to the caller that it should continue calling rebalance. Note that
// rebalance may return true if errors happen in the process and fail to apply
// the changes successfully.
func (m *mmaStoreRebalancer) rebalance(ctx context.Context, periodicCall bool) bool {
	opts := allocatorimpl.MakeDiskCapacityOptions(&m.st.SV)
	m.mma.SetDiskUtilThresholds(opts.RebalanceToThreshold, opts.ShedAndBlockAllThreshold)
	m.mma.UpdateStoresStatuses(ctx, m.as.GetMMAStoreStatuses())
	knownStoresByMMA := m.mma.KnownStores()
	storeLeaseholderMsg, numIgnoredRanges := m.store.MakeStoreLeaseholderMsg(ctx, knownStoresByMMA)
	if numIgnoredRanges > 0 {
		log.KvDistribution.Infof(ctx, "mma rebalancer: ignored %d ranges since the allocator does not know all stores",
			numIgnoredRanges)
	}

	changes := m.mma.ComputeChanges(ctx, &storeLeaseholderMsg, mmaprototype.ChangeOptions{
		LocalStoreID:  m.store.StoreID(),
		PeriodicCall:  periodicCall,
		IncludeRepair: kvserverbase.LoadBasedRebalancingModeIsMMARepairAndRebalance(&m.st.SV),
	})

	// TODO(wenyihu6): add allocator sync and post apply here
	for _, change := range changes {
		if err := m.applyChange(ctx, change); err != nil {
			log.KvDistribution.VInfof(ctx, 1, "failed to apply change for range %d: %v", change.RangeID, err)
		}
	}

	return len(changes) > 0
}
```

## 13. End-to-End Test: `TestMMAUpreplication`

The PR includes an integration test that verifies the entire pipeline works. It:
1. Starts a 3-node cluster with `ReplicationAuto` (so system ranges get upreplicated by the old path first)
2. Enables `LBRebalancingMultiMetricRepairAndRebalance` on all servers
3. Creates a scratch range (starts with 1 replica)
4. Calls `ForceReplicationScanAndProcess` on each store
5. Verifies the scratch range ends up with 3 voters

```bash
sed -n '27,104p' pkg/kv/kvserver/client_mma_test.go
```

```output
// TestMMAUpreplication verifies that MMA correctly upreplicates a scratch range
// from 1 to 3 voters when operating in MultiMetricRepairAndRebalance mode,
// where the replicate and lease queues are fully disabled.
func TestMMAUpreplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Pre-configure the rebalance interval so that the MMA rebalancer ticks
	// frequently from the start. This must be set before cluster start because
	// the MMA rebalancer creates its ticker in run() and only re-reads the
	// interval on subsequent ticks; it cannot react to interval changes between
	// ticks.
	//
	// TODO(tbg): the MMA rebalancer should register a settings change callback
	// to reset its ticker when the interval changes.
	st := cluster.MakeTestingClusterSettings()
	allocator.LoadBasedRebalanceInterval.Override(ctx, &st.SV, time.Second)

	// Use ReplicationAuto so that system ranges are upreplicated via the
	// replicate queue before we enable MMA. WaitForFullReplication (called
	// by StartTestCluster) drives this via ForceReplicationScanAndProcess.
	// Once MMA is enabled, ForceReplicationScanAndProcess delegates to the
	// MMA rebalancer instead of the replicate queue.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)

	// The scratch range starts with a single replica. Enable MMA
	// repair-and-rebalance mode on each server so it upreplicates.
	//
	// NB: TestCluster clones the Settings object per server (even when using
	// ServerArgsPerNode), so we must override on each server's own
	// ClusterSettings() rather than the original `st`.
	//
	// TODO(tbg): consider whether TestCluster should avoid this implicit
	// cloning, or at least document it more prominently.
	for _, server := range tc.Servers {
		serverSt := server.ClusterSettings()
		kvserverbase.LoadBasedRebalancingMode.Override(
			ctx, &serverSt.SV, kvserverbase.LBRebalancingMultiMetricRepairAndRebalance,
		)
	}

	// Wait for the scratch range to upreplicate to 3 voters. We explicitly
	// call ForceReplicationScanAndProcess on each store to deterministically
	// drive MMA repair (instead of waiting for the background timer).
	testutils.SucceedsSoon(t, func() error {
		for _, server := range tc.Servers {
			if err := server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				return s.ForceReplicationScanAndProcess()
			}); err != nil {
				t.Fatal(err)
			}
		}
		desc := tc.LookupRangeOrFatal(t, k)
		voters := desc.Replicas().VoterDescriptors()
		if len(voters) != 3 {
			return errors.Errorf("expected 3 voters, got %d: %s", len(voters), &desc)
		}
		return nil
	})

	desc := tc.LookupRangeOrFatal(t, k)
	require.Len(t, desc.Replicas().VoterDescriptors(), 3)
}
```

## 14. Summary: Data Flow Diagram

```
    mmaStoreRebalancer.run() (periodic timer or ForceReplicationScanAndProcess)
        |
        v
    rebalanceUntilStable()
        |
        v
    rebalance()
        |-- UpdateStoresStatuses()  (health/disposition from node liveness)
        |-- MakeStoreLeaseholderMsg() (range info from local store)
        |
        v
    ComputeChanges(IncludeRepair: true)
        |
        |-- processStoreLeaseholderMsg() -> updates rangeState, triggers updateRepairAction()
        |
        |-- repair(localStoreID)
        |   |-- iterate repairRanges in priority order
        |   |-- for each range needing repair where local store is leaseholder:
        |   |   |-- computeRepairAction() already determined what to do
        |   |   |-- dispatch to repairAddVoter / repairRemoveVoter / repairReplaceDeadVoter / ...
        |   |   |   |-- ensureAnalyzedConstraints()
        |   |   |   |-- constraintMatcher.constrainStoresForExpr() -> candidate stores
        |   |   |   |-- filterAddCandidates() -> valid candidates
        |   |   |   |-- pickStoreByDiversity() -> best target
        |   |   |   |-- MakePendingRangeChange() + preCheckOnApplyReplicaChanges()
        |   |   |   |-- enactRepair() -> adds pending change + appends to changes[]
        |   |
        |   v
        |   returns []ExternalRangeChange (repair changes)
        |
        |-- rebalanceStores(localStoreID) -> appends more ExternalRangeChange (rebalance changes)
        |   (ranges with repair pending changes are untouched here)
        |
        v
    []ExternalRangeChange returned to rebalance()
        |
        v
    for each change: applyChange()
        |-- IsPureTransferLease() -> AdminTransferLease()
        |-- IsLeaveJoint()        -> maybeLeaveAtomicChangeReplicas()  [NEW]
        |-- IsChangeReplicas()    -> changeReplicasImpl()
        |
        v
    PostApply() -> informs MMA of success/failure
```

## 15. Key Design Decisions

1. **Repair before rebalance**: `repair()` runs first in `ComputeChanges`. Its pending
   changes prevent `rebalanceStores()` from touching the same ranges. This ensures
   safety-critical operations (upreplication, dead node replacement) take priority
   over load balancing.

2. **Eagerly-maintained repair index**: Rather than scanning all ranges every pass,
   `repairRanges` is incrementally maintained as `updateRepairAction()` is called at
   every state mutation point. This makes the `repair()` pass cheap -- it only visits
   ranges that actually need work.

3. **Priority ordering**: The `RepairAction` enum is ordered so that `repair()` can
   simply iterate from lowest to highest enum value. Joint config finalization comes
   first (preventing cascading issues), then voter operations (availability), then
   non-voter operations (read serving), then constraint satisfaction (policy).

4. **Shared `rebalanceEnv`**: Repair reuses the existing `rebalanceEnv` struct, which
   provides access to `clusterState`, the constraint matcher, diversity scoring, and
   the RNG. This avoids creating a separate "repair environment" and shares the same
   pending change tracking.

5. **`replaceReplicaSpec`**: Four structurally-identical replace operations
   (dead/decommissioning x voter/non-voter) are parameterized rather than duplicated.
   Each caller fills out the spec and delegates to `repairReplaceReplica`.

6. **`IsLeaveJoint` routing**: MMA models leave-joint as regular type changes, but the
   kvserver uses a completely separate mechanism. The `IsLeaveJoint()` predicate detects
   this case so `applyChange` can route through `maybeLeaveAtomicChangeReplicas`.

