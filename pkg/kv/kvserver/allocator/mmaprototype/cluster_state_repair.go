// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

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

	// NoRepairNeeded indicates the range is healthy and conformant.
	NoRepairNeeded
)

var repairActionNames = [...]string{
	FinalizeAtomicReplicationChange: "FinalizeAtomicReplicationChange",
	RemoveLearner:                   "RemoveLearner",
	AddVoter:                        "AddVoter",
	ReplaceDeadVoter:                "ReplaceDeadVoter",
	ReplaceDecommissioningVoter:     "ReplaceDecommissioningVoter",
	RemoveVoter:                     "RemoveVoter",
	AddNonVoter:                     "AddNonVoter",
	ReplaceDeadNonVoter:             "ReplaceDeadNonVoter",
	ReplaceDecommissioningNonVoter:  "ReplaceDecommissioningNonVoter",
	RemoveNonVoter:                  "RemoveNonVoter",
	SwapVoterForConstraints:         "SwapVoterForConstraints",
	SwapNonVoterForConstraints:      "SwapNonVoterForConstraints",
	RepairSkipped:                   "RepairSkipped",
	NoRepairNeeded:                  "NoRepairNeeded",
}

// String implements fmt.Stringer.
func (a RepairAction) String() string {
	return redact.StringWithoutMarkers(a)
}

// SafeFormat implements redact.SafeFormatter.
func (a RepairAction) SafeFormat(w redact.SafePrinter, _ rune) {
	if int(a) >= 0 && int(a) < len(repairActionNames) && repairActionNames[a] != "" {
		w.SafeString(redact.SafeString(repairActionNames[a]))
		return
	}
	w.Printf("RepairAction(%d)", redact.SafeInt(a))
}

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
		return NoRepairNeeded
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
	if oldAction != NoRepairNeeded && oldAction != 0 {
		if m, ok := cs.repairRanges[oldAction]; ok {
			delete(m, rangeID)
			if len(m) == 0 {
				delete(cs.repairRanges, oldAction)
			}
		}
	}
	// Add to new bucket.
	if newAction != NoRepairNeeded {
		m, ok := cs.repairRanges[newAction]
		if !ok {
			m = map[roachpb.RangeID]struct{}{}
			cs.repairRanges[newAction] = m
		}
		m[rangeID] = struct{}{}
	}
	rs.repairAction = newAction
}

// removeFromRepairRanges removes a range from the repairRanges index. This
// should be called before deleting a range from cs.ranges.
func (cs *clusterState) removeFromRepairRanges(rangeID roachpb.RangeID, rs *rangeState) {
	if rs.repairAction != NoRepairNeeded && rs.repairAction != 0 {
		if m, ok := cs.repairRanges[rs.repairAction]; ok {
			delete(m, rangeID)
			if len(m) == 0 {
				delete(cs.repairRanges, rs.repairAction)
			}
		}
	}
}

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
			case AddVoter:
				re.repairAddVoter(ctx, localStoreID, rangeID, rs)
			default:
				log.KvDistribution.Infof(ctx,
					"repair action %s for r%d not yet implemented", action, rangeID)
			}
		}
	}
	return re.changes
}

// isLeaseholderOnStore returns true if the given store holds the lease for the
// range.
func isLeaseholderOnStore(rs *rangeState, storeID roachpb.StoreID) bool {
	for _, repl := range rs.replicas {
		if repl.StoreID == storeID && repl.IsLeaseholder {
			return true
		}
	}
	return false
}

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

	// Build the set of stores already hosting a replica for this range, and
	// the set of nodes already hosting a replica (for node-level diversity).
	var existingReplicas storeSet
	existingNodes := make(map[roachpb.NodeID]struct{})
	for _, repl := range rs.replicas {
		existingReplicas.insert(repl.StoreID)
		ss := re.stores[repl.StoreID]
		if ss != nil {
			existingNodes[ss.NodeID] = struct{}{}
		}
	}

	// Filter by ReplicaDispositionOK (excludes dead, decommissioning,
	// draining, IO-overloaded stores).
	candidateStores = retainReadyReplicaTargetStoresOnly(
		ctx, candidateStores, re.stores, existingReplicas)

	// Exclude stores already hosting a replica and stores on nodes already
	// hosting a replica.
	var validCandidates storeSet
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
		validCandidates = append(validCandidates, storeID)
	}

	if len(validCandidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: no valid target stores", rangeID)
		return
	}

	// Pick the target with the best voter diversity score.
	bestStoreID := re.pickBestStoreByVoterDiversity(rs, validCandidates)

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
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): AddVoter repair for r%v, adding voter on s%v",
		rangeID, bestStoreID)
}

// pickBestStoreByVoterDiversity selects the store from candidates that
// maximizes voter diversity score. Higher score means better locality
// diversity relative to existing voters. Ties are broken by lower StoreID
// for determinism. Returns 0 if no valid candidate is found (e.g. all
// candidates have nil storeState).
func (re *rebalanceEnv) pickBestStoreByVoterDiversity(
	rs *rangeState, candidates []roachpb.StoreID,
) roachpb.StoreID {
	voterLocalities := re.dsm.getExistingReplicaLocalities(
		rs.constraints.voterLocalityTiers)
	bestStoreID := roachpb.StoreID(0)
	bestScore := math.Inf(-1)
	for _, storeID := range candidates {
		ss := re.stores[storeID]
		if ss == nil {
			continue
		}
		score := voterLocalities.getScoreChangeForNewReplica(ss.localityTiers)
		if score > bestScore ||
			(diversityScoresAlmostEqual(score, bestScore) && storeID < bestStoreID) {
			bestScore = score
			bestStoreID = storeID
		}
	}
	return bestStoreID
}

// promoteNonVoterToVoter promotes the best non-voter candidate to voter,
// choosing by voter diversity score (higher is better), with ties broken by
// lower StoreID for determinism.
func (re *rebalanceEnv) promoteNonVoterToVoter(
	ctx context.Context,
	localStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
	rs *rangeState,
	promoteCands []roachpb.StoreID,
) {
	// Pick the best candidate by voter diversity score.
	bestStoreID := re.pickBestStoreByVoterDiversity(rs, promoteCands)
	if bestStoreID == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: no valid promotion candidates", rangeID)
		return
	}

	// Find the existing replica state for the non-voter being promoted.
	var prevState ReplicaState
	found := false
	for _, repl := range rs.replicas {
		if repl.StoreID == bestStoreID {
			prevState = repl.ReplicaState
			found = true
			break
		}
	}
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: non-voter on s%d not found in replicas",
			rangeID, bestStoreID)
		return
	}

	// Create the type change from NON_VOTER to VOTER_FULL.
	targetSS := re.stores[bestStoreID]
	promoteTarget := roachpb.ReplicationTarget{
		NodeID:  targetSS.NodeID,
		StoreID: bestStoreID,
	}
	nextIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
	}
	typeChange := MakeReplicaTypeChange(
		rangeID, rs.load, prevState, nextIDAndType, promoteTarget)
	rangeChange := MakePendingRangeChange(rangeID, []ReplicaChange{typeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: pre-check failed: %v", rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): AddVoter repair for r%d, promoting non-voter on s%d to voter",
		rangeID, bestStoreID)
}

// Verify interface compliance.
var _ fmt.Stringer = RepairAction(0)
var _ redact.SafeFormatter = RepairAction(0)
