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

	// RepairPending indicates the range needs repair but already has pending
	// changes in flight. No further repair is attempted until those complete.
	RepairPending

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
	RepairPending:                   "RepairPending",
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

// removeFromRepairRanges removes a range from the repairRanges index. This
// should be called before deleting a range from cs.ranges.
func (cs *clusterState) removeFromRepairRanges(rangeID roachpb.RangeID, rs *rangeState) {
	if rs.repairAction != NoRepairNeeded && rs.repairAction != RepairPending && rs.repairAction != 0 {
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

// replicaStateForStore returns the ReplicaState of the replica on the given
// store, and whether it was found.
func replicaStateForStore(rs *rangeState, storeID roachpb.StoreID) (ReplicaState, bool) {
	for _, repl := range rs.replicas {
		if repl.StoreID == storeID {
			return repl.ReplicaState, true
		}
	}
	return ReplicaState{}, false
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
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): AddVoter repair for r%v, adding voter on s%v",
		rangeID, bestStoreID)
}

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
	bestStoreID := re.pickStoreByDiversity(
		promoteCands, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)
	if bestStoreID == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping AddVoter repair for r%d: no valid promotion candidates", rangeID)
		return
	}

	// Find the existing replica state for the non-voter being promoted.
	prevState, found := replicaStateForStore(rs, bestStoreID)
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

// removalPriority returns a priority for removing a replica from the given
// store. Lower values indicate higher priority for removal (i.e. the store
// is a better candidate for removal). The ordering follows the design doc:
// dead > unknown > unhealthy > shedding > refusing > healthy.
func removalPriority(ss *storeState) int {
	if ss == nil {
		return 0 // unknown store, treat like dead
	}
	switch ss.status.Health {
	case HealthDead:
		return 0
	case HealthUnknown:
		return 1
	case HealthUnhealthy:
		return 2
	default: // HealthOK
		if ss.status.Disposition.Replica == ReplicaDispositionShedding {
			return 3
		}
		if ss.status.Disposition.Replica == ReplicaDispositionRefusing {
			return 4
		}
		return 5
	}
}

// repairRemoveVoter removes an over-replicated voter from the range. Candidate
// selection prefers stores that are dead > unknown > unhealthy > shedding >
// refusing > healthy. Within the worst-health bucket, the voter whose removal
// hurts diversity the least (most redundant locality) is chosen.
func (re *rebalanceEnv) repairRemoveVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveVoter repair for r%d: constraint analysis failed", rangeID)
		return
	}

	// Build candidates: all voters except the leaseholder.
	var candidates []roachpb.StoreID
	for _, repl := range rs.replicas {
		if !isVoter(repl.ReplicaType.ReplicaType) {
			continue
		}
		if repl.IsLeaseholder {
			continue
		}
		candidates = append(candidates, repl.StoreID)
	}
	if len(candidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveVoter repair for r%d: no removable voters (all are leaseholders)",
			rangeID)
		return
	}

	// Bucket candidates by removal priority. Take the lowest-priority bucket
	// (worst health = remove first).
	bestPriority := math.MaxInt
	for _, storeID := range candidates {
		p := removalPriority(re.stores[storeID])
		if p < bestPriority {
			bestPriority = p
		}
	}
	var bucket []roachpb.StoreID
	for _, storeID := range candidates {
		if removalPriority(re.stores[storeID]) == bestPriority {
			bucket = append(bucket, storeID)
		}
	}

	// Within the bucket, pick the voter whose removal hurts diversity the
	// least (most redundant). getScoreChangeForReplicaRemoval returns negative
	// values; the least negative (highest) is the most redundant.
	removeStoreID := re.pickStoreByDiversity(
		bucket, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if removeStoreID == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveVoter repair for r%d: diversity picker returned no candidate",
			rangeID)
		return
	}

	// Find the ReplicaState for the chosen voter.
	prevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveVoter repair for r%d: voter on s%d not found in replicas",
			rangeID, removeStoreID)
		return
	}

	// Create the pending change.
	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveVoter repair for r%d: store s%d has no state",
			rangeID, removeStoreID)
		return
	}
	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(rangeID, rs.load, prevState, removeTarget)
	rangeChange := MakePendingRangeChange(rangeID, []ReplicaChange{removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveVoter repair for r%d: pre-check failed: %v", rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): RemoveVoter repair for r%d, removing voter on s%d",
		rangeID, removeStoreID)
}

// repairAddNonVoter attempts to add a non-voter to an under-replicated range.
// It follows the decision tree from constraint.go: first try to demote a voter
// to non-voter, then find a new store to add a non-voter.
func (re *rebalanceEnv) repairAddNonVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddNonVoter repair for r%d: constraint analysis failed",
			rangeID)
		return
	}

	// Step 1: Try to demote a voter to non-voter.
	demoteCands, err := rs.constraints.candidatesToConvertFromVoterToNonVoter()
	if err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddNonVoter repair for r%d: %v", rangeID, err)
		return
	}
	if len(demoteCands) > 0 {
		re.demoteVoterToNonVoter(ctx, localStoreID, rangeID, rs, demoteCands)
		return
	}

	// Step 2: Find a new store to add a non-voter.
	constrDisj, err := rs.constraints.constraintsForAddingNonVoter()
	if err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddNonVoter repair for r%d: %v", rangeID, err)
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
			"skipping AddNonVoter repair for r%d: no valid target stores",
			rangeID)
		return
	}

	// Pick the target with the best replica diversity score.
	bestStoreID := re.pickStoreByDiversity(
		validCandidates, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)

	// Create the pending change.
	targetSS := re.stores[bestStoreID]
	addTarget := roachpb.ReplicationTarget{
		NodeID:  targetSS.NodeID,
		StoreID: bestStoreID,
	}
	addIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.NON_VOTER},
	}
	addChange := MakeAddReplicaChange(rangeID, rs.load, addIDAndType, addTarget)
	rangeChange := MakePendingRangeChange(rangeID, []ReplicaChange{addChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddNonVoter repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): AddNonVoter repair for r%d, adding non-voter on s%d",
		rangeID, bestStoreID)
}

// demoteVoterToNonVoter demotes the best voter candidate to non-voter,
// choosing by replica diversity score (higher is better for removal), with ties
// broken randomly via reservoir sampling. The leaseholder is excluded from
// demotion.
func (re *rebalanceEnv) demoteVoterToNonVoter(
	ctx context.Context,
	localStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
	rs *rangeState,
	demoteCands []roachpb.StoreID,
) {
	// Exclude the leaseholder from demotion candidates.
	var filteredCands []roachpb.StoreID
	for _, storeID := range demoteCands {
		for _, repl := range rs.replicas {
			if repl.StoreID == storeID && repl.IsLeaseholder {
				goto skip
			}
		}
		filteredCands = append(filteredCands, storeID)
	skip:
	}
	if len(filteredCands) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping AddNonVoter repair for r%d: no demotion candidates (all are leaseholders)",
			rangeID)
		return
	}

	// Pick the best candidate — the voter whose removal from the voter set
	// hurts voter diversity the least (most redundant).
	bestStoreID := re.pickStoreByDiversity(
		filteredCands, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if bestStoreID == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping AddNonVoter repair for r%d: no valid demotion candidates",
			rangeID)
		return
	}

	// Find the existing replica state for the voter being demoted.
	prevState, found := replicaStateForStore(rs, bestStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping AddNonVoter repair for r%d: voter on s%d not found in replicas",
			rangeID, bestStoreID)
		return
	}

	// Create the type change from VOTER_FULL to NON_VOTER.
	targetSS := re.stores[bestStoreID]
	demoteTarget := roachpb.ReplicationTarget{
		NodeID:  targetSS.NodeID,
		StoreID: bestStoreID,
	}
	nextIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.NON_VOTER},
	}
	typeChange := MakeReplicaTypeChange(
		rangeID, rs.load, prevState, nextIDAndType, demoteTarget)
	rangeChange := MakePendingRangeChange(rangeID, []ReplicaChange{typeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping AddNonVoter repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): AddNonVoter repair for r%d, demoting voter on s%d to non-voter",
		rangeID, bestStoreID)
}

// repairRemoveNonVoter removes an over-replicated non-voter from the range.
// Candidate selection prefers stores that are dead > unknown > unhealthy >
// shedding > refusing > healthy. Within the worst-health bucket, the non-voter
// whose removal hurts diversity the least (most redundant locality) is chosen.
func (re *rebalanceEnv) repairRemoveNonVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveNonVoter repair for r%d: constraint analysis failed",
			rangeID)
		return
	}

	// Build candidates: all non-voters.
	var candidates []roachpb.StoreID
	for _, repl := range rs.replicas {
		if !isNonVoter(repl.ReplicaType.ReplicaType) {
			continue
		}
		candidates = append(candidates, repl.StoreID)
	}
	if len(candidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveNonVoter repair for r%d: no non-voters found",
			rangeID)
		return
	}

	// Bucket candidates by removal priority. Take the lowest-priority bucket
	// (worst health = remove first).
	bestPriority := math.MaxInt
	for _, storeID := range candidates {
		p := removalPriority(re.stores[storeID])
		if p < bestPriority {
			bestPriority = p
		}
	}
	var bucket []roachpb.StoreID
	for _, storeID := range candidates {
		if removalPriority(re.stores[storeID]) == bestPriority {
			bucket = append(bucket, storeID)
		}
	}

	// Within the bucket, pick the non-voter whose removal hurts diversity the
	// least (most redundant). Use replicaLocalityTiers since non-voter diversity
	// is scored against all replicas.
	removeStoreID := re.pickStoreByDiversity(
		bucket, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if removeStoreID == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveNonVoter repair for r%d: diversity picker returned no candidate",
			rangeID)
		return
	}

	// Find the ReplicaState for the chosen non-voter.
	prevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveNonVoter repair for r%d: non-voter on s%d not found in replicas",
			rangeID, removeStoreID)
		return
	}

	// Create the pending change.
	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveNonVoter repair for r%d: store s%d has no state",
			rangeID, removeStoreID)
		return
	}
	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(rangeID, rs.load, prevState, removeTarget)
	rangeChange := MakePendingRangeChange(rangeID, []ReplicaChange{removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveNonVoter repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): RemoveNonVoter repair for r%d, removing non-voter on s%d",
		rangeID, removeStoreID)
}

// repairRemoveLearner removes a stuck LEARNER replica from the range. Learners
// are always removed unconditionally (computeRepairAction returns RemoveLearner
// whenever numLearners > 0). If multiple learners exist, candidate selection
// follows the same priority scheme as other removal actions: dead stores first,
// then diversity-based tiebreaking among the worst-health bucket.
func (re *rebalanceEnv) repairRemoveLearner(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	// Build candidates: all LEARNER replicas.
	var candidates []roachpb.StoreID
	for _, repl := range rs.replicas {
		if repl.ReplicaType.ReplicaType == roachpb.LEARNER {
			candidates = append(candidates, repl.StoreID)
		}
	}
	if len(candidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveLearner repair for r%d: no learners found",
			rangeID)
		return
	}

	// Bucket candidates by removal priority. Take the lowest-priority bucket
	// (worst health = remove first).
	bestPriority := math.MaxInt
	for _, storeID := range candidates {
		p := removalPriority(re.stores[storeID])
		if p < bestPriority {
			bestPriority = p
		}
	}
	var bucket []roachpb.StoreID
	for _, storeID := range candidates {
		if removalPriority(re.stores[storeID]) == bestPriority {
			bucket = append(bucket, storeID)
		}
	}

	// Within the bucket, pick by diversity. We need constraint analysis for
	// the locality tiers. If it fails (shouldn't normally happen), just pick
	// the first candidate — learners are always removed unconditionally.
	var removeStoreID roachpb.StoreID
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints != nil && len(bucket) > 1 {
		removeStoreID = re.pickStoreByDiversity(
			bucket, rs.constraints.replicaLocalityTiers,
			(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	}
	if removeStoreID == 0 {
		removeStoreID = bucket[0]
	}

	// Find the ReplicaState for the chosen learner.
	prevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveLearner repair for r%d: learner on s%d not found in replicas",
			rangeID, removeStoreID)
		return
	}

	// Create the pending change.
	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveLearner repair for r%d: store s%d has no state",
			rangeID, removeStoreID)
		return
	}
	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(rangeID, rs.load, prevState, removeTarget)
	rangeChange := MakePendingRangeChange(rangeID, []ReplicaChange{removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping RemoveLearner repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): RemoveLearner repair for r%d, removing learner on s%d",
		rangeID, removeStoreID)
}

// repairReplaceDeadVoter replaces a voter on a dead store with one on a healthy
// store. The voter count matches the config, so both the remove and add are
// bundled into a single atomic replication change. The dead voter to remove is
// chosen by diversity (most redundant locality), and the replacement store is
// chosen the same way as repairAddVoter.
func (re *rebalanceEnv) repairReplaceDeadVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadVoter repair for r%d: constraint analysis failed",
			rangeID)
		return
	}

	// Step 1: Find the dead voter to remove.
	// Build candidates: voters on dead stores, excluding the leaseholder.
	var deadCandidates []roachpb.StoreID
	for _, repl := range rs.replicas {
		if !isVoter(repl.ReplicaType.ReplicaType) {
			continue
		}
		if repl.IsLeaseholder {
			continue
		}
		ss := re.stores[repl.StoreID]
		if ss != nil && ss.status.Health == HealthDead {
			deadCandidates = append(deadCandidates, repl.StoreID)
		}
	}
	if len(deadCandidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadVoter repair for r%d: no dead voters found",
			rangeID)
		return
	}

	// If multiple dead voters, pick the most redundant (whose removal hurts
	// diversity the least).
	removeStoreID := re.pickStoreByDiversity(
		deadCandidates, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if removeStoreID == 0 {
		removeStoreID = deadCandidates[0]
	}

	// Find the ReplicaState for the chosen dead voter.
	removePrevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadVoter repair for r%d: voter on s%d not found in replicas",
			rangeID, removeStoreID)
		return
	}

	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadVoter repair for r%d: store s%d has no state",
			rangeID, removeStoreID)
		return
	}

	// Step 2: Find a healthy store for the replacement voter.
	// Unlike repairAddVoter, we can't use constraintsForAddingVoter() because
	// the voter count already matches config. We pass nil constraints to get
	// all stores as candidates and rely on diversity scoring for selection.
	// TODO(kvoli): use voter/replica constraints to narrow candidates when
	// constraints are configured.
	var candidateStores storeSet
	re.constraintMatcher.constrainStoresForExpr(nil, &candidateStores)

	// Build the set of stores already hosting a replica (excluding the dead
	// voter being removed) and the set of nodes already hosting a replica.
	var existingReplicas storeSet
	existingNodes := make(map[roachpb.NodeID]struct{})
	for _, repl := range rs.replicas {
		if repl.StoreID == removeStoreID {
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
			"skipping ReplaceDeadVoter repair for r%d: no valid target stores",
			rangeID)
		return
	}

	addStoreID := re.pickStoreByDiversity(
		validCandidates, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)

	// Step 3: Create the atomic change (remove dead voter + add new voter).
	addSS := re.stores[addStoreID]
	addTarget := roachpb.ReplicationTarget{
		NodeID:  addSS.NodeID,
		StoreID: addStoreID,
	}
	addIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
	}
	addChange := MakeAddReplicaChange(rangeID, rs.load, addIDAndType, addTarget)

	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(
		rangeID, rs.load, removePrevState, removeTarget)

	rangeChange := MakePendingRangeChange(
		rangeID, []ReplicaChange{addChange, removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadVoter repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): ReplaceDeadVoter repair for r%d, removing voter on s%d, adding voter on s%d",
		rangeID, removeStoreID, addStoreID)
}

// repairReplaceDeadNonVoter replaces a non-voter on a dead store with one on a
// healthy store. The non-voter count matches the config, so both the remove and
// add are bundled into a single atomic replication change. The dead non-voter to
// remove is chosen by diversity (most redundant locality), and the replacement
// store is chosen the same way as repairAddNonVoter.
func (re *rebalanceEnv) repairReplaceDeadNonVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadNonVoter repair for r%d: constraint analysis failed",
			rangeID)
		return
	}

	// Step 1: Find the dead non-voter to remove.
	var deadCandidates []roachpb.StoreID
	for _, repl := range rs.replicas {
		if !isNonVoter(repl.ReplicaType.ReplicaType) {
			continue
		}
		ss := re.stores[repl.StoreID]
		if ss != nil && ss.status.Health == HealthDead {
			deadCandidates = append(deadCandidates, repl.StoreID)
		}
	}
	if len(deadCandidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadNonVoter repair for r%d: no dead non-voters found",
			rangeID)
		return
	}

	// If multiple dead non-voters, pick the most redundant (whose removal
	// hurts diversity the least).
	removeStoreID := re.pickStoreByDiversity(
		deadCandidates, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if removeStoreID == 0 {
		removeStoreID = deadCandidates[0]
	}

	// Find the ReplicaState for the chosen dead non-voter.
	removePrevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadNonVoter repair for r%d: non-voter on s%d not found in replicas",
			rangeID, removeStoreID)
		return
	}

	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadNonVoter repair for r%d: store s%d has no state",
			rangeID, removeStoreID)
		return
	}

	// Step 2: Find a healthy store for the replacement non-voter.
	// The non-voter count already matches config, so we pass nil constraints
	// to get all stores as candidates and rely on diversity scoring.
	// TODO(kvoli): use voter/replica constraints to narrow candidates when
	// constraints are configured.
	var candidateStores storeSet
	re.constraintMatcher.constrainStoresForExpr(nil, &candidateStores)

	// Build the set of stores already hosting a replica (excluding the dead
	// non-voter being removed) and the set of nodes already hosting a replica.
	var existingReplicas storeSet
	existingNodes := make(map[roachpb.NodeID]struct{})
	for _, repl := range rs.replicas {
		if repl.StoreID == removeStoreID {
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
			"skipping ReplaceDeadNonVoter repair for r%d: no valid target stores",
			rangeID)
		return
	}

	addStoreID := re.pickStoreByDiversity(
		validCandidates, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)

	// Step 3: Create the atomic change (remove dead non-voter + add new
	// non-voter).
	addSS := re.stores[addStoreID]
	addTarget := roachpb.ReplicationTarget{
		NodeID:  addSS.NodeID,
		StoreID: addStoreID,
	}
	addIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.NON_VOTER},
	}
	addChange := MakeAddReplicaChange(rangeID, rs.load, addIDAndType, addTarget)

	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(
		rangeID, rs.load, removePrevState, removeTarget)

	rangeChange := MakePendingRangeChange(
		rangeID, []ReplicaChange{addChange, removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDeadNonVoter repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): ReplaceDeadNonVoter repair for r%d, removing non-voter on s%d, adding non-voter on s%d",
		rangeID, removeStoreID, addStoreID)
}

// repairReplaceDecommissioningVoter replaces a voter on a decommissioning
// (shedding) store with one on a healthy store. Identical to
// repairReplaceDeadVoter but targets stores with ReplicaDispositionShedding
// instead of HealthDead. The leaseholder is excluded from removal candidates.
func (re *rebalanceEnv) repairReplaceDecommissioningVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningVoter repair for r%d: constraint analysis failed",
			rangeID)
		return
	}

	// Step 1: Find the decommissioning voter to remove.
	// Build candidates: voters on shedding stores, excluding the leaseholder.
	var decomCandidates []roachpb.StoreID
	for _, repl := range rs.replicas {
		if !isVoter(repl.ReplicaType.ReplicaType) {
			continue
		}
		if repl.IsLeaseholder {
			continue
		}
		ss := re.stores[repl.StoreID]
		if ss != nil && ss.status.Disposition.Replica == ReplicaDispositionShedding {
			decomCandidates = append(decomCandidates, repl.StoreID)
		}
	}
	if len(decomCandidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningVoter repair for r%d: no decommissioning voters found",
			rangeID)
		return
	}

	// If multiple decommissioning voters, pick the most redundant (whose
	// removal hurts diversity the least).
	removeStoreID := re.pickStoreByDiversity(
		decomCandidates, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if removeStoreID == 0 {
		removeStoreID = decomCandidates[0]
	}

	// Find the ReplicaState for the chosen decommissioning voter.
	removePrevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningVoter repair for r%d: voter on s%d not found in replicas",
			rangeID, removeStoreID)
		return
	}

	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningVoter repair for r%d: store s%d has no state",
			rangeID, removeStoreID)
		return
	}

	// Step 2: Find a healthy store for the replacement voter.
	// The voter count already matches config, so we pass nil constraints
	// to get all stores as candidates and rely on diversity scoring.
	// TODO(kvoli): use voter/replica constraints to narrow candidates when
	// constraints are configured.
	var candidateStores storeSet
	re.constraintMatcher.constrainStoresForExpr(nil, &candidateStores)

	// Build the set of stores already hosting a replica (excluding the
	// decommissioning voter being removed) and the set of nodes already
	// hosting a replica.
	var existingReplicas storeSet
	existingNodes := make(map[roachpb.NodeID]struct{})
	for _, repl := range rs.replicas {
		if repl.StoreID == removeStoreID {
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
			"skipping ReplaceDecommissioningVoter repair for r%d: no valid target stores",
			rangeID)
		return
	}

	addStoreID := re.pickStoreByDiversity(
		validCandidates, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)

	// Step 3: Create the atomic change (remove decommissioning voter + add
	// new voter).
	addSS := re.stores[addStoreID]
	addTarget := roachpb.ReplicationTarget{
		NodeID:  addSS.NodeID,
		StoreID: addStoreID,
	}
	addIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
	}
	addChange := MakeAddReplicaChange(rangeID, rs.load, addIDAndType, addTarget)

	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(
		rangeID, rs.load, removePrevState, removeTarget)

	rangeChange := MakePendingRangeChange(
		rangeID, []ReplicaChange{addChange, removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningVoter repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): ReplaceDecommissioningVoter repair for r%d, removing voter on s%d, adding voter on s%d",
		rangeID, removeStoreID, addStoreID)
}

// repairReplaceDecommissioningNonVoter replaces a non-voter on a
// decommissioning (shedding) store with one on a healthy store. Identical to
// repairReplaceDeadNonVoter but targets stores with ReplicaDispositionShedding
// instead of HealthDead.
func (re *rebalanceEnv) repairReplaceDecommissioningNonVoter(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningNonVoter repair for r%d: constraint analysis failed",
			rangeID)
		return
	}

	// Step 1: Find the decommissioning non-voter to remove.
	var decomCandidates []roachpb.StoreID
	for _, repl := range rs.replicas {
		if !isNonVoter(repl.ReplicaType.ReplicaType) {
			continue
		}
		ss := re.stores[repl.StoreID]
		if ss != nil && ss.status.Disposition.Replica == ReplicaDispositionShedding {
			decomCandidates = append(decomCandidates, repl.StoreID)
		}
	}
	if len(decomCandidates) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningNonVoter repair for r%d: no decommissioning non-voters found",
			rangeID)
		return
	}

	// If multiple decommissioning non-voters, pick the most redundant (whose
	// removal hurts diversity the least).
	removeStoreID := re.pickStoreByDiversity(
		decomCandidates, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if removeStoreID == 0 {
		removeStoreID = decomCandidates[0]
	}

	// Find the ReplicaState for the chosen decommissioning non-voter.
	removePrevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningNonVoter repair for r%d: non-voter on s%d not found in replicas",
			rangeID, removeStoreID)
		return
	}

	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningNonVoter repair for r%d: store s%d has no state",
			rangeID, removeStoreID)
		return
	}

	// Step 2: Find a healthy store for the replacement non-voter.
	// The non-voter count already matches config, so we pass nil constraints
	// to get all stores as candidates and rely on diversity scoring.
	// TODO(kvoli): use voter/replica constraints to narrow candidates when
	// constraints are configured.
	var candidateStores storeSet
	re.constraintMatcher.constrainStoresForExpr(nil, &candidateStores)

	// Build the set of stores already hosting a replica (excluding the
	// decommissioning non-voter being removed) and the set of nodes already
	// hosting a replica.
	var existingReplicas storeSet
	existingNodes := make(map[roachpb.NodeID]struct{})
	for _, repl := range rs.replicas {
		if repl.StoreID == removeStoreID {
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
			"skipping ReplaceDecommissioningNonVoter repair for r%d: no valid target stores",
			rangeID)
		return
	}

	addStoreID := re.pickStoreByDiversity(
		validCandidates, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)

	// Step 3: Create the atomic change (remove decommissioning non-voter +
	// add new non-voter).
	addSS := re.stores[addStoreID]
	addTarget := roachpb.ReplicationTarget{
		NodeID:  addSS.NodeID,
		StoreID: addStoreID,
	}
	addIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.NON_VOTER},
	}
	addChange := MakeAddReplicaChange(rangeID, rs.load, addIDAndType, addTarget)

	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(
		rangeID, rs.load, removePrevState, removeTarget)

	rangeChange := MakePendingRangeChange(
		rangeID, []ReplicaChange{addChange, removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping ReplaceDecommissioningNonVoter repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): ReplaceDecommissioningNonVoter repair for r%d, removing non-voter on s%d, adding non-voter on s%d",
		rangeID, removeStoreID, addStoreID)
}

// repairFinalizeAtomicReplicationChange finalizes a range that is in a joint
// configuration. It scans for replicas with joint-config types and converts
// them to their finalized forms:
//   - VOTER_INCOMING -> VOTER_FULL (type change)
//   - VOTER_DEMOTING_LEARNER -> removed (removal)
//   - VOTER_DEMOTING_NON_VOTER -> NON_VOTER (type change)
func (re *rebalanceEnv) repairFinalizeAtomicReplicationChange(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	var changes []ReplicaChange
	for _, repl := range rs.replicas {
		typ := repl.ReplicaType.ReplicaType
		ss := re.stores[repl.StoreID]
		if ss == nil {
			continue
		}
		target := roachpb.ReplicationTarget{
			NodeID:  ss.NodeID,
			StoreID: repl.StoreID,
		}
		switch typ {
		case roachpb.VOTER_INCOMING:
			// Promote to full voter.
			nextIDAndType := ReplicaIDAndType{
				ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
			}
			changes = append(changes,
				MakeReplicaTypeChange(rangeID, rs.load, repl.ReplicaState, nextIDAndType, target))
		case roachpb.VOTER_DEMOTING_LEARNER:
			// Remove the demoting voter.
			changes = append(changes,
				MakeRemoveReplicaChange(rangeID, rs.load, repl.ReplicaState, target))
		case roachpb.VOTER_DEMOTING_NON_VOTER:
			// Demote to non-voter.
			nextIDAndType := ReplicaIDAndType{
				ReplicaType: ReplicaType{ReplicaType: roachpb.NON_VOTER},
			}
			changes = append(changes,
				MakeReplicaTypeChange(rangeID, rs.load, repl.ReplicaState, nextIDAndType, target))
		}
	}
	if len(changes) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping FinalizeAtomicReplicationChange repair for r%d: no joint-config replicas found",
			rangeID)
		return
	}

	rangeChange := MakePendingRangeChange(rangeID, changes)
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping FinalizeAtomicReplicationChange repair for r%d: pre-check failed: %v",
			rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): FinalizeAtomicReplicationChange repair for r%d",
		rangeID)
}

// repairSwapVoterForConstraints swaps a misplaced voter with one that
// satisfies voter constraints. The voter count matches config, so the fix is
// an atomic remove + add (or a pair of role swaps if a non-voter already
// exists in the right place).
//
// Two strategies are tried in order:
//  1. Role swap: if a voter can be demoted and a non-voter promoted to satisfy
//     voter constraints, emit two MakeReplicaTypeChange entries.
//  2. Fallback: remove a misplaced voter and add a new one on a store that
//     satisfies the unsatisfied constraint.
func (re *rebalanceEnv) repairSwapVoterForConstraints(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints repair for r%d: constraint analysis failed",
			rangeID)
		return
	}

	// Strategy 1: Try role swap (voter <-> non-voter).
	swapCands, err := rs.constraints.candidatesForRoleSwapForConstraints()
	if err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints repair for r%d: %v", rangeID, err)
		return
	}
	if len(swapCands[voterIndex]) > 0 && len(swapCands[nonVoterIndex]) > 0 {
		re.roleSwapVoterForConstraints(
			ctx, localStoreID, rangeID, rs, swapCands)
		return
	}

	// Strategy 2: Remove misplaced voter + add replacement.
	toRemoveVoters, toAdd, err :=
		rs.constraints.candidatesVoterConstraintsUnsatisfied()
	if err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints repair for r%d: %v", rangeID, err)
		return
	}
	if len(toRemoveVoters) == 0 || len(toAdd) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints repair for r%d: "+
				"no candidates (toRemove=%d, toAdd constraints=%d)",
			rangeID, len(toRemoveVoters), len(toAdd))
		return
	}

	// Pick voter to remove (exclude leaseholder, diversity-based).
	var removeCands []roachpb.StoreID
	for _, storeID := range toRemoveVoters {
		for _, repl := range rs.replicas {
			if repl.StoreID == storeID && repl.IsLeaseholder {
				goto skipRemove
			}
		}
		removeCands = append(removeCands, storeID)
	skipRemove:
	}
	if len(removeCands) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints repair for r%d: "+
				"no removable voters (all candidates are leaseholder)", rangeID)
		return
	}
	removeStoreID := re.pickStoreByDiversity(
		removeCands, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if removeStoreID == 0 {
		removeStoreID = removeCands[0]
	}

	// Find the ReplicaState for the voter being removed.
	removePrevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints repair for r%d: "+
				"voter on s%d not found in replicas", rangeID, removeStoreID)
		return
	}

	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints repair for r%d: "+
				"store s%d has no state", rangeID, removeStoreID)
		return
	}

	// Find a replacement store satisfying the unsatisfied constraint.
	var candidateStores storeSet
	re.constraintMatcher.constrainStoresForExpr(toAdd, &candidateStores)

	// Build existing replicas/nodes, excluding the voter being removed.
	var existingReplicas storeSet
	existingNodes := make(map[roachpb.NodeID]struct{})
	for _, repl := range rs.replicas {
		if repl.StoreID == removeStoreID {
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
			"skipping SwapVoterForConstraints repair for r%d: "+
				"no valid target stores", rangeID)
		return
	}

	addStoreID := re.pickStoreByDiversity(
		validCandidates, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)

	// Create atomic change (add + remove).
	addSS := re.stores[addStoreID]
	addTarget := roachpb.ReplicationTarget{
		NodeID:  addSS.NodeID,
		StoreID: addStoreID,
	}
	addIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
	}
	addChange := MakeAddReplicaChange(
		rangeID, rs.load, addIDAndType, addTarget)

	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(
		rangeID, rs.load, removePrevState, removeTarget)

	rangeChange := MakePendingRangeChange(
		rangeID, []ReplicaChange{addChange, removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints repair for r%d: "+
				"pre-check failed: %v", rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): SwapVoterForConstraints repair for r%d, "+
			"removing voter on s%d, adding voter on s%d",
		rangeID, removeStoreID, addStoreID)
}

// roleSwapVoterForConstraints performs a role swap to satisfy voter
// constraints: demotes a misplaced voter to non-voter and promotes a
// well-placed non-voter to voter, both in a single pending range change.
func (re *rebalanceEnv) roleSwapVoterForConstraints(
	ctx context.Context,
	localStoreID roachpb.StoreID,
	rangeID roachpb.RangeID,
	rs *rangeState,
	swapCands [numReplicaKinds][]roachpb.StoreID,
) {
	// Pick voter to demote (exclude leaseholder, diversity removal scoring).
	var demoteCands []roachpb.StoreID
	for _, storeID := range swapCands[voterIndex] {
		for _, repl := range rs.replicas {
			if repl.StoreID == storeID && repl.IsLeaseholder {
				goto skipDemote
			}
		}
		demoteCands = append(demoteCands, storeID)
	skipDemote:
	}
	if len(demoteCands) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints role swap for r%d: "+
				"no demotion candidates (all are leaseholders)", rangeID)
		return
	}

	demoteStoreID := re.pickStoreByDiversity(
		demoteCands, rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if demoteStoreID == 0 {
		demoteStoreID = demoteCands[0]
	}

	// Pick non-voter to promote (diversity addition scoring).
	promoteStoreID := re.pickStoreByDiversity(
		swapCands[nonVoterIndex], rs.constraints.voterLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)
	if promoteStoreID == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints role swap for r%d: "+
				"no valid promotion candidates", rangeID)
		return
	}

	// Find replica states for both.
	demotePrevState, demoteFound := replicaStateForStore(rs, demoteStoreID)
	promotePrevState, promoteFound := replicaStateForStore(rs, promoteStoreID)
	if !demoteFound || !promoteFound {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints role swap for r%d: "+
				"replica not found (demote s%d found=%t, promote s%d found=%t)",
			rangeID, demoteStoreID, demoteFound, promoteStoreID, promoteFound)
		return
	}

	// Create the two type changes: voter->NON_VOTER, non-voter->VOTER_FULL.
	demoteSS := re.stores[demoteStoreID]
	if demoteSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints role swap for r%d: "+
				"store s%d has no state", rangeID, demoteStoreID)
		return
	}
	promoteSS := re.stores[promoteStoreID]
	if promoteSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints role swap for r%d: "+
				"store s%d has no state", rangeID, promoteStoreID)
		return
	}

	demoteTarget := roachpb.ReplicationTarget{
		NodeID:  demoteSS.NodeID,
		StoreID: demoteStoreID,
	}
	demoteNextIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.NON_VOTER},
	}
	demoteChange := MakeReplicaTypeChange(
		rangeID, rs.load, demotePrevState, demoteNextIDAndType, demoteTarget)

	promoteTarget := roachpb.ReplicationTarget{
		NodeID:  promoteSS.NodeID,
		StoreID: promoteStoreID,
	}
	promoteNextIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.VOTER_FULL},
	}
	promoteChange := MakeReplicaTypeChange(
		rangeID, rs.load, promotePrevState, promoteNextIDAndType, promoteTarget)

	rangeChange := MakePendingRangeChange(
		rangeID, []ReplicaChange{demoteChange, promoteChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapVoterForConstraints role swap for r%d: "+
				"pre-check failed: %v", rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): SwapVoterForConstraints role swap for r%d, "+
			"demoting voter on s%d, promoting non-voter on s%d",
		rangeID, demoteStoreID, promoteStoreID)
}

// repairSwapNonVoterForConstraints swaps a misplaced non-voter with one that
// satisfies placement constraints. The non-voter count matches config, so the
// fix is an atomic remove + add.
func (re *rebalanceEnv) repairSwapNonVoterForConstraints(
	ctx context.Context, localStoreID roachpb.StoreID, rangeID roachpb.RangeID, rs *rangeState,
) {
	re.ensureAnalyzedConstraints(ctx, rs)
	if rs.constraints == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapNonVoterForConstraints repair for r%d: "+
				"constraint analysis failed", rangeID)
		return
	}

	toRemoveNonVoters, toAdd, err :=
		rs.constraints.candidatesNonVoterConstraintsUnsatisfied()
	if err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapNonVoterForConstraints repair for r%d: %v",
			rangeID, err)
		return
	}
	if len(toRemoveNonVoters) == 0 || len(toAdd) == 0 {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapNonVoterForConstraints repair for r%d: "+
				"no candidates (toRemove=%d, toAdd constraints=%d)",
			rangeID, len(toRemoveNonVoters), len(toAdd))
		return
	}

	// Pick non-voter to remove (diversity removal scoring with
	// replicaLocalityTiers — no leaseholder exclusion needed for non-voters).
	removeStoreID := re.pickStoreByDiversity(
		toRemoveNonVoters, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
	if removeStoreID == 0 {
		removeStoreID = toRemoveNonVoters[0]
	}

	// Find the ReplicaState for the non-voter being removed.
	removePrevState, found := replicaStateForStore(rs, removeStoreID)
	if !found {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapNonVoterForConstraints repair for r%d: "+
				"non-voter on s%d not found in replicas", rangeID, removeStoreID)
		return
	}

	removeSS := re.stores[removeStoreID]
	if removeSS == nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapNonVoterForConstraints repair for r%d: "+
				"store s%d has no state", rangeID, removeStoreID)
		return
	}

	// Find a replacement store satisfying the unsatisfied constraint.
	var candidateStores storeSet
	re.constraintMatcher.constrainStoresForExpr(toAdd, &candidateStores)

	// Build existing replicas/nodes, excluding the non-voter being removed.
	var existingReplicas storeSet
	existingNodes := make(map[roachpb.NodeID]struct{})
	for _, repl := range rs.replicas {
		if repl.StoreID == removeStoreID {
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
			"skipping SwapNonVoterForConstraints repair for r%d: "+
				"no valid target stores", rangeID)
		return
	}

	addStoreID := re.pickStoreByDiversity(
		validCandidates, rs.constraints.replicaLocalityTiers,
		(*existingReplicaLocalities).getScoreChangeForNewReplica)

	// Create atomic change (add + remove).
	addSS := re.stores[addStoreID]
	addTarget := roachpb.ReplicationTarget{
		NodeID:  addSS.NodeID,
		StoreID: addStoreID,
	}
	addIDAndType := ReplicaIDAndType{
		ReplicaType: ReplicaType{ReplicaType: roachpb.NON_VOTER},
	}
	addChange := MakeAddReplicaChange(
		rangeID, rs.load, addIDAndType, addTarget)

	removeTarget := roachpb.ReplicationTarget{
		NodeID:  removeSS.NodeID,
		StoreID: removeStoreID,
	}
	removeChange := MakeRemoveReplicaChange(
		rangeID, rs.load, removePrevState, removeTarget)

	rangeChange := MakePendingRangeChange(
		rangeID, []ReplicaChange{addChange, removeChange})
	if err := re.preCheckOnApplyReplicaChanges(rangeChange); err != nil {
		log.KvDistribution.Warningf(ctx,
			"skipping SwapNonVoterForConstraints repair for r%d: "+
				"pre-check failed: %v", rangeID, err)
		return
	}
	re.addPendingRangeChange(ctx, rangeChange)
	re.changes = append(re.changes,
		MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
	log.KvDistribution.Infof(ctx,
		"result(success): SwapNonVoterForConstraints repair for r%d, "+
			"removing non-voter on s%d, adding non-voter on s%d",
		rangeID, removeStoreID, addStoreID)
}

// Verify interface compliance.
var _ fmt.Stringer = RepairAction(0)
var _ redact.SafeFormatter = RepairAction(0)
