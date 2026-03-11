// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

//go:generate stringer -type=RepairAction
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

	// numRepairActions is the total number of RepairAction values (including
	// NoRepairNeeded). It must remain the last entry in the iota sequence so
	// that iteration over [1, numRepairActions) covers all valid actions.
	numRepairActions
)

// SafeFormat implements redact.SafeFormatter.
func (a RepairAction) SafeFormat(w redact.SafePrinter, _ rune) {
	w.SafeString(redact.SafeString(a.String()))
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
			// finalization. We count them here but the joint config check (step 5)
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

	// Step 4: Quorum check — must happen before any repair action since all
	// repairs require quorum to make progress.
	// Decommissioning voters are still alive for quorum purposes.
	aliveVoters := numVoters - deadVoters
	quorum := numVoters/2 + 1
	if aliveVoters < quorum {
		return RepairSkipped
	}

	// Step 5: Joint config / learner checks.
	if hasJointConfig {
		return FinalizeAtomicReplicationChange
	}
	if numLearners > 0 {
		return RemoveLearner
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
	// Remove from old bucket. All states except NoRepairNeeded (and the zero
	// value, which indicates an uninitialized field) are tracked in the index.
	if oldAction != NoRepairNeeded && oldAction != 0 {
		if m, ok := cs.repairRanges[oldAction]; ok {
			delete(m, rangeID)
			if len(m) == 0 {
				delete(cs.repairRanges, oldAction)
			}
		} else if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf(
				"repairRanges missing bucket for action %s on r%d", oldAction, rangeID))
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
	// Start at 1: RepairAction(0) is intentionally invalid (see enum definition).
	idsPtr := rangeIDSlicePool.Get().(*[]roachpb.RangeID)
	ids := (*idsPtr)[:0]
	defer func() {
		// Preserve any grown capacity for reuse.
		ids = ids[:0]
		*idsPtr = ids
		rangeIDSlicePool.Put(idsPtr)
	}()
	for action := RepairAction(1); action < numRepairActions; action++ {
		ranges := re.repairRanges[action]
		// RepairSkipped and RepairPending are tracked in the index for metrics
		// visibility but are not actionable.
		if len(ranges) == 0 || action == RepairSkipped || action == RepairPending {
			continue
		}
		// Collect and sort range IDs, then shuffle deterministically so that
		// iteration is not systematically biased toward any range.
		ids = ids[:0]
		for rid := range ranges {
			ids = append(ids, rid)
		}
		slices.Sort(ids)
		re.rng.Shuffle(len(ids), func(i, j int) {
			ids[i], ids[j] = ids[j], ids[i]
		})

		for _, rangeID := range ids {
			rs := re.ranges[rangeID]
			// Only repair ranges where localStoreID is the leaseholder.
			if !isLeaseholderOnStore(rs, localStoreID) {
				continue
			}

			switch action {
			default:
				log.KvDistribution.VEventf(ctx, 2,
					"repair action %s for r%d not yet implemented", action, rangeID)
			}
		}
	}
	return re.changes
}

var rangeIDSlicePool = sync.Pool{
	New: func() interface{} {
		s := make([]roachpb.RangeID, 0, 16)
		return &s
	},
}

// Verify interface compliance.
var _ fmt.Stringer = RepairAction(0)
var _ redact.SafeFormatter = RepairAction(0)
