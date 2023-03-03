// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aimpl

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// TODO(sumeer): none of the code here or other aimpl files is optimized to
// minimized allocations. We are repeatedly allocating slices and maps. Fix
// that.

// Ordered and de-duped list of storeIDs. Represents a set of stores. Used for
// fast set operations for constraint satisfaction.
type postingList []roachpb.StoreID

func (pl *postingList) union(a []roachpb.StoreID) {
	// TODO(sumeer): for now can do linear scans. For larger lists can use
	// binary search (same for other set operations).
}

func (pl *postingList) intersect(a []roachpb.StoreID) {
	// TODO(sumeer): implement.
}

func (pl *postingList) clone() postingList {
	// TODO(sumeer): implement.
	return nil
}

func (pl *postingList) subtract(a []roachpb.StoreID) {
	// TODO(sumeer): implement.
}

// Assumes that input is de-duped.
func makePostingList(storeIDs []roachpb.StoreID) postingList {
	// TODO(sumeer): copy and sort. Many callers are ok with reuse so don't need
	// to copy for them.
	return nil
}

// constraintMatchingState is used for fast constraint matching.
//
// TODO(sumeer): initialization and update when attributes change, and stores
// are added and removed from the cluster.
type constraintMatchingState struct {
	// Constraint to the list of stores that match that constraint.
	constraints map[roachpb.Constraint]postingList
}

// Returns a postingList matching the given conjunction of constraints.
func (ms *constraintMatchingState) constrainStores(constraints []roachpb.Constraint) postingList {
	var rv postingList
	for i := range constraints {
		pl, ok := ms.constraints[constraints[i]]
		if !ok {
			return nil
		}
		if i == 0 {
			rv = pl.clone()
		} else {
			rv.intersect(pl)
			if len(rv) == 0 {
				return nil
			}
		}
	}
	return rv
}

// Returns whether the given storeID matches the given conjunction of constraints.
func (ms *constraintMatchingState) storeMatchesConstraints(
	storeID roachpb.StoreID, constraints []roachpb.Constraint,
) bool {
	for i := range constraints {
		pl, ok := ms.constraints[constraints[i]]
		if !ok {
			return false
		}
		// TODO(sumeer): binary search in pl which is in ascending int order.
		found := false
		for _, s := range pl {
			if s == storeID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Returns the subset of underloadedList that satisfies one of the unsatisfied constraints.
//
// REQUIRES: ac.hasUnsatisfiedConstraints()
func (ms *constraintMatchingState) intersectWithUnsatisfiedConstraints(
	underloadedList postingList, ac *analyzedConstraints,
) postingList {
	var plUnion postingList
	initialized := false
	for i := range ac.constraints {
		if int(ac.constraints[i].NumReplicas)-len(ac.satisfiedBy[i]) > 0 {
			cpl := ms.constrainStores(ac.constraints[i].Constraints)
			initialized = true
			if len(plUnion) == 0 {
				plUnion = cpl
			} else {
				plUnion.union(cpl)
			}
		}
	}
	if !initialized {
		panic("")
	}
	underloadedList.intersect(plUnion)
	return underloadedList
}

// Lease preferences: we are assuming that []LeasePreference is not something
// we need to pay attention to when choosing replicas, in that the various
// []ConstraintsConjunction will give us good replicas on which we can later
// apply the lease preferences in a best-effort manner. Otherwise, we will
// need to make multiple passes where we consider each LeasePreference as
// strict, then try to add/rebalance while satisfying them, and if not
// satisfied consider the next LeasePreference -- this is doable but makes for
// more costly decision making.

// NB: candidates can include existing non-voters, but not existing voters.
// REQUIRES: constraints.hasUnsatisfiedConstraints()
func (ms *constraintMatchingState) storeCandidatesForAddVoterReplica(
	underloadedList postingList,
	existingVoters []roachpb.StoreID,
	constraints *analyzedConstraints,
	voterConstraints *analyzedConstraints,
) postingList {
	underloadedList = ms.intersectWithUnsatisfiedConstraints(underloadedList, constraints)
	if voterConstraints != nil {
		underloadedList = ms.intersectWithUnsatisfiedConstraints(underloadedList, voterConstraints)
	}
	existingVotersPL := makePostingList(existingVoters)
	underloadedList.subtract(existingVotersPL)
	return underloadedList
}

// NB: candidates will not include existing replicas, voters or non-voters.
// REQUIRES: constraints.hasUnsatisfiedConstraints()
func (ms *constraintMatchingState) storeCandidatesForAddNonVoterReplica(
	underloadedList postingList,
	existingVoters []roachpb.StoreID,
	existingNonVoters []roachpb.StoreID,
	constraints *analyzedConstraints,
) postingList {
	underloadedList = ms.intersectWithUnsatisfiedConstraints(underloadedList, constraints)
	existingVotersPL := makePostingList(existingVoters)
	underloadedList.subtract(existingVotersPL)
	existingNonVotersPL := makePostingList(existingNonVoters)
	underloadedList.subtract(existingNonVotersPL)
	return underloadedList
}

// Rebalance case.
// NB: assumes that voterToReplace is not the leaseholder, and is in constraints and
// voterConstraints.
// REQUIRES: !constraints.hasUnsatisfiedConstraints() && !constraints.hasOversatisfiedConstraint().
func (ms *constraintMatchingState) storeCandidatesForReplacingVoterReplica(
	underloadedList postingList,
	existingVoters []roachpb.StoreID,
	voterToReplace roachpb.StoreID,
	constraints *analyzedConstraints,
	voterConstraints *analyzedConstraints,
) postingList {
	pl := ms.computeCandidatesToReplaceExistingSatisfied(underloadedList, voterToReplace, constraints)
	if voterConstraints != nil {
		pl = ms.computeCandidatesToReplaceExistingSatisfied(
			pl, voterToReplace, voterConstraints)
	}
	pl.subtract(makePostingList(existingVoters))
	return pl
}

// Rebalance case.
func (ms *constraintMatchingState) storeCandidatesForReplacingNonVoterReplica(
	underloadedList postingList,
	existingNonVoters []roachpb.StoreID,
	nonVoterToReplace roachpb.StoreID,
	constraints *analyzedConstraints,
) postingList {
	pl := ms.computeCandidatesToReplaceExistingSatisfied(underloadedList, nonVoterToReplace, constraints)
	pl.subtract(makePostingList(existingNonVoters))
	return pl
}

func (ms *constraintMatchingState) computeCandidatesToReplaceExistingSatisfied(
	underloadedList postingList, toReplace roachpb.StoreID, constraints *analyzedConstraints,
) postingList {
	for i := range constraints.satisfiedBy {
		for _, storeID := range constraints.satisfiedBy[i] {
			if storeID == toReplace {
				underloadedList.intersect(ms.constrainStores(constraints.constraints[i].Constraints))
				return underloadedList
			}
		}
	}
	panic("did not find StoreID")
}

// TODO(sumeer): follow-the-workload leaseholder and replica placement. Even
// though not hard constraint, we may want to express it as a dynamically
// constructed Constraint that is used to best-effort constrain. Do we even
// need to support follow-the-workload if it is a dying feature?

// Note on pre-processing. The high-level goal (of which the following are
// first-cut embodiments) is to do as much pre-processing as possible to save
// later repeated costs at iteration time. This means:
// - pre-processing store and node attributes and locality information since
//   it rarely changes.
// - pre-processing SpanConfigs.
// - processing the current state of a range's replicas in
//   rangeAnalyzedConstraints, and caching it (in rangeState) if we are not
//   able to enact whatever change that caused us to look at the range. The
//   caching will help when we have to repeatedly look at a range (and fail to
//   do something each time).

// Subset of SpanConfig with some pre-processing.
//
// TODO(sumeer): we need to tweak this normalization -- see the comment in
// allocator.go that starts with "TODO(sumeer): this is incomplete, and
// assumes that". Specifically, we can normalize to three
// []roachpb.ConstraintsConjunctions, two for voters and one for non-voters.
// Because what we have here is slightly broken, we are not really doing
// anything about NON_VOTERs in allocator.go.
type spanConfig struct {
	// []roachpb.ConstraintsConjunction is normalized such that
	// - If SpanConfig has one or more ConstraintsConjunction with
	//   NumReplicas=0, they have all been added to the same Contraints slice
	//   (since all need to be satisfied). We know that SpanConfig must only
	//   have ConstraintsConjunction with NumReplicas=0 in this case. The
	//   NumReplicas is set to the required number (NumReplics or NumVoters as
	//   relevant) for this ConstraintsConjunction. So there is no special case
	//   of ConstraintsConjunction.NumReplicas=0
	//
	// - If SpanConfig has ConstraintsConjunctions with NumReplicas != 0, they
	//   are unchanged, but if the sum did not add up to the required number
	//   (NumReplics or NumVoters as relevant), then another
	//   ConstraintsConjunction is added with an empty constraints slice, and
	//   the remaining count. This ConstraintsConjunction with the empty slice
	//   is always the last conjunction in this slice.
	constraints []roachpb.ConstraintsConjunction
	// Can be nil.
	voterConstraints []roachpb.ConstraintsConjunction
	// Best-effort. Conjunctions are in order of preference, and it is ok if
	// none is satisfied.
	leasePreferences []roachpb.LeasePreference
}

// NB: Not reusing AnalyzedConstraints. We want to quickly know whether there
// are any constraints that have not been satisfied.
// AnalyzedConstraints.UnconstrainedReplicas can be true and there could be
// some unsatisfied constraints. Also, it seems like the same store can be in
// multiple SatisfiedBy slices.

// analyzedConstraints is the result of processing a
// []roachpb.ConstraintsConjunction from the spanConfig against the current
// set of replicas. We only do this when there are no pendingChanges for a
// range.
type analyzedConstraints struct {
	// INVARIANT: len(constraints) > 0
	// NB: even if there are no real constraints we have normalized it to have a
	// ConstraintsConjunction with no constraints.
	constraints []roachpb.ConstraintsConjunction

	// There is nothing preventing overlapping []ConstraintsConjunction such
	// that the same store can satisfy multiple (TODO(sumeer): confirm this
	// understanding). This is algorithmically painful since duplicate stores in
	// satisfiedBy slices make it hard to decide what still needs to be
	// satisfied.
	//
	// For simplicity, we assume that if the same store can satisfy multiple
	// ConstraintsConjunctions, they are ordered in spanConfig from most strict
	// to least strict, such that once a store satisfies one conjunction we omit
	// considering it for a later conjunction. That is, we satisfy in a greedy
	// manner versus having to consider all possibilities. So all these
	// []roachpb.StoreID slices have no duplicate StoreIDs across them.
	satisfiedBy [][]roachpb.StoreID

	// These are stores that satisfy no constraint. Even though we are strict
	// about constraint satisfaction, this can happen if the SpanConfig changed
	// or the attributes of a store changed.
	satisfiedNoConstraint []roachpb.StoreID
}

func (ac *analyzedConstraints) hasUnsatisfiedConstraint() bool {
	for i := range ac.constraints {
		if int(ac.constraints[i].NumReplicas)-len(ac.satisfiedBy[i]) > 0 {
			return true
		}
	}
	return false
}

func (ac *analyzedConstraints) hasOversatisfiedConstraint() bool {
	if len(ac.satisfiedNoConstraint) > 0 {
		return true
	}
	for i := range ac.constraints {
		if int(ac.constraints[i].NumReplicas)-len(ac.satisfiedBy[i]) < 0 {
			return true
		}
	}
	return false
}

// Called when hasOversatisfiedConstraint() is true. It will identify the list
// of stores from which the caller can chose one store to either remove (if
// !hasUnsatisfiedContraint()) or move to another store (if
// hasUnsatisfiedConstraint()).
func (ac *analyzedConstraints) candidatesThatOversatisfy() []roachpb.StoreID {
	var stores []roachpb.StoreID
	stores = append(stores, ac.satisfiedNoConstraint...)
	for i := range ac.constraints {
		if int(ac.constraints[i].NumReplicas)-len(ac.satisfiedBy[i]) < 0 {
			stores = append(stores, ac.satisfiedBy[i]...)
		}
	}
	return stores
}

// Can include the leaseholder in the candidates. If the caller thinks
// it is the most attractive one to remove (say because of overload or because
// of locality), it can choose to first transfer the lease.
func voterCandidatesThatOversatisfy(ac *analyzedConstraints, vac *analyzedConstraints) postingList {
	candidates := ac.candidatesThatOversatisfy()
	pl := makePostingList(candidates)
	if vac != nil {
		pl.intersect(makePostingList(vac.candidatesThatOversatisfy()))
		// TODO(sumeer): is it possible that this is empty, and we need to do some
		// range movement to remove the over-satisfaction?
	}
	return pl
}

// rangeAnalyzedConstraints are a function of the spanConfig and the current
// stores that have replicas for that range (including the ReplicaType).
type rangeAnalyzedConstraints struct {
	replicaConstraints *analyzedConstraints
	// Can be nil.
	voterConstraints *analyzedConstraints

	// TODO(sumeer): populate and use.
	//
	// When something about a range changes we compute these
	// analyzedConstraints. At that point we will also compute the diversity
	// score. When there are NON_VOTERS, there are 2 diversity scores, one for
	// voters, and one for all replicas.
	//
	// We haven't placed this in the *analyzedConstraints above since we are
	// rethinking how those are split.
	votersDiversityScore      float64
	allReplicasDiversityScore float64
}

// Helper for constructing rangeAnalyzedConstraints.
//
// We ignore the LEARNER state below, since a badly behaved replica can stay
// in LEARNER for a prolonged period of time, even if the raft group has
// quorum (TODO(sumeer): confirm this understanding). This is usually fine
// since the LEARNER state must be happening because of a pending change, so
// typically we will not be analyzing this range. If the pending change
// expires, this LEARNER state has probably gone on too long and we can no
// longer depend on this LEARNER being useful, so we should up-replicate
// (which is what is likely to happen as a side-effect of ignoring the
// LEARNER).
//
// All ReplicaTypes are translated into VOTER_FULL or NON_VOTER.
type storeIDsForAnalysis struct {
	voters map[roachpb.StoreID]struct{}
	all    map[roachpb.StoreID]struct{}
}

func (s *storeIDsForAnalysis) tryAddingStore(
	storeID roachpb.StoreID, replicaType roachpb.ReplicaType,
) {
	if s.voters == nil {
		s.voters = map[roachpb.StoreID]struct{}{}
	}
	if s.all == nil {
		s.all = map[roachpb.StoreID]struct{}{}
	}
	switch replicaType {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING:
		s.voters[storeID] = struct{}{}
		s.all[storeID] = struct{}{}
	case roachpb.NON_VOTER, roachpb.VOTER_DEMOTING_NON_VOTER:
		s.all[storeID] = struct{}{}
	}
}

// Computes the rangeAnalyzedConstraints for a range.
func (ms *constraintMatchingState) computeAnalyzedConstraints(
	stores storeIDsForAnalysis, spanConfig *spanConfig,
) *rangeAnalyzedConstraints {
	analyzeFunc := func(
		constraints []roachpb.ConstraintsConjunction, stores map[roachpb.StoreID]struct{},
	) *analyzedConstraints {
		if len(constraints) == 0 {
			return nil
		}
		ac := analyzedConstraints{constraints: constraints}
		for i, c := range constraints {
			for storeID := range stores {
				// Does storeID satisfy c?
				if ms.storeMatchesConstraints(storeID, c.Constraints) {
					ac.satisfiedBy[i] = append(ac.satisfiedBy[i], storeID)
					delete(stores, storeID)
					// Don't over-satisfy yet.
					if len(ac.satisfiedBy[i]) == int(constraints[i].NumReplicas) {
						break
					}
				}
			}
		}
		// Typically there will be no stores left in the stores map at this point.
		// But if there are, they can be used to over-satisfy constraints.
		for storeID := range stores {
			satisfied := false
			for i, c := range constraints {
				if ms.storeMatchesConstraints(storeID, c.Constraints) {
					ac.satisfiedBy[i] = append(ac.satisfiedBy[i], storeID)
					delete(stores, storeID)
					satisfied = true
					break
				}
			}
			if !satisfied {
				ac.satisfiedNoConstraint = append(ac.satisfiedNoConstraint, storeID)
			}
		}
		return &ac
	}
	rac := rangeAnalyzedConstraints{}
	rac.replicaConstraints = analyzeFunc(spanConfig.constraints, stores.all)
	rac.voterConstraints = analyzeFunc(spanConfig.voterConstraints, stores.voters)
	return &rac
}

// Tier.Value strings are de-duped in a map and assigned a unique int, so that
// we don't need to do expensive string equality comparisons.
//
// TODO(sumeer): add this map and assignment logic.
type localityTierValueInt int32

type localityTiers []localityTierValueInt

func (l localityTiers) diversityScore(other localityTiers) float64 {
	length := len(l)
	lengthOther := len(other)
	if lengthOther < length {
		length = lengthOther
	}
	for i := 0; i < length; i++ {
		if l[i] != other[i] {
			return float64(length-i) / float64(length)
		}
	}
	if length != lengthOther {
		return roachpb.MaxDiversityScore / float64(length+1)
	}
	return 0
}

func (l localityTiers) string() string {
	// TODO(sumeer): just turn it into <tier>-<tier>-... so can use as a map
	// key.
	return ""
}

type localityState struct {
	tiers localityTiers
	// Output of localityTiers.string() to use as a map key.
	str string
}
