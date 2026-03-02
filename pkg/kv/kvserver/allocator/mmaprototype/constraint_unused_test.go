// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

func (nconf *normalizedSpanConfig) uninternedConfig() roachpb.SpanConfig {
	var conf roachpb.SpanConfig
	conf.NumReplicas = nconf.numReplicas
	conf.NumVoters = nconf.numVoters
	makeConstraints := func(
		nconstraints []internedConstraintsConjunction) []roachpb.ConstraintsConjunction {
		var rv []roachpb.ConstraintsConjunction
		for _, ncc := range nconstraints {
			cc := ncc.unintern(nconf.interner)
			rv = append(rv, cc)
		}
		return rv
	}
	conf.Constraints = makeConstraints(nconf.constraints)
	conf.VoterConstraints = makeConstraints(nconf.voterConstraints)
	for _, nlp := range nconf.leasePreferences {
		var cc []roachpb.Constraint
		for _, c := range nlp.constraints {
			cc = append(cc, roachpb.Constraint{
				Type:  c.typ,
				Key:   nconf.interner.toString(c.key),
				Value: nconf.interner.toString(c.value),
			})
		}
		conf.LeasePreferences = append(conf.LeasePreferences, roachpb.LeasePreference{Constraints: cc})
	}
	return conf
}

// Constraint unsatisfaction can happen if constraints or store attributes
// changed. The presence of two sets of constraints-conjunctions (CCs),
// complicates matters. Consider CC's A, B, C, that constrain all replicas and
// require 2 replicas each. And consider CC's A', B', C' that constrain voters
// and require 1 replica each. And assume that A' permits the same set as A,
// ... (not that we know that in the code). The current state is 2 non-voters
// in A, 1 voter and 1 non-voter in B, and 2 voters in C. We can't find
// anything unsatisfied in the regular constraints. But have an unsatisfied
// (and oversatisfied) voter constraint. We can decide to satisfy A' and
// remove a voter from C'. But removing a replica from C' will also unsatisfy
// C. What this requires is switching a voter in C to non-voter and switching
// a non-voter in A' to voter.
// REQUIRES: !notEnoughVoters() && !notEnoughNonVoters()
func (rac *rangeAnalyzedConstraints) candidatesForRoleSwapForConstraints() (
	[numReplicaKinds][]roachpb.StoreID,
	error,
) {
	if err := rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return [numReplicaKinds][]roachpb.StoreID{}, err
	}
	// We have enough voters and enough non-voters. We have either the right
	// number of each kind, or may have extra of some kind.
	//
	// Let us consider constraints. It is possible we are unsatisfying some
	// conjunction. If we are unsatisfying a conjunction there are no replicas
	// to deal with this unsatisfaction, otherwise we would have correctly
	// classified them. So there is nothing to do here.
	//
	// Let us consider voterConstraints and conjunctions being unsatisfied.
	// There may be voters oversatisfying some conjunction or satisfying no
	// conjunction. Any of these can be swapped to be a non-voter with no effect
	// on constraints satisfaction. If any unsatisfied conjunction can be
	// satisfied by a non-voter we can make into a voter.
	if rac.voterConstraints.isEmpty() {
		return [numReplicaKinds][]roachpb.StoreID{}, nil
	}
	var swapCands [numReplicaKinds][]roachpb.StoreID
	for i, c := range rac.voterConstraints.constraints {
		neededReplicas := int(c.numReplicas)
		actualVoterReplicas := len(rac.voterConstraints.satisfiedByReplica[voterIndex][i])
		if neededReplicas < actualVoterReplicas {
			// Oversatisfied.
			swapCands[voterIndex] = append(
				swapCands[voterIndex], rac.voterConstraints.satisfiedByReplica[voterIndex][i]...)
		} else if neededReplicas > actualVoterReplicas {
			// Unsatisfied.
			swapCands[nonVoterIndex] = append(
				swapCands[nonVoterIndex], rac.voterConstraints.satisfiedByReplica[nonVoterIndex][i]...)
		}
	}
	swapCands[voterIndex] = append(
		swapCands[voterIndex], rac.voterConstraints.satisfiedNoConstraintReplica[voterIndex]...)
	if len(swapCands[nonVoterIndex]) == 0 {
		swapCands[voterIndex] = nil
	} else if len(swapCands[voterIndex]) == 0 {
		swapCands[nonVoterIndex] = nil
	}
	return swapCands, nil
}

// REQUIRES: !notEnoughVoters() && !notEnoughNonVoters()
func (rac *rangeAnalyzedConstraints) candidatesToRemove() ([]roachpb.StoreID, error) {
	if err := rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, err
	}

	var cands []roachpb.StoreID
	if len(rac.replicas[nonVoterIndex]) > int(rac.numNeededReplicas[nonVoterIndex]) {
		if !rac.constraints.isEmpty() {
			for i, c := range rac.constraints.constraints {
				if int(c.numReplicas) < len(rac.constraints.satisfiedByReplica[voterIndex][i])+
					len(rac.constraints.satisfiedByReplica[nonVoterIndex][i]) {
					// Oversatisfied. Can remove a non-voter.
					cands = append(cands, rac.constraints.satisfiedByReplica[nonVoterIndex][i]...)
				}
			}
			cands = append(cands, rac.constraints.satisfiedNoConstraintReplica[nonVoterIndex]...)
			// It is possible that cands is empty since all non-voters may be
			// getting used to satisfy some constraint as voters may not be
			// satisfying any constraint, or oversatisfying some constraint. This is
			// ok -- we don't want to remove these non-voters.
		} else {
			// No constraints. Can remove any non-voter.
			for i := range rac.replicas[nonVoterIndex] {
				cands = append(cands, rac.replicas[nonVoterIndex][i].StoreID)
			}
		}
		if len(cands) > 0 {
			return cands, nil
		}
	}
	if len(rac.replicas[voterIndex]) > int(rac.numNeededReplicas[voterIndex]) {
		if !rac.voterConstraints.isEmpty() {
			for i, c := range rac.voterConstraints.constraints {
				if int(c.numReplicas) < len(rac.voterConstraints.satisfiedByReplica[voterIndex][i]) {
					// Oversatisfied. Can remove a voter.
					cands = append(
						cands, rac.voterConstraints.satisfiedByReplica[voterIndex][i]...)
				}
			}
			cands = append(
				cands, rac.voterConstraints.satisfiedNoConstraintReplica[voterIndex]...)
			return cands, nil
		}
		if !rac.constraints.isEmpty() {
			for i, c := range rac.constraints.constraints {
				if int(c.numReplicas) < len(rac.constraints.satisfiedByReplica[voterIndex][i])+
					len(rac.constraints.satisfiedByReplica[nonVoterIndex][i]) {
					// Oversatisfied. Can remove a voter.
					cands = append(cands, rac.constraints.satisfiedByReplica[voterIndex][i]...)
				}
			}
			cands = append(cands, rac.constraints.satisfiedNoConstraintReplica[voterIndex]...)
			return cands, nil
		}
		// No constraints. Can remove any voter.
		for i := range rac.replicas[voterIndex] {
			cands = append(cands, rac.replicas[voterIndex][i].StoreID)
		}
		panic("impossible to be here")
	}
	return nil, nil
}

// REQUIRES: !notEnoughVoters() and !notEnoughNonVoters()
func (rac *rangeAnalyzedConstraints) candidatesVoterConstraintsUnsatisfied() (
	toRemoveVoters []roachpb.StoreID,
	toAdd constraintsDisj,
	err error,
) {
	if err := rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, nil, err
	}
	if rac.voterConstraints.isEmpty() {
		// There are some constraints, and no voter constraints.
		//
		// Only need to remove a voter if some conjunction is oversatisfied purely
		// due to voters. If there is a non-voter there too, move it first.
		for i, c := range rac.constraints.constraints {
			neededReplicas := int(c.numReplicas)
			actualVoterReplicas := len(rac.constraints.satisfiedByReplica[voterIndex][i])
			actualNonVoterReplicas := len(rac.constraints.satisfiedByReplica[nonVoterIndex][i])
			if neededReplicas > actualVoterReplicas+actualNonVoterReplicas {
				toAdd = append(toAdd, c.constraints)
			} else if neededReplicas < actualVoterReplicas {
				toRemoveVoters = append(
					toRemoveVoters, rac.constraints.satisfiedByReplica[voterIndex][i]...)
			}
		}
		// Always include the voters which are satisfying no constraints as
		// candidates to remove.
		toRemoveVoters = append(
			toRemoveVoters, rac.constraints.satisfiedNoConstraintReplica[voterIndex]...)
	} else {
		for i, c := range rac.voterConstraints.constraints {
			neededReplicas := int(c.numReplicas)
			actualVoterReplicas := len(rac.voterConstraints.satisfiedByReplica[voterIndex][i])
			if neededReplicas > actualVoterReplicas {
				toAdd = append(toAdd, c.constraints)
			} else if neededReplicas < actualVoterReplicas {
				toRemoveVoters = append(
					toRemoveVoters, rac.voterConstraints.satisfiedByReplica[voterIndex][i]...)
			}
		}
		// Always include the voters which are satisfying no voter constraints as
		// candidates to remove.
		toRemoveVoters = append(
			toRemoveVoters, rac.voterConstraints.satisfiedNoConstraintReplica[voterIndex]...)
	}
	if len(toRemoveVoters) == 0 {
		toAdd = nil
	} else if len(toAdd) == 0 {
		toRemoveVoters = nil
	}
	return toRemoveVoters, toAdd, nil
}

// REQUIRES: !notEnoughVoters() and !notEnoughNonVoters()
func (rac *rangeAnalyzedConstraints) candidatesNonVoterConstraintsUnsatisfied() (
	toRemoveNonVoters []roachpb.StoreID,
	toAdd constraintsDisj,
	err error,
) {
	if err = rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, nil, err
	}
	if !rac.constraints.isEmpty() {
		// If some conjunction is oversatisfied, include all non-voters which satisfy
		// the constraint as candidates to be removed. If there are not enough
		// replicas to satisfy an all-replica constraint, include the constraint for
		// toAdd.
		for i, c := range rac.constraints.constraints {
			neededReplicas := int(c.numReplicas)
			actualReplicas := len(rac.constraints.satisfiedByReplica[voterIndex][i]) +
				len(rac.constraints.satisfiedByReplica[nonVoterIndex][i])
			if neededReplicas > actualReplicas {
				toAdd = append(toAdd, c.constraints)
			} else if neededReplicas < actualReplicas {
				toRemoveNonVoters = append(toRemoveNonVoters,
					rac.constraints.satisfiedByReplica[nonVoterIndex][i]...)
			}
		}
	}
	// Always include the non-voters which are satisfying no constraints as
	// candidates to remove.
	toRemoveNonVoters = append(
		toRemoveNonVoters, rac.constraints.satisfiedNoConstraintReplica[nonVoterIndex]...)
	if len(toRemoveNonVoters) == 0 {
		toAdd = nil
	} else if len(toAdd) == 0 {
		toRemoveNonVoters = nil
	}
	return toRemoveNonVoters, toAdd, nil
}

// removeStore is called for a store that is removed from the cluster.
func (cm *constraintMatcher) removeStore(storeID roachpb.StoreID) {
	mc := cm.stores[storeID]
	if mc == nil {
		return
	}
	cm.allStores.remove(storeID)
	delete(cm.stores, storeID)
	for c := range mc.matched {
		matchedSet := cm.constraints[c]
		if matchedSet == nil {
			panic(errors.AssertionFailedf(
				"inconsistent state: store %d not found", storeID))
		}
		found := matchedSet.remove(storeID)
		if !found {
			panic(errors.AssertionFailedf(
				"inconsistent state: store %d not found", storeID))
		}
	}
}

func (cm *constraintMatcher) checkConsistency() error {
	for storeID, mc := range cm.stores {
		for c := range mc.matched {
			pl, ok := cm.constraints[c]
			if !ok {
				return errors.AssertionFailedf("constraint not found")
			}
			if !pl.contains(storeID) {
				return errors.AssertionFailedf("constraint set does not include storeID %d", storeID)
			}
		}
	}
	for c, pl := range cm.constraints {
		for _, storeID := range pl.storeSet {
			store, ok := cm.stores[storeID]
			if !ok {
				return errors.AssertionFailedf("constraint set mentions unknown storeID %d", storeID)
			}
			_, ok = store.matched[c]
			if !ok {
				return errors.AssertionFailedf("stores and constraints map are out of sync")
			}
		}
	}
	return nil
}
