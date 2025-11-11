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

// REQUIRES: notEnoughVoters()
func (rac *rangeAnalyzedConstraints) candidatesToConvertFromNonVoterToVoter() (
	[]roachpb.StoreID,
	error,
) {
	if err := rac.expectEnoughVoters(false); err != nil {
		return nil, err
	}
	if len(rac.replicas[nonVoterIndex]) == 0 {
		return nil, nil
	}
	var cands []roachpb.StoreID
	if rac.voterConstraints.isEmpty() && !rac.constraints.isEmpty() {
		// There are some constraints, and no voter constraints.
		for i, c := range rac.constraints.constraints {
			if int(c.numReplicas) > len(rac.constraints.satisfiedByReplica[voterIndex][i]) {
				// Unsatisfied when solely looking at voter replica. It is acceptable
				// to add another voter here.
				cands =
					append(cands, rac.constraints.satisfiedByReplica[nonVoterIndex][i]...)
			}
		}
		// NB: it is possible that cands is empty since none of the non-voters
		// satisfy a constraint.
		return cands, nil
	}
	if !rac.voterConstraints.isEmpty() {
		// There are some voter constraints that need satisfaction.
		for i, c := range rac.voterConstraints.constraints {
			if int(c.numReplicas) > len(rac.voterConstraints.satisfiedByReplica[voterIndex][i]) {
				// Unsatisfied.
				cands = append(
					cands, rac.voterConstraints.satisfiedByReplica[nonVoterIndex][i]...)
			}
		}
		// NB: it is possible that cands is empty since none of the non-voters
		// satisfy a constraint.
		return cands, nil
	}
	// No constraints, so all non-voters qualify.
	for i := range rac.replicas[nonVoterIndex] {
		cands = append(cands, rac.replicas[nonVoterIndex][i].StoreID)
	}
	return cands, nil
}

// REQUIRES: notEnoughVoters() and candidatesToConvertFromNonVoterToVoter() is empty.
func (rac *rangeAnalyzedConstraints) constraintsForAddingVoter() (constraintsDisj, error) {
	if err := rac.expectEnoughVoters(false); err != nil {
		return nil, err
	}

	var constrDisj constraintsDisj
	if rac.voterConstraints.isEmpty() && !rac.constraints.isEmpty() {
		// There are some constraints that must not be satisfied since don't have
		// enough voters and could not find a non-voter to convert.
		for i, c := range rac.constraints.constraints {
			if int(c.numReplicas) > len(rac.constraints.satisfiedByReplica[voterIndex][i])+
				len(rac.constraints.satisfiedByReplica[nonVoterIndex][i]) {
				constrDisj = append(constrDisj, c.constraints)
			}
		}
		if len(constrDisj) == 0 {
			return nil, errors.Errorf("could not find unsatisfied constraint")
		}
		return constrDisj, nil
	}
	if !rac.voterConstraints.isEmpty() {
		// There are some constraints that are not satisfied.
		for i, c := range rac.voterConstraints.constraints {
			if int(c.numReplicas) > len(rac.voterConstraints.satisfiedByReplica[voterIndex][i]) {
				// Unsatisfied.
				constrDisj = append(constrDisj, c.constraints)
			}
		}
		if len(constrDisj) == 0 {
			return nil, errors.Errorf("could not find unsatisfied constraint")
		}
		return constrDisj, nil
	}
	return nil, nil
}

// This is only useful when constraints or store attributes change and we can
// more optimally fix things without moving replicas.
//
// TODO(sumeer): do we do this in the current allocator? If not, should we
// bother with this complexity?
//
// REQUIRES: notEnoughNonVoters()
func (rac *rangeAnalyzedConstraints) candidatesToConvertFromVoterToNonVoter() (
	[]roachpb.StoreID,
	error,
) {
	if err := rac.expectEnoughNonVoters(false); err != nil {
		return nil, err
	}
	extraVoters := len(rac.replicas[voterIndex]) - int(rac.numNeededReplicas[voterIndex])
	if extraVoters <= 0 {
		return nil, nil
	}
	if !rac.constraints.isEmpty() &&
		extraVoters <= len(rac.constraints.satisfiedNoConstraintReplica[voterIndex]) {
		// We have voters that satisfy no constraint. Once we get rid of them
		// there will not be extra voters.
		return nil, nil
	}
	var constraintSet []roachpb.StoreID
	constraintSetNeeded := false
	if !rac.constraints.isEmpty() {
		// There are some constraints that need satisfaction.
		constraintSetNeeded = true
		for i, c := range rac.constraints.constraints {
			if int(c.numReplicas) > len(rac.constraints.satisfiedByReplica[nonVoterIndex][i]) {
				// Unsatisfied when solely looking at non-voter replica. It is acceptable
				// to add another non-voter here.
				if len(rac.constraints.satisfiedByReplica[voterIndex][i]) > 0 {
					// Consider losing one of these voters. We don't know yet whether we can
					// surely afford to lose it. It depends on voterConstraints too.
					constraintSet =
						append(constraintSet, rac.constraints.satisfiedByReplica[voterIndex][i]...)
				}
			}
		}
		// NB: constraintSet should never be empty since we confirmed that
		// extraVoters <= len(rac.constraints.satisfiedNoConstraintReplica[voterIndex])
	}
	var voterConstraintSet []roachpb.StoreID
	voterConstraintSetNeeded := false
	if !rac.voterConstraints.isEmpty() {
		// There are some voter constraints that need satisfaction.
		voterConstraintSetNeeded = true
		// These voters are definitely not needed.
		voterConstraintSet = append(
			voterConstraintSet, rac.voterConstraints.satisfiedNoConstraintReplica[voterIndex]...)
		if extraVoters > len(rac.voterConstraints.satisfiedNoConstraintReplica[voterIndex]) {
			// Once we get rid of the voters that satisfy no constraint, there will
			// be still be extra voters. So consider over satisfied constraints.
			for i, c := range rac.voterConstraints.constraints {
				if int(c.numReplicas) < len(rac.voterConstraints.satisfiedByReplica[voterIndex][i]) {
					// Oversatisfied.
					voterConstraintSet = append(
						voterConstraintSet, rac.voterConstraints.satisfiedByReplica[voterIndex][i]...)
				}
			}
		}
		// NB: it is possible that voterConstraintSet is empty.
	}
	if !constraintSetNeeded && !voterConstraintSetNeeded {
		// No constraints, so all voters qualify.
		var voterStores []roachpb.StoreID
		for i := range rac.replicas[voterIndex] {
			voterStores = append(voterStores, rac.replicas[voterIndex][i].StoreID)
		}
		return voterStores, nil
	}
	if !constraintSetNeeded && voterConstraintSetNeeded {
		return voterConstraintSet, nil
	}
	if constraintSetNeeded && !voterConstraintSetNeeded {
		return constraintSet, nil
	}
	cset := makeStoreSet(constraintSet)
	cset.intersect(makeStoreSet(voterConstraintSet))
	return cset, nil
}

// REQUIRES: notEnoughNonVoters() and candidatesToConvertVoterToNonVoter() is empty.
func (rac *rangeAnalyzedConstraints) constraintsForAddingNonVoter() (constraintsDisj, error) {
	if err := rac.expectEnoughNonVoters(false); err != nil {
		return nil, err
	}
	var constrDisj constraintsDisj
	if !rac.constraints.isEmpty() {
		// There are some constraints that are not satisfied since don't have
		// enough non-voters and could not find a voter to convert.
		for i, c := range rac.constraints.constraints {
			if int(c.numReplicas) > len(rac.constraints.satisfiedByReplica[voterIndex][i])+
				len(rac.constraints.satisfiedByReplica[nonVoterIndex][i]) {
				constrDisj = append(constrDisj, c.constraints)
			}
		}
		if len(constrDisj) == 0 {
			return nil, errors.Errorf("could not find unsatisfied constraint")
		}
	}
	return constrDisj, nil
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
			cands = append(cands, rac.replicas[nonVoterIndex][i].StoreID)
		}
		return cands, nil
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
