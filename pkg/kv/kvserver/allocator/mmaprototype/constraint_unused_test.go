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
