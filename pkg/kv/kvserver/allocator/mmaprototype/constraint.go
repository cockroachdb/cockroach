// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// This file contains helper classes and functions for the allocator related
// to constraint satisfaction, where "constraints" include both replica counts
// and constraints conjunctions. The primary ones are normalizedSpanConfig and
// rangeAnalyzedConstraints.
//
// Other misc pieces: storeSet represents a set of stores, and is
// used here and will be used elsewhere for set operations.
// localityTierInterner is used for interning the tiers to avoid string
// comparisons, used for diversity computation. It will also be used
// elsewhere.
//
// The goal here is to decompose the allocator functionality into modules: the
// constraints "module" does not know about the full set of stores in a
// cluster, and only has a glimpse into those stores via knowing about the
// current set of replica stores for a range and what constraints they
// satisfy. rangeAnalyzedConstraints has various methods to aid in allocator
// decision-making, and these return two kinds of candidates: candidates
// consisting of existing replicas (which can be explicitly enumerated), and
// candidates representing an unknown set that needs to satisfy some
// constraint expression.

// Subset of roachpb.SpanConfig with some normalization.
type normalizedSpanConfig struct {
	numVoters   int32
	numReplicas int32

	// If the corresponding []roachpb.ConstraintsConjunction in the SpanConfig
	// is nil, the one here is also nil. Note, we allow for either or both
	// constraints and voterConstraints to be nil.
	//
	// If non-nil, the []roachpb.ConstraintsConjunction is normalized:
	// - If SpanConfig has one or more ConstraintsConjunction with
	//   NumReplicas=0, they have all been added to the same
	//   ContraintsConjunction slice (since all need to be satisfied). We know
	//   that SpanConfig must only have ConstraintsConjunction with
	//   NumReplicas=0 in this case. The NumReplicas is set to the required
	//   number. So there is no special case of
	//   ConstraintsConjunction.NumReplicas=0
	//
	// - If SpanConfig has ConstraintsConjunctions with NumReplicas != 0, they
	//   are unchanged, but if the sum does not add up to the required number of
	//   replicas, then another ConstraintsConjunction is added with an empty
	//   constraints slice, and the remaining count. This ConstraintsConjunction
	//   with the empty slice is always the last conjunction in this slice.
	//
	// If both voterConstraints and constraints are non-nil, we require that
	// voterConstraints is stricter than constraints. That is, if we satisfy the
	// voterConstraints, any replicas chosen there will satisfy some constraint
	// in constraints, and if no constraint in voterConstraints is
	// oversatisfied, then no constraint in constraints is oversatisfied (when
	// we don't consider any non-voter replicas).
	//
	// It is also highly-encouraged that when there are multiple
	// ConstraintsConjunctions (except for the one we synthesize above with the
	// empty slice) they are structured such that the same store cannot satisfy
	// multiple conjunctions.
	//
	// TODO(sumeer):
	// - For existing clusters this strictness requirement may not be met, so we
	//   will do a structural-normalization to meet this requirement, and if
	//   this structural-normalization is not possible, we will log an error and
	//   not switch the cluster to the new allocator until the operator fixes
	//   their SpanConfigs and retries.

	// constraints applies to all replicas.
	constraints []internedConstraintsConjunction
	// voterConstraints applies to voter replicas.
	voterConstraints []internedConstraintsConjunction
	// Best-effort. Conjunctions are in order of preference, and it is ok if
	// none are satisfied.
	leasePreferences []internedLeasePreference

	// For pretty-printing.
	interner *stringInterner
}

type internedConstraint struct {
	// type captures the kind of constraint this is: required or prohibited.
	typ roachpb.Constraint_Type
	// key captures the locality tag key we're constraining against.
	key stringCode
	// value is the locality tag value we're constraining against.
	value stringCode
}

func (ic internedConstraint) unintern(interner *stringInterner) roachpb.Constraint {
	return roachpb.Constraint{
		Type:  ic.typ,
		Key:   interner.toString(ic.key),
		Value: interner.toString(ic.value),
	}
}

func (ic internedConstraint) less(b internedConstraint) bool {
	if ic.typ != b.typ {
		return ic.typ < b.typ
	}
	if ic.key != b.key {
		return ic.key < b.key
	}
	if ic.value != b.value {
		return ic.value < b.value
	}
	return false
}

// constraints are in increasing order using internedConstraint.less.
type constraintsConj []internedConstraint

// conjunctionRelationship describes the binary relationship between sets of
// stores satisfying two constraint conjunctions. A constraint conjunction
// (constraintsConj) represents all possible sets of stores that satisfy all
// constraints in the conjunction. For example, the conjunction [+region=a,
// +zone=a1] represents all possible sets of stores located in region "a" and
// zone "a1".
//
// This relationship (between two constraintsConj denoted A and B in their
// enum comments) is computed purely by comparing the conjuncts of A and B,
// without knowledge of which stores actually exist in the cluster. In some
// cases, specifically conjEqualSet, conjStrictSubset, conjStrictSuperset,
// these can be understood to hold for any possible concrete set of stores. In
// other cases, specifically conjNonIntersecting and conjPossiblyIntersecting,
// these are a best-effort guess based on the structure of the conjuncts
// alone.
//
// The best-effort relationship computation assumes that if two conjuncts are
// not equal their sets are non-overlapping. This simplifying assumption is ok
// since this relationship feeds into a best-effort structural normalization.
type conjunctionRelationship int

const (
	// conjPossiblyIntersecting: This is an indeterminate case: The structural
	// relationship alone cannot determine how the store sets relate; the actual
	// set of stores in the cluster would need to be known. The store sets may
	// be disjoint, overlapping, or even have a subset relationship, depending
	// on which stores actually exist. In practice, this is the case in which A
	// and B share some constraints, but differ in others.
	//
	// Example: A=[+region=a, +zone=a1], B=[+region=a, -zone=a2].
	//   - if all stores are in a2, B is empty and thus a subset of A.
	//   - if all stores are in a1, A and B are equal.
	//   - if only a1 and a2 have stores, then -zone=a2 is equivalent to +zone=a1,
	//     so A and B are equal.
	//   - if only a2 and a3 have stores, then A is empty and thus a strict
	//     subset of B, which is not empty.
	//   - if a1, a2, and a3 have stores, then A and B intersect but are not equal:
	//     - stores in a1 match both A and B.
	//     - stores in a2 match neither constraint
	//     - stores in a3 match only B
	//     and thus B is a strict subset of A.
	// Note how the relationship between A and B varies based on the actual set of
	// stores: sometimes B contains A, and sometimes the other way around.
	conjPossiblyIntersecting conjunctionRelationship = iota

	// conjEqualSet: A and B have identical constraint lists, so they represent
	// the same set of stores.
	//
	// Example: A=[+region=a, +zone=a1], B=A
	conjEqualSet

	// conjStrictSubset: A has more constraints than B, so A's store set is a
	// strict subset of B's store set. Every store matching A also matches B,
	// but some stores matching B do not match A.
	//
	// Example: A=[+region=a, +zone=a1], B=[+region=a]
	//   A matches stores in region=a AND zone=a1.
	//   B matches all stores in region=a (any zone).
	//   Therefore, A's store set ⊆ B's store set.
	conjStrictSubset

	// conjStrictSuperset: A has fewer constraints than B, so A's store set is a
	// strict superset of B's store set. Every store matching B also matches A,
	// but some stores matching A do not match B.
	//
	// Example: A=[+region=a], B=[+region=a, +zone=a1]
	//   A matches all stores in region=a.
	//   B matches only stores in region=a AND zone=a1.
	//   Therefore, A's store set ⊇ B's store set.
	conjStrictSuperset

	// conjNonIntersecting: This is another indeterminate case. A and B have
	// no constraints in common, so we assume their store sets are disjoint
	// (no store can match both).
	//
	// Example: A=[+region=a], B=[+region=b]
	//   A matches stores in region=a, B matches stores in region=b.
	//   Since a store cannot be in both regions, the sets are disjoint.
	// Example: A=[+region=a], B=[+zone=a1]
	//   If zone=a1 happens to be in region=a, then the disjoint result is
	//   not correct.
	conjNonIntersecting
)

func (cc constraintsConj) relationship(b constraintsConj) conjunctionRelationship {
	n := len(cc)
	m := len(b)
	extraInCC := 0
	extraInB := 0
	inBoth := 0
	for i, j := 0, 0; i < n || j < m; {
		if i >= n {
			extraInB++
			j++
			continue
		}
		if j >= m {
			extraInCC++
			i++
			continue
		}
		if cc[i] == b[j] {
			inBoth++
			i++
			j++
			continue
		}
		if cc[i].less(b[j]) {
			// Found a conjunct that is not in b.
			extraInCC++
			i++
			continue
		} else {
			extraInB++
			j++
			continue
		}
	}
	if extraInCC > 0 && extraInB == 0 {
		return conjStrictSubset
	}
	if extraInB > 0 && extraInCC == 0 {
		return conjStrictSuperset
	}
	// (extraInCC == 0 || extraInB > 0) && (extraInB == 0 || extraInCC > 0)
	// =>
	// (extraInCC == 0 && extraInB == 0) || (extraInB > 0 && extraInCC > 0)
	if extraInCC == 0 && extraInB == 0 {
		return conjEqualSet
	}
	// (extraInB > 0 && extraInCC > 0)
	if inBoth > 0 {
		return conjPossiblyIntersecting
	}
	return conjNonIntersecting
}

type internedConstraintsConjunction struct {
	numReplicas int32
	constraints constraintsConj
}

func (icc internedConstraintsConjunction) unintern(
	interner *stringInterner,
) roachpb.ConstraintsConjunction {
	var cc roachpb.ConstraintsConjunction
	cc.NumReplicas = icc.numReplicas
	for _, c := range icc.constraints {
		cc.Constraints = append(cc.Constraints, c.unintern(interner))
	}
	return cc
}

type internedLeasePreference struct {
	constraints constraintsConj
}

// makeNormalizedSpanConfig is called infrequently, when there is a new
// SpanConfig for which we don't have a normalized value. The rest of the
// allocator code works with normalizedSpanConfig. Due to the infrequent
// nature of this, we don't attempt to reduce memory allocations.
//
// It does:
//
//   - Basic normalization of the constraints and voterConstraints, to specify
//     the number of replicas for every constraints conjunction, and ensure that
//     the sum adds up to the required number of replicas. These are
//     requirements documented in roachpb.SpanConfig. If this basic
//     normalization fails, it returns a nil normalizedSpanConfig and an error.
//
//   - Structural normalization: see comment in doStructuralNormalization. An
//     error in this step causes it to return a non-nil normalizedSpanConfig,
//     with an error, so that the error can be surfaced somehow while keeping
//     the system running.
func makeNormalizedSpanConfig(
	conf *roachpb.SpanConfig, interner *stringInterner,
) (*normalizedSpanConfig, error) {
	numVoters := conf.GetNumVoters()
	var normalizedConstraints, normalizedVoterConstraints []internedConstraintsConjunction
	var err error
	if conf.VoterConstraints != nil {
		normalizedVoterConstraints, err = normalizeConstraints(
			conf.VoterConstraints, numVoters, interner)
		if err != nil {
			return nil, err
		}
	}
	if conf.Constraints != nil {
		normalizedConstraints, err = normalizeConstraints(conf.Constraints, conf.NumReplicas, interner)
		if err != nil {
			return nil, err
		}
	} else if (conf.NumReplicas-numVoters > 0) || len(normalizedVoterConstraints) == 0 {
		// - No constraints, but have some non-voters.
		// - No voter constraints either.
		// Need an empty constraints conjunction so that non-voters or voters have
		// some constraint they can satisfy.
		normalizedConstraints = []internedConstraintsConjunction{
			{
				numReplicas: conf.NumReplicas,
				constraints: nil,
			},
		}
	}
	var lps []internedLeasePreference
	for i := range conf.LeasePreferences {
		lps = append(lps, internedLeasePreference{
			constraints: interner.internConstraintsConj(conf.LeasePreferences[i].Constraints)})
	}
	nConf := &normalizedSpanConfig{
		numVoters:        numVoters,
		numReplicas:      conf.NumReplicas,
		constraints:      normalizedConstraints,
		voterConstraints: normalizedVoterConstraints,
		leasePreferences: lps,
		interner:         interner,
	}
	err = doStructuralNormalization(nConf)
	return nConf, err
}

// normalizeConstraints normalizes and interns the given constraints. Every
// internedConstraintsConjunction has numReplicas > 0, and the sum of these
// equals the parameter numReplicas.
//
// It returns an error if the input constraints don't satisfy the requirements
// documented in roachpb.SpanConfig related to NumReplicas.
func normalizeConstraints(
	constraints []roachpb.ConstraintsConjunction, numReplicas int32, interner *stringInterner,
) ([]internedConstraintsConjunction, error) {
	var nc []roachpb.ConstraintsConjunction
	haveZero := false
	sumReplicas := int32(0)
	for i := range constraints {
		if constraints[i].NumReplicas == 0 {
			haveZero = true
			if len(nc) == 0 {
				nc = append(nc, roachpb.ConstraintsConjunction{})
			}
			// Conjunction of conjunctions, since they all must be satisfied.
			nc[0].Constraints = append(nc[0].Constraints, constraints[i].Constraints...)
		} else {
			sumReplicas += constraints[i].NumReplicas
		}
	}
	if haveZero && sumReplicas > 0 {
		return nil, errors.Errorf("invalid mix of constraints")
	}
	if sumReplicas > numReplicas {
		return nil, errors.Errorf("constraint replicas add up to more than configured replicas")
	}
	if haveZero {
		nc[0].NumReplicas = numReplicas
	} else {
		nc = append(nc, constraints...)
		if sumReplicas < numReplicas {
			cc := roachpb.ConstraintsConjunction{
				NumReplicas: numReplicas - sumReplicas,
				Constraints: nil,
			}
			nc = append(nc, cc)
		}
	}
	var rv []internedConstraintsConjunction
	for i := range nc {
		icc := internedConstraintsConjunction{
			numReplicas: nc[i].NumReplicas,
			constraints: interner.internConstraintsConj(nc[i].Constraints),
		}
		rv = append(rv, icc)
	}
	return rv, nil
}

// Structural normalization establishes relationships between every pair of
// ConstraintsConjunctions in constraints and voterConstraints, and then tries
// to map conjunctions in voterConstraints to narrower conjunctions in
// constraints. This is done to handle configs which under-specify
// conjunctions in voterConstraints under the assumption that one does not
// need to repeat information provided in constraints (see the new
// "strictness" comment in roachpb.SpanConfig which now requires users to
// repeat the information).
//
// This function mutates the conf argument. It does some structural
// normalization even when returning an error. See the under-specified voter
// constraint examples in the datadriven test -- we sometimes see these in
// production settings, and we want to fix ones that we can, and raise an
// error for users to fix their configs.
func doStructuralNormalization(conf *normalizedSpanConfig) error {
	if len(conf.constraints) == 0 || len(conf.voterConstraints) == 0 {
		return nil
	}
	// Relationships between each voter constraint and each all replica
	// constraint.
	type relationshipVoterAndAll struct {
		voterIndex     int
		allIndex       int
		voterAndAllRel conjunctionRelationship
	}
	emptyConstraintIndex := -1
	emptyVoterConstraintIndex := -1
	var rels []relationshipVoterAndAll
	for i := range conf.voterConstraints {
		if len(conf.voterConstraints[i].constraints) == 0 {
			emptyVoterConstraintIndex = i
		}
		for j := range conf.constraints {
			if len(conf.constraints[j].constraints) == 0 {
				emptyConstraintIndex = j
			}
			rels = append(rels, relationshipVoterAndAll{
				voterIndex:     i,
				allIndex:       j,
				voterAndAllRel: conf.voterConstraints[i].constraints.relationship(conf.constraints[j].constraints),
			})
		}
	}
	// Sort these relationships in the order we want to examine them.
	slices.SortFunc(rels, func(a, b relationshipVoterAndAll) int {
		return cmp.Compare(a.voterAndAllRel, b.voterAndAllRel)
	})
	// First are the intersecting constraints, which cause an error (though we
	// proceed with normalization). This is because when there are both shared
	// and non shared conjuncts, we cannot be certain that the conjunctions are
	// non-intersecting. When the conjunctions are intersecting, we cannot
	// promote from one to the other to fill out the set of conjunctions.
	//
	// Example 1: +region=a,+zone=a1 and +region=a,+zone=a2 are classified as
	// conjPossiblyIntersecting, but we could do better in knowing that the
	// conjunctions are non-intersecting since zone=a1 and zone=a2 are disjoint.
	//
	// TODO(sumeer): improve the case of example 1.
	//
	// Example 2: +region=a,+zone=a1 and +region=a,-zone=a2 are classified as
	// conjPossiblyIntersecting. And if there happens to be a zone=a3 in the
	// region, they are actually intersecting. We cannot do better since we
	// don't know the semantics of regions, zones or the universe of possible
	// values of the zone.
	index := 0
	for rels[index].voterAndAllRel == conjPossiblyIntersecting {
		index++
	}
	var err error
	if index > 0 {
		err = errors.Errorf("intersecting conjunctions in constraints and voter constraints")
	}
	// Even if there was an error, we will continue normalization.

	// For each all-replica constraint, track how many replicas are remaining
	// (not already taken by a voter constraint). Additionally, when we find an
	// all replica constraint that is a subset of one or more voter constraints,
	// and create a new voter constraint, we record the index of that new voter
	// constraint in newVoterIndex.
	type allReplicaConstraintsInfo struct {
		remainingReplicas int32
		newVoterIndex     int
	}
	var allReplicaConstraints []allReplicaConstraintsInfo
	for i := range conf.constraints {
		allReplicaConstraints = append(allReplicaConstraints,
			allReplicaConstraintsInfo{
				// Initially all of numReplicas are remaining.
				remainingReplicas: conf.constraints[i].numReplicas,
				newVoterIndex:     -1,
			})
	}
	// For each voter replica constraint, we keep the current
	// internedConstraintsConjunction. The numReplicas start with 0 and build up
	// towards the desired number in the corresponding un-normalized voter
	// constraint. In addition, if the voter constraint had a superset
	// relationship with one or more all-replica constraint, we may take some of
	// its voter count and construct narrower voter constraints.
	// additionalReplicas tracks the total count of replicas in such narrower
	// voter constraints.
	type voterConstraintsAndAdditionalInfo struct {
		internedConstraintsConjunction
		additionalReplicas int32
	}
	var voterConstraints []voterConstraintsAndAdditionalInfo
	for _, constraint := range conf.voterConstraints {
		constraint.numReplicas = 0
		voterConstraints = append(voterConstraints, voterConstraintsAndAdditionalInfo{
			internedConstraintsConjunction: constraint,
		})
	}
	// Now resume iterating from index, and consume all relationships that are
	// conjEqualSet and conjStrictSubset.
	for ; index < len(rels) && rels[index].voterAndAllRel <= conjStrictSubset; index++ {
		rel := rels[index]
		if rel.voterIndex == emptyVoterConstraintIndex {
			// Don't try to satisfy the empty constraint with the corresponding
			// empty constraint since the latter may be needed by some other voter
			// constraint.
			continue
		}
		remainingAll := allReplicaConstraints[rel.allIndex].remainingReplicas
		// NB: we don't bother subtracting
		// voterConstraints[rel.voterIndex].additionalReplicas since it is always
		// 0 at this point in the code.
		neededVoterReplicas := conf.voterConstraints[rel.voterIndex].numReplicas -
			voterConstraints[rel.voterIndex].numReplicas
		if neededVoterReplicas > 0 && remainingAll > 0 {
			// We can satisfy some voter replicas.
			toAdd := remainingAll
			if toAdd > neededVoterReplicas {
				toAdd = neededVoterReplicas
			}
			voterConstraints[rel.voterIndex].numReplicas += toAdd
			allReplicaConstraints[rel.allIndex].remainingReplicas -= toAdd
		}
	}
	// The only relationships remaining are conjStrictSuperset and
	// conjNonIntersecting. We don't care about the latter. conjStrictSuperset
	// can be used to narrow a voter constraint. As a heuristic we try to even
	// out the satisfaction across various constraints. For example, if we have
	// a voter constraint with conjunction c1 that needs 2 more replicas, and we
	// have all constraints with conjuctions:
	// - c1 and c2, with 2 remainingReplicas
	// - c1 and c3, with 2 remainingReplicas
	// instead of greedily adding a voter constraint c1 and c2 with 2 voter
	// replicas, we add both with 1 voter replica each ("load-balancing").
	//
	// Before we do the narrowing, we consider the pair of conjunctions that are
	// empty: emptyVoterConstraintIndex and emptyConstraintIndex. We don't want
	// to narrow unnecessarily, and so if emptyConstraintIndex has some
	// remainingReplicas, we take them here.
	if emptyVoterConstraintIndex > 0 && emptyConstraintIndex > 0 {
		neededReplicas := conf.voterConstraints[emptyVoterConstraintIndex].numReplicas
		actualReplicas := voterConstraints[emptyVoterConstraintIndex].numReplicas
		remaining := neededReplicas - actualReplicas
		if remaining > 0 {
			remainingSatisfiable := allReplicaConstraints[emptyConstraintIndex].remainingReplicas
			if remainingSatisfiable > 0 {
				count := remainingSatisfiable
				if count > remaining {
					count = remaining
				}
				voterConstraints[emptyVoterConstraintIndex].numReplicas += count
				allReplicaConstraints[emptyConstraintIndex].remainingReplicas -= count
			}
		}
	}

	// The aforementioned "load-balancing" of the satisfaction is why we need
	// the outer for loop below.
	//
	// The outer for loop causes repeated iteration over the strict superset
	// relationship. When the inner loop finds a voter constraint that needs
	// more replicas, and the subset conjunction in constraints has some
	// remaining replicas, we replace the weaker voter constraint with the
	// stronger/tighter conjunction for 1 voter. Note that we don't exclude the
	// emptyVoterConstraintIndex for consideration here, since this is exactly
	// the place where we want to see if we can replace it with tighter
	// conjunctions.
	for {
		added := false
		for i := index; i < len(rels) && rels[i].voterAndAllRel == conjStrictSuperset; i++ {
			rel := rels[i]
			remainingAll := allReplicaConstraints[rel.allIndex].remainingReplicas
			neededVoterReplicas := conf.voterConstraints[rel.voterIndex].numReplicas -
				voterConstraints[rel.voterIndex].numReplicas -
				voterConstraints[rel.voterIndex].additionalReplicas
			if neededVoterReplicas > 0 && remainingAll > 0 {
				// Satisfy 1 replica.
				voterConstraints[rel.voterIndex].additionalReplicas++
				allReplicaConstraints[rel.allIndex].remainingReplicas--
				newVoterIndex := allReplicaConstraints[rel.allIndex].newVoterIndex
				if newVoterIndex == -1 {
					// We haven't yet created a narrower voter constraint for this.
					newVoterIndex = len(voterConstraints)
					allReplicaConstraints[rel.allIndex].newVoterIndex = newVoterIndex
					voterConstraints = append(voterConstraints, voterConstraintsAndAdditionalInfo{
						internedConstraintsConjunction: internedConstraintsConjunction{
							numReplicas: 0,
							constraints: conf.constraints[rel.allIndex].constraints,
						},
						additionalReplicas: 0,
					})
				}
				voterConstraints[newVoterIndex].numReplicas++
				added = true
			}
		}
		if !added {
			break
		}
	}
	for i := range conf.voterConstraints {
		neededReplicas := conf.voterConstraints[i].numReplicas
		actualReplicas := voterConstraints[i].numReplicas + voterConstraints[i].additionalReplicas
		if actualReplicas > neededReplicas {
			panic("code bug")
		}
		if actualReplicas < neededReplicas {
			err = errors.Errorf("could not satisfy all voter constraints due to " +
				"non-intersecting conjunctions in voter and all replica constraints")
			// Just force the satisfaction.
			voterConstraints[i].numReplicas += neededReplicas - actualReplicas
		}
	}
	n := len(voterConstraints) - 1
	if emptyVoterConstraintIndex >= 0 && emptyVoterConstraintIndex < n {
		// Move it to the end, since it is the biggest set.
		voterConstraints[emptyVoterConstraintIndex], voterConstraints[n] =
			voterConstraints[n], voterConstraints[emptyVoterConstraintIndex]
	}
	var vc []internedConstraintsConjunction
	for i := range voterConstraints {
		if voterConstraints[i].numReplicas > 0 {
			vc = append(vc, voterConstraints[i].internedConstraintsConjunction)
		}
	}
	conf.voterConstraints = vc

	// We are done with normalizing voter constraints. We also do some basic
	// normalization for constraints: we have seen examples where the
	// constraints are under-specified and give freedom in the choice of
	// constraintsForAddingNonVoter() that isn't actually there. Note that when
	// all the voter constraints are satisfied (which we try to do before
	// satisfying non-voter constraints), then the under-specification does not
	// hurt the choice made in constraintsForAddingNonVoter(). However, we could
	// have situations where an outage of some kind is preventing all the voter
	// constraints from being satisfied, and now we need to place a non-voter --
	// placing that non-voter correctly will avoid the need to move it later
	// when the voter constraint does get satisfied.
	//
	// For example, see the config from #106559 in testdata/normalize_config.
	// The constraints for us-west-1 and us-east-1 are under-specified in
	// needing only 1 replica, while voter constraints specify we need 2
	// replicas in each. Consider if it were left under-specified, and we had
	// only 3 voters, 2 in us-west-1 and 1 in us-east-1 and we were temporarily
	// unable to add a voter in us-east-1. Say we lose the non-voter too, and
	// need to add one. With the under-specified constraint we could add the
	// non-voter anywhere, since we think we are allowed 2 replicas with the
	// empty constraint conjunction. This is technically true, but once we have
	// the required second voter in us-east-1, we will need to move that
	// non-voter to us-central-1, which is wasteful.
	if emptyConstraintIndex >= 0 {
		// Recompute the relationship since voterConstraints have changed.
		emptyVoterConstraintIndex = -1
		rels = rels[:0]
		for i := range conf.voterConstraints {
			if len(conf.voterConstraints[i].constraints) == 0 {
				// We don't actually use emptyVoterConstraintIndex later, but it is
				// harmless to recompute, and will avoid subtle bugs if we change the
				// logic below to start using it.
				emptyVoterConstraintIndex = i
			}
			for j := range conf.constraints {
				rels = append(rels, relationshipVoterAndAll{
					voterIndex: i,
					allIndex:   j,
					voterAndAllRel: conf.voterConstraints[i].constraints.relationship(
						conf.constraints[j].constraints),
				})
			}
		}
		// Sort these relationships in the order we want to examine them.
		slices.SortFunc(rels, func(a, b relationshipVoterAndAll) int {
			return cmp.Compare(a.voterAndAllRel, b.voterAndAllRel)
		})
		// Ignore conjPossiblyIntersecting.
		index = 0
		for rels[index].voterAndAllRel == conjPossiblyIntersecting {
			index++
		}
		voterConstraintHasEqualityWithConstraint := make([]bool, len(conf.voterConstraints))
		// For conjEqualSet, if we can grab from the emptyConstraintIndex, do so.
		for ; index < len(rels) && rels[index].voterAndAllRel <= conjEqualSet; index++ {
			rel := rels[index]
			voterConstraintHasEqualityWithConstraint[rel.voterIndex] = true
			if rel.allIndex == emptyConstraintIndex {
				// rel.voterIndex must be emptyVoterConstraintIndex.
				continue
			}
			if conf.constraints[rel.allIndex].numReplicas < conf.voterConstraints[rel.voterIndex].numReplicas {
				toAddCount := conf.voterConstraints[rel.voterIndex].numReplicas -
					conf.constraints[rel.allIndex].numReplicas
				availableCount := conf.constraints[emptyConstraintIndex].numReplicas
				if availableCount < toAddCount {
					toAddCount = availableCount
				}
				conf.constraints[emptyConstraintIndex].numReplicas -= toAddCount
				conf.constraints[rel.allIndex].numReplicas += toAddCount
			}
		}
		// For conjStrictSubset, if the subset relationship is with
		// emptyConstraintIndex, grab from there.
		for ; index < len(rels) && rels[index].voterAndAllRel <= conjStrictSubset; index++ {
			rel := rels[index]
			if rel.allIndex != emptyConstraintIndex {
				continue
			}
			if voterConstraintHasEqualityWithConstraint[rel.voterIndex] {
				// Already has a corresponding constraint, that we have considered in
				// the previous loop.
				continue
			}
			availableCount := conf.constraints[emptyConstraintIndex].numReplicas
			if availableCount > 0 {
				toAddCount := conf.voterConstraints[rel.voterIndex].numReplicas
				if toAddCount > availableCount {
					toAddCount = availableCount
				}
				conf.constraints[emptyConstraintIndex].numReplicas -= toAddCount
				conf.constraints = append(conf.constraints, internedConstraintsConjunction{
					numReplicas: toAddCount,
					constraints: conf.voterConstraints[rel.voterIndex].constraints,
				})
			}
		}
		// We may have appended to conf.constraints in the previous loop. We like
		// to keep the empty constraint as the last one, so do a swap if needed.
		n := len(conf.constraints) - 1
		if n != emptyConstraintIndex {
			conf.constraints[n], conf.constraints[emptyConstraintIndex] =
				conf.constraints[emptyConstraintIndex], conf.constraints[n]
		}
		// If the empty constraint does not have any replicas due to the
		// normalization, discard it.
		if conf.constraints[n].numReplicas == 0 {
			conf.constraints = conf.constraints[:n]
		}
	}

	return err
}

type replicaKindIndex int32

const (
	voterIndex replicaKindIndex = iota
	nonVoterIndex
	numReplicaKinds
)

// NB: To optimize allocations, try to avoid maps in rangeAnalyzedConstraints,
// analyzedConstraints, and analyzeConstraintsBuf. Slices are easy to reuse.

// analyzedConstraints is the result of processing a normalized
// []roachpb.ConstraintsConjunction from the spanConfig against the current
// set of replicas.
type analyzedConstraints struct {
	// If len(constraints) == 0, there are no constraints, and the satisfiedBy*
	// slices are also empty.
	constraints []internedConstraintsConjunction

	// satisfiedByReplica[kind][i] contains the set of storeIDs that satisfy
	// constraints[i]. These are for stores that satisfy at least one
	// constraint. Each store should appear exactly once in the 2D slice
	// ac.satisfiedByReplica[kind]. For example,
	// satisfiedByReplica[voterIndex][0] = [1, 2, 3], means that the first
	// constraint (constraints[0]) is satisfied by storeIDs 1, 2, and 3.
	//
	// NB: satisfiedByReplica[nonVoterIndex][i] is populated even if this
	// analyzedConstraints represents a voter constraint. This is done because
	// satisfiedByReplica[voterIndex][i] may not sufficiently satisfy a
	// constraint (or even if it does, at a later point one could be
	// decommissioning one of the voters), and populating the nonVoterIndex
	// allows MMA to decide to promote a non-voter.
	//
	// Overlapping conjunctions: There is nothing preventing overlapping
	// ConstraintsConjunctions such that the same store can satisfy multiple,
	// though we expect this to be uncommon. This is algorithmically painful
	// since:
	// - Duplicate stores in satisfiedBy* slices make it hard to decide what
	//   still needs to be satisfied.
	// - When we need to move a replica from s1 (for rebalancing) we want to
	//   cheaply compute which constraint the new store needs to satisfy.
	//   Consider the case where existing replicas s1 and s2 both could satisfy
	//   constraint conjunction cc1 and cc2 each of which needed 1 replica to be
	//   satisfied. Now when trying to find a new store to take the place of s1,
	//   ideally we can consider either cc1 or cc2, since s2 can take the place
	//   of s1 in either constraint conjunction. But considering the
	//   combinations of existing replicas is complicated, so we avoid it.
	//
	// For simplicity, we assume that if the same store can satisfy multiple
	// conjunctions, users have ordered the conjunctions in spanConfig from most
	// strict to least strict, such that once a store satisfies one conjunction
	// we omit considering it for a later conjunction. That is, we satisfy in a
	// greedy manner instead of considering all possibilities. So all the
	// satisfiedBy slices represent sets that are non-intersecting.
	//
	// Example on why ordering from most strict to least strict is important:
	// If the constraints were [region=a,zone=a1:1],[region=a:1] (which is
	// sorted in decreasing strictness), a voter store in (region=a,zone=a1)
	// would satisfy both, but it would be present only in
	// satisfiedByReplica[voterIndex][i] for i=0, not i=1. If the user specified
	// the constraints in irregular order, [region=a]:1,[region=a,zone=a1]:1,
	// the voter would only be present in satisfiedByReplica[voterIndex][0] as
	// well (which now refers to the relaxed constraint), and as a consequence
	// we would consider (region=a,zone=a1) unfulfilled by this voter. If the
	// full constraints conjunction had been [region=a]:1, [region=a,zone=a1]:1,
	// [region=a,zone=a2]:1, and there were voters in that region in zones a1,
	// a2, and a3, we could end up satisfying [region=a]:1 with the voter in a1,
	// but then wouldn't consider it for the stricter constraint
	// [region=a,zone=a1]:1 which no other voter can satisfy. The constraints
	// would then appear unsatisfiable to the allocator.

	satisfiedByReplica [numReplicaKinds][][]roachpb.StoreID

	// ac.satisfiedNoConstraintReplica[kind] contains the set of storeIDs that
	// satisfy no constraint. Even though we are strict about constraint
	// satisfaction, this can happen if the SpanConfig changed or the attributes
	// of a store changed. Note that if these analyzedConstraints correspond to
	// voterConstraints, there can be non-voters here which is never used but
	// harmless.
	satisfiedNoConstraintReplica [numReplicaKinds][]roachpb.StoreID
}

func (ac *analyzedConstraints) clear() {
	ac.constraints = ac.constraints[:0]
	for i := range ac.satisfiedByReplica {
		ac.satisfiedByReplica[i] = clear2DSlice(ac.satisfiedByReplica[i])
		ac.satisfiedNoConstraintReplica[i] = ac.satisfiedNoConstraintReplica[i][:0]
	}
}

func (ac *analyzedConstraints) isEmpty() bool {
	return len(ac.constraints) == 0
}

func extend2DSlice[T any](v [][]T) [][]T {
	n := len(v)
	if cap(v) > n {
		v = v[:n+1]
		v[n] = v[n][:0]
	} else {
		v = append(v, nil)
	}
	return v
}

func clear2DSlice[T any](v [][]T) [][]T {
	for i := range v {
		v[i] = v[i][:0]
	}
	v = v[:0]
	return v
}

// rangeAnalyzedConstraints represents the analyzed constraint state for a
// range, derived from the range's span config and current replica placement
// (including ReplicaType). It contains information used to handle constraint
// satisfaction as part of allocation decisions (e.g. helping identify candidate
// stores for range rebalancing, lease transfers, up-replication,
// down-replication, validating whether constraints are satisfied).
//
// LEARNER and VOTER_DEMOTING_LEARNER replicas are ignored.
type rangeAnalyzedConstraints struct {
	numNeededReplicas [numReplicaKinds]int32
	replicas          [numReplicaKinds][]storeAndLocality
	constraints       analyzedConstraints
	voterConstraints  analyzedConstraints
	// TODO(sumeer): add unit test for these.
	replicaLocalityTiers replicasLocalityTiers
	voterLocalityTiers   replicasLocalityTiers

	// leasePreferenceIndices[i] is the index of the earliest entry in
	// normalizedSpanConfig.leasePreferences, matched by the replica at
	// replicas[voterIndex][i]. If the replica does not match any lease
	// preference, this is set to math.MaxInt32.
	leasePreferenceIndices     []int32
	leaseholderID              roachpb.StoreID
	leaseholderPreferenceIndex int32

	votersDiversityScore   float64
	replicasDiversityScore float64

	spanConfig *normalizedSpanConfig
	buf        analyzeConstraintsBuf
}

func (rac *rangeAnalyzedConstraints) String() string {
	return redact.StringWithoutMarkers(rac)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rac *rangeAnalyzedConstraints) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("leaseholder=%v", rac.leaseholderID)
	w.Print(" voters=[")
	for i := range rac.replicas[voterIndex] {
		if i > 0 {
			w.Print(", ")
		}
		w.Printf("s%v", rac.replicas[voterIndex][i].StoreID)
	}
	w.Print("]")
	w.Print(" non-voters=[")
	for i := range rac.replicas[nonVoterIndex] {
		if i > 0 {
			w.Print(", ")
		}
		w.Printf("s%v", rac.replicas[nonVoterIndex][i].StoreID)
	}
	w.Print("]")
	w.Printf(" req_num_voters=%v req_num_non_voters=%v",
		rac.numNeededReplicas[voterIndex], rac.numNeededReplicas[nonVoterIndex])
}

var rangeAnalyzedConstraintsPool = sync.Pool{
	New: func() interface{} {
		return &rangeAnalyzedConstraints{}
	},
}

func newRangeAnalyzedConstraints() *rangeAnalyzedConstraints {
	return rangeAnalyzedConstraintsPool.Get().(*rangeAnalyzedConstraints)
}

func releaseRangeAnalyzedConstraints(rac *rangeAnalyzedConstraints) {
	rac.constraints.clear()
	rac.voterConstraints.clear()
	rac.buf.clear()
	for i := range rac.replicas {
		rac.replicas[i] = rac.replicas[i][:0]
	}
	rac.leasePreferenceIndices = rac.leasePreferenceIndices[:0]
	*rac = rangeAnalyzedConstraints{
		replicas:         rac.replicas,
		constraints:      rac.constraints,
		voterConstraints: rac.voterConstraints,
		buf:              rac.buf,
	}
	rangeAnalyzedConstraintsPool.Put(rac)
}

// Initialization usage:
//
// Initialization of rangeAnalyzedConstraints is done by first fetching (using
// stateForInit) and initializing the analyzeConstraintsBuf via
// analyzeConstraintsBuf.tryAddingStore, followed by calling finishInit. The
// rangeAnalyzedConstraints should be retrieved from
// rangeAnalyzedConstraintsPool and when no longer needed returned using
// releaseRangeAnalyzedConstraints.
func (rac *rangeAnalyzedConstraints) stateForInit() *analyzeConstraintsBuf {
	return &rac.buf
}

type storeMatchesConstraintInterface interface {
	storeMatches(storeID roachpb.StoreID, constraintConj constraintsConj) bool
}

// initialize takes in the constraints, current replica set, and a constraint
// matcher. It populates ac.constraints, ac.satisfiedByReplica and
// ac.satisfiedNoConstraintReplica by analyzing the current replica set and the
// constraints replicas satisfy. The given buf.replicas is already populated
// with the current replica set from buf.tryAddingStore.
//
// The algorithm proceeds in 3 phases, with the description of each phase
// outlined below at the start of each phase.
//
// NB: ac.initialize guarantees to assign exactly one constraint in
// ac.satisfiedByReplica to each store that satisfies at least one constraint.
// But constraints may remain under-satisfied. If a constraint remains
// under-satisfied after phase 2, it cannot be corrected in phase 3 and remain
// to be under-satisfied. This is not essential to correctness but just happens
// to be true of the algorithm.
//
// TODO(wenyihu6): Add an example for phase 2 - if s1 satisfies c1(num=1) and s2
// satisfies both c1(num=1) and c2(num=1), we should prefer assigning s2 to c2
// rather than consuming it again for c1.
//
// NB: Note that buf.replicaConstraintIndices serves as a scratch space to
// reduce memory allocation and is expected to be empty at the start of every
// analyzedConstraints.initialize call and cleared at the end of the call. For
// every store that satisfies a constraint, we will be clearing the constraint
// indices for that store in buf.replicaConstraintIndices[kind][i] once we have
// assigned it to a constraint in ac.satisfiedByReplica[kind][i]. Since we
// guarantee that each store that satisfies a constraint is assigned to exactly
// one constraint, buf.replicaConstraintIndices will be an empty 2D slice as
// part of the algorithm. In addition, clearing the constraint indices for that
// store in buf.replicaConstraintIndices is also important to help us indicates
// that the store cannot be reused to satisfy a different constraint.
//
// TODO(wenyihu6): add more tests + examples here
func (ac *analyzedConstraints) initialize(
	constraints []internedConstraintsConjunction,
	buf *analyzeConstraintsBuf,
	constraintMatcher storeMatchesConstraintInterface,
) {
	ac.constraints = constraints
	if len(ac.constraints) == 0 {
		// Nothing to do.
		return
	}
	for i := 0; i < len(ac.constraints); i++ {
		ac.satisfiedByReplica[voterIndex] = extend2DSlice(ac.satisfiedByReplica[voterIndex])
		ac.satisfiedByReplica[nonVoterIndex] = extend2DSlice(ac.satisfiedByReplica[nonVoterIndex])
	}
	// In phase 1, for each replica (voter and non-voter), determine which
	// constraint indices in ac.constraints its store satisfies. We record these
	// matches in buf.replicaConstraintIndices by iterating over all replicas,
	// all constraint conjunctions, and checking store satisfaction for each
	// using constraintMatcher.storeMatches. In addition, it also populates
	// ac.satisfiedNoConstraintReplica[kind][i] for stores that satisfy no
	// constraint and ac.satisfiedByReplica[kind][i] for stores that satisfy
	// exactly one constraint. Since we will be assigning at least one
	// constraint to each store, these stores are unambiguous.
	//
	// Compute the list of all constraints satisfied by each store.
	for kind := voterIndex; kind < numReplicaKinds; kind++ {
		for i, store := range buf.replicas[kind] {
			buf.replicaConstraintIndices[kind] =
				extend2DSlice(buf.replicaConstraintIndices[kind])
			for j, c := range ac.constraints {
				if len(c.constraints) == 0 || constraintMatcher.storeMatches(store.StoreID, c.constraints) {
					buf.replicaConstraintIndices[kind][i] =
						append(buf.replicaConstraintIndices[kind][i], int32(j))
				}
			}
			n := len(buf.replicaConstraintIndices[kind][i])
			if n == 0 {
				ac.satisfiedNoConstraintReplica[kind] =
					append(ac.satisfiedNoConstraintReplica[kind], store.StoreID)
			} else if n == 1 {
				// Satisfies exactly one constraint, so place it there.
				constraintIndex := buf.replicaConstraintIndices[kind][i][0]
				ac.satisfiedByReplica[kind][constraintIndex] =
					append(ac.satisfiedByReplica[kind][constraintIndex], store.StoreID)
				buf.replicaConstraintIndices[kind][i] = buf.replicaConstraintIndices[kind][i][:0]
			}
			// Else, satisfied multiple constraints. Don't choose yet.
		}
	}

	// isConstraintSatisfied checks if the given constraint index has been fully
	// satisfied by the stores currently assigned to it.
	// TODO(wenyihu6): voter constraint should only count voters here (#158109)
	isConstraintSatisfied := func(constraintIndex int) bool {
		return len(ac.satisfiedByReplica[voterIndex][constraintIndex])+
			len(ac.satisfiedByReplica[nonVoterIndex][constraintIndex]) >= int(ac.constraints[constraintIndex].numReplicas)
	}

	// In phase 2, for each under-satisfied constraint, iterate through all replicas
	// and assign them to the first store that meets the constraint. Continue until
	// the constraint becomes satisfied, then move on to the next one. Skip further
	// matching once a constraint is fulfilled. Note that this phase does not allow
	// over-satisfaction.
	//
	// The only stores not yet in ac are the ones that satisfy multiple
	// constraints. For each store, the constraint indices it satisfies are in
	// increasing order. Satisfy constraints in order, while not
	// oversatisfying.
	for j := range ac.constraints {
		satisfied := isConstraintSatisfied(j)
		if satisfied {
			continue
		}
		for kind := voterIndex; kind < numReplicaKinds; kind++ {
			for i := range buf.replicaConstraintIndices[kind] {
				constraintIndices := buf.replicaConstraintIndices[kind][i]
				for _, index := range constraintIndices {
					if index == int32(j) {
						ac.satisfiedByReplica[kind][j] =
							append(ac.satisfiedByReplica[kind][j], buf.replicas[kind][i].StoreID)
						buf.replicaConstraintIndices[kind][i] = constraintIndices[:0]
						satisfied = isConstraintSatisfied(j)
						// This store is finished.
						break
					}
				}

				// Check if constraint is now satisfied to avoid
				// over-satisfaction.
				if satisfied {
					break
				}
			}
			// Check if constraint is now satisfied to avoid over-satisfaction.
			if satisfied {
				break
			}
		}
	}
	// In phase 3, for each remaining store that satisfies >1 constraints,
	// assign each to the first constraint it satisfies. This phase allows
	// over-satisfaction
	// (len(ac.satisfiedByReplica[voterIndex][i]+len(ac.satisfiedByReplica[nonVoterIndex][i]))
	// > ac.constraints[i].numReplicas).
	//
	// Nothing over-satisfied. Go and greedily assign.
	for kind := voterIndex; kind < numReplicaKinds; kind++ {
		for i := range buf.replicaConstraintIndices[kind] {
			constraintIndices := buf.replicaConstraintIndices[kind][i]
			for _, index := range constraintIndices {
				ac.satisfiedByReplica[kind][index] =
					append(ac.satisfiedByReplica[kind][index], buf.replicas[kind][i].StoreID)
				buf.replicaConstraintIndices[kind][i] = constraintIndices[:0]
				break
			}
		}
	}
}

// diversityOfTwoStoreSets performs a pairwise diversity score computation between
// the two sets of localities and returns their sum as well as the number of samples.
//
// To simplify the implementation, `this` and `other` must either be disjoint or
// the same set. If they are the same set (sameStores is true), de-duplication
// is performed (to avoid double counting). Otherwise, double-counting will
// occur if there are elements present in both sets, so the sets should be
// disjoint.
//
// For example, when sameStores is false, given two sets of stores [1, 2, 3] and
// [4, 5, 6], diversity score is computed among all pairs (1, 4), (1, 5), (1,
// 6), (2, 4), (2, 5), (2, 6), (3, 4), (3, 5), (3, 6).
//
// When sameStores is true, this and other are the same set and de-duplication
// is needed. For example, given two sets of stores [1, 2, 3] and [1, 2, 3],
// diversity score should be computed among pairs (1, 2), (1, 3), (2, 3) only.
func diversityOfTwoStoreSets(
	this []storeAndLocality, other []storeAndLocality, sameStores bool,
) (sumScore float64, numSamples int) {
	for i := range this {
		s1 := this[i]
		for j := range other {
			s2 := other[j]
			// Only compare pairs of replicas where s2.StoreID > s1.StoreID to avoid
			// computing the diversity score between each pair of stores twice.
			if sameStores && s2.StoreID <= s1.StoreID {
				continue
			}
			sumScore += s1.localityTiers.diversityScore(s2.localityTiers)
			numSamples++
		}
	}
	return sumScore, numSamples
}

// diversityScore measures how geographically spread out replicas are across the
// cluster for a range. Higher scores indicate better fault tolerance because
// replicas are placed further apart in the locality hierarchy.
//
// Returns the average pairwise diversity score between all distinct voter-voter
// pairs and all distinct replica-replica pairs, respectively.
//
// For example, for a range consisting of one voter and one non-voter only,
// voterDiversityScore is zero (there are no distinct voter pairs), but the
// replicaDiversityScore equals the diversity score between the voter and the
// non-voter (there are no distinct non-voter pairs either).
func diversityScore(
	replicas [numReplicaKinds][]storeAndLocality,
) (voterDiversityScore, replicaDiversityScore float64) {
	// Helper to calculate average, or a max-value if no samples (represents
	// lowest possible diversity).
	scoreFromSumAndSamples := func(sumScore float64, numSamples int) float64 {
		if numSamples == 0 {
			return roachpb.MaxDiversityScore
		}
		return sumScore / float64(numSamples)
	}

	voterSum, voterSamples := diversityOfTwoStoreSets(replicas[voterIndex], replicas[voterIndex], true)
	totalSum := voterSum
	totalSamples := voterSamples

	nonVoterSum, nonVoterSamples := diversityOfTwoStoreSets(replicas[nonVoterIndex], replicas[nonVoterIndex], true)
	totalSum += nonVoterSum
	totalSamples += nonVoterSamples

	voterNonVoterSum, voterNonVoterSamples := diversityOfTwoStoreSets(replicas[voterIndex], replicas[nonVoterIndex], false)
	totalSum += voterNonVoterSum
	totalSamples += voterNonVoterSamples
	return scoreFromSumAndSamples(voterSum, voterSamples), scoreFromSumAndSamples(totalSum, totalSamples)
}

// finishInit completes initialization for the rangeAnalyzedConstraints, It
// initializes the constraints and voterConstraints, determines the leaseholder
// preference index for all voter replicas, builds locality tier information,
// and computes diversity scores.
func (rac *rangeAnalyzedConstraints) finishInit(
	spanConfig *normalizedSpanConfig,
	constraintMatcher storeMatchesConstraintInterface,
	leaseholder roachpb.StoreID,
) {
	rac.spanConfig = spanConfig
	rac.numNeededReplicas[voterIndex] = spanConfig.numVoters
	rac.numNeededReplicas[nonVoterIndex] = spanConfig.numReplicas - spanConfig.numVoters
	rac.replicas = rac.buf.replicas

	if spanConfig.constraints != nil {
		rac.constraints.initialize(spanConfig.constraints, &rac.buf, constraintMatcher)
	}
	if spanConfig.voterConstraints != nil {
		rac.voterConstraints.initialize(spanConfig.voterConstraints, &rac.buf, constraintMatcher)
	}

	rac.leaseholderID = leaseholder
	rac.leaseholderPreferenceIndex = -1
	for i := range rac.replicas[voterIndex] {
		storeID := rac.replicas[voterIndex][i].StoreID
		leasePreferenceIndex := matchedLeasePreferenceIndex(
			storeID, spanConfig.leasePreferences, constraintMatcher)
		rac.leasePreferenceIndices = append(rac.leasePreferenceIndices, leasePreferenceIndex)
		if storeID == leaseholder {
			rac.leaseholderPreferenceIndex = leasePreferenceIndex
		}
	}
	if rac.leaseholderPreferenceIndex == -1 {
		// Must be a VOTER_DEMOTING_NON_VOTER, which we count as a non-voter.
		for i := range rac.replicas[nonVoterIndex] {
			storeID := rac.replicas[nonVoterIndex][i].StoreID
			if storeID == leaseholder {
				rac.leaseholderPreferenceIndex = matchedLeasePreferenceIndex(
					storeID, spanConfig.leasePreferences, constraintMatcher)
				break
			}
		}
		if rac.leaseholderPreferenceIndex == -1 {
			panic("leaseholder not found in replicas")
		}
	}

	var replicaLocalityTiers, voterLocalityTiers []localityTiers
	for i := range rac.replicas[voterIndex] {
		replicaLocalityTiers = append(replicaLocalityTiers, rac.replicas[voterIndex][i].localityTiers)
		voterLocalityTiers = append(voterLocalityTiers, rac.replicas[voterIndex][i].localityTiers)
	}
	for i := range rac.replicas[nonVoterIndex] {
		replicaLocalityTiers = append(replicaLocalityTiers, rac.replicas[nonVoterIndex][i].localityTiers)
	}
	rac.replicaLocalityTiers = makeReplicasLocalityTiers(replicaLocalityTiers)
	rac.voterLocalityTiers = makeReplicasLocalityTiers(voterLocalityTiers)

	rac.votersDiversityScore, rac.replicasDiversityScore = diversityScore(rac.replicas)
}

// Disjunction of conjunctions.
type constraintsDisj []constraintsConj

// FNV-1a hash algorithm.
func (cd constraintsDisj) hash() uint64 {
	h := uint64(offset64)
	for i := range cd {
		for _, c := range cd[i] {
			h ^= uint64(c.typ)
			h *= prime64
			h ^= uint64(c.key)
			h *= prime64
			h ^= uint64(c.value)
			h *= prime64
		}
		// Separator between conjunctions.
		h *= prime64
	}
	return h
}

func (cd constraintsDisj) isEqual(b mapKey) bool {
	other := b.(constraintsDisj)
	if len(cd) != len(other) {
		return false
	}
	for i := range cd {
		c1 := cd[i]
		c2 := other[i]
		if len(c1) != len(c2) {
			return false
		}
		for j := range c1 {
			if c1[j] != c2[j] {
				return false
			}
		}
	}
	return true
}

var _ mapKey = constraintsDisj{}

func (rac *rangeAnalyzedConstraints) replicaRole(
	storeID roachpb.StoreID,
) (isVoter bool, isNonVoter bool) {
	for _, s := range rac.replicas[voterIndex] {
		if s.StoreID == storeID {
			return true, false
		}
	}
	for _, s := range rac.replicas[nonVoterIndex] {
		if s.StoreID == storeID {
			return false, true
		}
	}
	return false, false
}

// Usage for a range that may need attention:
//
//				if notEnoughVoters() {
//				  stores := candidatesToConvertFromNonVoterToVoter()
//				  if len(stores) > 0 {
//				    // Pick the candidate that is best for voter diversity
//				    ...
//				  } else {
//			      conjOfDisj, err := constraintsForAddingVoter()
//			      // Use conjOfDisj to prune stores and then add store
//			      // that can handle the load and is best for voter diversity.
//			      ...
//			    }
//				} else if notEnoughNonVoters() {
//		      stores := candidatesToConvertFromVoterToNonVoter()
//		      if len(stores) > 0 {
//		        // Pick the candidate that is best for diversity.
//		      } else {
//		        disj := constraintsForAddingNonVoter()
//		        // Use disj to prune stores and then add store
//		        // that can handle the load and is best for voter diversity.
//		      }
//		    } else {
//          // Have enough replicas of each kind, but not necessarily in the right places.
//          swapCands := candidatesForRoleSwapForConstraints()
//          if len(swapCands[voterIndex]) > 0 {
//            ...
//          }
//          toRemove := candidatesToRemove()
//          if len(toRemove) > 0 {
//            // Have extra voters or non-voters. Remove one.
//            ...
//          } else {
//	          // Have right number of voters and non-voters, but constraints may
//	          // not be satisfied.
//	          storesToRemove, conjOfDisjToAdd := candidatesVoterConstraintsUnsatisfied()
//	          if ...{
//
//		        }
//		        storesToRemove, conjOfDisjToAdd := candidatesNonVoterConstraintsUnsatisfied() {
//	          if ...{
//
//		        }
//          }
//        }
//
// Rebalance:
//   Only if !notEnoughVoters() && !notEnoughNonVoters() && no constraints unsatisfied.
//   cc := candidatesToReplaceVoterForRebalance(storeID)
//   cc := candidatesToReplaceNonVoterForRebalance(storeID)

func (rac *rangeAnalyzedConstraints) notEnoughVoters() bool {
	return len(rac.replicas[voterIndex]) < int(rac.numNeededReplicas[voterIndex])
}

func (rac *rangeAnalyzedConstraints) notEnoughNonVoters() bool {
	return len(rac.replicas[nonVoterIndex]) < int(rac.numNeededReplicas[nonVoterIndex])
}

func (rac *rangeAnalyzedConstraints) expectEnoughNonVoters(enough bool) error {
	if rac.notEnoughNonVoters() == enough {
		return errors.AssertionFailedf(
			"expected enough=%v non-voters but have %d/%d",
			enough, len(rac.replicas[nonVoterIndex]), int(rac.numNeededReplicas[nonVoterIndex]),
		)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectEnoughVoters(enough bool) error {
	if rac.notEnoughVoters() == enough {
		return errors.AssertionFailedf(
			"expected enough=%v voters but have %d/%d",
			enough, len(rac.replicas[voterIndex]), int(rac.numNeededReplicas[voterIndex]),
		)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectEnoughVotersAndNonVoters() error {
	if rac.notEnoughVoters() || rac.notEnoughNonVoters() {
		return errors.AssertionFailedf(
			"expected enough voters and non-voters but have %d/%d voters and %d/%d non-voters",
			len(rac.replicas[voterIndex]), int(rac.numNeededReplicas[voterIndex]),
			len(rac.replicas[nonVoterIndex]), int(rac.numNeededReplicas[nonVoterIndex]),
		)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) voterConstraintCount() (under, match, over int) {
	for i, c := range rac.voterConstraints.constraints {
		neededReplicas := int(c.numReplicas)
		actualReplicas := len(rac.voterConstraints.satisfiedByReplica[voterIndex][i])
		if neededReplicas > actualReplicas {
			under++
		} else if neededReplicas == actualReplicas {
			match++
		} else {
			over++
		}
	}
	return under, match, over
}

func (rac *rangeAnalyzedConstraints) constraintCount() (under, match, over int) {
	for i, c := range rac.constraints.constraints {
		neededReplicas := int(c.numReplicas)
		actualReplicas := len(rac.constraints.satisfiedByReplica[voterIndex][i]) +
			len(rac.constraints.satisfiedByReplica[nonVoterIndex][i])
		if neededReplicas > actualReplicas {
			under++
		} else if neededReplicas == actualReplicas {
			match++
		} else {
			over++
		}
	}
	return under, match, over
}

func (rac *rangeAnalyzedConstraints) expectMatchedConstraints() error {
	if under, match, over := rac.constraintCount(); under > 0 || over > 0 {
		return errors.AssertionFailedf(
			"expected only matched constraints but found "+
				"under=%d match=%d over=%d", under, match, over)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectMatchedVoterConstraints() error {
	if under, match, over := rac.voterConstraintCount(); under > 0 || over > 0 {
		return errors.AssertionFailedf(
			"expected only matched voter constraints but found "+
				"under=%d match=%d over=%d", under, match, over)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectNoUnsatisfied() error {
	if err := rac.expectMatchedConstraints(); err != nil {
		return err
	}
	return rac.expectMatchedVoterConstraints()
}

// REQUIRES: !notEnoughVoters() and !notEnoughNonVoters() and no unsatisfied
// constraint.
func (rac *rangeAnalyzedConstraints) candidatesToReplaceVoterForRebalance(
	storeID roachpb.StoreID,
) (toReplace constraintsConj, err error) {
	if err = rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, err
	}
	if err = rac.expectNoUnsatisfied(); err != nil {
		// Need to address constraint satisfaction first.
		return nil, err
	}

	for i, c := range rac.voterConstraints.constraints {
		// Find the voter constraint which the store being replaced satisfies.
		for _, checkStoreID := range rac.voterConstraints.satisfiedByReplica[voterIndex][i] {
			if checkStoreID == storeID {
				return c.constraints, nil
			}
		}
	}
	for i, c := range rac.constraints.constraints {
		// Find the all-replica constraint which the store being replaced
		// satisfies.
		for _, checkStoreID := range rac.constraints.satisfiedByReplica[voterIndex][i] {
			if checkStoreID == storeID {
				return c.constraints, nil
			}
		}
	}
	return nil,
		errors.Errorf("expected replaced store %d to match a constraint", toReplace)
}

// REQUIRES: !notEnoughVoters() and !notEnoughNonVoters() and no unsatisfied
// constraint.
func (rac *rangeAnalyzedConstraints) candidatesToReplaceNonVoterForRebalance(
	storeID roachpb.StoreID,
) (toReplace constraintsConj, err error) {
	if err = rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, err
	}
	if err = rac.expectNoUnsatisfied(); err != nil {
		// Need to address constraint satisfaction first.
		return nil, err
	}

	for i, c := range rac.constraints.constraints {
		// Find the all-replica constraint which the store being replaced
		// satisfies.
		for _, checkStoreID := range rac.constraints.satisfiedByReplica[nonVoterIndex][i] {
			if checkStoreID == storeID {
				return c.constraints, nil
			}
		}
	}
	return nil,
		errors.Errorf("expected replaced store %d to match a constraint", toReplace)
}

type storeAndLeasePreference struct {
	// Smaller is better.
	leasePreferenceIndex int32
	storeID              roachpb.StoreID
}

// candidatesToMoveLease will return candidates equal to or better than the
// current leaseholder wrt satisfaction of lease preferences. The candidates
// exclude the current leaseholder, and are sorted in non-increasing order of
// lease preference satisfaction.
//
// TODO(sumeer): the computation in this method can be performed once, when
// constructing rangeAnalyzedConstraints.
//
// Should this return the set of candidates which satisfy the first lease
// preference. If none do, the second lease preference, and so on. This
// implies a tighter condition than the current one for candidate selection.
// See existing allocator candidate selection:
// https://github.com/sumeerbhola/cockroach/blob/c4c1dcdeda2c0f38c38270e28535f2139a077ec7/pkg/kv/kvserver/allocator/allocatorimpl/allocator.go#L2980-L2980
// This may be unnecessarily strict and not essential, since it seems many
// users only set one lease preference.
func (rac *rangeAnalyzedConstraints) candidatesToMoveLease() (
	cands []storeAndLeasePreference,
	curLeasePreferenceIndex int32,
) {
	curLeasePreferenceIndex = rac.leaseholderPreferenceIndex
	for i := range rac.leasePreferenceIndices {
		if rac.leasePreferenceIndices[i] <= curLeasePreferenceIndex &&
			rac.replicas[voterIndex][i].StoreID != rac.leaseholderID {
			cands = append(cands, storeAndLeasePreference{
				leasePreferenceIndex: rac.leasePreferenceIndices[i],
				storeID:              rac.replicas[voterIndex][i].StoreID,
			})
		}
	}
	slices.SortFunc(cands, func(a, b storeAndLeasePreference) int {
		return cmp.Compare(a.leasePreferenceIndex, b.leasePreferenceIndex)
	})
	return cands, curLeasePreferenceIndex
}

func isVoter(typ roachpb.ReplicaType) bool {
	return typ == roachpb.VOTER_FULL || typ == roachpb.VOTER_INCOMING
}

func isNonVoter(typ roachpb.ReplicaType) bool {
	return typ == roachpb.NON_VOTER || typ == roachpb.VOTER_DEMOTING_NON_VOTER
}

// Helper for constructing rangeAnalyzedConstraints. Contains initial state
// and intermediate scratch space needed for computing
// rangeAnalyzedConstraints.
//
// All ReplicaTypes are translated into VOTER_FULL or NON_VOTER. We ignore the
// LEARNER state below, since a badly behaved replica can stay in LEARNER for
// a prolonged period of time, even if the raft group has quorum. This is
// usually fine since the LEARNER state must be happening because of a pending
// change, so typically we will not be analyzing this range. If the pending
// change expires, this LEARNER state has probably gone on too long and we can
// no longer depend on this LEARNER being useful, so we should up-replicate
// (which is what is likely to happen as a side-effect of ignoring the
// LEARNER).
//
// We also ignore VOTER_DEMOTING_LEARNER, since it is going away.
//
// TODO(sumeer): the read-only methods should also use this buf to reduce
// allocations, if there is no concurrency.
type analyzeConstraintsBuf struct {
	replicas [numReplicaKinds][]storeAndLocality

	// Scratch space. buf.replicaConstraintIndices[kind][i] contains the
	// constraints indices that the buf.replicas[kind][i] replica satisfies.
	//
	// For example, buf.replicaConstraintIndices[voterIndex][0] = [0, 1, 2],
	// means that the first voter replica (buf.replicas[voterIndex][0])
	// satisfies the 0th, 1st, and 2nd constraint conjunction in ac.constraints.
	//
	// NB: given ac.constraints can represent both replica and voter
	// constraints. We still populate both voter and non-voter indexes because
	// we need to know which constraints are satisfied by all replicas to
	// determine when promotions or demotions should occur.
	//
	// NB: expected to be empty at the start of every
	// analyzedConstraints.initialize call.
	replicaConstraintIndices [numReplicaKinds][][]int32
}

type storeAndLocality struct {
	roachpb.StoreID
	localityTiers
}

func (acb *analyzeConstraintsBuf) clear() {
	for i := range acb.replicas {
		acb.replicas[i] = acb.replicas[i][:0]
		acb.replicaConstraintIndices[i] = clear2DSlice(acb.replicaConstraintIndices[i])
	}
}

func (acb *analyzeConstraintsBuf) tryAddingStore(
	storeID roachpb.StoreID, replicaType roachpb.ReplicaType, locality localityTiers,
) {
	// We ae ignoring LEARNER and VOTER_DEMOTING_LEARNER, since these are
	// in the process of going away.
	//
	// If we are in a joint config with VOTER_DEMOTING_LEARNER, and ignore it
	// here, we may propose another change. This is harmless since
	// Replica.AdminRelocateRange calls
	// Replica.maybeLeaveAtomicChangeReplicasAndRemoveLearners which will remove
	// the VOTER_DEMOTING_LEARNER and exit the joint config first.
	if isVoter(replicaType) {
		acb.replicas[voterIndex] = append(
			acb.replicas[voterIndex], storeAndLocality{storeID, locality})
	} else if isNonVoter(replicaType) {
		acb.replicas[nonVoterIndex] = append(
			acb.replicas[nonVoterIndex], storeAndLocality{storeID, locality})
	}
}

// stringInterner maps locality tiers and constraint strings to unique ints
// (code), so that we don't need to do expensive string equality comparisons.
// There is no removal from this map. It is very unlikely that new localities
// or constraints will be created fast enough for removal to be needed to
// lower memory consumption or to prevent overflow. The empty string is
// assigned code 0.
type stringInterner struct {
	stringToCode map[string]stringCode
	codeToString []string
}

type stringCode uint32

const emptyStringCode stringCode = 0

func newStringInterner() *stringInterner {
	si := &stringInterner{stringToCode: map[string]stringCode{}}
	si.stringToCode[""] = emptyStringCode
	si.codeToString = append(si.codeToString, "")
	return si
}

// lookup is like toCode, but is a pure lookup that won't intern the string.
// Unless it's already interned, returns (0, false), otherwise (*, true).
func (si *stringInterner) lookup(s string) (stringCode, bool) {
	code, ok := si.stringToCode[s]
	return code, ok
}

func (si *stringInterner) toCode(s string) stringCode {
	code, ok := si.stringToCode[s]
	if !ok {
		n := len(si.stringToCode)
		if n == math.MaxUint32 {
			panic("overflowed stringInterner")
		}
		code = stringCode(n)
		si.stringToCode[s] = code
		si.codeToString = append(si.codeToString, s)
	}
	return code
}

func (si *stringInterner) toString(code stringCode) string {
	return si.codeToString[code]
}

func (si *stringInterner) internConstraintsConj(constraints []roachpb.Constraint) constraintsConj {
	if len(constraints) == 0 {
		return nil
	}
	var rv []internedConstraint
	for i := range constraints {
		rv = append(rv, internedConstraint{
			typ:   constraints[i].Type,
			key:   si.toCode(constraints[i].Key),
			value: si.toCode(constraints[i].Value),
		})
	}
	sort.Slice(rv, func(j, k int) bool {
		return rv[j].less(rv[k])
	})
	j := 0
	// De-dup conjuncts in the conjunction.
	for k := 1; k < len(rv); k++ {
		if !(rv[j] == rv[k]) {
			j++
			rv[j] = rv[k]
		}
	}
	rv = rv[:j+1]
	return rv
}

// localityTierInterner maps Tier.Value strings to unique ints, so that we
// don't need to do expensive string equality comparisons.
type localityTierInterner struct {
	si *stringInterner
}

func newLocalityTierInterner(interner *stringInterner) *localityTierInterner {
	return &localityTierInterner{si: interner}
}

func (lti *localityTierInterner) changed(existing localityTiers, current roachpb.Locality) bool {
	if len(existing.tiers) != len(current.Tiers) {
		return true
	}

	for i, tier := range current.Tiers {
		if code, ok := lti.si.lookup(tier.Value); !ok || code != existing.tiers[i] {
			return true
		}
	}
	return false
}

// intern is called occasionally, when we have a new store, or the locality of
// a store changes.
func (lti *localityTierInterner) intern(locality roachpb.Locality) localityTiers {
	var lt localityTiers
	var buf strings.Builder
	for i := range locality.Tiers {
		code := lti.si.toCode(locality.Tiers[i].Value)
		lt.tiers = append(lt.tiers, code)
		fmt.Fprintf(&buf, "%d:", code)
	}
	lt.str = buf.String()
	return lt
}

type localityValues []string

// String formats like =val1,=val2,=val3 or an empty string if the slice
// contains no elements.
func (sl localityValues) String() string {
	if len(sl) == 0 {
		return ""
	}
	return "=" + strings.Join(sl, ",=")
}

// NB: because the tier keys aren't interned, we return a slice of strings
// (locality values) only instead of a half-populated roachpb.Locality.
func (lti *localityTierInterner) unintern(lt localityTiers) localityValues {
	sl := make([]string, 0, len(lt.tiers))
	for _, t := range lt.tiers {
		sl = append(sl, lti.si.toString(t))
	}
	return sl
}

// localityTiers encodes a locality value hierarchy, represented by codes
// from an associated stringInterner.
//
// Note that CockroachDB operators must use matching *keys*[1] across all nodes
// in each deployment, so we only need to deal with a slice of locality
// *values*.
//
// [1]: https://www.cockroachlabs.com/docs/stable/cockroach-start#locality
type localityTiers struct {
	tiers []stringCode
	// str is useful as a map key for caching computations.
	str string
}

// diversityScore returns a score comparing the two localities which ranges from
// 1, meaning completely diverse, to 0 which means not diverse at all (that
// their localities match). This function ignores the locality tier key names
// and only considers differences in their values.
//
// All localities are sorted from most global to most local so any localities
// after any differing values are irrelevant.
//
// While we recommend that all nodes have the same locality keys and same
// total number of keys, there's nothing wrong with having different locality
// keys as long as the immediately next keys are all the same for each value.
// For example:
// region:USA -> state:NY -> ...
// region:USA -> state:WA -> ...
// region:EUR -> country:UK -> ...
// region:EUR -> country:France -> ...
// is perfectly fine. This holds true at each level lower as well.
//
// There is also a need to consider the cases where the localities have
// different lengths. For these cases, we pessimistically treat the missing keys
// on one side as being identical.
func (l localityTiers) diversityScore(other localityTiers) float64 {
	minLen := min(len(l.tiers), len(other.tiers))

	for i := 0; i < minLen; i++ {
		if l.tiers[i] != other.tiers[i] {
			return float64(minLen-i) / float64(minLen)
		}
	}

	// The localities are the same up to the min length; pessimistically treat
	// the missing keys on one side as being identical.
	return 0
}

const notMatchedLeasePreferenceIndex = math.MaxInt32

// matchedLeasePreferenceIndex returns the index of the lease preference that
// matches, else notMatchedLeasePreferenceIndex
func matchedLeasePreferenceIndex(
	storeID roachpb.StoreID,
	leasePreferences []internedLeasePreference,
	constraintMatcher storeMatchesConstraintInterface,
) int32 {
	if len(leasePreferences) == 0 {
		return 0
	}
	for j := range leasePreferences {
		if constraintMatcher.storeMatches(storeID, leasePreferences[j].constraints) {
			return int32(j)
		}
	}
	return notMatchedLeasePreferenceIndex
}

// Avoid unused lint errors.

var _ = normalizedSpanConfig{}
var _ = makeNormalizedSpanConfig
var _ = normalizeConstraints
var _ = analyzedConstraints{}
var _ = rangeAnalyzedConstraints{}
var _ = releaseRangeAnalyzedConstraints
var _ = constraintsDisj{}
var _ = analyzeConstraintsBuf{}
var _ = storeAndLocality{}
var _ = localityTierInterner{}
var _ = localityTiers{}
var _ = storeSet{}

var _ = constraintsDisj{}.hash
var _ = constraintsDisj{}.isEqual
var _ = (&stringInterner{}).toString

func init() {
	var ac analyzedConstraints
	var _ = ac.clear
	var _ = ac.isEmpty

	var rac rangeAnalyzedConstraints
	var _ = rac.stateForInit
	var _ = rac.finishInit
	var _ = rac.notEnoughVoters
	var _ = rac.notEnoughNonVoters
	var _ = rac.candidatesToReplaceVoterForRebalance
	var _ = rac.candidatesToReplaceNonVoterForRebalance

	var acb analyzeConstraintsBuf
	var _ = acb.tryAddingStore
	var _ = acb.clear

	var ltt localityTierInterner
	var _ = ltt.intern
	var lt localityTiers
	var _ = lt.diversityScore

	var pl storeSet
	var _ = pl.union
	var _ = pl.intersect
	var _ = pl.isEqual
	var _ = pl.remove
	var _ = pl.insert
	var _ = pl.contains
	var _ = pl.hash

	var _ = storeAndLocality{StoreID: 0, localityTiers: localityTiers{}}
}
