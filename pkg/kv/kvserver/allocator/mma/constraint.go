// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

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
)

// This file contains helper classes and functions for the allocator related
// to constraint satisfaction, where "constraints" include both replica counts
// and constraints conjunctions. The primary ones are normalizedSpanConfig and
// rangeAnalyzedConstraints.
//
// Other misc pieces: storeIDPostingList represents a set of stores, and is
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

type conjunctionRelationship int

// Relationship between conjunctions used for structural normalization. This
// relationship is solely defined based on the conjunctions, and not based on
// what stores actually match. It simply assumes that if two conjuncts are not
// equal their sets are non-overlapping. This simplifying assumption is made
// since we are only trying to do a best-effort structural normalization.
const (
	conjIntersecting conjunctionRelationship = iota
	conjEqualSet
	conjStrictSubset
	conjStrictSuperset
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
		return conjIntersecting
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
func makeNormalizedSpanConfig(
	conf *roachpb.SpanConfig, interner *stringInterner,
) (*normalizedSpanConfig, error) {
	var normalizedConstraints, normalizedVoterConstraints []internedConstraintsConjunction
	var err error
	if conf.VoterConstraints != nil {
		normalizedVoterConstraints, err = normalizeConstraints(
			conf.VoterConstraints, conf.NumVoters, interner)
		if err != nil {
			return nil, err
		}
	}
	if conf.Constraints != nil {
		normalizedConstraints, err = normalizeConstraints(conf.Constraints, conf.NumReplicas, interner)
		if err != nil {
			return nil, err
		}
	} else if (conf.NumReplicas-conf.NumVoters > 0) || len(normalizedVoterConstraints) == 0 {
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
		numVoters:        conf.NumVoters,
		numReplicas:      conf.NumReplicas,
		constraints:      normalizedConstraints,
		voterConstraints: normalizedVoterConstraints,
		leasePreferences: lps,
		interner:         interner,
	}
	return doStructuralNormalization(nConf)
}

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
// This function does some structural normalization even when returning an
// error. See the under-specified voter constraint examples in the datadriven
// test -- we sometimes see these in production settings, and we want to fix
// ones that we can, and raise an error for users to fix their configs.
func doStructuralNormalization(conf *normalizedSpanConfig) (*normalizedSpanConfig, error) {
	if len(conf.constraints) == 0 || len(conf.voterConstraints) == 0 {
		return conf, nil
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
	// First are the intersecting constraints, which cause an error.
	index := 0
	for rels[index].voterAndAllRel == conjIntersecting {
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
		// Ignore conjIntersecting.
		index = 0
		for rels[index].voterAndAllRel == conjIntersecting {
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

	return conf, err
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

	satisfiedByReplica [numReplicaKinds][][]roachpb.StoreID

	// These are stores that satisfy no constraint. Even though we are strict
	// about constraint satisfaction, this can happen if the SpanConfig changed
	// or the attributes of a store changed. Additionally, if these
	// analyzedConstraints correspond to voterConstraints, there can be
	// non-voters here (which is harmless).
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

// rangeAnalyzedConstraints is a function of the spanConfig and the current
// stores that have replicas for that range (including the ReplicaType).
type rangeAnalyzedConstraints struct {
	numNeededReplicas [numReplicaKinds]int32
	replicas          [numReplicaKinds][]storeAndLocality
	constraints       analyzedConstraints
	voterConstraints  analyzedConstraints

	// leasePreferenceIndices[i] is the index of the earliest entry in
	// normalizedSpanConfig.leasePreferences, matched by the replica at
	// replicas[voterIndex][i]. If the replica does not match any lease
	// preference, this is set to math.MaxInt32.
	leasePreferenceIndices     []int32
	leaseholderID              roachpb.StoreID
	leaseholderPreferenceIndex int32

	votersDiversityScore   float64
	replicasDiversityScore float64

	buf analyzeConstraintsBuf
}

var rangeAnalyzedConstraintsPool = sync.Pool{
	New: func() interface{} {
		return &rangeAnalyzedConstraints{}
	},
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

func (rac *rangeAnalyzedConstraints) finishInit(
	spanConfig *normalizedSpanConfig,
	constraintMatcher storeMatchesConstraintInterface,
	leaseholder roachpb.StoreID,
) {
	rac.numNeededReplicas[voterIndex] = spanConfig.numVoters
	rac.numNeededReplicas[nonVoterIndex] = spanConfig.numReplicas - spanConfig.numVoters
	rac.replicas = rac.buf.replicas

	analyzeFunc := func(ac *analyzedConstraints) {
		if len(ac.constraints) == 0 {
			// Nothing to do.
			return
		}
		for i := 0; i < len(ac.constraints); i++ {
			ac.satisfiedByReplica[voterIndex] = extend2DSlice(ac.satisfiedByReplica[voterIndex])
			ac.satisfiedByReplica[nonVoterIndex] = extend2DSlice(ac.satisfiedByReplica[nonVoterIndex])
		}
		// Compute the list of all constraints satisfied by each store.
		for kind := voterIndex; kind < numReplicaKinds; kind++ {
			for i, store := range rac.buf.replicas[kind] {
				rac.buf.replicaConstraintIndices[kind] =
					extend2DSlice(rac.buf.replicaConstraintIndices[kind])
				for j, c := range ac.constraints {
					if len(c.constraints) == 0 || constraintMatcher.storeMatches(store.StoreID, c.constraints) {
						rac.buf.replicaConstraintIndices[kind][i] =
							append(rac.buf.replicaConstraintIndices[kind][i], int32(j))
					}
				}
				n := len(rac.buf.replicaConstraintIndices[kind][i])
				if n == 0 {
					ac.satisfiedNoConstraintReplica[kind] =
						append(ac.satisfiedNoConstraintReplica[kind], store.StoreID)
				} else if n == 1 {
					// Satisfies exactly one constraint, so place it there.
					constraintIndex := rac.buf.replicaConstraintIndices[kind][i][0]
					ac.satisfiedByReplica[kind][constraintIndex] =
						append(ac.satisfiedByReplica[kind][constraintIndex], store.StoreID)
					rac.buf.replicaConstraintIndices[kind][i] = rac.buf.replicaConstraintIndices[kind][i][:0]
				}
				// Else, satisfied multiple constraints. Don't choose yet.
			}
		}
		// The only stores not yet in ac are the ones that satisfy multiple
		// constraints. For each store, the constraint indices it satisfies are in
		// increasing order. Satisfy constraints in order, while not
		// oversatisfying.
		for j := range ac.constraints {
			doneFunc := func() bool {
				return len(ac.satisfiedByReplica[voterIndex][j])+
					len(ac.satisfiedByReplica[nonVoterIndex][j]) >= int(ac.constraints[j].numReplicas)
			}
			done := doneFunc()
			if done {
				continue
			}
			for kind := voterIndex; kind < numReplicaKinds; kind++ {
				for i := range rac.buf.replicaConstraintIndices[kind] {
					constraintIndices := rac.buf.replicaConstraintIndices[kind][i]
					for _, index := range constraintIndices {
						if index == int32(j) {
							ac.satisfiedByReplica[kind][j] =
								append(ac.satisfiedByReplica[kind][j], rac.replicas[kind][i].StoreID)
							rac.buf.replicaConstraintIndices[kind][i] = constraintIndices[:0]
							done = doneFunc()
							// This store is finished.
							break
						}
					}
					// done can be true if some store was appended to
					// ac.satisfiedByReplica[kind][j] and made it fully satisfied. Don't
					// need to look at other stores for this constraint.
					if done {
						break
					}
				}
				// done can be true if some store was appended to
				// ac.satisfiedByReplica[kind][j] and made it fully satisfied. Don't
				// need to look at other stores for this constraint.
				if done {
					break
				}
			}
		}
		// Nothing over-satisfied. Go and greedily assign.
		for kind := voterIndex; kind < numReplicaKinds; kind++ {
			for i := range rac.buf.replicaConstraintIndices[kind] {
				constraintIndices := rac.buf.replicaConstraintIndices[kind][i]
				for _, index := range constraintIndices {
					ac.satisfiedByReplica[kind][index] =
						append(ac.satisfiedByReplica[kind][index], rac.replicas[kind][i].StoreID)
					rac.buf.replicaConstraintIndices[kind][i] = constraintIndices[:0]
					break
				}
			}
		}
	}
	if spanConfig.constraints != nil {
		rac.constraints.constraints = spanConfig.constraints
		analyzeFunc(&rac.constraints)
	}
	if spanConfig.voterConstraints != nil {
		rac.voterConstraints.constraints = spanConfig.voterConstraints
		analyzeFunc(&rac.voterConstraints)
	}

	rac.leaseholderID = leaseholder
	rac.leaseholderPreferenceIndex = -1
	matchLeasePreferenceFunc := func(storeID roachpb.StoreID) int32 {
		for j := range spanConfig.leasePreferences {
			if constraintMatcher.storeMatches(storeID, spanConfig.leasePreferences[j].constraints) {
				return int32(j)
			}
		}
		return math.MaxInt32
	}
	for i := range rac.replicas[voterIndex] {
		storeID := rac.replicas[voterIndex][i].StoreID
		leasePreferenceIndex := matchLeasePreferenceFunc(storeID)
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
				rac.leaseholderPreferenceIndex = matchLeasePreferenceFunc(storeID)
				break
			}
		}
		if rac.leaseholderPreferenceIndex == -1 {
			panic("leaseholder not found in replicas")
		}
	}

	diversityFunc := func(
		stores1 []storeAndLocality, stores2 []storeAndLocality, sameStores bool,
	) (sumScore float64, numSamples int) {
		for i := range stores1 {
			s1 := stores1[i]
			for j := range stores2 {
				s2 := stores2[j]
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
	scoreFromSumAndSamples := func(sumScore float64, numSamples int) float64 {
		if numSamples == 0 {
			return roachpb.MaxDiversityScore
		}
		return sumScore / float64(numSamples)
	}
	sumVoterScore, numVoterSamples := diversityFunc(
		rac.replicas[voterIndex], rac.replicas[voterIndex], true)
	rac.votersDiversityScore = scoreFromSumAndSamples(sumVoterScore, numVoterSamples)

	sumReplicaScore, numReplicaSamples := sumVoterScore, numVoterSamples
	srs, nrs := diversityFunc(rac.replicas[nonVoterIndex], rac.replicas[nonVoterIndex], true)
	sumReplicaScore += srs
	numReplicaSamples += nrs
	srs, nrs = diversityFunc(rac.replicas[voterIndex], rac.replicas[nonVoterIndex], false)
	sumReplicaScore += srs
	numReplicaSamples += nrs
	rac.replicasDiversityScore = scoreFromSumAndSamples(sumReplicaScore, numReplicaSamples)
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
	cset := makeStoreIDPostingList(constraintSet)
	cset.intersect(makeStoreIDPostingList(voterConstraintSet))
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
// TODO(sumeer): the read-only methods should also use this buf to reduce
// allocations, if there is no concurrency.
type analyzeConstraintsBuf struct {
	replicas [numReplicaKinds][]storeAndLocality

	// Scratch space. replicaConstraintIndices[k][i] is the constraint matching
	// state for replicas[k][i].
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
	switch replicaType {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING:
		acb.replicas[voterIndex] = append(
			acb.replicas[voterIndex], storeAndLocality{storeID, locality})
	case roachpb.NON_VOTER, roachpb.VOTER_DEMOTING_NON_VOTER:
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

func (lti *localityTierInterner) unintern(lt localityTiers) roachpb.Locality {
	var locality roachpb.Locality
	for _, t := range lt.tiers {
		locality.Tiers = append(locality.Tiers, roachpb.Tier{Value: lti.si.toString(t)})
	}
	return locality
}

type localityTiers struct {
	tiers []stringCode
	// str is useful as a map key for caching computations.
	str string
}

func (l localityTiers) diversityScore(other localityTiers) float64 {
	length := len(l.tiers)
	lengthOther := len(other.tiers)
	if lengthOther < length {
		length = lengthOther
	}
	for i := 0; i < length; i++ {
		if l.tiers[i] != other.tiers[i] {
			return float64(length-i) / float64(length)
		}
	}
	if length != lengthOther {
		return roachpb.MaxDiversityScore / float64(length+1)
	}
	return 0
}

// Ordered and de-duped list of storeIDs. Represents a set of stores. Used for
// fast set operations for constraint satisfaction.
type storeIDPostingList []roachpb.StoreID

func makeStoreIDPostingList(a []roachpb.StoreID) storeIDPostingList {
	slices.Sort(a)
	return a
}

func (s *storeIDPostingList) union(b storeIDPostingList) {
	a := *s
	n := len(a)
	m := len(b)
	for i, j := 0, 0; j < m; {
		if i < n && a[i] < b[j] {
			i++
			continue
		}
		// i >= n || a[i] >= b[j]
		if i >= n || a[i] > b[j] {
			a = append(a, b[j])
			j++
			continue
		}
		// a[i] == b[j]
		i++
		j++
	}
	if len(a) > n {
		slices.Sort(a)
		*s = a
	}
}

func (s *storeIDPostingList) intersect(b storeIDPostingList) {
	// TODO(sumeer): For larger lists, probe using smaller list.
	a := *s
	n := len(a)
	m := len(b)
	k := 0
	for i, j := 0, 0; i < n && j < m; {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			a[k] = a[i]
			i++
			j++
			k++
		}
	}
	*s = a[:k]
}

func (s *storeIDPostingList) isEqual(b storeIDPostingList) bool {
	a := *s
	n := len(a)
	m := len(b)
	if n != m {
		return false
	}
	for i := range b {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Returns true iff found (and successfully removed).
func (s *storeIDPostingList) remove(storeID roachpb.StoreID) bool {
	a := *s
	n := len(a)
	found := false
	for i := range a {
		if a[i] == storeID {
			// INVARIANT: i < n, so i <= n-1 and i+1 <= n.
			copy(a[i:n-1], a[i+1:n])
			found = true
			break
		}
	}
	if !found {
		return false
	}
	*s = a[:n-1]
	return true
}

// Returns true iff the storeID was not already in the set.
func (s *storeIDPostingList) insert(storeID roachpb.StoreID) bool {
	a := *s
	n := len(a)
	var pos int
	for pos = 0; pos < n; pos++ {
		if storeID < a[pos] {
			break
		} else if storeID == a[pos] {
			return false
		}
	}
	var b storeIDPostingList
	if cap(a) > n {
		b = a[:n+1]
	} else {
		m := 2 * cap(a)
		const minLength = 10
		if m < minLength {
			m = minLength
		}
		b = make([]roachpb.StoreID, n+1, m)
		// Insert at pos, so pos-1 is the last element before the insertion.
		if pos > 0 {
			copy(b[:pos], a[:pos])
		}
	}
	copy(b[pos+1:n+1], a[pos:n])
	b[pos] = storeID
	*s = b
	return true
}

func (s *storeIDPostingList) contains(storeID roachpb.StoreID) bool {
	_, found := slices.BinarySearch(*s, storeID)
	return found
}

const (
	// offset64 is the initial hash value, and is taken from fnv.go
	offset64 = 14695981039346656037

	// prime64 is a large-ish prime number used in hashing and taken from fnv.go.
	prime64 = 1099511628211
)

// FNV-1a hash algorithm.
func (s *storeIDPostingList) hash() uint64 {
	h := uint64(offset64)
	for _, storeID := range *s {
		h ^= uint64(storeID)
		h *= prime64
	}
	return h
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
var _ = storeIDPostingList{}

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
	var _ = rac.candidatesToConvertFromNonVoterToVoter
	var _ = rac.constraintsForAddingVoter
	var _ = rac.notEnoughNonVoters
	var _ = rac.candidatesToConvertFromVoterToNonVoter
	var _ = rac.constraintsForAddingNonVoter
	var _ = rac.candidatesForRoleSwapForConstraints
	var _ = rac.candidatesToRemove
	var _ = rac.candidatesVoterConstraintsUnsatisfied
	var _ = rac.candidatesNonVoterConstraintsUnsatisfied
	var _ = rac.candidatesToReplaceVoterForRebalance
	var _ = rac.candidatesToReplaceNonVoterForRebalance

	var acb analyzeConstraintsBuf
	var _ = acb.tryAddingStore
	var _ = acb.clear

	var ltt localityTierInterner
	var _ = ltt.intern
	var lt localityTiers
	var _ = lt.diversityScore

	var pl storeIDPostingList
	var _ = pl.union
	var _ = pl.intersect
	var _ = pl.isEqual
	var _ = pl.remove
	var _ = pl.insert
	var _ = pl.contains
	var _ = pl.hash

	var _ = storeAndLocality{StoreID: 0, localityTiers: localityTiers{}}
}
