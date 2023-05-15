// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import (
	"fmt"
	"math"
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
	// - Document the above strictness requirement in roachpb.SpanConfig.
	//
	// - For existing clusters this strictness requirement may not be met, so we
	//   will do a structural-normalization to meet this requirement, and if
	//   this structural-normalization is not possible, we will log an error and
	//   not switch the cluster to the new allocator until the operator fixes
	//   their SpanConfigs and retries.
	//
	// - Write the code for this structural-normalization. It will establish
	//   subset or non-intersecting relationships between every pair of
	//   ConstraintsConjunctions, and then try to map CC's in replicaConstraints
	//   to the containing set in constraints and vice-versa to (a) check for no
	//   over-satisfaction, (b) split sets in replicaConstraints into subsets
	//   based on CC's in constraints. See
	//   https://cockroachlabs.slack.com/archives/D0367JZG864/p1679064458668199
	//   for an example where we need to do the latter.

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

type internedConstraintsConjunction struct {
	numReplicas int32
	constraints []internedConstraint
}

type internedLeasePreference struct {
	constraints []internedConstraint
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
	if conf.Constraints != nil {
		normalizedConstraints, err = normalizeConstraints(conf.Constraints, conf.NumReplicas, interner)
		if err != nil {
			return nil, err
		}
	}
	if conf.VoterConstraints != nil {
		normalizedVoterConstraints, err = normalizeConstraints(
			conf.VoterConstraints, conf.NumVoters, interner)
		if err != nil {
			return nil, err
		}
	}
	var lps []internedLeasePreference
	for i := range conf.LeasePreferences {
		lps = append(lps, internedLeasePreference{
			constraints: interner.internConstraints(conf.LeasePreferences[i].Constraints)})
	}
	return &normalizedSpanConfig{
		numVoters:        conf.NumVoters,
		numReplicas:      conf.NumReplicas,
		constraints:      normalizedConstraints,
		voterConstraints: normalizedVoterConstraints,
		leasePreferences: lps,
		interner:         interner,
	}, nil
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
		for i := range constraints {
			nc = append(nc, constraints[i])
		}
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
		rv = append(rv, internedConstraintsConjunction{
			numReplicas: nc[i].NumReplicas,
			constraints: interner.internConstraints(nc[i].Constraints),
		})
	}
	return rv, nil
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
	storeMatches(storeID roachpb.StoreID, constraintConj []internedConstraint) bool
}

func (rac *rangeAnalyzedConstraints) finishInit(
	spanConfig *normalizedSpanConfig, constraintMatcher storeMatchesConstraintInterface,
) {
	rac.numNeededReplicas[voterIndex] = spanConfig.numVoters
	rac.numNeededReplicas[nonVoterIndex] = spanConfig.numReplicas - spanConfig.numVoters
	rac.replicas = rac.buf.replicas

	analyzeFunc := func(ac *analyzedConstraints) {
		if len(ac.constraints) == 0 {
			// Nothing to do.
			return
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
								append(ac.satisfiedByReplica[kind][j], rac.replicas[voterIndex][i].StoreID)
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
						append(ac.satisfiedByReplica[kind][index], rac.replicas[voterIndex][i].StoreID)
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
type constraintsDisj []internedConstraintsConjunction

// FNV-1a hash algorithm.
func (cd constraintsDisj) hash() uint64 {
	h := uint64(offset64)
	for i := range cd {
		h ^= uint64(cd[i].numReplicas)
		h *= prime64
		for _, c := range cd[i].constraints {
			h ^= uint64(c.typ)
			h *= prime64
			h ^= uint64(c.key)
			h *= prime64
			h ^= uint64(c.value)
			h *= prime64
		}
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
		if cd[i].numReplicas != other[i].numReplicas {
			return false
		}
		c1 := cd[i].constraints
		c2 := other[i].constraints
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
			"expected enough=%v non-voters but have: %d actual and %d needed",
			enough, len(rac.replicas[nonVoterIndex]), int(rac.numNeededReplicas[nonVoterIndex]),
		)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectEnoughVoters(enough bool) error {
	if rac.notEnoughVoters() == enough {
		return errors.AssertionFailedf(
			"expected enough=%v voters but have: %d actual and %d needed",
			enough, len(rac.replicas[voterIndex]), int(rac.numNeededReplicas[voterIndex]),
		)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectEnoughVotersAndNonVoters() error {
	if rac.notEnoughVoters() || rac.notEnoughNonVoters() {
		return errors.AssertionFailedf(
			"expected enough voters and non-voters but have: voters %d/%d non-voters %d/%d",
			len(rac.replicas[voterIndex]), int(rac.numNeededReplicas[voterIndex]),
			len(rac.replicas[nonVoterIndex]), int(rac.numNeededReplicas[nonVoterIndex]),
		)
	}
	return nil
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
				constrDisj = append(constrDisj, c)
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
				constrDisj = append(constrDisj, c)
			}
		}
		if len(constrDisj) == 0 {
			return nil, errors.Errorf("could not find unsatisfied constraint")
		}
		return constrDisj, nil
	}
	return nil, nil
}

// options
// 1. manually craft an error each time and use notEnoughVoters/notEnoughVoters
// - a lot of duplication
// 2. use separate func() error for voter and non-voter
// - when we want to assert on both, we need to ignore one error or wrap

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
				constrDisj = append(constrDisj, c)
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
	if rac.voterConstraints.isEmpty() && rac.constraints.isEmpty() {
		return nil, nil, nil
	}

	if rac.voterConstraints.isEmpty() && !rac.constraints.isEmpty() {
		// There are some constraints, and no voter constraints.
		//
		// Only need to remove a voter if some conjunction is oversatisfied purely
		// due to voters. If there is a non-voter there too, move it first.
		for i, c := range rac.constraints.constraints {
			neededReplicas := int(c.numReplicas)
			actualVoterReplicas := len(rac.constraints.satisfiedByReplica[voterIndex][i])
			actualNonVoterReplicas := len(rac.constraints.satisfiedByReplica[nonVoterIndex][i])
			if neededReplicas > actualVoterReplicas+actualNonVoterReplicas {
				toAdd = append(toAdd, c)
			} else if neededReplicas < actualVoterReplicas {
				toRemoveVoters = append(
					toRemoveVoters, rac.constraints.satisfiedByReplica[voterIndex][i]...)
			}
		}
		// Always include the voters which are satisfying no constraints as
		// candidates to remove.
		toRemoveVoters = append(
			toRemoveVoters, rac.constraints.satisfiedNoConstraintReplica[voterIndex]...)
	} else if !rac.voterConstraints.isEmpty() {
		for i, c := range rac.voterConstraints.constraints {
			neededReplicas := int(c.numReplicas)
			actualVoterReplicas := len(rac.voterConstraints.satisfiedByReplica[voterIndex][i])
			if neededReplicas > actualVoterReplicas {
				toAdd = append(toAdd, c)
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
	if err := rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, nil, err
	}
	// We can ignore voter constraints.
	if rac.constraints.isEmpty() {
		return nil, nil, nil
	}
	// If some conjunction is oversatisfied, include all non-voters which satisfy
	// the constraint as candidates to be removed. If there are not enough
	// replicas to satisfy an all-replica constraint, include the constraint for
	// toAdd.
	for i, c := range rac.constraints.constraints {
		neededReplicas := int(c.numReplicas)
		actualReplicas := len(rac.constraints.satisfiedByReplica[voterIndex][i]) +
			len(rac.constraints.satisfiedByReplica[nonVoterIndex][i])
		if neededReplicas > actualReplicas {
			toAdd = append(toAdd, c)
		} else if neededReplicas < actualReplicas {
			toRemoveNonVoters = append(toRemoveNonVoters,
				rac.constraints.satisfiedByReplica[nonVoterIndex][i]...)
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
) (toReplace constraintsDisj, err error) {
	if err := rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, err
	}

	if !rac.voterConstraints.isEmpty() {
		for i, c := range rac.voterConstraints.constraints {
			// Find the first voter-constraint which the removing voter store
			// satisfies and is necessary to satisfy the constraint. i.e. without the
			// store, a voter constraint is undersatisfied.
			neededVoterReplicas := int(c.numReplicas)
			actualVoterReplicas := len(rac.voterConstraints.satisfiedByReplica[voterIndex][i])
			for _, checkStoreID := range rac.constraints.satisfiedByReplica[voterIndex][i] {
				if checkStoreID == storeID && neededVoterReplicas >= actualVoterReplicas {
					return append(toReplace, c), nil
				}
			}
		}
		return nil, nil
	}

	if rac.voterConstraints.isEmpty() && !rac.constraints.isEmpty() {
		for i, c := range rac.constraints.constraints {
			// Find the first all-replica constraint which the removing voter store
			// satisfies and is necessary to satisfy the constraint.
			neededReplicas := int(c.numReplicas)
			actualReplicas := len(rac.constraints.satisfiedByReplica[voterIndex][i]) +
				len(rac.constraints.satisfiedByReplica[nonVoterIndex][i])
			for _, checkStoreID := range rac.constraints.satisfiedByReplica[voterIndex][i] {
				if checkStoreID == storeID && neededReplicas >= actualReplicas {
					return append(toReplace, c), nil
				}
			}
		}
	}

	// If there were no voter or all-replica constraints to replace it implies
	// the range has an oversatisfied constraint, which in turn means there is an
	// undersatisfied constraint. Don't attempt to handle under/over
	// satifisfaction in rebalancing.
	return nil, errors.Errorf("expected no unsatisfied constraints")
}

// REQUIRES: !notEnoughVoters() and !notEnoughNonVoters() and no unsatisfied
// constraint.
func (rac *rangeAnalyzedConstraints) candidatesToReplaceNonVoterForRebalance(
	storeID roachpb.StoreID,
) (toReplace constraintsDisj, err error) {
	if err := rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, err
	}

	if !rac.constraints.isEmpty() {
		// Find the first all-replica constraint which the removing non-voter store
		// satisfies and is also necessary.
		for i, c := range rac.constraints.constraints {
			neededReplicas := int(c.numReplicas)
			actualReplicas := len(rac.constraints.satisfiedByReplica[voterIndex][i]) +
				len(rac.constraints.satisfiedByReplica[nonVoterIndex][i])
			for _, checkStoreID := range rac.constraints.satisfiedByReplica[nonVoterIndex][i] {
				if checkStoreID == storeID && neededReplicas >= actualReplicas {
					return append(toReplace, c), nil
				}
			}
		}
	}

	// If there were no constraints to replace it implies the range has an
	// oversatisfied constraint, which in turn means there is an undersatisfied
	// constraint. Don't attempt to handle under/over satifisfaction in
	// rebalancing.
	return nil, errors.Errorf("expected no unsatisfied constraints")
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

func (si *stringInterner) internConstraints(constraints []roachpb.Constraint) []internedConstraint {
	var rv []internedConstraint
	for i := range constraints {
		rv = append(rv, internedConstraint{
			typ:   constraints[i].Type,
			key:   si.toCode(constraints[i].Key),
			value: si.toCode(constraints[i].Value),
		})
	}
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
	sort.Sort(storeIDIncreasing(a))
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
		sort.Sort(storeIDIncreasing(a))
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
	for pos := range a {
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
		if pos > 0 {
			copy(b[:pos-1], a[:pos-1])
		}
	}
	copy(b[pos+1:n+1], a[pos:n])
	b[pos] = storeID
	*s = b
	return true
}

func (s *storeIDPostingList) contains(storeID roachpb.StoreID) bool {
	n := len(*s)
	index := sort.Search(n, func(i int) bool { return (*s)[i] >= storeID })
	return index != n && (*s)[index] == storeID
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

type storeIDIncreasing []roachpb.StoreID

func (s storeIDIncreasing) Len() int {
	return len(s)
}

func (s storeIDIncreasing) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s storeIDIncreasing) Less(i, j int) bool {
	return s[i] < s[j]
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
var _ = storeIDIncreasing{}

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
