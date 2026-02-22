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

// SafeFormat implements the redact.SafeFormatter interface.
func (conf *normalizedSpanConfig) SafeFormat(w redact.SafePrinter, _ rune) {
	if conf == nil {
		w.SafeString("<nil>")
		return
	}
	w.Printf("numVoters=%v numReplicas=%v constraints=%v voterConstraints=%v",
		redact.SafeInt(conf.numVoters), redact.SafeInt(conf.numReplicas),
		redact.SafeInt(len(conf.constraints)), redact.SafeInt(len(conf.voterConstraints)))
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

// cmp compares two internedConstraint, returning -1 if ic < b, 0 if ic == b,
// and 1 if ic > b.
//
// This ordering has no semantic meaning regarding strictness or set
// containment. It is purely lexicographic: first by typ, then key, then value.
// Its purpose is to enable merge-based comparison of two sorted constraint
// lists in relationship.
//
// Example: +region=a < -region=b because Required(0) < Prohibited(1). This does
// not imply that +region=a is stricter than -region=b. They simply mean that
// they are two distinct constraints.
func (ic internedConstraint) cmp(b internedConstraint) int {
	if ic.typ != b.typ {
		return cmp.Compare(ic.typ, b.typ)
	}
	if ic.key != b.key {
		return cmp.Compare(ic.key, b.key)
	}
	return cmp.Compare(ic.value, b.value)
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
	// Example: A=[+region=a, +zone=a1], B=[+region=a, +zone=a2]
	//   Since a store cannot be in both zones, the sets are disjoint.
	conjNonIntersecting
)

// relationship returns the logical relationship between two sets of constraints
// (represented as sorted, de-duplicated conjunctions).
func (cc constraintsConj) relationship(b constraintsConj) conjunctionRelationship {
	n := len(cc)
	m := len(b)
	extraInCC := 0 // conjuncts in cc but not in b
	extraInB := 0  // conjuncts in b but not in cc
	inBoth := 0    // conjuncts present in both

	// We are merging two lists that are already sorted based on less(). When we
	// see cc[i] < b[j], it simply means cc[i] comes earlier in the order defined
	// by less. Since b is also sorted, cc[i] can’t appear anywhere later in b.
	// That tells us that cc[i] is only in cc and not in b.
	//
	// Example: cc = [A, C, E] and b = [B, D, E] (where A < B < C < D < E):
	//
	// | Step | i | j | cc[i] | b[j] | Comparison | Action           | extraInCC | extraInB | inBoth |
	// |------|---|---|-------|------|------------|-----------------------------|-----------|----------|--------|
	// | 1    | 0 | 0 | A     | B    | A < B      | extraInCC++, i++ | 1         | 0        | 0      |
	// | 2    | 1 | 0 | C     | B    | C > B      | extraInB++, j++  | 1         | 1        | 0      |
	// | 3    | 1 | 1 | C     | D    | C < D      | extraInCC++, i++ | 2         | 1        | 0      |
	// | 4    | 2 | 1 | E     | D    | E > D      | extraInB++, j++  | 2         | 2        | 0      |
	// | 5    | 2 | 2 | E     | E    | E == E     | inBoth++, i++,j++| 2         | 2        | 1      |
	//
	// i and j are indices into cc and b respectively
	for i, j := 0, 0; i < n || j < m; {
		// If we've reached the end of cc, remaining items are only in b.
		if i >= n {
			extraInB++
			j++
			continue
		}
		// If we've reached the end of b, remaining items are only in cc.
		if j >= m {
			extraInCC++
			i++
			continue
		}
		// If both conjuncts are identical, increment inBoth.
		if cc[i] == b[j] {
			inBoth++
			i++
			j++
			continue
		}
		// If the type and key are the same but value differs,
		// then these constraints are non-intersecting.
		// Example: +zone=a1, +zone=a2 (disjoint)
		//
		if cc[i].typ == b[j].typ && cc[i].key == b[j].key {
			// For example, +zone=a1, +zone=a2.
			return conjNonIntersecting
			// NB: +zone=a1 and -zone=a1 are also non-intersecting, but we will
			// not detect this case. Finding this case requires searching through
			// b, and not simply walking in order, since the typ field is the
			// first in the sort order and differs between these two conjuncts.
		}
		// If cc[i] < b[j], we've found a conjunct unique to cc.
		if cc[i].cmp(b[j]) < 0 {
			extraInCC++
			i++
			continue
		} else {
			// Otherwise, found a conjunct unique to b.
			extraInB++
			j++
			continue
		}
	}

	// There are four possibilities:
	// 1. extraInCC > 0 and extraInB == 0: cc is a strict subset of b.
	if extraInCC > 0 && extraInB == 0 {
		return conjStrictSubset
	}
	// 2. extraInB > 0 and extraInCC == 0: cc is a strict superset of b.
	if extraInB > 0 && extraInCC == 0 {
		return conjStrictSuperset
	}

	// 3. extraInCC == 0 and extraInB == 0: sets are equal.
	if extraInCC == 0 && extraInB == 0 {
		return conjEqualSet
	}

	// 4. extraInCC > 0 and extraInB > 0:
	//    a) if inBoth > 0, sets may possibly intersect.
	if inBoth > 0 {
		return conjPossiblyIntersecting
	}
	//    b) if inBoth == 0, sets are disjoint.
	return conjNonIntersecting
}

func (rel conjunctionRelationship) SafeFormat(w redact.SafePrinter, _ rune) {
	switch rel {
	case conjPossiblyIntersecting:
		w.SafeString("possiblyIntersecting")
	case conjEqualSet:
		w.SafeString("equalSet")
	case conjStrictSubset:
		w.SafeString("strictSubset")
	case conjStrictSuperset:
		w.SafeString("strictSuperset")
	case conjNonIntersecting:
		w.SafeString("nonIntersecting")
	default:
		w.Printf("unknown(%d)", rel)
	}
}

func (rel conjunctionRelationship) String() string {
	return redact.StringWithoutMarkers(rel)
}

// internedConstraintsConjunction represents a single
// roachpb.ConstraintsConjunction in an interned form. Interning assigns unique
// integer codes to constraint keys and values, reducing memory usage and
// speeding up comparisons.
type internedConstraintsConjunction struct {
	numReplicas int32
	// De-duped and sorted using internedConstraint.less.
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

// cmp compares two constraintsConj, returning -1 if cc < b, 0 if cc == b, and 1
// if cc > b. Note that this does not perform a semantic comparison and just
// represents a sorting order.
func (cc constraintsConj) cmp(b constraintsConj) int {
	n := min(len(cc), len(b))
	for i := 0; i < n; i++ {
		cmp := cc[i].cmp(b[i])
		if cmp != 0 {
			return cmp
		}
	}
	if n < len(cc) {
		return +1
	}
	if n < len(b) {
		return -1
	}
	return 0
}

// dedupAndFilterConstraints filters out constraint conjunctions with
// numReplicas == 0 and combines duplicate conjunctions (those with the same
// constraints) by summing their numReplicas.
// Example:
// constraints: [+region=us-west-1]: 1, [+region=us-west-1]: 1, []: 3, []: 1,
// [+region=eu]: 0
// result: [+region=us-west-1]: 2, []: 4
// TODO(wenyihu6): we could take in a scratch space to avoid allocating indices
func dedupAndFilterConstraints(
	constraints []internedConstraintsConjunction,
) []internedConstraintsConjunction {
	n := len(constraints)
	if n == 0 {
		return constraints
	}
	// indices represents the positions in the original constraints.
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	// Sort the indices such that equal constraints get grouped and within a
	// sequence of equal constraints, the smallest index sorts first. The latter
	// allows us to preserve the original ordering after de-duplication.
	slices.SortFunc(indices, func(i, j int) int {
		return cmp.Or(constraints[i].constraints.cmp(constraints[j].constraints),
			cmp.Compare(i, j))
	})
	// j is the index into the updated indices slice, where the value
	// is the index of the original constraint that is preserved.
	j := 0
	// Say the original indices slice is the following, where the parentheses
	// represent equal constraints: (3, 5), (1, 4), (0, 2). At the end, we will
	// have 3, 1, 0 in the slice.
	for i := 1; i < n; i++ {
		if constraints[indices[j]].constraints.cmp(constraints[indices[i]].constraints) == 0 {
			constraints[indices[j]].numReplicas += constraints[indices[i]].numReplicas
		} else {
			j++
			indices[j] = indices[i]
		}
	}
	indices = indices[:j+1]
	// Sort these indices in increasing order to preserve original ordering.
	slices.Sort(indices)
	// Copy to result, filtering out numReplicas == 0.
	k := 0
	for _, idx := range indices {
		if constraints[idx].numReplicas != 0 {
			constraints[k] = constraints[idx]
			k++
		}
	}
	return constraints[:k]
}

type internedLeasePreference struct {
	constraints constraintsConj
}

// makeBasicNormalizedSpanConfig performs the first stage of normalization for
// SpanConfigs: it interns constraints and ensures every conjunction has
// numReplicas > 0 with the sum equaling the required number of replicas (adding
// an empty constraint if needed). It does not perform structural normalization.
// See makeNormalizedSpanConfig for more details.
func makeBasicNormalizedSpanConfig(
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
	return nConf, nil
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
	return makeNormalizedSpanConfigWithObserver(conf, interner, nil)
}

// NormalizationPhaseObserver is an optional callback invoked after each phase
// of structural normalization. It allows tests to observe intermediate states
// without coupling to internal implementation details.
type NormalizationPhaseObserver func(phaseName string, conf *normalizedSpanConfig)

// makeNormalizedSpanConfigWithObserver is like makeNormalizedSpanConfig but
// accepts an optional observer to inspect intermediate normalization states.
// The observer is called after basic normalization (phase "basic") and after
// each phase of structural normalization.
func makeNormalizedSpanConfigWithObserver(
	conf *roachpb.SpanConfig, interner *stringInterner, observer NormalizationPhaseObserver,
) (*normalizedSpanConfig, error) {
	nConf, err := makeBasicNormalizedSpanConfig(conf, interner)
	if err != nil {
		return nil, err
	}
	if observer != nil {
		observer("basic", nConf)
	}
	err = nConf.doStructuralNormalization(observer)
	if observer != nil {
		observer("after", nConf)
	}
	return nConf, err
}

// normalizeConstraints normalizes and interns the input constraint
// conjunctions, returning a slice of []internedConstraintsConjunction along
// with an error if the input violates the requirements documented in
// roachpb.SpanConfig. The given numReplicas corresponds to
// roachpb.SpanConfig.NumReplicas (when used for replica constraints) or
// roachpb.SpanConfig.NumVoters (when used for voter constraints).
//
// Normalization guarantees that the returned []internedConstraintsConjunction
// satisfies the following:
// 1. Every internedConstraintsConjunction has NumReplicas > 0.
// 2. The sum of the NumReplicas of the conjunctions sum up to the given
// numReplicas parameter.
// 3. If any conjunction specifies zero replicas (and thus all conjunctions must
// have NumReplicas = 0), it synthesizes a single conjunction with NumReplicas
// equal to the given numReplicas parameter. (Note: Zero-replica constraints
// implies that the constraint is applied to all numReplicas, and are allowed
// only if all conjunctions are zero-replica.)
//
// It returns an (nil, error) if the input violates the roachpb.SpanConfig rules:
//   - Sum of all input NumReplicas must be ≤ the given numReplicas.
//     E.g. +region=a:1 and +region=b:1 with numReplicas = 1 is invalid.
//   - If any conjunction has NumReplicas = 0, all conjunctions must have
//     NumReplicas = 0. Example: +region=a:0 and +region=b,zone=b1:1 is invalid
//     but +region=a:0 and +region=b,zone=b1:0 is valid.
//
// Caller should check for errors and return it to users. These are cases where
// the span configuration is invalid. Caller cannot ignore the error.
//
// TODO(wenyihu6): we should see what checks can be lifted up to the zone config
// validation stage.
func normalizeConstraints(
	constraints []roachpb.ConstraintsConjunction, numReplicas int32, interner *stringInterner,
) ([]internedConstraintsConjunction, error) {
	haveZero := false
	sumReplicas := int32(0)

	var zrc roachpb.ConstraintsConjunction
	// Combine zero-replica constraints into a single conjunction on nc[0] if
	// there are any.
	for i := range constraints {
		if constraints[i].NumReplicas == 0 {
			haveZero = true
			// Conjunction of conjunctions, since they all must be satisfied.
			zrc.Constraints = append(zrc.Constraints, constraints[i].Constraints...)
		} else {
			sumReplicas += constraints[i].NumReplicas
		}
	}

	// Validate that zero-replica constraints appear only if all conjunctions
	// are zero-replica.
	if haveZero && sumReplicas > 0 {
		return nil, errors.Errorf("cannot have zero-replica constraints mixed with non-zero-replica constraints")
	}

	// Validate that sum of non-zero numReplicas of constraints ≤ the given
	// numReplicas parameter.
	if sumReplicas > numReplicas {
		return nil, errors.Errorf("constraint replicas add up to more than configured replicas")
	}

	// After combining zero-replica constraints, set the constraint replica
	// count to numReplicas parameter.
	if haveZero {
		return []internedConstraintsConjunction{
			{
				numReplicas: numReplicas,
				constraints: interner.internConstraintsConj(zrc.Constraints),
			},
		}, nil
	}

	// Calculate total capacity needed (original + possible synthetic
	// constraint).
	capacity := len(constraints) + 1
	rv := make([]internedConstraintsConjunction, 0, capacity)
	// Intern existing constraints.
	for i := range constraints {
		rv = append(rv, internedConstraintsConjunction{
			numReplicas: constraints[i].NumReplicas,
			constraints: interner.internConstraintsConj(constraints[i].Constraints),
		})
	}
	// If the sum of non-zero numReplicas of constraints < numReplicas, add
	// a synthesized empty constraint conjunction with the difference as the
	// numReplicas. These represent the unconstrained replicas.
	if sumReplicas < numReplicas {
		rv = append(rv, internedConstraintsConjunction{
			numReplicas: numReplicas - sumReplicas,
			constraints: interner.internConstraintsConj(nil),
		})
	}
	return dedupAndFilterConstraints(rv), nil
}

// relationshipVoterAndAll represents the relationship between a voter constraint
// and an all replica constraint. It is only used within doStructuralNormalization
// as a helper struct.
type relationshipVoterAndAll struct {
	voterIndex     int
	allIndex       int
	voterAndAllRel conjunctionRelationship
}

// TODO(wenyihu6): Consider using a sync.Pool for the returned slice to avoid
// repeated allocations.
func (conf *normalizedSpanConfig) buildVoterAndAllRelationships() (
	rels []relationshipVoterAndAll,
	emptyConstraintIndex int,
	emptyVoterConstraintIndex int,
	err error,
) {
	// emptyConstraintIndex corresponds to the index of the empty constraint in
	// conf.constraints. We expect only one empty constraint.
	// Example:
	// constraints: []: 2, [+region=a]: 2 => emptyConstraintIndex = 0
	//
	// TODO(wenyihu6): we should not allow user to specify empty constraints;
	// there should only be one created by us during normalizeConstraints.
	// Instead, we can also combine empty constraints into a single one in
	// normalizeConstraints.
	emptyConstraintIndex = -1
	// emptyVoterConstraintIndex corresponds to the index of the empty voter
	// constraint in conf.voterConstraints. We expect only one empty voter
	// constraint.
	// Example:
	// voterConstraints: [+region=a]: 2, []: 2 => emptyVoterConstraintIndex = 1
	emptyVoterConstraintIndex = -1
	for i := range conf.voterConstraints {
		if len(conf.voterConstraints[i].constraints) == 0 {
			if emptyVoterConstraintIndex != -1 {
				return nil, -1, -1,
					errors.Errorf("multiple empty voter constraints: %v and %v",
						conf.voterConstraints[emptyVoterConstraintIndex], conf.voterConstraints[i])
			}
			emptyVoterConstraintIndex = i
		}
		for j := range conf.constraints {
			if len(conf.constraints[j].constraints) == 0 {
				if emptyConstraintIndex != -1 && emptyConstraintIndex != j {
					return nil, -1, -1,
						errors.Errorf("multiple empty constraints: %v and %v",
							conf.constraints[emptyConstraintIndex], conf.constraints[j])
				}
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
	return rels, emptyConstraintIndex, emptyVoterConstraintIndex, nil /*err*/
}

// Used by normalizedConstraintsEnv. Read more below on how it is used.
type allReplicaConstraintsInfo struct {
	remainingReplicas int32
	newVoterIndex     int
}

// Used by normalizedConstraintsEnv. Read more below on how it is used.
type voterConstraintsAndAdditionalInfo struct {
	internedConstraintsConjunction
	additionalReplicas int32
}

// normalizedConstraintsEnv is a helper struct created and used by
// normalizeVoterConstraints to track the state during voter constraint
// normalization.
type normalizedConstraintsEnv struct {
	// allReplicaConstraints[i] corresponds to conf.constraints[i].
	//
	// INVARIANT: len(allReplicaConstraints) == len(conf.constraints). It tracks
	// how many replicas are still available being used to satisfy voter
	// constraints.
	//
	// allReplicaConstraints[i].remainingReplicas starts out as
	// conf.constraints[i].numReplicas and decreases as we satisfy voter
	// constraints with it. allReplicaConstraints[i].newVoterIndex starts out as
	// -1 and is set to the index of the new voter constraint when we create a new
	// voter constraint to satisfy an all-replica constraint that is stricter than
	// voter constraints. newVoterIndex must correspond to an index in
	// voterConstraints where the index is >= len(conf.voterConstraints).
	allReplicaConstraints []allReplicaConstraintsInfo

	// voterConstraints[i] corresponds to conf.voterConstraints[i] for i <
	// len(conf.voterConstraints).
	//
	// INVARIANT: len(voterConstraints) >= len(conf.voterConstraints).
	//
	// voterConstraints[i].numReplicas starts with 0 and builds up
	// towards the desired number in conf.voterConstraints[i].numReplicas. It
	// represents the normalized voter constraint.
	//
	// If a voter constraint is less strict than ALL available (i.e., not fully
	// matched) all-replica constraints, we may take some of its numReplicas and
	// construct narrower voter constraints.
	//
	// voterConstraints[i].additionalReplicas tracks the total count of replicas
	// in such narrower voter constraints. In addition, a new voter constraint
	// would be appended to voterConstraints, and len(voterConstraints) starts out
	// as len(conf.voterConstraints) but may grow.
	voterConstraints []voterConstraintsAndAdditionalInfo

	// desiredVoterReplicas holds an immutable copy of the original voter
	// constraints from the config. Unlike voterConstraints (which is mutable
	// and starts with numReplicas=0 during normalization), this field preserves
	// the original target replica counts that the normalization process aims to
	// satisfy.
	desiredVoterReplicas []internedConstraintsConjunction
}

func makeNormalizedConstraintsEnv(conf *normalizedSpanConfig) normalizedConstraintsEnv {
	allReplicaConstraints := make([]allReplicaConstraintsInfo, 0, len(conf.constraints))
	voterConstraints := make([]voterConstraintsAndAdditionalInfo, 0, len(conf.voterConstraints))
	for _, constraint := range conf.voterConstraints {
		constraint.numReplicas = 0
		voterConstraints = append(voterConstraints, voterConstraintsAndAdditionalInfo{
			internedConstraintsConjunction: constraint,
		})
	}

	for i := range conf.constraints {
		allReplicaConstraints = append(allReplicaConstraints,
			allReplicaConstraintsInfo{
				// Initially all of numReplicas are remaining.
				remainingReplicas: conf.constraints[i].numReplicas,
				newVoterIndex:     -1,
			})
	}
	return normalizedConstraintsEnv{
		voterConstraints:      voterConstraints,
		allReplicaConstraints: allReplicaConstraints,
		desiredVoterReplicas:  conf.voterConstraints,
	}
}

// buildVoterConstraints returns the final normalized voter constraints. It
// filters out any constraints with numReplicas=0, which indicates an original
// voter constraint was completely narrowed and replaced by stricter
// constraints.
func (ncEnv *normalizedConstraintsEnv) buildVoterConstraints() []internedConstraintsConjunction {
	vc := make([]internedConstraintsConjunction, 0, len(ncEnv.voterConstraints))
	for i := range ncEnv.voterConstraints {
		vc = append(vc, ncEnv.voterConstraints[i].internedConstraintsConjunction)
	}
	return dedupAndFilterConstraints(vc)
}

// moveToEnd moves the voter constraint at idx to the end of the slice. This is
// used to place the empty voter constraint last just for convention (not
// required for correctness).
func (ncEnv *normalizedConstraintsEnv) moveToEnd(idx int) {
	n := len(ncEnv.voterConstraints) - 1
	if idx >= 0 && idx < n {
		ncEnv.voterConstraints[idx], ncEnv.voterConstraints[n] =
			ncEnv.voterConstraints[n], ncEnv.voterConstraints[idx]
	}
}

// satisfyVoterWithAll attempts to satisfy the voter constraint at voterIndex
// using remainingReplicas from the all-replica constraint at allIndex.
//
// NB: This is only called for conjEqualSet and conjStrictSubset relationships
// (steps 2-3), where additionalReplicas is always 0.
func (ncEnv *normalizedConstraintsEnv) satisfyVoterWithAll(voterIndex int, allIndex int) {
	remainingAll := ncEnv.allReplicaConstraints[allIndex].remainingReplicas
	neededVoterReplicas := ncEnv.desiredVoterReplicas[voterIndex].numReplicas - ncEnv.voterConstraints[voterIndex].numReplicas
	if neededVoterReplicas > 0 && remainingAll > 0 {
		toAdd := min(remainingAll, neededVoterReplicas)
		ncEnv.voterConstraints[voterIndex].numReplicas += toAdd
		ncEnv.allReplicaConstraints[allIndex].remainingReplicas -= toAdd
	}
}

// Phase 2: Normalizing All-Replica Constraints
// After Phase 1, voter constraints are normalized, but all-replica constraints
// may be under-specified compared to them. Phase 2 borrows from the empty
// all-replica constraint to top up specific constraints so they satisfy voter
// requirements.
//
// Example (num-replicas=5, num-voters=4):
// Original:
//
//	constraints:       [+region=us-west-1]: 1, [+region=us-east-1]: 1, []: 3
//	voterConstraints:  [+region=us-west-1]: 2, [+region=us-east-1]: 2
//
// After Phase 1 (voter constraints already explicit, so unchanged):
//
//	constraints:       [+region=us-west-1]: 1, [+region=us-east-1]: 1, []: 3
//	voterConstraints:  [+region=us-west-1]: 2, [+region=us-east-1]: 2
//
// The all-replica constraints only require 1 replica per region, but voters
// need 2. Consider if constraints were left under-specified, and we had only
// 3 voters: 2 in us-west-1, 1 in us-east-1, and were temporarily unable to add
// a voter in us-east-1. Say we lose the non-voter too, and need to add one.
// With the under-specified constraint we could add the non-voter anywhere,
// since we think we are allowed 3 replicas with the empty constraint
// conjunction. This is technically true, but adding the non-voter to
// us-east-1 where a voter is missing is more efficient.
//
// After Phase 2 (borrow from [] to satisfy voter requirements):
//
//	constraints:       [+region=us-west-1]: 2, [+region=us-east-1]: 2, []: 1
//	voterConstraints:  [+region=us-west-1]: 2, [+region=us-east-1]: 2
//
// Now non-voter placement is correctly guided to the unconstrained slot.
//
// (TODO(wenyihu6): this is the one that I am still confused about. Sumeer
// mentioned that it is unclear whether we truly need this. So lets revisit this
// later.)
func (conf *normalizedSpanConfig) normalizeEmptyConstraints() error {
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
	// unable to add a voter in us-east-1. Say we have a non-voter in
	// us-central-1, and we lose that non-voter, and need to add one. With the
	// under-specified constraint we could add the non-voter anywhere, since we
	// think we are allowed 2 replicas with the empty constraint conjunction,
	// and only one of those places is taken, by the second voter in us-west-1.
	// This is technically true, but once we have the required second voter in
	// us-east-1, both places in that empty constraint will be consumed, and we
	// will need to move that non-voter to us-central-1, which is wasteful.
	rels, emptyConstraintIndex, _, err := conf.buildVoterAndAllRelationships()
	if err != nil {
		return err
	}
	if emptyConstraintIndex >= 0 {
		// Ignore conjPossiblyIntersecting.
		index := 0
		for index < len(rels) && rels[index].voterAndAllRel == conjPossiblyIntersecting {
			index++
		}
		voterConstraintHasEqualityWithConstraint := make([]bool, len(conf.voterConstraints))
		// For conjEqualSet, except for the empty constraint, ensure that the
		// numReplicas in the replica constraint is at least that of the voter
		// constraint, since a voter is also a replica.
		for ; index < len(rels) && rels[index].voterAndAllRel == conjEqualSet; index++ {
			rel := rels[index]
			voterConstraintHasEqualityWithConstraint[rel.voterIndex] = true
			if rel.allIndex == emptyConstraintIndex {
				// rel.voterIndex must be emptyVoterConstraintIndex.
				continue
			}
			toAddCount := conf.voterConstraints[rel.voterIndex].numReplicas -
				conf.constraints[rel.allIndex].numReplicas
			availableCount := conf.constraints[emptyConstraintIndex].numReplicas
			if toAddCount > 0 && availableCount > 0 {
				// TODO(wenyihu6): this should also create an error, since we will
				// end up with two identical constraint conjunctions in replica
				// constraints and voter constraints, with the former having a lower
				// count than the latter.
				add := min(toAddCount, availableCount)
				conf.constraints[emptyConstraintIndex].numReplicas -= add
				conf.constraints[rel.allIndex].numReplicas += add
			}
		}
		// For conjStrictSubset, if the subset relationship is with
		// emptyConstraintIndex, grab from there, and create a new narrower
		// replica constraint.
		for ; index < len(rels) && rels[index].voterAndAllRel == conjStrictSubset; index++ {
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
			toAddCount := conf.voterConstraints[rel.voterIndex].numReplicas
			if availableCount > 0 && toAddCount > 0 {
				add := min(availableCount, toAddCount)
				conf.constraints[emptyConstraintIndex].numReplicas -= add
				conf.constraints = append(conf.constraints, internedConstraintsConjunction{
					numReplicas: add,
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
		conf.constraints = dedupAndFilterConstraints(conf.constraints)
	}
	return nil
}

// Phase 1: Normalizing Voter Constraints
// Goal:
//
//   - Voters must always be a subset of all replicas, so each voter constraint
//     must satisfy some all-replica constraint. When a voter constraint is too
//     broad or under-specified, we narrow it to a more specific form. This step
//     makes voter constraints explicit and strictly defined.
//
//     Example (too broad):
//     constraints:       [+region=a,+zone=a1]: 2, [+region=a,+zone=a2]: 2
//     voterConstraints:  [+region=a]: 3 (broader than the all-replica
//     constraints)
//     constraints and needs to be narrowed to [+region=a,+zone=a1]: 2,
//     [+region=a,+zone=a2]: 1.
//
//     Example (under-specified):
//     constraints:       [+region=a]: 2, [+region=b]: 2
//     voterConstraints:  num_voters=3 (no explicit conjunctions)
//     Historically, users did not need to repeat constraint information in
//     voterConstraints—the system would implicitly derive that voters must be
//     placed in region=a or region=b based on the all-replica constraints. The
//     new strictness requirement in roachpb.SpanConfig now requires users to
//     explicitly specify voterConstraints. In this example, we normalize legacy
//     configs to voterConstraints: [+region=a]: 2, [+region=b]: 1 by deriving
//     these explicit voter constraints from the all-replica constraints for
//     backwards compatibility.
//
//   - While tightening voter constraints, we also detect cases where the config
//     is too ambiguous for us to confidently determine coverage. In those cases,
//     we continue with best-effort normalization and return an error. See
//     example on conjPossiblyIntersecting for more details.
func (conf *normalizedSpanConfig) normalizeVoterConstraints() error {
	// To narrow voter constraints, we "satisfy" each voter constraint by incrementally building
	// up from 0 to its desired numReplicas. For each voter constraint, we try to
	// associate its replicas with all-replica constraints that can cover them.
	//
	// We first establish relationships between every voter constraint conjunction
	// and every all-replica constraint conjunction. Then we process those
	// relationships in the following order (refer to conjunctionRelationship for
	// more details): conjPossiblyIntersecting, conjEqualSet, conjStrictSubset,
	// conjStrictSuperset, and conjNonIntersecting. See inline comments below for
	// details on how each relationship type is handled.
	rels, emptyConstraintIndex, emptyVoterConstraintIndex, err := conf.buildVoterAndAllRelationships()
	if err != nil {
		return err
	}

	ncEnv := makeNormalizedConstraintsEnv(conf)

	// Step 1: Handle conjPossiblyIntersecting relationships.
	//
	// When a voter constraint and an all-replica constraint have some shared and
	// some non-shared conjunctions, we cannot prove whether a replica satisfying
	// the voter constraint is covered by the all-replica constraint. We treat
	// this as ambiguous and return an error (though we proceed with
	// normalization).
	//
	// Example:
	//   constraints:       [+region=a,+dc=dc1]: 2
	//   voterConstraints:  [+region=a,+zone=a2]: 2
	//
	// The constraints share +region=a but differ on +zone vs +dc. We cannot
	// determine if zone=a2 is in dc=dc1, so we skip matching and return an error.
	//
	// TODO(wenyihu6): It's odd that we return an error when not violating the
	// spanconfig contract. What should the operator do in this case?
	//
	// Implementation notes:
	// Example: +region=a,+zone=a1 and +region=a,-zone=a2 are classified as
	// conjPossiblyIntersecting. If zone=a3 exists in the region, they actually
	// intersect. We cannot do better without knowing the universe of values.
	index := 0
	for index < len(rels) && rels[index].voterAndAllRel == conjPossiblyIntersecting {
		index++
	}
	if index > 0 {
		err = errors.Errorf("intersecting conjunctions in constraints and voter constraints")
	}
	// Even if there was an error, we will continue normalization.

	// Step 2: Handle conjEqualSet and conjStrictSubset relationships.
	//
	// conjEqualSet: The voter and all-replica constraints have the same
	// conjunctions. We directly satisfy the voter using the all-replica's
	// numReplicas count.
	//
	// Example 1 (all-replica has fewer):
	//   constraints:       [+region=a,+zone=a1]: 1
	//   voterConstraints:  [+region=a,+zone=a1]: 2
	//   We satisfy 1 of 2 voters here; the remaining 1 must be satisfied later.
	//
	// Example 2 (all-replica has more):
	//   constraints:       [+region=a,+zone=a1]: 3
	//   voterConstraints:  [+region=a,+zone=a1]: 2
	//   We satisfy all 2 voters using 2 of 3 slots; the extra slot can hold a
	//   non-voter.
	//
	// conjStrictSubset: The voter constraint is more specific than the
	// all-replica constraint. We can directly satisfy since any replica matching
	// the voter will also match the broader all-replica constraint.
	//
	// Example:
	//   constraints:       [+region=a,+zone=a1]: 2
	//   voterConstraints:  [+region=a,+zone=a1,+dc=dc1]: 2
	//
	// TODO(wenyihu6): I'm not sure how to prove correctness when all-replica
	// constraints overlap - add test case for this.
	for ; index < len(rels) && rels[index].voterAndAllRel <= conjStrictSubset; index++ {
		rel := rels[index]
		if rel.voterIndex == emptyVoterConstraintIndex {
			// Don't try to satisfy the empty voter constraint since the empty voter
			// constraint may need to be narrowed due to replica constraints.
			continue
		}
		ncEnv.satisfyVoterWithAll(rel.voterIndex, rel.allIndex)
	}

	// Step 3: Handle empty voter constraints before conjStrictSuperset:
	//
	// The only relationships remaining are conjStrictSuperset and
	// conjNonIntersecting. Before handling conjStrictSuperset where we narrow
	// voter constraints (read more in step 4), we prioritize associating empty
	// voter constraints with empty all-replica constraints to avoid unnecessary
	// narrowing.
	//
	// Example:
	// voter_constraints: []: 1
	// constraints:       [+region=a]: 1, []: 1
	//
	// Without this step, voter_constraints: []:1 would be unnecessarily narrowed to
	// [+zone=a]:1 but []: 1 in constraints can already satisfy it.
	if emptyVoterConstraintIndex >= 0 && emptyConstraintIndex >= 0 {
		neededReplicas := conf.voterConstraints[emptyVoterConstraintIndex].numReplicas
		// While iterating over the previous relationships, we skipped over
		// emptyVoterConstraintIndex, so its corresponding
		// voterConstraints.numReplicas must be 0.
		remainingSatisfiable := ncEnv.allReplicaConstraints[emptyConstraintIndex].remainingReplicas
		if neededReplicas > 0 && remainingSatisfiable > 0 {
			count := min(remainingSatisfiable, neededReplicas)
			ncEnv.voterConstraints[emptyVoterConstraintIndex].numReplicas += count
			ncEnv.allReplicaConstraints[emptyConstraintIndex].remainingReplicas -= count
		}
		ncEnv.satisfyVoterWithAll(emptyVoterConstraintIndex, emptyConstraintIndex)
	}

	// Step 4: Handle conjStrictSuperset relationships (narrowing).
	//
	// The outer for loop causes repeated iteration over the strict superset
	// relationship. When the inner loop finds a voter constraint that needs
	// more replicas, and the subset conjunction in constraints has some
	// remaining replicas, we replace the weaker voter constraint with the
	// stronger/tighter conjunction for 1 voter. Note that we don't exclude the
	// emptyVoterConstraintIndex for consideration here, since this is exactly
	// the place where we want to see if we can replace it with tighter
	// conjunctions.

	// The voter constraint is more general than the all-replica constraint,
	// meaning it is under-specified. We satisfy such voter constraints by
	// "narrowing": creating new, tighter voter constraints derived from the
	// all-replica constraints. We process conjEqualSet and conjStrictSubset
	// before conjStrictSuperset because direct satisfaction is preferred over
	// narrowing.
	//
	// Example:
	//   constraints:       [+region=a,+zone=a1]: 2, [+region=a,+zone=a2]: 2
	//   voterConstraints:  [+region=a]: 3
	//
	// The voter constraint "+region=a" is broader than both all-replica
	// constraints. Historically, this under-specification was allowed. The new
	// strictness requirement in roachpb.SpanConfig requires explicit repetition,
	// but we normalize existing configs for backwards compatibility. We narrow:
	//   voterConstraints:  [+region=a,+zone=a1]: 2, [+region=a,+zone=a2]: 1
	//
	// As a heuristic, we "load-balance" satisfaction across constraints. For
	// example, if a voter constraint with conjunction c1 needs 2 more replicas,
	// and we have all-replica constraints [c1,c2]:2 and [c1,c3]:2, instead of
	// greedily assigning 2 to [c1,c2], we assign 1 to each.
	//
	// Without this step, voter_constraints []:1 would be unnecessarily narrowed
	// to [+region=a]:1, but []:1 in constraints can already satisfy it.
	for {
		added := false
		for i := index; i < len(rels) && rels[i].voterAndAllRel == conjStrictSuperset; i++ {
			rel := rels[i]
			remainingAll := ncEnv.allReplicaConstraints[rel.allIndex].remainingReplicas
			neededVoterReplicas := conf.voterConstraints[rel.voterIndex].numReplicas -
				ncEnv.voterConstraints[rel.voterIndex].numReplicas -
				ncEnv.voterConstraints[rel.voterIndex].additionalReplicas
			if neededVoterReplicas > 0 && remainingAll > 0 {
				// Satisfy 1 replica.
				ncEnv.voterConstraints[rel.voterIndex].additionalReplicas++
				ncEnv.allReplicaConstraints[rel.allIndex].remainingReplicas--
				newVoterIndex := ncEnv.allReplicaConstraints[rel.allIndex].newVoterIndex
				if newVoterIndex == -1 {
					// We haven't yet created a narrower voter constraint for this.
					newVoterIndex = len(ncEnv.voterConstraints)
					ncEnv.allReplicaConstraints[rel.allIndex].newVoterIndex = newVoterIndex
					ncEnv.voterConstraints = append(ncEnv.voterConstraints, voterConstraintsAndAdditionalInfo{
						internedConstraintsConjunction: internedConstraintsConjunction{
							numReplicas: 0,
							constraints: conf.constraints[rel.allIndex].constraints,
						},
						additionalReplicas: 0,
					})
				}
				ncEnv.voterConstraints[newVoterIndex].numReplicas++
				added = true
			}
		}
		if !added {
			break
		}
	}

	// Step 5: Handle conjNonIntersecting relationships.
	//
	// conjNonIntersecting relationships are implicitly handled here: when a voter
	// and all-replica constraint have no shared conjunctions, the voter replica
	// is not covered by that all-replica constraint. We skip such relationships
	// during processing, and if the voter constraint remains unsatisfied after
	// all steps, we return an error but keep the original numReplicas in output.
	//
	// Example:
	//   constraints:       [+zone=a1]: 1
	//   voterConstraints:  [+zone=a1]: 3
	//   We satisfy 1 of 3 voters using [+zone=a1]:1. The remaining 2 cannot be
	//   satisfied. We return an error but output voterConstraints: [+zone=a1]: 3
	//   unchanged.

	// Step 6: Verify all voter constraints are satisfied.
	//
	// Force the satisfaction by leaving the original numReplicas in the output
	// unchanged even though we couldn't satisfy it using any all-replica
	// constraint.
	//
	// TODO(wenyihu6): should we do something here for nonintersecting or
	// intersecting constraints? Does it make sense to surface an error
	for i := range conf.voterConstraints {
		neededReplicas := conf.voterConstraints[i].numReplicas
		actualReplicas := ncEnv.voterConstraints[i].numReplicas + ncEnv.voterConstraints[i].additionalReplicas
		if actualReplicas > neededReplicas {
			return errors.Errorf("programmer error: actualReplicas (%d) > neededReplicas (%d) for voter constraint %d",
				actualReplicas, neededReplicas, i)
		}
		if actualReplicas < neededReplicas {
			err = errors.Errorf("could not satisfy all voter constraints due to " +
				"non-intersecting conjunctions in voter and all replica constraints")
			// NB: We cannot simply set voterConstraints[i].numReplicas =
			// neededReplicas here since we may have used some
			// conf.voterConstraints[i].numReplicas to construct narrower voter
			// constraints to satisfy a stricter all-replica constraint in step 4.
			ncEnv.voterConstraints[i].numReplicas += neededReplicas - actualReplicas
		}
	}
	// Move empty voter constraint to the end (convention), then build the final
	// normalized voter constraints. This assignment to conf.voterConstraints is
	// the only mutation to conf in this function.
	ncEnv.moveToEnd(emptyVoterConstraintIndex)
	conf.voterConstraints = ncEnv.buildVoterConstraints()
	return err
}

// doStructuralNormalization mutates config.VoterConstraints and
// config.Constraints in place by making under-specified voter and all-replica
// constraints more explicit when possible.
//
// Along the way, it also checks whether the config meets the strictness
// requirements in roachpb.SpanConfig. If the config is ambiguous or cannot be
// fully normalized, we continue with best-effort normalization and return an
// error. The caller should surface that error to the operator, since the
// resulting normalization is not guaranteed to be valid.
//
// (TODO(wenyihu6): we should clarify what the operator should do with the error
// we return an error even when we re not violating spanconfig contract which is
// confusing.)
func (conf *normalizedSpanConfig) doStructuralNormalization(
	observer NormalizationPhaseObserver,
) error {
	if len(conf.constraints) == 0 || len(conf.voterConstraints) == 0 {
		return nil
	}

	// Do not return early on error from normalizeVoterConstraints since we want
	// to continue with best-effort normalization for constraints.
	err1 := conf.normalizeVoterConstraints()
	if observer != nil {
		observer("normalizeVoterConstraints", conf)
	}
	err2 := conf.normalizeEmptyConstraints()
	// Use errors.Join so both errors are visible in .Error() output, since
	// this error is surfaced to the operator.
	return errors.Join(err1, err2)
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
	w.SafeString(" voters=[")
	for i := range rac.replicas[voterIndex] {
		if i > 0 {
			w.SafeString(", ")
		}
		w.Printf("s%v", rac.replicas[voterIndex][i].StoreID)
	}
	w.SafeRune(']')
	w.SafeString(" non-voters=[")
	for i := range rac.replicas[nonVoterIndex] {
		if i > 0 {
			w.SafeString(", ")
		}
		w.Printf("s%v", rac.replicas[nonVoterIndex][i].StoreID)
	}
	w.SafeRune(']')
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
	isVoterConstraints bool,
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
	// exactly one constraint (with an exception for voter constraints).
	//
	// Exception for voter constraints: when ac corresponds to voter constraints,
	// we delay assigning non-voters that satisfy only one constraint until phase
	// 2, since we would like to give a chance for voters to satisfy the voter
	// constraint, even if the voters satisfy multiple constraints. That is, we
	// want to avoid a situation where we unnecessarily need to elevate a
	// non-voter to voter.
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
			} else if n == 1 && (!isVoterConstraints || kind == voterIndex) {
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
	// isConstraintSatisfied checks if the given constraint index has been fully
	// satisfied by the stores currently assigned to it.
	//
	// NB: This intentionally counts both voters and non-voters even when
	// isVoterConstraints is true. This is correct because Phase 2 iterates over
	// voters (kind=voterIndex) before non-voters (kind=nonVoterIndex). By
	// counting both, we avoid over-satisfying a constraint with non-voters before
	// other constraints get a chance to be satisfied.
	// Example: voter constraints [+region=a]:1, [+zone=b1]:1, [+region=b]:1 with
	// two non-voters on stores matching both region=a and zone=b1. If we only
	// counted voters, [+region=a] would appear unsatisfied when processing the
	// second non-voter, causing both non-voters to be assigned to [+region=a]. By
	// counting both voters and non-voters, the first non-voter assignment marks
	// [+region=a] as satisfied, allowing the second non-voter to be assigned to
	// [+zone=b1]. See testdata/range_analyzed_constraints for examples
	// illustrating this behavior.
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
) error {
	rac.spanConfig = spanConfig
	rac.numNeededReplicas[voterIndex] = spanConfig.numVoters
	rac.numNeededReplicas[nonVoterIndex] = spanConfig.numReplicas - spanConfig.numVoters
	rac.replicas = rac.buf.replicas

	if spanConfig.constraints != nil {
		rac.constraints.initialize(spanConfig.constraints, &rac.buf, constraintMatcher, false /* isVoterConstraints */)
	}
	if spanConfig.voterConstraints != nil {
		rac.voterConstraints.initialize(spanConfig.voterConstraints, &rac.buf, constraintMatcher, true /* isVoterConstraints */)
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
			return errors.Errorf("leaseholder not found in replicas %v", rac.replicas)
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
	return nil
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
//
// TODO(wenyihu6): There's a gap in MMA's filtering order compared to SMA. MMA
// filters by lease preference first (LPI <= current via candidatesToMoveLease),
// then by health (retainReadyLeaseTargetStoresOnly), and finally filters out
// candidates matching no preference (notMatchedLeasePreferenceIndex check in
// sortTargetCandidateSetAndPick). SMA filters by health first
// (ValidLeaseTargets calls LiveAndDeadReplicas) then finds the best LPI among
// healthy survivors (PreferredLeaseholders). If no healthy store satisfies any
// configured lease preference, then SMA falls back to considering all healthy
// stores as candidates.
//
// This means MMA can fail to find candidates when SMA would succeed. Example:
//   - Current leaseholder: s1 with LPI=0 (best), unhealthy
//   - Voters: s1(LPI=0, unhealthy), s2(LPI=0, unhealthy), s3(LPI=1, healthy)
//   - MMA: candidatesToMoveLease returns [s2] (LPI <= 0), health filter removes
//     s2, result: NO candidates.
//   - SMA: health filter leaves [s3], PreferredLeaseholders finds s3 (best
//     among healthy), result: s3 is candidate.
//
// To fix this, consider filtering by health first, then:
// - If current leaseholder is healthy: apply LPI <= current among healthy
// stores - If current leaseholder is unhealthy: find best LPI among healthy
// survivors (like SMA)
// - (Up for discussion) Fallback to all healthy stores if no preference
// matches.
//
// This would require restructuring to pass a health-filtered store set into
// this function.
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

// internConstraintsConj interns an array of roachpb.Constraint into a canonical, sorted and deduplicated
// constraintsConj (slice of internedConstraint).
// It interns the constraint Key and Value strings, sorts the resulting constraints, and removes duplicates.
// Returns nil if the input slice is empty.
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
		return rv[j].cmp(rv[k]) < 0
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
