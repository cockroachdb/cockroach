// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package constraint

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Unconstrained is an empty constraint set which does not impose any
// constraints on any columns.
var Unconstrained = &Set{}

// Contradiction is a special constraint set which indicates there are no
// possible values for the expression; it will always yield the empty result
// set.
var Contradiction = &Set{contradiction: true}

// Set is a conjunction of constraints that are inferred from scalar filter
// conditions. The constrained expression will always evaluate to a result set
// having values that conform to all of the constraints in the constraint set.
// Each constraint within the set is a disjunction of spans that together
// specify the domain of possible values which that constraint's column(s) can
// have. See the Constraint struct comment for more details.
//
// Constraint sets are useful for selecting indexes, pruning ranges, inferring
// non-null columns, and more. They serve as a "summary" of arbitrarily complex
// expressions, so that fast decisions can be made without analyzing the entire
// expression tree each time.
//
// A few examples:
//  - @1 > 10
//      /@1: (/10 - ]
//
//  - @1 > 10 AND @2 = 5
//      /@1: (/10 - ]
//      /@2: [/5 - /5]
//
//  - (@1 = 10 AND @2 > 5) OR (@1 = 20 AND @2 > 0)
//      /@1: [/10 - /10] [/20 - /20]
//      /@2: [/0 - ]
//
type Set struct {
	// firstConstraint holds the first constraint in the set and otherConstraints
	// hold any constraints beyond the first. These are separated in order to
	// optimize for the common case of a set with a single constraint.
	firstConstraint  Constraint
	otherConstraints []Constraint

	// length is the number of constraints in the set.
	length int32

	// contradiction is true if this is the special Contradiction constraint set.
	contradiction bool
}

// SingleConstraint creates a Set with a single Constraint.
func SingleConstraint(c *Constraint) *Set {
	if c.IsContradiction() {
		return Contradiction
	}
	if c.IsUnconstrained() {
		return Unconstrained
	}
	return &Set{length: 1, firstConstraint: *c}
}

// SingleSpanConstraint creates a Set with a single constraint which
// has one span.
func SingleSpanConstraint(keyCtx *KeyContext, span *Span) *Set {
	if span.IsUnconstrained() {
		return Unconstrained
	}
	s := &Set{length: 1}
	s.firstConstraint.InitSingleSpan(keyCtx, span)
	return s
}

// Length returns the number of constraints in the set.
func (s *Set) Length() int {
	return int(s.length)
}

// Constraint returns the nth constraint in the set. Together with the Length
// method, Constraint allows iteration over the list of constraints (since
// there is no method to return a slice of constraints).
func (s *Set) Constraint(nth int) *Constraint {
	if nth == 0 && s.length != 0 {
		return &s.firstConstraint
	}
	return &s.otherConstraints[nth-1]
}

// IsUnconstrained returns true if the constraint set contains no constraints,
// which means column values can have any possible values.
func (s *Set) IsUnconstrained() bool {
	return s.length == 0 && !s.contradiction
}

// Intersect finds the overlap between this constraint set and the given set.
// Constraints that exist in either of the input sets will get merged into the
// combined set. Compatible constraints (that share same column list) are
// intersected with one another. Intersect returns the merged set.
func (s *Set) Intersect(evalCtx *tree.EvalContext, other *Set) *Set {
	// Intersection with the contradiction set is always the contradiction set.
	if s == Contradiction || other == Contradiction {
		return Contradiction
	}

	// Intersection with the unconstrained set is the identity op.
	if s.IsUnconstrained() {
		return other
	}
	if other.IsUnconstrained() {
		return s
	}

	// Create a new set to hold the merged sets.
	mergeSet := &Set{}

	index := 0
	length := s.Length()
	otherIndex := 0
	otherLength := other.Length()

	// Constraints are ordered in the set by column indexes, with no duplicates,
	// so intersection can be done as a variation on merge sort.
	for index < length || otherIndex < otherLength {
		// Allocate the next constraint slot in the new set.
		merge := mergeSet.allocConstraint(length - index + otherLength - otherIndex)

		var cmp int
		if index >= length {
			cmp = 1
		} else if otherIndex >= otherLength {
			cmp = -1
		} else {
			cmp = compareConstraintsByCols(s.Constraint(index), other.Constraint(otherIndex))
		}

		if cmp == 0 {
			// Constraints have same columns, so they're compatible and need to
			// be merged.
			*merge = *s.Constraint(index)
			merge.IntersectWith(evalCtx, other.Constraint(otherIndex))
			if merge.IsContradiction() {
				return Contradiction
			}

			// Skip past both inputs.
			index++
			otherIndex++
		} else if cmp < 0 {
			// This constraint has no corresponding constraint in other set, so
			// add it to the set (absence of other constraint = unconstrained).
			*merge = *s.Constraint(index)
			index++
		} else {
			*merge = *other.Constraint(otherIndex)
			otherIndex++
		}
	}
	return mergeSet
}

// Union creates a new set with constraints that allow any value that either of
// the input sets allowed. Compatible constraints (that share same column list)
// that exist in both sets are merged with one another. Note that the results
// may not be "tight", meaning that the new constraint set might allow
// additional combinations of values that neither of the input sets allowed. For
// example:
//   (x > 1 AND y > 10) OR (x < 5 AND y < 50)
// the union is unconstrained (and thus allows combinations like x,y = 10,0).
//
// Union returns the merged set.
func (s *Set) Union(evalCtx *tree.EvalContext, other *Set) *Set {
	// Union with the contradiction set is an identity operation.
	if s == Contradiction {
		return other
	} else if other == Contradiction {
		return s
	}

	// Union with the unconstrained set yields an unconstrained set.
	if s.IsUnconstrained() || other.IsUnconstrained() {
		return Unconstrained
	}

	// Create a new set to hold the merged sets.
	mergeSet := &Set{}

	index := 0
	length := s.Length()
	otherIndex := 0
	otherLength := other.Length()

	// Constraints are ordered in the set by column indexes, with no duplicates,
	// so union can be done as a variation on merge sort. The constraints are
	// matched up against one another. All constraints that have a "compatible"
	// constraint in the other set can be merged into the new set. Currently,
	// a compatible constraint is one in which columns exactly match.
	for index < length && otherIndex < otherLength {
		// Skip past any constraint that does not have a corresponding
		// constraint in the other set. A missing constraint is equivalent to
		// a constraint that allows all values. Union of that unconstrained
		// range with any other range is also unconstrained, and the constraint
		// set never includes unconstrained ranges. Therefore, skipping
		// unmatched constraints is equivalent to doing a union operation and
		// then not adding the result to the set.
		cmp := compareConstraintsByCols(s.Constraint(index), other.Constraint(otherIndex))
		if cmp < 0 {
			index++
			continue
		} else if cmp > 0 {
			otherIndex++
			continue
		}

		// Constraints have same columns, so they're compatible and need to
		// be merged. Allocate the next constraint slot in the new set.
		merge := mergeSet.allocConstraint(length - index + otherLength - otherIndex)

		*merge = *s.Constraint(index)
		merge.UnionWith(evalCtx, other.Constraint(otherIndex))
		if merge.IsUnconstrained() {
			// Together, constraints allow any possible value, and so there's nothing
			// to add to the set.
			mergeSet.undoAllocConstraint()
		}

		// Skip past both inputs.
		index++
		otherIndex++
	}
	return mergeSet
}

// allocConstraint allocates space for a new constraint in the set and returns
// a pointer to it. The first constraint is stored inline, and subsequent
// constraints are stored in the otherConstraints slice.
func (s *Set) allocConstraint(capacity int) *Constraint {
	s.length++

	// First constraint does not require heap allocation.
	if s.length == 1 {
		return &s.firstConstraint
	}

	// Second constraint allocates slice.
	if s.otherConstraints == nil {
		s.otherConstraints = make([]Constraint, 1, capacity)
		return &s.otherConstraints[0]
	}

	// Subsequent constraints extend slice.
	if cap(s.otherConstraints) < capacity {
		panic("correct capacity should have been set when otherConstraints was allocated")
	}

	// Remember that otherConstraints' length is one less than the set length.
	s.otherConstraints = s.otherConstraints[:s.length-1]
	return &s.otherConstraints[s.length-2]
}

// undoAllocConstraint rolls back the previous allocation performed by
// allocConstraint. The next call to allocConstraint will allocate the same
// slot as before.
func (s *Set) undoAllocConstraint() {
	s.length--
}

func (s *Set) String() string {
	if s.IsUnconstrained() {
		return "unconstrained"
	}
	if s == Contradiction {
		return "contradiction"
	}

	var b strings.Builder
	for i := 0; i < s.Length(); i++ {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(s.Constraint(i).String())
	}
	return b.String()
}

// compareConstraintsByCols orders constraints by the indexes of their columns,
// with column position determining significance in the sort key (most
// significant first).
func compareConstraintsByCols(left, right *Constraint) int {
	leftCount := left.Columns.Count()
	rightCount := right.Columns.Count()
	for i := 0; i < leftCount && i < rightCount; i++ {
		diff := int(left.Columns.Get(i) - right.Columns.Get(i))
		if diff != 0 {
			return diff
		}
	}
	return leftCount - rightCount
}
