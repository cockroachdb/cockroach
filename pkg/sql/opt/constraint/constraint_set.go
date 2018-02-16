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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Unconstrained is an empty constraint set which does not impose any
// constraints on any columns.
var Unconstrained = &ConstraintSet{}

// Contradiction is a special constraint set which indicates there are no
// possible values for the expression; it will always yield the empty result
// set.
var Contradiction = &ConstraintSet{contradiction: true}

// ConstraintSet is a conjunction of constraints that are inferred from scalar
// filter conditions. The constrained expression will always evaluate to a
// result set having values that conform to all of the constraints in the
// constraint set. Each constraint within the set is a disjunction of spans
// that together specify the domain of possible values which that constraint's
// column(s) can have. See the Constraint struct comment for more details.
//
// Constraint sets are useful for selecting indexes, pruning ranges, inferring
// non-null columns, and more. They serve as a "summary" of arbitrarily complex
// expressions, so that fast decisions can be made without analyzing the entire
// expression tree each time.
//
// A few examples:
//  - @1 > 10
//      (/@1 (/1 - ∞])
//
//  - @1 > 10 AND @2 = 5
//      (/@1 (/1 - ∞])
//      (/@2 [/5 - /5])
//
//  - (@1 = 10 AND @2 > 5) OR (@1 = 20 AND @2 > 0)
//      (/@1 [/10 - /10] [/20 - /20])
//      (/@2 [/0 - ∞])
//
type ConstraintSet struct {
	// firstConstraint holds the first constraint in the set and otherConstraints
	// hold any constraints beyond the first. These are separated in order to
	// optimize for the common case of a set with a single constraint.
	firstConstraint  Constraint
	otherConstraints []Constraint

	// len is the number of constraints in the set.
	len int32

	// contradiction is true if this is the special Contradiction constraint set.
	contradiction bool
}

// NewForColumn creates a new constraint set containing a single constraint.
// The constraint has a single column and a single span.
func NewForColumn(col opt.ColumnIndex, sp *Span) *ConstraintSet {
	cs := &ConstraintSet{len: 1}
	cs.firstConstraint.init(col, sp)
	return cs
}

// NewForColumns creates a new constraint set containing a single constraint.
// The constraint has one or more columns and a single span.
func NewForColumns(cols []opt.ColumnIndex, sp *Span) *ConstraintSet {
	cs := &ConstraintSet{len: 1}
	cs.firstConstraint.initComposite(cols, sp)
	return cs
}

// Length returns the number of constraints in the set.
func (c *ConstraintSet) Length() int {
	return int(c.len)
}

// Constraint returns the nth constraint in the set. Together with the
// ConstraintCount method, Constraint allows iteration over the list of
// comstraints (since there is no method to return a slice of constraints).
func (c *ConstraintSet) Constraint(nth int) *Constraint {
	if nth == 0 && c.len != 0 {
		return &c.firstConstraint
	}
	return &c.otherConstraints[nth-1]
}

// IsUnconstrained returns true if the constraint set contains no constraints,
// which means column values can have any possible values.
func (c *ConstraintSet) IsUnconstrained() bool {
	return c.len == 0 && !c.contradiction
}

// Intersect finds the overlap between this constraint set and the given set.
// Constraints that exist in either of the input sets will get merged into the
// combined set. Compatible constraints (that share same column list) are
// intersected with one another. Intersect returns the merged set.
func (c *ConstraintSet) Intersect(evalCtx *tree.EvalContext, other *ConstraintSet) *ConstraintSet {
	// Intersection with the contradiction set is always the contradiction set.
	if c == Contradiction || other == Contradiction {
		return Contradiction
	}

	// Intersection with the unconstrained set is the identity op.
	if c.IsUnconstrained() {
		return other
	}
	if other.IsUnconstrained() {
		return c
	}

	// Create a new set to hold the merged sets.
	mergeSet := &ConstraintSet{}

	index := 0
	len := c.Length()
	otherIndex := 0
	otherLen := other.Length()

	// Constraints are ordered in the set by column indexes, with no duplicates,
	// so intersection can be done as a variation on merge sort.
	for index < len || otherIndex < otherLen {
		// Allocate the next constraint slot in the new set.
		merge := mergeSet.allocConstraint(len - index + otherLen - otherIndex)

		var cmp int
		if index >= len {
			cmp = 1
		} else if otherIndex >= otherLen {
			cmp = -1
		} else {
			cmp = compareConstraintsByCols(c.Constraint(index), other.Constraint(otherIndex))
		}

		if cmp == 0 {
			// Constraints have same columns, so they're compatible and need to
			// be merged.
			*merge = *c.Constraint(index)
			if !merge.tryIntersectWith(evalCtx, other.Constraint(otherIndex)) {
				// Constraints contradict one another.
				return Contradiction
			}

			// Skip past both inputs.
			index++
			otherIndex++
		} else if cmp < 0 {
			// This constraint has no corresponding constraint in other set, so
			// add it to the set (absence of other constraint = unconstrained).
			*merge = *c.Constraint(index)
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
// additional values that neither of the input sets allowed. Union returns the
// merged set.
func (c *ConstraintSet) Union(evalCtx *tree.EvalContext, other *ConstraintSet) *ConstraintSet {
	// Union with the contradiction set is an identity operation.
	if c == Contradiction {
		return other
	} else if other == Contradiction {
		return c
	}

	// Union with the unconstrained set yields an unconstrained set.
	if c.IsUnconstrained() {
		return Unconstrained
	} else if other.IsUnconstrained() {
		return Unconstrained
	}

	// Create a new set to hold the merged sets.
	mergeSet := &ConstraintSet{}

	index := 0
	len := c.Length()
	otherIndex := 0
	otherLen := other.Length()

	// Constraints are ordered in the set by column indexes, with no duplicates,
	// so union can be done as a variation on merge sort. The constraints are
	// matched up against one another. All constraints that have a "compatible"
	// constraint in the other set can be merged into the new set. Currently,
	// a compatible constraint is one in which columns exactly match.
	for index < len && otherIndex < otherLen {
		// Skip past constraints that do not have corresponding constraint in
		// the other set.
		cmp := compareConstraintsByCols(c.Constraint(index), other.Constraint(otherIndex))
		if cmp < 0 {
			index++
			continue
		} else if cmp > 0 {
			otherIndex++
			continue
		}

		// Constraints have same columns, so they're compatible and need to
		// be merged. Allocate the next constraint slot in the new set.
		merge := mergeSet.allocConstraint(len - index + otherLen - otherIndex)

		*merge = *c.Constraint(index)
		if !merge.tryUnionWith(evalCtx, other.Constraint(otherIndex)) {
			// Together, constraints allow any possible value (unconstrained
			// case), and so there's nothing to add to the set.
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
func (c *ConstraintSet) allocConstraint(capacity int) *Constraint {
	c.len++

	// First constraint does not require heap allocation.
	if c.len == 1 {
		return &c.firstConstraint
	}

	// Second constraint allocates slice.
	if c.otherConstraints == nil {
		c.otherConstraints = make([]Constraint, 1, capacity)
		return &c.otherConstraints[0]
	}

	// Subsequent constraints extend slice.
	if cap(c.otherConstraints) < capacity {
		panic("correct capacity should have been set when otherConstraints was allocated")
	}
	c.otherConstraints = c.otherConstraints[:c.len-1]
	return &c.otherConstraints[c.len-2]
}

// undoAllocConstraint rolls back the previous allocation performed by
// allocConstraint. The next call to allocConstraint will allocate the same
// slot as before.
func (c *ConstraintSet) undoAllocConstraint() {
	c.len--
}

func (c *ConstraintSet) String() string {
	if c.IsUnconstrained() {
		return "unconstrained\n"
	}
	if c == Contradiction {
		return "contradiction\n"
	}

	var buf bytes.Buffer
	for i := 0; i < c.Length(); i++ {
		fmt.Fprintf(&buf, "%s\n", c.Constraint(i))
	}
	return buf.String()
}

// compareConstraintsByCols orders constraints by the indexes of their columns,
// with column position determining significance in the sort key (most
// significant first).
func compareConstraintsByCols(left, right *Constraint) int {
	leftCount := left.ColumnCount()
	rightCount := right.ColumnCount()
	for i := 0; i < leftCount && i < rightCount; i++ {
		diff := int(left.Column(i) - right.Column(i))
		if diff != 0 {
			return diff
		}
	}
	return leftCount - rightCount
}
