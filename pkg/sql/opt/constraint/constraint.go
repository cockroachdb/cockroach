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

// Constraint specifies the possible set of values that one or more columns
// will have in the result set. If this is a single column constraint, then
// that column's value will always be part of one of the spans in this
// constraint. If this is a multi-column constraint, then the combination of
// column values will always be part of one of the spans.
//
// Constrained columns are specified as an ordered list, and span key values
// correspond to those columns by position. Constraints are inferred from
// scalar filter conditions, since these restrict the set of possible results.
// Constraints over different combinations of columns can be combined together
// in a constraint set, which is a conjunction of constraints. See the
// Set struct comment for more details.
//
// A few examples:
//  - a constraint on @1 > 1: a single span             /@1: (/1 - ]
//  - a constraint on @1 = 1 AND @2 >= 1: a single span /@1/@2: [/1/1 - /1]
//  - a constraint on @1 < 5 OR @1 > 10: multiple spans /@1: [ - /5) (10 - ]
type Constraint struct {
	Columns Columns

	// Spans contains the spans in this constraint. The spans are always ordered
	// and non-overlapping.
	Spans Spans
}

// Init initializes the constraint to the columns in the key context and with
// the given spans.
func (c *Constraint) Init(keyCtx KeyContext, spans Spans) {
	for i := 1; i < spans.Count(); i++ {
		if !spans.Get(i).StartsAfter(keyCtx, spans.Get(i-1)) {
			panic("spans must be ordered and non-overlapping")
		}
	}
	c.Columns = *keyCtx.Columns
	c.Spans = spans
	c.Spans.makeImmutable()
}

// IsContradiction returns true if there are no spans in the constraint.
func (c *Constraint) IsContradiction() bool {
	return c.Spans.Count() == 0
}

// IsUnconstrained returns true if the constraint contains an unconstrained
// span.
func (c *Constraint) IsUnconstrained() bool {
	return c.Spans.Count() == 1 && c.Spans.Get(0).IsUnconstrained()
}

// UnionWith merges the spans of the given constraint into this constraint.  The
// columns of both constraints must be the same. Constrained columns in the
// merged constraint can have values that are part of either of the input
// constraints.
func (c *Constraint) UnionWith(evalCtx *tree.EvalContext, other *Constraint) {
	if c.IsUnconstrained() || other.IsContradiction() {
		return
	}

	// Use variation on merge sort, because both sets of spans are ordered and
	// non-overlapping.

	left := &c.Spans
	leftIndex := 0
	right := &other.Spans
	rightIndex := 0
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)
	result := MakeSpans(left.Count() + right.Count())

	for leftIndex < left.Count() || rightIndex < right.Count() {
		if rightIndex < right.Count() {
			if leftIndex >= left.Count() ||
				left.Get(leftIndex).Compare(keyCtx, right.Get(rightIndex)) > 0 {
				// Swap the two sets, so that going forward the current left
				// span starts before the current right span.
				left, right = right, left
				leftIndex, rightIndex = rightIndex, leftIndex
			}
		}

		// Merge this span with any overlapping spans in left or right. Initially,
		// it can only overlap with spans in right, but after the merge we can
		// have new overlaps; hence why this is a loop and we check against both
		// left and right. For example:
		//   left : [/1 - /10] [/20 - /30] [/40 - /50]
		//   right: [/5 - /25] [/30 - /40]
		//                             span
		//   initial:                [/1 - /10]
		//   merge with [/5 - /25]:  [/1 - /25]
		//   merge with [/20 - /30]: [/1 - /30]
		//   merge with [/30 - /40]: [/1 - /40]
		//   merge with [/40 - /50]: [/1 - /50]
		//
		mergeSpan := left.Get(leftIndex).Copy()
		leftIndex++
		for {
			// Note that Span.TryUnionWith returns false for a different reason
			// than Constraint.tryUnionWith. Span.TryUnionWith returns false
			// when the spans are not contiguous, and therefore the union cannot
			// be represented as a valid Span. Constraint.tryUnionWith returns
			// false when the merged spans are unconstrained (cover entire key
			// range), and therefore the union cannot be represented as a valid
			// Constraint.
			var ok bool
			if leftIndex < left.Count() {
				if mergeSpan.TryUnionWith(keyCtx, left.Get(leftIndex)) {
					leftIndex++
					ok = true
				}
			}
			if rightIndex < right.Count() {
				if mergeSpan.TryUnionWith(keyCtx, right.Get(rightIndex)) {
					rightIndex++
					ok = true
				}
			}

			// If neither union succeeded, then it means either:
			//   1. The spans don't merge into a single contiguous span, and will
			//      need to be represented separately in this constraint.
			//   2. There are no more spans to merge.
			if !ok {
				break
			}
		}
		result.Append(&mergeSpan)
	}

	c.Spans = result
	c.Spans.makeImmutable()
}

// IntersectWith intersects the spans of this constraint with those in the
// given constraint and updates this constraint with any overlapping spans. The
// columns of both constraints must be the same. If there are no overlapping
// spans, then the intersection is empty, and tryIntersectWith returns false.
// If a constraint set has even one empty constraint, then the entire set
// should be marked as empty and all constraints removed.
func (c *Constraint) IntersectWith(evalCtx *tree.EvalContext, other *Constraint) {
	if c.IsContradiction() || other.IsUnconstrained() {
		return
	}

	// Use variation on merge sort, because both sets of spans are ordered and
	// non-overlapping.

	left := &c.Spans
	leftIndex := 0
	right := &other.Spans
	rightIndex := 0
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)
	result := MakeSpans(left.Count())

	for leftIndex < left.Count() && rightIndex < right.Count() {
		if left.Get(leftIndex).StartsAfter(keyCtx, right.Get(rightIndex)) {
			rightIndex++
			continue
		}

		mergeSpan := left.Get(leftIndex).Copy()
		if !mergeSpan.TryIntersectWith(keyCtx, right.Get(rightIndex)) {
			leftIndex++
			continue
		}
		result.Append(&mergeSpan)

		// Skip past whichever span ends first, or skip past both if they have
		// the same endpoint.
		cmp := left.Get(leftIndex).CompareEnds(keyCtx, right.Get(rightIndex))
		if cmp <= 0 {
			leftIndex++
		}
		if cmp >= 0 {
			rightIndex++
		}
	}

	c.Spans = result
	c.Spans.makeImmutable()
}

func (c *Constraint) String() string {
	var b strings.Builder
	b.WriteString(c.Columns.String())
	b.WriteString(": ")
	if c.IsUnconstrained() {
		b.WriteString("unconstrained")
	} else if c.IsContradiction() {
		b.WriteString("contradiction")
	} else {
		b.WriteString(c.Spans.String())
	}
	return b.String()
}
