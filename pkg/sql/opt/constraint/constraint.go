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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
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

	// firstSpan holds the first span and otherSpans hold any spans beyond the
	// first. These are separated in order to optimize for the common case of a
	// single-span constraint. The spans are always maintained in sorted order.
	firstSpan  Span
	otherSpans []Span
}

// init is for package use only. External callers should call NewConstraintSet.
func (c *Constraint) init(col opt.OrderingColumn, sp *Span) {
	if sp.IsUnconstrained() {
		// Don't allow unconstrained span in a constraint. Instead just discard
		// the constraint, as its absence already means it's unconstrained.
		panic("unconstrained span cannot be part of a constraint")
	}
	c.Columns.InitSingle(col)
	c.firstSpan = *sp
	c.firstSpan.makeImmutable()
}

// initComposite is for package use only. External callers should call
// NewForColumn or NewForColumns.
func (c *Constraint) initComposite(cols []opt.OrderingColumn, sp *Span) {
	if sp.IsUnconstrained() {
		// Don't allow unconstrained span in a constraint. Instead just discard
		// the constraint, as its absence already means it's unconstrained.
		panic("unconstrained span cannot be part of a constraint")
	}
	c.Columns.Init(cols)
	c.firstSpan = *sp
	c.firstSpan.makeImmutable()
}

// SpanCount returns the total number of spans in the constraint (always at
// least one).
func (c *Constraint) SpanCount() int {
	return 1 + len(c.otherSpans)
}

// Span returns the nth span. Together with the SpanCount method, Span allows
// iteration over the list of spans (since there is no method to return a slice
// of spans).
// Mutating the returned span is not permitted.
func (c *Constraint) Span(nth int) *Span {
	// There's always at least one span.
	if nth == 0 {
		return &c.firstSpan
	}
	return &c.otherSpans[nth-1]
}

// tryUnionWith merges the spans of the given constraint into this constraint.
// The columns of both constraints must be the same. Constrained columns in the
// merged constraint can have values that are part of either of the input
// constraints. If the merge results in an unconstrained span, then
// tryUnionWith returns false. An unconstrained span should never be part of a
// constraint, as then the constraint would not constrain anything, and should
// not be added to the constraint set.
func (c *Constraint) tryUnionWith(evalCtx *tree.EvalContext, other *Constraint) bool {
	// Use variation on merge sort, because both sets of spans are ordered and
	// non-overlapping.
	var firstSpan Span
	var otherSpans []Span

	foundSpan := false
	left := c
	leftIndex := 0
	right := other
	rightIndex := 0
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)

	for leftIndex < left.SpanCount() || rightIndex < right.SpanCount() {
		if rightIndex < right.SpanCount() {
			if leftIndex >= left.SpanCount() ||
				left.Span(leftIndex).Compare(keyCtx, right.Span(rightIndex)) > 0 {
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
		mergeSpan := left.Span(leftIndex).Copy()
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
			if leftIndex < left.SpanCount() {
				if mergeSpan.TryUnionWith(keyCtx, left.Span(leftIndex)) {
					leftIndex++
					ok = true
				}
			}
			if rightIndex < right.SpanCount() {
				if mergeSpan.TryUnionWith(keyCtx, right.Span(rightIndex)) {
					rightIndex++
					ok = true
				}
			}

			// If union has resulted in an unconstrained Span, then return false,
			// as that is not valid in a Constraint.
			if mergeSpan.IsUnconstrained() {
				return false
			}

			// If neither union succeeded, then it means either:
			//   1. The spans don't merge into a single contiguous span, and will
			//      need to be represented separately in this constraint.
			//   2. There are no more spans to merge.
			if !ok {
				break
			}
		}
		mergeSpan.makeImmutable()

		if otherSpans == nil {
			if !foundSpan {
				firstSpan = mergeSpan
				foundSpan = true
			} else {
				maxRemaining := left.SpanCount() - leftIndex + right.SpanCount() - rightIndex
				otherSpans = make([]Span, 1, 1+maxRemaining)
				otherSpans[0] = mergeSpan
			}
		} else {
			otherSpans = append(otherSpans, mergeSpan)
		}
	}

	if firstSpan.IsUnconstrained() {
		panic("unconstrained span should have been handled in the merge loop above")
	}

	// We've got a valid constraint.
	c.firstSpan = firstSpan
	c.otherSpans = otherSpans
	return true
}

// tryIntersectWith intersects the spans of this constraint with those in the
// given constraint and updates this constraint with any overlapping spans. The
// columns of both constraints must be the same. If there are no overlapping
// spans, then the intersection is empty, and tryIntersectWith returns false.
// If a constraint set has even one empty constraint, then the entire set
// should be marked as empty and all constraints removed.
func (c *Constraint) tryIntersectWith(evalCtx *tree.EvalContext, other *Constraint) bool {
	// Use variation on merge sort, because both sets of spans are ordered and
	// non-overlapping.
	var firstSpan Span
	var otherSpans []Span

	empty := true
	index := 0
	otherIndex := 0
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)

	for index < c.SpanCount() && otherIndex < other.SpanCount() {
		if c.Span(index).StartsAfter(keyCtx, other.Span(otherIndex)) {
			otherIndex++
			continue
		}

		mergeSpan := c.Span(index).Copy()
		if !mergeSpan.TryIntersectWith(keyCtx, other.Span(otherIndex)) {
			index++
			continue
		}
		mergeSpan.makeImmutable()

		if otherSpans == nil {
			if empty {
				firstSpan = mergeSpan
				empty = false
			} else {
				maxRemaining := c.SpanCount() - index
				otherSpans = make([]Span, 1, maxRemaining)
				otherSpans[0] = mergeSpan
			}
		} else {
			otherSpans = append(otherSpans, mergeSpan)
		}

		// Skip past whichever span ends first, or skip past both if they have
		// the same endpoint.
		cmp := c.Span(index).CompareEnds(keyCtx, other.Span(otherIndex))
		if cmp <= 0 {
			index++
		}
		if cmp >= 0 {
			otherIndex++
		}
	}

	// If empty was never set to false, then the intersection must be empty.
	if empty {
		return false
	}

	c.firstSpan = firstSpan
	c.otherSpans = otherSpans
	return true
}

func (c *Constraint) String() string {
	var buf bytes.Buffer

	for i := 0; i < c.Columns.Count(); i++ {
		buf.WriteRune('/')
		buf.WriteString(fmt.Sprintf("%d", c.Columns.Get(i)))
	}

	buf.WriteString(": ")

	for i := 0; i < c.SpanCount(); i++ {
		if i > 0 {
			buf.WriteRune(' ')
		}
		buf.WriteString(c.Span(i).String())
	}

	return buf.String()
}
