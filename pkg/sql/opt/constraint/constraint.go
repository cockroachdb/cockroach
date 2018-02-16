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

// Constraint specifies the possible set of values that one or more columns
// will have in the result set. If this is a single column constraint, then
// that column's value will always be part of one of the spans in this
// constraint. If this is a multi-column constraint, then the combination of
// column valus will always be part of one of the spans.
//
// Constrained columns are specified as an ordered list, and span key values
// correspond to those columns by position. Constraints are inferred from
// scalar filter conditions, since these restrict the set of possible results.
// Constraints over different combinations of columns can be combined together
// in a constraint set, which is a conjunction of constraints. See the
// ConstraintSet struct comment for more details.
//
// A few examples:
//  - a constraint on @1 > 1: a single span             (/@1 (/1 - ∞])
//  - a constraint on @1 = 1 AND @2 >= 1: a single span (/@1/@2 [/1/1 - /1])
//  - a constraint on @1 < 5 OR @1 > 10: multiple spans (/@1 [Ø - /5) (10 - ∞))
type Constraint struct {
	// firstCol holds the first column index and otherCols hold any indexes
	// beyond the first. These are separated in order to optimize for the common
	// case of a single-column constraint.
	firstCol  opt.ColumnIndex
	otherCols []opt.ColumnIndex

	// firstSpan holds the first span and otherSpans hold any spans beyond the
	// first. These are separated in order to optimize for the common case of a
	// single-span constraint. The spans are always maintained in sorted order.
	firstSpan  Span
	otherSpans []Span

	// spanCount is the total number of spans in the constraint, including both
	// the first and other constraints.
	spanCount uint32
}

// init is for package use only. External callers should call NewConstraintSet.
func (c *Constraint) init(col opt.ColumnIndex, sp *Span) {
	if sp.IsEmpty() {
		// Empty span should result in empty constraint set, not empty
		// constraint.
		panic("constraint should not be created for empty span")
	}
	if sp.IsFull() {
		// Constraint with full span does not need to be added to the constraint
		// set, since it's already implied.
		panic("constraint should not be created for full span")
	}
	c.firstCol = col
	c.firstSpan = *sp
	c.spanCount = 1
}

// initComposite is for package use only. External callers should call
// NewForColumn or NewForColumns.
func (c *Constraint) initComposite(cols []opt.ColumnIndex, sp *Span) {
	c.init(cols[0], sp)
	if len(cols) > 1 {
		c.otherCols = cols[1:]
	}
}

// ColumnCount returns the number of constrained columns (always at least one).
func (c *Constraint) ColumnCount() int {
	// There's always at least one column.
	return 1 + len(c.otherCols)
}

// Column returns the index of the nth constrained column. Together with the
// ColumnCount method, Column allows iteration over the list of constrained
// columns (since there is no method to return a slice of columns).
func (c *Constraint) Column(nth int) opt.ColumnIndex {
	// There's always at least one column.
	if nth == 0 {
		return c.firstCol
	}
	return c.otherCols[nth-1]
}

// SpanCount returns the total number of spans in the constraint (always at
// least one).
func (c *Constraint) SpanCount() int {
	return int(c.spanCount)
}

// Span returns the nth span. Together with the SpanCount method, Span allows
// iteration over the list of spans (since there is no method to return a slice
// of spans).
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
// constraints. If the merge results in a full span (i.e. no constraint), then
// tryUnionWith returns false. A constraint with a full span should never be
// added to a constraint set, because the absence of a constraint in the set
// already implies the columns are unconstrained.
func (c *Constraint) tryUnionWith(evalCtx *tree.EvalContext, other *Constraint) bool {
	// Use variation on merge sort, because both sets of spans are ordered and
	// non-overlapping.
	var firstSpan Span
	var otherSpans []Span
	left := c
	leftIndex := 0
	right := other
	rightIndex := 0

	for leftIndex < left.SpanCount() || rightIndex < right.SpanCount() {
		if rightIndex < right.SpanCount() {
			if leftIndex >= left.SpanCount() ||
				left.Span(leftIndex).Compare(evalCtx, right.Span(rightIndex)) > 0 {
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
		mergeSpan := *left.Span(leftIndex)
		leftIndex++
		for {
			var ok bool
			if leftIndex < left.SpanCount() {
				if mergeSpan.TryUnionWith(evalCtx, left.Span(leftIndex)) {
					leftIndex++
					ok = true
				}
			}
			if rightIndex < right.SpanCount() {
				if mergeSpan.TryUnionWith(evalCtx, right.Span(rightIndex)) {
					rightIndex++
					ok = true
				}
			}

			// If neither union succeeded (because spans are disjoint), then
			// this merge is complete.
			if !ok {
				break
			}
		}

		if otherSpans == nil {
			if firstSpan.IsEmpty() {
				firstSpan = mergeSpan
			} else {
				maxRemaining := left.SpanCount() - leftIndex + right.SpanCount() - rightIndex
				otherSpans = make([]Span, 1, 1+maxRemaining)
				otherSpans[0] = mergeSpan
			}
		} else {
			otherSpans = append(otherSpans, mergeSpan)
		}
	}

	if firstSpan.IsFull() {
		return false
	}

	// Not the full span, so update this constraint and return true.
	c.firstSpan = firstSpan
	c.otherSpans = otherSpans
	c.spanCount = uint32(1 + len(otherSpans))
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

	index := 0
	otherIndex := 0

	for index < c.SpanCount() && otherIndex < other.SpanCount() {
		if c.Span(index).StartsAfter(evalCtx, other.Span(otherIndex)) {
			otherIndex++
			continue
		}

		mergeSpan := *c.Span(index)
		if !mergeSpan.TryIntersectWith(evalCtx, other.Span(otherIndex)) {
			index++
			continue
		}

		if otherSpans == nil {
			if firstSpan.IsEmpty() {
				firstSpan = mergeSpan
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
		cmp := c.Span(index).CompareEnds(evalCtx, other.Span(otherIndex))
		if cmp <= 0 {
			index++
		}
		if cmp >= 0 {
			otherIndex++
		}
	}

	if firstSpan.IsEmpty() {
		return false
	}

	c.firstSpan = firstSpan
	c.otherSpans = otherSpans
	c.spanCount = uint32(1 + len(otherSpans))
	return true
}

func (c *Constraint) String() string {
	var buf bytes.Buffer

	buf.WriteRune('(')

	for i := 0; i < c.ColumnCount(); i++ {
		buf.WriteRune('/')
		buf.WriteString(fmt.Sprintf("%d", c.Column(i)))
	}

	buf.WriteRune(' ')

	for i := 0; i < c.SpanCount(); i++ {
		if i > 0 {
			buf.WriteRune(' ')
		}
		buf.WriteString(c.Span(i).String())
	}

	buf.WriteRune(')')
	return buf.String()
}
