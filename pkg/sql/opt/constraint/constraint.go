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
	"fmt"
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
func (c *Constraint) Init(keyCtx *KeyContext, spans Spans) {
	for i := 1; i < spans.Count(); i++ {
		if !spans.Get(i).StartsStrictlyAfter(keyCtx, spans.Get(i-1)) {
			panic("spans must be ordered and non-overlapping")
		}
	}
	c.Columns = keyCtx.Columns
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
	if !c.Columns.Equals(&other.Columns) {
		panic("column mismatch")
	}
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
				left.Get(leftIndex).Compare(&keyCtx, right.Get(rightIndex)) > 0 {
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
				if mergeSpan.TryUnionWith(&keyCtx, left.Get(leftIndex)) {
					leftIndex++
					ok = true
				}
			}
			if rightIndex < right.Count() {
				if mergeSpan.TryUnionWith(&keyCtx, right.Get(rightIndex)) {
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
	if !c.Columns.Equals(&other.Columns) {
		panic("column mismatch")
	}
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
		if left.Get(leftIndex).StartsAfter(&keyCtx, right.Get(rightIndex)) {
			rightIndex++
			continue
		}

		mergeSpan := left.Get(leftIndex).Copy()
		if !mergeSpan.TryIntersectWith(&keyCtx, right.Get(rightIndex)) {
			leftIndex++
			continue
		}
		result.Append(&mergeSpan)

		// Skip past whichever span ends first, or skip past both if they have
		// the same endpoint.
		cmp := left.Get(leftIndex).CompareEnds(&keyCtx, right.Get(rightIndex))
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

func (c Constraint) String() string {
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

// SubsetOf returns true if the receiver constraint is stronger than the other
// constraint (i.e. c.Spans are completely contained in other.Spans).
//
// The two constraints must be on the same set of columns.
func (c *Constraint) SubsetOf(evalCtx *tree.EvalContext, other *Constraint) bool {
	if !c.Columns.Equals(&other.Columns) {
		panic("column mismatch")
	}

	left := &c.Spans
	right := &other.Spans
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)

	for leftIndex, rightIndex := 0, 0; leftIndex < left.Count(); {
		if rightIndex == right.Count() {
			// There is no span that overlaps with leftSpan.
			return false
		}
		leftSpan := left.Get(leftIndex)
		rightSpan := right.Get(rightIndex)
		if leftSpan.StartsAfter(&keyCtx, rightSpan) {
			rightIndex++
			continue
		}
		// Check if rightSpan includes leftSpan.
		if leftSpan.CompareStarts(&keyCtx, rightSpan) < 0 ||
			leftSpan.CompareEnds(&keyCtx, rightSpan) > 0 {
			return false
		}
		leftIndex++
	}
	return true
}

// CutFirstColumn removes the first column from the constraint and tries to
// deduce a constraint on the remaining suffix. If no constraint can be deduced,
// the constraint becomes unconstrained.
//
// We can deduce a constraint only if all the spans have equal start and end
// keys on the first column. For example:
//   before: /a/b/c/d: [/1/5 - /1/6] [/2/3 - /2/4]
//   after:  /b/c/d: [/3 - /4] [/5 - /6]
// In contrast:
//   before: /a/b/c/d: [/1/2 - /3/1]
//   after:  /b/c/d: unconstrained.
func (c *Constraint) CutFirstColumn(evalCtx *tree.EvalContext) {
	c.Columns.firstCol = c.Columns.otherCols[0]
	c.Columns.otherCols = c.Columns.otherCols[1:]
	if c.Spans.Count() == 0 {
		return
	}
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i)
		if sp.start.IsEmpty() || sp.end.IsEmpty() ||
			sp.start.Value(0).Compare(evalCtx, sp.end.Value(0)) != 0 {
			c.Spans = SingleSpan(&UnconstrainedSpan)
			c.Spans.makeImmutable()
			return
		}
	}
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)
	result := MakeSpans(c.Spans.Count())
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i).Copy()
		sp.start = sp.start.CutFront(1)
		sp.end = sp.end.CutFront(1)
		result.Append(&sp)
	}
	result.SortAndMerge(&keyCtx)

	c.Spans = result
	c.Spans.makeImmutable()
}

// Combine refines the receiver constraint using constraints on suffixes of the
// same list of columns.
// Specifically, if the receiver constraint is on columns a,b,c,d,etc then
//  - suffixes[0] is a constraint on columns b,c,d, etc
//  - suffixes[1] is a constraint on columns c,d, etc
//  - and so on.
func (c *Constraint) Combine(evalCtx *tree.EvalContext, suffixes []Constraint) {
	if c.IsUnconstrained() || c.IsContradiction() {
		return
	}
	// Verify the invariants of the inputs.
	for i := range suffixes {
		if suffixes[i].IsContradiction() {
			c.Spans = Spans{}
			c.Spans.makeImmutable()
			return
		}
		cols := suffixes[i].Columns
		if cols.Count() != c.Columns.Count()-i-1 {
			panic(fmt.Sprintf("invalid constraint %d: %s", i, suffixes[i]))
		}
		for j := 0; j < cols.Count(); j++ {
			if cols.Get(j) != c.Columns.Get(i+1+j) {
				panic(fmt.Sprintf("invalid constraint %d: %s", i, suffixes[i]))
			}
		}
	}
	// We take the following approach:
	//  - maintain the unprocessed spans in a stack (last span on top)
	//  - while the stack is not empty: pop the last span from the stack and try
	//    to extend it
	//     - if we can't extend it, copy it to results.
	//     - if we can extend it, push back the extended span onto the stack (to
	//       try to extend it some more)
	//     - if it needs to be split into multiple spans, they all get pushed onto
	//       the stack.
	//  - finally, to reverse the resulting spans (we obtain them in reverse
	//    order).
	result := MakeSpans(c.Spans.Count())
	stack := MakeSpans(c.Spans.Count())
	for i := 0; i < c.Spans.Count(); i++ {
		stack.Append(c.Spans.Get(i))
	}
	keyCtx := KeyContext{Columns: c.Columns, EvalCtx: evalCtx}
	var sp Span
	for stack.Count() > 0 {
		// Pop a span from the stack.
		sp = stack.Get(stack.Count() - 1).Copy()
		stack.Truncate(stack.Count() - 1)

		startLen, endLen := sp.start.Length(), sp.end.Length()

		// We try to extend the start and end keys.
		// TODO(radu): we could look at offsets in between 0 and startLen/endLen and
		// potentially tighten up the spans (e.g. (a, b) > (1, 2) AND b > 10).

		if startLen == endLen && startLen > 0 && startLen <= len(suffixes) &&
			sp.start.Compare(&keyCtx, sp.end, ExtendLow, ExtendLow) == 0 {
			// Special case when the start and end keys are equal (i.e. an exact value
			// on this column). This is the only case where we can break up a single
			// span into multiple spans. Note that this implies inclusive boundaries.
			//
			// For example:
			//  @1 = 1 AND @2 IN (3, 4, 5)
			//  constraint[0]:
			//    [/1 - /1]
			//  constraint[1]:
			//    [/3 - /3]
			//    [/4 - /4]
			//    [/5 - /5]
			//  We break up the span to get:
			//    [/1/3 - /1/3]
			//    [/1/4 - /1/4]
			//    [/1/5 - /1/5]
			s := suffixes[startLen-1]
			if s.IsUnconstrained() {
				result.Append(&sp)
				continue
			}
			for j := 0; j < s.Spans.Count(); j++ {
				extSp := s.Spans.Get(j)
				var newSp Span
				newSp.Set(
					&keyCtx,
					sp.start.Concat(extSp.start), extSp.startBoundary,
					sp.end.Concat(extSp.end), extSp.endBoundary,
				)
				stack.Append(&newSp)
			}
			continue
		}
		var modified bool
		if startLen > 0 && startLen <= len(suffixes) && sp.startBoundary == IncludeBoundary {
			// We can advance the starting boundary. Calculate constraints for the
			// column that follows.
			if s := suffixes[startLen-1]; !s.IsUnconstrained() {
				// If we have multiple constraints, we can only use the start of the
				// first one to tighten the span.
				// For example:
				//   @1 >= 2 AND @2 IN (1, 2, 3).
				//   constraints[0]:
				//     [/2 - ]
				//   constraints[1]:
				//     [/1 - /1]
				//     [/2 - /2]
				//     [/3 - /3]
				//   The best we can do is tighten the span to:
				//     [/2/1 - ]

				extSp := s.Spans.Get(0)
				if extSp.start.Length() > 0 {
					sp.start = sp.start.Concat(extSp.start)
					sp.startBoundary = extSp.startBoundary
					modified = true
				}
			}
		}
		// End key case is symmetric with the one above.
		if endLen > 0 && endLen <= len(suffixes) && sp.endBoundary == IncludeBoundary {
			if s := suffixes[endLen-1]; !s.IsUnconstrained() {
				extSp := s.Spans.Get(s.Spans.Count() - 1)
				if extSp.end.Length() > 0 {
					sp.end = sp.end.Concat(extSp.end)
					sp.endBoundary = extSp.endBoundary
					modified = true
				}
			}
		}
		if modified {
			if sp.start.Compare(&keyCtx, sp.end, sp.startExt(), sp.endExt()) >= 0 {
				// The span became invalid (empty) and needs to be removed. For example:
				//   /1/2: [/1 - /1/2]
				//   /2: [/5 - /5]
				// This results in an invalid span [/1/5 - /1/2] which we must discard.
				continue
			}
			// Put it back on the stack for further extension
			stack.Append(&sp)
		} else {
			result.Append(&sp)
		}
	}
	// We get the spans in reverse order, because of how we use the stack.
	for i, j := 0, result.Count()-1; i < j; i, j = i+1, j-1 {
		si := result.Get(i)
		sj := result.Get(j)
		*si, *sj = *sj, *si
	}
	c.Spans = result
	c.Spans.makeImmutable()
}
