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
func (c *Constraint) Init(keyCtx *KeyContext, spans *Spans) {
	for i := 1; i < spans.Count(); i++ {
		if !spans.Get(i).StartsStrictlyAfter(keyCtx, spans.Get(i-1)) {
			panic("spans must be ordered and non-overlapping")
		}
	}
	c.Columns = keyCtx.Columns
	c.Spans = *spans
	c.Spans.makeImmutable()
}

// InitSingleSpan initializes the constraint to the columns in the key context
// and with one span.
func (c *Constraint) InitSingleSpan(keyCtx *KeyContext, span *Span) {
	c.Columns = keyCtx.Columns
	c.Spans.InitSingleSpan(span)
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
	var result Spans
	result.Alloc(left.Count() + right.Count())

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
		mergeSpan := *left.Get(leftIndex)
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
	var result Spans
	result.Alloc(left.Count())

	for leftIndex < left.Count() && rightIndex < right.Count() {
		if left.Get(leftIndex).StartsAfter(&keyCtx, right.Get(rightIndex)) {
			rightIndex++
			continue
		}

		mergeSpan := *left.Get(leftIndex)
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

// ContainsSpan returns true if the constraint contains the given span (or a
// span that contains it).
func (c *Constraint) ContainsSpan(evalCtx *tree.EvalContext, sp *Span) bool {
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)
	// Binary search to find an overlapping span.
	for l, r := 0, c.Spans.Count()-1; l <= r; {
		m := (l + r) / 2
		cSpan := c.Spans.Get(m)
		if sp.StartsAfter(&keyCtx, cSpan) {
			l = m + 1
		} else if cSpan.StartsAfter(&keyCtx, sp) {
			r = m - 1
		} else {
			// The spans must overlap. Check if sp is fully contained.
			return sp.CompareStarts(&keyCtx, cSpan) >= 0 &&
				sp.CompareEnds(&keyCtx, cSpan) <= 0
		}
	}
	return false
}

// Combine refines the receiver constraint using constraints on a suffix of the
// same list of columns. For example:
//  c:      /a/b: [/1 - /2] [/4 - /4]
//  other:  /b: [/5 - /5]
//  result: /a/b: [/1/5 - /2/5] [/4/5 - /4/5]
func (c *Constraint) Combine(evalCtx *tree.EvalContext, other *Constraint) {
	if !other.Columns.IsStrictSuffixOf(&c.Columns) {
		// Note: we don't want to let the c and other pointers escape by passing
		// them directly to Sprintf.
		panic(fmt.Sprintf("%s not a suffix of %s", other.String(), c.String()))
	}
	if c.IsUnconstrained() || c.IsContradiction() || other.IsUnconstrained() {
		return
	}
	if other.IsContradiction() {
		c.Spans = Spans{}
		c.Spans.makeImmutable()
		return
	}
	offset := c.Columns.Count() - other.Columns.Count()

	var result Spans
	// We only initialize result if we determine that we need to modify the list
	// of spans.
	var resultInitialized bool
	keyCtx := KeyContext{Columns: c.Columns, EvalCtx: evalCtx}

	for i := 0; i < c.Spans.Count(); i++ {
		sp := *c.Spans.Get(i)

		startLen, endLen := sp.start.Length(), sp.end.Length()

		// Try to extend the start and end keys.
		// TODO(radu): we could look at offsets in between 0 and startLen/endLen and
		// potentially tighten up the spans (e.g. (a, b) > (1, 2) AND b > 10).

		if startLen == endLen && startLen == offset &&
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

			if !resultInitialized {
				resultInitialized = true
				result.Alloc(c.Spans.Count() + other.Spans.Count())
				for j := 0; j < i; j++ {
					result.Append(c.Spans.Get(j))
				}
			}
			for j := 0; j < other.Spans.Count(); j++ {
				extSp := other.Spans.Get(j)
				var newSp Span
				newSp.Init(
					sp.start.Concat(extSp.start), extSp.startBoundary,
					sp.end.Concat(extSp.end), extSp.endBoundary,
				)
				result.Append(&newSp)
			}
			continue
		}

		var modified bool
		if startLen == offset && sp.startBoundary == IncludeBoundary {
			// We can advance the starting boundary. Calculate constraints for the
			// column that follows. If we have multiple constraints, we can only use
			// the start of the first one to tighten the span.
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
			extSp := other.Spans.Get(0)
			if extSp.start.Length() > 0 {
				sp.start = sp.start.Concat(extSp.start)
				sp.startBoundary = extSp.startBoundary
				modified = true
			}
		}
		// End key case is symmetric with the one above.
		if endLen == offset && sp.endBoundary == IncludeBoundary {
			extSp := other.Spans.Get(other.Spans.Count() - 1)
			if extSp.end.Length() > 0 {
				sp.end = sp.end.Concat(extSp.end)
				sp.endBoundary = extSp.endBoundary
				modified = true
			}
		}
		if modified {
			// We only initialize result if we need to modify the list of spans.
			if !resultInitialized {
				resultInitialized = true
				result.Alloc(c.Spans.Count())
				for j := 0; j < i; j++ {
					result.Append(c.Spans.Get(j))
				}
			}
			// The span can become invalid (empty). For example:
			//   /1/2: [/1 - /1/2]
			//   /2: [/5 - /5]
			// This results in an invalid span [/1/5 - /1/2] which we must discard.
			if sp.start.Compare(&keyCtx, sp.end, sp.startExt(), sp.endExt()) < 0 {
				result.Append(&sp)
			}
		} else {
			if resultInitialized {
				result.Append(&sp)
			}
		}
	}
	if resultInitialized {
		c.Spans = result
		c.Spans.makeImmutable()
	}
}

// ConsolidateSpans merges spans that have consecutive boundaries. For example:
//   [/1 - /2] [/3 - /4] becomes [/1 - /4].
func (c *Constraint) ConsolidateSpans(evalCtx *tree.EvalContext) {
	keyCtx := KeyContext{Columns: c.Columns, EvalCtx: evalCtx}
	var result Spans
	for i := 1; i < c.Spans.Count(); i++ {
		last := c.Spans.Get(i - 1)
		sp := c.Spans.Get(i)
		if last.endBoundary == IncludeBoundary && sp.startBoundary == IncludeBoundary &&
			sp.start.IsNextKey(&keyCtx, last.end) {
			// We only initialize `result` if we need to change something.
			if result.Count() == 0 {
				result.Alloc(c.Spans.Count() - 1)
				for j := 0; j < i; j++ {
					result.Append(c.Spans.Get(j))
				}
			}
			result.Get(result.Count() - 1).end = sp.end
		} else {
			if result.Count() != 0 {
				result.Append(sp)
			}
		}
	}
	if result.Count() != 0 {
		c.Spans = result
		c.Spans.makeImmutable()
	}
}

// ExactPrefix returns the length of the longest column prefix which are
// constrained to a single value. For example:
//   /a/b/c: [/1/2/3 - /1/2/3]                    ->  ExactPrefix = 3
//   /a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]  ->  ExactPrefix = 2
//   /a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/3/8]  ->  ExactPrefix = 1
//   /a/b/c: [/1/2/3 - /1/2/3] [/3 - /4]          ->  ExactPrefix = 0
func (c *Constraint) ExactPrefix(evalCtx *tree.EvalContext) int {
	if c.IsContradiction() {
		return 0
	}

	for col := 0; ; col++ {
		// Check if all spans have the same value for this column.
		var val tree.Datum
		for i := 0; i < c.Spans.Count(); i++ {
			sp := c.Spans.Get(i)
			if sp.start.Length() <= col || sp.end.Length() <= col {
				return col
			}
			startVal := sp.start.Value(col)
			if startVal.Compare(evalCtx, sp.end.Value(col)) != 0 {
				return col
			}
			if i == 0 {
				val = startVal
			} else if startVal.Compare(evalCtx, val) != 0 {
				return col
			}
		}
	}
}
