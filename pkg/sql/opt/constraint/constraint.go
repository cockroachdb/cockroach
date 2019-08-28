// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraint

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
			panic(errors.AssertionFailedf("spans must be ordered and non-overlapping"))
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
		panic(errors.AssertionFailedf("column mismatch"))
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
		panic(errors.AssertionFailedf("column mismatch"))
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
		panic(errors.AssertionFailedf("%s not a suffix of %s", other.String(), c.String()))
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
			r := result.Get(result.Count() - 1)
			r.end = sp.end
			r.endBoundary = sp.endBoundary
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
//   /a/b/c: [/1/2/3 - /1/2/3] [/1/3/3 - /1/3/3]  ->  ExactPrefix = 1
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

// ConstrainedColumns returns the number of columns which are constrained by
// the Constraint. For example:
//   /a/b/c: [/1/1 - /1] [/3 - /3]
// has 2 constrained columns. This may be less than the total number of columns
// in the constraint, especially if it represents an index constraint.
func (c *Constraint) ConstrainedColumns(evalCtx *tree.EvalContext) int {
	count := 0
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i)
		start := sp.StartKey()
		end := sp.EndKey()
		if start.Length() > count {
			count = start.Length()
		}
		if end.Length() > count {
			count = end.Length()
		}
	}

	return count
}

// Prefix returns the length of the longest prefix of columns for which all the
// spans have the same start and end values. For example:
//   /a/b/c: [/1/1/1 - /1/1/2] [/3/3/3 - /3/3/4]
// has prefix 2.
//
// Note that Prefix returns a value that is greater than or equal to the value
// returned by ExactPrefix. For example:
//   /a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/3/8] -> ExactPrefix = 1, Prefix = 1
//   /a/b/c: [/1/2/3 - /1/2/3] [/1/3/3 - /1/3/3] -> ExactPrefix = 1, Prefix = 3
func (c *Constraint) Prefix(evalCtx *tree.EvalContext) int {
	prefix := 0
	for ; prefix < c.Columns.Count(); prefix++ {
		for i := 0; i < c.Spans.Count(); i++ {
			sp := c.Spans.Get(i)
			start := sp.StartKey()
			end := sp.EndKey()
			if start.Length() <= prefix || end.Length() <= prefix ||
				start.Value(prefix).Compare(evalCtx, end.Value(prefix)) != 0 {
				return prefix
			}
		}
	}

	return prefix
}

// ExtractConstCols returns a set of columns which are restricted to be
// constant by the constraint.
func (c *Constraint) ExtractConstCols(evalCtx *tree.EvalContext) opt.ColSet {
	var res opt.ColSet
	pre := c.ExactPrefix(evalCtx)
	for i := 0; i < pre; i++ {
		res.Add(c.Columns.Get(i).ID())
	}
	return res
}

// ExtractNotNullCols returns a set of columns that cannot be NULL when the
// constraint holds.
func (c *Constraint) ExtractNotNullCols(evalCtx *tree.EvalContext) opt.ColSet {
	if c.IsUnconstrained() || c.IsContradiction() {
		return opt.ColSet{}
	}

	var res opt.ColSet

	// If we have a span where the start and end key value diverge for a column,
	// none of the columns that follow can be not-null. For example:
	//   /1/2/3: [/1/2/3 - /1/4/1]
	// Because the span is not restricted to a single value on column 2, column 3
	// can take any value, like /1/3/NULL.
	//
	// Find the longest prefix of columns for which all the spans have the same
	// start and end values. For example:
	//   [/1/1/1 - /1/1/2] [/3/3/3 - /3/3/4]
	// has prefix 2. Only these columns and the first following column can be
	// known to be not-null.
	prefix := c.Prefix(evalCtx)
	for i := 0; i < prefix; i++ {
		// hasNull identifies cases like [/1/NULL/1 - /1/NULL/2].
		hasNull := false
		for j := 0; j < c.Spans.Count(); j++ {
			start := c.Spans.Get(j).StartKey()
			hasNull = hasNull || start.Value(i) == tree.DNull
		}
		if !hasNull {
			res.Add(c.Columns.Get(i).ID())
		}
	}
	if prefix == c.Columns.Count() {
		return res
	}

	// Now look at the first column that follows the prefix.
	col := c.Columns.Get(prefix)
	for i := 0; i < c.Spans.Count(); i++ {
		span := c.Spans.Get(i)
		var key Key
		var boundary SpanBoundary
		if !col.Descending() {
			key, boundary = span.StartKey(), span.StartBoundary()
		} else {
			key, boundary = span.EndKey(), span.EndBoundary()
		}
		// If the span is unbounded on the NULL side, or if it is of the form
		// [/NULL - /x], the column is nullable.
		if key.Length() <= prefix || (key.Value(prefix) == tree.DNull && boundary == IncludeBoundary) {
			return res
		}
	}
	// All spans constrain col to be not-null.
	res.Add(col.ID())
	return res
}

// CalculateMaxResults returns a non-zero integer indicating the maximum number
// of results that can be read from indexCols by using c.Spans. The indexCols
// are assumed to form at least a weak key.
// If 0 is returned, the maximum number of results could not be deduced.
// We can calculate the maximum number of results when both of the following
// are satisfied:
//  1. The index columns form a weak key (assumption), and the spans do not
//     specify any nulls.
//  2. All spans cover all the columns of the index and have equal start and
//     end keys up to but not necessarily including the last column.
// TODO(asubiotto): The only reason to extract this is that both the heuristic
// planner and optimizer need this logic, due to the heuristic planner planning
// mutations. Once the optimizer plans mutations, this method can go away.
func (c *Constraint) CalculateMaxResults(
	evalCtx *tree.EvalContext, indexCols opt.ColSet, notNullCols opt.ColSet,
) uint64 {
	// Ensure that if we have nullable columns, we are only reading non-null
	// values, given that a unique index allows an arbitrary number of duplicate
	// entries if they have NULLs.
	if !indexCols.SubsetOf(notNullCols.Union(c.ExtractNotNullCols(evalCtx))) {
		return 0
	}

	numCols := c.Columns.Count()

	// Check if the longest prefix of columns for which all the spans have the
	// same start and end values covers all columns.
	prefix := c.Prefix(evalCtx)
	var distinctVals uint64
	if prefix < numCols-1 {
		return 0
	} else if prefix == numCols-1 {
		// If the prefix does not include the last column, calculate the number of
		// distinct values possible in the span. This is only supported for int
		// and date types.
		for i := 0; i < c.Spans.Count(); i++ {
			sp := c.Spans.Get(i)
			start := sp.StartKey()
			end := sp.EndKey()

			// Ensure that the keys specify the last column.
			if start.Length() != numCols || end.Length() != numCols {
				return 0
			}

			// TODO(asubiotto): This logic is very similar to
			// updateDistinctCountsFromConstraint. It would be nice to extract this
			// logic somewhere.
			colIdx := numCols - 1
			startVal := start.Value(colIdx)
			endVal := end.Value(colIdx)
			var startIntVal, endIntVal int64
			if startVal.ResolvedType().Family() == types.IntFamily &&
				endVal.ResolvedType().Family() == types.IntFamily {
				startIntVal = int64(*startVal.(*tree.DInt))
				endIntVal = int64(*endVal.(*tree.DInt))
			} else if startVal.ResolvedType().Family() == types.DateFamily &&
				endVal.ResolvedType().Family() == types.DateFamily {
				startDate := startVal.(*tree.DDate)
				endDate := endVal.(*tree.DDate)
				if !startDate.IsFinite() || !endDate.IsFinite() {
					// One of the boundaries is not finite, so we can't determine the
					// distinct count for this column.
					return 0
				}
				startIntVal = int64(startDate.PGEpochDays())
				endIntVal = int64(endDate.PGEpochDays())
			} else {
				return 0
			}

			if c.Columns.Get(colIdx).Ascending() {
				distinctVals += uint64(endIntVal - startIntVal)
			} else {
				distinctVals += uint64(startIntVal - endIntVal)
			}

			// Add one since both start and end boundaries should be inclusive
			// (due to Span.PreferInclusive).
			distinctVals++
		}
	} else {
		distinctVals = uint64(c.Spans.Count())
	}
	return distinctVals
}
