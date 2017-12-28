// Copyright 2017 The Cockroach Authors.
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

package opt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// makeEqSpan returns a span that constraints column <offset> to a single value.
// The value can be NULL.
func (c *indexConstraintCtx) makeEqSpan(offset int, value tree.Datum) LogicalSpan {
	return LogicalSpan{
		Start: LogicalKey{Vals: tree.Datums{value}, Inclusive: true},
		End:   LogicalKey{Vals: tree.Datums{value}, Inclusive: true},
	}
}

// makeNotNullSpan returns a span that constrains the column to non-NULL values.
// If the column is not nullable, returns a full span.
func (c *indexConstraintCtx) makeNotNullSpan(offset int) LogicalSpan {
	sp := MakeFullSpan()
	if !c.colInfos[offset].Nullable {
		// The column is not nullable; not-null constraints aren't useful.
		return sp
	}
	if c.colInfos[offset].Direction == encoding.Ascending {
		sp.Start = LogicalKey{Vals: tree.Datums{tree.DNull}, Inclusive: false}
	} else {
		sp.End = LogicalKey{Vals: tree.Datums{tree.DNull}, Inclusive: false}
	}

	return sp
}

// makeSpansForSingleColumn creates spans for a single index column from a
// simple comparison expression. The arguments are the operator and right
// operand. The <tight> return value indicates if the spans are exactly
// equivalent to the expression (and not weaker).
func (c *indexConstraintCtx) makeSpansForSingleColumn(
	offset int, op operator, val *expr,
) (_ LogicalSpans, ok bool, tight bool) {
	if op == inOp && isTupleOfConstants(val) {
		// We assume that the values of the tuple are already ordered and distinct.
		spans := make(LogicalSpans, len(val.children))
		for i, v := range val.children {
			datum := v.private.(tree.Datum)
			spans[i].Start = LogicalKey{Vals: tree.Datums{datum}, Inclusive: true}
			spans[i].End = spans[i].Start
		}
		if c.colInfos[offset].Direction == encoding.Descending {
			// Reverse the order of the spans.
			for i, j := 0, len(spans)-1; i < j; i, j = i+1, j-1 {
				spans[i], spans[j] = spans[j], spans[i]
			}
		}
		return spans, true, true
	}
	// The rest of the supported expressions must have a constant scalar on the
	// right-hand side.
	if val.op != constOp {
		return nil, false, true
	}
	datum := val.private.(tree.Datum)
	if datum == tree.DNull {
		switch op {
		case eqOp, ltOp, gtOp, leOp, geOp, neOp:
			// This expression should have been converted to NULL during
			// type checking.
			panic("comparison with NULL")

		case isOp:
			if !c.colInfos[offset].Nullable {
				// The column is not nullable; IS NULL is always false.
				return LogicalSpans{}, true, true
			}
			return LogicalSpans{c.makeEqSpan(offset, tree.DNull)}, true, true

		case isNotOp:
			return LogicalSpans{c.makeNotNullSpan(offset)}, true, true
		}
		return nil, false, false
	}

	switch op {
	case eqOp, isOp:
		return LogicalSpans{c.makeEqSpan(offset, datum)}, true, true

	case ltOp, gtOp, leOp, geOp:
		sp := c.makeNotNullSpan(offset)
		inclusive := (op == leOp || op == geOp)
		if (op == ltOp || op == leOp) == (c.colInfos[offset].Direction == encoding.Ascending) {
			sp.End = LogicalKey{Vals: tree.Datums{datum}, Inclusive: inclusive}
		} else {
			sp.Start = LogicalKey{Vals: tree.Datums{datum}, Inclusive: inclusive}
		}
		if !inclusive {
			c.preferInclusive(offset, &sp)
		}
		return LogicalSpans{sp}, true, true

	case neOp, isNotOp:
		if datum == tree.DNull {
			panic("comparison with NULL")
		}
		spans := LogicalSpans{c.makeNotNullSpan(offset), c.makeNotNullSpan(offset)}
		spans[0].End = LogicalKey{Vals: tree.Datums{datum}, Inclusive: false}
		spans[1].Start = LogicalKey{Vals: tree.Datums{datum}, Inclusive: false}
		c.preferInclusive(offset, &spans[0])
		c.preferInclusive(offset, &spans[1])
		return spans, true, true

	default:
		return nil, false, false
	}
}

// makeSpansForTupleInequality creates spans for index columns starting at
// <offset> from a tuple inequality.
// Assumes that e.op is an inequality and both sides are tuples.
// The <tight> return value indicates if the spans are exactly equivalent
// to the expression (and not weaker).
func (c *indexConstraintCtx) makeSpansForTupleInequality(
	offset int, e *expr,
) (_ LogicalSpans, ok bool, tight bool) {
	lhs, rhs := e.children[0], e.children[1]

	// Find the longest prefix of the tuple that maps to index columns (with the
	// same direction) starting at <offset>.
	prefixLen := 0
	dir := c.colInfos[offset].Direction
	for i := range lhs.children {
		if !c.isIndexColumn(lhs.children[i], offset+i) {
			// Variable doesn't refer to the right column.
			break
		}
		if rhs.children[i].op != constOp {
			// Right-hand value is not a constant.
			break
		}
		if c.colInfos[offset+i].Direction != dir {
			// The direction changed. For example:
			//   a ASCENDING, b DESCENDING, c ASCENDING
			//   (a, b, c) >= (1, 2, 3)
			// We can only use a >= 1 here.
			//
			// TODO(radu): we could support inequalities for cases like this by
			// allowing negation, for example:
			//  (a, -b, c) >= (1, -2, 3)
			break
		}
		prefixLen++
	}
	if prefixLen == 0 {
		return nil, false, false
	}

	datums := make(tree.Datums, prefixLen)
	for i := range datums {
		datums[i] = rhs.children[i].private.(tree.Datum)
	}

	// less is true if the op is < or <= and false if the op is > or >=.
	// inclusive is true if the op is <= or >= and false if the op is < or >.
	var less, inclusive bool

	switch e.op {
	case neOp:
		if prefixLen < len(lhs.children) {
			// If we have (a, b, c) != (1, 2, 3), we cannot
			// determine any constraint on (a, b).
			return nil, false, false
		}
		spans := LogicalSpans{MakeFullSpan(), MakeFullSpan()}
		spans[0].End = LogicalKey{Vals: datums, Inclusive: false}
		// We don't want the spans to alias each other, the preferInclusive
		// calls below could mangle them.
		datumsCopy := append([]tree.Datum(nil), datums...)
		spans[1].Start = LogicalKey{Vals: datumsCopy, Inclusive: false}
		c.preferInclusive(offset, &spans[0])
		c.preferInclusive(offset, &spans[1])
		return spans, true, true

	case ltOp:
		less, inclusive = true, false
	case leOp:
		less, inclusive = true, true
	case gtOp:
		less, inclusive = false, false
	case geOp:
		less, inclusive = false, true
	default:
		panic(fmt.Sprintf("unsupported op %s", e.op))
	}

	if prefixLen < len(lhs.children) {
		// If we only keep a prefix, exclusive inequalities become exclusive.
		// For example:
		//   (a, b, c) > (1, 2, 3) becomes (a, b) >= (1, 2)
		inclusive = true
	}

	if dir == encoding.Descending {
		// If the direction is descending, the inequality is reversed.
		less = !less
	}

	sp := MakeFullSpan()
	if less {
		sp.End = LogicalKey{Vals: datums, Inclusive: inclusive}
	} else {
		sp.Start = LogicalKey{Vals: datums, Inclusive: inclusive}
	}
	c.preferInclusive(offset, &sp)
	// The spans are "tight" unless we used just a prefix.
	tight = (prefixLen == len(lhs.children))
	return LogicalSpans{sp}, true, tight
}

// makeSpansForTupleIn creates spans for index columns starting at
// <offset> from a tuple IN tuple expression, for example:
//   (a, b, c) IN ((1, 2, 3), (4, 5, 6))
// Assumes that both sides are tuples.
// The <tight> return value indicates if the spans are exactly equivalent
// to the expression (and not weaker).
func (c *indexConstraintCtx) makeSpansForTupleIn(
	offset int, e *expr,
) (_ LogicalSpans, ok bool, tight bool) {
	lhs, rhs := e.children[0], e.children[1]

	// Find the longest prefix of columns starting at <offset> which is contained
	// in the left-hand tuple; tuplePos[i] is the position of column <offset+i> in
	// the tuple.
	var tuplePos []int
Outer:
	for i := offset; i < len(c.colInfos); i++ {
		for j := range lhs.children {
			if c.isIndexColumn(lhs.children[j], i) {
				tuplePos = append(tuplePos, j)
				continue Outer
			}
		}
		break
	}
	if len(tuplePos) == 0 {
		return nil, false, false
	}

	// Create a span for each (tuple) value inside the right-hand side tuple.
	spans := make(LogicalSpans, len(rhs.children))
	for i, valTuple := range rhs.children {
		if valTuple.op != tupleOp {
			return nil, false, false
		}
		vals := make(tree.Datums, len(tuplePos))
		for j, c := range tuplePos {
			val := valTuple.children[c]
			if val.op != constOp {
				return nil, false, false
			}
			vals[j] = val.private.(tree.Datum)
		}
		spans[i].Start = LogicalKey{Vals: vals, Inclusive: true}
		spans[i].End = spans[i].Start
	}

	// Sort and de-duplicate the values.
	// We don't rely on the sorted order of the right-hand side tuple because it's
	// only useful if all of the following are met:
	//  - we use all columns in the tuple, i.e. len(tuplePos) = len(lhs.children).
	//  - the columns are in the right order, i.e. tuplePos[i] = i.
	//  - the columns have the same directions
	c.sortSpans(offset, spans)
	res := spans[:0]
	for i := range spans {
		if i > 0 && c.compare(offset, spans[i-1].Start, spans[i].Start, compareStartKeys) == 0 {
			continue
		}
		res = append(res, spans[i])
	}
	// The spans are "tight" unless we used just a prefix.
	tight = len(tuplePos) == len(lhs.children)
	return res, true, tight
}

// makeSpansForExpr creates spans for index columns starting at <offset>
// from the given expression.
//
// The <tight> return value indicates if the spans are exactly equivalent to the
// expression (and not weaker). See simplifyFilter for more information.
func (c *indexConstraintCtx) makeSpansForExpr(
	offset int, e *expr,
) (_ LogicalSpans, ok bool, tight bool) {
	switch e.op {
	case constOp:
		datum := e.private.(tree.Datum)
		if datum == tree.DBoolFalse || datum == tree.DNull {
			// Condition is never true, return no spans.
			return LogicalSpans{}, true, true
		}
		return nil, false, false

	case andOp:
		spans, ok := c.makeSpansForAndExprs(offset, e.children)
		// We don't have enough information to know if the spans are "tight".
		return spans, ok, false

	case orOp:
		spans, ok := c.makeSpansForOrExprs(offset, e.children)
		// We don't have enough information to know if the spans are "tight".
		return spans, ok, false
	}

	if len(e.children) < 2 {
		return nil, false, false
	}
	// Check for an operation where the left-hand side is an
	// indexed var for this column.
	if c.isIndexColumn(e.children[0], offset) {
		return c.makeSpansForSingleColumn(offset, e.op, e.children[1])
	}
	// Check for tuple operations.
	if e.children[0].op == tupleOp && e.children[1].op == tupleOp {
		switch e.op {
		case ltOp, leOp, gtOp, geOp, neOp:
			// Tuple inequality.
			return c.makeSpansForTupleInequality(offset, e)
		case inOp:
			// Tuple IN tuple.
			return c.makeSpansForTupleIn(offset, e)
		}
	}

	// Last resort: for conditions like a > b, our column can appear on the right
	// side. We can deduce a not-null constraint from such conditions.
	if c.colInfos[offset].Nullable && c.isIndexColumn(e.children[1], offset) {
		switch e.op {
		case eqOp, ltOp, leOp, gtOp, geOp, neOp:
			return LogicalSpans{c.makeNotNullSpan(offset)}, true, false
		}
	}

	return nil, false, false
}

// indexConstraintConjunctionCtx stores the context for an index constraint
// calculation on a conjunction (AND-ed expressions).
type indexConstraintConjunctionCtx struct {
	*indexConstraintCtx

	// andExprs is a set of conjuncts that make up the filter.
	andExprs []*expr

	// Memoization data structure for calcOffset.
	results []calcOffsetResult
}

// calcOffsetResult stores the result of calcOffset for a given offset.
type calcOffsetResult struct {
	// visited indicates if calcOffset was already called for this given offset,
	// in which case the other fields are populated.
	visited        bool
	spansPopulated bool
	spans          LogicalSpans
}

// makeSpansForAndExprs calculates spans for a conjunction.
func (c *indexConstraintCtx) makeSpansForAndExprs(
	offset int, andExprs []*expr,
) (_ LogicalSpans, ok bool) {
	conjCtx := indexConstraintConjunctionCtx{
		indexConstraintCtx: c,
		andExprs:           andExprs,
		results:            make([]calcOffsetResult, len(c.colInfos)),
	}
	return conjCtx.calcOffset(offset)
}

// calcOffset calculates constraints for the sequence of index columns starting
// at <offset>.
//
// It works as follows: we look at expressions which constrain column <offset>
// (or a sequence of columns starting at <offset>) and generate spans from those
// expressions. Then, we try to extend those span keys with more columns by
// recursively calculating the constraints for a higher <offset>. The results
// are memoized so each offset is calculated at most once.
//
// For example:
//   @1 >= 5 AND @1 <= 10 AND @2 = 2 AND @3 > 1
//
// - calcOffset(offset = 0):          // calculate constraints for @1, @2, @3
//   - process expressions, generating span [/5 - /10]
//   - call calcOffset(offset = 1):   // calculate constraints for @2, @3
//     - process expressions, generating span [/2 - /2]
//     - call calcOffset(offset = 2): // calculate constraints for @3
//       - process expressions, return span (/1 - ]
//     - extend the keys in the span [/2 - /2] with (/1 - ], return (/2/1 - /2].
//   - extend the keys in the span [/5 - /10] with (/2/1 - /2], return
//     (/5/2/1 - /10/2].
//
// TODO(radu): add an example with tuple constraints once they are supported.
func (c indexConstraintConjunctionCtx) calcOffset(offset int) (_ LogicalSpans, ok bool) {
	if c.results[offset].visited {
		// The results of this function are memoized.
		return c.results[offset].spans, c.results[offset].spansPopulated
	}

	// Start with a span with no boundaries.
	var spans LogicalSpans
	var spansPopulated bool

	// TODO(radu): sorting the expressions by the variable index, or pre-building
	// a map could help here.
	for _, e := range c.andExprs {
		exprSpans, ok, _ := c.makeSpansForExpr(offset, e)
		if !ok {
			continue
		}
		if !spansPopulated {
			spans = exprSpans
			spansPopulated = true
			continue
		}

		if len(exprSpans) == 1 {
			// More efficient path for the common case of a single expression span.
			for i := 0; i < len(spans); i++ {
				if spans[i], ok = c.intersectSpan(offset, &spans[i], &exprSpans[0]); !ok {
					// Remove this span.
					copy(spans[i:], spans[i+1:])
					spans = spans[:len(spans)-1]
				}
			}
		} else {
			var newSpans LogicalSpans
			for i := range spans {
				newSpans = append(newSpans, c.intersectSpanSet(offset, &spans[i], exprSpans)...)
			}
			spans = newSpans
		}
	}
	if !spansPopulated {
		c.results[offset] = calcOffsetResult{visited: true}
		return nil, false
	}

	// Try to extend the spans with constraints on more columns.

	// newSpans accumulates the extended spans, but is initialized only if we need
	// to break up a span into multiple spans (otherwise spans is modified in
	// place).
	var newSpans LogicalSpans
	for i := 0; i < len(spans); i++ {
		start, end := spans[i].Start, spans[i].End
		startLen, endLen := len(start.Vals), len(end.Vals)

		// Currently startLen, endLen can be at most 1; but this will change
		// when we will support tuple expressions.

		if startLen == endLen && startLen > 0 && offset+startLen < len(c.colInfos) &&
			c.compareKeyVals(offset, start.Vals, end.Vals) == 0 {
			// Special case when the start and end keys are equal (i.e. an exact value
			// on this column). This is the only case where we can break up a single
			// span into multiple spans.
			//
			// For example:
			//  @1 = 1 AND @2 IN (3, 4, 5)
			//  At offset 0 so far we have:
			//    [/1 - /1]
			//  At offset 1 we have:
			//    [/3 - /3]
			//    [/4 - /4]
			//    [/5 - /5]
			//  We break up the span to get:
			//    [/1/3 - /1/3]
			//    [/1/4 - /1/4]
			//    [/1/5 - /1/5]
			s, ok := c.calcOffset(offset + startLen)
			if !ok {
				continue
			}
			switch len(s) {
			case 0:
				// We found a contradiction.
				spans = LogicalSpans{}
				c.results[offset] = calcOffsetResult{
					visited:        true,
					spansPopulated: true,
					spans:          spans,
				}
				return spans, true

			case 1:
				spans[i].Start.extend(s[0].Start)
				spans[i].End.extend(s[0].End)
				if newSpans != nil {
					newSpans = append(newSpans, spans[i])
				}

			default:
				if newSpans == nil {
					newSpans = make(LogicalSpans, 0, len(spans)-1+len(s))
					newSpans = append(newSpans, spans[:i]...)
				}
				for j := range s {
					newSpan := spans[i]
					newSpan.Start.extend(s[j].Start)
					newSpan.End.extend(s[j].End)
					newSpans = append(newSpans, newSpan)
				}
			}
			continue
		}

		if startLen > 0 && offset+startLen < len(c.colInfos) && spans[i].Start.Inclusive {
			// We can advance the starting boundary. Calculate constraints for the
			// column that follows.
			if s, ok := c.calcOffset(offset + startLen); ok && len(s) > 0 {
				// If we have multiple constraints, we can only use the start of the
				// first one to tighten the span.
				// For example:
				//   @1 >= 2 AND @2 IN (1, 2, 3).
				//   At offset 0 so far we have:
				//     [/2 - ]
				//   At offset 1 we have:
				//     [/1 - /1]
				//     [/2 - /2]
				//     [/3 - /3]
				//   The best we can do is tighten the span to:
				//     [/2/1 - ]
				spans[i].Start.extend(s[0].Start)
			}
		}

		if endLen > 0 && offset+endLen < len(c.colInfos) && spans[i].End.Inclusive {
			// We can restrict the ending boundary. Calculate constraints for the
			// column that follows.
			if s, ok := c.calcOffset(offset + endLen); ok && len(s) > 0 {
				// If we have multiple constraints, we can only use the end of the
				// last one to tighten the span.
				spans[i].End.extend(s[len(s)-1].End)
			}
		}
		if newSpans != nil {
			newSpans = append(newSpans, spans[i])
		}
	}
	if newSpans != nil {
		spans = newSpans
	}
	c.checkSpans(offset, spans)
	c.results[offset] = calcOffsetResult{
		visited:        true,
		spansPopulated: true,
		spans:          spans,
	}
	return spans, true
}

// makeSpansForOrExprs calculates spans for a disjunction.
func (c *indexConstraintCtx) makeSpansForOrExprs(
	offset int, orExprs []*expr,
) (_ LogicalSpans, ok bool) {
	var spans LogicalSpans

	for i, orExpr := range orExprs {
		exprSpans, ok, _ := c.makeSpansForExpr(offset, orExpr)
		if !ok {
			// If we can't generate spans for a disjunct, exit early (this is
			// equivalent to a full span).
			return nil, false
		}
		if i == 0 {
			spans = exprSpans
		} else {
			spans = c.mergeSpanSets(offset, spans, exprSpans)
		}
	}
	return spans, true
}

var constTrueExpr = &expr{
	op:          constOp,
	scalarProps: &scalarProps{typ: types.Bool},
	private:     tree.DBoolTrue,
}

var constFalseExpr = &expr{
	op:          constOp,
	scalarProps: &scalarProps{typ: types.Bool},
	private:     tree.DBoolFalse,
}

func constBoolExpr(val bool) *expr {
	if val {
		return constTrueExpr
	}
	return constFalseExpr
}

// getMaxSimplifyPrefix finds the longest prefix (maxSimplifyPrefix) such that
// every span has the same first maxSimplifyPrefix values for the start and end
// key. For example, for:
//  [/1/2/3 - /1/2/4]
//  [/2/3/4 - /2/3/4]
// the longest prefix is 2.
//
// This prefix is significant for filter simplification: we can only
// drop an expression based on its spans if the offset is at most
// maxSimplifyPrefix. Examples:
//
//   Filter:           @1 = 1 AND @2 >= 5
//   Spans:            [/1/5 - /1]
//   Remaining filter: <none>
//   Here maxSimplifyPrefix is 1; we can drop @2 >= 5 from the filter.
//
//   Filter:           @1 >= 1 AND @1 <= 3 AND @2 >= 5
//   Spans:            [/1/5 - /3]
//   Remaining filter: @2 >= 5
//   Here maxSimplifyPrefix is 0; we cannot drop @2 >= 5. Because the span
//   contains more than one value for the first column, there are areas where
//   the condition needs to be checked, e.g for /2/0 to /2/4.
//
//   Filter:           (@1, @2) IN ((1, 1), (2, 2)) AND @3 >= 3 AND @4 = 4
//   Spans:            [/1/1/3/4 - /1/1]
//                     [/2/2/3/4 - /2/2]
//   Remaining filter: @4 = 4
//   Here maxSimplifyPrefix is 2; we can drop the IN and @3 >= 3 but we can't
//   drop @4 = 4.
func (c *indexConstraintCtx) getMaxSimplifyPrefix(spans LogicalSpans) int {
	maxOffset := len(c.colInfos) - 1
	for _, sp := range spans {
		i := 0
		// Find the longest prefix of equal values.
		for ; i < len(sp.Start.Vals) && i < len(sp.End.Vals); i++ {
			if sp.Start.Vals[i].Compare(c.evalCtx, sp.End.Vals[i]) != 0 {
				break
			}
		}
		if i == 0 {
			return 0
		}
		if maxOffset > i {
			maxOffset = i
		}
	}
	return maxOffset
}

// IndexColumnInfo encompasses the information for index columns, needed for
// index constraints.
type IndexColumnInfo struct {
	// VarIdx identifies the indexed var that corresponds to this column.
	VarIdx    int
	Typ       types.T
	Direction encoding.Direction
	// Nullable should be set to false if this column cannot store NULLs; used
	// to keep the spans simple, e.g. [ - /5] instead of (/NULL - /5].
	Nullable bool
}

// MakeIndexConstraints generates constraints from a scalar boolean filter
// expression. See LogicalSpans for more information on how constraints are
// represented.
func MakeIndexConstraints(
	filter *expr, colInfos []IndexColumnInfo, evalCtx *tree.EvalContext,
) (_ LogicalSpans, ok bool) {
	c := makeIndexConstraintCtx(colInfos, evalCtx)
	spans, ok, _ := c.makeSpansForExpr(0 /* offset */, filter)
	return spans, ok
}

// simplifyFilter is the internal implementation of SimplifyFilter.
// <maxSimplifyPrefix> must be the result of getMaxSimplifyPrefix(spans); see that
// function for more information.
// Can return true (as a constOp).
//
// We use an approach based on spans: we have the generated spans for the entire
// filter; for each sub-expression, we use existing code to generate spans for
// that sub-expression and see if we can prove that the sub-expression is always
// true when the space is restricted to the spans for the entire filter.
//
// The following conditions are (together) sufficient for a sub-expression to be
// true:
//
//  - the spans generated for this sub-expression are equivalent to the
//    expression; we call such spans "tight". For example the condition
//    `@1 >= 1` results in span `[/1 - ]` which is tight: inside this span, the
//    condition is always true. On the other hand, if we have an index on
//    @1,@2,@3 and condition `(@1, @3) >= (1, 3)`, the generated span is
//    `[/1 - ]` which is not tight: we still need to verify the condition on @3
//    inside this span.
//
//  - the spans for the entire filter are completely contained in the (tight)
//    spans for this sub-expression. In this case, there can be no rows that are
//    inside the filter span but outside the expression span.
//
//    For example: `@1 = 1 AND @2 = 2` with span `[/1/2 - /1/2]`. When looking
//    at sub-expression `@1 = 1` and its span `[/1 - /1]`, we see that it
//    contains the filter span `[/1/2 - /1/2]` and thus the condition is always
//    true inside `[/1/2 - /1/2`].  For `@2 = 2` we have the span `[/2 - /2]`
//    but this span refers to the second index column (so it's actually
//    equivalent to a collection of spans `[/?/2 - /?/2]`); the only way we can
//    compare it against the filter span is if the latter restricts the previous
//    column to a single value (which it does in this case; this is determined
//    by getMaxSimplifyPrefix). So `[/1/2 - /1/2]` is contained in the
//    expression span and we can simplify `@2 = 2` to `true`.
//
//    An example where this doesn't work well is with disjunctions:
//    `@1 <= 1 OR @1 >= 4` has spans `[ - /1], [/1 - ]` but in separation neither
//    sub-expression is always true inside these spans.
func (c *indexConstraintCtx) simplifyFilter(
	e *expr, spans LogicalSpans, maxSimplifyPrefix int,
) *expr {
	// Special handling for AND and OR.
	if e.op == orOp || e.op == andOp {
		// If a child expression is simplified to a const bool of
		// shortcircuitValue, then the entire node has the same value;
		// false for AND and true for OR.
		var shortcircuitValue = (e.op == orOp)

		var children []*expr
		for i, child := range e.children {
			simplified := c.simplifyFilter(child, spans, maxSimplifyPrefix)
			if ok, val := isConstBool(simplified); ok {
				if val == shortcircuitValue {
					return constBoolExpr(shortcircuitValue)
				}
				// We can ignore this child (it is true for AND, false for OR).
			} else {
				if children == nil {
					children = make([]*expr, 0, len(e.children)-i)
				}
				children = append(children, simplified)
			}
		}
		switch len(children) {
		case 0:
			// All children simplify to nothing.
			return constBoolExpr(!shortcircuitValue)
		case 1:
			return children[0]
		default:
			scalarPropsCopy := *e.scalarProps
			return &expr{
				op:          e.op,
				scalarProps: &scalarPropsCopy,
				private:     e.private,
				children:    children,
			}
		}
	}

	// We try to create tight spans for the expression (as allowed by
	// maxSimplifyPrefix), and check if the condition is implied by the final
	// spans.
	for offset := 0; offset <= maxSimplifyPrefix; offset++ {
		if offset > 0 {
			if offset == 1 {
				// Copy the spans, we are about to modify them.
				spans = append(LogicalSpans(nil), spans...)
			}
			// Chop away the first value in all spans to end up with spans
			// for this offset.
			for i := range spans {
				spans[i].Start.Vals = spans[i].Start.Vals[1:]
				spans[i].End.Vals = spans[i].End.Vals[1:]
			}
		}
		if exprSpans, ok, tight := c.makeSpansForExpr(offset, e); ok && tight {
			if c.isSpanSubset(offset, spans, exprSpans) {
				// The final spans are a subset of the spans for this expression; there
				// is no need for a remaining filter for this condition.
				return constTrueExpr
			}
		}
	}

	return e.deepCopy()
}

// simplifyFilter removes parts of the filter that are satisfied by the spans. It
// is best-effort. Returns nil if there is no remaining filter.
func simplifyFilter(
	filter *expr, spans LogicalSpans, colInfos []IndexColumnInfo, evalCtx *tree.EvalContext,
) *expr {
	c := makeIndexConstraintCtx(colInfos, evalCtx)
	remainingFilter := c.simplifyFilter(filter, spans, c.getMaxSimplifyPrefix(spans))
	if ok, val := isConstBool(remainingFilter); ok && val {
		return nil
	}
	return remainingFilter
}
