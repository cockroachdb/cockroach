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

// indexConstraintCalc calculates index constraints from a
// conjunction (AND-ed expressions).
type indexConstraintCalc struct {
	indexConstraintCtx

	// andExprs is a set of conjuncts that make up the filter.
	andExprs []*expr

	// constraints[] is used as the memoization data structure for calcOffset.
	constraints []LogicalSpans
}

func makeIndexConstraintCalc(
	colInfos []IndexColumnInfo, evalCtx *tree.EvalContext, e *expr,
) indexConstraintCalc {
	c := indexConstraintCalc{
		indexConstraintCtx: indexConstraintCtx{
			colInfos: colInfos,
			evalCtx:  evalCtx,
		},
		constraints: make([]LogicalSpans, len(colInfos)),
	}
	if e != nil {
		if e.op == andOp {
			c.andExprs = e.children
		} else {
			c.andExprs = []*expr{e}
		}
	}
	return c
}

// makeEqSpan returns a span that constraints column <offset> to a single value.
// The value can be NULL.
func (c *indexConstraintCalc) makeEqSpan(offset int, value tree.Datum) LogicalSpan {
	return LogicalSpan{
		Start: LogicalKey{Vals: tree.Datums{value}, Inclusive: true},
		End:   LogicalKey{Vals: tree.Datums{value}, Inclusive: true},
	}
}

// makeNotNullSpan returns a span that constrains the column to non-NULL values.
// If the column is not nullable, returns a full span.
func (c *indexConstraintCalc) makeNotNullSpan(offset int) LogicalSpan {
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
func (c *indexConstraintCalc) makeSpansForSingleColumn(
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
func (c *indexConstraintCalc) makeSpansForTupleInequality(
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
func (c *indexConstraintCalc) makeSpansForTupleIn(
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
		if valTuple.op != orderedListOp {
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
func (c *indexConstraintCalc) makeSpansForExpr(
	offset int, e *expr,
) (_ LogicalSpans, ok bool, tight bool) {
	if e.op == constOp {
		datum := e.private.(tree.Datum)
		if datum == tree.DBoolFalse || datum == tree.DNull {
			// Condition is never true, return no spans.
			return LogicalSpans{}, true, true
		}
		return nil, false, false
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
	if e.children[0].op == orderedListOp && e.children[1].op == orderedListOp {
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
func (c *indexConstraintCalc) calcOffset(offset int) LogicalSpans {
	if c.constraints[offset] != nil {
		// The results of this function are memoized.
		return c.constraints[offset]
	}

	// Start with a span with no boundaries.
	spans := LogicalSpans{MakeFullSpan()}

	// TODO(radu): sorting the expressions by the variable index, or pre-building
	// a map could help here.
	for _, e := range c.andExprs {
		exprSpans, ok, _ := c.makeSpansForExpr(offset, e)
		if !ok {
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
			s := c.calcOffset(offset + startLen)
			switch len(s) {
			case 0:
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
			s := c.calcOffset(offset + startLen)
			if len(s) > 0 {
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
			s := c.calcOffset(offset + endLen)
			if len(s) > 0 {
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
	c.constraints[offset] = spans
	return spans
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

// getMaxSimplifyOffset finds the longest prefix for which every span has the
// same first <maxoffset>+1 values for the start and end key. For example, for:
//  [/1/2/3 - /1/2/4]
//  [/2/3/4 - /2/3/4]
// the longest prefix is 2 (maxOffset is 1).
//
// This prefix is significant for filter simplification: we can only
// drop an expression based on its spans if the offset is at most
// <maxOffset>. Examples:
//
//   Filter:           @1 = 1 AND @2 >= 5
//   Spans:            [/1/5 - /1]
//   Remaining filter: <none>
//   Here maxOffset is 1; we can drop @2 >= 5 from the filter.
//
//   Filter:           @1 >= 1 AND @1 <= 3 AND @2 >= 5
//   Spans:            [/1/5 - /3]
//   Remaining filter: @2 >= 5
//   Here maxOffset is 0; we cannot drop @2 >= 5. Because the span contains more
//   than one value for the first column, there are areas where the condition
//   needs to be checked, e.g for /2/0 to /2/4.
//
//   Filter:           (@1, @2) IN ((1, 1), (2, 2)) AND @3 >= 3 AND @4 = 4
//   Spans:            [/1/1/3/4 - /1/1]
//                     [/2/2/3/4 - /2/2]
//   Remaining filter: @4 = 4
//   Here maxOffset is 2; we can drop the IN and @3 >= 3 but we can't drop @4 = 4.
func (c *indexConstraintCtx) getMaxSimplifyOffset(spans LogicalSpans) int {
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
) LogicalSpans {
	if filter.op == orOp {
		var spans LogicalSpans
		for i, orExpr := range filter.children {
			c := makeIndexConstraintCalc(colInfos, evalCtx, orExpr)
			exprSpans := c.calcOffset(0)
			if i == 0 {
				spans = exprSpans
			} else {
				spans = c.mergeSpanSets(0 /* offset */, spans, exprSpans)
			}
		}
		return spans
	}
	c := makeIndexConstraintCalc(colInfos, evalCtx, filter)
	return c.calcOffset(0)
}

// simplifyFilter is the internal implementation of SimplifyFilter.
// <maxOffset> must be the result of getMaxSimplifyOffset(spans); see that
// function for more information.
// Can return true (as a constOp).
func (c *indexConstraintCalc) simplifyFilter(e *expr, spans LogicalSpans, maxOffset int) *expr {
	// Special handling for AND and OR.
	if e.op == orOp || e.op == andOp {
		// If a child expression is simplified to a const bool of
		// shortcircuitValue, then the entire node has the same value;
		// false for AND and true for OR.
		var shortcircuitValue = (e.op == orOp)

		var children []*expr
		for i, child := range e.children {
			simplified := c.simplifyFilter(child, spans, maxOffset)
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
			r := &expr{
				op:          e.op,
				scalarProps: &scalarProps{},
				private:     e.private,
			}
			*r.scalarProps = *e.scalarProps
			r.children = children
			return r
		}
	}

	// We try to create tight spans for the expression (as allowed by maxOffset),
	// and check if the condition is implied by the final spans.
	for offset := 0; offset <= maxOffset; offset++ {
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
	c := makeIndexConstraintCalc(colInfos, evalCtx, nil /* expr */)
	remainingFilter := c.simplifyFilter(filter, spans, c.getMaxSimplifyOffset(spans))
	if ok, val := isConstBool(remainingFilter); ok && val {
		return nil
	}
	return remainingFilter
}
