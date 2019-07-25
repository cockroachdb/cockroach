// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxconstraint

import (
	"context"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Convenience aliases to avoid the constraint prefix everywhere.
const includeBoundary = constraint.IncludeBoundary
const excludeBoundary = constraint.ExcludeBoundary

var emptyKey = constraint.EmptyKey
var emptySpans = constraint.Spans{}

func (c *indexConstraintCtx) contradiction(offset int, out *constraint.Constraint) {
	out.Init(&c.keyCtx[offset], &emptySpans)
}

func (c *indexConstraintCtx) unconstrained(offset int, out *constraint.Constraint) {
	out.InitSingleSpan(&c.keyCtx[offset], &constraint.UnconstrainedSpan)
}

// singleSpan creates a constraint with a single span.
//
// If swap is true, the start and end key/boundary are swapped (this is provided
// just for convenience, to avoid having two branches in every caller).
func (c *indexConstraintCtx) singleSpan(
	offset int,
	start constraint.Key,
	startBoundary constraint.SpanBoundary,
	end constraint.Key,
	endBoundary constraint.SpanBoundary,
	swap bool,
	out *constraint.Constraint,
) {
	var span constraint.Span
	if !swap {
		span.Init(start, startBoundary, end, endBoundary)
	} else {
		span.Init(end, endBoundary, start, startBoundary)
	}
	keyCtx := &c.keyCtx[offset]
	span.PreferInclusive(keyCtx)
	out.InitSingleSpan(keyCtx, &span)
}

// eqSpan returns a span that constrains a column to a single value (which
// can be DNull).
func (c *indexConstraintCtx) eqSpan(offset int, value tree.Datum, out *constraint.Constraint) {
	var span constraint.Span
	key := constraint.MakeKey(value)
	span.Init(key, includeBoundary, key, includeBoundary)
	out.InitSingleSpan(&c.keyCtx[offset], &span)
}

func (c *indexConstraintCtx) notNullStartKey(offset int) (constraint.Key, constraint.SpanBoundary) {
	if !c.isNullable(offset) {
		return emptyKey, includeBoundary
	}
	return constraint.MakeKey(tree.DNull), excludeBoundary
}

// makeNotNullSpan returns a span that constrains the column to non-NULL values.
// If the column is not nullable, returns a full span.
func (c *indexConstraintCtx) makeNotNullSpan(offset int, out *constraint.Constraint) {
	if !c.isNullable(offset) {
		// The column is not nullable; not-null constraints aren't useful.
		c.unconstrained(offset, out)
		return
	}
	c.singleSpan(
		offset,
		constraint.MakeKey(tree.DNull), excludeBoundary,
		emptyKey, includeBoundary,
		c.columns[offset].Descending(),
		out,
	)
}

// makeStringPrefixSpan returns a span that constrains string column <offset>
// to strings having the given prefix.
func (c *indexConstraintCtx) makeStringPrefixSpan(
	offset int, prefix string, out *constraint.Constraint,
) {
	startKey, startBoundary := constraint.MakeKey(tree.NewDString(prefix)), includeBoundary
	endKey, endBoundary := emptyKey, includeBoundary

	i := len(prefix) - 1
	for ; i >= 0 && prefix[i] == 0xFF; i-- {
	}

	// If i < 0, we have a prefix like "\xff\xff\xff"; there is no ending value.
	if i >= 0 {
		// A few examples:
		//   prefix      -> endValue
		//   ABC         -> ABD
		//   ABC\xff     -> ABD
		//   ABC\xff\xff -> ABD
		endVal := []byte(prefix[:i+1])
		endVal[i]++
		endDatum := tree.NewDString(string(endVal))
		endKey = constraint.MakeKey(endDatum)
		endBoundary = excludeBoundary
	}
	c.singleSpan(
		offset,
		startKey, startBoundary, endKey, endBoundary,
		c.columns[offset].Descending(),
		out,
	)
}

// verifyType checks that the type of the index column <offset> matches the
// given type. We disallow mixed-type comparisons because it would result in
// incorrect encodings (#4313).
func (c *indexConstraintCtx) verifyType(offset int, typ *types.T) bool {
	return typ.Family() == types.UnknownFamily || c.colType(offset).Equivalent(typ)
}

// makeSpansForSingleColumn creates spans for a single index column from a
// simple comparison expression. The arguments are the operator and right
// operand. The <tight> return value indicates if the spans are exactly
// equivalent to the expression (and not weaker).
func (c *indexConstraintCtx) makeSpansForSingleColumn(
	offset int, op opt.Operator, val opt.Expr, out *constraint.Constraint,
) (tight bool) {
	if op == opt.InOp && memo.CanExtractConstTuple(val) {
		tupVal := val.(*memo.TupleExpr)
		keyCtx := &c.keyCtx[offset]
		var spans constraint.Spans
		spans.Alloc(len(tupVal.Elems))
		for _, child := range tupVal.Elems {
			datum := memo.ExtractConstDatum(child)
			if !c.verifyType(offset, datum.ResolvedType()) {
				c.unconstrained(offset, out)
				return false
			}
			if datum == tree.DNull {
				// Ignore NULLs - they can't match any values
				continue
			}
			var sp constraint.Span
			sp.Init(
				constraint.MakeKey(datum), includeBoundary,
				constraint.MakeKey(datum), includeBoundary,
			)
			spans.Append(&sp)
		}
		if c.columns[offset].Descending() {
			// Reverse the order of the spans.
			for i, j := 0, spans.Count()-1; i < j; i, j = i+1, j-1 {
				si, sj := spans.Get(i), spans.Get(j)
				*si, *sj = *sj, *si
			}
		}
		spans.SortAndMerge(keyCtx)
		out.Init(keyCtx, &spans)
		return true
	}

	if opt.IsConstValueOp(val) {
		return c.makeSpansForSingleColumnDatum(offset, op, memo.ExtractConstDatum(val), out)
	}

	c.unconstrained(offset, out)
	return false
}

// makeSpansForSingleColumnDatum creates spans for a single index column from a
// simple comparison expression with a constant value on the right-hand side.
func (c *indexConstraintCtx) makeSpansForSingleColumnDatum(
	offset int, op opt.Operator, datum tree.Datum, out *constraint.Constraint,
) (tight bool) {
	if !c.verifyType(offset, datum.ResolvedType()) {
		c.unconstrained(offset, out)
		return false
	}
	if datum == tree.DNull {
		switch op {
		case opt.EqOp, opt.LtOp, opt.GtOp, opt.LeOp, opt.GeOp, opt.NeOp:
			// The result of this expression is always NULL. Normally, this expression
			// should have been converted to NULL during type checking; but if the
			// NULL is coming from a placeholder, that doesn't happen.
			c.contradiction(offset, out)
			return true

		case opt.IsOp:
			if !c.isNullable(offset) {
				// The column is not nullable; IS NULL is always false.
				c.contradiction(offset, out)
				return true
			}
			c.eqSpan(offset, tree.DNull, out)
			return true

		case opt.IsNotOp:
			c.makeNotNullSpan(offset, out)
			return true
		}
		c.unconstrained(offset, out)
		return false
	}

	switch op {
	case opt.EqOp, opt.IsOp:
		c.eqSpan(offset, datum, out)
		return true

	case opt.LtOp, opt.GtOp, opt.LeOp, opt.GeOp:
		startKey, startBoundary := c.notNullStartKey(offset)
		endKey, endBoundary := emptyKey, includeBoundary
		k := constraint.MakeKey(datum)
		switch op {
		case opt.LtOp:
			endKey, endBoundary = k, excludeBoundary
		case opt.LeOp:
			endKey, endBoundary = k, includeBoundary
		case opt.GtOp:
			startKey, startBoundary = k, excludeBoundary
		case opt.GeOp:
			startKey, startBoundary = k, includeBoundary
		}

		c.singleSpan(
			offset, startKey, startBoundary, endKey, endBoundary,
			c.columns[offset].Descending(),
			out,
		)
		return true

	case opt.NeOp, opt.IsNotOp:
		// Build constraint that doesn't contain the key:
		//   if nullable or IsNotOp   : [ - key) (key - ]
		//   if not nullable and NeOp : (/NULL - key) (key - ]
		//
		// If the key is the minimum possible value for the column type, the span
		// (/NULL - key) will never contain any values and can be omitted. The span
		// [ - key) is similar if the column is not nullable.
		//
		// Similarly, if the key is the maximum possible value, the span (key - ]
		// can be omitted.
		startKey, startBoundary := emptyKey, includeBoundary
		if op == opt.NeOp {
			startKey, startBoundary = c.notNullStartKey(offset)
		}
		key := constraint.MakeKey(datum)
		descending := c.columns[offset].Descending()
		if !(startKey.IsEmpty() && c.isNullable(offset)) && datum.IsMin(c.evalCtx) {
			// Omit the (/NULL - key) span by setting a contradiction, so that the
			// UnionWith call below will result in just the second span.
			c.contradiction(offset, out)
		} else {
			c.singleSpan(offset, startKey, startBoundary, key, excludeBoundary, descending, out)
		}
		if !datum.IsMax(c.evalCtx) {
			var other constraint.Constraint
			c.singleSpan(offset, key, excludeBoundary, emptyKey, includeBoundary, descending, &other)
			out.UnionWith(c.evalCtx, &other)
		}
		return true

	case opt.LikeOp:
		if s, ok := tree.AsDString(datum); ok {
			if i := strings.IndexAny(string(s), "_%"); i >= 0 {
				if i == 0 {
					// Mask starts with _ or %.
					c.unconstrained(offset, out)
					return false
				}
				c.makeStringPrefixSpan(offset, string(s[:i]), out)
				// A mask like ABC% is equivalent to restricting the prefix to ABC.
				// A mask like ABC%Z requires restricting the prefix, but is a stronger
				// condition.
				return (i == len(s)-1) && s[i] == '%'
			}
			// No wildcard characters, this is an equality.
			c.eqSpan(offset, &s, out)
			return true
		}

	case opt.SimilarToOp:
		// a SIMILAR TO 'foo_*' -> prefix "foo"
		if s, ok := tree.AsDString(datum); ok {
			pattern := tree.SimilarEscape(string(s))
			if re, err := regexp.Compile(pattern); err == nil {
				prefix, complete := re.LiteralPrefix()
				if complete {
					c.eqSpan(offset, tree.NewDString(prefix), out)
					return true
				}
				c.makeStringPrefixSpan(offset, prefix, out)
				return false
			}
		}
	}
	c.unconstrained(offset, out)
	return false
}

// makeSpansForTupleInequality creates spans for index columns starting at
// <offset> from a tuple inequality.
// Assumes that ev.Operator() is an inequality and both sides are tuples.
// The <tight> return value indicates if the spans are exactly equivalent
// to the expression (and not weaker).
func (c *indexConstraintCtx) makeSpansForTupleInequality(
	offset int, e opt.Expr, out *constraint.Constraint,
) (tight bool) {
	lhs, rhs := e.Child(0).(*memo.TupleExpr), e.Child(1).(*memo.TupleExpr)

	// Find the longest prefix of the tuple that maps to index columns (with the
	// same direction) starting at <offset>.
	prefixLen := 0
	descending := c.columns[offset].Descending()
	nullVal := false
	for i, leftChild := range lhs.Elems {
		rightChild := rhs.Elems[i]
		if !(offset+i < len(c.columns) && c.isIndexColumn(leftChild, offset+i)) {
			// Variable doesn't refer to the column of interest.
			break
		}
		if !opt.IsConstValueOp(rightChild) {
			// Right-hand value is not a constant.
			break
		}
		if !c.verifyType(offset+i, rightChild.DataType()) {
			// We have a mixed-type comparison; we can't encode this in a span
			// (see #4313).
			break
		}
		if rightChild.Op() == opt.NullOp {
			// NULLs are tricky and require special handling; see
			// nullVal related code below.
			nullVal = true
			break
		}
		if c.columns[offset+i].Descending() != descending && e.Op() != opt.NeOp {
			// The direction changed. For example:
			//   a ASCENDING, b DESCENDING, c ASCENDING
			//   (a, b, c) >= (1, 2, 3)
			// We can only use a >= 1 here.
			//
			// TODO(radu): we could support inequalities for cases like this by
			// allowing negation, for example:
			//  (a, -b, c) >= (1, -2, 3)
			//
			// The != operator is an exception where the column directions don't
			// matter. For example, for (a, b, c) != (1, 2, 3) the spans
			// [ - /1/2/2], [1/2/4 - ] apply for any combination of directions.
			break
		}
		prefixLen++
	}
	if prefixLen == 0 {
		c.unconstrained(offset, out)
		return false
	}

	datums := make(tree.Datums, prefixLen)
	for i := range datums {
		datums[i] = memo.ExtractConstDatum(rhs.Elems[i])
	}

	// less is true if the op is < or <= and false if the op is > or >=.
	// boundary is inclusive if the op is <= or >= and exclusive if the op
	// is < or >.
	var less bool
	var boundary constraint.SpanBoundary

	switch e.Op() {
	case opt.NeOp:
		if prefixLen < len(lhs.Elems) {
			// If we have (a, b, c) != (1, 2, 3), we cannot
			// determine any constraint on (a, b).
			c.unconstrained(offset, out)
			return false
		}
		key := constraint.MakeCompositeKey(datums...)
		c.singleSpan(offset, emptyKey, includeBoundary, key, excludeBoundary, false /* swap */, out)
		var other constraint.Constraint
		c.singleSpan(offset, key, excludeBoundary, emptyKey, includeBoundary, false /* swap */, &other)
		out.UnionWith(c.evalCtx, &other)
		// If any columns are nullable, the spans could include unwanted NULLs.
		// For example, for
		//   (@1, @2, @3) != (1, 2, 3)
		// we need to exclude tuples like
		//   (1, NULL, NULL)
		//   (NULL, 2, 3)
		//
		// But note that some tuples with NULLs can satisfy the condition, e.g.
		//   (5, NULL, NULL)
		//   (NULL, 5, 5)
		tight = true
		for i := 0; i < prefixLen; i++ {
			if c.isNullable(offset + i) {
				tight = false
				break
			}
		}
		return tight

	case opt.LtOp:
		less, boundary = true, excludeBoundary
	case opt.LeOp:
		less, boundary = true, includeBoundary
	case opt.GtOp:
		less, boundary = false, excludeBoundary
	case opt.GeOp:
		less, boundary = false, includeBoundary
	default:
		panic(errors.AssertionFailedf("unsupported op %s", errors.Safe(e.Op())))
	}

	// The spans are "tight" unless we used just a prefix.
	tight = (prefixLen == len(lhs.Elems))

	if nullVal {
		// NULL is treated semantically as "unknown value", so
		//   (1, 2) > (1, NULL) is NULL,
		// but
		//   (2, 2) > (1, NULL) is true.
		//
		// So either of these constraints:
		//   (a, b) > (1, NULL)
		//   (a, b) >= (1, NULL)
		// is true if and only if a > 1.
		boundary = excludeBoundary
		tight = true
	} else if !tight {
		// If we only keep a prefix, exclusive inequalities become inclusive.
		// For example:
		//   (a, b, c) > (1, 2, 3) becomes (a, b) >= (1, 2)
		boundary = includeBoundary
	}

	// We use notNullStartKey to disallow NULLs on the first column.
	startKey, startBoundary := c.notNullStartKey(offset)
	endKey, endBoundary := emptyKey, includeBoundary
	if less {
		endKey, endBoundary = constraint.MakeCompositeKey(datums...), boundary
	} else {
		startKey, startBoundary = constraint.MakeCompositeKey(datums...), boundary
	}

	// Consider (a, b, c) <= (1, 2, 3).
	//
	// If the columns are not null, the condition is equivalent to
	// the span [ - /1/2/3].
	//
	// If column a is nullable, the condition is equivalent to the
	// span (/NULL - /1/2/3].
	//
	// However, if column b or c is nullable, we still have to filter
	// out the NULLs on those columns, so whatever span we generate is
	// not "tight". For example, the span (/NULL - /1/2/3] can contain
	// (1, NULL, NULL) which is not <= (1, 2, 3).
	// TODO(radu): we could generate multiple spans:
	//   (/NULL - /0], (/1/NULL - /1/1], (/1/2/NULL, /1/2/3]
	//
	// If the condition is > or >= this is not a problem. For example
	// (a, b, c) >= (1, 2, 3) has the span [/1/2/3 - ] which excludes
	// any values of the form (1, NULL, x) or (1, 2, NULL). Other values
	// with NULLs like (2, NULL, NULL) are ok because
	// (2, NULL, NULL) >= (1, 2, 3).
	// Note that if the direction is descending, the handling of < and > flips.
	if tight && (less != descending) {
		for i := 1; i < prefixLen; i++ {
			if c.isNullable(offset + i) {
				tight = false
				break
			}
		}
	}
	c.singleSpan(offset, startKey, startBoundary, endKey, endBoundary, descending, out)
	return tight
}

// makeSpansForTupleIn creates spans for index columns starting at
// <offset> from a tuple IN tuple expression, for example:
//   (a, b, c) IN ((1, 2, 3), (4, 5, 6))
// Assumes that both sides are tuples.
// The <tight> return value indicates if the spans are exactly equivalent
// to the expression (and not weaker).
func (c *indexConstraintCtx) makeSpansForTupleIn(
	offset int, e opt.Expr, out *constraint.Constraint,
) (tight bool) {
	lhs, rhs := e.Child(0).(*memo.TupleExpr), e.Child(1).(*memo.TupleExpr)

	// Find the longest prefix of columns starting at <offset> which is contained
	// in the left-hand tuple; tuplePos[i] is the position of column <offset+i> in
	// the tuple.
	var tuplePos []int
	for i := offset; i < len(c.columns); i++ {
		found := false
		for j, child := range lhs.Elems {
			if c.isIndexColumn(child, i) {
				tuplePos = append(tuplePos, j)
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	if len(tuplePos) == 0 {
		c.unconstrained(offset, out)
		return false
	}

	// Create a span for each (tuple) value inside the right-hand side tuple.
	keyCtx := &c.keyCtx[offset]
	var spans constraint.Spans
	var sp constraint.Span
	spans.Alloc(len(rhs.Elems))
	for _, child := range rhs.Elems {
		valTuple, ok := child.(*memo.TupleExpr)
		if !ok {
			c.unconstrained(offset, out)
			return false
		}
		vals := make(tree.Datums, len(tuplePos))
		for i, pos := range tuplePos {
			val := valTuple.Elems[pos]
			if !opt.IsConstValueOp(val) {
				c.unconstrained(offset, out)
				return false
			}
			datum := memo.ExtractConstDatum(val)
			if !c.verifyType(offset+i, datum.ResolvedType()) {
				c.unconstrained(offset, out)
				return false
			}
			vals[i] = datum
		}
		containsNull := false
		for _, d := range vals {
			containsNull = containsNull || (d == tree.DNull)
		}
		// If the tuple contains a NULL, ignore it (it can't match any values).
		if !containsNull {
			key := constraint.MakeCompositeKey(vals...)
			sp.Init(key, includeBoundary, key, includeBoundary)
			spans.Append(&sp)
		}
	}

	// Sort and de-duplicate the values.
	// We don't rely on the sorted order of the right-hand side tuple because it's
	// only useful if all of the following are met:
	//  - we use all columns in the tuple, i.e. len(tuplePos) = lhs.ChildCount().
	//  - the columns are in the right order, i.e. tuplePos[i] = i.
	//  - the columns have the same directions
	// Note that SortAndMerge exits quickly if the ordering is already correct.
	spans.SortAndMerge(keyCtx)
	out.Init(keyCtx, &spans)
	// The spans are "tight" unless we used just a prefix.
	return len(tuplePos) == len(lhs.Elems)
}

// makeSpansForExpr creates spans for index columns starting at <offset>
// from the given expression.
//
// The <tight> return value indicates if the spans are exactly equivalent to the
// expression (and not weaker). See simplifyFilter for more information.
func (c *indexConstraintCtx) makeSpansForExpr(
	offset int, e opt.Expr, out *constraint.Constraint,
) (tight bool) {

	if opt.IsConstValueOp(e) {
		datum := memo.ExtractConstDatum(e)
		if datum == tree.DBoolFalse || datum == tree.DNull {
			// Condition is never true.
			c.contradiction(offset, out)
			return true
		}
		c.unconstrained(offset, out)
		return datum == tree.DBoolTrue
	}

	switch t := e.(type) {
	case *memo.FiltersExpr:
		switch len(*t) {
		case 0:
			c.unconstrained(offset, out)
			return true
		case 1:
			return c.makeSpansForExpr(offset, (*t)[0].Condition, out)
		default:
			// We don't have enough information to know if the spans are "tight".
			c.makeSpansForAnd(offset, t, out)
			return false
		}

	case *memo.FiltersItem:
		// Pass through the call.
		return c.makeSpansForExpr(offset, t.Condition, out)

	case *memo.AndExpr:
		// We don't have enough information to know if the spans are "tight".
		c.makeSpansForAnd(offset, t, out)
		return false

	case *memo.OrExpr:
		return c.makeSpansForOr(offset, t, out)

	case *memo.VariableExpr:
		// Support (@1) as (@1 = TRUE) if @1 is boolean.
		if c.colType(offset).Family() == types.BoolFamily && c.isIndexColumn(t, offset) {
			return c.makeSpansForSingleColumnDatum(offset, opt.EqOp, tree.DBoolTrue, out)
		}

	case *memo.NotExpr:
		// Support (NOT @1) as (@1 = FALSE) if @1 is boolean.
		if c.colType(offset).Family() == types.BoolFamily && c.isIndexColumn(t.Input, offset) {
			return c.makeSpansForSingleColumnDatum(offset, opt.EqOp, tree.DBoolFalse, out)
		}

	case *memo.RangeExpr:
		return c.makeSpansForExpr(offset, t.And, out)
	}

	if e.ChildCount() < 2 {
		c.unconstrained(offset, out)
		return false
	}
	child0, child1 := e.Child(0), e.Child(1)

	// Check for an operation where the left-hand side is an
	// indexed var for this column.
	if c.isIndexColumn(child0, offset) {
		tight := c.makeSpansForSingleColumn(offset, e.Op(), child1, out)
		if !out.IsUnconstrained() || tight {
			return tight
		}
		// We couldn't get any constraints for the column; see if we can at least
		// deduce a not-NULL constraint.
		if c.isNullable(offset) && opt.BoolOperatorRequiresNotNullArgs(e.Op()) {
			c.makeNotNullSpan(offset, out)
			return false
		}
	}
	// Check for tuple operations.
	if child0.Op() == opt.TupleOp && child1.Op() == opt.TupleOp {
		switch e.Op() {
		case opt.LtOp, opt.LeOp, opt.GtOp, opt.GeOp, opt.NeOp:
			// Tuple inequality.
			return c.makeSpansForTupleInequality(offset, e, out)
		case opt.InOp:
			// Tuple IN tuple.
			return c.makeSpansForTupleIn(offset, e, out)
		}
	}

	// Last resort: for conditions like a > b, our column can appear on the right
	// side. We can deduce a not-null constraint from such conditions.
	if c.isNullable(offset) && c.isIndexColumn(child1, offset) &&
		opt.BoolOperatorRequiresNotNullArgs(e.Op()) {
		c.makeNotNullSpan(offset, out)
		return false
	}

	c.unconstrained(offset, out)
	return false
}

// makeSpansForAnd calculates spans for an AndOp or FiltersOp.
func (c *indexConstraintCtx) makeSpansForAnd(offset int, e opt.Expr, out *constraint.Constraint) {
	// TODO(radu): sorting the expressions by the variable index, or pre-building
	// a map could help here.
	c.makeSpansForExpr(offset, e.Child(0), out)
	var exprConstraint constraint.Constraint
	for i, n := 1, e.ChildCount(); i < n; i++ {
		c.makeSpansForExpr(offset, e.Child(i), &exprConstraint)
		out.IntersectWith(c.evalCtx, &exprConstraint)
	}
	if out.IsUnconstrained() {
		return
	}

	// Now we try to refine the result with constraints on suffixes of the index
	// columns (i.e. higher offsets).
	// TODO(radu): we should be able to get the constraints for all offsets in a
	// single traversal of the subtree.
	var ofsC constraint.Constraint
	for delta := 0; ; {
		// In each iteration, we try to extend keys with constraints for the offset
		// that corresponds to where the key ends. We can skip offsets at which no
		// keys end.
		// To calculate this, we get the minimum length of any key that doesn't ends
		// at or after the current offset.
		minLen := len(c.columns)
		for j := 0; j < out.Spans.Count(); j++ {
			sp := out.Spans.Get(j)
			l := sp.StartKey().Length()
			if l > delta && minLen > l {
				minLen = l
			}
			l = sp.EndKey().Length()
			if l > delta && minLen > l {
				minLen = l
			}
		}
		delta = minLen
		if offset+delta >= len(c.columns) {
			break
		}

		c.makeSpansForExpr(offset+delta, e.Child(0), &ofsC)
		for j, n := 1, e.ChildCount(); j < n; j++ {
			c.makeSpansForExpr(offset+delta, e.Child(j), &exprConstraint)
			ofsC.IntersectWith(c.evalCtx, &exprConstraint)
		}
		out.Combine(c.evalCtx, &ofsC)
	}
}

// makeSpansForOr calculates spans for an OrOp.
func (c *indexConstraintCtx) makeSpansForOr(
	offset int, e opt.Expr, out *constraint.Constraint,
) (tight bool) {
	c.contradiction(offset, out)
	tight = true
	var exprConstraint constraint.Constraint
	for i, n := 0, e.ChildCount(); i < n; i++ {
		exprTight := c.makeSpansForExpr(offset, e.Child(i), &exprConstraint)
		if exprConstraint.IsUnconstrained() {
			// If we can't generate spans for a disjunct, exit early.
			c.unconstrained(offset, out)
			return false
		}
		// The OR is "tight" if all the spans are tight.
		tight = tight && exprTight
		out.UnionWith(c.evalCtx, &exprConstraint)
	}
	return tight
}

// makeInvertedIndexSpansForExpr is analogous to makeSpansForExpr, but it is
// used for inverted indexes. If allPaths is true, the slice is populated with
// all constraints found. Otherwise, this function stops at the first
// constraint. Note that we use a slice instead of a constraint.Set, because
// constraint.Set.Union merges spans of constraints on the same columns. This
// logically represents an OR of the different span constraints, while what
// we want to represent is effectively an AND of the different spans, forcing
// us to store every span in its own constraint.Constraint and store it in
// a slice.
func (c *indexConstraintCtx) makeInvertedIndexSpansForExpr(
	nd opt.Expr, constraints []*constraint.Constraint, allPaths bool,
) (bool, []*constraint.Constraint) {
	var tight bool
	out := &constraint.Constraint{}
	constrained := false
	switch nd.Op() {
	case opt.ContainsOp:
		lhs, rhs := nd.Child(0), nd.Child(1)

		if !c.isIndexColumn(lhs, 0 /* index */) || !opt.IsConstValueOp(rhs) {
			c.unconstrained(0 /* offset */, out)
			return false, append(constraints, out)
		}

		rightDatum := memo.ExtractConstDatum(rhs)

		if rightDatum == tree.DNull {
			c.contradiction(0 /* offset */, out)
			return false, append(constraints, out)
		}

		rd := rightDatum.(*tree.DJSON).JSON

		switch rd.Type() {
		case json.ArrayJSONType, json.ObjectJSONType:
			// First, check if there's more than one path through the datum.
			paths, err := json.AllPaths(rd)
			if err != nil {
				log.Errorf(context.TODO(), "unexpected JSON error: %+v", err)
				c.unconstrained(0 /* offset */, out)
				return false, append(constraints, out)
			}
			for i := range paths {
				hasContainerLeaf, err := paths[i].HasContainerLeaf()
				if err != nil {
					log.Errorf(context.TODO(), "unexpected JSON error: %+v", err)
					c.unconstrained(0 /* offset */, out)
					return false, append(constraints, out)
				}
				if hasContainerLeaf {
					// We want to have a full index scan if the RHS contains either [] or {}.
					continue
				}
				pathDatum, err := tree.MakeDJSON(paths[i])
				if err != nil {
					log.Errorf(context.TODO(), "unexpected JSON error: %+v", err)
					c.unconstrained(0 /* offset */, out)
					return false, append(constraints, out)
				}
				c.eqSpan(0 /* offset */, pathDatum, out)
				constraints = append(constraints, out)
				// The span is tight if we just had 1 path through the index constraint.
				constrained = true
				if !allPaths {
					return len(paths) == 1, constraints
				}
				// Reset out for next iteration
				out = &constraint.Constraint{}
			}

			// We found no paths that could constrain the scan.
			if !constrained {
				c.unconstrained(0 /* offset */, out)
				return false, append(constraints, out)
			}
			return len(paths) == 1, constraints

		default:
			// If we find a scalar on the right side of the @> operator it means that we need to find
			// both matching scalars and arrays that contain that value. In order to do this we generate
			// two logical spans, one for the original scalar and one for arrays containing the scalar.
			// This is valid because in JSON something can either be an array or scalar so the spans are
			// guaranteed not to overlap when mapped onto the primary key space. Therefore there won't be
			// any duplicate primary keys when we retrieve rows for both sets.
			j := json.NewArrayBuilder(1)
			j.Add(rd)
			dJSON, err := tree.MakeDJSON(j.Build())
			if err != nil {
				break
			}

			// This is the span for the scalar.
			c.eqSpan(0 /* offset */, rightDatum, out)

			// This is the span to match arrays.
			var other constraint.Constraint
			c.eqSpan(0 /* offset */, dJSON, &other)
			out.UnionWith(c.evalCtx, &other)
			return true, append(constraints, out)
		}

	case opt.AndOp, opt.FiltersOp:
		for i, n := 0, nd.ChildCount(); i < n; i++ {
			tight, constraints = c.makeInvertedIndexSpansForExpr(
				nd.Child(i), constraints, allPaths,
			)
			if n == 1 {
				// Single child.
				return tight, constraints
			}
			// Only need to check the last appended element in constraints
			// to figure out whether we are unconstrained.
			out = constraints[len(constraints)-1]
			if !out.IsUnconstrained() {
				if !allPaths {
					// TODO(radu, masha): for now, the best we can do is to generate
					// constraints for at most one "contains" op in the disjunction; the
					// rest are remaining filters.
					//
					// The spans are not tight because we have other conditions in the
					// conjunction.
					return false, constraints
				}
				constrained = true
			} else {
				// The last added element is unconstrained. Remove it from the slice
				// in case the next iteration finds a constraint that works.
				constraints = constraints[:len(constraints)-1]
			}
		}

	case opt.FiltersItemOp:
		// Pass through the call.
		return c.makeInvertedIndexSpansForExpr(nd.Child(0), constraints, allPaths)
	}
	if !constrained {
		c.unconstrained(0 /* offset */, out)
		constraints = append(constraints, out)
	}
	return false, constraints
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
func (c *indexConstraintCtx) getMaxSimplifyPrefix(idxConstraint *constraint.Constraint) int {
	maxOffset := len(c.columns) - 1
	for i := 0; i < idxConstraint.Spans.Count(); i++ {
		sp := idxConstraint.Spans.Get(i)
		j := 0
		// Find the longest prefix of equal values.
		for ; j < sp.StartKey().Length() && j < sp.EndKey().Length(); j++ {
			if sp.StartKey().Value(j).Compare(c.evalCtx, sp.EndKey().Value(j)) != 0 {
				break
			}
		}
		if j == 0 {
			return 0
		}
		if maxOffset > j {
			maxOffset = j
		}
	}
	return maxOffset
}

// simplifyFilter removes parts of the filter that are satisfied by the spans. It
// is best-effort. Returns nil if there is no remaining filter.
//
// Can return an opt.TrueOp.
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
	scalar opt.ScalarExpr, final *constraint.Constraint, maxSimplifyPrefix int,
) opt.ScalarExpr {
	// Special handling for And, Or, and Range.
	switch t := scalar.(type) {
	case *memo.AndExpr:
		left := c.simplifyFilter(t.Left, final, maxSimplifyPrefix)
		right := c.simplifyFilter(t.Right, final, maxSimplifyPrefix)
		return c.factory.ConstructAnd(left, right)

	case *memo.OrExpr:
		left := c.simplifyFilter(t.Left, final, maxSimplifyPrefix)
		right := c.simplifyFilter(t.Right, final, maxSimplifyPrefix)
		return c.factory.ConstructOr(left, right)

	case *memo.RangeExpr:
		return c.factory.ConstructRange(c.simplifyFilter(t.And, final, maxSimplifyPrefix))
	}

	// We try to create tight spans for the expression (as allowed by
	// maxSimplifyPrefix), and check if the condition is implied by the final
	// spans. See getMaxSimplifyPrefix for more information.
	for offset := 0; offset <= maxSimplifyPrefix; offset++ {
		var cExpr constraint.Constraint
		var tight bool
		if c.isInverted {
			if offset == 0 {
				constraints := make([]*constraint.Constraint, 0, 1)
				tight, constraints = c.makeInvertedIndexSpansForExpr(scalar, constraints, false)
				// makeInvertedIndexSpansForExpr is guaranteed to add at least one
				// constraint. It may be an "unconstrained" constraint.
				cExpr = *constraints[0]
			} else {
				c.unconstrained(0, &cExpr)
			}
		} else {
			tight = c.makeSpansForExpr(offset, scalar, &cExpr)
		}

		if !tight {
			continue
		}
		for i := 0; i < final.Spans.Count(); i++ {
			sp := *final.Spans.Get(i)
			if offset > 0 {
				sp.CutFront(offset)
			}
			if !cExpr.ContainsSpan(c.evalCtx, &sp) {
				// We won't get another tight constraint at another offset.
				return scalar
			}
		}

		// The final spans are a subset of the spans for this expression; there
		// is no need for a remaining filter for this condition.
		return memo.TrueSingleton
	}
	return scalar
}

// Instance is used to generate index constraints from a scalar boolean filter
// expression.
//
// Sample usage:
//   var ic Instance
//   if err := ic.Init(...); err != nil {
//     ..
//   }
//   spans, ok := ic.Spans()
//   remFilterGroup := ic.RemainingFilter()
//   remFilter := o.Optimize(remFilterGroup, &opt.PhysicalProps{})
type Instance struct {
	indexConstraintCtx

	filters memo.FiltersExpr

	constraint   constraint.Constraint
	consolidated constraint.Constraint
	tight        bool
	initialized  bool
}

// Init processes the filter and calculates the spans.
func (ic *Instance) Init(
	filters memo.FiltersExpr,
	columns []opt.OrderingColumn,
	notNullCols opt.ColSet,
	isInverted bool,
	evalCtx *tree.EvalContext,
	factory *norm.Factory,
) {
	ic.filters = filters
	ic.indexConstraintCtx.init(columns, notNullCols, isInverted, evalCtx, factory)
	constraints := make([]*constraint.Constraint, 0, 1)
	if isInverted {
		ic.tight, constraints = ic.makeInvertedIndexSpansForExpr(
			&ic.filters, constraints, false,
		)
		// makeInvertedIndexSpansForExpr is guaranteed to add at least one
		// constraint. It may be an "unconstrained" constraint.
		ic.constraint = *constraints[0]
	} else {
		ic.tight = ic.makeSpansForExpr(0 /* offset */, &ic.filters, &ic.constraint)
	}
	// Note: we only consolidate spans at the end; consolidating partial results
	// can lead to worse spans, for example:
	//   a IN (1, 2) AND b = 4
	//
	// We want this to be:
	//   [/1/4 - /1/4]
	//   [/2/4 - /2/4]
	//
	// If we eagerly consolidate the spans for a, we would get loose spans:
	//   [/1/4 - /2/4]
	//
	// We also want to remember the original spans because the filter
	// simplification code works better with them. For example:
	//   @1 = 1 AND @2 IN (1, 2)
	// The filter simplification code is able to simplify both expressions
	// if we have the spans [/1/1 - /1/1], [/1/2 - /1/2] but not if
	// we have [/1/1 - /1/2].
	ic.consolidated = ic.constraint
	ic.consolidated.ConsolidateSpans(evalCtx)
	ic.initialized = true
}

// Constraint returns the constraint created by Init. If Init wasn't called,
// the result is nil.
func (ic *Instance) Constraint() *constraint.Constraint {
	if !ic.initialized {
		return nil
	}
	return &ic.consolidated
}

// AllInvertedIndexConstraints returns all constraints that can be created on
// the specified inverted index. Only works for inverted indexes.
func (ic *Instance) AllInvertedIndexConstraints() ([]*constraint.Constraint, error) {
	allConstraints := make([]*constraint.Constraint, 0)
	_, allConstraints = ic.makeInvertedIndexSpansForExpr(&ic.filters, allConstraints, true)

	return allConstraints, nil
}

// RemainingFilters calculates a simplified FiltersExpr that needs to be applied
// within the returned Spans.
func (ic *Instance) RemainingFilters() memo.FiltersExpr {
	if ic.tight || ic.constraint.IsContradiction() {
		// The spans are "tight", or we have a contradiction; there is no remaining filter.
		return memo.TrueFilter
	}
	if ic.constraint.IsUnconstrained() {
		return ic.filters
	}
	var newFilters memo.FiltersExpr
	for i := range ic.filters {
		prefix := ic.getMaxSimplifyPrefix(&ic.constraint)
		condition := ic.simplifyFilter(ic.filters[i].Condition, &ic.constraint, prefix)
		if condition.Op() != opt.TrueOp {
			if newFilters == nil {
				newFilters = make(memo.FiltersExpr, 0, len(ic.filters))
			}
			newFilters = append(newFilters, memo.FiltersItem{Condition: condition})
		}
	}
	return newFilters
}

type indexConstraintCtx struct {
	md *opt.Metadata

	columns []opt.OrderingColumn

	notNullCols opt.ColSet

	// isInverted indicates if the index is an inverted index (e.g. JSONB).
	// An inverted index behaves differently than a normal index because a PK
	// can appear in multiple index entries. For example, `a @> x AND a @> y` is
	// not a contradiction because some PKs can appear in both regions of the
	// index (so AND is no longer just span intersection).
	isInverted bool

	evalCtx *tree.EvalContext

	// We pre-initialize the KeyContext for each suffix of the index columns.
	keyCtx []constraint.KeyContext

	factory *norm.Factory
}

func (c *indexConstraintCtx) init(
	columns []opt.OrderingColumn,
	notNullCols opt.ColSet,
	isInverted bool,
	evalCtx *tree.EvalContext,
	factory *norm.Factory,
) {
	c.md = factory.Metadata()
	c.columns = columns
	c.notNullCols = notNullCols
	c.isInverted = isInverted
	c.evalCtx = evalCtx
	c.factory = factory

	c.keyCtx = make([]constraint.KeyContext, len(columns))
	for i := range columns {
		c.keyCtx[i].EvalCtx = evalCtx
		c.keyCtx[i].Columns.Init(columns[i:])
	}
}

// isIndexColumn returns true if ev is a variable on the n indexed var that
// corresponds to index column <offset>.
func (c *indexConstraintCtx) isIndexColumn(nd opt.Expr, offset int) bool {
	if v, ok := nd.(*memo.VariableExpr); ok && v.Col == c.columns[offset].ID() {
		return true
	}
	return false
}

// isNullable returns true if the index column <offset> is nullable.
func (c *indexConstraintCtx) isNullable(offset int) bool {
	return !c.notNullCols.Contains(c.columns[offset].ID())
}

// colType returns the type of the index column <offset>.
func (c *indexConstraintCtx) colType(offset int) *types.T {
	return c.md.ColumnMeta(c.columns[offset].ID()).Type
}
