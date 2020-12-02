// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// BuildConstraints returns a constraint.Set that represents the given scalar
// expression. A "tight" boolean is also returned which is true if the
// constraint is exactly equivalent to the expression.
func BuildConstraints(
	e opt.ScalarExpr, md *opt.Metadata, evalCtx *tree.EvalContext,
) (_ *constraint.Set, tight bool) {
	cb := constraintsBuilder{md: md, evalCtx: evalCtx}
	return cb.buildConstraints(e)
}

// Convenience aliases to avoid the constraint prefix everywhere.
const includeBoundary = constraint.IncludeBoundary
const excludeBoundary = constraint.ExcludeBoundary

var emptyKey = constraint.EmptyKey
var unconstrained = constraint.Unconstrained
var contradiction = constraint.Contradiction

// constraintsBuilder is used to create constraints (constraint.Set) from
// boolean scalar expressions. The constraints are stored in the logical
// properties; in certain cases, they become constraints for relational
// operators (e.g. Select). They can also be used to transfer over conditions
// between the two sides of a join.
//
// A constraint is "tight" if it is exactly equivalent to the expression. A
// constraint that is not tight is weaker than the expression.
type constraintsBuilder struct {
	md      *opt.Metadata
	evalCtx *tree.EvalContext
}

// buildSingleColumnConstraint creates a constraint set implied by
// a binary boolean operator.
func (cb *constraintsBuilder) buildSingleColumnConstraint(
	col opt.ColumnID, op opt.Operator, val opt.Expr,
) (_ *constraint.Set, tight bool) {
	if op == opt.InOp && CanExtractConstTuple(val) {
		els := val.(*TupleExpr).Elems
		keyCtx := constraint.KeyContext{EvalCtx: cb.evalCtx}
		keyCtx.Columns.InitSingle(opt.MakeOrderingColumn(col, false /* descending */))

		var spans constraint.Spans
		spans.Alloc(len(els))
		var sp constraint.Span
		for _, child := range els {
			datum := ExtractConstDatum(child)
			if !cb.verifyType(col, datum.ResolvedType()) {
				return unconstrained, false
			}
			if datum == tree.DNull {
				// Ignore NULLs - they can't match any values
				continue
			}
			key := constraint.MakeKey(datum)
			sp.Init(key, includeBoundary, key, includeBoundary)
			spans.Append(&sp)
		}
		var c constraint.Constraint
		spans.SortAndMerge(&keyCtx)
		c.Init(&keyCtx, &spans)
		return constraint.SingleConstraint(&c), true
	}

	if opt.IsConstValueOp(val) || CanExtractConstTuple(val) {
		res, tight := cb.buildSingleColumnConstraintConst(col, op, ExtractConstDatum(val))
		if res != unconstrained {
			return res, tight
		}
	}

	// Try to at least deduce a not-null constraint.
	if opt.BoolOperatorRequiresNotNullArgs(op) {
		res := cb.notNullSpan(col)
		// Check if the right-hand side is a variable too (e.g. a > b).
		if v, ok := val.(*VariableExpr); ok {
			res = res.Intersect(cb.evalCtx, cb.notNullSpan(v.Col))
		}
		return res, false
	}

	return unconstrained, false
}

func (cb *constraintsBuilder) buildSingleColumnConstraintConst(
	col opt.ColumnID, op opt.Operator, datum tree.Datum,
) (_ *constraint.Set, tight bool) {
	if !cb.verifyType(col, datum.ResolvedType()) {
		return unconstrained, false
	}
	if datum == tree.DNull {
		switch op {
		case opt.EqOp, opt.LtOp, opt.GtOp, opt.LeOp, opt.GeOp, opt.NeOp:
			// The result of this expression is always NULL. Normally, this expression
			// should have been converted to NULL during type checking; but if the
			// NULL is coming from a placeholder, that doesn't happen.
			return contradiction, true

		case opt.IsOp:
			return cb.eqSpan(col, tree.DNull), true

		case opt.IsNotOp:
			return cb.notNullSpan(col), true
		}
		return unconstrained, false
	}

	switch op {
	case opt.EqOp, opt.IsOp:
		return cb.eqSpan(col, datum), true

	case opt.LtOp, opt.GtOp, opt.LeOp, opt.GeOp:
		startKey, startBoundary := constraint.MakeKey(tree.DNull), excludeBoundary
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

		return cb.singleSpan(col, startKey, startBoundary, endKey, endBoundary), true

	case opt.NeOp, opt.IsNotOp:
		// Build constraint that doesn't contain the key:
		//   IsNotOp  : [ - key) (key - ]
		//   NeOp     : (/NULL - key) (key - ]
		//
		// If the key is the minimum possible value for the column type, the span
		// (/NULL - key) will never contain any values and can be omitted.
		//
		// Similarly, if the key is the maximum possible value, the span (key - ]
		// can be omitted.
		startKey, startBoundary := emptyKey, includeBoundary
		if op == opt.NeOp {
			startKey, startBoundary = constraint.MakeKey(tree.DNull), excludeBoundary
		}
		key := constraint.MakeKey(datum)
		c := contradiction
		if startKey.IsEmpty() || !datum.IsMin(cb.evalCtx) {
			c = cb.singleSpan(col, startKey, startBoundary, key, excludeBoundary)
		}
		if !datum.IsMax(cb.evalCtx) {
			other := cb.singleSpan(col, key, excludeBoundary, emptyKey, includeBoundary)
			c = c.Union(cb.evalCtx, other)
		}
		return c, true

	case opt.LikeOp:
		if s, ok := tree.AsDString(datum); ok {
			if i := strings.IndexAny(string(s), "_%"); i >= 0 {
				if i == 0 {
					// Mask starts with _ or %.
					return unconstrained, false
				}
				c := cb.makeStringPrefixSpan(col, string(s[:i]))
				// A mask like ABC% is equivalent to restricting the prefix to ABC.
				// A mask like ABC%Z requires restricting the prefix, but is a stronger
				// condition.
				tight := (i == len(s)-1) && s[i] == '%'
				return c, tight
			}
			// No wildcard characters, this is an equality.
			return cb.eqSpan(col, &s), true
		}

	case opt.SimilarToOp:
		// a SIMILAR TO 'foo_*' -> prefix "foo"
		if s, ok := tree.AsDString(datum); ok {
			pattern := tree.SimilarEscape(string(s))
			if re, err := regexp.Compile(pattern); err == nil {
				prefix, complete := re.LiteralPrefix()
				if complete {
					return cb.eqSpan(col, tree.NewDString(prefix)), true
				}
				return cb.makeStringPrefixSpan(col, prefix), false
			}
		}

	case opt.ContainsOp:
		if arr, ok := datum.(*tree.DArray); ok {
			if arr.HasNulls {
				return contradiction, true
			}
		}
		// NULL cannot contain anything, so a non-tight, not-null span is built.
		return cb.notNullSpan(col), false
	}
	return unconstrained, false
}

// buildConstraintForTupleIn handles the case where we have a tuple IN another
// tuple, for instance:
//
//   (a, b, c) IN ((1, 2, 3), (4, 5, 6))
//
// This function is a less powerful version of makeSpansForTupleIn, since it
// does not operate on a particular index.  The <tight> return value indicates
// if the spans are exactly equivalent to the expression (and not weaker).
// Assumes that ev is an InOp and both children are TupleOps.
func (cb *constraintsBuilder) buildConstraintForTupleIn(
	in *InExpr,
) (_ *constraint.Set, tight bool) {
	lhs, rhs := in.Left.(*TupleExpr), in.Right.(*TupleExpr)

	// We can only constrain here if every element of rhs is a TupleOp.
	for _, elem := range rhs.Elems {
		if elem.Op() != opt.TupleOp {
			return unconstrained, false
		}
	}

	constrainedCols := make([]opt.OrderingColumn, 0, len(lhs.Elems))
	colIdxsInLHS := make([]int, 0, len(lhs.Elems))
	for i, lelem := range lhs.Elems {
		if v, ok := lelem.(*VariableExpr); ok {
			// We can't constrain a column if it's compared to anything besides a constant.
			allConstant := true
			for _, relem := range rhs.Elems {
				// Element must be tuple (checked above).
				tup := relem.(*TupleExpr)
				if !opt.IsConstValueOp(tup.Elems[i]) {
					allConstant = false
					break
				}
			}

			if allConstant {
				constrainedCols = append(
					constrainedCols,
					opt.MakeOrderingColumn(v.Col, false /* descending */),
				)
				colIdxsInLHS = append(colIdxsInLHS, i)
			}
		}
	}

	if len(constrainedCols) == 0 {
		return unconstrained, false
	}

	// If any of the LHS entries are not constrained then our constraints are not
	// tight.
	tight = (len(constrainedCols) == len(lhs.Elems))

	keyCtx := constraint.KeyContext{EvalCtx: cb.evalCtx}
	keyCtx.Columns.Init(constrainedCols)
	var sp constraint.Span
	var spans constraint.Spans
	spans.Alloc(len(rhs.Elems))

	keyCtx.Columns.Init(constrainedCols)
	for _, elem := range rhs.Elems {
		// Element must be tuple (checked above).
		tup := elem.(*TupleExpr)
		vals := make(tree.Datums, len(colIdxsInLHS))

		hasNull := false
		for j := range colIdxsInLHS {
			constval := tup.Elems[colIdxsInLHS[j]]
			datum := ExtractConstDatum(constval)
			if datum == tree.DNull {
				hasNull = true
				break
			}
			vals[j] = datum
		}

		// Nothing can match a tuple containing a NULL, so it introduces no
		// constraints.
		if hasNull {
			// TODO(justin): consider redefining "tight" so that this is included in
			// it.  The spans are not "exactly equivalent" in the presence of NULLs,
			// because of examples like the following:
			//   (x, y) IN ((1, 2), (NULL, 4))
			// is not the same as
			//   (x, y) IN ((1, 2)),
			// because the former is NULL (not false) on (3,4).
			tight = false
			continue
		}

		key := constraint.MakeCompositeKey(vals...)
		sp.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
		spans.Append(&sp)
	}

	spans.SortAndMerge(&keyCtx)

	var c constraint.Constraint
	c.Init(&keyCtx, &spans)
	con := constraint.SingleConstraint(&c)

	// Now add a constraint for each individual column. This makes extracting
	// constant columns much simpler.
	// TODO(justin): remove this when #27018 is resolved.
	// We already have a constraint starting with the first column: the
	// multi-column constraint we added above.
	for i := 1; i < len(colIdxsInLHS); i++ {
		var spans constraint.Spans
		keyCtx := constraint.KeyContext{EvalCtx: cb.evalCtx}
		keyCtx.Columns.InitSingle(constrainedCols[i])
		for _, elem := range rhs.Elems {
			// Element must be tuple (checked above).
			constVal := elem.(*TupleExpr).Elems[colIdxsInLHS[i]]
			datum := ExtractConstDatum(constVal)
			key := constraint.MakeKey(datum)
			var sp constraint.Span
			sp.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
			spans.Append(&sp)
		}

		spans.SortAndMerge(&keyCtx)
		var c constraint.Constraint
		c.Init(&keyCtx, &spans)
		con = con.Intersect(cb.evalCtx, constraint.SingleConstraint(&c))
	}

	return con, tight
}

func (cb *constraintsBuilder) buildConstraintForTupleInequality(
	e opt.ScalarExpr,
) (_ *constraint.Set, tight bool) {
	lhs, rhs := e.Child(0).(*TupleExpr), e.Child(1).(*TupleExpr)
	if !CanExtractConstDatum(rhs) {
		return unconstrained, false
	}

	// Find the longest prefix that has only variables on the left side and only
	// non-NULL constants on the right side.
	for i, leftChild := range lhs.Elems {
		rightChild := rhs.Elems[i]
		variable, ok := leftChild.(*VariableExpr)
		if !ok {
			return unconstrained, false
		}
		if !cb.verifyType(variable.Col, rightChild.DataType()) {
			// We have a mixed-type comparison.
			return unconstrained, false
		}
		if rightChild.Op() == opt.NullOp {
			// TODO(radu): NULLs are tricky and require special handling; we ignore
			// the expression for now.
			return unconstrained, false
		}
	}

	datums := make(tree.Datums, len(lhs.Elems))
	for i := range datums {
		datums[i] = ExtractConstDatum(rhs.Elems[i])
	}
	key := constraint.MakeCompositeKey(datums...)

	// less is true if the op is < or <= and false if the op is > or >=.
	// boundary is inclusive if the op is <= or >= and exclusive if the op
	// is < or >.
	var less bool
	var boundary constraint.SpanBoundary

	switch e.Op() {
	case opt.NeOp:
		// TODO(radu)
		return unconstrained, false
	case opt.LtOp:
		less, boundary = true, excludeBoundary
	case opt.LeOp:
		less, boundary = true, includeBoundary
	case opt.GtOp:
		less, boundary = false, excludeBoundary
	case opt.GeOp:
		less, boundary = false, includeBoundary
	default:
		panic(errors.AssertionFailedf("unsupported operator type %s", log.Safe(e.Op())))
	}
	// Disallow NULLs on the first column.
	startKey, startBoundary := constraint.MakeKey(tree.DNull), excludeBoundary
	endKey, endBoundary := emptyKey, includeBoundary
	if less {
		endKey, endBoundary = key, boundary
	} else {
		startKey, startBoundary = key, boundary
	}

	var span constraint.Span
	span.Init(startKey, startBoundary, endKey, endBoundary)

	keyCtx := constraint.KeyContext{EvalCtx: cb.evalCtx}
	cols := make([]opt.OrderingColumn, len(lhs.Elems))
	for i := range cols {
		v := lhs.Elems[i].(*VariableExpr)
		cols[i] = opt.MakeOrderingColumn(v.Col, false /* descending */)
	}
	keyCtx.Columns.Init(cols)
	span.PreferInclusive(&keyCtx)
	return constraint.SingleSpanConstraint(&keyCtx, &span), true
}

func (cb *constraintsBuilder) buildFunctionConstraints(
	f *FunctionExpr,
) (_ *constraint.Set, tight bool) {
	if f.Properties.NullableArgs {
		return unconstrained, false
	}

	// For an arbitrary function, the best we can do is deduce a set of not-null
	// constraints.
	cs := unconstrained
	for _, arg := range f.Args {
		if variable, ok := arg.(*VariableExpr); ok {
			cs = cs.Intersect(cb.evalCtx, cb.notNullSpan(variable.Col))
		}
	}

	return cs, false
}

func (cb *constraintsBuilder) buildConstraints(e opt.ScalarExpr) (_ *constraint.Set, tight bool) {
	switch t := e.(type) {
	case *FalseExpr, *NullExpr:
		return contradiction, true

	case *VariableExpr:
		// (x) is equivalent to (x = TRUE) if x is boolean.
		if cb.md.ColumnMeta(t.Col).Type.Family() == types.BoolFamily {
			return cb.buildSingleColumnConstraintConst(t.Col, opt.EqOp, tree.DBoolTrue)
		}
		return unconstrained, false

	case *NotExpr:
		// (NOT x) is equivalent to (x = FALSE) if x is boolean.
		if v, ok := t.Input.(*VariableExpr); ok {
			if cb.md.ColumnMeta(v.Col).Type.Family() == types.BoolFamily {
				return cb.buildSingleColumnConstraintConst(v.Col, opt.EqOp, tree.DBoolFalse)
			}
		}
		return unconstrained, false

	case *AndExpr:
		cl, tightl := cb.buildConstraints(t.Left)
		cr, tightr := cb.buildConstraints(t.Right)
		cl = cl.Intersect(cb.evalCtx, cr)
		tightl = tightl && tightr
		return cl, (tightl || cl == contradiction)

	case *OrExpr:
		cl, tightl := cb.buildConstraints(t.Left)
		cr, tightr := cb.buildConstraints(t.Right)
		res := cl.Union(cb.evalCtx, cr)

		// The union may not be "tight" because the new constraint set might
		// allow combinations of values that the expression does not allow.
		//
		// For example, consider the expression:
		//
		//   (@1 = 4 AND @2 = 6) OR (@1 = 5 AND @2 = 7)
		//
		// The resulting constraint set is:
		//
		//   /1: [/4 - /4] [/5 - /5]
		//   /2: [/6 - /6] [/7 - /7]
		//
		// This constraint set is not tight, because it allows values for @1
		// and @2 that the original expression does not, such as @1=4, @2=7.
		//
		// However, there are three cases in which the union constraint set is
		// tight.
		//
		// First, if the left, right, and result sets have a single constraint,
		// then the result constraint is tight if the left and right are tight.
		// If there is a single constraint for all three sets, it implies that
		// the sets involve the same column. Therefore it is safe to determine
		// the tightness of the union based on the tightness of the left and
		// right.
		//
		// Second, if one of the left or right set is a contradiction, then the
		// result constraint is tight if the other input set is tight. This is
		// because contradictions are tight and fully describe the set of
		// values that the original expression allows - none.
		//
		// For example, consider the expression:
		//
		//   (@1 = 4 AND @1 = 6) OR (@1 = 5 AND @2 = 7)
		//
		// The resulting constraint set is:
		//
		//   /1: [/5 - /5]
		//   /2: [/7 - /7]
		//
		// This constraint set is tight, because there are no values for @1 and
		// @2 that satisfy the set but do not satisfy the expression.
		//
		// Third, if both the left and the right set are contradictions, then
		// the result set is tight. This is because contradictions are tight
		// and, as explained above, they fully describe the set of values that
		// satisfy their expression. Note that this third case is generally
		// covered by the second case, but it's mentioned here for the sake of
		// explicitness.
		if cl == contradiction {
			return res, tightr
		}
		if cr == contradiction {
			return res, tightl
		}
		tight := tightl && tightr && cl.Length() == 1 && cr.Length() == 1 && res.Length() == 1
		return res, tight

	case *RangeExpr:
		return cb.buildConstraints(t.And)

	case *FunctionExpr:
		return cb.buildFunctionConstraints(t)
	}

	if e.ChildCount() < 2 {
		return unconstrained, false
	}

	child0, child1 := e.Child(0), e.Child(1)
	// Check for an operation where the left-hand side is an
	// indexed var for this column.

	// Check for tuple operations.
	if child0.Op() == opt.TupleOp && child1.Op() == opt.TupleOp {
		switch e.Op() {
		case opt.LtOp, opt.LeOp, opt.GtOp, opt.GeOp, opt.NeOp:
			// Tuple inequality.
			return cb.buildConstraintForTupleInequality(e)

		case opt.InOp:
			return cb.buildConstraintForTupleIn(e.(*InExpr))
		}
	}
	if v, ok := child0.(*VariableExpr); ok {
		return cb.buildSingleColumnConstraint(v.Col, e.Op(), child1)
	}
	return unconstrained, false
}

func (cb *constraintsBuilder) singleSpan(
	col opt.ColumnID,
	start constraint.Key,
	startBoundary constraint.SpanBoundary,
	end constraint.Key,
	endBoundary constraint.SpanBoundary,
) *constraint.Set {
	var span constraint.Span
	span.Init(start, startBoundary, end, endBoundary)
	keyCtx := constraint.KeyContext{EvalCtx: cb.evalCtx}
	keyCtx.Columns.InitSingle(opt.MakeOrderingColumn(col, false /* descending */))
	span.PreferInclusive(&keyCtx)
	return constraint.SingleSpanConstraint(&keyCtx, &span)
}

func (cb *constraintsBuilder) notNullSpan(col opt.ColumnID) *constraint.Set {
	key := constraint.MakeKey(tree.DNull)
	return cb.singleSpan(col, key, excludeBoundary, emptyKey, includeBoundary)
}

// eqSpan constrains a column to a single value (which can be DNull).
func (cb *constraintsBuilder) eqSpan(col opt.ColumnID, value tree.Datum) *constraint.Set {
	key := constraint.MakeKey(value)
	return cb.singleSpan(col, key, includeBoundary, key, includeBoundary)
}

// makeStringPrefixSpan constraints a string column to strings having the given prefix.
func (cb *constraintsBuilder) makeStringPrefixSpan(
	col opt.ColumnID, prefix string,
) *constraint.Set {
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
	return cb.singleSpan(col, startKey, startBoundary, endKey, endBoundary)
}

// verifyType checks that the type of column matches the given type. We disallow
// mixed-type comparisons because if they become index constraints, we would
// generate incorrect encodings (#4313).
func (cb *constraintsBuilder) verifyType(col opt.ColumnID, typ *types.T) bool {
	return typ.Family() == types.UnknownFamily || cb.md.ColumnMeta(col).Type.Equivalent(typ)
}
