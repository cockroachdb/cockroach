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

package memo

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

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
	col opt.ColumnID, op opt.Operator, val ExprView,
) (_ *constraint.Set, tight bool) {
	if op == opt.InOp && MatchesTupleOfConstants(val) {
		keyCtx := constraint.KeyContext{EvalCtx: cb.evalCtx}
		keyCtx.Columns.InitSingle(opt.MakeOrderingColumn(col, false /* descending */))

		var spans constraint.Spans
		spans.Alloc(val.ChildCount())
		var sp constraint.Span
		for i, n := 0, val.ChildCount(); i < n; i++ {
			datum := ExtractConstDatum(val.Child(i))
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
		c.Init(&keyCtx, &spans)
		return constraint.SingleConstraint(&c), true
	}

	if val.IsConstValue() || MatchesTupleOfConstants(val) {
		return cb.buildSingleColumnConstraintConst(col, op, ExtractConstDatum(val))
	}

	// Try to at least deduce a not-null constraint.
	if opt.BoolOperatorRequiresNotNullArgs(op) {
		res := cb.notNullSpan(col)
		// Check if the right-hand side is a variable too (e.g. a > b).
		if val.Operator() == opt.VariableOp {
			res = res.Intersect(cb.evalCtx, cb.notNullSpan(val.Private().(opt.ColumnID)))
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

		// TODO(radu): handle other ops.
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
	ev ExprView,
) (_ *constraint.Set, tight bool) {
	lhs, rhs := ev.Child(0), ev.Child(1)

	// We can only constrain here if every element of rhs is a TupleOp.
	for i, n := 0, rhs.ChildCount(); i < n; i++ {
		val := rhs.Child(i)
		if val.Operator() != opt.TupleOp {
			return unconstrained, false
		}
	}

	constrainedCols := make([]opt.OrderingColumn, 0, lhs.ChildCount())
	colIdxsInLHS := make([]int, 0, lhs.ChildCount())
	for i, n := 0, lhs.ChildCount(); i < n; i++ {
		if colID, ok := lhs.Child(i).Private().(opt.ColumnID); ok {
			// We can't constrain a column if it's compared to anything besides a constant.
			allConstant := true
			for j, m := 0, rhs.ChildCount(); j < m; j++ {
				val := rhs.Child(j)

				if val.Operator() != opt.TupleOp {
					return unconstrained, false
				}

				if !val.Child(i).IsConstValue() {
					allConstant = false
					break
				}
			}

			if allConstant {
				constrainedCols = append(
					constrainedCols,
					opt.MakeOrderingColumn(colID, false /* descending */),
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
	tight = (len(constrainedCols) == lhs.ChildCount())

	keyCtx := constraint.KeyContext{EvalCtx: cb.evalCtx}
	keyCtx.Columns.Init(constrainedCols)
	var sp constraint.Span
	var spans constraint.Spans
	spans.Alloc(rhs.ChildCount())

	keyCtx.Columns.Init(constrainedCols)
	for i, n := 0, rhs.ChildCount(); i < n; i++ {
		vals := make(tree.Datums, len(colIdxsInLHS))
		val := rhs.Child(i)

		hasNull := false
		for j := range colIdxsInLHS {
			elem := val.Child(colIdxsInLHS[j])
			datum := ExtractConstDatum(elem)
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
		for j, n := 0, rhs.ChildCount(); j < n; j++ {
			val := rhs.Child(j)
			elem := val.Child(colIdxsInLHS[i])
			datum := ExtractConstDatum(elem)
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
	ev ExprView,
) (_ *constraint.Set, tight bool) {
	lhs, rhs := ev.Child(0), ev.Child(1)
	if !MatchesTupleOfConstants(rhs) {
		return unconstrained, false
	}

	// Find the longest prefix that has only variables on the left side and only
	// non-NULL constants on the right side.
	for i, n := 0, lhs.ChildCount(); i < n; i++ {
		leftChild, rightChild := lhs.Child(i), rhs.Child(i)
		if leftChild.Operator() != opt.VariableOp {
			return unconstrained, false
		}
		if !cb.verifyType(leftChild.Private().(opt.ColumnID), rightChild.Logical().Scalar.Type) {
			// We have a mixed-type comparison.
			return unconstrained, false
		}
		if rightChild.Operator() == opt.NullOp {
			// TODO(radu): NULLs are tricky and require special handling; we ignore
			// the expression for now.
			return unconstrained, false
		}
	}

	datums := make(tree.Datums, lhs.ChildCount())
	for i := range datums {
		datums[i] = ExtractConstDatum(rhs.Child(i))
	}
	key := constraint.MakeCompositeKey(datums...)

	// less is true if the op is < or <= and false if the op is > or >=.
	// boundary is inclusive if the op is <= or >= and exclusive if the op
	// is < or >.
	var less bool
	var boundary constraint.SpanBoundary

	switch ev.Operator() {
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
		panic(fmt.Sprintf("unsupported op %s", ev.Operator()))
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
	cols := make([]opt.OrderingColumn, lhs.ChildCount())
	for i := range cols {
		cols[i] = opt.MakeOrderingColumn(lhs.Child(i).Private().(opt.ColumnID), false /* descending */)
	}
	keyCtx.Columns.Init(cols)
	span.PreferInclusive(&keyCtx)
	return constraint.SingleSpanConstraint(&keyCtx, &span), true
}

// getConstraints retrieves the constraints already calculated for an
// expression.
func (cb *constraintsBuilder) getConstraints(ev ExprView) (_ *constraint.Set, tight bool) {
	s := ev.Logical().Scalar
	if s.Constraints == nil {
		s.Constraints, s.TightConstraints = cb.buildConstraints(ev)
	}
	return s.Constraints, s.TightConstraints
}

func (cb *constraintsBuilder) buildConstraints(ev ExprView) (_ *constraint.Set, tight bool) {
	switch ev.Operator() {
	case opt.NullOp:
		return contradiction, true

	case opt.VariableOp:
		// (x) is equivalent to (x = TRUE) if x is boolean.
		if col := ev.Private().(opt.ColumnID); cb.md.ColumnType(col).Equivalent(types.Bool) {
			return cb.buildSingleColumnConstraintConst(col, opt.EqOp, tree.DBoolTrue)
		}
		return unconstrained, false

	case opt.NotOp:
		// (NOT x) is equivalent to (x = FALSE) if x is boolean.
		if child := ev.Child(0); child.Operator() == opt.VariableOp {
			if col := child.Private().(opt.ColumnID); cb.md.ColumnType(col).Equivalent(types.Bool) {
				return cb.buildSingleColumnConstraintConst(col, opt.EqOp, tree.DBoolFalse)
			}
		}
		return unconstrained, false

	case opt.AndOp, opt.FiltersOp:
		// ChildCount can be zero if this is not a fully normalized expression
		// (e.g. when using optsteps).
		if ev.ChildCount() > 0 {
			c, tight := cb.getConstraints(ev.Child(0))
			for i, n := 1, ev.ChildCount(); i < n; i++ {
				ci, tighti := cb.getConstraints(ev.Child(i))
				c = c.Intersect(cb.evalCtx, ci)
				tight = tight && tighti
			}
			return c, (tight || c == contradiction)
		}
		return unconstrained, false
	}

	if ev.ChildCount() < 2 {
		return unconstrained, false
	}

	child0, child1 := ev.Child(0), ev.Child(1)
	// Check for an operation where the left-hand side is an
	// indexed var for this column.

	// Check for tuple operations.
	if child0.Operator() == opt.TupleOp && child1.Operator() == opt.TupleOp {
		switch ev.Operator() {
		case opt.LtOp, opt.LeOp, opt.GtOp, opt.GeOp, opt.NeOp:
			// Tuple inequality.
			return cb.buildConstraintForTupleInequality(ev)

		case opt.InOp:
			return cb.buildConstraintForTupleIn(ev)
		}
	}
	if child0.Operator() == opt.VariableOp {
		return cb.buildSingleColumnConstraint(child0.Private().(opt.ColumnID), ev.Operator(), child1)
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

// verifyType checks that the type of column matches the given type. We disallow
// mixed-type comparisons because if they become index constraints, we would
// generate incorrect encodings (#4313).
func (cb *constraintsBuilder) verifyType(col opt.ColumnID, typ types.T) bool {
	return typ == types.Unknown || cb.md.ColumnType(col).Equivalent(typ)
}
