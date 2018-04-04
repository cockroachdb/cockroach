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

	if val.IsConstValue() {
		return cb.buildSingleColumnConstraintConst(col, op, ExtractConstDatum(val))
	}

	// TODO(radu): try to deduce a not-NULL constraint.

	return unconstrained, false
}

func (cb *constraintsBuilder) buildSingleColumnConstraintConst(
	col opt.ColumnID, op opt.Operator, datum tree.Datum,
) (_ *constraint.Set, tight bool) {
	if !cb.verifyType(col, datum.ResolvedType()) {
		return unconstrained, false
	}
	if datum == tree.DNull {
		// TODO(radu): handle NULL datum
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

		// TODO(radu): handle other ops.
	}
	return unconstrained, false
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

func (cb *constraintsBuilder) buildConstraints(ev ExprView) (_ *constraint.Set, tight bool) {
	// TODO(radu): handle const bool value.

	switch ev.Operator() {
	case opt.AndOp, opt.FiltersOp:
		// ChildCount can be zero if this is not a fully normalized expression
		// (e.g. when using optsteps).
		if ev.ChildCount() > 0 {
			c, tight := cb.buildConstraints(ev.Child(0))
			for i := 1; i < ev.ChildCount(); i++ {
				ci, tighti := cb.buildConstraints(ev.Child(i))
				c = c.Intersect(cb.evalCtx, ci)
				tight = tight && tighti
			}
			return c, tight
		}
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

			//TODO(radu): case opt.InOp:
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
