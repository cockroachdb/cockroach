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
	// TODO(radu): handle IN

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

func (cb *constraintsBuilder) buildConstraints(ev ExprView) (_ *constraint.Set, tight bool) {
	// TODO(radu): handle const bool value.

	switch ev.Operator() {
	case opt.AndOp, opt.FiltersOp:
		c, tight := cb.buildConstraints(ev.Child(0))
		for i := 1; i < ev.ChildCount(); i++ {
			ci, tighti := cb.buildConstraints(ev.Child(i))
			c = c.Intersect(cb.evalCtx, ci)
			tight = tight && tighti
		}
		return c, tight
	}

	if ev.ChildCount() < 2 {
		return unconstrained, false
	}

	child0, child1 := ev.Child(0), ev.Child(1)
	// Check for an operation where the left-hand side is an
	// indexed var for this column.
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
