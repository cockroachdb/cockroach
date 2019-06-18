// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// subquery represents a subquery expression in an expression tree
// after it has been converted to a query plan. It is stored in
// planTop.subqueryPlans.
type subquery struct {
	subquery *tree.Subquery
	execMode distsqlrun.SubqueryExecMode
	expanded bool
	started  bool
	plan     planNode
	result   tree.Datum
}

// EvalSubquery is called by `tree.Eval()` method implementations to
// retrieve the Datum result of a subquery.
func (p *planner) EvalSubquery(expr *tree.Subquery) (result tree.Datum, err error) {
	if expr.Idx == 0 {
		return nil, errors.AssertionFailedf("subquery %q was not processed, analyzeSubqueries not called?", expr)
	}
	if expr.Idx < 0 || expr.Idx-1 >= len(p.curPlan.subqueryPlans) {
		return nil, errors.AssertionFailedf("invalid index %d for %q", expr.Idx, expr)
	}

	s := &p.curPlan.subqueryPlans[expr.Idx-1]
	if !s.started {
		return nil, errors.AssertionFailedf("subquery %d (%q) not started prior to evaluation", expr.Idx, expr)
	}
	return s.result, nil
}

// analyzeSubqueries finds tree.Subquery syntax nodes; for each one, it builds
// an initial plan, adds an entry in planTop.subqueryPlans, and annotates the
// Subquery node with a type and a link (Idx) to that entry.
func (p *planner) analyzeSubqueries(ctx context.Context, expr tree.Expr, columns int) error {
	p.subqueryVisitor = subqueryVisitor{planner: p, columns: columns, ctx: ctx}
	tree.WalkExprConst(&p.subqueryVisitor, expr)
	return p.subqueryVisitor.err
}

// subqueryVisitor is used to implement analyzeSubqueries.
type subqueryVisitor struct {
	*planner
	columns int
	err     error

	// TODO(andrei): plumb the context through the tree.Visitor.
	ctx context.Context
}

var _ tree.Visitor = &subqueryVisitor{}

// subqueryAlreadyAnalyzed returns true iff VisitPre already has
// called extractSubquery on the given subquery node. The condition
// `t.Idx > 0` is not sufficient because the AST may be reused more
// than once (AST caching between PREPARE and EXECUTE). In between
// uses, the Idx and Typ fields are preserved but the current plan's
// subquery slice is not.
func (v *subqueryVisitor) subqueryAlreadyAnalyzed(t *tree.Subquery) bool {
	return t.Idx > 0 && t.Idx-1 < len(v.curPlan.subqueryPlans) && v.curPlan.subqueryPlans[t.Idx-1].plan != nil
}

func (v *subqueryVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	// TODO(knz): if/when type checking can be extracted from the newPlan
	// recursion, we can propagate the desired type to the subquery. For now, the
	// type is simply derived during the subquery node creation by looking at the
	// result column types.

	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.ArrayFlatten:
		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			if v.subqueryAlreadyAnalyzed(sub) {
				// Subquery was already processed. Nothing to do.
				return false, expr
			}

			result, err := v.extractSubquery(sub, true /* multi-row */, 1 /* desired-columns */)
			if err != nil {
				v.err = err
				return false, expr
			}
			result.execMode = distsqlrun.SubqueryExecModeAllRows
			// Multi-row types are always wrapped in a tuple-type, but the ARRAY
			// flatten operator wants the unwrapped type.
			sub.SetType(&sub.ResolvedType().TupleContents()[0])
		}

	case *tree.Subquery:
		if v.subqueryAlreadyAnalyzed(t) {
			// Subquery was already processed. Nothing to do.
			return false, expr
		}

		multiRow := false
		desiredColumns := v.columns
		if t.Exists {
			multiRow = true
			desiredColumns = -1
		}
		result, err := v.extractSubquery(t, multiRow, desiredColumns)
		if err != nil {
			v.err = err
			return false, expr
		}
		if t.Exists {
			result.plan = &limitNode{plan: result.plan, countExpr: tree.NewDInt(1)}
			result.execMode = distsqlrun.SubqueryExecModeExists
			t.SetType(types.Bool)
		} else {
			result.plan = &max1RowNode{
				plan: &limitNode{
					plan:      result.plan,
					countExpr: tree.NewDInt(2)}}
			result.execMode = distsqlrun.SubqueryExecModeOneRow
		}

	case *tree.ComparisonExpr:
		switch t.Operator {
		case tree.In, tree.NotIn, tree.Any, tree.Some, tree.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				if v.subqueryAlreadyAnalyzed(sub) {
					// Subquery was already processed. Nothing to do.
					return false, expr
				}

				result, err := v.extractSubquery(sub, true /* multi-row */, -1 /* desired-columns */)
				if err != nil {
					v.err = err
					return false, expr
				}
				result.execMode = distsqlrun.SubqueryExecModeAllRowsNormalized
			}

			// Note that we recurse into the comparison expression and a subquery in
			// the left-hand side is handled by the *tree.Subquery case above.
		}
	}

	// If the subquery is a child of any other expression, type checking will
	// verify the number of columns.
	v.columns = -1

	return true, expr
}

func (v *subqueryVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

// extractSubquery extracts the subquery's AST into the top-level
// curPlan.subqueryPlans slice, creates the logical plan for it there,
// then populates the tree.Subquery node with the index into the
// top-level slice.
func (v *subqueryVisitor) extractSubquery(
	sub *tree.Subquery, multiRow bool, desiredColumns int,
) (*subquery, error) {
	// Calling newPlan() might recursively invoke replaceSubqueries, so we need
	// to preserve the state of the visitor across the call to newPlan().
	visitorCopy := v.planner.subqueryVisitor
	plan, err := v.planner.newPlan(v.ctx, sub.Select, nil /* desiredTypes */)
	v.planner.subqueryVisitor = visitorCopy
	if err != nil {
		return nil, err
	}

	cols := planColumns(plan)
	if desiredColumns > 0 && len(cols) != desiredColumns {
		switch desiredColumns {
		case 1:
			plan.Close(v.ctx)
			return nil, pgerror.Newf(pgcode.Syntax,
				"subquery must return only one column, found %d", len(cols))
		default:
			plan.Close(v.ctx)
			return nil, pgerror.Newf(pgcode.Syntax,
				"subquery must return %d columns, found %d", desiredColumns, len(cols))
		}
	}

	v.curPlan.subqueryPlans = append(v.curPlan.subqueryPlans, subquery{subquery: sub, plan: plan})
	sub.Idx = len(v.curPlan.subqueryPlans) // Note: subquery node are 1-indexed, so that 0 remains invalid.
	result := &v.curPlan.subqueryPlans[sub.Idx-1]

	if log.V(2) {
		log.Infof(v.ctx, "collected subquery: %q -> %d", sub, sub.Idx)
	}

	telemetry.Inc(sqltelemetry.SubqueryUseCounter)

	// The typing for subqueries is complex, but regular.
	//
	// * If the subquery is used in a single-row context:
	//
	//   - If the subquery returns a single column with type "U", the type of the
	//     subquery is the type of the column "U". For example:
	//
	//       SELECT 1 = (SELECT 1)
	//
	//     The type of the subquery is "int".
	//
	//   - If the subquery returns multiple columns, the type of the subquery is
	//     "tuple{C}" where "C" expands to all of the types of the columns of the
	//     subquery. For example:
	//
	//       SELECT (1, 'a') = (SELECT 1, 'a')
	//
	//     The type of the subquery is "tuple{int,string}"
	//
	// * If the subquery is used in a multi-row context:
	//
	//   - If the subquery returns a single column with type "U", the type of the
	//     subquery is the singleton tuple of type "U": "tuple{U}". For example:
	//
	//       SELECT 1 IN (SELECT 1)
	//
	//     The type of the subquery's columns is "int" and the type of the
	//     subquery is "tuple{int}".
	//
	//   - If the subquery returns multiple columns, the type of the subquery is
	//     "tuple{tuple{C}}" where "C expands to all of the types of the columns
	//     of the subquery. For example:
	//
	//       SELECT (1, 'a') IN (SELECT 1, 'a')
	//
	//     The types of the subquery's columns are "int" and "string". These are
	//     wrapped into "tuple{int,string}" to form the row type. And these are
	//     wrapped again to form the subquery type "tuple{tuple{int,string}}".
	//
	// Note that these rules produce a somewhat surprising equivalence:
	//
	//   SELECT (SELECT 1, 2) = (SELECT (1, 2))
	//
	// A subquery which returns a single column tuple is equivalent to a subquery
	// which returns the elements of the tuple as individual columns. While
	// surprising, this is necessary for regularity and in order to handle:
	//
	//   SELECT 1 IN (SELECT 1)
	//
	// Without that auto-unwrapping of single-column subqueries, this query would
	// type check as "<int> IN <tuple{tuple{int}}>" which would fail.

	if len(cols) == 1 {
		sub.SetType(cols[0].Typ)
	} else {
		contents := make([]types.T, len(cols))
		labels := make([]string, len(cols))
		for i, col := range cols {
			contents[i] = *col.Typ
			labels[i] = col.Name
		}
		sub.SetType(types.MakeLabeledTuple(contents, labels))
	}

	if multiRow {
		// The subquery is in a multi-row context. For example:
		//
		//   SELECT 1 IN (SELECT * FROM t)
		//
		// Wrap the type in a tuple.
		//
		// TODO(peter): Using a tuple type to represent a multi-row
		// subquery works with the current type checking code, but seems
		// semantically incorrect. A tuple represents a fixed number of
		// elements. Instead, we should introduce a new vtuple type.
		sub.SetType(types.MakeTuple([]types.T{*sub.ResolvedType()}))
	}

	return result, nil
}
