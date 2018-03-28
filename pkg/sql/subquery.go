// Copyright 2015 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// subquery represents a subquery expression in an expression tree
// after it has been converted to a query plan. It is stored in
// planTop.subqueryPlans.
type subquery struct {
	subquery *tree.Subquery
	execMode subqueryExecMode
	expanded bool
	started  bool
	plan     planNode
	result   tree.Datum
}

type subqueryExecMode int

const (
	execModeUnknown subqueryExecMode = iota
	// Subquery is argument to EXISTS. Only 0 or 1 row is expected.
	// Result type is Bool.
	execModeExists
	// Subquery is argument to IN, ANY, SOME, or ALL. Any number of rows
	// expected. Result type is tuple of rows. As a special case, if
	// there is only one column selected, the result is a tuple of the
	// selected values (instead of a tuple of 1-tuples).
	execModeAllRowsNormalized
	// Subquery is argument to an ARRAY constructor. Any number of rows
	// expected, and exactly one column is expected. Result type is tuple
	// of selected values.
	execModeAllRows
	// Subquery is argument to another function. Exactly 1 row
	// expected. Result type is tuple of columns, unless there is
	// exactly 1 column in which case the result type is that column's
	// type.
	execModeOneRow
)

var execModeNames = map[subqueryExecMode]string{
	execModeUnknown:           "<unknown>",
	execModeExists:            "exists",
	execModeAllRowsNormalized: "all rows normalized",
	execModeAllRows:           "all rows",
	execModeOneRow:            "one row",
}

// EvalSubquery is called by `tree.Eval()` method implementations to
// retrieve the Datum result of a subquery.
func (p *planner) EvalSubquery(expr *tree.Subquery) (result tree.Datum, err error) {
	if expr.Idx == 0 {
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: subquery %q was not processed, analyzeSubqueries not called?", expr)
	}
	if expr.Idx < 0 || expr.Idx-1 >= len(p.curPlan.subqueryPlans) {
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: invalid index %d for %q", expr.Idx, expr)
	}

	s := &p.curPlan.subqueryPlans[expr.Idx-1]
	if !s.started {
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: subquery %d (%q) not started prior to evaluation", expr.Idx, expr)
	}
	return s.result, nil
}

func (p *planTop) evalSubqueries(params runParams) error {
	for i := range p.subqueryPlans {
		sq := &p.subqueryPlans[i]
		if sq.started {
			// Already started. Nothing to do.
			continue
		}

		if !sq.expanded {
			return pgerror.NewErrorf(pgerror.CodeInternalError,
				"programming error: subquery %d (%q) was not expanded properly", i+1, sq.subquery)
		}

		if log.V(2) {
			log.Infof(params.ctx, "starting subquery %d (%q)", i+1, sq.subquery)
		}

		if err := startPlan(params, sq.plan); err != nil {
			return err
		}
		sq.started = true
		res, err := sq.doEval(params)
		if err != nil {
			return err
		}
		sq.result = res
	}
	return nil
}

func (s *subquery) doEval(params runParams) (result tree.Datum, err error) {
	// After evaluation, there is no plan remaining.
	defer func() { s.plan.Close(params.ctx); s.plan = nil }()

	switch s.execMode {
	case execModeExists:
		// For EXISTS expressions, all we want to know is if there is at least one
		// result.
		next, err := s.plan.Next(params)
		if err != nil {
			return result, err
		}
		if next {
			result = tree.DBoolTrue
		}
		if result == nil {
			result = tree.DBoolFalse
		}

	case execModeAllRows, execModeAllRowsNormalized:
		var rows tree.DTuple
		next, err := s.plan.Next(params)
		for ; next; next, err = s.plan.Next(params) {
			values := s.plan.Values()
			switch len(values) {
			case 1:
				// This seems hokey, but if we don't do this then the subquery expands
				// to a tuple of tuples instead of a tuple of values and an expression
				// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
				// a single value against a tuple.
				rows.D = append(rows.D, values[0])
			default:
				// The result from plan.Values() is only valid until the next call to
				// plan.Next(), so make a copy.
				valuesCopy := tree.NewDTupleWithLen(len(values))
				copy(valuesCopy.D, values)
				rows.D = append(rows.D, valuesCopy)
			}
		}
		if err != nil {
			return result, err
		}

		if ok, dir := s.subqueryTupleOrdering(); ok {
			if dir == encoding.Descending {
				rows.D.Reverse()
			}
			rows.SetSorted()
		}
		if s.execMode == execModeAllRowsNormalized {
			rows.Normalize(params.EvalContext())
		}
		result = &rows

	case execModeOneRow:
		result = tree.DNull
		hasRow, err := s.plan.Next(params)
		if err != nil {
			return result, err
		}
		if hasRow {
			values := s.plan.Values()
			switch len(values) {
			case 1:
				result = values[0]
			default:
				valuesCopy := tree.NewDTupleWithLen(len(values))
				copy(valuesCopy.D, values)
				result = valuesCopy
			}
			another, err := s.plan.Next(params)
			if err != nil {
				return result, err
			}
			if another {
				return result, fmt.Errorf("more than one row returned by a subquery used as an expression")
			}
		}
	default:
		panic(fmt.Sprintf("unexpected subqueryExecMode: %d", s.execMode))
	}

	return result, nil
}

// subqueryTupleOrdering returns whether the rows of the subquery are ordered
// such that the resulting subquery tuple can be considered fully sorted.
// For this to happen, the columns in the subquery must be sorted in the same
// direction and with the same order of precedence that the tuple will have. The
// method will return a boolean specifying whether the result is in sorted order,
// and if so, will specify which direction it is sorted in.
//
// TODO(knz): This will not work for subquery renders that are not row dependent
// like
//   SELECT 1 IN (SELECT 1 ORDER BY 1)
// because even if they are included in an ORDER BY clause, they will not be part
// of the plan.Ordering().
func (s *subquery) subqueryTupleOrdering() (bool, encoding.Direction) {
	// Columns must be sorted in the order that they appear in the render
	// and which they will later appear in the resulting tuple.
	desired := make(sqlbase.ColumnOrdering, len(planColumns(s.plan)))
	for i := range desired {
		desired[i] = sqlbase.ColumnOrderInfo{
			ColIdx:    i,
			Direction: encoding.Ascending,
		}
	}

	// Check Ascending direction.
	order := planPhysicalProps(s.plan)
	match := order.computeMatch(desired)
	if match == len(desired) {
		return true, encoding.Ascending
	}
	// Check Descending direction.
	match = order.reverse().computeMatch(desired)
	if match == len(desired) {
		return true, encoding.Descending
	}
	return false, 0
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
			result, err := v.extractSubquery(sub, true /* multi-row */, 1 /* desired-columns */)
			if err != nil {
				v.err = err
				return false, expr
			}
			result.execMode = execModeAllRows
			// Multi-row types are always wrapped in a tuple-type, but the ARRAY
			// flatten operator wants the unwrapped type.
			sub.SetType(sub.ResolvedType().(types.TTuple)[0])
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
			result.execMode = execModeExists
			t.SetType(types.Bool)
		} else {
			result.execMode = execModeOneRow
		}

	case *tree.ComparisonExpr:
		switch t.Operator {
		case tree.In, tree.NotIn, tree.Any, tree.Some, tree.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				result, err := v.extractSubquery(sub, true /* multi-row */, -1 /* desired-columns */)
				if err != nil {
					v.err = err
					return false, expr
				}
				result.execMode = execModeAllRowsNormalized
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
			return nil, fmt.Errorf("subquery must return only one column, found %d", len(cols))
		default:
			plan.Close(v.ctx)
			return nil, fmt.Errorf("subquery must return %d columns, found %d", desiredColumns, len(cols))
		}
	}

	v.curPlan.subqueryPlans = append(v.curPlan.subqueryPlans, subquery{subquery: sub, plan: plan})
	sub.Idx = len(v.curPlan.subqueryPlans) // Note: subquery node are 1-indexed, so that 0 remains invalid.
	result := &v.curPlan.subqueryPlans[sub.Idx-1]

	if log.V(2) {
		log.Infof(v.ctx, "collected subquery: %q -> %d", sub, sub.Idx)
	}

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
		colTypes := make(types.TTuple, len(cols))
		for i, col := range cols {
			colTypes[i] = col.Typ
		}
		sub.SetType(colTypes)
	}

	if multiRow {
		// The subquery is in a multi-row context. For example:
		//
		//   SELECT 1 IN (SELECT * FROM t)
		//
		// Wrap the type in a tuple.
		//
		// TODO(peter): Using a tuple type to represent a multi-row subquery works
		// with the current type checking code, but seems semantically incorrect. A
		// tuple represents a fixed number of elements. Instead, we should either
		// be using the table type (TTable) or introduce a new vtuple type.
		sub.SetType(types.TTuple{sub.ResolvedType()})
	}

	return result, nil
}
