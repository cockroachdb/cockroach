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
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// subquery represents a subquery expression in an expression tree
// after it has been converted to a query plan. It is carried
// in the expression tree from the point type checking occurs to
// the point the query starts execution / evaluation.
type subquery struct {
	typ      types.T
	subquery *tree.Subquery
	execMode subqueryExecMode
	expanded bool
	started  bool
	plan     planNode
	result   tree.Datum
}

type subqueryExecMode int

const (
	// Subquery is argument to EXISTS. Only 0 or 1 row is expected.
	// Result type is Bool.
	execModeExists subqueryExecMode = iota
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

var _ tree.TypedExpr = &subquery{}
var _ tree.VariableExpr = &subquery{}

func (s *subquery) Format(buf *bytes.Buffer, f tree.FmtFlags) {
	if s.execMode == execModeExists {
		buf.WriteString("EXISTS ")
	}
	// Note: we remove the flag to print types, because subqueries can
	// be printed in a context where type resolution has not occurred
	// yet. We do not use FmtSimple directly however, in case the
	// caller wants to use other flags (e.g. to anonymize identifiers).
	tree.FormatNode(buf, tree.StripTypeFormatting(f), s.subquery)
}

func (s *subquery) String() string { return tree.AsString(s) }

func (s *subquery) Walk(v tree.Visitor) tree.Expr {
	return s
}

func (s *subquery) Variable() {}

func (s *subquery) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	// TODO(knz): if/when type checking can be extracted from the newPlan
	// recursion, we can propagate the desired type to the subquery. For now, the
	// type is simply derived during the subquery node creation by looking at the
	// result column types.

	// TODO(nvanbenschoten): Type checking for the comparison operator(s)
	// should take this new node into account. In particular it should
	// check that the tuple types match pairwise.
	return s, nil
}

func (s *subquery) ResolvedType() types.T { return s.typ }

func (s *subquery) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	if s.result == nil {
		panic("subquery was not pre-evaluated properly")
	}
	return s.result, nil
}

func (s *subquery) doEval(ctx context.Context, p *planner) (result tree.Datum, err error) {
	// After evaluation, there is no plan remaining.
	defer func() { s.plan.Close(ctx); s.plan = nil }()

	params := runParams{
		ctx:     ctx,
		evalCtx: &p.evalCtx,
		p:       p,
	}
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
			rows.Normalize(&p.evalCtx)
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

// subquerySpanCollector is responsible for collecting all read spans that
// subqueries in a query plan may touch. Subqueries should never be performing
// any write operations, so only the read spans are collected.
type subquerySpanCollector struct {
	params runParams
	reads  roachpb.Spans
}

func (v *subquerySpanCollector) subqueryNode(ctx context.Context, sq *subquery) error {
	reads, writes, err := collectSpans(v.params, sq.plan)
	if err != nil {
		return err
	}
	if len(writes) > 0 {
		return errors.Errorf("unexpected span writes in subquery: %v", writes)
	}
	v.reads = append(v.reads, reads...)
	return nil
}

func collectSubquerySpans(params runParams, plan planNode) (roachpb.Spans, error) {
	v := subquerySpanCollector{params: params}
	po := planObserver{subqueryNode: v.subqueryNode}
	if err := walkPlan(params.ctx, plan, po); err != nil {
		return nil, err
	}
	return v.reads, nil
}

// subqueryVisitor replaces tree.Subquery syntax nodes by a
// sql.subquery node and an initial query plan for running the
// subquery.
type subqueryVisitor struct {
	*planner
	columns int
	err     error

	// TODO(andrei): plumb the context through the tree.Visitor.
	ctx context.Context
}

var _ tree.Visitor = &subqueryVisitor{}

func (v *subqueryVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	if _, ok := expr.(*subquery); ok {
		// We already replaced this one; do nothing.
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.ArrayFlatten:
		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			result, err := v.replaceSubquery(sub, true /* multi-row */, 1 /* desired-columns */)
			if err != nil {
				v.err = err
				return false, expr
			}
			result.execMode = execModeAllRows
			// Multi-row types are always wrapped in a tuple-type, but the ARRAY
			// flatten operator wants the unwrapped type.
			result.typ = result.typ.(types.TTuple)[0]
			t.Subquery = result
		}

	case *tree.ExistsExpr:
		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			result, err := v.replaceSubquery(sub, true /* multi-row */, -1 /* desired-columns */)
			if err != nil {
				v.err = err
				return false, expr
			}
			result.execMode = execModeExists
			result.typ = types.Bool
			expr = result
		}

	case *tree.Subquery:
		result, err := v.replaceSubquery(t, false /* multi-row */, v.columns /* desired-columns */)
		if err != nil {
			v.err = err
			return false, expr
		}
		result.execMode = execModeOneRow
		expr = result

	case *tree.ComparisonExpr:
		switch t.Operator {
		case tree.In, tree.NotIn, tree.Any, tree.Some, tree.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				result, err := v.replaceSubquery(sub, true /* multi-row */, -1 /* desired-columns */)
				if err != nil {
					v.err = err
					return false, expr
				}
				result.execMode = execModeAllRowsNormalized
				t.Right = result
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

func (v *subqueryVisitor) replaceSubquery(
	sub *tree.Subquery, multiRow bool, desiredColumns int,
) (*subquery, error) {
	v.hasSubqueries = true

	// Calling newPlan() might recursively invoke replaceSubqueries, so we need
	// to preserve the state of the visitor across the call to newPlan().
	visitorCopy := v.planner.subqueryVisitor
	plan, err := v.planner.newPlan(v.ctx, sub.Select, nil)
	v.planner.subqueryVisitor = visitorCopy
	if err != nil {
		return nil, err
	}

	cols := planColumns(plan)
	if desiredColumns > 0 && len(cols) != desiredColumns {
		switch desiredColumns {
		case 1:
			return nil, fmt.Errorf("subquery must return only one column, found %d", len(cols))
		default:
			return nil, fmt.Errorf("subquery must return %d columns, found %d", desiredColumns, len(cols))
		}
	}

	result := &subquery{subquery: sub, plan: plan}

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
		result.typ = cols[0].Typ
	} else {
		colTypes := make(types.TTuple, len(cols))
		for i, col := range cols {
			colTypes[i] = col.Typ
		}
		result.typ = colTypes
	}

	if multiRow {
		// The subquery is in a multi-row context. For example:
		//
		//   SELECT 1 IN (SELECT * FROM t)
		//
		// Wrap the type in a tuple.
		result.typ = types.TTuple{result.typ}
	}

	return result, nil
}

func (p *planner) replaceSubqueries(
	ctx context.Context, expr tree.Expr, columns int,
) (tree.Expr, error) {
	p.subqueryVisitor = subqueryVisitor{planner: p, columns: columns, ctx: ctx}
	expr, _ = tree.WalkExpr(&p.subqueryVisitor, expr)
	return expr, p.subqueryVisitor.err
}
