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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// CheckExpr does sanity checking on an Expr. This code is called in testrace
// builds (which gives us test/CI coverage but elides this code in regular
// builds).
// This method does not assume that the expression has been fully normalized.
func (m *Memo) CheckExpr(eid ExprID) {
	expr := m.Expr(eid)

	// Check logical properties.
	logical := m.GroupProperties(eid.Group)
	logical.Verify()

	switch expr.Operator() {
	case opt.ScanOp:
		def := expr.Private(m).(*ScanOpDef)
		if def.Flags.NoIndexJoin && def.Flags.ForceIndex {
			panic("NoIndexJoin and ForceIndex set")
		}

	case opt.ProjectionsOp:
		// Check that we aren't passing through columns in projection expressions.
		n := expr.ChildCount()
		def := expr.Private(m).(*ProjectionsOpDef)
		colList := def.SynthesizedCols
		if len(colList) != n {
			panic(fmt.Sprintf("%d projections but %d columns", n, len(colList)))
		}
		for i := 0; i < n; i++ {
			child := m.NormExpr(expr.ChildGroup(m, i))
			if child.Operator() == opt.VariableOp {
				if child.Private(m).(opt.ColumnID) == colList[i] {
					panic(fmt.Sprintf("projection passes through column %d", colList[i]))
				}
			}
		}

	case opt.AggregationsOp:
		// Check that we don't have any bare variables as aggregations.
		n := expr.ChildCount()
		colList := expr.Private(m).(opt.ColList)
		if len(colList) != n {
			panic(fmt.Sprintf("%d aggregations but %d columns", n, len(colList)))
		}
		for i := 0; i < n; i++ {
			child := m.NormExpr(expr.ChildGroup(m, i))
			if child.Operator() == opt.VariableOp {
				panic("aggregation contains bare variable")
			}
		}

	case opt.DistinctOnOp:
		// Aggregates can be only FirstAgg or ConstAgg.
		agg := m.NormExpr(expr.ChildGroup(m, 1))
		for i, n := 0, agg.ChildCount(); i < n; i++ {
			childOp := m.NormExpr(agg.ChildGroup(m, i)).Operator()
			if childOp != opt.FirstAggOp && childOp != opt.ConstAggOp {
				panic(fmt.Sprintf("distinct-on contains %s", childOp))
			}
		}

	case opt.GroupByOp, opt.ScalarGroupByOp:
		// Aggregates cannot be FirstAgg.
		agg := m.NormExpr(expr.ChildGroup(m, 1))
		for i, n := 0, agg.ChildCount(); i < n; i++ {
			childOp := m.NormExpr(agg.ChildGroup(m, i)).Operator()
			if childOp == opt.FirstAggOp {
				panic(fmt.Sprintf("group-by contains %s", childOp))
			}
		}

	case opt.IndexJoinOp:
		def := expr.Private(m).(*IndexJoinDef)
		if def.Cols.Empty() {
			panic(fmt.Sprintf("index join with no columns"))
		}

	case opt.LookupJoinOp:
		def := expr.Private(m).(*LookupJoinDef)
		inputProps := m.GroupProperties(expr.AsLookupJoin().Input()).Relational
		if len(def.KeyCols) == 0 {
			panic(fmt.Sprintf("lookup join with no key columns"))
		}
		if def.Cols.SubsetOf(inputProps.OutputCols) {
			panic(fmt.Sprintf("lookup join with no lookup columns"))
		}

	case opt.SelectOp:
		filter := m.NormExpr(expr.AsSelect().Filter())
		switch filter.Operator() {
		case opt.FiltersOp, opt.TrueOp, opt.FalseOp:
		default:
			panic(fmt.Sprintf("select contains %s", filter.Operator()))
		}

	default:
		if expr.IsJoin() {
			on := m.NormExpr(expr.ChildGroup(m, 2))
			switch on.Operator() {
			case opt.FiltersOp, opt.TrueOp, opt.FalseOp:
			default:
				panic(fmt.Sprintf("join contains %s", on.Operator()))
			}
		}
	}

	m.checkExprOrdering(expr)
}

// checkExprOrdering runs checks on orderings stored inside operators.
func (m *Memo) checkExprOrdering(expr *Expr) {
	// Verify that orderings stored in operators only refer to columns produced by
	// their input.
	var ordering *props.OrderingChoice
	switch expr.Operator() {
	case opt.LimitOp, opt.OffsetOp:
		ordering = expr.Private(m).(*props.OrderingChoice)
	case opt.RowNumberOp:
		ordering = &expr.Private(m).(*RowNumberDef).Ordering
	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		ordering = &expr.Private(m).(*GroupByDef).Ordering
	default:
		return
	}
	outCols := m.GroupProperties(expr.ChildGroup(m, 0)).Relational.OutputCols
	if !ordering.SubsetOfCols(outCols) {
		panic(fmt.Sprintf(
			"invalid ordering %v (op: %s, outcols: %v)", ordering, expr.Operator(), outCols,
		))
	}
}
