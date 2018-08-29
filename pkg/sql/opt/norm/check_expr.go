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

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// checkExpr does sanity checking on an Expr. This code is called from
// onConstruct in testrace builds (which gives us test/CI coverage but elides
// this code in regular builds).
func (f *Factory) checkExpr(ev memo.ExprView) {
	// Check logical properties.
	ev.Logical().Verify()

	switch ev.Operator() {
	case opt.ScanOp:
		def := ev.Private().(*memo.ScanOpDef)
		if def.Flags.NoIndexJoin && def.Flags.ForceIndex {
			panic("NoIndexJoin and ForceIndex set")
		}

	case opt.ProjectionsOp:
		// Check that we aren't passing through columns in projection expressions.
		n := ev.ChildCount()
		def := ev.Private().(*memo.ProjectionsOpDef)
		colList := def.SynthesizedCols
		if len(colList) != n {
			panic(fmt.Sprintf("%d projections but %d columns", n, len(colList)))
		}
		for i := 0; i < n; i++ {
			if child := ev.Child(i); child.Operator() == opt.VariableOp {
				if child.Private().(opt.ColumnID) == colList[i] {
					panic(fmt.Sprintf("projection passes through column %d", colList[i]))
				}
			}
		}

	case opt.AggregationsOp:
		// Check that we don't have any bare variables as aggregations.
		n := ev.ChildCount()
		colList := ev.Private().(opt.ColList)
		if len(colList) != n {
			panic(fmt.Sprintf("%d aggregations but %d columns", n, len(colList)))
		}
		for i := 0; i < n; i++ {
			if child := ev.Child(i); child.Operator() == opt.VariableOp {
				panic("aggregation contains bare variable")
			}
		}

	case opt.DistinctOnOp:
		// Aggregates can be only FirstAgg or ConstAgg.
		agg := ev.Child(1)
		for i, n := 0, agg.ChildCount(); i < n; i++ {
			if childOp := agg.Child(i).Operator(); childOp != opt.FirstAggOp && childOp != opt.ConstAggOp {
				panic(fmt.Sprintf("distinct-on contains %s", childOp))
			}
		}

	case opt.GroupByOp, opt.ScalarGroupByOp:
		// Aggregates cannot be FirstAgg.
		agg := ev.Child(1)
		for i, n := 0, agg.ChildCount(); i < n; i++ {
			if childOp := agg.Child(i).Operator(); childOp == opt.FirstAggOp {
				panic(fmt.Sprintf("group-by contains %s", childOp))
			}
		}

	case opt.IndexJoinOp:
		def := ev.Private().(*memo.IndexJoinDef)
		if def.Cols.Empty() {
			panic(fmt.Sprintf("index join with no columns"))
		}

	case opt.LookupJoinOp:
		def := ev.Private().(*memo.LookupJoinDef)
		if len(def.KeyCols) == 0 {
			panic(fmt.Sprintf("lookup join with no key columns"))
		}
		if def.LookupCols.Empty() {
			panic(fmt.Sprintf("lookup join with no lookup columns"))
		}

	case opt.SelectOp:
		filter := ev.Child(1)
		switch filter.Operator() {
		case opt.FiltersOp:
		default:
			panic(fmt.Sprintf("select contains %s", filter.Operator()))
		}

	default:
		if ev.IsJoin() {
			on := ev.Child(2)
			switch on.Operator() {
			case opt.FiltersOp, opt.TrueOp, opt.FalseOp:
			default:
				panic(fmt.Sprintf("join contains %s", on.Operator()))
			}
		}
	}

	f.checkExprOrdering(ev)
}

// checkExprOrdering runs checks on orderings stored inside operators.
func (f *Factory) checkExprOrdering(ev memo.ExprView) {
	// Verify that orderings stored in operators only refer to columns produced by
	// their input.
	var ordering *props.OrderingChoice
	switch ev.Operator() {
	case opt.LimitOp, opt.OffsetOp:
		ordering = ev.Private().(*props.OrderingChoice)
	case opt.RowNumberOp:
		ordering = &ev.Private().(*memo.RowNumberDef).Ordering
	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		ordering = &ev.Private().(*memo.GroupByDef).Ordering
	default:
		return
	}
	if outCols := ev.Child(0).Logical().Relational.OutputCols; !ordering.SubsetOfCols(outCols) {
		panic(fmt.Sprintf("invalid ordering %v (op: %s, outcols: %v)", ordering, ev.Operator(), outCols))
	}
}
