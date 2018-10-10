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
func (f *Factory) checkExpr(e opt.Expr) {
	// Check properties.
	switch t := e.(type) {
	case memo.RelExpr:
		t.Relational().Verify()

	case memo.ScalarPropsExpr:
		t.ScalarProps(f.Memo()).Verify()
	}

	switch t := e.(type) {
	case *memo.ScanExpr:
		if t.Flags.NoIndexJoin && t.Flags.ForceIndex {
			panic("NoIndexJoin and ForceIndex set")
		}

	case *memo.ProjectExpr:
		for _, item := range t.Projections {
			// Check that list items are not nested.
			if opt.IsListItemOp(item.Element) {
				panic("projections list item cannot contain another list item")
			}

			// Check that column id is set.
			if item.Col == 0 {
				panic("projections column cannot have id of 0")
			}

			// Check that column is not both passthrough and synthesized.
			if t.Passthrough.Contains(int(item.Col)) {
				panic(fmt.Sprintf("both passthrough and synthesized have column %d", item.Col))
			}

			// Check that columns aren't passed through in projection expressions.
			if v, ok := item.Element.(*memo.VariableExpr); ok {
				if v.Col == item.Col {
					panic(fmt.Sprintf("projection passes through column %d", item.Col))
				}
			}
		}

	case *memo.SelectExpr:
		f.checkFilters(t.Filters)

	case *memo.AggregationsExpr:
		var checkAggs func(scalar opt.ScalarExpr)
		checkAggs = func(scalar opt.ScalarExpr) {
			switch scalar.Op() {
			case opt.AggDistinctOp:
				checkAggs(scalar.Child(0).(opt.ScalarExpr))

			case opt.VariableOp:

			default:
				if !opt.IsAggregateOp(scalar) {
					panic(fmt.Sprintf("aggregate contains illegal op: %s", scalar.Op()))
				}
			}
		}
		for _, item := range *t {
			// Check that aggregations only contain aggregates and variables.
			checkAggs(item.Agg)

			// Check that column id is set.
			if item.Col == 0 {
				panic("aggregations column cannot have id of 0")
			}

			// Check that we don't have any bare variables as aggregations.
			if item.Agg.Op() == opt.VariableOp {
				panic("aggregation contains bare variable")
			}
		}

	case *memo.DistinctOnExpr:
		// Check that aggregates can be only FirstAgg or ConstAgg.
		for _, item := range t.Aggregations {
			switch item.Agg.Op() {
			case opt.FirstAggOp, opt.ConstAggOp:

			default:
				panic(fmt.Sprintf("distinct-on contains %s", item.Agg.Op()))
			}
		}

	case *memo.GroupByExpr, *memo.ScalarGroupByExpr:
		// Check that aggregates cannot be FirstAgg.
		for _, item := range *t.Child(1).(*memo.AggregationsExpr) {
			switch item.Agg.Op() {
			case opt.FirstAggOp:
				panic(fmt.Sprintf("group-by contains %s", item.Agg.Op()))
			}
		}

	case *memo.IndexJoinExpr:
		if t.Cols.Empty() {
			panic(fmt.Sprintf("index join with no columns"))
		}

	case *memo.LookupJoinExpr:
		if len(t.KeyCols) == 0 {
			panic(fmt.Sprintf("lookup join with no key columns"))
		}
		if t.LookupCols.Empty() {
			panic(fmt.Sprintf("lookup join with no lookup columns"))
		}

	default:
		if !opt.IsListOp(e) {
			for i := 0; i < e.ChildCount(); i++ {
				child := e.Child(i)
				if opt.IsListItemOp(child) {
					panic(fmt.Sprintf("non-list op contains item op: %s", child.Op()))
				}
			}
		}

		if opt.IsJoinOp(e) {
			f.checkFilters(*e.Child(2).(*memo.FiltersExpr))
		}
	}

	f.checkExprOrdering(e)
}

// checkExprOrdering runs checks on orderings stored inside operators.
func (f *Factory) checkExprOrdering(e opt.Expr) {
	// Verify that orderings stored in operators only refer to columns produced by
	// their input.
	var ordering props.OrderingChoice
	switch t := e.Private().(type) {
	case *props.OrderingChoice:
		ordering = *t
	case *memo.RowNumberPrivate:
		ordering = t.Ordering
	case memo.GroupingPrivate:
		ordering = t.Ordering
	default:
		return
	}
	if outCols := e.(memo.RelExpr).Relational().OutputCols; !ordering.SubsetOfCols(outCols) {
		panic(fmt.Sprintf("invalid ordering %v (op: %s, outcols: %v)", ordering, e.Op(), outCols))
	}
}

func (f *Factory) checkFilters(filters memo.FiltersExpr) {
	for _, item := range filters {
		if opt.IsListItemOp(item.Condition) {
			panic("filters list item cannot contain another list item")
		}
	}
}
