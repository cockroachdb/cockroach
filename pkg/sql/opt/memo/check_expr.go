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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// CheckExpr does sanity checking on an Expr. This code is called in testrace
// builds (which gives us test/CI coverage but elides this code in regular
// builds).
//
// This function does not assume that the expression has been fully normalized.
func (m *Memo) checkExpr(e opt.Expr) {
	// RaceEnabled ensures that checks are run on every PR (as part of make
	// testrace) while keeping the check code out of non-test builds.
	if !util.RaceEnabled {
		return
	}

	// Check properties.
	switch t := e.(type) {
	case RelExpr:
		t.Relational().Verify()

		// If the expression was added to an existing group, cross-check its
		// properties against the properties of the group. Skip this check if the
		// operator is known to not have code for building logical props.
		if t != t.FirstExpr() && t.Op() != opt.MergeJoinOp {
			var relProps props.Relational
			// Don't build stats when verifying logical props - unintentionally
			// building stats for non-normalized expressions could add extra colStats
			// to the output in opt_tester in cases where checkExpr runs (i.e. testrace)
			// compared to cases where it doesn't.
			m.logPropsBuilder.disableStats = true
			m.logPropsBuilder.buildProps(t, &relProps)
			m.logPropsBuilder.disableStats = false
			t.Relational().VerifyAgainst(&relProps)
		}

	case ScalarPropsExpr:
		t.ScalarProps(m).Verify()
	}

	// Check operator-specific fields.
	switch t := e.(type) {
	case *ScanExpr:
		if t.Flags.NoIndexJoin && t.Flags.ForceIndex {
			panic("NoIndexJoin and ForceIndex set")
		}

	case *ProjectExpr:
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
			if v, ok := item.Element.(*VariableExpr); ok {
				if v.Col == item.Col {
					panic(fmt.Sprintf("projection passes through column %d", item.Col))
				}
			}
		}

	case *SelectExpr:
		checkFilters(t.Filters)

	case *AggregationsExpr:
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

	case *DistinctOnExpr:
		// Check that aggregates can be only FirstAgg or ConstAgg.
		for _, item := range t.Aggregations {
			switch item.Agg.Op() {
			case opt.FirstAggOp, opt.ConstAggOp:

			default:
				panic(fmt.Sprintf("distinct-on contains %s", item.Agg.Op()))
			}
		}

	case *GroupByExpr, *ScalarGroupByExpr:
		// Check that aggregates cannot be FirstAgg.
		for _, item := range *t.Child(1).(*AggregationsExpr) {
			switch item.Agg.Op() {
			case opt.FirstAggOp:
				panic(fmt.Sprintf("group-by contains %s", item.Agg.Op()))
			}
		}

	case *IndexJoinExpr:
		if t.Cols.Empty() {
			panic(fmt.Sprintf("index join with no columns"))
		}

	case *LookupJoinExpr:
		if len(t.KeyCols) == 0 {
			panic(fmt.Sprintf("lookup join with no key columns"))
		}
		if t.Cols.Empty() {
			panic(fmt.Sprintf("lookup join with no output columns"))
		}
		if t.Cols.SubsetOf(t.Input.Relational().OutputCols) {
			panic(fmt.Sprintf("lookup join with no lookup columns"))
		}

	case *ZigzagJoinExpr:
		if len(t.LeftEqCols) != len(t.RightEqCols) {
			panic(fmt.Sprintf("zigzag join with mismatching eq columns"))
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
			checkFilters(*e.Child(2).(*FiltersExpr))
		}
	}

	// Check orderings within operators.
	checkExprOrdering(e)
}

// checkExprOrdering runs checks on orderings stored inside operators.
func checkExprOrdering(e opt.Expr) {
	// Verify that orderings stored in operators only refer to columns produced by
	// their input.
	var ordering physical.OrderingChoice
	switch t := e.Private().(type) {
	case *physical.OrderingChoice:
		ordering = *t
	case *RowNumberPrivate:
		ordering = t.Ordering
	case GroupingPrivate:
		ordering = t.Ordering
	default:
		return
	}
	if outCols := e.(RelExpr).Relational().OutputCols; !ordering.SubsetOfCols(outCols) {
		panic(fmt.Sprintf("invalid ordering %v (op: %s, outcols: %v)", ordering, e.Op(), outCols))
	}
}

func checkFilters(filters FiltersExpr) {
	for _, item := range filters {
		if opt.IsListItemOp(item.Condition) {
			panic("filters list item cannot contain another list item")
		}
	}
}
