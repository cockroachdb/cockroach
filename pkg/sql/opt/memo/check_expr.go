// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build race

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CheckExpr does sanity checking on an Expr. This code is called in testrace
// builds (which gives us test/CI coverage but elides this code in regular
// builds).
//
// This function does not assume that the expression has been fully normalized.
//
// This function is only defined in race builds, so that checks are run on every
// PR (as part of make testrace) while keeping the check code out of non-test
// builds, since it can be expensive to run.
func (m *Memo) CheckExpr(e opt.Expr) {
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
		t.ScalarProps().Verify()

		// Check that list items are not nested.
		if opt.IsListItemOp(t.Child(0)) {
			panic(errors.AssertionFailedf("projections list item cannot contain another list item"))
		}
	}

	if !opt.IsListOp(e) {
		for i := 0; i < e.ChildCount(); i++ {
			child := e.Child(i)
			if opt.IsListItemOp(child) {
				panic(errors.AssertionFailedf("non-list op contains item op: %s", log.Safe(child.Op())))
			}
		}
	}

	// Check operator-specific fields.
	switch t := e.(type) {
	case *ScanExpr:
		if t.Flags.NoIndexJoin && t.Flags.ForceIndex {
			panic(errors.AssertionFailedf("NoIndexJoin and ForceIndex set"))
		}

	case *ProjectExpr:
		for _, item := range t.Projections {
			// Check that column id is set.
			if item.Col == 0 {
				panic(errors.AssertionFailedf("projections column cannot have id of 0"))
			}

			// Check that column is not both passthrough and synthesized.
			if t.Passthrough.Contains(item.Col) {
				panic(errors.AssertionFailedf(
					"both passthrough and synthesized have column %d", log.Safe(item.Col)))
			}

			// Check that columns aren't passed through in projection expressions.
			if v, ok := item.Element.(*VariableExpr); ok {
				if v.Col == item.Col {
					panic(errors.AssertionFailedf("projection passes through column %d", log.Safe(item.Col)))
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
					panic(errors.AssertionFailedf("aggregate contains illegal op: %s", log.Safe(scalar.Op())))
				}
			}
		}
		for _, item := range *t {
			// Check that aggregations only contain aggregates and variables.
			checkAggs(item.Agg)

			// Check that column id is set.
			if item.Col == 0 {
				panic(errors.AssertionFailedf("aggregations column cannot have id of 0"))
			}

			// Check that we don't have any bare variables as aggregations.
			if item.Agg.Op() == opt.VariableOp {
				panic(errors.AssertionFailedf("aggregation contains bare variable"))
			}
		}

	case *DistinctOnExpr, *EnsureDistinctOnExpr, *UpsertDistinctOnExpr, *EnsureUpsertDistinctOnExpr:
		checkErrorOnDup(e.(RelExpr))

		// Check that aggregates can be only FirstAgg or ConstAgg.
		for _, item := range *t.Child(1).(*AggregationsExpr) {
			switch item.Agg.Op() {
			case opt.FirstAggOp, opt.ConstAggOp:

			default:
				panic(errors.AssertionFailedf("distinct-on contains %s", log.Safe(item.Agg.Op())))
			}
		}

	case *GroupByExpr, *ScalarGroupByExpr:
		checkErrorOnDup(e.(RelExpr))

		// Check that aggregates cannot be FirstAgg.
		for _, item := range *t.Child(1).(*AggregationsExpr) {
			switch item.Agg.Op() {
			case opt.FirstAggOp:
				panic(errors.AssertionFailedf("group-by contains %s", log.Safe(item.Agg.Op())))
			}
		}

	case *IndexJoinExpr:
		if t.Cols.Empty() {
			panic(errors.AssertionFailedf("index join with no columns"))
		}

	case *LookupJoinExpr:
		if len(t.KeyCols) == 0 {
			panic(errors.AssertionFailedf("lookup join with no key columns"))
		}
		if t.Cols.Empty() {
			panic(errors.AssertionFailedf("lookup join with no output columns"))
		}
		if t.Cols.SubsetOf(t.Input.Relational().OutputCols) {
			panic(errors.AssertionFailedf("lookup join with no lookup columns"))
		}

	case *InsertExpr:
		tab := m.Metadata().Table(t.Table)
		m.checkColListLen(t.InsertCols, tab.DeletableColumnCount(), "InsertCols")
		m.checkColListLen(t.FetchCols, 0, "FetchCols")
		m.checkColListLen(t.UpdateCols, 0, "UpdateCols")

		// Ensure that insert columns include all columns except for delete-only
		// mutation columns (which do not need to be part of INSERT).
		for i, n := 0, tab.WritableColumnCount(); i < n; i++ {
			if t.InsertCols[i] == 0 {
				panic(errors.AssertionFailedf("insert values not provided for all table columns"))
			}
		}

		m.checkMutationExpr(t, &t.MutationPrivate)

	case *UpdateExpr:
		tab := m.Metadata().Table(t.Table)
		m.checkColListLen(t.InsertCols, 0, "InsertCols")
		m.checkColListLen(t.FetchCols, tab.DeletableColumnCount(), "FetchCols")
		m.checkColListLen(t.UpdateCols, tab.DeletableColumnCount(), "UpdateCols")
		m.checkMutationExpr(t, &t.MutationPrivate)

	case *ZigzagJoinExpr:
		if len(t.LeftEqCols) != len(t.RightEqCols) {
			panic(errors.AssertionFailedf("zigzag join with mismatching eq columns"))
		}

	case *AggDistinctExpr:
		if t.Input.Op() == opt.AggFilterOp {
			panic(errors.AssertionFailedf("AggFilter should always be on top of AggDistinct"))
		}

	case *ConstExpr:
		if t.Value == tree.DNull {
			panic(errors.AssertionFailedf("NULL values should always use NullExpr, not ConstExpr"))
		}

	default:
		if opt.IsJoinOp(e) {
			left := e.Child(0).(RelExpr)
			right := e.Child(1).(RelExpr)
			// The left side cannot depend on the right side columns.
			if left.Relational().OuterCols.Intersects(right.Relational().OutputCols) {
				panic(errors.AssertionFailedf(
					"%s left side has outer cols in right side", log.Safe(e.Op()),
				))
			}

			// The reverse is allowed but only for apply variants.
			if !opt.IsJoinApplyOp(e) {
				if right.Relational().OuterCols.Intersects(left.Relational().OutputCols) {
					panic(errors.AssertionFailedf("%s is correlated", log.Safe(e.Op())))
				}
			}
			checkFilters(*e.Child(2).(*FiltersExpr))
		}
	}

	// Check orderings within operators.
	checkExprOrdering(e)

	// Check for overlapping column IDs in child relational expressions.
	if opt.IsRelationalOp(e) {
		checkOutputCols(e)
	}
}

func (m *Memo) checkColListLen(colList opt.ColList, expectedLen int, listName string) {
	if len(colList) != expectedLen {
		panic(errors.AssertionFailedf("column list %s expected length = %d, actual length = %d",
			listName, log.Safe(expectedLen), len(colList)))
	}
}

func (m *Memo) checkMutationExpr(rel RelExpr, private *MutationPrivate) {
	// Output columns should never include mutation columns.
	tab := m.Metadata().Table(private.Table)
	var mutCols opt.ColSet
	for i, n := tab.ColumnCount(), tab.DeletableColumnCount(); i < n; i++ {
		mutCols.Add(private.Table.ColumnID(i))
	}
	if rel.Relational().OutputCols.Intersects(mutCols) {
		panic(errors.AssertionFailedf("output columns cannot include mutation columns"))
	}
}

// checkExprOrdering runs checks on orderings stored inside operators.
func checkExprOrdering(e opt.Expr) {
	// Verify that orderings stored in operators only refer to columns produced by
	// their input.
	var ordering physical.OrderingChoice
	switch t := e.Private().(type) {
	case *physical.OrderingChoice:
		ordering = *t
	case *OrdinalityPrivate:
		ordering = t.Ordering
	case GroupingPrivate:
		ordering = t.Ordering
	default:
		return
	}
	if outCols := e.(RelExpr).Relational().OutputCols; !ordering.SubsetOfCols(outCols) {
		panic(errors.AssertionFailedf(
			"invalid ordering %v (op: %s, outcols: %v)",
			log.Safe(ordering), log.Safe(e.Op()), log.Safe(outCols),
		))
	}
}

func checkFilters(filters FiltersExpr) {
	for _, item := range filters {
		if item.Condition.Op() == opt.RangeOp {
			if !item.scalar.TightConstraints {
				panic(errors.AssertionFailedf("Range operator should always have tight constraints"))
			}
			if item.scalar.OuterCols.Len() != 1 {
				panic(errors.AssertionFailedf("Range operator should have exactly one outer col"))
			}
		}
	}
}

func checkErrorOnDup(e RelExpr) {
	// Only EnsureDistinctOn and EnsureUpsertDistinctOn should set the ErrorOnDup
	// field to a value.
	if e.Op() != opt.EnsureDistinctOnOp &&
		e.Op() != opt.EnsureUpsertDistinctOnOp &&
		e.Private().(*GroupingPrivate).ErrorOnDup != "" {
		panic(errors.AssertionFailedf(
			"%s should never set ErrorOnDup to a non-empty string", log.Safe(e.Op())))
	}
	if (e.Op() == opt.EnsureDistinctOnOp ||
		e.Op() == opt.EnsureUpsertDistinctOnOp) &&
		e.Private().(*GroupingPrivate).ErrorOnDup == "" {
		panic(errors.AssertionFailedf(
			"%s should never leave ErrorOnDup as an empty string", log.Safe(e.Op())))
	}
}

func checkOutputCols(e opt.Expr) {
	set := opt.ColSet{}

	for i := 0; i < e.ChildCount(); i++ {
		rel, ok := e.Child(i).(RelExpr)
		if !ok {
			continue
		}

		// The output columns of child expressions cannot overlap.
		cols := rel.Relational().OutputCols
		if set.Intersects(cols) {
			panic(errors.AssertionFailedf(
				"%s RelExpr children have intersecting columns", log.Safe(e.Op()),
			))
		}

		set.UnionWith(cols)
	}
}
