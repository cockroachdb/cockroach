// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// CheckExpr does sanity checking on an Expr. This function is only defined in
// crdb_test builds so that checks are run for tests while keeping the check
// code out of non-test builds, since it can be expensive to run.
//
// This function does not assume that the expression has been fully normalized.
func (m *Memo) CheckExpr(e opt.Expr) {
	if !buildutil.CrdbTestBuild {
		return
	}

	if m.disableCheckExpr {
		return
	}

	// Check properties.
	switch t := e.(type) {
	case RelExpr:
		t.Relational().Verify()

		// If the expression was added to an existing group, cross-check its
		// properties against the properties of the group. Skip this check if the
		// operator is known to not have code for building logical props.
		if t != t.FirstExpr() && t.Op() != opt.MergeJoinOp && t.Op() != opt.PlaceholderScanOp {
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
				panic(errors.AssertionFailedf("non-list op contains item op: %s", redact.Safe(child.Op())))
			}
		}
	}

	// Check operator-specific fields.
	switch t := e.(type) {
	case *ScanExpr:
		if t.Flags.NoIndexJoin && t.Flags.ForceIndex {
			panic(errors.AssertionFailedf("NoIndexJoin and ForceIndex set"))
		}
		if evalCtx := m.logPropsBuilder.evalCtx; evalCtx != nil && t.Constraint != nil {
			if expected := t.Constraint.ExactPrefix(evalCtx); expected != t.ExactPrefix {
				panic(errors.AssertionFailedf(
					"expected exact prefix %d but found %d", expected, t.ExactPrefix,
				))
			}
		}

	case *ProjectExpr:
		if !t.Passthrough.SubsetOf(t.Input.Relational().OutputCols) {
			panic(errors.AssertionFailedf(
				"projection passes through columns not in input: %v",
				t.Input.Relational().OutputCols.Difference(t.Passthrough),
			))
		}
		for _, item := range t.Projections {
			// Check that column id is set.
			if item.Col == 0 {
				panic(errors.AssertionFailedf("projections column cannot have id of 0"))
			}

			// Check that column is not both passthrough and synthesized.
			if t.Passthrough.Contains(item.Col) {
				panic(errors.AssertionFailedf(
					"both passthrough and synthesized have column %d", redact.Safe(item.Col)))
			}

			// Check that columns aren't passed through in projection expressions.
			if v, ok := item.Element.(*VariableExpr); ok {
				if v.Col == item.Col {
					panic(errors.AssertionFailedf("projection passes through column %d", redact.Safe(item.Col)))
				}
			}
		}

	case *SelectExpr:
		checkFilters(t.Filters)

	case *UnionExpr, *UnionAllExpr, *LocalityOptimizedSearchExpr:
		setPrivate := t.Private().(*SetPrivate)
		outColSet := setPrivate.OutCols.ToSet()

		// Check that columns on the left side of the union are not reused in
		// the output.
		leftColSet := setPrivate.LeftCols.ToSet()
		if outColSet.Intersects(leftColSet) {
			panic(errors.AssertionFailedf(
				"union reuses columns in left input: %v",
				outColSet.Intersection(leftColSet),
			))
		}

		// Check that columns on the right side of the union are not reused in
		// the output.
		rightColSet := setPrivate.RightCols.ToSet()
		if outColSet.Intersects(rightColSet) {
			panic(errors.AssertionFailedf(
				"union reuses columns in right input: %v",
				outColSet.Intersection(rightColSet),
			))
		}

		switch t.Op() {
		case opt.LocalityOptimizedSearchOp:
			if !setPrivate.Ordering.Any() {
				panic(errors.AssertionFailedf("locality optimized search op has a non-empty ordering"))
			}
		}

	case *AggregationsExpr:
		var checkAggs func(scalar opt.ScalarExpr)
		checkAggs = func(scalar opt.ScalarExpr) {
			switch scalar.Op() {
			case opt.AggDistinctOp:
				checkAggs(scalar.Child(0).(opt.ScalarExpr))

			case opt.VariableOp:

			default:
				if !opt.IsAggregateOp(scalar) {
					panic(errors.AssertionFailedf("aggregate contains illegal op: %s", redact.Safe(scalar.Op())))
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
		checkNullsAreDistinct(e.(RelExpr))

		// Check that aggregates can be only FirstAgg or ConstAgg.
		for _, item := range *t.Child(1).(*AggregationsExpr) {
			switch item.Agg.Op() {
			case opt.FirstAggOp, opt.ConstAggOp:

			default:
				panic(errors.AssertionFailedf("distinct-on contains %s", redact.Safe(item.Agg.Op())))
			}
		}

	case *GroupByExpr, *ScalarGroupByExpr:
		checkErrorOnDup(e.(RelExpr))
		checkNullsAreDistinct(e.(RelExpr))

		// Check that aggregates cannot be FirstAgg.
		for _, item := range *t.Child(1).(*AggregationsExpr) {
			switch item.Agg.Op() {
			case opt.FirstAggOp:
				panic(errors.AssertionFailedf("group-by contains %s", redact.Safe(item.Agg.Op())))
			}
		}

	case *IndexJoinExpr:
		if t.Cols.Empty() {
			panic(errors.AssertionFailedf("index join with no columns"))
		}

	case *LookupJoinExpr:
		if len(t.KeyCols) == 0 && len(t.LookupExpr) == 0 {
			panic(errors.AssertionFailedf("lookup join with no key columns or lookup filters"))
		}
		if len(t.KeyCols) != 0 && len(t.LookupExpr) != 0 {
			panic(errors.AssertionFailedf("lookup join with both key columns and lookup filters"))
		}
		if t.Cols.Empty() {
			panic(errors.AssertionFailedf("lookup join with no output columns"))
		}
		if t.Cols.SubsetOf(t.Input.Relational().OutputCols) {
			panic(errors.AssertionFailedf("lookup join with no lookup columns"))
		}
		var requiredCols opt.ColSet
		requiredCols.UnionWith(t.Relational().OutputCols)
		requiredCols.UnionWith(t.ConstFilters.OuterCols())
		requiredCols.UnionWith(t.On.OuterCols())
		requiredCols.UnionWith(t.KeyCols.ToSet())
		requiredCols.UnionWith(t.LookupExpr.OuterCols())
		idx := m.Metadata().Table(t.Table).Index(t.Index)
		for i := range t.KeyCols {
			requiredCols.Add(t.Table.ColumnID(idx.Column(i).Ordinal()))
		}
		if !t.Cols.SubsetOf(requiredCols) {
			panic(errors.AssertionFailedf("lookup join with columns that are not required"))
		}
		if t.IsFirstJoinInPairedJoiner {
			switch t.JoinType {
			case opt.InnerJoinOp, opt.LeftJoinOp:
			default:
				panic(errors.AssertionFailedf(
					"first join in paired joiner must be an inner or left join. found %s",
					t.JoinType.String(),
				))
			}
			if t.ContinuationCol == 0 {
				panic(errors.AssertionFailedf("first join in paired joiner must have a continuation column"))
			}
		}
		if t.IsSecondJoinInPairedJoiner {
			switch firstJoin := t.Input.(type) {
			case *InvertedJoinExpr:
				if !firstJoin.IsFirstJoinInPairedJoiner {
					panic(errors.AssertionFailedf(
						"lookup paired-join is paired with inverted join that thinks it is unpaired"))
				}
			case *LookupJoinExpr:
				if !firstJoin.IsFirstJoinInPairedJoiner {
					panic(errors.AssertionFailedf(
						"lookup paired-join is paired with lookup join that thinks it is unpaired"))
				}
			default:
				panic(errors.AssertionFailedf("lookup paired-join is paired with %T", t.Input))
			}
		}

	case *InsertExpr:
		tab := m.Metadata().Table(t.Table)
		m.checkColListLen(t.InsertCols, tab.ColumnCount(), "InsertCols")
		m.checkColListLen(t.FetchCols, 0, "FetchCols")
		m.checkColListLen(t.UpdateCols, 0, "UpdateCols")

		// Ensure that insert columns include all columns except for delete-only
		// mutation columns (which do not need to be part of INSERT).
		for i, n := 0, tab.ColumnCount(); i < n; i++ {
			kind := tab.Column(i).Kind()
			if (kind == cat.Ordinary || kind == cat.WriteOnly) && t.InsertCols[i] == 0 {
				panic(errors.AssertionFailedf("insert values not provided for all table columns"))
			}
			if t.InsertCols[i] != 0 {
				switch kind {
				case cat.System:
					panic(errors.AssertionFailedf("system column found in insertion columns"))
				case cat.Inverted:
					panic(errors.AssertionFailedf("inverted column found in insertion columns"))
				}
			}
		}

		m.checkMutationExpr(t, &t.MutationPrivate)

	case *UpdateExpr:
		tab := m.Metadata().Table(t.Table)
		m.checkColListLen(t.InsertCols, 0, "InsertCols")
		m.checkColListLen(t.FetchCols, tab.ColumnCount(), "FetchCols")
		m.checkColListLen(t.UpdateCols, tab.ColumnCount(), "UpdateCols")
		m.checkMutationExpr(t, &t.MutationPrivate)

	case *ZigzagJoinExpr:
		if len(t.LeftEqCols) != len(t.RightEqCols) {
			panic(errors.AssertionFailedf("zigzag join with mismatching eq columns"))
		}
		meta := m.Metadata()
		left, right := meta.Table(t.LeftTable), meta.Table(t.RightTable)
		for i := 0; i < left.ColumnCount(); i++ {
			if left.Column(i).Kind() == cat.System && t.Cols.Contains(t.LeftTable.ColumnID(i)) {
				panic(errors.AssertionFailedf("zigzag join should not contain system column"))
			}
		}
		for i := 0; i < right.ColumnCount(); i++ {
			if right.Column(i).Kind() == cat.System && t.Cols.Contains(t.RightTable.ColumnID(i)) {
				panic(errors.AssertionFailedf("zigzag join should not contain system column"))
			}
		}

	case *AggDistinctExpr:
		if t.Input.Op() == opt.AggFilterOp {
			panic(errors.AssertionFailedf("AggFilter should always be on top of AggDistinct"))
		}

	case *ConstExpr:
		if t.Value == tree.DNull {
			panic(errors.AssertionFailedf("NULL values should always use NullExpr, not ConstExpr"))
		}
		if t.Value == tree.DBoolTrue {
			panic(errors.AssertionFailedf("true values should always use TrueSingleton, not ConstExpr"))
		}
		if t.Value == tree.DBoolFalse {
			panic(errors.AssertionFailedf("false values should always use FalseSingleton, not ConstExpr"))
		}

	case *WithExpr:
		if !t.BindingOrdering.Any() && (!t.Mtr.Set || !t.Mtr.Materialize) {
			panic(errors.AssertionFailedf("with ordering can only be specified with forced materialization"))
		}

	case *WithScanExpr:
		// Verify the input columns exist in the binding.
		binding := m.Metadata().WithBinding(t.With)
		if binding == nil {
			panic(errors.AssertionFailedf("WithScan binding missing"))
		}
		if !t.InCols.ToSet().SubsetOf(binding.(RelExpr).Relational().OutputCols) {
			panic(errors.AssertionFailedf("invalid WithScan input columns %v", t.InCols))
		}

	default:
		if opt.IsJoinOp(e) {
			left := e.Child(0).(RelExpr)
			right := e.Child(1).(RelExpr)
			// The left side cannot depend on the right side columns.
			if left.Relational().OuterCols.Intersects(right.Relational().OutputCols) {
				panic(errors.AssertionFailedf(
					"%s left side has outer cols in right side", redact.Safe(e.Op()),
				))
			}

			// The reverse is allowed but only for apply variants.
			if !opt.IsJoinApplyOp(e) {
				if right.Relational().OuterCols.Intersects(left.Relational().OutputCols) {
					panic(errors.AssertionFailedf("%s is correlated", redact.Safe(e.Op())))
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

func (m *Memo) checkColListLen(colList opt.OptionalColList, expectedLen int, listName string) {
	if len(colList) != expectedLen {
		panic(errors.AssertionFailedf("column list %s expected length = %d, actual length = %d",
			listName, redact.Safe(expectedLen), len(colList)))
	}
}

func (m *Memo) checkMutationExpr(rel RelExpr, private *MutationPrivate) {
	// Output columns should never include mutation columns.
	tab := m.Metadata().Table(private.Table)
	var mutCols opt.ColSet
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		if tab.Column(i).IsMutation() {
			mutCols.Add(private.Table.ColumnID(i))
		}
	}
	if rel.Relational().OutputCols.Intersects(mutCols) {
		panic(errors.AssertionFailedf("output columns cannot include mutation columns"))
	}
}

// checkExprOrdering runs checks on orderings stored inside operators.
func checkExprOrdering(e opt.Expr) {
	// Verify that orderings stored in operators only refer to output columns.
	var ordering props.OrderingChoice
	switch t := e.Private().(type) {
	case *props.OrderingChoice:
		ordering = *t
	case *OrdinalityPrivate:
		ordering = t.Ordering
	case GroupingPrivate:
		ordering = t.Ordering
	case *SetPrivate:
		ordering = t.Ordering
		switch e.Op() {
		case opt.ExceptOp, opt.ExceptAllOp, opt.IntersectOp, opt.IntersectAllOp, opt.UnionOp:
			// For these operators, the ordering must include all output columns.
			if !ordering.Any() {
				if outCols := e.(RelExpr).Relational().OutputCols; !outCols.SubsetOf(ordering.ColSet()) {
					panic(errors.AssertionFailedf(
						"ordering for streaming set ops must include all output columns %v (op: %s, outcols: %v)",
						redact.Safe(ordering), redact.Safe(e.Op()), redact.Safe(outCols),
					))
				}
			}
		}
	default:
		return
	}
	if outCols := e.(RelExpr).Relational().OutputCols; !ordering.SubsetOfCols(outCols) {
		panic(errors.AssertionFailedf(
			"invalid ordering %v (op: %s, outcols: %v)",
			redact.Safe(ordering), redact.Safe(e.Op()), redact.Safe(outCols),
		))
	}
}

func checkFilters(filters FiltersExpr) {
	for _, item := range filters {
		if item.Condition.Op() == opt.RangeOp {
			if !item.scalar.TightConstraints {
				panic(errors.AssertionFailedf("range operator should always have tight constraints"))
			}
			if item.scalar.OuterCols.Len() != 1 {
				panic(errors.AssertionFailedf("range operator should have exactly one outer col"))
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
			"%s should never set ErrorOnDup to a non-empty string", redact.Safe(e.Op())))
	}
	if (e.Op() == opt.EnsureDistinctOnOp ||
		e.Op() == opt.EnsureUpsertDistinctOnOp) &&
		e.Private().(*GroupingPrivate).ErrorOnDup == "" {
		panic(errors.AssertionFailedf(
			"%s should never leave ErrorOnDup as an empty string", redact.Safe(e.Op())))
	}
}

func checkNullsAreDistinct(e RelExpr) {
	// Only UpsertDistinctOn and EnsureUpsertDistinctOn should set the
	// NullsAreDistinct field to true.
	if e.Op() != opt.UpsertDistinctOnOp &&
		e.Op() != opt.EnsureUpsertDistinctOnOp &&
		e.Private().(*GroupingPrivate).NullsAreDistinct {
		panic(errors.AssertionFailedf(
			"%s should never set NullsAreDistinct to true", redact.Safe(e.Op())))
	}
	if (e.Op() == opt.UpsertDistinctOnOp ||
		e.Op() == opt.EnsureUpsertDistinctOnOp) &&
		!e.Private().(*GroupingPrivate).NullsAreDistinct {
		panic(errors.AssertionFailedf(
			"%s should never set NullsAreDistinct to false", redact.Safe(e.Op())))
	}
}

func checkOutputCols(e opt.Expr) {
	set := opt.ColSet{}

	for i := 0; i < e.ChildCount(); i++ {
		rel, ok := e.Child(i).(RelExpr)
		if !ok {
			continue
		}

		// The output columns of child expressions cannot overlap. The only
		// exception is the first child of RecursiveCTE.
		if e.Op() == opt.RecursiveCTEOp && i == 0 {
			continue
		}
		cols := rel.Relational().OutputCols
		if set.Intersects(cols) {
			panic(errors.AssertionFailedf(
				"%s RelExpr children have intersecting columns", redact.Safe(e.Op()),
			))
		}

		set.UnionWith(cols)
	}
}
