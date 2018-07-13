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

// rulePropsBuilder is a helper class that fills out some fields (those needed
// by normalization rules) in the Rule property structure in props.Relational.
// See the comment for the props.Relational.Rule field for more details.
type rulePropsBuilder struct {
	mem *memo.Memo
}

// buildProps fills in props.Relational.Rule fields in the given expression's
// logical properties. Each rule property is "owned" by a family of Optgen
// rules, like the PruneCols rules, or the Decorrelate rules, that uses that
// property. How the rule property is derived is tightly bound to how the
// corresponding rules work. Therefore, any changes to property derivation in
// this class must be coordinated with corresponding changes to the owner rules.
//
// Currently the following properties are built:
//
//   PruneCols: for more details, see the comment header in prune_cols.opt and
//              the comment header for props.Relational.Rule.PruneCols.
//
func (b *rulePropsBuilder) buildProps(ev memo.ExprView) {
	// Only relational rule properties exist so far.
	if !ev.IsRelational() {
		return
	}

	switch ev.Operator() {
	case opt.ScanOp:
		b.buildScanProps(ev)

	case opt.SelectOp:
		b.buildSelectProps(ev)

	case opt.ProjectOp:
		b.buildProjectProps(ev)

	case opt.ValuesOp:
		b.buildValuesProps(ev)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		b.buildJoinProps(ev)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		b.buildSetProps(ev)

	case opt.GroupByOp:
		b.buildGroupByProps(ev)

	case opt.LimitOp:
		b.buildLimitProps(ev)

	case opt.OffsetOp:
		b.buildOffsetProps(ev)

	case opt.Max1RowOp:
		b.buildMax1RowProps(ev)

	case opt.RowNumberOp:
		b.buildRowNumberProps(ev)

	case opt.IndexJoinOp, opt.LookupJoinOp:
		// There is no need to prune columns projected by Index or Lookup joins,
		// since its parent will always be an "alternate" expression in the memo.
		// Any pruneable columns should have already been pruned at the time the
		// IndexJoin is constructed. Additionally, there is not currently a
		// PruneCols rule for these operators.

	case opt.ExplainOp, opt.ShowTraceForSessionOp:
		// Don't allow any columns to be pruned, since that would trigger the
		// creation of a wrapper Project around the Explain (it's not capable
		// of pruning columns or of passing through Project operators).

	default:
		panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.Operator()))
	}
}

func (b *rulePropsBuilder) buildScanProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	// Column Pruning
	// --------------
	// All columns can potentially be pruned from the Scan.
	relational.Rule.PruneCols = ev.Logical().Relational.OutputCols
}

func (b *rulePropsBuilder) buildSelectProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	filterProps := ev.Child(1).Logical().Scalar

	// Column Pruning
	// --------------
	// Any pruneable input columns can potentially be pruned, as long as they're
	// not used by the filter.
	relational.Rule.PruneCols = inputProps.Rule.PruneCols.Difference(filterProps.OuterCols)
}

func (b *rulePropsBuilder) buildProjectProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	// Column Pruning
	// --------------
	// All columns can potentially be pruned from the Project, if they're never
	// used in a higher-level expression.
	relational.Rule.PruneCols = ev.Logical().Relational.OutputCols
}

func (b *rulePropsBuilder) buildJoinProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	leftProps := ev.Child(0).Logical().Relational
	rightProps := ev.Child(1).Logical().Relational
	onProps := ev.Child(2).Logical().Scalar

	// Column Pruning
	// --------------
	// Any pruneable columns from projected inputs can potentially be pruned, as
	// long as they're not used by the right input (i.e. in Apply case) or by
	// the join filter.
	switch ev.Operator() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		relational.Rule.PruneCols = leftProps.Rule.PruneCols.Copy()

	default:
		relational.Rule.PruneCols = leftProps.Rule.PruneCols.Union(rightProps.Rule.PruneCols)
	}
	relational.Rule.PruneCols.DifferenceWith(rightProps.OuterCols)
	relational.Rule.PruneCols.DifferenceWith(onProps.OuterCols)

	// Null Rejection
	// --------------
	switch ev.Operator() {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		// Pass through null-rejection columns from both inputs.
		relational.Rule.RejectNullCols = leftProps.Rule.RejectNullCols
		relational.Rule.RejectNullCols.UnionWith(rightProps.Rule.RejectNullCols)

	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// Pass through null-rejection columns from left input, and request null-
		// rejection on right columns.
		relational.Rule.RejectNullCols = leftProps.Rule.RejectNullCols
		relational.Rule.RejectNullCols.UnionWith(rightProps.OutputCols)

	case opt.RightJoinOp, opt.RightJoinApplyOp:
		// Pass through null-rejection columns from right input, and request null-
		// rejection on left columns.
		relational.Rule.RejectNullCols = leftProps.OutputCols
		relational.Rule.RejectNullCols.UnionWith(rightProps.Rule.RejectNullCols)

	case opt.FullJoinOp, opt.FullJoinApplyOp:
		// Request null-rejection on all output columns.
		relational.Rule.RejectNullCols = relational.OutputCols
	}
}

func (b *rulePropsBuilder) buildGroupByProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	aggs := ev.Child(1)
	aggColList := aggs.Private().(opt.ColList)
	groupingColSet := ev.Private().(*memo.GroupByDef).GroupingCols

	// Column Pruning
	// --------------
	// Grouping columns can't be pruned, because they were used to group rows.
	// However, aggregation columns can potentially be pruned.
	if groupingColSet.Empty() {
		relational.Rule.PruneCols = relational.OutputCols
	} else {
		relational.Rule.PruneCols = relational.OutputCols.Difference(groupingColSet)
	}

	// Null Rejection
	// --------------
	// If an aggregate input column has requested NULL rejection, then pass along
	// its request if the following criteria are met:
	//   1. The aggregate function ignores NULL values, meaning that its value
	//      would not change if input NULL values are filtered.
	//   2. The aggregate function returns NULL if its input is empty. And since
	//      by #1, the presence of NULLs does not alter the result, the aggregate
	//      function would return NULL if its input contains only NULL values.
	//   3. No other input columns are referenced by other aggregate functions in
	//      the GroupBy (all functions must refer to the same column), with the
	//      exception of AnyNotNull, covered in #3.
	//   4. Ignore AnyNotNull aggregate if its input column is functionally
	//      determined by the grouping columns (use the input operator's FD set
	//      to check that). If true, then input rows in each group must have the
	//      same value for this column (i.e. the column is constant per group).
	//      In that case, it doesn't matter which rows are filtered.
	//
	var anyNotNullCols opt.ColSet
	var savedInColID opt.ColumnID
	for i := aggs.ChildCount() - 1; i >= 0; i-- {
		agg := aggs.Child(i)
		aggOp := agg.Operator()
		if aggOp == opt.AnyNotNullOp {
			// AnyNotNullOp shares a ColumnID with its input, so no need to dig
			// into the aggregate's Variable.
			anyNotNullCols.Add(int(aggColList[i]))
			if inputProps.FuncDeps.InClosureOf(anyNotNullCols, groupingColSet) {
				continue
			}
			// Else column is not functionally determined by grouping columns, so
			// can't reject NULLs.
		} else if opt.AggregateIgnoresNulls(aggOp) && opt.AggregateIsNullOnEmpty(aggOp) {
			// Get column ID of Variable operator input.
			inColID := agg.Child(0).Private().(opt.ColumnID)
			if savedInColID == 0 || savedInColID == inColID {
				savedInColID = inColID
				if inputProps.Rule.RejectNullCols.Contains(int(inColID)) {
					relational.Rule.RejectNullCols.Add(int(aggColList[i]))

					// Can possibly reject column, but keep searching, since if
					// multiple columns are used by aggregate functions, then nulls
					// can't be rejected on any column.
					continue
				}
				// Else input has not requested null rejection on the input column.
			}
			// Else multiple columns used by aggregate functions, so can't reject
			// NULLs for any of them.
		}

		// Don't reject NULLs for the aggregate. Clear any columns already added.
		relational.Rule.RejectNullCols = opt.ColSet{}
		break
	}
}

func (b *rulePropsBuilder) buildSetProps(ev memo.ExprView) {
	// Column Pruning
	// --------------
	// Don't allow any columns to be pruned, since that would trigger the
	// creation of a wrapper Project around the set operator, since there's not
	// yet a PruneCols rule for set operators.
}

func (b *rulePropsBuilder) buildValuesProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	// Column Pruning
	// --------------
	// All columns can potentially be pruned from the Values operator.
	relational.Rule.PruneCols = ev.Logical().Relational.OutputCols
}

func (b *rulePropsBuilder) buildLimitProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	ordering := ev.Private().(*props.OrderingChoice).ColSet()

	// Column Pruning
	// --------------
	// Any pruneable input columns can potentially be pruned, as long as they're
	// not used as an ordering column.
	relational.Rule.PruneCols = inputProps.Rule.PruneCols.Difference(ordering)
}

func (b *rulePropsBuilder) buildOffsetProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	ordering := ev.Private().(*props.OrderingChoice).ColSet()

	// Column Pruning
	// --------------
	// Any pruneable input columns can potentially be pruned, as long as they're
	// not used as an ordering column.
	relational.Rule.PruneCols = inputProps.Rule.PruneCols.Difference(ordering)
}

func (b *rulePropsBuilder) buildMax1RowProps(ev memo.ExprView) {
	// Column Pruning
	// --------------
	// Don't allow any columns to be pruned, since that would trigger the
	// creation of a wrapper Project around the Max1Row, since there's not
	// a PruneCols rule for Max1Row.
}

func (b *rulePropsBuilder) buildRowNumberProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	ordering := ev.Private().(*memo.RowNumberDef).Ordering.ColSet()

	// Column Pruning
	// --------------
	// Any pruneable input columns can potentially be pruned, as long as they're
	// not used as an ordering column. The new row number column cannot be pruned
	// without adding an additional Project operator, so don't add it to the set.
	relational.Rule.PruneCols = inputProps.Rule.PruneCols.Difference(ordering)
}
