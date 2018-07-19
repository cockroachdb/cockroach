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

	case opt.GroupByOp, opt.ScalarGroupByOp:
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

	case opt.ZipOp:
		b.buildZipProps(ev)

	case opt.VirtualScanOp, opt.ExplainOp, opt.ShowTraceForSessionOp:
		// Don't allow any columns to be pruned, since that would trigger the
		// creation of a wrapper Project around the operator (they're not capable
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
	relational.Rule.RejectNullCols = deriveGroupByRejectNullCols(ev)
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

func (b *rulePropsBuilder) buildZipProps(ev memo.ExprView) {
	// Don't allow any columns to be pruned, since that would trigger the
	// creation of a wrapper Project around the Zip, and there's not yet a
	// PruneCols rule for Zip.
}

// deriveGroupByRejectNullCols returns the set of GroupBy columns that are
// eligible for null rejection. If an aggregate input column has requested null
// rejection, then pass along its request if the following criteria are met:
//
//   1. The aggregate function ignores null values, meaning that its value
//      would not change if input null values are filtered.
//   2. The aggregate function returns null if its input is empty. And since
//      by #1, the presence of nulls does not alter the result, the aggregate
//      function would return null if its input contains only null values.
//   3. The aggregate function is not AnyNotNull. While the AnyNotNull aggregate
//      does qualify according to #1 and #2, it is only generated as a side-
//      effect of other rules, and is difficult to test. Therefore, for now
//      it will never qualify for null rejection, though it may be ignored
//      according to criteria #4.
//   4. No other input columns are referenced by other aggregate functions in
//      the GroupBy (all functions must refer to the same column), with the
//      possible exception of AnyNotNull. An AnyNotNull aggregate can be
//      ignored if its input column is functionally determined by the grouping
//      columns (use the input operator's FD set to check that). If true, then
//      input rows in each group must have the same value for this column
//      (i.e. the column is constant per group). In that case, it doesn't
//      matter which rows are filtered.
//
func deriveGroupByRejectNullCols(ev memo.ExprView) opt.ColSet {
	inputProps := ev.Child(0).Logical().Relational
	aggs := ev.Child(1)
	aggColList := aggs.Private().(opt.ColList)
	groupingColSet := ev.Private().(*memo.GroupByDef).GroupingCols

	var rejectNullCols, anyNotNullCols opt.ColSet
	var savedInColID opt.ColumnID
	for i, end := 0, aggs.ChildCount(); i < end; i++ {
		agg := aggs.Child(i)
		aggOp := agg.Operator()

		// Criteria #1 and #2.
		if !opt.AggregateIgnoresNulls(aggOp) || !opt.AggregateIsNullOnEmpty(aggOp) {
			// Can't reject nulls for the aggregate.
			return opt.ColSet{}
		}

		// Get column ID of aggregate's Variable operator input.
		inColID := agg.Child(0).Private().(opt.ColumnID)

		// Criteria #3 (and AnyNotNull check for criteria #4).
		if aggOp == opt.AnyNotNullOp {
			anyNotNullCols.Add(int(inColID))
			if !inputProps.FuncDeps.InClosureOf(anyNotNullCols, groupingColSet) {
				// Column is not functionally determined by grouping columns, so
				// can't reject any nulls.
				return opt.ColSet{}
			}
			continue
		}

		// Criteria #4.
		if savedInColID != 0 && savedInColID != inColID {
			// Multiple columns used by aggregate functions, so can't reject nulls
			// for any of them.
			return opt.ColSet{}
		}
		savedInColID = inColID

		if !inputProps.Rule.RejectNullCols.Contains(int(inColID)) {
			// Input has not requested null rejection on the input column.
			return opt.ColSet{}
		}

		// Can possibly reject column, but keep searching, since if
		// multiple columns are used by aggregate functions, then nulls
		// can't be rejected on any column.
		rejectNullCols.Add(int(aggColList[i]))
	}
	return rejectNullCols
}
