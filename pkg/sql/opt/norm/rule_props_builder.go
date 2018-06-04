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

// rulePropsBuilder is a helper class that fills out the rule property structure
// in props.Relational. See the comment for the props.Relational.Rule field for
// more details.
type rulePropsBuilder struct {
	mem *memo.Memo
}

// buildProps fills the props.Relational.Rule field in the given expression's
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

	case opt.LookupJoinOp:
		b.buildLookupJoinProps(ev)

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

	case opt.ExplainOp, opt.ShowTraceOp, opt.ShowTraceForSessionOp:
		// Don't allow any columns to be pruned, since that would trigger the
		// creation of a wrapper Project around the Explain (it's not capable
		// of pruning columns or of passing through Project operators).

	default:
		panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.Operator()))
	}
}

func (b *rulePropsBuilder) buildScanProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	// All columns can potentially be pruned from the Scan.
	relational.Rule.PruneCols = ev.Logical().Relational.OutputCols
}

func (b *rulePropsBuilder) buildSelectProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	filterProps := ev.Child(1).Logical().Scalar

	// Any pruneable input columns can potentially be pruned, as long as they're
	// not used by the filter.
	relational.Rule.PruneCols = inputProps.Rule.PruneCols.Difference(filterProps.OuterCols)
}

func (b *rulePropsBuilder) buildProjectProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	// All columns can potentially be pruned from the Project, if they're never
	// used in a higher-level expression.
	relational.Rule.PruneCols = ev.Logical().Relational.OutputCols
}

func (b *rulePropsBuilder) buildJoinProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	leftProps := ev.Child(0).Logical().Relational
	rightProps := ev.Child(1).Logical().Relational
	onProps := ev.Child(2).Logical().Scalar

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
}

func (b *rulePropsBuilder) buildLookupJoinProps(ev memo.ExprView) {
	// There is no need to prune columns projected by the LookupJoin, since its
	// parent will always be an "alternate" expression in the memo. Any pruneable
	// columns should have already been pruned at the time the LookupJoin is
	// constructed. Additionally, there is not currently a PruneCols rule for
	// LookupJoin.
}

func (b *rulePropsBuilder) buildGroupByProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	groupingColSet := ev.Private().(*memo.GroupByDef).GroupingCols

	// Grouping columns can't be pruned, because they were used to group rows.
	// However, aggregation columns can potentially be pruned.
	if groupingColSet.Empty() {
		relational.Rule.PruneCols = relational.OutputCols
	} else {
		relational.Rule.PruneCols = relational.OutputCols.Difference(groupingColSet)
	}
}

func (b *rulePropsBuilder) buildSetProps(ev memo.ExprView) {
	// Don't allow any columns to be pruned, since that would trigger the
	// creation of a wrapper Project around the set operator, since there's not
	// yet a PruneCols rule for set operators.
}

func (b *rulePropsBuilder) buildValuesProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	// All columns can potentially be pruned from the Values operator.
	relational.Rule.PruneCols = ev.Logical().Relational.OutputCols
}

func (b *rulePropsBuilder) buildLimitProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	ordering := ev.Private().(props.Ordering).ColSet()

	// Any pruneable input columns can potentially be pruned, as long as they're
	// not used as an ordering column.
	relational.Rule.PruneCols = inputProps.Rule.PruneCols.Difference(ordering)
}

func (b *rulePropsBuilder) buildOffsetProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	ordering := ev.Private().(props.Ordering).ColSet()

	// Any pruneable input columns can potentially be pruned, as long as they're
	// not used as an ordering column.
	relational.Rule.PruneCols = inputProps.Rule.PruneCols.Difference(ordering)
}

func (b *rulePropsBuilder) buildMax1RowProps(ev memo.ExprView) {
	// Don't allow any columns to be pruned, since that would trigger the
	// creation of a wrapper Project around the Max1Row, since there's not
	// a PruneCols rule for Max1Row.
}

func (b *rulePropsBuilder) buildRowNumberProps(ev memo.ExprView) {
	relational := ev.Logical().Relational

	inputProps := ev.Child(0).Logical().Relational
	ordering := ev.Private().(*memo.RowNumberDef).Ordering.ColSet()

	// Any pruneable input columns can potentially be pruned, as long as they're
	// not used as an ordering column. The new row number column cannot be pruned
	// without adding an additional Project operator, so don't add it to the set.
	relational.Rule.PruneCols = inputProps.Rule.PruneCols.Difference(ordering)
}
