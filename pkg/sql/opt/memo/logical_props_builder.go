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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// logicalPropsBuilder is a helper class that consolidates the code that derives
// a parent expression's logical properties from those of its children.
type logicalPropsBuilder struct {
	evalCtx *tree.EvalContext
}

// buildProps is called by the memo group construction code in order to
// initialize the new group's logical properties.
// NOTE: When deriving properties from children, be sure to keep the child
//       properties immutable by copying them if necessary.
// NOTE: The parent expression is passed as an ExprView for convenient access
//       to children, but certain properties on it are not yet defined (like
//       its logical properties!).
func (f logicalPropsBuilder) buildProps(ev ExprView) LogicalProps {
	if ev.IsRelational() {
		return f.buildRelationalProps(ev)
	}
	return f.buildScalarProps(ev)
}

func (f logicalPropsBuilder) buildRelationalProps(ev ExprView) LogicalProps {
	switch ev.Operator() {
	case opt.ScanOp:
		return f.buildScanProps(ev)

	case opt.SelectOp:
		return f.buildSelectProps(ev)

	case opt.ProjectOp:
		return f.buildProjectProps(ev)

	case opt.ValuesOp:
		return f.buildValuesProps(ev)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		return f.buildJoinProps(ev)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		return f.buildSetProps(ev)

	case opt.GroupByOp:
		return f.buildGroupByProps(ev)

	case opt.LimitOp:
		return f.buildLimitProps(ev)

	case opt.OffsetOp:
		return f.buildOffsetProps(ev)

	case opt.Max1RowOp:
		return f.buildMax1RowProps(ev)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.op))
}

func (f logicalPropsBuilder) buildScanProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	md := ev.Metadata()
	def := ev.Private().(*ScanOpDef)
	tab := md.Table(def.Table)

	// Scan output columns are stored in the definition.
	props.Relational.OutputCols = def.Cols

	// Initialize not-NULL columns from the table schema.
	for i := 0; i < tab.ColumnCount(); i++ {
		if !tab.Column(i).IsNullable() {
			colID := md.TableColumn(def.Table, i)
			if def.Cols.Contains(int(colID)) {
				props.Relational.NotNullCols.Add(int(colID))
			}
		}
	}

	// Initialize weak keys from the table schema.
	props.Relational.WeakKeys = md.TableWeakKeys(def.Table)
	filterWeakKeys(props.Relational)

	props.Relational.Stats.initScan(f.evalCtx, md, def, tab)

	return props
}

func (f logicalPropsBuilder) buildSelectProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.childGroup(0).logical.Relational
	filterProps := ev.childGroup(1).logical.Scalar

	// Inherit input properties as starting point.
	*props.Relational = *inputProps

	// Any outer columns from the filter that are not bound by the input columns
	// are outer columns for the Select expression, in addition to any outer
	// columns inherited from the input expression.
	if !filterProps.OuterCols.SubsetOf(inputProps.OutputCols) {
		props.Relational.OuterCols = filterProps.OuterCols.Difference(inputProps.OutputCols)
		props.Relational.OuterCols.UnionWith(inputProps.OuterCols)
	}

	props.Relational.Stats.initSelect(f.evalCtx, ev.Child(1), &inputProps.Stats)

	return props
}

func (f logicalPropsBuilder) buildProjectProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.childGroup(0).logical.Relational
	projectionProps := ev.childGroup(1).logical.Scalar

	// Use output columns from projection list.
	props.Relational.OutputCols = opt.ColListToSet(ev.Child(1).Private().(opt.ColList))

	// Inherit not null columns from input, but only use those that are also
	// output columns.
	props.Relational.NotNullCols = inputProps.NotNullCols
	filterNullCols(props.Relational)

	// Any outer columns from the projection list that are not bound by the input
	// columns are outer columns from the Project expression, in addition to any
	// outer columns inherited from the input expression.
	if !projectionProps.OuterCols.SubsetOf(inputProps.OutputCols) {
		props.Relational.OuterCols = projectionProps.OuterCols.Difference(inputProps.OutputCols)
	}
	props.Relational.OuterCols.UnionWith(inputProps.OuterCols)

	// Inherit weak keys that are composed entirely of output columns.
	props.Relational.WeakKeys = inputProps.WeakKeys
	filterWeakKeys(props.Relational)

	props.Relational.Stats.initProject(&inputProps.Stats, &props.Relational.OutputCols)

	return props
}

func (f logicalPropsBuilder) buildJoinProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	leftProps := ev.childGroup(0).logical.Relational
	rightProps := ev.childGroup(1).logical.Relational
	onProps := ev.childGroup(2).logical.Scalar

	// Output columns are union of columns from left and right inputs, except
	// in case of semi and anti joins, which only project the left columns.
	props.Relational.OutputCols = leftProps.OutputCols.Copy()
	switch ev.Operator() {
	case opt.SemiJoinOp, opt.AntiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:

	default:
		props.Relational.OutputCols.UnionWith(rightProps.OutputCols)
	}

	// Left/full outer joins can result in right columns becoming null.
	// Otherwise, propagate not null setting from right child.
	switch ev.Operator() {
	case opt.LeftJoinOp, opt.FullJoinOp, opt.LeftJoinApplyOp, opt.FullJoinApplyOp,
		opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:

	default:
		props.Relational.NotNullCols = rightProps.NotNullCols.Copy()
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch ev.Operator() {
	case opt.RightJoinOp, opt.FullJoinOp, opt.RightJoinApplyOp, opt.FullJoinApplyOp:

	default:
		props.Relational.NotNullCols.UnionWith(leftProps.NotNullCols)
	}

	// Any outer columns from the filter that are not bound by the input columns
	// are outer columns for the Join expression, in addition to any outer columns
	// inherited from the input expressions.
	inputCols := leftProps.OutputCols.Union(rightProps.OutputCols)
	if !onProps.OuterCols.SubsetOf(inputCols) {
		props.Relational.OuterCols = onProps.OuterCols.Difference(inputCols)
	}
	if ev.IsJoinApply() {
		// Outer columns of right side of apply join can be bound by output
		// columns of left side of apply join.
		if !rightProps.OuterCols.SubsetOf(leftProps.OutputCols) {
			props.Relational.OuterCols.UnionWith(rightProps.OuterCols.Difference(leftProps.OutputCols))
		}
	} else {
		props.Relational.OuterCols.UnionWith(rightProps.OuterCols)
	}
	props.Relational.OuterCols.UnionWith(leftProps.OuterCols)

	// TODO(andyk): Need to derive weak keys for joins, for example when weak
	//              keys on both sides are equivalent cols.

	props.Relational.Stats.initJoin(
		ev.Operator(), &leftProps.Stats, &rightProps.Stats, ev.Child(2),
	)

	return props
}

func (f logicalPropsBuilder) buildGroupByProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.childGroup(0).logical.Relational
	aggProps := ev.childGroup(1).logical.Scalar

	// Output columns are the union of grouping columns with columns from the
	// aggregate projection list.
	groupingColSet := ev.Private().(opt.ColSet)
	aggColList := ev.Child(1).Private().(opt.ColList)
	props.Relational.OutputCols = groupingColSet.Union(opt.ColListToSet(aggColList))

	// Propagate not null setting from input columns that are being grouped.
	props.Relational.NotNullCols = inputProps.NotNullCols.Copy()
	props.Relational.NotNullCols.IntersectionWith(groupingColSet)

	// Any outer columns from aggregation expressions that are not bound by the
	// input columns are outer columns.
	props.Relational.OuterCols = aggProps.OuterCols.Difference(inputProps.OutputCols)
	props.Relational.OuterCols.UnionWith(inputProps.OuterCols)

	// Scalar group by has no grouping columns and always a single row.
	if groupingColSet.Empty() {
		// Any combination of columns is a weak key when there is one row.
		props.Relational.WeakKeys = opt.WeakKeys{groupingColSet}
	} else {
		// The grouping columns always form a key because the GroupBy operation
		// eliminates all duplicates. The result WeakKeys property either contains
		// only the grouping column set, or else it contains one or more weak keys
		// that are strict subsets of the grouping column set. This is because
		// the grouping column set contains every output column (except aggregate
		// columns, which aren't relevant since they're newly synthesized).
		if inputProps.WeakKeys.ContainsSubsetOf(groupingColSet) {
			props.Relational.WeakKeys = inputProps.WeakKeys
			filterWeakKeys(props.Relational)
		} else {
			props.Relational.WeakKeys = opt.WeakKeys{groupingColSet}
		}
	}

	props.Relational.Stats.initGroupBy(
		&inputProps.Stats, &groupingColSet, &props.Relational.OutputCols,
	)

	return props
}

func (f logicalPropsBuilder) buildSetProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	leftProps := ev.childGroup(0).logical.Relational
	rightProps := ev.childGroup(1).logical.Relational
	colMap := *ev.Private().(*SetOpColMap)
	if len(colMap.Out) != len(colMap.Left) || len(colMap.Out) != len(colMap.Right) {
		panic(fmt.Errorf("lists in SetOpColMap are not all the same length. new:%d, left:%d, right:%d",
			len(colMap.Out), len(colMap.Left), len(colMap.Right)))
	}

	// Set the new output columns.
	props.Relational.OutputCols = opt.ColListToSet(colMap.Out)

	// Columns have to be not-null on both sides to be not-null in result.
	// colMap matches columns on the left and right sides of the operator
	// with the output columns, since OutputCols are not ordered and may
	// not correspond to each other.
	for i := range colMap.Out {
		if leftProps.NotNullCols.Contains(int((colMap.Left)[i])) &&
			rightProps.NotNullCols.Contains(int((colMap.Right)[i])) {
			props.Relational.NotNullCols.Add(int((colMap.Out)[i]))
		}
	}

	// Outer columns from either side are outer columns for set operation.
	props.Relational.OuterCols = leftProps.OuterCols.Union(rightProps.OuterCols)

	props.Relational.Stats.initSetOp(ev.Operator(), &leftProps.Stats, &rightProps.Stats, &colMap)

	return props
}

func (f logicalPropsBuilder) buildValuesProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	// Use output columns that are attached to the values op.
	props.Relational.OutputCols = opt.ColListToSet(ev.Private().(opt.ColList))

	// Union outer columns from all row expressions.
	for i := 0; i < ev.ChildCount(); i++ {
		props.Relational.OuterCols.UnionWith(ev.childGroup(i).logical.Scalar.OuterCols)
	}

	props.Relational.Stats.initValues(ev, &props.Relational.OutputCols)

	return props
}

func (f logicalPropsBuilder) buildLimitProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.Child(0).Logical().Relational
	limit := ev.Child(1)
	limitProps := limit.Logical().Scalar

	// Start with pass-through props from input.
	*props.Relational = *inputProps

	// Inherit outer columns from limit expression.
	if !limitProps.OuterCols.Empty() {
		props.Relational.OuterCols = limitProps.OuterCols.Union(inputProps.OuterCols)
	}

	props.Relational.Stats.initLimit(limit, &inputProps.Stats)

	return props
}

func (f logicalPropsBuilder) buildOffsetProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.Child(0).Logical().Relational
	offsetProps := ev.Child(1).Logical().Scalar

	// Start with pass-through props from input.
	*props.Relational = *inputProps

	// Inherit outer columns from offset expression.
	if !offsetProps.OuterCols.Empty() {
		props.Relational.OuterCols = offsetProps.OuterCols.Union(inputProps.OuterCols)
	}

	return props
}

func (f logicalPropsBuilder) buildMax1RowProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.Child(0).Logical().Relational

	// Start with pass-through props from input.
	*props.Relational = *inputProps

	props.Relational.Stats.initMax1Row(&inputProps.Stats)

	return props
}

func (f logicalPropsBuilder) buildScalarProps(ev ExprView) LogicalProps {
	props := LogicalProps{Scalar: &ScalarProps{Type: InferType(ev)}}

	switch ev.Operator() {
	case opt.VariableOp:
		// Variable introduces outer column.
		props.Scalar.OuterCols.Add(int(ev.Private().(opt.ColumnID)))
		return props

	case opt.SubqueryOp:
		inputProps := ev.childGroup(0).logical.Relational
		projectionProps := ev.childGroup(1).logical.Scalar
		props.Scalar.OuterCols = projectionProps.OuterCols.Difference(inputProps.OutputCols)
		props.Scalar.OuterCols.UnionWith(inputProps.OuterCols)
		return props
	}

	// By default, union outer cols from all children, both relational and scalar.
	for i := 0; i < ev.ChildCount(); i++ {
		logical := &ev.childGroup(i).logical
		if logical.Scalar != nil {
			props.Scalar.OuterCols.UnionWith(logical.Scalar.OuterCols)
		} else {
			props.Scalar.OuterCols.UnionWith(logical.Relational.OuterCols)
		}
	}

	if props.Scalar.Type == types.Bool {
		cb := constraintsBuilder{md: ev.Metadata(), evalCtx: f.evalCtx}
		props.Scalar.Constraints, props.Scalar.TightConstraints = cb.buildConstraints(ev)
	}
	return props
}

// filterNullCols ensures that the set of null columns is a subset of the output
// columns. It respects immutability by making a copy of the null columns if
// they need to be updated.
func filterNullCols(props *RelationalProps) {
	if !props.NotNullCols.SubsetOf(props.OutputCols) {
		props.NotNullCols = props.NotNullCols.Intersection(props.OutputCols)
	}
}

// filterWeakKeys ensures that each weak key is a subset of the output columns.
// It respects immutability by making a copy of the weak keys if they need to be
// updated.
func filterWeakKeys(props *RelationalProps) {
	var filtered opt.WeakKeys
	for i, weakKey := range props.WeakKeys {
		// Discard weak keys that have columns that are not part of the output
		// column set.
		if !weakKey.SubsetOf(props.OutputCols) {
			if filtered == nil {
				filtered = make(opt.WeakKeys, i, len(props.WeakKeys)-1)
				copy(filtered, props.WeakKeys[:i])
			}
		} else {
			if filtered != nil {
				filtered = append(filtered, weakKey)
			}
		}
	}
	if filtered != nil {
		props.WeakKeys = filtered
	}
}
