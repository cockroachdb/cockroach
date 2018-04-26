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
	"math"

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
func (b logicalPropsBuilder) buildProps(ev ExprView) LogicalProps {
	if ev.IsRelational() {
		return b.buildRelationalProps(ev)
	}
	return b.buildScalarProps(ev)
}

func (b logicalPropsBuilder) buildRelationalProps(ev ExprView) LogicalProps {
	switch ev.Operator() {
	case opt.ScanOp:
		return b.buildScanProps(ev)

	case opt.SelectOp:
		return b.buildSelectProps(ev)

	case opt.ProjectOp:
		return b.buildProjectProps(ev)

	case opt.ValuesOp:
		return b.buildValuesProps(ev)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		return b.buildJoinProps(ev)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		return b.buildSetProps(ev)

	case opt.GroupByOp:
		return b.buildGroupByProps(ev)

	case opt.LimitOp:
		return b.buildLimitProps(ev)

	case opt.OffsetOp:
		return b.buildOffsetProps(ev)

	case opt.Max1RowOp:
		return b.buildMax1RowProps(ev)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.op))
}

func (b logicalPropsBuilder) buildScanProps(ev ExprView) LogicalProps {
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

	// Don't bother setting cardinality from scan's HardLimit or Constraint,
	// since those are created by exploration patterns and won't ever be the
	// basis for the logical props on a newly created memo group.
	props.Relational.Cardinality = AnyCardinality

	props.Relational.Stats.initScan(b.evalCtx, md, def, tab)

	return props
}

func (b logicalPropsBuilder) buildSelectProps(ev ExprView) LogicalProps {
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

	// Select filter can filter any or all rows.
	props.Relational.Cardinality = inputProps.Cardinality.AsLowAs(0)

	props.Relational.Stats.initSelect(b.evalCtx, ev.Child(1), &inputProps.Stats)

	return props
}

func (b logicalPropsBuilder) buildProjectProps(ev ExprView) LogicalProps {
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

	// Inherit cardinality from input.
	props.Relational.Cardinality = inputProps.Cardinality

	props.Relational.Stats.initProject(&inputProps.Stats, &props.Relational.OutputCols)

	return props
}

func (b logicalPropsBuilder) buildJoinProps(ev ExprView) LogicalProps {
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

	// Calculate cardinality of each join type.
	props.Relational.Cardinality = calcJoinCardinality(
		ev, leftProps.Cardinality, rightProps.Cardinality,
	)

	props.Relational.Stats.initJoin(
		ev.Operator(), &leftProps.Stats, &rightProps.Stats, ev.Child(2),
	)

	return props
}

func (b logicalPropsBuilder) buildGroupByProps(ev ExprView) LogicalProps {
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

	if groupingColSet.Empty() {
		// Scalar GroupBy returns exactly one row.
		props.Relational.Cardinality = OneCardinality
	} else {
		// GroupBy acts like a filter, never returning more rows than the input
		// has. However, if the input has at least one row, then at least one row
		// will also be returned by GroupBy.
		props.Relational.Cardinality = inputProps.Cardinality.AsLowAs(1)
	}

	props.Relational.Stats.initGroupBy(
		&inputProps.Stats, &groupingColSet, &props.Relational.OutputCols,
	)

	return props
}

func (b logicalPropsBuilder) buildSetProps(ev ExprView) LogicalProps {
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

	// Calculate cardinality of the set operator.
	props.Relational.Cardinality = calcSetCardinality(
		ev, leftProps.Cardinality, rightProps.Cardinality,
	)

	props.Relational.Stats.initSetOp(ev.Operator(), &leftProps.Stats, &rightProps.Stats, &colMap)

	return props
}

func (b logicalPropsBuilder) buildValuesProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	// Use output columns that are attached to the values op.
	props.Relational.OutputCols = opt.ColListToSet(ev.Private().(opt.ColList))

	// Union outer columns from all row expressions.
	for i := 0; i < ev.ChildCount(); i++ {
		props.Relational.OuterCols.UnionWith(ev.childGroup(i).logical.Scalar.OuterCols)
	}

	// Cardinality is number of tuples in the Values operator.
	card := uint32(ev.ChildCount())
	props.Relational.Cardinality = Cardinality{Min: card, Max: card}

	props.Relational.Stats.initValues(ev, &props.Relational.OutputCols)

	return props
}

func (b logicalPropsBuilder) buildLimitProps(ev ExprView) LogicalProps {
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

	// Limit puts a cap on the number of rows returned by input.
	if limit.Operator() == opt.ConstOp {
		constLimit := int64(*limit.Private().(*tree.DInt))
		if constLimit <= 0 {
			props.Relational.Cardinality = ZeroCardinality
		} else if constLimit < math.MaxUint32 {
			props.Relational.Cardinality = props.Relational.Cardinality.AtMost(uint32(constLimit))
		}
	}

	props.Relational.Stats.initLimit(limit, &inputProps.Stats)

	return props
}

func (b logicalPropsBuilder) buildOffsetProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.Child(0).Logical().Relational
	offset := ev.Child(1)
	offsetProps := offset.Logical().Scalar

	// Start with pass-through props from input.
	*props.Relational = *inputProps

	// Inherit outer columns from offset expression.
	if !offsetProps.OuterCols.Empty() {
		props.Relational.OuterCols = offsetProps.OuterCols.Union(inputProps.OuterCols)
	}

	// Offset decreases the number of rows that are passed through from input.
	if offset.Operator() == opt.ConstOp {
		constOffset := int64(*offset.Private().(*tree.DInt))
		if constOffset > 0 {
			if constOffset > math.MaxUint32 {
				constOffset = math.MaxUint32
			}
			props.Relational.Cardinality = inputProps.Cardinality.Skip(uint32(constOffset))
		}
	}

	return props
}

func (b logicalPropsBuilder) buildMax1RowProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.Child(0).Logical().Relational

	// Start with pass-through props from input.
	*props.Relational = *inputProps

	// Max1Row ensures that zero or one row is returned by input.
	props.Relational.Cardinality = props.Relational.Cardinality.AtMost(1)

	props.Relational.Stats.initMax1Row(&inputProps.Stats)

	return props
}

func (b logicalPropsBuilder) buildScalarProps(ev ExprView) LogicalProps {
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
		cb := constraintsBuilder{md: ev.Metadata(), evalCtx: b.evalCtx}
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

func calcJoinCardinality(ev ExprView, left, right Cardinality) Cardinality {
	var card Cardinality
	switch ev.Operator() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Semi/Anti join cardinality never exceeds left input cardinality, and
		// allows zero rows.
		return left.AsLowAs(0)
	}

	// Other join types can return up to cross product of rows.
	card = left.Product(right)

	// Outer joins return minimum number of rows, depending on type.
	switch ev.Operator() {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp, opt.FullJoinOp, opt.FullJoinApplyOp:
		card = card.AtLeast(left.Min)
	}
	switch ev.Operator() {
	case opt.RightJoinOp, opt.RightJoinApplyOp, opt.FullJoinOp, opt.FullJoinApplyOp:
		card = card.AtLeast(right.Min)
	}

	// Apply filter to cardinality.
	if ev.Child(2).Operator() == opt.TrueOp {
		return card
	}
	return card.AsLowAs(0)
}

func calcSetCardinality(ev ExprView, left, right Cardinality) Cardinality {
	var card Cardinality
	switch ev.Operator() {
	case opt.UnionOp, opt.UnionAllOp:
		// Add cardinality of left and right inputs.
		card = left.Add(right)

	case opt.IntersectOp, opt.IntersectAllOp:
		// Use minimum of left and right Max cardinality.
		card = Cardinality{Min: 0, Max: left.Max}
		card = card.AtMost(right.Max)

	case opt.ExceptOp, opt.ExceptAllOp:
		// Use left Max cardinality.
		card = Cardinality{Min: 0, Max: left.Max}
		if left.Min > right.Max {
			card.Min = left.Min - right.Max
		}
	}
	switch ev.Operator() {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// Removing distinct values results in at least one row if input has at
		// least one row.
		card = card.AsLowAs(1)
	}
	return card
}
