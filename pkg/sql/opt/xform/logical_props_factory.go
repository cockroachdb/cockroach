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

package xform

import (
	"fmt"
)

// logicalPropsFactory is a helper class that consolidates the code that
// derives a parent expression's logical properties from those of its
// children.
type logicalPropsFactory struct {
	mem *memo
}

func (f *logicalPropsFactory) init(mem *memo) {
	f.mem = mem
}

// constructProps is called by the memo group construction code in order to
// initialize the new group's logical properties.
// NOTE: When deriving properties from children, be sure to keep the child
//       properties immutable by copying them if necessary.
// NOTE: The parent expression is passed as an ExprView for convenient access
//       to children, but certain properties on it are not yet defined (like
//       its logical properties!).
func (f *logicalPropsFactory) constructProps(ev *ExprView) LogicalProps {
	if ev.IsRelational() {
		return f.constructRelationalProps(ev)
	}

	return f.constructScalarProps(ev)
}

func (f *logicalPropsFactory) constructRelationalProps(ev *ExprView) LogicalProps {
	switch ev.Operator() {
	case ScanOp:
		return f.constructScanProps(ev)

	case SelectOp:
		return f.constructSelectProps(ev)

	case ProjectOp:
		return f.constructProjectProps(ev)

	case ValuesOp:
		return f.constructValuesProps(ev)

	case InnerJoinOp, LeftJoinOp, RightJoinOp, FullJoinOp,
		SemiJoinOp, AntiJoinOp, InnerJoinApplyOp, LeftJoinApplyOp,
		RightJoinApplyOp, FullJoinApplyOp, SemiJoinApplyOp, AntiJoinApplyOp:
		return f.constructJoinProps(ev)

	case UnionOp:
		return f.constructSetProps(ev)

	case GroupByOp:
		return f.constructGroupByProps(ev)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.op))
}

func (f *logicalPropsFactory) constructScanProps(ev *ExprView) (props LogicalProps) {
	props.Relational = &RelationalProps{}

	tblIndex := ev.Private().(TableIndex)
	tbl := f.mem.metadata.Table(tblIndex)

	// A table's output column indexes are contiguous.
	props.Relational.OutputCols.AddRange(int(tblIndex), int(tblIndex)+tbl.NumColumns()-1)

	// Initialize not-NULL columns from the table schema.
	for i := 0; i < tbl.NumColumns(); i++ {
		if !tbl.Column(i).IsNullable() {
			props.Relational.NotNullCols.Add(int(tblIndex) + i)
		}
	}

	return
}

func (f *logicalPropsFactory) constructSelectProps(ev *ExprView) (props LogicalProps) {
	props.Relational = &RelationalProps{}

	inputProps := f.mem.lookupGroup(ev.ChildGroup(0)).logical

	// Inherit output columns from input.
	props.Relational.OutputCols = inputProps.Relational.OutputCols

	// Inherit not null columns from input.
	props.Relational.NotNullCols = inputProps.Relational.NotNullCols

	return
}

func (f *logicalPropsFactory) constructProjectProps(ev *ExprView) (props LogicalProps) {
	props.Relational = &RelationalProps{}

	inputProps := f.mem.lookupGroup(ev.ChildGroup(0)).logical

	// Use output columns from projection list.
	projections := ev.Child(1)
	props.Relational.OutputCols = *projections.Private().(*ColSet)

	// Inherit not null columns from input.
	props.Relational.NotNullCols = inputProps.Relational.NotNullCols
	filterNullCols(props.Relational)

	return
}

func (f *logicalPropsFactory) constructJoinProps(ev *ExprView) (props LogicalProps) {
	props.Relational = &RelationalProps{}

	leftProps := f.mem.lookupGroup(ev.ChildGroup(0)).logical
	rightProps := f.mem.lookupGroup(ev.ChildGroup(1)).logical

	// Output columns are union of columns from left and right inputs, except
	// in case of semi and anti joins, which only project the left columns.
	props.Relational.OutputCols = leftProps.Relational.OutputCols.Copy()
	switch ev.Operator() {
	case SemiJoinOp, AntiJoinOp, SemiJoinApplyOp, AntiJoinApplyOp:

	default:
		props.Relational.OutputCols.UnionWith(rightProps.Relational.OutputCols)
	}

	// Left/full outer joins can result in right columns becoming null.
	// Otherwise, propagate not null setting from right child.
	switch ev.Operator() {
	case LeftJoinOp, FullJoinOp, LeftJoinApplyOp, FullJoinApplyOp,
		SemiJoinOp, SemiJoinApplyOp, AntiJoinOp, AntiJoinApplyOp:

	default:
		props.Relational.NotNullCols = rightProps.Relational.NotNullCols.Copy()
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch ev.Operator() {
	case RightJoinOp, FullJoinOp, RightJoinApplyOp, FullJoinApplyOp:

	default:
		props.Relational.NotNullCols.UnionWith(leftProps.Relational.NotNullCols)
	}

	return
}

func (f *logicalPropsFactory) constructGroupByProps(ev *ExprView) (props LogicalProps) {
	props.Relational = &RelationalProps{}

	// Output columns are union of columns from grouping and aggregate
	// projection lists.
	groupings := ev.Child(1)
	props.Relational.OutputCols = groupings.Private().(*ColSet).Copy()
	agg := ev.Child(2)
	props.Relational.OutputCols.UnionWith(*agg.Private().(*ColSet))

	return
}

func (f *logicalPropsFactory) constructSetProps(ev *ExprView) (props LogicalProps) {
	props.Relational = &RelationalProps{}

	leftProps := f.mem.lookupGroup(ev.ChildGroup(0)).logical
	rightProps := f.mem.lookupGroup(ev.ChildGroup(1)).logical
	colMap := *ev.Private().(*ColMap)

	// Use left input's output columns.
	props.Relational.OutputCols = leftProps.Relational.OutputCols

	// Columns have to be not-null on both sides to be not-null in result.
	// colMap matches columns on the left side of the operator with columns on
	// the right side, since OutputCols are not ordered and may not correspond
	// to each other.
	for leftIndex, rightIndex := range colMap {
		if !leftProps.Relational.NotNullCols.Contains(int(leftIndex)) {
			continue
		}
		if !rightProps.Relational.NotNullCols.Contains(int(rightIndex)) {
			continue
		}
		props.Relational.NotNullCols.Add(int(leftIndex))
	}

	return
}

func (f *logicalPropsFactory) constructValuesProps(ev *ExprView) (props LogicalProps) {
	props.Relational = &RelationalProps{}

	// Use output columns that are attached to the values op.
	props.Relational.OutputCols = *ev.Private().(*ColSet)
	return
}

func (f *logicalPropsFactory) constructScalarProps(ev *ExprView) LogicalProps {
	return LogicalProps{}
}

// filterNullCols will ensure that the set of null columns is a subset of the
// output columns. It respects immutability by making a copy of the null
// columns if they need to be updated.
func filterNullCols(props *RelationalProps) {
	if !props.NotNullCols.SubsetOf(props.OutputCols) {
		props.NotNullCols = props.NotNullCols.Intersection(props.OutputCols)
	}
}
