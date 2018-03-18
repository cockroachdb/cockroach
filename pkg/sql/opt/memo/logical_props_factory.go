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
)

// logicalPropsFactory is a helper class that consolidates the code that
// derives a parent expression's logical properties from those of its
// children.
// NOTE: The factory is defined as an empty struct with methods rather than as
//       functions in order to keep the methods grouped and self-contained.
type logicalPropsFactory struct{}

// constructProps is called by the memo group construction code in order to
// initialize the new group's logical properties.
// NOTE: When deriving properties from children, be sure to keep the child
//       properties immutable by copying them if necessary.
// NOTE: The parent expression is passed as an ExprView for convenient access
//       to children, but certain properties on it are not yet defined (like
//       its logical properties!).
func (f logicalPropsFactory) constructProps(ev ExprView) LogicalProps {
	if ev.IsRelational() {
		return f.constructRelationalProps(ev)
	}
	return f.constructScalarProps(ev)
}

func (f logicalPropsFactory) constructRelationalProps(ev ExprView) LogicalProps {
	switch ev.Operator() {
	case opt.ScanOp:
		return f.constructScanProps(ev)

	case opt.SelectOp:
		return f.constructSelectProps(ev)

	case opt.ProjectOp:
		return f.constructProjectProps(ev)

	case opt.ValuesOp:
		return f.constructValuesProps(ev)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		return f.constructJoinProps(ev)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		return f.constructSetProps(ev)

	case opt.GroupByOp:
		return f.constructGroupByProps(ev)

	case opt.LimitOp, opt.OffsetOp:
		return f.passThroughRelationalProps(ev, 0 /* inputChildIdx */)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.op))
}

func (f logicalPropsFactory) constructScanProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	md := ev.Metadata()
	def := ev.Private().(*ScanOpDef)
	tbl := md.Table(def.Table)

	// Scan output columns are stored in the definition.
	props.Relational.OutputCols = def.Cols

	// Initialize not-NULL columns from the table schema.
	for i := 0; i < tbl.ColumnCount(); i++ {
		if !tbl.Column(i).IsNullable() {
			colIndex := md.TableColumn(def.Table, i)
			if def.Cols.Contains(int(colIndex)) {
				props.Relational.NotNullCols.Add(int(colIndex))
			}
		}
	}

	return props
}

func (f logicalPropsFactory) constructSelectProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.lookupChildGroup(0).logical

	// Inherit output columns from input.
	props.Relational.OutputCols = inputProps.Relational.OutputCols

	// Inherit not null columns from input.
	props.Relational.NotNullCols = inputProps.Relational.NotNullCols

	return props
}

func (f logicalPropsFactory) constructProjectProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.lookupChildGroup(0).logical

	// Use output columns from projection list.
	props.Relational.OutputCols = opt.ColListToSet(*ev.Child(1).Private().(*opt.ColList))

	// Inherit not null columns from input.
	props.Relational.NotNullCols = inputProps.Relational.NotNullCols
	filterNullCols(props.Relational)

	return props
}

func (f logicalPropsFactory) constructJoinProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	leftProps := ev.lookupChildGroup(0).logical
	rightProps := ev.lookupChildGroup(1).logical

	// Output columns are union of columns from left and right inputs, except
	// in case of semi and anti joins, which only project the left columns.
	props.Relational.OutputCols = leftProps.Relational.OutputCols.Copy()
	switch ev.Operator() {
	case opt.SemiJoinOp, opt.AntiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:

	default:
		props.Relational.OutputCols.UnionWith(rightProps.Relational.OutputCols)
	}

	// Left/full outer joins can result in right columns becoming null.
	// Otherwise, propagate not null setting from right child.
	switch ev.Operator() {
	case opt.LeftJoinOp, opt.FullJoinOp, opt.LeftJoinApplyOp, opt.FullJoinApplyOp,
		opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:

	default:
		props.Relational.NotNullCols = rightProps.Relational.NotNullCols.Copy()
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch ev.Operator() {
	case opt.RightJoinOp, opt.FullJoinOp, opt.RightJoinApplyOp, opt.FullJoinApplyOp:

	default:
		props.Relational.NotNullCols.UnionWith(leftProps.Relational.NotNullCols)
	}

	return props
}

func (f logicalPropsFactory) constructGroupByProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	// Output columns are the union of grouping columns with columns from the
	// aggregate projection list.
	groupingColSet := *ev.Private().(*opt.ColSet)
	props.Relational.OutputCols = groupingColSet
	aggColList := *ev.Child(1).Private().(*opt.ColList)
	props.Relational.OutputCols.UnionWith(opt.ColListToSet(aggColList))

	// Propagate not null setting from grouping columns.
	props.Relational.NotNullCols = ev.Child(0).Logical().Relational.NotNullCols.Copy()
	props.Relational.NotNullCols.IntersectionWith(groupingColSet)

	return props
}

func (f logicalPropsFactory) constructSetProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	leftProps := ev.lookupChildGroup(0).logical
	rightProps := ev.lookupChildGroup(1).logical
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
		if leftProps.Relational.NotNullCols.Contains(int((colMap.Left)[i])) &&
			rightProps.Relational.NotNullCols.Contains(int((colMap.Right)[i])) {
			props.Relational.NotNullCols.Add(int((colMap.Out)[i]))
		}
	}

	return props
}

func (f logicalPropsFactory) constructValuesProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	// Use output columns that are attached to the values op.
	props.Relational.OutputCols = opt.ColListToSet(*ev.Private().(*opt.ColList))
	return props
}

// passThroughRelationalProps returns a copy of the relational properties of the
// given child group.
func (f logicalPropsFactory) passThroughRelationalProps(
	ev ExprView, inputChildIdx int,
) LogicalProps {
	inputProps := ev.lookupChildGroup(inputChildIdx).logical
	relPropsCopy := *inputProps.Relational
	return LogicalProps{Relational: &relPropsCopy}
}

func (f logicalPropsFactory) constructScalarProps(ev ExprView) LogicalProps {
	props := LogicalProps{Scalar: &ScalarProps{Type: InferType(ev)}}

	switch ev.Operator() {
	case opt.VariableOp:
		// Variable introduces outer column.
		props.Scalar.OuterCols.Add(int(ev.Private().(opt.ColumnIndex)))
		return props
	}

	// By default, union outer cols from all children, both relational and scalar.
	for i := 0; i < ev.ChildCount(); i++ {
		logical := &ev.lookupChildGroup(i).logical
		if logical.Scalar != nil {
			props.Scalar.OuterCols.UnionWith(logical.Scalar.OuterCols)
		} else {
			props.Scalar.OuterCols.UnionWith(logical.Relational.OuterCols)
		}
	}
	return props
}

// filterNullCols will ensure that the set of null columns is a subset of the
// output columns. It respects immutability by making a copy of the null
// columns if they need to be updated.
func filterNullCols(props *RelationalProps) {
	if !props.NotNullCols.SubsetOf(props.OutputCols) {
		props.NotNullCols = props.NotNullCols.Intersection(props.OutputCols)
	}
}
