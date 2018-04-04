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

// logicalPropsFactory is a helper class that consolidates the code that
// derives a parent expression's logical properties from those of its
// children.
// NOTE: The factory is defined as an empty struct with methods rather than as
//       functions in order to keep the methods grouped and self-contained.
type logicalPropsFactory struct {
	evalCtx *tree.EvalContext
}

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

	case opt.LimitOp:
		return f.constructLimitProps(ev)

	case opt.OffsetOp, opt.Max1RowOp:
		return f.passThroughRelationalProps(ev, 0 /* childIdx */)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.op))
}

func (f logicalPropsFactory) constructScanProps(ev ExprView) LogicalProps {
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

	// TODO: Need actual number of rows.
	if def.Constraint != nil {
		props.Relational.Stats.RowCount = 100
	} else {
		props.Relational.Stats.RowCount = 1000
	}

	// Cap number of rows at limit, if it exists.
	if def.HardLimit > 0 && uint64(def.HardLimit) < props.Relational.Stats.RowCount {
		props.Relational.Stats.RowCount = uint64(def.HardLimit)
	}

	return props
}

func (f logicalPropsFactory) constructSelectProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.lookupChildGroup(0).logical.Relational

	// Inherit output columns from input.
	props.Relational.OutputCols = inputProps.OutputCols

	// Inherit not null columns from input.
	props.Relational.NotNullCols = inputProps.NotNullCols

	// TODO: Need better estimate based on actual filter conditions.
	props.Relational.Stats.RowCount = inputProps.Stats.RowCount / 10

	return props
}

func (f logicalPropsFactory) constructProjectProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.lookupChildGroup(0).logical.Relational

	// Use output columns from projection list.
	props.Relational.OutputCols = opt.ColListToSet(ev.Child(1).Private().(opt.ColList))

	// Inherit not null columns from input.
	props.Relational.NotNullCols = inputProps.NotNullCols
	filterNullCols(props.Relational)

	props.Relational.Stats.RowCount = inputProps.Stats.RowCount

	return props
}

func (f logicalPropsFactory) constructJoinProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	leftProps := ev.lookupChildGroup(0).logical.Relational
	rightProps := ev.lookupChildGroup(1).logical.Relational

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

	// TODO: Need better estimate based on actual on conditions.
	props.Relational.Stats.RowCount = leftProps.Stats.RowCount * rightProps.Stats.RowCount
	if ev.Child(2).Operator() != opt.TrueOp {
		props.Relational.Stats.RowCount /= 10
	}

	return props
}

func (f logicalPropsFactory) constructGroupByProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.lookupChildGroup(0).logical.Relational

	// Output columns are the union of grouping columns with columns from the
	// aggregate projection list.
	groupingColSet := ev.Private().(opt.ColSet)
	props.Relational.OutputCols = groupingColSet
	aggColList := ev.Child(1).Private().(opt.ColList)
	props.Relational.OutputCols.UnionWith(opt.ColListToSet(aggColList))

	// Propagate not null setting from input columns that are being grouped.
	props.Relational.NotNullCols = inputProps.NotNullCols.Copy()
	props.Relational.NotNullCols.IntersectionWith(groupingColSet)

	if groupingColSet.Empty() {
		// Scalar group by.
		props.Relational.Stats.RowCount = 1
	} else {
		// TODO: Need better estimate.
		props.Relational.Stats.RowCount = inputProps.Stats.RowCount / 10
	}

	return props
}

func (f logicalPropsFactory) constructSetProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	leftProps := ev.lookupChildGroup(0).logical.Relational
	rightProps := ev.lookupChildGroup(1).logical.Relational
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

	// TODO: Need better estimate.
	switch ev.Operator() {
	case opt.UnionOp, opt.UnionAllOp:
		props.Relational.Stats.RowCount = leftProps.Stats.RowCount + rightProps.Stats.RowCount

	default:
		props.Relational.Stats.RowCount = (leftProps.Stats.RowCount + rightProps.Stats.RowCount) / 2
	}

	return props
}

func (f logicalPropsFactory) constructValuesProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	// Use output columns that are attached to the values op.
	props.Relational.OutputCols = opt.ColListToSet(ev.Private().(opt.ColList))

	props.Relational.Stats.RowCount = uint64(ev.ChildCount())

	return props
}

func (f logicalPropsFactory) constructLimitProps(ev ExprView) LogicalProps {
	props := LogicalProps{Relational: &RelationalProps{}}

	inputProps := ev.Child(0).Logical().Relational
	limit := ev.Child(1)

	// Start with pass-through props from input.
	*props.Relational = *inputProps

	// Update row count if limit is a constant.
	if limit.Operator() == opt.ConstOp {
		hardLimit := *limit.Private().(*tree.DInt)
		if hardLimit > 0 {
			props.Relational.Stats.RowCount = uint64(hardLimit)
		}
	}

	return props
}

// passThroughRelationalProps returns the relational properties of the given
// child group.
func (f logicalPropsFactory) passThroughRelationalProps(ev ExprView, childIdx int) LogicalProps {
	// Properties are immutable after construction, so just inherit relational
	// props pointer from child.
	return LogicalProps{Relational: ev.lookupChildGroup(childIdx).logical.Relational}
}

func (f logicalPropsFactory) constructScalarProps(ev ExprView) LogicalProps {
	props := LogicalProps{Scalar: &ScalarProps{Type: InferType(ev)}}

	switch ev.Operator() {
	case opt.VariableOp:
		// Variable introduces outer column.
		props.Scalar.OuterCols.Add(int(ev.Private().(opt.ColumnID)))
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

	if props.Scalar.Type == types.Bool {
		cb := constraintsBuilder{md: ev.Metadata(), evalCtx: f.evalCtx}
		props.Scalar.Constraints, props.Scalar.TightConstraints = cb.buildConstraints(ev)
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
