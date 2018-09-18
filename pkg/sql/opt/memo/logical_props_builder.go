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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

var fdAnnID = opt.NewTableAnnID()

// logicalPropsBuilder is a helper class that consolidates the code that derives
// a parent expression's logical properties from those of its children.
type logicalPropsBuilder struct {
	evalCtx         *tree.EvalContext
	sb              statisticsBuilder
	relationalAlloc []props.Relational
	scalarAlloc     []props.Scalar
}

// buildProps is called by the memo group construction code in order to
// initialize the new group's logical properties.
// NOTE: When deriving properties from children, be sure to keep the child
//       properties immutable by copying them if necessary.
// NOTE: The parent expression is passed as an ExprView for convenient access
//       to children, but certain properties on it are not yet defined (like
//       its logical properties!).
func (b *logicalPropsBuilder) buildProps(evalCtx *tree.EvalContext, ev ExprView) props.Logical {
	b.evalCtx = evalCtx
	if ev.IsRelational() {
		return b.buildRelationalProps(ev)
	}
	return b.buildScalarProps(ev)
}

func (b *logicalPropsBuilder) buildRelationalProps(ev ExprView) props.Logical {
	var logical props.Logical

	switch ev.Operator() {
	case opt.ScanOp:
		logical = b.buildScanProps(ev)

	case opt.VirtualScanOp:
		logical = b.buildVirtualScanProps(ev)

	case opt.SelectOp:
		logical = b.buildSelectProps(ev)

	case opt.ProjectOp:
		logical = b.buildProjectProps(ev)

	case opt.ValuesOp:
		logical = b.buildValuesProps(ev)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp,
		opt.LookupJoinOp:
		logical = b.buildJoinProps(ev)

	case opt.IndexJoinOp:
		logical = b.buildIndexJoinProps(ev)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		logical = b.buildSetProps(ev)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		logical = b.buildGroupByProps(ev)

	case opt.LimitOp:
		logical = b.buildLimitProps(ev)

	case opt.OffsetOp:
		logical = b.buildOffsetProps(ev)

	case opt.Max1RowOp:
		logical = b.buildMax1RowProps(ev)

	case opt.ExplainOp:
		logical = b.buildExplainProps(ev)

	case opt.ShowTraceForSessionOp:
		logical = b.buildShowTraceProps(ev)

	case opt.RowNumberOp:
		logical = b.buildRowNumberProps(ev)

	case opt.ZipOp:
		logical = b.buildZipProps(ev)

	default:
		panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.op))
	}

	// If CanHaveSideEffects is true for any child, it's true for the expression.
	for i, n := 0, ev.ChildCount(); i < n; i++ {
		childLogical := ev.childGroup(i).logical
		if childLogical.CanHaveSideEffects() {
			logical.Relational.CanHaveSideEffects = true
		}
		if childLogical.HasPlaceholder() {
			logical.Relational.HasPlaceholder = true
		}
	}

	return logical
}

func (b *logicalPropsBuilder) buildScanProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	md := ev.Metadata()
	def := ev.Private().(*ScanOpDef)
	hardLimit := def.HardLimit.RowCount()

	// Output Columns
	// --------------
	// Scan output columns are stored in the definition.
	relational.OutputCols = def.Cols

	// Not Null Columns
	// ----------------
	// Initialize not-NULL columns from the table schema.
	relational.NotNullCols = tableNotNullCols(md, def.Table)
	if def.Constraint != nil {
		relational.NotNullCols.UnionWith(def.Constraint.ExtractNotNullCols(b.evalCtx))
	}
	relational.NotNullCols.IntersectionWith(relational.OutputCols)

	// Outer Columns
	// -------------
	// Scan operator never has outer columns.

	// Functional Dependencies
	// -----------------------
	// Check the hard limit to determine whether there is at most one row. Note
	// that def.HardLimit = 0 indicates there is no known limit.
	if hardLimit == 1 {
		relational.FuncDeps.MakeMax1Row(relational.OutputCols)
	} else {
		// Initialize key FD's from the table schema, including constant columns from
		// the constraint, minus any columns that are not projected by the Scan
		// operator.
		relational.FuncDeps.CopyFrom(makeTableFuncDep(md, def.Table))
		if def.Constraint != nil {
			relational.FuncDeps.AddConstants(def.Constraint.ExtractConstCols(b.evalCtx))
		}
		relational.FuncDeps.MakeNotNull(relational.NotNullCols)
		relational.FuncDeps.ProjectCols(relational.OutputCols)
	}

	// Cardinality
	// -----------
	// Restrict cardinality based on constraint, FDs, and hard limit.
	relational.Cardinality = props.AnyCardinality
	if def.Constraint != nil && def.Constraint.IsContradiction() {
		relational.Cardinality = props.ZeroCardinality
	} else if relational.FuncDeps.HasMax1Row() {
		relational.Cardinality = relational.Cardinality.Limit(1)
	} else if hardLimit > 0 && hardLimit < math.MaxUint32 {
		relational.Cardinality = relational.Cardinality.Limit(uint32(hardLimit))
	}

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, md)
	b.sb.buildScan(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildVirtualScanProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	def := ev.Private().(*VirtualScanOpDef)

	// Output Columns
	// --------------
	// VirtualScan output columns are stored in the definition.
	relational.OutputCols = def.Cols

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable.

	// Outer Columns
	// -------------
	// VirtualScan operator never has outer columns.

	// Functional Dependencies
	// -----------------------
	// VirtualScan operator has an empty FD set.

	// Cardinality
	// -----------
	// Don't make any assumptions about cardinality of output.
	relational.Cardinality = props.AnyCardinality

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildVirtualScan(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildSelectProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	inputProps := ev.childGroup(0).logical.Relational
	filterProps := ev.childGroup(1).logical.Scalar
	filter := ev.Child(1)

	// Output Columns
	// --------------
	// Inherit output columns from input.
	relational.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// A column can become not null due to a null rejecting filter expression:
	//
	//   SELECT y FROM xy WHERE y=5
	//
	// "y" cannot be null because the SQL equality operator rejects nulls.
	relational.NotNullCols = inputProps.NotNullCols
	if filterProps.Constraints != nil {
		b.addNotNullCols(relational, filterProps.Constraints.ExtractNotNullCols(b.evalCtx))
	}

	// Outer Columns
	// -------------
	// Any outer columns from the filter that are not bound by the input columns
	// are outer columns for the Select expression, in addition to any outer
	// columns inherited from the input expression.
	if !filterProps.OuterCols.SubsetOf(inputProps.OutputCols) {
		relational.OuterCols = filterProps.OuterCols.Difference(inputProps.OutputCols)
	}
	relational.OuterCols.UnionWith(inputProps.OuterCols)

	// Functional Dependencies
	// -----------------------
	// Start with copy of FuncDepSet from input, add FDs from the WHERE clause
	// and outer columns, modify with any additional not-null columns, then
	// possibly simplify by calling ProjectCols.
	relational.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	relational.FuncDeps.AddFrom(&filterProps.FuncDeps)
	b.applyOuterColConstants(relational)
	relational.FuncDeps.MakeNotNull(relational.NotNullCols)
	relational.FuncDeps.ProjectCols(relational.OutputCols)

	// Cardinality
	// -----------
	// Select filter can filter any or all rows.
	relational.Cardinality = inputProps.Cardinality.AsLowAs(0)
	if filter.Operator() == opt.FalseOp || filterProps.Constraints == constraint.Contradiction {
		relational.Cardinality = props.ZeroCardinality
	} else if relational.FuncDeps.HasMax1Row() {
		relational.Cardinality = relational.Cardinality.Limit(1)
	}

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildSelect(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildProjectProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	inputProps := ev.childGroup(0).logical.Relational
	projectionProps := ev.childGroup(1).logical.Scalar
	projections := ev.Child(1)
	projectionsDef := ev.Child(1).Private().(*ProjectionsOpDef)

	// Output Columns
	// --------------
	// Use output columns from projection list.
	relational.OutputCols = projectionsDef.AllCols()

	// Not Null Columns
	// ----------------
	// Inherit not null columns from input, but only use those that are also
	// output columns.
	relational.NotNullCols = inputProps.NotNullCols.Copy()
	relational.NotNullCols.IntersectionWith(relational.OutputCols)

	// Also add any column that projects a constant value, since the optimizer
	// sometimes constructs these in order to guarantee a not-null column.
	for i, n := 0, projections.ChildCount(); i < n; i++ {
		child := projections.Child(i)
		if child.IsConstValue() {
			if ExtractConstDatum(child) != tree.DNull {
				relational.NotNullCols.Add(int(projectionsDef.SynthesizedCols[i]))
			}
		}
	}

	// Outer Columns
	// -------------
	// Any outer columns from the projection list that are not bound by the input
	// columns are outer columns from the Project expression, in addition to any
	// outer columns inherited from the input expression.
	if !projectionProps.OuterCols.SubsetOf(inputProps.OutputCols) {
		relational.OuterCols = projectionProps.OuterCols.Difference(inputProps.OutputCols)
	}
	relational.OuterCols.UnionWith(inputProps.OuterCols)

	// Functional Dependencies
	// -----------------------
	// Start with copy of FuncDepSet, add synthesized column dependencies, and then
	// remove columns that are not projected.
	relational.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	for i, colID := range projectionsDef.SynthesizedCols {
		childLogical := projections.Child(i).Logical()
		if !childLogical.Scalar.CanHaveSideEffects {
			from := childLogical.Scalar.OuterCols.Intersection(inputProps.OutputCols)

			// We want to set up the FD: from --> colID.
			// This does not necessarily hold for "composite" types like decimals or
			// collated strings. For example if d is a decimal, d::TEXT can have
			// different values for equal values of d, like 1 and 1.0.
			//
			// We only add the FD if composite types are not involved.
			//
			// TODO(radu): add a whitelist of expressions/operators that are ok, like
			// arithmetic.
			composite := false
			for i, ok := from.Next(0); ok; i, ok = from.Next(i + 1) {
				typ := ev.Metadata().ColumnType(opt.ColumnID(i))
				if sqlbase.DatumTypeHasCompositeKeyEncoding(typ) {
					composite = true
					break
				}
			}
			if !composite {
				relational.FuncDeps.AddSynthesizedCol(from, colID)
			}
		}
	}
	relational.FuncDeps.MakeNotNull(relational.NotNullCols)
	relational.FuncDeps.ProjectCols(relational.OutputCols)

	// Cardinality
	// -----------
	// Inherit cardinality from input.
	relational.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildProject(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildJoinProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	var h joinPropsHelper
	h.init(ev, b.evalCtx)

	leftProps := ev.childGroup(0).logical.Relational

	// Output Columns
	// --------------
	// Output columns are union of columns from left and right inputs, except
	// in case of semi and anti joins, which only project the left columns.
	relational.OutputCols = leftProps.OutputCols.Copy()
	switch h.joinType {
	case opt.SemiJoinOp, opt.AntiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:

	default:
		relational.OutputCols.UnionWith(h.rightOutputCols)
	}

	// Not Null Columns
	// ----------------
	// Left/full outer joins can result in right columns becoming null.
	// Otherwise, propagate not null setting from right child.
	switch h.joinType {
	case opt.LeftJoinOp, opt.FullJoinOp, opt.LeftJoinApplyOp, opt.FullJoinApplyOp,
		opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:

	default:
		relational.NotNullCols = h.rightNotNullCols.Copy()
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch h.joinType {
	case opt.RightJoinOp, opt.FullJoinOp, opt.RightJoinApplyOp, opt.FullJoinApplyOp:

	default:
		relational.NotNullCols.UnionWith(leftProps.NotNullCols)
	}

	// Add not-null constraints from ON predicate for inner and semi-joins.
	switch h.joinType {
	case opt.InnerJoinOp, opt.SemiJoinApplyOp:
		b.addNotNullCols(relational, h.filterNotNullCols)
	}

	// Outer Columns
	// -------------
	// Any outer columns from the filter that are not bound by the input columns
	// are outer columns for the Join expression, in addition to any outer columns
	// inherited from the input expressions.
	inputCols := leftProps.OutputCols.Union(h.rightOutputCols)
	if !h.filterOuterCols.SubsetOf(inputCols) {
		relational.OuterCols = h.filterOuterCols.Difference(inputCols)
	}
	if ev.IsJoinApply() {
		// Outer columns of right side of apply join can be bound by output
		// columns of left side of apply join.
		if !h.rightOuterCols.SubsetOf(leftProps.OutputCols) {
			relational.OuterCols.UnionWith(h.rightOuterCols.Difference(leftProps.OutputCols))
		}
	} else {
		relational.OuterCols.UnionWith(h.rightOuterCols)
	}
	relational.OuterCols.UnionWith(leftProps.OuterCols)

	// Functional Dependencies
	// -----------------------
	// Start with FDs from left side, and modify based on join type.
	relational.FuncDeps.CopyFrom(&leftProps.FuncDeps)

	// Anti and semi joins only inherit FDs from left side, since right side
	// simply acts like a filter.
	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		// Add FDs from the ON predicate, which include equivalent columns and
		// constant columns. Any outer columns become constants as well.
		relational.FuncDeps.AddFrom(&h.filterFD)
		b.applyOuterColConstants(relational)
		relational.FuncDeps.MakeNotNull(relational.NotNullCols)

		// Call ProjectCols to remove any FDs involving columns from the right side.
		relational.FuncDeps.ProjectCols(relational.OutputCols)

	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Anti-joins inherit FDs from left input, and nothing more, since the
		// right input is not projected, and the ON predicate doesn't filter rows
		// in the usual way.

	default:
		// Joins are modeled as consisting of several steps:
		//   1. Compute cartesian product of left and right inputs.
		//   2. For inner joins, apply ON predicate filter on resulting rows.
		//   3. For outer joins, add non-matching rows, extended with NULL values
		//      for the null-supplying side of the join.
		if ev.IsJoinApply() {
			relational.FuncDeps.MakeApply(&h.rightFD)
		} else {
			relational.FuncDeps.MakeProduct(&h.rightFD)
		}

		notNullCols := leftProps.NotNullCols.Union(h.rightNotNullCols)

		switch h.joinType {
		case opt.InnerJoinOp, opt.InnerJoinApplyOp:
			// Add FDs from the ON predicate, which include equivalent columns and
			// constant columns.
			relational.FuncDeps.AddFrom(&h.filterFD)
			b.applyOuterColConstants(relational)

		case opt.RightJoinOp, opt.RightJoinApplyOp:
			relational.FuncDeps.MakeOuter(leftProps.OutputCols, notNullCols)

		case opt.LeftJoinOp, opt.LeftJoinApplyOp:
			relational.FuncDeps.MakeOuter(h.rightOutputCols, notNullCols)

		case opt.FullJoinOp, opt.FullJoinApplyOp:
			// Clear the relation's key if all columns are nullable, because
			// duplicate all-null rows are possible:
			//
			//   -- t1 and t2 each have one row containing NULL for column x.
			//   SELECT * FROM t1 FULL JOIN t2 ON t1.x=t2.x
			//
			//   t1.x  t2.x
			//   ----------
			//   NULL  NULL
			//   NULL  NULL
			//
			if !relational.OutputCols.Intersects(notNullCols) {
				relational.FuncDeps.ClearKey()
			}
			relational.FuncDeps.MakeOuter(leftProps.OutputCols, notNullCols)
			relational.FuncDeps.MakeOuter(h.rightOutputCols, notNullCols)
		}

		relational.FuncDeps.MakeNotNull(relational.NotNullCols)

		// Call ProjectCols to trigger simplification, since outer joins may have
		// created new possibilities for simplifying removed columns.
		relational.FuncDeps.ProjectCols(relational.OutputCols)
	}

	// Cardinality
	// -----------
	// Calculate cardinality, depending on join type.
	relational.Cardinality = b.makeJoinCardinality(leftProps.Cardinality, &h)
	if relational.FuncDeps.HasMax1Row() {
		relational.Cardinality = relational.Cardinality.Limit(1)
	}

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildJoin(ev, relational, &h)

	return logical
}

func (b *logicalPropsBuilder) buildIndexJoinProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	inputProps := ev.childGroup(0).logical.Relational
	md := ev.Metadata()
	def := ev.Private().(*IndexJoinDef)

	// Output Columns
	// --------------
	logical.Relational.OutputCols = def.Cols

	// Not Null Columns
	// ----------------
	// Add not-NULL columns from the table schema, and filter out any not-NULL
	// columns from the input that are not projected by the index join.
	relational.NotNullCols = tableNotNullCols(md, def.Table)
	relational.NotNullCols.IntersectionWith(relational.OutputCols)

	// Outer Columns
	// -------------
	// Inherit outer columns from input.
	relational.OuterCols = inputProps.OuterCols

	// Functional Dependencies
	// -----------------------
	// Start with the input FD set, and join that with the table's FD.
	relational.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	relational.FuncDeps.AddFrom(makeTableFuncDep(md, def.Table))
	relational.FuncDeps.MakeNotNull(relational.NotNullCols)
	relational.FuncDeps.ProjectCols(relational.OutputCols)

	// Cardinality
	// -----------
	// Inherit cardinality from input.
	relational.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, md)
	b.sb.buildIndexJoin(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildGroupByProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	inputProps := ev.childGroup(0).logical.Relational
	aggProps := ev.childGroup(1).logical.Scalar

	// Output Columns
	// --------------
	// Output columns are the union of grouping columns with columns from the
	// aggregate projection list.
	groupingColSet := ev.Private().(*GroupByDef).GroupingCols
	aggColList := ev.Child(1).Private().(opt.ColList)
	relational.OutputCols = groupingColSet.Union(aggColList.ToSet())

	// Not Null Columns
	// ----------------
	// Propagate not null setting from input columns that are being grouped.
	relational.NotNullCols = inputProps.NotNullCols.Intersection(groupingColSet)

	// Outer Columns
	// -------------
	// Any outer columns from aggregation expressions that are not bound by the
	// input columns are outer columns.
	relational.OuterCols = aggProps.OuterCols.Difference(inputProps.OutputCols)
	relational.OuterCols.UnionWith(inputProps.OuterCols)

	// Functional Dependencies
	// -----------------------
	relational.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	if groupingColSet.Empty() {
		// Scalar group by has no grouping columns and always a single row.
		relational.FuncDeps.MakeMax1Row(relational.OutputCols)
	} else {
		// The grouping columns always form a strict key because the GroupBy
		// operation eliminates all duplicates in those columns.
		relational.FuncDeps.ProjectCols(relational.OutputCols)
		relational.FuncDeps.AddStrictKey(groupingColSet, relational.OutputCols)
	}

	// Cardinality
	// -----------
	if ev.Operator() == opt.ScalarGroupByOp {
		// Scalar GroupBy returns exactly one row.
		relational.Cardinality = props.OneCardinality
	} else {
		// GroupBy and DistinctOn act like a filter, never returning more rows than the input
		// has. However, if the input has at least one row, then at least one row
		// will also be returned by GroupBy and DistinctOn.
		relational.Cardinality = inputProps.Cardinality.AsLowAs(1)
		if relational.FuncDeps.HasMax1Row() {
			relational.Cardinality = relational.Cardinality.Limit(1)
		}
	}

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildGroupBy(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildSetProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	leftProps := ev.childGroup(0).logical.Relational
	rightProps := ev.childGroup(1).logical.Relational
	colMap := *ev.Private().(*SetOpColMap)
	if len(colMap.Out) != len(colMap.Left) || len(colMap.Out) != len(colMap.Right) {
		panic(fmt.Errorf("lists in SetOpColMap are not all the same length. new:%d, left:%d, right:%d",
			len(colMap.Out), len(colMap.Left), len(colMap.Right)))
	}

	// Output Columns
	// --------------
	// Output columns are stored in the definition.
	relational.OutputCols = colMap.Out.ToSet()

	// Not Null Columns
	// ----------------
	// Columns have to be not-null on both sides to be not-null in result.
	// colMap matches columns on the left and right sides of the operator
	// with the output columns, since OutputCols are not ordered and may
	// not correspond to each other.
	for i := range colMap.Out {
		if leftProps.NotNullCols.Contains(int((colMap.Left)[i])) &&
			rightProps.NotNullCols.Contains(int((colMap.Right)[i])) {
			relational.NotNullCols.Add(int((colMap.Out)[i]))
		}
	}

	// Outer Columns
	// -------------
	// Outer columns from either side are outer columns for set operation.
	relational.OuterCols = leftProps.OuterCols.Union(rightProps.OuterCols)

	// Functional Dependencies
	// -----------------------
	switch ev.Operator() {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// These operators eliminate duplicates, so a strict key exists.
		relational.FuncDeps.AddStrictKey(relational.OutputCols, relational.OutputCols)
	}

	// Cardinality
	// -----------
	// Calculate cardinality of the set operator.
	relational.Cardinality = b.makeSetCardinality(
		ev, leftProps.Cardinality, rightProps.Cardinality,
	)

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildSetOp(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildValuesProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	card := uint32(ev.ChildCount())

	// Output Columns
	// --------------
	// Use output columns that are attached to the values op.
	relational.OutputCols = ev.Private().(opt.ColList).ToSet()

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable.

	// Outer Columns
	// -------------
	// Union outer columns from all row expressions.
	for i, n := 0, ev.ChildCount(); i < n; i++ {
		relational.OuterCols.UnionWith(ev.childGroup(i).logical.Scalar.OuterCols)
	}

	// Functional Dependencies
	// -----------------------
	if card <= 1 {
		relational.FuncDeps.MakeMax1Row(relational.OutputCols)
	}

	// Cardinality
	// -----------
	// Cardinality is number of tuples in the Values operator.
	relational.Cardinality = props.Cardinality{Min: card, Max: card}

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildValues(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildExplainProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	def := ev.Private().(*ExplainOpDef)

	// Output Columns
	// --------------
	// Output columns are stored in the definition.
	relational.OutputCols = def.ColList.ToSet()

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable.

	// Outer Columns
	// -------------
	// EXPLAIN doesn't allow outer columns.

	// Functional Dependencies
	// -----------------------
	// Explain operator has an empty FD set.

	// Cardinality
	// -----------
	// Don't make any assumptions about cardinality of output.
	relational.Cardinality = props.AnyCardinality

	// Statistics
	// ----------
	// Zero value for Stats is ok for Explain.

	return logical
}

func (b *logicalPropsBuilder) buildShowTraceProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	def := ev.Private().(*ShowTraceOpDef)

	// Output Columns
	// --------------
	// Output columns are stored in the definition.
	relational.OutputCols = def.ColList.ToSet()

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable.

	// Outer Columns
	// -------------
	// SHOW TRACE doesn't allow outer columns.

	// Functional Dependencies
	// -----------------------
	// ShowTrace operator has an empty FD set.

	// Cardinality
	// -----------
	// Don't make any assumptions about cardinality of output.
	relational.Cardinality = props.AnyCardinality

	// Statistics
	// ----------
	// Zero value for Stats is ok for ShowTrace.

	return logical
}

func (b *logicalPropsBuilder) buildLimitProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	inputProps := ev.Child(0).Logical().Relational
	limit := ev.Child(1)
	limitProps := limit.Logical().Scalar

	constLimit := int64(math.MaxUint32)
	haveConstLimit := false
	if limit.Operator() == opt.ConstOp {
		haveConstLimit = true
		constLimit = int64(*limit.Private().(*tree.DInt))
	}

	// Output Columns
	// --------------
	// Output columns are inherited from input.
	relational.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// Not null columns are inherited from input.
	relational.NotNullCols = inputProps.NotNullCols

	// Outer Columns
	// -------------
	// Inherit outer columns from input, and add any outer columns from the limit
	// expression,
	if limitProps.OuterCols.Empty() {
		relational.OuterCols = inputProps.OuterCols
	} else {
		relational.OuterCols = limitProps.OuterCols.Union(inputProps.OuterCols)
	}

	// Functional Dependencies
	// -----------------------
	// Inherit functional dependencies from input if limit is > 1, else just use
	// single row dependencies.
	if constLimit > 1 {
		relational.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	} else {
		relational.FuncDeps.MakeMax1Row(relational.OutputCols)
	}

	// Cardinality
	// -----------
	// Limit puts a cap on the number of rows returned by input.
	relational.Cardinality = inputProps.Cardinality
	if constLimit <= 0 {
		relational.Cardinality = props.ZeroCardinality
	} else if constLimit < math.MaxUint32 {
		relational.Cardinality = relational.Cardinality.Limit(uint32(constLimit))
	}

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildLimit(ev, relational)

	// Side Effects
	// ------------
	if constLimit < 0 || !haveConstLimit {
		relational.CanHaveSideEffects = true
	}

	return logical
}

func (b *logicalPropsBuilder) buildOffsetProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	inputProps := ev.Child(0).Logical().Relational
	offset := ev.Child(1)
	offsetProps := offset.Logical().Scalar

	// Output Columns
	// --------------
	// Output columns are inherited from input.
	relational.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// Not null columns are inherited from input.
	relational.NotNullCols = inputProps.NotNullCols

	// Outer Columns
	// -------------
	// Inherit outer columns from input, and add any outer columns from the offset
	// expression,
	if offsetProps.OuterCols.Empty() {
		relational.OuterCols = inputProps.OuterCols
	} else {
		relational.OuterCols = offsetProps.OuterCols.Union(inputProps.OuterCols)
	}

	// Functional Dependencies
	// -----------------------
	// Inherit functional dependencies from input.
	relational.FuncDeps.CopyFrom(&inputProps.FuncDeps)

	// Cardinality
	// -----------
	// Offset decreases the number of rows that are passed through from input.
	relational.Cardinality = inputProps.Cardinality
	if offset.Operator() == opt.ConstOp {
		constOffset := int64(*offset.Private().(*tree.DInt))
		if constOffset > 0 {
			if constOffset > math.MaxUint32 {
				constOffset = math.MaxUint32
			}
			relational.Cardinality = inputProps.Cardinality.Skip(uint32(constOffset))
		}
	}

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildOffset(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildMax1RowProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	inputProps := ev.Child(0).Logical().Relational

	// Output Columns
	// --------------
	// Output columns are inherited from input.
	relational.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// Not null columns are inherited from input.
	relational.NotNullCols = inputProps.NotNullCols

	// Outer Columns
	// -------------
	// Outer columns are inherited from input.
	relational.OuterCols = inputProps.OuterCols

	// Functional Dependencies
	// -----------------------
	// Max1Row always returns zero or one rows.
	relational.FuncDeps.MakeMax1Row(relational.OutputCols)

	// Cardinality
	// -----------
	// Max1Row ensures that zero or one row is returned from input.
	relational.Cardinality = inputProps.Cardinality.Limit(1)

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildMax1Row(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildRowNumberProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	inputProps := ev.Child(0).Logical().Relational
	def := ev.Private().(*RowNumberDef)

	// Output Columns
	// --------------
	// An extra output column is added to those projected by input operator.
	relational.OutputCols = inputProps.OutputCols.Copy()
	relational.OutputCols.Add(int(def.ColID))

	// Not Null Columns
	// ----------------
	// The new output column is not null, and other columns inherit not null
	// property from input.
	relational.NotNullCols = inputProps.NotNullCols.Copy()
	relational.NotNullCols.Add(int(def.ColID))

	// Outer Columns
	// -------------
	// Outer columns are inherited from input.
	relational.OuterCols = inputProps.OuterCols

	// Functional Dependencies
	// -----------------------
	// Inherit functional dependencies from input, and add strict key FD for the
	// additional key column.
	relational.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	if key, ok := relational.FuncDeps.Key(); ok {
		// Any existing keys are still keys.
		relational.FuncDeps.AddStrictKey(key, relational.OutputCols)
	}
	relational.FuncDeps.AddStrictKey(util.MakeFastIntSet(int(def.ColID)), relational.OutputCols)

	// Cardinality
	// -----------
	// Inherit cardinality from input.
	relational.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildRowNumber(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildZipProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: b.allocRelationalProps()}
	relational := logical.Relational

	// Output Columns
	// --------------
	// Output columns are stored in the definition.
	relational.OutputCols = ev.Private().(opt.ColList).ToSet()

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable.

	// Outer Columns
	// -------------
	// Union outer columns from all input expressions.
	for i, n := 0, ev.ChildCount(); i < n; i++ {
		relational.OuterCols.UnionWith(ev.childGroup(i).logical.OuterCols())
	}

	// Functional Dependencies
	// -----------------------
	// Zip operator has an empty FD set.

	// Cardinality
	// -----------
	// Don't make any assumptions about cardinality of output.
	relational.Cardinality = props.AnyCardinality

	// Statistics
	// ----------
	b.sb.init(b.evalCtx, ev.Metadata())
	b.sb.buildZip(ev, relational)

	return logical
}

func (b *logicalPropsBuilder) buildScalarProps(ev ExprView) props.Logical {
	logical := props.Logical{Scalar: b.allocScalarProps()}
	scalar := logical.Scalar
	scalar.Type = InferType(ev)

	switch ev.Operator() {
	case opt.VariableOp:
		// Variable introduces outer column.
		scalar.OuterCols.Add(int(ev.Private().(opt.ColumnID)))
		return logical

	case opt.PlaceholderOp:
		scalar.HasPlaceholder = true
		return logical
	}

	// By default, derive OuterCols, HasPlaceholder, and CanHaveSideEffects from
	// all children, both relational and scalar. Derive HasCorrelatedSubquery from
	// scalar children.
	for i, n := 0, ev.ChildCount(); i < n; i++ {
		childLogical := &ev.childGroup(i).logical

		scalar.OuterCols.UnionWith(childLogical.OuterCols())

		if childLogical.CanHaveSideEffects() {
			scalar.CanHaveSideEffects = true
		}

		if childLogical.HasPlaceholder() {
			scalar.HasPlaceholder = true
		}

		if childLogical.Scalar != nil && childLogical.Scalar.HasCorrelatedSubquery {
			scalar.HasCorrelatedSubquery = true
		}
	}

	switch ev.Operator() {
	case opt.ProjectionsOp:
		// For a ProjectionsOp, the passthrough cols are also outer cols.
		scalar.OuterCols.UnionWith(ev.Private().(*ProjectionsOpDef).PassthroughCols)

	case opt.FiltersOp:
		// Calculate constraints and FDs for filters.

		// Constraints
		// -----------
		cb := constraintsBuilder{md: ev.Metadata(), evalCtx: b.evalCtx}
		scalar.Constraints, scalar.TightConstraints = cb.buildConstraints(ev)
		if scalar.Constraints.IsUnconstrained() {
			scalar.Constraints, scalar.TightConstraints = nil, false
		}

		// Functional Dependencies
		// -----------------------
		// Add constant columns. No need to add not null columns, because they
		// are only relevant if there are lax FDs that can be made strict.
		if scalar.Constraints != nil {
			constCols := scalar.Constraints.ExtractConstCols(b.evalCtx)
			scalar.FuncDeps.AddConstants(constCols)
		}

		// Check for filter conjuncts of the form: x = y.
		for i, n := 0, ev.ChildCount(); i < n; i++ {
			child := ev.Child(i)
			if child.Operator() == opt.EqOp {
				left := child.Child(0)
				right := child.Child(1)
				if left.Operator() == opt.VariableOp && right.Operator() == opt.VariableOp {
					colLeft := left.Private().(opt.ColumnID)
					colRight := right.Private().(opt.ColumnID)
					scalar.FuncDeps.AddEquivalency(colLeft, colRight)
				}
			}
		}

	case opt.FunctionOp:
		funcOpDef := ev.Private().(*FuncOpDef)
		if funcOpDef.Properties.Impure {
			// Impure functions can return different value on each call.
			scalar.CanHaveSideEffects = true
		}

	case opt.DivOp:
		// Division by zero error is possible.
		scalar.CanHaveSideEffects = true

	case opt.SubqueryOp, opt.ExistsOp, opt.AnyOp:
		if !scalar.OuterCols.Empty() {
			scalar.HasCorrelatedSubquery = true
		}
	}

	return logical
}

// makeTableFuncDep returns the set of functional dependencies derived from the
// given base table. The set is derived lazily and is cached in the metadata,
// since it may be accessed multiple times during query optimization. For more
// details, see Relational.FuncDepSet.
func makeTableFuncDep(md *opt.Metadata, tabID opt.TableID) *props.FuncDepSet {
	fd, ok := md.TableAnnotation(tabID, fdAnnID).(*props.FuncDepSet)
	if ok {
		// Already made.
		return fd
	}

	// Make now and annotate the metadata table with it for next time.
	var allCols opt.ColSet
	tab := md.Table(tabID)
	for i := 0; i < tab.ColumnCount(); i++ {
		allCols.Add(int(tabID.ColumnID(i)))
	}

	fd = &props.FuncDepSet{}
	for i := 0; i < tab.IndexCount(); i++ {
		var keyCols opt.ColSet
		index := tab.Index(i)
		if index.IsInverted() {
			// Skip inverted indexes for now.
			continue
		}

		// If index has a separate lax key, add a lax key FD. Otherwise, add a
		// strict key. See the comment for opt.Index.LaxKeyColumnCount.
		for col := 0; col < index.LaxKeyColumnCount(); col++ {
			ord := index.Column(col).Ordinal
			keyCols.Add(int(tabID.ColumnID(ord)))
		}
		if index.LaxKeyColumnCount() < index.KeyColumnCount() {
			// This case only occurs for a UNIQUE index having a NULL-able column.
			fd.AddLaxKey(keyCols, allCols)
		} else {
			fd.AddStrictKey(keyCols, allCols)
		}
	}
	md.SetTableAnnotation(tabID, fdAnnID, fd)
	return fd
}

func (b *logicalPropsBuilder) makeJoinCardinality(
	left props.Cardinality, h *joinPropsHelper,
) props.Cardinality {
	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Semi/Anti join cardinality never exceeds left input cardinality, and
		// allows zero rows.
		return left.AsLowAs(0)
	}

	// Other join types can return up to cross product of rows.
	right := h.rightCardinality
	innerJoinCard := left.Product(right)

	// Apply filter to cardinality.
	if !h.filterIsTrue {
		if h.filterIsFalse {
			innerJoinCard = props.ZeroCardinality
		} else {
			innerJoinCard = innerJoinCard.AsLowAs(0)
		}
	}

	// Outer joins return minimum number of rows, depending on type.
	switch h.joinType {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return innerJoinCard.AtLeast(left)

	case opt.RightJoinOp, opt.RightJoinApplyOp:
		return innerJoinCard.AtLeast(right)

	case opt.FullJoinOp, opt.FullJoinApplyOp:
		if innerJoinCard.IsZero() {
			// In this case, we know that each left or right row will generate an
			// output row.
			return left.Add(right)
		}
		var c props.Cardinality
		// We get at least MAX(left.Min, right.Min) rows.
		c.Min = left.Min
		if c.Min < right.Min {
			c.Min = right.Min
		}
		// We could get left.Max + right.Max rows (if the filter doesn't match
		// anything). We use Add here because it handles overflow.
		c.Max = left.Add(right).Max
		return innerJoinCard.AtLeast(c)

	default:
		return innerJoinCard
	}
}

func (b *logicalPropsBuilder) makeSetCardinality(
	ev ExprView, left, right props.Cardinality,
) props.Cardinality {
	var card props.Cardinality
	switch ev.Operator() {
	case opt.UnionOp, opt.UnionAllOp:
		// Add cardinality of left and right inputs.
		card = left.Add(right)

	case opt.IntersectOp, opt.IntersectAllOp:
		// Use minimum of left and right Max cardinality.
		card = props.Cardinality{Min: 0, Max: left.Max}
		card = card.Limit(right.Max)

	case opt.ExceptOp, opt.ExceptAllOp:
		// Use left Max cardinality.
		card = props.Cardinality{Min: 0, Max: left.Max}
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

// tableNotNullCols returns the set of not-NULL columns from the given table.
func tableNotNullCols(md *opt.Metadata, tabID opt.TableID) opt.ColSet {
	cs := opt.ColSet{}
	tab := md.Table(tabID)
	for i := 0; i < tab.ColumnCount(); i++ {
		if !tab.Column(i).IsNullable() {
			cs.Add(int(tabID.ColumnID(i)))
		}
	}
	return cs
}

// addNotNullCols adds not-null cols to the relational properties. The given
// notNullCols set can be modified.
func (b *logicalPropsBuilder) addNotNullCols(relational *props.Relational, notNullCols opt.ColSet) {
	if !notNullCols.SubsetOf(relational.NotNullCols) {
		notNullCols.UnionWith(relational.NotNullCols)
		notNullCols.IntersectionWith(relational.OutputCols)
		relational.NotNullCols = notNullCols
	}
}

// applyOuterColConstants adds all outer columns and columns equivalent to them
// to the FD set in the relational properties. References to outer columns act
// like constants, since they are the same for all rows in the inner relation.
func (b *logicalPropsBuilder) applyOuterColConstants(relational *props.Relational) {
	equivCols := relational.FuncDeps.ComputeEquivClosure(relational.OuterCols)
	relational.FuncDeps.AddConstants(equivCols)
}

func (b *logicalPropsBuilder) allocRelationalProps() *props.Relational {
	if len(b.relationalAlloc) == 0 {
		// props.Relational is pretty hefty (200+ bytes), hence the small chunk
		// size. A larger chunk size can be a pessimization as the GC has to scan
		// the pointers in any unallocated entries. A value of 2 was chosen at this
		// captures scans from a single relation and 2-way joins.
		b.relationalAlloc = make([]props.Relational, 2)
	}
	r := &b.relationalAlloc[0]
	b.relationalAlloc = b.relationalAlloc[1:]
	return r
}

func (b *logicalPropsBuilder) allocScalarProps() *props.Scalar {
	if len(b.scalarAlloc) == 0 {
		// props.Scalar is pretty hefty (100+ bytes), hence the small chunk size. A
		// larger chunk size can be a pessimization as the GC has to scan the
		// pointers in any unallocated entries.
		b.scalarAlloc = make([]props.Scalar, 4)
	}
	r := &b.scalarAlloc[0]
	b.scalarAlloc = b.scalarAlloc[1:]
	return r
}

// joinPropsHelper is a helper that calculates and stores properties related to
// joins that are used internally when deriving logical properties and
// statistics.
type joinPropsHelper struct {
	joinType opt.Operator

	lookupJoinDef *LookupJoinDef

	rightOutputCols  opt.ColSet
	rightNotNullCols opt.ColSet
	rightOuterCols   opt.ColSet
	rightCardinality props.Cardinality
	rightFD          props.FuncDepSet

	filter ExprView
	// The following fields are properties of the "filter" (ON condition). For
	// lookup join, the properties take into account the implicit lookup equalities.
	filterOuterCols   opt.ColSet
	filterFD          props.FuncDepSet
	filterNotNullCols opt.ColSet
	filterIsTrue      bool
	filterIsFalse     bool
}

func (h *joinPropsHelper) init(ev ExprView, evalCtx *tree.EvalContext) {
	if ev.Operator() != opt.LookupJoinOp {
		h.joinType = ev.Operator()

		rightProps := ev.childGroup(1).logical.Relational
		h.rightOutputCols = rightProps.OutputCols
		h.rightNotNullCols = rightProps.NotNullCols
		h.rightOuterCols = rightProps.OuterCols
		h.rightCardinality = rightProps.Cardinality
		h.rightFD = rightProps.FuncDeps

		h.filter = ev.Child(2)
		filterProps := h.filter.Logical().Scalar
		h.filterFD = filterProps.FuncDeps
		h.filterOuterCols = filterProps.OuterCols
		if filterProps.Constraints != nil {
			h.filterNotNullCols = filterProps.Constraints.ExtractNotNullCols(evalCtx)
		}
		h.filterIsTrue = (h.filter.Operator() == opt.TrueOp)
		h.filterIsFalse = (h.filter.Operator() == opt.FalseOp ||
			filterProps.Constraints == constraint.Contradiction)
		return
	}

	md := ev.Metadata()
	def := ev.Private().(*LookupJoinDef)
	h.joinType = def.JoinType
	h.lookupJoinDef = def

	h.rightOutputCols = def.LookupCols
	h.rightNotNullCols = tableNotNullCols(md, def.Table)
	h.rightNotNullCols.IntersectionWith(def.LookupCols)
	h.rightCardinality = props.AnyCardinality
	h.rightFD.CopyFrom(makeTableFuncDep(md, def.Table))
	h.rightFD.MakeNotNull(h.rightNotNullCols)
	h.rightFD.ProjectCols(h.rightOutputCols)

	h.filter = ev.Child(1)
	filterProps := h.filter.Logical().Scalar
	h.filterOuterCols = filterProps.OuterCols
	h.filterFD.CopyFrom(&filterProps.FuncDeps)
	if filterProps.Constraints != nil {
		h.filterNotNullCols = filterProps.Constraints.ExtractNotNullCols(evalCtx)
	}
	index := md.Table(def.Table).Index(def.Index)
	// Apply the lookup join equalities.
	for i, colID := range def.KeyCols {
		indexColID := def.Table.ColumnID(index.Column(i).Ordinal)
		h.filterNotNullCols.Add(int(colID))
		h.filterNotNullCols.Add(int(indexColID))
		h.filterFD.AddEquivalency(colID, indexColID)
	}
	// Lookup join has implicit equalities as part of the ON condition.
	h.filterIsTrue = false
	h.filterIsFalse = (h.filter.Operator() == opt.FalseOp ||
		filterProps.Constraints == constraint.Contradiction)
}
