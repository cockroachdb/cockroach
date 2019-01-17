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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

var fdAnnID = opt.NewTableAnnID()

// logicalPropsBuilder is a helper class that consolidates the code that derives
// a parent expression's logical properties from those of its children.
//
// buildProps is called by the memo group construction code in order to
// initialize the new group's logical properties.
// NOTE: When deriving properties from children, be sure to keep the child
//       properties immutable by copying them if necessary.
// NOTE: The parent expression is passed as an expression for convenient access
//       to children, but certain properties on it are not yet defined (like
//       its logical properties!).
type logicalPropsBuilder struct {
	evalCtx *tree.EvalContext
	mem     *Memo
	sb      statisticsBuilder

	// When set to true, disableStats disables stat generation during
	// logical prop building. Useful in checkExpr when we don't want
	// to create stats for non-normalized expressions and potentially
	// mutate opt_tester output compared to cases where checkExpr is
	// not run.
	disableStats bool
}

func (b *logicalPropsBuilder) init(evalCtx *tree.EvalContext, mem *Memo) {
	b.evalCtx = evalCtx
	b.mem = mem
	b.sb.init(evalCtx, mem.Metadata())
}

func (b *logicalPropsBuilder) clear() {
	b.evalCtx = nil
	b.mem = nil
	b.sb.clear()
}

func (b *logicalPropsBuilder) buildScanProps(scan *ScanExpr, rel *props.Relational) {
	md := scan.Memo().Metadata()
	hardLimit := scan.HardLimit.RowCount()

	// Output Columns
	// --------------
	// Scan output columns are stored in the definition.
	rel.OutputCols = scan.Cols

	// Not Null Columns
	// ----------------
	// Initialize not-NULL columns from the table schema.
	rel.NotNullCols = tableNotNullCols(md, scan.Table)
	if scan.Constraint != nil {
		rel.NotNullCols.UnionWith(scan.Constraint.ExtractNotNullCols(b.evalCtx))
	}
	rel.NotNullCols.IntersectionWith(rel.OutputCols)

	// Outer Columns
	// -------------
	// Scan operator never has outer columns.

	// Functional Dependencies
	// -----------------------
	// Check the hard limit to determine whether there is at most one row. Note
	// that def.HardLimit = 0 indicates there is no known limit.
	if hardLimit == 1 {
		rel.FuncDeps.MakeMax1Row(rel.OutputCols)
	} else {
		// Initialize key FD's from the table schema, including constant columns from
		// the constraint, minus any columns that are not projected by the Scan
		// operator.
		rel.FuncDeps.CopyFrom(makeTableFuncDep(md, scan.Table))
		if scan.Constraint != nil {
			rel.FuncDeps.AddConstants(scan.Constraint.ExtractConstCols(b.evalCtx))
		}
		rel.FuncDeps.MakeNotNull(rel.NotNullCols)
		rel.FuncDeps.ProjectCols(rel.OutputCols)
	}

	// Cardinality
	// -----------
	// Restrict cardinality based on constraint, FDs, and hard limit.
	rel.Cardinality = props.AnyCardinality
	if scan.Constraint != nil && scan.Constraint.IsContradiction() {
		rel.Cardinality = props.ZeroCardinality
	} else if rel.FuncDeps.HasMax1Row() {
		rel.Cardinality = rel.Cardinality.Limit(1)
	} else if hardLimit > 0 && hardLimit < math.MaxUint32 {
		rel.Cardinality = rel.Cardinality.Limit(uint32(hardLimit))
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildScan(scan, rel)
	}
}

func (b *logicalPropsBuilder) buildVirtualScanProps(scan *VirtualScanExpr, rel *props.Relational) {
	// Output Columns
	// --------------
	// VirtualScan output columns are stored in the definition.
	rel.OutputCols = scan.Cols

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
	rel.Cardinality = props.AnyCardinality

	// Statistics
	// ----------
	b.sb.buildVirtualScan(scan, rel)
}

func (b *logicalPropsBuilder) buildSelectProps(sel *SelectExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, sel, &rel.Shared)

	inputProps := sel.Input.Relational()

	// Output Columns
	// --------------
	// Inherit output columns from input.
	rel.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// A column can become not null due to a null rejecting filter expression:
	//
	//   SELECT y FROM xy WHERE y=5
	//
	// "y" cannot be null because the SQL equality operator rejects nulls.
	rel.NotNullCols = b.rejectNullCols(sel.Filters)
	rel.NotNullCols.UnionWith(inputProps.NotNullCols)
	rel.NotNullCols.IntersectionWith(rel.OutputCols)

	// Outer Columns
	// -------------
	// Outer columns were derived by buildSharedProps; remove any that are bound
	// by input columns.
	rel.OuterCols.DifferenceWith(inputProps.OutputCols)

	// Functional Dependencies
	// -----------------------
	// Start with copy of FuncDepSet from input, add FDs from the WHERE clause
	// and outer columns, modify with any additional not-null columns, then
	// possibly simplify by calling ProjectCols.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	b.addFiltersToFuncDep(sel.Filters, &rel.FuncDeps)
	addOuterColsToFuncDep(rel.OuterCols, &rel.FuncDeps)
	rel.FuncDeps.MakeNotNull(rel.NotNullCols)
	rel.FuncDeps.ProjectCols(rel.OutputCols)

	// Cardinality
	// -----------
	// Select filter can filter any or all rows.
	rel.Cardinality = inputProps.Cardinality.AsLowAs(0)
	if sel.Filters.IsFalse() {
		rel.Cardinality = props.ZeroCardinality
	} else if rel.FuncDeps.HasMax1Row() {
		rel.Cardinality = rel.Cardinality.Limit(1)
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildSelect(sel, rel)
	}
}

func (b *logicalPropsBuilder) buildProjectProps(prj *ProjectExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, prj, &rel.Shared)

	inputProps := prj.Input.Relational()

	// Output Columns
	// --------------
	// Output columns are the union of synthesized columns and passthrough columns.
	for i := range prj.Projections {
		rel.OutputCols.Add(int(prj.Projections[i].Col))
	}
	rel.OutputCols.UnionWith(prj.Passthrough)

	// Not Null Columns
	// ----------------
	// Inherit not null columns from input, but only use those that are also
	// output columns.
	rel.NotNullCols = inputProps.NotNullCols.Copy()
	rel.NotNullCols.IntersectionWith(rel.OutputCols)

	// Also add any column that projects a constant value, since the optimizer
	// sometimes constructs these in order to guarantee a not-null column.
	for i := range prj.Projections {
		item := &prj.Projections[i]
		if opt.IsConstValueOp(item.Element) {
			if ExtractConstDatum(item.Element) != tree.DNull {
				rel.NotNullCols.Add(int(item.Col))
			}
		}
	}

	// Outer Columns
	// -------------
	// Outer columns were derived by buildSharedProps; remove any that are bound
	// by input columns.
	rel.OuterCols.DifferenceWith(inputProps.OutputCols)

	// Functional Dependencies
	// -----------------------
	// Start with copy of FuncDepSet, add synthesized column dependencies, and then
	// remove columns that are not projected.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	for i := range prj.Projections {
		item := &prj.Projections[i]
		if !item.scalar.CanHaveSideEffects {
			from := item.scalar.OuterCols.Intersection(inputProps.OutputCols)

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
				typ := b.mem.Metadata().ColumnMeta(opt.ColumnID(i)).Type
				if sqlbase.DatumTypeHasCompositeKeyEncoding(typ) {
					composite = true
					break
				}
			}
			if !composite {
				rel.FuncDeps.AddSynthesizedCol(from, item.Col)
			}
		}
	}
	rel.FuncDeps.MakeNotNull(rel.NotNullCols)
	rel.FuncDeps.ProjectCols(rel.OutputCols)

	// Cardinality
	// -----------
	// Inherit cardinality from input.
	rel.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildProject(prj, rel)
	}
}

func (b *logicalPropsBuilder) buildInnerJoinProps(join *InnerJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildLeftJoinProps(join *LeftJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildRightJoinProps(join *RightJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildFullJoinProps(join *FullJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildSemiJoinProps(join *SemiJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildAntiJoinProps(join *AntiJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildInnerJoinApplyProps(
	join *InnerJoinApplyExpr, rel *props.Relational,
) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildLeftJoinApplyProps(
	join *LeftJoinApplyExpr, rel *props.Relational,
) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildRightJoinApplyProps(
	join *RightJoinApplyExpr, rel *props.Relational,
) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildFullJoinApplyProps(
	join *FullJoinApplyExpr, rel *props.Relational,
) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildSemiJoinApplyProps(
	join *SemiJoinApplyExpr, rel *props.Relational,
) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildAntiJoinApplyProps(
	join *AntiJoinApplyExpr, rel *props.Relational,
) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildJoinProps(join RelExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, join, &rel.Shared)

	var h joinPropsHelper
	h.init(b, join)

	// Output Columns
	// --------------
	rel.OutputCols = h.outputCols()

	// Not Null Columns
	// ----------------
	rel.NotNullCols = h.notNullCols()
	rel.NotNullCols.IntersectionWith(rel.OutputCols)

	// Outer Columns
	// -------------
	// Outer columns were initially set by buildSharedProps. Remove any that are
	// bound by the input columns.
	inputCols := h.leftProps.OutputCols.Union(h.rightProps.OutputCols)
	rel.OuterCols.DifferenceWith(inputCols)
	if opt.IsJoinApplyOp(join) {
		// Outer columns of right side of apply join can be bound by output columns
		// of left side of apply join. Since this is apply join, there is always a
		// right input.
		rightOuterCols := join.Child(1).(RelExpr).Relational().OuterCols
		rel.OuterCols.DifferenceWith(rightOuterCols)
	}

	// Functional Dependencies
	// -----------------------
	h.setFuncDeps(rel)

	// Cardinality
	// -----------
	// Calculate cardinality, depending on join type.
	rel.Cardinality = h.cardinality()
	if rel.FuncDeps.HasMax1Row() {
		rel.Cardinality = rel.Cardinality.Limit(1)
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildJoin(join, rel, &h)
	}
}

func (b *logicalPropsBuilder) buildIndexJoinProps(indexJoin *IndexJoinExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, indexJoin, &rel.Shared)

	inputProps := indexJoin.Input.Relational()
	md := b.mem.Metadata()

	// Output Columns
	// --------------
	rel.OutputCols = indexJoin.Cols

	// Not Null Columns
	// ----------------
	// Add not-NULL columns from the table schema, and filter out any not-NULL
	// columns from the input that are not projected by the index join.
	rel.NotNullCols = tableNotNullCols(md, indexJoin.Table)
	rel.NotNullCols.IntersectionWith(rel.OutputCols)

	// Outer Columns
	// -------------
	// Outer columns were already derived by buildSharedProps.

	// Functional Dependencies
	// -----------------------
	// Start with the input FD set, and join that with the table's FD.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	rel.FuncDeps.AddFrom(makeTableFuncDep(md, indexJoin.Table))
	rel.FuncDeps.MakeNotNull(rel.NotNullCols)
	rel.FuncDeps.ProjectCols(rel.OutputCols)

	// Cardinality
	// -----------
	// Inherit cardinality from input.
	rel.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildIndexJoin(indexJoin, rel)
	}
}

func (b *logicalPropsBuilder) buildLookupJoinProps(join *LookupJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildZigzagJoinProps(join *ZigzagJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildMergeJoinProps(join *MergeJoinExpr, rel *props.Relational) {
	panic("no relational properties for merge join expression")
}

func (b *logicalPropsBuilder) buildGroupByProps(groupBy *GroupByExpr, rel *props.Relational) {
	b.buildGroupingExprProps(groupBy, rel)
}

func (b *logicalPropsBuilder) buildScalarGroupByProps(
	scalarGroupBy *ScalarGroupByExpr, rel *props.Relational,
) {
	b.buildGroupingExprProps(scalarGroupBy, rel)
}

func (b *logicalPropsBuilder) buildDistinctOnProps(
	distinctOn *DistinctOnExpr, rel *props.Relational,
) {
	b.buildGroupingExprProps(distinctOn, rel)
}

func (b *logicalPropsBuilder) buildGroupingExprProps(groupExpr RelExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, groupExpr, &rel.Shared)

	inputProps := groupExpr.Child(0).(RelExpr).Relational()
	aggs := *groupExpr.Child(1).(*AggregationsExpr)
	groupPrivate := groupExpr.Private().(*GroupingPrivate)

	// Output Columns
	// --------------
	// Output columns are the union of grouping columns with columns from the
	// aggregate projection list.
	rel.OutputCols = groupPrivate.GroupingCols.Copy()
	for i := range aggs {
		rel.OutputCols.Add(int(aggs[i].Col))
	}

	// Not Null Columns
	// ----------------
	// Propagate not null setting from input columns that are being grouped.
	rel.NotNullCols = inputProps.NotNullCols.Intersection(groupPrivate.GroupingCols)

	// Outer Columns
	// -------------
	// Outer columns were derived by buildSharedProps; remove any that are bound
	// by input columns.
	rel.OuterCols.DifferenceWith(inputProps.OutputCols)

	// Functional Dependencies
	// -----------------------
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	if groupPrivate.GroupingCols.Empty() {
		// Scalar group by has no grouping columns and always a single row.
		rel.FuncDeps.MakeMax1Row(rel.OutputCols)
	} else {
		// The grouping columns always form a strict key because the GroupBy
		// operation eliminates all duplicates in those columns.
		rel.FuncDeps.ProjectCols(rel.OutputCols)
		rel.FuncDeps.AddStrictKey(groupPrivate.GroupingCols, rel.OutputCols)
	}

	// Cardinality
	// -----------
	if groupExpr.Op() == opt.ScalarGroupByOp {
		// Scalar GroupBy returns exactly one row.
		rel.Cardinality = props.OneCardinality
	} else {
		// GroupBy and DistinctOn act like a filter, never returning more rows than the input
		// has. However, if the input has at least one row, then at least one row
		// will also be returned by GroupBy and DistinctOn.
		rel.Cardinality = inputProps.Cardinality.AsLowAs(1)
		if rel.FuncDeps.HasMax1Row() {
			rel.Cardinality = rel.Cardinality.Limit(1)
		}
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildGroupBy(groupExpr, rel)
	}
}

func (b *logicalPropsBuilder) buildUnionProps(union *UnionExpr, rel *props.Relational) {
	b.buildSetProps(union, rel)
}

func (b *logicalPropsBuilder) buildIntersectProps(isect *IntersectExpr, rel *props.Relational) {
	b.buildSetProps(isect, rel)
}

func (b *logicalPropsBuilder) buildExceptProps(except *ExceptExpr, rel *props.Relational) {
	b.buildSetProps(except, rel)
}

func (b *logicalPropsBuilder) buildUnionAllProps(union *UnionAllExpr, rel *props.Relational) {
	b.buildSetProps(union, rel)
}

func (b *logicalPropsBuilder) buildIntersectAllProps(
	isect *IntersectAllExpr, rel *props.Relational,
) {
	b.buildSetProps(isect, rel)
}

func (b *logicalPropsBuilder) buildExceptAllProps(except *ExceptAllExpr, rel *props.Relational) {
	b.buildSetProps(except, rel)
}

func (b *logicalPropsBuilder) buildSetProps(setNode RelExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, setNode, &rel.Shared)

	leftProps := setNode.Child(0).(RelExpr).Relational()
	rightProps := setNode.Child(1).(RelExpr).Relational()
	setPrivate := setNode.Private().(*SetPrivate)
	if len(setPrivate.OutCols) != len(setPrivate.LeftCols) ||
		len(setPrivate.OutCols) != len(setPrivate.RightCols) {
		panic(fmt.Errorf("lists in SetPrivate are not all the same length. new:%d, left:%d, right:%d",
			len(setPrivate.OutCols), len(setPrivate.LeftCols), len(setPrivate.RightCols)))
	}

	// Output Columns
	// --------------
	// Output columns are stored in the definition.
	rel.OutputCols = setPrivate.OutCols.ToSet()

	// Not Null Columns
	// ----------------
	// Columns have to be not-null on both sides to be not-null in result.
	// setPrivate matches columns on the left and right sides of the operator
	// with the output columns, since OutputCols are not ordered and may
	// not correspond to each other.
	for i := range setPrivate.OutCols {
		if leftProps.NotNullCols.Contains(int((setPrivate.LeftCols)[i])) &&
			rightProps.NotNullCols.Contains(int((setPrivate.RightCols)[i])) {
			rel.NotNullCols.Add(int((setPrivate.OutCols)[i]))
		}
	}

	// Outer Columns
	// -------------
	// Outer columns were already derived by buildSharedProps.

	// Functional Dependencies
	// -----------------------
	switch setNode.Op() {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// These operators eliminate duplicates, so a strict key exists.
		rel.FuncDeps.AddStrictKey(rel.OutputCols, rel.OutputCols)
	}

	// Cardinality
	// -----------
	// Calculate cardinality of the set operator.
	rel.Cardinality = b.makeSetCardinality(
		setNode.Op(), leftProps.Cardinality, rightProps.Cardinality)

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildSetNode(setNode, rel)
	}
}

func (b *logicalPropsBuilder) buildValuesProps(values *ValuesExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, values, &rel.Shared)

	card := uint32(len(values.Rows))

	// Output Columns
	// --------------
	// Use output columns that are attached to the values op.
	rel.OutputCols = values.Cols.ToSet()

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable.

	// Outer Columns
	// -------------
	// Outer columns were already derived by buildSharedProps.

	// Functional Dependencies
	// -----------------------
	if card <= 1 {
		rel.FuncDeps.MakeMax1Row(rel.OutputCols)
	}

	// Cardinality
	// -----------
	// Cardinality is number of tuples in the Values operator.
	rel.Cardinality = props.Cardinality{Min: card, Max: card}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildValues(values, rel)
	}
}

func (b *logicalPropsBuilder) buildExplainProps(explain *ExplainExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, explain, &rel.Shared)

	// Output Columns
	// --------------
	// Output columns are stored in the definition.
	rel.OutputCols = explain.ColList.ToSet()

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
	rel.Cardinality = props.AnyCardinality

	// Statistics
	// ----------
	// Zero value for Stats is ok for Explain.
}

func (b *logicalPropsBuilder) buildShowTraceForSessionProps(
	showTrace *ShowTraceForSessionExpr, rel *props.Relational,
) {
	BuildSharedProps(b.mem, showTrace, &rel.Shared)

	// Output Columns
	// --------------
	// Output columns are stored in the definition.
	rel.OutputCols = showTrace.ColList.ToSet()

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
	rel.Cardinality = props.AnyCardinality

	// Statistics
	// ----------
	// Zero value for Stats is ok for ShowTrace.
}

func (b *logicalPropsBuilder) buildLimitProps(limit *LimitExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, limit, &rel.Shared)

	inputProps := limit.Input.Relational()

	haveConstLimit := false
	constLimit := int64(math.MaxUint32)
	if cnst, ok := limit.Limit.(*ConstExpr); ok {
		haveConstLimit = true
		constLimit = int64(*cnst.Value.(*tree.DInt))
	}

	// Side Effects
	// ------------
	// Negative limits can trigger a runtime error.
	if constLimit < 0 || !haveConstLimit {
		rel.CanHaveSideEffects = true
	}

	// Output Columns
	// --------------
	// Output columns are inherited from input.
	rel.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// Not null columns are inherited from input.
	rel.NotNullCols = inputProps.NotNullCols

	// Outer Columns
	// -------------
	// Outer columns were already derived by buildSharedProps.

	// Functional Dependencies
	// -----------------------
	// Inherit functional dependencies from input if limit is > 1, else just use
	// single row dependencies.
	if constLimit > 1 {
		rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	} else {
		rel.FuncDeps.MakeMax1Row(rel.OutputCols)
	}

	// Cardinality
	// -----------
	// Limit puts a cap on the number of rows returned by input.
	rel.Cardinality = inputProps.Cardinality
	if constLimit <= 0 {
		rel.Cardinality = props.ZeroCardinality
	} else if constLimit < math.MaxUint32 {
		rel.Cardinality = rel.Cardinality.Limit(uint32(constLimit))
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildLimit(limit, rel)
	}
}

func (b *logicalPropsBuilder) buildOffsetProps(offset *OffsetExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, offset, &rel.Shared)

	inputProps := offset.Input.Relational()

	// Output Columns
	// --------------
	// Output columns are inherited from input.
	rel.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// Not null columns are inherited from input.
	rel.NotNullCols = inputProps.NotNullCols

	// Outer Columns
	// -------------
	// Outer columns were already derived by buildSharedProps.

	// Functional Dependencies
	// -----------------------
	// Inherit functional dependencies from input.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)

	// Cardinality
	// -----------
	// Offset decreases the number of rows that are passed through from input.
	rel.Cardinality = inputProps.Cardinality
	if cnst, ok := offset.Offset.(*ConstExpr); ok {
		constOffset := int64(*cnst.Value.(*tree.DInt))
		if constOffset > 0 {
			if constOffset > math.MaxUint32 {
				constOffset = math.MaxUint32
			}
			rel.Cardinality = inputProps.Cardinality.Skip(uint32(constOffset))
		}
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildOffset(offset, rel)
	}
}

func (b *logicalPropsBuilder) buildMax1RowProps(max1Row *Max1RowExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, max1Row, &rel.Shared)

	inputProps := max1Row.Input.Relational()

	// Output Columns
	// --------------
	// Output columns are inherited from input.
	rel.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// Not null columns are inherited from input.
	rel.NotNullCols = inputProps.NotNullCols

	// Outer Columns
	// -------------
	// Outer columns were already derived by buildSharedProps.

	// Functional Dependencies
	// -----------------------
	// Max1Row always returns zero or one rows.
	rel.FuncDeps.MakeMax1Row(rel.OutputCols)

	// Cardinality
	// -----------
	// Max1Row ensures that zero or one row is returned from input.
	rel.Cardinality = inputProps.Cardinality.Limit(1)

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildMax1Row(max1Row, rel)
	}
}

func (b *logicalPropsBuilder) buildRowNumberProps(rowNum *RowNumberExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, rowNum, &rel.Shared)

	inputProps := rowNum.Input.Relational()

	// Output Columns
	// --------------
	// An extra output column is added to those projected by input operator.
	rel.OutputCols = inputProps.OutputCols.Copy()
	rel.OutputCols.Add(int(rowNum.ColID))

	// Not Null Columns
	// ----------------
	// The new output column is not null, and other columns inherit not null
	// property from input.
	rel.NotNullCols = inputProps.NotNullCols.Copy()
	rel.NotNullCols.Add(int(rowNum.ColID))

	// Outer Columns
	// -------------
	// Outer columns were already derived by buildSharedProps.

	// Functional Dependencies
	// -----------------------
	// Inherit functional dependencies from input, and add strict key FD for the
	// additional key column.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	if key, ok := rel.FuncDeps.StrictKey(); ok {
		// Any existing keys are still keys.
		rel.FuncDeps.AddStrictKey(key, rel.OutputCols)
	}
	rel.FuncDeps.AddStrictKey(util.MakeFastIntSet(int(rowNum.ColID)), rel.OutputCols)

	// Cardinality
	// -----------
	// Inherit cardinality from input.
	rel.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildRowNumber(rowNum, rel)
	}
}

func (b *logicalPropsBuilder) buildProjectSetProps(
	projectSet *ProjectSetExpr, rel *props.Relational,
) {
	BuildSharedProps(b.mem, projectSet, &rel.Shared)

	inputProps := projectSet.Input.Relational()

	// Output Columns
	// --------------
	// Output columns are the union between the output columns from the Zip and
	// the input.
	rel.OutputCols = projectSet.Zip.OutputCols()
	rel.OutputCols.UnionWith(inputProps.OutputCols)

	// Not Null Columns
	// ----------------
	// Inherit not null columns from input. All other columns are assumed to be
	// nullable.
	rel.NotNullCols = inputProps.NotNullCols.Copy()

	// Outer Columns
	// -------------
	// Outer columns were derived by BuildSharedProps; remove any that are bound
	// by input columns.
	rel.OuterCols.DifferenceWith(inputProps.OutputCols)

	// Functional Dependencies
	// -----------------------
	// Start with copy of FuncDepSet. Since ProjectSet is a lateral cross join
	// between the input and the functional zip (which has an empty FD set), call
	// MakeApply with an empty FD set. Then add outer columns, modify with
	// any additional not-null columns, and possibly simplify by calling
	// ProjectCols.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	rel.FuncDeps.MakeApply(&props.FuncDepSet{})
	addOuterColsToFuncDep(rel.OuterCols, &rel.FuncDeps)
	rel.FuncDeps.MakeNotNull(rel.NotNullCols)
	rel.FuncDeps.ProjectCols(rel.OutputCols)

	// Cardinality
	// -----------
	// Don't make any assumptions about cardinality of ProjectSet unless the
	// input cardinality is zero.
	if inputProps.Cardinality == props.ZeroCardinality {
		rel.Cardinality = props.ZeroCardinality
	} else {
		rel.Cardinality = props.AnyCardinality
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildProjectSet(projectSet, rel)
	}
}

func (b *logicalPropsBuilder) buildInsertProps(ins *InsertExpr, rel *props.Relational) {
	b.buildMutationProps(ins, rel)
}

func (b *logicalPropsBuilder) buildUpdateProps(upd *UpdateExpr, rel *props.Relational) {
	b.buildMutationProps(upd, rel)
}

func (b *logicalPropsBuilder) buildUpsertProps(ups *UpsertExpr, rel *props.Relational) {
	b.buildMutationProps(ups, rel)
}

func (b *logicalPropsBuilder) buildDeleteProps(del *DeleteExpr, rel *props.Relational) {
	b.buildMutationProps(del, rel)
}

func (b *logicalPropsBuilder) buildMutationProps(mutation RelExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, mutation, &rel.Shared)

	private := mutation.Private().(*MutationPrivate)

	// If no columns are output by the operator, then all other properties retain
	// default values.
	if !private.NeedResults {
		return
	}

	inputProps := mutation.Child(0).(RelExpr).Relational()
	md := b.mem.Metadata()
	tab := md.Table(private.Table)

	// Output Columns
	// --------------
	// Only non-mutation columns are output columns.
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		if cat.IsMutationColumn(tab, i) {
			continue
		}

		colID := int(private.Table.ColumnID(i))
		rel.OutputCols.Add(colID)
	}

	// Not Null Columns
	// ----------------
	// A column should be marked as not-null if the target table column is not
	// null or the corresponding insert and fetch/update columns are not null. In
	// other words, if either the source or destination column is not null, then
	// the column must be not null.
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		tabColID := private.Table.ColumnID(i)
		if !rel.OutputCols.Contains(int(tabColID)) {
			continue
		}

		// If the target table column is not null, then mark the column as not null.
		if !tab.Column(i).IsNullable() {
			rel.NotNullCols.Add(int(tabColID))
			continue
		}

		// If the input column is not null, then the result will be not null.
		inputColID := private.MapToInputID(tabColID)
		if inputProps.NotNullCols.Contains(int(inputColID)) {
			rel.NotNullCols.Add(int(private.Table.ColumnID(i)))
		}
	}

	// Outer Columns
	// -------------
	// Outer columns were already derived by buildSharedProps.

	// Functional Dependencies
	// -----------------------
	// Start with copy of FuncDepSet from input. Map the FDs of each source column
	// to the corresponding destination column by making the columns equivalent
	// and then filtering out the source columns via a call to ProjectCols.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	private.AddEquivTableCols(md, &rel.FuncDeps)
	rel.FuncDeps.ProjectCols(rel.OutputCols)

	// Cardinality
	// -----------
	// Inherit cardinality from input.
	rel.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildMutation(mutation, rel)
	}
}

func (b *logicalPropsBuilder) buildCreateTableProps(ct *CreateTableExpr, rel *props.Relational) {
	BuildSharedProps(b.mem, ct, &rel.Shared)
}

func (b *logicalPropsBuilder) buildFiltersItemProps(item *FiltersItem, scalar *props.Scalar) {
	BuildSharedProps(b.mem, item.Condition, &scalar.Shared)

	// Constraints
	// -----------
	cb := constraintsBuilder{md: b.mem.Metadata(), evalCtx: b.evalCtx}
	scalar.Constraints, scalar.TightConstraints = cb.buildConstraints(item.Condition)
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

	// Check for filter conjunct of the form: x = y.
	if eq, ok := item.Condition.(*EqExpr); ok {
		if leftVar, ok := eq.Left.(*VariableExpr); ok {
			if rightVar, ok := eq.Right.(*VariableExpr); ok {
				scalar.FuncDeps.AddEquivalency(leftVar.Col, rightVar.Col)
			}
		}
	}

	scalar.Populated = true
}

func (b *logicalPropsBuilder) buildProjectionsItemProps(
	item *ProjectionsItem, scalar *props.Scalar,
) {
	item.Typ = item.Element.DataType()
	BuildSharedProps(b.mem, item.Element, &scalar.Shared)
}

func (b *logicalPropsBuilder) buildAggregationsItemProps(
	item *AggregationsItem, scalar *props.Scalar,
) {
	item.Typ = item.Agg.DataType()
	BuildSharedProps(b.mem, item.Agg, &scalar.Shared)
}

func (b *logicalPropsBuilder) buildZipItemProps(item *ZipItem, scalar *props.Scalar) {
	item.Typ = item.Func.DataType()
	BuildSharedProps(b.mem, item.Func, &scalar.Shared)
}

// BuildSharedProps fills in the shared properties derived from the given
// expression's subtree.
func BuildSharedProps(mem *Memo, e opt.Expr, shared *props.Shared) {
	switch t := e.(type) {
	case *VariableExpr:
		// Variable introduces outer column.
		shared.OuterCols.Add(int(t.Col))
		return

	case *PlaceholderExpr:
		shared.HasPlaceholder = true
		return

	case *DivExpr:
		// Division by zero error is possible.
		shared.CanHaveSideEffects = true

	case *SubqueryExpr, *ExistsExpr, *AnyExpr, *ArrayFlattenExpr:
		shared.HasSubquery = true
		shared.HasCorrelatedSubquery = !e.Child(0).(RelExpr).Relational().OuterCols.Empty()

	case *FunctionExpr:
		if t.Properties.Impure {
			// Impure functions can return different value on each call.
			shared.CanHaveSideEffects = true
		}

	default:
		if opt.IsMutationOp(e) {
			shared.CanHaveSideEffects = true
			shared.CanMutate = true
		}
	}

	// Recursively build the shared properties.
	for i, n := 0, e.ChildCount(); i < n; i++ {
		child := e.Child(i)

		// Some expressions cache shared properties.
		var cached *props.Shared
		switch t := child.(type) {
		case RelExpr:
			cached = &t.Relational().Shared
		case ScalarPropsExpr:
			cached = &t.ScalarProps(mem).Shared
		}

		// Don't need to recurse if properties are cached.
		if cached != nil {
			shared.OuterCols.UnionWith(cached.OuterCols)
			if cached.HasPlaceholder {
				shared.HasPlaceholder = true
			}
			if cached.CanHaveSideEffects {
				shared.CanHaveSideEffects = true
			}
			if cached.CanMutate {
				shared.CanMutate = true
			}
			if cached.HasSubquery {
				shared.HasSubquery = true
			}
			if cached.HasCorrelatedSubquery {
				shared.HasCorrelatedSubquery = true
			}
		} else {
			BuildSharedProps(mem, e.Child(i), shared)
		}
	}
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
		// strict key. See the comment for cat.Index.LaxKeyColumnCount.
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

func (b *logicalPropsBuilder) makeSetCardinality(
	nt opt.Operator, left, right props.Cardinality,
) props.Cardinality {
	var card props.Cardinality
	switch nt {
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
	switch nt {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// Removing distinct values results in at least one row if input has at
		// least one row.
		card = card.AsLowAs(1)
	}
	return card
}

// rejectNullCols returns the set of all columns that are inferred to be not-
// null, based on the filter conditions.
func (b *logicalPropsBuilder) rejectNullCols(filters FiltersExpr) opt.ColSet {
	var notNullCols opt.ColSet
	for i := range filters {
		filterProps := filters[i].ScalarProps(b.mem)
		if filterProps.Constraints != nil {
			notNullCols.UnionWith(filterProps.Constraints.ExtractNotNullCols(b.evalCtx))
		}
	}
	return notNullCols
}

// addFiltersToFuncDep returns the union of all functional dependencies from
// each condition in the filters.
func (b *logicalPropsBuilder) addFiltersToFuncDep(filters FiltersExpr, fdset *props.FuncDepSet) {
	for i := range filters {
		filterProps := filters[i].ScalarProps(b.mem)
		fdset.AddFrom(&filterProps.FuncDeps)
	}

	if len(filters) <= 1 {
		return
	}

	// Some columns can only be determined to be constant from multiple
	// constraints (e.g. x <= 1 AND x >= 1); we intersect the constraints and
	// extract const columns from the intersection. But intersection is expensive
	// so we first do a quick check to rule out cases where each constraint refers
	// to a different set of columns.
	var cols opt.ColSet
	possibleIntersection := false
	for i := range filters {
		if c := filters[i].ScalarProps(b.mem).Constraints; c != nil {
			s := c.ExtractCols()
			if cols.Intersects(s) {
				possibleIntersection = true
				break
			}
			cols.UnionWith(s)
		}
	}

	if possibleIntersection {
		intersection := constraint.Unconstrained
		for i := range filters {
			if c := filters[i].ScalarProps(b.mem).Constraints; c != nil {
				intersection = intersection.Intersect(b.evalCtx, c)
			}
		}
		constCols := intersection.ExtractConstCols(b.evalCtx)
		fdset.AddConstants(constCols)
	}
}

// ensureLookupJoinInputProps lazily populates the relational properties that
// apply to the lookup side of the join, as if it were a Scan operator.
func ensureLookupJoinInputProps(join *LookupJoinExpr, sb *statisticsBuilder) *props.Relational {
	relational := &join.lookupProps
	if relational.OutputCols.Empty() {
		md := join.Memo().Metadata()
		relational.OutputCols = join.Cols.Difference(join.Input.Relational().OutputCols)
		relational.NotNullCols = tableNotNullCols(md, join.Table)
		relational.NotNullCols.IntersectionWith(relational.OutputCols)
		relational.Cardinality = props.AnyCardinality
		relational.FuncDeps.CopyFrom(makeTableFuncDep(md, join.Table))
		relational.FuncDeps.ProjectCols(relational.OutputCols)
		relational.Stats = *sb.makeTableStatistics(join.Table)
	}
	return relational
}

// ensureZigzagJoinInputProps lazily populates the relational properties that
// apply to the two sides of the join, as if it were a Scan operator.
func ensureZigzagJoinInputProps(join *ZigzagJoinExpr, sb *statisticsBuilder) {
	ensureInputPropsForIndex(
		join.Memo().Metadata(),
		join.LeftTable,
		join.LeftIndex,
		join.Cols,
		&join.leftProps,
		sb,
	)
	// For stats purposes, ensure left and right column sets are disjoint.
	ensureInputPropsForIndex(
		join.Memo().Metadata(),
		join.RightTable,
		join.RightIndex,
		join.Cols.Difference(join.leftProps.OutputCols),
		&join.rightProps,
		sb,
	)
}

// ensureInputPropsForIndex populates relational properties for the specified
// table and index at the specified logical properties pointer.
func ensureInputPropsForIndex(
	md *opt.Metadata,
	tabID opt.TableID,
	indexOrd int,
	outputCols opt.ColSet,
	relProps *props.Relational,
	sb *statisticsBuilder,
) {
	if relProps.OutputCols.Empty() {
		relProps.OutputCols = md.TableMeta(tabID).IndexColumns(indexOrd)
		relProps.OutputCols.IntersectionWith(outputCols)
		relProps.NotNullCols = tableNotNullCols(md, tabID)
		relProps.NotNullCols.IntersectionWith(relProps.OutputCols)
		relProps.Cardinality = props.AnyCardinality
		relProps.FuncDeps.CopyFrom(makeTableFuncDep(md, tabID))
		relProps.FuncDeps.ProjectCols(relProps.OutputCols)
		relProps.Stats = *sb.makeTableStatistics(tabID)
	}
}

// tableNotNullCols returns the set of not-NULL columns from the given table.
func tableNotNullCols(md *opt.Metadata, tabID opt.TableID) opt.ColSet {
	cs := opt.ColSet{}
	tab := md.Table(tabID)

	// Only iterate over non-mutation columns, since even non-null mutation
	// columns can be null during backfill.
	for i := 0; i < tab.ColumnCount(); i++ {
		// Non-null mutation columns can be null during backfill.
		if !tab.Column(i).IsNullable() && !cat.IsMutationColumn(tab, i) {
			cs.Add(int(tabID.ColumnID(i)))
		}
	}
	return cs
}

// addOuterColsToFuncDep adds the given outer columns and columns equivalent to
// them to the FD set. References to outer columns act like constants, since
// they are the same for all rows in the inner relation.
func addOuterColsToFuncDep(outerCols opt.ColSet, fdset *props.FuncDepSet) {
	equivCols := fdset.ComputeEquivClosure(outerCols)
	fdset.AddConstants(equivCols)
}

// joinPropsHelper is a helper that calculates and stores properties related to
// joins that are used internally when deriving logical properties and
// statistics.
type joinPropsHelper struct {
	join     RelExpr
	joinType opt.Operator

	leftProps  *props.Relational
	rightProps *props.Relational

	filters           FiltersExpr
	filtersFD         props.FuncDepSet
	filterNotNullCols opt.ColSet
	filterIsTrue      bool
	filterIsFalse     bool
}

func (h *joinPropsHelper) init(b *logicalPropsBuilder, joinExpr RelExpr) {
	h.join = joinExpr

	switch join := joinExpr.(type) {
	case *LookupJoinExpr:
		h.leftProps = joinExpr.Child(0).(RelExpr).Relational()
		ensureLookupJoinInputProps(join, &b.sb)
		h.joinType = join.JoinType
		h.rightProps = &join.lookupProps
		h.filters = *join.Child(1).(*FiltersExpr)
		b.addFiltersToFuncDep(h.filters, &h.filtersFD)
		h.filterNotNullCols = b.rejectNullCols(h.filters)

		// Apply the lookup join equalities.
		md := join.Memo().Metadata()
		index := md.Table(join.Table).Index(join.Index)
		for i, colID := range join.KeyCols {
			indexColID := join.Table.ColumnID(index.Column(i).Ordinal)
			h.filterNotNullCols.Add(int(colID))
			h.filterNotNullCols.Add(int(indexColID))
			h.filtersFD.AddEquivalency(colID, indexColID)
		}

		// Lookup join has implicit equality conditions on KeyCols.
		h.filterIsTrue = false
		h.filterIsFalse = h.filters.IsFalse()

	case *ZigzagJoinExpr:
		ensureZigzagJoinInputProps(join, &b.sb)
		h.joinType = opt.InnerJoinOp
		h.leftProps = &join.leftProps
		h.rightProps = &join.rightProps
		h.filters = *join.Child(0).(*FiltersExpr)
		b.addFiltersToFuncDep(h.filters, &h.filtersFD)
		h.filterNotNullCols = b.rejectNullCols(h.filters)

		// Apply the zigzag join equalities.
		for i := range join.LeftEqCols {
			leftColID := join.LeftEqCols[i]
			rightColID := join.RightEqCols[i]

			h.filterNotNullCols.Add(int(leftColID))
			h.filterNotNullCols.Add(int(rightColID))
			h.filtersFD.AddEquivalency(leftColID, rightColID)
		}

	default:
		h.joinType = join.Op()
		h.leftProps = joinExpr.Child(0).(RelExpr).Relational()
		h.rightProps = join.Child(1).(RelExpr).Relational()

		h.filters = *join.Child(2).(*FiltersExpr)
		b.addFiltersToFuncDep(h.filters, &h.filtersFD)
		h.filterNotNullCols = b.rejectNullCols(h.filters)
		h.filterIsTrue = h.filters.IsTrue()
		h.filterIsFalse = h.filters.IsFalse()
	}
}

func (h *joinPropsHelper) outputCols() opt.ColSet {
	// Output columns are union of columns from left and right inputs, except
	// in case of:
	//
	//   1. semi and anti joins, which only project the left columns
	//   2. lookup joins, which can project a subset of input columns
	//
	var cols opt.ColSet
	switch h.joinType {
	case opt.SemiJoinOp, opt.AntiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		cols = h.leftProps.OutputCols.Copy()

	default:
		cols = h.leftProps.OutputCols.Union(h.rightProps.OutputCols)
	}

	if lookup, ok := h.join.(*LookupJoinExpr); ok {
		// Remove any columns that are not projected by the lookup join.
		cols.IntersectionWith(lookup.Cols)
	}

	return cols
}

func (h *joinPropsHelper) notNullCols() opt.ColSet {
	var notNullCols opt.ColSet

	// Left/full outer joins can result in right columns becoming null.
	// Otherwise, propagate not null setting from right child.
	switch h.joinType {
	case opt.LeftJoinOp, opt.FullJoinOp, opt.LeftJoinApplyOp, opt.FullJoinApplyOp,
		opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:

	default:
		notNullCols = h.rightProps.NotNullCols.Copy()
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch h.joinType {
	case opt.RightJoinOp, opt.FullJoinOp, opt.RightJoinApplyOp, opt.FullJoinApplyOp:

	default:
		notNullCols.UnionWith(h.leftProps.NotNullCols)
	}

	// Add not-null constraints from ON predicate for inner and semi-joins.
	switch h.joinType {
	case opt.InnerJoinOp, opt.SemiJoinApplyOp:
		notNullCols.UnionWith(h.filterNotNullCols)
	}

	return notNullCols
}

func (h *joinPropsHelper) setFuncDeps(rel *props.Relational) {
	// Start with FDs from left side, and modify based on join type.
	rel.FuncDeps.CopyFrom(&h.leftProps.FuncDeps)

	// Anti and semi joins only inherit FDs from left side, since right side
	// simply acts like a filter.
	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		// Add FDs from the ON predicate, which include equivalent columns and
		// constant columns. Any outer columns become constants as well.
		rel.FuncDeps.AddFrom(&h.filtersFD)
		addOuterColsToFuncDep(rel.OuterCols, &rel.FuncDeps)
		rel.FuncDeps.MakeNotNull(rel.NotNullCols)

		// Call ProjectCols to remove any FDs involving columns from the right side.
		rel.FuncDeps.ProjectCols(rel.OutputCols)

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
		if opt.IsJoinApplyOp(h.join) {
			rel.FuncDeps.MakeApply(&h.rightProps.FuncDeps)
		} else {
			rel.FuncDeps.MakeProduct(&h.rightProps.FuncDeps)
		}

		notNullInputCols := h.leftProps.NotNullCols.Union(h.rightProps.NotNullCols)

		switch h.joinType {
		case opt.InnerJoinOp, opt.InnerJoinApplyOp:
			// Add FDs from the ON predicate, which include equivalent columns and
			// constant columns.
			rel.FuncDeps.AddFrom(&h.filtersFD)
			addOuterColsToFuncDep(rel.OuterCols, &rel.FuncDeps)

		case opt.RightJoinOp, opt.RightJoinApplyOp:
			rel.FuncDeps.MakeOuter(h.leftProps.OutputCols, notNullInputCols)

		case opt.LeftJoinOp, opt.LeftJoinApplyOp:
			rel.FuncDeps.MakeOuter(h.rightProps.OutputCols, notNullInputCols)

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
			inputCols := h.leftProps.OutputCols.Union(h.rightProps.OutputCols)
			if !inputCols.Intersects(notNullInputCols) {
				rel.FuncDeps.DowngradeKey()
			}
			rel.FuncDeps.MakeOuter(h.leftProps.OutputCols, notNullInputCols)
			rel.FuncDeps.MakeOuter(h.rightProps.OutputCols, notNullInputCols)
		}

		rel.FuncDeps.MakeNotNull(rel.NotNullCols)

		// Call ProjectCols to trigger simplification, since outer joins may have
		// created new possibilities for simplifying removed columns.
		rel.FuncDeps.ProjectCols(rel.OutputCols)
	}
}

func (h *joinPropsHelper) cardinality() props.Cardinality {
	left := h.leftProps.Cardinality

	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Semi/Anti join cardinality never exceeds left input cardinality, and
		// allows zero rows.
		return left.AsLowAs(0)
	}

	// Other join types can return up to cross product of rows.
	right := h.rightProps.Cardinality
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
