// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

	// Side Effects
	// ------------
	// A Locking option is a side-effect (we don't want to elide this scan).
	if scan.Locking != nil {
		rel.CanHaveSideEffects = true
	}

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
		rel.FuncDeps.CopyFrom(MakeTableFuncDep(md, scan.Table))
		if scan.Constraint != nil {
			rel.FuncDeps.AddConstants(scan.Constraint.ExtractConstCols(b.evalCtx))
		}
		if tabMeta := md.TableMeta(scan.Table); tabMeta.Constraints != nil {
			b.addFiltersToFuncDep(*tabMeta.Constraints.(*FiltersExpr), &rel.FuncDeps)
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
	} else {
		if hardLimit > 0 && hardLimit < math.MaxUint32 {
			rel.Cardinality = rel.Cardinality.Limit(uint32(hardLimit))
		}
		if scan.Constraint != nil {
			b.updateCardinalityFromConstraint(scan.Constraint, rel)
		}
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildScan(scan, rel)
	}
}

func (b *logicalPropsBuilder) buildSequenceSelectProps(
	seq *SequenceSelectExpr, rel *props.Relational,
) {
	// Output Columns
	// --------------
	// Output columns are stored in the definition.
	rel.OutputCols = seq.Cols.ToSet()

	// Not Null Columns
	// ----------------
	// Every column is not null.
	rel.NotNullCols = rel.OutputCols

	// Outer Columns
	// -------------
	// The operator never has outer columns.

	// Functional Dependencies
	// -----------------------
	rel.FuncDeps.MakeMax1Row(rel.OutputCols)

	// Cardinality
	// -----------
	rel.Cardinality = props.OneCardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildSequenceSelect(rel)
	}
}

func (b *logicalPropsBuilder) buildSelectProps(sel *SelectExpr, rel *props.Relational) {
	BuildSharedProps(sel, &rel.Shared)

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
	// Outer columns were derived by BuildSharedProps; remove any that are bound
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
	} else {
		b.updateCardinalityFromFilters(sel.Filters, rel)
	}

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildSelect(sel, rel)
	}
}

func (b *logicalPropsBuilder) buildProjectProps(prj *ProjectExpr, rel *props.Relational) {
	BuildSharedProps(prj, &rel.Shared)

	inputProps := prj.Input.Relational()

	// Output Columns
	// --------------
	// Output columns are the union of synthesized columns and passthrough columns.
	for i := range prj.Projections {
		rel.OutputCols.Add(prj.Projections[i].Col)
	}
	rel.OutputCols.UnionWith(prj.Passthrough)

	// Not Null Columns
	// ----------------
	// Not null columns were derived by initUnexportedFields; just intersect them
	// with the output columns.
	rel.NotNullCols = prj.notNullCols.Intersection(rel.OutputCols)

	// Outer Columns
	// -------------
	// Outer columns were derived by BuildSharedProps; remove any that are bound
	// by input columns.
	rel.OuterCols.DifferenceWith(inputProps.OutputCols)

	// Functional Dependencies
	// -----------------------
	// The functional dependencies were derived by initUnexportedFields; just
	// remove columns that are not projected.
	rel.FuncDeps.CopyFrom(&prj.internalFuncDeps)
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
	BuildSharedProps(join, &rel.Shared)

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
	// Outer columns were initially set by BuildSharedProps. Remove any that are
	// bound by the input columns.
	inputCols := h.leftProps.OutputCols.Union(h.rightProps.OutputCols)
	rel.OuterCols.DifferenceWith(inputCols)

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
	BuildSharedProps(indexJoin, &rel.Shared)

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
	// Outer columns were already derived by BuildSharedProps.

	// Functional Dependencies
	// -----------------------
	// Start with the input FD set, and join that with the table's FD.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	rel.FuncDeps.AddFrom(MakeTableFuncDep(md, indexJoin.Table))
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

func (b *logicalPropsBuilder) buildGeoLookupJoinProps(
	join *GeoLookupJoinExpr, rel *props.Relational,
) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildZigzagJoinProps(join *ZigzagJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
}

func (b *logicalPropsBuilder) buildMergeJoinProps(join *MergeJoinExpr, rel *props.Relational) {
	b.buildJoinProps(join, rel)
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

func (b *logicalPropsBuilder) buildEnsureDistinctOnProps(
	distinctOn *EnsureDistinctOnExpr, rel *props.Relational,
) {
	b.buildGroupingExprProps(distinctOn, rel)
}

func (b *logicalPropsBuilder) buildUpsertDistinctOnProps(
	distinctOn *UpsertDistinctOnExpr, rel *props.Relational,
) {
	b.buildGroupingExprProps(distinctOn, rel)
}

func (b *logicalPropsBuilder) buildEnsureUpsertDistinctOnProps(
	distinctOn *EnsureUpsertDistinctOnExpr, rel *props.Relational,
) {
	b.buildGroupingExprProps(distinctOn, rel)
}

func (b *logicalPropsBuilder) buildGroupingExprProps(groupExpr RelExpr, rel *props.Relational) {
	BuildSharedProps(groupExpr, &rel.Shared)

	inputProps := groupExpr.Child(0).(RelExpr).Relational()
	aggs := *groupExpr.Child(1).(*AggregationsExpr)
	groupPrivate := groupExpr.Private().(*GroupingPrivate)
	groupingCols := groupPrivate.GroupingCols

	// Output Columns
	// --------------
	// Output columns are the union of grouping columns with columns from the
	// aggregate projection list.
	rel.OutputCols = groupingCols.Copy()
	for i := range aggs {
		rel.OutputCols.Add(aggs[i].Col)
	}

	// Not Null Columns
	// ----------------
	// Propagate not null setting from input columns that are being grouped.
	rel.NotNullCols = inputProps.NotNullCols.Intersection(groupingCols)

	for i := range aggs {
		item := &aggs[i]
		agg := ExtractAggFunc(item.Agg)

		// Some aggregates never return NULL, regardless of input.
		if opt.AggregateIsNeverNull(agg.Op()) {
			rel.NotNullCols.Add(item.Col)
			continue
		}

		// If there is a possibility that the aggregate function has zero input
		// rows, then it may return NULL. This is possible with ScalarGroupBy and
		// with AggFilter.
		if groupExpr.Op() == opt.ScalarGroupByOp || item.Agg.Op() == opt.AggFilterOp {
			continue
		}

		// Most aggregate functions return a non-NULL result if they have at least
		// one input row with non-NULL argument value, and if all argument values are non-NULL.
		if opt.AggregateIsNeverNullOnNonNullInput(agg.Op()) {
			inputCols := ExtractAggInputColumns(agg)
			if inputCols.SubsetOf(inputProps.NotNullCols) {
				rel.NotNullCols.Add(item.Col)
			}
		}
	}

	// Outer Columns
	// -------------
	// Outer columns were derived by BuildSharedProps; remove any that are bound
	// by input columns.
	rel.OuterCols.DifferenceWith(inputProps.OutputCols)

	// Functional Dependencies
	// -----------------------
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	if groupingCols.Empty() {
		// When there are no grouping columns, then there is a single group, and
		// therefore at most one output row.
		rel.FuncDeps.MakeMax1Row(rel.OutputCols)
	} else {
		// Start by eliminating input columns that aren't projected.
		rel.FuncDeps.ProjectCols(rel.OutputCols)

		// The output of most of the grouping operators forms a strict key because
		// they eliminate all duplicates in the grouping columns. However, the
		// UpsertDistinctOn and EnsureUpsertDistinctOn operators do not group
		// NULL values together, so they only form a lax key when NULL values
		// are possible.
		if groupPrivate.NullsAreDistinct && !groupingCols.SubsetOf(rel.NotNullCols) {
			rel.FuncDeps.AddLaxKey(groupingCols, rel.OutputCols)
		} else {
			rel.FuncDeps.AddStrictKey(groupingCols, rel.OutputCols)
		}
	}

	// Cardinality
	// -----------
	if groupExpr.Op() == opt.ScalarGroupByOp {
		// Scalar GroupBy returns exactly one row.
		rel.Cardinality = props.OneCardinality
	} else {
		// GroupBy and DistinctOn act like a filter, never returning more rows
		// than the input has. However, if the input has at least one row, then
		// at least one row will also be returned by GroupBy and DistinctOn.
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
	BuildSharedProps(setNode, &rel.Shared)

	leftProps := setNode.Child(0).(RelExpr).Relational()
	rightProps := setNode.Child(1).(RelExpr).Relational()
	setPrivate := setNode.Private().(*SetPrivate)
	if len(setPrivate.OutCols) != len(setPrivate.LeftCols) ||
		len(setPrivate.OutCols) != len(setPrivate.RightCols) {
		panic(errors.AssertionFailedf(
			"lists in SetPrivate are not all the same length. new:%d, left:%d, right:%d",
			log.Safe(len(setPrivate.OutCols)), log.Safe(len(setPrivate.LeftCols)), log.Safe(len(setPrivate.RightCols)),
		))
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
		if leftProps.NotNullCols.Contains((setPrivate.LeftCols)[i]) &&
			rightProps.NotNullCols.Contains((setPrivate.RightCols)[i]) {
			rel.NotNullCols.Add((setPrivate.OutCols)[i])
		}
	}

	// Outer Columns
	// -------------
	// Outer columns were already derived by BuildSharedProps.

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
	BuildSharedProps(values, &rel.Shared)

	card := uint32(len(values.Rows))

	// Output Columns
	// --------------
	// Use output columns that are attached to the values op.
	rel.OutputCols = values.Cols.ToSet()

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable, unless they contain only constant
	// non-null values.

	for colIdx, col := range values.Cols {
		notNull := true
		for rowIdx := range values.Rows {
			val := values.Rows[rowIdx].(*TupleExpr).Elems[colIdx]
			if !opt.IsConstValueOp(val) || val.Op() == opt.NullOp {
				// Null or not a constant.
				notNull = false
				break
			}
		}
		if notNull {
			rel.NotNullCols.Add(col)
		}
	}

	// Outer Columns
	// -------------
	// Outer columns were already derived by BuildSharedProps.

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

func (b *logicalPropsBuilder) buildBasicProps(e opt.Expr, cols opt.ColList, rel *props.Relational) {
	BuildSharedProps(e, &rel.Shared)

	// Output Columns
	// --------------
	rel.OutputCols = cols.ToSet()

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable.

	// Outer Columns
	// -------------
	// No outer columns.

	// Functional Dependencies
	// -----------------------
	// Empty FD set.

	// Cardinality
	// -----------
	// Don't make any assumptions about cardinality of output.
	rel.Cardinality = props.AnyCardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildUnknown(rel)
	}
}

func (b *logicalPropsBuilder) buildWithProps(with *WithExpr, rel *props.Relational) {
	// Copy over the props from the input.
	inputProps := with.Main.Relational()

	BuildSharedProps(with, &rel.Shared)

	// Side Effects
	// ------------
	// This expression has side effects if either Binding or Input has side
	// effects, which is what is computed by the call to BuildSharedProps.

	// Output Columns
	// --------------
	// Inherited from the input expression.
	rel.OutputCols = inputProps.OutputCols

	// Not Null Columns
	// ----------------
	// Inherited from the input expression.
	rel.NotNullCols = inputProps.NotNullCols

	// Outer Columns
	// -------------
	// The outer columns are the union of the outer columns from Binding or Input,
	// which is what is computed by the call to BuildSharedProps.

	// Functional Dependencies
	// -----------------------
	rel.FuncDeps = inputProps.FuncDeps

	// Cardinality
	// -----------
	rel.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	// Inherited from the input expression.
	rel.Stats = inputProps.Stats
}

func (b *logicalPropsBuilder) buildWithScanProps(withScan *WithScanExpr, rel *props.Relational) {
	BuildSharedProps(withScan, &rel.Shared)
	bindingProps := withScan.BindingProps

	// Side Effects
	// ------------
	// WithScan has no side effects (even if the original expression had them).

	// Output Columns
	// --------------
	rel.OutputCols = withScan.OutCols.ToSet()

	// Not Null Columns
	// ----------------
	rel.NotNullCols = opt.TranslateColSet(bindingProps.NotNullCols, withScan.InCols, withScan.OutCols)

	// Outer Columns
	// -------------
	// No outer columns.

	// Functional Dependencies
	// -----------------------
	// Inherit dependencies from the referenced expression (remapping the
	// columns).
	rel.FuncDeps.CopyFrom(&bindingProps.FuncDeps)
	for i := range withScan.InCols {
		rel.FuncDeps.AddEquivalency(withScan.InCols[i], withScan.OutCols[i])
	}
	rel.FuncDeps.ProjectCols(withScan.OutCols.ToSet())

	// Cardinality
	// -----------
	// Inherit from the referenced expression.
	rel.Cardinality = bindingProps.Cardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildWithScan(withScan, rel)
	}
}

func (b *logicalPropsBuilder) buildRecursiveCTEProps(rec *RecursiveCTEExpr, rel *props.Relational) {
	BuildSharedProps(rec, &rel.Shared)

	// Output Columns
	// --------------
	rel.OutputCols = rec.OutCols.ToSet()

	// Not Null Columns
	// ----------------
	// All columns are assumed to be nullable.

	// Outer Columns
	// -------------
	// No outer columns.

	// Functional Dependencies
	// -----------------------
	// No known FDs.

	// Cardinality
	// -----------
	// At least the cardinality of the initial buffer.
	rel.Cardinality = props.AnyCardinality.AtLeast(rec.Initial.Relational().Cardinality)

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildUnknown(rel)
	}
}

func (b *logicalPropsBuilder) buildExplainProps(explain *ExplainExpr, rel *props.Relational) {
	b.buildBasicProps(explain, explain.ColList, rel)
}

func (b *logicalPropsBuilder) buildShowTraceForSessionProps(
	showTrace *ShowTraceForSessionExpr, rel *props.Relational,
) {
	b.buildBasicProps(showTrace, showTrace.ColList, rel)
}

func (b *logicalPropsBuilder) buildOpaqueRelProps(op *OpaqueRelExpr, rel *props.Relational) {
	b.buildBasicProps(op, op.Columns, rel)
}

func (b *logicalPropsBuilder) buildOpaqueMutationProps(
	op *OpaqueMutationExpr, rel *props.Relational,
) {
	b.buildBasicProps(op, op.Columns, rel)
}

func (b *logicalPropsBuilder) buildOpaqueDDLProps(op *OpaqueDDLExpr, rel *props.Relational) {
	b.buildBasicProps(op, op.Columns, rel)
}

func (b *logicalPropsBuilder) buildAlterTableSplitProps(
	split *AlterTableSplitExpr, rel *props.Relational,
) {
	b.buildBasicProps(split, split.Columns, rel)
}

func (b *logicalPropsBuilder) buildAlterTableUnsplitProps(
	unsplit *AlterTableUnsplitExpr, rel *props.Relational,
) {
	b.buildBasicProps(unsplit, unsplit.Columns, rel)
}

func (b *logicalPropsBuilder) buildAlterTableUnsplitAllProps(
	unsplitAll *AlterTableUnsplitAllExpr, rel *props.Relational,
) {
	b.buildBasicProps(unsplitAll, unsplitAll.Columns, rel)
}

func (b *logicalPropsBuilder) buildAlterTableRelocateProps(
	relocate *AlterTableRelocateExpr, rel *props.Relational,
) {
	b.buildBasicProps(relocate, relocate.Columns, rel)
}

func (b *logicalPropsBuilder) buildControlJobsProps(ctl *ControlJobsExpr, rel *props.Relational) {
	b.buildBasicProps(ctl, opt.ColList{}, rel)
}

func (b *logicalPropsBuilder) buildCancelQueriesProps(
	cancel *CancelQueriesExpr, rel *props.Relational,
) {
	b.buildBasicProps(cancel, opt.ColList{}, rel)
}

func (b *logicalPropsBuilder) buildCancelSessionsProps(
	cancel *CancelSessionsExpr, rel *props.Relational,
) {
	b.buildBasicProps(cancel, opt.ColList{}, rel)
}

func (b *logicalPropsBuilder) buildExportProps(export *ExportExpr, rel *props.Relational) {
	b.buildBasicProps(export, export.Columns, rel)
}

func (b *logicalPropsBuilder) buildLimitProps(limit *LimitExpr, rel *props.Relational) {
	BuildSharedProps(limit, &rel.Shared)

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
	// Outer columns were already derived by BuildSharedProps.

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
	BuildSharedProps(offset, &rel.Shared)

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
	// Outer columns were already derived by BuildSharedProps.

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
	BuildSharedProps(max1Row, &rel.Shared)

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
	// Outer columns were already derived by BuildSharedProps.

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

func (b *logicalPropsBuilder) buildOrdinalityProps(ord *OrdinalityExpr, rel *props.Relational) {
	BuildSharedProps(ord, &rel.Shared)

	inputProps := ord.Input.Relational()

	// Output Columns
	// --------------
	// An extra output column is added to those projected by input operator.
	rel.OutputCols = inputProps.OutputCols.Copy()
	rel.OutputCols.Add(ord.ColID)

	// Not Null Columns
	// ----------------
	// The new output column is not null, and other columns inherit not null
	// property from input.
	rel.NotNullCols = inputProps.NotNullCols.Copy()
	rel.NotNullCols.Add(ord.ColID)

	// Outer Columns
	// -------------
	// Outer columns were already derived by BuildSharedProps.

	// Functional Dependencies
	// -----------------------
	// Inherit functional dependencies from input, and add strict key FD for the
	// additional key column.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)
	if key, ok := rel.FuncDeps.StrictKey(); ok {
		// Any existing keys are still keys.
		rel.FuncDeps.AddStrictKey(key, rel.OutputCols)
	}
	rel.FuncDeps.AddStrictKey(opt.MakeColSet(ord.ColID), rel.OutputCols)

	// Cardinality
	// -----------
	// Inherit cardinality from input.
	rel.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildOrdinality(ord, rel)
	}
}

func (b *logicalPropsBuilder) buildWindowProps(window *WindowExpr, rel *props.Relational) {
	BuildSharedProps(window, &rel.Shared)

	inputProps := window.Input.Relational()

	// Output Columns
	// --------------
	// Output columns are all the passthrough columns with the addition of the
	// window function column.
	rel.OutputCols = inputProps.OutputCols.Copy()
	for _, w := range window.Windows {
		rel.OutputCols.Add(w.Col)
	}

	// Not Null Columns
	// ----------------
	// Inherit not null columns from input.
	// TODO(justin): in many cases the added column may not be nullable.
	rel.NotNullCols = inputProps.NotNullCols.Intersection(rel.OutputCols)

	// Outer Columns
	// -------------
	// Outer columns were derived by BuildSharedProps; remove any that are bound
	// by input columns.
	rel.OuterCols.DifferenceWith(inputProps.OutputCols)

	// Functional Dependencies
	// -----------------------
	// Functional dependencies are the same as the input.
	// TODO(justin): in many cases there are more FDs to be derived, some
	// examples include:
	// * row_number+the partition is a key.
	// * rank is determined by the partition and the value being ordered by.
	// * aggregations/first_value/last_value are determined by the partition.
	rel.FuncDeps.CopyFrom(&inputProps.FuncDeps)

	// Cardinality
	// -----------
	// Window functions never change the cardinality of their input.
	rel.Cardinality = inputProps.Cardinality

	// Statistics
	// ----------
	if !b.disableStats {
		b.sb.buildWindow(window, rel)
	}
}

func (b *logicalPropsBuilder) buildProjectSetProps(
	projectSet *ProjectSetExpr, rel *props.Relational,
) {
	BuildSharedProps(projectSet, &rel.Shared)

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
	BuildSharedProps(mutation, &rel.Shared)

	private := mutation.Private().(*MutationPrivate)

	// If no rows are output by the operator, then all other properties retain
	// default values.
	if !private.NeedResults() {
		return
	}

	inputProps := mutation.Child(0).(RelExpr).Relational()
	md := b.mem.Metadata()
	tab := md.Table(private.Table)

	// Output Columns
	// --------------
	// Only non-mutation columns are output columns.
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		if private.IsColumnOutput(i) {
			colID := private.Table.ColumnID(i)
			rel.OutputCols.Add(colID)
		}
	}

	// The output columns of the mutation will also include all
	// columns it allowed to pass through.
	for _, col := range private.PassthroughCols {
		if col != 0 {
			rel.OutputCols.Add(col)
		}
	}

	// Not Null Columns
	// ----------------
	// A column should be marked as not-null if the target table column is not
	// null or the corresponding insert and fetch/update columns are not null. In
	// other words, if either the source or destination column is not null, then
	// the column must be not null.
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		tabColID := private.Table.ColumnID(i)
		if !rel.OutputCols.Contains(tabColID) {
			continue
		}

		// If the target table column is not null, then mark the column as not null.
		if !tab.Column(i).IsNullable() {
			rel.NotNullCols.Add(tabColID)
			continue
		}

		// If the input column is not null, then the result will be not null.
		if inputProps.NotNullCols.Contains(private.ReturnCols[i]) {
			rel.NotNullCols.Add(private.Table.ColumnID(i))
		}
	}

	// Outer Columns
	// -------------
	// Outer columns were already derived by BuildSharedProps.

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
	BuildSharedProps(ct, &rel.Shared)
}

func (b *logicalPropsBuilder) buildCreateViewProps(cv *CreateViewExpr, rel *props.Relational) {
	BuildSharedProps(cv, &rel.Shared)
}

func (b *logicalPropsBuilder) buildFiltersItemProps(item *FiltersItem, scalar *props.Scalar) {
	BuildSharedProps(item.Condition, &scalar.Shared)

	// Constraints
	// -----------
	cb := constraintsBuilder{md: b.mem.Metadata(), evalCtx: b.evalCtx}
	// TODO(rytaft): Using local variables here to avoid a data race. It would be
	// better to avoid lazy building of props altogether.
	constraints, tightConstraints := cb.buildConstraints(item.Condition)
	if constraints.IsUnconstrained() {
		scalar.Constraints, scalar.TightConstraints = nil, false
	} else {
		scalar.Constraints, scalar.TightConstraints = constraints, tightConstraints
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
}

func (b *logicalPropsBuilder) buildProjectionsItemProps(
	item *ProjectionsItem, scalar *props.Scalar,
) {
	item.Typ = item.Element.DataType()
	BuildSharedProps(item.Element, &scalar.Shared)
}

func (b *logicalPropsBuilder) buildAggregationsItemProps(
	item *AggregationsItem, scalar *props.Scalar,
) {
	item.Typ = item.Agg.DataType()
	BuildSharedProps(item.Agg, &scalar.Shared)
}

func (b *logicalPropsBuilder) buildWindowsItemProps(item *WindowsItem, scalar *props.Scalar) {
	item.Typ = item.Function.DataType()
	BuildSharedProps(item.Function, &scalar.Shared)
}

func (b *logicalPropsBuilder) buildZipItemProps(item *ZipItem, scalar *props.Scalar) {
	item.Typ = item.Fn.DataType()
	BuildSharedProps(item.Fn, &scalar.Shared)
}

// BuildSharedProps fills in the shared properties derived from the given
// expression's subtree. It will only recurse into a child when it is not
// already caching properties.
//
// Note that shared is an "input-output" argument, and should be assumed
// to be partially filled in already. Boolean fields such as HasPlaceholder,
// CanHaveSideEffects and HasCorrelatedSubquery should never be reset to false
// once set to true.
func BuildSharedProps(e opt.Expr, shared *props.Shared) {
	switch t := e.(type) {
	case *VariableExpr:
		// Variable introduces outer column.
		shared.OuterCols.Add(t.Col)
		return

	case *PlaceholderExpr:
		shared.HasPlaceholder = true
		return

	case *DivExpr:
		// Division by zero error is possible, unless the right-hand side is a
		// non-zero constant.
		var nonZero bool
		if c, ok := t.Right.(*ConstExpr); ok {
			switch v := c.Value.(type) {
			case *tree.DInt:
				nonZero = (*v != 0)
			case *tree.DFloat:
				nonZero = (*v != 0.0)
			case *tree.DDecimal:
				nonZero = !v.IsZero()
			}
		}
		if !nonZero {
			shared.CanHaveSideEffects = true
		}

	case *SubqueryExpr, *ExistsExpr, *AnyExpr, *ArrayFlattenExpr:
		shared.HasSubquery = true
		if hasOuterCols(e.Child(0)) {
			shared.HasCorrelatedSubquery = true
		}
		if t.Op() == opt.AnyOp && hasOuterCols(e.Child(1)) {
			shared.HasCorrelatedSubquery = true
		}

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
			cached = &t.ScalarProps().Shared
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
			BuildSharedProps(e.Child(i), shared)
		}
	}
}

// hasOuterCols returns true if the given expression has outer columns (i.e.
// columns that are referenced by the expression but not bound by it).
func hasOuterCols(e opt.Expr) bool {
	switch t := e.(type) {
	case *VariableExpr:
		return true
	case RelExpr:
		return !t.Relational().OuterCols.Empty()
	case ScalarPropsExpr:
		return !t.ScalarProps().Shared.OuterCols.Empty()
	}

	for i, n := 0, e.ChildCount(); i < n; i++ {
		if hasOuterCols(e.Child(i)) {
			return true
		}
	}

	return false
}

// MakeTableFuncDep returns the set of functional dependencies derived from the
// given base table. The set is derived lazily and is cached in the metadata,
// since it may be accessed multiple times during query optimization. For more
// details, see Relational.FuncDepSet.
func MakeTableFuncDep(md *opt.Metadata, tabID opt.TableID) *props.FuncDepSet {
	fd, ok := md.TableAnnotation(tabID, fdAnnID).(*props.FuncDepSet)
	if ok {
		// Already made.
		return fd
	}

	// Make now and annotate the metadata table with it for next time.
	var allCols opt.ColSet
	tab := md.Table(tabID)
	for i := 0; i < tab.DeletableColumnCount(); i++ {
		allCols.Add(tabID.ColumnID(i))
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
			keyCols.Add(tabID.ColumnID(ord))
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
		filterProps := filters[i].ScalarProps()
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
		filterProps := filters[i].ScalarProps()
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
		if c := filters[i].ScalarProps().Constraints; c != nil {
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
			if c := filters[i].ScalarProps().Constraints; c != nil {
				intersection = intersection.Intersect(b.evalCtx, c)
			}
		}
		constCols := intersection.ExtractConstCols(b.evalCtx)
		fdset.AddConstants(constCols)
	}
}

// updateCardinalityFromFilters determines whether a tight cardinality bound
// can be determined from the filters, and updates the cardinality accordingly.
// Specifically, it may be possible to determine a tight bound if the key
// column(s) are constrained to a finite number of values.
func (b *logicalPropsBuilder) updateCardinalityFromFilters(
	filters FiltersExpr, rel *props.Relational,
) {
	for i := range filters {
		filterProps := filters[i].ScalarProps()
		if filterProps.Constraints == nil {
			continue
		}

		for j, n := 0, filterProps.Constraints.Length(); j < n; j++ {
			c := filterProps.Constraints.Constraint(j)
			b.updateCardinalityFromConstraint(c, rel)
		}
	}
}

// updateCardinalityFromConstraint determines whether a tight cardinality
// bound can be determined from the constraint, and updates the cardinality
// accordingly. Specifically, it may be possible to determine a tight bound
// if the key column(s) are constrained to a finite number of values.
func (b *logicalPropsBuilder) updateCardinalityFromConstraint(
	c *constraint.Constraint, rel *props.Relational,
) {
	cols := c.Columns.ColSet()
	if !rel.FuncDeps.ColsAreLaxKey(cols) {
		return
	}

	count := c.CalculateMaxResults(b.evalCtx, cols, rel.NotNullCols)
	if count != 0 && count < math.MaxUint32 {
		rel.Cardinality = rel.Cardinality.Limit(uint32(count))
	}
}

// ensureLookupJoinInputProps lazily populates the relational properties that
// apply to the lookup side of the join, as if it were a Scan operator.
func ensureLookupJoinInputProps(join *LookupJoinExpr, sb *statisticsBuilder) *props.Relational {
	relational := &join.lookupProps
	if relational.OutputCols.Empty() {
		md := join.Memo().Metadata()
		relational.OutputCols = join.Cols.Difference(join.Input.Relational().OutputCols)

		// Include the key columns in the output columns.
		index := md.Table(join.Table).Index(join.Index)
		for i := range join.KeyCols {
			indexColID := join.Table.ColumnID(index.Column(i).Ordinal)
			relational.OutputCols.Add(indexColID)
		}

		relational.NotNullCols = tableNotNullCols(md, join.Table)
		relational.NotNullCols.IntersectionWith(relational.OutputCols)
		relational.Cardinality = props.AnyCardinality
		relational.FuncDeps.CopyFrom(MakeTableFuncDep(md, join.Table))
		relational.FuncDeps.ProjectCols(relational.OutputCols)
		relational.Stats = *sb.makeTableStatistics(join.Table)
	}
	return relational
}

// ensureGeoLookupJoinInputProps lazily populates the relational properties
// that apply to the lookup side of the join, as if it were a Scan operator.
func ensureGeoLookupJoinInputProps(
	join *GeoLookupJoinExpr, sb *statisticsBuilder,
) *props.Relational {
	relational := &join.lookupProps
	if relational.OutputCols.Empty() {
		md := join.Memo().Metadata()
		relational.OutputCols = join.Cols.Difference(join.Input.Relational().OutputCols)
		relational.NotNullCols = tableNotNullCols(md, join.Table)
		relational.NotNullCols.IntersectionWith(relational.OutputCols)
		relational.Cardinality = props.AnyCardinality

		// TODO(rytaft): See if we need to use different functional dependencies
		// for the inverted index.
		relational.FuncDeps.CopyFrom(MakeTableFuncDep(md, join.Table))
		relational.FuncDeps.ProjectCols(relational.OutputCols)

		// TODO(rytaft): Change this to use inverted index stats once available.
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
		relProps.FuncDeps.CopyFrom(MakeTableFuncDep(md, tabID))
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
		if !tab.Column(i).IsNullable() {
			cs.Add(tabID.ColumnID(i))
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

	selfJoinCols opt.ColSet
}

func (h *joinPropsHelper) init(b *logicalPropsBuilder, joinExpr RelExpr) {
	h.join = joinExpr

	switch join := joinExpr.(type) {
	case *LookupJoinExpr:
		h.leftProps = joinExpr.Child(0).(RelExpr).Relational()
		ensureLookupJoinInputProps(join, &b.sb)
		h.joinType = join.JoinType
		h.rightProps = &join.lookupProps
		h.filters = join.On
		b.addFiltersToFuncDep(h.filters, &h.filtersFD)
		h.filterNotNullCols = b.rejectNullCols(h.filters)

		// Apply the lookup join equalities.
		md := join.Memo().Metadata()
		index := md.Table(join.Table).Index(join.Index)
		for i, colID := range join.KeyCols {
			indexColID := join.Table.ColumnID(index.Column(i).Ordinal)
			h.filterNotNullCols.Add(colID)
			h.filterNotNullCols.Add(indexColID)
			h.filtersFD.AddEquivalency(colID, indexColID)
			if colID == indexColID {
				// This can happen if an index join was converted into a lookup join.
				h.selfJoinCols.Add(colID)
			}
		}

		// Lookup join has implicit equality conditions on KeyCols.
		h.filterIsTrue = false
		h.filterIsFalse = h.filters.IsFalse()

	case *GeoLookupJoinExpr:
		h.leftProps = joinExpr.Child(0).(RelExpr).Relational()
		ensureGeoLookupJoinInputProps(join, &b.sb)
		h.joinType = join.JoinType
		h.rightProps = &join.lookupProps
		h.filters = join.On
		b.addFiltersToFuncDep(h.filters, &h.filtersFD)
		h.filterNotNullCols = b.rejectNullCols(h.filters)

		// Geospatial lookup join always has a filter condition on the index keys.
		h.filterIsTrue = false
		h.filterIsFalse = h.filters.IsFalse()

	case *MergeJoinExpr:
		h.joinType = join.JoinType
		h.leftProps = join.Left.Relational()
		h.rightProps = join.Right.Relational()
		h.filters = join.On
		b.addFiltersToFuncDep(h.filters, &h.filtersFD)
		h.filterNotNullCols = b.rejectNullCols(h.filters)

		// Apply the merge join equalities.
		for i := range join.LeftEq {
			l := join.LeftEq[i].ID()
			r := join.RightEq[i].ID()
			h.filterNotNullCols.Add(l)
			h.filterNotNullCols.Add(r)
			h.filtersFD.AddEquivalency(l, r)
		}

		// Merge join has implicit equality conditions on the merge columns.
		h.filterIsTrue = false
		h.filterIsFalse = h.filters.IsFalse()

	case *ZigzagJoinExpr:
		ensureZigzagJoinInputProps(join, &b.sb)
		h.joinType = opt.InnerJoinOp
		h.leftProps = &join.leftProps
		h.rightProps = &join.rightProps
		h.filters = join.On
		b.addFiltersToFuncDep(h.filters, &h.filtersFD)
		h.filterNotNullCols = b.rejectNullCols(h.filters)

		// Apply the zigzag join equalities.
		for i := range join.LeftEqCols {
			leftColID := join.LeftEqCols[i]
			rightColID := join.RightEqCols[i]

			h.filterNotNullCols.Add(leftColID)
			h.filterNotNullCols.Add(rightColID)
			h.filtersFD.AddEquivalency(leftColID, rightColID)
		}

	default:
		h.joinType = join.Op()
		h.leftProps = join.Child(0).(RelExpr).Relational()
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
	case opt.LeftJoinOp, opt.FullJoinOp, opt.LeftJoinApplyOp,
		opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:

	default:
		notNullCols = h.rightProps.NotNullCols.Copy()
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch h.joinType {
	case opt.RightJoinOp, opt.FullJoinOp:

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

		case opt.LeftJoinOp, opt.LeftJoinApplyOp:
			rel.FuncDeps.MakeLeftOuter(
				&h.leftProps.FuncDeps, &h.filtersFD,
				h.leftProps.OutputCols, h.rightProps.OutputCols, notNullInputCols,
			)

		case opt.RightJoinOp:
			rel.FuncDeps.MakeLeftOuter(
				&h.rightProps.FuncDeps, &h.filtersFD,
				h.rightProps.OutputCols, h.leftProps.OutputCols, notNullInputCols,
			)

		case opt.FullJoinOp:
			rel.FuncDeps.MakeFullOuter(h.leftProps.OutputCols, h.rightProps.OutputCols, notNullInputCols)

		default:
			panic(errors.AssertionFailedf("unhandled join type %s", h.joinType))
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

	case opt.RightJoinOp:
		return innerJoinCard.AtLeast(right)

	case opt.FullJoinOp:
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

func (b *logicalPropsBuilder) buildFakeRelProps(fake *FakeRelExpr, rel *props.Relational) {
	*rel = *fake.Props
}
