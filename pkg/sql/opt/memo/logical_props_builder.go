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
	"github.com/cockroachdb/cockroach/pkg/util"
)

// logicalPropsBuilder is a helper class that consolidates the code that derives
// a parent expression's logical properties from those of its children.
type logicalPropsBuilder struct {
	evalCtx *tree.EvalContext
	sb      statisticsBuilder
}

// buildProps is called by the memo group construction code in order to
// initialize the new group's logical properties.
// NOTE: When deriving properties from children, be sure to keep the child
//       properties immutable by copying them if necessary.
// NOTE: The parent expression is passed as an ExprView for convenient access
//       to children, but certain properties on it are not yet defined (like
//       its logical properties!).
func (b *logicalPropsBuilder) buildProps(ev ExprView) props.Logical {
	if ev.IsRelational() {
		return b.buildRelationalProps(ev)
	}
	return b.buildScalarProps(ev)
}

func (b *logicalPropsBuilder) buildRelationalProps(ev ExprView) props.Logical {
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

	case opt.LookupJoinOp:
		return b.buildLookupJoinProps(ev)

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

	case opt.ExplainOp:
		return b.buildExplainProps(ev)

	case opt.ShowTraceOp, opt.ShowTraceForSessionOp:
		return b.buildShowTraceProps(ev)

	case opt.RowNumberOp:
		return b.buildRowNumberProps(ev)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.op))
}

func (b *logicalPropsBuilder) buildScanProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	md := ev.Metadata()
	def := ev.Private().(*ScanOpDef)

	// Scan output columns are stored in the definition.
	logical.Relational.OutputCols = def.Cols

	// Initialize not-NULL columns from the table schema.
	addNotNullColsFromTable(logical.Relational, md, def.Table, def.Cols)

	// Initialize weak keys from the table schema.
	logical.Relational.WeakKeys = md.TableWeakKeys(def.Table)
	filterWeakKeys(logical.Relational)

	// Don't bother setting cardinality from scan's HardLimit or Constraint,
	// since those are created by exploration patterns and won't ever be the
	// basis for the logical props on a newly created memo group.
	logical.Relational.Cardinality = props.AnyCardinality

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildScan(def)

	return logical
}

func (b *logicalPropsBuilder) buildSelectProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	inputProps := ev.childGroup(0).logical.Relational
	filterProps := ev.childGroup(1).logical.Scalar

	// Inherit input properties as starting point.
	*logical.Relational = *inputProps

	if filterProps.Constraints != nil && filterProps.Constraints != constraint.Unconstrained {
		constraintNotNullCols := filterProps.Constraints.ExtractNotNullCols()
		if !constraintNotNullCols.Empty() {
			logical.Relational.NotNullCols = logical.Relational.NotNullCols.Union(constraintNotNullCols)
		}
	}

	// Any outer columns from the filter that are not bound by the input columns
	// are outer columns for the Select expression, in addition to any outer
	// columns inherited from the input expression.
	if !filterProps.OuterCols.SubsetOf(inputProps.OutputCols) {
		logical.Relational.OuterCols = filterProps.OuterCols.Difference(inputProps.OutputCols)
		logical.Relational.OuterCols.UnionWith(inputProps.OuterCols)
	}

	// Select filter can filter any or all rows.
	logical.Relational.Cardinality = inputProps.Cardinality.AsLowAs(0)

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildSelect(ev.Child(1), &inputProps.Stats)

	return logical
}

func (b *logicalPropsBuilder) buildProjectProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	inputProps := ev.childGroup(0).logical.Relational
	projectionProps := ev.childGroup(1).logical.Scalar
	projections := ev.Child(1)
	projectionsDef := ev.Child(1).Private().(*ProjectionsOpDef)

	// Use output columns from projection list.
	logical.Relational.OutputCols = projectionsDef.AllCols()

	// Inherit not null columns from input, but only use those that are also
	// output columns.
	logical.Relational.NotNullCols = inputProps.NotNullCols.Copy()
	logical.Relational.NotNullCols.IntersectionWith(logical.Relational.OutputCols)

	// Also add any column that projects a constant value, since the optimizer
	// sometimes constructs these in order to guarantee a not-null column.
	for i := 0; i < projections.ChildCount(); i++ {
		child := projections.Child(i)
		if child.IsConstValue() {
			if ExtractConstDatum(child) != tree.DNull {
				logical.Relational.NotNullCols.Add(int(projectionsDef.SynthesizedCols[i]))
			}
		}
	}

	// Any outer columns from the projection list that are not bound by the input
	// columns are outer columns from the Project expression, in addition to any
	// outer columns inherited from the input expression.
	if !projectionProps.OuterCols.SubsetOf(inputProps.OutputCols) {
		logical.Relational.OuterCols = projectionProps.OuterCols.Difference(inputProps.OutputCols)
	}
	logical.Relational.OuterCols.UnionWith(inputProps.OuterCols)

	// Inherit weak keys that are composed entirely of output columns.
	logical.Relational.WeakKeys = inputProps.WeakKeys
	filterWeakKeys(logical.Relational)

	// Inherit cardinality from input.
	logical.Relational.Cardinality = inputProps.Cardinality

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildProject(&inputProps.Stats)

	return logical
}

func (b *logicalPropsBuilder) buildJoinProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	leftProps := ev.childGroup(0).logical.Relational
	rightProps := ev.childGroup(1).logical.Relational
	onProps := ev.childGroup(2).logical.Scalar

	// Output columns are union of columns from left and right inputs, except
	// in case of semi and anti joins, which only project the left columns.
	logical.Relational.OutputCols = leftProps.OutputCols.Copy()
	switch ev.Operator() {
	case opt.SemiJoinOp, opt.AntiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:

	default:
		logical.Relational.OutputCols.UnionWith(rightProps.OutputCols)
	}

	// Left/full outer joins can result in right columns becoming null.
	// Otherwise, propagate not null setting from right child.
	switch ev.Operator() {
	case opt.LeftJoinOp, opt.FullJoinOp, opt.LeftJoinApplyOp, opt.FullJoinApplyOp,
		opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:

	default:
		logical.Relational.NotNullCols = rightProps.NotNullCols.Copy()
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch ev.Operator() {
	case opt.RightJoinOp, opt.FullJoinOp, opt.RightJoinApplyOp, opt.FullJoinApplyOp:

	default:
		logical.Relational.NotNullCols.UnionWith(leftProps.NotNullCols)
	}

	// Any outer columns from the filter that are not bound by the input columns
	// are outer columns for the Join expression, in addition to any outer columns
	// inherited from the input expressions.
	inputCols := leftProps.OutputCols.Union(rightProps.OutputCols)
	if !onProps.OuterCols.SubsetOf(inputCols) {
		logical.Relational.OuterCols = onProps.OuterCols.Difference(inputCols)
	}
	if ev.IsJoinApply() {
		// Outer columns of right side of apply join can be bound by output
		// columns of left side of apply join.
		if !rightProps.OuterCols.SubsetOf(leftProps.OutputCols) {
			logical.Relational.OuterCols.UnionWith(rightProps.OuterCols.Difference(leftProps.OutputCols))
		}
	} else {
		logical.Relational.OuterCols.UnionWith(rightProps.OuterCols)
	}
	logical.Relational.OuterCols.UnionWith(leftProps.OuterCols)

	// TODO(andyk): Need to derive additional weak keys for joins, for example
	//              when weak keys on both sides are equivalent cols.
	switch ev.Operator() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		logical.Relational.WeakKeys = leftProps.WeakKeys

	default:
		// If cardinality of one side is <= 1, then inherit other side's keys.
		if rightProps.Cardinality.IsZeroOrOne() {
			logical.Relational.WeakKeys = leftProps.WeakKeys
		}
		if leftProps.Cardinality.IsZeroOrOne() {
			logical.Relational.WeakKeys = logical.Relational.WeakKeys.Combine(rightProps.WeakKeys)
		}
	}

	// Calculate cardinality of each join type.
	logical.Relational.Cardinality = calcJoinCardinality(
		ev, leftProps.Cardinality, rightProps.Cardinality,
	)

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildJoin(ev.Operator(), &leftProps.Stats, &rightProps.Stats, ev.Child(2))

	return logical
}

func (b *logicalPropsBuilder) buildLookupJoinProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	inputProps := ev.childGroup(0).logical.Relational

	// Inherit input properties as starting point.
	*logical.Relational = *inputProps

	md := ev.Metadata()
	def := ev.Private().(*LookupJoinDef)

	// Lookup join output columns are stored in the definition.
	logical.Relational.OutputCols = def.Cols

	// Add not-NULL columns from the table schema.
	addNotNullColsFromTable(logical.Relational, md, def.Table, def.Cols)

	// Add weak keys from the table schema. There may already be some weak keys
	// present from the input index.
	tableWeakKeys := md.TableWeakKeys(def.Table)
	for _, weakKey := range tableWeakKeys {
		logical.Relational.WeakKeys.Add(weakKey)
	}
	filterWeakKeys(logical.Relational)

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildLookupJoin(&inputProps.Stats)

	return logical
}

func (b *logicalPropsBuilder) buildGroupByProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	inputProps := ev.childGroup(0).logical.Relational
	aggProps := ev.childGroup(1).logical.Scalar

	// Output columns are the union of grouping columns with columns from the
	// aggregate projection list.
	groupingColSet := ev.Private().(*GroupByDef).GroupingCols
	aggColList := ev.Child(1).Private().(opt.ColList)
	logical.Relational.OutputCols = groupingColSet.Union(opt.ColListToSet(aggColList))

	// Propagate not null setting from input columns that are being grouped.
	logical.Relational.NotNullCols = inputProps.NotNullCols.Copy()
	logical.Relational.NotNullCols.IntersectionWith(groupingColSet)

	// Any outer columns from aggregation expressions that are not bound by the
	// input columns are outer columns.
	logical.Relational.OuterCols = aggProps.OuterCols.Difference(inputProps.OutputCols)
	logical.Relational.OuterCols.UnionWith(inputProps.OuterCols)

	// Scalar group by has no grouping columns and always a single row.
	if !groupingColSet.Empty() {
		// The grouping columns always form a key because the GroupBy operation
		// eliminates all duplicates. The result WeakKeys property either contains
		// only the grouping column set, or else it contains one or more weak keys
		// that are strict subsets of the grouping column set. This is because
		// the grouping column set contains every output column (except aggregate
		// columns, which aren't relevant since they're newly synthesized).
		if inputProps.WeakKeys.ContainsSubsetOf(groupingColSet) {
			logical.Relational.WeakKeys = inputProps.WeakKeys
			filterWeakKeys(logical.Relational)
		} else {
			logical.Relational.WeakKeys = opt.WeakKeys{groupingColSet}
		}
	}

	if groupingColSet.Empty() {
		// Scalar GroupBy returns exactly one row.
		logical.Relational.Cardinality = props.OneCardinality
	} else {
		// GroupBy acts like a filter, never returning more rows than the input
		// has. However, if the input has at least one row, then at least one row
		// will also be returned by GroupBy.
		logical.Relational.Cardinality = inputProps.Cardinality.AsLowAs(1)
	}

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildGroupBy(&inputProps.Stats, groupingColSet)

	return logical
}

func (b *logicalPropsBuilder) buildSetProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	leftProps := ev.childGroup(0).logical.Relational
	rightProps := ev.childGroup(1).logical.Relational
	colMap := *ev.Private().(*SetOpColMap)
	if len(colMap.Out) != len(colMap.Left) || len(colMap.Out) != len(colMap.Right) {
		panic(fmt.Errorf("lists in SetOpColMap are not all the same length. new:%d, left:%d, right:%d",
			len(colMap.Out), len(colMap.Left), len(colMap.Right)))
	}

	// Set the new output columns.
	logical.Relational.OutputCols = opt.ColListToSet(colMap.Out)

	// Columns have to be not-null on both sides to be not-null in result.
	// colMap matches columns on the left and right sides of the operator
	// with the output columns, since OutputCols are not ordered and may
	// not correspond to each other.
	for i := range colMap.Out {
		if leftProps.NotNullCols.Contains(int((colMap.Left)[i])) &&
			rightProps.NotNullCols.Contains(int((colMap.Right)[i])) {
			logical.Relational.NotNullCols.Add(int((colMap.Out)[i]))
		}
	}

	// Outer columns from either side are outer columns for set operation.
	logical.Relational.OuterCols = leftProps.OuterCols.Union(rightProps.OuterCols)

	// Calculate cardinality of the set operator.
	logical.Relational.Cardinality = calcSetCardinality(
		ev, leftProps.Cardinality, rightProps.Cardinality,
	)

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildSetOp(ev.Operator(), &leftProps.Stats, &rightProps.Stats, &colMap)

	return logical
}

func (b *logicalPropsBuilder) buildValuesProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	// Use output columns that are attached to the values op.
	logical.Relational.OutputCols = opt.ColListToSet(ev.Private().(opt.ColList))

	// Union outer columns from all row expressions.
	for i := 0; i < ev.ChildCount(); i++ {
		logical.Relational.OuterCols.UnionWith(ev.childGroup(i).logical.Scalar.OuterCols)
	}

	// Cardinality is number of tuples in the Values operator.
	card := uint32(ev.ChildCount())
	logical.Relational.Cardinality = props.Cardinality{Min: card, Max: card}

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildValues()

	return logical
}

func (b *logicalPropsBuilder) buildExplainProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	def := ev.Private().(*ExplainOpDef)
	logical.Relational.OutputCols = opt.ColListToSet(def.ColList)
	logical.Relational.Cardinality = props.AnyCardinality

	// Zero value for Stats is ok for Explain.

	return logical
}

func (b *logicalPropsBuilder) buildShowTraceProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	def := ev.Private().(*ShowTraceOpDef)
	logical.Relational.OutputCols = opt.ColListToSet(def.ColList)
	logical.Relational.Cardinality = props.AnyCardinality

	// Zero value for Stats is ok for ShowTrace.

	return logical
}

func (b *logicalPropsBuilder) buildLimitProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	inputProps := ev.Child(0).Logical().Relational
	limit := ev.Child(1)
	limitProps := limit.Logical().Scalar

	// Start with pass-through props from input.
	*logical.Relational = *inputProps

	// Inherit outer columns from limit expression.
	if !limitProps.OuterCols.Empty() {
		logical.Relational.OuterCols = limitProps.OuterCols.Union(inputProps.OuterCols)
	}

	// Limit puts a cap on the number of rows returned by input.
	if limit.Operator() == opt.ConstOp {
		constLimit := int64(*limit.Private().(*tree.DInt))
		if constLimit <= 0 {
			logical.Relational.Cardinality = props.ZeroCardinality
		} else if constLimit < math.MaxUint32 {
			logical.Relational.Cardinality = logical.Relational.Cardinality.AtMost(uint32(constLimit))
		}
	}

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildLimit(limit, &inputProps.Stats)

	return logical
}

func (b *logicalPropsBuilder) buildOffsetProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	inputProps := ev.Child(0).Logical().Relational
	offset := ev.Child(1)
	offsetProps := offset.Logical().Scalar

	// Start with pass-through props from input.
	*logical.Relational = *inputProps

	// Inherit outer columns from offset expression.
	if !offsetProps.OuterCols.Empty() {
		logical.Relational.OuterCols = offsetProps.OuterCols.Union(inputProps.OuterCols)
	}

	// Offset decreases the number of rows that are passed through from input.
	if offset.Operator() == opt.ConstOp {
		constOffset := int64(*offset.Private().(*tree.DInt))
		if constOffset > 0 {
			if constOffset > math.MaxUint32 {
				constOffset = math.MaxUint32
			}
			logical.Relational.Cardinality = inputProps.Cardinality.Skip(uint32(constOffset))
		}
	}

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildOffset(offset, &inputProps.Stats)

	return logical
}

func (b *logicalPropsBuilder) buildMax1RowProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	inputProps := ev.Child(0).Logical().Relational

	// Start with pass-through props from input.
	*logical.Relational = *inputProps

	// Max1Row ensures that zero or one row is returned by input.
	logical.Relational.Cardinality = logical.Relational.Cardinality.AtMost(1)

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildMax1Row(&inputProps.Stats)

	return logical
}

func (b *logicalPropsBuilder) buildRowNumberProps(ev ExprView) props.Logical {
	logical := props.Logical{Relational: &props.Relational{}}

	inputProps := ev.Child(0).Logical().Relational
	*logical.Relational = *inputProps
	def := ev.Private().(*RowNumberDef)

	logical.Relational.OutputCols = logical.Relational.OutputCols.Copy()
	logical.Relational.OutputCols.Add(int(def.ColID))

	logical.Relational.NotNullCols = logical.Relational.NotNullCols.Copy()
	logical.Relational.NotNullCols.Add(int(def.ColID))

	logical.Relational.WeakKeys = logical.Relational.WeakKeys.Copy()
	logical.Relational.WeakKeys.Add(util.MakeFastIntSet(int(def.ColID)))

	b.sb.init(b.evalCtx, &logical.Relational.Stats, logical.Relational, ev, &keyBuffer{})
	b.sb.buildRowNumber(&inputProps.Stats)

	return logical
}

func (b *logicalPropsBuilder) buildScalarProps(ev ExprView) props.Logical {
	logical := props.Logical{Scalar: &props.Scalar{Type: InferType(ev)}}

	switch ev.Operator() {
	case opt.VariableOp:
		// Variable introduces outer column.
		logical.Scalar.OuterCols.Add(int(ev.Private().(opt.ColumnID)))
		return logical

	case opt.SubqueryOp, opt.ExistsOp, opt.AnyOp:
		// Inherit outer columns from input query.
		logical.Scalar.OuterCols = ev.childGroup(0).logical.Relational.OuterCols

		// Any has additional scalar value that can contain outer references.
		if ev.Operator() == opt.AnyOp {
			cols := ev.childGroup(1).logical.Scalar.OuterCols
			logical.Scalar.OuterCols = logical.Scalar.OuterCols.Union(cols)
		}

		if !logical.Scalar.OuterCols.Empty() {
			logical.Scalar.HasCorrelatedSubquery = true
		}
		return logical
	}

	// By default, union outer cols from all children, both relational and scalar.
	for i := 0; i < ev.ChildCount(); i++ {
		childLogical := &ev.childGroup(i).logical
		logical.Scalar.OuterCols.UnionWith(childLogical.OuterCols())

		// Propagate HasCorrelatedSubquery up the scalar expression tree.
		if childLogical.Scalar != nil && childLogical.Scalar.HasCorrelatedSubquery {
			logical.Scalar.HasCorrelatedSubquery = true
		}
	}

	switch ev.Operator() {
	case opt.ProjectionsOp:
		// For a ProjectionsOp, the passthrough cols are also outer cols.
		logical.Scalar.OuterCols.UnionWith(ev.Private().(*ProjectionsOpDef).PassthroughCols)

	case opt.FiltersOp, opt.TrueOp, opt.FalseOp:
		// Calculate constraints for any expressions that could be filters.
		cb := constraintsBuilder{md: ev.Metadata(), evalCtx: b.evalCtx}
		logical.Scalar.Constraints, logical.Scalar.TightConstraints = cb.buildConstraints(ev)
	}
	return logical
}

// filterWeakKeys ensures that each weak key is a subset of the output columns.
// It respects immutability by making a copy of the weak keys if they need to be
// updated.
func filterWeakKeys(relational *props.Relational) {
	var filtered opt.WeakKeys
	for i, weakKey := range relational.WeakKeys {
		// Discard weak keys that have columns that are not part of the output
		// column set.
		if !weakKey.SubsetOf(relational.OutputCols) {
			if filtered == nil {
				filtered = make(opt.WeakKeys, i, len(relational.WeakKeys)-1)
				copy(filtered, relational.WeakKeys[:i])
			}
		} else {
			if filtered != nil {
				filtered = append(filtered, weakKey)
			}
		}
	}
	if filtered != nil {
		relational.WeakKeys = filtered
	}
}

func calcJoinCardinality(ev ExprView, left, right props.Cardinality) props.Cardinality {
	var card props.Cardinality
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

func calcSetCardinality(ev ExprView, left, right props.Cardinality) props.Cardinality {
	var card props.Cardinality
	switch ev.Operator() {
	case opt.UnionOp, opt.UnionAllOp:
		// Add cardinality of left and right inputs.
		card = left.Add(right)

	case opt.IntersectOp, opt.IntersectAllOp:
		// Use minimum of left and right Max cardinality.
		card = props.Cardinality{Min: 0, Max: left.Max}
		card = card.AtMost(right.Max)

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

// addNotNullColsFromTable adds not-NULL columns to the given relational props
// from the given table schema.
func addNotNullColsFromTable(
	relational *props.Relational, md *opt.Metadata, tabID opt.TableID, cols opt.ColSet,
) {
	tab := md.Table(tabID)

	for i := 0; i < tab.ColumnCount(); i++ {
		if !tab.Column(i).IsNullable() {
			colID := md.TableColumn(tabID, i)
			if cols.Contains(int(colID)) {
				relational.NotNullCols.Add(int(colID))
			}
		}
	}
}
