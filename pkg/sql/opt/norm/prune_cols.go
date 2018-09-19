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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// NeededCols returns the set of columns needed by the given group. It is an
// alias for outerCols that's used for clarity with the UnusedCols patterns.
func (c *CustomFuncs) NeededCols(group memo.GroupID) opt.ColSet {
	return c.OuterCols(group)
}

// NeededCols2 unions the set of columns needed by either of the given groups.
func (c *CustomFuncs) NeededCols2(left, right memo.GroupID) opt.ColSet {
	return c.OuterCols(left).Union(c.OuterCols(right))
}

// NeededCols3 unions the set of columns needed by any of the given groups.
func (c *CustomFuncs) NeededCols3(group1, group2, group3 memo.GroupID) opt.ColSet {
	cols := c.OuterCols(group1).Union(c.OuterCols(group2))
	cols.UnionWith(c.OuterCols(group3))
	return cols
}

// NeededColsGroupBy unions the columns needed by either of a GroupBy's
// operands - either aggregations, groupingCols, or requested orderings. This
// case doesn't fit any of the neededCols methods because groupingCols is a
// private, not a group.
func (c *CustomFuncs) NeededColsGroupBy(aggs memo.GroupID, def memo.PrivateID) opt.ColSet {
	groupByDef := c.mem.LookupPrivate(def).(*memo.GroupByDef)
	colSet := groupByDef.GroupingCols.Union(groupByDef.Ordering.ColSet())
	colSet.UnionWith(c.OuterCols(aggs))
	return colSet
}

// NeededColsLimit unions the columns needed by Projections with the columns in
// the Ordering of a Limit/Offset operator.
func (c *CustomFuncs) NeededColsLimit(
	projections memo.GroupID, ordering memo.PrivateID,
) opt.ColSet {
	return c.OuterCols(projections).Union(c.ExtractOrdering(ordering).ColSet())
}

// NeededColsRowNumber unions the columns needed by Projections with the columns
// in the Ordering of a RowNumber operator.
func (c *CustomFuncs) NeededColsRowNumber(projections memo.GroupID, def memo.PrivateID) opt.ColSet {
	rowNumberDef := c.mem.LookupPrivate(def).(*memo.RowNumberDef)
	return c.OuterCols(projections).Union(rowNumberDef.Ordering.ColSet())
}

// NeededColsExplain returns the columns needed by Explain's required physical
// properties.
func (c *CustomFuncs) NeededColsExplain(def memo.PrivateID) opt.ColSet {
	explainDef := c.mem.LookupPrivate(def).(*memo.ExplainOpDef)
	return explainDef.Props.ColSet()
}

// CanPruneCols returns true if the target group has extra columns that are not
// needed at this level of the tree, and can be eliminated by one of the
// PruneCols rules. CanPruneCols uses the PruneCols property to determine the
// set of columns which can be pruned, and subtracts the given set of
// additional needed columns from that. See the props.Relational.Rule.PruneCols
// comment for more details.
func (c *CustomFuncs) CanPruneCols(target memo.GroupID, neededCols opt.ColSet) bool {
	ev := memo.MakeNormExprView(c.mem, target)

	// Handle special scalar operators.
	var pruneCols opt.ColSet
	switch ev.Operator() {
	case opt.ProjectionsOp, opt.AggregationsOp:
		// Both Projections and Aggregations allow their columns to be pruned if
		// they're never used in a higher-level expression.
		pruneCols = c.OutputCols(target)

	default:
		pruneCols = DerivePruneCols(ev)
	}

	return !pruneCols.SubsetOf(neededCols)
}

// PruneCols creates an expression that discards any outputs columns of the
// given group that are not used. If the target expression type supports column
// filtering (like Scan, Values, Projections, etc.), then create a new instance
// of that operator that does the filtering. Otherwise, construct a Project
// operator that wraps the operator and does the filtering. The new Project
// operator will be pushed down the tree until it merges with another operator
// that supports column filtering.
func (c *CustomFuncs) PruneCols(target memo.GroupID, neededCols opt.ColSet) memo.GroupID {
	targetExpr := c.mem.NormExpr(target)
	switch targetExpr.Operator() {
	case opt.ScanOp:
		return c.pruneScanCols(target, neededCols)

	case opt.ValuesOp:
		return c.pruneValuesCols(target, neededCols)

	case opt.AggregationsOp:
		groups, cols := filterColList(
			c.mem.LookupList(targetExpr.AsAggregations().Aggs()),
			c.ExtractColList(targetExpr.AsAggregations().Cols()),
			neededCols,
		)
		return c.f.ConstructAggregations(c.f.InternList(groups), c.f.InternColList(cols))

	case opt.ProjectionsOp:
		def := c.ExtractProjectionsOpDef(targetExpr.AsProjections().Def())
		groups, cols := filterColList(
			c.mem.LookupList(targetExpr.AsProjections().Elems()),
			def.SynthesizedCols,
			neededCols,
		)
		newDef := memo.ProjectionsOpDef{
			SynthesizedCols: cols,
			PassthroughCols: def.PassthroughCols.Intersection(neededCols),
		}
		return c.f.ConstructProjections(c.f.InternList(groups), c.f.InternProjectionsOpDef(&newDef))

	default:
		// In other cases, we wrap the input in a Project operator.

		// Get the subset of the target group's output columns that should not be
		// pruned. Don't prune if the target output column is needed by a higher-
		// level expression, or if it's not part of the PruneCols set.
		pruneCols := DerivePruneCols(memo.MakeNormExprView(c.mem, target)).Difference(neededCols)
		colSet := c.OutputCols(target).Difference(pruneCols)
		return c.f.ConstructSimpleProject(target, colSet)
	}
}

// pruneScanCols constructs a new Scan operator based on the given existing Scan
// operator, but projecting only the needed columns.
func (c *CustomFuncs) pruneScanCols(scan memo.GroupID, neededCols opt.ColSet) memo.GroupID {
	colSet := c.OutputCols(scan).Intersection(neededCols)
	scanExpr := c.mem.NormExpr(scan).AsScan()
	existing := c.mem.LookupPrivate(scanExpr.Def()).(*memo.ScanOpDef)
	new := *existing
	new.Cols = colSet
	return c.f.ConstructScan(c.mem.InternScanOpDef(&new))
}

// pruneValuesCols constructs a new Values operator based on the given existing
// Values operator. The new operator will have the same set of rows, but
// containing only the needed columns. Other columns are discarded.
func (c *CustomFuncs) pruneValuesCols(values memo.GroupID, neededCols opt.ColSet) memo.GroupID {
	valuesExpr := c.mem.NormExpr(values).AsValues()
	existingCols := c.ExtractColList(valuesExpr.Cols())
	newCols := make(opt.ColList, 0, neededCols.Len())

	existingRows := c.mem.LookupList(valuesExpr.Rows())
	newRows := MakeListBuilder(c)

	// Create new list of columns that only contains needed columns.
	for _, colID := range existingCols {
		if !neededCols.Contains(int(colID)) {
			continue
		}
		newCols = append(newCols, colID)
	}

	// newElems is used to store tuple values.
	newElems := MakeListBuilder(c)

	for _, row := range existingRows {
		tuple := c.mem.NormExpr(row).AsTuple()
		existingElems := c.mem.LookupList(tuple.Elems())
		typ := c.ExtractType(tuple.Typ()).(types.TTuple)

		n := 0
		for i, elem := range existingElems {
			if !neededCols.Contains(int(existingCols[i])) {
				continue
			}
			if i != n {
				typ.Types[n] = typ.Types[i]
			}

			newElems.AddItem(elem)
			n++
		}
		typ.Types = typ.Types[:n]

		newRows.AddItem(c.f.ConstructTuple(newElems.BuildList(), c.f.InternType(typ)))
	}

	return c.f.ConstructValues(newRows.BuildList(), c.f.InternColList(newCols))
}

// PruneOrderingGroupBy removes any columns referenced by the Ordering inside
// a GroupByDef which are not output columns of the given group (variant of
// PruneOrdering).
func (c *CustomFuncs) PruneOrderingGroupBy(
	group memo.GroupID, private memo.PrivateID,
) memo.PrivateID {
	outCols := c.OutputCols(group)
	def := c.mem.LookupPrivate(private).(*memo.GroupByDef)
	if def.Ordering.SubsetOfCols(outCols) {
		return private
	}
	defCopy := *def
	defCopy.Ordering = defCopy.Ordering.Copy()
	defCopy.Ordering.ProjectCols(outCols)
	return c.f.InternGroupByDef(&defCopy)
}

// PruneOrderingRowNumber removes any columns referenced by the Ordering inside
// a RowNumberDef which are not output columns of the given group (variant of
// PruneOrdering).
func (c *CustomFuncs) PruneOrderingRowNumber(
	group memo.GroupID, private memo.PrivateID,
) memo.PrivateID {
	outCols := c.OutputCols(group)
	def := c.mem.LookupPrivate(private).(*memo.RowNumberDef)
	if def.Ordering.SubsetOfCols(outCols) {
		return private
	}
	defCopy := *def
	defCopy.Ordering = defCopy.Ordering.Copy()
	defCopy.Ordering.ProjectCols(outCols)
	return c.f.InternRowNumberDef(&defCopy)
}

// DerivePruneCols returns the subset of the given expression's output columns
// that are candidates for pruning. Each operator has its own custom rule for
// what columns it allows to be pruned. Note that if an operator allows columns
// to be pruned, then there must be logic in the PruneCols method to actually
// prune those columns when requested.
func DerivePruneCols(ev memo.ExprView) opt.ColSet {
	// Must be relational operator.
	relational := ev.Logical().Relational
	if relational.IsAvailable(props.PruneCols) {
		return relational.Rule.PruneCols
	}
	relational.SetAvailable(props.PruneCols)

	switch ev.Operator() {
	case opt.ScanOp, opt.ValuesOp:
		// All columns can potentially be pruned from the Scan and Values operators.
		relational.Rule.PruneCols = relational.OutputCols

	case opt.SelectOp:
		// Any pruneable input columns can potentially be pruned, as long as they're
		// not used by the filter.
		inputPruneCols := DerivePruneCols(ev.Child(0))
		relational.Rule.PruneCols = inputPruneCols.Difference(ev.Child(1).Logical().OuterCols())

	case opt.ProjectOp:
		// All columns can potentially be pruned from the Project, if they're never
		// used in a higher-level expression.
		relational.Rule.PruneCols = relational.OutputCols

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		// Any pruneable columns from projected inputs can potentially be pruned, as
		// long as they're not used by the right input (i.e. in Apply case) or by
		// the join filter.
		leftPruneCols := DerivePruneCols(ev.Child(0))
		rightPruneCols := DerivePruneCols(ev.Child(1))

		switch ev.Operator() {
		case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
			relational.Rule.PruneCols = leftPruneCols.Copy()

		default:
			relational.Rule.PruneCols = leftPruneCols.Union(rightPruneCols)
		}
		relational.Rule.PruneCols.DifferenceWith(ev.Child(1).Logical().OuterCols())
		relational.Rule.PruneCols.DifferenceWith(ev.Child(2).Logical().OuterCols())

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		// Grouping columns can't be pruned, because they were used to group rows.
		// However, aggregation columns can potentially be pruned.
		groupingColSet := ev.Private().(*memo.GroupByDef).GroupingCols
		if groupingColSet.Empty() {
			relational.Rule.PruneCols = relational.OutputCols
		} else {
			relational.Rule.PruneCols = relational.OutputCols.Difference(groupingColSet)
		}

	case opt.LimitOp, opt.OffsetOp:
		// Any pruneable input columns can potentially be pruned, as long as
		// they're not used as an ordering column.
		inputPruneCols := DerivePruneCols(ev.Child(0))
		ordering := ev.Private().(*props.OrderingChoice).ColSet()
		relational.Rule.PruneCols = inputPruneCols.Difference(ordering)

	case opt.RowNumberOp:
		// Any pruneable input columns can potentially be pruned, as long as
		// they're not used as an ordering column. The new row number column
		// cannot be pruned without adding an additional Project operator, so
		// don't add it to the set.
		inputPruneCols := DerivePruneCols(ev.Child(0))
		ordering := ev.Private().(*memo.RowNumberDef).Ordering.ColSet()
		relational.Rule.PruneCols = inputPruneCols.Difference(ordering)

	case opt.IndexJoinOp, opt.LookupJoinOp:
		// There is no need to prune columns projected by Index or Lookup joins,
		// since its parent will always be an "alternate" expression in the memo.
		// Any pruneable columns should have already been pruned at the time the
		// IndexJoin is constructed. Additionally, there is not currently a
		// PruneCols rule for these operators.

	default:
		// Don't allow any columns to be pruned, since that would trigger the
		// creation of a wrapper Project around an operator that does not have
		// a pruning rule that will eliminate that Project.
	}

	return relational.Rule.PruneCols
}

// filterColList removes columns not in colWhitelist from a list of groups and
// associated column IDs. Returns the new groups and associated column IDs.
func filterColList(
	groups []memo.GroupID, cols opt.ColList, colWhitelist opt.ColSet,
) ([]memo.GroupID, opt.ColList) {
	var newGroups []memo.GroupID
	var newCols opt.ColList
	for i, col := range cols {
		if colWhitelist.Contains(int(col)) {
			if newGroups == nil {
				newGroups = make([]memo.GroupID, 0, len(cols)-i)
				newCols = make(opt.ColList, 0, len(cols)-i)
			}
			newGroups = append(newGroups, groups[i])
			newCols = append(newCols, col)
		}
	}
	return newGroups, newCols
}
