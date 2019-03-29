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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// NeededGroupingCols returns the columns needed by a grouping operator's
// grouping columns or requested ordering.
func (c *CustomFuncs) NeededGroupingCols(private *memo.GroupingPrivate) opt.ColSet {
	return private.GroupingCols.Union(private.Ordering.ColSet())
}

// NeededRowNumberCols returns the columns needed by a RowNumber operator's
// requested ordering.
func (c *CustomFuncs) NeededRowNumberCols(private *memo.RowNumberPrivate) opt.ColSet {
	return private.Ordering.ColSet()
}

// NeededExplainCols returns the columns needed by Explain's required physical
// properties.
func (c *CustomFuncs) NeededExplainCols(private *memo.ExplainPrivate) opt.ColSet {
	return private.Props.ColSet()
}

// NeededMutationCols returns the columns needed by a mutation operator. Note
// that this function makes no attempt to determine the minimal set of columns
// needed by the mutation private; it simply returns the input columns that are
// referenced by it. Other rules filter the FetchCols, CheckCols, etc. and can
// in turn trigger the PruneMutationInputCols rule.
func (c *CustomFuncs) NeededMutationCols(private *memo.MutationPrivate) opt.ColSet {
	var cols opt.ColSet

	// Add all input columns referenced by the mutation private.
	addCols := func(list opt.ColList) {
		for _, id := range list {
			if id != 0 {
				cols.Add(int(id))
			}
		}
	}

	addCols(private.InsertCols)
	addCols(private.FetchCols)
	addCols(private.UpdateCols)
	addCols(private.CheckCols)
	addCols(private.ReturnCols)
	if private.CanaryCol != 0 {
		cols.Add(int(private.CanaryCol))
	}

	return cols
}

// NeededMutationFetchCols returns the set of FetchCols needed by the given
// mutation operator. FetchCols are existing values that are fetched from the
// store and are then used to construct keys and values for update or delete KV
// operations.
func (c *CustomFuncs) NeededMutationFetchCols(
	op opt.Operator, private *memo.MutationPrivate,
) opt.ColSet {

	var cols opt.ColSet
	tabMeta := c.mem.Metadata().TableMeta(private.Table)

	// familyCols returns the columns in the given family.
	familyCols := func(fam cat.Family) opt.ColSet {
		var colSet opt.ColSet
		for i, n := 0, fam.ColumnCount(); i < n; i++ {
			id := tabMeta.MetaID.ColumnID(fam.Column(i).Ordinal)
			colSet.Add(int(id))
		}
		return colSet
	}

	// addFamilyCols adds all columns in each family containing at least one
	// column that is being updated.
	addFamilyCols := func(updateCols opt.ColSet) {
		for i, n := 0, tabMeta.Table.FamilyCount(); i < n; i++ {
			famCols := familyCols(tabMeta.Table.Family(i))
			if famCols.Intersects(updateCols) {
				cols.UnionWith(famCols)
			}
		}
	}

	// Retain any FetchCols that are needed for ReturnCols. If a RETURN column
	// is needed, then:
	//   1. For Delete, the corresponding FETCH column is always needed, since
	//      it is always returned.
	//   2. For Update, the corresponding FETCH column is needed when there is
	//      no corresponding UPDATE column. In that case, the FETCH column always
	//      becomes the RETURN column.
	//   3. For Upsert, the corresponding FETCH column is needed when there is
	//      no corresponding UPDATE column. In that case, either the INSERT or
	//      FETCH column becomes the RETURN column, so both must be available
	//      for the CASE expression.
	for ord, col := range private.ReturnCols {
		if col != 0 {
			if op == opt.DeleteOp || len(private.UpdateCols) == 0 || private.UpdateCols[ord] == 0 {
				cols.Add(int(tabMeta.MetaID.ColumnID(ord)))
			}
		}
	}

	switch op {
	case opt.UpdateOp, opt.UpsertOp:
		// Determine set of target table columns that need to be updated.
		var updateCols opt.ColSet
		for ord, col := range private.UpdateCols {
			if col != 0 {
				updateCols.Add(int(tabMeta.MetaID.ColumnID(ord)))
			}
		}

		// Make sure to consider indexes that are being added or dropped.
		for i, n := 0, tabMeta.Table.DeletableIndexCount(); i < n; i++ {
			indexCols := tabMeta.IndexColumns(i)
			if !indexCols.Intersects(updateCols) {
				// This index is not being updated.
				continue
			}

			// Always add index strict key columns, since these are needed to fetch
			// existing rows from the store.
			keyCols := tabMeta.IndexKeyColumns(i)
			cols.UnionWith(keyCols)

			// Add all columns in any family that includes an update column.
			// It is possible to update a subset of families only for the primary
			// index, and only when key columns are not being updated. Otherwise,
			// all columns in the index must be fetched.
			// TODO(andyk): It should be possible to not include columns that are
			// being updated, since the existing value is not used. However, this
			// would require execution support.
			if i == cat.PrimaryIndex && !keyCols.Intersects(updateCols) {
				addFamilyCols(updateCols)
			} else {
				cols.UnionWith(indexCols)
			}
		}

	case opt.DeleteOp:
		// Add in all strict key columns from all indexes, since these are needed
		// to compose the keys of rows to delete. Include mutation indexes, since
		// it is necessary to delete rows even from indexes that are being added
		// or dropped.
		for i, n := 0, tabMeta.Table.DeletableIndexCount(); i < n; i++ {
			cols.UnionWith(tabMeta.IndexKeyColumns(i))
		}
	}

	return cols
}

// CanPruneCols returns true if the target expression has extra columns that are
// not needed at this level of the tree, and can be eliminated by one of the
// PruneCols rules. CanPruneCols uses the PruneCols property to determine the
// set of columns which can be pruned, and subtracts the given set of additional
// needed columns from that. See the props.Relational.Rule.PruneCols comment for
// more details.
func (c *CustomFuncs) CanPruneCols(target memo.RelExpr, neededCols opt.ColSet) bool {
	return !DerivePruneCols(target).SubsetOf(neededCols)
}

// CanPruneAggCols returns true if one or more of the target aggregations is not
// referenced and can be eliminated.
func (c *CustomFuncs) CanPruneAggCols(target memo.AggregationsExpr, neededCols opt.ColSet) bool {
	return !target.OutputCols().SubsetOf(neededCols)
}

// CanPruneMutationFetchCols returns true if there are any FetchCols that are
// not in the set of needed columns. Those extra FetchCols can be pruned.
func (c *CustomFuncs) CanPruneMutationFetchCols(
	private *memo.MutationPrivate, neededCols opt.ColSet,
) bool {
	tabMeta := c.mem.Metadata().TableMeta(private.Table)
	for ord, col := range private.FetchCols {
		if col != 0 && !neededCols.Contains(int(tabMeta.MetaID.ColumnID(ord))) {
			return true
		}
	}
	return false
}

// PruneCols creates an expression that discards any outputs columns of the
// target expression that are not used. If the target expression type supports
// column filtering (like Scan, Values, Projections, etc.), then create a new
// instance of that operator that does the filtering. Otherwise, construct a
// Project operator that wraps the operator and does the filtering. The new
// Project operator will be pushed down the tree until it merges with another
// operator that supports column filtering.
func (c *CustomFuncs) PruneCols(target memo.RelExpr, neededCols opt.ColSet) memo.RelExpr {
	switch t := target.(type) {
	case *memo.ScanExpr:
		return c.pruneScanCols(t, neededCols)

	case *memo.ValuesExpr:
		return c.pruneValuesCols(t, neededCols)

	case *memo.ProjectExpr:
		passthrough := t.Passthrough.Intersection(neededCols)
		projections := make(memo.ProjectionsExpr, 0, len(t.Projections))
		for i := range t.Projections {
			item := &t.Projections[i]
			if neededCols.Contains(int(item.Col)) {
				projections = append(projections, *item)
			}
		}
		return c.f.ConstructProject(t.Input, projections, passthrough)

	default:
		// In other cases, we wrap the input in a Project operator.

		// Get the subset of the target expression's output columns that should
		// not be pruned. Don't prune if the target output column is needed by a
		// higher-level expression, or if it's not part of the PruneCols set.
		pruneCols := DerivePruneCols(target).Difference(neededCols)
		colSet := c.OutputCols(target).Difference(pruneCols)
		return c.f.ConstructProject(target, memo.EmptyProjectionsExpr, colSet)
	}
}

// PruneAggCols creates a new AggregationsExpr that discards columns that are
// not referenced by the neededCols set.
func (c *CustomFuncs) PruneAggCols(
	target memo.AggregationsExpr, neededCols opt.ColSet,
) memo.AggregationsExpr {
	aggs := make(memo.AggregationsExpr, 0, len(target))
	for i := range target {
		item := &target[i]
		if neededCols.Contains(int(item.Col)) {
			aggs = append(aggs, *item)
		}
	}
	return aggs
}

// PruneMutationFetchCols rewrites the given mutation private to no longer
// reference FetchCols that are not part of the neededCols set. The caller must
// have already done the analysis to prove that these columns are not needed, by
// calling CanPruneMutationFetchCols.
func (c *CustomFuncs) PruneMutationFetchCols(
	private *memo.MutationPrivate, neededCols opt.ColSet,
) *memo.MutationPrivate {
	tabID := c.mem.Metadata().TableMeta(private.Table).MetaID
	newPrivate := *private
	newPrivate.FetchCols = c.filterMutationList(tabID, newPrivate.FetchCols, neededCols)
	return &newPrivate
}

// filterMutationList filters the given mutation list by setting any columns
// that are not in the neededCols set to zero. This indicates that those input
// columns are not needed by this mutation list.
func (c *CustomFuncs) filterMutationList(
	tabID opt.TableID, inList opt.ColList, neededCols opt.ColSet,
) opt.ColList {
	newList := make(opt.ColList, len(inList))
	for i, c := range inList {
		if !neededCols.Contains(int(tabID.ColumnID(i))) {
			newList[i] = 0
		} else {
			newList[i] = c
		}
	}
	return newList
}

// pruneScanCols constructs a new Scan operator based on the given existing Scan
// operator, but projecting only the needed columns.
func (c *CustomFuncs) pruneScanCols(scan *memo.ScanExpr, neededCols opt.ColSet) memo.RelExpr {
	// Make copy of scan private and update columns.
	new := scan.ScanPrivate
	new.Cols = c.OutputCols(scan).Intersection(neededCols)
	return c.f.ConstructScan(&new)
}

// pruneValuesCols constructs a new Values operator based on the given existing
// Values operator. The new operator will have the same set of rows, but
// containing only the needed columns. Other columns are discarded.
func (c *CustomFuncs) pruneValuesCols(values *memo.ValuesExpr, neededCols opt.ColSet) memo.RelExpr {
	// Create new list of columns that only contains needed columns.
	newCols := make(opt.ColList, 0, neededCols.Len())
	for _, colID := range values.Cols {
		if !neededCols.Contains(int(colID)) {
			continue
		}
		newCols = append(newCols, colID)
	}

	newRows := make(memo.ScalarListExpr, len(values.Rows))
	for irow, row := range values.Rows {
		tuple := row.(*memo.TupleExpr)
		typ := tuple.DataType().(types.TTuple)

		newElems := make(memo.ScalarListExpr, len(newCols))
		nelem := 0
		for ielem, elem := range tuple.Elems {
			if !neededCols.Contains(int(values.Cols[ielem])) {
				continue
			}
			if ielem != nelem {
				typ.Types[nelem] = typ.Types[ielem]
			}

			newElems[nelem] = elem
			nelem++
		}
		typ.Types = typ.Types[:nelem]

		newRows[irow] = c.f.ConstructTuple(newElems, typ)
	}

	return c.f.ConstructValues(newRows, &memo.ValuesPrivate{
		Cols: newCols,
		ID:   values.ID,
	})
}

// PruneOrderingGroupBy removes any columns referenced by the Ordering inside
// a GroupingPrivate which are not part of the neededCols set.
func (c *CustomFuncs) PruneOrderingGroupBy(
	private *memo.GroupingPrivate, neededCols opt.ColSet,
) *memo.GroupingPrivate {
	if private.Ordering.SubsetOfCols(neededCols) {
		return private
	}

	// Make copy of grouping private and update columns.
	new := *private
	new.Ordering = new.Ordering.Copy()
	new.Ordering.ProjectCols(neededCols)
	return &new
}

// PruneOrderingRowNumber removes any columns referenced by the Ordering inside
// a RowNumberPrivate which are not part of the neededCols set.
func (c *CustomFuncs) PruneOrderingRowNumber(
	private *memo.RowNumberPrivate, neededCols opt.ColSet,
) *memo.RowNumberPrivate {
	if private.Ordering.SubsetOfCols(neededCols) {
		return private
	}

	// Make copy of row number private and update columns.
	new := *private
	new.Ordering = new.Ordering.Copy()
	new.Ordering.ProjectCols(neededCols)
	return &new
}

// DerivePruneCols returns the subset of the given expression's output columns
// that are candidates for pruning. Each operator has its own custom rule for
// what columns it allows to be pruned. Note that if an operator allows columns
// to be pruned, then there must be logic in the PruneCols method to actually
// prune those columns when requested.
func DerivePruneCols(e memo.RelExpr) opt.ColSet {
	relProps := e.Relational()
	if relProps.IsAvailable(props.PruneCols) {
		return relProps.Rule.PruneCols
	}
	relProps.SetAvailable(props.PruneCols)

	switch e.Op() {
	case opt.ScanOp, opt.ValuesOp:
		// All columns can potentially be pruned from the Scan and Values operators.
		relProps.Rule.PruneCols = relProps.OutputCols

	case opt.SelectOp:
		// Any pruneable input columns can potentially be pruned, as long as they're
		// not used by the filter.
		sel := e.(*memo.SelectExpr)
		relProps.Rule.PruneCols = DerivePruneCols(sel.Input).Copy()
		usedCols := sel.Filters.OuterCols(e.Memo())
		relProps.Rule.PruneCols.DifferenceWith(usedCols)

	case opt.ProjectOp:
		// All columns can potentially be pruned from the Project, if they're never
		// used in a higher-level expression.
		relProps.Rule.PruneCols = relProps.OutputCols

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		// Any pruneable columns from projected inputs can potentially be pruned, as
		// long as they're not used by the right input (i.e. in Apply case) or by
		// the join filter.
		left := e.Child(0).(memo.RelExpr)
		leftPruneCols := DerivePruneCols(left)
		right := e.Child(1).(memo.RelExpr)
		rightPruneCols := DerivePruneCols(right)

		switch e.Op() {
		case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
			relProps.Rule.PruneCols = leftPruneCols.Copy()

		default:
			relProps.Rule.PruneCols = leftPruneCols.Union(rightPruneCols)
		}
		relProps.Rule.PruneCols.DifferenceWith(right.Relational().OuterCols)
		onCols := e.Child(2).(*memo.FiltersExpr).OuterCols(e.Memo())
		relProps.Rule.PruneCols.DifferenceWith(onCols)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		// Grouping columns can't be pruned, because they were used to group rows.
		// However, aggregation columns can potentially be pruned.
		groupingColSet := e.Private().(*memo.GroupingPrivate).GroupingCols
		if groupingColSet.Empty() {
			relProps.Rule.PruneCols = relProps.OutputCols
		} else {
			relProps.Rule.PruneCols = relProps.OutputCols.Difference(groupingColSet)
		}

	case opt.LimitOp, opt.OffsetOp:
		// Any pruneable input columns can potentially be pruned, as long as
		// they're not used as an ordering column.
		inputPruneCols := DerivePruneCols(e.Child(0).(memo.RelExpr))
		ordering := e.Private().(*physical.OrderingChoice).ColSet()
		relProps.Rule.PruneCols = inputPruneCols.Difference(ordering)

	case opt.RowNumberOp:
		// Any pruneable input columns can potentially be pruned, as long as
		// they're not used as an ordering column. The new row number column
		// cannot be pruned without adding an additional Project operator, so
		// don't add it to the set.
		rowNum := e.(*memo.RowNumberExpr)
		inputPruneCols := DerivePruneCols(rowNum.Input)
		relProps.Rule.PruneCols = inputPruneCols.Difference(rowNum.Ordering.ColSet())

	case opt.IndexJoinOp, opt.LookupJoinOp, opt.MergeJoinOp:
		// There is no need to prune columns projected by Index, Lookup or Merge
		// joins, since its parent will always be an "alternate" expression in the
		// memo. Any pruneable columns should have already been pruned at the time
		// one of these operators is constructed. Additionally, there is not
		// currently a PruneCols rule for these operators.

	case opt.ProjectSetOp:
		// Any pruneable input columns can potentially be pruned, as long as
		// they're not used in the Zip.
		// TODO(rytaft): It may be possible to prune Zip columns, but we need to
		// make sure that we still get the correct number of rows in the output.
		projectSet := e.(*memo.ProjectSetExpr)
		relProps.Rule.PruneCols = DerivePruneCols(projectSet.Input).Copy()
		usedCols := projectSet.Zip.OuterCols(e.Memo())
		relProps.Rule.PruneCols.DifferenceWith(usedCols)

	default:
		// Don't allow any columns to be pruned, since that would trigger the
		// creation of a wrapper Project around an operator that does not have
		// a pruning rule that will eliminate that Project.
	}

	return relProps.Rule.PruneCols
}
