// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NeededGroupingCols returns the columns needed by a grouping operator's
// grouping columns or requested ordering.
func (c *CustomFuncs) NeededGroupingCols(private *memo.GroupingPrivate) opt.ColSet {
	return private.GroupingCols.Union(private.Ordering.ColSet())
}

// NeededOrdinalityCols returns the columns needed by a Ordinality operator's
// requested ordering.
func (c *CustomFuncs) NeededOrdinalityCols(private *memo.OrdinalityPrivate) opt.ColSet {
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
func (c *CustomFuncs) NeededMutationCols(
	private *memo.MutationPrivate, uniqueChecks memo.UniqueChecksExpr, fkChecks memo.FKChecksExpr,
) opt.ColSet {
	return c.neededMutationCols(private, uniqueChecks, fkChecks, true /* includePartialIndexCols */)
}

func (c *CustomFuncs) neededMutationCols(
	private *memo.MutationPrivate,
	uniqueChecks memo.UniqueChecksExpr,
	fkChecks memo.FKChecksExpr,
	includePartialIndexCols bool,
) opt.ColSet {
	var cols opt.ColSet

	// Add all input columns referenced by the mutation private.
	addCols := func(list opt.OptionalColList) {
		for _, id := range list {
			if id != 0 {
				cols.Add(id)
			}
		}
	}

	addCols(private.InsertCols)
	addCols(private.FetchCols)
	addCols(private.UpdateCols)
	addCols(private.CheckCols)
	if includePartialIndexCols {
		addCols(private.PartialIndexPutCols)
		addCols(private.PartialIndexDelCols)
	}
	addCols(private.ReturnCols)
	addCols(opt.OptionalColList(private.PassthroughCols))
	if private.CanaryCol != 0 {
		cols.Add(private.CanaryCol)
	}

	if private.WithID != 0 {
		for i := range uniqueChecks {
			withUses := memo.WithUses(uniqueChecks[i].Check)
			cols.UnionWith(withUses[private.WithID].UsedCols)
		}
		for i := range fkChecks {
			withUses := memo.WithUses(fkChecks[i].Check)
			cols.UnionWith(withUses[private.WithID].UsedCols)
		}
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
	tabMeta := c.mem.Metadata().TableMeta(private.Table)

	// familyCols returns the columns in the given family.
	familyCols := func(fam cat.Family) opt.ColSet {
		var colSet opt.ColSet
		for i, n := 0, fam.ColumnCount(); i < n; i++ {
			id := tabMeta.MetaID.ColumnID(fam.Column(i).Ordinal)
			colSet.Add(id)
		}
		return colSet
	}

	// cols accumulates the result. The column IDs are relative to tabMeta.
	// TODO(radu): this should be a set of ordinals instead.
	var cols opt.ColSet

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
				cols.Add(tabMeta.MetaID.ColumnID(ord))
			}
		}
	}

	switch op {
	case opt.UpdateOp, opt.UpsertOp:
		// Determine set of target table columns that need to be updated.
		var updateCols opt.ColSet
		for ord, col := range private.UpdateCols {
			if col != 0 {
				updateCols.Add(tabMeta.MetaID.ColumnID(ord))
			}
		}

		// TODO(radu): The execution code requires that each update column has a
		// corresponding fetch column (even if the old value is not necessary).
		// Note that when this limitation is fixed, the rest of the code below
		// needs to be revisited as well.
		cols.UnionWith(updateCols)

		// Make sure to consider indexes that are being added or dropped.
		for i, n := 0, tabMeta.Table.DeletableIndexCount(); i < n; i++ {
			// If the columns being updated are not part of the index, then the
			// update does not require changes to the index. Partial indexes may
			// be updated (even when a column in the index is not changing) when
			// the predicate references columns that are being updated. For
			// example, rows that were not previously in the index must be added
			// to the index because they now satisfy the partial index
			// predicate, requiring the index columns to be fetched.
			//
			// Note that we use the set of index columns where the virtual
			// columns have been mapped to their source columns. Virtual columns
			// are never part of the updated columns. Updates to source columns
			// trigger index changes.
			indexCols := tabMeta.IndexColumnsMapVirtual(i)
			pred, isPartialIndex := tabMeta.PartialIndexPredicate(i)
			indexAndPredCols := indexCols.Copy()
			if isPartialIndex {
				predFilters := *pred.(*memo.FiltersExpr)
				indexAndPredCols.UnionWith(predFilters.OuterCols())
			}
			if !indexAndPredCols.Intersects(updateCols) {
				continue
			}

			// Always add index strict key columns, since these are needed to fetch
			// existing rows from the store.
			keyCols := tabMeta.IndexKeyColumnsMapVirtual(i)
			cols.UnionWith(keyCols)

			// Add all columns in any family that includes an update column.
			// It is possible to update a subset of families only for the primary
			// index, and only when key columns are not being updated. Otherwise,
			// all columns in the index must be fetched.
			if i == cat.PrimaryIndex && !keyCols.Intersects(updateCols) {
				addFamilyCols(updateCols)
			} else {
				// Add all of the index columns into cols.
				indexCols.ForEach(func(col opt.ColumnID) {
					ord := tabMeta.MetaID.ColumnOrdinal(col)
					// We don't want to include system columns.
					if tabMeta.Table.Column(ord).Kind() != cat.System {
						cols.Add(col)
					}
				})
			}
		}

		// Add inbound foreign keys that may require a check or cascade.
		for i, n := 0, tabMeta.Table.InboundForeignKeyCount(); i < n; i++ {
			inboundFK := tabMeta.Table.InboundForeignKey(i)
			var fkCols opt.ColSet
			for j, m := 0, inboundFK.ColumnCount(); j < m; j++ {
				ord := inboundFK.ReferencedColumnOrdinal(tabMeta.Table, j)
				fkCols.Add(tabMeta.MetaID.ColumnID(ord))
			}
			if fkCols.Intersects(updateCols) {
				cols.UnionWith(fkCols)
			}
		}

	case opt.DeleteOp:
		// Add in all strict key columns from all indexes, since these are needed
		// to compose the keys of rows to delete. Include mutation indexes, since
		// it is necessary to delete rows even from indexes that are being added
		// or dropped.
		for i, n := 0, tabMeta.Table.DeletableIndexCount(); i < n; i++ {
			cols.UnionWith(tabMeta.IndexKeyColumnsMapVirtual(i))
		}

		// Add inbound foreign keys that may require a check or cascade.
		for i, n := 0, tabMeta.Table.InboundForeignKeyCount(); i < n; i++ {
			inboundFK := tabMeta.Table.InboundForeignKey(i)
			for j, m := 0, inboundFK.ColumnCount(); j < m; j++ {
				ord := inboundFK.ReferencedColumnOrdinal(tabMeta.Table, j)
				cols.Add(tabMeta.MetaID.ColumnID(ord))
			}
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
		if col != 0 && !neededCols.Contains(tabMeta.MetaID.ColumnID(ord)) {
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

	case *memo.WithScanExpr:
		return c.pruneWithScanCols(t, neededCols)

	case *memo.ProjectExpr:
		passthrough := t.Passthrough.Intersection(neededCols)
		projections := make(memo.ProjectionsExpr, 0, len(t.Projections))
		for i := range t.Projections {
			item := &t.Projections[i]
			if neededCols.Contains(item.Col) {
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
		if neededCols.Contains(item.Col) {
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
	tabID opt.TableID, inList opt.OptionalColList, neededCols opt.ColSet,
) opt.OptionalColList {
	newList := make(opt.OptionalColList, len(inList))
	for i, c := range inList {
		if c != 0 && neededCols.Contains(tabID.ColumnID(i)) {
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

// pruneWithScanCols constructs a new WithScan operator based on the given
// existing WithScan operator, but projecting only the needed columns.
func (c *CustomFuncs) pruneWithScanCols(
	scan *memo.WithScanExpr, neededCols opt.ColSet,
) memo.RelExpr {
	// Make copy of scan private and update columns.
	new := scan.WithScanPrivate

	new.InCols = make(opt.ColList, 0, neededCols.Len())
	new.OutCols = make(opt.ColList, 0, neededCols.Len())
	for i := range scan.WithScanPrivate.OutCols {
		if neededCols.Contains(scan.WithScanPrivate.OutCols[i]) {
			new.InCols = append(new.InCols, scan.WithScanPrivate.InCols[i])
			new.OutCols = append(new.OutCols, scan.WithScanPrivate.OutCols[i])
		}
	}

	return c.f.ConstructWithScan(&new)
}

// pruneValuesCols constructs a new Values operator based on the given existing
// Values operator. The new operator will have the same set of rows, but
// containing only the needed columns. Other columns are discarded.
func (c *CustomFuncs) pruneValuesCols(values *memo.ValuesExpr, neededCols opt.ColSet) memo.RelExpr {
	// Create new list of columns that only contains needed columns.
	newCols := make(opt.ColList, 0, neededCols.Len())
	for _, colID := range values.Cols {
		if !neededCols.Contains(colID) {
			continue
		}
		newCols = append(newCols, colID)
	}

	newRows := make(memo.ScalarListExpr, len(values.Rows))
	for irow, row := range values.Rows {
		tuple := row.(*memo.TupleExpr)
		typ := tuple.DataType()

		newContents := make([]*types.T, len(newCols))
		newElems := make(memo.ScalarListExpr, len(newCols))
		nelem := 0
		for ielem, elem := range tuple.Elems {
			if !neededCols.Contains(values.Cols[ielem]) {
				continue
			}
			newContents[nelem] = typ.TupleContents()[ielem]
			newElems[nelem] = elem
			nelem++
		}

		newRows[irow] = c.f.ConstructTuple(newElems, types.MakeTuple(newContents))
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

// PruneOrderingOrdinality removes any columns referenced by the Ordering inside
// a OrdinalityPrivate which are not part of the neededCols set.
func (c *CustomFuncs) PruneOrderingOrdinality(
	private *memo.OrdinalityPrivate, neededCols opt.ColSet,
) *memo.OrdinalityPrivate {
	if private.Ordering.SubsetOfCols(neededCols) {
		return private
	}

	// Make copy of row number private and update columns.
	new := *private
	new.Ordering = new.Ordering.Copy()
	new.Ordering.ProjectCols(neededCols)
	return &new
}

// NeededWindowCols is the set of columns that the window function needs to
// execute.
func (c *CustomFuncs) NeededWindowCols(windows memo.WindowsExpr, p *memo.WindowPrivate) opt.ColSet {
	var needed opt.ColSet
	needed.UnionWith(p.Partition)
	needed.UnionWith(p.Ordering.ColSet())
	for i := range windows {
		needed.UnionWith(windows[i].ScalarProps().OuterCols)
	}
	return needed
}

// CanPruneWindows is true if the list of window functions contains a column
// which is not included in needed, meaning that it can be pruned.
func (c *CustomFuncs) CanPruneWindows(needed opt.ColSet, windows memo.WindowsExpr) bool {
	for _, w := range windows {
		if !needed.Contains(w.Col) {
			return true
		}
	}
	return false
}

// PruneWindows restricts windows to only the columns which appear in needed.
// If we eliminate all the window functions, EliminateWindow will trigger and
// remove the expression entirely.
func (c *CustomFuncs) PruneWindows(needed opt.ColSet, windows memo.WindowsExpr) memo.WindowsExpr {
	result := make(memo.WindowsExpr, 0, len(windows))
	for _, w := range windows {
		if needed.Contains(w.Col) {
			result = append(result, w)
		}
	}
	return result
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
	case opt.ScanOp, opt.ValuesOp, opt.WithScanOp:
		// All columns can potentially be pruned from the Scan, Values, and WithScan
		// operators.
		relProps.Rule.PruneCols = relProps.OutputCols.Copy()

	case opt.SelectOp:
		// Any pruneable input columns can potentially be pruned, as long as they're
		// not used by the filter.
		sel := e.(*memo.SelectExpr)
		relProps.Rule.PruneCols = DerivePruneCols(sel.Input).Copy()
		usedCols := sel.Filters.OuterCols()
		relProps.Rule.PruneCols.DifferenceWith(usedCols)

	case opt.ProjectOp:
		// All columns can potentially be pruned from the Project, if they're never
		// used in a higher-level expression.
		relProps.Rule.PruneCols = relProps.OutputCols.Copy()

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
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
		onCols := e.Child(2).(*memo.FiltersExpr).OuterCols()
		relProps.Rule.PruneCols.DifferenceWith(onCols)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp, opt.EnsureDistinctOnOp:
		// Grouping columns can't be pruned, because they were used to group rows.
		// However, aggregation columns can potentially be pruned.
		groupingColSet := e.Private().(*memo.GroupingPrivate).GroupingCols
		if groupingColSet.Empty() {
			relProps.Rule.PruneCols = relProps.OutputCols.Copy()
		} else {
			relProps.Rule.PruneCols = relProps.OutputCols.Difference(groupingColSet)
		}

	case opt.LimitOp, opt.OffsetOp:
		// Any pruneable input columns can potentially be pruned, as long as
		// they're not used as an ordering column.
		inputPruneCols := DerivePruneCols(e.Child(0).(memo.RelExpr))
		ordering := e.Private().(*props.OrderingChoice).ColSet()
		relProps.Rule.PruneCols = inputPruneCols.Difference(ordering)

	case opt.OrdinalityOp:
		// Any pruneable input columns can potentially be pruned, as long as
		// they're not used as an ordering column. The new row number column
		// cannot be pruned without adding an additional Project operator, so
		// don't add it to the set.
		ord := e.(*memo.OrdinalityExpr)
		inputPruneCols := DerivePruneCols(ord.Input)
		relProps.Rule.PruneCols = inputPruneCols.Difference(ord.Ordering.ColSet())

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
		usedCols := projectSet.Zip.OuterCols()
		relProps.Rule.PruneCols.DifferenceWith(usedCols)

	case opt.UnionAllOp:
		// Pruning can be beneficial as long as one of our inputs has advertised pruning,
		// so that we can push down the project and eliminate the advertisement.
		u := e.(*memo.UnionAllExpr)
		pruneFromLeft := opt.TranslateColSet(DerivePruneCols(u.Left), u.LeftCols, u.OutCols)
		pruneFromRight := opt.TranslateColSet(DerivePruneCols(u.Right), u.RightCols, u.OutCols)
		relProps.Rule.PruneCols = pruneFromLeft.Union(pruneFromRight)

	case opt.WindowOp:
		win := e.(*memo.WindowExpr)
		relProps.Rule.PruneCols = DerivePruneCols(win.Input).Copy()
		relProps.Rule.PruneCols.DifferenceWith(win.Partition)
		relProps.Rule.PruneCols.DifferenceWith(win.Ordering.ColSet())
		for _, w := range win.Windows {
			relProps.Rule.PruneCols.Add(w.Col)
			relProps.Rule.PruneCols.DifferenceWith(w.ScalarProps().OuterCols)
		}

	case opt.WithOp:
		// WithOp passes through its input unchanged, so it has the same pruning
		// characteristics as its input.
		relProps.Rule.PruneCols = DerivePruneCols(e.(*memo.WithExpr).Main)

	default:
		// Don't allow any columns to be pruned, since that would trigger the
		// creation of a wrapper Project around an operator that does not have
		// a pruning rule that will eliminate that Project.
	}

	return relProps.Rule.PruneCols
}

// CanPruneMutationReturnCols checks whether the mutation's return columns can
// be pruned. This is the pre-condition for the PruneMutationReturnCols rule.
func (c *CustomFuncs) CanPruneMutationReturnCols(
	private *memo.MutationPrivate, needed opt.ColSet,
) bool {
	if private.ReturnCols == nil {
		return false
	}

	tabID := c.mem.Metadata().TableMeta(private.Table).MetaID
	for i := range private.ReturnCols {
		if private.ReturnCols[i] != 0 && !needed.Contains(tabID.ColumnID(i)) {
			return true
		}
	}

	for _, passthroughCol := range private.PassthroughCols {
		if passthroughCol != 0 && !needed.Contains(passthroughCol) {
			return true
		}
	}

	return false
}

// PruneMutationReturnCols rewrites the given mutation private to no longer
// keep ReturnCols that are not referenced by the RETURNING clause or are not
// part of the primary key. The caller must have already done the analysis to
// prove that such columns exist, by calling CanPruneMutationReturnCols.
func (c *CustomFuncs) PruneMutationReturnCols(
	private *memo.MutationPrivate, needed opt.ColSet,
) *memo.MutationPrivate {
	newPrivate := *private
	tabID := c.mem.Metadata().TableMeta(private.Table).MetaID

	// Prune away the ReturnCols that are unused.
	newPrivate.ReturnCols = c.filterMutationList(tabID, private.ReturnCols, needed)

	newPassthroughCols := make(opt.ColList, 0, len(private.PassthroughCols))
	// Prune away the PassthroughCols that are unused.
	for _, passthroughCol := range private.PassthroughCols {
		if passthroughCol != 0 && needed.Contains(passthroughCol) {
			newPassthroughCols = append(newPassthroughCols, passthroughCol)
		}
	}

	newPrivate.PassthroughCols = newPassthroughCols
	return &newPrivate
}

// MutationTable returns the table upon which the mutation is applied.
// CHECK
func (c *CustomFuncs) MutationTable(private *memo.MutationPrivate) opt.TableID {
	return private.Table
}

// NeededColMapLeft returns the subset of a SetPrivate's LeftCols that corresponds to the
// needed subset of OutCols. This is useful for pruning columns in set operations.
func (c *CustomFuncs) NeededColMapLeft(needed opt.ColSet, set *memo.SetPrivate) opt.ColSet {
	return opt.TranslateColSetStrict(needed, set.OutCols, set.LeftCols)
}

// NeededColMapRight returns the subset of a SetPrivate's RightCols that corresponds to the
// needed subset of OutCols. This is useful for pruning columns in set operations.
func (c *CustomFuncs) NeededColMapRight(needed opt.ColSet, set *memo.SetPrivate) opt.ColSet {
	return opt.TranslateColSetStrict(needed, set.OutCols, set.RightCols)
}
