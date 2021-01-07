// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// SimplifiablePartialIndexProjectCols returns the set of projected partial
// index PUT and DEL columns with expressions that can be simplified to false.
// These projected expressions can only be simplified to false when an UPDATE
// mutates neither the associated index's columns nor the columns referenced in
// the partial index predicate.
func (c *CustomFuncs) SimplifiablePartialIndexProjectCols(
	private *memo.MutationPrivate,
	uniqueChecks memo.UniqueChecksExpr,
	fkChecks memo.FKChecksExpr,
	projections memo.ProjectionsExpr,
) opt.ColSet {
	tabMeta := c.mem.Metadata().TableMeta(private.Table)

	// Determine the set of target table columns that need to be updated. Notice
	// that we collect the target table column IDs, not the update column IDs.
	var updateCols opt.ColSet
	for ord, col := range private.UpdateCols {
		if col != 0 {
			updateCols.Add(tabMeta.MetaID.ColumnID(ord))
		}
	}

	// Determine the set of columns needed for the mutation operator, excluding
	// the partial index PUT and DEL columns.
	neededMutationCols := c.neededMutationCols(private, uniqueChecks, fkChecks, false /* includePartialIndexCols */)

	// Determine the set of project columns that are already simplified to
	// false.
	var simplifiedProjectCols opt.ColSet
	for i := range projections {
		project := &projections[i]
		if project.Element == memo.FalseSingleton {
			simplifiedProjectCols.Add(project.Col)
		}
	}

	// Columns that are required by the mutation operator and columns that
	// have already been simplified to false are ineligible to be simplified.
	ineligibleCols := neededMutationCols.Union(simplifiedProjectCols)

	// ord is an ordinal into the mutation's PartialIndexPutCols and
	// PartialIndexDelCols, which both have entries for each partial index
	// defined on the table.
	ord := -1
	var cols opt.ColSet
	for i, n := 0, tabMeta.Table.DeletableIndexCount(); i < n; i++ {
		pred, isPartialIndex := tabMeta.PartialIndexPredicate(i)

		// Skip non-partial indexes.
		if !isPartialIndex {
			continue
		}
		ord++

		// If the columns being updated are part of the index or referenced in
		// the partial index predicate, then updates to the index may be
		// required. Therefore, the partial index PUT and DEL columns cannot be
		// simplified.
		//
		// Note that we use the set of index columns where the virtual
		// columns have been mapped to their source columns. Virtual columns
		// are never part of the updated columns. Updates to source columns
		// trigger index changes.
		predFilters := *pred.(*memo.FiltersExpr)
		indexAndPredCols := tabMeta.IndexColumnsMapVirtual(i)
		indexAndPredCols.UnionWith(predFilters.OuterCols())
		if indexAndPredCols.Intersects(updateCols) {
			continue
		}

		// Add the projected PUT column if it is eligible to be simplified.
		putCol := private.PartialIndexPutCols[ord]
		if !ineligibleCols.Contains(putCol) {
			cols.Add(putCol)
		}

		// Add the projected DEL column if it is eligible to be simplified.
		delCol := private.PartialIndexDelCols[ord]
		if !ineligibleCols.Contains(delCol) {
			cols.Add(delCol)
		}
	}

	return cols
}

// SimplifyPartialIndexProjections returns a new projection expression with any
// projected column's expression simplified to false if the column exists in
// simplifiableCols.
func (c *CustomFuncs) SimplifyPartialIndexProjections(
	projections memo.ProjectionsExpr, simplifiableCols opt.ColSet,
) memo.ProjectionsExpr {
	simplified := make(memo.ProjectionsExpr, len(projections))
	for i := range projections {
		if col := projections[i].Col; simplifiableCols.Contains(col) {
			simplified[i] = c.f.ConstructProjectionsItem(memo.FalseSingleton, col)
		} else {
			simplified[i] = projections[i]
		}
	}
	return simplified
}
