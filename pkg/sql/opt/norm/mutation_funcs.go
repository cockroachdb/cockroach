// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

	// Partial index PUT and DEL columns that are used for multiple partial
	// indexes cannot be simplified to false. This may occur when multiple
	// partial indexes have the same predicate expression. If one index does not
	// have mutating columns, simplifying its PUT or DEL columns to false would
	// incorrectly prevent writes to other indexes that have mutating columns.
	//
	// For example, consider:
	//
	//   CREATE TABLE t (
	//     a INT,
	//     b INT,
	//     c INT,
	//     INDEX a_idx (a) WHERE c IS NULL,
	//     INDEX b_idx (b) WHERE c IS NULL
	//   )
	//
	//   UPDATE t SET a = NULL
	//
	// In the UPDATE, a single column is synthesized for the PUT and DEL columns
	// of both partial indexes. Even though the UPDATE does not mutate columns
	// in b_idx, the synthesized PUT/DEL column cannot be simplified to false.
	// If it were set to false, writes to a_idx would never occur.
	ineligibleCols.UnionWith(multiUsePartialIndexCols(private))

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
		indexAndPredCols := tabMeta.IndexColumnsMapInverted(i)
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

// multiUsePartialIndexCols returns the set of columns that are used as PUT or
// DEL columns for more than one partial index. This may occur when multiple
// partial indexes share the same predicate expression. Columns used as a PUT
// and DEL column for the same index and no other indexes are not included in
// the output.
func multiUsePartialIndexCols(mp *memo.MutationPrivate) opt.ColSet {
	var cols opt.ColSet

	for i := range mp.PartialIndexPutCols {
		putCol := mp.PartialIndexPutCols[i]
		delCol := mp.PartialIndexDelCols[i]
		putColUsedAgain := false
		delColUsedAgain := false

		for j := 0; j < i; j++ {
			// A PUT column used as a PUT or DEL column for another index should
			// be included in cols.
			if putCol == mp.PartialIndexPutCols[j] || putCol == mp.PartialIndexDelCols[j] {
				putColUsedAgain = true
			}

			// A DEL column used as a PUT or DEL column for another index should
			// be included in cols.
			if delCol == mp.PartialIndexPutCols[j] || delCol == mp.PartialIndexDelCols[j] {
				delColUsedAgain = true
			}

			if putColUsedAgain && delColUsedAgain {
				break
			}
		}

		if putColUsedAgain {
			cols.Add(putCol)
		}
		if delColUsedAgain {
			cols.Add(delCol)
		}
	}

	return cols
}

// SimplifyPartialIndexProjections returns a new projection expression with any
// projected column's expression simplified to false if the column exists in
// simplifiableCols.
func (c *CustomFuncs) SimplifyPartialIndexProjections(
	projections memo.ProjectionsExpr,
	passthrough opt.ColSet,
	simplifiableCols opt.ColSet,
	private *memo.MutationPrivate,
) (memo.ProjectionsExpr, *memo.MutationPrivate) {
	simplified := make(memo.ProjectionsExpr, len(projections))
	for i := range projections {
		if col := projections[i].Col; simplifiableCols.Contains(col) {
			simplified[i] = c.f.ConstructProjectionsItem(memo.FalseSingleton, col)
		} else {
			simplified[i] = projections[i]
		}
	}

	// Any simplifiable partial index expressions that are currently passthrough
	// columns must be changed to projected false expressions.
	simplifiableCols.IntersectionWith(passthrough)
	if simplifiableCols.Empty() {
		return simplified, private
	}

	// Copy the MutationPrivate in order to change the partial index columns.
	simplifiedPrivate := *private
	simplifiedPrivate.PartialIndexPutCols = append(opt.OptionalColList{}, private.PartialIndexPutCols...)
	simplifiedPrivate.PartialIndexDelCols = append(opt.OptionalColList{}, private.PartialIndexDelCols...)

	ord := len(simplifiedPrivate.PartialIndexPutCols)
	for i, col := range simplifiedPrivate.PartialIndexPutCols {
		if simplifiableCols.Contains(col) {
			name := fmt.Sprintf("partial_index_put%d", ord+1)
			newPutCol := c.f.Metadata().AddColumn(name, types.Bool)
			simplified = append(simplified, c.f.ConstructProjectionsItem(memo.FalseSingleton, newPutCol))
			simplifiedPrivate.PartialIndexPutCols[i] = newPutCol
			ord++
		}
	}

	ord = len(simplifiedPrivate.PartialIndexDelCols)
	for i, col := range simplifiedPrivate.PartialIndexDelCols {
		if simplifiableCols.Contains(col) {
			name := fmt.Sprintf("partial_index_del%d", ord+1)
			newDelCol := c.f.Metadata().AddColumn(name, types.Bool)
			simplified = append(simplified, c.f.ConstructProjectionsItem(memo.FalseSingleton, newDelCol))
			simplifiedPrivate.PartialIndexDelCols[i] = newDelCol
			ord++
		}
	}

	return simplified, &simplifiedPrivate
}
