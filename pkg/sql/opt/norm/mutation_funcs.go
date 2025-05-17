// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
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

func (c *CustomFuncs) CanUseSwapMutation(
	private *memo.MutationPrivate, filters memo.FiltersExpr, scan *memo.ScanPrivate,
) bool {
	// Update swap needs to avoid throwing an error if checks fail but the row
	// doesn't exist. This is not currently handled.
	if !private.CheckCols.IsEmpty() {
		return false
	}

	if len(private.FKCascades) != 0 {
		return false
	}

	if private.AfterTriggers != nil {
		return false
	}

	if !scan.IsCanonical() {
		return false
	}

	// To use update swap or delete swap, every column provided by the scan must
	// be constrained to exactly zero or one value by the filters.
	//
	// Actually, it's every column required by the mutation private that isn't
	// provided by the projection, and every column required by the projection.
	//
	// (And we need to ensure that every primary index column is required by the
	// mutation private.)
	//
	// Do we want to consider optional filters? (Maybe a column is constrained to
	// one possible value?)
	//
	// Do extra filters matter? I don't think so, but then we need to keep the
	// select?
	//
	// I think to do this we just need to try to build the values clause.

	// let's at least check that all primary index cols are in the fetch cols
	md := c.mem.Metadata()
	table := md.Table(private.Table)
	primaryIndex := table.Index(cat.PrimaryIndex)
	for i := 0; i < primaryIndex.ColumnCount(); i++ {
		col := primaryIndex.Column(i)
		if col.Kind() == cat.System {
			continue
		}
		if private.FetchCols[col.Ordinal()] == 0 {
			return false
		}
	}

	return true
}

func (c *CustomFuncs) BuildSwapMutationValues(
	private *memo.MutationPrivate, filters memo.FiltersExpr, scan *memo.ScanPrivate,
) (memo.RelExpr, bool) {

	// which columns need to go into the values?
	// every column provided by the scan after pruning
	// but the problem is if we prune beforehand, we might prune away columns we need
	// let's write a version where we assume that we've already pruned "correctly"
	// then we can figure out how to fix it

	// oh, wait
	// maybe the idea is to FIRST replace with update swap
	// then try to get rid of the scan
	// and if we cannot, then we go back to non-swap version and re-prune??
	// so how would that work?
	// hmm
	// well, let me try the version where we assume pruning is correct

	colExprs, remainingFilters, ok := c.constrainColsToScalarExprs(scan.Cols, filters)
	if !ok {
		return nil, false
	}

	md := c.mem.Metadata()

	newRow := make(memo.ScalarListExpr, 0, scan.Cols.Len())
	newTypes := make([]*types.T, 0, scan.Cols.Len())
	newCols := make(opt.ColList, 0, scan.Cols.Len())

	scan.Cols.ForEach(func(col opt.ColumnID) {
		if colExpr, ok := colExprs[col]; ok {
			newRow = append(newRow, colExpr)
			newTypes = append(newTypes, md.ColumnMeta(col).Type)
			newCols = append(newCols, col)
		} else {
			return
		}
	})
	if len(newRow) < scan.Cols.Len() {
		return nil, false
	}

	tupleTyp := types.MakeTuple(newTypes)
	tuple := c.f.ConstructTuple(newRow, tupleTyp)

	expr := c.f.ConstructValues(
		memo.ScalarListExpr{tuple},
		&memo.ValuesPrivate{
			Cols: newCols,
			ID:   c.mem.Metadata().NextUniqueID(),
		},
	)

	if len(remainingFilters) != 0 {
		expr = c.f.ConstructSelect(expr, remainingFilters)
	}

	return expr, true
}

// constrainColsToScalarExprs is a simplified version of
// idxconstraint.ConstrainIndexPrefixCols which constrains columns to scalar
// expressions instead of constraint.Constraint.
// TODO: optionalFilters memo.FiltersExpr,
// TODO: notNullCols opt.ColSet,
func (c *CustomFuncs) constrainColsToScalarExprs(
	cols opt.ColSet, requiredFilters memo.FiltersExpr,
) (colExprs map[opt.ColumnID]opt.ScalarExpr, remainingFilters memo.FiltersExpr, ok bool) {
	colExprs = make(map[opt.ColumnID]opt.ScalarExpr)
	buildColExpr := func(col opt.ColumnID, e opt.ScalarExpr) bool {
		if cols.Contains(col) {
			if _, ok := colExprs[col]; !ok {
				colExprs[col] = e
				return true
			}
		}
		return false
	}

	md := c.mem.Metadata()

	var constrainColsInExpr func(opt.ScalarExpr)
	constrainColsInExpr = func(e opt.ScalarExpr) {
		switch t := e.(type) {
		case *memo.VariableExpr:
			if md.ColumnMeta(t.Col).Type.Family() == types.BoolFamily {
				if buildColExpr(t.Col, memo.TrueSingleton) {
					return
				}
			}
		case *memo.NotExpr:
			if v, ok := t.Input.(*memo.VariableExpr); ok {
				if md.ColumnMeta(v.Col).Type.Family() == types.BoolFamily {
					if buildColExpr(v.Col, memo.FalseSingleton) {
						return
					}
				}
			}
		case *memo.AndExpr:
			constrainColsInExpr(t.Left)
			constrainColsInExpr(t.Right)
			return
		case *memo.EqExpr:
			if v, ok := t.Left.(*memo.VariableExpr); ok {
				if buildColExpr(v.Col, t.Right) {
					remainingFilters = append(remainingFilters, c.constructIsNotNull(v.Col))
					return
				}
			}
		case *memo.NeExpr:
			if v, ok := t.Left.(*memo.VariableExpr); ok {
				if md.ColumnMeta(v.Col).Type.Family() == types.BoolFamily {
					if buildColExpr(v.Col, c.f.ConstructNot(t.Right)) {
						remainingFilters = append(remainingFilters, c.constructIsNotNull(v.Col))
						return
					}
				}
			}
		case *memo.InExpr:
			if v, ok := t.Left.(*memo.VariableExpr); ok {
				// checking for cardinality zero or one should have limited this to a
				// single value, but I think we still need to unpack the tuple?
				if buildColExpr(v.Col, t.Right) {
					remainingFilters = append(remainingFilters, c.constructIsNotNull(v.Col))
					return
				}
			}
		case *memo.IsExpr:
			if v, ok := t.Left.(*memo.VariableExpr); ok {
				if buildColExpr(v.Col, t.Right) {
					return
				}
			}
		case *memo.IsNotExpr:
			if v, ok := t.Left.(*memo.VariableExpr); ok {
				if md.ColumnMeta(v.Col).Type.Family() == types.BoolFamily {
					if buildColExpr(v.Col, c.f.ConstructNot(t.Right)) {
						return
					}
				}
			}
		}
		remainingFilters = append(remainingFilters, c.f.ConstructFiltersItem(e))
	}

	for i := range requiredFilters {
		constrainColsInExpr(requiredFilters[i].Condition)
	}

	if len(colExprs) != cols.Len() {
		return nil, nil, false
	}
	return colExprs, remainingFilters, true
}

func (c *CustomFuncs) constructIsNotNull(colID opt.ColumnID) memo.FiltersItem {
	return c.f.ConstructFiltersItem(
		c.f.ConstructIsNot(
			c.f.ConstructVariable(colID), memo.NullSingleton,
		),
	)
}

func (c *CustomFuncs) UseSwapMutation(private *memo.MutationPrivate) *memo.MutationPrivate {
	swapPrivate := *private
	swapPrivate.Swap = true
	return &swapPrivate
}
