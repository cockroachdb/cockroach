// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
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

// CanUseSwapMutation checks whether Update or Delete can become UpdateSwap or
// DeleteSwap, respectively.
func (c *CustomFuncs) CanUseSwapMutation(private *memo.MutationPrivate) bool {
	// Checks are not currently handled by UpdateSwap or DeleteSwap.
	if !private.CheckCols.IsEmpty() {
		return false
	}

	// FK cascades are not currently handled by UpdateSwap or DeleteSwap.
	if len(private.FKCascades) != 0 {
		return false
	}

	// After triggers are not currently handled by UpdateSwap or DeleteSwap.
	if private.AfterTriggers != nil {
		return false
	}

	md := c.mem.Metadata()
	table := md.Table(private.Table)

	// UpdateSwap and DeleteSwap currently only operate on tables with a single
	// column family. We should be able to lift this restriction pretty easily.
	if table.FamilyCount() > 1 {
		return false
	}

	// UpdateSwap and DeleteSwap require fetch columns to contain every column in
	// the primary index, in order to build the expected value for the row.
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

// BuildSwapMutationInput tries to replace the initial scan of a mutation
// statement with a ValuesExpr containing a single row. If it can successfully
// rewrite the mutation input then the mutation can become a swap mutation.
func (c *CustomFuncs) BuildSwapMutationInput(input memo.RelExpr) (memo.RelExpr, bool) {
	switch t := input.(type) {
	case *memo.ProjectExpr:
		if newInput, ok := c.BuildSwapMutationInput(t.Input); ok {
			return c.f.ConstructProject(newInput, t.Projections, t.Passthrough), true
		}
	case *memo.SelectExpr:
		// Swap mutations can only operate on a single row.
		if c.HasZeroOrOneRow(t) {
			if scan, ok := t.Input.(*memo.ScanExpr); ok {
				// This must be a canonical scan to safely assume that the select
				// filters contain all predicates from the query.
				if scan.IsCanonical() {
					return c.buildSwapMutationValues(t.Filters, &scan.ScanPrivate)
				}
			}
		}
		// TODO: case *memo.VectorMutationSearchExpr
		// TODO: case *memo.BarrierExpr
		// TODO: joins
		// can we search for any scan providing the fetch cols?
	}
	return nil, false
}

// buildSwapMutationValues tries to construct a ValuesExpr containing the only
// possible row that could satisfy the given filters on the given canonical
// scan. UpdateSwap and DeleteSwap use this ValuesExpr in lieu of a ScanExpr.
func (c *CustomFuncs) buildSwapMutationValues(
	filters memo.FiltersExpr, scan *memo.ScanPrivate,
) (memo.RelExpr, bool) {
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
			// can we steal the col ID like this?
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

	// TODO: If any stored computed columns are still unconstrained, we could add
	// a projection here that computes them based on other column values.

	if len(remainingFilters) != 0 {
		expr = c.f.ConstructSelect(expr, remainingFilters)
	}

	return expr, true
}

// constrainColsToScalarExprs is a simplified version of
// idxconstraint.ConstrainIndexPrefixCols. It uses the given filters to
// constrain the given columns to scalar expressions (instead of
// constraint.Constraint like ConstrainIndexPrefixCols). If a filter does not
// constrain the column to a single value, the filter will be added to
// remainingFilters and the column will not be constrained.
//
// constrainColsToScalarExprs returns the mapping from constrained columns to
// scalar expressions, the remaining filters, and a boolean indicating whether
// all columns were constrained.
func (c *CustomFuncs) constrainColsToScalarExprs(
	cols opt.ColSet, requiredFilters memo.FiltersExpr,
) (colExprs map[opt.ColumnID]opt.ScalarExpr, remainingFilters memo.FiltersExpr, ok bool) {

	// colExprs collects the scalar expression for each constrained column.
	colExprs = make(map[opt.ColumnID]opt.ScalarExpr)

	// useExprAsScalarConstraint adds the scalar expression to colExprs.
	useExprAsScalarConstraint := func(col opt.ColumnID, e opt.ScalarExpr) bool {
		if cols.Contains(col) {
			// Only use the first constraining filter found. If there is another
			// constraining filter, it will become part of remainingFilters.
			if _, ok := colExprs[col]; !ok {
				colExprs[col] = e
				return true
			}
		}
		return false
	}

	md := c.mem.Metadata()

	// constrainColsInExpr walks a filter predicate, adding scalar expressions for
	// any columns constrained to a single value by the predicate.
	var constrainColsInExpr func(opt.ScalarExpr)
	constrainColsInExpr = func(e opt.ScalarExpr) {
		switch t := e.(type) {
		case *memo.VariableExpr:
			// `WHERE col` will be true if col is a boolean-typed column that has the
			// value True.
			if md.ColumnMeta(t.Col).Type.Family() == types.BoolFamily {
				if useExprAsScalarConstraint(t.Col, memo.TrueSingleton) {
					return
				}
			}
		case *memo.NotExpr:
			// `WHERE NOT col` will be true if col is a boolean-typed column that has
			// the value False.
			if v, ok := t.Input.(*memo.VariableExpr); ok {
				if md.ColumnMeta(v.Col).Type.Family() == types.BoolFamily {
					if useExprAsScalarConstraint(v.Col, memo.FalseSingleton) {
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
				// `WHERE col = <expr>` will be true if col has the value <expr>, <expr>
				// is not a composite-encoded value, and <expr> IS NOT NULL.
				if !colinfo.CanHaveCompositeKeyEncoding(md.ColumnMeta(v.Col).Type) {
					if useExprAsScalarConstraint(v.Col, t.Right) {
						// Because NULL = NULL evaluates to NULL, rather than true, we must
						// also guard against the scalar expression evaluating to NULL.
						remainingFilters = append(remainingFilters, c.constructIsNotNull(v.Col))
						return
					}
				}
			}
			// TODO: tuple of variables on LHS
		case *memo.IsExpr:
			if v, ok := t.Left.(*memo.VariableExpr); ok {
				// `WHERE col IS NOT DISTINCT FROM <expr>` will be true if col has the
				// value <expr>, and <expr> is not a composite-encoded value.
				if !colinfo.CanHaveCompositeKeyEncoding(md.ColumnMeta(v.Col).Type) {
					if useExprAsScalarConstraint(v.Col, t.Right) {
						return
					}
				}
			}
			// TODO: tuple of variables on LHS
		}
		// TODO: case *memo.InExpr

		// If we could not turn the filter into a scalar expression constraining a
		// column, add it to remainingFilters.
		remainingFilters = append(remainingFilters, c.f.ConstructFiltersItem(e))
	}

	for i := range requiredFilters {
		constrainColsInExpr(requiredFilters[i].Condition)
	}

	return colExprs, remainingFilters, len(colExprs) == cols.Len()
}

func (c *CustomFuncs) constructIsNotNull(colID opt.ColumnID) memo.FiltersItem {
	return c.f.ConstructFiltersItem(
		c.f.ConstructIsNot(
			c.f.ConstructVariable(colID), memo.NullSingleton,
		),
	)
}

// UseSwapMutation builds a copy of the given MutationPrivate with Swap = true.
func (c *CustomFuncs) UseSwapMutation(private *memo.MutationPrivate) *memo.MutationPrivate {
	swapPrivate := *private
	swapPrivate.Swap = true
	return &swapPrivate
}
