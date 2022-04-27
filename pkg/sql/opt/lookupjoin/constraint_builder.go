// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lookupjoin

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func stub() {
	// Find the longest prefix of index key columns that are constrained by
	// an equality with another column or a constant.
	numIndexKeyCols := index.LaxKeyColumnCount()

	var constraintFilters memo.FiltersExpr
	allFilters := append(onFilters, optionalFilters...)

	// Check if the first column in the index either:
	//
	//   1. Has an equality constraint.
	//   2. Is a computed column for which an equality constraint can be
	//      generated.
	//   3. Is constrained to a constant value or values.
	//
	// This check doesn't guarantee that we will find lookup join key
	// columns, but it avoids unnecessary work in most cases.
	firstIdxCol := scanPrivate.Table.IndexColumnID(index, 0)
	if _, ok := rightEq.Find(firstIdxCol); !ok {
		if _, ok := c.findComputedColJoinEquality(scanPrivate.Table, firstIdxCol, rightEqSet); !ok {
			if _, _, ok := c.findJoinFilterConstants(allFilters, firstIdxCol); !ok {
				return
			}
		}
	}

	shouldBuildMultiSpanLookupJoin := false

	// All the lookup conditions must apply to the prefix of the index and so
	// the projected columns created must be created in order.
	for j := 0; j < numIndexKeyCols; j++ {
		idxCol := scanPrivate.Table.IndexColumnID(index, j)
		if eqIdx, ok := rightEq.Find(idxCol); ok {
			lookupJoin.KeyCols = append(lookupJoin.KeyCols, leftEq[eqIdx])
			rightSideCols = append(rightSideCols, idxCol)
			continue
		}

		// If the column is computed and an equality constraint can be
		// synthesized for it, we can project a column from the join's input
		// that can be used as a key column. We create the projection here,
		// and construct a Project expression that wraps the join's input
		// below. See findComputedColJoinEquality for the requirements to
		// synthesize a computed column equality constraint.
		if expr, ok := c.findComputedColJoinEquality(scanPrivate.Table, idxCol, rightEqSet); ok {
			colMeta := md.ColumnMeta(idxCol)
			compEqCol := md.AddColumn(fmt.Sprintf("%s_eq", colMeta.Alias), colMeta.Type)

			// Lazily initialize eqColMap.
			if eqColMap.Empty() {
				for i := range rightEq {
					eqColMap.Set(int(rightEq[i]), int(leftEq[i]))
				}
			}

			// Project the computed column expression, mapping all columns
			// in rightEq to corresponding columns in leftEq.
			projection := c.e.f.ConstructProjectionsItem(c.e.f.RemapCols(expr, eqColMap), compEqCol)
			inputProjections = append(inputProjections, projection)
			lookupJoin.KeyCols = append(lookupJoin.KeyCols, compEqCol)
			rightSideCols = append(rightSideCols, idxCol)
			continue
		}

		// Try to find a filter that constrains this column to non-NULL
		// constant values. We cannot use a NULL value because the lookup
		// join implements logic equivalent to simple equality between
		// columns (where NULL never equals anything).
		foundVals, allIdx, ok := c.findJoinFilterConstants(allFilters, idxCol)
		if ok && len(foundVals) == 1 {
			// If a single constant value was found, project it in the input
			// and use it as an equality column.
			idxColType := c.e.f.Metadata().ColumnMeta(idxCol).Type
			constColID := c.e.f.Metadata().AddColumn(
				fmt.Sprintf("lookup_join_const_col_@%d", idxCol),
				idxColType,
			)
			inputProjections = append(inputProjections, c.e.f.ConstructProjectionsItem(
				c.e.f.ConstructConstVal(foundVals[0], idxColType),
				constColID,
			))
			constraintFilters = append(constraintFilters, allFilters[allIdx])
			lookupJoin.KeyCols = append(lookupJoin.KeyCols, constColID)
			rightSideCols = append(rightSideCols, idxCol)
			continue
		}

		var foundRange bool
		if !ok {
			// If constant values were not found, try to find a filter that
			// constrains this index column to a range.
			_, foundRange = c.findJoinFilterRange(allFilters, idxCol)
		}

		// If more than one constant value or a range to constrain the index
		// column was found, use a LookupExpr rather than KeyCols.
		if len(foundVals) > 1 || foundRange {
			shouldBuildMultiSpanLookupJoin = true
		}

		// Either multiple constant values or a range were found, or the
		// index column cannot be constrained. In all cases, we cannot
		// continue on to the next index column, so we break out of the
		// loop.
		break
	}

	if shouldBuildMultiSpanLookupJoin {
		// Some of the index columns were constrained to multiple constant
		// values or a range expression, so we cannot build a lookup join
		// with KeyCols. As an alternative, we store all the filters needed
		// for the lookup in LookupExpr, which will be used to construct
		// spans at execution time. Each input row will generate multiple
		// spans to lookup in the index.
		//
		// For example, if the index cols are (region, id) and the
		// LookupExpr is `region in ('east', 'west') AND id = input.id`,
		// each input row will generate two spans to be scanned in the
		// lookup:
		//
		//   [/'east'/<id> - /'east'/<id>]
		//   [/'west'/<id> - /'west'/<id>]
		//
		// Where <id> is the value of input.id for the current input row.
		var eqFilters memo.FiltersExpr
		extractEqualityFilter := func(leftCol, rightCol opt.ColumnID) memo.FiltersItem {
			return memo.ExtractJoinEqualityFilter(
				leftCol, rightCol, inputProps.OutputCols, rightCols, onFilters,
			)
		}
		eqFilters, constraintFilters, rightSideCols = c.findFiltersForIndexLookup(
			allFilters, scanPrivate.Table, index, leftEq, rightEq, extractEqualityFilter,
		)
		lookupJoin.LookupExpr = append(eqFilters, constraintFilters...)

		// Reset KeyCols since we're not using it anymore.
		lookupJoin.KeyCols = opt.ColList{}
		// Reset the input projections since we don't need any constant
		// values projected.
		inputProjections = nil
	}
}

// findComputedColJoinEquality returns the computed column expression of col and
// ok=true when a join equality constraint can be generated for the column. This
// is possible when:
//
//   1. col is non-nullable.
//   2. col is a computed column.
//   3. Columns referenced in the computed expression are a subset of columns
//      that already have equality constraints.
//
// For example, consider the table and query:
//
//   CREATE TABLE a (
//     a INT
//   )
//
//   CREATE TABLE bc (
//     b INT,
//     c INT NOT NULL AS (b + 1) STORED
//   )
//
//   SELECT * FROM a JOIN b ON a = b
//
// We can add an equality constraint for c because c is a function of b and b
// has an equality constraint in the join predicate:
//
//   SELECT * FROM a JOIN b ON a = b AND a + 1 = c
//
// Condition (1) is required to prevent generating invalid equality constraints
// for computed column expressions that can evaluate to NULL even when the
// columns referenced in the expression are non-NULL. For example, consider the
// table and query:
//
//   CREATE TABLE a (
//     a INT
//   )
//
//   CREATE TABLE bc (
//     b INT,
//     c INT AS (CASE WHEN b > 0 THEN NULL ELSE -1 END) STORED
//   )
//
//   SELECT a, b FROM a JOIN b ON a = b
//
// The following is an invalid transformation: a row such as (a=1, b=1) would no
// longer be returned because NULL=NULL is false.
//
//   SELECT a, b FROM a JOIN b ON a = b AND (CASE WHEN a > 0 THEN NULL ELSE -1 END) = c
//
// TODO(mgartner): We can relax condition (1) to allow nullable columns if it
// can be proven that the expression will never evaluate to NULL. We can use
// memo.ExprIsNeverNull to determine this, passing both NOT NULL and equality
// columns as notNullCols.
func (c *CustomFuncs) findComputedColJoinEquality(
	tabID opt.TableID, col opt.ColumnID, eqCols opt.ColSet,
) (_ opt.ScalarExpr, ok bool) {
	tabMeta := c.e.mem.Metadata().TableMeta(tabID)
	tab := c.e.mem.Metadata().Table(tabID)
	if tab.Column(tabID.ColumnOrdinal(col)).IsNullable() {
		return nil, false
	}
	expr, ok := tabMeta.ComputedColExpr(col)
	if !ok {
		return nil, false
	}
	var sharedProps props.Shared
	memo.BuildSharedProps(expr, &sharedProps, c.e.evalCtx)
	if !sharedProps.OuterCols.SubsetOf(eqCols) {
		return nil, false
	}
	return expr, true
}

// findFiltersForIndexLookup finds the equality and constraint filters in
// filters that can be used to constrain the given index. Constraint filters
// can be either constants or inequality conditions.
func (c *CustomFuncs) findFiltersForIndexLookup(
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	leftEq, rightEq opt.ColList,
	extractEqualityFilter func(opt.ColumnID, opt.ColumnID) memo.FiltersItem,
) (eqFilters, constraintFilters memo.FiltersExpr, rightSideCols opt.ColList) {
	numIndexKeyCols := index.LaxKeyColumnCount()

	eqFilters = make(memo.FiltersExpr, 0, len(filters))
	rightSideCols = make(opt.ColList, 0, len(filters))

	// All the lookup conditions must apply to the prefix of the index.
	for j := 0; j < numIndexKeyCols; j++ {
		idxCol := tabID.IndexColumnID(index, j)
		if eqIdx, ok := rightEq.Find(idxCol); ok {
			eqFilter := extractEqualityFilter(leftEq[eqIdx], rightEq[eqIdx])
			eqFilters = append(eqFilters, eqFilter)
			rightSideCols = append(rightSideCols, idxCol)
			continue
		}

		var foundRange bool
		// Try to find a filter that constrains this column to non-NULL
		// constant values. We cannot use a NULL value because the lookup
		// join implements logic equivalent to simple equality between
		// columns (where NULL never equals anything).
		values, allIdx, foundConstFilter := c.findJoinFilterConstants(filters, idxCol)
		if !foundConstFilter {
			// If there's no const filters look for an inequality range.
			allIdx, foundRange = c.findJoinFilterRange(filters, idxCol)
			if !foundRange {
				break
			}
		}

		if constraintFilters == nil {
			constraintFilters = make(memo.FiltersExpr, 0, numIndexKeyCols-j)
		}

		// Construct a constant filter as an equality, IN expression, or
		// inequality. These are the only types of expressions currently
		// supported by the lookupJoiner for building lookup spans.
		if foundConstFilter {
			constFilter := filters[allIdx]
			if !c.isCanonicalLookupJoinFilter(constFilter) {
				constFilter = c.makeConstFilter(idxCol, values)
			}
			constraintFilters = append(constraintFilters, constFilter)
		}
		// Non-canonical range filters aren't supported and are already filtered
		// out by findJoinFilterRange above.
		if foundRange {
			constraintFilters = append(constraintFilters, filters[allIdx])
			// Generating additional columns after a range isn't helpful so stop here.
			break
		}
	}

	if len(eqFilters) == 0 {
		// We couldn't find equality columns which we can lookup.
		return nil, nil, nil
	}

	return eqFilters, constraintFilters, rightSideCols
}

// makeConstFilter builds a filter that constrains the given column to the given
// set of constant values. This is performed by either constructing an equality
// expression or an IN expression.
func (c *CustomFuncs) makeConstFilter(col opt.ColumnID, values tree.Datums) memo.FiltersItem {
	if len(values) == 1 {
		return c.e.f.ConstructFiltersItem(c.e.f.ConstructEq(
			c.e.f.ConstructVariable(col),
			c.e.f.ConstructConstVal(values[0], values[0].ResolvedType()),
		))
	}
	elems := make(memo.ScalarListExpr, len(values))
	elemTypes := make([]*types.T, len(values))
	for i := range values {
		typ := values[i].ResolvedType()
		elems[i] = c.e.f.ConstructConstVal(values[i], typ)
		elemTypes[i] = typ
	}
	return c.e.f.ConstructFiltersItem(c.e.f.ConstructIn(
		c.e.f.ConstructVariable(col),
		c.e.f.ConstructTuple(elems, types.MakeTuple(elemTypes)),
	))
}

// findJoinFilterConstants tries to find a filter that is exactly equivalent to
// constraining the given column to a constant value or a set of constant
// values. If successful, the constant values and the index of the constraining
// FiltersItem are returned. If multiple filters match, the one that minimizes
// the number of returned values is chosen. Note that the returned constant
// values do not contain NULL.
func (c *CustomFuncs) findJoinFilterConstants(
	filters memo.FiltersExpr, col opt.ColumnID,
) (values tree.Datums, filterIdx int, ok bool) {
	var bestValues tree.Datums
	var bestFilterIdx int
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints {
			constCol, constVals, ok := props.Constraints.HasSingleColumnConstValues(c.e.evalCtx)
			if !ok || constCol != col {
				continue
			}
			hasNull := false
			for i := range constVals {
				if constVals[i] == tree.DNull {
					hasNull = true
					break
				}
			}
			if !hasNull && (bestValues == nil || len(bestValues) > len(constVals)) {
				bestValues = constVals
				bestFilterIdx = filterIdx
			}
		}
	}
	if bestValues == nil {
		return nil, -1, false
	}
	return bestValues, bestFilterIdx, true
}

// findJoinFilterRange tries to find an inequality range for this column.
func (c *CustomFuncs) findJoinFilterRange(
	filters memo.FiltersExpr, col opt.ColumnID,
) (filterIdx int, ok bool) {
	// canAdvance returns whether non-nil, non-NULL datum can be "advanced"
	// (i.e. both Next and Prev can be called on it).
	canAdvance := func(val tree.Datum) bool {
		if val.IsMax(c.e.evalCtx) {
			return false
		}
		_, ok := val.Next(c.e.evalCtx)
		if !ok {
			return false
		}
		if val.IsMin(c.e.evalCtx) {
			return false
		}
		_, ok = val.Prev(c.e.evalCtx)
		return ok
	}
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints && props.Constraints.Length() > 0 {
			constraintObj := props.Constraints.Constraint(0)
			constraintCol := constraintObj.Columns.Get(0)
			// Non-canonical filters aren't yet supported for range spans like
			// they are for const spans so filter those out here (const spans
			// from non-canonical filters can be turned into a canonical filter,
			// see makeConstFilter). We only support 1 span in the execution
			// engine so check that.
			if constraintCol.ID() != col || constraintObj.Spans.Count() != 1 ||
				!c.isCanonicalLookupJoinFilter(filters[filterIdx]) {
				continue
			}
			span := constraintObj.Spans.Get(0)
			// If we have a datum for either end of the span, we have to ensure
			// that it can be "advanced" if the corresponding span boundary is
			// exclusive.
			//
			// This limitation comes from the execution that must be able to
			// "advance" the start boundary, but since we don't know the
			// direction of the index here, we have to check both ends of the
			// span.
			if !span.StartKey().IsEmpty() && !span.StartKey().IsNull() {
				val := span.StartKey().Value(0)
				if span.StartBoundary() == constraint.ExcludeBoundary {
					if !canAdvance(val) {
						continue
					}
				}
			}
			if !span.EndKey().IsEmpty() && !span.EndKey().IsNull() {
				val := span.EndKey().Value(0)
				if span.EndBoundary() == constraint.ExcludeBoundary {
					if !canAdvance(val) {
						continue
					}
				}
			}
			return filterIdx, true
		}
	}
	return -1, false
}

// isCanonicalLookupJoinFilter returns true for the limited set of expr's that are
// supported by the lookup joiner at execution time.
func (c *CustomFuncs) isCanonicalLookupJoinFilter(filter memo.FiltersItem) bool {
	isVar := func(expr opt.Expr) bool {
		_, ok := expr.(*memo.VariableExpr)
		return ok
	}
	var isCanonicalInequality func(expr opt.Expr) bool
	isCanonicalInequality = func(expr opt.Expr) bool {
		switch t := expr.(type) {
		case *memo.RangeExpr:
			return isCanonicalInequality(t.And)
		case *memo.AndExpr:
			return isCanonicalInequality(t.Left) && isCanonicalInequality(t.Right)
		case *memo.GeExpr:
			return isCanonicalInequality(t.Left) && isCanonicalInequality(t.Right)
		case *memo.GtExpr:
			return isCanonicalInequality(t.Left) && isCanonicalInequality(t.Right)
		case *memo.LeExpr:
			return isCanonicalInequality(t.Left) && isCanonicalInequality(t.Right)
		case *memo.LtExpr:
			return isCanonicalInequality(t.Left) && isCanonicalInequality(t.Right)
		}
		return isVar(expr) || opt.IsConstValueOp(expr)
	}
	switch t := filter.Condition.(type) {
	case *memo.EqExpr:
		return isVar(t.Left) && opt.IsConstValueOp(t.Right)
	case *memo.InExpr:
		return isVar(t.Left) && memo.CanExtractConstTuple(t.Right)
	default:
		return isCanonicalInequality(t)
	}
}
