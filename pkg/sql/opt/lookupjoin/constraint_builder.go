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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Constraint is used to constrain a lookup join. There are two types of
// constraints:
//
//   1. Constraints with KeyCols use columns from the input to directly
//      constrain lookups into a target index.
//   2. Constraints with a LookupExpr build multiple spans from an expression
//      that is evaluated for each input row. These spans are used to perform
//      lookups into a target index.
//
// A constraint is not constraining if both KeyCols and LookupExpr are empty.
// See IsUnconstrained.
type Constraint struct {
	// KeyCols is an ordered list of columns from the left side of the join to
	// be used as lookup join key columns. This list corresponds to the columns
	// in RightSideCols. It will be nil if LookupExpr is non-nil.
	KeyCols opt.ColList

	// RightSideCols is an ordered list of prefix index columns that are
	// constrained by this constraint. It corresponds 1:1 with the columns in
	// KeyCols if KeyCols is non-nil. Otherwise, it includes the prefix of index
	// columns constrained by LookupExpr.
	RightSideCols opt.ColList

	// LookupExpr is a lookup expression for multi-span lookup joins. It will be
	// nil if KeyCols is non-nil.
	LookupExpr memo.FiltersExpr

	// InputProjections contains constant values and computed columns that must
	// be projected on the lookup join's input.
	InputProjections memo.ProjectionsExpr

	// ConstFilters contains constant equalities and ranges in either KeyCols or
	// LookupExpr that are used to aid selectivity estimation. See
	// memo.LookupJoinPrivate.ConstFilters.
	ConstFilters memo.FiltersExpr

	// RemainingFilters contains explicit ON filters that are not represented by
	// KeyCols or LookupExpr. These filters must be included as ON filters in
	// the lookup join.
	RemainingFilters memo.FiltersExpr
}

// IsUnconstrained returns true if the constraint does not constrain a lookup
// join.
func (c *Constraint) IsUnconstrained() bool {
	return len(c.KeyCols) == 0 && len(c.LookupExpr) == 0
}

// ConstraintBuilder determines how to constrain index key columns for a lookup
// join. See Build for more details.
type ConstraintBuilder struct {
	f       *norm.Factory
	md      *opt.Metadata
	evalCtx *eval.Context

	// The table on the right side of the join to perform the lookup into.
	table opt.TableID
	// The columns on the left and right side of the join.
	leftCols, rightCols opt.ColSet
	// A map of columns in rightEq to their corresponding columns in leftEq.
	// This is used to remap computed column expressions, and is only
	// initialized if needed.
	eqColMap opt.ColMap
}

// Init initializes a ConstraintBuilder. Once initialized, a ConstraintBuilder
// can be reused to build lookup join constraints for all indexes in the given
// table, as long as the join input and ON condition do not change. If no lookup
// join can be built from the given filters and left/right columns, then
// ok=false is returned.
func (b *ConstraintBuilder) Init(
	f *norm.Factory,
	md *opt.Metadata,
	evalCtx *eval.Context,
	table opt.TableID,
	leftCols, rightCols opt.ColSet,
	onFilters memo.FiltersExpr,
) (ok bool) {
	leftEq, _, _ := memo.ExtractJoinEqualityColumns(leftCols, rightCols, onFilters)
	if len(leftEq) == 0 {
		// Exploring a lookup join is only beneficial if there is at least one
		// pair of equality columns in the join filters.
		return false
	}

	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*b = ConstraintBuilder{
		f:         f,
		md:        md,
		evalCtx:   evalCtx,
		table:     table,
		leftCols:  leftCols,
		rightCols: rightCols,
	}
	return true
}

// Build returns a Constraint that constrains a lookup join on the given index.
// The constraint returned may be unconstrained if no constraint could be built.
func (b *ConstraintBuilder) Build(
	index cat.Index, onFilters, optionalFilters memo.FiltersExpr,
) Constraint {
	// Extract the equality columns from onFilters. We cannot use the results of
	// the extraction in Init because onFilters may be reduced by the caller
	// after Init due to partial index implication. If the filters are reduced,
	// eqFilterOrds calculated during Init would no longer be valid because the
	// ordinals of the filters will have changed.
	leftEq, rightEq, eqFilterOrds :=
		memo.ExtractJoinEqualityColumns(b.leftCols, b.rightCols, onFilters)
	rightEqSet := rightEq.ToSet()

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
	firstIdxCol := b.table.IndexColumnID(index, 0)
	if _, ok := rightEq.Find(firstIdxCol); !ok {
		if _, ok := b.findComputedColJoinEquality(b.table, firstIdxCol, rightEqSet); !ok {
			if _, _, ok := FindJoinFilterConstants(allFilters, firstIdxCol, b.evalCtx); !ok {
				return Constraint{}
			}
		}
	}

	// Find the longest prefix of index key columns that are constrained by
	// an equality with another column or a constant.
	numIndexKeyCols := index.LaxKeyColumnCount()

	keyCols := make(opt.ColList, 0, numIndexKeyCols)
	rightSideCols := make(opt.ColList, 0, numIndexKeyCols)
	var inputProjections memo.ProjectionsExpr
	var lookupExpr memo.FiltersExpr
	var constFilters memo.FiltersExpr
	var filterOrdsToExclude util.FastIntSet
	shouldBuildMultiSpanLookupJoin := false

	// All the lookup conditions must apply to the prefix of the index and so
	// the projected columns created must be created in order.
	for j := 0; j < numIndexKeyCols; j++ {
		idxCol := b.table.IndexColumnID(index, j)
		if eqIdx, ok := rightEq.Find(idxCol); ok {
			keyCols = append(keyCols, leftEq[eqIdx])
			rightSideCols = append(rightSideCols, idxCol)
			filterOrdsToExclude.Add(eqFilterOrds[eqIdx])
			continue
		}

		// If the column is computed and an equality constraint can be
		// synthesized for it, we can project a column from the join's input
		// that can be used as a key column. We create the projection here,
		// and construct a Project expression that wraps the join's input
		// below. See findComputedColJoinEquality for the requirements to
		// synthesize a computed column equality constraint.
		if expr, ok := b.findComputedColJoinEquality(b.table, idxCol, rightEqSet); ok {
			colMeta := b.md.ColumnMeta(idxCol)
			compEqCol := b.md.AddColumn(fmt.Sprintf("%s_eq", colMeta.Alias), colMeta.Type)

			// Lazily initialize eqColMap.
			if b.eqColMap.Empty() {
				for i := range rightEq {
					b.eqColMap.Set(int(rightEq[i]), int(leftEq[i]))
				}
			}

			// Project the computed column expression, mapping all columns
			// in rightEq to corresponding columns in leftEq.
			projection := b.f.ConstructProjectionsItem(b.f.RemapCols(expr, b.eqColMap), compEqCol)
			inputProjections = append(inputProjections, projection)
			keyCols = append(keyCols, compEqCol)
			rightSideCols = append(rightSideCols, idxCol)
			continue
		}

		// Try to find a filter that constrains this column to non-NULL
		// constant values. We cannot use a NULL value because the lookup
		// join implements logic equivalent to simple equality between
		// columns (where NULL never equals anything).
		foundVals, allIdx, ok := FindJoinFilterConstants(allFilters, idxCol, b.evalCtx)
		if ok && len(foundVals) == 1 {
			// If a single constant value was found, project it in the input
			// and use it as an equality column.
			idxColType := b.md.ColumnMeta(idxCol).Type
			constColID := b.md.AddColumn(
				fmt.Sprintf("lookup_join_const_col_@%d", idxCol),
				idxColType,
			)
			inputProjections = append(inputProjections, b.f.ConstructProjectionsItem(
				b.f.ConstructConstVal(foundVals[0], idxColType),
				constColID,
			))
			constFilters = append(constFilters, allFilters[allIdx])
			keyCols = append(keyCols, constColID)
			rightSideCols = append(rightSideCols, idxCol)
			filterOrdsToExclude.Add(allIdx)
			continue
		}

		var foundRange bool
		if !ok {
			// If constant values were not found, try to find a filter that
			// constrains this index column to a range.
			_, foundRange = b.findJoinFilterRange(allFilters, idxCol)
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

	var c Constraint
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
				leftCol, rightCol, b.leftCols, b.rightCols, onFilters,
			)
		}
		var multiSpanFilterOrdsToExclude util.FastIntSet
		eqFilters, constFilters, rightSideCols, multiSpanFilterOrdsToExclude =
			b.findFiltersForIndexLookup(
				allFilters, b.table, index, leftEq, rightEq, eqFilterOrds, extractEqualityFilter,
			)
		lookupExpr = append(eqFilters, constFilters...)
		filterOrdsToExclude.UnionWith(multiSpanFilterOrdsToExclude)

		// A multi-span lookup join with a lookup expression has no key columns
		// and requires no projections on the input.
		c = Constraint{
			RightSideCols: rightSideCols,
			LookupExpr:    lookupExpr,
			ConstFilters:  constFilters,
		}
	} else {
		// If we did not build a lookup expression, use the key columns we
		// found, if any.
		c = Constraint{
			KeyCols:          keyCols,
			RightSideCols:    rightSideCols,
			InputProjections: inputProjections,
			ConstFilters:     constFilters,
		}
	}

	// We have not found a valid constraint, so there is no need to calculate
	// the remaining filters.
	if c.IsUnconstrained() {
		return c
	}

	// Reduce the remaining filters.
	c.RemainingFilters = make(memo.FiltersExpr, 0, len(onFilters))
	for i := range onFilters {
		if !filterOrdsToExclude.Contains(i) {
			c.RemainingFilters = append(c.RemainingFilters, onFilters[i])
		}
	}

	return c
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
func (b *ConstraintBuilder) findComputedColJoinEquality(
	tabID opt.TableID, col opt.ColumnID, eqCols opt.ColSet,
) (_ opt.ScalarExpr, ok bool) {
	tabMeta := b.md.TableMeta(tabID)
	tab := b.md.Table(tabID)
	if tab.Column(tabID.ColumnOrdinal(col)).IsNullable() {
		return nil, false
	}
	expr, ok := tabMeta.ComputedColExpr(col)
	if !ok {
		return nil, false
	}
	var sharedProps props.Shared
	memo.BuildSharedProps(expr, &sharedProps, b.evalCtx)
	if !sharedProps.OuterCols.SubsetOf(eqCols) {
		return nil, false
	}
	return expr, true
}

// findFiltersForIndexLookup finds the equality and constraint filters in
// filters that can be used to constrain the given index. Constraint filters
// can be either constants or inequality conditions.
func (b *ConstraintBuilder) findFiltersForIndexLookup(
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	leftEq, rightEq opt.ColList,
	eqFiltersOrds []int,
	extractEqualityFilter func(opt.ColumnID, opt.ColumnID) memo.FiltersItem,
) (
	eqFilters, constFilters memo.FiltersExpr,
	rightSideCols opt.ColList,
	filterOrdsToExclude util.FastIntSet,
) {
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
			filterOrdsToExclude.Add(eqFiltersOrds[eqIdx])
			continue
		}

		var foundRange bool
		// Try to find a filter that constrains this column to non-NULL
		// constant values. We cannot use a NULL value because the lookup
		// join implements logic equivalent to simple equality between
		// columns (where NULL never equals anything).
		values, allIdx, foundConstFilter := FindJoinFilterConstants(filters, idxCol, b.evalCtx)
		if !foundConstFilter {
			// If there's no const filters look for an inequality range.
			allIdx, foundRange = b.findJoinFilterRange(filters, idxCol)
			if !foundRange {
				break
			}
		}

		// At this point, we've found either a set of values or a range that
		// constrain the index column.
		filterOrdsToExclude.Add(allIdx)
		if constFilters == nil {
			constFilters = make(memo.FiltersExpr, 0, numIndexKeyCols-j)
		}

		// Construct a constant filter as an equality, IN expression, or
		// inequality. These are the only types of expressions currently
		// supported by the lookupJoiner for building lookup spans.
		if foundConstFilter {
			constFilter := filters[allIdx]
			if !isCanonicalFilter(constFilter) {
				b.f.DisableOptimizationsTemporarily(func() {
					constFilter = b.f.ConstructConstFilter(idxCol, values)
				})
			}
			constFilters = append(constFilters, constFilter)
		}
		// Non-canonical range filters aren't supported and are already filtered
		// out by findJoinFilterRange above.
		if foundRange {
			constFilters = append(constFilters, filters[allIdx])
			// Generating additional columns after a range isn't helpful so stop here.
			break
		}
	}

	if len(eqFilters) == 0 {
		// We couldn't find equality columns which we can lookup.
		return nil, nil, nil, util.FastIntSet{}
	}

	return eqFilters, constFilters, rightSideCols, filterOrdsToExclude
}

// findJoinFilterRange tries to find an inequality range for this column.
func (b *ConstraintBuilder) findJoinFilterRange(
	filters memo.FiltersExpr, col opt.ColumnID,
) (filterIdx int, ok bool) {
	// canAdvance returns whether non-nil, non-NULL datum can be "advanced"
	// (i.e. both Next and Prev can be called on it).
	canAdvance := func(val tree.Datum) bool {
		if val.IsMax(b.evalCtx) {
			return false
		}
		_, ok := val.Next(b.evalCtx)
		if !ok {
			return false
		}
		if val.IsMin(b.evalCtx) {
			return false
		}
		_, ok = val.Prev(b.evalCtx)
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
				!isCanonicalFilter(filters[filterIdx]) {
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

// isCanonicalFilter returns true for the limited set of expr's that are
// supported by the lookup joiner at execution time.
func isCanonicalFilter(filter memo.FiltersItem) bool {
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
