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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ConstraintBuilder determines how to constrain index key columns for a lookup
// join. See Build for more details.
type ConstraintBuilder struct {
	f       *norm.Factory
	md      *opt.Metadata
	evalCtx *tree.EvalContext

	inputProps      *props.Relational
	sp              *memo.ScanPrivate
	rightCols       opt.ColSet
	leftEq, rightEq opt.ColList
	rightEqSet      opt.ColSet
	eqColMap        opt.ColMap
}

// Init initializes a ConstraintBuilder. Once initialized, a ConstraintBuilder
// can be reused to build lookup join constraints for all indexes in the given
// ScanPrivate, as long as the join input and ON condition do not change.
func (b *ConstraintBuilder) Init(
	f *norm.Factory,
	md *opt.Metadata,
	evalCtx *tree.EvalContext,
	inputProps *props.Relational,
	sp *memo.ScanPrivate,
	rightCols opt.ColSet,
	leftEq, rightEq opt.ColList,
) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*b = ConstraintBuilder{
		f:          f,
		md:         md,
		evalCtx:    evalCtx,
		inputProps: inputProps,
		sp:         sp,
		rightCols:  rightCols,
		leftEq:     leftEq,
		rightEq:    rightEq,
		rightEqSet: rightEq.ToSet(),
	}
}

// Build determines how to constrain index key columns for a lookup join. It
// returns either a list of key columns or a lookup expression. It has several
// return values:
//
//   keyCols          - A list of columns to be used as lookup join key columns.
//   lookupExpr       - A lookup expression for multi-span lookup joins.
//   inputProjections - Projections of constant values and computed columns that
//                      must be projected on the lookup join's input.
//   constFilters     - Filters representing constant equalities and ranges in
//                      either keyCols or lookupExpr that are used to aid
//                      selectivity estimation.
//   rightSideCols    - A list of constrained index columns.
//
// Build will return either non-nil keyCols or a non-nil lookupExpr, but not
// both. If both keyCols and lookupExpr are nil, then the index cannot be used
// for a lookup join.
func (b *ConstraintBuilder) Build(
	index cat.Index, onFilters, optionalFilters memo.FiltersExpr,
) (
	keyCols opt.ColList,
	lookupExpr memo.FiltersExpr,
	inputProjections memo.ProjectionsExpr,
	constFilters memo.FiltersExpr,
	rightSideCols opt.ColList,
) {
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
	firstIdxCol := b.sp.Table.IndexColumnID(index, 0)
	if _, ok := b.rightEq.Find(firstIdxCol); !ok {
		if _, ok := b.findComputedColJoinEquality(b.sp.Table, firstIdxCol, b.rightEqSet); !ok {
			if _, _, ok := b.findJoinFilterConstants(allFilters, firstIdxCol); !ok {
				return
			}
		}
	}

	// Find the longest prefix of index key columns that are constrained by
	// an equality with another column or a constant.
	numIndexKeyCols := index.LaxKeyColumnCount()

	keyCols = make(opt.ColList, 0, numIndexKeyCols)
	rightSideCols = make(opt.ColList, 0, numIndexKeyCols)
	shouldBuildMultiSpanLookupJoin := false

	// All the lookup conditions must apply to the prefix of the index and so
	// the projected columns created must be created in order.
	for j := 0; j < numIndexKeyCols; j++ {
		idxCol := b.sp.Table.IndexColumnID(index, j)
		if eqIdx, ok := b.rightEq.Find(idxCol); ok {
			keyCols = append(keyCols, b.leftEq[eqIdx])
			rightSideCols = append(rightSideCols, idxCol)
			continue
		}

		// If the column is computed and an equality constraint can be
		// synthesized for it, we can project a column from the join's input
		// that can be used as a key column. We create the projection here,
		// and construct a Project expression that wraps the join's input
		// below. See findComputedColJoinEquality for the requirements to
		// synthesize a computed column equality constraint.
		if expr, ok := b.findComputedColJoinEquality(b.sp.Table, idxCol, b.rightEqSet); ok {
			colMeta := b.md.ColumnMeta(idxCol)
			compEqCol := b.md.AddColumn(fmt.Sprintf("%s_eq", colMeta.Alias), colMeta.Type)

			// Lazily initialize eqColMap.
			if b.eqColMap.Empty() {
				for i := range b.rightEq {
					b.eqColMap.Set(int(b.rightEq[i]), int(b.leftEq[i]))
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
		foundVals, allIdx, ok := b.findJoinFilterConstants(allFilters, idxCol)
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
			continue
		}

		var foundRange bool
		if !ok {
			// If constant values were not found, try to find a filter that
			// constrains this index column to a range.
			_, foundRange = findJoinFilterRange(allFilters, idxCol)
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
				leftCol, rightCol, b.inputProps.OutputCols, b.rightCols, onFilters,
			)
		}
		eqFilters, constFilters, rightSideCols = b.findFiltersForIndexLookup(
			allFilters, b.sp.Table, index, b.leftEq, b.rightEq, extractEqualityFilter,
		)
		lookupExpr = append(eqFilters, constFilters...)

		// A multi-span lookup join with a lookup expression has no key columns
		// and requires no projections on the input.
		return nil /* keyCols */, lookupExpr, nil /* inputProjections */, constFilters, rightSideCols
	}

	// If we did not build a lookup expression, return the key columms we found,
	// if any.
	return keyCols, nil /* lookupExpr */, inputProjections, constFilters, rightSideCols
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

// findFiltersForIndexLookup finds the equality and constant filters in
// filters that can be used to constrain the given index.
func (b *ConstraintBuilder) findFiltersForIndexLookup(
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	leftEq, rightEq opt.ColList,
	extractEqualityFilter func(opt.ColumnID, opt.ColumnID) memo.FiltersItem,
) (eqFilters, constFilters memo.FiltersExpr, rightSideCols opt.ColList) {
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
		values, allIdx, ok := b.findJoinFilterConstants(filters, idxCol)
		if !ok {
			// If there's no const filters look for an inequality range.
			allIdx, foundRange = findJoinFilterRange(filters, idxCol)
			if !foundRange {
				break
			}
		}

		if constFilters == nil {
			constFilters = make(memo.FiltersExpr, 0, numIndexKeyCols-j)
		}

		// Construct a constant filter as an equality, IN expression, or
		// inequality. These are the only types of expressions currently
		// supported by the lookupJoiner for building lookup spans.
		constFilter := filters[allIdx]
		if !isCanonicalFilter(constFilter) {
			if len(values) > 0 {
				constFilter = b.makeConstFilter(idxCol, values)
			} else if foundRange {
				constFilter = b.makeRangeFilter(idxCol, constFilter)
			}
		}
		constFilters = append(constFilters, constFilter)

		// Generating additional columns after a range isn't helpful so stop here.
		if foundRange {
			break
		}
	}

	if len(eqFilters) == 0 {
		// We couldn't find equality columns which we can lookup.
		return nil, nil, nil
	}

	return eqFilters, constFilters, rightSideCols
}

// makeConstFilter builds a filter that constrains the given column to the given
// set of constant values. This is performed by either constructing an equality
// expression or an IN expression.
func (b *ConstraintBuilder) makeConstFilter(col opt.ColumnID, values tree.Datums) memo.FiltersItem {
	if len(values) == 1 {
		return b.f.ConstructFiltersItem(b.f.ConstructEq(
			b.f.ConstructVariable(col),
			b.f.ConstructConstVal(values[0], values[0].ResolvedType()),
		))
	}
	elems := make(memo.ScalarListExpr, len(values))
	elemTypes := make([]*types.T, len(values))
	for i := range values {
		typ := values[i].ResolvedType()
		elems[i] = b.f.ConstructConstVal(values[i], typ)
		elemTypes[i] = typ
	}
	return b.f.ConstructFiltersItem(b.f.ConstructIn(
		b.f.ConstructVariable(col),
		b.f.ConstructTuple(elems, types.MakeTuple(elemTypes)),
	))
}

// makeRangeFilter builds a filter from a constrained column, we assume the
// column is constrained by at least 1 tight constraint. This code doesn't
// handle descending columns.
func (b *ConstraintBuilder) makeRangeFilter(
	col opt.ColumnID, filter memo.FiltersItem,
) memo.FiltersItem {
	props := filter.ScalarProps()
	if props.Constraints.Length() == 0 ||
		props.Constraints.Constraint(0).Spans.Count() != 1 ||
		props.Constraints.Constraint(0).Columns.Get(0).Descending() {
		panic(errors.AssertionFailedf("makeRangeFilter needs at least one ascending constraint with one span"))
	}
	span := props.Constraints.Constraint(0).Spans.Get(0)
	return b.makeRangeFilterFromSpan(col, span)
}

// makeRangeFilterFromSpan constructs a filter from a constraint.Span.
func (b *ConstraintBuilder) makeRangeFilterFromSpan(
	col opt.ColumnID, span *constraint.Span,
) memo.FiltersItem {
	variable := b.f.ConstructVariable(col)
	var left, right opt.ScalarExpr

	// Here and below we need to check for IsEmpty and IsNull because sometimes
	// Null is used for unbounded spans.  Found empirically by forcing
	// findFiltersForIndexLookup to always wrap the filters with makeRangeFilter.
	if !span.StartKey().IsEmpty() && !span.StartKey().IsNull() {
		val := span.StartKey().Value(0)
		if span.StartBoundary() == constraint.IncludeBoundary {
			left = b.f.ConstructGe(variable, b.f.ConstructConstVal(val, val.ResolvedType()))
		} else {
			left = b.f.ConstructGt(variable, b.f.ConstructConstVal(val, val.ResolvedType()))
		}
	}

	if !span.EndKey().IsEmpty() && !span.EndKey().IsNull() {
		val := span.EndKey().Value(0)
		if span.EndBoundary() == constraint.IncludeBoundary {
			right = b.f.ConstructLe(variable, b.f.ConstructConstVal(val, val.ResolvedType()))
		} else {
			right = b.f.ConstructLt(variable, b.f.ConstructConstVal(val, val.ResolvedType()))
		}
	}

	if left != nil && right != nil {
		return b.f.ConstructFiltersItem(b.f.ConstructRange(b.f.ConstructAnd(right, left)))
	} else if left != nil {
		return b.f.ConstructFiltersItem(left)
	} else if right != nil {
		return b.f.ConstructFiltersItem(right)
	}

	panic(errors.AssertionFailedf("Constraint needs a valid start or end key"))
}

// findJoinFilterConstants tries to find a filter that is exactly equivalent to
// constraining the given column to a constant value or a set of constant
// values. If successful, the constant values and the index of the constraining
// FiltersItem are returned. If multiple filters match, the one that minimizes
// the number of returned values is chosen. Note that the returned constant
// values do not contain NULL.
func (b *ConstraintBuilder) findJoinFilterConstants(
	filters memo.FiltersExpr, col opt.ColumnID,
) (values tree.Datums, filterIdx int, ok bool) {
	var bestValues tree.Datums
	var bestFilterIdx int
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints {
			constCol, constVals, ok := props.Constraints.HasSingleColumnConstValues(b.evalCtx)
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
func findJoinFilterRange(filters memo.FiltersExpr, col opt.ColumnID) (filterIdx int, ok bool) {
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints && props.Constraints.Length() > 0 {
			constraint := props.Constraints.Constraint(0)
			constraintCol := constraint.Columns.Get(0).ID()
			// See comment in findFiltersForIndexLookup for why we check filter here.
			// We only support 1 span in the execution engine so check that.
			if constraintCol != col || constraint.Spans.Count() != 1 {
				continue
			}
			return filterIdx, true
		}
	}
	return 0, false
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
