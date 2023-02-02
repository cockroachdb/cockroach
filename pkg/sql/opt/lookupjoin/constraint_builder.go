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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// Constraint is used to constrain a lookup join. There are two types of
// constraints:
//
//  1. Constraints with KeyCols use columns from the input to directly
//     constrain lookups into a target index.
//  2. Constraints with a LookupExpr build multiple spans from an expression
//     that is evaluated for each input row. These spans are used to perform
//     lookups into a target index.
//
// A constraint is not constraining if both KeyCols and LookupExpr are empty.
// See IsUnconstrained.
type Constraint struct {
	// KeyCols is an ordered list of columns from the left side of the join to
	// be used as lookup join key columns. This list corresponds to the columns
	// in RightSideCols. It will be nil if LookupExpr is non-nil.
	KeyCols opt.ColList

	// DerivedEquivCols is the set of lookup join equijoin columns which are part
	// of synthesized equality constraints based on another equality join
	// condition and a computed index key column in the lookup table. Since these
	// columns are not reducing the selectivity of the join, but are just added to
	// facilitate index lookups, they should not be used in determining join
	// selectivity.
	DerivedEquivCols opt.ColSet

	// RightSideCols is an ordered list of prefix index columns that are
	// constrained by this constraint. It corresponds 1:1 with the columns in
	// KeyCols if KeyCols is non-nil. Otherwise, it includes the prefix of index
	// columns constrained by LookupExpr.
	RightSideCols opt.ColList

	// LookupExpr is a lookup expression for multi-span lookup joins. It is used
	// when some index columns were constrained to multiple constant values or a
	// range expression, making it impossible to construct a lookup join with
	// KeyCols. LookupExpr is used to construct multiple lookup spans for each
	// input row at execution time.
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
	//
	// LookupExpr will be nil if KeyCols is non-nil.
	LookupExpr memo.FiltersExpr

	// InputProjections contains constant values and computed columns that must
	// be projected on the lookup join's input.
	InputProjections memo.ProjectionsExpr

	// AllLookupFilters contains equalities and other filters in either KeyCols or
	// LookupExpr that are used to aid selectivity estimation and logical props
	// calculation. See memo.LookupJoinPrivate.AllLookupFilters.
	AllLookupFilters memo.FiltersExpr

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
// table, as long as the join input and ON condition do not change.
func (b *ConstraintBuilder) Init(
	f *norm.Factory,
	md *opt.Metadata,
	evalCtx *eval.Context,
	table opt.TableID,
	leftCols, rightCols opt.ColSet,
) {
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
}

// Build returns a Constraint that constrains a lookup join on the given index.
// The constraint returned may be unconstrained if no constraint could be built.
// foundEqualityCols indicates whether any equality conditions were used to
// constrain the index columns; this can be used to decide whether to build a
// lookup join. `derivedFkOnFilters` is a set of extra equijoin predicates,
// derived from a foreign key constraint, to add to the explicit ON clause,
// but which should not be used in calculating join selectivity estimates.
func (b *ConstraintBuilder) Build(
	index cat.Index, onFilters, optionalFilters, derivedFkOnFilters memo.FiltersExpr,
) (_ Constraint, foundEqualityCols bool) {
	onFilters = append(onFilters, derivedFkOnFilters...)

	// Extract the equality columns from onFilters. We cannot use the results of
	// the extraction in Init because onFilters may be reduced by the caller
	// after Init due to partial index implication. If the filters are reduced,
	// eqFilterOrds calculated during Init would no longer be valid because the
	// ordinals of the filters will have changed.
	leftEq, rightEq, eqFilterOrds :=
		memo.ExtractJoinEqualityColumnsWithFilterOrds(b.leftCols, b.rightCols, onFilters)
	rightEqSet := rightEq.ToSet()

	// Retrieve the inequality columns from onFilters.
	var rightCmp opt.ColList
	var inequalityFilterOrds []int
	if b.evalCtx.SessionData().VariableInequalityLookupJoinEnabled {
		rightCmp, inequalityFilterOrds =
			memo.ExtractJoinInequalityRightColumnsWithFilterOrds(b.leftCols, b.rightCols, onFilters)
	}

	allFilters := append(onFilters, optionalFilters...)

	// Check if the first column in the index either:
	//
	//   1. Has an equality constraint.
	//   2. Is a computed column for which an equality constraint can be
	//      generated.
	//   3. Is constrained to a constant value or values.
	//   4. Has an inequality constraint between input and index columns.
	//
	// This check doesn't guarantee that we will find lookup join key
	// columns, but it avoids unnecessary work in most cases.
	firstIdxCol := b.table.IndexColumnID(index, 0)
	if _, ok := rightEq.Find(firstIdxCol); !ok {
		if _, ok := b.findComputedColJoinEquality(b.table, firstIdxCol, rightEqSet); !ok {
			if _, _, ok := FindJoinFilterConstants(allFilters, firstIdxCol, b.evalCtx); !ok {
				if _, ok := rightCmp.Find(firstIdxCol); !ok {
					return Constraint{}, false
				}
			}
		}
	}

	// Find the longest prefix of index key columns that are constrained by
	// an equality with another column or a constant.
	numIndexKeyCols := index.LaxKeyColumnCount()

	keyCols := make(opt.ColList, 0, numIndexKeyCols)
	var derivedEquivCols opt.ColSet
	// Don't change the selectivity estimate of this join vs. other joins which
	// don't use derivedFkOnFilters. Add column IDs from these filters to the set
	// of columns to ignore for join selectivity estimation. The alternative would
	// be to derive these filters for all joins, but it's not clear that this
	// would always be better given that some joins like hash join would have more
	// terms to evaluate. Also, that approach may cause selectivity
	// underestimation, so would require more effort to make sure correlations
	// between columns are accurately captured.
	for _, filtersItem := range derivedFkOnFilters {
		if eqExpr, ok := filtersItem.Condition.(*memo.EqExpr); ok {
			leftVariable, leftOk := eqExpr.Left.(*memo.VariableExpr)
			rightVariable, rightOk := eqExpr.Right.(*memo.VariableExpr)
			if leftOk && rightOk {
				derivedEquivCols.Add(leftVariable.Col)
				derivedEquivCols.Add(rightVariable.Col)
			}
		}
	}

	rightSideCols := make(opt.ColList, 0, numIndexKeyCols)
	var inputProjections memo.ProjectionsExpr
	var lookupExpr memo.FiltersExpr
	var allLookupFilters memo.FiltersExpr
	var filterOrdsToExclude intsets.Fast
	foundLookupCols := false
	lookupExprRequired := false
	remainingFilters := make(memo.FiltersExpr, 0, len(onFilters))

	// addEqualityColumns adds the given columns as an equality in keyCols if
	// lookupExprRequired is false. Otherwise, the equality is added as an
	// expression in lookupExpr. In both cases, rightCol is added to
	// rightSideCols so the caller of Build can determine if the right equality
	// columns form a key.
	addEqualityColumns := func(leftCol, rightCol opt.ColumnID) {
		if !lookupExprRequired {
			keyCols = append(keyCols, leftCol)
		} else {
			lookupExpr = append(lookupExpr, b.constructColEquality(leftCol, rightCol))
		}
		rightSideCols = append(rightSideCols, rightCol)
	}

	// convertToLookupExpr converts previously collected keyCols and
	// rightSideCols to equality expressions in lookupExpr. It is used when it
	// is discovered that a lookup expression is required to build a constraint,
	// and keyCols and rightSideCols have already been collected. After building
	// expressions, keyCols is reset to nil.
	convertToLookupExpr := func() {
		if lookupExprRequired {
			// Return early if we've already converted the key columns to a
			// lookup expression.
			return
		}
		lookupExprRequired = true
		for i := range keyCols {
			lookupExpr = append(lookupExpr, b.constructColEquality(keyCols[i], rightSideCols[i]))
		}
		keyCols = nil
	}

	// All the lookup conditions must apply to the prefix of the index and so
	// the projected columns created must be created in order.
	for j := 0; j < numIndexKeyCols; j++ {
		idxCol := b.table.IndexColumnID(index, j)
		idxColIsDesc := index.Column(j).Descending
		if eqIdx, ok := rightEq.Find(idxCol); ok {
			allLookupFilters = append(allLookupFilters, allFilters[eqFilterOrds[eqIdx]])
			addEqualityColumns(leftEq[eqIdx], idxCol)
			filterOrdsToExclude.Add(eqFilterOrds[eqIdx])
			foundEqualityCols = true
			foundLookupCols = true
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
			addEqualityColumns(compEqCol, idxCol)
			derivedEquivCols.Add(compEqCol)
			derivedEquivCols.Add(idxCol)
			foundEqualityCols = true
			foundLookupCols = true
			continue
		}

		// Try to find a filter that constrains this column to non-NULL
		// constant values. We cannot use a NULL value because the lookup
		// join implements logic equivalent to simple equality between
		// columns (where NULL never equals anything).
		foundVals, allIdx, ok := FindJoinFilterConstants(allFilters, idxCol, b.evalCtx)

		// If a single constant value was found, project it in the input
		// and use it as an equality column.
		if ok && len(foundVals) == 1 {
			idxColType := b.md.ColumnMeta(idxCol).Type
			constColID := b.md.AddColumn(
				fmt.Sprintf("lookup_join_const_col_@%d", idxCol),
				idxColType,
			)
			inputProjections = append(inputProjections, b.f.ConstructProjectionsItem(
				b.f.ConstructConstVal(foundVals[0], idxColType),
				constColID,
			))
			allLookupFilters = append(allLookupFilters, allFilters[allIdx])
			addEqualityColumns(constColID, idxCol)
			filterOrdsToExclude.Add(allIdx)
			continue
		}

		// If multiple constant values were found, we must use a lookup
		// expression.
		if ok {
			// Convert previously collected keyCols and rightSideCols to
			// expressions in lookupExpr and clear keyCols.
			convertToLookupExpr()

			valsFilter := allFilters[allIdx]
			if !isCanonicalFilter(valsFilter) {
				// Disable normalization rules when constructing the lookup
				// expression so that it does not get normalized into a
				// non-canonical expression.
				b.f.DisableOptimizationsTemporarily(func() {
					valsFilter = b.f.ConstructConstFilter(idxCol, foundVals)
				})
			}
			lookupExpr = append(lookupExpr, valsFilter)
			allLookupFilters = append(allLookupFilters, allFilters[allIdx])
			filterOrdsToExclude.Add(allIdx)
			continue
		}

		// If constant equality values were not found, try to find filters that
		// constrain this index column to a range on input columns.
		startIdx, endIdx, foundStart, foundEnd := b.findJoinVariableRangeFilters(
			rightCmp, inequalityFilterOrds, allFilters, idxCol, idxColIsDesc,
		)
		if foundStart {
			convertToLookupExpr()
			lookupExpr = append(lookupExpr, allFilters[startIdx])
			allLookupFilters = append(allLookupFilters, allFilters[startIdx])
			filterOrdsToExclude.Add(startIdx)
			foundLookupCols = true
		}
		if foundEnd {
			convertToLookupExpr()
			lookupExpr = append(lookupExpr, allFilters[endIdx])
			allLookupFilters = append(allLookupFilters, allFilters[endIdx])
			filterOrdsToExclude.Add(endIdx)
			foundLookupCols = true
		}
		if foundStart && foundEnd {
			// The column is constrained above and below by an inequality; no further
			// expressions can be added to the lookup.
			break
		}

		// If no variable range expressions were found, try to find a filter that
		// constrains this index column to a range on constant values. It may be the
		// case that only the start or end bound could be constrained with
		// an input column; in this case, it still may be possible to use a constant
		// to form the other bound.
		rangeFilter, remaining, filterIdx := b.findJoinConstantRangeFilter(
			allFilters, idxCol, idxColIsDesc, !foundStart, !foundEnd,
		)
		if rangeFilter != nil {
			// A constant range filter could be found.
			convertToLookupExpr()
			lookupExpr = append(lookupExpr, *rangeFilter)
			allLookupFilters = append(allLookupFilters, allFilters[filterIdx])
			filterOrdsToExclude.Add(filterIdx)
			if remaining != nil {
				remainingFilters = append(remainingFilters, *remaining)
			}
		}

		// Either a range was found, or the index column cannot be constrained.
		// In both cases, we cannot continue on to the next index column, so we
		// break out of the loop.
		break
	}

	// Lookup join constraints that contain no lookup columns (e.g., a lookup
	// expression x=1) are not useful.
	if !foundLookupCols {
		return Constraint{}, false
	}

	if len(keyCols) > 0 && len(lookupExpr) > 0 {
		panic(errors.AssertionFailedf("expected lookup constraint to have either KeyCols or LookupExpr, not both"))
	}

	c := Constraint{
		KeyCols:          keyCols,
		DerivedEquivCols: derivedEquivCols,
		RightSideCols:    rightSideCols,
		LookupExpr:       lookupExpr,
		InputProjections: inputProjections,
		AllLookupFilters: allLookupFilters,
	}

	// Reduce the remaining filters.
	for i := range onFilters {
		if !filterOrdsToExclude.Contains(i) {
			remainingFilters = append(remainingFilters, onFilters[i])
		}
	}
	c.RemainingFilters = remainingFilters

	return c, foundEqualityCols
}

// findComputedColJoinEquality returns the computed column expression of col and
// ok=true when a join equality constraint can be generated for the column. This
// is possible when:
//
//  1. col is non-nullable.
//  2. col is a computed column.
//  3. Columns referenced in the computed expression are a subset of columns
//     that already have equality constraints.
//
// For example, consider the table and query:
//
//	CREATE TABLE a (
//	  a INT
//	)
//
//	CREATE TABLE bc (
//	  b INT,
//	  c INT NOT NULL AS (b + 1) STORED
//	)
//
//	SELECT * FROM a JOIN b ON a = b
//
// We can add an equality constraint for c because c is a function of b and b
// has an equality constraint in the join predicate:
//
//	SELECT * FROM a JOIN b ON a = b AND a + 1 = c
//
// Condition (1) is required to prevent generating invalid equality constraints
// for computed column expressions that can evaluate to NULL even when the
// columns referenced in the expression are non-NULL. For example, consider the
// table and query:
//
//	CREATE TABLE a (
//	  a INT
//	)
//
//	CREATE TABLE bc (
//	  b INT,
//	  c INT AS (CASE WHEN b > 0 THEN NULL ELSE -1 END) STORED
//	)
//
//	SELECT a, b FROM a JOIN b ON a = b
//
// The following is an invalid transformation: a row such as (a=1, b=1) would no
// longer be returned because NULL=NULL is false.
//
//	SELECT a, b FROM a JOIN b ON a = b AND (CASE WHEN a > 0 THEN NULL ELSE -1 END) = c
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

// findJoinVariableRangeFilters attempts to find inequality constraints for the
// given index column that reference input columns (not constants). If either
// (or both) start and end bounds are found, findJoinVariableInequalityFilter
// returns the corresponding filter indices.
func (b *ConstraintBuilder) findJoinVariableRangeFilters(
	rightCmp opt.ColList,
	inequalityFilterOrds []int,
	filters memo.FiltersExpr,
	idxCol opt.ColumnID,
	idxColIsDesc bool,
) (startIdx, endIdx int, foundStart, foundEnd bool) {
	// Iterate through the extracted variable inequality filters to see if any
	// can be used to constrain the index column.
	for i := range rightCmp {
		if foundStart && foundEnd {
			break
		}
		if rightCmp[i] != idxCol {
			continue
		}
		cond := filters[inequalityFilterOrds[i]].Condition
		op := cond.Op()
		if cond.Child(0).(*memo.VariableExpr).Col != idxCol {
			// Normalize the condition so the index column is on the left side.
			op = opt.CommuteEqualityOrInequalityOp(op)
		}
		if idxColIsDesc && op == opt.LtOp {
			// We have to ensure that any value from this column can always be
			// advanced to the first value that orders immediately before it. This is
			// only possible for a subset of types. We have already ensured that both
			// sides of the inequality are of identical types, so it doesn't matter
			// which one we check here.
			typ := cond.Child(0).(*memo.VariableExpr).Typ
			switch typ.Family() {
			case types.BoolFamily, types.FloatFamily, types.INetFamily,
				types.IntFamily, types.OidFamily, types.TimeFamily, types.TimeTZFamily,
				types.TimestampFamily, types.TimestampTZFamily, types.UuidFamily:
			default:
				continue
			}
		}
		isStartBound := op == opt.GtOp || op == opt.GeOp
		if !foundStart && isStartBound {
			foundStart = true
			startIdx = inequalityFilterOrds[i]
		} else if !foundEnd && !isStartBound {
			foundEnd = true
			endIdx = inequalityFilterOrds[i]
		}
	}
	return startIdx, endIdx, foundStart, foundEnd
}

// findJoinConstantRangeFilter tries to find a constant inequality range for this
// column. If no such range filter can be found, rangeFilter is nil. If
// remaining is non-nil, it should be appended to the RemainingFilters field of
// the resulting Constraint. filterIdx is the index of the filter used to
// constrain the index column. needStart and needEnd indicate whether the index
// column's start and end bounds are still unconstrained respectively. At least
// one of needStart and needEnd must be true.
func (b *ConstraintBuilder) findJoinConstantRangeFilter(
	filters memo.FiltersExpr, col opt.ColumnID, idxColIsDesc, needStart, needEnd bool,
) (rangeFilter, remaining *memo.FiltersItem, filterIdx int) {
	for i := range filters {
		props := filters[i].ScalarProps()
		if props.TightConstraints && props.Constraints.Length() == 1 {
			constraintObj := props.Constraints.Constraint(0)
			constraintCol := constraintObj.Columns.Get(0)
			// Non-canonical filters aren't yet supported for range spans like
			// they are for const spans so filter those out here (const spans
			// from non-canonical filters can be turned into a canonical filter,
			// see makeConstFilter). We only support 1 span in the execution
			// engine so check that. Additionally, inequality filter constraints
			// should be constructed so that the column is ascending
			// (see buildSingleColumnConstraint in memo.constraint_builder.go), so we
			// can ignore the descending case.
			if constraintCol.ID() != col || constraintObj.Spans.Count() != 1 ||
				constraintCol.Descending() || !isCanonicalFilter(filters[i]) {
				continue
			}
			span := constraintObj.Spans.Get(0)

			var canUseFilter bool
			if needStart && !span.StartKey().IsEmpty() && !span.StartKey().IsNull() {
				canUseFilter = true
			}
			if needEnd && !span.EndKey().IsEmpty() && !span.EndKey().IsNull() {
				val := span.EndKey().Value(0)
				if span.EndBoundary() == constraint.ExcludeBoundary && idxColIsDesc {
					// If we have a datum for the end of a span and the index column is
					// DESC, we have to ensure that it can be "advanced" to the immediate
					// previous value if the corresponding span boundary is exclusive.
					//
					// This limitation comes from the execution that must be able to
					// "advance" the end boundary to the previous value in order to make
					// it inclusive. This operation cannot be directly performed on the
					// encoded key, so the Datum.Prev method is necessary here.
					if val.IsMin(b.evalCtx) {
						continue
					}
					if _, ok := val.Prev(b.evalCtx); !ok {
						continue
					}
				}
				canUseFilter = true
			}
			if !canUseFilter {
				continue
			}
			if (!needStart || !needEnd) && !span.StartKey().IsEmpty() && !span.EndKey().IsEmpty() &&
				!span.StartKey().IsNull() && !span.EndKey().IsNull() {
				// The filter supplies both start and end bounds, but we only need one
				// of them. Construct a new filter to be used in the lookup, and another
				// filter with the unused bound to be included in the ON condition. The
				// original filter should still be removed from the ON condition.
				//
				// We've already filtered cases where the column isn't constrained by a
				// single span, so we only need to consider start and end bounds.
				var startFilter, endFilter memo.FiltersItem
				startOp, endOp := opt.GtOp, opt.LtOp
				if span.StartBoundary() == constraint.IncludeBoundary {
					startOp = opt.GeOp
				}
				if span.EndBoundary() == constraint.IncludeBoundary {
					endOp = opt.LeOp
				}
				startDatum, endDatum := span.StartKey().Value(0), span.EndKey().Value(0)
				b.f.DisableOptimizationsTemporarily(func() {
					// Disable normalization rules when constructing the lookup expression
					// so that it does not get normalized into a non-canonical expression.
					indexVariable := b.f.ConstructVariable(col)
					startBound := b.f.ConstructConstVal(startDatum, startDatum.ResolvedType())
					endBound := b.f.ConstructConstVal(endDatum, endDatum.ResolvedType())
					startFilter = b.f.ConstructFiltersItem(
						b.f.DynamicConstruct(startOp, indexVariable, startBound).(opt.ScalarExpr),
					)
					endFilter = b.f.ConstructFiltersItem(
						b.f.DynamicConstruct(endOp, indexVariable, endBound).(opt.ScalarExpr),
					)
				})
				if !needStart {
					rangeFilter, remaining = &endFilter, &startFilter
				} else if !needEnd {
					rangeFilter, remaining = &startFilter, &endFilter
				}
			} else {
				// The filter can be used as-is in the lookup expression. No remaining
				// filter needs to be added to the ON condition.
				rangeFilter = &filters[i]
			}
			return rangeFilter, remaining, i
		}
	}
	return nil, nil, -1
}

// constructColEquality returns a FiltersItem representing equality between the
// given columns.
func (b *ConstraintBuilder) constructColEquality(leftCol, rightCol opt.ColumnID) memo.FiltersItem {
	var filters memo.FiltersItem
	// Disable normalization rules when constructing the lookup expression so
	// that it does not get normalized into a non-canonical expression.
	b.f.DisableOptimizationsTemporarily(func() {
		filters = b.f.ConstructFiltersItem(
			b.f.ConstructEq(
				b.f.ConstructVariable(leftCol),
				b.f.ConstructVariable(rightCol),
			),
		)
	})
	return filters
}

// isCanonicalFilter returns true for the limited set of expr's that are
// supported by the lookup joiner at execution time. Note that
// indexLookupJoinPerLookupCost in coster.go depends on the validation performed
// by this function, so changes made here should be reflected there.
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
