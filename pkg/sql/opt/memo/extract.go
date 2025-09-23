// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// This file contains various helper functions that extract useful information
// from expressions.

// CanExtractConstTuple returns true if the expression is a TupleOp with
// constant values (a nested tuple of constant values is considered constant).
func CanExtractConstTuple(e opt.Expr) bool {
	return e.Op() == opt.TupleOp && CanExtractConstDatum(e)
}

// CanExtractConstDatum returns true if a constant datum can be created from the
// given expression (tuples and arrays of constant values are considered
// constant values). If CanExtractConstDatum returns true, then
// ExtractConstDatum is guaranteed to work as well.
func CanExtractConstDatum(e opt.Expr) bool {
	if opt.IsConstValueOp(e) {
		return true
	}

	if tup, ok := e.(*TupleExpr); ok {
		for _, elem := range tup.Elems {
			if !CanExtractConstDatum(elem) {
				return false
			}
		}
		return true
	}

	if arr, ok := e.(*ArrayExpr); ok {
		for _, elem := range arr.Elems {
			if !CanExtractConstDatum(elem) {
				return false
			}
		}
		return true
	}

	return false
}

// ExtractConstDatum returns the Datum that represents the value of an
// expression with a constant value. An expression with a constant value is:
//   - one that has a ConstValue tag, or
//   - a tuple or array where all children are constant values.
func ExtractConstDatum(e opt.Expr) tree.Datum {
	switch t := e.(type) {
	case *NullExpr:
		return tree.DNull

	case *TrueExpr:
		return tree.DBoolTrue

	case *FalseExpr:
		return tree.DBoolFalse

	case *ConstExpr:
		return t.Value

	case *TupleExpr:
		datums := make(tree.Datums, len(t.Elems))
		for i := range datums {
			datums[i] = ExtractConstDatum(t.Elems[i])
		}
		return tree.NewDTuple(t.Typ, datums...)

	case *ArrayExpr:
		elementType := t.Typ.ArrayContents()
		elements := make(tree.Datums, len(t.Elems))
		for i := range elements {
			elements[i] = ExtractConstDatum(t.Elems[i])
		}
		return tree.NewDArrayFromDatums(elementType, elements)
	}
	panic(errors.AssertionFailedf("non-const expression: %+v", e))
}

// ExtractAggFunc digs down into the given aggregate expression and returns the
// aggregate function, skipping past any AggFilter or AggDistinct operators.
func ExtractAggFunc(e opt.ScalarExpr) opt.ScalarExpr {
	if filter, ok := e.(*AggFilterExpr); ok {
		e = filter.Input
	}

	if distinct, ok := e.(*AggDistinctExpr); ok {
		e = distinct.Input
	}

	if !opt.IsAggregateOp(e) {
		panic(errors.AssertionFailedf("not an Aggregate"))
	}

	return e
}

// ExtractAggInputColumns returns the set of columns the aggregate depends on.
func ExtractAggInputColumns(e opt.ScalarExpr) opt.ColSet {
	var res opt.ColSet
	if filter, ok := e.(*AggFilterExpr); ok {
		res.Add(filter.Filter.(*VariableExpr).Col)
		e = filter.Input
	}

	if distinct, ok := e.(*AggDistinctExpr); ok {
		e = distinct.Input
	}

	if !opt.IsAggregateOp(e) {
		panic(errors.AssertionFailedf("not an Aggregate"))
	}

	for i, n := 0, e.ChildCount(); i < n; i++ {
		if variable, ok := e.Child(i).(*VariableExpr); ok {
			res.Add(variable.Col)
		}
	}

	return res
}

// AddAggInputColumns adds the set of columns the aggregate depands on to the
// given set. The modified set is returned.
//
// NOTE: The original set may be mutated.
func AddAggInputColumns(cols opt.ColSet, e opt.ScalarExpr) opt.ColSet {
	if filter, ok := e.(*AggFilterExpr); ok {
		cols.Add(filter.Filter.(*VariableExpr).Col)
		e = filter.Input
	}

	if distinct, ok := e.(*AggDistinctExpr); ok {
		e = distinct.Input
	}

	if !opt.IsAggregateOp(e) {
		panic(errors.AssertionFailedf("not an Aggregate"))
	}

	for i, n := 0, e.ChildCount(); i < n; i++ {
		if variable, ok := e.Child(i).(*VariableExpr); ok {
			cols.Add(variable.Col)
		}
	}

	return cols
}

// ExtractAggFirstVar is given an aggregate expression and returns the Variable
// expression for the first argument, skipping past modifiers like AggDistinct.
func ExtractAggFirstVar(e opt.ScalarExpr) *VariableExpr {
	e = ExtractAggFunc(e)
	if e.ChildCount() == 0 {
		panic(errors.AssertionFailedf("aggregate does not have any arguments"))
	}

	if variable, ok := e.Child(0).(*VariableExpr); ok {
		return variable
	}

	panic(errors.AssertionFailedf("first aggregate input is not a Variable"))
}

// HasJoinCondition returns true if the given on filters contain at least one
// join conditions between a column in leftCols and a column in rightCols. If
// inequality is true, the join condition may be an inequality, otherwise the
// join condition must be an equality.
func HasJoinCondition(leftCols, rightCols opt.ColSet, on FiltersExpr, inequality bool) bool {
	for i := range on {
		condition := on[i].Condition
		if ok, _, _ := ExtractJoinCondition(leftCols, rightCols, condition, inequality); ok {
			return true
		}
	}
	return false
}

// ExtractJoinConditionFilterOrds returns the join conditions within on filters
// as a set of ordinals into the on filters. If inequality is true, the join
// conditions may be inequalities, otherwise the join conditions must be an
// equalities.
func ExtractJoinConditionFilterOrds(
	leftCols, rightCols opt.ColSet, on FiltersExpr, inequality bool,
) (filterOrds intsets.Fast) {
	var seenCols opt.ColSet
	for i := range on {
		condition := on[i].Condition
		ok, left, right := ExtractJoinCondition(leftCols, rightCols, condition, inequality)
		if !ok {
			continue
		}
		if seenCols.Contains(left) || seenCols.Contains(right) {
			// Don't allow any column to show up twice.
			// TODO(radu): need to figure out the right thing to do in cases
			//  like: left.a = right.a AND left.a = right.b
			continue
		}
		seenCols.Add(left)
		seenCols.Add(right)
		filterOrds.Add(i)
	}
	return filterOrds
}

// ExtractJoinEqualityColumns returns pairs of columns (one from the left side,
// one from the right side) which are constrained to be equal in a join (and
// have equivalent types).
func ExtractJoinEqualityColumns(
	leftCols, rightCols opt.ColSet, on FiltersExpr,
) (leftEq, rightEq opt.ColList) {
	leftEq, rightEq, _ = extractJoinConditionImpl(
		leftCols, rightCols, on, false, /* inequality */
		joinExtractLeftCols|joinExtractRightCols,
	)
	return leftEq, rightEq
}

// ExtractJoinEqualityLeftColumns returns the columns from leftCols which are
// constrained to be equal to columns in rightCols in a join (and have
// equivalent types).
func ExtractJoinEqualityLeftColumns(
	leftCols, rightCols opt.ColSet, on FiltersExpr,
) (leftEq opt.ColList) {
	leftEq, _, _ = extractJoinConditionImpl(
		leftCols, rightCols, on, false, /* inequality */
		joinExtractLeftCols,
	)
	return leftEq
}

// ExtractJoinEqualityColumnsWithFilterOrds returns pairs of columns (one from
// the left side, one from the right side) which are constrained to be equal in
// a join (and have equivalent types). The returned filterOrds contains ordinals
// of the on filters where each column pair was found.
func ExtractJoinEqualityColumnsWithFilterOrds(
	leftCols, rightCols opt.ColSet, on FiltersExpr,
) (leftEq, rightEq opt.ColList, filterOrds []int) {
	leftEq, rightEq, filterOrds = extractJoinConditionImpl(
		leftCols, rightCols, on, false, /* inequality */
		joinExtractLeftCols|joinExtractRightCols|joinExtractFilterOrds,
	)
	return leftEq, rightEq, filterOrds
}

// ExtractJoinInequalityRightColumnsWithFilterOrds returns columns from
// rightCols which are constrained by an inequality in a join (and have
// equivalent types) with columns in leftCols. The returned filterOrds contains
// ordinals of the on filters where each join condition was found.
func ExtractJoinInequalityRightColumnsWithFilterOrds(
	leftCols, rightCols opt.ColSet, on FiltersExpr,
) (rightCmp opt.ColList, filterOrds []int) {
	_, rightCmp, filterOrds = extractJoinConditionImpl(
		leftCols, rightCols, on, true, /* inequality */
		joinExtractRightCols|joinExtractFilterOrds,
	)
	return rightCmp, filterOrds
}

// joinExtractFlags are used to request specific values to extract from join
// conditions in extractJoinConditionImpl.
type joinExtractFlags uint8

const (
	// Extract the left columns from join conditions.
	joinExtractLeftCols joinExtractFlags = 1 << iota
	// Extract the right columns from join conditions.
	joinExtractRightCols
	// Extract the filter ordinals from join conditions.
	joinExtractFilterOrds
)

// extractJoinConditionImpl implements the core logic for other join condition
// extraction functions. It returns the left and right columns that have join
// conditions in the given on filters, and the ordinals of those conditions
// within the filters. It only allocates slices for the values request in flags.
// If a return value is not requested in flags, nil will be returned.
func extractJoinConditionImpl(
	leftCols, rightCols opt.ColSet, on FiltersExpr, inequality bool, flags joinExtractFlags,
) (leftCmp, rightCmp opt.ColList, filterOrds []int) {
	joinCondOrds := ExtractJoinConditionFilterOrds(leftCols, rightCols, on, inequality)
	n := joinCondOrds.Len()
	if flags&joinExtractLeftCols != 0 {
		leftCmp = make(opt.ColList, 0, n)
	}
	if flags&joinExtractRightCols != 0 {
		rightCmp = make(opt.ColList, 0, n)
	}
	if flags&joinExtractFilterOrds != 0 {
		filterOrds = make([]int, 0, n)
	}
	for i, ok := joinCondOrds.Next(0); ok; i, ok = joinCondOrds.Next(i + 1) {
		condition := on[i].Condition
		ok, left, right := ExtractJoinCondition(leftCols, rightCols, condition, inequality)
		if !ok {
			panic(errors.AssertionFailedf("expected on[%d] to be a join condition", i))
		}
		if leftCmp != nil {
			leftCmp = append(leftCmp, left)
		}
		if rightCmp != nil {
			rightCmp = append(rightCmp, right)
		}
		if filterOrds != nil {
			filterOrds = append(filterOrds, i)
		}
	}
	return leftCmp, rightCmp, filterOrds
}

// ExtractJoinEqualityFilters returns the filters containing pairs of columns
// (one from the left side, one from the right side) which are constrained to
// be equal in a join (and have equivalent types).
func ExtractJoinEqualityFilters(leftCols, rightCols opt.ColSet, on FiltersExpr) FiltersExpr {
	// We want to avoid allocating a new slice unless strictly necessary.
	var newFilters FiltersExpr
	for i := range on {
		condition := on[i].Condition
		ok, _, _ := ExtractJoinEquality(leftCols, rightCols, condition)
		if ok {
			if newFilters != nil {
				newFilters = append(newFilters, on[i])
			}
		} else {
			if newFilters == nil {
				newFilters = make(FiltersExpr, i, len(on)-1)
				copy(newFilters, on[:i])
			}
		}
	}
	if newFilters != nil {
		return newFilters
	}
	return on
}

func isVarEqualityOrInequality(
	condition opt.ScalarExpr, inequality bool,
) (leftVar, rightVar *VariableExpr, ok bool) {
	switch condition.Op() {
	case opt.EqOp:
		if inequality {
			return nil, nil, false
		}
	case opt.LtOp, opt.LeOp, opt.GtOp, opt.GeOp:
		if !inequality {
			return nil, nil, false
		}
	default:
		return nil, nil, false
	}
	leftVar, leftOk := condition.Child(0).(*VariableExpr)
	rightVar, rightOk := condition.Child(1).(*VariableExpr)
	return leftVar, rightVar, leftOk && rightOk
}

// ExtractJoinEquality restricts ExtractJoinCondition to only allow equalities.
func ExtractJoinEquality(
	leftCols, rightCols opt.ColSet, condition opt.ScalarExpr,
) (ok bool, left, right opt.ColumnID) {
	return ExtractJoinCondition(leftCols, rightCols, condition, false /* inequality */)
}

// ExtractJoinCondition returns true if the given condition is a simple equality
// or inequality condition with two variables (e.g. a=b), where one of the
// variables (returned as "left") is in the set of leftCols and the other
// (returned as "right") is in the set of rightCols. inequality is used to
// indicate whether the condition should be an inequality (e.g. < or >).
func ExtractJoinCondition(
	leftCols, rightCols opt.ColSet, condition opt.ScalarExpr, inequality bool,
) (ok bool, left, right opt.ColumnID) {
	lvar, rvar, ok := isVarEqualityOrInequality(condition, inequality)
	if !ok {
		return false, 0, 0
	}

	// Don't allow mixed types (see #22519).
	if !lvar.DataType().Equivalent(rvar.DataType()) {
		return false, 0, 0
	}

	if leftCols.Contains(lvar.Col) && rightCols.Contains(rvar.Col) {
		return true, lvar.Col, rvar.Col
	}
	if leftCols.Contains(rvar.Col) && rightCols.Contains(lvar.Col) {
		return true, rvar.Col, lvar.Col
	}

	return false, 0, 0
}

// ExtractRemainingJoinFilters calculates the remaining ON condition after
// removing equalities that are handled separately. The given function
// determines if an equality is redundant. The result is empty if there are no
// remaining conditions. Panics if leftEq and rightEq are not the same length.
func ExtractRemainingJoinFilters(on FiltersExpr, leftEq, rightEq opt.ColList) FiltersExpr {
	if len(leftEq) != len(rightEq) {
		panic(errors.AssertionFailedf("leftEq and rightEq have different lengths"))
	}
	if len(leftEq) == 0 {
		return on
	}
	var newFilters FiltersExpr
	for i := range on {
		leftVar, rightVar, ok := isVarEqualityOrInequality(on[i].Condition, false /* inequality */)
		if ok {
			a, b := leftVar.Col, rightVar.Col
			found := false
			for j := range leftEq {
				if (a == leftEq[j] && b == rightEq[j]) ||
					(a == rightEq[j] && b == leftEq[j]) {
					found = true
					break
				}
			}
			if found {
				// Skip this condition.
				continue
			}
		}
		if newFilters == nil {
			newFilters = make(FiltersExpr, 0, len(on)-i)
		}
		newFilters = append(newFilters, on[i])
	}
	return newFilters
}

// ExtractConstColumns returns columns in the filters expression that have been
// constrained to fixed values.
func ExtractConstColumns(
	ctx context.Context, on FiltersExpr, evalCtx *eval.Context,
) (fixedCols opt.ColSet) {
	for i := range on {
		scalar := on[i]
		scalarProps := scalar.ScalarProps()
		if scalarProps.Constraints != nil && !scalarProps.Constraints.IsUnconstrained() {
			fixedCols.UnionWith(scalarProps.Constraints.ExtractConstCols(ctx, evalCtx))
		}
	}
	return fixedCols
}

// ExtractValueForConstColumn returns the constant value of a column returned by
// ExtractConstColumns.
func ExtractValueForConstColumn(
	ctx context.Context, on FiltersExpr, evalCtx *eval.Context, col opt.ColumnID,
) tree.Datum {
	for i := range on {
		scalar := on[i]
		scalarProps := scalar.ScalarProps()
		if scalarProps.Constraints != nil {
			if val := scalarProps.Constraints.ExtractValueForConstCol(ctx, evalCtx, col); val != nil {
				return val
			}
		}
	}
	return nil
}

// ExtractTailCalls traverses the given expression tree, searching for routines
// that are in tail-call position relative to the (assumed) calling routine.
// ExtractTailCalls assumes that the given expression is the last body statement
// of the calling routine, and that the map is already initialized.
//
// In order for a nested routine to qualify as a tail-call, the following
// condition must be true: If the nested routine is evaluated, then the calling
// routine must return the result of the nested routine without further
// modification. This means even simple expressions like CAST are not allowed.
//
// ExtractTailCalls is best-effort, but is sufficient to identify the tail-calls
// produced among PL/pgSQL sub-routines.
//
// NOTE: ExtractTailCalls does not take into account whether the calling routine
// has an exception handler. The execution engine must take this into account
// before applying tail-call optimization.
func ExtractTailCalls(expr opt.Expr, tailCalls map[opt.ScalarExpr]struct{}) {
	switch t := expr.(type) {
	case *ProjectExpr:
		// * The cardinality cannot be greater than one: Otherwise, a nested routine
		// will be evaluated more than once, and all evaluations other than the last
		// are not tail-calls.
		//
		// * There must be a single projection: the execution does not provide
		// guarantees about order of evaluation for projections (though it may in
		// the future).
		//
		// * The passthrough set must be empty: Otherwise, the result of the nested
		// routine cannot directly be used as the result of the calling routine.
		//
		// * No routine in the input of the project can be a tail-call, since the
		// Project will perform work after the nested routine evaluates.
		// Note: this condition is enforced by simply not calling ExtractTailCalls
		// on the input of the Project.
		if t.Relational().Cardinality.IsZeroOrOne() &&
			len(t.Projections) == 1 && t.Passthrough.Empty() {
			ExtractTailCalls(t.Projections[0].Element, tailCalls)
		}

	case *ValuesExpr:
		// Allow only the case where the Values expression contains only a single
		// expression. Note: it may be possible to make an explicit guarantee that
		// expressions in a row are evaluated in order, in which case it would be
		// sufficient to ensure that the nested routine is in the last column.
		if len(t.Rows) == 1 && len(t.Rows[0].(*TupleExpr).Elems) == 1 {
			ExtractTailCalls(t.Rows[0].(*TupleExpr).Elems[0], tailCalls)
		}

	case *CaseExpr:
		// Case expressions guarantee that exactly one branch is evaluated, and pass
		// through the result of the chosen branch. Therefore, a routine within a
		// CASE branch can be a tail-call.
		for i := range t.Whens {
			ExtractTailCalls(t.Whens[i].(*WhenExpr).Value, tailCalls)
		}
		ExtractTailCalls(t.OrElse, tailCalls)

	case *SubqueryExpr:
		// A subquery within a routine is lazily evaluated as a nested routine.
		// Therefore, we consider it a tail call.
		tailCalls[t] = struct{}{}

	case *UDFCallExpr:
		// If we reached a scalar UDFCall expression, it is a tail call.
		if !t.Def.SetReturning {
			tailCalls[t] = struct{}{}
		}
	}
}

// ExtractExactScalarExprsForColumns extracts scalar expressions that tightly
// constrain the given columns from the given filters. Unlike constraint
// builder, this function:
//   - constrains to scalar expressions instead of constants
//   - always constrains tightly
//   - in fact, always constrains _exactly_ (that is, to scalar expressions that
//     produce not only equivalent values, but _exactly_ the same bytes)
//
// If a filter does not exactly constrain one of the given columns, or
// references more than one of the given columns, the filter will be added to
// remainingFilters. ExtractExactScalarExprsForColumns returns the mapping from
// constrained columns to scalar expressions, the remaining filters, and a
// boolean indicating whether all columns were constrained.
func ExtractExactScalarExprsForColumns(
	cols opt.ColSet,
	filters FiltersExpr,
	evalCtx *eval.Context,
	md *opt.Metadata,
	constructIsNotNull func(opt.ColumnID) FiltersItem,
	constructFiltersItem func(opt.ScalarExpr) FiltersItem,
) (constraints map[opt.ColumnID]opt.ScalarExpr, remainingFilters FiltersExpr, ok bool) {
	constraints = make(map[opt.ColumnID]opt.ScalarExpr)

	// useExprAsConstraint adds the scalar expression to the constraints map.
	useExprAsConstraint := func(col opt.ColumnID, e opt.ScalarExpr) bool {
		// Filters on other columns become remainingFilters.
		if !cols.Contains(col) {
			return false
		}
		// Only use the first constraining filter found. If there is already a
		// constraining filter, any additional filters become part of
		// remainingFilters.
		if _, ok := constraints[col]; ok {
			return false
		}
		// In the general case, the = operator (SQL equivalence) does not imply
		// exactly the same bytes. The scalar expression is exactly the same when:
		// - the column does not have composite type
		// - the scalar type is identical to the column type OR the scalar is NULL
		colType := md.ColumnMeta(col).Type
		if colinfo.CanHaveCompositeKeyEncoding(colType) {
			// TODO(michae2): If ExtractConstDatum(e) returns a non-composite datum,
			// we could still constrain using this expression.
			return false
		}
		if !e.DataType().Identical(colType) && e.DataType().Family() != types.UnknownFamily {
			// TODO(michae2): Explore whether we can use identical canonical types
			// here. Also add support for tuples.
			return false
		}
		// If this expression references other columns we're trying to constrain, it
		// will have to be a remainingFilter.
		var sharedProps props.Shared
		BuildSharedProps(e, &sharedProps, evalCtx)
		if sharedProps.OuterCols.Intersects(cols) {
			return false
		}
		constraints[col] = e
		return true
	}

	// constrainColsInExpr walks a filter predicate, adding scalar expressions for
	// any columns constrained to a single value by the predicate.
	var constrainColsInExpr func(opt.ScalarExpr)
	constrainColsInExpr = func(e opt.ScalarExpr) {
		switch t := e.(type) {
		case *VariableExpr:
			// `WHERE col` will be true if col is a boolean-typed column that has the
			// value True.
			if md.ColumnMeta(t.Col).Type.Family() == types.BoolFamily {
				if useExprAsConstraint(t.Col, TrueSingleton) {
					return
				}
			}
		case *NotExpr:
			// `WHERE NOT col` will be true if col is a boolean-typed column that has
			// the value False.
			if v, ok := t.Input.(*VariableExpr); ok {
				if md.ColumnMeta(v.Col).Type.Family() == types.BoolFamily {
					if useExprAsConstraint(v.Col, FalseSingleton) {
						return
					}
				}
			}
		case *AndExpr:
			constrainColsInExpr(t.Left)
			constrainColsInExpr(t.Right)
			return
		case *EqExpr:
			if v, ok := t.Left.(*VariableExpr); ok {
				// `WHERE col = <expr>` will be true if col has the value <expr> and
				// <expr> IS NOT NULL.
				if useExprAsConstraint(v.Col, t.Right) {
					// Because NULL = NULL evaluates to NULL, rather than true, we must
					// also guard against the scalar expression evaluating to NULL.
					remainingFilters = append(remainingFilters, constructIsNotNull(v.Col))
					return
				}
			}
			// TODO(michae2): Handle tuple of variables on LHS of EqExpr.
		case *IsExpr:
			if v, ok := t.Left.(*VariableExpr); ok {
				// `WHERE col IS NOT DISTINCT FROM <expr>` will be true if col has the
				// value <expr>.
				if useExprAsConstraint(v.Col, t.Right) {
					return
				}
			}
			// TODO(michae2): Handle tuple of variables on LHS of IsExpr.
		}
		// TODO(michae2): Add case for *InExpr.

		// If the filter did not constrain a column, add it to remainingFilters.
		remainingFilters = append(remainingFilters, constructFiltersItem(e))
	}

	for i := range filters {
		constrainColsInExpr(filters[i].Condition)
	}

	return constraints, remainingFilters, len(constraints) == cols.Len()
}
