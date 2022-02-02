// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package invertedidx

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewDatumsToInvertedExpr returns a new DatumsToInvertedExpr. Currently there
// is only one possible implementation returned, geoDatumsToInvertedExpr.
func NewDatumsToInvertedExpr(
	evalCtx *tree.EvalContext, colTypes []*types.T, expr tree.TypedExpr, idx catalog.Index,
) (invertedexpr.DatumsToInvertedExpr, error) {
	geoConfig := idx.GetGeoConfig()
	if !geoindex.IsEmptyConfig(&geoConfig) {
		return NewGeoDatumsToInvertedExpr(evalCtx, colTypes, expr, &geoConfig)
	}

	return NewJSONOrArrayDatumsToInvertedExpr(evalCtx, colTypes, expr)
}

// NewBoundPreFilterer returns a PreFilterer for the given expr where the type
// of the bound param is specified by typ. Unlike the use of PreFilterer in an
// inverted join, where each left value is bound, this function is for the
// invertedFilterer where the param to be bound is already specified as a
// constant in the expr. The callee will bind this parameter and return the
// opaque pre-filtering state for that binding (the interface{}) in the return
// values).
func NewBoundPreFilterer(typ *types.T, expr tree.TypedExpr) (*PreFilterer, interface{}, error) {
	if !typ.Equivalent(types.Geometry) && !typ.Equivalent(types.Geography) {
		return nil, nil, fmt.Errorf("pre-filtering not supported for type %s", typ)
	}
	return newGeoBoundPreFilterer(typ, expr)
}

// TryFilterInvertedIndex tries to derive an inverted filter condition for
// the given inverted index from the specified filters. If an inverted filter
// condition is derived, it is returned with ok=true. If no condition can be
// derived, then TryFilterInvertedIndex returns ok=false.
//
// In addition to the inverted filter condition (spanExpr), returns:
// - a constraint of the prefix columns if there are any,
// - remaining filters that must be applied if the span expression is not tight,
//   and
// - pre-filterer state that can be used by the invertedFilterer operator to
//   reduce the number of false positives returned by the span expression.
func TryFilterInvertedIndex(
	evalCtx *tree.EvalContext,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	computedColumns map[opt.ColumnID]opt.ScalarExpr,
) (
	spanExpr *inverted.SpanExpression,
	constraint *constraint.Constraint,
	remainingFilters memo.FiltersExpr,
	preFiltererState *invertedexpr.PreFiltererStateForInvertedFilterer,
	ok bool,
) {
	// Attempt to constrain the prefix columns, if there are any. If they cannot
	// be constrained to single values, the index cannot be used.
	constraint, filters, ok = constrainPrefixColumns(
		evalCtx, factory, filters, optionalFilters, tabID, index,
	)
	if !ok {
		return nil, nil, nil, nil, false
	}

	config := index.GeoConfig()
	var typ *types.T
	var filterPlanner invertedFilterPlanner
	if geoindex.IsGeographyConfig(config) {
		filterPlanner = &geoFilterPlanner{
			factory:     factory,
			tabID:       tabID,
			index:       index,
			getSpanExpr: getSpanExprForGeographyIndex,
		}
		typ = types.Geography
	} else if geoindex.IsGeometryConfig(config) {
		filterPlanner = &geoFilterPlanner{
			factory:     factory,
			tabID:       tabID,
			index:       index,
			getSpanExpr: getSpanExprForGeometryIndex,
		}
		typ = types.Geometry
	} else {
		filterPlanner = &jsonOrArrayFilterPlanner{
			tabID:           tabID,
			index:           index,
			computedColumns: computedColumns,
		}
		col := index.InvertedColumn().InvertedSourceColumnOrdinal()
		typ = factory.Metadata().Table(tabID).Column(col).DatumType()
	}

	var invertedExpr inverted.Expression
	var pfState *invertedexpr.PreFiltererStateForInvertedFilterer
	for i := range filters {
		invertedExprLocal, remFiltersLocal, pfStateLocal := extractInvertedFilterCondition(
			evalCtx, factory, filters[i].Condition, filterPlanner,
		)
		if invertedExpr == nil {
			invertedExpr = invertedExprLocal
			pfState = pfStateLocal
		} else {
			invertedExpr = inverted.And(invertedExpr, invertedExprLocal)
			// Do pre-filtering using the first of the conjuncts that provided
			// non-nil pre-filtering state.
			if pfState == nil {
				pfState = pfStateLocal
			}
		}
		if remFiltersLocal != nil {
			remainingFilters = append(remainingFilters, factory.ConstructFiltersItem(remFiltersLocal))
		}
	}

	if invertedExpr == nil {
		return nil, nil, nil, nil, false
	}

	spanExpr, ok = invertedExpr.(*inverted.SpanExpression)
	if !ok {
		return nil, nil, nil, nil, false
	}
	if pfState != nil {
		pfState.Typ = typ
	}

	return spanExpr, constraint, remainingFilters, pfState, true
}

// TryJoinInvertedIndex tries to create an inverted join with the given input
// and inverted index from the specified filters. If a join is created, the
// inverted join condition is returned. If no join can be created, then
// TryJoinInvertedIndex returns nil.
func TryJoinInvertedIndex(
	ctx context.Context,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	inputCols opt.ColSet,
) opt.ScalarExpr {
	if !index.IsInverted() {
		return nil
	}

	config := index.GeoConfig()
	var joinPlanner invertedJoinPlanner
	if geoindex.IsGeographyConfig(config) {
		joinPlanner = &geoJoinPlanner{
			factory:     factory,
			tabID:       tabID,
			index:       index,
			inputCols:   inputCols,
			getSpanExpr: getSpanExprForGeographyIndex,
		}
	} else if geoindex.IsGeometryConfig(config) {
		joinPlanner = &geoJoinPlanner{
			factory:     factory,
			tabID:       tabID,
			index:       index,
			inputCols:   inputCols,
			getSpanExpr: getSpanExprForGeometryIndex,
		}
	} else {
		joinPlanner = &jsonOrArrayJoinPlanner{
			factory:   factory,
			tabID:     tabID,
			index:     index,
			inputCols: inputCols,
		}
	}

	var invertedExpr opt.ScalarExpr
	for i := range filters {
		invertedExprLocal := extractInvertedJoinCondition(
			ctx, factory, filters[i].Condition, joinPlanner,
		)
		if invertedExprLocal == nil {
			continue
		}
		if invertedExpr == nil {
			invertedExpr = invertedExprLocal
		} else {
			invertedExpr = factory.ConstructAnd(invertedExpr, invertedExprLocal)
		}
	}

	if invertedExpr == nil {
		return nil
	}

	// The resulting expression must contain at least one column from the input.
	var p props.Shared
	memo.BuildSharedProps(invertedExpr, &p, factory.EvalContext())
	if !p.OuterCols.Intersects(inputCols) {
		return nil
	}

	return invertedExpr
}

type invertedJoinPlanner interface {
	// extractInvertedJoinConditionFromLeaf extracts a join condition from the
	// given expression, which represents a leaf of an expression tree in which
	// the internal nodes are And and/or Or expressions. Returns nil if no join
	// condition could be extracted.
	extractInvertedJoinConditionFromLeaf(ctx context.Context, expr opt.ScalarExpr) opt.ScalarExpr
}

// extractInvertedJoinCondition extracts a scalar expression from the given
// filter condition, where the scalar expression represents a join condition
// between the input columns and inverted index. Returns nil if no join
// condition could be extracted.
//
// The filter condition should be an expression tree of And, Or, and leaf
// expressions. Extraction of the join condition from the leaves is delegated
// to the given invertedJoinPlanner.
func extractInvertedJoinCondition(
	ctx context.Context,
	factory *norm.Factory,
	filterCond opt.ScalarExpr,
	joinPlanner invertedJoinPlanner,
) opt.ScalarExpr {
	switch t := filterCond.(type) {
	case *memo.AndExpr:
		leftExpr := extractInvertedJoinCondition(ctx, factory, t.Left, joinPlanner)
		rightExpr := extractInvertedJoinCondition(ctx, factory, t.Right, joinPlanner)
		if leftExpr == nil {
			return rightExpr
		}
		if rightExpr == nil {
			return leftExpr
		}
		return factory.ConstructAnd(leftExpr, rightExpr)

	case *memo.OrExpr:
		leftExpr := extractInvertedJoinCondition(ctx, factory, t.Left, joinPlanner)
		rightExpr := extractInvertedJoinCondition(ctx, factory, t.Right, joinPlanner)
		if leftExpr == nil || rightExpr == nil {
			return nil
		}
		return factory.ConstructOr(leftExpr, rightExpr)

	default:
		return joinPlanner.extractInvertedJoinConditionFromLeaf(ctx, filterCond)
	}
}

// getInvertedExpr takes a TypedExpr tree consisting of And, Or and leaf
// expressions, and constructs a new TypedExpr tree in which the leaves are
// replaced by the given getInvertedExprLeaf function.
func getInvertedExpr(
	expr tree.TypedExpr, getInvertedExprLeaf func(expr tree.TypedExpr) (tree.TypedExpr, error),
) (tree.TypedExpr, error) {
	switch t := expr.(type) {
	case *tree.AndExpr:
		leftExpr, err := getInvertedExpr(t.TypedLeft(), getInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		rightExpr, err := getInvertedExpr(t.TypedRight(), getInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		return tree.NewTypedAndExpr(leftExpr, rightExpr), nil

	case *tree.OrExpr:
		leftExpr, err := getInvertedExpr(t.TypedLeft(), getInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		rightExpr, err := getInvertedExpr(t.TypedRight(), getInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		return tree.NewTypedOrExpr(leftExpr, rightExpr), nil

	default:
		return getInvertedExprLeaf(expr)
	}
}

// evalInvertedExpr evaluates a TypedExpr tree consisting of And, Or and leaf
// expressions, and returns the resulting inverted.Expression. Delegates
// evaluation of leaf expressions to the given evalInvertedExprLeaf function.
func evalInvertedExpr(
	expr tree.TypedExpr, evalInvertedExprLeaf func(expr tree.TypedExpr) (inverted.Expression, error),
) (inverted.Expression, error) {
	switch t := expr.(type) {
	case *tree.AndExpr:
		leftExpr, err := evalInvertedExpr(t.TypedLeft(), evalInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		rightExpr, err := evalInvertedExpr(t.TypedRight(), evalInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		if leftExpr == nil || rightExpr == nil {
			return nil, nil
		}
		return inverted.And(leftExpr, rightExpr), nil

	case *tree.OrExpr:
		leftExpr, err := evalInvertedExpr(t.TypedLeft(), evalInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		rightExpr, err := evalInvertedExpr(t.TypedRight(), evalInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		if leftExpr == nil {
			return rightExpr, nil
		}
		if rightExpr == nil {
			return leftExpr, nil
		}
		return inverted.Or(leftExpr, rightExpr), nil

	default:
		return evalInvertedExprLeaf(expr)
	}
}

// constrainPrefixColumns attempts to build a constraint for the non-inverted
// prefix columns of the given index. If a constraint is successfully built, it
// is returned along with remaining filters and ok=true. The function is only
// successful if it can generate a constraint where all spans have the same
// start and end keys for all non-inverted prefix columns. This is required for
// building spans for scanning multi-column inverted indexes (see
// span.Builder.SpansFromInvertedSpans).
//
// If the index is a single-column inverted index, there are no prefix columns
// to constrain, and ok=true is returned.
func constrainPrefixColumns(
	evalCtx *tree.EvalContext,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
) (constraint *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	tabMeta := factory.Metadata().TableMeta(tabID)
	prefixColumnCount := index.NonInvertedPrefixColumnCount()
	ps, _ := tabMeta.IndexPartitionLocality(index.Ordinal(), index, evalCtx)

	// If this is a single-column inverted index, there are no prefix columns to
	// constrain.
	if prefixColumnCount == 0 {
		return nil, filters, true
	}

	prefixColumns := make([]opt.OrderingColumn, prefixColumnCount)
	var notNullCols opt.ColSet
	for i := range prefixColumns {
		col := index.Column(i)
		colID := tabID.ColumnID(col.Ordinal())
		prefixColumns[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !col.IsNullable() {
			notNullCols.Add(colID)
		}
	}

	// Consolidation of a constraint converts contiguous spans into a single
	// span. By definition, the consolidated span would have different start and
	// end keys and could not be used for multi-column inverted index scans.
	// Therefore, we only generate and check the unconsolidated constraint,
	// allowing the optimizer to plan multi-column inverted index scans in more
	// cases.
	//
	// For example, the consolidated constraint for (x IN (1, 2, 3)) is:
	//
	//   /x: [/1 - /3]
	//   Prefix: 0
	//
	// The unconsolidated constraint for the same expression is:
	//
	//   /x: [/1 - /1] [/2 - /2] [/3 - /3]
	//   Prefix: 1
	//
	var ic idxconstraint.Instance
	ic.Init(
		filters, optionalFilters,
		prefixColumns, notNullCols, tabMeta.ComputedCols,
		false, /* consolidate */
		evalCtx, factory, ps,
	)
	constraint = ic.UnconsolidatedConstraint()
	if constraint.Prefix(evalCtx) < prefixColumnCount {
		// If all of the constraint spans do not have the same start and end keys
		// for all columns, the index cannot be used.
		return nil, nil, false
	}

	// Make a copy of constraint so that the idxconstraint.Instance is not
	// referenced.
	copy := *constraint
	remainingFilters = ic.RemainingFilters()
	return &copy, remainingFilters, true
}

type invertedFilterPlanner interface {
	// extractInvertedFilterConditionFromLeaf extracts an inverted filter
	// condition from the given expression, which represents a leaf of an
	// expression tree in which the internal nodes are And and/or Or expressions.
	// Returns an empty inverted.Expression if no inverted filter condition could
	// be extracted.
	//
	// Additionally, returns:
	// - remaining filters that must be applied if the inverted expression is not
	//   tight, and
	// - pre-filterer state that can be used to reduce false positives.
	extractInvertedFilterConditionFromLeaf(evalCtx *tree.EvalContext, expr opt.ScalarExpr) (
		invertedExpr inverted.Expression,
		remainingFilters opt.ScalarExpr,
		_ *invertedexpr.PreFiltererStateForInvertedFilterer,
	)
}

// extractInvertedFilterCondition extracts an inverted.Expression from the given
// filter condition, where the inverted.Expression represents an inverted filter
// over the given inverted index. Returns an empty inverted.Expression if no
// inverted filter condition could be extracted.
//
// The filter condition should be an expression tree of And, Or, and leaf
// expressions. Extraction of the inverted.Expression from the leaves is
// delegated to the given invertedFilterPlanner.
//
// In addition to the inverted.Expression, returns:
// - remaining filters that must be applied if the inverted expression is not
//   tight, and
// - pre-filterer state that can be used to reduce false positives. This is
//   only non-nil if filterCond is a leaf condition (i.e., has no ANDs or ORs).
func extractInvertedFilterCondition(
	evalCtx *tree.EvalContext,
	factory *norm.Factory,
	filterCond opt.ScalarExpr,
	filterPlanner invertedFilterPlanner,
) (
	invertedExpr inverted.Expression,
	remainingFilters opt.ScalarExpr,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
) {
	switch t := filterCond.(type) {
	case *memo.AndExpr:
		l, remLeft, _ := extractInvertedFilterCondition(evalCtx, factory, t.Left, filterPlanner)
		r, remRight, _ := extractInvertedFilterCondition(evalCtx, factory, t.Right, filterPlanner)
		if remLeft == nil {
			remainingFilters = remRight
		} else if remRight == nil {
			remainingFilters = remLeft
		} else {
			remainingFilters = factory.ConstructAnd(remLeft, remRight)
		}
		return inverted.And(l, r), remainingFilters, nil

	case *memo.OrExpr:
		l, remLeft, _ := extractInvertedFilterCondition(evalCtx, factory, t.Left, filterPlanner)
		r, remRight, _ := extractInvertedFilterCondition(evalCtx, factory, t.Right, filterPlanner)
		if remLeft != nil || remRight != nil {
			// If either child has remaining filters, we must return the original
			// condition as the remaining filter. It would be incorrect to return
			// only part of the original condition.
			remainingFilters = filterCond
		}
		return inverted.Or(l, r), remainingFilters, nil

	default:
		return filterPlanner.extractInvertedFilterConditionFromLeaf(evalCtx, filterCond)
	}
}

// isIndexColumn returns true if e is an expression that corresponds to an
// inverted index column. The expression can be either:
//  - a variable on the index column, or
//  - an expression that matches the computed column expression (if the index
//    column is computed).
//
func isIndexColumn(
	tabID opt.TableID, index cat.Index, e opt.Expr, computedColumns map[opt.ColumnID]opt.ScalarExpr,
) bool {
	invertedSourceCol := tabID.ColumnID(index.InvertedColumn().InvertedSourceColumnOrdinal())
	if v, ok := e.(*memo.VariableExpr); ok && v.Col == invertedSourceCol {
		return true
	}
	if computedColumns != nil && e == computedColumns[invertedSourceCol] {
		return true
	}
	return false
}
