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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	evalCtx *tree.EvalContext, colTypes []*types.T, expr tree.TypedExpr, desc *descpb.IndexDescriptor,
) (invertedexpr.DatumsToInvertedExpr, error) {
	if !geoindex.IsEmptyConfig(&desc.GeoConfig) {
		return NewGeoDatumsToInvertedExpr(evalCtx, colTypes, expr, &desc.GeoConfig)
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
	memo.BuildSharedProps(invertedExpr, &p)
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
// expressions, and returns the resulting invertedexpr.InvertedExpression.
// Delegates evaluation of leaf expressions to the given evalInvertedExprLeaf
// function.
func evalInvertedExpr(
	expr tree.TypedExpr,
	evalInvertedExprLeaf func(expr tree.TypedExpr) (invertedexpr.InvertedExpression, error),
) (invertedexpr.InvertedExpression, error) {
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
		return invertedexpr.And(leftExpr, rightExpr), nil

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
		return invertedexpr.Or(leftExpr, rightExpr), nil

	default:
		return evalInvertedExprLeaf(expr)
	}
}

// TryFilterInvertedIndex tries to derive an inverted filter condition for
// the given inverted index from the specified filters. If an inverted filter
// condition is derived, it is returned with ok=true. If no condition can be
// derived, then TryFilterInvertedIndex returns ok=false.
//
// In addition to the inverted filter condition (spanExpr), returns:
// - a constraint of the prefix columns if there are any,
// - remaining filters that must be applied if the span expression is not tight,
// - pre-filterer state that can be used by the invertedFilterer operator to
//   reduce the number of false positives returned by the span expression,
// - noDuplicates=true if the spans are guaranteed not to produce duplicate
//   primary keys. Otherwise, returns noDuplicates=false, and
// - ok=true if the spanExpr is a valid inverted filter condition. Otherwise,
//   returns ok=false.
func TryFilterInvertedIndex(
	evalCtx *tree.EvalContext,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
) (
	spanExpr *invertedexpr.SpanExpression,
	constraint *constraint.Constraint,
	remainingFilters memo.FiltersExpr,
	preFiltererState *invertedexpr.PreFiltererStateForInvertedFilterer,
	noDuplicates bool,
	ok bool,
) {
	// Attempt to constrain the prefix columns, if there are any. If they cannot
	// be constrained to single values, the index cannot be used.
	constraint, filters, ok = constrainPrefixColumns(evalCtx, factory, filters, tabID, index)
	if !ok {
		return nil, nil, nil, nil, false, false
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
			tabID: tabID,
			index: index,
		}
		col := index.VirtualInvertedColumn().InvertedSourceColumnOrdinal()
		typ = factory.Metadata().Table(tabID).Column(col).DatumType()
	}

	var invertedExpr invertedexpr.InvertedExpression
	var pfState *invertedexpr.PreFiltererStateForInvertedFilterer

	// We can only guarantee that there are no duplicate primary keys if there is
	// one filter that is also guaranteed not to produce duplicates.
	noDuplicates = len(filters) == 1
	for i := range filters {
		invertedExprLocal, pfStateLocal, noDupsLocal := extractInvertedFilterCondition(
			evalCtx, filters[i].Condition, filterPlanner,
		)
		if invertedExpr == nil {
			invertedExpr = invertedExprLocal
			pfState = pfStateLocal
		} else {
			invertedExpr = invertedexpr.And(invertedExpr, invertedExprLocal)
			// Do pre-filtering using the first of the conjuncts that provided
			// non-nil pre-filtering state.
			if pfState == nil {
				pfState = pfStateLocal
			}
		}
		noDuplicates = noDuplicates && noDupsLocal
	}

	if invertedExpr == nil {
		return nil, nil, nil, nil, false, false
	}

	spanExpr, ok = invertedExpr.(*invertedexpr.SpanExpression)
	if !ok {
		return nil, nil, nil, nil, false, false
	}
	if pfState != nil {
		pfState.Typ = typ
	}

	if spanExpr.Tight {
		// If the span expression is tight, there are no remaining filters.
		return spanExpr, constraint, nil /* remainingFilters */, pfState, noDuplicates, true
	}
	return spanExpr, constraint, filters, pfState, noDuplicates, true
}

// constrainPrefixColumns attempts to build a constraint for the non-inverted
// prefix columns of the given index. If a constraint is successfully built, it
// is returned along with remaining filters and ok=true. The function is only
// successful if it can generate a constraint where all spans have the same
// start and end keys for all non-inverted prefix columns. If the index is a
// single-column inverted index, there are no prefix columns to constrain, and
// ok=true is returned.
func constrainPrefixColumns(
	evalCtx *tree.EvalContext,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
) (constraint *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	tabMeta := factory.Metadata().TableMeta(tabID)
	prefixColumnCount := index.NonInvertedPrefixColumnCount()

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

	var ic idxconstraint.Instance
	ic.Init(
		filters, nil, /* optionalFilters */
		prefixColumns, notNullCols, tabMeta.ComputedCols,
		false /* isInverted */, evalCtx, factory,
	)
	constraint = ic.Constraint()
	if constraint.Prefix(evalCtx) < prefixColumnCount {
		// If all spans do not have the same start and end keys for all columns,
		// the index cannot be used.
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
	// Returns an empty InvertedExpression if no inverted filter condition could
	// be extracted.
	//
	// Additionally, returns:
	// - pre-filterer state that can be used to reduce false positives, and
	// - noDuplicates=true if the spans in the InvertedExpression are guaranteed
	//   not to produce duplicate primary keys. Otherwise, returns
	//   noDuplicates=false.
	extractInvertedFilterConditionFromLeaf(evalCtx *tree.EvalContext, expr opt.ScalarExpr) (
		_ invertedexpr.InvertedExpression,
		_ *invertedexpr.PreFiltererStateForInvertedFilterer,
		noDuplicates bool,
	)
}

// extractInvertedFilterCondition extracts an InvertedExpression from the given
// filter condition, where the InvertedExpression represents an inverted filter
// over the given inverted index. Returns an empty InvertedExpression if no
// inverted filter condition could be extracted.
//
// The filter condition should be an expression tree of And, Or, and leaf
// expressions. Extraction of the InvertedExpression from the leaves is
// delegated to the given invertedFilterPlanner.
//
// In addition to the InvertedExpression, returns:
// - pre-filterer state that can be used to reduce false positives, and
// - noDuplicates=true if the spans in the InvertedExpression are guaranteed
//   not to produce duplicate primary keys. Otherwise, returns
//   noDuplicates=false.
func extractInvertedFilterCondition(
	evalCtx *tree.EvalContext, filterCond opt.ScalarExpr, filterPlanner invertedFilterPlanner,
) (
	_ invertedexpr.InvertedExpression,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
	noDuplicates bool,
) {
	switch t := filterCond.(type) {
	// For both And and Or cases, return noDuplicates=false since we cannot
	// guarantee that two different filters will produce different primary keys.
	case *memo.AndExpr:
		l, _, _ := extractInvertedFilterCondition(evalCtx, t.Left, filterPlanner)
		r, _, _ := extractInvertedFilterCondition(evalCtx, t.Right, filterPlanner)
		return invertedexpr.And(l, r), nil, false /* noDuplicates */

	case *memo.OrExpr:
		l, _, _ := extractInvertedFilterCondition(evalCtx, t.Left, filterPlanner)
		r, _, _ := extractInvertedFilterCondition(evalCtx, t.Right, filterPlanner)
		return invertedexpr.Or(l, r), nil, false /* noDuplicates */

	default:
		return filterPlanner.extractInvertedFilterConditionFromLeaf(evalCtx, filterCond)
	}
}
