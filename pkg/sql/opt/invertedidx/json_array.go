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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type jsonOrArrayJoinPlanner struct {
	tabID     opt.TableID
	index     cat.Index
	inputCols opt.ColSet
}

var _ invertedJoinPlanner = &jsonOrArrayJoinPlanner{}

// extractInvertedJoinConditionFromLeaf is part of the invertedJoinPlanner interface.
func (j *jsonOrArrayJoinPlanner) extractInvertedJoinConditionFromLeaf(
	ctx context.Context, expr opt.ScalarExpr,
) opt.ScalarExpr {
	switch t := expr.(type) {
	case *memo.ContainsExpr:
		if j.canExtractJSONOrArrayJoinCondition(t.Left, t.Right) {
			return t
		}
		return nil

	default:
		return nil
	}
}

// canExtractJSONOrArrayJoinCondition returns true if it is possible to extract
// an inverted join condition from the given left and right expression
// arguments. Returns false otherwise.
func (j *jsonOrArrayJoinPlanner) canExtractJSONOrArrayJoinCondition(
	left, right opt.ScalarExpr,
) bool {
	// The first argument should be a variable or expression corresponding to
	// the index column.
	// TODO(mgartner): The first argument could be an expression that matches a
	// computed column expression if the computed column is indexed. Pass
	// computedColumns to enable this.
	if !isIndexColumn(j.tabID, j.index, left, nil /* computedColumns */) {
		return false
	}
	if left.DataType().Family() == types.ArrayFamily &&
		j.index.Version() < descpb.EmptyArraysInInvertedIndexesVersion {
		// We cannot plan inverted joins on array indexes that do not include
		// keys for empty arrays.
		return false
	}

	// The second argument should either come from the input or be a constant.
	var p props.Shared
	memo.BuildSharedProps(right, &p)
	if !p.OuterCols.Empty() {
		if !p.OuterCols.SubsetOf(j.inputCols) {
			return false
		}
	} else if !memo.CanExtractConstDatum(right) {
		return false
	}

	return true
}

// getInvertedExprForJSONOrArrayIndex gets an inverted.Expression that
// constrains a json or array index according to the given constant.
func getInvertedExprForJSONOrArrayIndex(
	evalCtx *tree.EvalContext, d tree.Datum,
) inverted.Expression {
	var b []byte
	invertedExpr, err := rowenc.EncodeContainingInvertedIndexSpans(
		evalCtx, d, b, descpb.EmptyArraysInInvertedIndexesVersion,
	)
	if err != nil {
		panic(err)
	}
	return invertedExpr
}

type jsonOrArrayInvertedExpr struct {
	tree.ComparisonExpr

	nonIndexParam tree.TypedExpr

	// spanExpr is the result of evaluating the comparison expression represented
	// by this jsonOrArrayInvertedExpr. It is nil prior to evaluation.
	spanExpr *inverted.SpanExpression
}

var _ tree.TypedExpr = &jsonOrArrayInvertedExpr{}

// jsonOrArrayDatumsToInvertedExpr implements invertedexpr.DatumsToInvertedExpr for
// JSON and Array columns.
type jsonOrArrayDatumsToInvertedExpr struct {
	evalCtx      *tree.EvalContext
	colTypes     []*types.T
	invertedExpr tree.TypedExpr

	row   rowenc.EncDatumRow
	alloc rowenc.DatumAlloc
}

var _ invertedexpr.DatumsToInvertedExpr = &jsonOrArrayDatumsToInvertedExpr{}
var _ tree.IndexedVarContainer = &jsonOrArrayDatumsToInvertedExpr{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (g *jsonOrArrayDatumsToInvertedExpr) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	err := g.row[idx].EnsureDecoded(g.colTypes[idx], &g.alloc)
	if err != nil {
		return nil, err
	}
	return g.row[idx].Datum.Eval(ctx)
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (g *jsonOrArrayDatumsToInvertedExpr) IndexedVarResolvedType(idx int) *types.T {
	return g.colTypes[idx]
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (g *jsonOrArrayDatumsToInvertedExpr) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

// NewJSONOrArrayDatumsToInvertedExpr returns a new
// jsonOrArrayDatumsToInvertedExpr.
func NewJSONOrArrayDatumsToInvertedExpr(
	evalCtx *tree.EvalContext, colTypes []*types.T, expr tree.TypedExpr,
) (invertedexpr.DatumsToInvertedExpr, error) {
	g := &jsonOrArrayDatumsToInvertedExpr{
		evalCtx:  evalCtx,
		colTypes: colTypes,
	}

	// getInvertedExprLeaf takes a TypedExpr consisting of a ComparisonExpr and
	// constructs a new TypedExpr tree consisting of a jsonOrArrayInvertedExpr.
	// The jsonOrArrayInvertedExpr serves to improve the performance of
	// jsonOrArrayDatumsToInvertedExpr.Convert by reducing the amount of
	// computation needed to convert an input row to a SpanExpression. It does
	// this by pre-computing and caching the SpanExpressions for any comparison
	// expressions that have a constant as the non-indexed argument.
	getInvertedExprLeaf := func(expr tree.TypedExpr) (tree.TypedExpr, error) {
		switch t := expr.(type) {
		case *tree.ComparisonExpr:
			if t.Operator != tree.Contains {
				return nil, fmt.Errorf("%s cannot be index-accelerated", t)
			}

			// We know that the non-index param is the second param.
			nonIndexParam := t.Right.(tree.TypedExpr)

			// If possible, get the span expression now so we don't need to recompute
			// it for every row.
			var spanExpr *inverted.SpanExpression
			if d, ok := nonIndexParam.(tree.Datum); ok {
				invertedExpr := getInvertedExprForJSONOrArrayIndex(evalCtx, d)
				spanExpr, _ = invertedExpr.(*inverted.SpanExpression)
			}

			return &jsonOrArrayInvertedExpr{
				ComparisonExpr: *t,
				nonIndexParam:  nonIndexParam,
				spanExpr:       spanExpr,
			}, nil

		default:
			return nil, fmt.Errorf("unsupported expression %v", t)
		}
	}

	var err error
	g.invertedExpr, err = getInvertedExpr(expr, getInvertedExprLeaf)
	if err != nil {
		return nil, err
	}
	return g, nil
}

// Convert implements the invertedexpr.DatumsToInvertedExpr interface.
func (g *jsonOrArrayDatumsToInvertedExpr) Convert(
	ctx context.Context, datums rowenc.EncDatumRow,
) (*inverted.SpanExpressionProto, interface{}, error) {
	g.row = datums
	g.evalCtx.PushIVarContainer(g)
	defer g.evalCtx.PopIVarContainer()

	evalInvertedExprLeaf := func(expr tree.TypedExpr) (inverted.Expression, error) {
		switch t := expr.(type) {
		case *jsonOrArrayInvertedExpr:
			if t.spanExpr != nil {
				// We call Copy so the caller can modify the returned expression.
				return t.spanExpr.Copy(), nil
			}
			d, err := t.nonIndexParam.Eval(g.evalCtx)
			if err != nil {
				return nil, err
			}
			if d == tree.DNull {
				return nil, nil
			}
			return getInvertedExprForJSONOrArrayIndex(g.evalCtx, d), nil

		default:
			return nil, fmt.Errorf("unsupported expression %v", t)
		}
	}

	invertedExpr, err := evalInvertedExpr(g.invertedExpr, evalInvertedExprLeaf)
	if err != nil {
		return nil, nil, err
	}

	if invertedExpr == nil {
		return nil, nil, nil
	}

	spanExpr, ok := invertedExpr.(*inverted.SpanExpression)
	if !ok {
		return nil, nil, fmt.Errorf("unable to construct span expression")
	}

	return spanExpr.ToProto(), nil, nil
}

func (g *jsonOrArrayDatumsToInvertedExpr) CanPreFilter() bool {
	return false
}

func (g *jsonOrArrayDatumsToInvertedExpr) PreFilter(
	enc inverted.EncVal, preFilters []interface{}, result []bool,
) (bool, error) {
	return false, errors.AssertionFailedf("PreFilter called on jsonOrArrayDatumsToInvertedExpr")
}

type jsonOrArrayFilterPlanner struct {
	tabID           opt.TableID
	index           cat.Index
	computedColumns map[opt.ColumnID]opt.ScalarExpr
}

var _ invertedFilterPlanner = &jsonOrArrayFilterPlanner{}

// extractInvertedFilterConditionFromLeaf is part of the invertedFilterPlanner
// interface.
func (j *jsonOrArrayFilterPlanner) extractInvertedFilterConditionFromLeaf(
	evalCtx *tree.EvalContext, expr opt.ScalarExpr,
) (
	invertedExpr inverted.Expression,
	remainingFilters opt.ScalarExpr,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
) {
	switch t := expr.(type) {
	case *memo.ContainsExpr:
		invertedExpr = j.extractJSONOrArrayContainsCondition(evalCtx, t.Left, t.Right)
	case *memo.EqExpr:
		if fetch, ok := t.Left.(*memo.FetchValExpr); ok {
			invertedExpr = j.extractJSONFetchValEqCondition(evalCtx, fetch, t.Right)
		}
	}

	if invertedExpr == nil {
		// An inverted expression could not be extracted.
		return inverted.NonInvertedColExpression{}, expr, nil
	}

	// If the extracted inverted expression is not tight then remaining filters
	// must be applied after the inverted index scan.
	if !invertedExpr.IsTight() {
		remainingFilters = expr
	}

	// We do not currently support pre-filtering for JSON and Array indexes, so
	// the returned pre-filter state is nil.
	return invertedExpr, remainingFilters, nil
}

// extractJSONOrArrayContainsCondition extracts an InvertedExpression
// representing an inverted filter over the planner's inverted index, based
// on the given left and right expression arguments. Returns an empty
// InvertedExpression if no inverted filter could be extracted.
func (j *jsonOrArrayFilterPlanner) extractJSONOrArrayContainsCondition(
	evalCtx *tree.EvalContext, left, right opt.ScalarExpr,
) inverted.Expression {
	// The first argument should be a variable or expression corresponding to
	// the index column.
	if !isIndexColumn(j.tabID, j.index, left, j.computedColumns) {
		return inverted.NonInvertedColExpression{}
	}

	// The second argument should be a constant.
	if !memo.CanExtractConstDatum(right) {
		return inverted.NonInvertedColExpression{}
	}
	d := memo.ExtractConstDatum(right)
	if left.DataType().Family() == types.ArrayFamily &&
		j.index.Version() < descpb.EmptyArraysInInvertedIndexesVersion {
		if arr, ok := d.(*tree.DArray); ok && arr.Len() == 0 {
			// We cannot constrain array indexes that do not include
			// keys for empty arrays.
			return inverted.NonInvertedColExpression{}
		}
	}

	return getInvertedExprForJSONOrArrayIndex(evalCtx, d)
}

// extractJSONFetchValEqCondition extracts an InvertedExpression representing an
// inverted filter over the planner's inverted index, based on equality between
// a fetch val expression and a right scalar expression. If the following criteria
// are not met, an empty InvertedExpression is returned.
//
//   1. The fetch value operator's left expression must be a variable
//      referencing the inverted column in the index.
//   2. The fetch value operator's right expression must be a constant string.
//   3. The right expression in the equality expression must be a constant JSON
//      value that is not an object or an array.
//
// TODO(mgartner): Support chained fetch val operators, e.g., j->'a'->'b' = '1'.
func (j *jsonOrArrayFilterPlanner) extractJSONFetchValEqCondition(
	evalCtx *tree.EvalContext, fetch *memo.FetchValExpr, right opt.ScalarExpr,
) inverted.Expression {
	// The left side of the fetch val expression, the Json field, should be a
	// variable corresponding to the index column.
	if !isIndexColumn(j.tabID, j.index, fetch.Json, j.computedColumns) {
		return inverted.NonInvertedColExpression{}
	}

	// The right side of the fetch val expression, the Index field, should be a
	// constant string.
	if !memo.CanExtractConstDatum(fetch.Index) {
		return inverted.NonInvertedColExpression{}
	}
	key, ok := memo.ExtractConstDatum(fetch.Index).(*tree.DString)
	if !ok {
		return inverted.NonInvertedColExpression{}
	}

	// The right side of the equals expression should be a constant JSON value
	// that is not an object or array.
	if !memo.CanExtractConstDatum(right) {
		return inverted.NonInvertedColExpression{}
	}
	val, ok := memo.ExtractConstDatum(right).(*tree.DJSON)
	if !ok {
		return inverted.NonInvertedColExpression{}
	}
	typ := val.JSON.Type()
	if typ == json.ObjectJSONType || typ == json.ArrayJSONType {
		return inverted.NonInvertedColExpression{}
	}

	// Build a new JSON object of the form: {<key>: <right>}.
	b := json.NewObjectBuilder(1)
	b.Add(string(*key), val.JSON)
	obj := tree.NewDJSON(b.Build())

	return getInvertedExprForJSONOrArrayIndex(evalCtx, obj)
}
