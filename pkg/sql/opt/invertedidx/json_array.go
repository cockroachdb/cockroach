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
// a chain of fetch val expressions and a right scalar expression. If an
// InvertedExpression cannot be generated from the expression, an
// inverted.NonInvertedColExpression is returned.
//
// In order to generate an InvertedExpression, left must be a fetch val
// expression in the form [col]->[index0]->[index1]->...->[indexN] where col is
// a variable or expression referencing the inverted column in the inverted
// index and each index is a constant string. The right expression must be a
// constant JSON value that is not an object or an array.
func (j *jsonOrArrayFilterPlanner) extractJSONFetchValEqCondition(
	evalCtx *tree.EvalContext, left *memo.FetchValExpr, right opt.ScalarExpr,
) inverted.Expression {
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

	// Recursively traverse fetch val expressions and collect keys with which to
	// build the InvertedExpression. If it is not possible to build an inverted
	// expression from the tree of fetch val expressions, collectKeys returns
	// early and foundKeys remains false. If successful, foundKeys is set to
	// true and JSON fetch value indexes are collected in keys. The keys are
	// ordered by the outer-most fetch val index first. The outer-most fetch val
	// index is the right-most in the -> chain, for example (j->'a'->'b') is
	// equivalent to ((j->'a')->'b') and 'b' is the outer-most fetch val index.
	//
	// Later on, we iterate forward through these keys to build a JSON object
	// from the inside-out with the inner-most value being the JSON scalar
	// extracted above from the right ScalarExpr function argument. In the
	// resulting JSON object, the outer-most JSON fetch value indexes are the
	// inner most JSON object keys.
	//
	// As an example, when left is (j->'a'->'b') and right is ('1'), the keys
	// {"b", "a"} are collected and the JSON object {"a": {"b": 1}} is built.
	foundKeys := false
	var keys []string
	var collectKeys func(fetch *memo.FetchValExpr)
	collectKeys = func(fetch *memo.FetchValExpr) {
		// The right side of the fetch val expression, the Index field, must be
		// a constant string. If not, then we cannot build an inverted
		// expression.
		if !memo.CanExtractConstDatum(fetch.Index) {
			return
		}
		key, ok := memo.ExtractConstDatum(fetch.Index).(*tree.DString)
		if !ok {
			return
		}

		// Append the key to the list of keys.
		keys = append(keys, string(*key))

		// If the left side of the fetch val expression, the Json field, is a
		// variable or expression corresponding to the index column, then we
		// have found a valid list of keys to build an inverted expression.
		if isIndexColumn(j.tabID, j.index, fetch.Json, j.computedColumns) {
			foundKeys = true
			return
		}

		// If the left side of the fetch val expression is another fetch val
		// expression, recursively collect its keys.
		if innerFetch, ok := fetch.Json.(*memo.FetchValExpr); ok {
			collectKeys(innerFetch)
		}

		// Otherwise, we cannot build an inverted expression.
	}
	collectKeys(left)
	if !foundKeys {
		return inverted.NonInvertedColExpression{}
	}

	// Build a new JSON object of the form:
	//   {<keyN>: ... {<key1>: {key0: <val>}}}
	// Note that key0 is the outer-most fetch val index, so the expression
	// j->'a'->'b' = 1 results in {"a": {"b": 1}}.
	var obj json.JSON
	for i := 0; i < len(keys); i++ {
		b := json.NewObjectBuilder(1)
		if i == 0 {
			b.Add(keys[i], val.JSON)
		} else {
			b.Add(keys[i], obj)
		}
		obj = b.Build()
	}

	return getInvertedExprForJSONOrArrayIndex(evalCtx, tree.NewDJSON(obj))
}
