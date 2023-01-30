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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type jsonOrArrayJoinPlanner struct {
	factory   *norm.Factory
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
	case *memo.ContainsExpr, *memo.ContainedByExpr:
		return j.extractJSONOrArrayJoinCondition(t)
	default:
		return nil
	}
}

// extractJSONOrArrayJoinCondition returns a scalar expression if it is
// possible to extract an inverted join condition from the left and right
// arguments of the given expression. If the arguments need to be commuted,
// it will construct a new expression. Returns nil otherwise.
func (j *jsonOrArrayJoinPlanner) extractJSONOrArrayJoinCondition(
	expr opt.ScalarExpr,
) opt.ScalarExpr {
	var left, right, indexCol, val opt.ScalarExpr
	commuteArgs, containedBy := false, false
	switch t := expr.(type) {
	case *memo.ContainsExpr:
		left = t.Left
		right = t.Right
	case *memo.ContainedByExpr:
		left = t.Left
		right = t.Right
		containedBy = true
	default:
		return nil
	}
	if isIndexColumn(j.tabID, j.index, left, nil /* computedColumns */) {
		// When the first argument is a variable or expression corresponding to the
		// index column, we keep the order of arguments as is and get the
		// InvertedExpression for either left @> right or left <@ right.
		// TODO(mgartner): The first argument could be an expression that matches a
		// computed column expression if the computed column is indexed. Pass
		// computedColumns to enable this.
		indexCol, val = left, right
	} else if isIndexColumn(j.tabID, j.index, right, nil /* computedColumns */) {
		// When the second argument is a variable or expression corresponding to
		// the index column, we must commute the right and left arguments and
		// construct a new expression. We get the equivalent InvertedExpression for
		// right <@ left or right @> left.
		commuteArgs = true
		indexCol, val = right, left
	} else {
		// If neither condition is met, we cannot create an InvertedExpression.
		return nil
	}
	if indexCol.DataType().Family() == types.ArrayFamily &&
		j.index.Version() < descpb.EmptyArraysInInvertedIndexesVersion {
		// We cannot plan inverted joins on array indexes that do not include
		// keys for empty arrays.
		return nil
	}
	// The non-indexed argument should either come from the input or be a
	// constant.
	var p props.Shared
	memo.BuildSharedProps(val, &p, j.factory.EvalContext())
	if !p.OuterCols.Empty() {
		if !p.OuterCols.SubsetOf(j.inputCols) {
			return nil
		}
	} else if !memo.CanExtractConstDatum(val) {
		return nil
	}

	// If commuteArgs is true, we construct a new equivalent expression so that
	// the left argument is the indexed column.
	if commuteArgs {
		if containedBy {
			return j.factory.ConstructContains(right, left)
		}
		return j.factory.ConstructContainedBy(right, left)
	}
	return expr
}

// getInvertedExprForJSONOrArrayIndexForContaining gets an inverted.Expression that
// constrains a JSON or Array index according to the given constant.
// This results in a span expression representing the intersection of all paths
// through the JSON or Array. This function is used when checking if an indexed
// column contains (@>) a constant.
func getInvertedExprForJSONOrArrayIndexForContaining(
	ctx context.Context, evalCtx *eval.Context, d tree.Datum,
) inverted.Expression {
	invertedExpr, err := rowenc.EncodeContainingInvertedIndexSpans(ctx, evalCtx, d)
	if err != nil {
		panic(err)
	}
	return invertedExpr
}

// getInvertedExprForJSONOrArrayIndexForContainedBy gets an inverted.Expression
// that constrains a JSON or Array index according to the given constant.
// This results in a span expression representing the union of all paths
// through the JSON or Array. This function is only used when checking if an
// indexed column is contained by (<@) a constant.
func getInvertedExprForJSONOrArrayIndexForContainedBy(
	ctx context.Context, evalCtx *eval.Context, d tree.Datum,
) inverted.Expression {
	invertedExpr, err := rowenc.EncodeContainedInvertedIndexSpans(ctx, evalCtx, d)
	if err != nil {
		panic(err)
	}
	return invertedExpr
}

// getInvertedExprForJSONIndexForExists gets an inverted.Expression that
// constrains a JSON index according to the given constant. This results in a
// span expression representing all objects that have the input string datum
// as a top level key.
// If d is an array, then the inverted expression is a conjunction if all is
// true, and a disjunction otherwise.
func getInvertedExprForJSONIndexForExists(
	ctx context.Context, evalCtx *eval.Context, d tree.Datum, all bool,
) inverted.Expression {
	invertedExpr, err := rowenc.EncodeExistsInvertedIndexSpans(ctx, evalCtx, d, all)
	if err != nil {
		panic(err)
	}
	return invertedExpr
}

// getInvertedExprForArrayIndexForOverlaps gets an inverted.Expression
// that constrains an Array index according to the given constant.
// This results in a span expression representing the union of all paths
// through the Array. This function is only used when checking if an
// indexed Array column overlaps (&&) with a constant.
func getInvertedExprForArrayIndexForOverlaps(
	ctx context.Context, evalCtx *eval.Context, d tree.Datum,
) inverted.Expression {
	invertedExpr, err := rowenc.EncodeOverlapsInvertedIndexSpans(ctx, evalCtx, d)
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
	evalCtx      *eval.Context
	colTypes     []*types.T
	invertedExpr tree.TypedExpr

	row   rowenc.EncDatumRow
	alloc tree.DatumAlloc
}

var _ invertedexpr.DatumsToInvertedExpr = &jsonOrArrayDatumsToInvertedExpr{}
var _ eval.IndexedVarContainer = &jsonOrArrayDatumsToInvertedExpr{}

// IndexedVarEval is part of the eval.IndexedVarContainer interface.
func (g *jsonOrArrayDatumsToInvertedExpr) IndexedVarEval(
	ctx context.Context, idx int, e tree.ExprEvaluator,
) (tree.Datum, error) {
	err := g.row[idx].EnsureDecoded(g.colTypes[idx], &g.alloc)
	if err != nil {
		return nil, err
	}
	return g.row[idx].Datum.Eval(ctx, e)
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
	ctx context.Context, evalCtx *eval.Context, colTypes []*types.T, expr tree.TypedExpr,
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
			// We know that the non-index param is the second param.
			nonIndexParam := t.Right.(tree.TypedExpr)

			// If possible, get the span expression now so we don't need to recompute
			// it for every row.
			var spanExpr *inverted.SpanExpression
			if d, ok := nonIndexParam.(tree.Datum); ok {
				var invertedExpr inverted.Expression
				switch t.Operator.Symbol {
				case treecmp.ContainedBy:
					invertedExpr = getInvertedExprForJSONOrArrayIndexForContainedBy(ctx, evalCtx, d)
				case treecmp.Contains:
					invertedExpr = getInvertedExprForJSONOrArrayIndexForContaining(ctx, evalCtx, d)
				case treecmp.Overlaps:
					invertedExpr = getInvertedExprForArrayIndexForOverlaps(ctx, evalCtx, d)
				case treecmp.JSONExists:
					invertedExpr = getInvertedExprForJSONIndexForExists(ctx, evalCtx, d, true /* all */)
				case treecmp.JSONSomeExists:
					invertedExpr = getInvertedExprForJSONIndexForExists(ctx, evalCtx, d, false /* all */)
				case treecmp.JSONAllExists:
					invertedExpr = getInvertedExprForJSONIndexForExists(ctx, evalCtx, d, true /* all */)
				default:
					return nil, fmt.Errorf("%s cannot be index-accelerated", t)
				}
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
			d, err := eval.Expr(ctx, g.evalCtx, t.nonIndexParam)
			if err != nil {
				return nil, err
			}
			if d == tree.DNull {
				return nil, nil
			}
			switch t.Operator.Symbol {
			case treecmp.Contains:
				return getInvertedExprForJSONOrArrayIndexForContaining(ctx, g.evalCtx, d), nil

			case treecmp.ContainedBy:
				return getInvertedExprForJSONOrArrayIndexForContainedBy(ctx, g.evalCtx, d), nil

			default:
				return nil, fmt.Errorf("unsupported expression %v", t)
			}

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
	ctx context.Context, evalCtx *eval.Context, expr opt.ScalarExpr,
) (
	invertedExpr inverted.Expression,
	remainingFilters opt.ScalarExpr,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
) {
	switch t := expr.(type) {
	case *memo.ContainsExpr:
		invertedExpr = j.extractJSONOrArrayContainsCondition(ctx, evalCtx, t.Left, t.Right, false /* containedBy */)
	case *memo.ContainedByExpr:
		invertedExpr = j.extractJSONOrArrayContainsCondition(ctx, evalCtx, t.Left, t.Right, true /* containedBy */)
	case *memo.JsonExistsExpr:
		invertedExpr = j.extractJSONExistsCondition(ctx, evalCtx, t.Left, t.Right, false /* all */)
	case *memo.JsonSomeExistsExpr:
		invertedExpr = j.extractJSONExistsCondition(ctx, evalCtx, t.Left, t.Right, false /* all */)
	case *memo.JsonAllExistsExpr:
		invertedExpr = j.extractJSONExistsCondition(ctx, evalCtx, t.Left, t.Right, true /* all */)
	case *memo.EqExpr:
		if fetch, ok := t.Left.(*memo.FetchValExpr); ok {
			invertedExpr = j.extractJSONFetchValEqCondition(ctx, evalCtx, fetch, t.Right)
		}
	case *memo.OverlapsExpr:
		invertedExpr = j.extractArrayOverlapsCondition(ctx, evalCtx, t.Left, t.Right)
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

// extractArrayOverlapsCondition extracts an InvertedExpression
// representing an inverted filter over the planner's inverted index, based
// on the given left and right expression arguments. Returns an empty
// InvertedExpression if no inverted filter could be extracted.
func (j *jsonOrArrayFilterPlanner) extractArrayOverlapsCondition(
	ctx context.Context, evalCtx *eval.Context, left, right opt.ScalarExpr,
) inverted.Expression {
	var constantVal opt.ScalarExpr
	if isIndexColumn(j.tabID, j.index, left, j.computedColumns) && memo.CanExtractConstDatum(right) {
		// When the first argument is a variable or expression corresponding to the
		// index column and the second argument is a constant, we can generate an
		// inverted expression with the constant value picked from right.
		constantVal = right
	} else if isIndexColumn(j.tabID, j.index, right, j.computedColumns) && memo.CanExtractConstDatum(left) {
		// When the second argument is a variable or expression corresponding to
		// the index column and the first argument is a constant, we can generate an
		// inverted expression with the constant value picked from left.
		constantVal = left
	} else {
		// If none of the conditions are met, we cannot create an InvertedExpression.
		return inverted.NonInvertedColExpression{}
	}
	return getInvertedExprForArrayIndexForOverlaps(ctx, evalCtx, memo.ExtractConstDatum(constantVal))
}

// extractJSONOrArrayContainsCondition extracts an InvertedExpression
// representing an inverted filter over the planner's inverted index, based
// on the given left and right expression arguments. Returns an empty
// InvertedExpression if no inverted filter could be extracted.
func (j *jsonOrArrayFilterPlanner) extractJSONOrArrayContainsCondition(
	ctx context.Context, evalCtx *eval.Context, left, right opt.ScalarExpr, containedBy bool,
) inverted.Expression {
	var indexColumn, constantVal opt.ScalarExpr
	if isIndexColumn(j.tabID, j.index, left, j.computedColumns) && memo.CanExtractConstDatum(right) {
		// When the first argument is a variable or expression corresponding to the
		// index column and the second argument is a constant, we get the
		// InvertedExpression for left @> right or left <@ right.
		indexColumn, constantVal = left, right
	} else if isIndexColumn(j.tabID, j.index, right, j.computedColumns) && memo.CanExtractConstDatum(left) {
		// When the second argument is a variable or expression corresponding to
		// the index column and the first argument is a constant, we get the
		// equivalent InvertedExpression for right <@ left or right @> left.
		indexColumn, constantVal = right, left
		containedBy = !containedBy
	} else {
		if fetch, ok := left.(*memo.FetchValExpr); ok {
			// When the expression has a JSON fetch operator on the left, it is
			// handled in extractJSONFetchValContainsCondition.
			return j.extractJSONFetchValContainsCondition(ctx, evalCtx, fetch, right, containedBy)
		} else if fetch, ok := right.(*memo.FetchValExpr); ok {
			// When the expression has a JSON fetch operator on the right, it is
			// handled in extractJSONFetchValContainsCondition as an equivalent
			// expression with right and left swapped.
			return j.extractJSONFetchValContainsCondition(ctx, evalCtx, fetch, left, !containedBy)
		}
		// If none of the conditions are met, we cannot create an InvertedExpression.
		return inverted.NonInvertedColExpression{}
	}
	d := memo.ExtractConstDatum(constantVal)
	if indexColumn.DataType().Family() == types.ArrayFamily &&
		j.index.Version() < descpb.EmptyArraysInInvertedIndexesVersion {
		if arr, ok := d.(*tree.DArray); ok && (containedBy || arr.Len() == 0) {
			// We cannot constrain array indexes that do not include
			// keys for empty arrays.
			return inverted.NonInvertedColExpression{}
		}
	}
	if containedBy {
		return getInvertedExprForJSONOrArrayIndexForContainedBy(ctx, evalCtx, d)
	}
	return getInvertedExprForJSONOrArrayIndexForContaining(ctx, evalCtx, d)
}

// extractJSONExistsCondition extracts an InvertedExpression representing an
// inverted filter with the JSON{Some,All,}Exists (?, ?|, ?&) operators over the
// planner's inverted index, based on the given left and right expression
// arguments. Returns an empty InvertedExpression if no inverted filter could be
// extracted.
func (j *jsonOrArrayFilterPlanner) extractJSONExistsCondition(
	ctx context.Context, evalCtx *eval.Context, left, right opt.ScalarExpr, all bool,
) inverted.Expression {
	if isIndexColumn(j.tabID, j.index, left, j.computedColumns) && memo.CanExtractConstDatum(right) {
		// When the first argument is a variable or expression corresponding to the
		// index column and the second argument is a constant, we get the
		// InvertedExpression for left ? right.
		constantVal := right
		d := memo.ExtractConstDatum(constantVal)
		return getInvertedExprForJSONIndexForExists(ctx, evalCtx, d, all)
	}
	// If none of the conditions are met, we cannot create an InvertedExpression.
	return inverted.NonInvertedColExpression{}
}

// extractJSONFetchValEqCondition extracts an InvertedExpression representing an
// inverted filter over the planner's inverted index, based on equality between
// a chain of fetch val expressions and a scalar expression. If an
// InvertedExpression cannot be generated from the expression, an
// inverted.NonInvertedColExpression is returned.
//
// In order to generate an InvertedExpression, left must be a fetch val
// expression in the form [col]->[index0]->[index1]->...->[indexN] where col is
// a variable or expression referencing the inverted column in the inverted
// index and each index is a constant string. The right expression must be a
// constant JSON value.
func (j *jsonOrArrayFilterPlanner) extractJSONFetchValEqCondition(
	ctx context.Context, evalCtx *eval.Context, left *memo.FetchValExpr, right opt.ScalarExpr,
) inverted.Expression {
	// The right side of the expression should be a constant JSON value.
	if !memo.CanExtractConstDatum(right) {
		return inverted.NonInvertedColExpression{}
	}
	val, ok := memo.ExtractConstDatum(right).(*tree.DJSON)
	if !ok {
		return inverted.NonInvertedColExpression{}
	}

	// Collect a slice of keys from the fetch val expression.
	var keys tree.Datums
	keys = j.collectKeys(keys, left)
	if len(keys) == 0 {
		return inverted.NonInvertedColExpression{}
	}

	// Build a new JSON object with the collected keys and val.
	obj := buildObject(keys, val.JSON)

	// For Equals expressions, we will generate the inverted expression for the
	// single object built from the keys and val.
	invertedExpr := getInvertedExprForJSONOrArrayIndexForContaining(ctx, evalCtx, tree.NewDJSON(obj))

	// When the right side is an array or object, the InvertedExpression
	// generated is not tight. We must indicate it is non-tight so an additional
	// filter is added.
	typ := val.JSON.Type()
	if typ == json.ArrayJSONType || typ == json.ObjectJSONType {
		invertedExpr.SetNotTight()
	}

	// If a key is of the type DInt, the InvertedExpression generated is
	// not tight. This is because key encodings for JSON arrays don't
	// contain the respective index positions for each of their elements.
	// A JSON array of the form ["a", 31] will have the following encoding
	// into an index key:
	//
	// 1/2/arr/a/pk1
	// 1/2/arr/31/pk1
	//
	// where arr is an ARRAY type tag used to indicate that the next key is
	// part of an array, 1 is the table id, 2 is the inverted index id and
	// pk is a primary key of a row in the table. Since the array
	// elements do not have their respective indices stored in
	// the encoding, the original filter needs to be applied after the initial
	// scan.
	for i := range keys {
		if _, ok := keys[i].(*tree.DInt); ok {
			invertedExpr.SetNotTight()
			break
		}
	}
	return invertedExpr
}

// extractJSONFetchValContainsCondition extracts an InvertedExpression
// representing an inverted filter over the planner's inverted index, based on
// containment between a chain of fetch val expressions and a scalar
// expression. If an InvertedExpression cannot be generated from the
// expression, an inverted.NonInvertedColExpression is returned.
//
// In order to generate an InvertedExpression, left must be a fetch val
// expression in the form [col]->[index0]->[index1]->...->[indexN] where col is
// a variable or expression referencing the inverted column in the inverted
// index and each index is a constant string. The right expression must be a
// constant JSON value. For expressions with a left constant value and a right
// fetch val expression, the arguments will be swapped when passed in.
//
// The type of operator is indicated by the containedBy parameter, which is
// true for <@ and false for @>.
func (j *jsonOrArrayFilterPlanner) extractJSONFetchValContainsCondition(
	ctx context.Context,
	evalCtx *eval.Context,
	left *memo.FetchValExpr,
	right opt.ScalarExpr,
	containedBy bool,
) inverted.Expression {
	// The right side of the expression should be a constant JSON value.
	if !memo.CanExtractConstDatum(right) {
		return inverted.NonInvertedColExpression{}
	}
	val, ok := memo.ExtractConstDatum(right).(*tree.DJSON)
	if !ok {
		return inverted.NonInvertedColExpression{}
	}

	// Collect a slice of keys from the fetch val expression.
	var keys tree.Datums
	keys = j.collectKeys(keys, left)
	if len(keys) == 0 {
		return inverted.NonInvertedColExpression{}
	}

	// Build a new JSON object with the collected keys and val.
	obj := buildObject(keys, val.JSON)

	var invertedExpr inverted.Expression

	// For Contains and ContainedBy expressions, we may need to build additional
	// objects to cover all possibilities.
	objs, err := buildFetchContainmentObjects(keys, val.JSON, containedBy)
	if err != nil {
		return inverted.NonInvertedColExpression{}
	}
	objs = append(objs, obj)
	// We get an inverted expression for each object constructed, and union
	// these expressions.
	for i := range objs {
		var expr inverted.Expression
		if containedBy {
			expr = getInvertedExprForJSONOrArrayIndexForContainedBy(ctx, evalCtx, tree.NewDJSON(objs[i]))
		} else {
			expr = getInvertedExprForJSONOrArrayIndexForContaining(ctx, evalCtx, tree.NewDJSON(objs[i]))
		}
		if invertedExpr == nil {
			invertedExpr = expr
		} else {
			invertedExpr = inverted.Or(invertedExpr, expr)
		}
	}

	// If a key is of the type DInt, the InvertedExpression generated is
	// not tight. This is because key encodings for JSON arrays don't
	// contain the respective index positions for each of their elements.
	// A JSON array of the form ["a", 31] will have the following encoding
	// into an index key:
	//
	// 1/2/arr/a/pk1
	// 1/2/arr/31/pk1
	//
	// where arr is an ARRAY type tag used to indicate that the next key is
	// part of an array, 1 is the table id, 2 is the inverted index id and
	// pk is a primary key of a row in the table. Since the array
	// elements do not have their respective indices stored in
	// the encoding, the original filter needs to be applied after the initial
	// scan.
	for i := range keys {
		if _, ok := keys[i].(*tree.DInt); ok {
			invertedExpr.SetNotTight()
			break
		}
	}

	return invertedExpr
}

// collectKeys is called on fetch val expressions to the find corresponding
// keys used to build a JSON object. It recursively traverses the fetch val
// expressions and collects keys with which to build the InvertedExpression.
// If it is not possible to build an inverted expression from the tree of fetch
// val expressions, collectKeys returns nil for keys. If successful, the JSON
// fetch value indexes are collected in keys. The keys are ordered by the
// outer-most fetch val index first. The outer-most fetch val index is the
// right-most in the -> chain, for example (j->'a'->'b') is equivalent to
// ((j->'a')->'b') and 'b' is the outer-most fetch val index.
//
// Callers of this function should iterate forward through these keys to build
// a JSON object from the inside-out with the inner-most value being the JSON
// scalar extracted above from the right ScalarExpr function argument. In the
// resulting JSON object, the outer-most JSON fetch value indexes are the
// inner most JSON object keys.
//
// As an example, when left is (j->'a'->'b') and right is ('1'), the keys
// {"b", "a"} are collected and the JSON object {"a": {"b": 1}} is built.
//
// Moreover, the index of the fetch value can also be a DInt, the Int
// Datum which refers to an index of a JSON array. In this case, as an
// example, left can be (j->0) and right can be ('1') resulting in the
// collected keys to become {0} with the JSON object built as ['1'].
func (j *jsonOrArrayFilterPlanner) collectKeys(
	currKeys tree.Datums, fetch *memo.FetchValExpr,
) (keys tree.Datums) {
	// The right side of the fetch val expression, the Index field, must be
	// a constant string or an integer. If not, then we cannot build an
	// inverted expression.
	if !memo.CanExtractConstDatum(fetch.Index) {
		return nil
	}
	key := memo.ExtractConstDatum(fetch.Index)

	switch key.(type) {
	case *tree.DString, *tree.DInt:
	default:
		return nil
	}

	// Append the key to the list of keys.
	keys = append(currKeys, key)

	// If the left side of the fetch val expression, the Json field, is a
	// variable or expression corresponding to the index column, then we
	// have found a valid list of keys to build an inverted expression.
	if isIndexColumn(j.tabID, j.index, fetch.Json, j.computedColumns) {
		return keys
	}

	// If the left side of the fetch val expression is another fetch val
	// expression, recursively collect its keys.
	if innerFetch, ok := fetch.Json.(*memo.FetchValExpr); ok {
		return j.collectKeys(keys, innerFetch)
	}
	// Otherwise, we cannot build an inverted expression.
	return nil
}

// buildFetchContainmentObjects constructs new JSON objects with given keys and val.
// The keys and val are extracted from a fetch val containment expression, and
// the objects constructed depend on the value type and whether the expression
// uses <@ or @>. For example, the expression j->'a'->'b' @> "c" would have
// {"a", "b"} as keys, "c" as val, and construct {"a": "b": ["c"]}.
// An array of the constructed JSONs is returned.
func buildFetchContainmentObjects(
	keys tree.Datums, val json.JSON, containedBy bool,
) ([]json.JSON, error) {
	var objs []json.JSON
	typ := val.Type()
	switch typ {
	case json.ArrayJSONType:
		// For arrays in ContainedBy expressions, we must create a scalar value
		// object, because getInvertedExprForJSONOrArrayIndexForContainedBy will
		// not include the scalar value spans.

		// Array value examples:
		//   j->'a' @> '[1]', no new object required, we already have '{"a": [1]}'
		//   j->'a' <@ '[1]', build '{"a": 1}', we already have '{"a": [1]}'
		//   j->'a' <@ '[1, [2], 3]', build '{"a": 1}', '{"a": 3}', we already have '{"a": [1, [2], 3]}'
		if containedBy {
			for i := 0; i < val.Len(); i++ {
				v, err := val.FetchValIdx(i)
				if err != nil {
					return nil, err
				}
				t := v.Type()
				if t == json.ArrayJSONType || t == json.ObjectJSONType {
					// The scalar value is only needed for non-nested arrays and objects.
					continue
				}
				newObj := buildObject(keys, v)
				objs = append(objs, newObj)
			}
		}

	case json.ObjectJSONType:
		// For objects in ContainedBy expressions, we do not need to generate the
		// empty object value for each level of nesting, because the spans will be
		// added for us in getInvertedExprForJSONOrArrayIndexForContainedBy.
		// For objects in Contains expressions, no additional spans are required
		// outside of the given object's spans.

		// Object value examples:
		//   j->'a' @> '{"b": 2}', we already have '{"a": {"b": 2}}'
		//   j->'a' <@ '{"b": 2}', we already have '{"a": {"b": 2}}'
		return nil, nil

	default:
		// For scalars in Contains expressions, we construct an array value
		// containing the scalar.

		// Scalar value examples:
		//   j->'a' @> '1', build '{"a": [1]}', we already have '{"a": 1}'
		//   j->'a' <@ '1', we already have '{"a": 1}'
		if !containedBy {
			arr := json.NewArrayBuilder(1)
			arr.Add(val)
			v := arr.Build()
			newObj := buildObject(keys, v)
			objs = append(objs, newObj)
		}
	}
	return objs, nil
}

// buildObject constructs a new JSON object of the form:
//
//	{<keyN>: ... {<key1>: {key0: <val>}}}
//
// Where the keys and val are extracted from a fetch val expression by the
// caller. Note that key0 is the outer-most fetch val index, so the expression
// j->'a'->'b' = 1 results in {"a": {"b": 1}}.
//
// It can also construct a new JSON array in the form:
//
// [val]
//
// where val is extracted from a fetch val expression and an array
// is built due to the filter being of the form j->index_pos = val, where
// index_pos is an integer that denotes the index position in a JSON array.
// An array is built with val at position 0 (regardless of the actual value
// of index_pos), since the index position of the JSON array is not present
// in the inverted index key. This removes the need for the actual integer
// value of the index position and the original filter will be applied after
// the initial scan.

// Moreover, the function can also result in creating JSON objects that are
// a combination of both JSON objects and JSON arrays.
// For example, the expression j->0->'a' = 1 results in [{'a': 1}].
func buildObject(keys tree.Datums, val json.JSON) json.JSON {
	for i := 0; i < len(keys); i++ {
		switch t := keys[i].(type) {
		case *tree.DString:
			b := json.NewObjectBuilder(1)
			b.Add(string(*t), val)
			val = b.Build()
		case *tree.DInt:
			b := json.NewArrayBuilder(1)
			// We never look at the integer value of the key since
			// the integer value, denoting an index position in the
			// JSON array, is never considered while building an
			// inverted expression. The inverted expression generated
			// is thus not tight and will thus have the original
			// filter applied.
			b.Add(val)
			val = b.Build()
		}
	}
	return val
}
