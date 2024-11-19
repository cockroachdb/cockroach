// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package invertedidx

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geogfn"
	// Blank import so projections are initialized correctly.
	_ "github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/r1"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// This file contains functions for building geospatial inverted index scans
// and joins that are used throughout the xform package.

// GetGeoIndexRelationship returns the corresponding geospatial relationship
// and ok=true if the given expression is either a geospatial function or
// bounding box comparison operator that can be index-accelerated. Otherwise
// returns ok=false.
func GetGeoIndexRelationship(expr opt.ScalarExpr) (_ geoindex.RelationshipType, ok bool) {
	if function, ok := expr.(*memo.FunctionExpr); ok {
		rel, ok := geoindex.RelationshipMap[function.Name]
		return rel, ok
	}
	if _, ok := expr.(*memo.BBoxCoversExpr); ok {
		return geoindex.Covers, true
	}
	if _, ok := expr.(*memo.BBoxIntersectsExpr); ok {
		return geoindex.Intersects, true
	}
	return 0, false
}

// getSpanExprForGeoIndexFn is a function that returns a SpanExpression that
// constrains the given geo index according to the given constant and
// geospatial relationship. It is implemented by getSpanExprForGeographyIndex
// and getSpanExprForGeometryIndex and used in extractGeoFilterCondition.
type getSpanExprForGeoIndexFn func(
	context.Context, tree.Datum, []tree.Datum, geoindex.RelationshipType, geopb.Config,
) inverted.Expression

// getSpanExprForGeographyIndex gets a SpanExpression that constrains the given
// geography index according to the given constant and geospatial relationship.
func getSpanExprForGeographyIndex(
	ctx context.Context,
	d tree.Datum,
	additionalParams []tree.Datum,
	relationship geoindex.RelationshipType,
	indexConfig geopb.Config,
) inverted.Expression {
	geogIdx := geoindex.NewS2GeographyIndex(*indexConfig.S2Geography)
	geog := d.(*tree.DGeography).Geography

	switch relationship {
	case geoindex.Covers:
		unionKeySpans, err := geogIdx.Covers(ctx, geog)
		if err != nil {
			panic(err)
		}
		return invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.CoveredBy:
		rpKeyExpr, err := geogIdx.CoveredBy(ctx, geog)
		if err != nil {
			panic(err)
		}
		spanExpr, err := invertedexpr.GeoRPKeyExprToSpanExpr(rpKeyExpr)
		if err != nil {
			panic(err)
		}
		return spanExpr

	case geoindex.DWithin:
		// Parameters are type checked earlier. Keep this consistent with the definition
		// in geo_builtins.go.
		if len(additionalParams) != 1 && len(additionalParams) != 2 {
			panic(errors.AssertionFailedf("unexpected param length %d", len(additionalParams)))
		}
		d, ok := additionalParams[0].(*tree.DFloat)
		if !ok {
			panic(errors.AssertionFailedf(
				"parameter %s is not float", additionalParams[0].ResolvedType()))
		}
		distanceMeters := float64(*d)
		useSpheroid := geogfn.UseSpheroid
		if len(additionalParams) == 2 {
			b, ok := additionalParams[1].(*tree.DBool)
			if !ok {
				panic(errors.AssertionFailedf(
					"parameter %s is not bool", additionalParams[1].ResolvedType()))
			}
			if !*b {
				useSpheroid = geogfn.UseSphere
			}
		}
		unionKeySpans, err := geogIdx.DWithin(ctx, geog, distanceMeters, useSpheroid)
		if err != nil {
			panic(err)
		}
		return invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.Intersects:
		unionKeySpans, err := geogIdx.Intersects(ctx, geog)
		if err != nil {
			panic(err)
		}
		return invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	default:
		panic(errors.AssertionFailedf("unhandled relationship: %v", relationship))
	}
}

// Helper for DWithin and DFullyWithin.
func getDistanceParam(params []tree.Datum) float64 {
	// Parameters are type checked earlier when the expression is built by
	// optbuilder. extractInfoFromExpr ensures that the parameters are non-NULL
	// constants. Keep this consistent with the definition in geo_builtins.go.
	if len(params) != 1 {
		panic(errors.AssertionFailedf("unexpected param length %d", len(params)))
	}
	d, ok := params[0].(*tree.DFloat)
	if !ok {
		panic(errors.AssertionFailedf("parameter %s is not float", params[0].ResolvedType()))
	}
	return float64(*d)
}

// getSpanExprForGeometryIndex gets a SpanExpression that constrains the given
// geometry index according to the given constant and geospatial relationship.
func getSpanExprForGeometryIndex(
	ctx context.Context,
	d tree.Datum,
	additionalParams []tree.Datum,
	relationship geoindex.RelationshipType,
	indexConfig geopb.Config,
) inverted.Expression {
	geomIdx := geoindex.NewS2GeometryIndex(*indexConfig.S2Geometry)
	geom := d.(*tree.DGeometry).Geometry

	switch relationship {
	case geoindex.Covers:
		unionKeySpans, err := geomIdx.Covers(ctx, geom)
		if err != nil {
			panic(err)
		}
		return invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.CoveredBy:
		rpKeyExpr, err := geomIdx.CoveredBy(ctx, geom)
		if err != nil {
			panic(err)
		}
		spanExpr, err := invertedexpr.GeoRPKeyExprToSpanExpr(rpKeyExpr)
		if err != nil {
			panic(err)
		}
		return spanExpr

	case geoindex.DFullyWithin:
		distance := getDistanceParam(additionalParams)
		unionKeySpans, err := geomIdx.DFullyWithin(ctx, geom, distance)
		if err != nil {
			panic(err)
		}
		return invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.DWithin:
		distance := getDistanceParam(additionalParams)
		unionKeySpans, err := geomIdx.DWithin(ctx, geom, distance)
		if err != nil {
			panic(err)
		}
		return invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.Intersects:
		unionKeySpans, err := geomIdx.Intersects(ctx, geom)
		if err != nil {
			panic(err)
		}
		return invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	default:
		panic(errors.AssertionFailedf("unhandled relationship: %v", relationship))
	}
}

type geoJoinPlanner struct {
	factory     *norm.Factory
	tabID       opt.TableID
	index       cat.Index
	inputCols   opt.ColSet
	getSpanExpr getSpanExprForGeoIndexFn
}

var _ invertedJoinPlanner = &geoJoinPlanner{}

// extractInvertedJoinConditionFromLeaf is part of the invertedJoinPlanner
// interface.
func (g *geoJoinPlanner) extractInvertedJoinConditionFromLeaf(
	ctx context.Context, expr opt.ScalarExpr,
) opt.ScalarExpr {
	var args memo.ScalarListExpr
	switch t := expr.(type) {
	case *memo.FunctionExpr:
		args = t.Args

	case *memo.BBoxCoversExpr, *memo.BBoxIntersectsExpr:
		args = memo.ScalarListExpr{
			t.Child(0).(opt.ScalarExpr), t.Child(1).(opt.ScalarExpr),
		}
		// Cast the arguments to type Geometry if they are type Box2d.
		for i := 0; i < len(args); i++ {
			if args[i].DataType().Family() == types.Box2DFamily {
				args[i] = g.factory.ConstructCast(args[i], types.Geometry)
			}
		}

	default:
		return nil
	}

	// Try to extract an inverted join condition from the given filter condition.
	// If unsuccessful, try to extract a join condition from an equivalent
	// function in which the arguments are commuted. For example:
	//
	//   ST_Intersects(g1, g2) <-> ST_Intersects(g2, g1)
	//   ST_Covers(g1, g2) <-> ST_CoveredBy(g2, g1)
	//   g1 && g2 -> ST_Intersects(g2, g1)
	//   g1 ~ g2 -> ST_CoveredBy(g2, g1)
	//
	// See extractGeoJoinCondition for more details.
	fn := g.extractGeoJoinCondition(expr, args, false /* commuteArgs */)
	if fn == nil {
		fn = g.extractGeoJoinCondition(expr, args, true /* commuteArgs */)
	}
	return fn
}

// extractGeoJoinCondition tries to extract an inverted join condition from the
// given expression, which should be either a function or comparison operation.
// If commuteArgs is true, extractGeoJoinCondition tries to extract an inverted
// join condition from an equivalent version of the given expression in which
// the first two arguments are swapped.
//
// If commuteArgs is false, returns the original function (if the expression
// was a function) or a new function representing the geospatial relationship
// of the comparison operation. If commuteArgs is true, returns a new function
// representing the same relationship but with commuted arguments. For example:
//
//	ST_Intersects(g1, g2) <-> ST_Intersects(g2, g1)
//	ST_Covers(g1, g2) <-> ST_CoveredBy(g2, g1)
//	g1 && g2 -> ST_Intersects(g2, g1)
//	g1 ~ g2 -> ST_CoveredBy(g2, g1)
//
// See geoindex.CommuteRelationshipMap for the full list of mappings.
//
// Returns nil if a join condition was not successfully extracted.
func (g *geoJoinPlanner) extractGeoJoinCondition(
	expr opt.ScalarExpr, args memo.ScalarListExpr, commuteArgs bool,
) opt.ScalarExpr {
	rel, arg1, _, _, ok := extractInfoFromExpr(expr, args, commuteArgs, g.tabID, g.index)
	if !ok {
		return nil
	}

	// The first argument should either come from the input or be a constant.
	var p props.Shared
	memo.BuildSharedProps(arg1, &p, g.factory.EvalContext())
	if !p.OuterCols.Empty() {
		if !p.OuterCols.SubsetOf(g.inputCols) {
			return nil
		}
	} else if !memo.CanExtractConstDatum(arg1) {
		return nil
	}

	return makeExprFromRelationshipAndParams(g.factory, expr, args, commuteArgs, rel)
}

// constructFunction finds a function overload matching the given name and
// argument types, and uses the factory to construct a function. The return
// type of the function must be bool.
func constructFunction(
	factory *norm.Factory, name string, args memo.ScalarListExpr,
) opt.ScalarExpr {
	props, overload, ok := memo.FindFunction(&args, name)
	if !ok {
		panic(errors.AssertionFailedf("could not find overload for %s", name))
	}
	return factory.ConstructFunction(args, &memo.FunctionPrivate{
		Name:       name,
		Typ:        types.Bool,
		Properties: props,
		Overload:   overload,
	})
}

type geoFilterPlanner struct {
	factory     *norm.Factory
	tabID       opt.TableID
	index       cat.Index
	getSpanExpr getSpanExprForGeoIndexFn
}

var _ invertedFilterPlanner = &geoFilterPlanner{}

// STDistanceUseSpheroid returns true if the use_spheroid argument of
// st_distance is not explicitly false. use_spheroid is the third argument of
// st_distance for the geography overload and it is true by default. The
// geometry overload does not have a use_spheroid argument, so if either of the
// first two arguments are geometries, it returns false.
func (g *geoFilterPlanner) STDistanceUseSpheroid(args memo.ScalarListExpr) bool {
	if len(args) < 2 {
		panic(errors.AssertionFailedf("expected st_distance to have at least two arguments"))
	}
	if args[0].DataType().Family() == types.GeometryFamily ||
		args[1].DataType().Family() == types.GeometryFamily {
		return false
	}
	const useSpheroidIdx = 2
	if len(args) <= useSpheroidIdx {
		// The use_spheroid argument is true by default, so return true if it
		// was not provided.
		return true
	}
	return args[useSpheroidIdx].Op() != opt.FalseOp
}

// MaybeMakeSTDWithin attempts to derive an ST_DWithin (or ST_DFullyWithin)
// function that is similar to an expression of the following form:
// ST_Distance(a,b) <= x. The ST_Distance function can be on either side of the
// inequality, and the inequality can be one of the following: '<', '<=', '>',
// '>='. This replacement allows early-exit behavior, and may enable use of an
// inverted index scan. If the derived expression would need to be negated with
// a NotExpr, so the derivation fails, and expr, ok=false is returned.
func (g *geoFilterPlanner) MaybeMakeSTDWithin(
	expr opt.ScalarExpr,
	args memo.ScalarListExpr,
	bound opt.ScalarExpr,
	fnIsLeftArg bool,
	fullyWithin bool,
) (derivedExpr opt.ScalarExpr, ok bool) {
	op := expr.Op()
	var not bool
	var name string
	fnName := "st_dwithin"
	if fullyWithin {
		fnName = "st_dfullywithin"
	}
	incName := fnName
	exName := fnName + "exclusive"
	switch op {
	case opt.GeOp:
		if fnIsLeftArg {
			// Matched expression: ST_Distance(a,b) >= x.
			not = true
			name = exName
		} else {
			// Matched expression: x >= ST_Distance(a,b).
			not = false
			name = incName
		}

	case opt.GtOp:
		if fnIsLeftArg {
			// Matched expression: ST_Distance(a,b) > x.
			not = true
			name = incName
		} else {
			// Matched expression: x > ST_Distance(a,b).
			not = false
			name = exName
		}

	case opt.LeOp:
		if fnIsLeftArg {
			// Matched expression: ST_Distance(a,b) <= x.
			not = false
			name = incName
		} else {
			// Matched expression: x <= ST_Distance(a,b).
			not = true
			name = exName
		}

	case opt.LtOp:
		if fnIsLeftArg {
			// Matched expression: ST_Distance(a,b) < x.
			not = false
			name = exName
		} else {
			// Matched expression: x < ST_Distance(a,b).
			not = true
			name = incName
		}
	}
	if not {
		// ST_DWithin and ST_DWithinExclusive are equivalent to ST_Distance <= x and
		// ST_Distance < x respectively. The comparison operator in the matched
		// expression (if ST_Distance is normalized to be on the left) is either '>'
		// or '>='. Therefore, we would have to take the opposite of within. This
		// would not result in a useful expression for inverted index scan, so return
		// ok=false.
		return expr, false
	}
	newArgs := make(memo.ScalarListExpr, len(args)+1)
	const distanceIdx, useSpheroidIdx = 2, 3
	copy(newArgs, args[:distanceIdx])

	// The distance parameter must be type float.
	newArgs[distanceIdx] = g.factory.ConstructCast(bound, types.Float)

	// Add the use_spheroid parameter if it exists.
	if len(newArgs) > useSpheroidIdx {
		newArgs[useSpheroidIdx] = args[useSpheroidIdx-1]
	}

	props, overload, ok := memo.FindFunction(&newArgs, name)
	if !ok {
		panic(errors.AssertionFailedf("could not find overload for %s", name))
	}
	within := g.factory.ConstructFunction(newArgs, &memo.FunctionPrivate{
		Name:       name,
		Typ:        types.Bool,
		Properties: props,
		Overload:   overload,
	})
	return within, true
}

// maybeDeriveUsefulInvertedFilterCondition identifies an expression of the
// form: 'st_distance(a, b, bool) = 0', 'st_distance(...) <= x' or
// 'st_maxdistance(...) <= x', and returns a function call to st_dwithin,
// st_dwithinexclusive, st_dfullywithin, st_dfullywithinexclusive or
// st_intersects which is almost equivalent to the original expression, except
// for empty or null geography/geometry inputs (it may evaluate to true in more
// cases). The derived expression may enable use of an inverted index scan. See
// the MaybeMakeSTDWithin or MakeIntersectionFunction method for the specific
// function that is used to replace expressions with different comparison
// operators (e.g. '<' vs '<='). Note that the `st_distance` or `st_maxdistance`
// may be on the left or right of the comparison operation (LT, GT, LE, GE).
func (g *geoFilterPlanner) maybeDeriveUsefulInvertedFilterCondition(
	expr opt.ScalarExpr,
) (opt.ScalarExpr, bool) {
	var left, right opt.ScalarExpr
	var function *memo.FunctionExpr
	leftIsFunction := false
	rightIsFunction := false
	c := g.factory.CustomFuncs()
	switch t := expr.(type) {
	case *memo.EqExpr:
		left = t.Left
		right = t.Right
		function, leftIsFunction = left.(*memo.FunctionExpr)
		if !leftIsFunction {
			return expr, false
		}
		private := &function.FunctionPrivate
		if private.Name != "st_distance" {
			return expr, false
		}
		if g.STDistanceUseSpheroid(function.Args) {
			return expr, false
		}
		constant, rightIsConstant := right.(*memo.ConstExpr)
		if !rightIsConstant {
			return expr, false
		}
		value := constant.Value
		if !c.IsFloatDatum(value) {
			return expr, false
		}
		if !c.DatumsEqual(value, tree.NewDInt(0)) {
			return expr, false
		}
		return c.MakeIntersectionFunction(function.Args), true
	case *memo.LtExpr, *memo.GtExpr, *memo.LeExpr, *memo.GeExpr:
		left = t.Child(0).(opt.ScalarExpr)
		right = t.Child(1).(opt.ScalarExpr)
		function, leftIsFunction = left.(*memo.FunctionExpr)
		if !leftIsFunction {
			function, rightIsFunction = right.(*memo.FunctionExpr)
			if !rightIsFunction {
				return expr, false
			}
		}
		// Combinations which result in a `NOT st_d*` function would not enable
		// inverted index scan, so no need to derive filters for these cases.
		if leftIsFunction && (t.Op() == opt.GtOp || t.Op() == opt.GeOp) {
			return expr, false
		} else if rightIsFunction && (t.Op() == opt.LtOp || t.Op() == opt.LeOp) {
			return expr, false
		}
		// Main logic below to eliminate a code nesting level.
	default:
		return expr, false
	}

	if function == nil {
		return expr, false
	}
	args := function.Args
	private := &function.FunctionPrivate
	if private.Name != "st_distance" && private.Name != "st_maxdistance" {
		return expr, false
	}
	var boundExpr opt.ScalarExpr
	if leftIsFunction {
		boundExpr = right
	} else {
		boundExpr = left
	}
	if private.Name == "st_distance" {
		return g.MaybeMakeSTDWithin(expr, args, boundExpr, leftIsFunction, false /* fullyWithin */)
	}
	return g.MaybeMakeSTDWithin(expr, args, boundExpr, leftIsFunction, true /* fullyWithin */)
}

// extractInvertedFilterConditionFromLeaf is part of the invertedFilterPlanner
// interface.
func (g *geoFilterPlanner) extractInvertedFilterConditionFromLeaf(
	ctx context.Context, evalCtx *eval.Context, expr opt.ScalarExpr,
) (
	invertedExpr inverted.Expression,
	remainingFilters opt.ScalarExpr,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
) {
	var args memo.ScalarListExpr
	filterIsDerived := false
	originalExpr := expr
	expr, filterIsDerived = g.maybeDeriveUsefulInvertedFilterCondition(expr)
	switch t := expr.(type) {
	case *memo.FunctionExpr:
		args = t.Args

	case *memo.BBoxCoversExpr, *memo.BBoxIntersectsExpr:
		args = memo.ScalarListExpr{
			t.Child(0).(opt.ScalarExpr), t.Child(1).(opt.ScalarExpr),
		}
		// Cast the arguments to type Geometry if they are type Box2d.
		for i := 0; i < len(args); i++ {
			if args[i].DataType().Family() == types.Box2DFamily {
				args[i] = g.factory.ConstructCast(args[i], types.Geometry)
			}
		}

	default:
		return inverted.NonInvertedColExpression{}, expr, nil
	}

	// Try to extract an inverted filter condition from the given expression.
	// If the resulting inverted expression is not a SpanExpression, try
	// extracting the condition with an equivalent function in which the
	// arguments are commuted. For example:
	//
	//   ST_Intersects(g1, g2) <-> ST_Intersects(g2, g1)
	//   ST_Covers(g1, g2) <-> ST_CoveredBy(g2, g1)
	//   g1 && g2 -> ST_Intersects(g2, g1)
	//   g1 ~ g2 -> ST_CoveredBy(g2, g1)
	//
	// See geoindex.CommuteRelationshipMap for the full list of mappings.
	invertedExpr, pfState := extractGeoFilterCondition(
		ctx, g.factory, expr, args, false /* commuteArgs */, g.tabID, g.index, g.getSpanExpr,
	)
	if _, ok := invertedExpr.(inverted.NonInvertedColExpression); ok {
		invertedExpr, pfState = extractGeoFilterCondition(
			ctx, g.factory, expr, args, true /* commuteArgs */, g.tabID, g.index, g.getSpanExpr,
		)
	}
	// A derived filter may not be semantically equivalent to the original, so we
	// need to apply the original filter in that case, the same as when the
	// inverted expression is not tight.
	if !invertedExpr.IsTight() || filterIsDerived {
		remainingFilters = originalExpr
	}
	return invertedExpr, remainingFilters, pfState
}

// extractGeoFilterCondition extracts an inverted.Expression representing an
// inverted filter condition over the given geospatial index, based on the
// given expression. If commuteArgs is true, extractGeoFilterCondition extracts
// the inverted.Expression based on an equivalent version of the given
// expression in which the first two arguments are swapped.
func extractGeoFilterCondition(
	ctx context.Context,
	factory *norm.Factory,
	expr opt.ScalarExpr,
	args memo.ScalarListExpr,
	commuteArgs bool,
	tabID opt.TableID,
	index cat.Index,
	getSpanExpr getSpanExprForGeoIndexFn,
) (inverted.Expression, *invertedexpr.PreFiltererStateForInvertedFilterer) {
	relationship, arg1, arg2, additionalParams, ok :=
		extractInfoFromExpr(expr, args, commuteArgs, tabID, index)
	if !ok {
		return inverted.NonInvertedColExpression{}, nil
	}
	// The first argument should be a constant.
	if !memo.CanExtractConstDatum(arg1) {
		return inverted.NonInvertedColExpression{}, nil
	}
	d := memo.ExtractConstDatum(arg1)

	preFilterExpr :=
		makeExprFromRelationshipAndParams(factory, expr, args, commuteArgs, relationship)

	return getSpanExpr(ctx, d, additionalParams, relationship, index.GeoConfig()),
		&invertedexpr.PreFiltererStateForInvertedFilterer{
			Expr: preFilterExpr,
			Col:  arg2.Col,
		}
}

// extractInfoFromExpr is a helper used for extracting information from a
// function or operation represented by expr, with args. The second arg is
// checked to be the one corresponding to the indexed column and all args
// after the second are checked to be constant. The returned relationship
// includes the effect of commuteArgs.
// REQUIRES: len(args) >= 2.
func extractInfoFromExpr(
	expr opt.ScalarExpr,
	args memo.ScalarListExpr,
	commuteArgs bool,
	tabID opt.TableID,
	index cat.Index,
) (
	relationship geoindex.RelationshipType,
	arg1 opt.Expr,
	arg2 *memo.VariableExpr,
	additionalParams []tree.Datum,
	ok bool,
) {
	relationship, ok = GetGeoIndexRelationship(expr)
	if !ok {
		return 0, nil, nil, nil, false
	}

	if args.ChildCount() < 2 {
		panic(errors.AssertionFailedf(
			"all index-accelerated geospatial functions should have at least two arguments",
		))
	}

	var exprArg2 opt.Expr
	arg1, exprArg2 = args.Child(0), args.Child(1)
	if commuteArgs {
		arg1, exprArg2 = exprArg2, arg1
	}

	// The first argument must be non-NULL.
	if arg1.Op() == opt.NullOp {
		return 0, nil, nil, nil, false
	}

	// The second argument should be a variable corresponding to the index
	// column.
	arg2, ok = exprArg2.(*memo.VariableExpr)
	if !ok {
		return 0, nil, nil, nil, false
	}
	if arg2.Col != tabID.ColumnID(index.InvertedColumn().InvertedSourceColumnOrdinal()) {
		// The column in the function does not match the index column.
		return 0, nil, nil, nil, false
	}

	// Any additional params must be non-NULL constants.
	for i := 2; i < args.ChildCount(); i++ {
		arg := args.Child(i)
		if arg.Op() == opt.NullOp || !memo.CanExtractConstDatum(arg) {
			return 0, nil, nil, nil, false
		}
		additionalParams = append(additionalParams, memo.ExtractConstDatum(args.Child(i)))
	}

	if commuteArgs {
		relationship, ok = geoindex.CommuteRelationshipMap[relationship]
		if !ok {
			// It's not possible to commute this relationship.
			return 0, nil, nil, nil, false
		}
	}
	return relationship, arg1, arg2, additionalParams, true
}

// makeExprFromRelationshipAndParams is a helper used for making a function
// from a function or operation represented by expr, with args. The function
// or operation represents the specified relationship (after incorporating the
// commuteArgs). It can return the expr passed as a parameter if it is already
// the desired function expression.
func makeExprFromRelationshipAndParams(
	factory *norm.Factory,
	expr opt.ScalarExpr,
	args memo.ScalarListExpr,
	commuteArgs bool,
	relationship geoindex.RelationshipType,
) opt.ScalarExpr {
	if commuteArgs {
		name := geoindex.RelationshipReverseMap[relationship]
		// Copy the original arguments into a new list, and swap the first two
		// arguments.
		commutedArgs := make(memo.ScalarListExpr, len(args))
		copy(commutedArgs, args)
		commutedArgs[0], commutedArgs[1] = commutedArgs[1], commutedArgs[0]
		return constructFunction(factory, name, commutedArgs)
	}
	if _, ok := expr.(*memo.FunctionExpr); !ok {
		// This expression was one of the bounding box comparison operators.
		// Construct a function that represents the same geospatial relationship.
		name := geoindex.RelationshipReverseMap[relationship]
		return constructFunction(factory, name, args)
	}
	// Reuse the expr.
	return expr
}

type geoInvertedExpr struct {
	tree.FuncExpr

	relationship     geoindex.RelationshipType
	nonIndexParam    tree.TypedExpr
	additionalParams []tree.Datum

	// invertedExpr is the result of evaluating the geospatial relationship
	// represented by this geoInvertedExpr. It is nil prior to evaluation.
	invertedExpr inverted.Expression
}

var _ tree.TypedExpr = &geoInvertedExpr{}

// State for pre-filtering, returned by PreFilterer.Bind.
type filterState struct {
	geopb.BoundingBox
	srid geopb.SRID
}

// The pre-filtering interface{} returned by Convert refers to *filterState
// which are backed by batch allocated []filterState to reduce the number of
// heap allocations.
const preFilterAllocBatch = 100

// PreFilterer captures the pre-filtering state for a function whose
// non-indexed parameter (the lookup column for an inverted join) has not been
// bound to a value. The bound value is captured in the interface{} returned
// by Bind, to allow the caller to hold onto that state for a batch of lookup
// columns.
//
// TODO(sumeer):
// - extend PreFilterer to more general expressions.
// - use PreFilterer for invertedFilterer (where it will be bound once).
type PreFilterer struct {
	// The type of the lookup column.
	typ *types.T
	// The relationship represented by the function.
	preFilterRelationship     geoindex.RelationshipType
	additionalPreFilterParams []tree.Datum
	// Batch allocated for reducing heap allocations.
	preFilterState []filterState
}

// NewPreFilterer constructs a PreFilterer
func NewPreFilterer(
	typ *types.T, preFilterRelationship geoindex.RelationshipType, additionalParams []tree.Datum,
) *PreFilterer {
	return &PreFilterer{
		typ:                       typ,
		preFilterRelationship:     preFilterRelationship,
		additionalPreFilterParams: additionalParams,
	}
}

// Bind binds the datum and returns the pre-filter state.
func (p *PreFilterer) Bind(d tree.Datum) interface{} {
	bbox := geopb.BoundingBox{}
	var srid geopb.SRID
	// Earlier type-checking ensures we only see these two types.
	switch g := d.(type) {
	case *tree.DGeometry:
		bboxRef := g.BoundingBoxRef()
		if bboxRef != nil {
			bbox = *bboxRef
		}
		srid = g.SRID()
	case *tree.DGeography:
		rect := g.BoundingRect()
		bbox = geopb.BoundingBox{
			LoX: rect.Lng.Lo,
			HiX: rect.Lng.Hi,
			LoY: rect.Lat.Lo,
			HiY: rect.Lat.Hi,
		}
		srid = g.SRID()
	default:
		panic(errors.AssertionFailedf("datum of unhandled type: %s", d))
	}
	if len(p.preFilterState) == 0 {
		p.preFilterState = make([]filterState, preFilterAllocBatch)
	}
	p.preFilterState[0] = filterState{BoundingBox: bbox, srid: srid}
	rv := &p.preFilterState[0]
	p.preFilterState = p.preFilterState[1:]
	return rv
}

// PreFilter pre-filters a retrieved inverted value against a set of
// pre-filter states. The function signature matches the PreFilter function of
// the DatumsToInvertedExpr interface (PreFilterer does not implement the full
// interface): the result slice indicates which pre-filters matched and the
// single bool in the return value is true iff there is at least one result
// index with a true value.
func (p *PreFilterer) PreFilter(
	enc inverted.EncVal, preFilters []interface{}, result []bool,
) (bool, error) {
	loX, loY, hiX, hiY, _, err := encoding.DecodeGeoInvertedKey(enc)
	if err != nil {
		return false, err
	}
	switch p.typ.Family() {
	case types.GeometryFamily:
		var bb geo.CartesianBoundingBox
		bb.LoX, bb.LoY, bb.HiX, bb.HiY = loX, loY, hiX, hiY
		switch p.preFilterRelationship {
		case geoindex.DWithin, geoindex.DFullyWithin:
			distance := float64(tree.MustBeDFloat(p.additionalPreFilterParams[0]))
			bb.LoX -= distance
			bb.LoY -= distance
			bb.HiX += distance
			bb.HiY += distance
		}
		rv := false
		for i := range preFilters {
			pbb := geo.CartesianBoundingBox{BoundingBox: preFilters[i].(*filterState).BoundingBox}
			switch p.preFilterRelationship {
			case geoindex.Intersects, geoindex.DWithin:
				result[i] = bb.Intersects(&pbb)
			case geoindex.Covers:
				result[i] = pbb.Covers(&bb)
			case geoindex.CoveredBy, geoindex.DFullyWithin:
				result[i] = bb.Covers(&pbb)
			default:
				return false, errors.Errorf("unhandled relationship %s", p.preFilterRelationship)
			}
			if result[i] {
				rv = true
			}
		}
		return rv, nil
	case types.GeographyFamily:
		bb := s2.Rect{
			Lat: r1.Interval{Lo: loY, Hi: hiY},
			Lng: s1.Interval{Lo: loX, Hi: hiX},
		}
		rv := false
		for i := range preFilters {
			fs := preFilters[i].(*filterState)
			pbb := s2.Rect{
				Lat: r1.Interval{Lo: fs.LoY, Hi: fs.HiY},
				Lng: s1.Interval{Lo: fs.LoX, Hi: fs.HiX},
			}
			switch p.preFilterRelationship {
			case geoindex.Intersects:
				result[i] = pbb.Intersects(bb)
			case geoindex.Covers:
				result[i] = pbb.Contains(bb)
			case geoindex.CoveredBy:
				result[i] = bb.Contains(pbb)
			case geoindex.DWithin:
				distance := float64(tree.MustBeDFloat(p.additionalPreFilterParams[0]))
				useSphereOrSpheroid := geogfn.UseSpheroid
				if len(p.additionalPreFilterParams) == 2 {
					useSphereOrSpheroid =
						geogfn.UseSphereOrSpheroid(tree.MustBeDBool(p.additionalPreFilterParams[1]))
				}
				// TODO(sumeer): refactor to share code with geogfn.DWithin.
				proj, err := geoprojbase.Projection(fs.srid)
				if err != nil {
					return false, err
				}
				angleToExpand := s1.Angle(distance / proj.Spheroid.SphereRadius())
				if useSphereOrSpheroid == geogfn.UseSpheroid {
					angleToExpand *= (1 + geogfn.SpheroidErrorFraction)
				}
				result[i] = pbb.CapBound().Expanded(angleToExpand).Intersects(bb.CapBound())
			default:
				return false, errors.Errorf("unhandled relationship %s", p.preFilterRelationship)
			}
			if result[i] {
				rv = true
			}
		}
		return rv, nil
	}
	panic(errors.AssertionFailedf("unhandled type %s", p.typ))
}

// geoDatumsToInvertedExpr implements invertedexpr.DatumsToInvertedExpr for
// geospatial columns.
type geoDatumsToInvertedExpr struct {
	evalCtx      *eval.Context
	colTypes     []*types.T
	invertedExpr tree.TypedExpr
	indexConfig  geopb.Config
	typ          *types.T
	getSpanExpr  getSpanExprForGeoIndexFn

	// Non-nil only when it can pre-filter.
	filterer *PreFilterer

	row   rowenc.EncDatumRow
	alloc tree.DatumAlloc
}

var _ invertedexpr.DatumsToInvertedExpr = &geoDatumsToInvertedExpr{}
var _ eval.IndexedVarContainer = &geoDatumsToInvertedExpr{}

// IndexedVarEval is part of the eval.IndexedVarContainer interface.
func (g *geoDatumsToInvertedExpr) IndexedVarEval(idx int) (tree.Datum, error) {
	err := g.row[idx].EnsureDecoded(g.colTypes[idx], &g.alloc)
	if err != nil {
		return nil, err
	}
	return g.row[idx].Datum, nil
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (g *geoDatumsToInvertedExpr) IndexedVarResolvedType(idx int) *types.T {
	return g.colTypes[idx]
}

// NewGeoDatumsToInvertedExpr returns a new geoDatumsToInvertedExpr.
func NewGeoDatumsToInvertedExpr(
	ctx context.Context,
	evalCtx *eval.Context,
	colTypes []*types.T,
	expr tree.TypedExpr,
	config geopb.Config,
) (invertedexpr.DatumsToInvertedExpr, error) {
	if config.IsEmpty() {
		return nil, fmt.Errorf("inverted joins are currently only supported for geospatial indexes")
	}

	g := &geoDatumsToInvertedExpr{
		evalCtx:     evalCtx,
		colTypes:    colTypes,
		indexConfig: config,
	}
	if config.IsGeography() {
		g.typ = types.Geography
		g.getSpanExpr = getSpanExprForGeographyIndex
	} else if config.IsGeometry() {
		g.typ = types.Geometry
		g.getSpanExpr = getSpanExprForGeometryIndex
	} else {
		panic(errors.AssertionFailedf("not a geography or geometry index"))
	}

	// getInvertedExprLeaf takes a TypedExpr consisting of a FuncExpr and
	// constructs a new TypedExpr consisting of a geoInvertedExpr expression.
	// The geoInvertedExpr serves to improve the performance of
	// geoDatumsToInvertedExpr.Convert by reducing the amount of computation
	// needed to convert an input row to a SpanExpression. It does this by
	// caching the geospatial relationship of each function, and pre-computing
	// and caching the SpanExpressions for any functions that have a constant as
	// the non-indexed argument.
	funcExprCount := 0
	var preFilterRelationship geoindex.RelationshipType
	var additionalPreFilterParams []tree.Datum
	getInvertedExprLeaf := func(expr tree.TypedExpr) (tree.TypedExpr, error) {
		switch t := expr.(type) {
		case *tree.FuncExpr:
			funcExprCount++
			name := t.Func.FunctionReference.String()
			relationship, ok := geoindex.RelationshipMap[name]
			if !ok {
				return nil, fmt.Errorf("%s cannot be index-accelerated", name)
			}

			if len(t.Exprs) < 2 {
				return nil, fmt.Errorf("index-accelerated functions must have at least two arguments")
			}

			// We know that the non-index param is the first param, because the
			// optimizer already commuted the arguments of any functions where that
			// was not the case. See extractGeoJoinCondition for details.
			nonIndexParam := t.Exprs[0].(tree.TypedExpr)

			var additionalParams []tree.Datum
			for i := 2; i < len(t.Exprs); i++ {
				datum, ok := t.Exprs[i].(tree.Datum)
				if !ok {
					return nil, fmt.Errorf("non constant additional parameter for %s", name)
				}
				additionalParams = append(additionalParams, datum)
			}

			// If possible, get the span expression now so we don't need to recompute
			// it for every row.
			var invertedExpr inverted.Expression
			if d, ok := nonIndexParam.(tree.Datum); ok {
				invertedExpr = g.getSpanExpr(ctx, d, additionalParams, relationship, g.indexConfig)
			} else if funcExprCount == 1 {
				// Currently pre-filtering is limited to a single FuncExpr.
				preFilterRelationship = relationship
				additionalPreFilterParams = additionalParams
			}

			return &geoInvertedExpr{
				FuncExpr:         *t,
				relationship:     relationship,
				nonIndexParam:    nonIndexParam,
				additionalParams: additionalParams,
				invertedExpr:     invertedExpr,
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
	if funcExprCount == 1 {
		g.filterer = NewPreFilterer(g.typ, preFilterRelationship, additionalPreFilterParams)
	}
	return g, nil
}

// Convert implements the invertedexpr.DatumsToInvertedExpr interface.
func (g *geoDatumsToInvertedExpr) Convert(
	ctx context.Context, datums rowenc.EncDatumRow,
) (*inverted.SpanExpressionProto, interface{}, error) {
	g.row = datums
	g.evalCtx.PushIVarContainer(g)
	defer g.evalCtx.PopIVarContainer()

	var preFilterState interface{}
	evalInvertedExprLeaf := func(expr tree.TypedExpr) (inverted.Expression, error) {
		switch t := expr.(type) {
		case *geoInvertedExpr:
			if t.invertedExpr != nil {
				// We call Copy so the caller can modify the returned expression.
				return t.invertedExpr.Copy(), nil
			}
			d, err := eval.Expr(ctx, g.evalCtx, t.nonIndexParam)
			if err != nil {
				return nil, err
			}
			if d == tree.DNull {
				return nil, nil
			}
			if g.filterer != nil {
				preFilterState = g.filterer.Bind(d)
			}
			return g.getSpanExpr(ctx, d, t.additionalParams, t.relationship, g.indexConfig), nil

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

	return spanExpr.ToProto(), preFilterState, nil
}

func (g *geoDatumsToInvertedExpr) CanPreFilter() bool {
	return g.filterer != nil
}

func (g *geoDatumsToInvertedExpr) PreFilter(
	enc inverted.EncVal, preFilters []interface{}, result []bool,
) (bool, error) {
	return g.filterer.PreFilter(enc, preFilters, result)
}

func (g *geoDatumsToInvertedExpr) String() string {
	return fmt.Sprintf("inverted-expr: %s", g.invertedExpr)
}

func newGeoBoundPreFilterer(typ *types.T, expr tree.TypedExpr) (*PreFilterer, interface{}, error) {
	f, ok := expr.(*tree.FuncExpr)
	if !ok {
		return nil, nil,
			errors.Errorf("pre-filtering only supported for single function expression")
	}
	name := f.Func.FunctionReference.String()
	relationship, ok := geoindex.RelationshipMap[name]
	if !ok {
		return nil, nil, fmt.Errorf("%s cannot be index-accelerated", name)
	}
	if len(f.Exprs) < 2 {
		return nil, nil,
			fmt.Errorf("index-accelerated functions must have at least two arguments")
	}
	// We know that the constant geo parameter is the first parameter, because
	// the optimizer has already commuted the arguments of any function where
	// that was not the case.
	bindParam, ok := f.Exprs[0].(tree.Datum)
	if !ok {
		return nil, nil, fmt.Errorf("first param must be datum")
	}
	if !bindParam.ResolvedType().Equivalent(typ) {
		return nil, nil, fmt.Errorf("bind param type %s should be %s", bindParam, typ)
	}
	var additionalParams []tree.Datum
	for i := 2; i < len(f.Exprs); i++ {
		datum, ok := f.Exprs[i].(tree.Datum)
		if !ok {
			return nil, nil, fmt.Errorf("non constant additional parameter for %s", name)
		}
		additionalParams = append(additionalParams, datum)
	}
	preFilterer := NewPreFilterer(typ, relationship, additionalParams)
	preFilterState := preFilterer.Bind(bindParam)
	return preFilterer, preFilterState, nil
}
