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

	"github.com/cockroachdb/cockroach/pkg/geo/geogfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
// and getSpanExprForGeometryIndex and used in constrainGeoIndex.
type getSpanExprForGeoIndexFn func(
	context.Context, tree.Datum, []tree.Datum, geoindex.RelationshipType, *geoindex.Config,
) *invertedexpr.SpanExpression

// TryJoinGeoIndex tries to create an inverted join with the given input and
// geospatial index from the specified filters. If a join is created, the
// inverted join condition is returned. If no join can be created, then
// TryJoinGeoIndex returns nil.
func TryJoinGeoIndex(
	ctx context.Context,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	inputCols opt.ColSet,
) opt.ScalarExpr {
	config := index.GeoConfig()
	var getSpanExpr getSpanExprForGeoIndexFn
	if geoindex.IsGeographyConfig(config) {
		getSpanExpr = getSpanExprForGeographyIndex
	} else if geoindex.IsGeometryConfig(config) {
		getSpanExpr = getSpanExprForGeometryIndex
	} else {
		return nil
	}

	var invertedExpr opt.ScalarExpr
	for i := range filters {
		invertedExprLocal := joinGeoIndex(
			ctx, factory, filters[i].Condition, tabID, index, inputCols, getSpanExpr,
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

// TryConstrainGeoIndex tries to derive an inverted index constraint for the
// given geospatial index from the specified filters. If a constraint is
// derived, it is returned with ok=true. If no constraint can be derived,
// then TryConstrainGeoIndex returns ok=false.
func TryConstrainGeoIndex(
	ctx context.Context,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
) (invertedConstraint *invertedexpr.SpanExpression, ok bool) {
	config := index.GeoConfig()
	var getSpanExpr getSpanExprForGeoIndexFn
	if geoindex.IsGeographyConfig(config) {
		getSpanExpr = getSpanExprForGeographyIndex
	} else if geoindex.IsGeometryConfig(config) {
		getSpanExpr = getSpanExprForGeometryIndex
	} else {
		return nil, false
	}

	var invertedExpr invertedexpr.InvertedExpression
	for i := range filters {
		invertedExprLocal := constrainGeoIndex(
			ctx, factory, filters[i].Condition, tabID, index, getSpanExpr,
		)
		if invertedExpr == nil {
			invertedExpr = invertedExprLocal
		} else {
			invertedExpr = invertedexpr.And(invertedExpr, invertedExprLocal)
		}
	}

	if invertedExpr == nil {
		return nil, false
	}

	spanExpr, ok := invertedExpr.(*invertedexpr.SpanExpression)
	if !ok {
		return nil, false
	}

	return spanExpr, true
}

// getSpanExprForGeographyIndex gets a SpanExpression that constrains the given
// geography index according to the given constant and geospatial relationship.
func getSpanExprForGeographyIndex(
	ctx context.Context,
	d tree.Datum,
	additionalParams []tree.Datum,
	relationship geoindex.RelationshipType,
	indexConfig *geoindex.Config,
) *invertedexpr.SpanExpression {
	geogIdx := geoindex.NewS2GeographyIndex(*indexConfig.S2Geography)
	geog := d.(*tree.DGeography).Geography
	var spanExpr *invertedexpr.SpanExpression

	switch relationship {
	case geoindex.Covers:
		unionKeySpans, err := geogIdx.Covers(ctx, geog)
		if err != nil {
			panic(err)
		}
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.CoveredBy:
		rpKeyExpr, err := geogIdx.CoveredBy(ctx, geog)
		if err != nil {
			panic(err)
		}
		if spanExpr, err = invertedexpr.GeoRPKeyExprToSpanExpr(rpKeyExpr); err != nil {
			panic(err)
		}

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
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.Intersects:
		unionKeySpans, err := geogIdx.Intersects(ctx, geog)
		if err != nil {
			panic(err)
		}
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	default:
		panic(errors.AssertionFailedf("unhandled relationship: %v", relationship))
	}

	return spanExpr
}

// Helper for DWithin and DFullyWithin.
func getDistanceParam(params []tree.Datum) float64 {
	// Parameters are type checked earlier. Keep this consistent with the definition
	// in geo_builtins.go.
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
	indexConfig *geoindex.Config,
) *invertedexpr.SpanExpression {
	geomIdx := geoindex.NewS2GeometryIndex(*indexConfig.S2Geometry)
	geom := d.(*tree.DGeometry).Geometry
	var spanExpr *invertedexpr.SpanExpression

	switch relationship {
	case geoindex.Covers:
		unionKeySpans, err := geomIdx.Covers(ctx, geom)
		if err != nil {
			panic(err)
		}
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.CoveredBy:
		rpKeyExpr, err := geomIdx.CoveredBy(ctx, geom)
		if err != nil {
			panic(err)
		}
		if spanExpr, err = invertedexpr.GeoRPKeyExprToSpanExpr(rpKeyExpr); err != nil {
			panic(err)
		}

	case geoindex.DFullyWithin:
		distance := getDistanceParam(additionalParams)
		unionKeySpans, err := geomIdx.DFullyWithin(ctx, geom, distance)
		if err != nil {
			panic(err)
		}
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.DWithin:
		distance := getDistanceParam(additionalParams)
		unionKeySpans, err := geomIdx.DWithin(ctx, geom, distance)
		if err != nil {
			panic(err)
		}
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.Intersects:
		unionKeySpans, err := geomIdx.Intersects(ctx, geom)
		if err != nil {
			panic(err)
		}
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	default:
		panic(errors.AssertionFailedf("unhandled relationship: %v", relationship))
	}

	return spanExpr
}

// joinGeoIndex extracts a scalar expression from the given filter condition,
// where the scalar expression represents a join condition between the given
// input columns and geospatial index. Returns nil if no join condition could
// be extracted.
func joinGeoIndex(
	ctx context.Context,
	factory *norm.Factory,
	filterCond opt.ScalarExpr,
	tabID opt.TableID,
	index cat.Index,
	inputCols opt.ColSet,
	getSpanExpr getSpanExprForGeoIndexFn,
) opt.ScalarExpr {
	var args memo.ScalarListExpr
	switch t := filterCond.(type) {
	case *memo.AndExpr:
		leftExpr := joinGeoIndex(ctx, factory, t.Left, tabID, index, inputCols, getSpanExpr)
		rightExpr := joinGeoIndex(ctx, factory, t.Right, tabID, index, inputCols, getSpanExpr)
		if leftExpr == nil {
			return rightExpr
		}
		if rightExpr == nil {
			return leftExpr
		}
		return factory.ConstructAnd(leftExpr, rightExpr)

	case *memo.OrExpr:
		leftExpr := joinGeoIndex(ctx, factory, t.Left, tabID, index, inputCols, getSpanExpr)
		rightExpr := joinGeoIndex(ctx, factory, t.Right, tabID, index, inputCols, getSpanExpr)
		if leftExpr == nil || rightExpr == nil {
			return nil
		}
		return factory.ConstructOr(leftExpr, rightExpr)

	case *memo.FunctionExpr:
		args = t.Args

	case *memo.BBoxCoversExpr, *memo.BBoxIntersectsExpr:
		args = memo.ScalarListExpr{
			t.Child(0).(opt.ScalarExpr), t.Child(1).(opt.ScalarExpr),
		}
		// Cast the arguments to type Geometry if they are type Box2d.
		for i := 0; i < len(args); i++ {
			if args[i].DataType().Family() == types.Box2DFamily {
				args[i] = factory.ConstructCast(args[i], types.Geometry)
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
	// See joinGeoIndexFromExpr for more details.
	fn := joinGeoIndexFromExpr(
		factory, filterCond, args, false /* commuteArgs */, inputCols, tabID, index,
	)
	if fn == nil {
		fn = joinGeoIndexFromExpr(
			factory, filterCond, args, true /* commuteArgs */, inputCols, tabID, index,
		)
	}
	return fn
}

// joinGeoIndexFromExpr tries to extract an inverted join condition from the
// given expression, which should be either a function or comparison operation.
// If commuteArgs is true, joinGeoIndexFromExpr tries to extract an inverted
// join condition from an equivalent version of the given expression in which
// the first two arguments are swapped.
//
// If commuteArgs is false, returns the original function (if the expression
// was a function) or a new function representing the geospatial relationship
// of the comparison operation. If commuteArgs is true, returns a new function
// representing the same relationship but with commuted arguments. For example:
//
//   ST_Intersects(g1, g2) <-> ST_Intersects(g2, g1)
//   ST_Covers(g1, g2) <-> ST_CoveredBy(g2, g1)
//   g1 && g2 -> ST_Intersects(g2, g1)
//   g1 ~ g2 -> ST_CoveredBy(g2, g1)
//
// See geoindex.CommuteRelationshipMap for the full list of mappings.
//
// Returns nil if a join condition was not successfully extracted.
func joinGeoIndexFromExpr(
	factory *norm.Factory,
	expr opt.ScalarExpr,
	args memo.ScalarListExpr,
	commuteArgs bool,
	inputCols opt.ColSet,
	tabID opt.TableID,
	index cat.Index,
) opt.ScalarExpr {
	rel, ok := GetGeoIndexRelationship(expr)
	if !ok {
		return nil
	}

	// Extract the the inputs to the geospatial function.
	if args.ChildCount() < 2 {
		panic(errors.AssertionFailedf(
			"all index-accelerated geospatial functions should have at least two arguments",
		))
	}

	arg1, arg2 := args.Child(0), args.Child(1)
	if commuteArgs {
		arg1, arg2 = arg2, arg1
	}

	// The first argument should either come from the input or be a constant.
	var p props.Shared
	memo.BuildSharedProps(arg1, &p)
	if !p.OuterCols.Empty() {
		if !p.OuterCols.SubsetOf(inputCols) {
			return nil
		}
	} else if !memo.CanExtractConstDatum(arg1) {
		return nil
	}

	// The second argument should be a variable corresponding to the index
	// column.
	variable, ok := arg2.(*memo.VariableExpr)
	if !ok {
		return nil
	}
	if variable.Col != tabID.ColumnID(index.Column(0).InvertedSourceColumnOrdinal()) {
		// The column in the function does not match the index column.
		return nil
	}

	// Any additional params must be constant.
	for i := 2; i < args.ChildCount(); i++ {
		if !memo.CanExtractConstDatum(args.Child(i)) {
			return nil
		}
	}

	if commuteArgs {
		// Get the geospatial relationship that is equivalent to this one with the
		// arguments commuted, and construct a new function that represents that
		// relationship.
		commutedRel, ok := geoindex.CommuteRelationshipMap[rel]
		if !ok {
			// It's not possible to commute this relationship.
			return nil
		}

		name := geoindex.RelationshipReverseMap[commutedRel]

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
		name := geoindex.RelationshipReverseMap[rel]
		return constructFunction(factory, name, args)
	}

	return expr
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

// constrainGeoIndex returns an InvertedExpression representing a constraint
// of the given geospatial index.
func constrainGeoIndex(
	ctx context.Context,
	factory *norm.Factory,
	expr opt.ScalarExpr,
	tabID opt.TableID,
	index cat.Index,
	getSpanExpr getSpanExprForGeoIndexFn,
) invertedexpr.InvertedExpression {
	var args memo.ScalarListExpr
	switch t := expr.(type) {
	case *memo.AndExpr:
		return invertedexpr.And(
			constrainGeoIndex(ctx, factory, t.Left, tabID, index, getSpanExpr),
			constrainGeoIndex(ctx, factory, t.Right, tabID, index, getSpanExpr),
		)

	case *memo.OrExpr:
		return invertedexpr.Or(
			constrainGeoIndex(ctx, factory, t.Left, tabID, index, getSpanExpr),
			constrainGeoIndex(ctx, factory, t.Right, tabID, index, getSpanExpr),
		)

	case *memo.FunctionExpr:
		args = t.Args

	case *memo.BBoxCoversExpr, *memo.BBoxIntersectsExpr:
		args = memo.ScalarListExpr{
			t.Child(0).(opt.ScalarExpr), t.Child(1).(opt.ScalarExpr),
		}
		// Cast the arguments to type Geometry if they are type Box2d.
		for i := 0; i < len(args); i++ {
			if args[i].DataType().Family() == types.Box2DFamily {
				args[i] = factory.ConstructCast(args[i], types.Geometry)
			}
		}

	default:
		return invertedexpr.NonInvertedColExpression{}
	}

	// Try to constrain the index with the given expression. If the resulting
	// inverted expression is not a SpanExpression, try constraining the index
	// with an equivalent function in which the arguments are commuted. For
	// example:
	//
	//   ST_Intersects(g1, g2) <-> ST_Intersects(g2, g1)
	//   ST_Covers(g1, g2) <-> ST_CoveredBy(g2, g1)
	//   g1 && g2 -> ST_Intersects(g2, g1)
	//   g1 ~ g2 -> ST_CoveredBy(g2, g1)
	//
	// See geoindex.CommuteRelationshipMap for the full list of mappings.
	invertedExpr := constrainGeoIndexFromExpr(
		ctx, expr, args, false /* commuteArgs */, tabID, index, getSpanExpr,
	)
	if _, ok := invertedExpr.(invertedexpr.NonInvertedColExpression); ok {
		invertedExpr = constrainGeoIndexFromExpr(
			ctx, expr, args, true /* commuteArgs */, tabID, index, getSpanExpr,
		)
	}
	return invertedExpr
}

// constrainGeoIndexFromExpr returns an InvertedExpression representing a
// constraint of the given geospatial index, based on the given expression.
// If commuteArgs is true, constrainGeoIndexFromExpr constrains the index
// based on an equivalent version of the given expression in which the first
// two arguments are swapped.
func constrainGeoIndexFromExpr(
	ctx context.Context,
	expr opt.ScalarExpr,
	args memo.ScalarListExpr,
	commuteArgs bool,
	tabID opt.TableID,
	index cat.Index,
	getSpanExpr getSpanExprForGeoIndexFn,
) invertedexpr.InvertedExpression {
	relationship, ok := GetGeoIndexRelationship(expr)
	if !ok {
		return invertedexpr.NonInvertedColExpression{}
	}

	if args.ChildCount() < 2 {
		panic(errors.AssertionFailedf(
			"all index-accelerated geospatial functions should have at least two arguments",
		))
	}

	arg1, arg2 := args.Child(0), args.Child(1)
	if commuteArgs {
		arg1, arg2 = arg2, arg1
	}

	// The first argument should be a constant.
	if !memo.CanExtractConstDatum(arg1) {
		return invertedexpr.NonInvertedColExpression{}
	}
	d := memo.ExtractConstDatum(arg1)

	// The second argument should be a variable corresponding to the index
	// column.
	variable, ok := arg2.(*memo.VariableExpr)
	if !ok {
		return invertedexpr.NonInvertedColExpression{}
	}
	if variable.Col != tabID.ColumnID(index.Column(0).InvertedSourceColumnOrdinal()) {
		// The column in the function does not match the index column.
		return invertedexpr.NonInvertedColExpression{}
	}

	// Any additional params must be constant.
	var additionalParams []tree.Datum
	for i := 2; i < args.ChildCount(); i++ {
		if !memo.CanExtractConstDatum(args.Child(i)) {
			return invertedexpr.NonInvertedColExpression{}
		}
		additionalParams = append(additionalParams, memo.ExtractConstDatum(args.Child(i)))
	}

	if commuteArgs {
		relationship, ok = geoindex.CommuteRelationshipMap[relationship]
		if !ok {
			// It's not possible to commute this relationship.
			return invertedexpr.NonInvertedColExpression{}
		}
	}

	return getSpanExpr(ctx, d, additionalParams, relationship, index.GeoConfig())
}

type geoInvertedExpr struct {
	tree.FuncExpr

	relationship     geoindex.RelationshipType
	nonIndexParam    tree.TypedExpr
	additionalParams []tree.Datum

	// spanExpr is the result of evaluating the geospatial relationship
	// represented by this geoInvertedExpr. It is nil prior to evaluation.
	spanExpr *invertedexpr.SpanExpression
}

var _ tree.TypedExpr = &geoInvertedExpr{}

// geoDatumsToInvertedExpr implements invertedexpr.DatumsToInvertedExpr for
// geospatial columns.
type geoDatumsToInvertedExpr struct {
	evalCtx      *tree.EvalContext
	colTypes     []*types.T
	invertedExpr tree.TypedExpr
	indexConfig  *geoindex.Config
	typ          *types.T
	getSpanExpr  getSpanExprForGeoIndexFn

	row   sqlbase.EncDatumRow
	alloc sqlbase.DatumAlloc
}

var _ invertedexpr.DatumsToInvertedExpr = &geoDatumsToInvertedExpr{}
var _ tree.IndexedVarContainer = &geoDatumsToInvertedExpr{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (g *geoDatumsToInvertedExpr) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	err := g.row[idx].EnsureDecoded(g.colTypes[idx], &g.alloc)
	if err != nil {
		return nil, err
	}
	return g.row[idx].Datum.Eval(ctx)
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (g *geoDatumsToInvertedExpr) IndexedVarResolvedType(idx int) *types.T {
	return g.colTypes[idx]
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (g *geoDatumsToInvertedExpr) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

// NewGeoDatumsToInvertedExpr returns a new geoDatumsToInvertedExpr.
func NewGeoDatumsToInvertedExpr(
	evalCtx *tree.EvalContext, colTypes []*types.T, expr tree.TypedExpr, config *geoindex.Config,
) (invertedexpr.DatumsToInvertedExpr, error) {
	if geoindex.IsEmptyConfig(config) {
		return nil, fmt.Errorf("inverted joins are currently only supported for geospatial indexes")
	}

	g := &geoDatumsToInvertedExpr{
		evalCtx:     evalCtx,
		colTypes:    colTypes,
		indexConfig: config,
	}
	if geoindex.IsGeographyConfig(config) {
		g.typ = types.Geography
		g.getSpanExpr = getSpanExprForGeographyIndex
	} else if geoindex.IsGeometryConfig(config) {
		g.typ = types.Geometry
		g.getSpanExpr = getSpanExprForGeometryIndex
	} else {
		panic(errors.AssertionFailedf("not a geography or geometry index"))
	}

	// getInvertedExpr takes a TypedExpr tree consisting of And, Or and Func
	// expressions, and constructs a new TypedExpr tree consisting of And, Or and
	// geoInvertedExpr expressions. The geoInvertedExpr serves to improve the
	// performance of geoDatumsToInvertedExpr.Convert by reducing the amount of
	// computation needed to convert an input row to a SpanExpression. It does
	// this by caching the geospatial relationship of each function, and pre-
	// computing and caching the SpanExpressions for any functions that have a
	// constant as the non-indexed argument.
	var getInvertedExpr func(expr tree.TypedExpr) (tree.TypedExpr, error)
	getInvertedExpr = func(expr tree.TypedExpr) (tree.TypedExpr, error) {
		switch t := expr.(type) {
		case *tree.AndExpr:
			leftExpr, err := getInvertedExpr(t.TypedLeft())
			if err != nil {
				return nil, err
			}
			rightExpr, err := getInvertedExpr(t.TypedRight())
			if err != nil {
				return nil, err
			}
			return tree.NewTypedAndExpr(leftExpr, rightExpr), nil

		case *tree.OrExpr:
			leftExpr, err := getInvertedExpr(t.TypedLeft())
			if err != nil {
				return nil, err
			}
			rightExpr, err := getInvertedExpr(t.TypedRight())
			if err != nil {
				return nil, err
			}
			return tree.NewTypedOrExpr(leftExpr, rightExpr), nil

		case *tree.FuncExpr:
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
			// was not the case. See joinGeoIndexFromExpr for details.
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
			var spanExpr *invertedexpr.SpanExpression
			if d, ok := nonIndexParam.(tree.Datum); ok {
				spanExpr = g.getSpanExpr(evalCtx.Ctx(), d, additionalParams, relationship, g.indexConfig)
			}

			return &geoInvertedExpr{
				FuncExpr:         *t,
				relationship:     relationship,
				nonIndexParam:    nonIndexParam,
				additionalParams: additionalParams,
				spanExpr:         spanExpr,
			}, nil

		default:
			return nil, fmt.Errorf("unsupported expression %v", t)
		}
	}

	var err error
	g.invertedExpr, err = getInvertedExpr(expr)
	if err != nil {
		return nil, err
	}

	return g, nil
}

// Convert implements the invertedexpr.DatumsToInvertedExpr interface.
func (g *geoDatumsToInvertedExpr) Convert(
	ctx context.Context, datums sqlbase.EncDatumRow,
) (*invertedexpr.SpanExpressionProto, error) {
	g.row = datums
	g.evalCtx.PushIVarContainer(g)
	defer g.evalCtx.PopIVarContainer()

	var evalInvertedExpr func(expr tree.TypedExpr) (invertedexpr.InvertedExpression, error)
	evalInvertedExpr = func(expr tree.TypedExpr) (invertedexpr.InvertedExpression, error) {
		switch t := expr.(type) {
		case *tree.AndExpr:
			leftExpr, err := evalInvertedExpr(t.TypedLeft())
			if err != nil {
				return nil, err
			}
			rightExpr, err := evalInvertedExpr(t.TypedRight())
			if err != nil {
				return nil, err
			}
			if leftExpr == nil || rightExpr == nil {
				return nil, nil
			}
			return invertedexpr.And(leftExpr, rightExpr), nil

		case *tree.OrExpr:
			leftExpr, err := evalInvertedExpr(t.TypedLeft())
			if err != nil {
				return nil, err
			}
			rightExpr, err := evalInvertedExpr(t.TypedRight())
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

		case *geoInvertedExpr:
			if t.spanExpr != nil {
				return t.spanExpr, nil
			}
			d, err := t.nonIndexParam.Eval(g.evalCtx)
			if err != nil {
				return nil, err
			}
			if d == tree.DNull {
				return nil, nil
			}
			return g.getSpanExpr(ctx, d, t.additionalParams, t.relationship, g.indexConfig), nil

		default:
			return nil, fmt.Errorf("unsupported expression %v", t)
		}
	}

	invertedExpr, err := evalInvertedExpr(g.invertedExpr)
	if err != nil {
		return nil, err
	}

	if invertedExpr == nil {
		return nil, nil
	}

	spanExpr, ok := invertedExpr.(*invertedexpr.SpanExpression)
	if !ok {
		return nil, fmt.Errorf("unable to construct span expression")
	}

	return spanExpr.ToProto(), nil
}

func (g *geoDatumsToInvertedExpr) String() string {
	return fmt.Sprintf("inverted-expr: %s", g.invertedExpr)
}
