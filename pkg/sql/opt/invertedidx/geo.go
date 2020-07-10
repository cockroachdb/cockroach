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

// IsGeoIndexFunction returns true if the given function is a geospatial
// function that can be index-accelerated.
func IsGeoIndexFunction(fn opt.ScalarExpr) bool {
	function := fn.(*memo.FunctionExpr)
	_, ok := geoindex.RelationshipMap[function.Name]
	return ok
}

// TryGetInvertedJoinCondFromGeoFunc tries to extract an inverted join
// condition from the given geospatial function. If commuteArgs is true,
// TryGetInvertedJoinCondFromGeoFunc tries to extract an inverted join
// condition from an equivalent version of the given function in which the
// first two arguments are swapped.
//
// Returns the original function if commuteArgs is false, or a new function
// representing the same relationship but with commuted arguments if
// commuteArgs is true. For example:
//
//   ST_Intersects(g1, g2) <-> ST_Intersects(g2, g1)
//   ST_Covers(g1, g2) <-> ST_CoveredBy(g2, g1)
//
// See geoindex.CommuteRelationshipMap for the full list of mappings.
//
// Also returns the column ID corresponding to the input geospatial column
// as well as the column ID corresponding to the inverted index key column,
// which are the two columns relevant for the join condition. Returns ok=true
// if a join condition was successfully extracted or ok=false otherwise.
//
// The given function must be a valid geospatial function that can be index-
// accelerated, and must contain two variable arguments. Otherwise,
// TryGetInvertedJoinCondFromGeoFunc will panic.
func TryGetInvertedJoinCondFromGeoFunc(
	factory *norm.Factory, fn opt.ScalarExpr, commuteArgs bool, inputProps *props.Relational,
) (_ opt.ScalarExpr, inputGeoCol, indexGeoCol opt.ColumnID, ok bool) {
	if !IsGeoIndexFunction(fn) {
		panic(errors.AssertionFailedf(
			"TryGetInvertedJoinCondFromGeoFunc called on a function that cannot be index-accelerated",
		))
	}
	function := fn.(*memo.FunctionExpr)

	// Extract the the variable inputs to the geospatial function.
	if function.Args.ChildCount() < 2 {
		panic(errors.AssertionFailedf(
			"all index-accelerated geospatial functions should have at least two arguments",
		))
	}

	arg1, arg2 := function.Args.Child(0), function.Args.Child(1)
	if commuteArgs {
		arg1, arg2 = arg2, arg1
	}

	// The first argument should come from the input.
	variable, ok := arg1.(*memo.VariableExpr)
	if !ok {
		panic(errors.AssertionFailedf(
			"TryGetInvertedJoinCondFromGeoFunc called on function containing non-variable inputs",
		))
	}
	if !inputProps.OutputCols.Contains(variable.Col) {
		return nil, 0, 0, false
	}
	inputGeoCol = variable.Col

	// The second argument should be a variable corresponding to the index
	// column. The caller must verify that this column is actually the key column
	// of an inverted geospatial index.
	variable, ok = arg2.(*memo.VariableExpr)
	if !ok {
		panic(errors.AssertionFailedf(
			"TryGetInvertedJoinCondFromGeoFunc called on function containing non-variable inputs",
		))
	}
	indexGeoCol = variable.Col

	if commuteArgs {
		// Get the geospatial relationship that is equivalent to this one with the
		// arguments commuted, and construct a new function that represents that
		// relationship.
		rel := geoindex.RelationshipMap[function.Name]
		commutedRel, ok := geoindex.CommuteRelationshipMap[rel]
		if !ok {
			// It's not possible to commute this relationship.
			return nil, 0, 0, false
		}
		if rel != commutedRel {
			// If the commuted relationship is not the same as the original
			// relationship, we can't use the same function overload.
			name := geoindex.RelationshipReverseMap[commutedRel]

			// Copy the original arguments into a new list, and swap the first two
			// arguments.
			args := make(memo.ScalarListExpr, len(function.Args))
			copy(args, function.Args)
			args[0], args[1] = args[1], args[0]

			props, overload, ok := memo.FindFunction(&args, name)
			if !ok {
				panic(errors.AssertionFailedf("could not find overload for %s", name))
			}
			fn = factory.ConstructFunction(args, &memo.FunctionPrivate{
				Name:       name,
				Typ:        function.Typ,
				Properties: props,
				Overload:   overload,
			})
		}
	}

	return fn, inputGeoCol, indexGeoCol, true
}

// getSpanExprForGeoIndexFn is a function that returns a SpanExpression that
// constrains the given geo index according to the given constant and
// geospatial relationship. It is implemented by getSpanExprForGeographyIndex
// and getSpanExprForGeometryIndex and used in constrainGeoIndex.
type getSpanExprForGeoIndexFn func(
	context.Context, tree.Datum, []tree.Datum, geoindex.RelationshipType, *geoindex.Config,
) *invertedexpr.SpanExpression

// TryConstrainGeoIndex tries to derive an inverted index constraint for the
// given geospatial index from the specified filters. If a constraint is
// derived, it is returned with ok=true. If no constraint can be derived,
// then TryConstrainGeoIndex returns ok=false.
func TryConstrainGeoIndex(
	ctx context.Context, filters memo.FiltersExpr, tabID opt.TableID, index cat.Index,
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
			ctx, filters[i].Condition, tabID, index, getSpanExpr,
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

// constrainGeoIndex returns an InvertedExpression representing a constraint
// of the given geospatial index.
func constrainGeoIndex(
	ctx context.Context,
	expr opt.ScalarExpr,
	tabID opt.TableID,
	index cat.Index,
	getSpanExpr getSpanExprForGeoIndexFn,
) invertedexpr.InvertedExpression {
	switch t := expr.(type) {
	case *memo.AndExpr:
		return invertedexpr.And(
			constrainGeoIndex(ctx, t.Left, tabID, index, getSpanExpr),
			constrainGeoIndex(ctx, t.Right, tabID, index, getSpanExpr),
		)

	case *memo.OrExpr:
		return invertedexpr.Or(
			constrainGeoIndex(ctx, t.Left, tabID, index, getSpanExpr),
			constrainGeoIndex(ctx, t.Right, tabID, index, getSpanExpr),
		)

	case *memo.FunctionExpr:
		// Try to constrain the index with the given function. If the resulting
		// inverted expression is not a SpanExpression, try constraining the index
		// with an equivalent function in which the arguments are commuted. For
		// example:
		//
		//   ST_Intersects(g1, g2) <-> ST_Intersects(g2, g1)
		//   ST_Covers(g1, g2) <-> ST_CoveredBy(g2, g1)
		//
		// See geoindex.CommuteRelationshipMap for the full list of mappings.
		invertedExpr := constrainGeoIndexFromFunction(
			ctx, t, false /* commuteArgs */, tabID, index, getSpanExpr,
		)
		if _, ok := invertedExpr.(invertedexpr.NonInvertedColExpression); ok {
			invertedExpr = constrainGeoIndexFromFunction(
				ctx, t, true /* commuteArgs */, tabID, index, getSpanExpr,
			)
		}
		return invertedExpr

	default:
		return invertedexpr.NonInvertedColExpression{}
	}
}

// constrainGeoIndexFromFunction returns an InvertedExpression representing a
// constraint of the given geospatial index, based on the given function.
// If commuteArgs is true, constrainGeoIndexFromFunction constrains the index
// based on an equivalent version of the given function in which the first two
// arguments are swapped.
func constrainGeoIndexFromFunction(
	ctx context.Context,
	fn *memo.FunctionExpr,
	commuteArgs bool,
	tabID opt.TableID,
	index cat.Index,
	getSpanExpr getSpanExprForGeoIndexFn,
) invertedexpr.InvertedExpression {
	if !IsGeoIndexFunction(fn) {
		return invertedexpr.NonInvertedColExpression{}
	}

	if fn.Args.ChildCount() < 2 {
		panic(errors.AssertionFailedf(
			"all index-accelerated geospatial functions should have at least two arguments",
		))
	}

	arg1, arg2 := fn.Args.Child(0), fn.Args.Child(1)
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
	if variable.Col != tabID.ColumnID(index.Column(0).Ordinal) {
		// The column in the function does not match the index column.
		return invertedexpr.NonInvertedColExpression{}
	}

	// Any additional params must be constant.
	var additionalParams []tree.Datum
	for i := 2; i < fn.Args.ChildCount(); i++ {
		if !memo.CanExtractConstDatum(fn.Args.Child(i)) {
			return invertedexpr.NonInvertedColExpression{}
		}
		additionalParams = append(additionalParams, memo.ExtractConstDatum(fn.Args.Child(i)))
	}

	relationship := geoindex.RelationshipMap[fn.Name]
	if commuteArgs {
		relationship, ok = geoindex.CommuteRelationshipMap[relationship]
		if !ok {
			// It's not possible to commute this relationship.
			return invertedexpr.NonInvertedColExpression{}
		}
	}

	return getSpanExpr(ctx, d, additionalParams, relationship, index.GeoConfig())
}

// geoDatumToInvertedExpr implements invertedexpr.DatumToInvertedExpr for
// geospatial columns.
type geoDatumToInvertedExpr struct {
	relationship     geoindex.RelationshipType
	additionalParams []tree.Datum
	indexConfig      *geoindex.Config
	typ              *types.T
	getSpanExpr      getSpanExprForGeoIndexFn
	alloc            sqlbase.DatumAlloc
}

var _ invertedexpr.DatumToInvertedExpr = &geoDatumToInvertedExpr{}

// NewGeoDatumToInvertedExpr returns a new geoDatumToInvertedExpr.
func NewGeoDatumToInvertedExpr(
	expr tree.TypedExpr, config *geoindex.Config,
) (invertedexpr.DatumToInvertedExpr, error) {
	if geoindex.IsEmptyConfig(config) {
		return nil, fmt.Errorf("inverted joins are currently only supported for geospatial indexes")
	}

	fn, ok := expr.(*tree.FuncExpr)
	if !ok {
		return nil, fmt.Errorf("inverted joins are currently only supported for single geospatial functions")
	}

	name := fn.Func.FunctionReference.String()
	relationship, ok := geoindex.RelationshipMap[name]
	if !ok {
		return nil, fmt.Errorf("%s cannot be index-accelerated", name)
	}

	var additionalParams []tree.Datum
	for i := 2; i < len(fn.Exprs); i++ {
		datum, ok := fn.Exprs[i].(tree.Datum)
		if !ok {
			return nil, fmt.Errorf("non constant additional parameter for %s", name)
		}
		additionalParams = append(additionalParams, datum)
	}

	g := &geoDatumToInvertedExpr{
		relationship:     relationship,
		additionalParams: additionalParams,
		indexConfig:      config,
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

	return g, nil
}

// Convert implements the invertedexpr.DatumToInvertedExpr interface.
func (g *geoDatumToInvertedExpr) Convert(
	ctx context.Context, d sqlbase.EncDatum,
) (*invertedexpr.SpanExpressionProto, error) {
	if err := d.EnsureDecoded(g.typ, &g.alloc); err != nil {
		return nil, err
	}
	spanExpr := g.getSpanExpr(ctx, d.Datum, g.additionalParams, g.relationship, g.indexConfig)
	return spanExpr.ToProto(), nil
}

func (g *geoDatumToInvertedExpr) String() string {
	return fmt.Sprintf("geo-relationship: %v", g.relationship)
}
