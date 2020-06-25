// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// This file contains functions for building geospatial inverted index scans
// and joins that are used throughout the xform package.

// geoRelationshipMap contains all the geospatial functions that can be index-
// accelerated. Each function implies a certain type of geospatial relationship,
// which affects how the index is queried as part of a constrained scan or
// geospatial lookup join. geoRelationshipMap maps the function name to its
// corresponding relationship (Covers, CoveredBy, or Intersects).
//
// Note that for all of these functions, a geospatial lookup join or constrained
// index scan may produce false positives. Therefore, the original function must
// be called on the output of the index operation to filter the results.
// TODO(rytaft): add ST_DFullyWithin (geoindex.Covers) and ST_DWithin
//  (geoindex.Intersects) once we add support for extending a geometry.
var geoRelationshipMap = map[string]geoindex.RelationshipType{
	"st_covers":           geoindex.Covers,
	"st_coveredby":        geoindex.CoveredBy,
	"st_contains":         geoindex.Covers,
	"st_containsproperly": geoindex.Covers,
	"st_crosses":          geoindex.Intersects,
	"st_equals":           geoindex.Intersects,
	"st_intersects":       geoindex.Intersects,
	"st_overlaps":         geoindex.Intersects,
	"st_touches":          geoindex.Intersects,
	"st_within":           geoindex.CoveredBy,
}

// IsGeoIndexFunction returns true if the given function is a geospatial
// function that can be index-accelerated.
func IsGeoIndexFunction(fn opt.ScalarExpr) bool {
	function := fn.(*memo.FunctionExpr)
	_, ok := geoRelationshipMap[function.Name]
	return ok
}

// getSpanExprForGeoIndexFn is a function that returns a SpanExpression that
// constrains the given geo index according to the given constant and
// geospatial relationship. It is implemented by getSpanExprForGeographyIndex
// and getSpanExprForGeometryIndex and used in constrainGeoIndex.
type getSpanExprForGeoIndexFn func(
	context.Context, tree.Datum, geoindex.RelationshipType, *geoindex.Config,
) *invertedexpr.SpanExpression

// tryConstrainGeoIndex tries to derive an inverted index constraint for the
// given geospatial index from the specified filters. If a constraint is
// derived, it is returned with ok=true. If no constraint can be derived,
// then tryConstrainGeoIndex returns ok=false.
func tryConstrainGeoIndex(
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

// getSpanExprForGeometryIndex gets a SpanExpression that constrains the given
// geometry index according to the given constant and geospatial relationship.
func getSpanExprForGeometryIndex(
	ctx context.Context,
	d tree.Datum,
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
) (_ invertedexpr.InvertedExpression) {
	var fn *memo.FunctionExpr
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
		fn = t

	default:
		return invertedexpr.NonInvertedColExpression{}
	}

	if !IsGeoIndexFunction(fn) {
		return invertedexpr.NonInvertedColExpression{}
	}

	if fn.Args.ChildCount() < 2 {
		panic(errors.AssertionFailedf(
			"all index-accelerated geospatial functions should have at least two arguments",
		))
	}

	// The first argument should be a constant.
	if !memo.CanExtractConstDatum(fn.Args.Child(0)) {
		return invertedexpr.NonInvertedColExpression{}
	}
	d := memo.ExtractConstDatum(fn.Args.Child(0))

	// The second argument should be a variable corresponding to the index
	// column.
	variable, ok := fn.Args.Child(1).(*memo.VariableExpr)
	if !ok {
		// TODO(rytaft): Commute the geospatial function in this case.
		//   Covers      <->  CoveredBy
		//   Intersects  <->  Intersects
		return invertedexpr.NonInvertedColExpression{}
	}
	if variable.Col != tabID.ColumnID(index.Column(0).Ordinal) {
		// The column in the function does not match the index column.
		return invertedexpr.NonInvertedColExpression{}
	}

	relationship := geoRelationshipMap[fn.Name]
	return getSpanExpr(ctx, d, relationship, index.GeoConfig())
}

// geoDatumToInvertedExpr implements invertedexpr.DatumToInvertedExpr for
// geospatial columns.
type geoDatumToInvertedExpr struct {
	relationship geoindex.RelationshipType
	indexConfig  *geoindex.Config
	typ          *types.T
	getSpanExpr  getSpanExprForGeoIndexFn
	alloc        sqlbase.DatumAlloc
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
	relationship, ok := geoRelationshipMap[name]
	if !ok {
		return nil, fmt.Errorf("%s cannot be index-accelerated", name)
	}

	g := &geoDatumToInvertedExpr{
		relationship: relationship,
		indexConfig:  config,
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
	spanExpr := g.getSpanExpr(ctx, d.Datum, g.relationship, g.indexConfig)
	return spanExpr.ToProto(), nil
}

func (g *geoDatumToInvertedExpr) String() string {
	return fmt.Sprintf("geo-relationship: %v", g.relationship)
}
