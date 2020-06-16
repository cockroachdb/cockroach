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
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

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
	tree.Datum, geoindex.RelationshipType, cat.Index,
) *invertedexpr.SpanExpression

// tryConstrainGeoIndex tries to derive an inverted index constraint for the
// given geospatial index from the specified filters. If a constraint is
// derived, it is returned with ok=true. If no constraint can be derived,
// then tryConstrainGeoIndex returns ok=false.
func (c *CustomFuncs) tryConstrainGeoIndex(
	filters memo.FiltersExpr, tabID opt.TableID, index cat.Index,
) (invertedConstraint *invertedexpr.SpanExpression, ok bool) {
	config := index.GeoConfig()
	var getSpanExpr getSpanExprForGeoIndexFn
	if geoindex.IsGeographyConfig(config) {
		getSpanExpr = c.getSpanExprForGeographyIndex
	} else if geoindex.IsGeometryConfig(config) {
		getSpanExpr = c.getSpanExprForGeometryIndex
	} else {
		return nil, false
	}

	var invertedExpr invertedexpr.InvertedExpression
	for i := range filters {
		invertedExprLocal := c.constrainGeoIndex(filters[i].Condition, tabID, index, getSpanExpr)
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
func (c *CustomFuncs) getSpanExprForGeographyIndex(
	d tree.Datum, relationship geoindex.RelationshipType, index cat.Index,
) *invertedexpr.SpanExpression {
	geogIdx := geoindex.NewS2GeographyIndex(*index.GeoConfig().S2Geography)
	geog := d.(*tree.DGeography).Geography
	var spanExpr *invertedexpr.SpanExpression

	switch relationship {
	case geoindex.Covers:
		unionKeySpans, err := geogIdx.Covers(c.e.evalCtx.Context, geog)
		if err != nil {
			panic(err)
		}
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.CoveredBy:
		rpKeyExpr, err := geogIdx.CoveredBy(c.e.evalCtx.Context, geog)
		if err != nil {
			panic(err)
		}
		if spanExpr, err = invertedexpr.GeoRPKeyExprToSpanExpr(rpKeyExpr); err != nil {
			panic(err)
		}

	case geoindex.Intersects:
		unionKeySpans, err := geogIdx.Intersects(c.e.evalCtx.Context, geog)
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
func (c *CustomFuncs) getSpanExprForGeometryIndex(
	d tree.Datum, relationship geoindex.RelationshipType, index cat.Index,
) *invertedexpr.SpanExpression {
	geomIdx := geoindex.NewS2GeometryIndex(*index.GeoConfig().S2Geometry)
	geom := d.(*tree.DGeometry).Geometry
	var spanExpr *invertedexpr.SpanExpression

	switch relationship {
	case geoindex.Covers:
		unionKeySpans, err := geomIdx.Covers(c.e.evalCtx.Context, geom)
		if err != nil {
			panic(err)
		}
		spanExpr = invertedexpr.GeoUnionKeySpansToSpanExpr(unionKeySpans)

	case geoindex.CoveredBy:
		rpKeyExpr, err := geomIdx.CoveredBy(c.e.evalCtx.Context, geom)
		if err != nil {
			panic(err)
		}
		if spanExpr, err = invertedexpr.GeoRPKeyExprToSpanExpr(rpKeyExpr); err != nil {
			panic(err)
		}

	case geoindex.Intersects:
		unionKeySpans, err := geomIdx.Intersects(c.e.evalCtx.Context, geom)
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
func (c *CustomFuncs) constrainGeoIndex(
	expr opt.ScalarExpr, tabID opt.TableID, index cat.Index, getSpanExpr getSpanExprForGeoIndexFn,
) (_ invertedexpr.InvertedExpression) {
	var fn *memo.FunctionExpr
	switch t := expr.(type) {
	case *memo.AndExpr:
		return invertedexpr.And(
			c.constrainGeoIndex(t.Left, tabID, index, getSpanExpr),
			c.constrainGeoIndex(t.Right, tabID, index, getSpanExpr),
		)

	case *memo.OrExpr:
		return invertedexpr.Or(
			c.constrainGeoIndex(t.Left, tabID, index, getSpanExpr),
			c.constrainGeoIndex(t.Right, tabID, index, getSpanExpr),
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
	return getSpanExpr(d, relationship, index)
}
