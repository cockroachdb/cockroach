// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

// Length returns the length of a given Geometry.
// Note only (MULTI)LINESTRING objects have a length.
// (MULTI)POLYGON objects should use Perimeter.
func Length(g *geo.Geometry) (float64, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return 0, err
	}
	// Length in GEOS will also include polygon "perimeters".
	// As such, gate based on on shape underneath.
	switch geomRepr := geomRepr.(type) {
	case *geom.Point, *geom.MultiPoint, *geom.Polygon, *geom.MultiPolygon:
		return 0, nil
	case *geom.LineString, *geom.MultiLineString:
		return geos.Length(g.EWKB())
	case *geom.GeometryCollection:
		total := float64(0)
		for _, subG := range geomRepr.Geoms() {
			switch subG := subG.(type) {
			case *geom.Point, *geom.MultiPoint, *geom.Polygon, *geom.MultiPolygon:
				continue
			case *geom.LineString, *geom.MultiLineString:
				subGEWKB, err := ewkb.Marshal(subG, geo.DefaultEWKBEncodingFormat)
				if err != nil {
					return 0, err
				}
				length, err := geos.Length(geopb.EWKB(subGEWKB))
				if err != nil {
					return 0, err
				}
				total += length
			default:
				return 0, errors.AssertionFailedf("unknown geometry type in GeometryCollection: %T", subG)
			}
		}
		return total, nil
	default:
		return 0, errors.AssertionFailedf("unknown geometry type: %T", geomRepr)
	}
}

// Perimeter returns the perimeter of a given Geometry.
// Note only (MULTI)POLYGON objects have a perimeter.
// (MULTI)LineString objects should use Length.
func Perimeter(g *geo.Geometry) (float64, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return 0, err
	}
	switch geomRepr := geomRepr.(type) {
	case *geom.Point, *geom.MultiPoint, *geom.LineString, *geom.MultiLineString:
		return 0, nil
	case *geom.Polygon, *geom.MultiPolygon:
		return geos.Length(g.EWKB())
	case *geom.GeometryCollection:
		total := float64(0)
		for _, subG := range geomRepr.Geoms() {
			switch subG := subG.(type) {
			case *geom.Point, *geom.MultiPoint, *geom.LineString, *geom.MultiLineString:
				continue
			case *geom.Polygon, *geom.MultiPolygon:
				subGEWKB, err := ewkb.Marshal(subG, geo.DefaultEWKBEncodingFormat)
				if err != nil {
					return 0, err
				}
				perimeter, err := geos.Length(geopb.EWKB(subGEWKB))
				if err != nil {
					return 0, err
				}
				total += perimeter
			default:
				return 0, errors.AssertionFailedf("unknown geometry type in GeometryCollection: %T", subG)
			}
		}
		return total, nil
	default:
		return 0, errors.AssertionFailedf("unknown geometry type: %T", geomRepr)
	}
}

// Area returns the area of a given Geometry.
func Area(g *geo.Geometry) (float64, error) {
	return geos.Area(g.EWKB())
}
