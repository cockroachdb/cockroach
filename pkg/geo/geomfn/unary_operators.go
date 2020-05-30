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

// Centroid returns the Centroid of a given Geometry.
func Centroid(g *geo.Geometry) (*geo.Geometry, error) {
	// Empty geometries do not react well in GEOS, so we have to
	// convert and check beforehand.
	// Remove after #49209 is resolved.
	t, err := g.AsGeomT()
	if err != nil {
		return nil, err
	}
	if t.Empty() {
		return geo.NewGeometryFromGeom(geom.NewPointEmpty(geom.XY))
	}
	centroidEWKB, err := geos.Centroid(g.EWKB())
	if err != nil {
		return nil, err
	}
	return geo.ParseGeometryFromEWKB(centroidEWKB)
}

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

// LineInterpolatePoints returns one or more points along the given
// LineString which are at an integral multiples of given fraction of
// LineString's total length. When repeat is set to false, it returns
// the first point.
func LineInterpolatePoints(g *geo.Geometry, fraction float64, repeat bool) (*geo.Geometry, error) {
	if fraction < 0 || fraction > 1 {
		return nil, errors.Newf("fraction %v should be within [0 1] range", fraction)
	}
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return nil, err
	}
	// Empty geometries do not react well in GEOS, so we have to
	// convert and check beforehand.
	// Remove after #49209 is resolved.
	if geomRepr.Empty() {
		return geo.NewGeometryFromGeom(geom.NewPointEmpty(geom.XY))
	}
	switch geomRepr := geomRepr.(type) {
	case *geom.LineString:
		// In case fraction is greater than 0.5 or equal to 0 or repeat is false,
		// then we will have only one interpolated point.
		if repeat && fraction <= 0.5 && fraction != 0 {
			numberOfInterpolatedPoints := int(1 / fraction)
			interpolatedPoints := geom.NewMultiPoint(geom.XY).SetSRID(geomRepr.SRID())
			for pointInserted := 1; pointInserted <= numberOfInterpolatedPoints; pointInserted++ {
				pointEWKB, err := geos.InterpolateLine(g.EWKB(), float64(pointInserted)*fraction)
				if err != nil {
					return nil, err
				}
				point, err := ewkb.Unmarshal(pointEWKB)
				if err != nil {
					return nil, err
				}
				err = interpolatedPoints.Push(point.(*geom.Point))
				if err != nil {
					return nil, err
				}
			}
			return geo.NewGeometryFromGeom(interpolatedPoints)
		}
		interpolatedPointEWKB, err := geos.InterpolateLine(g.EWKB(), fraction)
		if err != nil {
			return nil, err
		}
		return geo.ParseGeometryFromEWKB(interpolatedPointEWKB)
	default:
		return nil, errors.Newf("Geometry %T should be LineString", geomRepr)
	}
}
