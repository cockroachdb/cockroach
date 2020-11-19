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
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

// Length returns the length of a given Geometry.
// Note only (MULTI)LINESTRING objects have a length.
// (MULTI)POLYGON objects should use Perimeter.
func Length(g geo.Geometry) (float64, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return 0, err
	}
	// Fast path.
	switch geomRepr.(type) {
	case *geom.LineString, *geom.MultiLineString:
		return geos.Length(g.EWKB())
	}
	return lengthFromGeomT(geomRepr)
}

// lengthFromGeomT returns the length from a geom.T, recursing down
// GeometryCollections if required.
func lengthFromGeomT(geomRepr geom.T) (float64, error) {
	// Length in GEOS will also include polygon "perimeters".
	// As such, gate based on on shape underneath.
	switch geomRepr := geomRepr.(type) {
	case *geom.Point, *geom.MultiPoint, *geom.Polygon, *geom.MultiPolygon:
		return 0, nil
	case *geom.LineString, *geom.MultiLineString:
		ewkb, err := ewkb.Marshal(geomRepr, geo.DefaultEWKBEncodingFormat)
		if err != nil {
			return 0, err
		}
		return geos.Length(ewkb)
	case *geom.GeometryCollection:
		total := float64(0)
		for _, subG := range geomRepr.Geoms() {
			subLength, err := lengthFromGeomT(subG)
			if err != nil {
				return 0, err
			}
			total += subLength
		}
		return total, nil
	default:
		return 0, errors.AssertionFailedf("unknown geometry type: %T", geomRepr)
	}
}

// Perimeter returns the perimeter of a given Geometry.
// Note only (MULTI)POLYGON objects have a perimeter.
// (MULTI)LineString objects should use Length.
func Perimeter(g geo.Geometry) (float64, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return 0, err
	}
	// Fast path.
	switch geomRepr.(type) {
	case *geom.Polygon, *geom.MultiPolygon:
		return geos.Length(g.EWKB())
	}
	return perimeterFromGeomT(geomRepr)
}

// perimeterFromGeomT returns the perimeter from a geom.T, recursing down
// GeometryCollections if required.
func perimeterFromGeomT(geomRepr geom.T) (float64, error) {
	// Length in GEOS will also include polygon "perimeters".
	// As such, gate based on on shape underneath.
	switch geomRepr := geomRepr.(type) {
	case *geom.Point, *geom.MultiPoint, *geom.LineString, *geom.MultiLineString:
		return 0, nil
	case *geom.Polygon, *geom.MultiPolygon:
		ewkb, err := ewkb.Marshal(geomRepr, geo.DefaultEWKBEncodingFormat)
		if err != nil {
			return 0, err
		}
		return geos.Length(ewkb)
	case *geom.GeometryCollection:
		total := float64(0)
		for _, subG := range geomRepr.Geoms() {
			subLength, err := perimeterFromGeomT(subG)
			if err != nil {
				return 0, err
			}
			total += subLength
		}
		return total, nil
	default:
		return 0, errors.AssertionFailedf("unknown geometry type: %T", geomRepr)
	}
}

// Area returns the area of a given Geometry.
func Area(g geo.Geometry) (float64, error) {
	return geos.Area(g.EWKB())
}

// Dimension returns the topological dimension of a given Geometry.
func Dimension(g geo.Geometry) (int, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return 0, err
	}
	return dimensionFromGeomT(t)
}

// dimensionFromGeomT returns the dimension from a geom.T, recursing down
// GeometryCollections if required.
func dimensionFromGeomT(geomRepr geom.T) (int, error) {
	switch geomRepr := geomRepr.(type) {
	case *geom.Point, *geom.MultiPoint:
		return 0, nil
	case *geom.LineString, *geom.MultiLineString:
		return 1, nil
	case *geom.Polygon, *geom.MultiPolygon:
		return 2, nil
	case *geom.GeometryCollection:
		maxDim := 0
		for _, g := range geomRepr.Geoms() {
			dim, err := dimensionFromGeomT(g)
			if err != nil {
				return 0, err
			}
			if dim > maxDim {
				maxDim = dim
			}
		}
		return maxDim, nil
	default:
		return 0, errors.AssertionFailedf("unknown geometry type: %T", geomRepr)
	}
}

// Points returns the points of all coordinates in a geometry as a multipoint.
func Points(g geo.Geometry) (geo.Geometry, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	layout := t.Layout()
	if gc, ok := t.(*geom.GeometryCollection); ok && gc.Empty() {
		layout = geom.XY
	}
	points := geom.NewMultiPoint(layout).SetSRID(t.SRID())
	iter := geo.NewGeomTIterator(t, geo.EmptyBehaviorOmit)
	for {
		geomRepr, hasNext, err := iter.Next()
		if err != nil {
			return geo.Geometry{}, err
		} else if !hasNext {
			break
		} else if geomRepr.Empty() {
			continue
		}
		switch geomRepr := geomRepr.(type) {
		case *geom.Point:
			if err = pushCoord(points, geomRepr.Coords()); err != nil {
				return geo.Geometry{}, err
			}
		case *geom.LineString:
			for i := 0; i < geomRepr.NumCoords(); i++ {
				if err = pushCoord(points, geomRepr.Coord(i)); err != nil {
					return geo.Geometry{}, err
				}
			}
		case *geom.Polygon:
			for i := 0; i < geomRepr.NumLinearRings(); i++ {
				linearRing := geomRepr.LinearRing(i)
				for j := 0; j < linearRing.NumCoords(); j++ {
					if err = pushCoord(points, linearRing.Coord(j)); err != nil {
						return geo.Geometry{}, err
					}
				}
			}
		default:
			return geo.Geometry{}, errors.AssertionFailedf("unexpected type: %T", geomRepr)
		}
	}
	return geo.MakeGeometryFromGeomT(points)
}

// pushCoord is a helper function for PointsFromGeomT that appends
// a coordinate to a multipoint as a point.
func pushCoord(points *geom.MultiPoint, coord geom.Coord) error {
	point, err := geom.NewPoint(points.Layout()).SetCoords(coord)
	if err != nil {
		return err
	}
	return points.Push(point)
}

// Normalize returns the geometry in its normalized form.
func Normalize(g geo.Geometry) (geo.Geometry, error) {
	retEWKB, err := geos.Normalize(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// MinimumClearance returns the minimum distance a vertex can move to produce
// an invalid geometry, or infinity if no clearance was found.
func MinimumClearance(g geo.Geometry) (float64, error) {
	return geos.MinimumClearance(g.EWKB())
}

// MinimumClearanceLine returns the line spanning the minimum distance a vertex
// can move to produce an invalid geometry, or an empty line if no clearance was
// found.
func MinimumClearanceLine(g geo.Geometry) (geo.Geometry, error) {
	retEWKB, err := geos.MinimumClearanceLine(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// CountVertices returns a number of vertices (points) for the geom.T provided.
func CountVertices(t geom.T) int {
	switch t := t.(type) {
	case *geom.GeometryCollection:
		// FlatCoords() does not work on GeometryCollection.
		numPoints := 0
		for _, g := range t.Geoms() {
			numPoints += CountVertices(g)
		}
		return numPoints
	default:
		return len(t.FlatCoords()) / t.Stride()
	}
}
