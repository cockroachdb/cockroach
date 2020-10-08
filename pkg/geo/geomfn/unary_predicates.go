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
)

// IsClosed returns whether the given geometry has equal start and end points.
// Collections and multi-types must contain all-closed geometries. Empty
// geometries are not closed.
func IsClosed(g geo.Geometry) (bool, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return false, err
	}
	return isClosedFromGeomT(t)
}

// isClosedFromGeomT returns whether the given geom.T is closed, recursing
// into collections.
func isClosedFromGeomT(t geom.T) (bool, error) {
	if t.Empty() {
		return false, nil
	}
	switch t := t.(type) {
	case *geom.Point, *geom.MultiPoint:
		return true, nil
	case *geom.LinearRing:
		return t.Coord(0).Equal(t.Layout(), t.Coord(t.NumCoords()-1)), nil
	case *geom.LineString:
		return t.Coord(0).Equal(t.Layout(), t.Coord(t.NumCoords()-1)), nil
	case *geom.MultiLineString:
		for i := 0; i < t.NumLineStrings(); i++ {
			if closed, err := isClosedFromGeomT(t.LineString(i)); err != nil || !closed {
				return false, err
			}
		}
		return true, nil
	case *geom.Polygon:
		for i := 0; i < t.NumLinearRings(); i++ {
			if closed, err := isClosedFromGeomT(t.LinearRing(i)); err != nil || !closed {
				return false, err
			}
		}
		return true, nil
	case *geom.MultiPolygon:
		for i := 0; i < t.NumPolygons(); i++ {
			if closed, err := isClosedFromGeomT(t.Polygon(i)); err != nil || !closed {
				return false, err
			}
		}
		return true, nil
	case *geom.GeometryCollection:
		for _, g := range t.Geoms() {
			if closed, err := isClosedFromGeomT(g); err != nil || !closed {
				return false, err
			}
		}
		return true, nil
	default:
		return false, errors.AssertionFailedf("unknown geometry type: %T", t)
	}
}

// IsCollection returns whether the given geometry is of a collection type.
func IsCollection(g geo.Geometry) (bool, error) {
	switch g.ShapeType() {
	case geopb.ShapeType_MultiPoint, geopb.ShapeType_MultiLineString, geopb.ShapeType_MultiPolygon,
		geopb.ShapeType_GeometryCollection:
		return true, nil
	default:
		return false, nil
	}
}

// IsEmpty returns whether the given geometry is empty.
func IsEmpty(g geo.Geometry) (bool, error) {
	return g.Empty(), nil
}

// IsRing returns whether the given geometry is a ring, i.e. that it is a
// simple and closed line.
func IsRing(g geo.Geometry) (bool, error) {
	// We explicitly check for empty geometries before checking the type,
	// to follow PostGIS behavior where all empty geometries return false.
	if g.Empty() {
		return false, nil
	}
	if g.ShapeType() != geopb.ShapeType_LineString {
		t, err := g.AsGeomT()
		if err != nil {
			return false, err
		}
		e := geom.ErrUnsupportedType{Value: t}
		return false, errors.Wrap(e, "should only be called on a linear feature")
	}
	if closed, err := IsClosed(g); err != nil || !closed {
		return false, err
	}
	if simple, err := IsSimple(g); err != nil || !simple {
		return false, err
	}
	return true, nil
}

// IsSimple returns whether the given geometry is simple, i.e. that it does not
// intersect or lie tangent to itself.
func IsSimple(g geo.Geometry) (bool, error) {
	return geos.IsSimple(g.EWKB())
}
