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
)

// Covers returns whether geometry A covers geometry B.
func Covers(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !a.CartesianBoundingBox().Covers(b.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.Covers(a.EWKB(), b.EWKB())
}

// CoveredBy returns whether geometry A is covered by geometry B.
func CoveredBy(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !b.CartesianBoundingBox().Covers(a.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.CoveredBy(a.EWKB(), b.EWKB())
}

// Contains returns whether geometry A contains geometry B.
func Contains(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !a.CartesianBoundingBox().Covers(b.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.Contains(a.EWKB(), b.EWKB())
}

// ContainsProperly returns whether geometry A properly contains geometry B.
func ContainsProperly(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !a.CartesianBoundingBox().Covers(b.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.RelatePattern(a.EWKB(), b.EWKB(), "T**FF*FF*")
}

// Crosses returns whether geometry A crosses geometry B.
func Crosses(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.Crosses(a.EWKB(), b.EWKB())
}

// Disjoint returns whether geometry A is disjoint from geometry B.
func Disjoint(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	return geos.Disjoint(a.EWKB(), b.EWKB())
}

// Equals returns whether geometry A equals geometry B.
func Equals(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	// Empty items are equal to each other.
	// Do this check before the BoundingBoxIntersects check, as we would otherwise
	// return false.
	if a.Empty() && b.Empty() {
		return true, nil
	}
	if !a.CartesianBoundingBox().Covers(b.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.Equals(a.EWKB(), b.EWKB())
}

// Intersects returns whether geometry A intersects geometry B.
func Intersects(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.Intersects(a.EWKB(), b.EWKB())
}

// OrderingEquals returns whether geometry A is equal to B, with all constituents
// and coordinates in the same order.
func OrderingEquals(a, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, nil
	}
	aBox, bBox := a.CartesianBoundingBox(), b.CartesianBoundingBox()
	switch {
	case aBox == nil && bBox == nil:
	case aBox == nil || bBox == nil:
		return false, nil
	case aBox.Compare(bBox) != 0:
		return false, nil
	}

	geomA, err := a.AsGeomT()
	if err != nil {
		return false, err
	}
	geomB, err := b.AsGeomT()
	if err != nil {
		return false, err
	}
	return orderingEqualsFromGeomT(geomA, geomB)
}

// orderingEqualsFromGeomT returns whether geometry A is equal to B.
func orderingEqualsFromGeomT(a, b geom.T) (bool, error) {
	if a.Layout() != b.Layout() {
		return false, nil
	}
	switch a := a.(type) {
	case *geom.Point:
		if b, ok := b.(*geom.Point); ok {
			// Point.Coords() panics on empty points
			switch {
			case a.Empty() && b.Empty():
				return true, nil
			case a.Empty() || b.Empty():
				return false, nil
			default:
				return a.Coords().Equal(b.Layout(), b.Coords()), nil
			}
		}
	case *geom.LineString:
		if b, ok := b.(*geom.LineString); ok && a.NumCoords() == b.NumCoords() {
			for i := 0; i < a.NumCoords(); i++ {
				if !a.Coord(i).Equal(b.Layout(), b.Coord(i)) {
					return false, nil
				}
			}
			return true, nil
		}
	case *geom.Polygon:
		if b, ok := b.(*geom.Polygon); ok && a.NumLinearRings() == b.NumLinearRings() {
			for i := 0; i < a.NumLinearRings(); i++ {
				for j := 0; j < a.LinearRing(i).NumCoords(); j++ {
					if !a.LinearRing(i).Coord(j).Equal(b.Layout(), b.LinearRing(i).Coord(j)) {
						return false, nil
					}
				}
			}
			return true, nil
		}
	case *geom.MultiPoint:
		if b, ok := b.(*geom.MultiPoint); ok && a.NumPoints() == b.NumPoints() {
			for i := 0; i < a.NumPoints(); i++ {
				if eq, err := orderingEqualsFromGeomT(a.Point(i), b.Point(i)); err != nil || !eq {
					return false, err
				}
			}
			return true, nil
		}
	case *geom.MultiLineString:
		if b, ok := b.(*geom.MultiLineString); ok && a.NumLineStrings() == b.NumLineStrings() {
			for i := 0; i < a.NumLineStrings(); i++ {
				if eq, err := orderingEqualsFromGeomT(a.LineString(i), b.LineString(i)); err != nil || !eq {
					return false, err
				}
			}
			return true, nil
		}
	case *geom.MultiPolygon:
		if b, ok := b.(*geom.MultiPolygon); ok && a.NumPolygons() == b.NumPolygons() {
			for i := 0; i < a.NumPolygons(); i++ {
				if eq, err := orderingEqualsFromGeomT(a.Polygon(i), b.Polygon(i)); err != nil || !eq {
					return false, err
				}
			}
			return true, nil
		}
	case *geom.GeometryCollection:
		if b, ok := b.(*geom.GeometryCollection); ok && a.NumGeoms() == b.NumGeoms() {
			for i := 0; i < a.NumGeoms(); i++ {
				if eq, err := orderingEqualsFromGeomT(a.Geom(i), b.Geom(i)); err != nil || !eq {
					return false, err
				}
			}
			return true, nil
		}
	default:
		return false, errors.AssertionFailedf("unknown geometry type: %T", a)
	}
	return false, nil
}

// Overlaps returns whether geometry A overlaps geometry B.
func Overlaps(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.Overlaps(a.EWKB(), b.EWKB())
}

// Touches returns whether geometry A touches geometry B.
func Touches(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.Touches(a.EWKB(), b.EWKB())
}

// Within returns whether geometry A is within geometry B.
func Within(a geo.Geometry, b geo.Geometry) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if !b.CartesianBoundingBox().Covers(a.CartesianBoundingBox()) {
		return false, nil
	}
	return geos.Within(a.EWKB(), b.EWKB())
}
