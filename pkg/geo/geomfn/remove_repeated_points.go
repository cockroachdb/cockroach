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
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// RemoveRepeatedPoints returns the geometry with repeated points removed.
func RemoveRepeatedPoints(g geo.Geometry, tolerance float64) (geo.Geometry, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	// Use the square of the tolerance to avoid taking the square root of distance results.
	t, err = removeRepeatedPointsFromGeomT(t, tolerance*tolerance)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(t)
}

func removeRepeatedPointsFromGeomT(t geom.T, tolerance2 float64) (geom.T, error) {
	switch t := t.(type) {
	case *geom.Point:
	case *geom.LineString:
		if coords, modified := removeRepeatedCoords(t.Layout(), t.Coords(), tolerance2, 2); modified {
			return t.SetCoords(coords)
		}
	case *geom.Polygon:
		if coords, modified := removeRepeatedCoords2(t.Layout(), t.Coords(), tolerance2, 4); modified {
			return t.SetCoords(coords)
		}
	case *geom.MultiPoint:
		if coords, modified := removeRepeatedCoords(t.Layout(), t.Coords(), tolerance2, 0); modified {
			return t.SetCoords(coords)
		}
	case *geom.MultiLineString:
		if coords, modified := removeRepeatedCoords2(t.Layout(), t.Coords(), tolerance2, 2); modified {
			return t.SetCoords(coords)
		}
	case *geom.MultiPolygon:
		if coords, modified := removeRepeatedCoords3(t.Layout(), t.Coords(), tolerance2, 4); modified {
			return t.SetCoords(coords)
		}
	case *geom.GeometryCollection:
		for _, g := range t.Geoms() {
			if _, err := removeRepeatedPointsFromGeomT(g, tolerance2); err != nil {
				return nil, err
			}
		}
	default:
		return nil, errors.AssertionFailedf("unknown geometry type: %T", t)
	}
	return t, nil
}

func removeRepeatedCoords(
	layout geom.Layout, coords []geom.Coord, tolerance2 float64, minCoords int,
) ([]geom.Coord, bool) {
	modified := false
	switch tolerance2 {
	case 0:
		for i := 1; i < len(coords) && len(coords) > minCoords; i++ {
			if coords[i].Equal(layout, coords[i-1]) {
				coords = append(coords[:i], coords[i+1:]...)
				modified = true
				i--
			}
		}
	default:
		for i := 1; i < len(coords) && len(coords) > minCoords; i++ {
			if coordMag2(coordSub(coords[i], coords[i-1])) <= tolerance2 {
				coords = append(coords[:i], coords[i+1:]...)
				modified = true
				i--
			}
		}
	}
	return coords, modified
}

func removeRepeatedCoords2(
	layout geom.Layout, coords2 [][]geom.Coord, tolerance2 float64, minCoords int,
) ([][]geom.Coord, bool) {
	modified := false
	for i, coords := range coords2 {
		if c, m := removeRepeatedCoords(layout, coords, tolerance2, minCoords); m {
			coords2[i] = c
			modified = true
		}
	}
	return coords2, modified
}

func removeRepeatedCoords3(
	layout geom.Layout, coords3 [][][]geom.Coord, tolerance2 float64, minCoords int,
) ([][][]geom.Coord, bool) {
	modified := false
	for i, coords2 := range coords3 {
		for j, coords := range coords2 {
			if c, m := removeRepeatedCoords(layout, coords, tolerance2, minCoords); m {
				coords3[i][j] = c
				modified = true
			}
		}
	}
	return coords3, modified
}
