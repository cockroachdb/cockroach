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
)

// Centroid returns the Centroid of a given Geometry.
func Centroid(g geo.Geometry) (geo.Geometry, error) {
	centroidEWKB, err := geos.Centroid(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(centroidEWKB)
}

// ClipByRect clips a given Geometry by the given BoundingBox.
func ClipByRect(g geo.Geometry, b geo.CartesianBoundingBox) (geo.Geometry, error) {
	if g.Empty() {
		return g, nil
	}
	clipByRectEWKB, err := geos.ClipByRect(g.EWKB(), b.LoX, b.LoY, b.HiX, b.HiY)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(clipByRectEWKB)
}

// ConvexHull returns the convex hull of a given Geometry.
func ConvexHull(g geo.Geometry) (geo.Geometry, error) {
	convexHullEWKB, err := geos.ConvexHull(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(convexHullEWKB)
}

// PointOnSurface returns the PointOnSurface of a given Geometry.
func PointOnSurface(g geo.Geometry) (geo.Geometry, error) {
	pointOnSurfaceEWKB, err := geos.PointOnSurface(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(pointOnSurfaceEWKB)
}

// Intersection returns the geometries of intersection between A and B.
func Intersection(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	retEWKB, err := geos.Intersection(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// Union returns the geometries of union between A and B.
func Union(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	retEWKB, err := geos.Union(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// SymDifference returns the geometries of symmetric difference between A and B.
func SymDifference(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	retEWKB, err := geos.SymDifference(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// SharedPaths Returns a geometry collection containing paths shared by the two input geometries.
func SharedPaths(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	paths, err := geos.SharedPaths(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	gm, err := geo.ParseGeometryFromEWKB(paths)
	if err != nil {
		return geo.Geometry{}, err
	}
	return gm, nil
}
