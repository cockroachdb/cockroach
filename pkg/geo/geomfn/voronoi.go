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

// Computes the Voronoi Polygons from the vertices of the supplied geometry.
func VoronoiPolygons(
	a geo.Geometry, tolerance float64, onlyEdges int,
) (geo.Geometry, error) {
	paths, err := geos.VoronoiPolygons(a.EWKB(), tolerance, onlyEdges)
	gm, err := geo.ParseGeometryFromEWKB(paths)
	if err != nil {
		return geo.Geometry{}, err
	}
	return gm, nil
}

func VoronoiPolygonsWithEnv(
	a, b geo.Geometry, tolerance float64, onlyEdges int,
) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	paths, err := geos.VoronoiPolygonsWithEnv(a.EWKB(), b.EWKB(), tolerance, onlyEdges)
	gm, err := geo.ParseGeometryFromEWKB(paths)
	if err != nil {
		return geo.Geometry{}, err
	}
	return gm, nil
}
