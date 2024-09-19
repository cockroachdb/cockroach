// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
)

// VoronoiDiagram Computes the Voronoi Diagram from the vertices of the supplied geometry.
func VoronoiDiagram(
	g geo.Geometry, env *geo.Geometry, tolerance float64, onlyEdges bool,
) (geo.Geometry, error) {
	var envEWKB geopb.EWKB
	if !(env == nil) {
		if g.SRID() != env.SRID() {
			return geo.Geometry{}, geo.NewMismatchingSRIDsError(g.SpatialObject(), env.SpatialObject())
		}
		envEWKB = env.EWKB()
	}
	paths, err := geos.VoronoiDiagram(g.EWKB(), envEWKB, tolerance, onlyEdges)
	if err != nil {
		return geo.Geometry{}, err
	}
	gm, err := geo.ParseGeometryFromEWKB(paths)
	if err != nil {
		return geo.Geometry{}, err
	}
	return gm, nil
}
