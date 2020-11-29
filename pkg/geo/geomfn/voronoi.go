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
)

// VoronoiDiagram Computes the Voronoi Diagram from the vertices of the supplied geometry.
func VoronoiDiagram(
	a, env geo.Geometry, tolerance float64, onlyEdges bool,
) (geo.Geometry, error) {
	var envEWKB geopb.EWKB
	if !env.Empty() {
		if a.SRID() != env.SRID() {
			return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), env.SpatialObject())
		}
		envEWKB = env.EWKB()
	}
	paths, err := geos.VoronoiDiagram(a.EWKB(), envEWKB, tolerance, onlyEdges)
	gm, err := geo.ParseGeometryFromEWKB(paths)
	if err != nil {
		return geo.Geometry{}, err
	}
	return gm, nil
}
