// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import "github.com/cockroachdb/cockroach/pkg/geo"

// Envelope forms an envelope (compliant with the OGC spec) of the given Geometry.
// It uses the bounding box to return a Polygon, but can return a Point or
// Line if the bounding box is degenerate and not a box.
func Envelope(g geo.Geometry) (geo.Geometry, error) {
	if g.Empty() {
		return g, nil
	}
	return geo.MakeGeometryFromGeomT(g.CartesianBoundingBox().ToGeomT(g.SRID()))
}
