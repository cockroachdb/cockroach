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

// MakePolygon creates a Polygon geometry from linestring and optional inner linestrings.
// Returns errors if geometries are not linestrings.
func MakePolygon(outer *geo.Geometry, interior ...*geo.Geometry) (*geo.Geometry, error) {
	layout := geom.XY
	outerGeomT, err := outer.AsGeomT()
	if err != nil {
		return nil, err
	}
	outerRing, ok := outerGeomT.(*geom.LineString)
	if !ok {
		return nil, errors.Newf("argument must be LINESTRING geometries")
	}
	srid := outerRing.SRID()
	coords := make([][]geom.Coord, len(interior)+1)
	coords[0] = outerRing.Coords()
	for i, g := range interior {
		interiorRingGeomT, err := g.AsGeomT()
		if err != nil {
			return nil, err
		}
		interiorRing, ok := interiorRingGeomT.(*geom.LineString)
		if !ok {
			return nil, errors.Newf("argument must be LINESTRING geometries")
		}
		if interiorRing.SRID() != srid {
			return nil, errors.Newf("mixed SRIDs are not allowed")
		}
		coords[i+1] = interiorRing.Coords()
	}

	polygon, err := geom.NewPolygon(layout).SetSRID(srid).SetCoords(coords)
	if err != nil {
		return nil, err
	}
	return geo.NewGeometryFromGeomT(polygon)
}
