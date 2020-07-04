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
	outerGeomT, err := outer.AsGeomT()
	if err != nil {
		return nil, err
	}
	outerLineStr, ok := outerGeomT.(*geom.LineString)
	if !ok {
		return nil, errors.Newf("Argument must be LINESTRING geometries")
	}
	coords := make([][]geom.Coord, len(interior)+1)
	coords[0] = outerLineStr.Coords()
	for i, ring := range interior {
		ringGeomT, err := ring.AsGeomT()
		if err != nil {
			return nil, err
		}
		ringLineStr, ok := ringGeomT.(*geom.LineString)
		if !ok {
			return nil, errors.Newf("Argument must be LINESTRING geometries")
		}
		coords[i+1] = ringLineStr.Coords()
	}

	polygon, err := geom.NewPolygon(geom.XY).SetCoords(coords)
	if err != nil {
		return nil, err
	}
	return geo.NewGeometryFromGeom(polygon)
}
