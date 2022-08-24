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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/twpayne/go-geom"
)

// MakePolygon creates a Polygon geometry from linestring and optional inner linestrings.
// Returns errors if geometries are not linestrings.
func MakePolygon(outer geo.Geometry, interior ...geo.Geometry) (geo.Geometry, error) {
	layout := geom.XY
	outerGeomT, err := outer.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	outerRing, ok := outerGeomT.(*geom.LineString)
	if !ok {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "argument must be LINESTRING geometries")
	}
	if outerRing.Empty() {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "polygon shell must not be empty")
	}
	srid := outerRing.SRID()
	coords := make([][]geom.Coord, len(interior)+1)
	coords[0] = outerRing.Coords()
	for i, g := range interior {
		interiorRingGeomT, err := g.AsGeomT()
		if err != nil {
			return geo.Geometry{}, err
		}
		interiorRing, ok := interiorRingGeomT.(*geom.LineString)
		if !ok {
			return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "argument must be LINESTRING geometries")
		}
		if interiorRing.SRID() != srid {
			return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "mixed SRIDs are not allowed")
		}
		if outerRing.Layout() != interiorRing.Layout() {
			return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "mixed dimension rings")
		}
		coords[i+1] = interiorRing.Coords()
	}

	polygon, err := geom.NewPolygon(layout).SetSRID(srid).SetCoords(coords)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(polygon)
}

// MakePolygonWithSRID is like MakePolygon but also sets the SRID, like ST_Polygon.
func MakePolygonWithSRID(g geo.Geometry, srid int) (geo.Geometry, error) {
	polygon, err := MakePolygon(g)
	if err != nil {
		return geo.Geometry{}, err
	}
	t, err := polygon.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	geo.AdjustGeomTSRID(t, geopb.SRID(srid))
	return geo.MakeGeometryFromGeomT(t)
}
