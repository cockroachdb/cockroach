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
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/twpayne/go-geom"
)

// SnapToGrid snaps all coordinates in the Geometry to the given grid size,
// offset by the given origin. It will remove duplicate points from the results.
// If the resulting geometry is invalid, it will be converted to it's EMPTY form.
func SnapToGrid(g geo.Geometry, origin geom.Coord, gridSize geom.Coord) (geo.Geometry, error) {
	if len(origin) != 4 {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "origin must be 4D")
	}
	if len(gridSize) != 4 {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "gridSize must be 4D")
	}
	if g.Empty() {
		return g, nil
	}
	geomT, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	retGeomT, err := snapToGrid(geomT, origin, gridSize)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(retGeomT)
}

func snapCoordinateToGrid(
	l geom.Layout, dst []float64, src []float64, origin geom.Coord, gridSize geom.Coord,
) {
	dst[0] = snapOrdinateToGrid(src[0], origin[0], gridSize[0])
	dst[1] = snapOrdinateToGrid(src[1], origin[1], gridSize[1])
	if l.ZIndex() != -1 {
		dst[l.ZIndex()] = snapOrdinateToGrid(src[l.ZIndex()], origin[l.ZIndex()], gridSize[l.ZIndex()])
	}
	if l.MIndex() != -1 {
		dst[l.MIndex()] = snapOrdinateToGrid(src[l.MIndex()], origin[l.MIndex()], gridSize[l.MIndex()])
	}
}

func snapOrdinateToGrid(
	ordinate float64, originOrdinate float64, gridSizeOrdinate float64,
) float64 {
	// A zero grid size ordinate indicates a dimension should not be snapped.
	// For PostGIS compatibility, a negative grid size ordinate also results
	// in a dimension not being snapped.
	if gridSizeOrdinate <= 0 {
		return ordinate
	}
	return math.RoundToEven((ordinate-originOrdinate)/gridSizeOrdinate)*gridSizeOrdinate + originOrdinate
}

func snapToGrid(t geom.T, origin geom.Coord, gridSize geom.Coord) (geom.T, error) {
	if t.Empty() {
		return t, nil
	}
	t, err := applyOnCoordsForGeomT(t, func(l geom.Layout, dst []float64, src []float64) error {
		snapCoordinateToGrid(l, dst, src, origin, gridSize)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return removeConsecutivePointsFromGeomT(t)
}
