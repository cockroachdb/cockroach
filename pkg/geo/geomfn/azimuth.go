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

// Azimuth returns the azimuth in radians of the segment defined by the given point geometries,
// where point a is the reference point.
// The reference direction from which the azimuth is calculated is north, and is positive clockwise.
// i.e. North = 0; East = π/2; South = π; West = 3π/2.
// See https://en.wikipedia.org/wiki/Polar_coordinate_system.
// Returns nil if the two points are the same.
// Returns an error if any of the two Geometry items are not points.
func Azimuth(a geo.Geometry, b geo.Geometry) (*float64, error) {
	if a.SRID() != b.SRID() {
		return nil, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}

	aGeomT, err := a.AsGeomT()
	if err != nil {
		return nil, err
	}

	aPoint, ok := aGeomT.(*geom.Point)
	if !ok {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "arguments must be POINT geometries")
	}

	bGeomT, err := b.AsGeomT()
	if err != nil {
		return nil, err
	}

	bPoint, ok := bGeomT.(*geom.Point)
	if !ok {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "arguments must be POINT geometries")
	}

	if aPoint.Empty() || bPoint.Empty() {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "cannot call ST_Azimuth with POINT EMPTY")
	}

	if aPoint.X() == bPoint.X() && aPoint.Y() == bPoint.Y() {
		return nil, nil
	}

	atan := math.Atan2(bPoint.Y()-aPoint.Y(), bPoint.X()-aPoint.X())
	// math.Pi / 2 is North from the atan calculation this is a CCW direction.
	// We want to return a CW direction, so subtract atan from math.Pi / 2 to get it into a CW direction.
	// Then add 2*math.Pi to ensure a positive azimuth.
	azimuth := math.Mod(math.Pi/2-atan+2*math.Pi, 2*math.Pi)
	return &azimuth, nil
}
