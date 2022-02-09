// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// Azimuth returns the azimuth in radians of the segment defined by the given point geometries.
// The azimuth is angle is referenced from north, and is positive clockwise.
// North = 0; East = π/2; South = π; West = 3π/2.
// Returns nil if the two points are the same.
// Returns an error if any of the two Geography items are not points.
func Azimuth(a geo.Geography, b geo.Geography) (*float64, error) {
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

	s, err := a.Spheroid()
	if err != nil {
		return nil, err
	}

	_, az1, _ := s.Inverse(
		s2.LatLngFromDegrees(aPoint.Y(), aPoint.X()),
		s2.LatLngFromDegrees(bPoint.Y(), bPoint.X()),
	)
	// Convert to radians.
	az1 = az1 * math.Pi / 180
	return &az1, nil
}
