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
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Azimuth returns the azimuth in radians of the segment defined by the given point geometries.
// The azimuth is angle is referenced from north, and is positive clockwise.
// North = 0; East = π/2; South = π; West = 3π/2.
// Returns nil if the two points are the same.
// Returns an error if any of the two Geometry items are not points.
func Azimuth(a *geo.Geometry, b *geo.Geometry) (*float64, error) {
	aGeomT, err := a.AsGeomT()
	if err != nil {
		return nil, err
	}

	aPoint, ok := aGeomT.(*geom.Point)
	if !ok {
		return nil, errors.Newf("Argument must be POINT geometries")
	}

	bGeomT, err := b.AsGeomT()
	if err != nil {
		return nil, err
	}

	bPoint, ok := bGeomT.(*geom.Point)
	if !ok {
		return nil, errors.Newf("Argument must be POINT geometries")
	}

	if aPoint.X() == bPoint.X() && aPoint.Y() == bPoint.Y() {
		return nil, nil
	}

	atan := math.Atan2(bPoint.Y()-aPoint.Y(), bPoint.X()-aPoint.X()) // Started at East(90) counterclockwise.
	const degree360 = 2 * math.Pi                                    // Added 360 degrees for always returns a positive value.
	const degree90 = math.Pi / 2                                     // Added 90 degrees to get it started at North(0).

	azimuth := math.Mod(degree360+degree90-atan, 2*math.Pi)

	return &azimuth, nil
}
