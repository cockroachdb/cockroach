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

	"github.com/twpayne/go-geom"
)

// Azimuth returns the azimuth in radians of the segment defined by the given point geometries.
// The azimuth is angle is referenced from north, and is positive clockwise.
// North = 0; East = π/2; South = π; West = 3π/2.
func Azimuth(a *geom.Point, b *geom.Point) *float64 {
	if a.X() == b.X() && a.Y() == b.Y() {
		return nil
	}

	azimuth := math.Mod(2*math.Pi+math.Pi/2-math.Atan2(b.Y()-a.Y(), b.X()-a.X()), 2*math.Pi)
	return &azimuth
}
