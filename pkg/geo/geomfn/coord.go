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

// coordAdd adds two coordinates and returns a new result.
func coordAdd(a geom.Coord, b geom.Coord) geom.Coord {
	return geom.Coord{a.X() + b.X(), a.Y() + b.Y()}
}

// coordSub subtracts two coordinates and returns a new result.
func coordSub(a geom.Coord, b geom.Coord) geom.Coord {
	return geom.Coord{a.X() - b.X(), a.Y() - b.Y()}
}

// coordMul multiplies a coord by a scalar and returns the new result.
func coordMul(a geom.Coord, s float64) geom.Coord {
	return geom.Coord{a.X() * s, a.Y() * s}
}

// coordDet returns the determinant of the 2x2 matrix formed by the vectors a and b.
func coordDet(a geom.Coord, b geom.Coord) float64 {
	return a.X()*b.Y() - b.X()*a.Y()
}

// coordDot returns the dot product of two coords if the coord was a vector.
func coordDot(a geom.Coord, b geom.Coord) float64 {
	return a.X()*b.X() + a.Y()*b.Y()
}

// coordCross returns the cross product of two coords if the coord was a vector.
func coordCross(a geom.Coord, b geom.Coord) float64 {
	return a.X()*b.Y() - a.Y()*b.X()
}

// coordNorm2 returns the normalization^2 of a coordinate if the coord was a vector.
func coordNorm2(c geom.Coord) float64 {
	return coordDot(c, c)
}

// coordNorm returns the normalization of a coordinate if the coord was a vector.
func coordNorm(c geom.Coord) float64 {
	return math.Sqrt(coordNorm2(c))
}

// coordEqual returns whether two coordinates are equal.
func coordEqual(a geom.Coord, b geom.Coord) bool {
	return a.X() == b.X() && a.Y() == b.Y()
}

// coordMag2 returns the magnitude^2 of a coordinate if the coord was a vector.
func coordMag2(c geom.Coord) float64 {
	return coordDot(c, c)
}
