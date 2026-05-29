// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// coordGetZ returns the Z value from a coordinate, or 0 if no Z is present.
func coordGetZ(c geom.Coord) float64 {
	if len(c) > 2 {
		return c[2]
	}
	return 0
}

// coordAdd3D adds two coordinates in 3D and returns a new result.
func coordAdd3D(a geom.Coord, b geom.Coord) geom.Coord {
	return geom.Coord{a.X() + b.X(), a.Y() + b.Y(), coordGetZ(a) + coordGetZ(b)}
}

// coordSub3D subtracts two coordinates in 3D and returns a new result.
func coordSub3D(a geom.Coord, b geom.Coord) geom.Coord {
	return geom.Coord{a.X() - b.X(), a.Y() - b.Y(), coordGetZ(a) - coordGetZ(b)}
}

// coordMul3D multiplies a 3D coord by a scalar and returns the new result.
func coordMul3D(a geom.Coord, s float64) geom.Coord {
	return geom.Coord{a.X() * s, a.Y() * s, coordGetZ(a) * s}
}

// coordDot3D returns the dot product of two 3D coord vectors.
func coordDot3D(a geom.Coord, b geom.Coord) float64 {
	return a.X()*b.X() + a.Y()*b.Y() + coordGetZ(a)*coordGetZ(b)
}

// coordNorm2_3D returns the squared norm of a 3D coordinate vector.
func coordNorm2_3D(c geom.Coord) float64 {
	return coordDot3D(c, c)
}

// coordNorm3D returns the Euclidean norm of a 3D coordinate vector.
func coordNorm3D(c geom.Coord) float64 {
	return math.Sqrt(coordNorm2_3D(c))
}

// coordEqual3D returns whether two coordinates are equal in 3D.
func coordEqual3D(a geom.Coord, b geom.Coord) bool {
	return a.X() == b.X() && a.Y() == b.Y() && coordGetZ(a) == coordGetZ(b)
}

// closest3DSegmentSegment finds the closest pair of points on 3D
// segments [a0, a1] and [b0, b1], using the standard parameterized
// approach (see Ericson, "Real-Time Collision Detection", §5.1.9).
// Returns the closest point on segment A and the closest point on
// segment B as XYZ coordinates.
func closest3DSegmentSegment(a0, a1, b0, b1 geom.Coord) (geom.Coord, geom.Coord) {
	const epsilon = 1e-30
	d1 := coordSub3D(a1, a0)
	d2 := coordSub3D(b1, b0)
	r := coordSub3D(a0, b0)
	a := coordDot3D(d1, d1)
	e := coordDot3D(d2, d2)
	f := coordDot3D(d2, r)

	var s, t float64
	switch {
	case a <= epsilon && e <= epsilon:
		// Both segments are degenerate points.
		return a0, b0
	case a <= epsilon:
		// Segment A is a degenerate point; project onto B.
		t = clamp01(f / e)
	default:
		c := coordDot3D(d1, r)
		if e <= epsilon {
			// Segment B is a degenerate point; project onto A.
			s = clamp01(-c / a)
		} else {
			// General nondegenerate case.
			b := coordDot3D(d1, d2)
			denom := a*e - b*b
			if denom != 0 {
				s = clamp01((b*f - c*e) / denom)
			}
			t = (b*s + f) / e
			if t < 0 {
				t = 0
				s = clamp01(-c / a)
			} else if t > 1 {
				t = 1
				s = clamp01((b - c) / a)
			}
		}
	}
	return coordAdd3D(a0, coordMul3D(d1, s)), coordAdd3D(b0, coordMul3D(d2, t))
}

// clamp01 clamps v to [0, 1].
func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
