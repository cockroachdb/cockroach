// Copyright 2015 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s1

import (
	"math"
)

// ChordAngle represents the angle subtended by a chord (i.e., the straight
// line segment connecting two points on the sphere). Its representation
// makes it very efficient for computing and comparing distances, but unlike
// Angle it is only capable of representing angles between 0 and π radians.
// Generally, ChordAngle should only be used in loops where many angles need
// to be calculated and compared. Otherwise it is simpler to use Angle.
//
// ChordAngle loses some accuracy as the angle approaches π radians.
// Specifically, the representation of (π - x) radians has an error of about
// (1e-15 / x), with a maximum error of about 2e-8 radians (about 13cm on the
// Earth's surface). For comparison, for angles up to π/2 radians (10000km)
// the worst-case representation error is about 2e-16 radians (1 nanonmeter),
// which is about the same as Angle.
//
// ChordAngles are represented by the squared chord length, which can
// range from 0 to 4. Positive infinity represents an infinite squared length.
type ChordAngle float64

const (
	// NegativeChordAngle represents a chord angle smaller than the zero angle.
	// The only valid operations on a NegativeChordAngle are comparisons,
	// Angle conversions, and Successor/Predecessor.
	NegativeChordAngle = ChordAngle(-1)

	// RightChordAngle represents a chord angle of 90 degrees (a "right angle").
	RightChordAngle = ChordAngle(2)

	// StraightChordAngle represents a chord angle of 180 degrees (a "straight angle").
	// This is the maximum finite chord angle.
	StraightChordAngle = ChordAngle(4)

	// maxLength2 is the square of the maximum length allowed in a ChordAngle.
	maxLength2 = 4.0
)

// ChordAngleFromAngle returns a ChordAngle from the given Angle.
func ChordAngleFromAngle(a Angle) ChordAngle {
	if a < 0 {
		return NegativeChordAngle
	}
	if a.isInf() {
		return InfChordAngle()
	}
	l := 2 * math.Sin(0.5*math.Min(math.Pi, a.Radians()))
	return ChordAngle(l * l)
}

// ChordAngleFromSquaredLength returns a ChordAngle from the squared chord length.
// Note that the argument is automatically clamped to a maximum of 4 to
// handle possible roundoff errors. The argument must be non-negative.
func ChordAngleFromSquaredLength(length2 float64) ChordAngle {
	if length2 > maxLength2 {
		return StraightChordAngle
	}
	return ChordAngle(length2)
}

// Expanded returns a new ChordAngle that has been adjusted by the given error
// bound (which can be positive or negative). Error should be the value
// returned by either MaxPointError or MaxAngleError. For example:
//    a := ChordAngleFromPoints(x, y)
//    a1 := a.Expanded(a.MaxPointError())
func (c ChordAngle) Expanded(e float64) ChordAngle {
	// If the angle is special, don't change it. Otherwise clamp it to the valid range.
	if c.isSpecial() {
		return c
	}
	return ChordAngle(math.Max(0.0, math.Min(maxLength2, float64(c)+e)))
}

// Angle converts this ChordAngle to an Angle.
func (c ChordAngle) Angle() Angle {
	if c < 0 {
		return -1 * Radian
	}
	if c.isInf() {
		return InfAngle()
	}
	return Angle(2 * math.Asin(0.5*math.Sqrt(float64(c))))
}

// InfChordAngle returns a chord angle larger than any finite chord angle.
// The only valid operations on an InfChordAngle are comparisons, Angle
// conversions, and Successor/Predecessor.
func InfChordAngle() ChordAngle {
	return ChordAngle(math.Inf(1))
}

// isInf reports whether this ChordAngle is infinite.
func (c ChordAngle) isInf() bool {
	return math.IsInf(float64(c), 1)
}

// isSpecial reports whether this ChordAngle is one of the special cases.
func (c ChordAngle) isSpecial() bool {
	return c < 0 || c.isInf()
}

// isValid reports whether this ChordAngle is valid or not.
func (c ChordAngle) isValid() bool {
	return (c >= 0 && c <= maxLength2) || c.isSpecial()
}

// Successor returns the smallest representable ChordAngle larger than this one.
// This can be used to convert a "<" comparison to a "<=" comparison.
//
// Note the following special cases:
//   NegativeChordAngle.Successor == 0
//   StraightChordAngle.Successor == InfChordAngle
//   InfChordAngle.Successor == InfChordAngle
func (c ChordAngle) Successor() ChordAngle {
	if c >= maxLength2 {
		return InfChordAngle()
	}
	if c < 0 {
		return 0
	}
	return ChordAngle(math.Nextafter(float64(c), 10.0))
}

// Predecessor returns the largest representable ChordAngle less than this one.
//
// Note the following special cases:
//   InfChordAngle.Predecessor == StraightChordAngle
//   ChordAngle(0).Predecessor == NegativeChordAngle
//   NegativeChordAngle.Predecessor == NegativeChordAngle
func (c ChordAngle) Predecessor() ChordAngle {
	if c <= 0 {
		return NegativeChordAngle
	}
	if c > maxLength2 {
		return StraightChordAngle
	}

	return ChordAngle(math.Nextafter(float64(c), -10.0))
}

// MaxPointError returns the maximum error size for a ChordAngle constructed
// from 2 Points x and y, assuming that x and y are normalized to within the
// bounds guaranteed by s2.Point.Normalize. The error is defined with respect to
// the true distance after the points are projected to lie exactly on the sphere.
func (c ChordAngle) MaxPointError() float64 {
	// There is a relative error of (2.5*dblEpsilon) when computing the squared
	// distance, plus a relative error of 2 * dblEpsilon, plus an absolute error
	// of (16 * dblEpsilon**2) because the lengths of the input points may differ
	// from 1 by up to (2*dblEpsilon) each. (This is the maximum error in Normalize).
	return 4.5*dblEpsilon*float64(c) + 16*dblEpsilon*dblEpsilon
}

// MaxAngleError returns the maximum error for a ChordAngle constructed
// as an Angle distance.
func (c ChordAngle) MaxAngleError() float64 {
	return dblEpsilon * float64(c)
}

// Add adds the other ChordAngle to this one and returns the resulting value.
// This method assumes the ChordAngles are not special.
func (c ChordAngle) Add(other ChordAngle) ChordAngle {
	// Note that this method (and Sub) is much more efficient than converting
	// the ChordAngle to an Angle and adding those and converting back. It
	// requires only one square root plus a few additions and multiplications.

	// Optimization for the common case where b is an error tolerance
	// parameter that happens to be set to zero.
	if other == 0 {
		return c
	}

	// Clamp the angle sum to at most 180 degrees.
	if c+other >= maxLength2 {
		return StraightChordAngle
	}

	// Let a and b be the (non-squared) chord lengths, and let c = a+b.
	// Let A, B, and C be the corresponding half-angles (a = 2*sin(A), etc).
	// Then the formula below can be derived from c = 2 * sin(A+B) and the
	// relationships   sin(A+B) = sin(A)*cos(B) + sin(B)*cos(A)
	//                 cos(X) = sqrt(1 - sin^2(X))
	x := float64(c * (1 - 0.25*other))
	y := float64(other * (1 - 0.25*c))
	return ChordAngle(math.Min(maxLength2, x+y+2*math.Sqrt(x*y)))
}

// Sub subtracts the other ChordAngle from this one and returns the resulting
// value. This method assumes the ChordAngles are not special.
func (c ChordAngle) Sub(other ChordAngle) ChordAngle {
	if other == 0 {
		return c
	}
	if c <= other {
		return 0
	}
	x := float64(c * (1 - 0.25*other))
	y := float64(other * (1 - 0.25*c))
	return ChordAngle(math.Max(0.0, x+y-2*math.Sqrt(x*y)))
}

// Sin returns the sine of this chord angle. This method is more efficient
// than converting to Angle and performing the computation.
func (c ChordAngle) Sin() float64 {
	return math.Sqrt(c.Sin2())
}

// Sin2 returns the square of the sine of this chord angle.
// It is more efficient than Sin.
func (c ChordAngle) Sin2() float64 {
	// Let a be the (non-squared) chord length, and let A be the corresponding
	// half-angle (a = 2*sin(A)).  The formula below can be derived from:
	//   sin(2*A) = 2 * sin(A) * cos(A)
	//   cos^2(A) = 1 - sin^2(A)
	// This is much faster than converting to an angle and computing its sine.
	return float64(c * (1 - 0.25*c))
}

// Cos returns the cosine of this chord angle. This method is more efficient
// than converting to Angle and performing the computation.
func (c ChordAngle) Cos() float64 {
	// cos(2*A) = cos^2(A) - sin^2(A) = 1 - 2*sin^2(A)
	return float64(1 - 0.5*c)
}

// Tan returns the tangent of this chord angle.
func (c ChordAngle) Tan() float64 {
	return c.Sin() / c.Cos()
}

// TODO(roberts): Differences from C++:
//   Helpers to/from E5/E6/E7
//   Helpers to/from degrees and radians directly.
//   FastUpperBoundFrom(angle Angle)
