package xy

import (
	"math"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/orientation"
)

const piTimes2 = math.Pi * 2

// Angle calculates the angle of the vector from p0 to p1,
// relative to the positive X-axis.
// The angle is normalized to be in the range [ -Pi, Pi ].
func Angle(p0, p1 geom.Coord) float64 {
	dx := p1[0] - p0[0]
	dy := p1[1] - p0[1]
	return math.Atan2(dy, dx)
}

// AngleFromOrigin calculates the angle that the vector from (0,0) to p,
// relative to the positive X-axis.
// The angle is normalized to be in the range ( -Pi, Pi ].
func AngleFromOrigin(p geom.Coord) float64 {
	return math.Atan2(p[1], p[0])
}

// IsAcute tests whether the angle between endpoint1-base-endpoint2 is acute.
// An angle is acute if it is less than 90 degrees.
//
// Note: this implementation is not precise (deterministic) for angles very close to 90 degrees
func IsAcute(endpoint1, base, endpoint2 geom.Coord) bool {
	// relies on fact that A dot B is positive iff A ang B is acute
	dx0 := endpoint1[0] - base[0]
	dy0 := endpoint1[1] - base[1]
	dx1 := endpoint2[0] - base[0]
	dy1 := endpoint2[1] - base[1]
	dotprod := dx0*dx1 + dy0*dy1
	return dotprod > 0
}

// IsObtuse tests whether the angle between endpoint1-base-endpoint2 is obtuse.
// An angle is obtuse if it is greater than 90 degrees.
//
// Note: this implementation is not precise (deterministic) for angles very close to 90 degrees
func IsObtuse(endpoint1, base, endpoint2 geom.Coord) bool {
	// relies on fact that A dot B is negative iff A ang B is obtuse
	dx0 := endpoint1[0] - base[0]
	dy0 := endpoint1[1] - base[1]
	dx1 := endpoint2[0] - base[0]
	dy1 := endpoint2[1] - base[1]
	dotprod := dx0*dx1 + dy0*dy1
	return dotprod < 0
}

// AngleBetween calculates the un-oriented smallest angle between two vectors.
// The computed angle will be in the range [0, Pi).
//
// Param tip1 - the tip of one vector
// param tail - the tail of each vector
// param tip2 - the tip of the other vector
func AngleBetween(tip1, tail, tip2 geom.Coord) float64 {
	a1 := Angle(tail, tip1)
	a2 := Angle(tail, tip2)

	return Diff(a1, a2)
}

// AngleBetweenOriented calculates the oriented smallest angle between two vectors.
// The computed angle will be in the range (-Pi, Pi].
// A positive result corresponds to a counterclockwise (CCW) rotation from v1 to v2;
// A negative result corresponds to a clockwise (CW) rotation;
// A zero result corresponds to no rotation.
func AngleBetweenOriented(tip1, tail, tip2 geom.Coord) float64 {
	a1 := Angle(tail, tip1)
	a2 := Angle(tail, tip2)
	angDel := a2 - a1

	return Normalize(angDel)
}

// InteriorAngle cmputes the interior angle between two segments of a ring. The ring is
// assumed to be oriented in a clockwise direction. The computed angle will be
// in the range [0, 2Pi]
func InteriorAngle(p0, p1, p2 geom.Coord) float64 {
	anglePrev := Angle(p1, p0)
	angleNext := Angle(p1, p2)
	return math.Abs(angleNext - anglePrev)
}

// AngleOrientation returns whether an angle must turn clockwise or counterclockwise
// overlap another angle.
func AngleOrientation(ang1, ang2 float64) orientation.Type {
	crossproduct := math.Sin(ang2 - ang1)

	switch {
	case crossproduct > 0:
		return orientation.CounterClockwise
	case crossproduct < 0:
		return orientation.Clockwise
	default:
		return orientation.Collinear
	}
}

// Normalize computes the normalized value of an angle, which is the
// equivalent angle in the range ( -Pi, Pi ].
func Normalize(angle float64) float64 {
	for angle > math.Pi {
		angle -= piTimes2
	}
	for angle <= -math.Pi {
		angle += piTimes2
	}
	return angle
}

// NormalizePositive computes the normalized positive value of an angle, which is the
// equivalent angle in the range [ 0, 2*Pi ).
// E.g.:
// * normalizePositive(0.0) = 0.0
// * normalizePositive(-PI) = PI
// * normalizePositive(-2PI) = 0.0
// * normalizePositive(-3PI) = PI
// * normalizePositive(-4PI) = 0
// * normalizePositive(PI) = PI
// * normalizePositive(2PI) = 0.0
// * normalizePositive(3PI) = PI
// * normalizePositive(4PI) = 0.0
func NormalizePositive(angle float64) float64 {
	if angle < 0.0 {
		for angle < 0.0 {
			angle += piTimes2
		}
		// in case round-off error bumps the value over
		if angle >= piTimes2 {
			angle = 0.0
		}
	} else {
		for angle >= piTimes2 {
			angle -= piTimes2
		}
		// in case round-off error bumps the value under
		if angle < 0.0 {
			angle = 0.0
		}
	}
	return angle
}

// Diff computes the un-oriented smallest difference between two angles.
// The angles are assumed to be normalized to the range [-Pi, Pi].
// The result will be in the range [0, Pi].
//
// Param ang1 - the angle of one vector (in [-Pi, Pi] )
// Param ang2 - the angle of the other vector (in range [-Pi, Pi] )
func Diff(ang1, ang2 float64) float64 {
	var delAngle float64

	if ang1 < ang2 {
		delAngle = ang2 - ang1
	} else {
		delAngle = ang1 - ang2
	}

	if delAngle > math.Pi {
		delAngle = piTimes2 - delAngle
	}

	return delAngle
}
