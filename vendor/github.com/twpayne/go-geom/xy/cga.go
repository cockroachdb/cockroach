// Package xy contains low-level planar (xy) geographic functions.  The data can be of any dimension, however the first
// two ordinates for each coordinate must be the x,y coordinates.  All other ordinates will be ignored.
package xy

import (
	"fmt"
	"math"

	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/bigxy"
	"github.com/twpayne/go-geom/xy/internal"
	"github.com/twpayne/go-geom/xy/internal/raycrossing"
	"github.com/twpayne/go-geom/xy/lineintersector"
	"github.com/twpayne/go-geom/xy/location"
	"github.com/twpayne/go-geom/xy/orientation"
)

// OrientationIndex returns the index of the direction of the point <code>q</code> relative to
// a vector specified by <code>p1-p2</code>.
//
// vectorOrigin - the origin point of the vector
// vectorEnd - the final point of the vector
// point - the point to compute the direction to
func OrientationIndex(vectorOrigin, vectorEnd, point geom.Coord) orientation.Type {
	return bigxy.OrientationIndex(vectorOrigin, vectorEnd, point)
}

// IsPointInRing tests whether a point lies inside or on a ring. The ring may be oriented in
// either direction. A point lying exactly on the ring boundary is considered
// to be inside the ring.
//
// This method does <i>not</i> first check the point against the envelope of
// the ring.
//
// p - point to check for ring inclusion
// ring - an array of coordinates representing the ring (which must have
//        first point identical to last point)
// Returns true if p is inside ring
//
func IsPointInRing(layout geom.Layout, p geom.Coord, ring []float64) bool {
	return LocatePointInRing(layout, p, ring) != location.Exterior
}

// LocatePointInRing determines whether a point lies in the interior, on the boundary, or in the
// exterior of a ring. The ring may be oriented in either direction.
//
// This method does <i>not</i> first check the point against the envelope of
// the ring.
//
// p - point to check for ring inclusion
// ring - an array of coordinates representing the ring (which must have
//        first point identical to last point)
// Returns the Location of p relative to the ring
func LocatePointInRing(layout geom.Layout, p geom.Coord, ring []float64) location.Type {
	return raycrossing.LocatePointInRing(layout, p, ring)
}

// IsOnLine tests whether a point lies on the line segments defined by a list of
// coordinates.
//
// Returns true if the point is a vertex of the line or lies in the interior
//         of a line segment in the linestring
func IsOnLine(layout geom.Layout, point geom.Coord, lineSegmentCoordinates []float64) bool {
	stride := layout.Stride()
	if len(lineSegmentCoordinates) < (2 * stride) {
		panic(fmt.Sprintf("At least two coordinates are required in the lineSegmentsCoordinates array in 'algorithms.IsOnLine', was: %v", lineSegmentCoordinates))
	}
	strategy := lineintersector.RobustLineIntersector{}

	for i := stride; i < len(lineSegmentCoordinates); i += stride {
		segmentStart := lineSegmentCoordinates[i-stride : i-stride+2]
		segmentEnd := lineSegmentCoordinates[i : i+2]

		if lineintersector.PointIntersectsLine(strategy, point, geom.Coord(segmentStart), geom.Coord(segmentEnd)) {
			return true
		}
	}
	return false
}

// IsRingCounterClockwise computes whether a ring defined by an array of geom.Coords is
// oriented counter-clockwise.
//
// - The list of points is assumed to have the first and last points equal.
// - This will handle coordinate lists which contain repeated points.
//
// This algorithm is <b>only</b> guaranteed to work with valid rings. If the
// ring is invalid (e.g. self-crosses or touches), the computed result may not
// be correct.
//
// Param ring - an array of Coordinates forming a ring
// Returns true if the ring is oriented counter-clockwise.
// Panics if there are too few points to determine orientation (< 3)
func IsRingCounterClockwise(layout geom.Layout, ring []float64) bool {
	stride := layout.Stride()

	// # of ordinates without closing endpoint
	nOrds := len(ring) - stride
	// # of points without closing endpoint
	// sanity check
	if nPts := nOrds / stride; nPts < 3 {
		panic("Ring has fewer than 3 points, so orientation cannot be determined")
	}

	// find highest point
	hiIndex := 0
	for i := stride; i <= len(ring)-stride; i += stride {
		if ring[i+1] > ring[hiIndex+1] {
			hiIndex = i
		}
	}

	// find distinct point before highest point
	iPrev := hiIndex
	for {
		iPrev -= stride
		if iPrev < 0 {
			iPrev = nOrds
		}

		if !internal.Equal(ring, iPrev, ring, hiIndex) || iPrev == hiIndex {
			break
		}
	}

	// find distinct point after highest point
	iNext := hiIndex
	for {
		iNext = (iNext + stride) % nOrds

		if !internal.Equal(ring, iNext, ring, hiIndex) || iNext == hiIndex {
			break
		}
	}

	// This check catches cases where the ring contains an A-B-A configuration
	// of points. This can happen if the ring does not contain 3 distinct points
	// (including the case where the input array has fewer than 4 elements), or
	// it contains coincident line segments.
	if internal.Equal(ring, iPrev, ring, hiIndex) || internal.Equal(ring, iNext, ring, hiIndex) || internal.Equal(ring, iPrev, ring, iNext) {
		return false
	}

	disc := bigxy.OrientationIndex(geom.Coord(ring[iPrev:iPrev+2]), geom.Coord(ring[hiIndex:hiIndex+2]), geom.Coord(ring[iNext:iNext+2]))

	// If disc is exactly 0, lines are collinear. There are two possible cases:
	// (1) the lines lie along the x axis in opposite directions (2) the lines
	// lie on top of one another
	//
	// (1) is handled by checking if next is left of prev ==> CCW (2) will never
	// happen if the ring is valid, so don't check for it (Might want to assert
	// this)
	var isCCW bool
	if disc == 0 {
		// poly is CCW if prev x is right of next x
		isCCW = (ring[iPrev] > ring[iNext])
	} else {
		// if area is positive, points are ordered CCW
		isCCW = (disc > 0)
	}
	return isCCW
}

// DistanceFromPointToLine computes the distance from a point p to a line segment lineStart/lineEnd
//
// Note: NON-ROBUST!
func DistanceFromPointToLine(p, lineStart, lineEnd geom.Coord) float64 {
	// if start = end, then just compute distance to one of the endpoints
	if lineStart[0] == lineEnd[0] && lineStart[1] == lineEnd[1] {
		return internal.Distance2D(p, lineStart)
	}

	// otherwise use comp.graphics.algorithms Frequently Asked Questions method

	// (1) r = AC dot AB
	//         ---------
	//         ||AB||^2
	//
	// r has the following meaning:
	//   r=0 P = A
	//   r=1 P = B
	//   r<0 P is on the backward extension of AB
	//   r>1 P is on the forward extension of AB
	//   0<r<1 P is interior to AB

	len2 := (lineEnd[0]-lineStart[0])*(lineEnd[0]-lineStart[0]) + (lineEnd[1]-lineStart[1])*(lineEnd[1]-lineStart[1])
	r := ((p[0]-lineStart[0])*(lineEnd[0]-lineStart[0]) + (p[1]-lineStart[1])*(lineEnd[1]-lineStart[1])) / len2

	if r <= 0.0 {
		return internal.Distance2D(p, lineStart)
	}
	if r >= 1.0 {
		return internal.Distance2D(p, lineEnd)
	}

	// (2) s = (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
	//         -----------------------------
	//                    L^2
	//
	// Then the distance from C to P = |s|*L.
	//
	// This is the same calculation as {@link #distancePointLinePerpendicular}.
	// Unrolled here for performance.
	s := ((lineStart[1]-p[1])*(lineEnd[0]-lineStart[0]) - (lineStart[0]-p[0])*(lineEnd[1]-lineStart[1])) / len2
	return math.Abs(s) * math.Sqrt(len2)
}

// PerpendicularDistanceFromPointToLine computes the perpendicular distance from a point p to the (infinite) line
// containing the points lineStart/lineEnd
func PerpendicularDistanceFromPointToLine(p, lineStart, lineEnd geom.Coord) float64 {
	// use comp.graphics.algorithms Frequently Asked Questions method
	/*
	 * (2) s = (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
	 *         -----------------------------
	 *                    L^2
	 *
	 * Then the distance from C to P = |s|*L.
	 */
	len2 := (lineEnd[0]-lineStart[0])*(lineEnd[0]-lineStart[0]) + (lineEnd[1]-lineStart[1])*(lineEnd[1]-lineStart[1])
	s := ((lineStart[1]-p[1])*(lineEnd[0]-lineStart[0]) - (lineStart[0]-p[0])*(lineEnd[1]-lineStart[1])) / len2

	distance := math.Abs(s) * math.Sqrt(len2)
	return distance
}

// DistanceFromPointToLineString computes the distance from a point to a sequence of line segments.
//
// Param p - a point
// Param line - a sequence of contiguous line segments defined by their vertices
func DistanceFromPointToLineString(layout geom.Layout, p geom.Coord, line []float64) float64 {
	if len(line) < 2 {
		panic(fmt.Sprintf("Line array must contain at least one vertex: %v", line))
	}
	// this handles the case of length = 1
	firstPoint := line[0:2]
	minDistance := internal.Distance2D(p, firstPoint)
	stride := layout.Stride()
	for i := 0; i < len(line)-stride; i += stride {
		point1 := geom.Coord(line[i : i+2])
		point2 := geom.Coord(line[i+stride : i+stride+2])
		dist := DistanceFromPointToLine(p, point1, point2)
		if dist < minDistance {
			minDistance = dist
		}
	}
	return minDistance
}

// DistanceFromLineToLine computes the distance from a line segment line1(Start/End) to a line segment line2(Start/End)
//
// Note: NON-ROBUST!
//
// param line1Start - the start point of line1
// param line1End - the end point of line1 (must be different to line1Start)
// param line2Start - the start point of line2
// param line2End - the end point of line2 (must be different to line2Start)
func DistanceFromLineToLine(line1Start, line1End, line2Start, line2End geom.Coord) float64 {
	// check for zero-length segments
	if line1Start.Equal(geom.XY, line1End) {
		return DistanceFromPointToLine(line1Start, line2Start, line2End)
	}
	if line2Start.Equal(geom.XY, line2End) {
		return DistanceFromPointToLine(line2End, line1Start, line1End)
	}
	// Let AB == line1 where A == line1Start and B == line1End
	// Let CD == line2 where C == line2Start and D == line2End
	//
	// AB and CD are line segments
	// from comp.graphics.algo
	//
	// Solving the above for r and s yields
	//
	//     (Ay-Cy)(Dx-Cx)-(Ax-Cx)(Dy-Cy)
	// r = ----------------------------- (eqn 1)
	//     (Bx-Ax)(Dy-Cy)-(By-Ay)(Dx-Cx)
	//
	//     (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
	// s = ----------------------------- (eqn 2)
	//     (Bx-Ax)(Dy-Cy)-(By-Ay)(Dx-Cx)
	//
	// Let P be the position vector of the
	// intersection point, then
	//   P=A+r(B-A) or
	//   Px=Ax+r(Bx-Ax)
	//   Py=Ay+r(By-Ay)
	// By examining the values of r & s, you can also determine some other limiting
	// conditions:
	//   If 0<=r<=1 & 0<=s<=1, intersection exists
	//      r<0 or r>1 or s<0 or s>1 line segments do not intersect
	//   If the denominator in eqn 1 is zero, AB & CD are parallel
	//   If the numerator in eqn 1 is also zero, AB & CD are collinear.

	noIntersection := false
	if !internal.DoLinesOverlap(line1Start, line1End, line2Start, line2End) {
		noIntersection = true
	} else {
		denom := (line1End[0]-line1Start[0])*(line2End[1]-line2Start[1]) - (line1End[1]-line1Start[1])*(line2End[0]-line2Start[0])

		if denom == 0 {
			noIntersection = true
		} else {
			rNum := (line1Start[1]-line2Start[1])*(line2End[0]-line2Start[0]) - (line1Start[0]-line2Start[0])*(line2End[1]-line2Start[1])
			sNum := (line1Start[1]-line2Start[1])*(line1End[0]-line1Start[0]) - (line1Start[0]-line2Start[0])*(line1End[1]-line1Start[1])

			s := sNum / denom
			r := rNum / denom

			if (r < 0) || (r > 1) || (s < 0) || (s > 1) {
				noIntersection = true
			}
		}
	}
	if noIntersection {
		return internal.Min(
			DistanceFromPointToLine(line1Start, line2Start, line2End),
			DistanceFromPointToLine(line1End, line2Start, line2End),
			DistanceFromPointToLine(line2Start, line1Start, line1End),
			DistanceFromPointToLine(line2End, line1Start, line1End))
	}
	// segments intersect
	return 0.0
}

// SignedArea computes the signed area for a ring. The signed area is positive if the
// ring is oriented CW, negative if the ring is oriented CCW, and zero if the
// ring is degenerate or flat.
func SignedArea(layout geom.Layout, ring []float64) float64 {
	stride := layout.Stride()
	if len(ring) < 3*stride {
		return 0.0
	}
	sum := 0.0
	// Based on the Shoelace formula.
	// http://en.wikipedia.org/wiki/Shoelace_formula
	x0 := ring[0]
	lenMinusOnePoint := len(ring) - stride
	for i := stride; i < lenMinusOnePoint; i += stride {
		x := ring[i] - x0
		y1 := ring[i+stride+1]
		y2 := ring[i-stride+1]
		sum += float64(x * (y2 - y1)) // nolint:unconvert
	}
	return sum / 2.0
}

// IsPointWithinLineBounds calculates if the point p lays within the bounds of the line
// between end points lineEndpoint1 and lineEndpoint2
func IsPointWithinLineBounds(p, lineEndpoint1, lineEndpoint2 geom.Coord) bool {
	return internal.IsPointWithinLineBounds(p, lineEndpoint1, lineEndpoint2)
}

// DoLinesOverlap calculates if the bounding boxes of the two lines (line1End1, line1End2) and
// (line2End1, line2End2) overlap
func DoLinesOverlap(line1End1, line1End2, line2End1, line2End2 geom.Coord) bool {
	return internal.DoLinesOverlap(line1End1, line1End2, line2End1, line2End2)
}

// Equal checks if the point starting at start one in array coords1 is equal to the
// point starting at start2 in the array coords2.
// Only x and y ordinates are compared and x is assumed to be the first ordinate and y as the second
// This is a utility method intended to be used only when performance is critical as it
// reduces readability.
func Equal(coords1 []float64, start1 int, coords2 []float64, start2 int) bool {
	return internal.Equal(coords1, start1, coords2, start2)
}

// Distance calculates the 2d distance between the two coordinates
func Distance(c1, c2 geom.Coord) float64 {
	return internal.Distance2D(c1, c2)
}
