package lineintersector

import (
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/bigxy"
	"github.com/twpayne/go-geom/xy/internal"
	"github.com/twpayne/go-geom/xy/internal/centralendpoint"
	"github.com/twpayne/go-geom/xy/internal/hcoords"
	"github.com/twpayne/go-geom/xy/lineintersection"
	"github.com/twpayne/go-geom/xy/orientation"
)

// RobustLineIntersector is a less performant implementation when compared to the non robust implementation but
// provides more consistent results in extreme cases
type RobustLineIntersector struct{}

func (intersector RobustLineIntersector) computePointOnLineIntersection(data *lineIntersectorData, point, lineStart, lineEnd geom.Coord) {
	data.isProper = false
	// do between check first, since it is faster than the orientation test
	if internal.IsPointWithinLineBounds(point, lineStart, lineEnd) {
		if bigxy.OrientationIndex(lineStart, lineEnd, point) == orientation.Collinear && bigxy.OrientationIndex(lineEnd, lineStart, point) == orientation.Collinear {
			data.isProper = true
			if internal.Equal(point, 0, lineStart, 0) || internal.Equal(point, 0, lineEnd, 0) {
				data.isProper = false
			}
			data.intersectionType = lineintersection.PointIntersection
			return
		}
	}
	data.intersectionType = lineintersection.NoIntersection
}

func (intersector RobustLineIntersector) computeLineOnLineIntersection(data *lineIntersectorData, line1Start, line1End, line2Start, line2End geom.Coord) {
	data.isProper = false

	// first try a fast test to see if the envelopes of the lines intersect
	if !internal.DoLinesOverlap(line1Start, line1End, line2Start, line2End) {
		data.intersectionType = lineintersection.NoIntersection
		return
	}

	// for each endpoint, compute which side of the other segment it lies
	// if both endpoints lie on the same side of the other segment,
	// the segments do not intersect
	line2StartToLine1Orientation := bigxy.OrientationIndex(line1Start, line1End, line2Start)
	line2EndToLine1Orientation := bigxy.OrientationIndex(line1Start, line1End, line2End)

	if (line2StartToLine1Orientation > orientation.Collinear && line2EndToLine1Orientation > orientation.Collinear) || (line2StartToLine1Orientation < orientation.Collinear && line2EndToLine1Orientation < orientation.Collinear) {
		data.intersectionType = lineintersection.NoIntersection
		return
	}

	line1StartToLine2Orientation := bigxy.OrientationIndex(line2Start, line2End, line1Start)
	line1EndToLine2Orientation := bigxy.OrientationIndex(line2Start, line2End, line1End)

	if (line1StartToLine2Orientation > orientation.Collinear && line1EndToLine2Orientation > orientation.Collinear) || (line1StartToLine2Orientation < 0 && line1EndToLine2Orientation < 0) {
		data.intersectionType = lineintersection.NoIntersection
		return
	}

	collinear := line2StartToLine1Orientation == orientation.Collinear && line2EndToLine1Orientation == orientation.Collinear &&
		line1StartToLine2Orientation == orientation.Collinear && line1EndToLine2Orientation == orientation.Collinear

	if collinear {
		data.intersectionType = computeCollinearIntersection(data, line1Start, line1End, line2Start, line2End)
		return
	}

	/*
	 * At this point we know that there is a single intersection point
	 * (since the lines are not collinear).
	 */

	/*
	 *  Check if the intersection is an endpoint. If it is, copy the endpoint as
	 *  the intersection point. Copying the point rather than computing it
	 *  ensures the point has the exact value, which is important for
	 *  robustness. It is sufficient to simply check for an endpoint which is on
	 *  the other line, since at this point we know that the inputLines must
	 *  intersect.
	 */
	if line2StartToLine1Orientation == orientation.Collinear || line2EndToLine1Orientation == orientation.Collinear ||
		line1StartToLine2Orientation == orientation.Collinear || line1EndToLine2Orientation == orientation.Collinear {
		data.isProper = false

		/*
		 * Check for two equal endpoints.
		 * This is done explicitly rather than by the orientation tests
		 * below in order to improve robustness.
		 *
		 * [An example where the orientation tests fail to be consistent is
		 * the following (where the true intersection is at the shared endpoint
		 * POINT (19.850257749638203 46.29709338043669)
		 *
		 * LINESTRING ( 19.850257749638203 46.29709338043669, 20.31970698357233 46.76654261437082 )
		 * and
		 * LINESTRING ( -48.51001596420236 -22.063180333403878, 19.850257749638203 46.29709338043669 )
		 *
		 * which used to produce the INCORRECT result: (20.31970698357233, 46.76654261437082, NaN)
		 *
		 */
		switch {
		case internal.Equal(line1Start, 0, line2Start, 0) || internal.Equal(line1Start, 0, line2End, 0):
			copy(data.intersectionPoints[0], line1Start)
		case internal.Equal(line1End, 0, line2Start, 0) || internal.Equal(line1End, 0, line2End, 0):
			copy(data.intersectionPoints[0], line1End)
		case line2StartToLine1Orientation == orientation.Collinear:
			// Now check to see if any endpoint lies on the interior of the other segment.
			copy(data.intersectionPoints[0], line2Start)
		case line2EndToLine1Orientation == orientation.Collinear:
			copy(data.intersectionPoints[0], line2End)
		case line1StartToLine2Orientation == orientation.Collinear:
			copy(data.intersectionPoints[0], line1Start)
		case line1EndToLine2Orientation == orientation.Collinear:
			copy(data.intersectionPoints[0], line1End)
		}
	} else {
		data.isProper = true
		data.intersectionPoints[0] = intersection(data, line1Start, line1End, line2Start, line2End)
	}

	data.intersectionType = lineintersection.PointIntersection
}

func computeCollinearIntersection(data *lineIntersectorData, line1Start, line1End, line2Start, line2End geom.Coord) lineintersection.Type {
	line2StartWithinLine1Bounds := internal.IsPointWithinLineBounds(line2Start, line1Start, line1End)
	line2EndWithinLine1Bounds := internal.IsPointWithinLineBounds(line2End, line1Start, line1End)
	line1StartWithinLine2Bounds := internal.IsPointWithinLineBounds(line1Start, line2Start, line2End)
	line1EndWithinLine2Bounds := internal.IsPointWithinLineBounds(line1End, line2Start, line2End)

	if line1StartWithinLine2Bounds && line1EndWithinLine2Bounds {
		data.intersectionPoints[0] = line1Start
		data.intersectionPoints[1] = line1End
		return lineintersection.CollinearIntersection
	}

	if line2StartWithinLine1Bounds && line2EndWithinLine1Bounds {
		data.intersectionPoints[0] = line2Start
		data.intersectionPoints[1] = line2End
		return lineintersection.CollinearIntersection
	}

	if line2StartWithinLine1Bounds && line1StartWithinLine2Bounds {
		data.intersectionPoints[0] = line2Start
		data.intersectionPoints[1] = line1Start

		return isPointOrCollinearIntersection(line2Start, line1Start, line2EndWithinLine1Bounds, line1EndWithinLine2Bounds)
	}
	if line2StartWithinLine1Bounds && line1EndWithinLine2Bounds {
		data.intersectionPoints[0] = line2Start
		data.intersectionPoints[1] = line1End

		return isPointOrCollinearIntersection(line2Start, line1End, line2EndWithinLine1Bounds, line1StartWithinLine2Bounds)
	}

	if line2EndWithinLine1Bounds && line1StartWithinLine2Bounds {
		data.intersectionPoints[0] = line2End
		data.intersectionPoints[1] = line1Start

		return isPointOrCollinearIntersection(line2End, line1Start, line2StartWithinLine1Bounds, line1EndWithinLine2Bounds)
	}

	if line2EndWithinLine1Bounds && line1EndWithinLine2Bounds {
		data.intersectionPoints[0] = line2End
		data.intersectionPoints[1] = line1End

		return isPointOrCollinearIntersection(line2End, line1End, line2StartWithinLine1Bounds, line1StartWithinLine2Bounds)
	}

	return lineintersection.NoIntersection
}

func isPointOrCollinearIntersection(lineStart, lineEnd geom.Coord, intersection1, intersection2 bool) lineintersection.Type {
	if internal.Equal(lineStart, 0, lineEnd, 0) && !intersection1 && !intersection2 {
		return lineintersection.PointIntersection
	}
	return lineintersection.CollinearIntersection
}

/**
 * This method computes the actual value of the intersection point.
 * To obtain the maximum precision from the intersection calculation,
 * the coordinates are normalized by subtracting the minimum
 * ordinate values (in absolute value).  This has the effect of
 * removing common significant digits from the calculation to
 * maintain more bits of precision.
 */
func intersection(data *lineIntersectorData, line1Start, line1End, line2Start, line2End geom.Coord) geom.Coord {
	intPt := intersectionWithNormalization(line1Start, line1End, line2Start, line2End)

	/**
	 * Due to rounding it can happen that the computed intersection is
	 * outside the envelopes of the input segments.  Clearly this
	 * is inconsistent.
	 * This code checks this condition and forces a more reasonable answer
	 */
	if !isInSegmentEnvelopes(data, intPt) {
		intPt = centralendpoint.GetIntersection(line1Start, line1End, line2Start, line2End)
	}

	// TODO Enable if we add a precision model
	// if precisionModel != null {
	//	precisionModel.makePrecise(intPt);
	//}

	return intPt
}

func intersectionWithNormalization(line1Start, line1End, line2Start, line2End geom.Coord) geom.Coord {
	var (
		line1End1Norm = make(geom.Coord, 2)
		line1End2Norm = make(geom.Coord, 2)
		line2End1Norm = make(geom.Coord, 2)
		line2End2Norm = make(geom.Coord, 2)
	)
	copy(line1End1Norm, line1Start)
	copy(line1End2Norm, line1End)
	copy(line2End1Norm, line2Start)
	copy(line2End2Norm, line2End)

	normPt := geom.Coord{0, 0}
	normalizeToEnvCentre(line1End1Norm, line1End2Norm, line2End1Norm, line2End2Norm, normPt)

	intPt := safeHCoordinateIntersection(line1End1Norm, line1End2Norm, line2End1Norm, line2End2Norm)

	intPt[0] += normPt[0]
	intPt[1] += normPt[1]

	return intPt
}

/**
 * Computes a segment intersection using homogeneous coordinates.
 * Round-off error can cause the raw computation to fail,
 * (usually due to the segments being approximately parallel).
 * If this happens, a reasonable approximation is computed instead.
 */
func safeHCoordinateIntersection(line1Start, line1End, line2Start, line2End geom.Coord) geom.Coord {
	intPt, err := hcoords.GetIntersection(line1Start, line1End, line2Start, line2End)
	if err != nil {
		return centralendpoint.GetIntersection(line1Start, line1End, line2Start, line2End)
	}
	return intPt
}

/*
 * Test whether a point lies in the envelopes of both input segments.
 * A correctly computed intersection point should return <code>true</code>
 * for this test.
 * Since this test is for debugging purposes only, no attempt is
 * made to optimize the envelope test.
 *
 * returns true if the input point lies within both input segment envelopes
 */
func isInSegmentEnvelopes(data *lineIntersectorData, intersectionPoint geom.Coord) bool {
	intersection1 := internal.IsPointWithinLineBounds(intersectionPoint, data.inputLines[0][0], data.inputLines[0][1])
	intersection2 := internal.IsPointWithinLineBounds(intersectionPoint, data.inputLines[1][0], data.inputLines[1][1])

	return intersection1 && intersection2
}

/**
 * Normalize the supplied coordinates to
 * so that the midpoint of their intersection envelope
 * lies at the origin.
 */
func normalizeToEnvCentre(line1Start, line1End, line2Start, line2End, normPt geom.Coord) {
	// Note: All these "max" checks are inlined for performance.
	// It would be visually cleaner to do that but requires more function calls

	line1MinX := line1End[0]
	if line1Start[0] < line1End[0] {
		line1MinX = line1Start[0]
	}

	line1MinY := line1End[1]
	if line1Start[1] < line1End[1] {
		line1MinY = line1Start[1]
	}
	line1MaxX := line1End[0]
	if line1Start[0] > line1End[0] {
		line1MaxX = line1Start[0]
	}
	line1MaxY := line1End[1]
	if line1Start[1] > line1End[1] {
		line1MaxY = line1Start[1]
	}

	line2MinX := line2End[0]
	if line2Start[0] < line2End[0] {
		line2MinX = line2Start[0]
	}
	line2MinY := line2End[1]
	if line2Start[1] < line2End[1] {
		line2MinY = line2Start[1]
	}
	line2MaxX := line2End[0]
	if line2Start[0] > line2End[0] {
		line2MaxX = line2Start[0]
	}
	line2MaxY := line2End[1]
	if line2Start[1] > line2End[1] {
		line2MaxY = line2Start[1]
	}

	intMinX := line2MinX
	if line1MinX > line2MinX {
		intMinX = line1MinX
	}
	intMaxX := line2MaxX
	if line1MaxX < line2MaxX {
		intMaxX = line1MaxX
	}
	intMinY := line2MinY
	if line1MinY > line2MinY {
		intMinY = line1MinY
	}
	intMaxY := line2MaxY
	if line1MaxY < line2MaxY {
		intMaxY = line1MaxY
	}

	intMidX := (intMinX + intMaxX) / 2.0
	intMidY := (intMinY + intMaxY) / 2.0
	normPt[0] = intMidX
	normPt[1] = intMidY

	line1Start[0] -= normPt[0]
	line1Start[1] -= normPt[1]
	line1End[0] -= normPt[0]
	line1End[1] -= normPt[1]
	line2Start[0] -= normPt[0]
	line2Start[1] -= normPt[1]
	line2End[0] -= normPt[0]
	line2End[1] -= normPt[1]
}
