package lineintersector

import (
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/internal"
	"github.com/twpayne/go-geom/xy/lineintersection"
)

// NonRobustLineIntersector is a performant but non robust line intersection implementation.
type NonRobustLineIntersector struct{}

func (li NonRobustLineIntersector) computePointOnLineIntersection(data *lineIntersectorData, p, lineStart, lineEnd geom.Coord) {
	/*
	 *  Coefficients of line eqns.
	 */
	var r float64
	/*
	 *  'Sign' values
	 */
	data.isProper = false

	/*
	 *  Compute a1, b1, c1, where line joining points 1 and 2
	 *  is "a1 x  +  b1 y  +  c1  =  0".
	 */
	a1 := lineEnd[1] - lineStart[1]
	b1 := lineStart[0] - lineEnd[0]
	c1 := float64(lineEnd[0]*lineStart[1]) - float64(lineStart[0]*lineEnd[1]) // nolint:unconvert

	/*
	 *  Compute r3 and r4.
	 */
	r = float64(a1*p[0]) + float64(b1*p[1]) + c1 // nolint:unconvert

	// if r != 0 the point does not lie on the line
	if r != 0 {
		data.intersectionType = lineintersection.NoIntersection
		return
	}

	// Point lies on line - check to see whether it lies in line segment.

	dist := rParameter(lineStart, lineEnd, p)
	if dist < 0.0 || dist > 1.0 {
		data.intersectionType = lineintersection.NoIntersection
		return
	}

	data.isProper = true
	if p.Equal(geom.XY, lineStart) || p.Equal(geom.XY, lineEnd) {
		data.isProper = false
	}
	data.intersectionType = lineintersection.PointIntersection
}

func (li NonRobustLineIntersector) computeLineOnLineIntersection(data *lineIntersectorData, line1Start, line1End, line2Start, line2End geom.Coord) {
	/*
	 *  Coefficients of line eqns.
	 */
	var a2 float64
	/*
	 *  Coefficients of line eqns.
	 */
	var b2 float64
	/*
	 *  Coefficients of line eqns.
	 */
	var c2, r1, r2, r3, r4 float64
	/*
	 *  'Sign' values
	 */
	// double denom, offset, num;     /* Intermediate values */

	data.isProper = false

	/*
	 *  Compute a1, b1, c1, where line joining points 1 and 2
	 *  is "a1 x  +  b1 y  +  c1  =  0".
	 */
	a1 := line1End[1] - line1Start[1]
	b1 := line1Start[0] - line1End[0]
	c1 := float64(line1End[0]*line1Start[1]) - float64(line1Start[0]*line1End[1]) // nolint:unconvert

	/*
	 *  Compute r3 and r4.
	 */
	r3 = float64(a1*line2Start[0]) + float64(b1*line2Start[1]) + c1 // nolint:unconvert
	r4 = float64(a1*line2End[0]) + float64(b1*line2End[1]) + c1     // nolint:unconvert

	/*
	 *  Check signs of r3 and r4.  If both point 3 and point 4 lie on
	 *  same side of line 1, the line segments do not intersect.
	 */
	if r3 != 0 && r4 != 0 && internal.IsSameSignAndNonZero(r3, r4) {
		data.intersectionType = lineintersection.NoIntersection
		return
	}

	/*
	 *  Compute a2, b2, c2
	 */
	a2 = line2End[1] - line2Start[1]
	b2 = line2Start[0] - line2End[0]
	c2 = float64(line2End[0]*line2Start[1]) - float64(line2Start[0]*line2End[1]) // nolint:unconvert

	/*
	 *  Compute r1 and r2
	 */
	r1 = float64(a2*line1Start[0]) + float64(b2*line1Start[1]) + c2 // nolint:unconvert
	r2 = float64(a2*line1End[0]) + float64(b2*line1End[1]) + c2     // nolint:unconvert

	/*
	 *  Check signs of r1 and r2.  If both point 1 and point 2 lie
	 *  on same side of second line segment, the line segments do
	 *  not intersect.
	 */
	if r1 != 0 && r2 != 0 && internal.IsSameSignAndNonZero(r1, r2) {
		data.intersectionType = lineintersection.NoIntersection
		return
	}

	/**
	 *  Line segments intersect: compute intersection point.
	 */
	denom := float64(a1*b2) - float64(a2*b1) // nolint:unconvert
	if denom == 0 {
		li.computeCollinearIntersection(data, line1Start, line1End, line2Start, line2End)
		return
	}
	numX := float64(b1*c2) - float64(b2*c1) // nolint:unconvert
	data.pa[0] = numX / denom

	numY := float64(a2*c1) - float64(a1*c2) // nolint:unconvert
	data.pa[1] = numY / denom

	// check if this is a proper intersection BEFORE truncating values,
	// to avoid spurious equality comparisons with endpoints
	data.isProper = true
	if data.pa.Equal(geom.XY, line1Start) || data.pa.Equal(geom.XY, line1End) || data.pa.Equal(geom.XY, line2Start) || data.pa.Equal(geom.XY, line2End) {
		data.isProper = false
	}

	data.intersectionType = lineintersection.PointIntersection
}

func (li NonRobustLineIntersector) computeCollinearIntersection(data *lineIntersectorData, line1Start, line1End, line2Start, line2End geom.Coord) {
	var q3, q4 geom.Coord
	var t3, t4 float64
	r1 := float64(0)
	r2 := float64(1)
	r3 := rParameter(line1Start, line1End, line2Start)
	r4 := rParameter(line1Start, line1End, line2End)
	// make sure p3-p4 is in same direction as p1-p2
	if r3 < r4 {
		q3 = line2Start
		t3 = r3
		q4 = line2End
		t4 = r4
	} else {
		q3 = line2End
		t3 = r4
		q4 = line2Start
		t4 = r3
	}
	switch {
	case t3 > r2 || t4 < r1:
		data.intersectionType = lineintersection.NoIntersection
	case &q4 == &line1Start:
		copy(data.pa, line1Start)
		data.intersectionType = lineintersection.PointIntersection
	case &q3 == &line1End:
		copy(data.pa, line1End)
		data.intersectionType = lineintersection.PointIntersection
	default:
		// intersection MUST be a segment - compute endpoints
		copy(data.pa, line1Start)
		if t3 > r1 {
			copy(data.pa, q3)
		}
		copy(data.pb, line1End)
		if t4 < r2 {
			copy(data.pb, q4)
		}
		data.intersectionType = lineintersection.CollinearIntersection
	}
}
