package raycrossing

import (
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/internal/robustdeterminate"
	"github.com/twpayne/go-geom/xy/location"
)

// LocatePointInRing determine where the point is with regards to the ring
func LocatePointInRing(layout geom.Layout, p geom.Coord, ring []float64) location.Type {
	counter := rayCrossingCounter{p: p}

	stride := layout.Stride()

	for i := stride; i < len(ring); i += stride {
		p1 := geom.Coord(ring[i : i+2])
		p2 := geom.Coord(ring[i-stride : i-stride+2])

		counter.countSegment(p1, p2)
		if counter.isPointOnSegment {
			return counter.getLocation()
		}
	}
	return counter.getLocation()
}

type rayCrossingCounter struct {
	p             geom.Coord
	crossingCount int
	// true if the test point lies on an input segment
	isPointOnSegment bool
}

// Gets the {@link Location} of the point relative to
// the ring, polygon
//  or multipolygon from which the processed segments were provided.
//
// This method only determines the correct location
// if <b>all</b> relevant segments must have been processed.
//
// return the Location of the point

func (counter rayCrossingCounter) getLocation() location.Type {
	if counter.isPointOnSegment {
		return location.Boundary
	}

	// The point is in the interior of the ring if the number of X-crossings is
	// odd.
	if (counter.crossingCount % 2) == 1 {
		return location.Interior
	}
	return location.Exterior
}

/**
 * Counts a segment
 *
 * @param p1 an endpoint of the segment
 * @param p2 another endpoint of the segment
 */
func (counter *rayCrossingCounter) countSegment(p1, p2 geom.Coord) {
	/**
	 * For each segment, check if it crosses
	 * a horizontal ray running from the test point in the positive x direction.
	 */

	// check if the segment is strictly to the left of the test point
	if p1[0] < counter.p[0] && p2[0] < counter.p[0] {
		return
	}

	// check if the point is equal to the current ring vertex
	if counter.p[0] == p2[0] && counter.p[1] == p2[1] {
		counter.isPointOnSegment = true
		return
	}

	/**
	 * For horizontal segments, check if the point is on the segment.
	 * Otherwise, horizontal segments are not counted.
	 */
	if p1[1] == counter.p[1] && p2[1] == counter.p[1] {
		minx := p1[0]
		maxx := p2[0]
		if minx > maxx {
			minx = p2[0]
			maxx = p1[0]
		}
		if counter.p[0] >= minx && counter.p[0] <= maxx {
			counter.isPointOnSegment = true
		}
		return
	}
	/**
	 * Evaluate all non-horizontal segments which cross a horizontal ray to the
	 * right of the test pt. To avoid double-counting shared vertices, we use the
	 * convention that
	 * <ul>
	 * <li>an upward edge includes its starting endpoint, and excludes its
	 * final endpoint
	 * <li>a downward edge excludes its starting endpoint, and includes its
	 * final endpoint
	 * </ul>
	 */
	if ((p1[1] > counter.p[1]) && (p2[1] <= counter.p[1])) || ((p2[1] > counter.p[1]) && (p1[1] <= counter.p[1])) {
		// translate the segment so that the test point lies on the origin
		x1 := p1[0] - counter.p[0]
		y1 := p1[1] - counter.p[1]
		x2 := p2[0] - counter.p[0]
		y2 := p2[1] - counter.p[1]

		/**
		 * The translated segment straddles the x-axis. Compute the sign of the
		 * ordinate of intersection with the x-axis. (y2 != y1, so denominator
		 * will never be 0.0)
		 */
		// double xIntSign = RobustDeterminant.signOfDet2x2(x1, y1, x2, y2) / (y2
		// - y1);
		// MD - faster & more robust computation?
		xIntSign := robustdeterminate.SignOfDet2x2(x1, y1, x2, y2)
		if xIntSign == 0.0 {
			counter.isPointOnSegment = true
			return
		}
		if y2 < y1 {
			xIntSign = -xIntSign
		}
		// xsave = xInt;

		// System.out.println("xIntSign(" + x1 + ", " + y1 + ", " + x2 + ", " + y2 + " = " + xIntSign);
		// The segment crosses the ray if the sign is strictly positive.
		if xIntSign > 0.0 {
			counter.crossingCount++
		}
	}
}
