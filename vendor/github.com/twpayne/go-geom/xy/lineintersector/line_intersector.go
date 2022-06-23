package lineintersector

import (
	"math"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/lineintersection"
)

// Strategy is the line intersection implementation
type Strategy interface {
	computePointOnLineIntersection(data *lineIntersectorData, p, lineEndpoint1, lineEndpoint2 geom.Coord)
	computeLineOnLineIntersection(data *lineIntersectorData, line1End1, line1End2, line2End1, line2End2 geom.Coord)
}

// PointIntersectsLine tests if point intersects the line
func PointIntersectsLine(strategy Strategy, point, lineStart, lineEnd geom.Coord) (hasIntersection bool) {
	intersectorData := &lineIntersectorData{
		strategy:           strategy,
		inputLines:         [2][2]geom.Coord{{lineStart, lineEnd}, {}},
		intersectionPoints: [2]geom.Coord{{0, 0}, {0, 0}},
	}

	intersectorData.pa = intersectorData.intersectionPoints[0]
	intersectorData.pb = intersectorData.intersectionPoints[1]

	strategy.computePointOnLineIntersection(intersectorData, point, lineStart, lineEnd)

	return intersectorData.intersectionType != lineintersection.NoIntersection
}

// LineIntersectsLine tests if the first line (line1Start,line1End) intersects the second line (line2Start, line2End)
// and returns a data structure that indicates if there was an intersection, the type of intersection and where the intersection
// was.  See lineintersection.Result for a more detailed explanation of the result object
func LineIntersectsLine(strategy Strategy, line1Start, line1End, line2Start, line2End geom.Coord) lineintersection.Result {
	intersectorData := &lineIntersectorData{
		strategy:           strategy,
		inputLines:         [2][2]geom.Coord{{line2Start, line2End}, {line1Start, line1End}},
		intersectionPoints: [2]geom.Coord{{0, 0}, {0, 0}},
	}

	intersectorData.pa = intersectorData.intersectionPoints[0]
	intersectorData.pb = intersectorData.intersectionPoints[1]

	strategy.computeLineOnLineIntersection(intersectorData, line1Start, line1End, line2Start, line2End)

	var intersections []geom.Coord

	switch intersectorData.intersectionType {
	case lineintersection.NoIntersection:
		intersections = []geom.Coord{}
	case lineintersection.PointIntersection:
		intersections = intersectorData.intersectionPoints[:1]
	case lineintersection.CollinearIntersection:
		intersections = intersectorData.intersectionPoints[:2]
	}
	return lineintersection.NewResult(intersectorData.intersectionType, intersections)
}

// An internal data structure for containing the data during calculations
type lineIntersectorData struct {
	// new Coordinate[2][2];
	inputLines [2][2]geom.Coord

	// if only a point intersection then 0 index coord will contain the intersection point
	// if co-linear (lines overlay each other) the two coordinates represent the start and end points of the overlapping lines.
	intersectionPoints [2]geom.Coord
	intersectionType   lineintersection.Type

	// The indexes of the endpoints of the intersection lines, in order along
	// the corresponding line
	isProper bool
	pa, pb   geom.Coord
	strategy Strategy
}

/**
 *  RParameter computes the parameter for the point p
 *  in the parameterized equation
 *  of the line from p1 to p2.
 *  This is equal to the 'distance' of p along p1-p2
 */
func rParameter(p1, p2, p geom.Coord) float64 {
	var r float64
	// compute maximum delta, for numerical stability
	// also handle case of p1-p2 being vertical or horizontal
	if dx, dy := math.Abs(p2[0]-p1[0]), math.Abs(p2[1]-p1[1]); dx > dy {
		r = (p[0] - p1[0]) / (p2[0] - p1[0])
	} else {
		r = (p[1] - p1[1]) / (p2[1] - p1[1])
	}
	return r
}
