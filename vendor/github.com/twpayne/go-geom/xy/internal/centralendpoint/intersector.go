package centralendpoint

import (
	"math"

	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/internal"
)

// GetIntersection computes an approximate intersection of two line segments by taking the most central of the endpoints of the segments.
//
// This is effective in cases where the segments are nearly parallel and should intersect at an endpoint.
// It is also a reasonable strategy for cases where the endpoint of one segment lies on or almost on the interior of another one.
// Taking the most central endpoint ensures that the computed intersection point lies in the envelope of the segments.
//
// Also, by always returning one of the input points, this should result  in reducing segment fragmentation.
// Intended to be used as a last resort for  computing ill-conditioned intersection situations which cause other methods to fail.
func GetIntersection(line1End1, line1End2, line2End1, line2End2 geom.Coord) geom.Coord {
	intersector := centralEndpointIntersector{
		line1End1: line1End1,
		line1End2: line1End2,
		line2End1: line2End1,
		line2End2: line2End2,
	}
	intersector.compute()
	return intersector.intersectionPoint
}

type centralEndpointIntersector struct {
	line1End1, line1End2, line2End1, line2End2, intersectionPoint geom.Coord
}

func (intersector *centralEndpointIntersector) compute() {
	pts := [4]geom.Coord{intersector.line1End1, intersector.line1End2, intersector.line2End1, intersector.line2End2}
	centroid := average(pts)
	intersector.intersectionPoint = findNearestPoint(centroid, pts)
}

func average(pts [4]geom.Coord) geom.Coord {
	avg := geom.Coord{0, 0}

	for i := 0; i < len(pts); i++ {
		avg[0] += pts[i][0]
		avg[1] += pts[i][1]
	}
	if n := float64(len(pts)); n > 0 {
		avg[0] /= n
		avg[1] /= n
	}
	return avg
}

func findNearestPoint(p geom.Coord, pts [4]geom.Coord) geom.Coord {
	minDist := math.MaxFloat64
	result := geom.Coord{}
	for i := 0; i < len(pts); i++ {
		dist := internal.Distance2D(p, pts[i])

		// always initialize the result
		if i == 0 || dist < minDist {
			minDist = dist
			result = pts[i]
		}
	}
	return result
}
