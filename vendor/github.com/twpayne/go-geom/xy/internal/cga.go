package internal

import (
	"math"

	"github.com/twpayne/go-geom"
)

// IsPointWithinLineBounds calculates if the point p lays within the bounds of the line
// between end points lineEndpoint1 and lineEndpoint2
func IsPointWithinLineBounds(p, lineEndpoint1, lineEndpoint2 geom.Coord) bool {
	minx := math.Min(lineEndpoint1[0], lineEndpoint2[0])
	maxx := math.Max(lineEndpoint1[0], lineEndpoint2[0])
	miny := math.Min(lineEndpoint1[1], lineEndpoint2[1])
	maxy := math.Max(lineEndpoint1[1], lineEndpoint2[1])
	return minx <= p[0] && maxx >= p[0] && miny <= p[1] && maxy >= p[1]
}

// DoLinesOverlap calculates if the bounding boxes of the two lines (line1End1, line1End2) and
// (line2End1, line2End2) overlap
func DoLinesOverlap(line1End1, line1End2, line2End1, line2End2 geom.Coord) bool {
	min1x := math.Min(line1End1[0], line1End2[0])
	max1x := math.Max(line1End1[0], line1End2[0])
	min1y := math.Min(line1End1[1], line1End2[1])
	max1y := math.Max(line1End1[1], line1End2[1])

	min2x := math.Min(line2End1[0], line2End2[0])
	max2x := math.Max(line2End1[0], line2End2[0])
	min2y := math.Min(line2End1[1], line2End2[1])
	max2y := math.Max(line2End1[1], line2End2[1])

	if min1x > max2x || max1x < min2x {
		return false
	}
	if min1y > max2y || max1y < min2y {
		return false
	}
	return true
}

// Equal checks if the point starting at start one in array coords1 is equal to the
// point starting at start2 in the array coords2.
// Only x and y ordinates are compared and x is assumed to be the first ordinate and y as the second
// This is a utility method intended to be used only when performance is critical as it
// reduces readability.
func Equal(coords1 []float64, start1 int, coords2 []float64, start2 int) bool {
	if coords1[start1] != coords2[start2] {
		return false
	}

	if coords1[start1+1] != coords2[start2+1] {
		return false
	}

	return true
}

// Distance2D calculates the 2d distance between the two coordinates
func Distance2D(c1, c2 geom.Coord) float64 {
	dx := c1[0] - c2[0]
	dy := c1[1] - c2[1]

	return math.Sqrt(float64(dx*dx) + float64(dy*dy)) // nolint:unconvert
}
