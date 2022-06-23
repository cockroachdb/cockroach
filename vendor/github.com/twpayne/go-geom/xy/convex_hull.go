package xy

import (
	"sort"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/bigxy"
	"github.com/twpayne/go-geom/sorting"
	"github.com/twpayne/go-geom/transform"
	"github.com/twpayne/go-geom/xy/internal"
	"github.com/twpayne/go-geom/xy/orientation"
)

type convexHullCalculator struct {
	layout   geom.Layout
	stride   int
	inputPts []float64
}

// ConvexHull computes the convex hull of the geometry.
// A convex hull is the smallest convex geometry that contains
// all the points in the input geometry
// Uses the Graham Scan algorithm
func ConvexHull(geometry geom.T) geom.T {
	// copy coords because the algorithm reorders them
	calc := convexHullCalculator{
		layout:   geometry.Layout(),
		stride:   geometry.Layout().Stride(),
		inputPts: geometry.FlatCoords(),
	}

	return calc.getConvexHull()
}

// ConvexHullFlat computes the convex hull of the geometry.
// A convex hull is the smallest convex geometry that contains
// all the points in the input coordinates
// Uses the Graham Scan algorithm
func ConvexHullFlat(layout geom.Layout, coords []float64) geom.T {
	calc := convexHullCalculator{
		inputPts: coords,
		layout:   layout,
		stride:   layout.Stride(),
	}
	return calc.getConvexHull()
}

func (calc convexHullCalculator) getConvexHull() geom.T {
	if len(calc.inputPts) == 0 {
		return nil
	}
	if len(calc.inputPts)/calc.stride == 1 {
		return geom.NewPointFlat(calc.layout, calc.inputPts)
	}
	if len(calc.inputPts)/calc.stride == 2 {
		return geom.NewLineStringFlat(calc.layout, calc.inputPts)
	}

	reducedPts := transform.UniqueCoords(calc.layout, comparator{}, calc.inputPts)

	// use heuristic to reduce points, if large
	if len(calc.inputPts)/calc.stride > 50 {
		reducedPts = calc.reduce(calc.inputPts)
	}
	// sort points for Graham scan.
	calc.preSort(reducedPts)

	// Use Graham scan to find convex hull.
	convexHullCoords := calc.grahamScan(reducedPts)

	// Convert array to appropriate output geometry.
	return calc.lineOrPolygon(convexHullCoords)
}

func (calc *convexHullCalculator) lineOrPolygon(coordinates []float64) geom.T {
	cleanCoords := calc.cleanRing(coordinates)
	if len(cleanCoords) == 3*calc.stride {
		return geom.NewLineStringFlat(calc.layout, cleanCoords[0:len(cleanCoords)-calc.stride])
	}
	return geom.NewPolygonFlat(calc.layout, cleanCoords, []int{len(cleanCoords)})
}

func (calc *convexHullCalculator) cleanRing(original []float64) []float64 {
	cleanedRing := []float64{}
	var previousDistinctCoordinate []float64
	for i := 0; i < len(original)-calc.stride; i += calc.stride {
		if internal.Equal(original, i, original, i+calc.stride) {
			continue
		}
		currentCoordinate := original[i : i+calc.stride]
		nextCoordinate := original[i+calc.stride : i+calc.stride+calc.stride]
		if previousDistinctCoordinate != nil && calc.isBetween(previousDistinctCoordinate, currentCoordinate, nextCoordinate) {
			continue
		}
		cleanedRing = append(cleanedRing, currentCoordinate...)
		previousDistinctCoordinate = currentCoordinate
	}
	return append(cleanedRing, original[len(original)-calc.stride:]...)
}

func (calc *convexHullCalculator) isBetween(c1, c2, c3 []float64) bool {
	if bigxy.OrientationIndex(c1, c2, c3) != orientation.Collinear {
		return false
	}
	if c1[0] != c3[0] {
		if c1[0] <= c2[0] && c2[0] <= c3[0] {
			return true
		}
		if c3[0] <= c2[0] && c2[0] <= c1[0] {
			return true
		}
	}
	if c1[1] != c3[1] {
		if c1[1] <= c2[1] && c2[1] <= c3[1] {
			return true
		}
		if c3[1] <= c2[1] && c2[1] <= c1[1] {
			return true
		}
	}
	return false
}

func (calc *convexHullCalculator) grahamScan(coordData []float64) []float64 {
	coordStack := internal.NewCoordStack(calc.layout)
	coordStack.Push(coordData, 0)
	coordStack.Push(coordData, calc.stride)
	coordStack.Push(coordData, calc.stride*2)
	for i := 3 * calc.stride; i < len(coordData); i += calc.stride {
		p, remaining := coordStack.Pop()
		// check for empty stack to guard against robustness problems
		for remaining > 0 && bigxy.OrientationIndex(geom.Coord(coordStack.Peek()), geom.Coord(p), geom.Coord(coordData[i:i+calc.stride])) > 0 {
			p, _ = coordStack.Pop()
		}
		coordStack.Push(p, 0)
		coordStack.Push(coordData, i)
	}
	coordStack.Push(coordData, 0)
	return coordStack.Data
}

func (calc *convexHullCalculator) preSort(pts []float64) {
	// find the lowest point in the set. If two or more points have
	// the same minimum y coordinate choose the one with the minimu x.
	// This focal point is put in array location pts[0].
	for i := calc.stride; i < len(pts); i += calc.stride {
		if pts[i+1] < pts[1] || (pts[i+1] == pts[1] && pts[i] < pts[0]) {
			for k := 0; k < calc.stride; k++ {
				pts[k], pts[i+k] = pts[i+k], pts[k]
			}
		}
	}

	// sort the points radially around the focal point.
	sort.Sort(NewRadialSorting(calc.layout, pts, geom.Coord{pts[0], pts[1]}))
}

// Uses a heuristic to reduce the number of points scanned
// to compute the hull.
// The heuristic is to find a polygon guaranteed to
// be in (or on) the hull, and eliminate all points inside it.
// A quadrilateral defined by the extremal points
// in the four orthogonal directions
// can be used, but even more inclusive is
// to use an octilateral defined by the points in the 8 cardinal directions.
//
// Note that even if the method used to determine the polygon vertices
// is not 100% robust, this does not affect the robustness of the convex hull.
//
// To satisfy the requirements of the Graham Scan algorithm,
// the returned array has at least 3 entries.
//
func (calc *convexHullCalculator) reduce(inputPts []float64) []float64 {
	polyPts := calc.computeOctRing(inputPts)

	if polyPts == nil {
		return inputPts
	}

	// add points defining polygon
	reducedSet := transform.NewTreeSet(calc.layout, comparator{})
	for i := 0; i < len(polyPts); i += calc.stride {
		reducedSet.Insert(polyPts[i : i+calc.stride])
	}

	/**
	 * Add all unique points not in the interior poly.
	 * CGAlgorithms.isPointInRing is not defined for points actually on the ring,
	 * but this doesn't matter since the points of the interior polygon
	 * are forced to be in the reduced set.
	 */
	for i := 0; i < len(inputPts); i += calc.stride {
		pt := geom.Coord(inputPts[i : i+calc.stride])
		if !IsPointInRing(calc.layout, pt, polyPts) {
			reducedSet.Insert(pt)
		}
	}

	reducedPts := reducedSet.ToFlatArray()

	// ensure that computed array has at least 3 points (not necessarily unique)
	if len(reducedPts) < 3*calc.stride {
		return calc.padArray3(reducedPts)
	}
	return reducedPts
}

func (calc *convexHullCalculator) padArray3(pts []float64) []float64 {
	pad := make([]float64, 3*calc.stride)

	for i := 0; i < len(pad); i++ {
		if i < len(pts) {
			pad[i] = pts[i]
		} else {
			pad[i] = pts[0]
		}
	}
	return pad
}

func (calc *convexHullCalculator) computeOctRing(inputPts []float64) []float64 {
	stride := calc.stride
	octPts := calc.computeOctPts(inputPts)

	// Dedup adjacent points, only keep ones that are different from previous.
	uniquePts := octPts[0:stride]
	for i := stride; i < len(octPts); i += stride {
		if !internal.Equal(octPts, i-stride, octPts, i) {
			uniquePts = append(uniquePts, octPts[i:i+stride]...)
		}
	}

	// Need at least 3 unique points (a triangle) to exclude anything inside.
	if len(uniquePts) < 3*stride {
		return nil
	}

	return uniquePts
}

func (calc *convexHullCalculator) computeOctPts(inputPts []float64) []float64 {
	stride := calc.stride
	pts := make([]float64, 8*stride)

	for j := 0; j < len(pts); j += stride {
		for k := 0; k < stride; k++ {
			pts[j+k] = inputPts[k]
		}
	}

	for i := stride; i < len(inputPts); i += stride {
		if inputPts[i] < pts[0] {
			for k := 0; k < stride; k++ {
				pts[k] = inputPts[i+k]
			}
		}
		if inputPts[i]-inputPts[i+1] < pts[stride]-pts[stride+1] {
			for k := 0; k < stride; k++ {
				pts[stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i+1] > pts[2*stride+1] {
			for k := 0; k < stride; k++ {
				pts[2*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i]+inputPts[i+1] > pts[3*stride]+pts[3*stride+1] {
			for k := 0; k < stride; k++ {
				pts[3*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i] > pts[4*stride] {
			for k := 0; k < stride; k++ {
				pts[4*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i]-inputPts[i+1] > pts[5*stride]-pts[5*stride+1] {
			for k := 0; k < stride; k++ {
				pts[5*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i+1] < pts[6*stride+1] {
			for k := 0; k < stride; k++ {
				pts[6*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i]+inputPts[i+1] < pts[7*stride]+pts[7*stride+1] {
			for k := 0; k < stride; k++ {
				pts[7*stride+k] = inputPts[i+k]
			}
		}
	}
	return pts
}

type comparator struct{}

func (c comparator) IsEquals(x, y geom.Coord) bool {
	return internal.Equal(x, 0, y, 0)
}

func (c comparator) IsLess(x, y geom.Coord) bool {
	return sorting.IsLess2D(x, y)
}
