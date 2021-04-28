// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Simplify simplifies the given Geometry using the Douglas-Pecker algorithm.
// If preserveCollapsed is specified, we will always return a partial LineString
// or Polygon if the entire shape is collapsed.
// Returns a bool if the geometry should be NULL
func Simplify(
	g geo.Geometry, tolerance float64, preserveCollapsed bool,
) (geo.Geometry, bool, error) {
	if tolerance < 0 {
		return geo.Geometry{}, true, nil
	}
	if math.IsNaN(tolerance) ||
		g.ShapeType2D() == geopb.ShapeType_Point ||
		g.ShapeType2D() == geopb.ShapeType_MultiPoint ||
		g.Empty() {
		return g, false, nil
	}
	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, false, err
	}
	simplifiedT, err := simplify(t, tolerance, preserveCollapsed)
	if err != nil {
		return geo.Geometry{}, false, err
	}
	if simplifiedT == nil {
		return geo.Geometry{}, true, nil
	}

	ret, err := geo.MakeGeometryFromGeomT(simplifiedT)
	return ret, false, err
}

func simplify(t geom.T, tolerance float64, preserveCollapsed bool) (geom.T, error) {
	if t.Empty() {
		return t, nil
	}
	switch t := t.(type) {
	case *geom.Point, *geom.MultiPoint:
		return t, nil
	case *geom.LineString:
		minPoints := 2
		if !preserveCollapsed {
			minPoints = 0
		}
		newFlatCoords := simplifySimplifiableShape(t, tolerance, minPoints)
		ls := geom.NewLineStringFlat(t.Layout(), newFlatCoords).SetSRID(t.SRID())
		// If we have a LineString with the same first and last point, we should
		// collapse the geometry entirely.
		if !preserveCollapsed && ls.NumCoords() == 2 && coordEqual(ls.Coord(0), ls.Coord(1)) {
			return nil, nil
		}
		return ls, nil
	case *geom.MultiLineString:
		ret := geom.NewMultiLineString(t.Layout()).SetSRID(t.SRID())
		for i := 0; i < t.NumLineStrings(); i++ {
			appendT, err := simplify(t.LineString(i), tolerance, preserveCollapsed)
			if err != nil {
				return nil, err
			}
			if appendT != nil {
				if err := ret.Push(appendT.(*geom.LineString)); err != nil {
					return nil, err
				}
			}
		}
		return ret, nil
	case *geom.Polygon:
		p := geom.NewPolygon(t.Layout()).SetSRID(t.SRID())
		for i := 0; i < t.NumLinearRings(); i++ {
			// We want at least 4 points on the outer ring if we preserve collapsed.
			minPoints := 4
			if !preserveCollapsed || i > 0 {
				minPoints = 0
			}

			newFlatCoords := simplifySimplifiableShape(t.LinearRing(i), tolerance, minPoints)
			lr := geom.NewLinearRingFlat(t.Layout(), newFlatCoords)
			if lr.NumCoords() < 4 {
				// If we are the exterior ring and we do not preserve collapsed items,
				// return nil.
				if i == 0 {
					return nil, nil
				}
				// Otherwise, ignore this inner ring.
				continue
			}
			if err := p.Push(lr); err != nil {
				return nil, err
			}
		}
		return p, nil
	case *geom.MultiPolygon:
		ret := geom.NewMultiPolygon(t.Layout()).SetSRID(t.SRID())
		for i := 0; i < t.NumPolygons(); i++ {
			appendT, err := simplify(t.Polygon(i), tolerance, preserveCollapsed)
			if err != nil {
				return nil, err
			}
			if appendT != nil {
				if err := ret.Push(appendT.(*geom.Polygon)); err != nil {
					return nil, err
				}
			}
		}
		return ret, nil
	case *geom.GeometryCollection:
		ret := geom.NewGeometryCollection().SetSRID(t.SRID())
		for _, gcT := range t.Geoms() {
			appendT, err := simplify(gcT, tolerance, preserveCollapsed)
			if err != nil {
				return nil, err
			}
			if appendT != nil {
				if err := ret.Push(appendT); err != nil {
					return nil, err
				}
			}
		}
		return ret, nil
	default:
		return nil, errors.Newf("unknown shape type: %T", t)
	}
}

type simplifiableShape interface {
	geom.T

	Coord(i int) geom.Coord
	NumCoords() int
}

// simplifySimplifiableShape applies the Douglas-Peucker algorithm to the given
// shape, returning the flatCoords.
// Algorithm at https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm#Algorithm.
func simplifySimplifiableShape(t simplifiableShape, tolerance float64, minPoints int) []float64 {
	numCoords := t.NumCoords()
	// If we already have the minimum number of points, return early.
	if numCoords <= 2 || numCoords <= minPoints {
		return t.FlatCoords()
	}

	// Keep a track of points to keep. Always keep the first and last point.
	keepPoints := make([]bool, numCoords)
	keepPoints[0] = true
	keepPoints[numCoords-1] = true
	numKeepPoints := 2

	// Apply the algorithm, noting points to keep in the keepPoints array.
	var recurse func(startIdx, endIdx int)
	recurse = func(startIdx, endIdx int) {
		// Find the index to split at.
		splitIdx := findSplitIndexForSimplifiableShape(t, startIdx, endIdx, tolerance, minPoints > numKeepPoints)
		// If there is no point to split at,
		if splitIdx == startIdx {
			if !keepPoints[startIdx] {
				keepPoints[startIdx] = true
				numKeepPoints++
			}
			if !keepPoints[endIdx] {
				keepPoints[endIdx] = true
				numKeepPoints++
			}
			return
		}
		recurse(startIdx, splitIdx)
		recurse(splitIdx, endIdx)
	}
	recurse(0, numCoords-1)

	// If we keep every point, return the existing flat coordinates.
	if numKeepPoints == numCoords {
		return t.FlatCoords()
	}

	// Copy the flat coordinates over in the order they came in.
	newFlatCoords := make([]float64, 0, numKeepPoints*t.Layout().Stride())
	for i, keep := range keepPoints {
		shouldAdd := keep
		// Always add the point if we don't have enough points to satisfy minPoints.
		if !shouldAdd && numKeepPoints < minPoints {
			shouldAdd = true
			numKeepPoints++
		}
		if shouldAdd {
			newFlatCoords = append(newFlatCoords, []float64(t.Coord(i))...)
		}
	}
	return newFlatCoords
}

// findSplitIndexForSimplifiableShape finds the perpendicular distance for all
// points between the coordinates of the start and end index exclusive to the edge
// formed by coordinates of the start and end index.
// Returns the startIdx if there is no further points to split.
func findSplitIndexForSimplifiableShape(
	t simplifiableShape, startIdx int, endIdx int, tolerance float64, forceSplit bool,
) int {
	// If there are no points between, then we have no more points to split.
	if endIdx-startIdx < 2 {
		return startIdx
	}

	edgeV0 := t.Coord(startIdx)
	edgeV1 := t.Coord(endIdx)

	edgeIsSamePoint := coordEqual(edgeV0, edgeV1)
	splitIdx := startIdx
	var maxDistance float64

	for i := startIdx + 1; i < endIdx; i++ {
		curr := t.Coord(i)
		var dist float64
		// If we are not equal any of the edges, we have to find the closest
		// perpendicular distance from the point to the edge.
		if !coordEqual(curr, edgeV0) && !coordEqual(curr, edgeV1) {
			// Project the point onto the edge. If the point lies on the edge,
			// use that projection. Otherwise, the distance is the minimum distance
			// to one vertex of the edge. We use the math below.
			//
			// From http://www.faqs.org/faqs/graphics/algorithms-faq/, section 1.02
			//
			//  Let the point be C (Cx,Cy) and the line be AB (Ax,Ay) to (Bx,By).
			//  Let P be the point of perpendicular projection of C on AB.  The parameter
			//  r, which indicates P's position along AB, is computed by the dot product
			//  of AC and AB divided by the square of the length of AB:
			//
			//  (1)     AC dot AB
			//      r = ---------
			//          ||AB||^2
			//
			//  r has the following meaning:
			//
			//      r=0      P = A
			//      r=1      P = B
			//      r<0      P is on the backward extension of AB
			//      r>1      P is on the forward extension of AB
			//      0<r<1    P is interior to AB
			closestPointToEdge := edgeV0
			if !edgeIsSamePoint {
				ac := coordSub(curr, edgeV0)
				ab := coordSub(edgeV1, edgeV0)

				r := coordDot(ac, ab) / coordNorm2(ab)
				if r > 1 {
					// Closest to V1.
					closestPointToEdge = edgeV1
				} else if r >= 0 {
					// Use the projection as the closest point.
					closestPointToEdge = coordAdd(edgeV0, coordMul(ab, r))
				}
			}

			// Now find the distance.
			dist = coordNorm(coordSub(closestPointToEdge, curr))
		}
		if (dist > tolerance && dist > maxDistance) || forceSplit {
			forceSplit = false
			splitIdx = i
			maxDistance = dist
		}
	}
	return splitIdx
}
