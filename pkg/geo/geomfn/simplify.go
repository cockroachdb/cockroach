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
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Simplify simplifies the given Geometry using the Douglas-Pecker algorithm.
// If preserveCollapsed is specified, we will always return a partial LineString
// or Polygon if the entire shape is collapsed.
// Returns a bool if the geometry should be NULL.
func Simplify(
	g geo.Geometry, tolerance float64, preserveCollapsed bool,
) (geo.Geometry, bool, error) {
	if g.ShapeType2D() == geopb.ShapeType_Point ||
		g.ShapeType2D() == geopb.ShapeType_MultiPoint ||
		g.Empty() {
		return g, false, nil
	}
	// Negative tolerances round to zero, similar to PostGIS.
	if tolerance < 0 {
		tolerance = 0
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
		// Though handled by the callee, GeometryCollections can have points/multipoints.
		return t, nil
	case *geom.LineString:
		return simplifySimplifiableShape(t, tolerance, preserveCollapsed)
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
			lrT, err := simplifySimplifiableShape(
				t.LinearRing(i),
				tolerance,
				// We only want to preserve the outer ring.
				preserveCollapsed && i == 0,
			)
			if err != nil {
				return nil, err
			}
			lr := lrT.(*geom.LinearRing)
			if lr.NumCoords() < 4 {
				// If we are the exterior ring without at least 4 points, return nil.
				if i == 0 {
					return nil, nil
				}
				// Otherwise, ignore this inner ring.
				continue
			}
			// Note it is possible for an inner ring to extend outside of the outer ring.
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
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "unknown shape type: %T", t)
	}
}

type simplifiableShape interface {
	geom.T

	Coord(i int) geom.Coord
	NumCoords() int
}

// simplifySimplifiableShape wraps simplifySimplifiableShapeInternal, performing extra
// processing for preserveCollapsed.
func simplifySimplifiableShape(
	t simplifiableShape, tolerance float64, preserveCollapsed bool,
) (geom.T, error) {
	minPoints := 0
	if preserveCollapsed {
		switch t.(type) {
		case *geom.LinearRing:
			minPoints = 4
		case *geom.LineString:
			minPoints = 2
		}
	}

	ret, err := simplifySimplifiableShapeInternal(t, tolerance, minPoints)
	if err != nil {
		return nil, err
	}
	switch ret := ret.(type) {
	case *geom.LineString:
		// If we have a LineString with the same first and last point, we should
		// collapse the geometry entirely.
		if !preserveCollapsed && ret.NumCoords() == 2 && coordEqual(ret.Coord(0), ret.Coord(1)) {
			return nil, nil
		}
	}
	return ret, nil
}

// simplifySimplifiableShapeInternal applies the Douglas-Peucker algorithm to the given
// shape, returning the shape which is simplified.
// Algorithm at https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm#Algorithm.
func simplifySimplifiableShapeInternal(
	t simplifiableShape, tolerance float64, minPoints int,
) (geom.T, error) {
	numCoords := t.NumCoords()
	// If we already have the minimum number of points, return early.
	if numCoords <= 2 || numCoords <= minPoints {
		return t, nil
	}

	// Keep a track of points to keep. Always keep the first and last point.
	keepPoints := make([]bool, numCoords)
	keepPoints[0] = true
	keepPoints[numCoords-1] = true
	numKeepPoints := 2

	if tolerance == 0 {
		// We can special case tolerance 0 to be O(n) by only through
		// the list of coordinates once and determining whether a given
		// intermediate point lies on the edge of the previous and next
		// point.
		prevCoord := t.Coord(0)
		for i := 1; i < t.NumCoords()-1; i++ {
			currCoord := t.Coord(i)
			nextCoord := t.Coord(i + 1)

			// Let AB by the line from the prev coord to the current coord.
			// Let AC be the line from the prev coord to the coord one ahead.
			ab := coordSub(currCoord, prevCoord)
			ac := coordSub(nextCoord, prevCoord)

			// If AB and AC have a cross product of 0, they are parallel.
			// If these are parallel, then we need to determine whether B
			// is on the line segment AC.
			if coordCross(ab, ac) == 0 {
				// Project AB onto line AC. If the projection lies on the edge bounded by
				// AC, we should skip the current point.
				// See `findSplitIndexForSimplifiableShape` for description of the logic.
				r := coordDot(ab, ac) / coordNorm2(ac)
				if r >= 0 && r <= 1 {
					continue
				}
			}

			// Since B does not lie on line AC, we should keep the point.
			prevCoord = currCoord
			keepPoints[i] = true
			numKeepPoints++
		}
	} else {
		// Apply the algorithm, noting points to keep in the keepPoints array.
		var recurse func(startIdx, endIdx int)
		recurse = func(startIdx, endIdx int) {
			// Find the index to split at.
			splitIdx := findSplitIndexForSimplifiableShape(t, startIdx, endIdx, tolerance, minPoints > numKeepPoints)
			// Return if there are no points to split at.
			if splitIdx == startIdx {
				return
			}
			keepPoints[splitIdx] = true
			numKeepPoints++
			recurse(startIdx, splitIdx)
			recurse(splitIdx, endIdx)
		}
		recurse(0, numCoords-1)
	}

	// If we keep every point, return the existing shape.
	if numKeepPoints == numCoords {
		return t, nil
	}

	// Copy the flat coordinates over in the order they came in.
	newFlatCoords := make([]float64, 0, numKeepPoints*t.Layout().Stride())
	for i, keep := range keepPoints {
		shouldAdd := keep
		// We need to add enough points to satisfy minPoints. If we have not marked
		// enough points to keep, add the first points we initially marked as not to
		// keep until we hit minPoints. This is safe as we will still create a shape
		// resembling the original shape.
		// This is required for cases where polygons may simplify into a 2 or 3 point
		// linestring, in which simplification may result in an invalid outer ring.
		if !shouldAdd && numKeepPoints < minPoints {
			shouldAdd = true
			numKeepPoints++
		}
		if shouldAdd {
			newFlatCoords = append(newFlatCoords, []float64(t.Coord(i))...)
		}
	}

	switch t.(type) {
	case *geom.LineString:
		return geom.NewLineStringFlat(t.Layout(), newFlatCoords).SetSRID(t.SRID()), nil
	case *geom.LinearRing:
		return geom.NewLinearRingFlat(t.Layout(), newFlatCoords), nil
	}
	return nil, errors.AssertionFailedf("unknown shape: %T", t)
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
				if r >= 1 {
					// Closest to V1.
					closestPointToEdge = edgeV1
				} else if r > 0 {
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
