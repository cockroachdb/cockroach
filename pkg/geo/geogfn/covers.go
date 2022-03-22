// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/golang/geo/s2"
)

// Covers returns whether geography A covers geography B.
//
// This calculation is done on the sphere.
//
// Due to minor inaccuracies and lack of certain primitives in S2,
// precision for Covers will be for up to 1cm.
//
// Current limitations (which are also limitations in PostGIS):
// * POLYGON/LINESTRING only works as "contains" - if any point of the LINESTRING
//   touches the boundary of the polygon, we will return false but should be true - e.g.
//     SELECT st_covers(
//       'multipolygon(((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0)), ((1.0 0.0, 2.0 0.0, 2.0 1.0, 1.0 1.0, 1.0 0.0)))',
//       'linestring(0.0 0.0, 1.0 0.0)'::geography
//     );
//
// * Furthermore, LINESTRINGS that are covered in multiple POLYGONs inside
//   MULTIPOLYGON but NOT within a single POLYGON in the MULTIPOLYGON
//   currently return false but should be true, e.g.
//     SELECT st_covers(
//       'multipolygon(((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0)), ((1.0 0.0, 2.0 0.0, 2.0 1.0, 1.0 1.0, 1.0 0.0)))',
//       'linestring(0.0 0.0, 2.0 0.0)'::geography
//     );
func Covers(a geo.Geography, b geo.Geography) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	return covers(a, b)
}

// covers is the internal calculation for Covers.
func covers(a geo.Geography, b geo.Geography) (bool, error) {
	// Rect "contains" is a version of covers.
	if !a.BoundingRect().Contains(b.BoundingRect()) {
		return false, nil
	}

	// Ignore EMPTY regions in a.
	aRegions, err := a.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return false, err
	}
	// If any of b is empty, we cannot cover it. Error and catch to return false.
	bRegions, err := b.AsS2(geo.EmptyBehaviorError)
	if err != nil {
		if geo.IsEmptyGeometryError(err) {
			return false, nil
		}
		return false, err
	}

	// We need to check each region in B is covered by at least
	// one region of A.
	bRegionsRemaining := make(map[int]struct{}, len(bRegions))
	for i := range bRegions {
		bRegionsRemaining[i] = struct{}{}
	}
	for _, aRegion := range aRegions {
		for bRegionIdx := range bRegionsRemaining {
			regionCovers, err := regionCovers(aRegion, bRegions[bRegionIdx])
			if err != nil {
				return false, err
			}
			if regionCovers {
				delete(bRegionsRemaining, bRegionIdx)
			}
		}
		if len(bRegionsRemaining) == 0 {
			return true, nil
		}
	}
	return false, nil
}

// CoveredBy returns whether geography A is covered by geography B.
// See Covers for limitations.
func CoveredBy(a geo.Geography, b geo.Geography) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	return covers(b, a)
}

// regionCovers returns whether aRegion completely covers bRegion.
func regionCovers(aRegion s2.Region, bRegion s2.Region) (bool, error) {
	switch aRegion := aRegion.(type) {
	case s2.Point:
		switch bRegion := bRegion.(type) {
		case s2.Point:
			return aRegion.ContainsPoint(bRegion), nil
		case *s2.Polyline:
			return false, nil
		case *s2.Polygon:
			return false, nil
		default:
			return false, pgerror.Newf(pgcode.InvalidParameterValue, "unknown s2 type of b: %#v", bRegion)
		}
	case *s2.Polyline:
		switch bRegion := bRegion.(type) {
		case s2.Point:
			return polylineCoversPoint(aRegion, bRegion), nil
		case *s2.Polyline:
			return polylineCoversPolyline(aRegion, bRegion), nil
		case *s2.Polygon:
			return false, nil
		default:
			return false, pgerror.Newf(pgcode.InvalidParameterValue, "unknown s2 type of b: %#v", bRegion)
		}
	case *s2.Polygon:
		switch bRegion := bRegion.(type) {
		case s2.Point:
			return polygonCoversPoint(aRegion, bRegion), nil
		case *s2.Polyline:
			return polygonCoversPolyline(aRegion, bRegion), nil
		case *s2.Polygon:
			return polygonCoversPolygon(aRegion, bRegion), nil
		default:
			return false, pgerror.Newf(pgcode.InvalidParameterValue, "unknown s2 type of b: %#v", bRegion)
		}
	}
	return false, pgerror.Newf(pgcode.InvalidParameterValue, "unknown s2 type of a: %#v", aRegion)
}

// polylineCoversPoints returns whether a polyline covers a given point.
func polylineCoversPoint(a *s2.Polyline, b s2.Point) bool {
	return a.IntersectsCell(s2.CellFromPoint(b))
}

// polylineCoversPointsWithIdx returns whether a polyline covers a given point.
// If true, it will also return an index of the start of the edge where there
// was an intersection.
func polylineCoversPointWithIdx(a *s2.Polyline, b s2.Point) (bool, int) {
	for edgeIdx, aNumEdges := 0, a.NumEdges(); edgeIdx < aNumEdges; edgeIdx++ {
		if edgeCoversPoint(a.Edge(edgeIdx), b) {
			return true, edgeIdx
		}
	}
	return false, -1
}

// polygonCoversPoints returns whether a polygon covers a given point.
func polygonCoversPoint(a *s2.Polygon, b s2.Point) bool {
	return a.IntersectsCell(s2.CellFromPoint(b))
}

// edgeCoversPoint determines whether a given edge contains a point.
func edgeCoversPoint(e s2.Edge, p s2.Point) bool {
	return (&s2.Polyline{e.V0, e.V1}).IntersectsCell(s2.CellFromPoint(p))
}

// polylineCoversPolyline returns whether polyline a covers polyline b.
func polylineCoversPolyline(a *s2.Polyline, b *s2.Polyline) bool {
	if polylineCoversPolylineOrdered(a, b) {
		return true
	}
	// Check reverse ordering works as well.
	reversedB := make([]s2.Point, len(*b))
	for i, point := range *b {
		reversedB[len(reversedB)-1-i] = point
	}
	newBAsPolyline := s2.Polyline(reversedB)
	return polylineCoversPolylineOrdered(a, &newBAsPolyline)
}

// polylineCoversPolylineOrdered returns whether a polyline covers a polyline
// in the same ordering.
func polylineCoversPolylineOrdered(a *s2.Polyline, b *s2.Polyline) bool {
	aCoversStartOfB, aCoverBStart := polylineCoversPointWithIdx(a, (*b)[0])
	// We must first check that the start of B is contained by A.
	if !aCoversStartOfB {
		return false
	}

	aPoints := *a
	bPoints := *b
	// We have found "aIdx" which is the first edge in polyline A
	// that includes the starting vertex of polyline "B".
	// Start checking the covering from this edge.
	aIdx := aCoverBStart
	bIdx := 0

	aEdge := s2.Edge{V0: aPoints[aIdx], V1: aPoints[aIdx+1]}
	bEdge := s2.Edge{V0: bPoints[bIdx], V1: bPoints[bIdx+1]}
	for {
		aEdgeCoversBStart := edgeCoversPoint(aEdge, bEdge.V0)
		aEdgeCoversBEnd := edgeCoversPoint(aEdge, bEdge.V1)
		bEdgeCoversAEnd := edgeCoversPoint(bEdge, aEdge.V1)
		if aEdgeCoversBStart && aEdgeCoversBEnd {
			// If the edge A fully covers edge B, check the next edge.
			bIdx++
			// We are out of edges in B, and A keeps going or stops at the same point.
			// This is a covering.
			if bIdx == len(bPoints)-1 {
				return true
			}
			bEdge = s2.Edge{V0: bPoints[bIdx], V1: bPoints[bIdx+1]}
			// If A and B end at the same place, we need to move A forward.
			if bEdgeCoversAEnd {
				aIdx++
				if aIdx == len(aPoints)-1 {
					// At this point, B extends past A. return false.
					return false
				}
				aEdge = s2.Edge{V0: aPoints[aIdx], V1: aPoints[aIdx+1]}
			}
			continue
		}

		if aEdgeCoversBStart {
			// Edge A doesn't cover the end of B, but it does cover the start.
			// If B doesn't cover the end of A, we're done.
			if !bEdgeCoversAEnd {
				return false
			}
			// If the end of edge B covers the end of A, that means that
			// B is possibly longer than A. If that's the case, truncate B
			// to be the end of A, and move A forward.
			bEdge.V0 = aEdge.V1
			aIdx++
			if aIdx == len(aPoints)-1 {
				// At this point, B extends past A. return false.
				return false
			}
			aEdge = s2.Edge{V0: aPoints[aIdx], V1: aPoints[aIdx+1]}
			continue
		}

		// Otherwise, we're doomed.
		// Edge A does not contain edge B.
		return false
	}
}

// polygonCoversPolyline returns whether polygon a covers polyline b.
func polygonCoversPolyline(a *s2.Polygon, b *s2.Polyline) bool {
	// Check everything of polyline B is in the interior of polygon A.
	for _, vertex := range *b {
		if !polygonCoversPoint(a, vertex) {
			return false
		}
	}
	// Even if every point of polyline B is inside polygon A, they
	// may form an edge which goes outside of polygon A and back in
	// due to holes and concavities.
	//
	// As such, check if there are any intersections - if so,
	// we do not consider it a covering.
	//
	// NOTE: this implementation has a limitation where a vertex of the line could
	// be on the boundary and still technically be "covered" (using GEOS).
	//
	// However, PostGIS seems to consider this as non-covering so we can go
	// with this for now.
	// i.e. `
	//   select st_covers(
	//    'POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))'::geography,
	//    'LINESTRING(0.0 0.0, 1.0 1.0)'::geography);
	// ` returns false, but should be true. This requires some more math to resolve.
	return !polygonIntersectsPolylineEdge(a, b)
}

// polygonIntersectsPolylineEdge returns whether polygon a intersects with
// polyline b by edge. It does not return true if the polyline is completely
// within the polygon.
func polygonIntersectsPolylineEdge(a *s2.Polygon, b *s2.Polyline) bool {
	// Avoid using NumEdges / Edge of the Polygon type as it is not O(1).
	for _, loop := range a.Loops() {
		for loopEdgeIdx, loopNumEdges := 0, loop.NumEdges(); loopEdgeIdx < loopNumEdges; loopEdgeIdx++ {
			loopEdge := loop.Edge(loopEdgeIdx)
			crosser := s2.NewChainEdgeCrosser(loopEdge.V0, loopEdge.V1, (*b)[0])
			for _, nextVertex := range (*b)[1:] {
				if crosser.ChainCrossingSign(nextVertex) != s2.DoNotCross {
					return true
				}
			}
		}
	}
	return false
}

// polygonCoversPolygon returns whether polygon a intersects with polygon b.
func polygonCoversPolygon(a *s2.Polygon, b *s2.Polygon) bool {
	// We can rely on Contains here, as if the boundaries of A and B are on top
	// of each other, it is still considered a containment as well as a covering.
	return a.Contains(b)
}
