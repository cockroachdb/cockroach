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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/golang/geo/s2"
)

// Covers returns whether geography A covers geography B.
// This calculation is done on the sphere.
// Due to minor inaccuracies and lack of certain primitives in S2,
// precision for Covers will be for up to 1cm.
func Covers(a *geo.Geography, b *geo.Geography) (bool, error) {
	return doBinaryPredicate(a, b, &regionCoverPredicate{})
}

// CoveredBy returns whether geography A is covered by geography B.
// This calculation is done on the sphere.
func CoveredBy(a *geo.Geography, b *geo.Geography) (bool, error) {
	return doBinaryPredicate(b, a, &regionCoverPredicate{})
}

// regionCoverPredicate returns whether aRegion covers bRegion.
type regionCoverPredicate struct{}

var _ binaryPredicate = (*regionCoverPredicate)(nil)

func (p *regionCoverPredicate) op(aRegion s2.Region, bRegion s2.Region) (bool, error) {
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
			return false, fmt.Errorf("unknown s2 type of b: %#v", bRegion)
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
			return false, fmt.Errorf("unknown s2 type of b: %#v", bRegion)
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
			return false, fmt.Errorf("unknown s2 type of b: %#v", bRegion)
		}
	default:
		return false, fmt.Errorf("unknown s2 type of a: %#v", aRegion)
	}
	return true, nil
}

// polylineCoversPoints returns whether a polyline covers a given point.
func polylineCoversPoint(a *s2.Polyline, b s2.Point) bool {
	return a.IntersectsCell(s2.CellFromPoint(b))
}

// polylineCoversPointsWithIdx returns whether a polyline covers a given point.
// If true, it will also return an index of which point there was an intersection.
func polylineCoversPointWithIdx(a *s2.Polyline, b s2.Point) (bool, int) {
	for edgeIdx := 0; edgeIdx < a.NumEdges(); edgeIdx++ {
		if edgeCoversPoint(a.Edge(edgeIdx), b) {
			return true, edgeIdx
		}
	}
	return false, -1
}

// polygonCoversPoints returns whether a polygon covers a given point.
func polygonCoversPoint(a *s2.Polygon, b s2.Point) bool {
	// Account for the case where b is on the edge of the polygon.
	return a.ContainsPoint(b) || a.IntersectsCell(s2.CellFromPoint(b))
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
	// We must first check that the start and end of B is contained by A.
	if !aCoversStartOfB || !polylineCoversPoint(a, (*b)[len(*b)-1]) {
		return false
	}

	// We have found "aIdx", which is the first edge that polyline "B".
	aPoints := *a
	bPoints := *b
	aIdx := aCoverBStart
	bIdx := 0

	for aIdx < len(aPoints)-1 && bIdx < len(bPoints)-1 {
		// moved indicates whether we've made forward progress in the lookup.
		moved := false

		aEdge := s2.Edge{V0: aPoints[aIdx], V1: aPoints[aIdx+1]}
		bEdge := s2.Edge{V0: bPoints[bIdx], V1: bPoints[bIdx+1]}

		// If the edge of A contains the start of edge B, move B forward.
		if edgeCoversPoint(aEdge, bEdge.V0) {
			bIdx++
			moved = true
		}

		// If the edge of B contains the end of edge A, move A forward.
		if edgeCoversPoint(bEdge, aEdge.V1) {
			aIdx++
			moved = true
		}

		// Since we have not been able to make progress, we fail since something along
		// the way did not match.
		if !moved {
			return false
		}
	}

	// At this point, everything in B is found to be within A. Since
	// we checked that the start and end of B
	return true
}

// polygonCoversPolyline returns whether polygon a covers polyline b.
func polygonCoversPolyline(a *s2.Polygon, b *s2.Polyline) bool {
	// Check everything of polyline B is within polygon A.
	for _, point := range *b {
		if !polygonCoversPoint(a, point) {
			return false
		}
	}
	// Even if every point of polyline B is inside polygon A, they
	// may form an edge which goes outside of polygon A and back in
	// due to geodetic lines.
	//
	// As such, check if there are any intersections - if so,
	// we do not consider it a covering.
	return !polygonIntersectsPolyline(a, b)
}

// polygonCoversPolygon returns whether polygon a intersects with polygon b.
func polygonCoversPolygon(a *s2.Polygon, b *s2.Polygon) bool {
	// We can rely on Contains here, as if the exteriors are the same it will
	// still return true in here.
	return a.Contains(b)
}
