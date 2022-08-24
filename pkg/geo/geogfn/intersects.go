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

// Intersects returns whether geography A intersects geography B.
// This calculation is done on the sphere.
// Precision of intersect measurements is up to 1cm.
func Intersects(a geo.Geography, b geo.Geography) (bool, error) {
	if !a.BoundingRect().Intersects(b.BoundingRect()) {
		return false, nil
	}
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}

	aRegions, err := a.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return false, err
	}
	bRegions, err := b.AsS2(geo.EmptyBehaviorOmit)
	if err != nil {
		return false, err
	}
	// If any of aRegions intersects any of bRegions, return true.
	for _, aRegion := range aRegions {
		for _, bRegion := range bRegions {
			intersects, err := singleRegionIntersects(aRegion, bRegion)
			if err != nil {
				return false, err
			}
			if intersects {
				return true, nil
			}
		}
	}
	return false, nil
}

// singleRegionIntersects returns true if aRegion intersects bRegion.
func singleRegionIntersects(aRegion s2.Region, bRegion s2.Region) (bool, error) {
	switch aRegion := aRegion.(type) {
	case s2.Point:
		switch bRegion := bRegion.(type) {
		case s2.Point:
			return aRegion.IntersectsCell(s2.CellFromPoint(bRegion)), nil
		case *s2.Polyline:
			return bRegion.IntersectsCell(s2.CellFromPoint(aRegion)), nil
		case *s2.Polygon:
			return bRegion.IntersectsCell(s2.CellFromPoint(aRegion)), nil
		default:
			return false, pgerror.Newf(pgcode.InvalidParameterValue, "unknown s2 type of b: %#v", bRegion)
		}
	case *s2.Polyline:
		switch bRegion := bRegion.(type) {
		case s2.Point:
			return aRegion.IntersectsCell(s2.CellFromPoint(bRegion)), nil
		case *s2.Polyline:
			return polylineIntersectsPolyline(aRegion, bRegion), nil
		case *s2.Polygon:
			return polygonIntersectsPolyline(bRegion, aRegion), nil
		default:
			return false, pgerror.Newf(pgcode.InvalidParameterValue, "unknown s2 type of b: %#v", bRegion)
		}
	case *s2.Polygon:
		switch bRegion := bRegion.(type) {
		case s2.Point:
			return aRegion.IntersectsCell(s2.CellFromPoint(bRegion)), nil
		case *s2.Polyline:
			return polygonIntersectsPolyline(aRegion, bRegion), nil
		case *s2.Polygon:
			return aRegion.Intersects(bRegion), nil
		default:
			return false, pgerror.Newf(pgcode.InvalidParameterValue, "unknown s2 type of b: %#v", bRegion)
		}
	}
	return false, pgerror.Newf(pgcode.InvalidParameterValue, "unknown s2 type of a: %#v", aRegion)
}

// polylineIntersectsPolyline returns whether polyline a intersects with
// polyline b.
func polylineIntersectsPolyline(a *s2.Polyline, b *s2.Polyline) bool {
	for aEdgeIdx, aNumEdges := 0, a.NumEdges(); aEdgeIdx < aNumEdges; aEdgeIdx++ {
		edge := a.Edge(aEdgeIdx)
		crosser := s2.NewChainEdgeCrosser(edge.V0, edge.V1, (*b)[0])
		for _, nextVertex := range (*b)[1:] {
			crossing := crosser.ChainCrossingSign(nextVertex)
			if crossing != s2.DoNotCross {
				return true
			}
		}
	}
	return false
}

// polygonIntersectsPolyline returns whether polygon a intersects with
// polyline b.
func polygonIntersectsPolyline(a *s2.Polygon, b *s2.Polyline) bool {
	// Check if the polygon contains any vertex of the line b.
	for _, vertex := range *b {
		if a.IntersectsCell(s2.CellFromPoint(vertex)) {
			return true
		}
	}
	// Here the polygon does not contain any vertex of the polyline.
	// The polyline can intersect the polygon if a line goes through the polygon
	// with both vertexes that are not in the interior of the polygon.
	// This technique works for holes touching, or holes touching the exterior
	// as the point in which the holes touch is considered an intersection.
	for _, loop := range a.Loops() {
		for loopEdgeIdx, loopNumEdges := 0, loop.NumEdges(); loopEdgeIdx < loopNumEdges; loopEdgeIdx++ {
			loopEdge := loop.Edge(loopEdgeIdx)
			crosser := s2.NewChainEdgeCrosser(loopEdge.V0, loopEdge.V1, (*b)[0])
			for _, nextVertex := range (*b)[1:] {
				crossing := crosser.ChainCrossingSign(nextVertex)
				if crossing != s2.DoNotCross {
					return true
				}
			}
		}
	}
	return false
}
