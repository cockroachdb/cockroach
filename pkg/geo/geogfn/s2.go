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

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// s2RegionsToPointsAndShapeIndexes separates S2 regions into either being part of the ShapeIndex,
// or an array of points depending on their type.
// ShapeIndexes have useful operations that are optimized, which is why we sometimes
// desire this grouping.
func s2RegionsToPointsAndShapeIndexes(regions []s2.Region) (*s2.ShapeIndex, []s2.Point, error) {
	shapeIndex := s2.NewShapeIndex()
	points := []s2.Point{}
	for _, region := range regions {
		switch region := region.(type) {
		case s2.Point:
			points = append(points, region)
		case *s2.Polygon:
			shapeIndex.Add(region)
		case *s2.Polyline:
			shapeIndex.Add(region)
		default:
			return nil, nil, fmt.Errorf("invalid region: %#v", region)
		}
	}
	return shapeIndex, points, nil
}

// minChordAngle returns the minimum chord angle of a and b.
func minChordAngle(a s1.ChordAngle, b s1.ChordAngle) s1.ChordAngle {
	if a < b {
		return a
	}
	return b
}

// maybeClosestPointToEdge projects the point onto the infinite line represented
// by the edge. This will return the point on the line closest to the edge.
// It will return the closest point on the line, as well as a bool representing
// whether the point that is projected lies directly on the edge as a segment.
//
// For visualization and more, see: Section 6 / Figure 4 of
// "Projective configuration theorems: old wine into new wineskins", Tabachnikov, Serge, 2016/07/16
func maybeClosestPointToEdge(edge s2.Edge, point s2.Point) (s2.Point, bool) {
	// Project the point onto the normal of the edge. A great circle passing through
	// the normal and the point will intersect with the great circle represented
	// by the given edge.
	normal := edge.V0.Vector.Cross(edge.V1.Vector).Normalize()
	// To find the point where the great circle represented by the edge and the
	// great circle represented by (normal, point), we project the point
	// onto the normal.
	normalScaledToPoint := normal.Mul(normal.Dot(point.Vector))
	// The difference between the point and the projection of the normal when normalized
	// should give us a point on the great circle which contains the vertexes of the edge.
	closestPoint := s2.Point{Vector: point.Vector.Sub(normalScaledToPoint).Normalize()}
	// We then check whether the given point lies on the geodesic of the edge,
	// as the above algorithm only generates a point on the great circle
	// represented by the edge.
	return closestPoint, (&s2.Polyline{edge.V0, edge.V1}).IntersectsCell(s2.CellFromPoint(closestPoint))
}
