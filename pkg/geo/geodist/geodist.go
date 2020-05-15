// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geodist finds distances between two geospatial shapes.
package geodist

import "github.com/cockroachdb/errors"

// Point is an interface that represents a geospatial Point.
type Point interface {
	IsShape()
	IsPoint()
}

// Edge is a struct that represents a connection between two points.
type Edge struct {
	V0, V1 Point
}

// LineString is an interface that represents a geospatial LineString.
type LineString interface {
	Edge(i int) Edge
	NumEdges() int
	Vertex(i int) Point
	NumVertexes() int
	IsShape()
	IsLineString()
}

// LinearRing is an interface that represents a geospatial LinearRing.
type LinearRing interface {
	Edge(i int) Edge
	NumEdges() int
	Vertex(i int) Point
	NumVertexes() int
	IsShape()
	IsLinearRing()
}

// shapeWithEdges represents any shape that contains edges.
type shapeWithEdges interface {
	Edge(i int) Edge
	NumEdges() int
}

// Polygon is an interface that represents a geospatial Polygon.
type Polygon interface {
	LinearRing(i int) LinearRing
	NumLinearRings() int
	IsShape()
	IsPolygon()
}

// Shape is an interface that represents any Geospatial shape.
type Shape interface {
	IsShape()
}

var _ Shape = (Point)(nil)
var _ Shape = (LineString)(nil)
var _ Shape = (LinearRing)(nil)
var _ Shape = (Polygon)(nil)

// DistanceUpdater is a provided hook that has a series of functions that allows
// the caller to maintain the distance value desired.
type DistanceUpdater interface {
	// Update updates the distance based on two provided points,
	// returning if the function should return early.
	Update(a Point, b Point) bool
	// OnIntersects is called when two shapes intersects.
	OnIntersects() bool
	// Distance returns the distance to return so far.
	Distance() float64
}

// Crosser is a provided hook that has a series of functions that can help
// to determine edge intersections.
type Crosser interface {
	// ChainCrosser compares the previously evaluated point and the current point
	// and checks whether it crosses another given line.
	ChainCrossing(p Point) bool
}

// DistanceCalculator contains calculations which allow ShapeDistance to calculate
// the distance between two shapes.
type DistanceCalculator interface {
	// DistanceUpdater returns the DistanceUpdater for the given point.
	DistanceUpdater() DistanceUpdater
	// NewCrosser returns a new Crosser.
	NewCrosser(edge Edge, startPoint Point) Crosser
	// PointInLinearRing returns whether the point is inside the given linearRing.
	PointInLinearRing(point Point, linearRing LinearRing) bool
	// ClosestPointToEdge returns the closest point to the infinite line denoted by
	// the edge, and a bool on whether this point lies on the edge segment.
	ClosestPointToEdge(edge Edge, point Point) (Point, bool)
}

// ShapeDistance returns the distance between two given shapes.
// Distance is defined by the DistanceUpdater provided by the interface.
// It returns whether the function above should return early.
func ShapeDistance(c DistanceCalculator, a Shape, b Shape) (bool, error) {
	switch a := a.(type) {
	case Point:
		switch b := b.(type) {
		case Point:
			return c.DistanceUpdater().Update(a, b), nil
		case LineString:
			return onPointToLineString(c, a, b), nil
		case Polygon:
			return onPointToPolygon(c, a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	case LineString:
		switch b := b.(type) {
		case Point:
			return onPointToLineString(c, b, a), nil
		case LineString:
			return onShapeEdgesToShapeEdges(c, a, b), nil
		case Polygon:
			return onLineStringToPolygon(c, a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	case Polygon:
		switch b := b.(type) {
		case Point:
			return onPointToPolygon(c, b, a), nil
		case LineString:
			return onLineStringToPolygon(c, b, a), nil
		case Polygon:
			return onPolygonToPolygon(c, a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	}
	return false, errors.Newf("unknown shape: %T", a)
}

// onPointToEdgeEnds updates the distance using the edges between a point and a shape.
// It will only check the ends of the edges, and assumes the check against .Edge(0).V0 is not required.
func onPointToEdgeEnds(c DistanceCalculator, a Point, b shapeWithEdges) bool {
	for edgeIdx := 0; edgeIdx < b.NumEdges(); edgeIdx++ {
		edge := b.Edge(edgeIdx)
		// Check against all V1 of every edge.
		if c.DistanceUpdater().Update(a, edge.V1) {
			return true
		}
		// Also project the point to the infinite line of the edge, and compare if the closestPoint
		// lies on the edge.
		if closestPoint, ok := c.ClosestPointToEdge(edge, a); ok {
			if c.DistanceUpdater().Update(a, closestPoint) {
				return true
			}
		}
	}
	return false
}

// onPointToLineString updates the distance between a point and a polyline.
// Returns true if the calling function should early exit.
func onPointToLineString(c DistanceCalculator, a Point, b LineString) bool {
	// Compare the first point, to avoid checking each V0 in the chain afterwards.
	if c.DistanceUpdater().Update(a, b.Vertex(0)) {
		return true
	}
	return onPointToEdgeEnds(c, a, b)
}

// onPointToPolygon updates the distance between a point and a polygon.
// Returns true if the calling function should early exit.
func onPointToPolygon(c DistanceCalculator, a Point, b Polygon) bool {
	// If the exterior ring does not cover the outer ring, we just need to calculate the distance
	// to the outer ring.
	if !c.PointInLinearRing(a, b.LinearRing(0)) {
		return onPointToEdgeEnds(c, a, b.LinearRing(0))
	}
	// At this point it may be inside a hole.
	// If it is in a hole, return the distance to the hole.
	for ringIdx := 1; ringIdx < b.NumLinearRings(); ringIdx++ {
		ring := b.LinearRing(ringIdx)
		if c.PointInLinearRing(a, ring) {
			return onPointToEdgeEnds(c, a, ring)
		}
	}

	// Otherwise, we are inside the polygon.
	return c.DistanceUpdater().OnIntersects()
}

// onShapeEdgesToShapeEdges updates the distance between two shapes by
// only looking at the edges.
// Returns true if the calling function should early exit.
func onShapeEdgesToShapeEdges(c DistanceCalculator, a shapeWithEdges, b shapeWithEdges) bool {
	for aEdgeIdx := 0; aEdgeIdx < a.NumEdges(); aEdgeIdx++ {
		aEdge := a.Edge(aEdgeIdx)
		crosser := c.NewCrosser(aEdge, b.Edge(0).V0)
		for bEdgeIdx := 0; bEdgeIdx < b.NumEdges(); bEdgeIdx++ {
			bEdge := b.Edge(bEdgeIdx)
			// If the edges cross, the distance is 0.
			if crosser.ChainCrossing(bEdge.V1) {
				return c.DistanceUpdater().OnIntersects()
			}

			// Compare each vertex against the edge of the other.
			for _, toCheck := range []struct {
				vertex Point
				edge   Edge
			}{
				{aEdge.V0, bEdge},
				{aEdge.V1, bEdge},
				{bEdge.V0, aEdge},
				{bEdge.V1, aEdge},
			} {
				// Check the vertex against the ends of the edges.
				if c.DistanceUpdater().Update(toCheck.vertex, toCheck.edge.V0) ||
					c.DistanceUpdater().Update(toCheck.vertex, toCheck.edge.V1) {
					return true
				}
				// Also check the projection of the vertex onto the edge.
				if closestPoint, ok := c.ClosestPointToEdge(toCheck.edge, toCheck.vertex); ok {
					if c.DistanceUpdater().Update(toCheck.vertex, closestPoint) {
						return true
					}
				}
			}
		}
	}
	return false
}

// onLineStringToPolygon updates the distance between a polyline and a polygon.
// Returns true if the calling function should early exit.
func onLineStringToPolygon(c DistanceCalculator, a LineString, b Polygon) bool {
	// If we know at least one point is outside the exterior ring, then there are two cases:
	// * the line is always outside the exterior ring. We only need to compare the line
	//   against the exterior ring.
	// * the line intersects with the exterior ring.
	// In both these cases, we can defer to the edge to edge comparison between the line
	// and the exterior ring.
	// We use the first point of the linestring for this check.
	if !c.PointInLinearRing(a.Vertex(0), b.LinearRing(0)) {
		return onShapeEdgesToShapeEdges(c, a, b.LinearRing(0))
	}

	// Now we are guaranteed that there is at least one point inside the exterior ring.
	//
	// For a polygon with no holes, the fact that there is a point inside the exterior
	// ring would imply that the distance is zero.
	//
	// However, when there are holes, it is possible that the distance is non-zero if
	// polyline A is completely contained inside a hole. We iterate over the holes and
	// compute the distance between the hole and polyline A.
	// * If polyline A is within the given distance, we can immediately return.
	// * If polyline A does not intersect the hole but there is at least one point inside
	//   the hole, we can immediately return since all points must be inside that hole
	//   (since distance must not have been zero, otherwise we would have returned in the
	//   previous step).
	for ringIdx := 1; ringIdx < b.NumLinearRings(); ringIdx++ {
		hole := b.LinearRing(ringIdx)
		if onShapeEdgesToShapeEdges(c, a, hole) {
			return true
		}
		for pointIdx := 0; pointIdx < a.NumVertexes(); pointIdx++ {
			if c.PointInLinearRing(a.Vertex(pointIdx), hole) {
				return false
			}
		}
	}

	// This means we are inside the exterior ring, and no points are inside a hole.
	// This means the point is inside the polygon.
	return c.DistanceUpdater().OnIntersects()
}

// onPolygonToPolygon updates the distance between two polygons.
// Returns true if the calling function should early exit.
func onPolygonToPolygon(c DistanceCalculator, a Polygon, b Polygon) bool {
	aFirstPoint := a.LinearRing(0).Vertex(0)
	bFirstPoint := b.LinearRing(0).Vertex(0)

	// If there is at least one point on the the exterior ring of B that is outside the exterior ring
	// of A, then we have one of these two cases:
	// * The exterior rings of A and B intersect. The distance can always be found by comparing
	//   the exterior rings.
	// * The exterior rings of A and B never meet. This distance can always be found
	//   by only comparing the exterior rings.
	// If we find the point is inside the exterior ring, A could contain B, so this reasoning
	// does not apply.
	//
	// The same reasoning applies if there is at least one point on the exterior ring of A
	// that is outside the exterior ring of B.
	//
	// As such, we only need to compare the exterior rings if we detect this.
	if !c.PointInLinearRing(bFirstPoint, a.LinearRing(0)) && !c.PointInLinearRing(aFirstPoint, b.LinearRing(0)) {
		return onShapeEdgesToShapeEdges(c, a.LinearRing(0), b.LinearRing(0))
	}

	// If any point of polygon A is inside a hole of polygon B, then either:
	// * A is inside the hole and the closest point can be found by comparing A's outer
	//   linearRing and the hole in B, or
	// * A intersects this hole and the distance is zero, which can also be found by comparing
	//   A's outer linearRing and the hole in B.
	// In this case, we only need to compare the holes of B to contain a single point A.
	for ringIdx := 1; ringIdx < b.NumLinearRings(); ringIdx++ {
		bHole := b.LinearRing(ringIdx)
		if c.PointInLinearRing(aFirstPoint, bHole) {
			return onShapeEdgesToShapeEdges(c, a.LinearRing(0), bHole)
		}
	}

	// Do the same check for the polygons the other way around.
	for ringIdx := 1; ringIdx < a.NumLinearRings(); ringIdx++ {
		aHole := a.LinearRing(ringIdx)
		if c.PointInLinearRing(bFirstPoint, aHole) {
			return onShapeEdgesToShapeEdges(c, b.LinearRing(0), aHole)
		}
	}

	// Now we know either a point of the exterior ring A is definitely inside polygon B
	// or vice versa. This is an intersection.
	return c.DistanceUpdater().OnIntersects()
}
