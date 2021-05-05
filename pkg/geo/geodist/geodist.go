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

import (
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// Point is a union of the point types used in geometry and geography representation.
// The interfaces for distance calculation defined below are shared for both representations,
// and this union helps us avoid heap allocations by doing cheap copy-by-value of points. The
// code that peers inside a Point knows which of the two fields is populated.
type Point struct {
	GeomPoint geom.Coord
	GeogPoint s2.Point
}

// IsShape implements the geodist.Shape interface.
func (p *Point) IsShape() {}

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

var _ Shape = (*Point)(nil)
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
	OnIntersects(p Point) bool
	// Distance returns the distance to return so far.
	Distance() float64
	// IsMaxDistance returns whether the updater is looking for maximum distance.
	IsMaxDistance() bool
	// FlipGeometries is called to flip the order of geometries.
	FlipGeometries()
}

// EdgeCrosser is a provided hook that calculates whether edges intersect.
type EdgeCrosser interface {
	// ChainCrossing assumes there is an edge to compare against, and the previous
	// point `p0` is the start of the next edge. It will then returns whether (p0, p)
	// intersects with the edge and point of intersection if they intersect.
	// When complete, point p will become p0.
	// Desired usage examples:
	//   crosser := NewEdgeCrosser(edge.V0, edge.V1, startingP0)
	//   intersects, _ := crosser.ChainCrossing(p1)
	//   laterIntersects, _ := crosser.ChainCrossing(p2)
	//   intersects |= laterIntersects ....
	ChainCrossing(p Point) (bool, Point)
}

// DistanceCalculator contains calculations which allow ShapeDistance to calculate
// the distance between two shapes.
type DistanceCalculator interface {
	// DistanceUpdater returns the DistanceUpdater for the current set of calculations.
	DistanceUpdater() DistanceUpdater
	// NewEdgeCrosser returns a new EdgeCrosser with the given edge initialized to be
	// the edge to compare against, and the start point to be the start of the first
	// edge to compare against.
	NewEdgeCrosser(edge Edge, startPoint Point) EdgeCrosser
	// PointIntersectsLinearRing returns whether the point intersects the given linearRing.
	PointIntersectsLinearRing(point Point, linearRing LinearRing) bool
	// ClosestPointToEdge returns the closest point to the infinite line denoted by
	// the edge, and a bool on whether this point lies on the edge segment.
	ClosestPointToEdge(edge Edge, point Point) (Point, bool)
	// BoundingBoxIntersects returns whether the bounding boxes of the shapes in
	// question intersect.
	BoundingBoxIntersects() bool
}

// ShapeDistance returns the distance between two given shapes.
// Distance is defined by the DistanceUpdater provided by the interface.
// It returns whether the function above should return early.
func ShapeDistance(c DistanceCalculator, a Shape, b Shape) (bool, error) {
	switch a := a.(type) {
	case *Point:
		switch b := b.(type) {
		case *Point:
			return c.DistanceUpdater().Update(*a, *b), nil
		case LineString:
			return onPointToLineString(c, *a, b), nil
		case Polygon:
			return onPointToPolygon(c, *a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	case LineString:
		switch b := b.(type) {
		case *Point:
			c.DistanceUpdater().FlipGeometries()
			// defer to restore the order of geometries at the end of the function call.
			defer c.DistanceUpdater().FlipGeometries()
			return onPointToLineString(c, *b, a), nil
		case LineString:
			return onShapeEdgesToShapeEdges(c, a, b), nil
		case Polygon:
			return onLineStringToPolygon(c, a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	case Polygon:
		switch b := b.(type) {
		case *Point:
			c.DistanceUpdater().FlipGeometries()
			// defer to restore the order of geometries at the end of the function call.
			defer c.DistanceUpdater().FlipGeometries()
			return onPointToPolygon(c, *b, a), nil
		case LineString:
			c.DistanceUpdater().FlipGeometries()
			// defer to restore the order of geometries at the end of the function call.
			defer c.DistanceUpdater().FlipGeometries()
			return onLineStringToPolygon(c, b, a), nil
		case Polygon:
			return onPolygonToPolygon(c, a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	}
	return false, errors.Newf("unknown shape: %T", a)
}

// onPointToEdgesExceptFirstEdgeStart updates the distance against the edges of a shape and a point.
// It will only check the V1 of each edge and assumes the first edge start does not need the distance
// to be computed.
func onPointToEdgesExceptFirstEdgeStart(c DistanceCalculator, a Point, b shapeWithEdges) bool {
	for edgeIdx, bNumEdges := 0, b.NumEdges(); edgeIdx < bNumEdges; edgeIdx++ {
		edge := b.Edge(edgeIdx)
		// Check against all V1 of every edge.
		if c.DistanceUpdater().Update(a, edge.V1) {
			return true
		}
		// The max distance between a point and the set of points representing an edge is the
		// maximum distance from the point and the pair of end-points of the edge, so we don't
		// need to update the distance using the projected point.
		if !c.DistanceUpdater().IsMaxDistance() {
			// Also project the point to the infinite line of the edge, and compare if the closestPoint
			// lies on the edge.
			if closestPoint, ok := c.ClosestPointToEdge(edge, a); ok {
				if c.DistanceUpdater().Update(a, closestPoint) {
					return true
				}
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
	return onPointToEdgesExceptFirstEdgeStart(c, a, b)
}

// onPointToPolygon updates the distance between a point and a polygon.
// Returns true if the calling function should early exit.
func onPointToPolygon(c DistanceCalculator, a Point, b Polygon) bool {
	// MaxDistance: When computing the maximum distance, the cases are:
	//   - The point P is not contained in the exterior of the polygon G.
	//     Say vertex V is the vertex of the exterior of the polygon that is
	//     furthest away from point P (among all the exterior vertices).
	//   - One can prove that any vertex of the holes will be closer to point P than vertex V.
	//     Similarly we can prove that any point in the interior of the polygon is closer to P than vertex V.
	//     Therefore we only need to compare with the exterior.
	//   - The point P is contained in the exterior and inside a hole of polygon G.
	//     One can again prove that the furthest point in the polygon from P is one of the vertices of the exterior.
	//   - The point P is contained in the polygon. One can again prove the same property.
	//   So we only need to compare with the exterior ring.
	// MinDistance: If the exterior ring does not contain the point, we just need to calculate the distance to
	//   the exterior ring.
	// BoundingBoxIntersects: if the bounding box of the shape being calculated does not intersect,
	//   then we only need to compare the outer loop.
	if c.DistanceUpdater().IsMaxDistance() || !c.BoundingBoxIntersects() ||
		!c.PointIntersectsLinearRing(a, b.LinearRing(0)) {
		return onPointToEdgesExceptFirstEdgeStart(c, a, b.LinearRing(0))
	}
	// At this point it may be inside a hole.
	// If it is in a hole, return the distance to the hole.
	for ringIdx := 1; ringIdx < b.NumLinearRings(); ringIdx++ {
		ring := b.LinearRing(ringIdx)
		if c.PointIntersectsLinearRing(a, ring) {
			return onPointToEdgesExceptFirstEdgeStart(c, a, ring)
		}
	}

	// Otherwise, we are inside the polygon.
	return c.DistanceUpdater().OnIntersects(a)
}

// onShapeEdgesToShapeEdges updates the distance between two shapes by
// only looking at the edges.
// Returns true if the calling function should early exit.
func onShapeEdgesToShapeEdges(c DistanceCalculator, a shapeWithEdges, b shapeWithEdges) bool {
	for aEdgeIdx, aNumEdges := 0, a.NumEdges(); aEdgeIdx < aNumEdges; aEdgeIdx++ {
		aEdge := a.Edge(aEdgeIdx)
		var crosser EdgeCrosser
		// MaxDistance: the max distance between 2 edges is the maximum of the distance across
		//   pairs of vertices chosen from each edge.
		//   It does not matter whether the edges cross, so we skip this check.
		// BoundingBoxIntersects: if the bounding box of the two shapes do not intersect,
		//   then we don't need to check whether edges intersect either.
		if !c.DistanceUpdater().IsMaxDistance() && c.BoundingBoxIntersects() {
			crosser = c.NewEdgeCrosser(aEdge, b.Edge(0).V0)
		}
		for bEdgeIdx, bNumEdges := 0, b.NumEdges(); bEdgeIdx < bNumEdges; bEdgeIdx++ {
			bEdge := b.Edge(bEdgeIdx)
			if crosser != nil {
				// If the edges cross, the distance is 0.
				intersects, intersectionPoint := crosser.ChainCrossing(bEdge.V1)
				if intersects {
					return c.DistanceUpdater().OnIntersects(intersectionPoint)
				}
			}

			// Check the vertex against the ends of the edges.
			if c.DistanceUpdater().Update(aEdge.V0, bEdge.V0) ||
				c.DistanceUpdater().Update(aEdge.V0, bEdge.V1) ||
				c.DistanceUpdater().Update(aEdge.V1, bEdge.V0) ||
				c.DistanceUpdater().Update(aEdge.V1, bEdge.V1) {
				return true
			}
			// Only project vertexes to edges if we are looking at the edges.
			if !c.DistanceUpdater().IsMaxDistance() {
				if projectVertexToEdge(c, aEdge.V0, bEdge) ||
					projectVertexToEdge(c, aEdge.V1, bEdge) {
					return true
				}
				c.DistanceUpdater().FlipGeometries()
				if projectVertexToEdge(c, bEdge.V0, aEdge) ||
					projectVertexToEdge(c, bEdge.V1, aEdge) {
					// Restore the order of geometries.
					c.DistanceUpdater().FlipGeometries()
					return true
				}
				// Restore the order of geometries.
				c.DistanceUpdater().FlipGeometries()
			}
		}
	}
	return false
}

// projectVertexToEdge attempts to project the point onto the given edge.
// Returns true if the calling function should early exit.
func projectVertexToEdge(c DistanceCalculator, vertex Point, edge Edge) bool {
	// Also check the projection of the vertex onto the edge.
	if closestPoint, ok := c.ClosestPointToEdge(edge, vertex); ok {
		if c.DistanceUpdater().Update(vertex, closestPoint) {
			return true
		}
	}
	return false
}

// onLineStringToPolygon updates the distance between a polyline and a polygon.
// Returns true if the calling function should early exit.
func onLineStringToPolygon(c DistanceCalculator, a LineString, b Polygon) bool {
	// MinDistance: If we know at least one point is outside the exterior ring, then there are two cases:
	//   * the line is always outside the exterior ring. We only need to compare the line
	//     against the exterior ring.
	//   * the line intersects with the exterior ring.
	//   In both these cases, we can defer to the edge to edge comparison between the line
	//   and the exterior ring.
	//   We use the first point of the linestring for this check.
	// MaxDistance: the furthest distance from a LineString to a Polygon is always against the
	//   exterior ring. This follows the reasoning under "onPointToPolygon", but we must now
	//   check each point in the LineString.
	// BoundingBoxIntersects: if the bounding box of the two shapes do not intersect,
	//   then the distance is always from the LineString to the exterior ring.
	if c.DistanceUpdater().IsMaxDistance() ||
		!c.BoundingBoxIntersects() ||
		!c.PointIntersectsLinearRing(a.Vertex(0), b.LinearRing(0)) {
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
	//   the hole, must be inside that hole and so the distance of this polyline to this hole
	//   is the distance of this polyline to this polygon.
	for ringIdx := 1; ringIdx < b.NumLinearRings(); ringIdx++ {
		hole := b.LinearRing(ringIdx)
		if onShapeEdgesToShapeEdges(c, a, hole) {
			return true
		}
		for pointIdx := 0; pointIdx < a.NumVertexes(); pointIdx++ {
			if c.PointIntersectsLinearRing(a.Vertex(pointIdx), hole) {
				return false
			}
		}
	}

	// This means we are inside the exterior ring, and no points are inside a hole.
	// This means the point is inside the polygon.
	return c.DistanceUpdater().OnIntersects(a.Vertex(0))
}

// onPolygonToPolygon updates the distance between two polygons.
// Returns true if the calling function should early exit.
func onPolygonToPolygon(c DistanceCalculator, a Polygon, b Polygon) bool {
	aFirstPoint := a.LinearRing(0).Vertex(0)
	bFirstPoint := b.LinearRing(0).Vertex(0)

	// MinDistance:
	// If there is at least one point on the exterior ring of B that is outside the exterior ring
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
	//
	// MaxDistance:
	//   The furthest distance between two polygons is always against the exterior rings of each other.
	//   This closely follows the reasoning pointed out in "onPointToPolygon". Holes are always located
	//   inside the exterior ring of a polygon, so the exterior ring will always contain a point
	//   with a larger max distance.
	// BoundingBoxIntersects: if the bounding box of the two shapes do not intersect,
	//   then the distance is always between the two exterior rings.
	if c.DistanceUpdater().IsMaxDistance() ||
		!c.BoundingBoxIntersects() ||
		(!c.PointIntersectsLinearRing(bFirstPoint, a.LinearRing(0)) &&
			!c.PointIntersectsLinearRing(aFirstPoint, b.LinearRing(0))) {
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
		if c.PointIntersectsLinearRing(aFirstPoint, bHole) {
			return onShapeEdgesToShapeEdges(c, a.LinearRing(0), bHole)
		}
	}

	// Do the same check for the polygons the other way around.
	c.DistanceUpdater().FlipGeometries()
	// defer to restore the order of geometries at the end of the function call.
	defer c.DistanceUpdater().FlipGeometries()
	for ringIdx := 1; ringIdx < a.NumLinearRings(); ringIdx++ {
		aHole := a.LinearRing(ringIdx)
		if c.PointIntersectsLinearRing(bFirstPoint, aHole) {
			return onShapeEdgesToShapeEdges(c, b.LinearRing(0), aHole)
		}
	}

	// Now we know either a point of the exterior ring A is definitely inside polygon B
	// or vice versa. This is an intersection.
	if c.PointIntersectsLinearRing(aFirstPoint, b.LinearRing(0)) {
		return c.DistanceUpdater().OnIntersects(aFirstPoint)
	}
	return c.DistanceUpdater().OnIntersects(bFirstPoint)
}
