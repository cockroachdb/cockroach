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
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// Distance returns the distance between geographies a and b on a sphere or spheroid.
func Distance(
	a *geo.Geography, b *geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid,
) (float64, error) {
	if a.SRID() != b.SRID() {
		return 0, geo.NewMismatchingSRIDsError(a, b)
	}

	aRegions, err := a.AsS2()
	if err != nil {
		return 0, err
	}
	bRegions, err := b.AsS2()
	if err != nil {
		return 0, err
	}
	spheroid := geographiclib.WGS84Spheroid
	if useSphereOrSpheroid == UseSpheroid {
		return distanceSpheroidRegions(spheroid, aRegions, bRegions, 0.0)
	}
	return distanceSphereRegions(spheroid, aRegions, bRegions)
}

//
// Spheroids
//

// distanceSpheroidRegions calculates the distance between two sets of regions on a spheroid.
// It will quit if it finds a distance that is less than stopAfterLE.
// It is not guaranteed to find the absolute minimum distance if stopAfterLE > 0.
//
// !!! SURPRISING BEHAVIOR WARNING !!!
// PostGIS evaluates the distance between spheroid regions by computing the min of
// the pair-wise distance between the cross-product of the regions in A and the regions
// in B, where the pair-wise distance is computed as:
// * Find the two closest points between the pairs of regions using the sphere
//   for distance calculations.
// * Compute the spheroid distance between the two closest points.
//
// This is technically incorrect, since it is possible that the two closest points on
// the spheroid are different than the two closest points on the sphere.
// See distance_test.go for examples of the "truer" distance values.
// Since we aim to be compatible with PostGIS, we adopt the same approach.
//
// TODO(otan): accelerate checks with bounding boxes.
func distanceSpheroidRegions(
	spheroid geographiclib.Spheroid, aRegions []s2.Region, bRegions []s2.Region, stopAfterLE float64,
) (float64, error) {
	minDistance := math.MaxFloat64
	for _, aRegion := range aRegions {
		for _, bRegion := range bRegions {
			minDistanceUpdater := newSpheroidMinDistanceUpdater(spheroid, stopAfterLE)
			earlyExit, err := minDistanceUpdater.onRegionToRegion(aRegion, bRegion)
			if err != nil {
				return 0, err
			}
			minDistance = math.Min(minDistance, minDistanceUpdater.minDistance())
			if earlyExit {
				return minDistance, nil
			}
		}
	}
	return minDistance, nil
}

// spheroidMinDistanceUpdater finds the minimum distance using spheroid calculations.
// Methods will return early if it finds a minimum distance <= stopAfterLE.
type spheroidMinDistanceUpdater struct {
	spheroid    geographiclib.Spheroid
	minEdge     s2.Edge
	minD        s1.ChordAngle
	stopAfterLE s1.ChordAngle
}

// newSpheroidMinDistanceUpdater returns a new spheroidMinDistanceUpdater with the
// correct arguments set up.
func newSpheroidMinDistanceUpdater(
	spheroid geographiclib.Spheroid, stopAfterLE float64,
) *spheroidMinDistanceUpdater {
	return &spheroidMinDistanceUpdater{
		spheroid: spheroid,
		minD:     math.MaxFloat64,
		// Modify the stopAfterLE distance to be 95% of the proposed stopAfterLE, since
		// we use the sphere to calculate the distance and we want to leave a buffer
		// for spheroid distances being slightly off.
		// Distances should differ by a maximum of (2 * spheroid.Flattening)%,
		// so the 5% margin is pretty safe.
		stopAfterLE: s1.ChordAngleFromAngle(s1.Angle(stopAfterLE/spheroid.SphereRadius)) * 0.95,
	}
}

// minDistance returns the minimum distance found so far.
func (u *spheroidMinDistanceUpdater) minDistance() float64 {
	// If the distance is zero, avoid the call to spheroidDistance and return early.
	if u.minD == 0 {
		return 0
	}
	return spheroidDistance(u.spheroid, u.minEdge.V0, u.minEdge.V1)
}

// updateMinDistance updates the minimum distance based on the distance between
// two given points.
// Returns whether a new minimum distance was found which is less than stopAfterLE.
func (u *spheroidMinDistanceUpdater) updateMinDistance(a s2.Point, b s2.Point) bool {
	// Calculate the sphere distance first, as it is much cheaper.
	sphereDistance := s2.ChordAngleBetweenPoints(a, b)
	if sphereDistance < u.minD {
		u.minD = sphereDistance
		u.minEdge = s2.Edge{V0: a, V1: b}
		// If we have a threshold, determine if we can stop early.
		// If the sphere distance is within range of the stopAfter, we can
		// definitively say we've reach the close enough point.
		if u.minD <= u.stopAfterLE {
			return true
		}
	}
	return false
}

// onIntersects sets the minimum distance to 0 and return true.
func (u *spheroidMinDistanceUpdater) onIntersects() bool {
	u.minD = 0
	return true
}

// onRegionToRegion updates the minimum distance between two regions.
// Returns true if a minimum distance <= u.stopAfterLE was found.
func (u *spheroidMinDistanceUpdater) onRegionToRegion(a s2.Region, b s2.Region) (bool, error) {
	switch a := a.(type) {
	case s2.Point:
		switch b := b.(type) {
		case s2.Point:
			return u.updateMinDistance(a, b), nil
		case *s2.Polyline:
			return u.onPointToPolyline(a, b), nil
		case *s2.Polygon:
			return u.onPointToPolygon(a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	case *s2.Polyline:
		switch b := b.(type) {
		case s2.Point:
			return u.onPointToPolyline(b, a), nil
		case *s2.Polyline:
			return u.onShapeEdgesToShapeEdges(a, b), nil
		case *s2.Polygon:
			return u.onPolylineToPolygon(a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	case *s2.Polygon:
		switch b := b.(type) {
		case s2.Point:
			return u.onPointToPolygon(b, a), nil
		case *s2.Polyline:
			return u.onPolylineToPolygon(b, a), nil
		case *s2.Polygon:
			return u.onPolygonToPolygon(a, b), nil
		default:
			return false, errors.Newf("unknown shape: %T", b)
		}
	default:
		return false, errors.Newf("unknown shape: %T", a)
	}
}

// onPointToShapeEdgeEnds updates the minimum distance using the edges between a point and a shape.
// It will only check the ends of the edges, and assumes the check against .Edge(0).V0 is not required.
func (u *spheroidMinDistanceUpdater) onPointToShapeEdgeEnds(a s2.Point, b s2.Shape) bool {
	for edgeIdx := 0; edgeIdx < b.NumEdges(); edgeIdx++ {
		edge := b.Edge(edgeIdx)
		// Check against all V1 of every edge.
		if u.updateMinDistance(a, edge.V1) {
			return true
		}
		// Also project the point to the infinite line of the edge, and compare if the closestPoint
		// lies on the edge.
		if closestPoint, ok := maybeClosestPointToEdge(edge, a); ok {
			if u.updateMinDistance(a, closestPoint) {
				return true
			}
		}
	}
	return false
}

// onPointToPolyline updates the minimum distance between a point and a polyline.
// Returns true if a minimum distance <= u.stopAfterLE was found.
func (u *spheroidMinDistanceUpdater) onPointToPolyline(a s2.Point, b *s2.Polyline) bool {
	// Compare the first point, to avoid checking each V0 in the chain afterwards.
	if u.updateMinDistance(a, (*b)[0]) {
		return true
	}
	return u.onPointToShapeEdgeEnds(a, b)
}

// onPointToPolygon updates the minimum distance between a point and a polygon.
// Returns true if a minimum distance <= u.stopAfterLE was found.
func (u *spheroidMinDistanceUpdater) onPointToPolygon(a s2.Point, b *s2.Polygon) bool {
	// If the point lines in the polygon, return true.
	if b.ContainsPoint(a) {
		return u.onIntersects()
	}
	for _, loop := range b.Loops() {
		// Note we don't have to check against the first vertex of the loop, as it
		// will always appear in V1 at the end.
		if u.onPointToShapeEdgeEnds(a, loop) {
			return true
		}
	}
	return false
}

// onShapeEdgesToShapeEdges updates the minimum distance between two shapes by
// only looking at the edges.
// Returns true if a minimum distance <= u.stopAfterLE was found.
func (u *spheroidMinDistanceUpdater) onShapeEdgesToShapeEdges(a s2.Shape, b s2.Shape) bool {
	for aEdgeIdx := 0; aEdgeIdx < a.NumEdges(); aEdgeIdx++ {
		aEdge := a.Edge(aEdgeIdx)
		crosser := s2.NewChainEdgeCrosser(aEdge.V0, aEdge.V1, b.Edge(0).V0)
		for bEdgeIdx := 0; bEdgeIdx < b.NumEdges(); bEdgeIdx++ {
			bEdge := b.Edge(bEdgeIdx)
			// If the edges cross, the distance is 0.
			if crosser.ChainCrossingSign(bEdge.V1) != s2.DoNotCross {
				return u.onIntersects()
			}

			// Compare each vertex against the edge of the other.
			for _, toCheck := range []struct {
				vertex s2.Point
				edge   s2.Edge
			}{
				{aEdge.V0, bEdge},
				{aEdge.V1, bEdge},
				{bEdge.V0, aEdge},
				{bEdge.V1, aEdge},
			} {
				// Check the vertex against the ends of the edges.
				if u.updateMinDistance(toCheck.vertex, toCheck.edge.V0) ||
					u.updateMinDistance(toCheck.vertex, toCheck.edge.V1) {
					return true
				}
				// Also check the projection of the vertex onto the edge.
				if closestPoint, ok := maybeClosestPointToEdge(toCheck.edge, toCheck.vertex); ok {
					if u.updateMinDistance(toCheck.vertex, closestPoint) {
						return true
					}
				}
			}
		}
	}
	return false
}

// onPolylineToPolygon updates the minimum distance between a polyline and a polygon.
// Returns true if a minimum distance <= u.stopAfterLE was found.
func (u *spheroidMinDistanceUpdater) onPolylineToPolygon(a *s2.Polyline, b *s2.Polygon) bool {
	// If we know at least one point is outside the exterior ring, then there are two cases:
	// * the line is always outside the exterior ring. We only need to compare the line
	//   against the exterior ring.
	// * the line intersects with the exterior ring.
	// In both these cases, we can defer to the edge to edge comparison between the line
	// and the exterior ring.
	// We use the first point of the linestring for this check.
	if !b.Loop(0).ContainsPoint((*a)[0]) {
		return u.onShapeEdgesToShapeEdges(a, b.Loop(0))
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
	for _, hole := range b.Loops()[1:] {
		if u.onShapeEdgesToShapeEdges(a, hole) {
			return true
		}
		for _, linePoint := range *a {
			if hole.ContainsPoint(linePoint) {
				return false
			}
		}
	}

	// This means we are inside the exterior ring, and no points are inside a hole.
	// This means the point is inside the polygon.
	return u.onIntersects()
}

// onPolygonToPolygon updates the minimum distance between two polygons.
// Returns true if a minimum distance <= u.stopAfterLE was found.
func (u *spheroidMinDistanceUpdater) onPolygonToPolygon(a *s2.Polygon, b *s2.Polygon) bool {
	aFirstPoint := a.Loop(0).Vertex(0)
	bFirstPoint := b.Loop(0).Vertex(0)

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
	if !a.Loop(0).ContainsPoint(bFirstPoint) && !b.Loop(0).ContainsPoint(aFirstPoint) {
		return u.onShapeEdgesToShapeEdges(a.Loop(0), b.Loop(0))
	}

	// If any point of polygon A is inside a hole of polygon B, then either:
	// * A is inside the hole and the closest point can be found by comparing A's outer
	//   loop and the hole in B, or
	// * A intersects this hole and the distance is zero, which can also be found by comparing
	//   A's outer loop and the hole in B.
	// In this case, we only need to compare the holes of B to contain a single point A.
	for _, bHole := range b.Loops()[1:] {
		if bHole.ContainsPoint(aFirstPoint) {
			return u.onShapeEdgesToShapeEdges(a.Loop(0), bHole)
		}
	}

	// Do the same check for the polygons the other way around.
	for _, aHole := range a.Loops()[1:] {
		if aHole.ContainsPoint(bFirstPoint) {
			return u.onShapeEdgesToShapeEdges(aHole, b.Loop(0))
		}
	}

	// Now we know either a point of the exterior ring A is definitely inside polygon B
	// or vice versa. This is an intersection.
	return u.onIntersects()
}

//
// Spheres
//

// distanceSphereRegions calculates the distance between two objects on a sphere.
func distanceSphereRegions(
	spheroid geographiclib.Spheroid, aRegions []s2.Region, bRegions []s2.Region,
) (float64, error) {
	aShapeIndex, aPoints, err := s2RegionsToPointsAndShapeIndexes(aRegions)
	if err != nil {
		return 0, err
	}
	bShapeIndex, bPoints, err := s2RegionsToPointsAndShapeIndexes(bRegions)
	if err != nil {
		return 0, err
	}

	minDistanceUpdater := newSphereMinDistanceUpdater(spheroid)
	// Compare aShapeIndex to bShapeIndex as well as all points in B.
	if aShapeIndex.Len() > 0 {
		if bShapeIndex.Len() > 0 {
			if minDistanceUpdater.onShapeIndexToShapeIndex(aShapeIndex, bShapeIndex) {
				return minDistanceUpdater.minDistance(), nil
			}
		}
		for _, bPoint := range bPoints {
			if minDistanceUpdater.onShapeIndexToPoint(aShapeIndex, bPoint) {
				return minDistanceUpdater.minDistance(), nil
			}
		}
	}

	// Then try compare all A points against bShapeIndex and bPoints.
	for _, aPoint := range aPoints {
		if bShapeIndex.Len() > 0 {
			if minDistanceUpdater.onShapeIndexToPoint(bShapeIndex, aPoint) {
				return minDistanceUpdater.minDistance(), nil
			}
		}
		for _, bPoint := range bPoints {
			if minDistanceUpdater.onPointToPoint(aPoint, bPoint) {
				return minDistanceUpdater.minDistance(), nil
			}
		}
	}
	return minDistanceUpdater.minDistance(), nil
}

// sphereMinDistanceUpdater finds the minimum distance on a sphere.
type sphereMinDistanceUpdater struct {
	spheroid geographiclib.Spheroid
	minD     s1.ChordAngle
}

// newSphereMinDistanceUpdater returns a new sphereMinDistanceUpdater.
func newSphereMinDistanceUpdater(spheroid geographiclib.Spheroid) *sphereMinDistanceUpdater {
	return &sphereMinDistanceUpdater{spheroid: spheroid, minD: s1.StraightChordAngle}
}

// onShapeIndexToShapeIndex updates the minimum distance and returns true if distance is 0.
func (u *sphereMinDistanceUpdater) onShapeIndexToShapeIndex(
	a *s2.ShapeIndex, b *s2.ShapeIndex,
) bool {
	u.minD = minChordAngle(u.minD, s2.NewClosestEdgeQuery(a, nil).Distance(s2.NewMinDistanceToShapeIndexTarget(b)))
	return u.minD == 0
}

// onShapeIndexToPoint updates the minimum distance and returns true if distance is 0.
func (u *sphereMinDistanceUpdater) onShapeIndexToPoint(a *s2.ShapeIndex, b s2.Point) bool {
	u.minD = minChordAngle(u.minD, s2.NewClosestEdgeQuery(a, nil).Distance(s2.NewMinDistanceToPointTarget(b)))
	return u.minD == 0
}

// onPointToPoint updates the minimum distance and return true if the distance is 0.
func (u *sphereMinDistanceUpdater) onPointToPoint(a s2.Point, b s2.Point) bool {
	if a == b {
		u.minD = 0
		return true
	}
	u.minD = minChordAngle(u.minD, s2.ChordAngleBetweenPoints(a, b))
	return u.minD == 0
}

// minDistance returns the minimum distance in meters found in the sphereMinDistanceUpdater
// so far.
func (u *sphereMinDistanceUpdater) minDistance() float64 {
	return u.minD.Angle().Radians() * u.spheroid.SphereRadius
}
