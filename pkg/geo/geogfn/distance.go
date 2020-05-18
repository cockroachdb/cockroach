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
	"github.com/cockroachdb/cockroach/pkg/geo/geodist"
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

// s2GeodistPoint implements geodist.Point.
type s2GeodistPoint struct {
	s2.Point
}

var _ geodist.Point = (*s2GeodistPoint)(nil)

// IsShape implements the geodist.Point interface.
func (*s2GeodistPoint) IsShape() {}

// Point implements the geodist.Point interface.
func (*s2GeodistPoint) IsPoint() {}

// s2GeodistLineString implements geodist.LineString.
type s2GeodistLineString struct {
	*s2.Polyline
}

var _ geodist.LineString = (*s2GeodistLineString)(nil)

// IsShape implements the geodist.LineString interface.
func (*s2GeodistLineString) IsShape() {}

// LineString implements the geodist.LineString interface.
func (*s2GeodistLineString) IsLineString() {}

// Edge implements the geodist.LineString interface.
func (g *s2GeodistLineString) Edge(i int) geodist.Edge {
	return geodist.Edge{V0: &s2GeodistPoint{Point: (*g.Polyline)[i]}, V1: &s2GeodistPoint{Point: (*g.Polyline)[i+1]}}
}

// NumEdges implements the geodist.LineString interface.
func (g *s2GeodistLineString) NumEdges() int {
	return len(*g.Polyline) - 1
}

// Vertex implements the geodist.LineString interface.
func (g *s2GeodistLineString) Vertex(i int) geodist.Point {
	return &s2GeodistPoint{Point: (*g.Polyline)[i]}
}

// NumVertexes implements the geodist.LineString interface.
func (g *s2GeodistLineString) NumVertexes() int {
	return len(*g.Polyline)
}

// s2GeodistLinearRing implements geodist.LinearRing.
type s2GeodistLinearRing struct {
	*s2.Loop
}

var _ geodist.LinearRing = (*s2GeodistLinearRing)(nil)

// IsShape implements the geodist.LinearRing interface.
func (*s2GeodistLinearRing) IsShape() {}

// LinearRing implements the geodist.LinearRing interface.
func (*s2GeodistLinearRing) IsLinearRing() {}

// Edge implements the geodist.LinearRing interface.
func (g *s2GeodistLinearRing) Edge(i int) geodist.Edge {
	return geodist.Edge{V0: &s2GeodistPoint{Point: g.Loop.Vertex(i)}, V1: &s2GeodistPoint{Point: g.Loop.Vertex(i + 1)}}
}

// NumEdges implements the geodist.LinearRing interface.
func (g *s2GeodistLinearRing) NumEdges() int {
	return g.Loop.NumEdges()
}

// Vertex implements the geodist.LinearRing interface.
func (g *s2GeodistLinearRing) Vertex(i int) geodist.Point {
	return &s2GeodistPoint{Point: g.Loop.Vertex(i)}
}

// NumVertexes implements the geodist.LinearRing interface.
func (g *s2GeodistLinearRing) NumVertexes() int {
	return g.Loop.NumVertices()
}

// s2GeodistPolygon implements geodist.Polygon.
type s2GeodistPolygon struct {
	*s2.Polygon
}

var _ geodist.Polygon = (*s2GeodistPolygon)(nil)

// IsShape implements the geodist.Polygon interface.
func (*s2GeodistPolygon) IsShape() {}

// Polygon implements the geodist.Polygon interface.
func (*s2GeodistPolygon) IsPolygon() {}

// LinearRing implements the geodist.Polygon interface.
func (g *s2GeodistPolygon) LinearRing(i int) geodist.LinearRing {
	return &s2GeodistLinearRing{Loop: g.Polygon.Loop(i)}
}

// NumLinearRings implements the geodist.Polygon interface.
func (g *s2GeodistPolygon) NumLinearRings() int {
	return g.Polygon.NumLoops()
}

// s2GeodistEdgeCrosser implements geodist.EdgeCrosser.
type s2GeodistEdgeCrosser struct {
	*s2.EdgeCrosser
}

var _ geodist.EdgeCrosser = (*s2GeodistEdgeCrosser)(nil)

// ChainCrossing implements geodist.EdgeCrosser.
func (c *s2GeodistEdgeCrosser) ChainCrossing(p geodist.Point) bool {
	return c.EdgeCrosser.ChainCrossingSign(p.(*s2GeodistPoint).Point) != s2.DoNotCross
}

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
		aGeodist, err := regionToGeodistShape(aRegion)
		if err != nil {
			return 0, err
		}
		for _, bRegion := range bRegions {
			minDistanceUpdater := newSpheroidMinDistanceUpdater(spheroid, stopAfterLE)
			bGeodist, err := regionToGeodistShape(bRegion)
			if err != nil {
				return 0, err
			}
			earlyExit, err := geodist.ShapeDistance(
				&spheroidDistanceCalculator{updater: minDistanceUpdater},
				aGeodist,
				bGeodist,
			)
			if err != nil {
				return 0, err
			}
			minDistance = math.Min(minDistance, minDistanceUpdater.Distance())
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

var _ geodist.DistanceUpdater = (*spheroidMinDistanceUpdater)(nil)

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

// Distance implements the DistanceUpdater interface.
func (u *spheroidMinDistanceUpdater) Distance() float64 {
	// If the distance is zero, avoid the call to spheroidDistance and return early.
	if u.minD == 0 {
		return 0
	}
	return spheroidDistance(u.spheroid, u.minEdge.V0, u.minEdge.V1)
}

// Update implements the geodist.DistanceUpdater interface.
func (u *spheroidMinDistanceUpdater) Update(
	aInterface geodist.Point, bInterface geodist.Point,
) bool {
	a := aInterface.(*s2GeodistPoint).Point
	b := bInterface.(*s2GeodistPoint).Point

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

// OnIntersects implements the geodist.DistanceUpdater interface.
func (u *spheroidMinDistanceUpdater) OnIntersects() bool {
	u.minD = 0
	return true
}

// IsMaxDistance implements the geodist.DistanceUpdater interface.
func (u *spheroidMinDistanceUpdater) IsMaxDistance() bool {
	return false
}

// spheroidDistanceCalculator implements geodist.DistanceCalculator
type spheroidDistanceCalculator struct {
	updater *spheroidMinDistanceUpdater
}

var _ geodist.DistanceCalculator = (*spheroidDistanceCalculator)(nil)

// DistanceUpdater implements geodist.DistanceCalculator.
func (c *spheroidDistanceCalculator) DistanceUpdater() geodist.DistanceUpdater {
	return c.updater
}

// NewEdgeCrosser implements geodist.DistanceCalculator.
func (c *spheroidDistanceCalculator) NewEdgeCrosser(
	edge geodist.Edge, startPoint geodist.Point,
) geodist.EdgeCrosser {
	return &s2GeodistEdgeCrosser{
		EdgeCrosser: s2.NewChainEdgeCrosser(
			edge.V0.(*s2GeodistPoint).Point,
			edge.V1.(*s2GeodistPoint).Point,
			startPoint.(*s2GeodistPoint).Point,
		),
	}
}

// PointInLinearRing implements geodist.DistanceCalculator.
func (c *spheroidDistanceCalculator) PointInLinearRing(
	point geodist.Point, polygon geodist.LinearRing,
) bool {
	return polygon.(*s2GeodistLinearRing).ContainsPoint(point.(*s2GeodistPoint).Point)
}

// ClosestPointToEdge implements geodist.DistanceCalculator.
//
// ClosestPointToEdge projects the point onto the infinite line represented
// by the edge. This will return the point on the line closest to the edge.
// It will return the closest point on the line, as well as a bool representing
// whether the point that is projected lies directly on the edge as a segment.
//
// For visualization and more, see: Section 6 / Figure 4 of
// "Projective configuration theorems: old wine into new wineskins", Tabachnikov, Serge, 2016/07/16
func (c *spheroidDistanceCalculator) ClosestPointToEdge(
	edge geodist.Edge, pointInterface geodist.Point,
) (geodist.Point, bool) {
	eV0 := edge.V0.(*s2GeodistPoint).Point
	eV1 := edge.V1.(*s2GeodistPoint).Point
	point := pointInterface.(*s2GeodistPoint).Point

	// Project the point onto the normal of the edge. A great circle passing through
	// the normal and the point will intersect with the great circle represented
	// by the given edge.
	normal := eV0.Vector.Cross(eV1.Vector).Normalize()
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
	return &s2GeodistPoint{Point: closestPoint}, (&s2.Polyline{eV0, eV1}).IntersectsCell(s2.CellFromPoint(closestPoint))
}

// regionToGeodistShape converts the s2 Region to a geodist object.
func regionToGeodistShape(r s2.Region) (geodist.Shape, error) {
	switch r := r.(type) {
	case s2.Point:
		return &s2GeodistPoint{Point: r}, nil
	case *s2.Polyline:
		return &s2GeodistLineString{Polyline: r}, nil
	case *s2.Polygon:
		return &s2GeodistPolygon{Polygon: r}, nil
	}
	return nil, errors.Newf("unknown region: %T", r)
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
