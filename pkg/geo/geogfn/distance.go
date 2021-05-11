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

// SpheroidErrorFraction is an error fraction to compensate for using a sphere
// to calculate the distance for what is actually a spheroid. The distance
// calculation has an error that is bounded by (2 * spheroid.Flattening)%.
// This 5% margin is pretty safe.
const SpheroidErrorFraction = 0.05

// Distance returns the distance between geographies a and b on a sphere or spheroid.
// Returns a geo.EmptyGeometryError if any of the Geographies are EMPTY.
func Distance(
	a geo.Geography, b geo.Geography, useSphereOrSpheroid UseSphereOrSpheroid,
) (float64, error) {
	if a.SRID() != b.SRID() {
		return 0, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}

	aRegions, err := a.AsS2(geo.EmptyBehaviorError)
	if err != nil {
		return 0, err
	}
	bRegions, err := b.AsS2(geo.EmptyBehaviorError)
	if err != nil {
		return 0, err
	}
	spheroid, err := a.Spheroid()
	if err != nil {
		return 0, err
	}
	return distanceGeographyRegions(
		spheroid,
		useSphereOrSpheroid,
		aRegions,
		bRegions,
		a.BoundingRect().Intersects(b.BoundingRect()),
		0, /* stopAfter */
		geo.FnInclusive,
	)
}

//
// Spheroids
//

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
	return geodist.Edge{
		V0: geodist.Point{GeogPoint: (*g.Polyline)[i]},
		V1: geodist.Point{GeogPoint: (*g.Polyline)[i+1]},
	}
}

// NumEdges implements the geodist.LineString interface.
func (g *s2GeodistLineString) NumEdges() int {
	return len(*g.Polyline) - 1
}

// Vertex implements the geodist.LineString interface.
func (g *s2GeodistLineString) Vertex(i int) geodist.Point {
	return geodist.Point{
		GeogPoint: (*g.Polyline)[i],
	}
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
	return geodist.Edge{
		V0: geodist.Point{GeogPoint: g.Loop.Vertex(i)},
		V1: geodist.Point{GeogPoint: g.Loop.Vertex(i + 1)},
	}
}

// NumEdges implements the geodist.LinearRing interface.
func (g *s2GeodistLinearRing) NumEdges() int {
	return g.Loop.NumEdges()
}

// Vertex implements the geodist.LinearRing interface.
func (g *s2GeodistLinearRing) Vertex(i int) geodist.Point {
	return geodist.Point{
		GeogPoint: g.Loop.Vertex(i),
	}
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
func (c *s2GeodistEdgeCrosser) ChainCrossing(p geodist.Point) (bool, geodist.Point) {
	// Returns nil for the intersection point as we don't require the intersection
	// point as we do not have to implement ShortestLine in geography.
	return c.EdgeCrosser.ChainCrossingSign(p.GeogPoint) != s2.DoNotCross, geodist.Point{}
}

// distanceGeographyRegions calculates the distance between two sets of regions.
// If inclusive, it will quit if it finds a distance that is less than or equal
// to stopAfter. Otherwise, it will quit if a distance less than stopAfter is
// found. It is not guaranteed to find the absolute minimum distance if
// stopAfter > 0.
//
// !!! SURPRISING BEHAVIOR WARNING FOR SPHEROIDS !!!
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
func distanceGeographyRegions(
	spheroid *geographiclib.Spheroid,
	useSphereOrSpheroid UseSphereOrSpheroid,
	aRegions []s2.Region,
	bRegions []s2.Region,
	boundingBoxIntersects bool,
	stopAfter float64,
	exclusivity geo.FnExclusivity,
) (float64, error) {
	minDistance := math.MaxFloat64
	for _, aRegion := range aRegions {
		aGeodist, err := regionToGeodistShape(aRegion)
		if err != nil {
			return 0, err
		}
		for _, bRegion := range bRegions {
			minDistanceUpdater := newGeographyMinDistanceUpdater(
				spheroid,
				useSphereOrSpheroid,
				stopAfter,
				exclusivity,
			)
			bGeodist, err := regionToGeodistShape(bRegion)
			if err != nil {
				return 0, err
			}
			earlyExit, err := geodist.ShapeDistance(
				&geographyDistanceCalculator{
					updater:               minDistanceUpdater,
					boundingBoxIntersects: boundingBoxIntersects,
				},
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

// geographyMinDistanceUpdater finds the minimum distance using a sphere.
// Methods will return early if it finds a minimum distance <= stopAfterLE.
type geographyMinDistanceUpdater struct {
	spheroid            *geographiclib.Spheroid
	useSphereOrSpheroid UseSphereOrSpheroid
	minEdge             s2.Edge
	minD                s1.ChordAngle
	stopAfter           s1.ChordAngle
	exclusivity         geo.FnExclusivity
}

var _ geodist.DistanceUpdater = (*geographyMinDistanceUpdater)(nil)

// newGeographyMinDistanceUpdater returns a new geographyMinDistanceUpdater with the
// correct arguments set up.
func newGeographyMinDistanceUpdater(
	spheroid *geographiclib.Spheroid,
	useSphereOrSpheroid UseSphereOrSpheroid,
	stopAfter float64,
	exclusivity geo.FnExclusivity,
) *geographyMinDistanceUpdater {
	multiplier := 1.0
	if useSphereOrSpheroid == UseSpheroid {
		// Modify the stopAfterLE distance to be less by the error fraction, since
		// we use the sphere to calculate the distance and we want to leave a
		// buffer for spheroid distances being slightly off.
		multiplier -= SpheroidErrorFraction
	}
	stopAfterChordAngle := s1.ChordAngleFromAngle(s1.Angle(stopAfter * multiplier / spheroid.SphereRadius))
	return &geographyMinDistanceUpdater{
		spheroid:            spheroid,
		minD:                math.MaxFloat64,
		useSphereOrSpheroid: useSphereOrSpheroid,
		stopAfter:           stopAfterChordAngle,
		exclusivity:         exclusivity,
	}
}

// Distance implements the DistanceUpdater interface.
func (u *geographyMinDistanceUpdater) Distance() float64 {
	// If the distance is zero, avoid the call to spheroidDistance and return early.
	if u.minD == 0 {
		return 0
	}
	if u.useSphereOrSpheroid == UseSpheroid {
		return spheroidDistance(u.spheroid, u.minEdge.V0, u.minEdge.V1)
	}
	return u.minD.Angle().Radians() * u.spheroid.SphereRadius
}

// Update implements the geodist.DistanceUpdater interface.
func (u *geographyMinDistanceUpdater) Update(aPoint geodist.Point, bPoint geodist.Point) bool {
	a := aPoint.GeogPoint
	b := bPoint.GeogPoint

	sphereDistance := s2.ChordAngleBetweenPoints(a, b)
	if sphereDistance < u.minD {
		u.minD = sphereDistance
		u.minEdge = s2.Edge{V0: a, V1: b}
		// If we have a threshold, determine if we can stop early.
		// If the sphere distance is within range of the stopAfter, we can
		// definitively say we've reach the close enough point.
		if (u.exclusivity == geo.FnInclusive && u.minD <= u.stopAfter) ||
			(u.exclusivity == geo.FnExclusive && u.minD < u.stopAfter) {
			return true
		}
	}
	return false
}

// OnIntersects implements the geodist.DistanceUpdater interface.
func (u *geographyMinDistanceUpdater) OnIntersects(p geodist.Point) bool {
	u.minD = 0
	return true
}

// IsMaxDistance implements the geodist.DistanceUpdater interface.
func (u *geographyMinDistanceUpdater) IsMaxDistance() bool {
	return false
}

// FlipGeometries implements the geodist.DistanceUpdater interface.
func (u *geographyMinDistanceUpdater) FlipGeometries() {
	// FlipGeometries is unimplemented for geographyMinDistanceUpdater as we don't
	// require the order of geometries for calculation of minimum distance.
}

// geographyDistanceCalculator implements geodist.DistanceCalculator
type geographyDistanceCalculator struct {
	updater               *geographyMinDistanceUpdater
	boundingBoxIntersects bool
}

var _ geodist.DistanceCalculator = (*geographyDistanceCalculator)(nil)

// DistanceUpdater implements geodist.DistanceCalculator.
func (c *geographyDistanceCalculator) DistanceUpdater() geodist.DistanceUpdater {
	return c.updater
}

// BoundingBoxIntersects implements geodist.DistanceCalculator.
func (c *geographyDistanceCalculator) BoundingBoxIntersects() bool {
	return c.boundingBoxIntersects
}

// NewEdgeCrosser implements geodist.DistanceCalculator.
func (c *geographyDistanceCalculator) NewEdgeCrosser(
	edge geodist.Edge, startPoint geodist.Point,
) geodist.EdgeCrosser {
	return &s2GeodistEdgeCrosser{
		EdgeCrosser: s2.NewChainEdgeCrosser(
			edge.V0.GeogPoint,
			edge.V1.GeogPoint,
			startPoint.GeogPoint,
		),
	}
}

// PointIntersectsLinearRing implements geodist.DistanceCalculator.
func (c *geographyDistanceCalculator) PointIntersectsLinearRing(
	point geodist.Point, polygon geodist.LinearRing,
) bool {
	return polygon.(*s2GeodistLinearRing).ContainsPoint(point.GeogPoint)
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
func (c *geographyDistanceCalculator) ClosestPointToEdge(
	edge geodist.Edge, point geodist.Point,
) (geodist.Point, bool) {
	eV0 := edge.V0.GeogPoint
	eV1 := edge.V1.GeogPoint

	// Project the point onto the normal of the edge. A great circle passing through
	// the normal and the point will intersect with the great circle represented
	// by the given edge.
	normal := eV0.Vector.Cross(eV1.Vector).Normalize()
	// To find the point where the great circle represented by the edge and the
	// great circle represented by (normal, point), we project the point
	// onto the normal.
	normalScaledToPoint := normal.Mul(normal.Dot(point.GeogPoint.Vector))
	// The difference between the point and the projection of the normal when normalized
	// should give us a point on the great circle which contains the vertexes of the edge.
	closestPoint := s2.Point{Vector: point.GeogPoint.Vector.Sub(normalScaledToPoint).Normalize()}
	// We then check whether the given point lies on the geodesic of the edge,
	// as the above algorithm only generates a point on the great circle
	// represented by the edge.
	return geodist.Point{GeogPoint: closestPoint}, (&s2.Polyline{eV0, eV1}).IntersectsCell(s2.CellFromPoint(closestPoint))
}

// regionToGeodistShape converts the s2 Region to a geodist object.
func regionToGeodistShape(r s2.Region) (geodist.Shape, error) {
	switch r := r.(type) {
	case s2.Point:
		return &geodist.Point{GeogPoint: r}, nil
	case *s2.Polyline:
		return &s2GeodistLineString{Polyline: r}, nil
	case *s2.Polygon:
		return &s2GeodistPolygon{Polygon: r}, nil
	}
	return nil, errors.Newf("unknown region: %T", r)
}
