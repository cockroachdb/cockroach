// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geodist"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/lineintersector"
)

// geometricalObjectsOrder allows us to preserve the order of geometrical objects to
// match the start and endpoint in the shortest and longest LineString.
type geometricalObjectsOrder int

const (
	// geometricalObjectsFlipped represents that the given order of two geometrical
	// objects has been flipped.
	geometricalObjectsFlipped geometricalObjectsOrder = -1
	// geometricalObjectsNotFlipped represents that the given order of two geometrical
	// objects has not been flipped.
	geometricalObjectsNotFlipped geometricalObjectsOrder = 1
)

// MinDistance returns the minimum distance between geometries A and B.
// This returns a geo.EmptyGeometryError if either A or B is EMPTY.
func MinDistance(a geo.Geometry, b geo.Geometry) (float64, error) {
	if a.SRID() != b.SRID() {
		return 0, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	return minDistanceInternal(a, b, 0, geo.EmptyBehaviorOmit, geo.FnInclusive)
}

// MaxDistance returns the maximum distance across every pair of points comprising
// geometries A and B.
func MaxDistance(a geo.Geometry, b geo.Geometry) (float64, error) {
	if a.SRID() != b.SRID() {
		return 0, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	return maxDistanceInternal(a, b, math.MaxFloat64, geo.EmptyBehaviorOmit, geo.FnInclusive)
}

// DWithin determines if any part of geometry A is within D units of geometry B.
// If exclusive, DWithin is equivalent to Distance(a, b) < d. Otherwise, DWithin
// is equivalent to Distance(a, b) <= d.
func DWithin(
	a geo.Geometry, b geo.Geometry, d float64, exclusivity geo.FnExclusivity,
) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if d < 0 {
		return false, pgerror.Newf(pgcode.InvalidParameterValue, "dwithin distance cannot be less than zero")
	}
	if !a.CartesianBoundingBox().Buffer(d, d).Intersects(b.CartesianBoundingBox()) {
		return false, nil
	}
	dist, err := minDistanceInternal(a, b, d, geo.EmptyBehaviorError, exclusivity)
	if err != nil {
		// In case of any empty geometries return false.
		if geo.IsEmptyGeometryError(err) {
			return false, nil
		}
		return false, err
	}
	if exclusivity == geo.FnExclusive {
		return dist < d, nil
	}
	return dist <= d, nil
}

// DFullyWithin determines whether the maximum distance across every pair of
// points comprising geometries A and B is within D units. If exclusive,
// DFullyWithin is equivalent to MaxDistance(a, b) < d. Otherwise, DFullyWithin
// is equivalent to MaxDistance(a, b) <= d.
func DFullyWithin(
	a geo.Geometry, b geo.Geometry, d float64, exclusivity geo.FnExclusivity,
) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if d < 0 {
		return false, pgerror.Newf(pgcode.InvalidParameterValue, "dwithin distance cannot be less than zero")
	}
	if !a.CartesianBoundingBox().Buffer(d, d).Covers(b.CartesianBoundingBox()) {
		return false, nil
	}
	dist, err := maxDistanceInternal(a, b, d, geo.EmptyBehaviorError, exclusivity)
	if err != nil {
		// In case of any empty geometries return false.
		if geo.IsEmptyGeometryError(err) {
			return false, nil
		}
		return false, err
	}
	if exclusivity == geo.FnExclusive {
		return dist < d, nil
	}
	return dist <= d, nil
}

// LongestLineString returns the LineString corresponds to maximum distance across
// every pair of points comprising geometries A and B.
func LongestLineString(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	u := newGeomMaxDistanceUpdater(math.MaxFloat64, geo.FnInclusive)
	return distanceLineStringInternal(a, b, u, geo.EmptyBehaviorOmit)
}

// ShortestLineString returns the LineString corresponds to minimum distance across
// every pair of points comprising geometries A and B.
func ShortestLineString(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	u := newGeomMinDistanceUpdater(0 /*stopAfter */, geo.FnInclusive)
	return distanceLineStringInternal(a, b, u, geo.EmptyBehaviorOmit)
}

// distanceLineStringInternal calculates the LineString between two geometries using
// the DistanceCalculator operator.
// If there are any EMPTY Geometry objects, they will be ignored. It will return an
// EmptyGeometryError if A or B contains only EMPTY geometries, even if emptyBehavior
// is set to EmptyBehaviorOmit.
func distanceLineStringInternal(
	a geo.Geometry, b geo.Geometry, u geodist.DistanceUpdater, emptyBehavior geo.EmptyBehavior,
) (geo.Geometry, error) {
	c := &geomDistanceCalculator{updater: u, boundingBoxIntersects: a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox())}
	_, err := distanceInternal(a, b, c, emptyBehavior)
	if err != nil {
		return geo.Geometry{}, err
	}
	var coordA, coordB geom.Coord
	switch u := u.(type) {
	case *geomMaxDistanceUpdater:
		coordA = u.coordA
		coordB = u.coordB
	case *geomMinDistanceUpdater:
		coordA = u.coordA
		coordB = u.coordB
	default:
		return geo.Geometry{}, errors.AssertionFailedf("programmer error: unknown behavior")
	}
	lineCoords := []float64{coordA.X(), coordA.Y(), coordB.X(), coordB.Y()}
	lineString := geom.NewLineStringFlat(geom.XY, lineCoords).SetSRID(int(a.SRID()))
	return geo.MakeGeometryFromGeomT(lineString)
}

// maxDistanceInternal finds the maximum distance between two geometries.
// We can re-use the same algorithm as min-distance, allowing skips of checks that involve
// the interiors or intersections as those will always be less then the maximum min-distance.
func maxDistanceInternal(
	a geo.Geometry,
	b geo.Geometry,
	stopAfter float64,
	emptyBehavior geo.EmptyBehavior,
	exclusivity geo.FnExclusivity,
) (float64, error) {
	u := newGeomMaxDistanceUpdater(stopAfter, exclusivity)
	c := &geomDistanceCalculator{updater: u, boundingBoxIntersects: a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox())}
	return distanceInternal(a, b, c, emptyBehavior)
}

// minDistanceInternal finds the minimum distance between two geometries.
// This implementation is done in-house, as compared to using GEOS.
func minDistanceInternal(
	a geo.Geometry,
	b geo.Geometry,
	stopAfter float64,
	emptyBehavior geo.EmptyBehavior,
	exclusivity geo.FnExclusivity,
) (float64, error) {
	u := newGeomMinDistanceUpdater(stopAfter, exclusivity)
	c := &geomDistanceCalculator{updater: u, boundingBoxIntersects: a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox())}
	return distanceInternal(a, b, c, emptyBehavior)
}

// distanceInternal calculates the distance between two geometries using
// the DistanceCalculator operator.
// If there are any EMPTY Geometry objects, they will be ignored. It will return an
// EmptyGeometryError if A or B contains only EMPTY geometries, even if emptyBehavior
// is set to EmptyBehaviorOmit.
func distanceInternal(
	a geo.Geometry, b geo.Geometry, c geodist.DistanceCalculator, emptyBehavior geo.EmptyBehavior,
) (float64, error) {
	// If either side has no geoms, then we error out regardless of emptyBehavior.
	if a.Empty() || b.Empty() {
		return 0, geo.NewEmptyGeometryError()
	}

	aGeomT, err := a.AsGeomT()
	if err != nil {
		return 0, err
	}
	bGeomT, err := b.AsGeomT()
	if err != nil {
		return 0, err
	}

	// If we early exit, we have to check empty behavior upfront to return
	// the appropriate error message.
	// This matches PostGIS's behavior for DWithin, which is always false
	// if at least one element is empty.
	if emptyBehavior == geo.EmptyBehaviorError &&
		(geo.GeomTContainsEmpty(aGeomT) || geo.GeomTContainsEmpty(bGeomT)) {
		return 0, geo.NewEmptyGeometryError()
	}

	aIt := geo.NewGeomTIterator(aGeomT, emptyBehavior)
	aGeom, aNext, aErr := aIt.Next()
	if aErr != nil {
		return 0, aErr
	}
	for aNext {
		aGeodist, err := geomToGeodist(aGeom)
		if err != nil {
			return 0, err
		}

		bIt := geo.NewGeomTIterator(bGeomT, emptyBehavior)
		bGeom, bNext, bErr := bIt.Next()
		if bErr != nil {
			return 0, bErr
		}
		for bNext {
			bGeodist, err := geomToGeodist(bGeom)
			if err != nil {
				return 0, err
			}
			earlyExit, err := geodist.ShapeDistance(c, aGeodist, bGeodist)
			if err != nil {
				return 0, err
			}
			if earlyExit {
				return c.DistanceUpdater().Distance(), nil
			}

			bGeom, bNext, bErr = bIt.Next()
			if bErr != nil {
				return 0, bErr
			}
		}

		aGeom, aNext, aErr = aIt.Next()
		if aErr != nil {
			return 0, aErr
		}
	}
	return c.DistanceUpdater().Distance(), nil
}

// geomToGeodist converts a given geom object to a geodist shape.
func geomToGeodist(g geom.T) (geodist.Shape, error) {
	switch g := g.(type) {
	case *geom.Point:
		return &geodist.Point{GeomPoint: g.Coords()}, nil
	case *geom.LineString:
		return &geomGeodistLineString{LineString: g}, nil
	case *geom.Polygon:
		return &geomGeodistPolygon{Polygon: g}, nil
	}
	return nil, pgerror.Newf(pgcode.InvalidParameterValue, "could not find shape: %T", g)
}

// geomGeodistLineString implements geodist.LineString.
type geomGeodistLineString struct {
	*geom.LineString
}

var _ geodist.LineString = (*geomGeodistLineString)(nil)

// IsShape implements the geodist.LineString interface.
func (*geomGeodistLineString) IsShape() {}

// LineString implements the geodist.LineString interface.
func (*geomGeodistLineString) IsLineString() {}

// Edge implements the geodist.LineString interface.
func (g *geomGeodistLineString) Edge(i int) geodist.Edge {
	return geodist.Edge{
		V0: geodist.Point{GeomPoint: g.LineString.Coord(i)},
		V1: geodist.Point{GeomPoint: g.LineString.Coord(i + 1)},
	}
}

// NumEdges implements the geodist.LineString interface.
func (g *geomGeodistLineString) NumEdges() int {
	return g.LineString.NumCoords() - 1
}

// Vertex implements the geodist.LineString interface.
func (g *geomGeodistLineString) Vertex(i int) geodist.Point {
	return geodist.Point{GeomPoint: g.LineString.Coord(i)}
}

// NumVertexes implements the geodist.LineString interface.
func (g *geomGeodistLineString) NumVertexes() int {
	return g.LineString.NumCoords()
}

// geomGeodistLinearRing implements geodist.LinearRing.
type geomGeodistLinearRing struct {
	*geom.LinearRing
}

var _ geodist.LinearRing = (*geomGeodistLinearRing)(nil)

// IsShape implements the geodist.LinearRing interface.
func (*geomGeodistLinearRing) IsShape() {}

// LinearRing implements the geodist.LinearRing interface.
func (*geomGeodistLinearRing) IsLinearRing() {}

// Edge implements the geodist.LinearRing interface.
func (g *geomGeodistLinearRing) Edge(i int) geodist.Edge {
	return geodist.Edge{
		V0: geodist.Point{GeomPoint: g.LinearRing.Coord(i)},
		V1: geodist.Point{GeomPoint: g.LinearRing.Coord(i + 1)},
	}
}

// NumEdges implements the geodist.LinearRing interface.
func (g *geomGeodistLinearRing) NumEdges() int {
	return g.LinearRing.NumCoords() - 1
}

// Vertex implements the geodist.LinearRing interface.
func (g *geomGeodistLinearRing) Vertex(i int) geodist.Point {
	return geodist.Point{GeomPoint: g.LinearRing.Coord(i)}
}

// NumVertexes implements the geodist.LinearRing interface.
func (g *geomGeodistLinearRing) NumVertexes() int {
	return g.LinearRing.NumCoords()
}

// geomGeodistPolygon implements geodist.Polygon.
type geomGeodistPolygon struct {
	*geom.Polygon
}

var _ geodist.Polygon = (*geomGeodistPolygon)(nil)

// IsShape implements the geodist.Polygon interface.
func (*geomGeodistPolygon) IsShape() {}

// Polygon implements the geodist.Polygon interface.
func (*geomGeodistPolygon) IsPolygon() {}

// LinearRing implements the geodist.Polygon interface.
func (g *geomGeodistPolygon) LinearRing(i int) geodist.LinearRing {
	return &geomGeodistLinearRing{LinearRing: g.Polygon.LinearRing(i)}
}

// NumLinearRings implements the geodist.Polygon interface.
func (g *geomGeodistPolygon) NumLinearRings() int {
	return g.Polygon.NumLinearRings()
}

// geomGeodistEdgeCrosser implements geodist.EdgeCrosser.
type geomGeodistEdgeCrosser struct {
	strategy   lineintersector.Strategy
	edgeV0     geom.Coord
	edgeV1     geom.Coord
	nextEdgeV0 geom.Coord
}

var _ geodist.EdgeCrosser = (*geomGeodistEdgeCrosser)(nil)

// ChainCrossing implements geodist.EdgeCrosser.
func (c *geomGeodistEdgeCrosser) ChainCrossing(p geodist.Point) (bool, geodist.Point) {
	nextEdgeV1 := p.GeomPoint
	result := lineintersector.LineIntersectsLine(
		c.strategy,
		c.edgeV0,
		c.edgeV1,
		c.nextEdgeV0,
		nextEdgeV1,
	)
	c.nextEdgeV0 = nextEdgeV1
	if result.HasIntersection() {
		return true, geodist.Point{GeomPoint: result.Intersection()[0]}
	}
	return false, geodist.Point{}
}

// geomMinDistanceUpdater finds the minimum distance using geom calculations.
// And preserve the line's endpoints as geom.Coord which corresponds to minimum
// distance. If inclusive, methods will return early if it finds a minimum
// distance <= stopAfter. Otherwise, methods will return early if it finds a
// minimum distance < stopAfter.
type geomMinDistanceUpdater struct {
	currentValue float64
	stopAfter    float64
	exclusivity  geo.FnExclusivity
	// coordA represents the first vertex of the edge that holds the maximum distance.
	coordA geom.Coord
	// coordB represents the second vertex of the edge that holds the maximum distance.
	coordB geom.Coord

	geometricalObjOrder geometricalObjectsOrder
}

var _ geodist.DistanceUpdater = (*geomMinDistanceUpdater)(nil)

// newGeomMinDistanceUpdater returns a new geomMinDistanceUpdater with the
// correct arguments set up.
func newGeomMinDistanceUpdater(
	stopAfter float64, exclusivity geo.FnExclusivity,
) *geomMinDistanceUpdater {
	return &geomMinDistanceUpdater{
		currentValue:        math.MaxFloat64,
		stopAfter:           stopAfter,
		exclusivity:         exclusivity,
		coordA:              nil,
		coordB:              nil,
		geometricalObjOrder: geometricalObjectsNotFlipped,
	}
}

// Distance implements the geodist.DistanceUpdater interface.
func (u *geomMinDistanceUpdater) Distance() float64 {
	return u.currentValue
}

// Update implements the geodist.DistanceUpdater interface.
func (u *geomMinDistanceUpdater) Update(aPoint geodist.Point, bPoint geodist.Point) bool {
	a := aPoint.GeomPoint
	b := bPoint.GeomPoint

	dist := coordNorm(coordSub(a, b))
	if dist < u.currentValue || u.coordA == nil {
		u.currentValue = dist
		if u.geometricalObjOrder == geometricalObjectsFlipped {
			u.coordA = b
			u.coordB = a
		} else {
			u.coordA = a
			u.coordB = b
		}
		if u.exclusivity == geo.FnExclusive {
			return dist < u.stopAfter
		}
		return dist <= u.stopAfter
	}
	return false
}

// OnIntersects implements the geodist.DistanceUpdater interface.
func (u *geomMinDistanceUpdater) OnIntersects(p geodist.Point) bool {
	u.coordA = p.GeomPoint
	u.coordB = p.GeomPoint
	u.currentValue = 0
	return true
}

// IsMaxDistance implements the geodist.DistanceUpdater interface.
func (u *geomMinDistanceUpdater) IsMaxDistance() bool {
	return false
}

// FlipGeometries implements the geodist.DistanceUpdater interface.
func (u *geomMinDistanceUpdater) FlipGeometries() {
	u.geometricalObjOrder = -u.geometricalObjOrder
}

// geomMaxDistanceUpdater finds the maximum distance using geom calculations.
// And preserve the line's endpoints as geom.Coord which corresponds to maximum
// distance. If exclusive, methods will return early if it finds that
// distance >= stopAfter. Otherwise, methods will return early if distance >
// stopAfter.
type geomMaxDistanceUpdater struct {
	currentValue float64
	stopAfter    float64
	exclusivity  geo.FnExclusivity

	// coordA represents the first vertex of the edge that holds the maximum distance.
	coordA geom.Coord
	// coordB represents the second vertex of the edge that holds the maximum distance.
	coordB geom.Coord

	geometricalObjOrder geometricalObjectsOrder
}

var _ geodist.DistanceUpdater = (*geomMaxDistanceUpdater)(nil)

// newGeomMaxDistanceUpdater returns a new geomMaxDistanceUpdater with the
// correct arguments set up. currentValue is initially populated with least
// possible value instead of 0 because there may be the case where maximum
// distance is 0 and we may require to find the line for 0 maximum distance.
func newGeomMaxDistanceUpdater(
	stopAfter float64, exclusivity geo.FnExclusivity,
) *geomMaxDistanceUpdater {
	return &geomMaxDistanceUpdater{
		currentValue:        -math.MaxFloat64,
		stopAfter:           stopAfter,
		exclusivity:         exclusivity,
		coordA:              nil,
		coordB:              nil,
		geometricalObjOrder: geometricalObjectsNotFlipped,
	}
}

// Distance implements the geodist.DistanceUpdater interface.
func (u *geomMaxDistanceUpdater) Distance() float64 {
	return u.currentValue
}

// Update implements the geodist.DistanceUpdater interface.
func (u *geomMaxDistanceUpdater) Update(aPoint geodist.Point, bPoint geodist.Point) bool {
	a := aPoint.GeomPoint
	b := bPoint.GeomPoint

	dist := coordNorm(coordSub(a, b))
	if dist > u.currentValue || u.coordA == nil {
		u.currentValue = dist
		if u.geometricalObjOrder == geometricalObjectsFlipped {
			u.coordA = b
			u.coordB = a
		} else {
			u.coordA = a
			u.coordB = b
		}
		if u.exclusivity == geo.FnExclusive {
			return dist >= u.stopAfter
		}
		return dist > u.stopAfter
	}
	return false
}

// OnIntersects implements the geodist.DistanceUpdater interface.
func (u *geomMaxDistanceUpdater) OnIntersects(p geodist.Point) bool {
	return false
}

// IsMaxDistance implements the geodist.DistanceUpdater interface.
func (u *geomMaxDistanceUpdater) IsMaxDistance() bool {
	return true
}

// FlipGeometries implements the geodist.DistanceUpdater interface.
func (u *geomMaxDistanceUpdater) FlipGeometries() {
	u.geometricalObjOrder = -u.geometricalObjOrder
}

// MinDistance3D returns the 3D minimum distance between geometries A and B.
// For 2D geometries, Z is treated as 0, so this degrades to 2D distance.
// This returns a geo.EmptyGeometryError if either A or B is EMPTY.
func MinDistance3D(a geo.Geometry, b geo.Geometry) (float64, error) {
	if a.SRID() != b.SRID() {
		return 0, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	a, b, err := normalizeFor3D(a, b)
	if err != nil {
		return 0, err
	}
	return minDistance3DInternal(a, b, 0, geo.EmptyBehaviorOmit)
}

// DWithin3D determines if any part of geometry A is within D units of
// geometry B using 3D Euclidean distance.
// DWithin3D is equivalent to ST_3DDistance(a, b) <= d.
func DWithin3D(a geo.Geometry, b geo.Geometry, d float64) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if d < 0 {
		return false, pgerror.Newf(
			pgcode.InvalidParameterValue, "dwithin distance cannot be less than zero",
		)
	}
	// Use the 2D bounding box as a conservative fast-reject filter.
	// If 2D bounding boxes don't overlap by d, the 3D distance is at
	// least as large.
	if !a.CartesianBoundingBox().Buffer(d, d).Intersects(b.CartesianBoundingBox()) {
		return false, nil
	}
	a, b, err := normalizeFor3D(a, b)
	if err != nil {
		return false, err
	}
	dist, err := minDistance3DInternal(a, b, d, geo.EmptyBehaviorError)
	if err != nil {
		if geo.IsEmptyGeometryError(err) {
			return false, nil
		}
		return false, err
	}
	return dist <= d, nil
}

// normalizeFor3D drops the M dimension from inputs so that coord[2] is
// always Z (or the coord has no Z at all). This matches PostGIS semantics:
// M is not used in 3D distance calculations, and missing Z is treated as
// "any value" (effectively 2D distance, since both sides degrade together).
func normalizeFor3D(a, b geo.Geometry) (geo.Geometry, geo.Geometry, error) {
	a, err := stripMDimension(a)
	if err != nil {
		return geo.Geometry{}, geo.Geometry{}, err
	}
	b, err = stripMDimension(b)
	if err != nil {
		return geo.Geometry{}, geo.Geometry{}, err
	}
	return a, b, nil
}

// stripMDimension converts an XYM geometry to XY and an XYZM geometry to
// XYZ. Other layouts pass through unchanged.
func stripMDimension(g geo.Geometry) (geo.Geometry, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	switch t.Layout() {
	case geom.XYM:
		return ForceLayout(g, geom.XY)
	case geom.XYZM:
		return ForceLayout(g, geom.XYZ)
	}
	return g, nil
}

// minDistance3DInternal finds the 3D minimum distance between two geometries.
func minDistance3DInternal(
	a geo.Geometry, b geo.Geometry, stopAfter float64, emptyBehavior geo.EmptyBehavior,
) (float64, error) {
	u := newGeomMinDistance3DUpdater(stopAfter)
	c := &geomDistance3DCalculator{
		updater:               u,
		boundingBoxIntersects: a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox()),
	}
	return distanceInternal(a, b, c, emptyBehavior)
}

// geomMinDistance3DUpdater finds the minimum distance using 3D geom
// calculations. Methods return early if the minimum distance found is
// <= stopAfter.
type geomMinDistance3DUpdater struct {
	currentValue float64
	stopAfter    float64
	coordA       geom.Coord
	coordB       geom.Coord

	geometricalObjOrder geometricalObjectsOrder

	// planeDistance is set by the 3D calculator's PointIntersectsLinearRing
	// when a point is inside a polygon's XY projection. It holds the
	// perpendicular distance from the point to the polygon's plane.
	// A negative value means no plane distance is pending.
	planeDistance float64
}

var _ geodist.DistanceUpdater = (*geomMinDistance3DUpdater)(nil)

func newGeomMinDistance3DUpdater(stopAfter float64) *geomMinDistance3DUpdater {
	return &geomMinDistance3DUpdater{
		currentValue:        math.MaxFloat64,
		stopAfter:           stopAfter,
		planeDistance:       -1,
		geometricalObjOrder: geometricalObjectsNotFlipped,
	}
}

// Distance implements the geodist.DistanceUpdater interface.
func (u *geomMinDistance3DUpdater) Distance() float64 {
	return u.currentValue
}

// Update implements the geodist.DistanceUpdater interface.
//
// Returns true (request early exit) whenever the running minimum is at
// or below stopAfter, regardless of whether this call updated it. This
// lets edge-crosser-supplied candidates (which set the minimum from
// inside ChainCrossing without being able to signal early exit through
// the EdgeCrosser API) propagate via the next Update call.
func (u *geomMinDistance3DUpdater) Update(aPoint geodist.Point, bPoint geodist.Point) bool {
	a := aPoint.GeomPoint
	b := bPoint.GeomPoint

	dist := coordNorm3D(coordSub3D(a, b))
	if dist < u.currentValue || u.coordA == nil {
		u.currentValue = dist
		if u.geometricalObjOrder == geometricalObjectsFlipped {
			u.coordA = b
			u.coordB = a
		} else {
			u.coordA = a
			u.coordB = b
		}
	}
	return u.currentValue <= u.stopAfter
}

// OnIntersects implements the geodist.DistanceUpdater interface.
//
// In the 3D case, this is only called from polygon-containment paths in
// geodist (onPointToPolygon, onLineStringToPolygon, onPolygonToPolygon),
// each of which calls PointIntersectsLinearRing on a polygon ring
// immediately before. That call sets planeDistance to the perpendicular
// distance from the point to the polygon's plane. We use that as the 3D
// distance rather than treating containment as zero distance. Edge
// crossings are handled separately by the 3D edge crosser, which never
// triggers OnIntersects.
func (u *geomMinDistance3DUpdater) OnIntersects(p geodist.Point) bool {
	dist := u.planeDistance
	u.planeDistance = -1
	if dist < 0 {
		// Should not happen: a polygon-containment path always sets
		// planeDistance via PointIntersectsLinearRing first. Treat as
		// zero distance so we still produce a sensible answer.
		dist = 0
	}
	if dist < u.currentValue || u.coordA == nil {
		u.currentValue = dist
		u.coordA = p.GeomPoint
		u.coordB = p.GeomPoint
	}
	return u.currentValue <= u.stopAfter
}

// IsMaxDistance implements the geodist.DistanceUpdater interface.
func (u *geomMinDistance3DUpdater) IsMaxDistance() bool {
	return false
}

// FlipGeometries implements the geodist.DistanceUpdater interface.
func (u *geomMinDistance3DUpdater) FlipGeometries() {
	u.geometricalObjOrder = -u.geometricalObjOrder
}

// geomDistance3DCalculator implements geodist.DistanceCalculator using
// 3D coordinate math. Point-in-polygon and edge crossing tests remain
// 2D (XY projection), matching PostGIS behavior for 3D geometries.
type geomDistance3DCalculator struct {
	updater               geodist.DistanceUpdater
	boundingBoxIntersects bool
}

var _ geodist.DistanceCalculator = (*geomDistance3DCalculator)(nil)

// DistanceUpdater implements geodist.DistanceCalculator.
func (c *geomDistance3DCalculator) DistanceUpdater() geodist.DistanceUpdater {
	return c.updater
}

// BoundingBoxIntersects implements geodist.DistanceCalculator.
func (c *geomDistance3DCalculator) BoundingBoxIntersects() bool {
	return c.boundingBoxIntersects
}

// NewEdgeCrosser implements geodist.DistanceCalculator.
//
// Returns a 3D-aware crosser: instead of detecting 2D intersections
// (which are meaningless in 3D — segments may project to crossing lines
// in XY while being far apart in Z), it computes the true 3D
// segment-segment minimum distance and feeds the closest pair to the
// updater. The crosser never reports a crossing, so OnIntersects is
// never called from edge iteration.
func (c *geomDistance3DCalculator) NewEdgeCrosser(
	edge geodist.Edge, startPoint geodist.Point,
) geodist.EdgeCrosser {
	updater, ok := c.updater.(*geomMinDistance3DUpdater)
	if !ok {
		return nil
	}
	return &geom3DEdgeCrosser{
		updater:    updater,
		edgeV0:     edge.V0.GeomPoint,
		edgeV1:     edge.V1.GeomPoint,
		nextEdgeV0: startPoint.GeomPoint,
	}
}

// PointIntersectsLinearRing implements geodist.DistanceCalculator.
// Point-in-polygon containment is a 2D topological operation (XY
// projection). When the point is inside, we pre-compute the
// perpendicular distance from the point to the polygon's plane and
// store it in the updater so OnIntersects can use it instead of 0.
func (c *geomDistance3DCalculator) PointIntersectsLinearRing(
	point geodist.Point, linearRing geodist.LinearRing,
) bool {
	side := findPointSideOfLinearRing(point, linearRing)
	switch side {
	case insideLinearRing, onLinearRing:
		if u, ok := c.updater.(*geomMinDistance3DUpdater); ok {
			u.planeDistance = perpendicularDistanceToRingPlane(
				point.GeomPoint, linearRing,
			)
		}
		return true
	default:
		return false
	}
}

// ClosestPointToEdge implements geodist.DistanceCalculator using 3D
// projection. The formula r = dot(AC, AB) / norm2(AB) generalizes
// naturally to 3D.
func (c *geomDistance3DCalculator) ClosestPointToEdge(
	e geodist.Edge, p geodist.Point,
) (geodist.Point, bool) {
	if coordEqual3D(e.V0.GeomPoint, e.V1.GeomPoint) {
		return e.V0, coordEqual3D(e.V0.GeomPoint, p.GeomPoint)
	}
	if coordEqual3D(p.GeomPoint, e.V0.GeomPoint) {
		return p, true
	}
	if coordEqual3D(p.GeomPoint, e.V1.GeomPoint) {
		return p, true
	}

	ac := coordSub3D(p.GeomPoint, e.V0.GeomPoint)
	ab := coordSub3D(e.V1.GeomPoint, e.V0.GeomPoint)
	r := coordDot3D(ac, ab) / coordNorm2_3D(ab)
	if r < 0 || r > 1 {
		return p, false
	}
	return geodist.Point{
		GeomPoint: coordAdd3D(e.V0.GeomPoint, coordMul3D(ab, r)),
	}, true
}

// geom3DEdgeCrosser computes the true 3D segment-segment minimum
// distance for each edge pair and feeds the closest pair to the
// updater. ChainCrossing always returns (false, _) so the caller never
// invokes OnIntersects from edge iteration; the global minimum is
// determined by the updater across all candidates.
type geom3DEdgeCrosser struct {
	updater    *geomMinDistance3DUpdater
	edgeV0     geom.Coord
	edgeV1     geom.Coord
	nextEdgeV0 geom.Coord
}

var _ geodist.EdgeCrosser = (*geom3DEdgeCrosser)(nil)

// ChainCrossing implements geodist.EdgeCrosser.
func (c *geom3DEdgeCrosser) ChainCrossing(p geodist.Point) (bool, geodist.Point) {
	nextEdgeV1 := p.GeomPoint
	pA, pB := closest3DSegmentSegment(c.edgeV0, c.edgeV1, c.nextEdgeV0, nextEdgeV1)
	c.nextEdgeV0 = nextEdgeV1
	c.updater.Update(geodist.Point{GeomPoint: pA}, geodist.Point{GeomPoint: pB})
	return false, geodist.Point{}
}

// perpendicularDistanceToRingPlane computes the perpendicular distance
// from a point to the plane defined by a linear ring. The plane is
// determined from the first three non-collinear vertices of the ring.
// If all vertices are collinear or the ring has fewer than 3 vertices,
// this falls back to 0 (treating the ring as degenerate).
func perpendicularDistanceToRingPlane(p geom.Coord, ring geodist.LinearRing) float64 {
	n := ring.NumVertexes()
	if n < 3 {
		return 0
	}
	v0 := ring.Vertex(0).GeomPoint
	// Find two edges that form a non-degenerate normal vector.
	for i := 1; i < n-1; i++ {
		v1 := ring.Vertex(i).GeomPoint
		v2 := ring.Vertex(i + 1).GeomPoint
		e1 := coordSub3D(v1, v0)
		e2 := coordSub3D(v2, v0)
		// Cross product e1 x e2 gives the plane normal.
		nx := e1[1]*e2[2] - e1[2]*e2[1]
		ny := e1[2]*e2[0] - e1[0]*e2[2]
		nz := e1[0]*e2[1] - e1[1]*e2[0]
		normN := math.Sqrt(nx*nx + ny*ny + nz*nz)
		if normN == 0 {
			continue
		}
		// Distance = |dot(p - v0, normal)| / |normal|.
		d := coordSub3D(p, v0)
		return math.Abs(d[0]*nx+d[1]*ny+d[2]*nz) / normN
	}
	return 0
}

// geomDistanceCalculator implements geodist.DistanceCalculator
type geomDistanceCalculator struct {
	updater               geodist.DistanceUpdater
	boundingBoxIntersects bool
}

var _ geodist.DistanceCalculator = (*geomDistanceCalculator)(nil)

// DistanceUpdater implements geodist.DistanceCalculator.
func (c *geomDistanceCalculator) DistanceUpdater() geodist.DistanceUpdater {
	return c.updater
}

// BoundingBoxIntersects implements geodist.DistanceCalculator.
func (c *geomDistanceCalculator) BoundingBoxIntersects() bool {
	return c.boundingBoxIntersects
}

// NewEdgeCrosser implements geodist.DistanceCalculator.
func (c *geomDistanceCalculator) NewEdgeCrosser(
	edge geodist.Edge, startPoint geodist.Point,
) geodist.EdgeCrosser {
	return &geomGeodistEdgeCrosser{
		strategy:   &lineintersector.NonRobustLineIntersector{},
		edgeV0:     edge.V0.GeomPoint,
		edgeV1:     edge.V1.GeomPoint,
		nextEdgeV0: startPoint.GeomPoint,
	}
}

// side corresponds to the side in which a point is relative to a line.
type pointSide int

const (
	pointSideLeft  pointSide = -1
	pointSideOn    pointSide = 0
	pointSideRight pointSide = 1
)

// findPointSide finds which side a point is relative to the infinite line
// given by the edge.
// Note this side is relative to the orientation of the line.
func findPointSide(p geom.Coord, eV0 geom.Coord, eV1 geom.Coord) pointSide {
	// This is the equivalent of using the point-gradient formula
	// and determining the sign, i.e. the sign of
	// d = (x-x1)(y2-y1) - (y-y1)(x2-x1)
	// where (x1,y1) and (x2,y2) is the edge and (x,y) is the point
	sign := (p.X()-eV0.X())*(eV1.Y()-eV0.Y()) - (eV1.X()-eV0.X())*(p.Y()-eV0.Y())
	switch {
	case sign == 0:
		return pointSideOn
	case sign > 0:
		return pointSideRight
	default:
		return pointSideLeft
	}
}

type linearRingSide int

const (
	outsideLinearRing linearRingSide = -1
	onLinearRing      linearRingSide = 0
	insideLinearRing  linearRingSide = 1
)

// findPointSideOfLinearRing returns whether a point is outside, on, or inside a
// linear ring.
func findPointSideOfLinearRing(point geodist.Point, linearRing geodist.LinearRing) linearRingSide {
	// This is done using the winding number algorithm, also known as the
	// "non-zero rule".
	// See: https://en.wikipedia.org/wiki/Point_in_polygon for intro.
	// See: http://geomalgorithms.com/a03-_inclusion.html for algorithm.
	// See also: https://en.wikipedia.org/wiki/Winding_number
	// See also: https://en.wikipedia.org/wiki/Nonzero-rule
	windingNumber := 0
	p := point.GeomPoint
	for edgeIdx, numEdges := 0, linearRing.NumEdges(); edgeIdx < numEdges; edgeIdx++ {
		e := linearRing.Edge(edgeIdx)
		eV0 := e.V0.GeomPoint
		eV1 := e.V1.GeomPoint
		// Same vertex; none of these checks will pass.
		if coordEqual(eV0, eV1) {
			continue
		}
		yMin := math.Min(eV0.Y(), eV1.Y())
		yMax := math.Max(eV0.Y(), eV1.Y())
		// If the edge isn't on the same level as Y, this edge isn't worth considering.
		if p.Y() > yMax || p.Y() < yMin {
			continue
		}
		side := findPointSide(p, eV0, eV1)
		// If the point is on the line if the edge was infinite, and the point is within the bounds
		// of the line segment denoted by the edge, there is a covering.
		if side == pointSideOn && (eV0.X() <= p.X() && p.X() <= eV1.X()) {
			return onLinearRing
		}
		// If the point is left of the segment and the line is rising
		// we have a circle going CCW, so increment.
		// Note we only compare [start, end) as we do not want to double count points
		// which are on the same X / Y axis as an edge vertex.
		if side == pointSideLeft && eV0.Y() <= p.Y() && p.Y() < eV1.Y() {
			windingNumber++
		}
		// If the line is to the right of the segment and the
		// line is falling, we a have a circle going CW so decrement.
		// Note we only compare [start, end) as we do not want to double count points
		// which are on the same X / Y axis as an edge vertex.
		if side == pointSideRight && eV1.Y() <= p.Y() && p.Y() < eV0.Y() {
			windingNumber--
		}
	}
	if windingNumber != 0 {
		return insideLinearRing
	}
	return outsideLinearRing
}

// PointIntersectsLinearRing implements geodist.DistanceCalculator.
func (c *geomDistanceCalculator) PointIntersectsLinearRing(
	point geodist.Point, linearRing geodist.LinearRing,
) bool {
	switch findPointSideOfLinearRing(point, linearRing) {
	case insideLinearRing, onLinearRing:
		return true
	default:
		return false
	}
}

// ClosestPointToEdge implements geodist.DistanceCalculator.
func (c *geomDistanceCalculator) ClosestPointToEdge(
	e geodist.Edge, p geodist.Point,
) (geodist.Point, bool) {
	// Edge is a single point. Closest point must be any edge vertex.
	if coordEqual(e.V0.GeomPoint, e.V1.GeomPoint) {
		return e.V0, coordEqual(e.V0.GeomPoint, p.GeomPoint)
	}

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
	if coordEqual(p.GeomPoint, e.V0.GeomPoint) {
		return p, true
	}
	if coordEqual(p.GeomPoint, e.V1.GeomPoint) {
		return p, true
	}

	ac := coordSub(p.GeomPoint, e.V0.GeomPoint)
	ab := coordSub(e.V1.GeomPoint, e.V0.GeomPoint)

	r := coordDot(ac, ab) / coordNorm2(ab)
	if r < 0 || r > 1 {
		return p, false
	}
	return geodist.Point{GeomPoint: coordAdd(e.V0.GeomPoint, coordMul(ab, r))}, true
}

// FrechetDistance calculates the Frechet distance between two geometries.
func FrechetDistance(a, b geo.Geometry) (*float64, error) {
	if a.Empty() || b.Empty() {
		return nil, nil
	}
	if a.SRID() != b.SRID() {
		return nil, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	distance, err := geos.FrechetDistance(a.EWKB(), b.EWKB())
	if err != nil {
		return nil, err
	}
	return &distance, nil
}

// FrechetDistanceDensify calculates the Frechet distance between two geometries.
func FrechetDistanceDensify(a, b geo.Geometry, densifyFrac float64) (*float64, error) {
	// For Frechet distance, we take <= 0 to disable the densifyFrac parameter.
	// This differs from HausdorffDistance, but follows PostGIS behavior.
	if densifyFrac <= 0 {
		return FrechetDistance(a, b)
	}
	if a.Empty() || b.Empty() {
		return nil, nil
	}
	if a.SRID() != b.SRID() {
		return nil, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if err := verifyDensifyFrac(densifyFrac); err != nil {
		return nil, err
	}
	distance, err := geos.FrechetDistanceDensify(a.EWKB(), b.EWKB(), densifyFrac)
	if err != nil {
		return nil, err
	}
	return &distance, nil
}

// HausdorffDistance calculates the Hausdorff distance between two geometries.
func HausdorffDistance(a, b geo.Geometry) (*float64, error) {
	if a.Empty() || b.Empty() {
		return nil, nil
	}
	if a.SRID() != b.SRID() {
		return nil, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	distance, err := geos.HausdorffDistance(a.EWKB(), b.EWKB())
	if err != nil {
		return nil, err
	}
	return &distance, nil
}

// HausdorffDistanceDensify calculates the Hausdorff distance between two geometries.
func HausdorffDistanceDensify(a, b geo.Geometry, densifyFrac float64) (*float64, error) {
	if a.Empty() || b.Empty() {
		return nil, nil
	}
	if a.SRID() != b.SRID() {
		return nil, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	if err := verifyDensifyFrac(densifyFrac); err != nil {
		return nil, err
	}

	distance, err := geos.HausdorffDistanceDensify(a.EWKB(), b.EWKB(), densifyFrac)
	if err != nil {
		return nil, err
	}
	return &distance, nil
}

// ClosestPoint returns the first point located on geometry A on the shortest line between the geometries.
func ClosestPoint(a, b geo.Geometry) (geo.Geometry, error) {
	shortestLine, err := ShortestLineString(a, b)
	if err != nil {
		return geo.Geometry{}, err
	}
	shortestLineT, err := shortestLine.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	closestPoint, err := geo.MakeGeometryFromPointCoords(
		shortestLineT.(*geom.LineString).Coord(0).X(),
		shortestLineT.(*geom.LineString).Coord(0).Y(),
	)
	if err != nil {
		return geo.Geometry{}, err
	}
	return closestPoint.CloneWithSRID(a.SRID())
}

func verifyDensifyFrac(f float64) error {
	if f < 0 || f > 1 {
		return pgerror.Newf(pgcode.InvalidParameterValue, "fraction must be in range [0, 1], got %f", f)
	}
	// Very small densifyFrac potentially causes a SIGFPE or generate a large
	// amount of memory. Guard against this.
	const fracTooSmall = 1e-6
	if f > 0 && f < fracTooSmall {
		return pgerror.Newf(pgcode.InvalidParameterValue, "fraction %f is too small, must be at least %f", f, fracTooSmall)
	}
	return nil
}

// findPointSideOfPolygon returns whether a point intersects with a polygon.
func findPointSideOfPolygon(point geom.T, polygon geom.T) (linearRingSide, error) {
	// Convert point from a geom.T to a *geodist.Point.
	_, ok := point.(*geom.Point)
	if !ok {
		return outsideLinearRing, pgerror.Newf(pgcode.InvalidParameterValue, "first geometry passed to findPointSideOfPolygon must be a point")
	}
	pointGeodistShape, err := geomToGeodist(point)
	if err != nil {
		return outsideLinearRing, err
	}
	pointGeodistPoint, ok := pointGeodistShape.(*geodist.Point)
	if !ok {
		return outsideLinearRing, pgerror.Newf(pgcode.InvalidParameterValue, "geomToGeodist failed to convert a *geom.Point to a *geodist.Point")
	}

	// Convert polygon from a geom.T to a geodist.Polygon.
	_, ok = polygon.(*geom.Polygon)
	if !ok {
		return outsideLinearRing, pgerror.Newf(pgcode.InvalidParameterValue, "second geometry passed to findPointSideOfPolygon must be a polygon")
	}
	polygonGeodistShape, err := geomToGeodist(polygon)
	if err != nil {
		return outsideLinearRing, err
	}
	polygonGeodistPolygon, ok := polygonGeodistShape.(geodist.Polygon)
	if !ok {
		return outsideLinearRing, pgerror.Newf(pgcode.InvalidParameterValue, "geomToGeodist failed to convert a *geom.Polygon to a geodist.Polygon")
	}

	// Point cannot be inside an empty polygon.
	if polygonGeodistPolygon.NumLinearRings() == 0 {
		return outsideLinearRing, nil
	}

	// Find which side the point is relative to the main outer boundary of
	// the polygon. If it outside or on the boundary, we can conclude
	// that the point is on that side of the overall polygon as well.
	mainRing := polygonGeodistPolygon.LinearRing(0)
	switch pointSide := findPointSideOfLinearRing(*pointGeodistPoint, mainRing); pointSide {
	case insideLinearRing:
	case outsideLinearRing, onLinearRing:
		return pointSide, nil
	default:
		return outsideLinearRing, pgerror.Newf(pgcode.InvalidParameterValue, "unknown linearRingSide %d", pointSide)
	}

	// If the point is inside the main outer boundary of the polygon, we must
	// determine which side it is relative to every hole in the polygon.
	// If it is inside any hole, it is outside the polygon. If it is on the
	// ring of any hole, it is on the boundary of the polygon. Otherwise,
	// it is inside the polygon.
	for ringNum := 1; ringNum < polygonGeodistPolygon.NumLinearRings(); ringNum++ {
		polygonHole := polygonGeodistPolygon.LinearRing(ringNum)
		switch pointSide := findPointSideOfLinearRing(*pointGeodistPoint, polygonHole); pointSide {
		case insideLinearRing:
			return outsideLinearRing, nil
		case onLinearRing:
			return onLinearRing, nil
		case outsideLinearRing:
			continue
		default:
			return outsideLinearRing, pgerror.Newf(pgcode.InvalidParameterValue, "unknown linearRingSide %d", pointSide)
		}
	}

	return insideLinearRing, nil
}
