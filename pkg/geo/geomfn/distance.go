// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geodist"
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
func MinDistance(a *geo.Geometry, b *geo.Geometry) (float64, error) {
	if a.SRID() != b.SRID() {
		return 0, geo.NewMismatchingSRIDsError(a, b)
	}
	return minDistanceInternal(a, b, 0, geo.EmptyBehaviorOmit)
}

// MaxDistance returns the maximum distance across every pair of points comprising
// geometries A and B.
func MaxDistance(a *geo.Geometry, b *geo.Geometry) (float64, error) {
	if a.SRID() != b.SRID() {
		return 0, geo.NewMismatchingSRIDsError(a, b)
	}
	return maxDistanceInternal(a, b, math.MaxFloat64, geo.EmptyBehaviorOmit)
}

// DWithin determines if any part of geometry A is within D units of geometry B.
func DWithin(a *geo.Geometry, b *geo.Geometry, d float64) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a, b)
	}
	if d < 0 {
		return false, errors.Newf("dwithin distance cannot be less than zero")
	}
	if !a.CartesianBoundingBox().Buffer(d).Intersects(b.CartesianBoundingBox()) {
		return false, nil
	}
	dist, err := minDistanceInternal(a, b, d, geo.EmptyBehaviorError)
	if err != nil {
		// In case of any empty geometries return false.
		if geo.IsEmptyGeometryError(err) {
			return false, nil
		}
		return false, err
	}
	return dist <= d, nil
}

// DFullyWithin determines whether the maximum distance across every pair of points
// comprising geometries A and B is within D units.
func DFullyWithin(a *geo.Geometry, b *geo.Geometry, d float64) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a, b)
	}
	if d < 0 {
		return false, errors.Newf("dwithin distance cannot be less than zero")
	}
	if !a.CartesianBoundingBox().Buffer(d).Covers(b.CartesianBoundingBox()) {
		return false, nil
	}
	dist, err := maxDistanceInternal(a, b, d, geo.EmptyBehaviorError)
	if err != nil {
		// In case of any empty geometries return false.
		if geo.IsEmptyGeometryError(err) {
			return false, nil
		}
		return false, err
	}
	return dist <= d, nil
}

// LongestLineString returns the LineString corresponds to maximum distance across
// every pair of points comprising geometries A and B.
func LongestLineString(a *geo.Geometry, b *geo.Geometry) (*geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return nil, geo.NewMismatchingSRIDsError(a, b)
	}
	u := newGeomMaxDistanceUpdater(math.MaxFloat64)
	return distanceLineStringInternal(a, b, u, geo.EmptyBehaviorOmit)
}

// ShortestLineString returns the LineString corresponds to minimum distance across
// every pair of points comprising geometries A and B.
func ShortestLineString(a *geo.Geometry, b *geo.Geometry) (*geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return nil, geo.NewMismatchingSRIDsError(a, b)
	}
	u := newGeomMinDistanceUpdater(0)
	return distanceLineStringInternal(a, b, u, geo.EmptyBehaviorOmit)
}

// distanceLineStringInternal calculates the LineString between two geometries using
// the DistanceCalculator operator.
// If there are any EMPTY Geometry objects, they will be ignored. It will return an
// EmptyGeometryError if A or B contains only EMPTY geometries, even if emptyBehavior
// is set to EmptyBehaviorOmit.
func distanceLineStringInternal(
	a *geo.Geometry, b *geo.Geometry, u geodist.DistanceUpdater, emptyBehavior geo.EmptyBehavior,
) (*geo.Geometry, error) {
	c := &geomDistanceCalculator{updater: u, boundingBoxIntersects: a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox())}
	_, err := distanceInternal(a, b, c, emptyBehavior)
	if err != nil {
		return nil, err
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
		return nil, errors.Newf("programmer error: unknown behavior")
	}
	lineString := geom.NewLineStringFlat(geom.XY, append(coordA, coordB...)).SetSRID(int(a.SRID()))
	return geo.NewGeometryFromGeom(lineString)
}

// maxDistanceInternal finds the maximum distance between two geometries.
// We can re-use the same algorithm as min-distance, allowing skips of checks that involve
// the interiors or intersections as those will always be less then the maximum min-distance.
func maxDistanceInternal(
	a *geo.Geometry, b *geo.Geometry, stopAfterGT float64, emptyBehavior geo.EmptyBehavior,
) (float64, error) {
	u := newGeomMaxDistanceUpdater(stopAfterGT)
	c := &geomDistanceCalculator{updater: u, boundingBoxIntersects: a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox())}
	return distanceInternal(a, b, c, emptyBehavior)
}

// minDistanceInternal finds the minimum distance between two geometries.
// This implementation is done in-house, as compared to using GEOS.
func minDistanceInternal(
	a *geo.Geometry, b *geo.Geometry, stopAfterLE float64, emptyBehavior geo.EmptyBehavior,
) (float64, error) {
	u := newGeomMinDistanceUpdater(stopAfterLE)
	c := &geomDistanceCalculator{updater: u, boundingBoxIntersects: a.CartesianBoundingBox().Intersects(b.CartesianBoundingBox())}
	return distanceInternal(a, b, c, emptyBehavior)
}

// distanceInternal calculates the distance between two geometries using
// the DistanceCalculator operator.
// If there are any EMPTY Geometry objects, they will be ignored. It will return an
// EmptyGeometryError if A or B contains only EMPTY geometries, even if emptyBehavior
// is set to EmptyBehaviorOmit.
func distanceInternal(
	a *geo.Geometry, b *geo.Geometry, c geodist.DistanceCalculator, emptyBehavior geo.EmptyBehavior,
) (float64, error) {
	aGeoms, err := flattenGeometry(a, emptyBehavior)
	if err != nil {
		return 0, err
	}
	bGeoms, err := flattenGeometry(b, emptyBehavior)
	if err != nil {
		return 0, err
	}
	// If either side has no geoms, then we error out.
	if len(aGeoms) == 0 || len(bGeoms) == 0 {
		return 0, geo.NewEmptyGeometryError()
	}

	for _, aGeom := range aGeoms {
		aGeodist, err := geomToGeodist(aGeom)
		if err != nil {
			return 0, err
		}
		for _, bGeom := range bGeoms {
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
		}
	}
	return c.DistanceUpdater().Distance(), nil
}

// geomToGeodist converts a given geom object to a geodist shape.
func geomToGeodist(g geom.T) (geodist.Shape, error) {
	switch g := g.(type) {
	case *geom.Point:
		return &geomGeodistPoint{Coord: g.Coords()}, nil
	case *geom.LineString:
		return &geomGeodistLineString{LineString: g}, nil
	case *geom.Polygon:
		return &geomGeodistPolygon{Polygon: g}, nil
	}
	return nil, errors.Newf("could not find shape: %T", g)
}

// geomGeodistPoint implements geodist.Point.
type geomGeodistPoint struct {
	geom.Coord
}

var _ geodist.Point = (*geomGeodistPoint)(nil)

// IsShape implements the geodist.Point interface.
func (*geomGeodistPoint) IsShape() {}

// Point implements the geodist.Point interface.
func (*geomGeodistPoint) IsPoint() {}

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
		V0: &geomGeodistPoint{Coord: g.LineString.Coord(i)},
		V1: &geomGeodistPoint{Coord: g.LineString.Coord(i + 1)},
	}
}

// NumEdges implements the geodist.LineString interface.
func (g *geomGeodistLineString) NumEdges() int {
	return g.LineString.NumCoords() - 1
}

// Vertex implements the geodist.LineString interface.
func (g *geomGeodistLineString) Vertex(i int) geodist.Point {
	return &geomGeodistPoint{Coord: g.LineString.Coord(i)}
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
		V0: &geomGeodistPoint{Coord: g.LinearRing.Coord(i)},
		V1: &geomGeodistPoint{Coord: g.LinearRing.Coord(i + 1)},
	}
}

// NumEdges implements the geodist.LinearRing interface.
func (g *geomGeodistLinearRing) NumEdges() int {
	return g.LinearRing.NumCoords() - 1
}

// Vertex implements the geodist.LinearRing interface.
func (g *geomGeodistLinearRing) Vertex(i int) geodist.Point {
	return &geomGeodistPoint{Coord: g.LinearRing.Coord(i)}
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
	nextEdgeV1 := p.(*geomGeodistPoint).Coord
	result := lineintersector.LineIntersectsLine(
		c.strategy,
		c.edgeV0,
		c.edgeV1,
		c.nextEdgeV0,
		nextEdgeV1,
	)
	c.nextEdgeV0 = nextEdgeV1
	if result.HasIntersection() {
		return true, &geomGeodistPoint{result.Intersection()[0]}
	}
	return false, nil
}

// geomMinDistanceUpdater finds the minimum distance using geom calculations.
// And preserve the line's endpoints as geom.Coord which corresponds to minimum
// distance. Methods will return early if it finds a minimum distance <= stopAfterLE.
type geomMinDistanceUpdater struct {
	currentValue float64
	stopAfterLE  float64
	// coordA represents the first vertex of the edge that holds the maximum distance.
	coordA geom.Coord
	// coordB represents the second vertex of the edge that holds the maximum distance.
	coordB geom.Coord

	geometricalObjOrder geometricalObjectsOrder
}

var _ geodist.DistanceUpdater = (*geomMinDistanceUpdater)(nil)

// newGeomMinDistanceUpdater returns a new geomMinDistanceUpdater with the
// correct arguments set up.
func newGeomMinDistanceUpdater(stopAfterLE float64) *geomMinDistanceUpdater {
	return &geomMinDistanceUpdater{
		currentValue:        math.MaxFloat64,
		stopAfterLE:         stopAfterLE,
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
func (u *geomMinDistanceUpdater) Update(aInterface geodist.Point, bInterface geodist.Point) bool {
	a := aInterface.(*geomGeodistPoint).Coord
	b := bInterface.(*geomGeodistPoint).Coord

	dist := coordNorm(coordSub(a, b))
	if dist < u.currentValue {
		u.currentValue = dist
		if u.geometricalObjOrder == geometricalObjectsFlipped {
			u.coordA = b
			u.coordB = a
		} else {
			u.coordA = a
			u.coordB = b
		}
		return dist <= u.stopAfterLE
	}
	return false
}

// OnIntersects implements the geodist.DistanceUpdater interface.
func (u *geomMinDistanceUpdater) OnIntersects(p geodist.Point) bool {
	u.coordA = p.(*geomGeodistPoint).Coord
	u.coordB = p.(*geomGeodistPoint).Coord
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
// distance. Methods will return early if it finds a distance > stopAfterGT.
type geomMaxDistanceUpdater struct {
	currentValue float64
	stopAfterGT  float64

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
func newGeomMaxDistanceUpdater(stopAfterGT float64) *geomMaxDistanceUpdater {
	return &geomMaxDistanceUpdater{
		currentValue:        -math.MaxFloat64,
		stopAfterGT:         stopAfterGT,
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
func (u *geomMaxDistanceUpdater) Update(aInterface geodist.Point, bInterface geodist.Point) bool {
	a := aInterface.(*geomGeodistPoint).Coord
	b := bInterface.(*geomGeodistPoint).Coord

	dist := coordNorm(coordSub(a, b))
	if dist > u.currentValue {
		u.currentValue = dist
		if u.geometricalObjOrder == geometricalObjectsFlipped {
			u.coordA = b
			u.coordB = a
		} else {
			u.coordA = a
			u.coordB = b
		}
		return dist > u.stopAfterGT
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
		edgeV0:     edge.V0.(*geomGeodistPoint).Coord,
		edgeV1:     edge.V1.(*geomGeodistPoint).Coord,
		nextEdgeV0: startPoint.(*geomGeodistPoint).Coord,
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

// PointInLinearRing implements geodist.DistanceCalculator.
func (c *geomDistanceCalculator) PointInLinearRing(
	point geodist.Point, polygon geodist.LinearRing,
) bool {
	// This is done using the winding number algorithm, also known as the
	// "non-zero rule".
	// See: https://en.wikipedia.org/wiki/Point_in_polygon for intro.
	// See: http://geomalgorithms.com/a03-_inclusion.html for algorithm.
	// See also: https://en.wikipedia.org/wiki/Winding_number
	// See also: https://en.wikipedia.org/wiki/Nonzero-rule
	windingNumber := 0
	p := point.(*geomGeodistPoint).Coord
	for edgeIdx := 0; edgeIdx < polygon.NumEdges(); edgeIdx++ {
		e := polygon.Edge(edgeIdx)
		eV0 := e.V0.(*geomGeodistPoint).Coord
		eV1 := e.V1.(*geomGeodistPoint).Coord
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
		if side == pointSideOn &&
			((eV0.X() <= p.X() && p.X() < eV1.X()) || (eV1.X() <= p.X() && p.X() < eV0.X()) ||
				(eV0.Y() <= p.Y() && p.Y() < eV1.Y()) || (eV1.Y() <= p.Y() && p.Y() < eV0.Y())) {
			return true
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
	return windingNumber != 0
}

// ClosestPointToEdge implements geodist.DistanceCalculator.
func (c *geomDistanceCalculator) ClosestPointToEdge(
	edge geodist.Edge, pointInterface geodist.Point,
) (geodist.Point, bool) {
	eV0 := edge.V0.(*geomGeodistPoint).Coord
	eV1 := edge.V1.(*geomGeodistPoint).Coord
	p := pointInterface.(*geomGeodistPoint).Coord

	// Edge is a single point. Closest point must be any edge vertex.
	if coordEqual(eV0, eV1) {
		return edge.V0, coordEqual(eV0, p)
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
	if coordEqual(p, eV0) {
		return pointInterface, true
	}
	if coordEqual(p, eV1) {
		return pointInterface, true
	}

	ac := coordSub(p, eV0)
	ab := coordSub(eV1, eV0)

	r := coordDot(ac, ab) / coordNorm2(ab)
	if r < 0 || r > 1 {
		return pointInterface, false
	}
	return &geomGeodistPoint{Coord: coordAdd(eV0, coordMul(ab, r))}, true
}
