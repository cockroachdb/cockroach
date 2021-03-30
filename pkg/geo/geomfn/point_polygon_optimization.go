// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
)

// PointPolygonControlFlowType signals what control flow to follow.
type PointPolygonControlFlowType int

const (
	// PPCFCheckNextPolygon signals that the current point should be checked
	// against the next polygon.
	PPCFCheckNextPolygon PointPolygonControlFlowType = iota
	// PPCFSkipToNextPoint signals that the rest of the checking for the current
	// point can be skipped.
	PPCFSkipToNextPoint
	// PPCFReturnTrue signals that the function should exit early and return true.
	PPCFReturnTrue
)

// PointInPolygonEventListener is an interface implemented for each
// binary predicate making use of the point in polygon optimization
// to specify the behavior in pointKindRelatesToPolygonKind.
type PointInPolygonEventListener interface {
	// OnPointIntersects returns whether the function should exit and return true,
	// skip to the next point, or check the current point against the next polygon
	// in the case where a point intersects with a polygon. The inside param
	// signifies whether the point is inside or on the boundary of the polygon.
	OnPointIntersects(inside bool) PointPolygonControlFlowType
	// OnPointDoesNotIntersect returns whether the function should early exit and
	// return false in the case where a point does not intersect any polygon.
	OnPointDoesNotIntersect() bool
	// AfterPointPolygonLoops returns the bool to return after the point-polygon
	// loops have finished.
	AfterPointPolygonLoops() bool
}

// For Intersects, at least one point must intersect with at least one polygon.
type intersectsPIPEventListener struct{}

func (el *intersectsPIPEventListener) OnPointIntersects(inside bool) PointPolygonControlFlowType {
	return PPCFReturnTrue
}

func (el *intersectsPIPEventListener) OnPointDoesNotIntersect() bool {
	return false
}

func (el *intersectsPIPEventListener) AfterPointPolygonLoops() bool {
	return false
}

var _ PointInPolygonEventListener = (*intersectsPIPEventListener)(nil)

func newIntersectsPIPEventListener() *intersectsPIPEventListener {
	return &intersectsPIPEventListener{}
}

// For CoveredBy, every point must intersect with at least one polygon.
type coveredByPIPEventListener struct {
	intersectsOnce bool
}

func (el *coveredByPIPEventListener) OnPointIntersects(inside bool) PointPolygonControlFlowType {
	el.intersectsOnce = true
	return PPCFSkipToNextPoint
}

func (el *coveredByPIPEventListener) OnPointDoesNotIntersect() bool {
	return true
}

func (el *coveredByPIPEventListener) AfterPointPolygonLoops() bool {
	return el.intersectsOnce
}

var _ PointInPolygonEventListener = (*coveredByPIPEventListener)(nil)

func newCoveredByPIPEventListener() *coveredByPIPEventListener {
	return &coveredByPIPEventListener{intersectsOnce: false}
}

// For Within, every point must intersect with at least one polygon.
type withinPIPEventListener struct {
	insideOnce bool
}

func (el *withinPIPEventListener) OnPointIntersects(inside bool) PointPolygonControlFlowType {
	if el.insideOnce {
		return PPCFSkipToNextPoint
	}
	if inside {
		el.insideOnce = true
		return PPCFSkipToNextPoint
	}
	return PPCFCheckNextPolygon
}

func (el *withinPIPEventListener) OnPointDoesNotIntersect() bool {
	return true
}

func (el *withinPIPEventListener) AfterPointPolygonLoops() bool {
	return el.insideOnce
}

var _ PointInPolygonEventListener = (*withinPIPEventListener)(nil)

func newWithinPIPEventListener() *withinPIPEventListener {
	return &withinPIPEventListener{insideOnce: false}
}

// PointKindIntersectsPolygonKind returns whether a (multi)point
// and a (multi)polygon intersect.
func PointKindIntersectsPolygonKind(
	pointKind geo.Geometry, polygonKind geo.Geometry,
) (bool, error) {
	return pointKindRelatesToPolygonKind(pointKind, polygonKind, newIntersectsPIPEventListener())
}

// PointKindCoveredByPolygonKind returns whether a (multi)point
// is covered by a (multi)polygon.
func PointKindCoveredByPolygonKind(pointKind geo.Geometry, polygonKind geo.Geometry) (bool, error) {
	return pointKindRelatesToPolygonKind(pointKind, polygonKind, newCoveredByPIPEventListener())
}

// PointKindWithinPolygonKind returns whether a (multi)point
// is contained within a (multi)polygon.
func PointKindWithinPolygonKind(pointKind geo.Geometry, polygonKind geo.Geometry) (bool, error) {
	return pointKindRelatesToPolygonKind(pointKind, polygonKind, newWithinPIPEventListener())
}

// pointKindRelatesToPolygonKind returns whether a (multi)point
// and a (multi)polygon have the given relationship.
func pointKindRelatesToPolygonKind(
	pointKind geo.Geometry, polygonKind geo.Geometry, eventListener PointInPolygonEventListener,
) (bool, error) {
	pointKindBaseT, err := pointKind.AsGeomT()
	if err != nil {
		return false, err
	}
	polygonKindBaseT, err := polygonKind.AsGeomT()
	if err != nil {
		return false, err
	}
	pointKindIterator := geo.NewGeomTIterator(pointKindBaseT, geo.EmptyBehaviorOmit)
	polygonKindIterator := geo.NewGeomTIterator(polygonKindBaseT, geo.EmptyBehaviorOmit)

	// Check whether each point intersects with at least one polygon.
	// The behavior for each predicate is dictated by eventListener.
pointOuterLoop:
	for {
		point, hasPoint, err := pointKindIterator.Next()
		if err != nil {
			return false, err
		}
		if !hasPoint {
			break
		}
		// Reset the polygon iterator on each iteration of the point iterator.
		polygonKindIterator.Reset()
		curIntersects := false
		for {
			polygon, hasPolygon, err := polygonKindIterator.Next()
			if err != nil {
				return false, err
			}
			if !hasPolygon {
				break
			}
			pointSide, err := findPointSideOfPolygon(point, polygon)
			if err != nil {
				return false, err
			}
			switch pointSide {
			case insideLinearRing, onLinearRing:
				curIntersects = true
				inside := pointSide == insideLinearRing
				switch eventListener.OnPointIntersects(inside) {
				case PPCFCheckNextPolygon:
				case PPCFSkipToNextPoint:
					continue pointOuterLoop
				case PPCFReturnTrue:
					return true, nil
				}
			case outsideLinearRing:
			default:
				return false, errors.Newf("findPointSideOfPolygon returned unknown linearRingSide %d", pointSide)
			}
		}
		if !curIntersects && eventListener.OnPointDoesNotIntersect() {
			return false, nil
		}
	}
	return eventListener.AfterPointPolygonLoops(), nil
}
