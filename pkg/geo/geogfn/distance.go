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
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// wgs84SphereRadiusMeters is the radius if WGS84 was a sphere.
const wgs84SphereRadiusMeters = 6371008.7714150598325213222

// Distance returns the distance between geographies a and b on a sphere or spheroid.
func Distance(a *geo.Geography, b *geo.Geography) (float64, error) {
	aRegions, err := a.AsS2()
	if err != nil {
		return 0, err
	}
	bRegions, err := b.AsS2()
	if err != nil {
		return 0, err
	}

	aShapeIndex, aPoints, err := s2RegionsToPointsAndShapeIndexes(aRegions)
	if err != nil {
		return 0, err
	}
	bShapeIndex, bPoints, err := s2RegionsToPointsAndShapeIndexes(bRegions)
	if err != nil {
		return 0, err
	}

	minDistanceUpdater := newSphereMinDistanceUpdater()
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
	minD s1.ChordAngle
}

// newSphereMinDistanceUpdater returns a new sphereMinDistanceUpdater.
func newSphereMinDistanceUpdater() *sphereMinDistanceUpdater {
	return &sphereMinDistanceUpdater{minD: s1.StraightChordAngle}
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
	return u.minD.Angle().Radians() * wgs84SphereRadiusMeters
}
