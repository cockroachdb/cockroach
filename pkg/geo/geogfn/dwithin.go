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
	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// DWithin returns whether a is within distance d of b, i.e. Distance(a, b) <= d.
func DWithin(
	a *geo.Geography, b *geo.Geography, distance float64, useSphereOrSpheroid UseSphereOrSpheroid,
) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a, b)
	}
	if distance < 0 {
		return false, errors.Newf("dwithin distance cannot be less than zero")
	}

	aRegions, err := a.AsS2()
	if err != nil {
		return false, err
	}
	bRegions, err := b.AsS2()
	if err != nil {
		return false, err
	}
	spheroid := geographiclib.WGS84Spheroid
	if useSphereOrSpheroid == UseSpheroid {
		maybeClosestDistance, err := distanceSpheroidRegions(spheroid, aRegions, bRegions, distance)
		if err != nil {
			return false, err
		}
		return maybeClosestDistance <= distance, nil
	}

	aShapeIndex, aPoints, err := s2RegionsToPointsAndShapeIndexes(aRegions)
	if err != nil {
		return false, err
	}
	bShapeIndex, bPoints, err := s2RegionsToPointsAndShapeIndexes(bRegions)
	if err != nil {
		return false, err
	}

	// Find the successor to the chord angle to signify <= distance when using `IsDistanceLess`.
	chordAngle := s1.ChordAngleFromAngle(s1.Angle(distance / spheroid.SphereRadius)).Successor()
	if aShapeIndex.Len() > 0 {
		aQuery := s2.NewClosestEdgeQuery(aShapeIndex, nil)

		if bShapeIndex.Len() > 0 {
			if aQuery.IsDistanceLess(s2.NewMinDistanceToShapeIndexTarget(bShapeIndex), chordAngle) {
				return true, nil
			}
		}
		for _, bPoint := range bPoints {
			if aQuery.IsDistanceLess(s2.NewMinDistanceToPointTarget(bPoint), chordAngle) {
				return true, nil
			}
		}
	}

	for _, aPoint := range aPoints {
		if bShapeIndex.Len() > 0 {
			bQuery := s2.NewClosestEdgeQuery(bShapeIndex, nil)
			if bQuery.IsDistanceLess(s2.NewMinDistanceToPointTarget(aPoint), chordAngle) {
				return true, nil
			}
		}
		for _, bPoint := range bPoints {
			if s2.ChordAngleBetweenPoints(aPoint, bPoint) <= chordAngle {
				return true, nil
			}
		}
	}
	return false, nil
}
