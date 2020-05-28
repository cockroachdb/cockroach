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
)

// DWithin returns whether a is within distance d of b, i.e. Distance(a, b) <= d.
// If A or B contains empty Geography objects, this will return false.
func DWithin(
	a *geo.Geography, b *geo.Geography, distance float64, useSphereOrSpheroid UseSphereOrSpheroid,
) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a, b)
	}
	if distance < 0 {
		return false, errors.Newf("dwithin distance cannot be less than zero")
	}

	aRegions, err := a.AsS2(geo.EmptyBehaviorError)
	if err != nil {
		if geo.IsEmptyGeometryError(err) {
			return false, nil
		}
		return false, err
	}
	bRegions, err := b.AsS2(geo.EmptyBehaviorError)
	if err != nil {
		if geo.IsEmptyGeometryError(err) {
			return false, nil
		}
		return false, err
	}
	spheroid := geographiclib.WGS84Spheroid
	maybeClosestDistance, err := distanceGeographyRegions(spheroid, useSphereOrSpheroid, aRegions, bRegions, distance)
	if err != nil {
		return false, err
	}
	return maybeClosestDistance <= distance, nil
}
