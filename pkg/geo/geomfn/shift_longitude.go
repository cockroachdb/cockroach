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
	"github.com/twpayne/go-geom"
)

// ShiftLongitude returns a modified version of a geometry in which the longitude (X coordinate)
// of each point is incremented by 360 if it is <0 and decremented by 360 if it is >180.
// The result is only meaningful if the coordinates are in longitude/latitude.
func ShiftLongitude(geometry geo.Geometry) (geo.Geometry, error) {
	t, err := geometry.AsGeomT()
	if err != nil {
		return geometry, err
	}

	newT, err := applyOnCoordsForGeomT(t, func(l geom.Layout, dst []float64, src []float64) error {
		copy(dst, src)
		if src[0] < 0 {
			dst[0] += 360
		} else if src[0] > 180 {
			dst[0] -= 360
		}
		return nil
	})

	if err != nil {
		return geometry, err
	}

	return geo.MakeGeometryFromGeomT(newT)
}
