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
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
)

// Snap returns the input geometry with the vertices snapped to the target
// geometry. Tolerance is used to control where snapping is performed.
// If no snapping occurs then the input geometry is returned unchanged.
func Snap(input, target geo.Geometry, tolerance float64) (geo.Geometry, error) {
	snappedEWKB, err := geos.Snap(input.EWKB(), target.EWKB(), tolerance)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(snappedEWKB)
}
