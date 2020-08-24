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
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
)

// SharedPaths Returns a geometry collection containing paths shared by the two input geometries.
func SharedPaths(a *geo.Geometry, b *geo.Geometry) (*geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return nil, geo.NewMismatchingSRIDsError(a, b)
	}
	paths, err := geos.SharedPaths(a.EWKB(), b.EWKB())
	if err != nil {
		return nil, err
	}
	gm, err := geo.ParseGeometryFromEWKB(paths)
	if err != nil {
		return nil, err
	}
	return gm, nil
}
