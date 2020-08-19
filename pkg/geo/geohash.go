// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import (
	"github.com/cockroachdb/errors"
	"github.com/pierrre/geohash"
)

// NewGeometryPointFromGeoHash converts a GeoHash to a Geometry Point
// using a Lng/Lat Point representation of the GeoHash.
func NewGeometryPointFromGeoHash(g string, precision int) (*Geometry, error) {
	if len(g) == 0 {
		return nil, errors.Newf("Length of geohash must be greater than 0")
	}

	// If precision is more than the length of the geohash
	// or if precision is less than 0 then set
	// precision equal to length of geohash.
	if precision > len(g) || precision < 0 {
		precision = len(g)
	}
	box, err := geohash.Decode(g[0:precision])
	if err != nil {
		return nil, err
	}
	point := box.Round()
	geom, gErr := NewGeometryFromPointCoords(point.Lon, point.Lat)
	if gErr != nil {
		return nil, gErr
	}
	return geom, nil
}
