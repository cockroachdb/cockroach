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

// #cgo LDFLAGS: -lm
// #include "geodesic.h"
import "C"

var (
	// WGS84Spheroid represents the default WGS84 ellipsoid.
	WGS84Spheroid = NewSpheroid(6378137, 1/298.257223563)
)

// Spheroid is an object that can perform geodesic operations
// on a given spheroid.
type Spheroid struct {
	geodesic C.struct_geod_geodesic
}

// NewSpheroid creates a spheroid from a radius and flattening.
func NewSpheroid(radius float64, flattening float64) *Spheroid {
	spheroid := &Spheroid{}
	C.geod_init(&spheroid.geodesic, C.double(radius), C.double(flattening))
	return spheroid
}

// LatLng is a placeholder for s2.LatLng.
type LatLng struct {
	Lat, Lng float64
}

// Inverse solves the inverse geodesic problem on the given spheroid.
// Returns s12 (distance in meters), az1 (azimuth to point 1) and az2 (azimuth to point 2).
func (s *Spheroid) Inverse(a, b LatLng) (s12, az1, az2 float64) {
	var retS12, retAZ1, retAZ2 C.double
	C.geod_inverse(
		&s.geodesic,
		C.double(a.Lat),
		C.double(a.Lng),
		C.double(b.Lat),
		C.double(b.Lng),
		&retS12,
		&retAZ1,
		&retAZ2,
	)
	return float64(retS12), float64(retAZ1), float64(retAZ2)
}
