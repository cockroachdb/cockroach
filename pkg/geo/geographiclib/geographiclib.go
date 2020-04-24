// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geographiclib is a wrapper around the GeographicLib library.
package geographiclib

// #cgo CXXFLAGS: -std=c++14
// #cgo LDFLAGS: -lm
//
// #include "geographiclib.h"
import "C"

import "github.com/golang/geo/s2"

var (
	// WGS84Spheroid represents the default WGS84 ellipsoid.
	WGS84Spheroid = MakeSpheroid(6378137, 1/298.257223563)
)

// Spheroid is an object that can perform geodesic operations
// on a given spheroid.
type Spheroid struct {
	Radius     float64
	Flattening float64
}

// MakeSpheroid creates a spheroid from a radius and flattening.
func MakeSpheroid(radius float64, flattening float64) Spheroid {
	return Spheroid{Radius: radius, Flattening: flattening}
}

// Inverse solves the inverse geodesic problem on the given spheroid.
// Returns s12 (distance in meters), az1 (azimuth to point 1) and az2 (azimuth to point 2).
func (s Spheroid) Inverse(a, b s2.LatLng) (s12, az1, az2 float64) {
	var retS12, retAZ1, retAZ2 C.double
	C.CR_GEOGRAPHICLIB_Inverse(
		C.double(s.Radius),
		C.double(s.Flattening),
		C.double(a.Lat.Degrees()),
		C.double(a.Lng.Degrees()),
		C.double(b.Lat.Degrees()),
		C.double(b.Lng.Degrees()),
		&retS12,
		&retAZ1,
		&retAZ2,
	)
	return float64(retS12), float64(retAZ1), float64(retAZ2)
}
