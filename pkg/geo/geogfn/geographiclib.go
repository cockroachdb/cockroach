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
	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/golang/geo/s2"
)

// spheroidDistance returns the s12 (meter) component of spheroid.Inverse from s2 Points.
func spheroidDistance(s *geographiclib.Spheroid, a s2.Point, b s2.Point) float64 {
	inv, _, _ := s.Inverse(s2.LatLngFromPoint(a), s2.LatLngFromPoint(b))
	return inv
}
