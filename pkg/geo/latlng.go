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

import "math"

// NormalizeLatitudeDegrees normalizes latitudes to the range [-90, 90].
func NormalizeLatitudeDegrees(lat float64) float64 {
	// math.Remainder(lat, 360) returns in the range [-180, 180].
	lat = math.Remainder(lat, 360)
	// If we are above 90 degrees, we curve back to 0, e.g. 91 -> 89, 100 -> 80.
	if lat > 90 {
		return 180 - lat
	}
	// If we are below 90 degrees, we curve back towards 0, e.g. -91 -> -89, -100 -> -80.
	if lat < -90 {
		return -180 - lat
	}
	return lat
}

// NormalizeLongitudeDegrees normalizes longitude to the range [-180, 180].
func NormalizeLongitudeDegrees(lng float64) float64 {
	// math.Remainder(lng, 360) returns in the range [-180, 180].
	return math.Remainder(lng, 360)
}
