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
	lat = math.Remainder(lat, 360)
	if lat > 90 {
		return 180 - lat
	}
	if lat < -90 {
		return -180 - lat
	}
	return lat
}

// NormalizeLongitudeDegrees normalizes lnggitude to the range [-180, 180].
func NormalizeLongitudeDegrees(lng float64) float64 {
	return math.Remainder(lng, 360)
}
