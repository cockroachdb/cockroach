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
	"math"
	"strings"
)

// decodePolylinePoints decodes encoded Polyline according to the polyline algorithm: https://developers.google.com/maps/documentation/utilities/polylinealgorithm
func decodePolylinePoints(encoded string, precision int) []float64 {
	idx := 0
	latitude := float64(0)
	longitude := float64(0)
	bytes := []byte(encoded)
	results := []float64{}
	for idx < len(bytes) {
		var deltaLat float64
		idx, deltaLat = decodePointValue(idx, bytes)
		latitude += deltaLat

		var deltaLng float64
		idx, deltaLng = decodePointValue(idx, bytes)
		longitude += deltaLng
		results = append(results,
			longitude/math.Pow10(precision),
			latitude/math.Pow10(precision))
	}
	return results
}

func decodePointValue(idx int, bytes []byte) (int, float64) {
	res := int32(0)
	shift := 0
	for byte := byte(0x20); byte >= 0x20; {
		if idx > len(bytes)-1 {
			return idx, 0
		}
		byte = bytes[idx] - 63
		idx++
		res |= int32(byte&0x1F) << shift
		shift += 5
	}
	var pointValue float64
	if (res & 1) == 1 {
		pointValue = float64(^(res >> 1))
	} else {
		pointValue = float64(res >> 1)
	}
	return idx, pointValue
}

// encodePolylinePoints encodes provided points using the algorithm: https://developers.google.com/maps/documentation/utilities/polylinealgorithm
// Assumes there are no malformed points - length of the input slice should be even.
func encodePolylinePoints(points []float64, precision int) string {
	lastLat := 0
	lastLng := 0
	var res strings.Builder
	for i := 1; i < len(points); i += 2 {
		lat := int(math.Round(points[i-1] * math.Pow10(precision)))
		lng := int(math.Round(points[i] * math.Pow10(precision)))
		res = encodePointValue(lng-lastLng, res)
		res = encodePointValue(lat-lastLat, res)
		lastLat = lat
		lastLng = lng
	}

	return res.String()
}

func encodePointValue(diff int, b strings.Builder) strings.Builder {
	var shifted int
	shifted = diff << 1
	if diff < 0 {
		shifted = ^shifted
	}
	rem := shifted
	for rem >= 0x20 {
		b.WriteRune(rune(0x20 | (rem & 0x1f) + 63))

		rem = rem >> 5
	}

	b.WriteRune(rune(rem + 63))
	return b
}
