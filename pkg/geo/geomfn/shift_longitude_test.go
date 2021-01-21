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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestShiftLongitude(t *testing.T) {
	testCases := []struct {
		desc     string
		input    geom.T
		expected geom.T
	}{
		{
			desc:     "shift longitude for a 2D point that has longitude within the range 0-180 on the smaller end",
			input:    geom.NewPointFlat(geom.XY, []float64{1, 60}),
			expected: geom.NewPointFlat(geom.XY, []float64{1, 60}),
		},
		{
			desc:     "shift longitude for a 2D point that has longitude within the range 0-180 on the larger end",
			input:    geom.NewPointFlat(geom.XY, []float64{179, 23}),
			expected: geom.NewPointFlat(geom.XY, []float64{179, 23}),
		},
		{
			desc:     "shift longitude for a 2D point that has longitude within the range 0-180 at the left boundary",
			input:    geom.NewPointFlat(geom.XY, []float64{0, 17}),
			expected: geom.NewPointFlat(geom.XY, []float64{0, 17}),
		},
		{
			desc:     "shift longitude for a 2D point that has longitude within the range 0-180 at the right boundary",
			input:    geom.NewPointFlat(geom.XY, []float64{180, -90}).SetSRID(4326),
			expected: geom.NewPointFlat(geom.XY, []float64{180, -90}).SetSRID(4326),
		},
		{
			desc:     "shift longitude for a 2D point that has longitude less than 0",
			input:    geom.NewPointFlat(geom.XY, []float64{-1, 60}).SetSRID(4326),
			expected: geom.NewPointFlat(geom.XY, []float64{359, 60}).SetSRID(4326),
		},
		{
			desc:     "shift longitude for a 2D point that has longitude greater than 180",
			input:    geom.NewPointFlat(geom.XY, []float64{181, 60}).SetSRID(26918),
			expected: geom.NewPointFlat(geom.XY, []float64{-179, 60}).SetSRID(26918),
		},
		{
			desc:     "shift longitude for a line string",
			input:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 24, 80, 190, 20, 5, 5, -5, 23}),
			expected: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 24, 80, -170, 20, 5, 5, 355, 23}),
		},
		{
			desc: "shift longitude for a polygon",
			input: geom.NewPolygonFlat(geom.XY,
				[]float64{-1, -1, 35, 35, 50, 2, -1, -1, 13, 3, 11, 1, 14, 2, 13, 3}, []int{8, 16}),
			expected: geom.NewPolygonFlat(geom.XY,
				[]float64{359, -1, 35, 35, 50, 2, 359, -1, 13, 3, 11, 1, 14, 2, 13, 3}, []int{8, 16}),
		},
		{
			desc:     "shift longitude for multiple 2D points",
			input:    geom.NewMultiPointFlat(geom.XY, []float64{1, 1, -40, 40}),
			expected: geom.NewMultiPointFlat(geom.XY, []float64{1, 1, 320, 40}),
		},
		{
			desc:     "shift longitude for multiple line strings",
			input:    geom.NewMultiLineStringFlat(geom.XY, []float64{3, 5, 56, 36, -2, 50, 200, 50}, []int{4, 8}),
			expected: geom.NewMultiLineStringFlat(geom.XY, []float64{3, 5, 56, 36, 358, 50, -160, 50}, []int{4, 8}),
		},
		{
			desc: "shift longitude for multiple polygons",
			input: geom.NewMultiPolygonFlat(geom.XY,
				[]float64{0, 90, 0, 0, 90, 0, 0, 90, 50, 50, 360, 23, 82, 49, 50, 50}, [][]int{{8}, {16}}),
			expected: geom.NewMultiPolygonFlat(geom.XY,
				[]float64{0, 90, 0, 0, 90, 0, 0, 90, 50, 50, 0, 23, 82, 49, 50, 50}, [][]int{{8}, {16}}),
		},
		{
			desc: "shift longitude for non-empty geometry collection",
			input: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{-1, 25}),
				geom.NewPointFlat(geom.XY, []float64{0, 0}),
				geom.NewLineStringFlat(geom.XY, []float64{-26, 85, 26, 75}),
				geom.NewPolygonFlat(geom.XY, []float64{5, 5, 270, 90, -118, -85, 5, 5}, []int{8}).SetSRID(4326),
			),
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{359, 25}),
				geom.NewPointFlat(geom.XY, []float64{0, 0}),
				geom.NewLineStringFlat(geom.XY, []float64{334, 85, 26, 75}),
				geom.NewPolygonFlat(geom.XY, []float64{5, 5, -90, 90, 242, -85, 5, 5}, []int{8}).SetSRID(4326),
			),
		},
		{
			desc:     "shift longitude for empty point",
			input:    geom.NewPointEmpty(geom.XY),
			expected: geom.NewPointEmpty(geom.XY),
		},
		{
			desc:     "shift longitude for empty line string",
			input:    geom.NewLineString(geom.XY),
			expected: geom.NewLineString(geom.XY),
		},
		{
			desc:     "shift longitude for empty polygon",
			input:    geom.NewPolygon(geom.XY),
			expected: geom.NewPolygon(geom.XY),
		},
		{
			desc:     "shift longitude for empty geometry collection",
			input:    geom.NewGeometryCollection(),
			expected: geom.NewGeometryCollection(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			got, err := ShiftLongitude(geometry)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.input.SRID(), got.SRID())
		})
	}
}
