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

func TestAddMeasure(t *testing.T) {
	testCases := []struct {
		desc     string
		input    geom.T
		start    float64
		end      float64
		expected geom.T
	}{
		{
			desc:     "add measure to 2D linestring",
			input:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}).SetSRID(4326),
			start:    0,
			end:      1,
			expected: geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 0, 1, 1, 0.5, 2, 2, 1}).SetSRID(4326),
		},
		{
			desc:     "add measure to 2D linestring with start larger than end",
			input:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}).SetSRID(26918),
			start:    3,
			end:      1,
			expected: geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 3, 1, 1, 2, 2, 2, 1}).SetSRID(26918),
		},
		{
			desc:     "add measure to zero length 2D linestring",
			input:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 0, 0, 0}),
			start:    0,
			end:      1,
			expected: geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 0, 0, 0, 0.5, 0, 0, 1}),
		},
		{
			desc:     "add measure to 2D linestring with zero length segment",
			input:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 1, 1, 1, 1, 2, 2}),
			start:    0,
			end:      10,
			expected: geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 0, 1, 1, 5, 1, 1, 5, 1, 1, 5, 2, 2, 10}),
		},
		{
			desc:     "add measure to 2D+M linestring",
			input:    geom.NewLineStringFlat(geom.XYM, []float64{0, 0, -25, 1, 1, -50, 2, 2, 0}),
			start:    1,
			end:      2,
			expected: geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 1, 1, 1, 1.5, 2, 2, 2}),
		},
		{
			desc:     "add measure to 2D+M linestring with same start and end",
			input:    geom.NewLineStringFlat(geom.XYM, []float64{0, 0, -25, 1, 1, -50, 2, 2, 0}),
			start:    100,
			end:      100,
			expected: geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 100, 1, 1, 100, 2, 2, 100}),
		},
		{
			desc:     "add measure to 3D linestring",
			input:    geom.NewLineStringFlat(geom.XYZ, []float64{0, 0, -25, 1, 1, -50, 2, 2, 0}),
			start:    5,
			end:      7,
			expected: geom.NewLineStringFlat(geom.XYZM, []float64{0, 0, -25, 5, 1, 1, -50, 6, 2, 2, 0, 7}),
		},
		{
			desc:     "add measure to 4D linestring",
			input:    geom.NewLineStringFlat(geom.XYZM, []float64{0, 0, -25, -25, 1, 1, -50, -50, 2, 2, 0, 0}),
			start:    5,
			end:      7,
			expected: geom.NewLineStringFlat(geom.XYZM, []float64{0, 0, -25, 5, 1, 1, -50, 6, 2, 2, 0, 7}),
		},
		{
			desc:     "add measure to empty 2D linestring",
			input:    geom.NewLineString(geom.XY).SetSRID(4326),
			start:    0,
			end:      1,
			expected: geom.NewLineString(geom.XYM).SetSRID(4326),
		},
		{
			desc:     "add measure to empty 2D+M linestring",
			input:    geom.NewLineString(geom.XYM),
			start:    0,
			end:      1,
			expected: geom.NewLineString(geom.XYM),
		},
		{
			desc:     "add measure to empty 3D linestring",
			input:    geom.NewLineString(geom.XYZ),
			start:    0,
			end:      1,
			expected: geom.NewLineString(geom.XYZM),
		},
		{
			desc:     "add measure to empty 4D linestring",
			input:    geom.NewLineString(geom.XYZM),
			start:    0,
			end:      1,
			expected: geom.NewLineString(geom.XYZM),
		},
		{
			desc:     "add measure to 2D multilinestring",
			input:    geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1}, []int{4}),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineStringFlat(geom.XYM, []float64{0, 0, 0, 1, 1, 1}, []int{6}),
		},
		{
			desc:     "add measure to 2D multilinestring with empty",
			input:    geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1}, []int{4, 4}),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineStringFlat(geom.XYM, []float64{0, 0, 0, 1, 1, 1}, []int{6, 6}),
		},
		{
			desc:  "add measure to 2D multilinestring with multiple linestrings",
			input: geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 0, 1, 0, 0, 0}, []int{4, 10}),
			start: 6,
			end:   0,
			expected: geom.NewMultiLineStringFlat(
				geom.XYM, []float64{0, 0, 6, 1, 1, 0, 2, 0, 6, 1, 0, 3, 0, 0, 0}, []int{6, 15}),
		},
		{
			desc:     "add measure to 2D+M multilinestring",
			input:    geom.NewMultiLineStringFlat(geom.XYM, []float64{0, 0, -5, 1, 1, 5}, []int{6}),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineStringFlat(geom.XYM, []float64{0, 0, 0, 1, 1, 1}, []int{6}),
		},
		{
			desc:     "add measure to 3D multilinestring",
			input:    geom.NewMultiLineStringFlat(geom.XYZ, []float64{0, 0, -5, 1, 1, 5}, []int{6}),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineStringFlat(geom.XYZM, []float64{0, 0, -5, 0, 1, 1, 5, 1}, []int{8}),
		},
		{
			desc:     "add measure to 4D multilinestring",
			input:    geom.NewMultiLineStringFlat(geom.XYZM, []float64{0, 0, -5, 23, 1, 1, 5, -23}, []int{8}),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineStringFlat(geom.XYZM, []float64{0, 0, -5, 0, 1, 1, 5, 1}, []int{8}),
		},
		{
			desc:     "add measure to empty 2D multilinestring",
			input:    geom.NewMultiLineString(geom.XY),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineString(geom.XYM),
		},
		{
			desc:     "add measure to empty 2D+M multilinestring",
			input:    geom.NewMultiLineString(geom.XYM),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineString(geom.XYM),
		},
		{
			desc:     "add measure to empty 3D multilinestring",
			input:    geom.NewMultiLineString(geom.XYZ),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineString(geom.XYZM),
		},
		{
			desc:     "add measure to empty 4D multilinestring",
			input:    geom.NewMultiLineString(geom.XYZM),
			start:    0,
			end:      1,
			expected: geom.NewMultiLineString(geom.XYZM),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			got, err := AddMeasure(geometry, tc.start, tc.end)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
		})
	}
}

func TestAddMeasureError(t *testing.T) {
	errorTestCases := []struct {
		geomType string
		input    geom.T
	}{
		{
			geomType: "point",
			input:    geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			geomType: "polygon",
			input:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, []int{8}),
		},
		{
			geomType: "multipoint",
			input:    geom.NewMultiPointFlat(geom.XY, []float64{0, 0, 1, 1}),
		},
		{
			geomType: "multipolygon",
			input:    geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, [][]int{{8}}),
		},
		{
			geomType: "geometrycollection",
			input:    geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1})),
		},
	}

	for _, tc := range errorTestCases {
		testName := "invalid attempt to add measure to a " + tc.geomType
		t.Run(testName, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			_, err = AddMeasure(geometry, 0, 1)
			require.EqualError(t, err, "input geometry must be LINESTRING or MULTILINESTRING")
		})
	}
}
