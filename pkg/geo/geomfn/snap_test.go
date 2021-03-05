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

func TestSnap(t *testing.T) {
	testCases := []struct {
		desc      string
		input     geom.T
		target    geom.T
		tolerance float64
		expected  geom.T
	}{
		{
			desc:      "Test snap on two linestrings with tolerance of 0",
			input:     geom.NewLineStringFlat(geom.XY, []float64{20, 25, 30, 35}),
			target:    geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25}),
			tolerance: 0,
			expected:  geom.NewLineStringFlat(geom.XY, []float64{20, 25, 30, 35}),
		},
		{
			desc:      "Test snapping a polygon on a linestring with tolerance of 150",
			input:     geom.NewLineStringFlat(geom.XY, []float64{10, 55, 78, 84, 100, 200}),
			target:    geom.NewPolygonFlat(geom.XY, []float64{26, 125, 26, 200, 126, 200, 126, 125, 26, 125}, []int{10}),
			tolerance: 150,
			expected:  geom.NewLineStringFlat(geom.XY, []float64{10, 55, 26, 125, 26, 200, 126, 200, 126, 125}),
		},
		{
			desc:      "Test snapping a linestring on a polygon with tolerance of 150",
			target:    geom.NewLineStringFlat(geom.XY, []float64{10, 55, 78, 84, 100, 200}),
			input:     geom.NewPolygonFlat(geom.XY, []float64{26, 125, 26, 200, 126, 200, 126, 125, 26, 125}, []int{10}),
			tolerance: 150,
			expected:  geom.NewPolygonFlat(geom.XY, []float64{10, 55, 26, 200, 100, 200, 78, 84, 10, 55}, []int{10}),
		},
		{
			desc:      "Test snapping a linestring on a multipolygon with tolerance of 200",
			input:     geom.NewLineStringFlat(geom.XY, []float64{5, 107, 54, 84, 101, 100}),
			target:    geom.NewMultiPolygonFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 1, 1, 4, 4, 5, 5, 6, 6, 4, 4}, [][]int{{8}, {16}}),
			tolerance: 200,
			expected:  geom.NewLineStringFlat(geom.XY, []float64{5, 107, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 101, 100}),
		},
		{
			desc:      "Test snapping a multipolygon on a linestring with tolerance of 200",
			input:     geom.NewMultiPolygonFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 1, 1, 4, 4, 5, 5, 6, 6, 4, 4}, [][]int{{8}, {16}}),
			target:    geom.NewLineStringFlat(geom.XY, []float64{5, 107, 54, 84, 101, 100}),
			tolerance: 200,
			expected: geom.NewMultiPolygonFlat(
				geom.XY,
				[]float64{
					1, 1, 2, 2, 5, 107, 54, 84, 101, 100, 1, 1, 4, 4, 5, 5, 5, 107, 54, 84, 101, 100, 4, 4,
				},
				[][]int{{12}, {24}},
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			input, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)
			target, err := geo.MakeGeometryFromGeomT(tc.target)
			require.NoError(t, err)

			actual, err := Snap(input, target, tc.tolerance)
			require.NoError(t, err)

			requireGeometryWithinEpsilon(t, requireGeometryFromGeomT(t, tc.expected), actual, 1e-5)
		})
	}
}
