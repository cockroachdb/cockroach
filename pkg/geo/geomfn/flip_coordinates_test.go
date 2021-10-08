// Copyright 2020 The Cockroach Authors.
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

func TestFlipCoordinates(t *testing.T) {
	testCases := []struct {
		desc     string
		input    geom.T
		expected geom.T
	}{
		{
			desc:     "flip coordinates of a 2D point with same X Y values",
			input:    geom.NewPointFlat(geom.XY, []float64{10.1, 10.1}),
			expected: geom.NewPointFlat(geom.XY, []float64{10.1, 10.1}),
		},
		{
			desc:     "flip coordinates of a 2D point with different coordinates",
			input:    geom.NewPointFlat(geom.XY, []float64{1, 2}),
			expected: geom.NewPointFlat(geom.XY, []float64{2, 1}),
		},
		{
			desc:     "flip coordinates of a line string",
			input:    geom.NewLineStringFlat(geom.XY, []float64{1, 2, 3, 4}),
			expected: geom.NewLineStringFlat(geom.XY, []float64{2, 1, 4, 3}),
		},
		{
			desc:     "flip coordinates of a line string with same X Y values",
			input:    geom.NewLineStringFlat(geom.XY, []float64{1.1, 1.1, 2.2, 2.2}),
			expected: geom.NewLineStringFlat(geom.XY, []float64{1.1, 1.1, 2.2, 2.2}),
		},
		{
			desc:     "flip coordinates of a polygon",
			input:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5.55, 5.55, 0, 10, 0, 0}, []int{8}),
			expected: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5.55, 5.55, 10, 0, 0, 0}, []int{8}),
		},
		{
			desc:     "flip coordinates of a multi polygon",
			input:    geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 5, 0, 4, 4, 0, 4, 0, 0, 1, 1, 2, 3, 2, 2, 1, 2, 1, 1}, [][]int{{10}, {20}}),
			expected: geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 0, 5, 4, 4, 4, 0, 0, 0, 1, 1, 3, 2, 2, 2, 2, 1, 1, 1}, [][]int{{10}, {20}}),
		},
		{
			desc:     "flip coordinates of multiple 2D points",
			input:    geom.NewMultiPointFlat(geom.XY, []float64{5, 10, -30.5, 40.2, 1, 1}),
			expected: geom.NewMultiPointFlat(geom.XY, []float64{10, 5, 40.2, -30.5, 1, 1}),
		},
		{
			desc:     "flip coordinates of multiple line strings",
			input:    geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2.2, 2.2, 3, 3, 4, 4}, []int{4, 8}),
			expected: geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2.2, 2.2, 3, 3, 4, 4}, []int{4, 8}),
		},
		{
			desc:     "flip coordinates of an empty line string",
			input:    geom.NewLineString(geom.XY),
			expected: geom.NewLineString(geom.XY),
		},
		{
			desc:     "flip coordinates of an empty polygon",
			input:    geom.NewPolygon(geom.XY),
			expected: geom.NewPolygon(geom.XY),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)
			got, err := FlipCoordinates(geometry)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.input.SRID(), got.SRID())
		})
	}

	collectionTestCases := []struct {
		desc     string
		input    *geom.GeometryCollection
		expected *geom.GeometryCollection
	}{
		{
			desc: "flip coordinates of a non-empty collection",
			input: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1.1, 2}),
				geom.NewPointFlat(geom.XY, []float64{3.33, 4}),
				geom.NewPolygonFlat(geom.XY, []float64{150, 85, -30, 85, -20, 85, 160, 85, 150, 85}, []int{10}),
			),
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{2, 1.1}),
				geom.NewPointFlat(geom.XY, []float64{4, 3.33}),
				geom.NewPolygonFlat(geom.XY, []float64{85, 150, 85, -30, 85, -20, 85, 160, 85, 150}, []int{10})),
		},
		{
			desc:     "flip coordinates of an empty collection",
			input:    geom.NewGeometryCollection().MustSetLayout(geom.XY),
			expected: geom.NewGeometryCollection().MustSetLayout(geom.XY),
		},
	}

	for _, tc := range collectionTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			geometry, err = FlipCoordinates(geometry)
			require.NoError(t, err)

			g, err := geometry.AsGeomT()
			require.NoError(t, err)

			gotCollection, ok := g.(*geom.GeometryCollection)
			require.True(t, ok)

			require.Equal(t, tc.expected, gotCollection)
		})
	}
}
