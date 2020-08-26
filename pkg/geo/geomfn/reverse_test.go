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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestReverse(t *testing.T) {
	testCases := []struct {
		desc     string
		input    geom.T
		expected geom.T
	}{
		{
			desc:     "reverse a line string",
			input:    geom.NewLineStringFlat(geom.XY, []float64{1, -1, 2, -2, 3, -3, 4, -4, 5, -5}),
			expected: geom.NewLineStringFlat(geom.XY, []float64{5, -5, 4, -4, 3, -3, 2, -2, 1, -1}),
		},
		{
			desc:     "reverse a polygon",
			input:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5, 5, 0, 10, 0, 0}, []int{8}),
			expected: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 0, 10, 5, 5, 0, 0}, []int{8}),
		},
		{
			desc:     "reverse multiple line strings",
			input:    geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, -1, -1, -2, -2}, []int{6, 10}),
			expected: geom.NewMultiLineStringFlat(geom.XY, []float64{3, 3, 2, 2, 1, 1, -2, -2, -1, -1}, []int{6, 10}),
		},
		{
			desc:     "reverse multiple polygons",
			input:    geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 5, 0, 4, 4, 0, 4, 0, 0, 1, 1, 2, 3, 2, 2, 1, 2, 1, 1}, [][]int{{10}, {20}}),
			expected: geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 0, 4, 4, 4, 5, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2, 3, 1, 1}, [][]int{{10}, {20}}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			got, err := Reverse(geometry)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.input.SRID(), got.SRID())
		})
	}

	collectionTestCases := []struct {
		input    *geom.GeometryCollection
		expected *geom.GeometryCollection
	}{
		{
			input: geom.NewGeometryCollection().MustPush(
				geom.NewLineStringFlat(geom.XY, []float64{1, -1, 2, -2}),
				geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5, 5, 0, 10, 0, 0}, []int{8}),
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
			),
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewLineStringFlat(geom.XY, []float64{2, -2, 1, -1}),
				geom.NewPolygonFlat(geom.XY, []float64{0, 0, 0, 10, 5, 5, 0, 0}, []int{8}),
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
			),
		},
	}

	for i, tc := range collectionTestCases {
		t.Run(fmt.Sprintf("collection-%d", i), func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			geometry, err = Reverse(geometry)
			require.NoError(t, err)

			g, err := geometry.AsGeomT()
			require.NoError(t, err)

			gotCollection, ok := g.(*geom.GeometryCollection)
			require.True(t, ok)

			require.Equal(t, tc.expected, gotCollection)
		})

	}

	noChangeTestCases := []geom.T{
		geom.NewPointFlat(geom.XY, []float64{1, 2}),
		geom.NewMultiPointFlat(geom.XY, []float64{1, 1, 2, 2}),
		geom.NewLineString(geom.XY),
		geom.NewPolygon(geom.XY),
		geom.NewGeometryCollection(),
	}

	for i, input := range noChangeTestCases {
		t.Run(fmt.Sprintf("no-change-%d", i), func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(input)
			require.NoError(t, err)

			got, err := Reverse(geometry)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(input)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, input.SRID(), got.SRID())
		})
	}
}
