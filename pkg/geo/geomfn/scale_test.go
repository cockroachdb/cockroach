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

func TestScale(t *testing.T) {
	testCases := []struct {
		desc     string
		input    geom.T
		factors  []float64
		expected geom.T
	}{
		{
			desc:     "scale a 2D point",
			input:    geom.NewPointFlat(geom.XY, []float64{10, 10}),
			factors:  []float64{.75, 2.8},
			expected: geom.NewPointFlat(geom.XY, []float64{7.5, 28.0}),
		},
		{
			desc:     "scale a line string",
			input:    geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			factors:  []float64{.1, .5},
			expected: geom.NewLineStringFlat(geom.XY, []float64{.1, .5, .2, 1.0}),
		},
		{
			desc:     "scale a polygon",
			input:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5, 5, 0, 10, 0, 0}, []int{8}),
			factors:  []float64{.75, 1.5},
			expected: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 3.75, 7.5, 0, 15, 0, 0}, []int{8}),
		},
		{
			desc:     "scale multiple 2D points",
			input:    geom.NewMultiPointFlat(geom.XY, []float64{5, 10, -30, 40}),
			factors:  []float64{.8, 5},
			expected: geom.NewMultiPointFlat(geom.XY, []float64{4, 50, -24, 200}),
		},
		{
			desc:     "scale multiple line strings",
			input:    geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 4, 4}, []int{4, 8}),
			factors:  []float64{.5, 0},
			expected: geom.NewMultiLineStringFlat(geom.XY, []float64{.5, 0, 1, 0, 1.5, 0, 2, 0}, []int{4, 8}),
		},
		{
			desc:     "scale an empty line string",
			input:    geom.NewLineString(geom.XY),
			factors:  []float64{.5, .5},
			expected: geom.NewLineString(geom.XY),
		},
		{
			desc:     "scale an empty polygon",
			input:    geom.NewPolygon(geom.XY),
			factors:  []float64{1.5, 1.5},
			expected: geom.NewPolygon(geom.XY),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.NewGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			got, err := Scale(geometry, tc.factors)
			require.NoError(t, err)

			want, err := geo.NewGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.input.SRID(), got.SRID())
		})
	}
}

func TestScaleRelativeToOrigin(t *testing.T) {
	testCases := []struct {
		desc     string
		input    geom.T
		factor   geom.T
		origin   geom.T
		expected geom.T
	}{
		{
			desc:     "scale a 2D point",
			input:    geom.NewPointFlat(geom.XY, []float64{10, 10}),
			factor:   geom.NewPointFlat(geom.XY, []float64{.75, 2.8}),
			origin:   geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected: geom.NewPointFlat(geom.XY, []float64{6.5, 27.0}),
		},
		{
			desc:     "scale a line string",
			input:    geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			factor:   geom.NewPointFlat(geom.XY, []float64{2, 2}),
			origin:   geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 3, 3}),
		},
		{
			desc:     "scale a polygon",
			input:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5, 5, 0, 10, 0, 0}, []int{8}),
			factor:   geom.NewPointFlat(geom.XY, []float64{2, 2}),
			origin:   geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected: geom.NewPolygonFlat(geom.XY, []float64{-1, -1, 9, 9, -1, 19, -1, -1}, []int{8}),
		},
		{
			desc:     "scale an empty 2D point",
			input:    geom.NewPointEmpty(geom.XY),
			factor:   geom.NewPointFlat(geom.XY, []float64{.5, .5}),
			origin:   geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected: geom.NewPointEmpty(geom.XY),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.NewGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			factor, err := geo.NewGeometryFromGeomT(tc.factor)
			require.NoError(t, err)

			origin, err := geo.NewGeometryFromGeomT(tc.origin)
			require.NoError(t, err)

			got, err := ScaleRelativeToOrigin(geometry, factor, origin)
			require.NoError(t, err)

			want, err := geo.NewGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.input.SRID(), got.SRID())
		})
	}
}

func TestCollectionScaleRelativeToOrigin(t *testing.T) {
	testCases := []struct {
		desc     string
		input    *geom.GeometryCollection
		factor   geom.T
		origin   geom.T
		expected *geom.GeometryCollection
	}{
		{
			desc: "scale non-empty collection",
			input: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{-2, 3}),
			),
			factor: geom.NewPointFlat(geom.XY, []float64{2, 2}),
			origin: geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1, 3}),
				geom.NewPointFlat(geom.XY, []float64{-5, 5}),
			),
		},
		{
			desc:     "scale empty collection",
			input:    geom.NewGeometryCollection(),
			factor:   geom.NewPointFlat(geom.XY, []float64{2, 2}),
			origin:   geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected: geom.NewGeometryCollection(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.NewGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			factor, err := geo.NewGeometryFromGeomT(tc.factor)
			require.NoError(t, err)

			origin, err := geo.NewGeometryFromGeomT(tc.origin)
			require.NoError(t, err)

			geometry, err = ScaleRelativeToOrigin(geometry, factor, origin)
			require.NoError(t, err)

			g, err := geometry.AsGeomT()
			require.NoError(t, err)

			gotCollection, ok := g.(*geom.GeometryCollection)
			require.True(t, ok)

			require.Equal(t, tc.expected, gotCollection)
		})
	}
}

func TestInvalidParametersWhenScalingRelativeToOrigin(t *testing.T) {
	testCases := []struct {
		desc        string
		input       geom.T
		factor      geom.T
		origin      geom.T
		expectedErr string
	}{
		{
			desc:        "scale using an invalid geometry as factor",
			input:       geom.NewPointFlat(geom.XY, []float64{10, 10}),
			factor:      geom.NewLineStringFlat(geom.XY, []float64{1, 1, 1, 1}),
			origin:      geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expectedErr: "the scaling factor must be a Point",
		},
		{
			desc:        "scale using an invalid geometry as origin",
			input:       geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			factor:      geom.NewPointFlat(geom.XY, []float64{2, 2}),
			origin:      geom.NewLineStringFlat(geom.XY, []float64{1, 1, 1, 1}),
			expectedErr: "the false origin must be a Point",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.NewGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			factor, err := geo.NewGeometryFromGeomT(tc.factor)
			require.NoError(t, err)

			origin, err := geo.NewGeometryFromGeomT(tc.origin)
			require.NoError(t, err)

			_, err = ScaleRelativeToOrigin(geometry, factor, origin)
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}
