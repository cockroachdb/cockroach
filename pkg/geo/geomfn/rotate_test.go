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
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestRotateGeom(t *testing.T) {
	testCases := []struct {
		desc       string
		input      geom.T
		rotRadians float64
		expected   []float64
	}{
		{
			desc:       "rotate a 2D point where angle is 2Pi",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: 2 * math.Pi,
			expected:   []float64{10, 10},
		},
		{
			desc:       "rotate a 2D point where angle is Pi",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: math.Pi,
			expected:   []float64{-10, -10},
		},
		{
			desc:       "rotate a 2D point where angle is Pi/4",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: 0.785,
			expected:   []float64{0.00563088, 14.1421345},
		},
		{
			desc:       "rotate a line string",
			input:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25}),
			rotRadians: 0.5,
			expected:   []float64{1.58444, 17.95799, 5.56601, 31.52807},
		},
		{
			desc:       "rotate a line polygon",
			input:      geom.NewPolygonFlat(geom.XY, []float64{1, 1, 5, 5, 1, 10, 1, 1}, []int{8}),
			rotRadians: math.Pi,
			expected:   []float64{-1, -1, -5, -5, -1, -10, -1, -1},
		},
		{
			desc:       "rotate multiple 2D points",
			input:      geom.NewMultiPointFlat(geom.XY, []float64{10, 10, 20, 20}),
			rotRadians: math.Pi / 2,
			expected:   []float64{-10, 10, -20, 20},
		},
		{
			desc:       "rotate multiple line strings",
			input:      geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 4, 4}, []int{4, 8}),
			rotRadians: (3 * math.Pi) / 2,
			expected:   []float64{1, -1, 2, -2, 3, -3, 4, -4},
		},
		{
			desc:       "rotate multiple polygon strings",
			input:      geom.NewMultiPolygonFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 1, 1, 4, 4, 5, 5, 6, 6, 4, 4}, [][]int{{8}, {16}}),
			rotRadians: math.Pi,
			expected:   []float64{-1, -1, -2, -2, -3, -3, -1, -1, -4, -4, -5, -5, -6, -6, -4, -4},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := rotateGeomT(tc.input, tc.rotRadians)
			require.NoError(t, err)

			require.InEpsilonSlice(t, tc.expected, actual.FlatCoords(), 0.00001)
			require.EqualValues(t, reflect.TypeOf(tc.input), reflect.TypeOf(actual))
			require.EqualValues(t, tc.input.SRID(), actual.SRID())
		})
	}
}

func TestRotateCollection(t *testing.T) {
	testCases := []struct {
		desc       string
		input      *geom.GeometryCollection
		rotRadians float64
		expected   *geom.GeometryCollection
	}{
		{
			desc:       "rotate empty collection",
			input:      geom.NewGeometryCollection(),
			rotRadians: math.Pi,
			expected:   geom.NewGeometryCollection(),
		},
		{
			desc: "rotate non-empty collection",
			input: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1, 1}),
				geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
				geom.NewMultiPointFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3}),
			),
			rotRadians: math.Pi,
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{-1, -1}),
				geom.NewLineStringFlat(geom.XY, []float64{-1, -1, -2, -2}),
				geom.NewMultiPointFlat(geom.XY, []float64{-1, -1, -2, -2, -3, -3}),
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			geometry, err = Rotate(geometry, tc.rotRadians)
			require.NoError(t, err)

			g, err := geometry.AsGeomT()
			require.NoError(t, err)

			_, ok := g.(*geom.GeometryCollection)
			require.True(t, ok)
		})
	}
}

func TestRotate(t *testing.T) {
	testCases := []struct {
		desc       string
		input      geom.T
		rotRadians float64
		expected   geom.T
	}{
		{
			desc:       "rotate a 2D point where angle is 2Pi",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: 2 * math.Pi,
			expected:   geom.NewPointFlat(geom.XY, []float64{10, 10}),
		},
		{
			desc:       "rotate a 2D point where angle is Pi",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: math.Pi,
			expected:   geom.NewPointFlat(geom.XY, []float64{-10, -10}),
		},
		{
			desc:       "rotate a 2D point where angle is Pi/4",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: 0.785,
			expected:   geom.NewPointFlat(geom.XY, []float64{0.00563088, 14.1421345}),
		},
		{
			desc:       "rotate a line string",
			input:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25}),
			rotRadians: 0.5,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1.58444, 17.95799, 5.56601, 31.52807}),
		},
		{
			desc:       "rotate a line polygon",
			input:      geom.NewPolygonFlat(geom.XY, []float64{1, 1, 5, 5, 1, 10, 1, 1}, []int{8}),
			rotRadians: math.Pi,
			expected:   geom.NewPolygonFlat(geom.XY, []float64{-1, -1, -5, -5, -1, -10, -1, -1}, []int{8}),
		},
		{
			desc:       "rotate multiple 2D points",
			input:      geom.NewMultiPointFlat(geom.XY, []float64{10, 10, 20, 20}),
			rotRadians: math.Pi / 2,
			expected:   geom.NewMultiPointFlat(geom.XY, []float64{-10, 10, -20, 20}),
		},
		{
			desc:       "rotate multiple line strings",
			input:      geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 4, 4}, []int{4, 8}),
			rotRadians: (3 * math.Pi) / 2,
			expected:   geom.NewMultiLineStringFlat(geom.XY, []float64{1, -1, 2, -2, 3, -3, 4, -4}, []int{4, 8}),
		},
		{
			desc:       "rotate multiple polygon strings",
			input:      geom.NewMultiPolygonFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 1, 1, 4, 4, 5, 5, 6, 6, 4, 4}, [][]int{{8}, {16}}),
			rotRadians: math.Pi,
			expected:   geom.NewMultiPolygonFlat(geom.XY, []float64{-1, -1, -2, -2, -3, -3, -1, -1, -4, -4, -5, -5, -6, -6, -4, -4}, [][]int{{8}, {16}}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			actual, err := Rotate(geometry, tc.rotRadians)
			require.NoError(t, err)

			_, err = geo.MakeGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.EqualValues(t, tc.input.SRID(), actual.SRID())
		})
	}
}
