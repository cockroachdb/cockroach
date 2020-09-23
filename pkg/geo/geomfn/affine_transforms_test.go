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
	"errors"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestTranslate(t *testing.T) {
	testCases := []struct {
		desc     string
		input    geom.T
		deltas   []float64
		expected geom.T
	}{
		{
			desc:     "translate a 2D point",
			input:    geom.NewPointFlat(geom.XY, []float64{10, 10}),
			deltas:   []float64{5, -5},
			expected: geom.NewPointFlat(geom.XY, []float64{15, 5}),
		},
		{
			desc:     "translate a line string",
			input:    geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 30}),
			deltas:   []float64{1, 5},
			expected: geom.NewLineStringFlat(geom.XY, []float64{11, 20, 21, 35}),
		},
		{
			desc:     "translate a polygon",
			input:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5, 5, 0, 10, 0, 0}, []int{8}),
			deltas:   []float64{1, 1},
			expected: geom.NewPolygonFlat(geom.XY, []float64{1, 1, 6, 6, 1, 11, 1, 1}, []int{8}),
		},
		{
			desc:     "translate multiple 2D points",
			input:    geom.NewMultiPointFlat(geom.XY, []float64{5, 10, -30, 40}),
			deltas:   []float64{1, 0.5},
			expected: geom.NewMultiPointFlat(geom.XY, []float64{6, 10.5, -29, 40.5}),
		},
		{
			desc:     "translate multiple line strings",
			input:    geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 4, 4}, []int{4, 8}),
			deltas:   []float64{.1, .1},
			expected: geom.NewMultiLineStringFlat(geom.XY, []float64{1.1, 1.1, 2.1, 2.1, 3.1, 3.1, 4.1, 4.1}, []int{4, 8}),
		},
		{
			desc:     "translate empty line string",
			input:    geom.NewLineString(geom.XY),
			deltas:   []float64{1, 1},
			expected: geom.NewLineString(geom.XY),
		},
		{
			desc:     "translate empty polygon",
			input:    geom.NewPolygon(geom.XY),
			deltas:   []float64{1, 1},
			expected: geom.NewPolygon(geom.XY),
		},
		{
			desc: "translate non-empty collection",
			input: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{10, 10}),
				geom.NewLineStringFlat(geom.XY, []float64{0, 0, 5, 5}),
				geom.NewMultiPointFlat(geom.XY, []float64{0, 0, -5, -5, -10, -10}),
			),
			deltas: []float64{1, -1},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{11, 9}),
				geom.NewLineStringFlat(geom.XY, []float64{1, -1, 6, 4}),
				geom.NewMultiPointFlat(geom.XY, []float64{1, -1, -4, -6, -9, -11}),
			),
		},
		{
			desc:     "translate empty collection",
			input:    geom.NewGeometryCollection(),
			deltas:   []float64{1, 1},
			expected: geom.NewGeometryCollection(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			got, err := Translate(geometry, tc.deltas)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.input.SRID(), got.SRID())
		})
	}

	t.Run("error if mismatching deltas", func(t *testing.T) {
		deltas := []float64{1, 1, 1}
		geometry, err := geo.MakeGeometryFromPointCoords(0, 0)
		require.NoError(t, err)

		_, err = Translate(geometry, deltas)
		isErr := errors.Is(err, geom.ErrStrideMismatch{
			Got:  3,
			Want: 2,
		})
		require.True(t, isErr)
	})
}

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
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			got, err := Scale(geometry, tc.factors)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
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
			expected: geom.NewPointFlat(geom.XY, []float64{7.75, 26.2}),
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
		{
			desc:     "scale using an empty point as factor",
			input:    geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			factor:   geom.NewPointEmpty(geom.XY),
			origin:   geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 1, 1}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			factor, err := geo.MakeGeometryFromGeomT(tc.factor)
			require.NoError(t, err)

			origin, err := geo.MakeGeometryFromGeomT(tc.origin)
			require.NoError(t, err)

			got, err := ScaleRelativeToOrigin(geometry, factor, origin)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
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
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			factor, err := geo.MakeGeometryFromGeomT(tc.factor)
			require.NoError(t, err)

			origin, err := geo.MakeGeometryFromGeomT(tc.origin)
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

	errorTestCases := []struct {
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
		{
			desc:        "scale using an empty point as origin",
			input:       geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			factor:      geom.NewPointFlat(geom.XY, []float64{2, 2}),
			origin:      geom.NewPointEmpty(geom.XY),
			expectedErr: "the origin must have at least 2 coordinates",
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			factor, err := geo.MakeGeometryFromGeomT(tc.factor)
			require.NoError(t, err)

			origin, err := geo.MakeGeometryFromGeomT(tc.origin)
			require.NoError(t, err)

			_, err = ScaleRelativeToOrigin(geometry, factor, origin)
			require.EqualError(t, err, tc.expectedErr)
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

			// Compare FlatCoords and assert they are within epsilon.
			// This is because they exact matches may encounter rounding issues.
			actualGeomT, err := actual.AsGeomT()
			require.NoError(t, err)
			require.Equal(t, tc.expected.SRID(), actualGeomT.SRID())
			require.Equal(t, tc.expected.Layout(), actualGeomT.Layout())
			require.IsType(t, tc.expected, actualGeomT)
			require.InEpsilonSlice(t, tc.expected.FlatCoords(), actualGeomT.FlatCoords(), 0.00001)
		})
	}
}
