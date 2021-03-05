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
			input:    geom.NewGeometryCollection().MustSetLayout(geom.XY),
			factor:   geom.NewPointFlat(geom.XY, []float64{2, 2}),
			origin:   geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected: geom.NewGeometryCollection().MustSetLayout(geom.XY),
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

			requireGeometryWithinEpsilon(t, requireGeometryFromGeomT(t, tc.expected), actual, 1e-5)
		})
	}
}

func TestRotateWithPointOrigin(t *testing.T) {
	tests := []struct {
		desc            string
		inputGeom       geom.T
		inputRotRadians float64
		inputPointGeom  geom.T
		wantGeom        geom.T
		wantErrStr      string
	}{
		{
			desc:            "rotate a 2D point where angle is 2Pi, the input geom is 10 10 and point geom is 5 5",
			inputGeom:       geom.NewPointFlat(geom.XY, []float64{10, 10}),
			inputRotRadians: 2 * math.Pi,
			inputPointGeom:  geom.NewPointFlat(geom.XY, []float64{5, 5}),
			wantGeom:        geom.NewPointFlat(geom.XY, []float64{10, 10}),
		},
		{
			desc:            "rotate a 2D point where angle is Pi, the input geom is 10 9 and the point geom is 7 5",
			inputGeom:       geom.NewPointFlat(geom.XY, []float64{10, 9}),
			inputRotRadians: math.Pi,
			inputPointGeom:  geom.NewPointFlat(geom.XY, []float64{7, 5}),
			wantGeom:        geom.NewPointFlat(geom.XY, []float64{4, 1}),
		},
		{
			desc:            "rotate a 2D point where angle is Pi, the input geom is 10.11 10.55 and the point geom is 5 5",
			inputGeom:       geom.NewPointFlat(geom.XY, []float64{10.11, 10.55}),
			inputRotRadians: math.Pi,
			inputPointGeom:  geom.NewPointFlat(geom.XY, []float64{5, 5}),
			wantGeom:        geom.NewPointFlat(geom.XY, []float64{-0.11, -0.55}),
		},
		{
			desc:            "rotate a 2D line string where angle is Pi, the input geom is 1 5, 5 1 and the point geom is 5 5",
			inputGeom:       geom.NewLineStringFlat(geom.XY, []float64{1, 5, 5, 1}),
			inputRotRadians: math.Pi,
			inputPointGeom:  geom.NewPointFlat(geom.XY, []float64{5, 5}),
			wantGeom:        geom.NewLineStringFlat(geom.XY, []float64{9, 5, 5, 9}),
		},
		{
			desc:            "rotate a 2D line string where angle is Pi/4, the input geom is 1 5, 5 1 and the point geom is 5 5",
			inputGeom:       geom.NewLineStringFlat(geom.XY, []float64{1, 5, 5, 1}),
			inputRotRadians: math.Pi / 4,
			inputPointGeom:  geom.NewPointFlat(geom.XY, []float64{5, 5}),
			wantGeom:        geom.NewLineStringFlat(geom.XY, []float64{2.17157287525381, 2.17157287525381, 7.82842712474619, 2.17157287525381}),
		},
		{
			desc:            "rotate a 2D line string where angle is 2Pi, the input geom is 1 5, 5 1 and the point geom is 4 8",
			inputGeom:       geom.NewLineStringFlat(geom.XY, []float64{1, 5, 5, 1}),
			inputRotRadians: 2 * math.Pi,
			inputPointGeom:  geom.NewPointFlat(geom.XY, []float64{4, 8}),
			wantGeom:        geom.NewLineStringFlat(geom.XY, []float64{1, 5, 5, 1}),
		},
		{
			desc:            "rotate multiple 2D points",
			inputGeom:       geom.NewMultiPointFlat(geom.XY, []float64{10, 10, 20, 20}),
			inputRotRadians: math.Pi / 2,
			inputPointGeom:  geom.NewPointFlat(geom.XY, []float64{10.55, 8.88}),
			wantGeom:        geom.NewMultiPointFlat(geom.XY, []float64{9.43, 8.33, -0.57, 18.33}),
		},
		{
			desc:            "rotate polygon strings",
			inputGeom:       geom.NewPolygonFlat(geom.XY, []float64{0, 0, 4, 0, 4, 4, 0, 4, 0, 0, 1, 1, 2, 1, 2, 2, 1, 2, 1, 1}, []int{10, 20}),
			inputRotRadians: math.Pi,
			inputPointGeom:  geom.NewPointFlat(geom.XY, []float64{9, 9}),
			wantGeom:        geom.NewPolygonFlat(geom.XY, []float64{18, 18, 14, 18, 14, 14, 18, 14, 18, 18, 17, 17, 16, 17, 16, 16, 17, 16, 17, 17}, []int{10, 20}),
		},
		{
			desc:            "rotate a 2D point where angle is Pi, the input geom is 10 9 and the line string geom is 7 5, 5 5",
			inputGeom:       geom.NewPointFlat(geom.XY, []float64{10, 9}),
			inputRotRadians: math.Pi,
			inputPointGeom:  geom.NewLineStringFlat(geom.XY, []float64{7, 5, 5, 5}),
			wantGeom:        geom.NewPointFlat(geom.XY, []float64{10, 9}),
			wantErrStr:      "origin is not a POINT",
		},
		{
			desc:            "rotate a 2D point where point origin is empty",
			inputGeom:       geom.NewPointFlat(geom.XY, []float64{10, 9}),
			inputRotRadians: math.Pi,
			inputPointGeom:  geom.NewPointEmpty(geom.XY),
			wantGeom:        geom.NewPointFlat(geom.XY, []float64{10, 9}),
			wantErrStr:      "origin is an empty point",
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tt.inputGeom)
			require.NoError(t, err)

			pointGeometry, err := geo.MakeGeometryFromGeomT(tt.inputPointGeom)
			require.NoError(t, err)

			got, err := RotateWithPointOrigin(geometry, tt.inputRotRadians, pointGeometry)
			if tt.wantErrStr != "" {
				require.EqualError(t, err, tt.wantErrStr)
			} else {
				require.NoError(t, err)
				requireGeometryWithinEpsilon(t, requireGeometryFromGeomT(t, tt.wantGeom), got, 1e-5)
			}
		})
	}
}

func TestTransScale(t *testing.T) {
	tests := []struct {
		name                       string
		inputGeom                  geom.T
		inputDeltaX, inputDeltaY   float64
		inputXFactor, inputYFactor float64
		wantGeom                   geom.T
	}{
		{
			name:         "transscale a 2D LINESTRING",
			inputGeom:    geom.NewLineStringFlat(geom.XY, []float64{1, 2, 1, 1}),
			inputDeltaX:  5,
			inputDeltaY:  1,
			inputXFactor: 1,
			inputYFactor: 2,
			wantGeom:     geom.NewLineStringFlat(geom.XY, []float64{6, 6, 6, 4}),
		},
		{
			name:         "transscale a 2D POINT",
			inputGeom:    geom.NewPointFlat(geom.XY, []float64{10, 20}),
			inputDeltaX:  5,
			inputDeltaY:  1,
			inputXFactor: 1,
			inputYFactor: 2,
			wantGeom:     geom.NewPointFlat(geom.XY, []float64{15, 42}),
		},
		{
			name:         "transscale a 2D POLYGON",
			inputGeom:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 0, 1, 1, 0, 1, 0, 0}, []int{10}),
			inputDeltaX:  10,
			inputDeltaY:  15,
			inputXFactor: 10,
			inputYFactor: 15,
			wantGeom:     geom.NewPolygonFlat(geom.XY, []float64{100, 225, 110, 225, 110, 240, 100, 240, 100, 225}, []int{10}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			g, err := geo.MakeGeometryFromGeomT(tt.inputGeom)
			require.NoError(t, err)

			got, err := TransScale(g, tt.inputDeltaX, tt.inputDeltaY, tt.inputXFactor, tt.inputYFactor)
			require.NoError(t, err)
			actualGeomT, err := got.AsGeomT()
			require.NoError(t, err)
			require.Equal(t, tt.wantGeom, actualGeomT)
		})
	}
}

func TestRotateX(t *testing.T) {
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
			expected:   geom.NewPointFlat(geom.XY, []float64{10, -10}),
		},
		{
			desc:       "rotate a 2D point where angle is Pi/4",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewPointFlat(geom.XY, []float64{10, 7.071067811865476}),
		},
		{
			desc:       "rotate a 3D point where angle is 2Pi",
			input:      geom.NewPointFlat(geom.XYZ, []float64{10, 10, 10}),
			rotRadians: 2 * math.Pi,
			expected:   geom.NewPointFlat(geom.XYZ, []float64{10, 10, 10}),
		},
		{
			desc:       "rotate a 3D point where angle is Pi",
			input:      geom.NewPointFlat(geom.XYZ, []float64{10, 10, 10}),
			rotRadians: math.Pi,
			expected:   geom.NewPointFlat(geom.XYZ, []float64{10, -10, -10}),
		},
		{
			desc:       "rotate a 3D point where angle is Pi/4",
			input:      geom.NewPointFlat(geom.XYZ, []float64{10, 10, 10}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewPointFlat(geom.XYZ, []float64{10, 8.881784197001252e-16, 14.142135623730951}),
		},
		{
			desc:       "rotate a 2D line string where angle is Pi/3",
			input:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25}),
			rotRadians: math.Pi / 3,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{10, 7.5, 20, 12.5}),
		},
		{
			desc:       "rotate a 2D line string where angle is Pi/5",
			input:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25, 30, 35}),
			rotRadians: math.Pi / 5,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{10, 12.135254915624213, 20, 20.225424859373685, 30, 28.31559480312316}),
		},
		{
			desc:       "rotate a line polygon where angle is 0.5 radians",
			input:      geom.NewPolygonFlat(geom.XY, []float64{1, 1, 5, 5, 1, 10, 1, 1}, []int{8}),
			rotRadians: 0.5,
			expected:   geom.NewPolygonFlat(geom.XY, []float64{1, 0.877582561890373, 5, 4.387912809451864, 1, 8.775825618903728, 1, 0.877582561890373}, []int{8}),
		},
		{
			desc:       "rotate multiple 2D points where angle is Pi/5",
			input:      geom.NewMultiPointFlat(geom.XY, []float64{10, 10, 20, 20, 30, 30}),
			rotRadians: math.Pi / 5,
			expected:   geom.NewMultiPointFlat(geom.XY, []float64{10, 8.090169943749475, 20, 16.18033988749895, 30, 24.270509831248425}),
		},
		{
			desc:       "rotate multiple line strings where angle is Pi/4",
			input:      geom.NewMultiLineStringFlat(geom.XY, []float64{10, 15, 20, 25, 30, 35, 40, 45}, []int{4, 8}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewMultiLineStringFlat(geom.XY, []float64{10, 10.606601717798213, 20, 17.67766952966369, 30, 24.748737341529164, 40, 31.81980515339464}, []int{4, 8}),
		},
		{
			desc:       "rotate multiple polygon strings where angle is Pi",
			input:      geom.NewMultiPolygonFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 1, 1, 4, 4, 5, 5, 6, 6, 4, 4}, [][]int{{8}, {16}}),
			rotRadians: math.Pi,
			expected:   geom.NewMultiPolygonFlat(geom.XY, []float64{1, -1, 2, -2, 3, -3, 1, -1, 4, -4, 5, -5, 6, -6, 4, -4}, [][]int{{8}, {16}}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			actual, err := RotateX(geometry, tc.rotRadians)
			require.NoError(t, err)

			requireGeometryWithinEpsilon(t, requireGeometryFromGeomT(t, tc.expected), actual, 1e-5)
		})
	}
}

func TestRotateY(t *testing.T) {
	testCases := []struct {
		desc       string
		input      geom.T
		rotRadians float64
		expected   geom.T
	}{
		{
			desc:       "rotate a 2D point where angle is Pi/4",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewPointFlat(geom.XY, []float64{7.071067811865476, 10}),
		},
		{
			desc:       "rotate a 3D point where angle is Pi",
			input:      geom.NewPointFlat(geom.XYZ, []float64{10, 10, 10}),
			rotRadians: math.Pi,
			expected:   geom.NewPointFlat(geom.XYZ, []float64{-10, 10, -10}),
		},
		{
			desc:       "rotate a 3D point where angle is Pi/4",
			input:      geom.NewPointFlat(geom.XYZ, []float64{10, 10, 10}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewPointFlat(geom.XYZ, []float64{14.142135623730951, 10, 8.881784197001252e-16}),
		},
		{
			desc:       "rotate a 2D line string where angle is Pi/3",
			input:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25}),
			rotRadians: math.Pi / 3,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{5, 15, 10, 25}),
		},
		{
			desc:       "rotate a 2D line string where angle is Pi/5",
			input:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25, 30, 35}),
			rotRadians: math.Pi / 5,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{8.090169943749475, 15, 16.18033988749895, 25, 24.270509831248425, 35}),
		},
		{
			desc:       "rotate a line polygon where angle is 0.5 radians",
			input:      geom.NewPolygonFlat(geom.XY, []float64{1, 1, 5, 5, 1, 10, 1, 1}, []int{8}),
			rotRadians: 0.5,
			expected:   geom.NewPolygonFlat(geom.XY, []float64{0.877582561890373, 1, 4.387912809451864, 5, 0.877582561890373, 10, 0.877582561890373, 1}, []int{8}),
		},
		{
			desc:       "rotate multiple 2D points where angle is Pi/5",
			input:      geom.NewMultiPointFlat(geom.XY, []float64{10, 10, 20, 20, 30, 30}),
			rotRadians: math.Pi / 5,
			expected:   geom.NewMultiPointFlat(geom.XY, []float64{8.090169943749475, 10, 16.18033988749895, 20, 24.270509831248425, 30}),
		},
		{
			desc:       "rotate multiple line strings where angle is Pi/4",
			input:      geom.NewMultiLineStringFlat(geom.XY, []float64{10, 15, 20, 25, 30, 35, 40, 45}, []int{4, 8}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewMultiLineStringFlat(geom.XY, []float64{7.071067811865476, 15, 14.142135623730951, 25, 21.213203435596427, 35, 28.284271247461902, 45}, []int{4, 8}),
		},
		{
			desc:       "rotate multiple polygon strings where angle is Pi",
			input:      geom.NewMultiPolygonFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 1, 1, 4, 4, 5, 5, 6, 6, 4, 4}, [][]int{{8}, {16}}),
			rotRadians: math.Pi,
			expected:   geom.NewMultiPolygonFlat(geom.XY, []float64{-1, 1, -2, 2, -3, 3, -1, 1, -4, 4, -5, 5, -6, 6, -4, 4}, [][]int{{8}, {16}}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			actual, err := RotateY(geometry, tc.rotRadians)
			require.NoError(t, err)
			requireGeometryWithinEpsilon(t, requireGeometryFromGeomT(t, tc.expected), actual, 1e-5)
		})
	}
}

func TestRotateZ(t *testing.T) {
	testCases := []struct {
		desc       string
		input      geom.T
		rotRadians float64
		expected   geom.T
	}{
		{
			desc:       "rotate a 2D point where angle is Pi/4",
			input:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewPointFlat(geom.XY, []float64{8.881784197001252e-16, 14.142135623730951}),
		},
		{
			desc:       "rotate a 3D point where angle is Pi",
			input:      geom.NewPointFlat(geom.XYZ, []float64{10, 10, 10}),
			rotRadians: math.Pi,
			expected:   geom.NewPointFlat(geom.XYZ, []float64{-10, -10, 10}),
		},
		{
			desc:       "rotate a 3D point where angle is Pi/4",
			input:      geom.NewPointFlat(geom.XYZ, []float64{10, 10, 10}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewPointFlat(geom.XYZ, []float64{8.881784197001252e-16, 14.142135623730951, 10}),
		},
		{
			desc:       "rotate a 2D line string where angle is Pi/3",
			input:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25}),
			rotRadians: math.Pi / 3,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{-7.990381056766577, 16.16025403784439, -11.650635094610964, 29.820508075688775}),
		},
		{
			desc:       "rotate a 2D line string where angle is Pi/5",
			input:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 25, 30, 35}),
			rotRadians: math.Pi / 5,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{-0.726608840637622, 18.013107438548943, 1.48570858018712, 31.98112990522315, 3.698026001011865, 45.949152371897355}),
		},
		{
			desc:       "rotate a line polygon where angle is 0.5 radians",
			input:      geom.NewPolygonFlat(geom.XY, []float64{1, 1, 5, 5, 1, 10, 1, 1}, []int{8}),
			rotRadians: 0.5,
			expected:   geom.NewPolygonFlat(geom.XY, []float64{0.39815702328617, 1.357008100494576, 1.990785116430849, 6.785040502472879, -3.916672824151657, 9.255251157507931, 0.39815702328617, 1.357008100494576}, []int{8}),
		},
		{
			desc:       "rotate multiple 2D points where angle is Pi/5",
			input:      geom.NewMultiPointFlat(geom.XY, []float64{10, 10, 20, 20, 30, 30}),
			rotRadians: math.Pi / 5,
			expected:   geom.NewMultiPointFlat(geom.XY, []float64{2.212317420824743, 13.968022466674206, 4.424634841649485, 27.936044933348413, 6.636952262474232, 41.90406740002262}),
		},
		{
			desc:       "rotate multiple line strings where angle is Pi/4",
			input:      geom.NewMultiLineStringFlat(geom.XY, []float64{10, 15, 20, 25, 30, 35, 40, 45}, []int{4, 8}),
			rotRadians: math.Pi / 4,
			expected:   geom.NewMultiLineStringFlat(geom.XY, []float64{-3.535533905932736, 17.67766952966369, -3.535533905932734, 31.81980515339464, -3.535533905932734, 45.96194077712559, -3.535533905932734, 60.10407640085654}, []int{4, 8}),
		},
		{
			desc:       "rotate multiple polygon strings where angle is Pi",
			input:      geom.NewMultiPolygonFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 1, 1, 4, 4, 5, 5, 6, 6, 4, 4}, [][]int{{8}, {16}}),
			rotRadians: math.Pi,
			expected:   geom.NewMultiPolygonFlat(geom.XY, []float64{-1, -1, -2, -2, -3, -3, -1, -1, -4, -4, -5, -5, -6, -6, -4, -4}, [][]int{{8}, {16}}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tc.input)
			require.NoError(t, err)

			actual, err := RotateZ(geometry, tc.rotRadians)
			require.NoError(t, err)
			requireGeometryWithinEpsilon(t, requireGeometryFromGeomT(t, tc.expected), actual, 1e-5)
		})
	}
}
