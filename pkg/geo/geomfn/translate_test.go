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
}

func TestTranslateCollection(t *testing.T) {
	testCases := []struct {
		desc     string
		input    *geom.GeometryCollection
		deltas   []float64
		expected *geom.GeometryCollection
	}{
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

			geometry, err = Translate(geometry, tc.deltas)
			require.NoError(t, err)

			g, err := geometry.AsGeomT()
			require.NoError(t, err)

			gotCollection, ok := g.(*geom.GeometryCollection)
			require.True(t, ok)

			require.Equal(t, tc.expected, gotCollection)
		})
	}

}

func TestTranslateErrorMismatchedDeltas(t *testing.T) {
	deltas := []float64{1, 1, 1}
	geometry, err := geo.MakeGeometryFromPointCoords(0, 0)
	require.NoError(t, err)

	_, err = Translate(geometry, deltas)
	isErr := errors.Is(err, geom.ErrStrideMismatch{
		Got:  3,
		Want: 2,
	})
	require.True(t, isErr)
}
