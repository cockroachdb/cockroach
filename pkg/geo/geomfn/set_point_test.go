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

func TestSetPoint(t *testing.T) {
	testCases := []struct {
		lineString *geom.LineString
		index      int
		point      *geom.Point
		expected   *geom.LineString
	}{
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      1,
			point:      geom.NewPointFlat(geom.XY, []float64{5, 5}),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1, 1, 5, 5}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 4, 4}),
			index:      -3,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1, 1, 0, 0, 3, 3, 4, 4}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 4, 4}),
			index:      -4,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{0, 0, 2, 2, 3, 3, 4, 4}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      0,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{0, 0, 2, 2}),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ls, err := geo.NewGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			p, err := geo.NewGeometryFromGeomT(tc.point)
			require.NoError(t, err)

			got, err := SetPoint(ls, tc.index, p)
			require.NoError(t, err)

			want, err := geo.NewGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.lineString.SRID(), got.SRID())
		})
	}

	errTestCases := []struct {
		lineString *geom.LineString
		index      int
		point      *geom.Point
	}{
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      3,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3}),
			index:      -4,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			lineString: geom.NewLineString(geom.XY),
			index:      0,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
	}

	for i, tc := range errTestCases {
		t.Run(fmt.Sprintf("error-%d", i), func(t *testing.T) {
			ls, err := geo.NewGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			p, err := geo.NewGeometryFromGeomT(tc.point)
			require.NoError(t, err)

			wantErr := fmt.Sprintf("index %d out of range of lineString with %d coordinates", tc.index, tc.lineString.NumCoords())
			_, err = SetPoint(ls, tc.index, p)
			require.EqualError(t, err, wantErr)
		})
	}
}
