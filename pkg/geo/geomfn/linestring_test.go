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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestLineStringFromMultiPoint(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"MULTIPOINT EMPTY", "LINESTRING EMPTY"},
		{"MULTIPOINT (1 2, 3 4, 5 6)", "LINESTRING (1 2, 3 4, 5 6)"},
		{"MULTIPOINT (1 2, EMPTY, 3 4)", "LINESTRING (1 2, 1 2, 3 4)"},
		{"MULTIPOINT (EMPTY, 1 2, EMPTY, 3 4)", "LINESTRING (0 0, 1 2, 1 2, 3 4)"},
		{"MULTIPOINT (EMPTY, EMPTY, 1 2, EMPTY, 3 4)", "LINESTRING (0 0, 0 0, 1 2, 1 2, 3 4)"},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			srid := geopb.SRID(4000)
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), srid, true)
			require.NoError(t, err)

			result, err := LineStringFromMultiPoint(g)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(result.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, result.SRID())
		})
	}

	errorTestCases := []struct {
		wkt string
	}{
		{"MULTIPOINT (1 1)"},
		{"POINT EMPTY"},
		{"POINT (1 1)"},
		{"LINESTRING (1 1, 2 2)"},
		{"MULTILINESTRING ((1 1, 2 2))"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))"},
		{"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)))"},
		{"GEOMETRYCOLLECTION (MULTIPOINT (1 1, 2 2))"},
	}

	t.Run("Errors on invalid input", func(t *testing.T) {
		for _, tc := range errorTestCases {
			t.Run(tc.wkt, func(t *testing.T) {
				g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), geopb.DefaultGeometrySRID, true)
				require.NoError(t, err)

				_, err = LineStringFromMultiPoint(g)
				require.Error(t, err)
			})
		}
	})
}

func TestLineMerge(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"MULTILINESTRING EMPTY", "MULTILINESTRING EMPTY"},
		{
			"MULTILINESTRING ((1 2, 2 3, 3 4), (3 4, 4 5, 5 6), (5 6, 6 7, 7 8))",
			"LINESTRING (1 2, 2 3, 3 4, 4 5, 5 6, 6 7, 7 8)",
		},
		{
			"MULTILINESTRING ((1 2, 2 3, 3 4), EMPTY, (3 4, 4 5, 5 6), EMPTY, (5 6, 6 7, 7 8))",
			"LINESTRING (1 2, 2 3, 3 4, 4 5, 5 6, 6 7, 7 8)",
		},
		{
			"MULTILINESTRING ((1 2, 2 3, 3 4), (3 4, 4 5, 5 6), (6 7, 7 8, 8 9), (8 9, 9 10, 10 11))",
			"MULTILINESTRING ((1 2, 2 3, 3 4, 4 5, 5 6), (6 7, 7 8, 8 9, 9 10, 10 11))",
		},
		{"POINT EMPTY", "POINT EMPTY"},
		{"POINT (1 1)", "GEOMETRYCOLLECTION EMPTY"},
		{"MULTIPOINT EMPTY", "MULTIPOINT EMPTY"},
		{"MULTIPOINT (1 1, 2 2)", "GEOMETRYCOLLECTION EMPTY"},
		{"LINESTRING EMPTY", "LINESTRING EMPTY"},
		{"LINESTRING (1 2, 3 4)", "LINESTRING (1 2, 3 4)"},
		{"POLYGON EMPTY", "POLYGON EMPTY"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", "GEOMETRYCOLLECTION EMPTY"},
		{"MULTIPOLYGON EMPTY", "MULTIPOLYGON EMPTY"},
		{"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)))", "GEOMETRYCOLLECTION EMPTY"},
		{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY"},
		{"GEOMETRYCOLLECTION (MULTILINESTRING ((1 2, 2 3, 3 4)))", "GEOMETRYCOLLECTION EMPTY"},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			srid := geopb.SRID(4000)
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), srid, true)
			require.NoError(t, err)

			result, err := LineMerge(g)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(result.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, result.SRID())
		})
	}
}

func TestLineLocatePoint(t *testing.T) {
	testCases := []struct {
		lineString *geom.LineString
		point      *geom.Point
		expected   float64
	}{
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{0, 1, 1, 0}),
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   0.5,
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{0, 1, 1, 0}),
			point:      geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected:   0.5,
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{-1, -1, -1, 1}),
			point:      geom.NewPointFlat(geom.XY, []float64{-1, 0}),
			expected:   0.5,
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, -1, -1, 1}),
			point:      geom.NewPointFlat(geom.XY, []float64{-1, 0}),
			expected:   0.75,
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{-1, 1, 1, -1}),
			point:      geom.NewPointFlat(geom.XY, []float64{-1, 0}),
			expected:   0.25,
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{0, 6, 3, 0}),
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   0.8,
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{6, 6, 3, 0}),
			point:      geom.NewPointFlat(geom.XY, []float64{1, 1}),
			expected:   1,
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{6, 6, 3, 0}),
			point:      geom.NewPointFlat(geom.XY, []float64{3, 1}),
			expected:   0.87,
		},
	}

	for index, tc := range testCases {
		t.Run(fmt.Sprintf("%d", index), func(t *testing.T) {
			line, err := geo.MakeGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			p, err := geo.MakeGeometryFromGeomT(tc.point)
			require.NoError(t, err)

			fraction, err := LineLocatePoint(line, p)
			require.NoError(t, err)

			fraction = math.Round(fraction*100) / 100

			require.Equal(t, tc.expected, fraction)
		})
	}
}

func TestAddPoint(t *testing.T) {
	testCases := []struct {
		lineString *geom.LineString
		index      int
		point      *geom.Point
		expected   *geom.LineString
	}{
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      0,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      1,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1, 1, 0, 0, 2, 2}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      2,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 0, 0}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      -1,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 0, 0}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      1,
			point:      geom.NewPointEmpty(geom.XY),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1, 1, 0, 0, 2, 2}),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ls, err := geo.MakeGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			p, err := geo.MakeGeometryFromGeomT(tc.point)
			require.NoError(t, err)

			got, err := AddPoint(ls, tc.index, p)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.lineString.SRID(), got.SRID())
		})
	}

	errTestCases := []struct {
		lineString geom.T
		index      int
		point      geom.T
	}{
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      3,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      -2,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			lineString: geom.NewPointFlat(geom.XY, []float64{1, 1}),
			index:      0,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			lineString: geom.NewPolygonFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 1, 1}, []int{8}),
			index:      0,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			lineString: geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2, 2}, []int{4}),
			index:      0,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      0,
			point:      geom.NewLineStringFlat(geom.XY, []float64{3, 3, 4, 4}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      0,
			point:      geom.NewPolygonFlat(geom.XY, []float64{3, 3, 4, 4, 5, 5, 3, 3}, []int{8}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      0,
			point:      geom.NewMultiPointFlat(geom.XY, []float64{3, 3, 4, 4}),
		},
		// The below test case deviates from PostGIS behavior, where it is in fact possible to
		// create a line with a single coordinate via ST_AddPoint, which is probably a bug:
		//
		// postgis=# SELECT st_astext(st_addpoint('LINESTRING EMPTY', 'POINT (1 1)'))
		//
		//     st_astext
		// -----------------
		// LINESTRING(1 1)
		{
			lineString: geom.NewLineString(geom.XY),
			index:      0,
			point:      geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
	}

	for i, tc := range errTestCases {
		t.Run(fmt.Sprintf("error-%d", i), func(t *testing.T) {
			ls, err := geo.MakeGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			p, err := geo.MakeGeometryFromGeomT(tc.point)
			require.NoError(t, err)

			_, err = AddPoint(ls, tc.index, p)
			require.Error(t, err)
		})
	}
}

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
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:      0,
			point:      geom.NewPointEmpty(geom.XY),
			expected:   geom.NewLineStringFlat(geom.XY, []float64{0, 0, 2, 2}),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ls, err := geo.MakeGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			p, err := geo.MakeGeometryFromGeomT(tc.point)
			require.NoError(t, err)

			got, err := SetPoint(ls, tc.index, p)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
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
			ls, err := geo.MakeGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			p, err := geo.MakeGeometryFromGeomT(tc.point)
			require.NoError(t, err)

			wantErr := fmt.Sprintf("index %d out of range of LineString with %d coordinates", tc.index, tc.lineString.NumCoords())
			_, err = SetPoint(ls, tc.index, p)
			require.EqualError(t, err, wantErr)
		})
	}
}

func TestRemovePoint(t *testing.T) {
	testCases := []struct {
		lineString *geom.LineString
		index      int
		expected   *geom.LineString
	}{
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3}),
			index:      0,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{2, 2, 3, 3}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3}),
			index:      1,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1, 1, 3, 3}),
		},
		{
			lineString: geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3}),
			index:      2,
			expected:   geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ls, err := geo.MakeGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			got, err := RemovePoint(ls, tc.index)
			require.NoError(t, err)

			want, err := geo.MakeGeometryFromGeomT(tc.expected)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.EqualValues(t, tc.lineString.SRID(), got.SRID())
		})
	}

	errTestCases := []struct {
		lineString  *geom.LineString
		index       int
		expectedErr string
	}{
		{
			lineString:  geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3}),
			index:       3,
			expectedErr: "index 3 out of range of LineString with 3 coordinates",
		},
		{
			lineString:  geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3}),
			index:       -1,
			expectedErr: "index -1 out of range of LineString with 3 coordinates",
		},
		{
			lineString:  geom.NewLineStringFlat(geom.XY, []float64{1, 1, 2, 2}),
			index:       1,
			expectedErr: "cannot remove a point from a LineString with only two Points",
		},
	}

	for i, tc := range errTestCases {
		t.Run(fmt.Sprintf("error-%d", i), func(t *testing.T) {
			ls, err := geo.MakeGeometryFromGeomT(tc.lineString)
			require.NoError(t, err)

			_, err = RemovePoint(ls, tc.index)
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}
