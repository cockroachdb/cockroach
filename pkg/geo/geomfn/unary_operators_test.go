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
)

func TestLength(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected float64
	}{
		{"POINT(1.0 1.0)", 0},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", 2.8284271247461903},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", 0},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))", 0},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", 0},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", 3.8284271247461903},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", 0},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))", 36.50281539872885},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40))))", 36.50281539872885},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Length(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestPerimeter(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected float64
	}{
		{"POINT(1.0 1.0)", 0},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", 0},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", 3.414213562373095},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))", 3.7556349186104043},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", 0},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", 0},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", 7.169848480983499},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))", 60.950627489813755},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40))))", 60.950627489813755},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Perimeter(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestArea(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected float64
	}{
		{"POINT(1.0 1.0)", 0},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", 0},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", 0.5},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))", 0.495},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", 0},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", 0},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", 0.995},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))", 87.5},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Area(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestDimension(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected int
	}{
		{"POINT(1.0 1.0)", 0},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", 1},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", 2},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))", 2},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", 0},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", 1},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", 2},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))", 2},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40))))", 2},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Dimension(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestPoints(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT EMPTY", "MULTIPOINT EMPTY"},
		{"POINT (1 2)", "MULTIPOINT (1 2)"},
		{"MULTIPOINT EMPTY", "MULTIPOINT EMPTY"},
		{"MULTIPOINT (1 2, 3 4)", "MULTIPOINT (1 2, 3 4)"},
		{"LINESTRING EMPTY", "MULTIPOINT EMPTY"},
		{"LINESTRING (1 2, 3 4)", "MULTIPOINT (1 2, 3 4)"},
		{"MULTILINESTRING EMPTY", "MULTIPOINT EMPTY"},
		{"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))", "MULTIPOINT (1 2, 3 4, 5 6, 7 8)"},
		{"POLYGON EMPTY", "MULTIPOINT EMPTY"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", "MULTIPOINT (1 2, 3 4, 5 6, 1 2)"},
		{"MULTIPOLYGON EMPTY", "MULTIPOINT EMPTY"},
		{"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((7 8, 9 0, 1 2, 7 8)))", "MULTIPOINT (1 2, 3 4, 5 6, 1 2, 7 8, 9 0, 1 2, 7 8)"},
		{"GEOMETRYCOLLECTION EMPTY", "MULTIPOINT EMPTY"},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)", "MULTIPOINT EMPTY"},
		{
			`GEOMETRYCOLLECTION (
				LINESTRING(1 1, 2 2),
				POINT(1 1),
				GEOMETRYCOLLECTION(
					MULTIPOINT(2 2, 3 3),
					GEOMETRYCOLLECTION EMPTY,
					POINT(1 1),
					MULTIPOLYGON(((1 2, 2 3, 3 4, 1 2))),
					LINESTRING(3 3, 4 4),
					GEOMETRYCOLLECTION(
						POINT(4 4),
						MULTIPOLYGON (EMPTY, ((1 2, 3 4, 5 6, 1 2), (2 3, 3 4, 4 5, 2 3)), EMPTY),
						POINT(5 5)
					)
				),
				MULTIPOINT EMPTY
			)`,
			"MULTIPOINT (1 1, 2 2, 1 1, 2 2, 3 3, 1 1, 1 2, 2 3, 3 4, 1 2, 3 3, 4 4, 4 4, 1 2, 3 4, 5 6, 1 2, 2 3, 3 4, 4 5, 2 3, 5 5)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			srid := geopb.SRID(4000)
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), srid, true)
			require.NoError(t, err)

			result, err := Points(g)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(result.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, result.SRID())
		})
	}
}

func TestNormalize(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		expected geo.Geometry
	}{
		{rightRect, geo.MustParseGeometry("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")},
		{emptyRect, emptyRect},
		{rightRectPoint, rightRectPoint},
		{middleLine, middleLine},
		{geo.MustParseGeometry(`GEOMETRYCOLLECTION(POINT(2 3),MULTILINESTRING((0 0, 1 1),(2 2, 3 3)),POLYGON((0 10,0 0,10 0,10 10,0 10),(4 2,2 2,2 4,4 4,4 2),(6 8,8 8,8 6,6 6,6 8)))`),
			geo.MustParseGeometry(`GEOMETRYCOLLECTION(POLYGON((0 0,0 10,10 10,10 0,0 0),(6 6,8 6,8 8,6 8,6 6),(2 2,4 2,4 4,2 4,2 2)),MULTILINESTRING((2 2,3 3),(0 0,1 1)),POINT(2 3))`)},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Normalize(tc.a)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}
}

func TestMinimumClearance(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected float64
	}{
		{"POINT (1 1)", math.Inf(1)},
		{"POLYGON EMPTY", math.Inf(1)},
		{"POLYGON ((0 0, 1 0, 1 1, 0.5 3.2e-4, 0 0))", 0.00032},
		{"GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0.5 3.2e-4, 0 0)), POINT (1 1))", 0.00032},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := MinimumClearance(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestMinimumClearanceLine(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT (1 1)", "LINESTRING EMPTY"},
		{"POLYGON EMPTY", "LINESTRING EMPTY"},
		{"POLYGON ((0 0, 1 0, 1 1, 0.5 3.2e-4, 0 0))", "LINESTRING (0.5 0.00032, 0.5 0)"},
		{
			"GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0.5 3.2e-4, 0 0)), POINT (1 1))",
			"LINESTRING (0.5 0.00032, 0.5 0)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			srid := geopb.SRID(4000)
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), srid, true)
			require.NoError(t, err)

			result, err := MinimumClearanceLine(g)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(result.SpatialObject(), 10)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, result.SRID())
		})
	}
}
