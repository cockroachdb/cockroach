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

func TestCentroid(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT(1.0 1.0)", "POINT (1.0 1.0)"},
		{"SRID=4326;POINT(1.0 1.0)", "SRID=4326;POINT (1.0 1.0)"},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", "POINT (2.0 2.0)"},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", "POINT (0.666666666666667 0.333333333333333)"},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))", "POINT (0.671717171717172 0.335353535353535)"},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", "POINT (1.5 1.5)"},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", "POINT (3.17541743733684 3.04481549985497)"},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", "POINT (2.17671691792295 1.84187604690117)"},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))", "POINT (35 38.3333333333333)"},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Centroid(g)
			require.NoError(t, err)

			retAsGeomT, err := ret.AsGeomT()
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)
			expectedAsGeomT, err := expected.AsGeomT()
			require.NoError(t, err)

			// Ensure points are close in terms of precision.
			require.InEpsilon(t, expectedAsGeomT.(*geom.Point).X(), retAsGeomT.(*geom.Point).X(), 2e-10)
			require.InEpsilon(t, expectedAsGeomT.(*geom.Point).Y(), retAsGeomT.(*geom.Point).Y(), 2e-10)
			require.Equal(t, expected.SRID(), ret.SRID())
		})
	}
}

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
