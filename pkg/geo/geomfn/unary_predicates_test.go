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
)

func TestIsClosed(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"POINT EMPTY", false},
		{"POINT(1 1)", true},
		{"LINESTRING EMPTY", false},
		{"LINESTRING(1 1, 2 2)", false},
		{"LINESTRING(1 1, 2 2, 1 1)", true},
		{"LINESTRING(1 1, 1 2, 2 2, 2 1, 1 1)", true},
		{"POLYGON EMPTY", false},
		{"POLYGON((0 0, 1 0, 1 1, 0 0))", true},
		{"MULTIPOINT EMPTY", false},
		{"MULTIPOINT((1 1), (2 2))", true},
		{"MULTILINESTRING EMPTY", false},
		{"MULTILINESTRING((1 1, 3 3), (1 1, 2 2, 1 1))", false},
		{"MULTILINESTRING((1 1, 3 3, 1 1), (1 1, 2 2, 1 1))", true},
		{"MULTIPOLYGON EMPTY", false},
		{"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((0 0, 0 1, 1 0, 0 0)))", true},
		{"GEOMETRYCOLLECTION EMPTY", false},
		{"GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1))", false},
		{"GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1, 0 0))", true},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)", false},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1)))", false},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1, 0 0)))", true},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsClosed(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestIsCollection(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"POINT(1.0 1.0)", false},
		{"POINT EMPTY", false},
		{"LINESTRING(1.0 1.0, 2.0 2.0)", false},
		{"LINESTRING EMPTY", false},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", false},
		{"POLYGON EMPTY", false},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", true},
		{"MULTIPOINT EMPTY", true},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", true},
		{"MULTILINESTRING EMPTY", true},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", true},
		{"MULTIPOLYGON EMPTY", true},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40))", true},
		{"GEOMETRYCOLLECTION EMPTY", true},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(POINT (40 10),LINESTRING (10 10, 20 20, 10 40)))", true},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)", true},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsCollection(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestIsEmpty(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"POINT(1.0 1.0)", false},
		{"POINT EMPTY", true},
		{"LINESTRING(1.0 1.0, 2.0 2.0)", false},
		{"LINESTRING EMPTY", true},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", false},
		{"POLYGON EMPTY", true},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", false},
		{"MULTIPOINT EMPTY", true},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", false},
		{"MULTILINESTRING EMPTY", true},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", false},
		{"MULTIPOLYGON EMPTY", true},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40))", false},
		{"GEOMETRYCOLLECTION EMPTY", true},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(POINT (40 10),LINESTRING (10 10, 20 20, 10 40)))", false},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)", true},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsEmpty(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestIsRing(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"LINESTRING EMPTY", false},
		{"LINESTRING(1 1, 2 2)", false},
		{"LINESTRING(1 1, 2 2, 1 1)", false},
		{"LINESTRING(1 1, 2 2, 2 1, 1 2, 1 1)", false},
		{"LINESTRING(1 1, 1 2, 2 2, 2 1, 1 1)", true},
		// Empty non-linestring geometries shouldn't error, to follow PostGIS behavior.
		{"POINT EMPTY", false},
		{"POLYGON EMPTY", false},
		{"MULTILINESTRING EMPTY", false},
		{"MULTIPOINT EMPTY", false},
		{"MULTIPOLYGON EMPTY", false},
		{"GEOMETRYCOLLECTION EMPTY", false},
	}

	errorTestCases := []struct {
		wkt string
	}{
		{"POINT(1 1)"},
		{"POLYGON((0 0, 1 0, 1 1, 0 0))"},
		{"MULTIPOINT((1 1), (2 2))"},
		{"MULTILINESTRING((1 1, 2 2), (2 1, 1 2))"},
		{"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((0 0, 0 1, 1 0, 0 0)))"},
		{"GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 2 2))"},
		{"GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), LINESTRING(0 1, 1 0))"},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsRing(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors on non-linestring", func(t *testing.T) {
		for _, tc := range errorTestCases {
			t.Run(tc.wkt, func(t *testing.T) {
				g, err := geo.ParseGeometry(tc.wkt)
				require.NoError(t, err)
				_, err = IsRing(g)
				require.Error(t, err)
			})
		}
	})
}

func TestIsSimple(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"POINT EMPTY", true},
		{"POINT(1 1)", true},
		{"LINESTRING EMPTY", true},
		{"LINESTRING(1 1, 2 2)", true},
		{"LINESTRING(1 1, 2 2, 1 1)", false},
		{"LINESTRING(1 1, 1 2, 2 2, 2 1, 1 1)", true},
		{"LINESTRING(1 1, 2 2, 2 1, 1 2)", false},
		{"POLYGON EMPTY", true},
		{"POLYGON((0 0, 1 0, 1 1, 0 0))", true},
		{"POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))", false},
		{"MULTIPOINT EMPTY", true},
		{"MULTIPOINT((1 1), (1 1))", false},
		{"MULTIPOINT((1 1), (2 2))", true},
		{"MULTILINESTRING EMPTY", true},
		{"MULTILINESTRING((1 1, 2 2), (2 1, 1 2))", false},
		{"MULTILINESTRING((1 1, 2 2), (0 0, 3 3))", false},
		{"MULTILINESTRING((1 1, 2 2), (3 3, 4 4))", true},
		{"MULTIPOLYGON EMPTY", true},
		// The next two test cases appear to be wrong, since the polygons overlap,
		// but this is returned by GEOS and matches PostGIS behavior.
		{"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((0 0, 0 1, 1 0, 0 0)))", true},
		{"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((0 0, 1 0, 1 1, 0 0)))", true},
		{"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((2 2, 2 3, 3 3, 2 2)))", true},
		{"MULTIPOLYGON(((0 0, 1 1, 1 0, 0 1, 0 0)), ((2 2, 2 3, 3 3, 2 2)))", false},
		{"GEOMETRYCOLLECTION EMPTY", true},
		{"GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 2 2))", true},
		{"GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), LINESTRING(0 1, 1 0))", true},
		{"GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1, 0 0), LINESTRING(0 1, 1 0))", false},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)", true},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 2 2)))", true},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), LINESTRING(0 1, 1 0)))", true},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1, 0 0), LINESTRING(0 1, 1 0)))", false},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsSimple(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}
