// Copyright 2021 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
)

func TestSimplify(t *testing.T) {
	testCases := []struct {
		desc              string
		wkt               string
		tolerance         float64
		preserveCollapsed bool
		expectedWKT       string
		expectedCollapsed bool
	}{
		{
			desc:        "tolerance -1",
			wkt:         "LINESTRING(0 0, 1 1, 2 2)",
			tolerance:   -1,
			expectedWKT: "LINESTRING(0 0,2 2)",
		},
		{
			desc:        "tolerance NaN",
			wkt:         "LINESTRING(0 0, 1 1, 2 2)",
			tolerance:   math.NaN(),
			expectedWKT: "LINESTRING(0 0,2 2)",
		},
		{
			desc:        "tolerance Inf",
			wkt:         "LINESTRING(0 0, 1 1, 2 2)",
			tolerance:   math.Inf(1),
			expectedWKT: "LINESTRING(0 0,2 2)",
		},
		{
			desc:        "tolerance -Inf",
			wkt:         "LINESTRING(0 0, 1 1, 2 2)",
			tolerance:   math.Inf(-1),
			expectedWKT: "LINESTRING(0 0,2 2)",
		},
		{
			desc:        "POINT",
			wkt:         "POINT( 24 55 )",
			tolerance:   100,
			expectedWKT: "POINT(24 55)",
		},
		{
			desc:        "MULTIPOINT",
			wkt:         "MULTIPOINT( 24 55 , 55 66)",
			tolerance:   100,
			expectedWKT: "MULTIPOINT( 24 55 , 55 66)",
		},
		{
			desc:        "NaN tolerance",
			wkt:         "MULTIPOINT( 24 55 , 55 66)",
			tolerance:   math.NaN(),
			expectedWKT: "MULTIPOINT( 24 55 , 55 66)",
		},
		{
			desc:        "empty LINESTRING",
			wkt:         "LINESTRING EMPTY",
			tolerance:   34,
			expectedWKT: "LINESTRING EMPTY",
		},
		{
			desc:        "LINESTRING with nothing to remove",
			wkt:         "LINESTRING(0 0, 1 1.1, 2.1 2, 3 3)",
			tolerance:   0,
			expectedWKT: "LINESTRING(0 0, 1 1.1, 2.1 2, 3 3)",
		},
		{
			desc:        "circular LINESTRING with nothing to remove",
			wkt:         "LINESTRING(0 0, 1 1.1, 2.1 2, 3 3.3, 0 0)",
			tolerance:   0,
			expectedWKT: "LINESTRING(0 0, 1 1.1, 2.1 2, 3 3.3, 0 0)",
		},
		{
			desc:              "linestring tolerance 0, empty everything",
			wkt:               "LINESTRING(15 15, 15 15)",
			tolerance:         0,
			expectedCollapsed: true,
		},
		{
			desc:              "linestring tolerance 0, preserve collapse",
			wkt:               "LINESTRING(15 15, 15 15)",
			tolerance:         0,
			preserveCollapsed: true,
			expectedWKT:       "LINESTRING(15 15, 15 15)",
		},
		{
			desc:              "linestring tolerance 1, empty everything",
			wkt:               "LINESTRING(15 15, 15 15)",
			tolerance:         1,
			expectedCollapsed: true,
		},
		{
			desc:              "linestring tolerance 1, preserve collapse",
			wkt:               "LINESTRING(15 15, 15 15)",
			tolerance:         1,
			preserveCollapsed: true,
			expectedWKT:       "LINESTRING(15 15, 15 15)",
		},
		{
			desc: "linestring, tolerance 0, lots of duplicate points",
			wkt: `LINESTRING(
				0 0, 1 0, 2 0, 3 0,
				4 2, 5 1, 6 0,
				4 -2, 2 -4, 0 -6,
				0 0, 0 1, 0 5, 0 100,
				100 100, 10 100, 50 100
			)`,
			tolerance:   0,
			expectedWKT: "LINESTRING(0 0, 3 0, 4 2, 6 0, 0 -6, 0 100, 100 100, 10 100, 50 100)",
		},
		{
			desc:        "linestring as a zig zag",
			wkt:         "LINESTRING(20 20, 19 19, -6 6, 45 -45, 46 -45, 30 30)",
			tolerance:   3,
			expectedWKT: "LINESTRING(20 20, -6 6, 46 -45, 30 30)",
		},
		{
			desc:        "linestring, go very forward then back",
			wkt:         "LINESTRING(1 1, 50 50, 1 1)",
			tolerance:   0,
			expectedWKT: "LINESTRING(1 1, 50 50, 1 1)",
		},
		{
			desc:        "linestring, go very forward then back with tolerance 1",
			wkt:         "LINESTRING(1 1, 50 50, 1 1)",
			tolerance:   1,
			expectedWKT: "LINESTRING(1 1, 50 50, 1 1)",
		},
		{
			desc:        "linestring, remove some elements",
			wkt:         "LINESTRING(10 10, 20 10, 20 15, 20 20, 15 20, 15.5 21.1, 10 20)",
			tolerance:   9,
			expectedWKT: "LINESTRING(10 10, 20 10, 10 20)",
		},
		{
			desc:        "multilinestring; keep some lose some",
			wkt:         "MULTILINESTRING((10 10, 20 10, 20 15, 20 20, 15 20, 15.5 21.1, 10 20), (100 100, 350.1 300, 350.1 299, 500 500), (0 0, 0 0))",
			tolerance:   9,
			expectedWKT: "MULTILINESTRING((10 10, 20 10, 10 20), (100 100, 350.1 299, 500 500))",
		},
		{
			desc:        "polygon",
			wkt:         "POLYGON ((20 10, 10 20, 20 20, 20 30, 30 30, 30 20, 40 20, 40 10, 30 0, 20 0, 20 10))",
			tolerance:   5,
			expectedWKT: "POLYGON ((20 10, 10 20, 30 30, 40 10, 30 00, 20 0, 20 10))",
		},
		{
			desc:        "polygon with rings",
			wkt:         "POLYGON ((5 7, 2 5, 5 4, 13 4, 18 7, 16 11, 7 9, 11 7, 5 7), (13 8, 13 6, 14 6, 15 9, 13 8))",
			tolerance:   3,
			expectedWKT: "POLYGON ((5 7, 2 5, 18 7, 16 11, 5 7))",
		},
		{
			desc: "polygon, keep some rings",
			wkt: `POLYGON(
	    (0 0, 100 0, 100 100, 0 100, 0 0),
	    (1 1, 1 5, 5 5, 5 1, 1 1),
	    (20 20, 20 40, 40 40, 40 20, 20 20)
	  )`,
			tolerance:   10,
			expectedWKT: "POLYGON((0 0,100 0,100 100,0 100,0 0),(20 20,20 40,40 40,40 20,20 20))",
		},
		{
			desc: "polygon, preserve collapsed, keep some rings",
			wkt: `POLYGON(
	    (0 0, 100 0, 100 100, 0 100, 0 0),
	    (1 1, 1 5, 5 5, 5 1, 1 1),
	    (20 20, 20 40, 40 40, 40 20, 20 20)
	  )`,
			tolerance:         10,
			preserveCollapsed: true,
			expectedWKT:       "POLYGON((0 0,100 0,100 100,0 100,0 0),(20 20,20 40,40 40,40 20,20 20))",
		},
		{
			desc:              "polygon which gets destroyed",
			wkt:               `POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1), (0 0, 100 0, 100 100, 0 100, 0 0))`,
			tolerance:         10,
			expectedCollapsed: true,
		},
		{
			desc:              "polygon which gets preserved",
			wkt:               `POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1), (0 0, 100 0, 100 100, 0 100, 0 0))`,
			tolerance:         10,
			preserveCollapsed: true,
			expectedWKT:       "POLYGON ((-1 -1, -1 1, 1 1, -1 -1), (0 0, 100 0, 100 100, 0 100, 0 0))",
		},
		{
			desc: "MULTIPOLYGON",
			wkt: `MULTIPOLYGON(
			(
				(0 0, 100 0, 100 100, 0 100, 0 0),
				(1 1, 1 5, 5 5, 5 1, 1 1),
				(20 20, 20 40, 40 40, 40 20, 20 20)
			),
			(
				(-1 -1, -1 1, 1 1, 1 -1, -1 -1)
			),
			((0 0, 100 0, 100 100, 0 100, 0 0))
		)`,
			tolerance:   10,
			expectedWKT: "MULTIPOLYGON (((0 0, 100 0, 100 100, 0 100, 0 0), (20 20, 20 40, 40 40, 40 20, 20 20)), ((0 0, 100 0, 100 100, 0 100, 0 0)))",
		},
		{
			desc: "GEOMETRYCOLLECTION",
			wkt: `GEOMETRYCOLLECTION(
		POINT EMPTY,
		GEOMETRYCOLLECTION(LINESTRING(0 0, 0 0)),
		POLYGON((0 0, 100 0, 100 100, 0 100, 0 0)),
		LINESTRING(-50 -50, 100 100)
)`,
			tolerance:   10,
			expectedWKT: "GEOMETRYCOLLECTION (POINT EMPTY, GEOMETRYCOLLECTION EMPTY, POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0)), LINESTRING (-50 -50, 100 100))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, collapsed, err := Simplify(g, tc.tolerance, tc.preserveCollapsed)
			require.NoError(t, err)
			var expected geo.Geometry
			require.Equal(t, tc.expectedCollapsed, collapsed)
			if !tc.expectedCollapsed {
				expected, err = geo.ParseGeometry(tc.expectedWKT)
				require.NoError(t, err)

				out, err := geo.SpatialObjectToEWKT(ret.SpatialObject(), -1)
				require.NoError(t, err)
				t.Logf("wkt out: %s", out)
				require.Equal(t, expected, ret)
			}
		})
	}
}
