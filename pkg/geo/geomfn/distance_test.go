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
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/stretchr/testify/require"
)

var distanceTestCases = []struct {
	desc                 string
	a                    string
	b                    string
	expectedMinDistance  float64
	expectedMaxDistance  float64
	expectedLongestLine  string
	expectedShortestLine string
}{
	{
		"Same POINTs",
		"POINT(1.0 1.0)",
		"POINT(1.0 1.0)",
		0,
		0,
		"LINESTRING (1 1, 1 1)",
		"LINESTRING (1 1, 1 1)",
	},
	{
		"Same 3D POINTs",
		"POINT(1.0 2.0 3.0)",
		"POINT(1.0 2.0 3.0)",
		0,
		0,
		"LINESTRING (1 2, 1 2)",
		"LINESTRING (1 2, 1 2)",
	},
	{
		"Different POINTs",
		"POINT(1.0 1.0)",
		"POINT(2.0 1.0)",
		1,
		1,
		"LINESTRING(1.0 1.0, 2.0 1.0)",
		"LINESTRING (1 1, 2 1)",
	},
	{
		"Different 3D POINTs",
		"POINT(0.0 1.0 2.0)",
		"POINT(0.0 3.0 5.0)",
		2,
		2,
		"LINESTRING (0 1, 0 3)",
		"LINESTRING (0 1, 0 3)",
	},
	{
		"POINT on LINESTRING",
		"POINT(0.5 0.5)",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		0,
		2.1213203435596424,
		"LINESTRING(0.5 0.5, 2.0 2.0)",
		"LINESTRING (0.5 0.5, 0.5 0.5)",
	},
	{
		"POINT away from LINESTRING",
		"POINT(3.0 3.0)",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		1.4142135623730951,
		4.242640687119285,
		"LINESTRING(3.0 3.0, 0.0 0.0)",
		"LINESTRING (3 3, 2 2)",
	},
	{
		"LINESTRING away from POINT",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		"POINT(3.0 3.0)",
		1.4142135623730951,
		4.242640687119285,
		"LINESTRING(0.0 0.0, 3.0 3.0)",
		"LINESTRING (2.0 2.0, 3.0 3.0)",
	},
	{
		"Same LINESTRING",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		0,
		2.8284271247461903,
		"LINESTRING(0.0 0.0, 2.0 2.0)",
		"LINESTRING (0 0, 0 0)",
	},
	{
		"Intersecting LINESTRING",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		"LINESTRING(0.5 0.0, 0.5 3.0)",
		0,
		3.0413812651491097,
		"LINESTRING(0.0 0.0, 0.5 3.0)",
		"LINESTRING (0.5 0.5, 0.5 0.5)",
	},
	{
		"LINESTRING does not meet",
		"LINESTRING(6.0 6.0, 7.0 7.0, 8.0 8.0)",
		"LINESTRING(0.0 0.0, 3.0 -3.0)",
		8.48528137423857,
		12.083045973594572,
		"LINESTRING(8.0 8.0, 3.0 -3.0)",
		"LINESTRING (6 6, 0 0)",
	},
	{
		"POINT in POLYGON",
		"POINT(0.5 0.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0,
		0.7071067811865476,
		"LINESTRING (0.5 0.5, 1.0 0.0)",
		"LINESTRING (0.5 0.5, 0.5 0.5)",
	},
	{
		"POINT in POLYGON hole",
		"POINT(0.5 0.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.6 0.2, 0.6 0.6, 0.2 0.6, 0.2 0.2))",
		0.09999999999999998,
		0.7071067811865476,
		"LINESTRING(0.5 0.5, 1.0 0.0)",
		"LINESTRING (0.5 0.5, 0.6 0.5)",
	},
	{
		"POINT not in POLYGON hole",
		"POINT(0.1 0.1)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.6 0.2, 0.6 0.6, 0.2 0.6, 0.2 0.2))",
		0,
		1.2727922061357855,
		"LINESTRING(0.1 0.1, 1.0 1.0)",
		"LINESTRING (0.1 0.1, 0.1 0.1)",
	},
	{
		"POINT outside of POLYGON",
		"POINT(1.5 1.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0.7071067811865476,
		2.1213203435596424,
		"LINESTRING (1.5 1.5, 0.0 0.0)",
		"LINESTRING (1.5 1.5, 1 1)",
	},
	{
		"LINESTRING intersects POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"LINESTRING(-0.5 -0.5, 0.5 0.5)",
		0,
		2.1213203435596424,
		"LINESTRING (1.0 1.0, -0.5 -0.5)",
		"LINESTRING (-0 -0, -0 -0)",
	},
	{
		"LINESTRING intersects POLYGON, duplicate points",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 1.0 1.0, 0.0 1.0, 0.0 0.0, 0.0 0.0))",
		"LINESTRING(-0.5 -0.5, 0.5 0.5, 0.5 0.5)",
		0,
		2.1213203435596424,
		"LINESTRING (1.0 1.0, -0.5 -0.5)",
		"LINESTRING (-0 -0, -0 -0)",
	},
	{
		"LINESTRING outside of POLYGON",
		"LINESTRING(-0.5 -0.5, -0.5 0.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0.5,
		2.1213203435596424,
		"LINESTRING (-0.5 -0.5, 1.0 1.0)",
		"LINESTRING (-0.5 0, 0 0)",
	},
	{
		"LINESTRING outside of POLYGON, duplicate points",
		"LINESTRING(-0.5 -0.5, -0.5 -0.5, -0.5 0.5, -0.5 0.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0.5,
		2.1213203435596424,
		"LINESTRING(-0.5 -0.5, 1.0 1.0)",
		"LINESTRING (-0.5 0, 0 0)",
	},
	{
		"POLYGON is the same",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0,
		1.4142135623730951,
		"LINESTRING(0.0 0.0, 1.0 1.0)",
		"LINESTRING (0 0, 0 0)",
	},
	{
		"POLYGON inside POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
		0,
		1.2727922061357855,
		"LINESTRING(0.0 0.0, 0.9 0.9)",
		"LINESTRING (0.1 0.1, 0.1 0.1)",
	},
	{
		"POLYGON to POLYGON intersecting through its hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"POLYGON((0.15 0.25, 0.35 0.25, 0.35 0.35, 0.25 0.35, 0.15 0.25))",
		0,
		1.1335784048754634,
		"LINESTRING(1.0 1.0, 0.15 0.25)",
		"LINESTRING (0.15 0.25, 0.15 0.25)",
	},
	{
		"POLYGON to POLYGON intersecting through its hole, duplicate points",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"POLYGON((0.15 0.25, 0.15 0.25, 0.35 0.25, 0.35 0.35, 0.35 0.35,0.25 0.35, 0.15 0.25))",
		0,
		1.1335784048754634,
		"LINESTRING(1.0 1.0, 0.15 0.25)",
		"LINESTRING (0.15 0.25, 0.15 0.25)",
	},
	{
		"POLYGON inside POLYGON hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
		"POLYGON((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))",
		0.09999999999999998,
		1.1313708498984762,
		"LINESTRING(0.0 0.0, 0.8 0.8)",
		"LINESTRING (0.9 0.2, 0.8 0.2)",
	},
	{
		"POLYGON outside POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))",
		2.8284271247461903,
		5.656854249492381,
		"LINESTRING(0.0 0.0, 4.0 4.0)",
		"LINESTRING (1 1, 3 3)",
	},
	{
		"MULTIPOINT to MULTIPOINT",
		"MULTIPOINT((1.0 1.0), (2.0 2.0))",
		"MULTIPOINT((2.5 2.5), (3.0 3.0))",
		0.7071067811865476,
		2.8284271247461903,
		"LINESTRING(1.0 1.0, 3.0 3.0)",
		"LINESTRING (2 2, 2.5 2.5)",
	},
	{
		"MULTIPOINT to MULTILINESTRING",
		"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1))",
		"MULTIPOINT(2.0 2.0, 1.0 1.0, 3.0 3.0)",
		0,
		2.8284271247461903,
		"LINESTRING (1.0 1.0, 3.0 3.0)",
		"LINESTRING (1 1, 1 1)",
	},
	{
		"MULTIPOINT to MULTIPOLYGON",
		"MULTIPOINT ((2.0 3.0), (10 42))",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		2,
		56.72741841473134,
		"LINESTRING (2.0 3.0, 45.0 40.0)",
		"LINESTRING (10 42, 10 40)",
	},
	{
		"MULTILINESTRING to MULTILINESTRING",
		"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1), (3.0 3.0, 3.1 3.1))",
		"MULTILINESTRING((2.0 2.0, 2.1 2.1), (4.0 3.0, 3.1 3.1))",
		0,
		3.605551275463989,
		"LINESTRING(1.0 1.0, 4.0 3.0)",
		"LINESTRING (2 2, 2 2)",
	},
	{
		"MULTILINESTRING to MULTIPOLYGON",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		"MULTILINESTRING((3 3, -4 -4), (45 41, 48 48, 52 52))",
		1,
		65.85590330410783,
		"LINESTRING (45.0 40.0, -4.0 -4.0)",
		"LINESTRING (45 40, 45 41)",
	},
	{
		"MULTIPOLYGON to MULTIPOLYGON",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		"MULTIPOLYGON (((30 20, 45 40, 15 45, 30 20)))",
		0,
		50,
		"LINESTRING (5 10, 45 40)",
		"LINESTRING (30 20, 30 20)",
	},
	{
		"GEOMETRYCOLLECTION (POINT, POINT) with POINT",
		"GEOMETRYCOLLECTION ( POINT(3 4), POINT(1 2) )",
		"POINT(1.0 2.0)",
		0,
		2.8284271247461903,
		"LINESTRING (3 4, 1 2)",
		"LINESTRING (1 2, 1 2)",
	},
	{
		"GEOMETRYCOLLECTION (POINT, EMPTY) with POINT",
		"GEOMETRYCOLLECTION ( POINT(1.0 2.0), LINESTRING EMPTY )",
		"POINT(1.0 2.0)",
		0,
		0,
		"LINESTRING (1 2, 1 2)",
		"LINESTRING (1 2, 1 2)",
	},
	{
		"GEOMETRYCOLLECTION (POINT, POINT) with DIFFERENT POINT",
		"GEOMETRYCOLLECTION ( POINT(1.0 2.0), POINT(6 3) )",
		"POINT(1.0 3.0)",
		1,
		5,
		"LINESTRING (6 3, 1 3)",
		"LINESTRING(1.0 2.0, 1.0 3.0)",
	},
	{
		"GEOMETRYCOLLECTION (POINT, EMPTY) with DIFFERENT POINT",
		"GEOMETRYCOLLECTION ( POINT(1.0 2.0), LINESTRING EMPTY )",
		"POINT(1.0 3.0)",
		1,
		1,
		"LINESTRING(1.0 2.0, 1.0 3.0)",
		"LINESTRING (1 2, 1 3)",
	},
	{
		"MULTIPOLYGON to MULTIPOINT",
		"MULTIPOLYGON(((15 5, 40 10, 10 20, 5 10, 15 5)), ((30 20, 45 40, 10 40, 30 20)))",
		"MULTIPOINT((2.0 3.0), (10 42))",
		2,
		56.72741841473134,
		"LINESTRING(45.0 40.0, 2.0 3.0)",
		"LINESTRING (10 40, 10 42)",
	},
	{
		"POLYGON inside POLYGON hole",
		"POLYGON((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
		0.09999999999999998,
		1.1313708498984762,
		"LINESTRING(0.2 0.2, 1.0 1.0)",
		"LINESTRING (0.8 0.2, 0.9 0.2)",
	},
}

// TODO(otan): delete after https://github.com/cockroachdb/cockroach/issues/49209
var knownGEOSPanics = map[string]struct{}{
	"GEOMETRYCOLLECTION (POINT, EMPTY) with POINT":           {},
	"GEOMETRYCOLLECTION (POINT, EMPTY) with DIFFERENT POINT": {},
}

var falseDWithinTestCases = map[string]struct{}{
	"GEOMETRYCOLLECTION (POINT, EMPTY) with POINT":           {},
	"GEOMETRYCOLLECTION (POINT, EMPTY) with DIFFERENT POINT": {},
}

var emptyDistanceTestCases = []struct {
	a string
	b string
}{
	{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY"},
	{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION (LINESTRING EMPTY)"},
	{"GEOMETRYCOLLECTION EMPTY", "POINT(1.0 1.0)"},
	{"POINT(1.0 1.0)", "GEOMETRYCOLLECTION EMPTY"},
}

func TestMinDistance(t *testing.T) {
	for _, tc := range distanceTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			// Try in both directions.
			ret, err := MinDistance(a, b)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMinDistance, ret)

			ret, err = MinDistance(b, a)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMinDistance, ret)

			// Check distance roughly the same as GEOS.
			if _, panicsInGEOS := knownGEOSPanics[tc.desc]; !panicsInGEOS {
				ret, err = geos.MinDistance(a.EWKB(), b.EWKB())
				require.NoError(t, err)
				require.LessOrEqualf(
					t,
					math.Abs(tc.expectedMinDistance-ret),
					0.0000001, // GEOS and PostGIS/CRDB can return results close by.
					"expected distance within %f, GEOS returns %f",
					tc.expectedMinDistance,
					ret,
				)
			}
		})
	}

	t.Run("errors for EMPTY geometries", func(t *testing.T) {
		for _, tc := range emptyDistanceTestCases {
			t.Run(fmt.Sprintf("%s to %s", tc.a, tc.b), func(t *testing.T) {
				a, err := geo.ParseGeometry(tc.a)
				require.NoError(t, err)
				b, err := geo.ParseGeometry(tc.b)
				require.NoError(t, err)
				_, err = MinDistance(a, b)
				require.Error(t, err)
				require.True(t, geo.IsEmptyGeometryError(err))
			})
		}
	})

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := MinDistance(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestMaxDistance(t *testing.T) {
	for _, tc := range distanceTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			// Try in both directions.
			ret, err := MaxDistance(a, b)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMaxDistance, ret)

			ret, err = MaxDistance(b, a)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMaxDistance, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := MinDistance(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestDWithin(t *testing.T) {
	for _, tc := range distanceTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			// empty geometries should always return false.
			expected := true
			if _, ok := falseDWithinTestCases[tc.desc]; ok {
				expected = false
			}

			for _, val := range []float64{
				tc.expectedMinDistance,
				tc.expectedMinDistance + 0.1,
				tc.expectedMinDistance + 1,
				tc.expectedMinDistance * 2,
			} {
				t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
					dwithin, err := DWithin(a, b, val, geo.FnInclusive)
					require.NoError(t, err)
					require.Equal(t, expected, dwithin)

					dwithin, err = DWithin(b, a, val, geo.FnInclusive)
					require.NoError(t, err)
					require.Equal(t, expected, dwithin)
				})
				t.Run(fmt.Sprintf("dwithinexclusive:%f", val), func(t *testing.T) {
					exclusiveExpected := expected
					if val == tc.expectedMinDistance {
						exclusiveExpected = false
					}
					dwithin, err := DWithin(a, b, val, geo.FnExclusive)
					require.NoError(t, err)
					require.Equal(t, exclusiveExpected, dwithin)

					dwithin, err = DWithin(b, a, val, geo.FnExclusive)
					require.NoError(t, err)
					require.Equal(t, exclusiveExpected, dwithin)
				})
			}

			for _, val := range []float64{
				tc.expectedMinDistance - 0.1,
				tc.expectedMinDistance - 1,
				tc.expectedMinDistance / 2,
			} {
				if val > 0 {
					t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
						dwithin, err := DWithin(a, b, val, geo.FnInclusive)
						require.NoError(t, err)
						require.False(t, dwithin)

						dwithin, err = DWithin(b, a, val, geo.FnInclusive)
						require.NoError(t, err)
						require.False(t, dwithin)
					})
					t.Run(fmt.Sprintf("dwithinexclusive:%f", val), func(t *testing.T) {
						dwithin, err := DWithin(a, b, val, geo.FnExclusive)
						require.NoError(t, err)
						require.False(t, dwithin)

						dwithin, err = DWithin(b, a, val, geo.FnExclusive)
						require.NoError(t, err)
						require.False(t, dwithin)
					})
				}
			}
		})
	}

	t.Run("returns false for EMPTY geometries", func(t *testing.T) {
		for _, tc := range emptyDistanceTestCases {
			t.Run(fmt.Sprintf("%s to %s", tc.a, tc.b), func(t *testing.T) {
				a, err := geo.ParseGeometry(tc.a)
				require.NoError(t, err)
				b, err := geo.ParseGeometry(tc.b)
				require.NoError(t, err)
				dwithin, err := DWithin(a, b, 0, geo.FnInclusive)
				require.NoError(t, err)
				require.False(t, dwithin)
			})
		}
	})

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := MinDistance(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})

	t.Run("errors if distance < 0", func(t *testing.T) {
		_, err := DWithin(geo.MustParseGeometry("POINT(1.0 2.0)"), geo.MustParseGeometry("POINT(3.0 4.0)"), -0.01, geo.FnInclusive)
		require.Error(t, err)
	})
}

func TestDFullyWithin(t *testing.T) {
	for _, tc := range distanceTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			// empty geometries should always return false.
			expected := true
			if _, ok := falseDWithinTestCases[tc.desc]; ok {
				expected = false
			}

			for _, val := range []float64{
				tc.expectedMaxDistance,
				tc.expectedMaxDistance + 0.1,
				tc.expectedMaxDistance + 1,
				tc.expectedMaxDistance * 2,
			} {
				t.Run(fmt.Sprintf("dfullywithin:%f", val), func(t *testing.T) {
					dfullywithin, err := DFullyWithin(a, b, val, geo.FnInclusive)
					require.NoError(t, err)
					require.Equal(t, expected, dfullywithin)

					dfullywithin, err = DFullyWithin(b, a, val, geo.FnInclusive)
					require.NoError(t, err)
					require.Equal(t, expected, dfullywithin)
				})
				t.Run(fmt.Sprintf("dfullywithinexclusive:%f", val), func(t *testing.T) {
					exclusiveExpected := expected
					if val == tc.expectedMaxDistance {
						exclusiveExpected = false
					}
					dfullywithin, err := DFullyWithin(a, b, val, geo.FnExclusive)
					require.NoError(t, err)
					require.Equal(t, exclusiveExpected, dfullywithin)

					dfullywithin, err = DFullyWithin(b, a, val, geo.FnExclusive)
					require.NoError(t, err)
					require.Equal(t, exclusiveExpected, dfullywithin)
				})
			}

			for _, val := range []float64{
				tc.expectedMaxDistance - 0.1,
				tc.expectedMaxDistance - 1,
				tc.expectedMaxDistance / 2,
			} {
				if val > 0 {
					t.Run(fmt.Sprintf("dfullywithin:%f", val), func(t *testing.T) {
						dfullywithin, err := DFullyWithin(a, b, val, geo.FnInclusive)
						require.NoError(t, err)
						require.False(t, dfullywithin)

						dfullywithin, err = DFullyWithin(b, a, val, geo.FnInclusive)
						require.NoError(t, err)
						require.False(t, dfullywithin)
					})
					t.Run(fmt.Sprintf("dfullywithinexclusive:%f", val), func(t *testing.T) {
						dfullywithin, err := DFullyWithin(a, b, val, geo.FnExclusive)
						require.NoError(t, err)
						require.False(t, dfullywithin)

						dfullywithin, err = DFullyWithin(b, a, val, geo.FnExclusive)
						require.NoError(t, err)
						require.False(t, dfullywithin)
					})
				}
			}
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := MinDistance(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})

	t.Run("errors if distance < 0", func(t *testing.T) {
		_, err := DWithin(geo.MustParseGeometry("POINT(1.0 2.0)"), geo.MustParseGeometry("POINT(3.0 4.0)"), -0.01, geo.FnInclusive)
		require.Error(t, err)
	})
}

func TestLongestLineString(t *testing.T) {
	for _, tc := range distanceTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			longestLineString, err := LongestLineString(a, b)
			require.NoError(t, err)
			expectedLongestLine, err := geo.ParseGeometry(tc.expectedLongestLine)
			require.NoError(t, err)
			require.Equal(t, expectedLongestLine, longestLineString)

			// Check length of longest line is same as expectedMaxLength.
			lengthOfLongestLine, err := Length(longestLineString)
			require.NoError(t, err)
			require.LessOrEqualf(
				t,
				math.Abs(lengthOfLongestLine-tc.expectedMaxDistance),
				0.0000001,
				"length of longest line %f, max distance between geometry's %f",
				lengthOfLongestLine,
				tc.expectedMaxDistance,
			)
		})
	}

	t.Run("returns error for EMPTY geometries", func(t *testing.T) {
		for _, tc := range emptyDistanceTestCases {
			t.Run(fmt.Sprintf("%s to %s", tc.a, tc.b), func(t *testing.T) {
				a, err := geo.ParseGeometry(tc.a)
				require.NoError(t, err)
				b, err := geo.ParseGeometry(tc.b)
				require.NoError(t, err)
				_, err = LongestLineString(a, b)
				require.Error(t, err)
				require.True(t, geo.IsEmptyGeometryError(err))
			})
		}
	})

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := LongestLineString(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestShortestLineString(t *testing.T) {
	for _, tc := range distanceTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			shortestLineString, err := ShortestLineString(a, b)
			require.NoError(t, err)

			expectedShortestLine, err := geo.ParseGeometry(tc.expectedShortestLine)
			require.NoError(t, err)
			require.Equal(t, shortestLineString, expectedShortestLine)

			lengthShortestLine, err := Length(shortestLineString)
			require.NoError(t, err)

			require.LessOrEqualf(
				t,
				math.Abs(lengthShortestLine-tc.expectedMinDistance),
				0.0000001,
				"length of shortest line %f, min distance between geometry's %f",
				lengthShortestLine,
				tc.expectedMinDistance,
			)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := ShortestLineString(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestFrechetDistance(t *testing.T) {
	pf := func(f float64) *float64 { return &f }

	testCases := []struct {
		a        string
		b        string
		expected *float64
	}{
		{"LINESTRING EMPTY", "LINESTRING EMPTY", nil},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING EMPTY", nil},
		{"LINESTRING EMPTY", "LINESTRING (0 0, 9 1, 2 2)", nil},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", pf(7.615773105863909)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v %v", tc.a, tc.b), func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			ret, err := FrechetDistance(a, b)
			require.NoError(t, err)
			if tc.expected != nil && ret != nil {
				require.Equal(t, *tc.expected, *ret)
			} else {
				require.Equal(t, tc.expected, ret)
			}
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := FrechetDistance(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestFrechetDistanceDensify(t *testing.T) {
	pf := func(f float64) *float64 { return &f }

	testCases := []struct {
		a           string
		b           string
		densifyFrac float64
		expected    *float64
	}{
		{"LINESTRING EMPTY", "LINESTRING EMPTY", 0.5, nil},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING EMPTY", 0.5, nil},
		{"LINESTRING EMPTY", "LINESTRING (0 0, 9 1, 2 2)", 0.5, nil},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", -1, pf(7.615773105863909)},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", -0.1, pf(7.615773105863909)},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", 0.0, pf(7.615773105863909)},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", 0.2, pf(6.627216610312356)},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", 0.4, pf(6.666666666666667)},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", 0.6, pf(6.670832032063167)},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", 0.8, pf(7.615773105863909)},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", 1.0, pf(7.615773105863909)},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%v %v densify %v", tc.a, tc.b, tc.densifyFrac), func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			ret, err := FrechetDistanceDensify(a, b, tc.densifyFrac)
			require.NoError(t, err)
			if tc.expected != nil && ret != nil {
				require.Equal(t, *tc.expected, *ret)
			} else {
				require.Equal(t, tc.expected, ret)
			}
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := FrechetDistanceDensify(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB, 0.5)
		requireMismatchingSRIDError(t, err)
	})

	errorTestCases := []struct {
		a           string
		b           string
		densifyFrac float64
	}{
		// Very small densifyFrac causes a SIGFPE in GEOS due to division-by-zero.
		// We explicitly disallow <1e-6 in the code, and test that both of these error.
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 1e-7},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 1e-19},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 1e-20},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 1e-100},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", 1e-19},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", 1e-20},
	}

	t.Run("errors on invalid densify fraction", func(t *testing.T) {
		for _, tc := range errorTestCases {
			tc := tc
			t.Run(fmt.Sprintf("%v %v densify %v", tc.a, tc.b, tc.densifyFrac), func(t *testing.T) {
				a, err := geo.ParseGeometry(tc.a)
				require.NoError(t, err)
				b, err := geo.ParseGeometry(tc.b)
				require.NoError(t, err)

				_, err = FrechetDistanceDensify(a, b, tc.densifyFrac)
				require.Error(t, err)
			})
		}
	})
}

func TestHausdorffDistance(t *testing.T) {
	pf := func(f float64) *float64 { return &f }

	testCases := []struct {
		a        string
		b        string
		expected *float64
	}{
		{"LINESTRING EMPTY", "LINESTRING EMPTY", nil},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING EMPTY", nil},
		{"LINESTRING EMPTY", "LINESTRING (0 0, 9 1, 2 2)", nil},
		{"LINESTRING (0 0, 3 7, 5 5)", "LINESTRING (0 0, 9 1, 2 2)", pf(5.656854249492381)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v %v", tc.a, tc.b), func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			ret, err := HausdorffDistance(a, b)
			require.NoError(t, err)
			if tc.expected != nil && ret != nil {
				require.Equal(t, *tc.expected, *ret)
			} else {
				require.Equal(t, tc.expected, ret)
			}
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := HausdorffDistance(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestHausdorffDistanceDensify(t *testing.T) {
	pf := func(f float64) *float64 { return &f }

	testCases := []struct {
		a        string
		b        string
		densify  float64
		expected *float64
	}{
		{"LINESTRING EMPTY", "LINESTRING EMPTY", 0.5, nil},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING EMPTY", 0.5, nil},
		{"LINESTRING EMPTY", "LINESTRING (10 10, 10 150, 130 10)", 0.5, nil},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 0.2, pf(66)},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 0.4, pf(56.66666666666667)},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 0.6, pf(70)},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 0.8, pf(14.142135623730951)},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 1.0, pf(14.142135623730951)},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%v %v densify %v", tc.a, tc.b, tc.densify), func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			ret, err := HausdorffDistanceDensify(a, b, tc.densify)
			require.NoError(t, err)
			if tc.expected != nil && ret != nil {
				require.Equal(t, *tc.expected, *ret)
			} else {
				require.Equal(t, tc.expected, ret)
			}
		})
	}

	errorTestCases := []struct {
		a       string
		b       string
		densify float64
	}{
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", -1},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", -0.1},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 0.0},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 0.0000001},
		{"LINESTRING (130 0, 0 0, 0 150)", "LINESTRING (10 10, 10 150, 130 10)", 1.1},
	}

	t.Run("errors on invalid densify fraction", func(t *testing.T) {
		for _, tc := range errorTestCases {
			tc := tc
			t.Run(fmt.Sprintf("%v %v densify %v", tc.a, tc.b, tc.densify), func(t *testing.T) {
				a, err := geo.ParseGeometry(tc.a)
				require.NoError(t, err)
				b, err := geo.ParseGeometry(tc.b)
				require.NoError(t, err)

				_, err = HausdorffDistanceDensify(a, b, tc.densify)
				require.Error(t, err)
			})
		}
	})

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := HausdorffDistanceDensify(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB, 0.5)
		requireMismatchingSRIDError(t, err)
	})
}

func TestClosestPoint(t *testing.T) {

	testCases := []struct {
		name     string
		geomA    string
		geomB    string
		expected string
	}{
		{"Closest point between a POINT and LINESTRING",
			"POINT(100 100)",
			"LINESTRING(20 80, 98 190, 110 180, 50 75 )",
			"POINT(100 100)",
		},
		{"Closest point between a LINESTRING and POINT",
			"LINESTRING(20 80, 98 190, 110 180, 50 75 )",
			"POINT(100 100)",
			"POINT(73.0769230769231 115.384615384615)",
		},
		{"Closest point between 2 POLYGONS",
			"POLYGON((175 150, 20 40, 50 60, 125 100, 175 150))",
			"POLYGON((15 50, 2 4, 5 6, 12 10, 15 50))",
			"POINT(20 40)",
		},
		{"Closest point between overlapping POLYGONS",
			"POLYGON((175 150, 20 40, 50 60, 125 100, 175 150))",
			"POLYGON((175 150, 20 40, 50 60, 125 100, 175 150))",
			"POINT(175 150)",
		},
		{"Closest point between partially-overlapping POLYGONS",
			"POLYGON((10 10, 14 14, 20 14, 20 10, 10 10))",
			"POLYGON((12 12, 16 12, 16 8, 12 8, 12 12))",
			"POINT(12 12)",
		},
		{"Closest point between MULTILINESTRING and POLYGON",
			"MULTILINESTRING((0 0, 1 1, 2 2),(3 3, 4 4, 5 5))",
			"POLYGON((10 10, 11 11, 14 11, 14 10, 10 10))",
			"POINT(5 5)",
		},
		{"Closest point between MULTILINESTRING and MULTIPOINT",
			"MULTILINESTRING((0 0, 1 1, 2 2),(3 3, 4 4, 5 5))",
			"MULTIPOINT((2 1),(10 10))",
			"POINT(1.5 1.5)",
		},
		{"Closest point between MULTIPOLYGON and MULTIPOINT",
			"MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)), ((-1 -1,-1 -2,-2 -2,-2 -1,-1 -1)))",
			"MULTIPOINT((20 10),(10 10))",
			"POINT(4 4)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gA, err := geo.ParseGeometry(tc.geomA)
			require.NoError(t, err)
			gB, err := geo.ParseGeometry(tc.geomB)
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)
			ret, err := ClosestPoint(gA, gB)
			require.NoError(t, err)

			requireGeometryWithinEpsilon(t, expected, ret, 2e-10)
		})
	}

	testCasesEmpty := []struct {
		name  string
		geomA string
		geomB string
	}{
		{"Closest point when both geometries are empty",
			"LINESTRING EMPTY",
			"LINESTRING EMPTY",
		},
		{"Closest point when first geometry is empty",
			"LINESTRING EMPTY",
			"POINT(100 100)",
		},
		{"Closest point when second geometry is empty",
			"POINT(100 100)",
			"LINESTRING EMPTY",
		},
	}

	t.Run("errors for EMPTY geometries", func(t *testing.T) {
		for _, tc := range testCasesEmpty {
			t.Run(tc.name, func(t *testing.T) {
				a, err := geo.ParseGeometry(tc.geomA)
				require.NoError(t, err)
				b, err := geo.ParseGeometry(tc.geomB)
				require.NoError(t, err)
				_, err = ClosestPoint(a, b)
				require.Error(t, err)
				require.True(t, geo.IsEmptyGeometryError(err))
			})
		}
	})

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := ClosestPoint(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}
