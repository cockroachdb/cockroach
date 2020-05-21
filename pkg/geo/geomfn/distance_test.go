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
	desc                string
	a                   string
	b                   string
	expectedMinDistance float64
	expectedMaxDistance float64
}{
	{
		"Same POINTs",
		"POINT(1.0 1.0)",
		"POINT(1.0 1.0)",
		0,
		0,
	},
	{
		"Different POINTs",
		"POINT(1.0 1.0)",
		"POINT(2.0 1.0)",
		1,
		1,
	},
	{
		"POINT on LINESTRING",
		"POINT(0.5 0.5)",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		0,
		2.1213203435596424,
	},
	{
		"POINT away from LINESTRING",
		"POINT(3.0 3.0)",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		1.4142135623730951,
		4.242640687119285,
	},
	{
		"Same LINESTRING",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		0,
		2.8284271247461903,
	},
	{
		"Intersecting LINESTRING",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
		"LINESTRING(0.5 0.0, 0.5 3.0)",
		0,
		3.0413812651491097,
	},
	{
		"LINESTRING does not meet",
		"LINESTRING(6.0 6.0, 7.0 7.0, 8.0 8.0)",
		"LINESTRING(0.0 0.0, 3.0 -3.0)",
		8.48528137423857,
		12.083045973594572,
	},
	{
		"POINT in POLYGON",
		"POINT(0.5 0.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0,
		0.7071067811865476,
	},
	{
		"POINT in POLYGON hole",
		"POINT(0.5 0.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.6 0.2, 0.6 0.6, 0.2 0.6, 0.2 0.2))",
		0.09999999999999998,
		0.7071067811865476,
	},
	{
		"POINT not in POLYGON hole",
		"POINT(0.1 0.1)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.6 0.2, 0.6 0.6, 0.2 0.6, 0.2 0.2))",
		0,
		1.2727922061357855,
	},
	{
		"POINT outside of POLYGON",
		"POINT(1.5 1.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0.7071067811865476,
		2.1213203435596424,
	},
	{
		"LINESTRING intersects POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"LINESTRING(-0.5 -0.5, 0.5 0.5)",
		0,
		2.1213203435596424,
	},
	{
		"LINESTRING outside of POLYGON",
		"LINESTRING(-0.5 -0.5, -0.5 0.5)",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0.5,
		2.1213203435596424,
	},
	{
		"POLYGON is the same",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		0,
		1.4142135623730951,
	},
	{
		"POLYGON inside POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
		0,
		1.2727922061357855,
	},
	{
		"POLYGON to POLYGON intersecting through its hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"POLYGON((0.15 0.25, 0.35 0.25, 0.35 0.35, 0.25 0.35, 0.15 0.25))",
		0,
		1.1335784048754634,
	},
	{
		"POLYGON inside POLYGON hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
		"POLYGON((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))",
		0.09999999999999998,
		1.1313708498984762,
	},
	{
		"POLYGON outside POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))",
		2.8284271247461903,
		5.656854249492381,
	},
	{
		"MULTIPOINT to MULTIPOINT",
		"MULTIPOINT((1.0 1.0), (2.0 2.0))",
		"MULTIPOINT((2.5 2.5), (3.0 3.0))",
		0.7071067811865476,
		2.8284271247461903,
	},
	{
		"MULTIPOINT to MULTILINESTRING",
		"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1))",
		"MULTIPOINT(2.0 2.0, 1.0 1.0, 3.0 3.0)",
		0,
		2.8284271247461903,
	},
	{
		"MULTIPOINT to MULTIPOLYGON",
		"MULTIPOINT ((2.0 3.0), (10 42))",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		2,
		56.72741841473134,
	},
	{
		"MULTILINESTRING to MULTILINESTRING",
		"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1), (3.0 3.0, 3.1 3.1))",
		"MULTILINESTRING((2.0 2.0, 2.1 2.1), (4.0 3.0, 3.1 3.1))",
		0,
		3.605551275463989,
	},
	{
		"MULTILINESTRING to MULTIPOLYGON",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		"MULTILINESTRING((3 3, -4 -4), (45 41, 48 48, 52 52))",
		1,
		65.85590330410783,
	},
	{
		"MULTIPOLYGON to MULTIPOLYGON",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		"MULTIPOLYGON (((30 20, 45 40, 15 45, 30 20)))",
		0,
		50,
	},
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
		})
	}

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

			for _, val := range []float64{
				tc.expectedMinDistance,
				tc.expectedMinDistance + 0.1,
				tc.expectedMinDistance + 1,
				tc.expectedMinDistance * 2,
			} {
				t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
					dwithin, err := DWithin(a, b, val)
					require.NoError(t, err)
					require.True(t, dwithin)

					dwithin, err = DWithin(a, b, val)
					require.NoError(t, err)
					require.True(t, dwithin)
				})
			}

			for _, val := range []float64{
				tc.expectedMinDistance - 0.1,
				tc.expectedMinDistance - 1,
				tc.expectedMinDistance / 2,
			} {
				if val > 0 {
					t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
						dwithin, err := DWithin(a, b, val)
						require.NoError(t, err)
						require.False(t, dwithin)

						dwithin, err = DWithin(a, b, val)
						require.NoError(t, err)
						require.False(t, dwithin)
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
		_, err := DWithin(geo.MustParseGeometry("POINT(1.0 2.0)"), geo.MustParseGeometry("POINT(3.0 4.0)"), -0.01)
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

			for _, val := range []float64{
				tc.expectedMaxDistance,
				tc.expectedMaxDistance + 0.1,
				tc.expectedMaxDistance + 1,
				tc.expectedMaxDistance * 2,
			} {
				t.Run(fmt.Sprintf("dfullywithin:%f", val), func(t *testing.T) {
					dwithax, err := DFullyWithin(a, b, val)
					require.NoError(t, err)
					require.True(t, dwithax)

					dwithax, err = DFullyWithin(a, b, val)
					require.NoError(t, err)
					require.True(t, dwithax)
				})
			}

			for _, val := range []float64{
				tc.expectedMaxDistance - 0.1,
				tc.expectedMaxDistance - 1,
				tc.expectedMaxDistance / 2,
			} {
				if val > 0 {
					t.Run(fmt.Sprintf("dfullywithin:%f", val), func(t *testing.T) {
						dwithin, err := DFullyWithin(a, b, val)
						require.NoError(t, err)
						require.False(t, dwithin)

						dwithin, err = DFullyWithin(a, b, val)
						require.NoError(t, err)
						require.False(t, dwithin)
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
		_, err := DWithin(geo.MustParseGeometry("POINT(1.0 2.0)"), geo.MustParseGeometry("POINT(3.0 4.0)"), -0.01)
		require.Error(t, err)
	})
}
