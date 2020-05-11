// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
)

var distanceTestCases = []struct {
	desc                     string
	a                        string
	b                        string
	expectedSphereDistance   float64
	expectedSpheroidDistance float64
}{
	{
		"POINT to itself",
		"POINT(1.0 1.0)",
		"POINT(1.0 1.0)",
		0,
		0,
	},
	{
		"POINT to POINT (CDG to LAX)",
		"POINT(-118.4079 33.9434)",
		"POINT(2.5559 49.0083)",
		9.103087983009e+06,
		9124665.27317673,
	},
	{
		"LINESTRING to POINT where POINT is on vertex",
		"LINESTRING(2.0 2.0, 3.0 3.0)",
		"POINT(3.0 3.0)",
		0,
		0,
	},
	{
		"LINESTRING to POINT where POINT is closest to a vertex",
		"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
		"POINT(5.0 5.0)",
		314116.2410064,
		313424.65220079,
	},
	{
		"LINESTRING to POINT where POINT is closer than the edge vertices",
		"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
		"POINT(2.4 2.6)",
		15695.12116722,
		15660.43959933,
	},
	{
		"LINESTRING to POINT on the line",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0, 3.0 3.0)",
		"POINT(1.5 1.5001714)",
		0,
		0,
	},
	{
		"POLYGON to POINT inside the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POINT(0.5 0.5)",
		0,
		0,
	},
	{
		"POLYGON to POINT on vertex of the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POINT(1.0 1.0)",
		0,
		0,
	},
	{
		"POLYGON to POINT outside the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POINT(1.5 1.6)",
		86836.81014284,
		86591.2400406,
	},
	{
		"POLYGON to POINT where POINT is inside a hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"POINT(0.3 0.3)",
		11119.35554984,
		11131.79750667, // 11057.396042 is the truer min distance, see "updateMinDistance".
	},
	{
		"LINESTRING to intersecting LINESTRING",
		"LINESTRING(0.0 0.0, 1.0 1.0)",
		"LINESTRING(0.0 1.0, 1.0 0.0)",
		0,
		0,
	},
	{
		"LINESTRING to faraway LINESTRING",
		"LINESTRING(0.0 0.0, 1.0 1.0)",
		"LINESTRING(5.0 5.0, 6.0 6.0)",
		628519.03378753,
		627129.50261075,
	},
	{
		"LINESTRING to intersecting POLYGON where LINESTRING is inside the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"LINESTRING(0.1 0.1, 0.15 0.15)",
		0,
		0,
	},
	{
		"LINESTRING to intersecting POLYGON with a hole where LINESTRING is inside the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"LINESTRING(0.1 0.1, 0.15 0.15)",
		0,
		0,
	},
	{
		"LINESTRING to intersecting POLYGON where LINESTRING is outside the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"LINESTRING(-1.0 -1.0, 1.0 1.0)",
		0,
		0,
	},
	{
		"LINESTRING to POLYGON inside its hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"LINESTRING(0.25 0.25, 0.35 0.35)",
		5559.65025416,
		5565.87138621, // 5528.689389 is the truer min distance, see "updateMinDistance".
	},
	{
		"LINESTRING to POLYGON inside its hole but intersects through hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"LINESTRING(0.25 0.25, 0.6 0.6)",
		0,
		0,
	},
	{
		"LINESTRING to faraway POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"LINESTRING(7.0 7.0, 5.0 5.0)",
		628519.03378753,
		627129.50261075,
	},
	{
		"POLYGON to intersecting POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((-1.0 0.0, 1.0 0.0, 1.0 1.0, -1.0 1.0, -1.0 0.0))",
		0,
		0,
	},
	{
		"POLYGON to POLYGON completely inside its hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"POLYGON((0.25 0.25, 0.35 0.25, 0.35 0.35, 0.25 0.35, 0.25 0.25))",
		5559.65025416,
		5565.87138621,
	},
	{
		"POLYGON to POLYGON intersecting through its hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"POLYGON((0.15 0.25, 0.35 0.25, 0.35 0.35, 0.25 0.35, 0.15 0.25))",
		0,
		0,
	},
	{
		"POLYGON to faraway POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((-8.0 -8.0, -4.0 -8.0, -4.0 -4.0, -8.0 -4.0, -8.0 -8.0))",
		628758.66301809,
		627363.8420706,
	},
	{
		"MULTIPOINT to MULTIPOINT",
		"MULTIPOINT((1.0 1.0), (2.0 2.0))",
		"MULTIPOINT((2.5 2.5), (3.0 3.0))",
		78596.36618378,
		78421.9811006,
	},
	{
		"MULTIPOINT to MULTILINESTRING",
		"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1))",
		"MULTIPOINT(2.0 2.0, 1.0 1.0, 3.0 3.0)",
		0,
		0,
	},
	{
		"MULTIPOINT to MULTIPOLYGON",
		"MULTIPOINT ((2.0 3.0), (10 42))",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		217957.10767526,
		217713.9665776,
	},
	{
		"MULTILINESTRING to MULTILINESTRING",
		"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1), (3.0 3.0, 3.1 3.1))",
		"MULTILINESTRING((2.0 2.0, 2.1 2.1), (4.0 3.0, 3.1 3.1))",
		0,
		0,
	},
	{
		"MULTILINESTRING to MULTIPOLYGON",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		"MULTILINESTRING((3 3, -4 -4), (45 41, 48 48, 52 52))",
		108979.20910668,
		108848.28520095,
	},
	{
		"MULTIPOLYGON to MULTIPOLYGON",
		"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
		"MULTIPOLYGON (((30 20, 45 40, 15 45, 30 20)))",
		0,
		0,
	},
}

func TestDistance(t *testing.T) {
	for _, tc := range distanceTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeography(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeography(tc.b)
			require.NoError(t, err)

			// Allow a 1cm margin of error for results.
			for _, subTC := range []struct {
				desc                string
				expected            float64
				useSphereOrSpheroid UseSphereOrSpheroid
			}{
				{"sphere", tc.expectedSphereDistance, UseSphere},
				{"spheroid", tc.expectedSpheroidDistance, UseSpheroid},
			} {
				t.Run(subTC.desc, func(t *testing.T) {
					distance, err := Distance(a, b, subTC.useSphereOrSpheroid)
					require.NoError(t, err)
					require.LessOrEqualf(
						t,
						math.Abs(subTC.expected-distance),
						0.01,
						"expected %f, got %f",
						subTC.expected,
						distance,
					)
					distance, err = Distance(b, a, subTC.useSphereOrSpheroid)
					require.NoError(t, err)
					require.LessOrEqualf(
						t,
						math.Abs(subTC.expected-distance),
						0.01,
						"expected %f, got %f",
						subTC.expected,
						distance,
					)
				})
			}
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Distance(mismatchingSRIDGeographyA, mismatchingSRIDGeographyB, UseSpheroid)
		requireMismatchingSRIDError(t, err)
	})
}
