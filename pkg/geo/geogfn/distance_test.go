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
	desc                   string
	a                      string
	b                      string
	expectedSphereDistance float64
}{
	{
		"POINT to itself",
		"POINT(1.0 1.0)",
		"POINT(1.0 1.0)",
		0,
	},
	{
		"POINT to POINT (CDG to LAX)",
		"POINT(-118.4079 33.9434)",
		"POINT(2.5559 49.0083)",
		9.103087983009e+06,
	},
	{
		"LINESTRING to POINT where POINT is on vertex",
		"LINESTRING(2.0 2.0, 3.0 3.0)",
		"POINT(3.0 3.0)",
		0,
	},
	{
		"LINESTRING to POINT where POINT is closest to a vertex",
		"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
		"POINT(5.0 5.0)",
		314116.2410064,
	},
	{
		"LINESTRING to POINT where POINT is closer than the edge vertices",
		"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
		"POINT(2.4 2.6)",
		15695.12116722,
	},
	{
		"LINESTRING to POINT on the line",
		"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0, 3.0 3.0)",
		"POINT(1.5 1.5001714)",
		0,
	},
	{
		"POLYGON to POINT inside the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POINT(0.5 0.5)",
		0,
	},
	{
		"POLYGON to POINT on vertex of the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POINT(1.0 1.0)",
		0,
	},
	{
		"POLYGON to POINT outside the polygon",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POINT(1.5 1.6)",
		86836.81014284,
	},
	{
		"POLYGON to POINT where POINT is inside a hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"POINT(0.3 0.3)",
		11119.35554984,
	},
	{
		"LINESTRING to intersecting LINESTRING",
		"LINESTRING(0.0 0.0, 1.0 1.0)",
		"LINESTRING(0.0 1.0, 1.0 0.0)",
		0,
	},
	{
		"LINESTRING to faraway LINESTRING",
		"LINESTRING(0.0 0.0, 1.0 1.0)",
		"LINESTRING(5.0 5.0, 6.0 6.0)",
		628519.03378753,
	},
	{
		"LINESTRING to intersecting POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"LINESTRING(-1.0 -1.0, 1.0 1.0)",
		0,
	},
	{
		"LINESTRING to POLYGON inside it's hole",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"LINESTRING(0.25 0.25, 0.35 0.35)",
		5559.65025416,
	},
	{
		"LINESTRING to faraway POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
		"LINESTRING(7.0 7.0, 5.0 5.0)",
		628519.03378753,
	},
	{
		"POLYGON to intersecting POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((-1.0 0.0, 1.0 0.0, 1.0 1.0, -1.0 1.0, -1.0 0.0))",
		0,
	},
	{
		"POLYGON to faraway POLYGON",
		"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
		"POLYGON((-8.0 -8.0, -4.0 -8.0, -4.0 -4.0, -8.0 -4.0, -8.0 -8.0))",
		628758.66301809,
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
			t.Run("sphere", func(t *testing.T) {
				distance, err := Distance(a, b)
				require.NoError(t, err)
				require.LessOrEqualf(
					t,
					math.Abs(tc.expectedSphereDistance-distance),
					0.01,
					"expected %f, got %f",
					tc.expectedSphereDistance,
					distance,
				)
				distance, err = Distance(b, a)
				require.NoError(t, err)
				require.LessOrEqualf(
					t,
					math.Abs(tc.expectedSphereDistance-distance),
					0.01,
					"expected %f, got %f",
					tc.expectedSphereDistance,
					distance,
				)
			})
		})
	}
}
