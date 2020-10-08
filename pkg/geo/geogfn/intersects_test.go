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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
)

func TestIntersects(t *testing.T) {
	testCases := []struct {
		desc     string
		a        string
		b        string
		expected bool
	}{
		{
			"POINTs which are the same intersect",
			"POINT(1.0 1.0)",
			"POINT(1.0 1.0)",
			true,
		},
		{
			"POINTs which are different do not intersect",
			"POINT(1.0 1.0)",
			"POINT(1.0 1.1)",
			false,
		},
		{
			"POINTs intersect with a vertex on a LINESTRING",
			"POINT(2.0 2.0)",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			true,
		},
		{
			"POINTs intersect with a point along the LINESTRING",
			"POINT(1.5 1.5001714)",
			"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0, 3.0 3.0)",
			true,
		},
		{
			"POINTs do not intersect with point outside the LINESTRING",
			"POINT(1.5 1.6001714)",
			"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0, 3.0 3.0)",
			false,
		},
		{
			"POINTs intersect with vertex along a POLYGON",
			"POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))",
			"POINT(2.0 2.0)",
			true,
		},
		{
			"POINTs intersect with edge along a POLYGON",
			"POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))",
			"POINT(1.5 1.5001714)",
			true,
		},
		{
			"POINTs intersect inside a POLYGON",
			"POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))",
			"POINT(1.5 1.9)",
			true,
		},
		{
			"POINTs do not intersect with any hole inside the polygon",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
			"POINT(0.3 0.3)",
			false,
		},
		{
			"LINESTRING intersects itself",
			"LINESTRING(2.0 2.0, 3.0 3.0)",
			"LINESTRING(2.0 2.0, 3.0 3.0)",
			true,
		},
		{
			"LINESTRING intersects LINESTRING at ends",
			"LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)",
			"LINESTRING(2.0 2.0, 3.0 3.0)",
			true,
		},
		{
			"LINESTRING intersects LINESTRING subline not at vertex",
			"LINESTRING(1.0 1.0, 2.0 2.0)",
			"LINESTRING(1.5499860 1.5501575, 3.0 3.0)",
			true,
		},
		{
			"LINESTRING intersects LINESTRING with subline completely inside LINESTRING",
			"LINESTRING(1.0 1.0, 2.0 2.0)",
			"LINESTRING(1.5499860 1.5501575, 1.5 1.5001714)",
			true,
		},
		{
			"LINESTRING intersects LINESTRING at some edge",
			"LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)",
			"LINESTRING(1.0 2.0, 2.0 1.0)",
			true,
		},
		{
			"LINESTRING does not intersect LINESTRING",
			"LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)",
			"LINESTRING(1.0 2.0, 1.0 4.0)",
			false,
		},
		{
			"LINESTRING intersects POLYGON at a vertex",
			"LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			true,
		},
		{
			"LINESTRING contained within POLYGON",
			"LINESTRING(0.2 0.2, 0.4 0.4)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			true,
		},
		{
			"LINESTRING intersects POLYGON at an edge",
			"LINESTRING(-0.5 0.5, 0.5 0.5)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			true,
		},
		{
			"LINESTRING intersects POLYGON through the whole thing",
			"LINESTRING(-0.5 0.5, 1.5 0.5)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			true,
		},
		{
			"LINESTRING missing the POLYGON does not intersect",
			"LINESTRING(-0.5 0.5, -1.5 -1.5)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			false,
		},
		{
			"LINESTRING does not intersect POLYGON if contained within the hole",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
			"LINESTRING(0.3 0.3, 0.35 0.35)",
			false,
		},
		{
			"LINESTRING intersects POLYGON through the hole",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
			"LINESTRING(0.3 0.3, 0.5 0.5)",
			true,
		},
		{
			"LINESTRING intersects POLYGON through touching holes",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.3 0.3, 0.4 0.2, 0.5 0.3, 0.4 0.4, 0.3 0.3), (0.5 0.3, 0.6 0.2, 0.7 0.3, 0.6 0.4, 0.5 0.3))",
			"LINESTRING(0.4 0.3, 0.6 0.3)",
			true,
		},
		{
			"LINESTRING intersects POINT across the longitudinal boundary",
			"LINESTRING(179 0, -179 0)",
			"POINT(179.1 0)",
			true,
		},
		{
			"reversed LINESTRING intersects POINT across the longitudinal boundary",
			"LINESTRING(-179 0, 179 0)",
			"POINT(179.1 0)",
			true,
		},
		{
			"LINESTRING does not intersect POINT with linestring crossing the longitudinal boundary but POINT on the other side",
			"LINESTRING(179 0, -179 0)",
			"POINT(170 0)",
			false,
		},
		{
			"reversed LINESTRING does not intersect POINT with linestring crossing the longitudinal boundary but POINT on the other side",
			"LINESTRING(-179 0, 179 0)",
			"POINT(170 0)",
			false,
		},
		{
			"POLYGON intersects POINT lying inside latitudinal boundary",
			"POLYGON((150 85, 160 85, -20 85, -30 85, 150 85))",
			"POINT (150 88)",
			true,
		},
		{
			"POLYGON does not intersect POINT lying outside latitudinal boundary",
			"POLYGON((150 85, 160 85, -20 85, -30 85, 150 85))",
			"POINT (170 88)",
			false,
		},
		{
			"POLYGON intersects itself",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			true,
		},
		{
			"POLYGON intersects a window of itself (with edges touches)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))",
			true,
		},
		{
			"POLYGON intersects a nested version of itself",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))",
			true,
		},
		{
			"POLYGON intersects POLYGON intersecting",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((-1.0 0.0, 1.0 0.0, 1.0 1.0, -1.0 1.0, -1.0 0.0))",
			true,
		},
		{
			"POLYGON does not intersect POLYGON totally out of range",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))",
			false,
		},
		{
			"MULTIPOINT intersects single MULTIPOINT",
			"MULTIPOINT((1.0 1.0), (2.0 2.0))",
			"MULTIPOINT((2.0 2.0))",
			true,
		},
		{
			"MULTILINESTRING intersects one of the MULTIPOINTs",
			"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1))",
			"MULTIPOINT((4.0 5.0), (66.0 66.0), (2.1 2.1))",
			true,
		},
		{
			"MULTILINESTRING does not intersect with MULTIPOINTS out of the range",
			"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1))",
			"MULTIPOINT((55.0 55.0), (66.0 66.0))",
			false,
		},
		{
			"MULTILINESTRING intersects MULTILINESTRING",
			"MULTILINESTRING((1.0 1.0, 2.0 2.0), (2.0 2.0, 2.1 2.1), (3.0 3.0, 3.1 3.1))",
			"MULTILINESTRING((0.0 0.5001714, -1.0 -1.0), (1.5 1.5001714, 2.0 2.0))",
			true,
		},
		{
			"MULTILINESTRING does not intersect all MULTILINESTRING",
			"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1), (3.0 3.0, 3.1 3.1))",
			"MULTILINESTRING((25.0 25.0, 25.1 25.1), (4.0 3.0, 3.12 3.12))",
			false,
		},
		{
			"MULTIPOLYGON intersects MULTIPOINT",
			"MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
			"MULTIPOINT((30 20), (30 30))",
			true,
		},
		{
			"MULTIPOLYGON does not intersect MULTIPOINT",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTIPOINT((30 -20), (-30 30), (45 66))",
			false,
		},
		{
			"MULTIPOLYGON intersects MULTILINESTRING",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTILINESTRING((65 40, 60 40), (45 40, 10 40, 30 20))",
			true,
		},
		{
			"MULTIPOLYGON does not intersect MULTILINESTRING",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTILINESTRING((45 41, 60 80), (-45 -40, -10 -40))",
			false,
		},
		{
			"MULTIPOLYGON intersects MULTIPOLYGON",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTIPOLYGON (((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0)), ((30 20, 45 40, 10 40, 30 20)))",
			true,
		},
		{
			"MULTIPOLYGON does not intersect MULTIPOLYGON",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTIPOLYGON (((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0)))",
			false,
		},
		{
			"GEOMETRYCOLLECTION EMPTY do not intersect with each other",
			"GEOMETRYCOLLECTION EMPTY",
			"GEOMETRYCOLLECTION EMPTY",
			false,
		},
		{
			"GEOMETRYCOLLECTION EMPTY do not intersect with a point",
			"POINT(1.0 2.0)",
			"GEOMETRYCOLLECTION EMPTY",
			false,
		},
		{
			"GEOMETRYCOLLECTION EMPTY and POINT intersect",
			"POINT(1.0 2.0)",
			"GEOMETRYCOLLECTION (LINESTRING EMPTY, POINT(1.0 2.0))",
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeography(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeography(tc.b)
			require.NoError(t, err)

			intersects, err := Intersects(a, b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, intersects)

			// Relationships should be reciprocal.
			intersects, err = Intersects(b, a)
			require.NoError(t, err)
			require.Equal(t, tc.expected, intersects)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Intersects(mismatchingSRIDGeographyA, mismatchingSRIDGeographyB)
		requireMismatchingSRIDError(t, err)
	})
}
