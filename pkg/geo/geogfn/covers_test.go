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

func TestCovers(t *testing.T) {
	testCases := []struct {
		desc     string
		a        string
		b        string
		expected bool
	}{
		{
			"POINT covers the same POINT",
			"POINT(1.0 1.0)",
			"POINT(1.0 1.0)",
			true,
		},
		{
			"POINT does not cover different POINT",
			"POINT(1.0 1.0)",
			"POINT(1.0 1.1)",
			false,
		},
		{
			"POINT does not cover different LINESTRING",
			"POINT(1.0 1.0)",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			false,
		},
		{
			"POINT does not cover different POLYGON",
			"POINT(1.0 1.0)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			false,
		},
		{
			"LINESTRING covers POINT at the start",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"POINT(1.0 1.0)",
			true,
		},
		{
			"LINESTRING covers POINT at the middle",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"POINT(2.0 2.0)",
			true,
		},
		{
			"LINESTRING covers POINT at the end",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"POINT(3.0 3.0)",
			true,
		},
		{
			"LINESTRING covers POINT in between",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"POINT(1.5 1.5001714)",
			true,
		},
		{
			"LINESTRING does not cover any POINT out of the way",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"POINT(2.0 4.0)",
			false,
		},
		{
			"LINESTRING does not cover longer LINESTRING",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)",
			false,
		},
		{
			"LINESTRING does not cover different LINESTRING",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"LINESTRING(5.0 5.0, 4.0 4.0)",
			false,
		},
		{
			"LINESTRING covers itself",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			true,
		},
		{
			"LINESTRING covers itself in reverse",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)",
			"LINESTRING(4.0 4.0, 3.0 3.0, 2.0 2.0, 1.0 1.0)",
			true,
		},
		{
			"LINESTRING covers subline of itself",
			"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"LINESTRING(1.5 1.5001714, 2.0 2.0)",
			true,
		},
		{
			"LINESTRING does not cover subline of with a tail in b",
			"LINESTRING(1.0 1.0, 2.0 2.0)",
			"LINESTRING(1.5 1.5001714, 2.0 2.0, 2.5 2.5)",
			false,
		},
		{
			"LINESTRING covers subline of itself but veers off",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"LINESTRING(1.5 1.5001714, 2.0 2.0, 2.5 2.5)",
			false,
		},
		{
			"LINESTRING covers LINESTRING with subline missing a vertex in A",
			"LINESTRING(0.999 0.999, 1.0 1.0, 1.01 1.01)",
			"LINESTRING(0.9995 0.9995, 1.005 1.005)",
			true,
		},
		{
			"LINESTRING does not cover LINESTRING with subline covering some point in A but B extends past A",
			"LINESTRING(0.9 0.9, 0.999 0.999, 1.0 1.0)",
			"LINESTRING(0.999 0.999, 0.9995 0.9995, 1.005 1.005)",
			false,
		},
		{
			"LINESTRING covers subline of itself from the beginning",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"LINESTRING(1.0 1.0, 1.5 1.5001714, 2.0 2.0, 2.5 2.5002856, 3.0 3.0)",
			true,
		},
		{
			"LINESTRING covers subline of itself from the beginning (swapped a and b)",
			"LINESTRING(1.0 1.0, 1.5 1.5001714, 2.0 2.0, 2.5 2.5002856, 3.0 3.0)",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			true,
		},
		{
			"LINESTRING covers subline of itself from the beginning (reversed)",
			"LINESTRING(1.0 1.0, 1.5 1.5001714, 2.0 2.0, 2.5 2.5002856, 3.0 3.0)",
			"LINESTRING(3.0 3.0, 2.0 2.0, 1.0 1.0)",
			true,
		},
		{
			"LINESTRING covers a substring of itself",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"LINESTRING(1.0 1.0, 2.0 2.0)",
			true,
		},
		{
			"LINESTRING covers a substring of itself",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			"LINESTRING(2.0 2.0, 3.0 3.0)",
			true,
		},
		{
			"LINESTRING covers a substring of itself",
			"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)",
			"LINESTRING(2.0 2.0, 3.0 3.0)",
			true,
		},
		{
			"LINESTRING does not cover any POLYGON",
			"POINT(1.0 1.0)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			false,
		},
		{
			"POLYGON covers POINT inside",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POINT(0.5 0.5)",
			true,
		},
		{
			"POLYGON does not cover POINT outside",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POINT(0.5 5.5)",
			false,
		},
		{
			"POLYGON does not cover POINT in hole",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
			"POINT(0.3 0.3)",
			false,
		},
		{
			"POLYGON covers POINT on vertex",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POINT(1.0 0.0)",
			true,
		},
		{
			"POLYGON covers POINT on edge of vertex",
			"POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))",
			"POINT(1.5 1.5001714)",
			true,
		},
		{
			"POLYGON covers LINESTRING inside the POLYGON",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"LINESTRING(0.1 0.1, 0.4 0.4)",
			true,
		},
		{
			"POLYGON does not cover LINESTRING in hole",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
			"LINESTRING(0.3 0.3, 0.35 0.35)",
			false,
		},
		{
			"POLYGON does not cover LINESTRING intersecting with hole",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
			"LINESTRING(0.3 0.3, 0.55 0.55)",
			false,
		},
		{
			"POLYGON does not cover LINESTRING intersecting with hole and outside",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2))",
			"LINESTRING(0.3 0.3, 4.0 4.0)",
			false,
		},
		{
			"POLYGON does not cover LINESTRING outside the POLYGON",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"LINESTRING(-1 -1, -2 -2)",
			false,
		},
		{
			"POLYGON does not cover LINESTRING intersecting the POLYGON",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"LINESTRING(-0.5 -0.5, 0.5 0.5)",
			false,
		},
		{
			"POLYGON covers itself",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			true,
		},
		{
			"POLYGON covers a window of itself, intersecting with the edges",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))",
			true,
		},
		{
			"POLYGON covers a nested version of itself",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))",
			true,
		},
		{
			"POLYGON does not cover POLYGON intersecting",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((-1.0 0.0, 1.0 0.0, 1.0 1.0, -1.0 1.0, -1.0 0.0))",
			false,
		},
		{
			"POLYGON does not cover POLYGON totally out of range",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))",
			false,
		},
		{
			"MULTIPOINT covers single MULTIPOINT",
			"MULTIPOINT((1.0 1.0), (2.0 2.0))",
			"MULTIPOINT((2.0 2.0))",
			true,
		},
		{
			"MULTIPOINT not covered by multiple MULTI POINTs",
			"MULTIPOINT((1.0 1.0))",
			"MULTIPOINT((1.0 1.0), (2.0 2.0))",
			false,
		},
		{
			"MULTILINESTRING covers MULTIPOINTs",
			"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1))",
			"MULTIPOINT(2.0 2.0, 2.1 2.1)",
			true,
		},
		{
			"MULTILINESTRING does not cover all MULTIPOINTs",
			"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1))",
			"MULTIPOINT(2.0 2.0, 1.0 1.0, 3.0 3.0)",
			false,
		},
		{
			"MULTILINESTRING covers all MULTILINESTRING",
			"MULTILINESTRING((1.0 1.0, 2.0 2.0), (2.0 2.0, 2.1 2.1), (3.0 3.0, 3.1 3.1))",
			"MULTILINESTRING((1.0 1.0, 1.5 1.5001714), (1.5 1.5001714, 2.0 2.0))",
			true,
		},
		{
			"MULTILINESTRING does not cover all MULTILINESTRING",
			"MULTILINESTRING((1.0 1.0, 1.1 1.1), (2.0 2.0, 2.1 2.1), (3.0 3.0, 3.1 3.1))",
			"MULTILINESTRING((2.0 2.0, 2.1 2.1), (4.0 3.0, 3.1 3.1))",
			false,
		},
		{
			"MULTIPOLYGON covers MULTIPOINT",
			"MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
			"MULTIPOINT((30 20), (30 30))",
			true,
		},
		{
			"MULTIPOLYGON does not cover MULTIPOINT",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTIPOINT((30 20), (30 30), (45 66))",
			false,
		},
		{
			"MULTIPOLYGON does not cover MULTIPOINT",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTIPOINT((30 20), (30 30), (45 66))",
			false,
		},
		{
			"MULTIPOLYGON does not cover MULTILINESTRING intersecting at the edge (known edge case *should* return true)",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTILINESTRING((45 40, 10 40), (45 40, 10 40, 30 20))",
			false,
		},
		{
			"MULTIPOLYGON does not cover MULTILINESTRING",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTILINESTRING((45 40, 10 40), (45 40, 10 40, 30 11))",
			false,
		},
		{
			"MULTIPOLYGON covers MULTIPOLYGON",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))",
			true,
		},
		{
			"MULTIPOLYGON does not cover MULTIPOLYGON",
			"MULTIPOLYGON (((15 5, 40 10, 10 20, 5 10, 15 5)),((30 20, 45 40, 10 40, 30 20)))",
			"MULTIPOLYGON (((30 20, 45 40, 15 45, 30 20)))",
			false,
		},
		{
			"multiple MULTIPOLYGONs required to cover MULTIPOINTS",
			"MULTIPOLYGON(((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0)), ((1.0 0.0, 2.0 0.0, 2.0 1.0, 1.0 1.0, 1.0 0.0)))",
			"MULTIPOINT((0.5 0.5), (1.5 0.5))",
			true,
		},
		{
			"EMPTY GEOMETRYCOLLECTION does not cover itself",
			"GEOMETRYCOLLECTION EMPTY",
			"GEOMETRYCOLLECTION EMPTY",
			false,
		},
		{
			"nothing covers an empty GEOMETRYCOLLECTION",
			"POINT(1.0 1.0)",
			"GEOMETRYCOLLECTION EMPTY",
			false,
		},
		{
			"nothing covers a GEOMETRYCOLLECTION with an EMPTY element",
			"POINT(1.0 1.0)",
			"GEOMETRYCOLLECTION EMPTY",
			false,
		},
		{
			"empty collection contains point which covers another",
			"GEOMETRYCOLLECTION(LINESTRING EMPTY, POINT(1.0 2.0))",
			"POINT(1.0 2.0)",
			true,
		},
		{
			"LINESTRING covers POINT across the longitudinal boundary",
			"LINESTRING(179 0, -179 0)",
			"POINT(179.1 0)",
			true,
		},
		{
			"reversed LINESTRING covers POINT across the longitudinal boundary",
			"LINESTRING(-179 0, 179 0)",
			"POINT(179.1 0)",
			true,
		},
		{
			"LINESTRING does not cover POINT with linestring crossing the longitudinal boundary but POINT on the other side",
			"LINESTRING(179 0, -179 0)",
			"POINT(170 0)",
			false,
		},
		{
			"reversed LINESTRING does not cover POINT with linestring crossing the longitudinal boundary but POINT on the other side",
			"LINESTRING(-179 0, 179 0)",
			"POINT(170 0)",
			false,
		},
		{
			"POLYGON covers POINT lying inside latitudinal boundary",
			"POLYGON((150 85, 160 85, -20 85, -30 85, 150 85))",
			"POINT (150 88)",
			true,
		},
		{
			"POLYGON does not cover POINT lying outside latitudinal boundary",
			"POLYGON((150 85, 160 85, -20 85, -30 85, 150 85))",
			"POINT (170 88)",
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeography(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeography(tc.b)
			require.NoError(t, err)

			covers, err := Covers(a, b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, covers)

			coveredBy, err := CoveredBy(b, a)
			require.NoError(t, err)
			require.Equal(t, tc.expected, coveredBy)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Covers(mismatchingSRIDGeographyA, mismatchingSRIDGeographyB)
		requireMismatchingSRIDError(t, err)
		_, err = CoveredBy(mismatchingSRIDGeographyA, mismatchingSRIDGeographyB)
		requireMismatchingSRIDError(t, err)
	})
}
