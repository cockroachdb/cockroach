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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/stretchr/testify/require"
)

var (
	emptyPoint           = geo.MustParseGeometry("POINT EMPTY")
	emptyRect            = geo.MustParseGeometry("POLYGON EMPTY")
	emptyLine            = geo.MustParseGeometry("LINESTRING EMPTY")
	leftRect             = geo.MustParseGeometry("POLYGON((-1.0 0.0, 0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0))")
	leftRectPoint        = geo.MustParseGeometry("POINT(-0.5 0.5)")
	rightRect            = geo.MustParseGeometry("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))")
	rightRectPoint       = geo.MustParseGeometry("POINT(0.5 0.5)")
	overlappingRightRect = geo.MustParseGeometry("POLYGON((-0.1 0.0, 1.0 0.0, 1.0 1.0, -0.1 1.0, -0.1 0.0))")
	middleLine           = geo.MustParseGeometry("LINESTRING(-0.5 0.5, 0.5 0.5)")
	leftRectWithHole     = geo.MustParseGeometry("POLYGON((-1.0 0.0, 0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0), " +
		"(-0.75 0.25, -0.75 0.75, -0.25 0.75, -0.25 0.25, -0.75 0.25))")
	bothLeftRects = geo.MustParseGeometry("MULTIPOLYGON(((-1.0 0.0, 0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0)), " +
		"((-1.0 0.0, 0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0), (-0.75 0.25, -0.75 0.75, -0.25 0.75, -0.25 0.25, -0.75 0.25)))")
	bothLeftRectsHoleFirst = geo.MustParseGeometry("MULTIPOLYGON(" +
		"((-1.0 0.0, 0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0), " +
		"(-0.75 0.25, -0.75 0.75, -0.25 0.75, -0.25 0.25, -0.75 0.25)), " +
		"((-1.0 0.0, 0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0)))")
	leftRectCornerPoint     = geo.MustParseGeometry("POINT(-1.0 0.0)")
	leftRectEdgePoint       = geo.MustParseGeometry("POINT(-1.0 0.2)")
	leftRectHoleCornerPoint = geo.MustParseGeometry("POINT(-0.75 0.75)")
	leftRectHoleEdgePoint   = geo.MustParseGeometry("POINT(-0.75 0.5)")
	leftRectMultiPoint      = geo.MustParseGeometry("MULTIPOINT(-0.5 0.5, -0.9 0.1)")
	leftRectEdgeMultiPoint  = geo.MustParseGeometry("MULTIPOINT(-1.0 0.2, -0.9 0.1)")
)

func TestCovers(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, true},
		{rightRectPoint, rightRect, false},
		{leftRect, rightRect, false},
		{leftRect, leftRectEdgePoint, true},
		{leftRectWithHole, leftRectPoint, false},
		{leftRect, emptyPoint, false},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Covers(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Covers(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestCoveredBy(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, true},
		{leftRect, rightRect, false},
		{leftRectEdgeMultiPoint, leftRectWithHole, true},
		{leftRectPoint, emptyRect, false},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := CoveredBy(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := CoveredBy(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestContains(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, true},
		{rightRectPoint, rightRect, false},
		{rightRectPoint, rightRectPoint, true},
		{rightRect, rightRect, true},
		{leftRect, rightRect, false},
		{emptyRect, emptyPoint, false},
		{leftRectWithHole, leftRectPoint, false},
		{leftRectWithHole, leftRectMultiPoint, false},
		{leftRect, leftRectMultiPoint, true},
		{bothLeftRectsHoleFirst, leftRectPoint, true},
		{leftRect, leftRectEdgePoint, false},
		{leftRect, leftRectEdgeMultiPoint, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Contains(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Contains(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestContainsProperly(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRect, false},
		{rightRect, rightRectPoint, true},
		{rightRectPoint, rightRectPoint, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := ContainsProperly(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := ContainsProperly(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestCrosses(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, false},
		{leftRect, rightRect, false},
		{leftRect, middleLine, true},
		{rightRect, middleLine, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Crosses(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Crosses(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestDisjoint(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{leftRect, rightRectPoint, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Disjoint(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Disjoint(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestEquals(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{emptyLine, emptyRect, true},
		{emptyLine, emptyLine, true},
		{emptyRect, emptyRect, true},
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, false},
		{leftRect, rightRect, false},
		{leftRect, geo.MustParseGeometry("POLYGON((0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0, 0.0 0.0))"), true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Equals(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Equals(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestIntersects(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, leftRectPoint, false},
		{rightRect, rightRectPoint, true},
		{rightRectPoint, rightRect, true},
		{leftRect, rightRect, true},
		{leftRect, middleLine, true},
		{rightRect, middleLine, true},
		{leftRectPoint, leftRect, true},
		{leftRectPoint, leftRectWithHole, false},
		{leftRect, leftRectPoint, true},
		{leftRectWithHole, leftRectPoint, false},
		{leftRectPoint, bothLeftRects, true},
		{leftRectWithHole, leftRectMultiPoint, true},
		{leftRectCornerPoint, leftRect, true},
		{leftRectEdgePoint, leftRect, true},
		{leftRectHoleEdgePoint, leftRectWithHole, true},
		{leftRectHoleCornerPoint, leftRectWithHole, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Intersects(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Intersects(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestOrderingEquals(t *testing.T) {
	testCases := []struct {
		a        string
		b        string
		expected bool
	}{
		{"POINT EMPTY", "POINT EMPTY", true},
		{"POINT (1 1)", "POINT (1 1)", true},
		{"POINT (1 1)", "POINT (2 2)", false},
		{"MULTIPOINT EMPTY", "MULTIPOINT EMPTY", true},
		{"MULTIPOINT (1 1, EMPTY, 2 2)", "MULTIPOINT (1 1, EMPTY, 2 2)", true},
		{"MULTIPOINT (1 1, 2 2)", "MULTIPOINT (2 2, 1 1)", false},
		{"MULTIPOINT (1 1, EMPTY, 2 2)", "MULTIPOINT (1 1, 2 2)", false},
		{"LINESTRING EMPTY", "LINESTRING EMPTY", true},
		{"LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2)", true},
		{"LINESTRING (1 1, 2 2)", "LINESTRING (2 2, 1 1)", false},
		{"LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2, 3 3)", false},
		{"MULTILINESTRING EMPTY", "MULTILINESTRING EMPTY", true},
		{
			"MULTILINESTRING ((1 1, 2 2), EMPTY, (3 3, 4 4))",
			"MULTILINESTRING ((1 1, 2 2), EMPTY, (3 3, 4 4))",
			true,
		},
		{
			"MULTILINESTRING ((1 1, 2 2), EMPTY, (3 3, 4 4))",
			"MULTILINESTRING ((1 1, 2 2), (3 3, 4 4))",
			false,
		},
		{"POLYGON EMPTY", "POLYGON EMPTY", true},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", "POLYGON ((1 2, 3 4, 5 6, 1 2))", true},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", "POLYGON ((1 2, 5 6, 3 4, 1 2))", false},
		{"MULTIPOLYGON EMPTY", "MULTIPOLYGON EMPTY", true},
		{
			"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), EMPTY, ((9 8, 7 6, 5 4, 9 8)))",
			"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), EMPTY, ((9 8, 7 6, 5 4, 9 8)))",
			true,
		},
		{
			"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), EMPTY, ((9 8, 7 6, 5 4, 9 8)))",
			"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((9 8, 7 6, 5 4, 9 8)))",
			false,
		},
		{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY", true},
		{
			"GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (1 1, 2 2))",
			"GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (1 1, 2 2))",
			true,
		},
		{
			"GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (1 1, 2 2))",
			"GEOMETRYCOLLECTION (LINESTRING (1 1, 2 2), POINT (1 1))",
			false,
		},
		{"POINT EMPTY", "LINESTRING EMPTY", false},
		{"POINT (1 1)", "MULTIPOINT (1 1)", false},
		{"SRID=4000;POINT (1 1)", "SRID=4326;POINT (1 1)", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v, %v", tc.a, tc.b), func(t *testing.T) {
			a, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.a), geopb.DefaultGeometrySRID, false)
			require.NoError(t, err)
			b, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.b), geopb.DefaultGeometrySRID, false)
			require.NoError(t, err)

			eq, err := OrderingEquals(a, b)
			require.NoError(t, err)
			require.Equal(t, eq, tc.expected)
		})
	}
}

func TestOverlaps(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, false},
		{leftRect, rightRect, false},
		{leftRect, overlappingRightRect, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Overlaps(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Overlaps(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestTouches(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, false},
		{leftRect, rightRect, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Touches(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Touches(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestWithin(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, true},
		{leftRect, rightRect, false},
		{emptyPoint, emptyRect, false},
		{leftRectPoint, leftRect, true},
		{leftRectMultiPoint, leftRect, true},
		{leftRectMultiPoint, leftRectWithHole, false},
		{leftRectHoleEdgePoint, leftRectWithHole, false},
		{leftRectEdgePoint, leftRectWithHole, false},
		{leftRectEdgeMultiPoint, leftRectWithHole, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Within(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Within(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}
