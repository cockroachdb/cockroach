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
	"github.com/stretchr/testify/require"
)

var (
	emptyRect            = geo.MustParseGeometry("POLYGON EMPTY")
	emptyLine            = geo.MustParseGeometry("LINESTRING EMPTY")
	leftRect             = geo.MustParseGeometry("POLYGON((-1.0 0.0, 0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0))")
	leftRectPoint        = geo.MustParseGeometry("POINT(-0.5 0.5)")
	rightRect            = geo.MustParseGeometry("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))")
	rightRectPoint       = geo.MustParseGeometry("POINT(0.5 0.5)")
	overlappingRightRect = geo.MustParseGeometry("POLYGON((-0.1 0.0, 1.0 0.0, 1.0 1.0, -0.1 1.0, -0.1 0.0))")
	middleLine           = geo.MustParseGeometry("LINESTRING(-0.5 0.5, 0.5 0.5)")
)

func TestCovers(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, true},
		{rightRectPoint, rightRect, false},
		{leftRect, rightRect, false},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Covers(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Covers(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestCoveredBy(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, true},
		{leftRect, rightRect, false},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := CoveredBy(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := CoveredBy(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestContains(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, true},
		{rightRectPoint, rightRect, false},
		{rightRectPoint, rightRectPoint, true},
		{rightRect, rightRect, true},
		{leftRect, rightRect, false},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Contains(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Contains(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestContainsProperly(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
		{rightRect, rightRect, false},
		{rightRect, rightRectPoint, true},
		{rightRectPoint, rightRectPoint, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := ContainsProperly(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := ContainsProperly(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestCrosses(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
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
			g, err := Crosses(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Crosses(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestEquals(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
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
			g, err := Equals(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Equals(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestIntersects(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
		{rightRect, leftRectPoint, false},
		{rightRect, rightRectPoint, true},
		{rightRectPoint, rightRect, true},
		{leftRect, rightRect, true},
		{leftRect, middleLine, true},
		{rightRect, middleLine, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Intersects(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Intersects(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestOverlaps(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, false},
		{leftRect, rightRect, false},
		{leftRect, overlappingRightRect, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Overlaps(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Overlaps(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestTouches(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, false},
		{leftRect, rightRect, true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Touches(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Touches(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestWithin(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
		{rightRect, rightRectPoint, false},
		{rightRectPoint, rightRect, true},
		{leftRect, rightRect, false},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Within(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Within(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}
