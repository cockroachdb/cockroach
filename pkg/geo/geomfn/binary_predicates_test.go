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
}

func TestContains(t *testing.T) {
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
			g, err := Contains(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}
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
}

func TestEquals(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected bool
	}{
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
}
