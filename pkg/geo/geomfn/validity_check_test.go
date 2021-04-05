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

func TestIsValid(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"POINT(1.0 1.0)", true},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", true},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", true},

		{"POLYGON((1.0 1.0, 2.0 2.0, 1.5 1.5, 1.5 -1.5, 1.0 1.0))", false},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsValid(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestIsValidReason(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT(1.0 1.0)", "Valid Geometry"},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", "Valid Geometry"},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", "Valid Geometry"},

		{"POLYGON((1.0 1.0, 2.0 2.0, 1.5 1.5, 1.5 -1.5, 1.0 1.0))", "Self-intersection[1.5 1.5]"},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsValidReason(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestIsValidDetail(t *testing.T) {
	testCases := []struct {
		wkt      string
		flags    int
		expected ValidDetail
	}{
		{"POINT(1.0 1.0)", 0, ValidDetail{IsValid: true}},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", 0, ValidDetail{IsValid: true}},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", 0, ValidDetail{IsValid: true}},
		{"POINT(1.0 1.0)", 1, ValidDetail{IsValid: true}},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", 1, ValidDetail{IsValid: true}},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", 1, ValidDetail{IsValid: true}},
		{
			"POLYGON ((14 20, 8 45, 20 35, 14 20, 16 30, 12 30, 14 20))",
			1,
			ValidDetail{IsValid: true},
		},

		{
			"POLYGON ((14 20, 8 45, 20 35, 14 20, 16 30, 12 30, 14 20))",
			0,
			ValidDetail{
				IsValid:         false,
				Reason:          "Ring Self-intersection",
				InvalidLocation: geo.MustParseGeometry("POINT(14 20)"),
			},
		},
		{
			"POLYGON((1.0 1.0, 2.0 2.0, 1.5 1.5, 1.5 -1.5, 1.0 1.0))",
			0,
			ValidDetail{
				IsValid:         false,
				Reason:          "Self-intersection",
				InvalidLocation: geo.MustParseGeometry("POINT(1.5 1.5)"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s(%d)", tc.wkt, tc.flags), func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsValidDetail(g, tc.flags)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestIsValidTrajectory(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"LINESTRINGM(0 0 1,0 1 2)", true},
		{"LINESTRINGM(0 0 1,1 1 1.01)", true},
		{"LINESTRINGM(2 2 1,1 1 2,0 0 3)", true},
		{"LINESTRINGZM(0 0 0 1,0 1 2 3)", true},
		{"LINESTRINGZM(2 2 2 1,1 1 1 2,0 0 0 3)", true},
		{"LINESTRINGM(0 0 1,0 0 1)", false},
		{"LINESTRINGM(2 2 1,1 1 2,3 3 1)", false},
		{"LINESTRINGZM(0 0 0 1,0 0 0 1)", false},
		{"LINESTRINGZM(2 2 2 1,1 1 1 2,3 3 3 1)", false},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsValidTrajectory(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}

	errorTestCases := []struct {
		wkt         string
		expected    bool
		expectedErr string
	}{
		{
			"POINT(1.0 1.0)",
			false,
			"expected LineString, got Point",
		},
		{
			"LINESTRING(0 0,0 1)",
			false,
			"LineString does not have M coordinates",
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			_, err = IsValidTrajectory(g)
			require.Error(t, err)
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}

func TestMakeValid(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT(1.0 1.0)", "POINT(1.0 1.0)"},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)"},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))"},

		{
			"POLYGON((1.0 1.0, 2.0 2.0, 1.5 1.5, 1.5 -1.5, 1.0 1.0))",
			"GEOMETRYCOLLECTION(POLYGON((1 1,1.5 1.5,1.5 -1.5,1 1)),LINESTRING(1.5 1.5,2 2))",
		},
		{
			"SRID=4326;POLYGON((1.0 1.0, 2.0 2.0, 1.5 1.5, 1.5 -1.5, 1.0 1.0))",
			"SRID=4326;GEOMETRYCOLLECTION(POLYGON((1 1,1.5 1.5,1.5 -1.5,1 1)),LINESTRING(1.5 1.5,2 2))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)

			ret, err := MakeValid(g)
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)
			require.Equal(t, expected, ret)
		})
	}
}
