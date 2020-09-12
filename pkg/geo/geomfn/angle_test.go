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
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/stretchr/testify/require"
)

func TestAngle(t *testing.T) {
	pf := func(f float64) *float64 {
		return &f
	}
	testCases := []struct {
		wkt1     string
		wkt2     string
		wkt3     string
		wkt4     string
		expected *float64
	}{
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 0)", "POINT (0 1)", pf(0)},
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 0)", "POINT (1 1)", pf(1.0 / 4.0 * math.Pi)},
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 0)", "POINT (1 0)", pf(2.0 / 4.0 * math.Pi)},
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 0)", "POINT (1 -1)", pf(3.0 / 4.0 * math.Pi)},
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 0)", "POINT (0 -1)", pf(math.Pi)},
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 0)", "POINT (-1 -1)", pf(5.0 / 4.0 * math.Pi)},
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 0)", "POINT (-1 0)", pf(6.0 / 4.0 * math.Pi)},
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 0)", "POINT (-1 1)", pf(7.0 / 4.0 * math.Pi)},
		{"POINT (10 10)", "POINT (10 11)", "POINT (-100 -100)", "POINT (-99 -99)", pf(1.0 / 4.0 * math.Pi)},
		{"POINT (1.2 -3.4)", "POINT (-5.6 7.8)", "POINT (-0.12 0.34)", "POINT (0.56 -0.78)", pf(math.Pi)},
		{"POINT (56 76)", "POINT (-34 -71)", "POINT (-3 27)", "POINT (-81 -36)", pf(0.3420080366154973)},
		{"POINT (0 0)", "POINT (1 0)", "POINT (1 1)", "POINT EMPTY", pf(1.0 / 2.0 * math.Pi)},
		{"POINT (0 0)", "POINT (0 1)", "POINT (0 -1)", "POINT EMPTY", pf(0)},
		// Ignore type of final empty geometry, following PostGIS.
		{"POINT (0 0)", "POINT (1 0)", "POINT (1 -1)", "MULTIPOLYGON EMPTY", pf(3.0 / 2.0 * math.Pi)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v, %v, %v, %v", tc.wkt1, tc.wkt2, tc.wkt3, tc.wkt4), func(t *testing.T) {
			srid := geopb.SRID(4000)
			g1, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt1), srid, true)
			require.NoError(t, err)
			g2, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt2), srid, true)
			require.NoError(t, err)
			g3, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt3), srid, true)
			require.NoError(t, err)
			g4, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt4), srid, true)
			require.NoError(t, err)

			angle, err := Angle(g1, g2, g3, g4)
			require.NoError(t, err)
			if tc.expected != nil && angle != nil {
				require.Equal(t, *tc.expected, *angle)
			} else {
				require.Equal(t, tc.expected, angle)
			}
		})
	}

	errorTestCases := []struct {
		wkt1 string
		wkt2 string
		wkt3 string
		wkt4 string
	}{
		{"POINT EMPTY", "POINT EMPTY", "POINT EMPTY", "POINT EMPTY"},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 0 1)"},
		{"LINESTRING EMPTY", "LINESTRING EMPTY", "LINESTRING EMPTY", "LINESTRING EMPTY"},
	}
	t.Run("errors on invalid arguments", func(t *testing.T) {
		for _, tc := range errorTestCases {
			t.Run(fmt.Sprintf("%v, %v, %v, %v", tc.wkt1, tc.wkt2, tc.wkt3, tc.wkt4), func(t *testing.T) {
				srid := geopb.SRID(4000)
				g1, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt1), srid, true)
				require.NoError(t, err)
				g2, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt2), srid, true)
				require.NoError(t, err)
				g3, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt3), srid, true)
				require.NoError(t, err)
				g4, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt4), srid, true)
				require.NoError(t, err)

				_, err = Angle(g1, g2, g3, g4)
				require.Error(t, err)
			})
		}
	})
}

func TestAngleLineString(t *testing.T) {
	pf := func(f float64) *float64 {
		return &f
	}
	testCases := []struct {
		wkt1     string
		wkt2     string
		expected *float64
	}{
		{"LINESTRING (0 0, 0 0)", "LINESTRING (0 0, 0 1)", nil},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 0 0)", nil},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 0 1)", pf(0)},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 1 1)", pf(1.0 / 4.0 * math.Pi)},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 1 0)", pf(2.0 / 4.0 * math.Pi)},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 1 -1)", pf(3.0 / 4.0 * math.Pi)},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, 0 -1)", pf(math.Pi)},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, -1 -1)", pf(5.0 / 4.0 * math.Pi)},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, -1 0)", pf(6.0 / 4.0 * math.Pi)},
		{"LINESTRING (0 0, 0 1)", "LINESTRING (0 0, -1 1)", pf(7.0 / 4.0 * math.Pi)},
		{"LINESTRING (10 10, 10 11)", "LINESTRING (-100 -100, -99 -99)", pf(1.0 / 4.0 * math.Pi)},
		{"LINESTRING (1.2 -3.4, -5.6 7.8)", "LINESTRING (-0.12 0.34, 0.56 -0.78)", pf(math.Pi)},
		{"LINESTRING (56 76, -34 -71)", "LINESTRING (-3 27, -81 -36)", pf(0.3420080366154973)},
		// Returns nil on type errors, to follow PostGIS behavior.
		{"LINESTRING EMPTY", "LINESTRING EMPTY", nil},
		{"GEOMETRYCOLLECTION EMPTY", "MULTIPOLYGON EMPTY", nil},
		{"POINT (0 1)", "POINT(1 0)", nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v, %v", tc.wkt1, tc.wkt2), func(t *testing.T) {
			srid := geopb.SRID(4000)
			g1, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt1), srid, true)
			require.NoError(t, err)
			g2, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt2), srid, true)
			require.NoError(t, err)

			angle, err := AngleLineString(g1, g2)
			require.NoError(t, err)
			if tc.expected != nil && angle != nil {
				require.Equal(t, *tc.expected, *angle)
			} else {
				require.Equal(t, tc.expected, angle)
			}
		})
	}
}
