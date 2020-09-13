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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakePolygon(t *testing.T) {
	testCases := []struct {
		name          string
		outer         string
		outerSRID     geopb.SRID
		interior      []string
		interiorSRIDs []geopb.SRID
		expected      string
		expectedSRID  geopb.SRID
		err           error
	}{
		{
			"Single input variant - 2D",
			"LINESTRING(75 29,77 29,77 29, 75 29)",
			geopb.DefaultGeometrySRID,
			[]string{},
			[]geopb.SRID{},
			"POLYGON((75 29,77 29,77 29,75 29))",
			geopb.DefaultGeometrySRID,
			nil,
		},
		{
			"Single input variant - 2D with SRID",
			"LINESTRING(75 29,77 29,77 29, 75 29)",
			geopb.SRID(4326),
			[]string{},
			[]geopb.SRID{},
			"POLYGON((75 29,77 29,77 29,75 29))",
			geopb.SRID(4326),
			nil,
		},
		{
			"Single input variant - 2D square",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.DefaultGeometrySRID,
			[]string{},
			[]geopb.SRID{},
			"POLYGON((40 80, 80 80, 80 40, 40 40, 40 80))",
			geopb.DefaultGeometrySRID,
			nil,
		},
		{
			"With inner holes variant - 2D single interior ring",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.DefaultGeometrySRID,
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
			},
			[]geopb.SRID{geopb.DefaultGeometrySRID},
			"POLYGON((40 80,80 80,80 40,40 40,40 80),(50 70,70 70,70 50,50 50,50 70))",
			geopb.DefaultGeometrySRID,
			nil,
		},
		{
			"With inner holes variant - 2D single interior ring with SRIDs",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.SRID(4326),
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
			},
			[]geopb.SRID{geopb.SRID(4326)},
			"POLYGON((40 80,80 80,80 40,40 40,40 80),(50 70,70 70,70 50,50 50,50 70))",
			geopb.SRID(4326),
			nil,
		},
		{
			"With inner holes variant - 2D two interior rings",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.DefaultGeometrySRID,
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
				"LINESTRING(60 60, 75 60, 75 45, 60 45, 60 60)",
			},
			[]geopb.SRID{geopb.DefaultGeometrySRID, geopb.DefaultGeometrySRID},
			"POLYGON((40 80,80 80,80 40,40 40,40 80),(50 70,70 70,70 50,50 50,50 70),(60 60,75 60,75 45,60 45,60 60))",
			geopb.DefaultGeometrySRID,
			nil,
		},
		{
			"With inner holes variant - 2D two interior rings with SRID",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.SRID(4326),
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
				"LINESTRING(60 60, 75 60, 75 45, 60 45, 60 60)",
			},
			[]geopb.SRID{geopb.SRID(4326), geopb.SRID(4326)},
			"POLYGON((40 80,80 80,80 40,40 40,40 80),(50 70,70 70,70 50,50 50,50 70),(60 60,75 60,75 45,60 45,60 60))",
			geopb.SRID(4326),
			nil,
		},
		{
			"ERROR: Invalid argument - POINT",
			"POINT(3 2)",
			geopb.DefaultGeometrySRID,
			[]string{},
			[]geopb.SRID{},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("argument must be LINESTRING geometries"),
		},
		{
			"ERROR: Invalid argument - POINT rings",
			"LINESTRING(75 29,77 29,77 29, 75 29)",
			geopb.DefaultGeometrySRID,
			[]string{"POINT(3 2)"},
			[]geopb.SRID{0},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("argument must be LINESTRING geometries"),
		},
		{
			"ERROR: Unmatched SRIDs - Single interior ring",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.SRID(4326),
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
			},
			[]geopb.SRID{geopb.SRID(26918)},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("mixed SRIDs are not allowed"),
		},
		{
			"ERROR: Unmatched SRIDs - Default SRID on interior ring",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.SRID(4326),
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
			},
			[]geopb.SRID{geopb.DefaultGeometrySRID},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("mixed SRIDs are not allowed"),
		},
		{
			"ERROR: Unmatched SRIDs - Two interior rings",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.SRID(4326),
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
				"LINESTRING(60 60, 75 60, 75 45, 60 45, 60 60)",
			},
			[]geopb.SRID{geopb.SRID(4326), geopb.SRID(26918)},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("mixed SRIDs are not allowed"),
		},
		{
			"ERROR: Unclosed shell",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 70)",
			geopb.DefaultGeographySRID,
			[]string{},
			[]geopb.SRID{},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("Polygon LinearRing at position 1 is not closed"),
		},
		{
			"ERROR: Unclosed interior ring",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.SRID(4326),
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 60)",
			},
			[]geopb.SRID{geopb.SRID(4326), geopb.SRID(26918)},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("Polygon LinearRing at position 2 is not closed"),
		},
		{
			"ERROR: Shell has 3 points",
			"LINESTRING(40 80, 80 80, 40 80)",
			geopb.DefaultGeographySRID,
			[]string{},
			[]geopb.SRID{},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("Polygon LinearRing must have at least 4 points, found 3 at position 1"),
		},
		{
			"ERROR: Shell has 2 points",
			"LINESTRING(40 80, 40 80)",
			geopb.DefaultGeographySRID,
			[]string{},
			[]geopb.SRID{},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("Polygon LinearRing must have at least 4 points, found 2 at position 1"),
		},
		{
			"ERROR: Interior ring has 3 points",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.SRID(4326),
			[]string{
				"LINESTRING(50 70, 70 70, 50 70)",
			},
			[]geopb.SRID{geopb.SRID(4326), geopb.SRID(26918)},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("Polygon LinearRing must have at least 4 points, found 3 at position 2"),
		},
		{
			"ERROR: Interior ring has 2 points",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			geopb.SRID(4326),
			[]string{
				"LINESTRING(50 70, 50 70)",
			},
			[]geopb.SRID{geopb.SRID(4326), geopb.SRID(26918)},
			"",
			geopb.DefaultGeometrySRID,
			errors.Newf("Polygon LinearRing must have at least 4 points, found 2 at position 2"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			outer := geo.MustParseGeometry(tc.outer)
			var err error
			outer, err = outer.CloneWithSRID(tc.outerSRID)
			require.NoError(t, err)
			interior := make([]geo.Geometry, 0, len(tc.interior))
			for i, g := range tc.interior {
				interiorRing := geo.MustParseGeometry(g)
				interiorRing, err = interiorRing.CloneWithSRID(tc.interiorSRIDs[i])
				require.NoError(t, err)
				interior = append(interior, interiorRing)
			}
			polygon, err := MakePolygon(outer, interior...)
			if tc.err != nil {
				require.Errorf(t, err, tc.err.Error())
				require.EqualError(t, err, tc.err.Error())
			} else {
				require.NoError(t, err)
				expected := geo.MustParseGeometry(tc.expected)
				expected, err := expected.CloneWithSRID(tc.expectedSRID)
				require.NoError(t, err)
				assert.Equal(t, expected, polygon)
			}
		})
	}
}

func TestMakePolygonWithSRID(t *testing.T) {
	testCases := []struct {
		name     string
		g        string
		srid     int
		expected string
	}{
		{
			"Single input variant - 2D",
			"LINESTRING(75 29,77 29,77 29, 75 29)",
			int(geopb.DefaultGeometrySRID),
			"SRID=0;POLYGON((75 29,77 29,77 29,75 29))",
		},
		{
			"Single input variant - 2D with SRID",
			"LINESTRING(75 29,77 29,77 29, 75 29)",
			4326,
			"SRID=4326;POLYGON((75 29,77 29,77 29,75 29))",
		},
		{
			"Single input variant - 2D square with SRID",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			4000,
			"SRID=4000;POLYGON((40 80, 80 80, 80 40, 40 40, 40 80))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := geo.MustParseGeometry(tc.g)
			polygon, err := MakePolygonWithSRID(g, tc.srid)
			require.NoError(t, err)
			expected := geo.MustParseGeometry(tc.expected)
			require.Equal(t, expected, polygon)
			require.EqualValues(t, tc.srid, polygon.SRID())
		})
	}
}
