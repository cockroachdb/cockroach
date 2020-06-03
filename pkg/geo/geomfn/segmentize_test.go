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
	"github.com/twpayne/go-geom"
)

func TestSegmentize(t *testing.T) {
	segmentizeTestCases := []struct {
		wkt              string
		maxSegmentLength float64
		expectedWKT      string
	}{
		{
			wkt:              "POINT (1.0 1.0)",
			maxSegmentLength: 1,
			expectedWKT:      "POINT (1.0 1.0)",
		},
		{
			wkt:              "LINESTRING (1.0 1.0, 2.0 2.0, 3.0 3.0)",
			maxSegmentLength: 1,
			expectedWKT:      "LINESTRING (1.0 1.0, 1.5 1.5, 2.0 2.0, 2.5 2.5, 3.0 3.0)",
		},
		{
			wkt:              "LINESTRING (1.0 1.0, 2.0 2.0, 3.0 3.0)",
			maxSegmentLength: 0.33333,
			expectedWKT:      "LINESTRING (1.0 1.0, 1.2000000000000002 1.2000000000000002, 1.4 1.4, 1.6 1.6, 1.8 1.8, 2.0 2.0, 2.2 2.2, 2.4000000000000004 2.4000000000000004, 2.5999999999999996 2.5999999999999996, 2.8000000000000003 2.8000000000000003, 3 3)",
		},
		{
			wkt:              "LINESTRING EMPTY",
			maxSegmentLength: 1,
			expectedWKT:      "LINESTRING EMPTY",
		},
		{
			wkt:              "LINESTRING (1.0 1.0, 2.0 2.0, 3.0 3.0)",
			maxSegmentLength: 2,
			expectedWKT:      "LINESTRING (1.0 1.0, 2.0 2.0, 3.0 3.0)",
		},
		{
			wkt:              "LINESTRING (0.0 0.0, 0.0 10.0, 0.0 16.0)",
			maxSegmentLength: 3,
			expectedWKT:      "LINESTRING (0.0 0.0,0.0 2.5,0.0 5.0,0.0 7.5,0.0 10.0,0.0 13.0,0.0 16.0)",
		},
		{
			wkt:              "POLYGON ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))",
			maxSegmentLength: 0.8,
			expectedWKT:      "POLYGON ((0.0 0.0, 0.5 0.0, 1.0 0.0, 1.0 0.5, 1.0 1.0, 0.5 0.5, 0.0 0.0))",
		},
		{
			wkt:              "POLYGON ((0.0 0.0, 1.0 0.0, 3.0 3.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
			maxSegmentLength: 1,
			expectedWKT:      "POLYGON ((0.0 0.0, 1.0 0.0, 1.5 0.75, 2.0 1.5, 2.5 2.25, 3.0 3.0, 2.4000000000000004 2.4000000000000004, 1.7999999999999998 1.7999999999999998, 1.1999999999999997 1.1999999999999997, 0.5999999999999999 0.5999999999999999, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
		},
		{
			wkt:              "POLYGON ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
			maxSegmentLength: 5,
			expectedWKT:      "POLYGON ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
		},
		{
			wkt:              "POLYGON EMPTY",
			maxSegmentLength: 1,
			expectedWKT:      "POLYGON EMPTY",
		},
		{
			wkt:              "MULTIPOINT ((1.0 1.0), (2.0 2.0))",
			maxSegmentLength: 1,
			expectedWKT:      "MULTIPOINT ((1.0 1.0), (2.0 2.0))",
		},
		{
			wkt:              "MULTILINESTRING ((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))",
			maxSegmentLength: 1,
			expectedWKT:      "MULTILINESTRING ((1.0 1.0, 1.5 1.5, 2.0 2.0, 2.5 2.5, 3.0 3.0), (6.0 6.0, 7.0 6.0))",
		},
		{
			wkt:              "MULTILINESTRING (EMPTY, (1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))",
			maxSegmentLength: 1,
			expectedWKT:      "MULTILINESTRING (EMPTY, (1.0 1.0, 1.5 1.5, 2.0 2.0, 2.5 2.5, 3.0 3.0), (6.0 6.0, 7.0 6.0))",
		},
		{
			wkt:              "MULTIPOLYGON (((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))",
			maxSegmentLength: 1,
			expectedWKT:      "MULTIPOLYGON (((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.5 3.5, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.5 0.5, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))",
		},
		{
			wkt:              "GEOMETRYCOLLECTION (POINT (40.0 10.0), LINESTRING (10.0 10.0, 20.0 20.0, 10.0 40.0), POLYGON ((40.0 40.0, 20.0 45.0, 45.0 30.0, 40.0 40.0)))",
			maxSegmentLength: 10,
			expectedWKT:      "GEOMETRYCOLLECTION (POINT (40.0 10.0), LINESTRING (10.0 10.0, 15.0 15.0, 20.0 20.0, 16.666666666666668 26.666666666666668, 13.333333333333334 33.33333333333333, 10.0 40.0), POLYGON ((40.0 40.0, 33.333333333333336 41.66666666666667, 26.666666666666668 43.333333333333336, 20.0 45.0, 28.333333333333336 40.0, 36.66666666666667 35.0, 45.0 30.0, 42.5 35.0, 40.0 40.0)))",
		},
		{
			wkt:              "MULTIPOINT ((0.0 0.0), (1.0 1.0))",
			maxSegmentLength: -1,
			expectedWKT:      "MULTIPOINT ((0.0 0.0), (1.0 1.0))",
		},
	}
	for _, test := range segmentizeTestCases {
		t.Run(fmt.Sprintf("%s, maximum segment length: %f", test.wkt, test.maxSegmentLength), func(t *testing.T) {
			geom, err := geo.ParseGeometry(test.wkt)
			require.NoError(t, err)
			modifiedGeom, err := Segmentize(geom, test.maxSegmentLength)
			require.NoError(t, err)
			expectedGeom, err := geo.ParseGeometry(test.expectedWKT)
			require.NoError(t, err)
			require.Equal(t, expectedGeom, modifiedGeom)
		})
	}
	// Test for segment maximum length as negative.
	t.Run("Error when maximum segment length is less than 0", func(t *testing.T) {
		geom, err := geo.ParseGeometry("MULTILINESTRING ((0 0, 1 1, 5 5), (5 5, 0 0))")
		require.NoError(t, err)
		_, err = Segmentize(geom, 0)
		require.EqualError(t, err, "maximum segment length must be positive")
	})
}

func TestSegmentizeCoords(t *testing.T) {
	testCases := []struct {
		desc                 string
		a                    geom.Coord
		b                    geom.Coord
		segmentMaxLength     float64
		resultantCoordinates []float64
	}{
		{
			desc:                 `Coordinate(0, 0) to Coordinate(1, 1), 1`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{1, 1},
			segmentMaxLength:     1,
			resultantCoordinates: []float64{0, 0, 0.5, 0.5},
		},
		{
			desc:                 `Coordinate(0, 0) to Coordinate(1, 1), 0.3`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{1, 1},
			segmentMaxLength:     0.3,
			resultantCoordinates: []float64{0, 0, 0.2, 0.2, 0.4, 0.4, 0.6000000000000001, 0.6000000000000001, 0.8, 0.8},
		},
		{
			desc:                 `Coordinate(0, 0) to Coordinate(1, 0), 0.49999999999999`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{1, 0},
			segmentMaxLength:     0.49999999999999,
			resultantCoordinates: []float64{0, 0, 0.3333333333333333, 0, 0.6666666666666666, 0},
		},
		{
			desc:                 `Coordinate(1, 1) to Coordinate(0, 0), -1`,
			a:                    geom.Coord{1, 1},
			b:                    geom.Coord{0, 0},
			segmentMaxLength:     -1,
			resultantCoordinates: []float64{1, 1},
		},
		{
			desc:                 `Coordinate(1, 1) to Coordinate(0, 0), 2`,
			a:                    geom.Coord{1, 1},
			b:                    geom.Coord{0, 0},
			segmentMaxLength:     2,
			resultantCoordinates: []float64{1, 1},
		},
		{
			desc:                 `Coordinate(0, 0) to Coordinate(0, 0), 1`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{0, 0},
			segmentMaxLength:     1,
			resultantCoordinates: []float64{0, 0},
		},
	}
	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			convertedPoints := segmentizeCoords(test.a, test.b, test.segmentMaxLength)
			require.Equal(t, test.resultantCoordinates, convertedPoints)
		})
	}
}
