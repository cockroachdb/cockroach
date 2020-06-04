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
			wkt:              "POINT(1.0 1.0)",
			maxSegmentLength: 10000.0,
			expectedWKT:      "POINT(1.0 1.0)",
		},
		{
			wkt:              "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			maxSegmentLength: 100000.0,
			expectedWKT:      "LINESTRING (1 1, 1.4998857365616758 1.5000570914791973, 2 2, 2.4998094835255658 2.500095075163195, 3 3)",
		},
		{
			wkt:              "LINESTRING EMPTY",
			maxSegmentLength: 10000.0,
			expectedWKT:      "LINESTRING EMPTY",
		},
		{
			wkt:              "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
			maxSegmentLength: 500000.0,
			expectedWKT:      "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
		},
		{
			wkt:              "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))",
			maxSegmentLength: 100000.0,
			expectedWKT:      "POLYGON ((0 0, 0.5 0, 1 0, 1 0.4999999999999999, 1 1, 0.4999619199226218 0.5000190382261059, 0 0))",
		},
		{
			wkt:              "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
			maxSegmentLength: 50000.0,
			expectedWKT:      "POLYGON ((0 0, 0.25 0, 0.5 0, 0.75 0, 1 0, 0.9999999999999998 0.25, 1 0.4999999999999999, 0.9999999999999998 0.7499999999999999, 1 1, 0.7499666792978344 0.7500166590029188, 0.4999619199226218 0.5000190382261059, 0.24997620022356357 0.2500118986534327, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
		},
		{
			wkt:              "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
			maxSegmentLength: 1000000.0,
			expectedWKT:      "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
		},
		{
			wkt:              "POLYGON EMPTY",
			maxSegmentLength: 10000.0,
			expectedWKT:      "POLYGON EMPTY",
		},
		{
			wkt:              "MULTIPOINT((1.0 1.0), (2.0 2.0))",
			maxSegmentLength: 100000.0,
			expectedWKT:      "MULTIPOINT((1.0 1.0), (2.0 2.0))",
		},
		{
			wkt:              "MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))",
			maxSegmentLength: 100000.0,
			expectedWKT:      "MULTILINESTRING ((1 1, 1.4998857365616758 1.5000570914791973, 2 2, 2.4998094835255658 2.500095075163195, 3 3), (6 6, 6.5 6.000226803574719, 7 6))",
		},
		{
			wkt:              "MULTILINESTRING (EMPTY, (1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))",
			maxSegmentLength: 100000.0,
			expectedWKT:      "MULTILINESTRING (EMPTY, (1 1, 1.4998857365616758 1.5000570914791973, 2 2, 2.4998094835255658 2.500095075163195, 3 3), (6 6, 6.5 6.000226803574719, 7 6))",
		},
		{
			wkt:              "MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))",
			maxSegmentLength: 100000.0,
			expectedWKT:      "MULTIPOLYGON (((3 3, 3.500000000000001 3.0001140264716564, 4 3, 3.999999999999999 3.5, 4 4, 3.4997331141752333 3.5001329429928956, 3 3)), ((0 0, 0.5 0, 1 0, 1 0.4999999999999999, 1 1, 0.4999619199226218 0.5000190382261059, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))",
		},
		{
			wkt:              "GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
			maxSegmentLength: 1000000.0,
			expectedWKT:      "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 14.88248913028353 15.054670903122675, 20 20, 17.84783705611024 25.063693381403255, 15.510294720403053 30.09369304154877, 12.922758839587525 35.07789165618019, 10 40), POLYGON ((40 40, 30.404184608679003 42.93646050008845, 20 45, 27.258698904670663 41.785178536245354, 33.78296512799812 38.158856049677816, 39.6629587979805 34.20684491196235, 45 30, 42.6532474552537 35.02554212454631, 40 40)))",
		},
		{
			wkt:              "MULTIPOINT((0 0), (1 1))",
			maxSegmentLength: -1,
			expectedWKT:      "MULTIPOINT((0 0), (1 1))",
		},
	}
	for _, test := range segmentizeTestCases {
		t.Run(fmt.Sprintf("%s, maximum segment length: %f", test.wkt, test.maxSegmentLength), func(t *testing.T) {
			geog, err := geo.ParseGeography(test.wkt)
			require.NoError(t, err)
			modifiedGeog, err := Segmentize(geog, test.maxSegmentLength)
			require.NoError(t, err)
			expectedGeog, err := geo.ParseGeography(test.expectedWKT)
			require.NoError(t, err)
			require.Equal(t, expectedGeog, modifiedGeog)
		})
	}
	// Test for segment maximum length as negative.
	t.Run("Error when maximum segment length is less than 0", func(t *testing.T) {
		geog, err := geo.ParseGeography("MULTILINESTRING ((0 0, 1 1, 5 5), (5 5, 0 0))")
		require.NoError(t, err)
		_, err = Segmentize(geog, 0)
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
			desc:                 `Coordinate(0, 0) to Coordinate(85, 85), 0.781600222084459`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{85, 85},
			segmentMaxLength:     0.781600222084459,
			resultantCoordinates: []float64{0, 0, 4.924985039227315, 44.568038920632105},
		},
		{
			desc:                 `Coordinate(0, 0) to Coordinate(85, 85), 0.7816000651234446`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{85, 85},
			segmentMaxLength:     0.7816000651234446,
			resultantCoordinates: []float64{0, 0, 2.0486953806277866, 22.302074138733936, 4.924985039227315, 44.568038920632105, 11.655816669136822, 66.66485041602017},
		},
		{
			desc:                 `Coordinate(85, 85) to Coordinate(0, 0), 0.29502092024628396`,
			a:                    geom.Coord{85, 85},
			b:                    geom.Coord{0, 0},
			segmentMaxLength:     0.29502092024628396,
			resultantCoordinates: []float64{85, 85, 22.871662720021178, 77.3609628116894, 11.655816669136824, 66.66485041602017, 7.329091976767396, 55.658764687902504, 4.924985039227315, 44.56803892063211, 3.299940624127866, 33.443216802941045, 2.048695380627787, 22.302074138733943, 0.9845421446758968, 11.1527721155093},
		},
		{
			desc:                 `Coordinate(85, 85) to Coordinate(0, 0), -1`,
			a:                    geom.Coord{85, 85},
			b:                    geom.Coord{0, 0},
			segmentMaxLength:     -1,
			resultantCoordinates: []float64{85, 85},
		},
		{
			desc:                 `Coordinate(0, 0) to Coordinate(0, 0), 0.29`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{0, 0},
			segmentMaxLength:     0.29,
			resultantCoordinates: []float64{0, 0},
		},
		{
			desc:                 `Coordinate(85, 85) to Coordinate(0, 0), 1.563200444168918`,
			a:                    geom.Coord{85, 85},
			b:                    geom.Coord{0, 0},
			segmentMaxLength:     1.563200444168918,
			resultantCoordinates: []float64{85, 85},
		},
	}
	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			convertedPoints := segmentizeCoords(test.a, test.b, test.segmentMaxLength)
			require.Equal(t, test.resultantCoordinates, convertedPoints)
		})
	}
}
