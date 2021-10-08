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
	"math"
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
		{
			wkt:              "LINESTRING(0 0 25, 0 1 0, 2 5 100)",
			maxSegmentLength: 150000.0,
			expectedWKT:      "LINESTRING Z (0 0 25,0 1 0,0.49878052093921765 2.0003038990352664 25,0.9981696941692514 3.0004561476391296 50,1.4984735304805308 4.000380457593079 75,2 5 100)",
		},
		{
			wkt:              "LINESTRING(0 0, 1 1)",
			maxSegmentLength: math.NaN(),
			expectedWKT:      "LINESTRING(0 0, 1 1)",
		},
		{
			wkt:              "LINESTRING M (0 0 0, 1 1 1)",
			maxSegmentLength: math.Sqrt(-1),
			expectedWKT:      "LINESTRING M (0 0 0, 1 1 1)",
		},
		{
			wkt:              "LINESTRING ZM (0 0 0 0, 1 1 1 1)",
			maxSegmentLength: -math.NaN(),
			expectedWKT:      "LINESTRING(0 0 0 0, 1 1 1 1)",
		},
		{
			wkt:              "LINESTRING(0 0, 1 1)",
			maxSegmentLength: math.Inf(1),
			expectedWKT:      "LINESTRING(0 0, 1 1)",
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

	t.Run("many coordinates to segmentize", func(t *testing.T) {
		g := geo.MustParseGeography("LINESTRING(0 0, 100 80)")
		_, err := Segmentize(g, 0.001)
		require.EqualError(
			t,
			err,
			fmt.Sprintf("attempting to segmentize into too many coordinates; need 34359738370 points between [0 0] and [100 80], max %d", geo.MaxAllowedSplitPoints),
		)
	})
	t.Run("overflowing number of coordinates to segmentize", func(t *testing.T) {
		g := geo.MustParseGeography("LINESTRING Z (-169.79088499002907 8.884172679558333 -4827356730.650944,50.66238188467506 -42.736039899804595 -8796356262.766115,81.80150918285182 -53.84161280004709 -8387269145.754486,30.179109503087716 67.82372267760985 3004789468.230095,20.563302070933446 62.59700266527605 -4314324960.005148)")
		_, err := Segmentize(g, 1.401298464324817e-45)
		require.EqualError(
			t,
			err,
			fmt.Sprintf("attempting to segmentize into too many coordinates; need 35917864239044270000000000000000000000000000000000000 points between [-169.79088499002907 8.884172679558333 -4.827356730650944e+09] and [50.66238188467506 -42.736039899804595 -8.796356262766115e+09], max %d", geo.MaxAllowedSplitPoints),
		)
	})
}

func TestSegmentizeCoords(t *testing.T) {
	testCases := []struct {
		desc                 string
		a                    geom.Coord
		b                    geom.Coord
		segmentMaxAngle      float64
		resultantCoordinates []float64
	}{
		{
			desc:                 `Coordinate(0, 0) to Coordinate(85, 85), 0.781600222084459`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{85, 85},
			segmentMaxAngle:      0.781600222084459,
			resultantCoordinates: []float64{0, 0, 4.924985039227315, 44.568038920632105},
		},
		{
			desc:                 `Coordinate(0, 0) to Coordinate(85, 85), 0.7816000651234446`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{85, 85},
			segmentMaxAngle:      0.7816000651234446,
			resultantCoordinates: []float64{0, 0, 2.0486953806277866, 22.302074138733936, 4.924985039227315, 44.568038920632105, 11.655816669136822, 66.66485041602017},
		},
		{
			desc:                 `Coordinate(85, 85) to Coordinate(0, 0), 0.29502092024628396`,
			a:                    geom.Coord{85, 85},
			b:                    geom.Coord{0, 0},
			segmentMaxAngle:      0.29502092024628396,
			resultantCoordinates: []float64{85, 85, 22.871662720021178, 77.3609628116894, 11.655816669136824, 66.66485041602017, 7.329091976767396, 55.658764687902504, 4.924985039227315, 44.56803892063211, 3.299940624127866, 33.443216802941045, 2.048695380627787, 22.302074138733943, 0.9845421446758968, 11.1527721155093},
		},
		{
			desc:                 `Coordinate(0, 0) to Coordinate(0, 0), 0.29`,
			a:                    geom.Coord{0, 0},
			b:                    geom.Coord{0, 0},
			segmentMaxAngle:      0.29,
			resultantCoordinates: []float64{0, 0},
		},
		{
			desc:                 `Coordinate(85, 85) to Coordinate(0, 0), 1.563200444168918`,
			a:                    geom.Coord{85, 85},
			b:                    geom.Coord{0, 0},
			segmentMaxAngle:      1.563200444168918,
			resultantCoordinates: []float64{85, 85},
		},
		{
			desc:                 `Coordinate(0, 16, 23, 10) to Coordinate(1, 0, -5, 0), 0.07848050723825097`,
			a:                    geom.Coord{0, 16, 23, 10},
			b:                    geom.Coord{1, 0, -5, 0},
			segmentMaxAngle:      0.07848050723825097,
			resultantCoordinates: []float64{0, 16, 23, 10, 0.2587272349066411, 12.000265604969918, 16, 7.5, 0.5098761137159087, 8.00030056571944, 9, 5, 0.7561364483510505, 4.000186752673144, 2, 2.5},
		},
	}
	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			convertedPoints, err := segmentizeCoords(test.a, test.b, test.segmentMaxAngle)
			require.NoError(t, err)
			require.Equal(t, test.resultantCoordinates, convertedPoints)
		})
	}

	errorTestCases := []struct {
		desc            string
		a               geom.Coord
		b               geom.Coord
		segmentMaxAngle float64
		expectedErr     string
	}{
		{
			desc:            "too many segments required",
			a:               geom.Coord{0, 100},
			b:               geom.Coord{1, 1},
			segmentMaxAngle: 2e-8,
			expectedErr:     fmt.Sprintf("attempting to segmentize into too many coordinates; need 268435458 points between [0 100] and [1 1], max %d", geo.MaxAllowedSplitPoints),
		},
		{
			desc:            "input coords have different dimensions",
			a:               geom.Coord{1, 1},
			b:               geom.Coord{1, 2, 3},
			segmentMaxAngle: 1,
			expectedErr:     "cannot segmentize two coordinates of different dimensions",
		},
		{
			desc:            "negative max segment angle",
			a:               geom.Coord{1, 1},
			b:               geom.Coord{2, 2},
			segmentMaxAngle: -1,
			expectedErr:     "maximum segment angle must be positive",
		},
	}
	for _, test := range errorTestCases {
		t.Run(test.desc, func(t *testing.T) {
			_, err := segmentizeCoords(test.a, test.b, test.segmentMaxAngle)
			require.EqualError(t, err, test.expectedErr)
		})
	}
}
