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
			expectedWKT:      "LINESTRING (1.0 1.0, 1.2000000000000002 1.2000000000000002, 1.4 1.4, 1.6 1.6, 1.8 1.8, 2.0 2.0, 2.2 2.2, 2.4000000000000004 2.4000000000000004, 2.6 2.6, 2.8000000000000003 2.8000000000000003, 3 3)",
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
			wkt:              "LINESTRING M (0 0 23, 1 0 -5)",
			maxSegmentLength: 0.25,
			expectedWKT:      "LINESTRING M (0 0 23, 0.25 0 16, 0.5 0 9, 0.75 0 2, 1 0 -5)",
		},
		{
			wkt:              "LINESTRING ZM (0 0 23 10, 1 0 -5 0)",
			maxSegmentLength: 0.5,
			expectedWKT:      "LINESTRING ZM (0 0 23 10,0.5 0 9 5,1 0 -5 0)",
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
			wkt:              "POLYGON Z EMPTY",
			maxSegmentLength: 1,
			expectedWKT:      "POLYGON Z EMPTY",
		},
		{
			wkt:              "POLYGON Z ((1 1 0, 5 1 10, 5 3 20, 1 3 30, 1 1 0))",
			maxSegmentLength: 2,
			expectedWKT:      "POLYGON Z ((1 1 0, 3 1 5, 5 1 10, 5 3 20, 3 3 25, 1 3 30, 1 1 0))",
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
	t.Run("Error when segments to make is infinite", func(t *testing.T) {
		geom, err := geo.ParseGeometry(" POLYGON M ((-5757990590.2166 -4519452260.862033 2804858792.6955757,-3495930342.911956 -3913707431.095213 9956876080.738373,-5456424425.598897 -8951413012.610432 3893065263.609459,5171950909.470152 -3160389565.052106 -1536304847.4179764,7229852360.871143 -1100415531.4846077 -2312950462.5578575,3596535169.705099 771146579.7308273 -405855460.51558876,7650485577.341057 3911494351.8519344 9371555787.233376,5202820209.160244 4927246719.614132 750359531.9446983,-379577383.76449776 8738277761.50978 -7426242721.149497,-4063987925.4022484 3815950665.1846447 4224483522.3448334,-5757990590.2166 -4519452260.862033 2804858792.6955757))")
		require.NoError(t, err)
		_, err = Segmentize(geom, 5e-324)
		require.EqualError(
			t,
			err,
			fmt.Sprintf("attempting to segmentize into too many coordinates; need +Inf points between [-5.7579905902166e+09 -4.519452260862033e+09 2.8048587926955757e+09] and [-3.495930342911956e+09 -3.913707431095213e+09 9.956876080738373e+09], max %d", geo.MaxAllowedSplitPoints),
		)
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
			convertedPoints, err := segmentizeCoords(test.a, test.b, test.segmentMaxLength)
			require.NoError(t, err)
			require.Equal(t, test.resultantCoordinates, convertedPoints)
		})
	}

	errorTestCases := []struct {
		desc             string
		a                geom.Coord
		b                geom.Coord
		segmentMaxLength float64
		expectedErr      string
	}{
		{
			desc:             "too many segments required",
			a:                geom.Coord{0, 0},
			b:                geom.Coord{100, 100},
			segmentMaxLength: 0.001,
			expectedErr:      fmt.Sprintf("attempting to segmentize into too many coordinates; need 282846 points between [0 0] and [100 100], max %d", geo.MaxAllowedSplitPoints),
		},
		{
			desc:             "input coords have different dimensions",
			a:                geom.Coord{1, 1},
			b:                geom.Coord{1, 2, 3},
			segmentMaxLength: 1,
			expectedErr:      "cannot segmentize two coordinates of different dimensions",
		},
		{
			desc:             "negative max segment length",
			a:                geom.Coord{1, 1},
			b:                geom.Coord{2, 2},
			segmentMaxLength: -1,
			expectedErr:      "maximum segment length must be positive",
		},
	}
	for _, test := range errorTestCases {
		t.Run(test.desc, func(t *testing.T) {
			_, err := segmentizeCoords(test.a, test.b, test.segmentMaxLength)
			require.EqualError(t, err, test.expectedErr)
		})
	}
}
