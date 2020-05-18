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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

type segmentizedMaxLengthAndExpGeog struct {
	maxSegmentLength float64
	segmentizedGeogWkt string
}

var modificationGeogTestCase = []struct {
	geogWkt string
	segmentize segmentizedMaxLengthAndExpGeog
}{
	{
		geogWkt: "POINT(1.0 1.0)",
		segmentize: segmentizedMaxLengthAndExpGeog{
			maxSegmentLength: 10000.0,
			segmentizedGeogWkt: "POINT(1.0 1.0)",
		},
	},
	{
		geogWkt: "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
		segmentize: segmentizedMaxLengthAndExpGeog{
			maxSegmentLength: 100000.0,
			segmentizedGeogWkt: "LINESTRING (1 1, 1.4998857365616758 1.5000570914791973, 2 2, 2.4998094835255658 2.500095075163195, 3 3)",
		},
	},
	{
		geogWkt: "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))",
		segmentize: segmentizedMaxLengthAndExpGeog{
			maxSegmentLength: 100000.0,
			segmentizedGeogWkt: "POLYGON ((0 0, 0.5 0, 1 0, 1 0.4999999999999999, 1 1, 0.4999619199226218 0.5000190382261059, 0 0))",
		},
	},
	{
		geogWkt: "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
		segmentize: segmentizedMaxLengthAndExpGeog{
			maxSegmentLength: 50000.0,
			segmentizedGeogWkt: "POLYGON ((0 0, 0.25 0, 0.5 0, 0.75 0, 1 0, 0.9999999999999998 0.25, 1 0.4999999999999999, 0.9999999999999998 0.7499999999999999, 1 1, 0.7499666792978344 0.7500166590029188, 0.4999619199226218 0.5000190382261059, 0.24997620022356357 0.2500118986534327, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
		},
	},
	{
		geogWkt: "MULTIPOINT((1.0 1.0), (2.0 2.0))",
		segmentize: segmentizedMaxLengthAndExpGeog{
			maxSegmentLength: 100000.0,
			segmentizedGeogWkt: "MULTIPOINT((1.0 1.0), (2.0 2.0))",
		},
	},
	{
		geogWkt: "MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))",
		segmentize: segmentizedMaxLengthAndExpGeog{
			maxSegmentLength: 100000.0,
			segmentizedGeogWkt: "MULTILINESTRING ((1 1, 1.4998857365616758 1.5000570914791973, 2 2, 2.4998094835255658 2.500095075163195, 3 3), (6 6, 6.5 6.000226803574719, 7 6))",
		},
	},
	{
		geogWkt: "MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))",
		segmentize: segmentizedMaxLengthAndExpGeog{
			maxSegmentLength: 100000.0,
			segmentizedGeogWkt: "MULTIPOLYGON (((3 3, 3.500000000000001 3.0001140264716564, 4 3, 3.999999999999999 3.5, 4 4, 3.4997331141752333 3.5001329429928956, 3 3)), ((0 0, 0.5 0, 1 0, 1 0.4999999999999999, 1 1, 0.4999619199226218 0.5000190382261059, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))",
		},
	},
	{
		geogWkt: "GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
		segmentize: segmentizedMaxLengthAndExpGeog{
			maxSegmentLength: 1000000.0,
			segmentizedGeogWkt: "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 14.88248913028353 15.054670903122675, 20 20, 17.84783705611024 25.063693381403255, 15.510294720403053 30.09369304154877, 12.922758839587525 35.07789165618019, 10 40), POLYGON ((40 40, 30.404184608679003 42.93646050008845, 20 45, 27.258698904670663 41.785178536245354, 33.78296512799812 38.158856049677816, 39.6629587979805 34.20684491196235, 45 30, 42.6532474552537 35.02554212454631, 40 40)))",
		},
	},
}

func TestSegmentizeGeography(t *testing.T) {
	for _, test := range modificationGeogTestCase {
		t.Run(fmt.Sprintf("%s, maximum segment length: %v", test.geogWkt, test.segmentize.maxSegmentLength), func(t *testing.T) {
			geog, err := geo.ParseGeography(test.geogWkt)
			require.NoError(t, err)
			modifiedGeog, err := SegmentizeGeography(geog, test.segmentize.maxSegmentLength)
			require.NoError(t, err)
			expectedGeog, err := geo.ParseGeography(test.segmentize.segmentizedGeogWkt)
			require.NoError(t, err)
			require.Equal(t, expectedGeog, modifiedGeog)
		})
	}
	// Test for segment maximum length as negative.
	t.Run(fmt.Sprintf("%s, maximum segment length: %v", modificationGeogTestCase[0].geogWkt, -1.0), func(t *testing.T) {
		geog, err := geo.ParseGeography(modificationGeogTestCase[0].geogWkt)
		require.NoError(t, err)
		_, err = SegmentizeGeography(geog, -1)
		require.Error(t, errors.Newf("maximum segment length must be positive"))
	})
}

func TestSegmentizeCoords(t *testing.T) {
	testCase := []struct{
		desc                string
		coor1               geom.Coord
		coor2               geom.Coord
		segmentMaxLength    float64
		resultantCoordinate []float64
	}{
		{
			`Coordinate(0, 0) to Coordinate(85, 85) max segment length 4979581.87064`,
			geom.Coord{0, 0},
			geom.Coord{85, 85},
			4979581.87064,
			[]float64{0, 0, 4.924985039227315, 44.568038920632105},
		},
		{
			`Coordinate(0, 0) to Coordinate(85, 85) max segment length 4979580.87064`,
			geom.Coord{0, 0},
			geom.Coord{85, 85},
			4979580.87064,
			[]float64{0, 0, 2.0486953806277866, 22.302074138733936, 4.924985039227315, 44.568038920632105, 11.655816669136822, 66.66485041602017},
		},
		{
			`Coordinate(85, 85) to Coordinate(0, 0) max segment length 2879580.87064`,
			geom.Coord{85, 85},
			geom.Coord{0, 0},
			1879580.87064,
			[]float64{85, 85, 22.871662720021178, 77.3609628116894, 11.655816669136824, 66.66485041602017, 7.329091976767396, 55.658764687902504, 4.924985039227315, 44.56803892063211, 3.299940624127866, 33.443216802941045, 2.048695380627787, 22.302074138733943, 0.9845421446758968, 11.1527721155093},
		},
		{`Coordinate(85, 85) to Coordinate(0, 0) max segment length -1`,
			geom.Coord{85, 85},
			geom.Coord{0, 0},
			-1,
			[]float64{85, 85},
		},
	}
	for _, test := range testCase {
		t.Run(test.desc, func(t * testing.T) {
			convertedPoints := segmentizeCoords(test.coor1, test.coor2, test.segmentMaxLength)
			require.Equal(t, test.resultantCoordinate, convertedPoints)
		})
	}
}
