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

func TestLineInterpolatePoints(t *testing.T) {
	var testCasesForLineInterpolate = []struct {
		wkb                       string
		errMsg                    string
		fraction                  float64
		expectedWKTForRepeatTrue  string
		expectedWKTForRepeatFalse string
	}{
		{
			wkb:      "LINESTRING (0 0, 1 1)",
			errMsg:   "fraction -0.200000 should be within [0 1] range",
			fraction: -0.2,
		},
		{
			wkb:      "LINESTRING (0 0, 1 1, 2 5)",
			errMsg:   "fraction 1.500000 should be within [0 1] range",
			fraction: 1.5,
		},
		{
			wkb:      "MULTILINESTRING ((0 0, 1 1, 2 5), (0 0, 1 1))",
			errMsg:   "geometry MultiLineString should be LineString",
			fraction: 0.3,
		},
		{
			wkb:      "POINT (0 0)",
			errMsg:   "geometry Point should be LineString",
			fraction: 0.3,
		},
		{
			wkb:      "POLYGON((-1.0 0.0, 0.0 0.0, 0.0 1.0, -1.0 1.0, -1.0 0.0))",
			errMsg:   "geometry Polygon should be LineString",
			fraction: 0.3,
		},
		{
			wkb:                       "LINESTRING (0 0, 1 1, 2 5)",
			fraction:                  0.51,
			expectedWKTForRepeatTrue:  "POINT (1.3419313865603413 2.367725546241365)",
			expectedWKTForRepeatFalse: "POINT (1.3419313865603413 2.367725546241365)",
		},
		{
			wkb:                       "LINESTRING (0 0, 1 1, 2 5)",
			fraction:                  0.5,
			expectedWKTForRepeatTrue:  "MULTIPOINT (1.3285014148574912 2.3140056594299647, 2 5)",
			expectedWKTForRepeatFalse: "POINT (1.3285014148574912 2.3140056594299647)",
		},
		{
			wkb:                       "LINESTRING (0 0, 1 1, 2 5)",
			fraction:                  0.2,
			expectedWKTForRepeatTrue:  "MULTIPOINT (0.78309518948453 0.78309518948453, 1.1942016978289893 1.7768067913159575, 1.462801131885993 2.851204527543972, 1.7314005659429965 3.925602263771986, 2 5)",
			expectedWKTForRepeatFalse: "POINT (0.78309518948453 0.78309518948453)",
		},
		{
			wkb:                       "LINESTRING (0 0, 1 1, 2 5)",
			fraction:                  0,
			expectedWKTForRepeatTrue:  "POINT (0 0)",
			expectedWKTForRepeatFalse: "POINT (0 0)",
		},
		{
			wkb:                       "LINESTRING (0 0, 1 1, 2 5)",
			fraction:                  1,
			expectedWKTForRepeatTrue:  "POINT (2 5)",
			expectedWKTForRepeatFalse: "POINT (2 5)",
		},
	}
	for _, test := range testCasesForLineInterpolate {
		for _, repeat := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s for fraction %f where repeat is %t", test.wkb, test.fraction, repeat),
				func(t *testing.T) {
					geometry, err := geo.ParseGeometry(test.wkb)
					require.NoError(t, err)
					interpolatedPoint, err := LineInterpolatePoints(geometry, test.fraction, repeat)
					if test.errMsg == "" {
						require.NoError(t, err)
						expectedWKTForRepeat := test.expectedWKTForRepeatFalse
						if repeat {
							expectedWKTForRepeat = test.expectedWKTForRepeatTrue
						}
						expectedInterpolatedPoint, err := geo.ParseGeometry(expectedWKTForRepeat)
						require.NoError(t, err)
						require.Equal(t, expectedInterpolatedPoint, interpolatedPoint)
					} else {
						require.EqualError(t, err, test.errMsg)
					}
				})
		}
	}

	t.Run("too many points when repeat=true", func(t *testing.T) {
		g := geo.MustParseGeometry("LINESTRING(0 0, 100 100)")
		_, err := LineInterpolatePoints(g, 0.000001, true)
		require.EqualError(
			t,
			err,
			fmt.Sprintf(
				"attempting to interpolate into too many points; requires 1000000 points, max %d",
				geo.MaxAllowedSplitPoints,
			),
		)
	})

}
