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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/golang/geo/s1"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

type unaryOperatorExpectedResult struct {
	expectedArea      float64
	expectedLength    float64
	expectedPerimeter float64
}

var unaryOperatorTestCases = []struct {
	wkt      string
	sphere   unaryOperatorExpectedResult
	spheroid unaryOperatorExpectedResult
}{
	{
		wkt: "POINT(1.0 1.0)",
	},
	{
		wkt: "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
		sphere: unaryOperatorExpectedResult{
			expectedLength: 314403.4167139704,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedLength: 313705.47851796006,
		},
	},
	{
		wkt: "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))",
		sphere: unaryOperatorExpectedResult{
			expectedArea:      6182486746.455541,
			expectedPerimeter: 379639.75723776827,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedArea:      6154854786.721433,
			expectedPerimeter: 378793.4476424126,
		},
	},
	{
		wkt: "SRID=4004;LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)",
		sphere: unaryOperatorExpectedResult{
			expectedLength: 314367.99984330626,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedLength: 313672.2213232639,
		},
	},
	{
		wkt: "SRID=4004;POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))",
		sphere: unaryOperatorExpectedResult{
			expectedArea:      6181093937.160788,
			expectedPerimeter: 379596.9916332415,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedArea:      6153550906.915973,
			expectedPerimeter: 378753.30454341066,
		},
	},
	{
		wkt: "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
		sphere: unaryOperatorExpectedResult{
			expectedArea:      6120665080.445181,
			expectedPerimeter: 417604.087288779,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedArea:      6093309483.796953,
			expectedPerimeter: 416673.1281208417,
		},
	},
	{
		wkt: "MULTIPOINT((1.0 1.0), (2.0 2.0))",
	},
	{
		wkt: "MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))",
		sphere: unaryOperatorExpectedResult{
			expectedLength: 424989.34283080546,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedLength: 424419.1832424484,
		},
	},
	{
		wkt: "MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))",
		sphere: unaryOperatorExpectedResult{
			expectedArea:      12294677441.341661,
			expectedPerimeter: 796947.8473004946,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedArea:      12240009431.86529,
			expectedPerimeter: 795178.6592721482,
		},
	},
	{
		wkt: "GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
		sphere: unaryOperatorExpectedResult{
			expectedArea:      691570576619.521,
			expectedLength:    9637039.459995955,
			expectedPerimeter: 9637039.459995955,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedArea:      691638769184.1753,
			expectedLength:    9632838.874863794,
			expectedPerimeter: 9632838.874863794,
		},
	},
	{
		wkt: "GEOMETRYCOLLECTION (MULTIPOINT EMPTY, POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
		sphere: unaryOperatorExpectedResult{
			expectedArea:      691570576619.521,
			expectedLength:    9637039.459995955,
			expectedPerimeter: 9637039.459995955,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedArea:      691638769184.1753,
			expectedLength:    9632838.874863794,
			expectedPerimeter: 9632838.874863794,
		},
	},
	{
		wkt: "GEOMETRYCOLLECTION EMPTY",
		sphere: unaryOperatorExpectedResult{
			expectedArea:      0,
			expectedLength:    0,
			expectedPerimeter: 0,
		},
		spheroid: unaryOperatorExpectedResult{
			expectedArea:      0,
			expectedLength:    0,
			expectedPerimeter: 0,
		},
	},
}

func TestArea(t *testing.T) {
	for _, tc := range unaryOperatorTestCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeography(tc.wkt)
			require.NoError(t, err)

			for _, subTC := range []struct {
				desc                string
				useSphereOrSpheroid UseSphereOrSpheroid
				expected            float64
			}{
				{"sphere", UseSphere, tc.sphere.expectedArea},
				{"spheroid", UseSpheroid, tc.spheroid.expectedArea},
			} {
				t.Run(subTC.desc, func(t *testing.T) {
					ret, err := Area(g, subTC.useSphereOrSpheroid)
					require.NoError(t, err)
					require.LessOrEqualf(
						t,
						math.Abs(ret-subTC.expected),
						0.1, // allow 0.1m^2 difference.
						"expected %f, found %f",
						subTC.expected,
						ret,
					)
				})
			}
		})
	}
}

func TestPerimeter(t *testing.T) {
	for _, tc := range unaryOperatorTestCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeography(tc.wkt)
			require.NoError(t, err)

			for _, subTC := range []struct {
				desc                string
				useSphereOrSpheroid UseSphereOrSpheroid
				expected            float64
			}{
				{"sphere", UseSphere, tc.sphere.expectedPerimeter},
				{"spheroid", UseSpheroid, tc.spheroid.expectedPerimeter},
			} {
				t.Run(subTC.desc, func(t *testing.T) {
					ret, err := Perimeter(g, subTC.useSphereOrSpheroid)
					require.NoError(t, err)
					require.LessOrEqualf(
						t,
						math.Abs(ret-subTC.expected),
						0.01, // allow 0.01m difference.
						"expected %f, found %f",
						subTC.expected,
						ret,
					)
				})
			}
		})
	}
}

func TestLength(t *testing.T) {
	for _, tc := range unaryOperatorTestCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeography(tc.wkt)
			require.NoError(t, err)

			for _, subTC := range []struct {
				desc                string
				useSphereOrSpheroid UseSphereOrSpheroid
				expected            float64
			}{
				{"sphere", UseSphere, tc.sphere.expectedLength},
				{"spheroid", UseSpheroid, tc.spheroid.expectedLength},
			} {
				t.Run(subTC.desc, func(t *testing.T) {
					ret, err := Length(g, subTC.useSphereOrSpheroid)
					require.NoError(t, err)
					require.LessOrEqualf(
						t,
						math.Abs(ret-subTC.expected),
						0.01, // allow 0.01m difference
						"expected %f, found %f",
						subTC.expected,
						ret,
					)
				})
			}
		})
	}
}

func TestProject(t *testing.T) {
	var testCases = []struct {
		desc      string
		point     geo.Geography
		distance  float64
		azimuth   float64
		projected geo.Geography
	}{
		{
			"POINT(0 0), 100000, radians(45)",
			geo.MustMakeGeographyFromGeomT(geom.NewPointFlat(geom.XY, []float64{0, 0}).SetSRID(4326)),
			100000,
			45 * math.Pi / 180.0,
			geo.MustMakeGeographyFromGeomT(geom.NewPointFlat(geom.XY, []float64{0.6352310291255374, 0.6394723347291977}).SetSRID(4326)),
		},
		{
			"SRID=4004;POINT(0 0), 100000, radians(45)",
			geo.MustMakeGeographyFromGeomT(geom.NewPointFlat(geom.XY, []float64{0, 0}).SetSRID(4004)),
			100000,
			45 * math.Pi / 180.0,
			geo.MustMakeGeographyFromGeomT(geom.NewPointFlat(geom.XY, []float64{0.6353047281438549, 0.6395336363116583}).SetSRID(4004)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			projected, err := Project(tc.point, tc.distance, s1.Angle(tc.azimuth))
			require.NoError(t, err)
			require.Equalf(
				t,
				tc.projected,
				projected,
				"expected %f, found %f",
				&tc.projected,
				projected,
			)
		})
	}

	errorTestCases := []struct {
		p           string
		d           float64
		a           s1.Angle
		expectedErr string
	}{
		{
			"POINT EMPTY",
			0,
			0,
			"cannot project POINT EMPTY",
		},
	}
	for _, tc := range errorTestCases {
		t.Run(tc.expectedErr, func(t *testing.T) {
			p, err := geo.ParseGeography(tc.p)
			require.NoError(t, err)

			_, err = Project(p, tc.d, tc.a)
			require.Error(t, err)
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}
