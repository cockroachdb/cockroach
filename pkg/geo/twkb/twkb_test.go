// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package twkb

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestMarshal(t *testing.T) {
	testCases := []struct {
		desc     string
		t        geom.T
		opts     []MarshalOption
		expected []byte
	}{
		// POINT
		{
			desc:     "empty point",
			t:        geom.NewPointEmpty(geom.XY),
			expected: mustDecodeHex("0110"),
		},
		{
			desc:     "point",
			t:        geom.NewPointFlat(geom.XY, []float64{1.5, 2.5}),
			expected: mustDecodeHex("01000406"),
		},
		// LINESTRING
		{
			desc:     "empty linestring",
			t:        geom.NewLineString(geom.XY),
			expected: mustDecodeHex("0210"),
		},
		{
			desc: "2D linestring",
			t: geom.NewLineStringFlat(
				geom.XY,
				[]float64{
					1.555, 2.666,
					55.444, 66.555,
					33.333, 21.211,
				},
			),
			expected: mustDecodeHex("02000304066a80012b5b"),
		},
		{
			desc: "2D linestring, precision 1",
			t: geom.NewLineStringFlat(
				geom.XY,
				[]float64{
					1.555, 2.666,
					55.444, 66.555,
					33.333, 21.211,
				},
			),
			opts: []MarshalOption{
				MarshalOptionPrecisionXY(1),
			},
			expected: mustDecodeHex("2200032036b408fe09b9038b07"),
		},
		{
			desc: "2D linestring, precision -1",
			t: geom.NewLineStringFlat(
				geom.XY,
				[]float64{
					1.555, 2.666,
					55.444, 66.555,
					33.333, 21.211,
				},
			),
			opts: []MarshalOption{
				MarshalOptionPrecisionXY(-1),
			},
			expected: mustDecodeHex("12000300000c0e0509"),
		},
		{
			desc: "3D linestring, precision XY 1",
			t: geom.NewLineStringFlat(
				geom.XYZ,
				[]float64{
					1.555, 2.666, 3.777,
					55.444, 66.555, 4.699,
					33.333, 21.211, 8.9111,
				},
			),
			opts: []MarshalOption{
				MarshalOptionPrecisionXY(1),
			},
			expected: mustDecodeHex("22080103203608b408fe0902b9038b0708"),
		},
		{
			desc: "M linestring, precision XY 1",
			t: geom.NewLineStringFlat(
				geom.XYM,
				[]float64{
					1.555, 2.666, 3.777,
					55.444, 66.555, 4.699,
					33.333, 21.211, 8.9111,
				},
			),
			opts: []MarshalOption{
				MarshalOptionPrecisionXY(1),
			},
			expected: mustDecodeHex("22080203203608b408fe0902b9038b0708"),
		},
		{
			desc: "4D linestring, precision XY 1, Z 2, M 3",
			t: geom.NewLineStringFlat(
				geom.XYZM,
				[]float64{
					1.555, 2.666, 3.777, -23232,
					55.444, 66.555, 4.699, 323224,
					33.333, 21.211, 8.9111, 231232132,
				},
			),
			opts: []MarshalOption{
				MarshalOptionPrecisionXY(1),
				MarshalOptionPrecisionZ(2),
				MarshalOptionPrecisionM(3),
			},
			expected: mustDecodeHex("22086b032036f405fff79316b408fe09b80180ffb3ca02b9038b07ca06c0c7f2b3b80d"),
		},
		// POLYGON
		{
			desc:     "POLYGON EMPTY",
			t:        geom.NewPolygon(geom.XY),
			expected: mustDecodeHex("0310"),
		},
		{
			desc: "POLYGON",
			t: geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					-1, -1,
					10, 10,
					10, 0,
					-1, -1,
				},
				[]int{8},
			),
			expected: mustDecodeHex("030001040101161600131501"),
		},
		{
			desc: "POLYGON with ring",
			t: geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					-1, -1,
					10, 10,
					10, 0,
					-1, -1,

					1, 1,
					5, 5,
					5, 1,
					1, 1,
				},
				[]int{8, 16},
			),
			expected: mustDecodeHex("030002040101161600131501040404080800070700"),
		},
		// MULTIPOINT
		{
			desc:     "MULTIPOINT EMPTY",
			t:        geom.NewMultiPoint(geom.XY),
			expected: mustDecodeHex("0410"),
		},
		{
			desc:     "MULTIPOINT",
			t:        geom.NewMultiPointFlat(geom.XY, []float64{10, 10, 15, 5}),
			expected: mustDecodeHex("04000214140a09"),
		},
		{
			desc: "MULTIPOINT with empty element",
			t: geom.NewMultiPointFlat(
				geom.XY,
				[]float64{10, 10, 15, 5},
				geom.NewMultiPointFlatOptionWithEnds([]int{2, 2, 4}),
			),
			expected: mustDecodeHex("04000214140a09"),
		},
		// MULTILINESTRING
		{
			desc:     "MULTILINESTRING EMPTY",
			t:        geom.NewMultiLineString(geom.XY),
			expected: mustDecodeHex("0510"),
		},
		{
			desc: "MULTILINESTRING",
			t: geom.NewMultiLineStringFlat(
				geom.XY,
				[]float64{
					10, 10, 15, 5,
					23, 13, -5, 77, 8, 33,
				},
				[]int{4, 10},
			),
			expected: mustDecodeHex("0500020214140a090310103780011a57"),
		},
		{
			desc: "MULTILINESTRING with EMPTY element",
			t: geom.NewMultiLineStringFlat(
				geom.XY,
				[]float64{
					10, 10, 15, 5,
					23, 13, -5, 77, 8, 33,
				},
				[]int{4, 10, 10},
			),
			expected: mustDecodeHex("0500030214140a090310103780011a5700"),
		},
		// MULTIPOLYGON
		{
			desc:     "MULTIPOLYGON EMPTY",
			t:        geom.NewMultiPolygon(geom.XY),
			expected: mustDecodeHex("0610"),
		},
		{
			desc: "MULTIPOLYGON",
			t: geom.NewMultiPolygonFlat(
				geom.XY,
				[]float64{
					-1, -1,
					10, 10,
					10, 0,
					-1, -1,

					1, 1,
					5, 5,
					5, 1,
					1, 1,

					15, 15,
					35, 15,
					65, 65,
					15, 15,
				},
				[][]int{
					{8, 16},
					{24},
				},
			),
			expected: mustDecodeHex("0600020204010116160013150104040408080007070001041c1c28003c646363"),
		},
		{
			desc: "MULTIPOLYGON with empty",
			t: geom.NewMultiPolygonFlat(
				geom.XY,
				[]float64{
					-1, -1,
					10, 10,
					10, 0,
					-1, -1,

					1, 1,
					5, 5,
					5, 1,
					1, 1,

					15, 15,
					35, 15,
					65, 65,
					15, 15,
				},
				[][]int{
					{8, 16},
					{},
					{24},
				},
			),
			expected: mustDecodeHex("060003020401011616001315010404040808000707000001041c1c28003c646363"),
		},
		// GEOMETRYCOLLECTION
		{
			desc:     "GEOMETRYCOLLECTION EMPTY",
			t:        geom.NewGeometryCollection().MustSetLayout(geom.XY),
			expected: mustDecodeHex("0710"),
		},
		{
			desc: "GEOMETRYCOLLECTION",
			t: geom.NewGeometryCollection().MustSetLayout(geom.XY).MustPush(
				geom.NewPointFlat(geom.XY, []float64{15, 25}),
				geom.NewPointFlat(geom.XY, []float64{25, 35}),
				geom.NewGeometryCollection().MustPush(
					geom.NewLineStringFlat(
						geom.XY,
						[]float64{
							-30, 60,
							-15, -15,
							30, 35,
						},
					),
				),
			),
			expected: mustDecodeHex("07000301001e32010032460700010200033b781e95015a64"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			out, err := Marshal(tc.t, tc.opts...)
			require.NoError(t, err)
			require.Equal(t, tc.expected, out)
		})
	}

	errorTestCases := []struct {
		desc                string
		t                   geom.T
		opts                []MarshalOption
		expectedErrorString string
	}{
		{
			desc: "XY precision too small",
			t:    geom.NewPointEmpty(geom.XYZM),
			opts: []MarshalOption{
				MarshalOptionPrecisionXY(-8),
			},
			expectedErrorString: "XY precision must be between -7 and 7 inclusive",
		},
		{
			desc: "XY precision too large",
			t:    geom.NewPointEmpty(geom.XYZM),
			opts: []MarshalOption{
				MarshalOptionPrecisionXY(8),
			},
			expectedErrorString: "XY precision must be between -7 and 7 inclusive",
		},
		{
			desc: "Z precision too small",
			t:    geom.NewPointEmpty(geom.XYZM),
			opts: []MarshalOption{
				MarshalOptionPrecisionZ(-1),
			},
			expectedErrorString: "Z precision must not be negative or greater than 7",
		},
		{
			desc: "Z precision too large",
			t:    geom.NewPointEmpty(geom.XYZM),
			opts: []MarshalOption{
				MarshalOptionPrecisionZ(8),
			},
			expectedErrorString: "Z precision must not be negative or greater than 7",
		},
		{
			desc: "M precision too small",
			t:    geom.NewPointEmpty(geom.XYZM),
			opts: []MarshalOption{
				MarshalOptionPrecisionM(-1),
			},
			expectedErrorString: "M precision must not be negative or greater than 7",
		},
		{
			desc: "M precision too large",
			t:    geom.NewPointEmpty(geom.XYZM),
			opts: []MarshalOption{
				MarshalOptionPrecisionM(8),
			},
			expectedErrorString: "M precision must not be negative or greater than 7",
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := Marshal(tc.t, tc.opts...)
			require.Error(t, err)
			require.EqualError(t, err, tc.expectedErrorString)
		})
	}
}

func mustDecodeHex(h string) []byte {
	ret, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	return ret
}
