// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package wkt

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestUnmarshal(t *testing.T) {
	testCases := []struct {
		desc        string
		equivInputs []string
		expected    geom.T
	}{
		// POINT tests
		{
			desc:        "parse 2D point",
			equivInputs: []string{"POINT(0 1)", "POINT (0 1)", "point(0 1)", "point ( 0 1 )"},
			expected:    geom.NewPointFlat(geom.XY, []float64{0, 1}),
		},
		{
			desc:        "parse 2D+M point",
			equivInputs: []string{"POINT M (-2 0 0.5)", "POINTM(-2 0 0.5)", "POINTM(-2 0 .5)"},
			expected:    geom.NewPointFlat(geom.XYM, []float64{-2, 0, 0.5}),
		},
		{
			desc:        "parse 3D point",
			equivInputs: []string{"POINT Z (2 3 4)", "POINTZ(2 3 4)", "POINT(2 3 4)"},
			expected:    geom.NewPointFlat(geom.XYZ, []float64{2, 3, 4}),
		},
		{
			desc:        "parse 4D point",
			equivInputs: []string{"POINT ZM (0 5 -10 15)", "POINTZM (0 5 -10 15)", "POINT(0 5 -10 15)"},
			expected:    geom.NewPointFlat(geom.XYZM, []float64{0, 5, -10, 15}),
		},
		{
			desc:        "parse empty 2D point",
			equivInputs: []string{"POINT EMPTY"},
			expected:    geom.NewPointEmpty(geom.XY),
		},
		{
			desc:        "parse empty 2D+M point",
			equivInputs: []string{"POINT M EMPTY", "POINTM EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYM),
		},
		{
			desc:        "parse empty 3D point",
			equivInputs: []string{"POINT Z EMPTY", "POINTZ EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYZ),
		},
		{
			desc:        "parse empty 4D point",
			equivInputs: []string{"POINT ZM EMPTY", "POINTZM EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYZM),
		},
		// LINESTRING tests
		{
			desc:        "parse 2D linestring",
			equivInputs: []string{"LINESTRING(0 0, 1 1, 3 4)", "LINESTRING (0 0, 1 1, 3 4)", "linestring ( 0 0, 1 1, 3 4 )"},
			expected:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 3, 4}),
		},
		{
			desc:        "parse 2D+M linestring",
			equivInputs: []string{"LINESTRING M(0 0 200, 0.1 -1 -20)", "LINESTRINGM(0 0 200, .1 -1 -20)"},
			expected:    geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 200, 0.1, -1, -20}),
		},
		{
			desc:        "parse 3D linestring",
			equivInputs: []string{"LINESTRING(0 -1 1, 7 -1 -9)", "LINESTRING Z(0 -1 1, 7 -1 -9)", "LINESTRINGZ(0 -1 1, 7 -1 -9)"},
			expected:    geom.NewLineStringFlat(geom.XYZ, []float64{0, -1, 1, 7, -1, -9}),
		},
		{
			desc:        "parse 4D linestring",
			equivInputs: []string{"LINESTRING(0 0 0 0, 1 1 1 1)", "LINESTRING ZM (0 0 0 0, 1 1 1 1)", "LINESTRINGZM (0 0 0 0, 1 1 1 1)"},
			expected:    geom.NewLineStringFlat(geom.XYZM, []float64{0, 0, 0, 0, 1, 1, 1, 1}),
		},
		{
			desc:        "parse empty 2D linestring",
			equivInputs: []string{"LINESTRING EMPTY"},
			expected:    geom.NewLineString(geom.XY),
		},
		{
			desc:        "parse empty 2D+M linestring",
			equivInputs: []string{"LINESTRING M EMPTY", "LINESTRINGM EMPTY"},
			expected:    geom.NewLineString(geom.XYM),
		},
		{
			desc:        "parse empty 3D linestring",
			equivInputs: []string{"LINESTRING Z EMPTY", "LINESTRINGZ EMPTY"},
			expected:    geom.NewLineString(geom.XYZ),
		},
		{
			desc:        "parse empty 4D linestring",
			equivInputs: []string{"LINESTRING ZM EMPTY", "LINESTRINGZM EMPTY"},
			expected:    geom.NewLineString(geom.XYZM),
		},
		// POLYGON tests
		{
			desc:        "parse 2D polygon",
			equivInputs: []string{"POLYGON((0 0, 1 -1, 2 0, 0 0))", "POLYGON ((0 0, 1 -1, 2 0, 0 0))"},
			expected:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, -1, 2, 0, 0, 0}, []int{8}),
		},
		{
			desc:        "parse 2D polygon with hole",
			equivInputs: []string{"POLYGON((0 0, 0 100, 100 100, 100 0, 0 0),(10 10, 11 11, 12 10, 10 10))"},
			expected: geom.NewPolygonFlat(geom.XY,
				[]float64{0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 10, 10, 11, 11, 12, 10, 10, 10}, []int{10, 18}),
		},
		{
			desc:        "parse 2D polygon with two holes",
			equivInputs: []string{"POLYGON((0 0, 0 100, 100 100, 100 0, 0 0),(10 10, 11 11, 12 10, 10 10), (2 2, 4 4, 5 1, 2 2))"},
			expected: geom.NewPolygonFlat(geom.XY,
				[]float64{0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 10, 10, 11, 11, 12, 10, 10, 10, 2, 2, 4, 4, 5, 1, 2, 2}, []int{10, 18, 26}),
		},
		{
			desc:        "parse 2D+M polygon",
			equivInputs: []string{"POLYGONM((0 0 7, 1 -1 -50, 2 0 0, 0 0 7))", "POLYGON M ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7))"},
			expected:    geom.NewPolygonFlat(geom.XYM, []float64{0, 0, 7, 1, -1, -50, 2, 0, 0, 0, 0, 7}, []int{12}),
		},
		{
			desc:        "parse 3D polygon",
			equivInputs: []string{"POLYGON((0 0 7, 1 -1 -50, 2 0 0, 0 0 7))", "POLYGON Z ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7))"},
			expected:    geom.NewPolygonFlat(geom.XYZ, []float64{0, 0, 7, 1, -1, -50, 2, 0, 0, 0, 0, 7}, []int{12}),
		},
		{
			desc:        "parse 4D polygon",
			equivInputs: []string{"POLYGON((0 0 12 7, 1 -1 12 -50, 2 0 12 0, 0 0 12 7))", "POLYGON ZM ((0 0 12 7, 1 -1 12 -50, 2 0 12 0, 0 0 12 7))"},
			expected:    geom.NewPolygonFlat(geom.XYZM, []float64{0, 0, 12, 7, 1, -1, 12, -50, 2, 0, 12, 0, 0, 0, 12, 7}, []int{16}),
		},
		{
			desc:        "parse empty 2D polygon",
			equivInputs: []string{"POLYGON EMPTY"},
			expected:    geom.NewPolygon(geom.XY),
		},
		{
			desc:        "parse empty 2D+M polygon",
			equivInputs: []string{"POLYGON M EMPTY", "POLYGONM EMPTY"},
			expected:    geom.NewPolygon(geom.XYM),
		},
		{
			desc:        "parse empty 3D polygon",
			equivInputs: []string{"POLYGON Z EMPTY", "POLYGONZ EMPTY"},
			expected:    geom.NewPolygon(geom.XYZ),
		},
		{
			desc:        "parse empty 4D polygon",
			equivInputs: []string{"POLYGON ZM EMPTY", "POLYGONZM EMPTY"},
			expected:    geom.NewPolygon(geom.XYZM),
		},
		{
			desc:        "parse 2D multipoint",
			equivInputs: []string{"MULTIPOINT(0 0, 1 1, 2 2)", "MULTIPOINT((0 0), 1 1, (2 2))", "MULTIPOINT (0 0, 1 1, 2 2)"},
			expected:    geom.NewMultiPointFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
		},
		{
			desc:        "parse 2D+M multipoint",
			equivInputs: []string{"MULTIPOINTM((-1 5 -16), .23 7 0)", "MULTIPOINT M (-1 5 -16, 0.23 7.0 0)"},
			expected:    geom.NewMultiPointFlat(geom.XYM, []float64{-1, 5, -16, 0.23, 7, 0}),
		},
		{
			desc:        "parse 3D multipoint",
			equivInputs: []string{"MULTIPOINT(2 1 3)", "MULTIPOINTZ(2 1 3)", "MULTIPOINT Z ((2 1 3))"},
			expected:    geom.NewMultiPointFlat(geom.XYZ, []float64{2, 1, 3}),
		},
		{
			desc:        "parse 4D multipoint",
			equivInputs: []string{"MULTIPOINT(2 -8 17 45, (0 0 0 0))", "MULTIPOINTZM((2 -8 17 45), (0 0 0 0))", "MULTIPOINT ZM (2 -8 17 45, 0 0 0 0)"},
			expected:    geom.NewMultiPointFlat(geom.XYZM, []float64{2, -8, 17, 45, 0, 0, 0, 0}),
		},
		{
			desc:        "parse 2D multipoint with EMPTY points",
			equivInputs: []string{"MULTIPOINT(EMPTY, 2 3, EMPTY)", "MULTIPOINT (EMPTY, (2 3), EMPTY)"},
			expected:    geom.NewMultiPointFlat(geom.XY, []float64{2, 3}, geom.NewMultiPointFlatOptionWithEnds([]int{0, 2, 2})),
		},
		{
			desc:        "parse 2D+M multipoint with EMPTY points",
			equivInputs: []string{"MULTIPOINTM(2 3 1, EMPTY)", "MULTIPOINT M ((2 3 1), EMPTY)"},
			expected:    geom.NewMultiPointFlat(geom.XYM, []float64{2, 3, 1}, geom.NewMultiPointFlatOptionWithEnds([]int{3, 3})),
		},
		{
			desc:        "parse 3D multipoint with EMPTY points",
			equivInputs: []string{"MULTIPOINTZ (EMPTY, EMPTY)", "MULTIPOINT Z (EMPTY, EMPTY)"},
			expected:    geom.NewMultiPointFlat(geom.XYZ, []float64(nil), geom.NewMultiPointFlatOptionWithEnds([]int{0, 0})),
		},
		{
			desc:        "parse 4D multipoint with EMPTY points",
			equivInputs: []string{"MULTIPOINTZM(EMPTY, 1 -1 1 -1)", "MULTIPOINT ZM (EMPTY, (1 -1 1 -1))"},
			expected:    geom.NewMultiPointFlat(geom.XYZM, []float64{1, -1, 1, -1}, geom.NewMultiPointFlatOptionWithEnds([]int{0, 4})),
		},
		{
			desc:        "parse empty 2D multipoint",
			equivInputs: []string{"MULTIPOINT EMPTY"},
			expected:    geom.NewMultiPoint(geom.XY),
		},
		{
			desc:        "parse empty 2D+M multipoint",
			equivInputs: []string{"MULTIPOINT M EMPTY", "MULTIPOINTM EMPTY"},
			expected:    geom.NewMultiPoint(geom.XYM),
		},
		{
			desc:        "parse empty 3D multipoint",
			equivInputs: []string{"MULTIPOINT Z EMPTY", "MULTIPOINTZ EMPTY"},
			expected:    geom.NewMultiPoint(geom.XYZ),
		},
		{
			desc:        "parse empty 4D multipoint",
			equivInputs: []string{"MULTIPOINT ZM EMPTY", "MULTIPOINTZM EMPTY"},
			expected:    geom.NewMultiPoint(geom.XYZM),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			want := tc.expected
			for _, input := range tc.equivInputs {
				got, err := Unmarshal(input)
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
}

func TestUnmarshalError(t *testing.T) {
	errorTestCases := []struct {
		desc           string
		input          string
		expectedErrStr string
	}{
		// LexError
		{
			desc:  "invalid character",
			input: "POINT{0 0}",
			expectedErrStr: `lex error: invalid character at pos 5
POINT{0 0}
     ^`,
		},
		{
			desc:  "invalid keyword",
			input: "DOT(0 0)",
			expectedErrStr: `lex error: invalid keyword at pos 0
DOT(0 0)
^`,
		},
		{
			desc:  "invalid number",
			input: "POINT(2 2.3.7)",
			expectedErrStr: `lex error: invalid number at pos 8
POINT(2 2.3.7)
        ^`,
		},
		{
			desc:  "invalid keyword when extraneous spaces are present in ZM",
			input: "POINT Z M (1 1 1 1)",
			expectedErrStr: `lex error: invalid keyword at pos 8
POINT Z M (1 1 1 1)
        ^`,
		},
		// ParseError
		{
			desc:  "invalid point",
			input: "POINT POINT",
			expectedErrStr: `syntax error: unexpected POINT, expecting EMPTY or '(' at pos 6
POINT POINT
      ^`,
		},
		{
			desc:  "2D point with extra comma",
			input: "POINT(0, 0)",
			expectedErrStr: `syntax error: unexpected ',', expecting NUM at pos 7
POINT(0, 0)
       ^`,
		},
		{
			desc:  "2D linestring with not enough points",
			input: "LINESTRING(0 0)",
			expectedErrStr: `syntax error: non-empty linestring with only one point at pos 14
LINESTRING(0 0)
              ^`,
		},
		{
			desc:  "linestring with mixed dimensionality",
			input: "LINESTRING(0 0, 1 1 1)",
			expectedErrStr: `syntax error: unexpected NUM, expecting ')' or ',' at pos 20
LINESTRING(0 0, 1 1 1)
                    ^`,
		},
		{
			desc:  "2D polygon with not enough points",
			input: "POLYGON((0 0, 1 1, 2 0))",
			expectedErrStr: `syntax error: polygon ring doesn't have enough points at pos 22
POLYGON((0 0, 1 1, 2 0))
                      ^`,
		},
		{
			desc:  "2D polygon with ring that isn't closed",
			input: "POLYGON((0 0, 1 1, 2 0, 1 -1))",
			expectedErrStr: `syntax error: polygon ring not closed at pos 28
POLYGON((0 0, 1 1, 2 0, 1 -1))
                            ^`,
		},
		{
			desc:  "2D polygon with empty second ring",
			input: "POLYGON((0 0, 1 -1, 2 0, 0 0), ())",
			expectedErrStr: `syntax error: unexpected ')', expecting NUM at pos 32
POLYGON((0 0, 1 -1, 2 0, 0 0), ())
                                ^`,
		},
		{
			desc:  "2D polygon with EMPTY as second ring",
			input: "POLYGON((0 0, 1 -1, 2 0, 0 0), EMPTY)",
			expectedErrStr: `syntax error: unexpected EMPTY, expecting '(' at pos 31
POLYGON((0 0, 1 -1, 2 0, 0 0), EMPTY)
                               ^`,
		},
		{
			desc:  "2D polygon with invalid second ring",
			input: "POLYGON((0 0, 1 -1, 2 0, 0 0), (0.5 -0.5))",
			expectedErrStr: `syntax error: polygon ring doesn't have enough points at pos 40
POLYGON((0 0, 1 -1, 2 0, 0 0), (0.5 -0.5))
                                        ^`,
		},
		{
			desc:  "2D multipoint without any points",
			input: "MULTIPOINT()",
			expectedErrStr: `syntax error: unexpected ')', expecting EMPTY or NUM or '(' at pos 11
MULTIPOINT()
           ^`,
		},
		{
			desc:  "3D multipoint without comma separating points",
			input: "MULTIPOINT Z (0 0 0 0 0 0)",
			expectedErrStr: `syntax error: unexpected NUM, expecting ')' or ',' at pos 20
MULTIPOINT Z (0 0 0 0 0 0)
                    ^`,
		},
		{
			desc:  "2D multipoint with EMPTY inside extraneous parentheses",
			input: "MULTIPOINT((EMPTY))",
			expectedErrStr: `syntax error: unexpected EMPTY, expecting NUM at pos 12
MULTIPOINT((EMPTY))
            ^`,
		},
		{
			desc:  "3D multipoint using EMPTY as a point without using Z in type",
			input: "MULTIPOINT(0 0 0, EMPTY)",
			expectedErrStr: `syntax error: unexpected EMPTY, expecting NUM or '(' at pos 18
MULTIPOINT(0 0 0, EMPTY)
                  ^`,
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := Unmarshal(tc.input)
			require.EqualError(t, err, tc.expectedErrStr)
		})
	}
}
