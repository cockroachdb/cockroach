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
		{
			desc:        "parse 2D point",
			equivInputs: []string{"POINT(0 1)", "POINT (0 1)", "point(0 1)", "point ( 0 1 )"},
			expected:    geom.NewPointFlat(geom.XY, []float64{0, 1}),
		},
		{
			desc:        "parse 3D point",
			equivInputs: []string{"POINT Z (2 3 4)", "POINTZ(2 3 4)", "POINT(2 3 4)"},
			expected:    geom.NewPointFlat(geom.XYZ, []float64{2, 3, 4}),
		},
		{
			desc:        "parse 2D+M point",
			equivInputs: []string{"POINT M (-2 0 0.5)", "POINTM(-2 0 0.5)", "POINTM(-2 0 .5)"},
			expected:    geom.NewPointFlat(geom.XYM, []float64{-2, 0, 0.5}),
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
			desc:        "parse empty 3D point",
			equivInputs: []string{"POINT Z EMPTY", "POINTZ EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYZ),
		},
		{
			desc:        "parse empty 2D+M point",
			equivInputs: []string{"POINT M EMPTY", "POINTM EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYM),
		},
		{
			desc:        "parse empty 4D point",
			equivInputs: []string{"POINT ZM EMPTY", "POINTZM EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYZM),
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
		{
			desc:           "unrecognized character",
			input:          "POINT{0 0}",
			expectedErrStr: "lex error: unrecognized character at pos 5",
		},
		{
			desc:           "invalid keyword",
			input:          "DOT(0 0)",
			expectedErrStr: "lex error: invalid keyword at pos 0",
		},
		{
			desc:           "invalid number",
			input:          "POINT(2 2.3.7)",
			expectedErrStr: "lex error: invalid number at pos 8",
		},
		{
			desc:           "2D point with extra comma",
			input:          "POINT(0, 0)",
			expectedErrStr: `parse error: could not parse "POINT(0, 0)"`,
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := Unmarshal(tc.input)
			require.EqualError(t, err, tc.expectedErrStr)
		})
	}
}
