// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geogfn

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSpheroid(t *testing.T) {
	const grs1980Rf = 298.257222101

	tests := []struct {
		name                      string
		input                     string
		expectedRadius            float64
		expectedInverseFlattening float64
		expectedErrRegex          string
	}{
		{
			name:                      "GRS_1980 with brackets",
			input:                     `SPHEROID["GRS_1980",6378137,298.257222101]`,
			expectedRadius:            6378137,
			expectedInverseFlattening: grs1980Rf,
		},
		{
			name:                      "WGS_84 with parens",
			input:                     `SPHEROID("WGS 84",6378137,298.257223563)`,
			expectedRadius:            6378137,
			expectedInverseFlattening: 298.257223563,
		},
		{
			name:                      "leading and trailing whitespace ignored",
			input:                     "  \tSPHEROID[\"x\",6378137,298.257222101]\n",
			expectedRadius:            6378137,
			expectedInverseFlattening: grs1980Rf,
		},
		{
			name:                      "scientific notation",
			input:                     `SPHEROID["x",6.378137e6,2.98257222101e2]`,
			expectedRadius:            6378137,
			expectedInverseFlattening: grs1980Rf,
		},
		{
			name:                      "internal whitespace around numbers tolerated",
			input:                     `SPHEROID["x", 6378137 , 298.257222101]`,
			expectedRadius:            6378137,
			expectedInverseFlattening: grs1980Rf,
		},
		{
			name:             "missing prefix",
			input:            `["GRS_1980",6378137,298.257222101]`,
			expectedErrRegex: "doesn't start with SPHEROID",
		},
		{
			name:             "whitespace between SPHEROID and opening delimiter",
			input:            `SPHEROID ["GRS_1980",6378137,298.257222101]`,
			expectedErrRegex: "couldn't parse the spheroid",
		},
		{
			name:             "mismatched delimiters",
			input:            `SPHEROID["GRS_1980",6378137,298.257222101)`,
			expectedErrRegex: "couldn't parse the spheroid",
		},
		{
			name:             "missing inverse flattening",
			input:            `SPHEROID["GRS_1980",6378137]`,
			expectedErrRegex: "couldn't parse the spheroid",
		},
		{
			name:             "name too long",
			input:            `SPHEROID["this_name_is_way_too_long_to_fit",6378137,298.257222101]`,
			expectedErrRegex: "couldn't parse the spheroid",
		},
		{
			name:             "embedded quote in name",
			input:            `SPHEROID["a"b",6378137,298.257222101]`,
			expectedErrRegex: "couldn't parse the spheroid",
		},
		{
			name:             "non-positive semi-major axis",
			input:            `SPHEROID["x",-1,298.257222101]`,
			expectedErrRegex: "semi-major axis must be a positive finite number",
		},
		{
			name:             "NaN semi-major axis",
			input:            `SPHEROID["x",NaN,298.257222101]`,
			expectedErrRegex: "semi-major axis must be a positive finite number",
		},
		{
			name:             "Inf semi-major axis",
			input:            `SPHEROID["x",+Inf,298.257222101]`,
			expectedErrRegex: "semi-major axis must be a positive finite number",
		},
		{
			name:             "zero inverse flattening",
			input:            `SPHEROID["x",6378137,0]`,
			expectedErrRegex: "inverse flattening must be a finite number >= 1",
		},
		{
			name:             "negative inverse flattening",
			input:            `SPHEROID["x",6378137,-298.257222101]`,
			expectedErrRegex: "inverse flattening must be a finite number >= 1",
		},
		{
			name:             "inverse flattening less than 1 yields negative minor axis",
			input:            `SPHEROID["x",6378137,0.5]`,
			expectedErrRegex: "inverse flattening must be a finite number >= 1",
		},
		{
			name:             "NaN inverse flattening",
			input:            `SPHEROID["x",6378137,NaN]`,
			expectedErrRegex: "inverse flattening must be a finite number >= 1",
		},
		{
			name:             "Inf inverse flattening",
			input:            `SPHEROID["x",6378137,Inf]`,
			expectedErrRegex: "inverse flattening must be a finite number >= 1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := ParseSpheroid(tc.input)
			if tc.expectedErrRegex != "" {
				require.Error(t, err)
				require.Regexp(t, tc.expectedErrRegex, err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedRadius, s.Radius())
			require.InEpsilon(t, 1.0/tc.expectedInverseFlattening, s.Flattening(), 1e-12)
			require.False(t, math.IsNaN(s.SphereRadius()))
		})
	}
}
