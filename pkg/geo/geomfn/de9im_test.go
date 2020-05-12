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

func TestRelate(t *testing.T) {
	testCases := []struct {
		a        *geo.Geometry
		b        *geo.Geometry
		expected string
	}{
		{leftRect, rightRect, "FF2F11212"},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			ret, err := Relate(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestMatchesDE9IM(t *testing.T) {
	testCases := []struct {
		str           string
		pattern       string
		expected      bool
		expectedError string
	}{
		{"", "T**FF*FF*", false, `relation "" should be of length 9`},
		{"TTTTTTTTT", "T**FF*FF*T", false, `pattern "T**FF*FF*T" should be of length 9`},
		{"TTTTTTTTT", "T**FF*FF*T", false, `pattern "T**FF*FF*T" should be of length 9`},
		{"000FFF000", "cTTFfFTTT", false, `unrecognized pattern character: c`},
		{"120FFF021", "TTTFfFTTT", true, ""},
		{"02FFFF000", "T**FfFTTT", true, ""},
		{"020F1F010", "TTTFFFTtT", false, ""},
		{"020FFF0f0", "TTTFFFTtT", false, ""},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s has pattern %s", tc.str, tc.pattern), func(t *testing.T) {
			ret, err := MatchesDE9IM(tc.str, tc.pattern)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ret)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Relate(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}
