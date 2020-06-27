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
	"github.com/stretchr/testify/require"
)

func TestAzimuth(t *testing.T) {
	testCases := []struct {
		a        string
		b        string
		expected float64
	}{
		{
			"POINT(0 0)",
			"POINT(1 1)",
			0.7886800845259658,
		},
		{
			"POINT(0 0)",
			"POINT(1 0)",
			1.5707963267948966,
		},
		{
			"POINT(0 0)",
			"POINT(1 -1)",
			2.352912569063827,
		},
		{
			"SRID=4004;POINT(0 0)",
			"SRID=4004;POINT(1 -1)",
			2.3529226390237774,
		},
		{
			"POINT(0 0)",
			"POINT(0 1)",
			0,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s <=> %s", tc.a, tc.b), func(t *testing.T) {
			a, err := geo.ParseGeography(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeography(tc.b)
			require.NoError(t, err)

			r, err := Azimuth(a, b)
			require.NoError(t, err)
			require.NotNil(t, r)
			require.Equal(t, tc.expected, *r)
		})
	}

	t.Run("same point", func(t *testing.T) {
		a, err := geo.ParseGeography("POINT(1.0 1.0)")
		require.NoError(t, err)
		ret, err := Azimuth(a, a)
		require.NoError(t, err)
		require.Nil(t, ret)
	})

	errorTestCases := []struct {
		a          string
		b          string
		errorMatch string
	}{
		{
			"LINESTRING(0 0, 1 0)",
			"POINT(0 0)",
			"arguments must be POINT geometries",
		},
	}
	for _, tc := range errorTestCases {
		t.Run(fmt.Sprintf("%s <=> %s", tc.a, tc.b), func(t *testing.T) {
			a, err := geo.ParseGeography(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeography(tc.b)
			require.NoError(t, err)

			_, err = Azimuth(a, b)
			require.Error(t, err)
			require.EqualError(t, err, tc.errorMatch)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Azimuth(mismatchingSRIDGeographyA, mismatchingSRIDGeographyB)
		requireMismatchingSRIDError(t, err)
	})
}
