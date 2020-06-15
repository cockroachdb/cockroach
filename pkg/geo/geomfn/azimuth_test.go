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

func TestAzimuth(t *testing.T) {
	zero := 0.0
	aQuarterPi := 0.7853981633974483
	towQuartersPi := 1.5707963267948966
	threeQuartersPi := 2.356194490192344

	testCases := []struct {
		a        string
		b        string
		expected *float64
	}{
		{
			"POINT(0 0)",
			"POINT(0 0)",
			nil,
		},
		{
			"POINT(0 0)",
			"POINT(1 1)",
			&aQuarterPi,
		},
		{
			"POINT(0 0)",
			"POINT(1 0)",
			&towQuartersPi,
		},
		{
			"POINT(0 0)",
			"POINT(1 -1)",
			&threeQuartersPi,
		},
		{
			"POINT(0 0)",
			"POINT(0 1)",
			&zero,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			r, err := Azimuth(a, b)
			require.NoError(t, err)

			if tc.expected == nil {
				require.Nil(t, r)
			} else {
				require.Equal(t, *tc.expected, *r)
			}
		})
	}
}
