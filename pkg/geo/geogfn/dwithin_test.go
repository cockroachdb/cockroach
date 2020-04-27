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

func TestDWithin(t *testing.T) {
	// These are cases where the distance is VERY CLOSE to zero.
	closeToZeroCases := map[string]struct{}{
		"LINESTRING to POINT on the line": {},
	}

	for _, tc := range distanceTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeography(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeography(tc.b)
			require.NoError(t, err)

			t.Run("sphere", func(t *testing.T) {
				if tc.expectedSphereDistance == 0 {
					zeroValue := float64(0)
					// Allow a 1cm margin of error for close to zero cases.
					if _, ok := closeToZeroCases[tc.desc]; ok {
						zeroValue = 0.01
					}
					for _, val := range []float64{zeroValue, 1, 10, 10000} {
						t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
							dwithin, err := DWithin(a, b, val)
							require.NoError(t, err)
							require.True(t, dwithin)

							dwithin, err = DWithin(b, a, val)
							require.NoError(t, err)
							require.True(t, dwithin)
						})
					}
				} else {
					for _, val := range []float64{
						tc.expectedSphereDistance + 0.01, // allow 1cm margin of error
						tc.expectedSphereDistance + 0.02,
						tc.expectedSphereDistance + 1,
						tc.expectedSphereDistance * 2,
					} {
						t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
							dwithin, err := DWithin(a, b, val)
							require.NoError(t, err)
							require.True(t, dwithin)

							dwithin, err = DWithin(b, a, val)
							require.NoError(t, err)
							require.True(t, dwithin)
						})
					}

					for _, val := range []float64{
						tc.expectedSphereDistance - 0.01, // allow 1cm margin of error
						tc.expectedSphereDistance - 0.02,
						tc.expectedSphereDistance - 1,
						tc.expectedSphereDistance / 2,
					} {
						t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
							dwithin, err := DWithin(a, b, val)
							require.NoError(t, err)
							require.False(t, dwithin)

							dwithin, err = DWithin(b, a, val)
							require.NoError(t, err)
							require.False(t, dwithin)
						})
					}
				}
			})
		})
	}
}
