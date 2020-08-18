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

			for _, subTC := range []struct {
				desc                string
				expected            float64
				useSphereOrSpheroid UseSphereOrSpheroid
			}{
				{"sphere", tc.expectedSphereDistance, UseSphere},
				{"spheroid", tc.expectedSpheroidDistance, UseSpheroid},
			} {
				t.Run(subTC.desc, func(t *testing.T) {
					if subTC.expected == 0 {
						zeroValue := float64(0)
						// Allow a 1cm margin of error for close to zero cases.
						if _, ok := closeToZeroCases[tc.desc]; ok {
							zeroValue = 0.01
						}
						for _, val := range []float64{zeroValue, 1, 10, 10000} {
							t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
								dwithin, err := DWithin(a, b, val, subTC.useSphereOrSpheroid, geo.FnInclusive)
								require.NoError(t, err)
								require.True(t, dwithin)

								dwithin, err = DWithin(b, a, val, subTC.useSphereOrSpheroid, geo.FnInclusive)
								require.NoError(t, err)
								require.True(t, dwithin)
							})
							t.Run(fmt.Sprintf("dwithinexclusive:%f", val), func(t *testing.T) {
								exclusiveExpected := true
								if val == subTC.expected {
									exclusiveExpected = false
								}
								dwithin, err := DWithin(a, b, val, subTC.useSphereOrSpheroid, geo.FnExclusive)
								require.NoError(t, err)
								require.Equal(t, exclusiveExpected, dwithin)

								dwithin, err = DWithin(b, a, val, subTC.useSphereOrSpheroid, geo.FnExclusive)
								require.NoError(t, err)
								require.Equal(t, exclusiveExpected, dwithin)
							})
						}
					} else {
						for _, val := range []float64{
							subTC.expected + 0.01, // allow 1cm margin of error
							subTC.expected + 0.02,
							subTC.expected + 1,
							subTC.expected * 2,
						} {
							t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
								dwithin, err := DWithin(a, b, val, subTC.useSphereOrSpheroid, geo.FnInclusive)
								require.NoError(t, err)
								require.True(t, dwithin)

								dwithin, err = DWithin(b, a, val, subTC.useSphereOrSpheroid, geo.FnInclusive)
								require.NoError(t, err)
								require.True(t, dwithin)
							})
							t.Run(fmt.Sprintf("dwithinexclusive:%f", val), func(t *testing.T) {
								dwithin, err := DWithin(a, b, val, subTC.useSphereOrSpheroid, geo.FnExclusive)
								require.NoError(t, err)
								require.True(t, dwithin)

								dwithin, err = DWithin(b, a, val, subTC.useSphereOrSpheroid, geo.FnExclusive)
								require.NoError(t, err)
								require.True(t, dwithin)
							})
						}

						for _, val := range []float64{
							subTC.expected - 0.01, // allow 1cm margin of error
							subTC.expected - 0.02,
							subTC.expected - 1,
							subTC.expected / 2,
						} {
							t.Run(fmt.Sprintf("dwithin:%f", val), func(t *testing.T) {
								dwithin, err := DWithin(a, b, val, subTC.useSphereOrSpheroid, geo.FnInclusive)
								require.NoError(t, err)
								require.False(t, dwithin)

								dwithin, err = DWithin(b, a, val, subTC.useSphereOrSpheroid, geo.FnInclusive)
								require.NoError(t, err)
								require.False(t, dwithin)
							})
							t.Run(fmt.Sprintf("dwithinexclusive:%f", val), func(t *testing.T) {
								dwithin, err := DWithin(a, b, val, subTC.useSphereOrSpheroid, geo.FnExclusive)
								require.NoError(t, err)
								require.False(t, dwithin)

								dwithin, err = DWithin(b, a, val, subTC.useSphereOrSpheroid, geo.FnExclusive)
								require.NoError(t, err)
								require.False(t, dwithin)
							})
						}
					}
				})
			}
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := DWithin(mismatchingSRIDGeographyA, mismatchingSRIDGeographyB, 0, UseSpheroid, geo.FnInclusive)
		requireMismatchingSRIDError(t, err)
	})

	t.Run("errors if distance < 0", func(t *testing.T) {
		_, err := DWithin(geo.MustParseGeography("POINT(1.0 2.0)"), geo.MustParseGeography("POINT(3.0 4.0)"), -0.01, UseSpheroid, geo.FnInclusive)
		require.Error(t, err)
	})

	t.Run("empty geographies are never dwithin each other", func(t *testing.T) {
		for _, tc := range []struct {
			a string
			b string
		}{
			{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY"},
			{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION (POINT(1.0 1.0), LINESTRING EMPTY)"},
			{"POINT(1.0 1.0)", "GEOMETRYCOLLECTION (POINT(1.0 1.0), LINESTRING EMPTY)"}, // This case errors (in a bad way) in PostGIS.
		} {
			for _, useSphereOrSpheroid := range []UseSphereOrSpheroid{
				UseSphere,
				UseSpheroid,
			} {
				t.Run(fmt.Sprintf("DWithin(%s,%s),spheroid=%t", tc.a, tc.b, useSphereOrSpheroid), func(t *testing.T) {
					a, err := geo.ParseGeography(tc.a)
					require.NoError(t, err)
					b, err := geo.ParseGeography(tc.b)
					require.NoError(t, err)
					dwithin, err := DWithin(a, b, 0, useSphereOrSpheroid, geo.FnInclusive)
					require.NoError(t, err)
					require.False(t, dwithin)
				})
			}
		}
	})
}
