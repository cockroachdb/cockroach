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

	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

var (
	zero            = 0.0
	aQuarterPi      = 0.7853981633974483
	towQuartersPi   = 1.5707963267948966
	threeQuartersPi = 2.356194490192344
)

func TestAzimuth(t *testing.T) {
	testCases := []struct {
		a        *geom.Point
		b        *geom.Point
		expected *float64
	}{
		{
			geom.NewPointFlat(geom.XY, []float64{0, 0}),
			geom.NewPointFlat(geom.XY, []float64{0, 0}),
			nil,
		},
		{
			geom.NewPointFlat(geom.XY, []float64{0, 0}),
			geom.NewPointFlat(geom.XY, []float64{1, 1}),
			&aQuarterPi,
		},
		{
			geom.NewPointFlat(geom.XY, []float64{0, 0}),
			geom.NewPointFlat(geom.XY, []float64{1, 0}),
			&towQuartersPi,
		},
		{
			geom.NewPointFlat(geom.XY, []float64{0, 0}),
			geom.NewPointFlat(geom.XY, []float64{1, -1}),
			&threeQuartersPi,
		},
		{
			geom.NewPointFlat(geom.XY, []float64{0, 0}),
			geom.NewPointFlat(geom.XY, []float64{0, 1}),
			&zero,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			r := Azimuth(tc.a, tc.b)
			if tc.expected == nil {
				require.Nil(t, r)
			} else {
				require.Equal(t, *tc.expected, *r)
			}
		})
	}
}
