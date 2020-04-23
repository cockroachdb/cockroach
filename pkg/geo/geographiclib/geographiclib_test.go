// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geographiclib

import (
	"testing"

	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/assert"
)

func TestInverse(t *testing.T) {
	testCases := []struct {
		desc     string
		spheroid Spheroid
		a, b     s2.LatLng

		s12, az1, az2 float64
	}{
		{
			desc:     "{0,0}, {1,1} on WGS84Spheroid",
			spheroid: WGS84Spheroid,
			a:        s2.LatLngFromDegrees(0, 0),
			b:        s2.LatLngFromDegrees(1, 1),
			s12:      156899.56829134029,
			az1:      45.188040229358869,
			az2:      45.196767321644863,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			s12, az1, az2 := WGS84Spheroid.Inverse(tc.a, tc.b)
			assert.Equal(t, tc.s12, s12)
			assert.Equal(t, tc.az1, az1)
			assert.Equal(t, tc.az2, az2)
		})
	}
}
