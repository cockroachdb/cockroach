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
	"github.com/stretchr/testify/require"
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
			s12, az1, az2 := tc.spheroid.Inverse(tc.a, tc.b)
			require.Equal(t, tc.s12, s12)
			require.Equal(t, tc.az1, az1)
			require.Equal(t, tc.az2, az2)
		})
	}
}

func TestInverseBatch(t *testing.T) {
	testCases := []struct {
		desc     string
		spheroid Spheroid
		points   []s2.Point
		sum      float64
	}{
		{
			desc:     "WKT from Wikipedia",
			spheroid: WGS84Spheroid,
			points: []s2.Point{
				s2.PointFromLatLng(s2.LatLngFromDegrees(40, 40)),
				s2.PointFromLatLng(s2.LatLngFromDegrees(45, 20)),
				s2.PointFromLatLng(s2.LatLngFromDegrees(30, 45)),
			},
			sum: 4.477956179118822e+06,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			sum := tc.spheroid.InverseBatch(tc.points)
			require.Equal(t, tc.sum, sum)
		})
	}
}

func TestAreaAndPerimeter(t *testing.T) {
	testCases := []struct {
		desc      string
		spheroid  Spheroid
		points    []s2.Point
		area      float64
		perimeter float64
	}{
		{
			desc:     "WKT from Wikipedia",
			spheroid: WGS84Spheroid,
			points: []s2.Point{
				s2.PointFromLatLng(s2.LatLngFromDegrees(40, 40)),
				s2.PointFromLatLng(s2.LatLngFromDegrees(45, 20)),
				s2.PointFromLatLng(s2.LatLngFromDegrees(30, 45)),
			},
			area:      6.91638769184179e+11,
			perimeter: 5.6770339842410665e+06,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			area, perimeter := tc.spheroid.AreaAndPerimeter(tc.points)
			require.Equal(t, tc.area, area)
			require.Equal(t, tc.perimeter, perimeter)
		})
	}
}
