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
	"math"
	"testing"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
)

const epsilon = 1e-8

func TestInverse(t *testing.T) {
	testCases := []struct {
		desc     string
		spheroid *Spheroid
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
			require.InEpsilon(t, tc.s12, s12, epsilon)
			require.InEpsilon(t, tc.az1, az1, epsilon)
			require.InEpsilon(t, tc.az2, az2, epsilon)
		})
	}
}

func TestInverseBatch(t *testing.T) {
	testCases := []struct {
		desc     string
		spheroid *Spheroid
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
			require.InEpsilon(t, tc.sum, sum, epsilon)
		})
	}
}

func TestAreaAndPerimeter(t *testing.T) {
	testCases := []struct {
		desc      string
		spheroid  *Spheroid
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
			require.InEpsilon(t, tc.area, area, epsilon)
			require.InEpsilon(t, tc.perimeter, perimeter, epsilon)
		})
	}
}

func TestProject(t *testing.T) {
	testCases := []struct {
		desc     string
		spheroid *Spheroid
		point    s2.LatLng
		distance float64
		azimuth  float64
		project  s2.LatLng
	}{
		{
			desc:     "{0,0} project to 100000, radians(45.0) on WGS84Spheroid",
			spheroid: WGS84Spheroid,
			point:    s2.LatLng{Lat: 0, Lng: 0},
			distance: 100000,
			azimuth:  45 * math.Pi / 180.0,
			project:  s2.LatLng{Lat: 0.011160897716439782, Lng: 0.011086872969072624},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			project := tc.spheroid.Project(tc.point, tc.distance, s1.Angle(tc.azimuth))
			require.Equal(t, tc.project, project)
		})
	}
}
