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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
)

func TestBestGeomProjection(t *testing.T) {
	testCases := []struct {
		desc     string
		rect     s2.Rect
		expected geoprojbase.Proj4Text
	}{
		{
			"north pole",
			s2.RectFromLatLng(s2.LatLngFromDegrees(75, 75)),
			geoprojbase.MustProjection(3574).Proj4Text,
		},
		{
			"south pole",
			s2.RectFromLatLng(s2.LatLngFromDegrees(-75, -75)),
			geoprojbase.MustProjection(3409).Proj4Text},
		{
			"utm 15 on top hemisphere",
			s2.RectFromLatLng(s2.LatLngFromDegrees(15, 93)),
			geoprojbase.MustProjection(32646).Proj4Text},
		{
			"utm -16 on bottom hemisphere",
			s2.RectFromLatLng(s2.LatLngFromDegrees(-15, -111)),
			geoprojbase.MustProjection(32712).Proj4Text,
		},
		{
			"LAEA at equator bands (north half)",
			s2.RectFromCenterSize(s2.LatLngFromDegrees(12, 22), s2.LatLngFromDegrees(10, 10)),
			geoprojbase.MakeProj4Text("+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=15 +lon_0=15 +units=m +no_defs"),
		},
		{
			"LAEA at equator bands (south half)",
			s2.RectFromCenterSize(s2.LatLngFromDegrees(-13, 123), s2.LatLngFromDegrees(10, 10)),
			geoprojbase.MakeProj4Text("+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=-15 +lon_0=135 +units=m +no_defs"),
		},
		{
			"LAEA at temperate band (north half)",
			s2.RectFromCenterSize(s2.LatLngFromDegrees(33, 87), s2.LatLngFromDegrees(10, 10)),
			geoprojbase.MakeProj4Text("+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=45 +lon_0=67.5 +units=m +no_defs"),
		},
		{
			"LAEA at temperate band (south half)",
			s2.RectFromCenterSize(s2.LatLngFromDegrees(-53, -120.5), s2.LatLngFromDegrees(10, 10)),
			geoprojbase.MakeProj4Text("+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=-45 +lon_0=-112.5 +units=m +no_defs"),
		},
		{
			"LAEA at polar band (north half)",
			s2.RectFromCenterSize(s2.LatLngFromDegrees(63, 87), s2.LatLngFromDegrees(10, 10)),
			geoprojbase.MakeProj4Text("+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=75 +lon_0=45 +units=m +no_defs"),
		},
		{
			"LAEA at polar band (south half)",
			s2.RectFromCenterSize(s2.LatLngFromDegrees(-66, -120.5), s2.LatLngFromDegrees(10, 10)),
			geoprojbase.MakeProj4Text("+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=-75 +lon_0=-135 +units=m +no_defs"),
		},
		{
			"UTM which should be 32V, but we return 31V as we do not handle UTM exceptions",
			s2.RectFromLatLng(s2.LatLngFromDegrees(59.4136, 5.26)),
			geoprojbase.MustProjection(32631).Proj4Text, // Should be 32632
		},
		{
			"wide example",
			s2.RectFromCenterSize(s2.LatLngFromDegrees(0, 0), s2.LatLngFromDegrees(50, 50)),
			geoprojbase.MustProjection(3857).Proj4Text,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := BestGeomProjection(tc.rect)
			require.NoError(t, err)
			require.Equal(t, tc.expected.String(), result.String())
		})
	}
}
