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

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
)

func TestGeoHash(t *testing.T) {
	testCases := []struct {
		desc     string
		a        string
		p        int
		expected string
	}{
		{
			desc:     "Point at 0,0",
			a:        "POINT(0.0 0.0)",
			p:        16,
			expected: "s000000000000000",
		},
		{
			"Point at 90, 0",
			"POINT(90.0 0.0)",
			16,
			"w000000000000000",
		},
		{
			"Point at a random location",
			"SRID=4004;POINT(20.012345 -20.012345)",
			15,
			"kkqnpkue9ktbpe5",
		},
		{
			"Geohash from a Multipolygon ",
			"POLYGON((-71.1776585052917 42.3902909739571,-71.1776820268866 42.3903701743239,                                       -71.1776063012595 42.3903825660754,-71.1775826583081 42.3903033653531,-71.1776585052917 42.3902909739571))",
			20,
			"drt3hkfj8gw86nz6tbx7",
		},
		{
			"Point at a random location",
			"SRID=4004;POINT(20.0123451111111111 -20.012345111111111)",
			20,
			"kkqnpkue9kqp6mbe5c6b",
		},
		{
			"Polygon to check automatic precision",
			"POLYGON((-8.359375000000018 34.36143956369891,-3.4375000000000178 34.36143956369891,-3.4375000000000178 30.8077684261472,-8.359375000000018 30.8077684261472,-8.359375000000018 34.36143956369891))",
			-1,
			"e",
		},
		{
			"Polygon to check manual precision",
			"POLYGON((-8.359375000000018 34.36143956369891,-3.4375000000000178 34.36143956369891,-3.4375000000000178 30.8077684261472,-8.359375000000018 30.8077684261472,-8.359375000000018 34.36143956369891))",
			10,
			"evgc3gbgh6",
		},
		{
			"Polygon to check manual precision",
			"POLYGON((-99.18139024416594 19.420811187791617,-99.17177720705656 19.433762205907612,-99.16903062502531 19.424372820694032,-99.17589708010344 19.415306692500074,-99.19134660402922 19.409802010814566,-99.17795701662688 19.40526860361888,-99.21709581057219 19.40624005865604,-99.18139024416594 19.420811187791617))",
			-1,
			"9g3q",
		},
		{
			"Polygon to check full precision",
			"POLYGON((-99.18139024416594 19.420811187791617,-99.17177720705656 19.433762205907612,-99.16903062502531 19.424372820694032,-99.17589708010344 19.415306692500074,-99.19134660402922 19.409802010814566,-99.17795701662688 19.40526860361888,-99.21709581057219 19.40624005865604,-99.18139024416594 19.420811187791617))",
			20,
			"9g3qqz1yfh51x7uke7fz",
		},
		{
			"Geohash from a linestring",
			"LINESTRING(-99.22962622216731 19.468542204204024,-99.2289395766595 19.46902774319579)",
			-1,
			"9g3qvbp",
		},
		{
			"Geohash from a linestring full precision",
			"LINESTRING(-99.22962622216731 19.468542204204024,-99.2289395766595 19.46902774319579)",
			20,
			"9g3qvbpmyhefh1ecdhpw",
		},
		{
			"empty string",
			"SRID=4326;GEOMETRYCOLLECTION EMPTY",
			20,
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeography(tc.a)
			require.NoError(t, err)
			geohash, err := GeoHash(a, tc.p)
			require.NoError(t, err)
			require.Equal(t, tc.expected, geohash)
		})
	}

	t.Run("geohashes with errors", func(t *testing.T) {
		for _, tc := range []struct {
			desc string
			a    string
			p    int
		}{
			{
				"Point at 90, 181",
				"POINT(90.0 181.0)",
				16,
			},
			{
				"Point at 90, 181",
				"POINT(-990 181)",
				16,
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				a, err := geo.ParseGeography(tc.a)
				require.NoError(t, err)
				_, err = GeoHash(a, tc.p)
				require.Error(t, err)
			})
		}
	})
}
