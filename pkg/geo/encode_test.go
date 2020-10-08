// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/stretchr/testify/require"
)

func TestSpatialObjectToWKT(t *testing.T) {
	testCases := []struct {
		ewkt             geopb.EWKT
		maxDecimalDigits int
		expected         geopb.WKT
	}{
		{"POINT(1.01 1.01)", 15, "POINT (1.01 1.01)"},
		{"POINT(1.01 1.01)", 1, "POINT (1 1)"},
		{"SRID=4004;POINT(1.0 1.0)", 15, "POINT (1 1)"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := SpatialObjectToWKT(so, tc.maxDecimalDigits)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestSpatialObjectToEWKT(t *testing.T) {
	testCases := []struct {
		ewkt             geopb.EWKT
		maxDecimalDigits int
		expected         geopb.EWKT
	}{
		{"POINT(1.01 1.01)", 15, "POINT (1.01 1.01)"},
		{"POINT(1.01 1.01)", 1, "POINT (1 1)"},
		{"GEOMETRYCOLLECTION (POINT EMPTY, POLYGON EMPTY)", -1, "GEOMETRYCOLLECTION (POINT EMPTY, POLYGON EMPTY)"},
		{"SRID=4004;POINT(1.0 1.0)", 15, "SRID=4004;POINT (1 1)"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := SpatialObjectToEWKT(so, tc.maxDecimalDigits)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestSpatialObjectToWKB(t *testing.T) {
	testCases := []struct {
		ewkt     geopb.EWKT
		expected geopb.WKB
	}{
		{"POINT(1.0 1.0)", []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f")},
		{"SRID=4004;POINT(1.0 1.0)", []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f")},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := SpatialObjectToWKB(so, DefaultEWKBEncodingFormat)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestSpatialObjectToGeoJSON(t *testing.T) {
	testCases := []struct {
		ewkt     geopb.EWKT
		flag     SpatialObjectToGeoJSONFlag
		expected string
	}{
		{"POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagZero, `{"type":"Point","coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagIncludeBBox, `{"type":"Point","bbox":[1,1,1,1],"coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagShortCRS | SpatialObjectToGeoJSONFlagIncludeBBox, `{"type":"Point","bbox":[1,1,1,1],"coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagShortCRS, `{"type":"Point","coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagLongCRS, `{"type":"Point","coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagShortCRSIfNot4326, `{"type":"Point","coordinates":[1,1]}`},
		{"POINT(1.1234567 1.9876543)", SpatialObjectToGeoJSONFlagShortCRSIfNot4326, `{"type":"Point","coordinates":[1.123457,1.987654]}`},
		{"SRID=4326;POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagZero, `{"type":"Point","coordinates":[1,1]}`},
		{"SRID=4326;POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagIncludeBBox, `{"type":"Point","bbox":[1,1,1,1],"coordinates":[1,1]}`},
		{"SRID=4326;POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagLongCRS, `{"type":"Point","crs":{"type":"name","properties":{"name":"urn:ogc:def:crs:EPSG::4326"}},"coordinates":[1,1]}`},
		{"SRID=4326;POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagShortCRS, `{"type":"Point","crs":{"type":"name","properties":{"name":"EPSG:4326"}},"coordinates":[1,1]}`},
		{"SRID=4004;POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagShortCRS, `{"type":"Point","crs":{"type":"name","properties":{"name":"EPSG:4004"}},"coordinates":[1,1]}`},
		{"SRID=4004;POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagShortCRS | SpatialObjectToGeoJSONFlagIncludeBBox, `{"type":"Point","bbox":[1,1,1,1],"crs":{"type":"name","properties":{"name":"EPSG:4004"}},"coordinates":[1,1]}`},
		{"SRID=4326;POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagShortCRSIfNot4326, `{"type":"Point","coordinates":[1,1]}`},
		{"SRID=4004;POINT(1.0 1.0)", SpatialObjectToGeoJSONFlagShortCRSIfNot4326, `{"type":"Point","crs":{"type":"name","properties":{"name":"EPSG:4004"}},"coordinates":[1,1]}`},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := SpatialObjectToGeoJSON(so, 6, tc.flag)
			require.NoError(t, err)
			require.Equal(t, tc.expected, string(encoded))
		})
	}
}

func TestSpatialObjectToWKBHex(t *testing.T) {
	testCases := []struct {
		ewkt     geopb.EWKT
		expected string
	}{
		{"POINT(1.0 1.0)", "0101000000000000000000F03F000000000000F03F"},
		{"SRID=4004;POINT(1.0 1.0)", "0101000000000000000000F03F000000000000F03F"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := SpatialObjectToWKBHex(so)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestSpatialObjectToKML(t *testing.T) {
	testCases := []struct {
		ewkt     geopb.EWKT
		expected string
	}{
		{"POINT(1.0 1.0)", `<?xml version="1.0" encoding="UTF-8"?>
<Point><coordinates>1,1</coordinates></Point>`},
		{"SRID=4004;POINT(1.0 1.0)", `<?xml version="1.0" encoding="UTF-8"?>
<Point><coordinates>1,1</coordinates></Point>`},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := SpatialObjectToKML(so)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestSpatialObjectToGeoHash(t *testing.T) {
	testCases := []struct {
		desc     string
		a        string
		p        int
		expected string
	}{
		{
			desc:     "POINT EMPTY",
			a:        "POINT EMPTY",
			p:        0,
			expected: "",
		},
		{
			desc:     "POLYGON EMPTY",
			a:        "POLYGON EMPTY",
			p:        0,
			expected: "",
		},
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
			"GeoHash from a MultiPolygon",
			"POLYGON((-71.1776585052917 42.3902909739571,-71.1776820268866 42.3903701743239, -71.1776063012595 42.3903825660754,-71.1775826583081 42.3903033653531,-71.1776585052917 42.3902909739571))",
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
			5,
			"evgc3",
		},
		{
			"Polygon to check automatic precision",
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
			"GeoHash from a LineString",
			"LineString(-99.22962622216731 19.468542204204024,-99.2289395766595 19.46902774319579)",
			-1,
			"9g3qvbp",
		},
		{
			"GeoHash from a LineString full precision",
			"LineString(-99.22962622216731 19.468542204204024,-99.2289395766595 19.46902774319579)",
			20,
			"9g3qvbpmyhefh1ecdhpw",
		},
		{
			"GeoHash of LineString crossing DateLine",
			"LINESTRING(-179 0, 179 0)",
			GeoHashAutoPrecision,
			"",
		},
		{
			"GeoHash of LineString crossing DateLine",
			"LINESTRING(-179 0, 179 0)",
			5,
			"s0000",
		},
		{
			"GEOMTRYCOLLECTION EMPTY",
			"SRID=4326;GEOMETRYCOLLECTION EMPTY",
			20,
			"",
		},
	}

	t.Run("geometry", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				a, err := ParseGeometry(tc.a)
				require.NoError(t, err)
				geohash, err := SpatialObjectToGeoHash(a.SpatialObject(), tc.p)
				require.NoError(t, err)
				require.Equal(t, tc.expected, geohash)
			})
		}
	})
	t.Run("geography", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				a, err := ParseGeography(tc.a)
				require.NoError(t, err)
				geohash, err := SpatialObjectToGeoHash(a.SpatialObject(), tc.p)
				require.NoError(t, err)
				require.Equal(t, tc.expected, geohash)
			})
		}
	})

	t.Run("crossing the date line", func(t *testing.T) {
		a, err := ParseGeography("LINESTRING(179 0, -179 0)")
		require.NoError(t, err)
		geohash, err := SpatialObjectToGeoHash(a.SpatialObject(), GeoHashAutoPrecision)
		require.NoError(t, err)
		require.Equal(t, "", geohash)
	})

	t.Run("geohashes errors with invalid bounds", func(t *testing.T) {
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
				a, err := ParseGeometry(tc.a)
				require.NoError(t, err)
				_, err = SpatialObjectToGeoHash(a.SpatialObject(), tc.p)
				require.Error(t, err)
			})
		}
	})
}
