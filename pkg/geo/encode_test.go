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

func TestEWKBToWKT(t *testing.T) {
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
			encoded, err := EWKBToWKT(so.EWKB, tc.maxDecimalDigits)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestEWKBToEWKT(t *testing.T) {
	testCases := []struct {
		ewkt             geopb.EWKT
		maxDecimalDigits int
		expected         geopb.EWKT
	}{
		{"POINT(1.01 1.01)", 15, "POINT (1.01 1.01)"},
		{"POINT(1.01 1.01)", 1, "POINT (1 1)"},
		{"SRID=4004;POINT(1.0 1.0)", 15, "SRID=4004;POINT (1 1)"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := EWKBToEWKT(so.EWKB, tc.maxDecimalDigits)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestEWKBToWKB(t *testing.T) {
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
			encoded, err := EWKBToWKB(so.EWKB, DefaultEWKBEncodingFormat)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestEWKBToGeoJSON(t *testing.T) {
	testCases := []struct {
		ewkt     geopb.EWKT
		flag     EWKBToGeoJSONFlag
		expected string
	}{
		{"POINT(1.0 1.0)", EWKBToGeoJSONFlagZero, `{"type":"Point","coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", EWKBToGeoJSONFlagIncludeBBox, `{"type":"Point","bbox":[1,1,1,1],"coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", EWKBToGeoJSONFlagShortCRS | EWKBToGeoJSONFlagIncludeBBox, `{"type":"Point","bbox":[1,1,1,1],"coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", EWKBToGeoJSONFlagShortCRS, `{"type":"Point","coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", EWKBToGeoJSONFlagLongCRS, `{"type":"Point","coordinates":[1,1]}`},
		{"POINT(1.0 1.0)", EWKBToGeoJSONFlagShortCRSIfNot4326, `{"type":"Point","coordinates":[1,1]}`},
		{"POINT(1.1234567 1.9876543)", EWKBToGeoJSONFlagShortCRSIfNot4326, `{"type":"Point","coordinates":[1.123457,1.987654]}`},
		{"SRID=4326;POINT(1.0 1.0)", EWKBToGeoJSONFlagZero, `{"type":"Point","coordinates":[1,1]}`},
		{"SRID=4326;POINT(1.0 1.0)", EWKBToGeoJSONFlagIncludeBBox, `{"type":"Point","bbox":[1,1,1,1],"coordinates":[1,1]}`},
		{"SRID=4326;POINT(1.0 1.0)", EWKBToGeoJSONFlagLongCRS, `{"type":"Point","crs":{"type":"name","properties":{"name":"urn:ogc:def:crs:EPSG::4326"}},"coordinates":[1,1]}`},
		{"SRID=4326;POINT(1.0 1.0)", EWKBToGeoJSONFlagShortCRS, `{"type":"Point","crs":{"type":"name","properties":{"name":"EPSG:4326"}},"coordinates":[1,1]}`},
		{"SRID=4004;POINT(1.0 1.0)", EWKBToGeoJSONFlagShortCRS, `{"type":"Point","crs":{"type":"name","properties":{"name":"EPSG:4004"}},"coordinates":[1,1]}`},
		{"SRID=4004;POINT(1.0 1.0)", EWKBToGeoJSONFlagShortCRS | EWKBToGeoJSONFlagIncludeBBox, `{"type":"Point","bbox":[1,1,1,1],"crs":{"type":"name","properties":{"name":"EPSG:4004"}},"coordinates":[1,1]}`},
		{"SRID=4326;POINT(1.0 1.0)", EWKBToGeoJSONFlagShortCRSIfNot4326, `{"type":"Point","coordinates":[1,1]}`},
		{"SRID=4004;POINT(1.0 1.0)", EWKBToGeoJSONFlagShortCRSIfNot4326, `{"type":"Point","crs":{"type":"name","properties":{"name":"EPSG:4004"}},"coordinates":[1,1]}`},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := EWKBToGeoJSON(so.EWKB, 6, tc.flag)
			require.NoError(t, err)
			require.Equal(t, tc.expected, string(encoded))
		})
	}
}

func TestEWKBToWKBHex(t *testing.T) {
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
			encoded, err := EWKBToWKBHex(so.EWKB)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestEWKBToKML(t *testing.T) {
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
			encoded, err := EWKBToKML(so.EWKB)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}
