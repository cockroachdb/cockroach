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
