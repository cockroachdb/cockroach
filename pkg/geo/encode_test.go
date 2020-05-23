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
		ewkt     geopb.EWKT
		expected geopb.WKT
	}{
		{"POINT(1.0 1.0)", "POINT (1 1)"},
		{"SRID=4;POINT(1.0 1.0)", "POINT (1 1)"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := EWKBToWKT(so.EWKB)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}

func TestEWKBToEWKT(t *testing.T) {
	testCases := []struct {
		ewkt     geopb.EWKT
		expected geopb.EWKT
	}{
		{"POINT(1.0 1.0)", "POINT (1 1)"},
		{"SRID=4;POINT(1.0 1.0)", "SRID=4;POINT (1 1)"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := EWKBToEWKT(so.EWKB)
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
		{"SRID=4;POINT(1.0 1.0)", []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f")},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
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
		expected string
	}{
		{"POINT(1.0 1.0)", `{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1]},"properties":null}`},
		{"SRID=4;POINT(1.0 1.0)", `{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1]},"properties":null}`},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := EWKBToGeoJSON(so.EWKB)
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
		{"SRID=4;POINT(1.0 1.0)", "0101000000000000000000F03F000000000000F03F"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
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
		{"SRID=4;POINT(1.0 1.0)", `<?xml version="1.0" encoding="UTF-8"?>
<Point><coordinates>1,1</coordinates></Point>`},
	}

	for _, tc := range testCases {
		t.Run(string(tc.ewkt), func(t *testing.T) {
			so, err := parseEWKT(tc.ewkt, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
			require.NoError(t, err)
			encoded, err := EWKBToKML(so.EWKB)
			require.NoError(t, err)
			require.Equal(t, tc.expected, encoded)
		})
	}
}
