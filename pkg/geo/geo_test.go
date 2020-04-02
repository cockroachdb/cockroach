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

func TestParseGeometry(t *testing.T) {
	testCases := []struct {
		wkt         geopb.WKT
		expected    *Geometry
		expectedErr bool
	}{
		{
			"POINT(1.0 1.0)",
			NewGeometry(geopb.EWKB([]byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"))),
			false,
		},
		{
			"invalid",
			nil,
			true,
		},
		{
			"",
			nil,
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.wkt), func(t *testing.T) {
			g, err := ParseGeometry(tc.wkt)
			if tc.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, g)
			}
		})
	}
}

func TestParseGeography(t *testing.T) {
	testCases := []struct {
		wkt         geopb.WKT
		expected    *Geography
		expectedErr bool
	}{
		{
			"POINT(1.0 1.0)",
			NewGeography(geopb.EWKB([]byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"))),
			false,
		},
		{
			"invalid",
			nil,
			true,
		},
		{
			"",
			nil,
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.wkt), func(t *testing.T) {
			g, err := ParseGeography(tc.wkt)
			if tc.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, g)
			}
		})
	}
}
