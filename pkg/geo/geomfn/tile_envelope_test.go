// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
)

func TestTileEnvelope(t *testing.T) {
	testCases := []struct {
		desc     string
		tileZoom int
		tileX    int
		tileY    int
		bounds   string
		margin   float64
		expected string
	}{
		{
			desc:     "with explicit POLYGON bounds",
			tileZoom: 3,
			tileX:    1,
			tileY:    1,
			bounds:   "SRID=4326;POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))",
			margin:   0.0,
			expected: "SRID=4326;POLYGON((-135 45,-135 67.5,-90 67.5,-90 45,-135 45))",
		},
		{
			desc:     "using default LINESTRING bounds value",
			tileZoom: 8,
			tileX:    1,
			tileY:    2,
			bounds:   "SRID=3857;LINESTRING(-20037508.342789244 -20037508.342789244, 20037508.342789244 20037508.342789244)",
			margin:   0.0,
			expected: "SRID=3857;POLYGON((-19880965.308861203 19567879.241005123,-19880965.308861203 19724422.274933163,-19724422.274933163 19724422.274933163,-19724422.274933163 19567879.241005123,-19880965.308861203 19567879.241005123))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			bounds, err := geo.ParseGeometry(tc.bounds)
			require.NoError(t, err)

			result, err := TileEnvelope(tc.tileZoom, tc.tileX, tc.tileY, bounds, tc.margin)
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)

			require.Equal(t, expected, result)
		})
	}

	errorTestCases := []struct {
		desc     string
		tileZoom int
		tileX    int
		tileY    int
		bounds   string
		margin   float64
	}{
		{
			desc:     "zoom level below range",
			tileZoom: -3,
			tileX:    1,
			tileY:    1,
			bounds:   "SRID=4326;POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))",
			margin:   0.0,
		},
		{
			desc:     "zoom level above range",
			tileZoom: 33,
			tileX:    1,
			tileY:    1,
			bounds:   "SRID=4326;POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))",
			margin:   0.0,
		},
		{
			desc:     "margin value less than -50%",
			tileZoom: 3,
			tileX:    1,
			tileY:    1,
			bounds:   "SRID=4326;POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))",
			margin:   -1,
		},
		{
			desc:     "Invalid tile x value",
			tileZoom: 1,
			tileX:    100,
			tileY:    1,
			bounds:   "SRID=4326;POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))",
			margin:   0.0,
		},
		{
			desc:     "Invalid tile y value",
			tileZoom: 1,
			tileX:    1,
			tileY:    100,
			bounds:   "SRID=4326;POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))",
			margin:   0.0,
		},
	}

	t.Run("Errors on invalid input", func(t *testing.T) {
		for _, tc := range errorTestCases {
			t.Run(tc.desc, func(t *testing.T) {
				bounds, err := geo.ParseGeometry(tc.bounds)
				require.NoError(t, err)

				_, err = TileEnvelope(tc.tileZoom, tc.tileX, tc.tileY, bounds, tc.margin)
				require.Error(t, err)
			})
		}
	})
}
