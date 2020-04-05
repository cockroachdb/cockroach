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
	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
)

func TestGeographyAsS2(t *testing.T) {
	testCases := []struct {
		wkt      geopb.WKT
		expected []s2.Region
	}{
		{
			"POINT(1.0 5.0)",
			[]s2.Region{s2.PointFromLatLng(s2.LatLngFromDegrees(5.0, 1.0))},
		},
		{
			"LINESTRING(1.0 5.0, 6.0 7.0)",
			[]s2.Region{
				s2.PolylineFromLatLngs([]s2.LatLng{
					s2.LatLngFromDegrees(5.0, 1.0),
					s2.LatLngFromDegrees(7.0, 6.0),
				}),
			},
		},
		{
			`POLYGON(
				(0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0),
				(0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2)
			)`,
			[]s2.Region{
				s2.PolygonFromLoops([]*s2.Loop{
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 1.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(1.0, 1.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(1.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)),
					}),
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.2)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.4)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.4, 0.4)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.4, 0.2)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.2)),
					}),
				}),
			},
		},
		{
			`GEOMETRYCOLLECTION(POINT(1.0 2.0), POLYGON(
				(0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0),
				(0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2)
			))`,
			[]s2.Region{
				s2.PointFromLatLng(s2.LatLngFromDegrees(2.0, 1.0)),
				s2.PolygonFromLoops([]*s2.Loop{
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 1.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(1.0, 1.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(1.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)),
					}),
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.2)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.4)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.4, 0.4)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.4, 0.2)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.2)),
					}),
				}),
			},
		},
		{
			"MULTIPOINT((1.0 5.0), (3.0 4.0))",
			[]s2.Region{
				s2.PointFromLatLng(s2.LatLngFromDegrees(5.0, 1.0)),
				s2.PointFromLatLng(s2.LatLngFromDegrees(4.0, 3.0)),
			},
		},
		{
			"MULTILINESTRING((1.0 5.0, 6.0 7.0), (3.0 4.0, 5.0 6.0))",
			[]s2.Region{
				s2.PolylineFromLatLngs([]s2.LatLng{
					s2.LatLngFromDegrees(5.0, 1.0),
					s2.LatLngFromDegrees(7.0, 6.0),
				}),
				s2.PolylineFromLatLngs([]s2.LatLng{
					s2.LatLngFromDegrees(4.0, 3.0),
					s2.LatLngFromDegrees(6.0, 5.0),
				}),
			},
		},
		{
			`MULTIPOLYGON(
				((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0),
				(0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2)),

				((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0),
				(3.2 3.2, 3.2 3.4, 3.4 3.4, 3.4 3.2, 3.2 3.2))
			)`,
			[]s2.Region{
				s2.PolygonFromLoops([]*s2.Loop{
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 1.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(1.0, 1.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(1.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)),
					}),
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.2)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.4)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.4, 0.4)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.4, 0.2)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.2, 0.2)),
					}),
				}),
				s2.PolygonFromLoops([]*s2.Loop{
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(3.0, 3.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(3.0, 4.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(4.0, 4.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(4.0, 3.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(3.0, 3.0)),
					}),
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(3.2, 3.2)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(3.2, 3.4)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(3.4, 3.4)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(3.4, 3.2)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(3.2, 3.2)),
					}),
				}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.wkt), func(t *testing.T) {
			g, err := ParseGeography(tc.wkt)
			require.NoError(t, err)

			figures, err := g.AsS2()
			require.NoError(t, err)

			require.Equal(t, tc.expected, figures)
		})
	}
}

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
