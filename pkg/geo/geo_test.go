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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
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
		expectedErr string
	}{
		{
			"POINT(1.0 1.0)",
			NewGeometry(geopb.EWKB([]byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"))),
			"",
		},
		{
			"invalid",
			nil,
			"geos error: ParseException: Unknown type: 'INVALID'",
		},
		{
			"",
			nil,
			"geos error: ParseException: Expected word but encountered end of stream",
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.wkt), func(t *testing.T) {
			g, err := ParseGeometry(tc.wkt)
			if len(tc.expectedErr) > 0 {
				require.Equal(t, tc.expectedErr, err.Error())
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
		expectedErr string
	}{
		{
			"POINT(1.0 1.0)",
			NewGeography(geopb.EWKB([]byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"))),
			"",
		},
		{
			"invalid",
			nil,
			"geos error: ParseException: Unknown type: 'INVALID'",
		},
		{
			"",
			nil,
			"geos error: ParseException: Expected word but encountered end of stream",
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.wkt), func(t *testing.T) {
			g, err := ParseGeography(tc.wkt)
			if len(tc.expectedErr) > 0 {
				require.Equal(t, tc.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, g)
			}
		})
	}
}

func TestClipWKBByRect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var g *Geometry
	var err error
	datadriven.RunTest(t, "testdata/clip", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "geometry":
			g, err = ParseGeometry(geopb.WKT(d.Input))
			if err != nil {
				return err.Error()
			}
			return ""
		case "clip":
			var xMin, yMin, xMax, yMax int
			d.ScanArgs(t, "xmin", &xMin)
			d.ScanArgs(t, "ymin", &yMin)
			d.ScanArgs(t, "xmax", &xMax)
			d.ScanArgs(t, "ymax", &yMax)
			wkb, err := geos.ClipWKBByRect(
				geopb.WKB(g.ewkb), float64(xMin), float64(yMin), float64(xMax), float64(yMax))
			if err != nil {
				return err.Error()
			}
			// TODO(sumeer):
			// - add WKB to WKT and print exact output
			// - expand test with more inputs
			return fmt.Sprintf("%d => %d", len(g.ewkb), len(wkb))
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
