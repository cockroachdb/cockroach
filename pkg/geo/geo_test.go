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
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

var (
	testGeomPoint              = geom.NewPointFlat(geom.XY, []float64{1.0, 2.0})
	testGeomLineString         = geom.NewLineStringFlat(geom.XY, []float64{1.0, 1.0, 2.0, 2.0})
	testGeomPolygon            = geom.NewPolygonFlat(geom.XY, []float64{1.0, 1.0, 2.0, 2.0, 1.0, 2.0, 1.0, 1.0}, []int{8})
	testGeomMultiPoint         = geom.NewMultiPointFlat(geom.XY, []float64{1.0, 1.0, 2.0, 2.0})
	testGeomMultiLineString    = geom.NewMultiLineStringFlat(geom.XY, []float64{1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0}, []int{4, 8})
	testGeomMultiPolygon       = geom.NewMultiPolygon(geom.XY)
	testGeomGeometryCollection = geom.NewGeometryCollection()
)
var (
	emptyGeomPoint                       = geom.NewPointEmpty(geom.XY)
	emptyGeomLineString                  = geom.NewLineString(geom.XY)
	emptyGeomPolygon                     = geom.NewPolygon(geom.XY)
	emptyGeomMultiPoint                  = geom.NewMultiPoint(geom.XY)
	emptyGeomMultiLineString             = geom.NewMultiLineString(geom.XY)
	emptyGeomMultiPolygon                = geom.NewMultiPolygon(geom.XY)
	emptyGeomGeometryCollection          = geom.NewGeometryCollection()
	emptyGeomObjectsInGeometryCollection = geom.NewGeometryCollection()
	emptyGeomPointInGeometryCollection   = geom.NewGeometryCollection()
)

func init() {
	testGeomLineString.SetSRID(4326)
	// Initialize geomTestMultiPolygon.
	{
		for _, polygon := range []*geom.Polygon{
			testGeomPolygon,
			geom.NewPolygonFlat(geom.XY, []float64{3.0, 3.0, 4.0, 4.0, 3.0, 4.0, 3.0, 3.0}, []int{8}),
		} {
			err := testGeomMultiPolygon.Push(polygon)
			if err != nil {
				panic(err)
			}
		}
	}
	// Initialize testGeomGeometryCollection.
	{
		err := testGeomGeometryCollection.Push(testGeomPoint, testGeomMultiPoint)
		if err != nil {
			panic(err)
		}
	}
	// Initialize emptyGeomPointInGeometryCollection.
	{
		err := emptyGeomPointInGeometryCollection.Push(
			geom.NewLineStringFlat(geom.XY, []float64{1.0, 1.0, 2.0, 2.0}),
			emptyGeomPoint,
		)
		if err != nil {
			panic(err)
		}
	}
	// Initialize emptyGeomObjectsInGeometryCollection.
	{
		err := emptyGeomObjectsInGeometryCollection.Push(
			emptyGeomPoint,
			emptyGeomLineString,
			emptyGeomPolygon,
		)
		if err != nil {
			panic(err)
		}
	}
}

func mustDecodeEWKBFromString(t *testing.T, h string) geopb.EWKB {
	decoded, err := hex.DecodeString(h)
	require.NoError(t, err)
	return geopb.EWKB(decoded)
}

func TestSpatialObjectFromGeom(t *testing.T) {
	testCases := []struct {
		desc string
		g    geom.T
		ret  geopb.SpatialObject
	}{
		{
			"point",
			testGeomPoint,
			geopb.SpatialObject{
				EWKB:        mustDecodeEWKBFromString(t, "0101000000000000000000F03F0000000000000040"),
				SRID:        0,
				Shape:       geopb.Shape_Point,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 2, MaxY: 2},
			},
		},
		{
			"linestring",
			testGeomLineString,
			geopb.SpatialObject{
				EWKB:        mustDecodeEWKBFromString(t, "0102000020E610000002000000000000000000F03F000000000000F03F00000000000000400000000000000040"),
				SRID:        4326,
				Shape:       geopb.Shape_LineString,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			},
		},
		{
			"polygon",
			testGeomPolygon,
			geopb.SpatialObject{
				EWKB:        mustDecodeEWKBFromString(t, "01030000000100000004000000000000000000F03F000000000000F03F00000000000000400000000000000040000000000000F03F0000000000000040000000000000F03F000000000000F03F"),
				SRID:        0,
				Shape:       geopb.Shape_Polygon,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			},
		},
		{
			"multipoint",
			testGeomMultiPoint,
			geopb.SpatialObject{
				EWKB:        mustDecodeEWKBFromString(t, "0104000000020000000101000000000000000000F03F000000000000F03F010100000000000000000000400000000000000040"),
				SRID:        0,
				Shape:       geopb.Shape_MultiPoint,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			},
		},
		{
			"multilinestring",
			testGeomMultiLineString,
			geopb.SpatialObject{
				EWKB:        mustDecodeEWKBFromString(t, "010500000002000000010200000002000000000000000000F03F000000000000F03F000000000000004000000000000000400102000000020000000000000000000840000000000000084000000000000010400000000000001040"),
				SRID:        0,
				Shape:       geopb.Shape_MultiLineString,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 4, MinY: 1, MaxY: 4},
			},
		},
		{
			"multipolygon",
			testGeomMultiPolygon,
			geopb.SpatialObject{
				EWKB:        mustDecodeEWKBFromString(t, "01060000000200000001030000000100000004000000000000000000F03F000000000000F03F00000000000000400000000000000040000000000000F03F0000000000000040000000000000F03F000000000000F03F0103000000010000000400000000000000000008400000000000000840000000000000104000000000000010400000000000000840000000000000104000000000000008400000000000000840"),
				SRID:        0,
				Shape:       geopb.Shape_MultiPolygon,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 4, MinY: 1, MaxY: 4},
			},
		},
		{
			"geometrycollection",
			testGeomGeometryCollection,
			geopb.SpatialObject{
				EWKB:        mustDecodeEWKBFromString(t, "0107000000020000000101000000000000000000F03F00000000000000400104000000020000000101000000000000000000F03F000000000000F03F010100000000000000000000400000000000000040"),
				SRID:        0,
				Shape:       geopb.Shape_GeometryCollection,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			so, err := spatialObjectFromGeom(tc.g)
			require.NoError(t, err)
			require.Equal(t, tc.ret, so)
		})
	}
}

func TestGeographyAsS2(t *testing.T) {
	testCases := []struct {
		wkt      string
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
			`POLYGON(
				(0.0 0.0, 0.5 0.0, 0.75 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0),
				(0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2)
			)`,
			[]s2.Region{
				s2.PolygonFromLoops([]*s2.Loop{
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.5)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.75)),
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
			`POLYGON(
				(0.0 0.0, 0.0 1.0, 1.0 1.0, 1.0 0.0, 0.0 0.0),
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
			`POLYGON(
				(0.0 0.0, 0.0 0.5, 0.0 1.0, 1.0 1.0, 1.0 0.0, 0.0 0.0),
				(0.2 0.2, 0.2 0.4, 0.4 0.4, 0.4 0.2, 0.2 0.2)
			)`,
			[]s2.Region{
				s2.PolygonFromLoops([]*s2.Loop{
					s2.LoopFromPoints([]s2.Point{
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.0, 1.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(1.0, 1.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(1.0, 0.0)),
						s2.PointFromLatLng(s2.LatLngFromDegrees(0.5, 0.0)),
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
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := ParseGeography(tc.wkt)
			require.NoError(t, err)

			shapes, err := g.AsS2()
			require.NoError(t, err)

			require.Equal(t, tc.expected, shapes)
		})
	}
}

func TestClipEWKBByRect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var g *Geometry
	var err error
	datadriven.RunTest(t, "testdata/clip", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "geometry":
			g, err = ParseGeometry(d.Input)
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
			ewkb, err := geos.ClipEWKBByRect(
				g.EWKB(), float64(xMin), float64(yMin), float64(xMax), float64(yMax))
			if err != nil {
				return err.Error()
			}
			// TODO(sumeer):
			// - add WKB to WKT and print exact output
			// - expand test with more inputs
			return fmt.Sprintf(
				"%d => %d (srid: %d)",
				len(g.EWKB()),
				len(ewkb),
				g.SRID(),
			)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
