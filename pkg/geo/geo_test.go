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

func TestGeospatialTypeFitsColumnMetadata(t *testing.T) {
	testCases := []struct {
		t             GeospatialType
		srid          geopb.SRID
		shape         geopb.ShapeType
		errorContains string
	}{
		{MustParseGeometry("POINT(1.0 1.0)"), 0, geopb.ShapeType_Geometry, ""},
		{MustParseGeometry("POINT(1.0 1.0)"), 0, geopb.ShapeType_Unset, ""},
		{MustParseGeometry("SRID=4326;POINT(1.0 1.0)"), 0, geopb.ShapeType_Geometry, ""},
		{MustParseGeometry("SRID=4326;POINT(1.0 1.0)"), 0, geopb.ShapeType_Unset, ""},
		{MustParseGeometry("SRID=4326;POINT(1.0 1.0)"), 0, geopb.ShapeType_LineString, "type Point does not match column type LineString"},
		{MustParseGeometry("POINT(1.0 1.0)"), 4326, geopb.ShapeType_Geometry, "SRID 0 does not match column SRID 4326"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%#v_fits_%d_%s", tc.t, tc.srid, tc.shape), func(t *testing.T) {
			err := GeospatialTypeFitsColumnMetadata(tc.t, tc.srid, tc.shape)
			if tc.errorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSpatialObjectFromGeomT(t *testing.T) {
	testCases := []struct {
		desc string
		g    geom.T
		ret  geopb.SpatialObject
	}{
		{
			"point",
			testGeomPoint,
			geopb.SpatialObject{
				SRID: 0,
				Shape: geopb.MakeSingleSpatialObjectShape(geopb.Shape{
					ShapeType: geopb.ShapeType_Point,
					Coords:    []float64{1, 2},
				}),
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 2, MaxY: 2},
			},
		},
		{
			"linestring",
			testGeomLineString,
			geopb.SpatialObject{
				SRID: 4326,
				Shape: geopb.MakeSingleSpatialObjectShape(geopb.Shape{
					ShapeType: geopb.ShapeType_LineString,
					Coords:    []float64{1, 1, 2, 2},
				}),
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			},
		},
		{
			"polygon",
			testGeomPolygon,
			geopb.SpatialObject{
				SRID: 0,
				Shape: geopb.MakeSingleSpatialObjectShape(geopb.Shape{
					ShapeType: geopb.ShapeType_Polygon,
					Coords:    []float64{1.0, 1.0, 2.0, 2.0, 1.0, 2.0, 1.0, 1.0},
					Ends:      []int64{8},
				}),
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			},
		},
		{
			"multipoint",
			testGeomMultiPoint,
			geopb.SpatialObject{
				SRID: 0,
				Shape: geopb.MakeSingleSpatialObjectShape(geopb.Shape{
					ShapeType: geopb.ShapeType_MultiPoint,
					Coords:    []float64{1.0, 1.0, 2.0, 2.0},
					Ends:      []int64{2, 4},
				}),
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			},
		},
		{
			"multilinestring",
			testGeomMultiLineString,
			geopb.SpatialObject{
				SRID: 0,
				Shape: geopb.MakeSingleSpatialObjectShape(geopb.Shape{
					ShapeType: geopb.ShapeType_MultiLineString,
					Coords:    []float64{1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0},
					Ends:      []int64{4, 8},
				}),
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 4, MinY: 1, MaxY: 4},
			},
		},
		{
			"multipolygon",
			testGeomMultiPolygon,
			geopb.SpatialObject{
				SRID: 0,
				Shape: geopb.MakeSingleSpatialObjectShape(geopb.Shape{
					ShapeType: geopb.ShapeType_MultiPolygon,
					Coords:    []float64{1.0, 1.0, 2.0, 2.0, 1.0, 2.0, 1.0, 1.0, 3.0, 3.0, 4.0, 4.0, 3.0, 4.0, 3.0, 3.0},
					Ends:      []int64{8, 16},
					Endss:     []int64{1, 2},
				}),
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 4, MinY: 1, MaxY: 4},
			},
		},
		{
			"geometrycollection",
			testGeomGeometryCollection,
			geopb.SpatialObject{
				SRID: 0,
				Shape: geopb.MakeGeometryCollectionSpatialObjectShape([]geopb.Shape{
					{
						ShapeType: geopb.ShapeType_Point,
						Coords:    []float64{1, 2},
					},
					{
						ShapeType: geopb.ShapeType_MultiPoint,
						Coords:    []float64{1.0, 1.0, 2.0, 2.0},
						Ends:      []int64{2, 4},
					},
				}),
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			so, err := spatialObjectFromGeomT(tc.g)
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

			shapes, err := g.AsS2(EmptyBehaviorError)
			require.NoError(t, err)

			require.Equal(t, tc.expected, shapes)
		})
	}

	// Test when things are empty.
	emptyTestCases := []struct {
		wkt          string
		expectedOmit []s2.Region
	}{
		{
			"GEOMETRYCOLLECTION ( LINESTRING EMPTY, MULTIPOINT((1.0 5.0), (3.0 4.0)) )",
			[]s2.Region{
				s2.PointFromLatLng(s2.LatLngFromDegrees(5.0, 1.0)),
				s2.PointFromLatLng(s2.LatLngFromDegrees(4.0, 3.0)),
			},
		},
		{
			"GEOMETRYCOLLECTION EMPTY",
			nil,
		},
		{
			"MULTILINESTRING (EMPTY, (1.0 2.0, 3.0 4.0))",
			[]s2.Region{
				s2.PolylineFromLatLngs([]s2.LatLng{
					s2.LatLngFromDegrees(2.0, 1.0),
					s2.LatLngFromDegrees(4.0, 3.0),
				}),
			},
		},
		{
			"MULTILINESTRING (EMPTY, EMPTY)",
			nil,
		},
	}

	for _, tc := range emptyTestCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := ParseGeography(tc.wkt)
			require.NoError(t, err)

			_, err = g.AsS2(EmptyBehaviorError)
			require.Error(t, err)
			require.True(t, IsEmptyGeometryError(err))

			shapes, err := g.AsS2(EmptyBehaviorOmit)
			require.NoError(t, err)
			require.Equal(t, tc.expectedOmit, shapes)
		})
	}
}
