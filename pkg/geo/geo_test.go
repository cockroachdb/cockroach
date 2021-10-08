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
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/assert"
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

func TestSpatialObjectFitsColumnMetadata(t *testing.T) {
	testCases := []struct {
		t             Geometry
		srid          geopb.SRID
		shape         geopb.ShapeType
		errorContains string
	}{
		{MustParseGeometry("POINT(1.0 1.0)"), 0, geopb.ShapeType_Geometry, ""},
		{MustParseGeometry("POINT Z(1.0 1.0 1.0)"), 0, geopb.ShapeType_GeometryZ, ""},
		{MustParseGeometry("POINT ZM(1.0 1.0 1.0 1.0)"), 0, geopb.ShapeType_GeometryZM, ""},
		{MustParseGeometry("POINT M(1.0 1.0 1.0)"), 0, geopb.ShapeType_GeometryM, ""},
		{MustParseGeometry("POINT(1.0 1.0)"), 0, geopb.ShapeType_Unset, ""},
		{MustParseGeometry("SRID=4326;POINT(1.0 1.0)"), 0, geopb.ShapeType_Geometry, ""},
		{MustParseGeometry("SRID=4326;POINT(1.0 1.0)"), 0, geopb.ShapeType_Unset, ""},
		{MustParseGeometry("SRID=4326;POINT(1.0 1.0)"), 0, geopb.ShapeType_LineString, "type Point does not match column type LineString"},
		{MustParseGeometry("POINT(1.0 1.0)"), 4326, geopb.ShapeType_Geometry, "SRID 0 does not match column SRID 4326"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%#v_fits_%d_%s", tc.t, tc.srid, tc.shape), func(t *testing.T) {
			err := SpatialObjectFitsColumnMetadata(tc.t.SpatialObject(), tc.srid, tc.shape)
			if tc.errorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMakeValidGeographyGeom(t *testing.T) {
	var (
		invalidGeomPoint              = geom.NewPointFlat(geom.XY, []float64{200.0, 199.0})
		invalidGeomLineString         = geom.NewLineStringFlat(geom.XY, []float64{90.0, 90.0, 180.0, 180.0})
		invalidGeomPolygon            = geom.NewPolygonFlat(geom.XY, []float64{360.0, 360.0, 450.0, 450.0, 540.0, 540.0, 630.0, 630.0}, []int{8})
		invalidGeomMultiPoint         = geom.NewMultiPointFlat(geom.XY, []float64{-90.0, -90.0, -180.0, -180.0})
		invalidGeomMultiLineString    = geom.NewMultiLineStringFlat(geom.XY, []float64{-270.0, -270.0, -360.0, -360.0, -450.0, -450.0, -540.0, -540.0}, []int{4, 8})
		invalidGeomMultiPolygon       = geom.NewMultiPolygon(geom.XY)
		invalidGeomGeometryCollection = geom.NewGeometryCollection()
	)
	invalidGeomGeometryCollection.MustPush(geom.NewPointFlat(geom.XY, []float64{200.0, 199.0}))
	invalidGeomGeometryCollection.MustPush(geom.NewLineStringFlat(geom.XY, []float64{90.0, 90.0, 180.0, 180.0}))
	var (
		validGeomPoint              = geom.NewPointFlat(geom.XY, []float64{-160.0, -19.0})
		validGeomLineString         = geom.NewLineStringFlat(geom.XY, []float64{90.0, 90.0, 180.0, 0.0})
		validGeomPolygon            = geom.NewPolygonFlat(geom.XY, []float64{0.0, 0.0, 90.0, 90.0, -180.0, 0.0, -90.0, -90.0}, []int{8})
		validGeomMultiPoint         = geom.NewMultiPointFlat(geom.XY, []float64{-90.0, -90.0, -180.0, 0.0})
		validGeomMultiLineString    = geom.NewMultiLineStringFlat(geom.XY, []float64{90.0, 90.0, 0.0, 0.0, -90.0, -90.0, 180.0, 0.0}, []int{4, 8})
		validGeomMultiPolygon       = geom.NewMultiPolygon(geom.XY)
		validGeomGeometryCollection = geom.NewGeometryCollection()
	)
	validGeomGeometryCollection.MustPush(validGeomPoint)
	validGeomGeometryCollection.MustPush(validGeomLineString)
	testCases := []struct {
		desc string
		g    geom.T
		ret  geom.T
	}{
		{
			"Point",
			invalidGeomPoint,
			validGeomPoint,
		},
		{
			"linestring",
			invalidGeomLineString,
			validGeomLineString,
		},
		{
			"polygon",
			invalidGeomPolygon,
			validGeomPolygon,
		},
		{
			"multipoint",
			invalidGeomMultiPoint,
			validGeomMultiPoint,
		},
		{
			"multilinestring",
			invalidGeomMultiLineString,
			validGeomMultiLineString,
		},
		{
			"multipolygon",
			invalidGeomMultiPolygon,
			validGeomMultiPolygon,
		},
		{
			"geometrycollection",
			invalidGeomGeometryCollection,
			validGeomGeometryCollection,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			normalizeGeographyGeomT(tc.g)
			require.Equal(t, tc.ret, tc.g)
		})
	}
}

func TestSpatialObjectFromGeomT(t *testing.T) {
	testCases := []struct {
		desc   string
		soType geopb.SpatialObjectType
		g      geom.T
		ret    geopb.SpatialObject
	}{
		{
			"point",
			geopb.SpatialObjectType_GeometryType,
			testGeomPoint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        mustDecodeEWKBFromString(t, "0101000000000000000000F03F0000000000000040"),
				SRID:        0,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 2, HiY: 2},
			},
		},
		{
			"linestring",
			geopb.SpatialObjectType_GeometryType,
			testGeomLineString,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        mustDecodeEWKBFromString(t, "0102000020E610000002000000000000000000F03F000000000000F03F00000000000000400000000000000040"),
				SRID:        4326,
				ShapeType:   geopb.ShapeType_LineString,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2},
			},
		},
		{
			"polygon",
			geopb.SpatialObjectType_GeometryType,
			testGeomPolygon,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        mustDecodeEWKBFromString(t, "01030000000100000004000000000000000000F03F000000000000F03F00000000000000400000000000000040000000000000F03F0000000000000040000000000000F03F000000000000F03F"),
				SRID:        0,
				ShapeType:   geopb.ShapeType_Polygon,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2},
			},
		},
		{
			"multipoint",
			geopb.SpatialObjectType_GeometryType,
			testGeomMultiPoint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        mustDecodeEWKBFromString(t, "0104000000020000000101000000000000000000F03F000000000000F03F010100000000000000000000400000000000000040"),
				SRID:        0,
				ShapeType:   geopb.ShapeType_MultiPoint,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2},
			},
		},
		{
			"multilinestring",
			geopb.SpatialObjectType_GeometryType,
			testGeomMultiLineString,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        mustDecodeEWKBFromString(t, "010500000002000000010200000002000000000000000000F03F000000000000F03F000000000000004000000000000000400102000000020000000000000000000840000000000000084000000000000010400000000000001040"),
				SRID:        0,
				ShapeType:   geopb.ShapeType_MultiLineString,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 4, LoY: 1, HiY: 4},
			},
		},
		{
			"multipolygon",
			geopb.SpatialObjectType_GeometryType,
			testGeomMultiPolygon,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        mustDecodeEWKBFromString(t, "01060000000200000001030000000100000004000000000000000000F03F000000000000F03F00000000000000400000000000000040000000000000F03F0000000000000040000000000000F03F000000000000F03F0103000000010000000400000000000000000008400000000000000840000000000000104000000000000010400000000000000840000000000000104000000000000008400000000000000840"),
				SRID:        0,
				ShapeType:   geopb.ShapeType_MultiPolygon,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 4, LoY: 1, HiY: 4},
			},
		},
		{
			"geometrycollection",
			geopb.SpatialObjectType_GeometryType,
			testGeomGeometryCollection,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        mustDecodeEWKBFromString(t, "0107000000020000000101000000000000000000F03F00000000000000400104000000020000000101000000000000000000F03F000000000000F03F010100000000000000000000400000000000000040"),
				SRID:        0,
				ShapeType:   geopb.ShapeType_GeometryCollection,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			so, err := spatialObjectFromGeomT(tc.g, tc.soType)
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

func TestGeographySpaceCurveIndex(t *testing.T) {
	orderedTestCases := []struct {
		orderedWKTs []string
		srid        geopb.SRID
	}{
		{
			[]string{
				"POINT EMPTY",
				"POLYGON EMPTY",
				"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
				"POINT(-80 80)",
				"LINESTRING(0 0, -90 -80)",
			},
			4326,
		},
		{
			[]string{
				"POINT EMPTY",
				"POLYGON EMPTY",
				"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
				"POINT(-80 80)",
				"LINESTRING(0 0, -90 -80)",
			},
			4004,
		},
	}
	for i, tc := range orderedTestCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			previous := uint64(0)
			for _, wkt := range tc.orderedWKTs {
				t.Run(wkt, func(t *testing.T) {
					g, err := ParseGeography(wkt)
					require.NoError(t, err)
					g, err = g.CloneWithSRID(tc.srid)
					require.NoError(t, err)

					h := g.SpaceCurveIndex()
					assert.GreaterOrEqual(t, h, previous)
					previous = h
				})
			}
		})
	}
}

func TestGeometrySpaceCurveIndex(t *testing.T) {
	valueTestCases := []struct {
		wkt      string
		expected uint64
	}{
		{
			wkt:      "POINT EMPTY",
			expected: 0,
		},
		{
			wkt:      "SRID=4326;POINT EMPTY",
			expected: 0,
		},
		{
			wkt:      "POINT (100 80)",
			expected: 9223372036854787504,
		},
		{
			wkt:      "SRID=4326;POINT(100 80)",
			expected: 11895367802890724441,
		},
		{
			wkt:      "POINT (1000 800)",
			expected: 9223372036855453930,
		},
		{
			wkt:      "SRID=4326;POINT(1000 800)",
			expected: math.MaxUint64,
		},
	}

	for _, tc := range valueTestCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := ParseGeometry(tc.wkt)
			require.NoError(t, err)
			spaceCurveIndex, err := g.SpaceCurveIndex()
			require.NoError(t, err)
			require.Equal(t, tc.expected, spaceCurveIndex)
		})
	}

	orderedTestCases := []struct {
		orderedWKTs []string
		srid        geopb.SRID
	}{
		{
			[]string{
				"POINT EMPTY",
				"POLYGON EMPTY",
				"LINESTRING(0 0, -90 -80)",
				"POINT(-80 80)",
				"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
			},
			4326,
		},
		{
			[]string{
				"POINT EMPTY",
				"POLYGON EMPTY",
				"LINESTRING(0 0, -90 -80)",
				"POINT(-80 80)",
				"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
			},
			3857,
		},
		{
			[]string{
				"POINT EMPTY",
				"POLYGON EMPTY",
				"LINESTRING(0 0, -90 -80)",
				"POINT(-80 80)",
				"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
			},
			0,
		},
	}
	for i, tc := range orderedTestCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			previous := uint64(0)
			for _, wkt := range tc.orderedWKTs {
				t.Run(wkt, func(t *testing.T) {
					g, err := ParseGeometry(wkt)
					require.NoError(t, err)
					g, err = g.CloneWithSRID(tc.srid)
					require.NoError(t, err)
					h, err := g.SpaceCurveIndex()
					require.NoError(t, err)
					assert.GreaterOrEqual(t, h, previous)
					previous = h
				})
			}
		})
	}
}

func TestGeometryAsGeography(t *testing.T) {
	for _, tc := range []struct {
		geom string
		geog string
	}{
		{"POINT(1 0)", "SRID=4326;POINT(1 0)"},
		{"SRID=4004;POINT(1 0)", "SRID=4004;POINT(1 0)"},
	} {
		t.Run(tc.geom, func(t *testing.T) {
			geom, err := ParseGeometry(tc.geom)
			require.NoError(t, err)
			geog, err := ParseGeography(tc.geog)
			require.NoError(t, err)

			to, err := geom.AsGeography()
			require.NoError(t, err)
			require.Equal(t, geog, to)
		})
	}
}

func TestGeographyAsGeometry(t *testing.T) {
	for _, tc := range []struct {
		geom string
		geog string
	}{
		{"SRID=4326;POINT(1 0)", "SRID=4326;POINT(1 0)"},
		{"SRID=4004;POINT(1 0)", "SRID=4004;POINT(1 0)"},
	} {
		t.Run(tc.geom, func(t *testing.T) {
			geom, err := ParseGeometry(tc.geom)
			require.NoError(t, err)
			geog, err := ParseGeography(tc.geog)
			require.NoError(t, err)

			to, err := geog.AsGeometry()
			require.NoError(t, err)
			require.Equal(t, geom, to)
		})
	}
}

func TestValidateGeomT(t *testing.T) {
	testCases := []struct {
		g        geom.T
		expected error
	}{
		{geom.NewPointEmpty(geom.XY), nil},
		{geom.NewLineString(geom.XY), nil},
		{geom.NewPolygon(geom.XY), nil},
		{geom.NewMultiPoint(geom.XY), nil},
		{geom.NewMultiLineString(geom.XY), nil},
		{geom.NewMultiPolygon(geom.XY), nil},
		{geom.NewGeometryCollection(), nil},

		{geom.NewLineStringFlat(geom.XY, []float64{1, 0}), errors.Newf("LineString must have at least 2 coordinates")},
		{
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{0, 0, 1, 0, 1, 1},
				[]int{6},
			),
			errors.Newf("Polygon LinearRing must have at least 4 points, found 3 at position 1"),
		},
		{
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{0, 0, 1, 0, 1, 1, 4, 4},
				[]int{8},
			),
			errors.Newf("Polygon LinearRing at position 1 is not closed"),
		},
		{
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{0, 0, 1, 0, 1, 1, 0, 0},
				[]int{8},
			),
			nil,
		},
		{
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					0, 0, 1, 0, 1, 1, 0, 0,
					0.1, 0.1, 1.0, 0.1, 0.9, 0.9,
				},
				[]int{8, 14},
			),
			errors.Newf("Polygon LinearRing must have at least 4 points, found 3 at position 2"),
		},
		{
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					0, 0, 1, 0, 1, 1, 0, 0,
					0.1, 0.1, 1.0, 0.1, 0.9, 0.9, 0.3, 0.3,
				},
				[]int{8, 16},
			),
			errors.Newf("Polygon LinearRing at position 2 is not closed"),
		},

		{
			geom.NewMultiLineStringFlat(
				geom.XY,
				[]float64{
					0, 0, 1, 1,
					2, 2,
				},
				[]int{4, 6},
			),
			errors.Newf("invalid MultiLineString component at position 2: LineString must have at least 2 coordinates"),
		},

		{
			geom.NewMultiPolygonFlat(
				geom.XY,
				[]float64{
					0, 0, 1, 0, 1, 1, 0, 0,
					0, 0, 1, 0, 1, 1,
				},
				[][]int{{8}, {14}},
			),
			errors.Newf("invalid MultiPolygon component at position 2: Polygon LinearRing must have at least 4 points, found 3 at position 1"),
		},

		{
			geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{0, 1})),
			errors.Newf("invalid GeometryCollection component at position 1: LineString must have at least 2 coordinates"),
		},
		{
			geom.NewGeometryCollection().MustPush(
				geom.NewLineStringFlat(geom.XY, []float64{0, 1, 2, 3}),
			).MustPush(
				geom.NewMultiPolygonFlat(
					geom.XY,
					[]float64{
						0, 0, 1, 0, 1, 1, 0, 0,
						0, 0, 1, 0, 1, 1,
					},
					[][]int{{8}, {14}},
				),
			),
			errors.Newf("invalid GeometryCollection component at position 2: invalid MultiPolygon component at position 2: Polygon LinearRing must have at least 4 points, found 3 at position 1"),
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			err := validateGeomT(tc.g)
			if tc.expected != nil {
				require.EqualError(t, err, tc.expected.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsLinearRingCCW(t *testing.T) {
	testCases := []struct {
		desc     string
		ring     *geom.LinearRing
		expected bool
	}{
		{
			desc: "flat linear ring",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				0, 0,
				0, 0,
				0, 0,
				0, 0,
			}),
			expected: false,
		},
		{
			desc: "invalid linear ring with duplicate points at end deemed CW",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				0, 0,
				0, 0,
				0, 0,
				0, 0,
				1, 0,
				1, 0,
				1, 0,
			}),
			expected: false,
		},
		{
			desc: "invalid linear ring with duplicate points at start deemed CW",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				0, 0,
				0, 0,
				0, 0,
				0, 0,
				-1, 1,
				-1, 1,
				-1, 1,
				0, 0,
			}),
			expected: false,
		},

		{
			desc: "CW linear ring",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				0, 0,
				1, 1,
				1, 0,
				0, 0,
			}),
			expected: false,
		},
		{
			desc: "CW linear ring, first point is bottom right",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				1, 0,
				0, 0,
				1, 1,
				1, 0,
			}),
			expected: false,
		},
		{
			desc: "CW linear ring, duplicate points for bottom right",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				0, 0,
				1, 1,
				1, 0,
				1, 0,
				1, 0,
				0, 0,
			}),
			expected: false,
		},
		{
			desc: "CCW linear ring",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				0, 0,
				1, 0,
				1, 1,
				0, 0,
			}),
			expected: true,
		},
		{
			desc: "CCW linear ring, duplicate points for bottom right",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				0, 0,
				1, 0,
				1, 0,
				1, 0,
				1, 0,
				1, 0,
				1, 1,
				0, 0,
			}),
			expected: true,
		},
		{
			desc: "CCW linear ring, first point is bottom right",
			ring: geom.NewLinearRingFlat(geom.XY, []float64{
				1, 0,
				1, 0,
				1, 1,
				0, 0,
				1, 0,
			}),
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, IsLinearRingCCW(tc.ring))
		})
	}
}
