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

func TestParseWKB(t *testing.T) {
	testCases := []struct {
		desc          string
		b             []byte
		defaultSRID   geopb.SRID
		expected      geopb.SpatialObject
		expectedError string
	}{
		{
			"EWKB should make this error",
			[]byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
			4326,
			geopb.SpatialObject{},
			"wkb: unknown type: 536870913",
		},
		{
			"Normal WKB should take the SRID",
			[]byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
			4326,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
			},
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret, err := parseWKB(geopb.SpatialObjectType_GeometryType, tc.b, tc.defaultSRID)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ret)
			}
		})
	}
}

func TestParseEWKB(t *testing.T) {
	testCases := []struct {
		desc        string
		b           []byte
		defaultSRID geopb.SRID
		overwrite   defaultSRIDOverwriteSetting
		expected    geopb.SpatialObject
	}{
		{
			"SRID 4326 is hint; EWKB has 4004",
			[]byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
			4326,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4004,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
			},
		},
		{
			"SRID 4326 is hint; EWKB has -1",
			[]byte("\x01\x01\x00\x00\x20\xFF\xFF\xFF\xFF\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
			4326,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
			},
		},
		{
			"Overwrite SRID 4004 with 4326",
			[]byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
			4326,
			DefaultSRIDShouldOverwrite,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret, err := parseEWKB(geopb.SpatialObjectType_GeometryType, tc.b, tc.defaultSRID, tc.overwrite)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestParseEWKT(t *testing.T) {
	testCases := []struct {
		desc        string
		wkt         geopb.EWKT
		defaultSRID geopb.SRID
		overwrite   defaultSRIDOverwriteSetting
		expected    geopb.SpatialObject
	}{
		{
			"EMPTY LINESTRING, no SRID",
			"LINESTRING EMPTY",
			0,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:      geopb.SpatialObjectType_GeometryType,
				EWKB:      []byte("\x01\x02\x00\x00\x00\x00\x00\x00\x00"),
				SRID:      0,
				ShapeType: geopb.ShapeType_LineString,
			},
		},
		{
			"EMPTY POINT, no SRID",
			"POINT EMPTY",
			0,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf8\x7f\x00\x00\x00\x00\x00\x00\xf8\x7f"),
				SRID:        0,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: nil,
			},
		},
		{
			"EMPTY LINESTRING, SRID 4326",
			"SRID=4326;LINESTRING EMPTY",
			0,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:      geopb.SpatialObjectType_GeometryType,
				EWKB:      []byte("\x01\x02\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00"),
				SRID:      4326,
				ShapeType: geopb.ShapeType_LineString,
			},
		},
		{
			"EMPTY POINT, SRID 4326",
			"SRID=4326;POINT EMPTY",
			0,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf8\x7f\x00\x00\x00\x00\x00\x00\xf8\x7f"),
				SRID:        4326,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: nil,
			},
		},
		{
			"SRID 4326 is hint; EWKT has 4004",
			"SRID=4004;POINT(1.0 1.0)",
			4326,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4004,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
			},
		},
		{
			"SRID 4326 is hint; SRID is negative",
			"SRID=-1;POINT(1.0 1.0)",
			4326,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
			},
		},
		{
			"SRID 4326 is hint; SRID is 0",
			"SRID=0;POINT(1.0 1.0)",
			4326,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
			},
		},
		{
			"Overwrite SRID 4004 with 4326",
			"SRID=4004;POINT(1.0 1.0)",
			4326,
			DefaultSRIDShouldOverwrite,
			geopb.SpatialObject{
				Type:        geopb.SpatialObjectType_GeometryType,
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				ShapeType:   geopb.ShapeType_Point,
				BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret, err := parseEWKT(geopb.SpatialObjectType_GeometryType, tc.wkt, tc.defaultSRID, tc.overwrite)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestParseGeometry(t *testing.T) {
	testCases := []struct {
		str         string
		expected    *Geometry
		expectedErr string
	}{
		{
			"0101000000000000000000F03F000000000000F03F",
			&Geometry{
				spatialObject: geopb.SpatialObject{
					Type:        geopb.SpatialObjectType_GeometryType,
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					ShapeType:   geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
				},
			},
			"",
		},
		{
			"0101000020E6100000000000000000F03F000000000000F03F",
			&Geometry{
				spatialObject: geopb.SpatialObject{
					Type:        geopb.SpatialObjectType_GeometryType,
					EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4326,
					ShapeType:   geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
				},
			},
			"",
		},
		{
			"0101000020FFFFFFFF000000000000f03f000000000000f03f",
			&Geometry{
				spatialObject: geopb.SpatialObject{
					Type:        geopb.SpatialObjectType_GeometryType,
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					ShapeType:   geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
				},
			},
			"",
		},
		{
			"POINT(1.0 1.0)",
			&Geometry{
				spatialObject: geopb.SpatialObject{
					Type:        geopb.SpatialObjectType_GeometryType,
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					ShapeType:   geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
				},
			},
			"",
		},
		{
			"SRID=4004;POINT(1.0 1.0)",
			&Geometry{
				spatialObject: geopb.SpatialObject{
					Type:        geopb.SpatialObjectType_GeometryType,
					EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4004,
					ShapeType:   geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
				},
			},
			"",
		},
		{
			"SRid=4004;POINT(1.0 1.0)",
			&Geometry{
				spatialObject: geopb.SpatialObject{
					Type:        geopb.SpatialObjectType_GeometryType,
					EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4004,
					ShapeType:   geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
				},
			},
			"",
		},
		{
			"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f",
			&Geometry{
				spatialObject: geopb.SpatialObject{
					Type:        geopb.SpatialObjectType_GeometryType,
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					ShapeType:   geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
				},
			},
			"",
		},
		{
			`{ "type": "Point", "coordinates": [1.0, 1.0] }`,
			&Geometry{
				spatialObject: geopb.SpatialObject{
					Type:        geopb.SpatialObjectType_GeometryType,
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					ShapeType:   geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 1, HiY: 1},
				},
			},
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
			"geo: parsing empty string to geo type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			g, err := ParseGeometry(tc.str)
			if len(tc.expectedErr) > 0 {
				require.Equal(t, tc.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, g)
				require.Equal(t, tc.expected.SRID(), g.spatialObject.SRID)
			}
		})
	}
}

func TestParseGeography(t *testing.T) {
	testCases := []struct {
		str         string
		expected    *Geography
		expectedErr string
	}{
		{
			// Even forcing an SRID to 0 using EWKB will make it 4326.
			"0101000000000000000000F03F000000000000F03F",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4326,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			"0101000020E6100000000000000000F03F000000000000F03F",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4326,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			"0101000020FFFFFFFF000000000000f03f000000000000f03f",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4326,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			"0101000020A40F0000000000000000F03F000000000000F03F",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4004,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			"POINT(1.0 1.0)",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4326,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			// Even forcing an SRID to 0 using WKT will make it 4326.
			"SRID=0;POINT(1.0 1.0)",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4326,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			"SRID=4004;POINT(1.0 1.0)",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4004,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4326,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			"\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f",
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4004,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
			"",
		},
		{
			`{ "type": "Point", "coordinates": [1.0, 1.0] }`,
			&Geography{
				spatialObject: geopb.SpatialObject{
					Type:      geopb.SpatialObjectType_GeographyType,
					EWKB:      []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:      4326,
					ShapeType: geopb.ShapeType_Point,
					BoundingBox: &geopb.BoundingBox{
						LoX: 0.017453292519943292,
						LoY: 0.017453292519943295,
						HiX: 0.017453292519943292,
						HiY: 0.017453292519943295,
					},
				},
			},
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
			"geo: parsing empty string to geo type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			g, err := ParseGeography(tc.str)
			if len(tc.expectedErr) > 0 {
				require.Equal(t, tc.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, g)
				require.Equal(t, tc.expected.SRID(), g.spatialObject.SRID)
			}
		})
	}
}
