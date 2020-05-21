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
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				Shape:       geopb.Shape_Point,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
			},
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret, err := parseWKB(tc.b, tc.defaultSRID)
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
				EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4004,
				Shape:       geopb.Shape_Point,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
			},
		},
		{
			"Overwrite SRID 4004 with 4326",
			[]byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
			4326,
			DefaultSRIDShouldOverwrite,
			geopb.SpatialObject{
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				Shape:       geopb.Shape_Point,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret, err := parseEWKB(tc.b, tc.defaultSRID, tc.overwrite)
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
			"SRID 4326 is hint; EWKT has 4004",
			"SRID=4004;POINT(1.0 1.0)",
			4326,
			DefaultSRIDIsHint,
			geopb.SpatialObject{
				EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4004,
				Shape:       geopb.Shape_Point,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
			},
		},
		{
			"Overwrite SRID 4004 with 4326",
			"SRID=4004;POINT(1.0 1.0)",
			4326,
			DefaultSRIDShouldOverwrite,
			geopb.SpatialObject{
				EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
				SRID:        4326,
				Shape:       geopb.Shape_Point,
				BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret, err := parseEWKT(tc.wkt, tc.defaultSRID, tc.overwrite)
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
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"0101000020E6100000000000000000F03F000000000000F03F",
			&Geometry{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4326,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"POINT(1.0 1.0)",
			&Geometry{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"SRID=4004;POINT(1.0 1.0)",
			&Geometry{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4004,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"SRid=4004;POINT(1.0 1.0)",
			&Geometry{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4004,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f",
			&Geometry{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			`{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [1.0, 1.0] }, "properties": { "name": "┳━┳ ヽ(ಠل͜ಠ)ﾉ" } }`,
			&Geometry{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        0,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
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
				require.Equal(t, tc.expected.SRID(), g.SpatialObject.SRID)
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
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4326,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"0101000020E6100000000000000000F03F000000000000F03F",
			&Geography{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4326,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"0101000020A40F0000000000000000F03F000000000000F03F",
			&Geography{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4004,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"POINT(1.0 1.0)",
			&Geography{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4326,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			// Even forcing an SRID to 0 using WKT will make it 4326.
			"SRID=0;POINT(1.0 1.0)",
			&Geography{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4326,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"SRID=4004;POINT(1.0 1.0)",
			&Geography{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4004,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f",
			&Geography{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4326,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			"\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f",
			&Geography{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xA4\x0F\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4004,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
				},
			},
			"",
		},
		{
			`{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [1.0, 1.0] }, "properties": { "name": "┳━┳ ヽ(ಠل͜ಠ)ﾉ" } }`,
			&Geography{
				SpatialObject: geopb.SpatialObject{
					EWKB:        []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"),
					SRID:        4326,
					Shape:       geopb.Shape_Point,
					BoundingBox: &geopb.BoundingBox{MinX: 1, MaxX: 1, MinY: 1, MaxY: 1},
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
				require.Equal(t, tc.expected.SRID(), g.SpatialObject.SRID)
			}
		})
	}
}
