// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/stretchr/testify/require"
)

func TestCollect(t *testing.T) {
	testCases := []struct {
		wkt1     string
		wkt2     string
		expected string
	}{
		{"POINT EMPTY", "POINT EMPTY", "MULTIPOINT (EMPTY, EMPTY)"},
		{"POINT (1 1)", "POINT EMPTY", "MULTIPOINT (1 1, EMPTY)"},
		{"POINT EMPTY", "POINT (1 1)", "MULTIPOINT (EMPTY, 1 1)"},
		{"POINT (1 1)", "POINT (1 1)", "MULTIPOINT (1 1, 1 1)"},
		{"POINT (1 1)", "POINT (2 2)", "MULTIPOINT (1 1, 2 2)"},
		{"MULTIPOINT EMPTY", "MULTIPOINT EMPTY", "GEOMETRYCOLLECTION (MULTIPOINT EMPTY, MULTIPOINT EMPTY)"},
		{
			"MULTIPOINT (1 1, 2 2)", "MULTIPOINT (3 3, 4 4)",
			"GEOMETRYCOLLECTION (MULTIPOINT (1 1, 2 2), MULTIPOINT (3 3, 4 4))",
		},
		{"LINESTRING EMPTY", "LINESTRING EMPTY", "MULTILINESTRING (EMPTY, EMPTY)"},
		{"LINESTRING (1 1, 2 2)", "LINESTRING (3 3, 4 4)", "MULTILINESTRING ((1 1, 2 2), (3 3, 4 4))"},
		{
			"MULTILINESTRING EMPTY", "MULTILINESTRING EMPTY",
			"GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING EMPTY)",
		},
		{
			"MULTILINESTRING ((1 1, 2 2), (3 3, 4 4))", "MULTILINESTRING ((5 5, 6 6), (7 7, 8 8))",
			"GEOMETRYCOLLECTION (MULTILINESTRING ((1 1, 2 2), (3 3, 4 4)), MULTILINESTRING ((5 5, 6 6), (7 7, 8 8)))",
		},
		{"POLYGON EMPTY", "POLYGON EMPTY", "MULTIPOLYGON (EMPTY, EMPTY)"},
		{
			"POLYGON ((1 2, 2 3, 3 4, 1 2))", "POLYGON ((4 5, 5 6, 6 7, 4 5))",
			"MULTIPOLYGON (((1 2, 2 3, 3 4, 1 2)), ((4 5, 5 6, 6 7, 4 5)))",
		},
		{
			"MULTIPOLYGON EMPTY", "MULTIPOLYGON EMPTY",
			"GEOMETRYCOLLECTION (MULTIPOLYGON EMPTY, MULTIPOLYGON EMPTY)",
		},
		{
			"MULTIPOLYGON (((1 2, 2 3, 3 4, 1 2)), ((2 3, 3 4, 4 5, 2 3)))",
			"MULTIPOLYGON (((3 4, 4 5, 5 6, 3 4)), ((4 5, 5 6, 6 7, 4 5)))",
			"GEOMETRYCOLLECTION (MULTIPOLYGON (((1 2, 2 3, 3 4, 1 2)), ((2 3, 3 4, 4 5, 2 3))), MULTIPOLYGON (((3 4, 4 5, 5 6, 3 4)), ((4 5, 5 6, 6 7, 4 5))))",
		},
		{"POINT (1 1)", "LINESTRING (2 2, 3 3)", "GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (2 2, 3 3))"},
		{"LINESTRING (1 1, 2 2)", "POLYGON ((1 2, 2 3, 3 4, 1 2))", "GEOMETRYCOLLECTION (LINESTRING (1 1, 2 2), POLYGON ((1 2, 2 3, 3 4, 1 2)))"},
		{
			"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY",
			"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY, GEOMETRYCOLLECTION EMPTY)",
		},
		{
			"GEOMETRYCOLLECTION (POINT (1 1))", "GEOMETRYCOLLECTION (POINT (2 2))",
			"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 1)), GEOMETRYCOLLECTION (POINT (2 2)))",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v %v", tc.wkt1, tc.wkt2), func(t *testing.T) {
			srid := geopb.SRID(4000)
			g1, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt1), srid, true)
			require.NoError(t, err)
			g2, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt2), srid, true)
			require.NoError(t, err)

			result, err := Collect(g1, g2)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(result.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, result.SRID())
		})
	}
}

func TestCollectionExtract(t *testing.T) {
	mixedWithDupes := `GEOMETRYCOLLECTION(
		POINT (1 1),
		MULTIPOINT (2 2, 3 3),
		LINESTRING (1 1, 2 2),
		MULTILINESTRING ((3 3, 4 4), (5 5, 6 6)),
		POLYGON ((1 2, 3 4, 5 6, 1 2)),
		GEOMETRYCOLLECTION(
			POINT (3 3),
			MULTIPOINT (4 4, 5 5),
			LINESTRING (3 3, 4 4),
			MULTILINESTRING ((7 7, 8 8), (9 9, 0 0)),
			MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((3 4, 4 5, 5 6, 3 4)))
		)
	)`

	testCases := []struct {
		wkt       string
		shapeType geopb.ShapeType
		expected  string
	}{
		{"POINT EMPTY", geopb.ShapeType_Point, "POINT EMPTY"},
		{"POINT EMPTY", geopb.ShapeType_LineString, "LINESTRING EMPTY"},
		{"POINT EMPTY", geopb.ShapeType_Polygon, "POLYGON EMPTY"},
		{"POINT (1 2)", geopb.ShapeType_Point, "POINT (1 2)"},
		{"POINT (1 2)", geopb.ShapeType_LineString, "LINESTRING EMPTY"},
		{"POINT (1 2)", geopb.ShapeType_Polygon, "POLYGON EMPTY"},
		{"LINESTRING EMPTY", geopb.ShapeType_Point, "POINT EMPTY"},
		{"LINESTRING EMPTY", geopb.ShapeType_LineString, "LINESTRING EMPTY"},
		{"LINESTRING EMPTY", geopb.ShapeType_Polygon, "POLYGON EMPTY"},
		{"LINESTRING (1 2, 3 4)", geopb.ShapeType_Point, "POINT EMPTY"},
		{"LINESTRING (1 2, 3 4)", geopb.ShapeType_LineString, "LINESTRING (1 2, 3 4)"},
		{"LINESTRING (1 2, 3 4)", geopb.ShapeType_Polygon, "POLYGON EMPTY"},
		{"POLYGON EMPTY", geopb.ShapeType_Point, "POINT EMPTY"},
		{"POLYGON EMPTY", geopb.ShapeType_LineString, "LINESTRING EMPTY"},
		{"POLYGON EMPTY", geopb.ShapeType_Polygon, "POLYGON EMPTY"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", geopb.ShapeType_Point, "POINT EMPTY"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", geopb.ShapeType_LineString, "LINESTRING EMPTY"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", geopb.ShapeType_Polygon, "POLYGON ((1 2, 3 4, 5 6, 1 2))"},
		{"MULTIPOINT EMPTY", geopb.ShapeType_Point, "MULTIPOINT EMPTY"},
		{"MULTIPOINT EMPTY", geopb.ShapeType_LineString, "MULTILINESTRING EMPTY"},
		{"MULTIPOINT EMPTY", geopb.ShapeType_Polygon, "MULTIPOLYGON EMPTY"},
		{"MULTIPOINT (1 2, 3 4)", geopb.ShapeType_Point, "MULTIPOINT (1 2, 3 4)"},
		{"MULTIPOINT (1 2, 3 4)", geopb.ShapeType_LineString, "MULTILINESTRING EMPTY"},
		{"MULTIPOINT (1 2, 3 4)", geopb.ShapeType_Polygon, "MULTIPOLYGON EMPTY"},
		{"MULTILINESTRING EMPTY", geopb.ShapeType_Point, "MULTIPOINT EMPTY"},
		{"MULTILINESTRING EMPTY", geopb.ShapeType_LineString, "MULTILINESTRING EMPTY"},
		{"MULTILINESTRING EMPTY", geopb.ShapeType_Polygon, "MULTIPOLYGON EMPTY"},
		{"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))", geopb.ShapeType_Point, "MULTIPOINT EMPTY"},
		{"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))", geopb.ShapeType_LineString, "MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))"},
		{"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))", geopb.ShapeType_Polygon, "MULTIPOLYGON EMPTY"},
		{"MULTIPOLYGON EMPTY", geopb.ShapeType_Point, "MULTIPOINT EMPTY"},
		{"MULTIPOLYGON EMPTY", geopb.ShapeType_LineString, "MULTILINESTRING EMPTY"},
		{"MULTIPOLYGON EMPTY", geopb.ShapeType_Polygon, "MULTIPOLYGON EMPTY"},
		{"MULTIPOLYGON (((0 1, 2 3, 4 5, 0 1)), ((5 6, 7 8, 9 0, 5 6)))", geopb.ShapeType_Point, "MULTIPOINT EMPTY"},
		{"MULTIPOLYGON (((0 1, 2 3, 4 5, 0 1)), ((5 6, 7 8, 9 0, 5 6)))", geopb.ShapeType_LineString, "MULTILINESTRING EMPTY"},
		{"MULTIPOLYGON (((0 1, 2 3, 4 5, 0 1)), ((5 6, 7 8, 9 0, 5 6)))", geopb.ShapeType_Polygon, "MULTIPOLYGON (((0 1, 2 3, 4 5, 0 1)), ((5 6, 7 8, 9 0, 5 6)))"},
		{"GEOMETRYCOLLECTION EMPTY", geopb.ShapeType_Point, "MULTIPOINT EMPTY"},
		{"GEOMETRYCOLLECTION EMPTY", geopb.ShapeType_LineString, "MULTILINESTRING EMPTY"},
		{"GEOMETRYCOLLECTION EMPTY", geopb.ShapeType_Polygon, "MULTIPOLYGON EMPTY"},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)", geopb.ShapeType_Point, "MULTIPOINT EMPTY"},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)", geopb.ShapeType_LineString, "MULTILINESTRING EMPTY"},
		{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)", geopb.ShapeType_Polygon, "MULTIPOLYGON EMPTY"},
		{mixedWithDupes, geopb.ShapeType_Point, "MULTIPOINT (1 1, 2 2, 3 3, 3 3, 4 4, 5 5)"},
		{mixedWithDupes, geopb.ShapeType_LineString, "MULTILINESTRING ((1 1, 2 2), (3 3, 4 4), (5 5, 6 6), (3 3, 4 4), (7 7, 8 8), (9 9, 0 0))"},
		{mixedWithDupes, geopb.ShapeType_Polygon, "MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((1 2, 3 4, 5 6, 1 2)), ((3 4, 4 5, 5 6, 3 4)))"},
	}
	errorTestCases := []struct {
		shapeType geopb.ShapeType
	}{
		{geopb.ShapeType_Unset},
		{geopb.ShapeType_MultiPoint},
		{geopb.ShapeType_MultiLineString},
		{geopb.ShapeType_MultiPolygon},
		{geopb.ShapeType_Geometry},
		{geopb.ShapeType_GeometryCollection},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			srid := geopb.SRID(4000)
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), srid, true)
			require.NoError(t, err)

			multi, err := CollectionExtract(g, tc.shapeType)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(multi.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, multi.SRID())
		})
	}

	t.Run("errors on wrong shape type", func(t *testing.T) {
		for _, tc := range errorTestCases {
			t.Run(tc.shapeType.String(), func(t *testing.T) {
				g, err := geo.ParseGeometry("POINT EMPTY")
				require.NoError(t, err)
				_, err = CollectionExtract(g, tc.shapeType)
				require.Error(t, err)
			})
		}
	})
}

func TestCollectionHomogenize(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT EMPTY", "POINT EMPTY"},
		{"POINT (1 2)", "POINT (1 2)"},
		{"MULTIPOINT EMPTY", "MULTIPOINT EMPTY"},
		{"MULTIPOINT (1 2)", "POINT (1 2)"},
		{"MULTIPOINT (1 2, 3 4)", "MULTIPOINT (1 2, 3 4)"},
		{"LINESTRING EMPTY", "LINESTRING EMPTY"},
		{"LINESTRING (1 2, 3 4)", "LINESTRING (1 2, 3 4)"},
		{"MULTILINESTRING EMPTY", "MULTILINESTRING EMPTY"},
		{"MULTILINESTRING ((1 2, 3 4))", "LINESTRING (1 2, 3 4)"},
		{"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))", "MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))"},
		{"POLYGON EMPTY", "POLYGON EMPTY"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", "POLYGON ((1 2, 3 4, 5 6, 1 2))"},
		{"MULTIPOLYGON EMPTY", "MULTIPOLYGON EMPTY"},
		{"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)))", "POLYGON ((1 2, 3 4, 5 6, 1 2))"},
		{"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((7 8, 9 0, 1 2, 7 8)))", "MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((7 8, 9 0, 1 2, 7 8)))"},
		{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY"},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)", "GEOMETRYCOLLECTION EMPTY"},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(MULTIPOINT (1 1)))", "POINT (1 1)"},
		{
			`GEOMETRYCOLLECTION (
				LINESTRING(1 1, 2 2),
				POINT(1 1),
				GEOMETRYCOLLECTION(
					MULTIPOINT(2 2, 3 3),
					GEOMETRYCOLLECTION EMPTY,
					POINT(1 1),
					MULTIPOLYGON(((1 2, 2 3, 3 4, 1 2))),
					LINESTRING(3 3, 4 4),
					GEOMETRYCOLLECTION(
						POINT(4 4),
						POINT(5 5)
					)
				),
				MULTIPOINT EMPTY
			)`,
			"GEOMETRYCOLLECTION (MULTIPOINT (1 1, 2 2, 3 3, 1 1, 4 4, 5 5), MULTILINESTRING ((1 1, 2 2), (3 3, 4 4)), POLYGON ((1 2, 2 3, 3 4, 1 2)))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			srid := geopb.SRID(4000)
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), srid, true)
			require.NoError(t, err)

			result, err := CollectionHomogenize(g)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(result.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, result.SRID())
		})
	}
}

func TestForceCollection(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT EMPTY", "GEOMETRYCOLLECTION (POINT EMPTY)"},
		{"POINT (1 2)", "GEOMETRYCOLLECTION (POINT (1 2))"},
		{"MULTIPOINT EMPTY", "GEOMETRYCOLLECTION EMPTY"},
		{"MULTIPOINT (1 2)", "GEOMETRYCOLLECTION (POINT (1 2))"},
		{"MULTIPOINT (1 2, 3 4)", "GEOMETRYCOLLECTION (POINT (1 2), POINT (3 4))"},
		{"LINESTRING EMPTY", "GEOMETRYCOLLECTION (LINESTRING EMPTY)"},
		{"LINESTRING (1 2, 3 4)", "GEOMETRYCOLLECTION (LINESTRING (1 2, 3 4))"},
		{"MULTILINESTRING EMPTY", "GEOMETRYCOLLECTION EMPTY"},
		{"MULTILINESTRING ((1 2, 3 4))", "GEOMETRYCOLLECTION (LINESTRING (1 2, 3 4))"},
		{
			"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))",
			"GEOMETRYCOLLECTION (LINESTRING (1 2, 3 4), LINESTRING (5 6, 7 8))",
		},
		{"POLYGON EMPTY", "GEOMETRYCOLLECTION (POLYGON EMPTY)"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", "GEOMETRYCOLLECTION (POLYGON ((1 2, 3 4, 5 6, 1 2)))"},
		{"MULTIPOLYGON EMPTY", "GEOMETRYCOLLECTION EMPTY"},
		{"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)))", "GEOMETRYCOLLECTION (POLYGON ((1 2, 3 4, 5 6, 1 2)))"},
		{
			"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((7 8, 9 0, 1 2, 7 8)))",
			"GEOMETRYCOLLECTION (POLYGON ((1 2, 3 4, 5 6, 1 2)), POLYGON ((7 8, 9 0, 1 2, 7 8)))",
		},
		{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY"},
		{"GEOMETRYCOLLECTION (MULTIPOINT (1 1, 2 2))", "GEOMETRYCOLLECTION (MULTIPOINT (1 1, 2 2))"},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)", "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)"},
		{
			"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(MULTIPOINT (1 1)))",
			"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (MULTIPOINT (1 1)))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			srid := geopb.SRID(4000)
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), srid, true)
			require.NoError(t, err)

			result, err := ForceCollection(g)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(result.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, result.SRID())
		})
	}
}

func TestMulti(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT EMPTY", "MULTIPOINT EMPTY"},
		{"POINT (1 2)", "MULTIPOINT (1 2)"},
		{"MULTIPOINT EMPTY", "MULTIPOINT EMPTY"},
		{"MULTIPOINT (1 2, 3 4)", "MULTIPOINT (1 2, 3 4)"},
		{"LINESTRING EMPTY", "MULTILINESTRING EMPTY"},
		{"LINESTRING (1 2, 3 4, 5 6)", "MULTILINESTRING ((1 2, 3 4, 5 6))"},
		{"MULTILINESTRING EMPTY", "MULTILINESTRING EMPTY"},
		{"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))", "MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))"},
		{"POLYGON EMPTY", "MULTIPOLYGON EMPTY"},
		{"POLYGON ((1 2, 3 4, 5 6, 1 2))", "MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)))"},
		{"MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((0 0, 1 1, 2 2, 0 0)))", "MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((0 0, 1 1, 2 2, 0 0)))"},
		{"GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY"},
		{"GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (1 2, 3 4))", "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (1 2, 3 4))"},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (1 2, 3 4)))", "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (1 2, 3 4)))"},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			srid := geopb.SRID(4000)
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(tc.wkt), srid, true)
			require.NoError(t, err)

			multi, err := Multi(g)
			require.NoError(t, err)
			wkt, err := geo.SpatialObjectToWKT(multi.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
			require.EqualValues(t, srid, multi.SRID())
		})
	}
}
