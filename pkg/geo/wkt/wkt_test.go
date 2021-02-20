// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package wkt

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestUnmarshal(t *testing.T) {
	testCases := []struct {
		desc        string
		equivInputs []string
		expected    geom.T
	}{
		// POINT tests
		{
			desc:        "parse 2D point",
			equivInputs: []string{"POINT(0 1)", "POINT (0 1)", "point(0 1)", "point ( 0 1 )"},
			expected:    geom.NewPointFlat(geom.XY, []float64{0, 1}),
		},
		{
			desc:        "parse 2D point with scientific notation",
			equivInputs: []string{"POINT(1e-2 2e3)", "POINT(0.1e-1 2e3)", "POINT(0.01e-0 2e+3)", "POINT(0.01 2000)"},
			expected:    geom.NewPointFlat(geom.XY, []float64{1e-2, 2e3}),
		},
		{
			desc:        "parse 2D+M point",
			equivInputs: []string{"POINT M (-2 0 0.5)", "POINTM(-2 0 0.5)", "POINTM(-2 0 .5)"},
			expected:    geom.NewPointFlat(geom.XYM, []float64{-2, 0, 0.5}),
		},
		{
			desc:        "parse 3D point",
			equivInputs: []string{"POINT Z (2 3 4)", "POINTZ(2 3 4)", "POINT(2 3 4)"},
			expected:    geom.NewPointFlat(geom.XYZ, []float64{2, 3, 4}),
		},
		{
			desc:        "parse 4D point",
			equivInputs: []string{"POINT ZM (0 5 -10 15)", "POINTZM (0 5 -10 15)", "POINT(0 5 -10 15)"},
			expected:    geom.NewPointFlat(geom.XYZM, []float64{0, 5, -10, 15}),
		},
		{
			desc:        "parse empty 2D point",
			equivInputs: []string{"POINT EMPTY"},
			expected:    geom.NewPointEmpty(geom.XY),
		},
		{
			desc:        "parse empty 2D+M point",
			equivInputs: []string{"POINT M EMPTY", "POINTM EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYM),
		},
		{
			desc:        "parse empty 3D point",
			equivInputs: []string{"POINT Z EMPTY", "POINTZ EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYZ),
		},
		{
			desc:        "parse empty 4D point",
			equivInputs: []string{"POINT ZM EMPTY", "POINTZM EMPTY"},
			expected:    geom.NewPointEmpty(geom.XYZM),
		},
		// LINESTRING tests
		{
			desc:        "parse 2D linestring",
			equivInputs: []string{"LINESTRING(0 0, 1 1, 3 4)", "LINESTRING (0 0, 1 1, 3 4)", "linestring ( 0 0, 1 1, 3 4 )"},
			expected:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 3, 4}),
		},
		{
			desc:        "parse 2D+M linestring",
			equivInputs: []string{"LINESTRING M(0 0 200, 0.1 -1 -20)", "LINESTRINGM(0 0 200, .1 -1 -20)"},
			expected:    geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 200, 0.1, -1, -20}),
		},
		{
			desc:        "parse 3D linestring",
			equivInputs: []string{"LINESTRING(0 -1 1, 7 -1 -9)", "LINESTRING Z(0 -1 1, 7 -1 -9)", "LINESTRINGZ(0 -1 1, 7 -1 -9)"},
			expected:    geom.NewLineStringFlat(geom.XYZ, []float64{0, -1, 1, 7, -1, -9}),
		},
		{
			desc:        "parse 4D linestring",
			equivInputs: []string{"LINESTRING(0 0 0 0, 1 1 1 1)", "LINESTRING ZM (0 0 0 0, 1 1 1 1)", "LINESTRINGZM (0 0 0 0, 1 1 1 1)"},
			expected:    geom.NewLineStringFlat(geom.XYZM, []float64{0, 0, 0, 0, 1, 1, 1, 1}),
		},
		{
			desc:        "parse empty 2D linestring",
			equivInputs: []string{"LINESTRING EMPTY"},
			expected:    geom.NewLineString(geom.XY),
		},
		{
			desc:        "parse empty 2D+M linestring",
			equivInputs: []string{"LINESTRING M EMPTY", "LINESTRINGM EMPTY"},
			expected:    geom.NewLineString(geom.XYM),
		},
		{
			desc:        "parse empty 3D linestring",
			equivInputs: []string{"LINESTRING Z EMPTY", "LINESTRINGZ EMPTY"},
			expected:    geom.NewLineString(geom.XYZ),
		},
		{
			desc:        "parse empty 4D linestring",
			equivInputs: []string{"LINESTRING ZM EMPTY", "LINESTRINGZM EMPTY"},
			expected:    geom.NewLineString(geom.XYZM),
		},
		// POLYGON tests
		{
			desc:        "parse 2D polygon",
			equivInputs: []string{"POLYGON((0 0, 1 -1, 2 0, 0 0))", "POLYGON ((0 0, 1 -1, 2 0, 0 0))"},
			expected:    geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, -1, 2, 0, 0, 0}, []int{8}),
		},
		{
			desc:        "parse 2D polygon with hole",
			equivInputs: []string{"POLYGON((0 0, 0 100, 100 100, 100 0, 0 0),(10 10, 11 11, 12 10, 10 10))"},
			expected: geom.NewPolygonFlat(geom.XY,
				[]float64{0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 10, 10, 11, 11, 12, 10, 10, 10}, []int{10, 18}),
		},
		{
			desc:        "parse 2D polygon with two holes",
			equivInputs: []string{"POLYGON((0 0, 0 100, 100 100, 100 0, 0 0),(10 10, 11 11, 12 10, 10 10), (2 2, 4 4, 5 1, 2 2))"},
			expected: geom.NewPolygonFlat(geom.XY,
				[]float64{0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 10, 10, 11, 11, 12, 10, 10, 10, 2, 2, 4, 4, 5, 1, 2, 2}, []int{10, 18, 26}),
		},
		{
			desc:        "parse 2D+M polygon",
			equivInputs: []string{"POLYGONM((0 0 7, 1 -1 -50, 2 0 0, 0 0 7))", "POLYGON M ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7))"},
			expected:    geom.NewPolygonFlat(geom.XYM, []float64{0, 0, 7, 1, -1, -50, 2, 0, 0, 0, 0, 7}, []int{12}),
		},
		{
			desc:        "parse 3D polygon",
			equivInputs: []string{"POLYGON((0 0 7, 1 -1 -50, 2 0 0, 0 0 7))", "POLYGON Z ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7))"},
			expected:    geom.NewPolygonFlat(geom.XYZ, []float64{0, 0, 7, 1, -1, -50, 2, 0, 0, 0, 0, 7}, []int{12}),
		},
		{
			desc:        "parse 4D polygon",
			equivInputs: []string{"POLYGON((0 0 12 7, 1 -1 12 -50, 2 0 12 0, 0 0 12 7))", "POLYGON ZM ((0 0 12 7, 1 -1 12 -50, 2 0 12 0, 0 0 12 7))"},
			expected:    geom.NewPolygonFlat(geom.XYZM, []float64{0, 0, 12, 7, 1, -1, 12, -50, 2, 0, 12, 0, 0, 0, 12, 7}, []int{16}),
		},
		{
			desc:        "parse empty 2D polygon",
			equivInputs: []string{"POLYGON EMPTY"},
			expected:    geom.NewPolygon(geom.XY),
		},
		{
			desc:        "parse empty 2D+M polygon",
			equivInputs: []string{"POLYGON M EMPTY", "POLYGONM EMPTY"},
			expected:    geom.NewPolygon(geom.XYM),
		},
		{
			desc:        "parse empty 3D polygon",
			equivInputs: []string{"POLYGON Z EMPTY", "POLYGONZ EMPTY"},
			expected:    geom.NewPolygon(geom.XYZ),
		},
		{
			desc:        "parse empty 4D polygon",
			equivInputs: []string{"POLYGON ZM EMPTY", "POLYGONZM EMPTY"},
			expected:    geom.NewPolygon(geom.XYZM),
		},
		// MULTIPOINT tests
		{
			desc:        "parse 2D multipoint",
			equivInputs: []string{"MULTIPOINT(0 0, 1 1, 2 2)", "MULTIPOINT((0 0), 1 1, (2 2))", "MULTIPOINT (0 0, 1 1, 2 2)"},
			expected:    geom.NewMultiPointFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
		},
		{
			desc:        "parse 2D+M multipoint",
			equivInputs: []string{"MULTIPOINTM((-1 5 -16), .23 7 0)", "MULTIPOINT M (-1 5 -16, 0.23 7.0 0)"},
			expected:    geom.NewMultiPointFlat(geom.XYM, []float64{-1, 5, -16, 0.23, 7, 0}),
		},
		{
			desc:        "parse 3D multipoint",
			equivInputs: []string{"MULTIPOINT(2 1 3)", "MULTIPOINTZ(2 1 3)", "MULTIPOINT Z ((2 1 3))"},
			expected:    geom.NewMultiPointFlat(geom.XYZ, []float64{2, 1, 3}),
		},
		{
			desc:        "parse 4D multipoint",
			equivInputs: []string{"MULTIPOINT(2 -8 17 45, (0 0 0 0))", "MULTIPOINTZM((2 -8 17 45), (0 0 0 0))", "MULTIPOINT ZM (2 -8 17 45, 0 0 0 0)"},
			expected:    geom.NewMultiPointFlat(geom.XYZM, []float64{2, -8, 17, 45, 0, 0, 0, 0}),
		},
		{
			desc:        "parse 2D multipoint with EMPTY points",
			equivInputs: []string{"MULTIPOINT(EMPTY, 2 3, EMPTY)", "MULTIPOINT (EMPTY, (2 3), EMPTY)"},
			expected:    geom.NewMultiPointFlat(geom.XY, []float64{2, 3}, geom.NewMultiPointFlatOptionWithEnds([]int{0, 2, 2})),
		},
		{
			desc:        "parse 2D+M multipoint with EMPTY points",
			equivInputs: []string{"MULTIPOINTM(2 3 1, EMPTY)", "MULTIPOINT M ((2 3 1), EMPTY)"},
			expected:    geom.NewMultiPointFlat(geom.XYM, []float64{2, 3, 1}, geom.NewMultiPointFlatOptionWithEnds([]int{3, 3})),
		},
		{
			desc:        "parse 3D multipoint with EMPTY points",
			equivInputs: []string{"MULTIPOINTZ (EMPTY, EMPTY)", "MULTIPOINT Z (EMPTY, EMPTY)"},
			expected:    geom.NewMultiPointFlat(geom.XYZ, []float64(nil), geom.NewMultiPointFlatOptionWithEnds([]int{0, 0})),
		},
		{
			desc:        "parse 4D multipoint with EMPTY points",
			equivInputs: []string{"MULTIPOINTZM(EMPTY, 1 -1 1 -1)", "MULTIPOINT ZM (EMPTY, (1 -1 1 -1))"},
			expected:    geom.NewMultiPointFlat(geom.XYZM, []float64{1, -1, 1, -1}, geom.NewMultiPointFlatOptionWithEnds([]int{0, 4})),
		},
		{
			desc:        "parse empty 2D multipoint",
			equivInputs: []string{"MULTIPOINT EMPTY"},
			expected:    geom.NewMultiPoint(geom.XY),
		},
		{
			desc:        "parse empty 2D+M multipoint",
			equivInputs: []string{"MULTIPOINT M EMPTY", "MULTIPOINTM EMPTY"},
			expected:    geom.NewMultiPoint(geom.XYM),
		},
		{
			desc:        "parse empty 3D multipoint",
			equivInputs: []string{"MULTIPOINT Z EMPTY", "MULTIPOINTZ EMPTY"},
			expected:    geom.NewMultiPoint(geom.XYZ),
		},
		{
			desc:        "parse empty 4D multipoint",
			equivInputs: []string{"MULTIPOINT ZM EMPTY", "MULTIPOINTZM EMPTY"},
			expected:    geom.NewMultiPoint(geom.XYZM),
		},
		// MULTILINESTRING tests
		{
			desc:        "parse 2D multilinestring",
			equivInputs: []string{"MULTILINESTRING((0 0, 1 1), EMPTY)", "MULTILINESTRING (( 0 0, 1 1 ), EMPTY )"},
			expected:    geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1}, []int{4, 4}),
		},
		{
			desc:        "parse 2D+M multilinestring",
			equivInputs: []string{"MULTILINESTRINGM((0 -1 -2, 2 5 7))", "multilinestring m ((0 -1 -2, 2 5 7))"},
			expected:    geom.NewMultiLineStringFlat(geom.XYM, []float64{0, -1, -2, 2, 5, 7}, []int{6}),
		},
		{
			desc:        "parse 3D multilinestring",
			equivInputs: []string{"MULTILINESTRING((0 -1 -2, 2 5 7))", "MULTILINESTRINGZ((0 -1 -2, 2 5 7))", "MULTILINESTRING Z ((0 -1 -2, 2 5 7))"},
			expected:    geom.NewMultiLineStringFlat(geom.XYZ, []float64{0, -1, -2, 2, 5, 7}, []int{6}),
		},
		{
			desc: "parse 4D multilinestring",
			equivInputs: []string{"MULTILINESTRING((0 0 0 0, 1 1 1 1), (-2 -3 -4 -5, 0.5 -0.75 1 -1.25, 0 1 5 7))",
				"MULTILINESTRING ZM ((0 0 0 0, 1 1 1 1), (-2 -3 -4 -5, 0.5 -0.75 1 -1.25, 0 1 5 7))",
				"multilinestringzm((0 0 0 0, 1 1 1 1), (-2 -3 -4 -5, 0.5 -0.75 1 -1.25, 0 1 5 7))"},
			expected: geom.NewMultiLineStringFlat(geom.XYZM,
				[]float64{0, 0, 0, 0, 1, 1, 1, 1, -2, -3, -4, -5, 0.5, -0.75, 1, -1.25, 0, 1, 5, 7}, []int{8, 20}),
		},
		{
			desc:        "parse 2D+M multilinestring with EMPTY linestrings",
			equivInputs: []string{"MultiLineString M ((1 -1 2, 3 -0.4 7), EMPTY, (0 0 0, -2 -4 -89))", "MULTILINESTRINGM ((1 -1 2, 3 -0.4 7), EMPTY, (0 0 0, -2 -4 -89))"},
			expected:    geom.NewMultiLineStringFlat(geom.XYM, []float64{1, -1, 2, 3, -0.4, 7, 0, 0, 0, -2, -4, -89}, []int{6, 6, 12}),
		},
		{
			desc:        "parse 3D multilinestring with EMPTY linestrings",
			equivInputs: []string{"MULTILINESTRINGZ(EMPTY, EMPTY, (1 1 1, 2 2 2, 3 3 3))", "multilinestring z (EMPTY, empty, (1 1 1, 2 2 2, 3 3 3))"},
			expected:    geom.NewMultiLineStringFlat(geom.XYZ, []float64{1, 1, 1, 2, 2, 2, 3, 3, 3}, []int{0, 0, 9}),
		},
		{
			desc:        "parse 4D multilinestring with EMPTY linestrings",
			equivInputs: []string{"MULTILINESTRINGZM(EMPTY)", "MuLTIliNeStRiNg zM (EMPTY)"},
			expected:    geom.NewMultiLineStringFlat(geom.XYZM, []float64(nil), []int{0}),
		},
		{
			desc:        "parse empty 2D multilinestring",
			equivInputs: []string{"MULTILINESTRING EMPTY"},
			expected:    geom.NewMultiLineString(geom.XY),
		},
		{
			desc:        "parse empty 2D+M multilinestring",
			equivInputs: []string{"MULTILINESTRING M EMPTY", "MULTILINESTRINGM EMPTY"},
			expected:    geom.NewMultiLineString(geom.XYM),
		},
		{
			desc:        "parse empty 3D multilinestring",
			equivInputs: []string{"MULTILINESTRING Z EMPTY", "MULTILINESTRINGZ EMPTY"},
			expected:    geom.NewMultiLineString(geom.XYZ),
		},
		{
			desc:        "parse empty 4D multilinestring",
			equivInputs: []string{"MULTILINESTRING ZM EMPTY", "MULTILINESTRINGZM EMPTY"},
			expected:    geom.NewMultiLineString(geom.XYZM),
		},
		// MULTIPOLYGON tests
		{
			desc:        "parse 2D multipolygon",
			equivInputs: []string{"MULTIPOLYGON(((1 0, 2 5, -2 5, 1 0)))"},
			expected:    geom.NewMultiPolygonFlat(geom.XY, []float64{1, 0, 2, 5, -2, 5, 1, 0}, [][]int{{8}}),
		},
		{
			desc:        "parse 2D multipolygon with EMPTY at rear",
			equivInputs: []string{"MULTIPOLYGON(((1 0, 2 5, -2 5, 1 0)), EMPTY)"},
			expected:    geom.NewMultiPolygonFlat(geom.XY, []float64{1, 0, 2, 5, -2, 5, 1, 0}, [][]int{{8}, []int(nil)}),
		},
		{
			desc:        "parse 2D multipolygon with EMPTY at front",
			equivInputs: []string{"MULTIPOLYGON(EMPTY, ((1 0, 2 5, -2 5, 1 0)))"},
			expected:    geom.NewMultiPolygonFlat(geom.XY, []float64{1, 0, 2, 5, -2, 5, 1, 0}, [][]int{[]int(nil), {8}}),
		},
		{
			desc:        "parse 2D multipolygon with multiple polygons",
			equivInputs: []string{"MULTIPOLYGON(((1 0, 2 5, -2 5, 1 0)), EMPTY, ((-1 -1, 2 7, 3 0, -1 -1)))"},
			expected:    geom.NewMultiPolygonFlat(geom.XY, []float64{1, 0, 2, 5, -2, 5, 1, 0, -1, -1, 2, 7, 3, 0, -1, -1}, [][]int{{8}, []int(nil), {16}}),
		},
		{
			desc:        "parse 2D+M multipolygon",
			equivInputs: []string{"MULTIPOLYGON M (((0 0 0, 1 1 1, 2 3 1, 0 0 0)))"},
			expected:    geom.NewMultiPolygonFlat(geom.XYM, []float64{0, 0, 0, 1, 1, 1, 2, 3, 1, 0, 0, 0}, [][]int{{12}}),
		},
		{
			desc:        "parse 3D multipolygon",
			equivInputs: []string{"MULTIPOLYGON(((0 0 0, 1 1 1, 2 3 1, 0 0 0)))", "MULTIPOLYGON Z (((0 0 0, 1 1 1, 2 3 1, 0 0 0)))"},
			expected:    geom.NewMultiPolygonFlat(geom.XYZ, []float64{0, 0, 0, 1, 1, 1, 2, 3, 1, 0, 0, 0}, [][]int{{12}}),
		},
		{
			desc:        "parse 4D multipolygon",
			equivInputs: []string{"MULTIPOLYGON(((0 0 0 0, 1 1 1 -1, 2 3 1 -2, 0 0 0 0)))", "MULTIPOLYGON ZM (((0 0 0 0, 1 1 1 -1, 2 3 1 -2, 0 0 0 0)))"},
			expected:    geom.NewMultiPolygonFlat(geom.XYZM, []float64{0, 0, 0, 0, 1, 1, 1, -1, 2, 3, 1, -2, 0, 0, 0, 0}, [][]int{{16}}),
		},
		{
			desc:        "parse empty 2D multipolygon",
			equivInputs: []string{"MULTIPOLYGON EMPTY"},
			expected:    geom.NewMultiPolygon(geom.XY),
		},
		{
			desc:        "parse empty 2D+M multipolygon",
			equivInputs: []string{"MULTIPOLYGON M EMPTY", "MULTIPOLYGONM EMPTY"},
			expected:    geom.NewMultiPolygon(geom.XYM),
		},
		{
			desc:        "parse empty 3D multipolygon",
			equivInputs: []string{"MULTIPOLYGON Z EMPTY", "MULTIPOLYGONZ EMPTY"},
			expected:    geom.NewMultiPolygon(geom.XYZ),
		},
		{
			desc:        "parse empty 4D multipolygon",
			equivInputs: []string{"MULTIPOLYGON ZM EMPTY", "MULTIPOLYGONZM EMPTY"},
			expected:    geom.NewMultiPolygon(geom.XYZM),
		},
		// GEOMETRYCOLLECTION tests
		{
			desc:        "parse 2D geometrycollection with a single point",
			equivInputs: []string{"GEOMETRYCOLLECTION(POINT(0 0))"},
			expected:    geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{0, 0})),
		},
		{
			desc:        "parse 2D+M base type geometrycollection",
			equivInputs: []string{"GEOMETRYCOLLECTION M (POINT M (0 0 0))", "GEOMETRYCOLLECTION(POINT M (0 0 0))"},
			expected:    geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XYM, []float64{0, 0, 0})),
		},
		{
			desc: "parse 2D+M geometrycollection with base type empty geometry",
			equivInputs: []string{
				"GEOMETRYCOLLECTION M (LINESTRING EMPTY)",
				"GEOMETRYCOLLECTION(LINESTRING M EMPTY)",
				"GEOMETRYCOLLECTION M (LINESTRING M EMPTY)",
			},
			expected: geom.NewGeometryCollection().MustPush(geom.NewLineString(geom.XYM)),
		},
		{
			desc:        "parse 3D geometrycollection with base type empty geometry",
			equivInputs: []string{"GEOMETRYCOLLECTION Z (LINESTRING EMPTY)", "GEOMETRYCOLLECTION Z (LINESTRING Z EMPTY)"},
			expected:    geom.NewGeometryCollection().MustPush(geom.NewLineString(geom.XYZ)),
		},
		{
			desc: "parse 2D+M geometrycollection with nested geometrycollection and empty geometry",
			equivInputs: []string{
				"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING M EMPTY), LINESTRING M EMPTY)",
				"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION M (LINESTRING EMPTY), LINESTRING M EMPTY)",
				"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION M (LINESTRING M EMPTY), LINESTRING M EMPTY)",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION(LINESTRING EMPTY), LINESTRING EMPTY)",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION M (LINESTRING EMPTY), LINESTRING EMPTY)",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION(LINESTRING M EMPTY), LINESTRING EMPTY)",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION(LINESTRING EMPTY), LINESTRING M EMPTY)",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION(LINESTRING M EMPTY), LINESTRING M EMPTY)",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION M (LINESTRING EMPTY), LINESTRING M EMPTY)",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION M (LINESTRING M EMPTY), LINESTRING M EMPTY)",
			},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewGeometryCollection().MustPush(geom.NewLineString(geom.XYM)),
				geom.NewLineString(geom.XYM),
			),
		},
		{
			desc: "parse 2D+M geometrycollection with empty geometrycollection",
			equivInputs: []string{
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION EMPTY)",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION M EMPTY)",
			},
			expected: geom.NewGeometryCollection().MustPush(geom.NewGeometryCollection()),
		},
		{
			desc: "parse 3D geometry collection with nested geometrycollection and empty geometry",
			equivInputs: []string{
				"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION Z (POLYGON Z EMPTY), POINT Z EMPTY)",
				"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON Z EMPTY), POINT Z EMPTY)",
				"GEOMETRYCOLLECTION Z (GEOMETRYCOLLECTION(POLYGON EMPTY), POINT EMPTY)",
				"GEOMETRYCOLLECTION Z (GEOMETRYCOLLECTION Z (POLYGON EMPTY), POINT EMPTY)",
				"GEOMETRYCOLLECTION Z (GEOMETRYCOLLECTION Z (POLYGON Z EMPTY), POINT Z EMPTY)",
			},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewGeometryCollection().MustPush(geom.NewPolygon(geom.XYZ)),
				geom.NewPointEmpty(geom.XYZ),
			),
		},
		{
			desc: "parse 2D geometrycollection",
			equivInputs: []string{`GEOMETRYCOLLECTION(
POINT(0 0),
LINESTRING(1 1, 0 0, 1 4),
POLYGON((0 0, 0 100, 100 100, 100 0, 0 0), (10 10, 11 11, 12 10, 10 10), (2 2, 4 4, 5 1, 2 2)),
MULTIPOINT((23 24), EMPTY),
MULTILINESTRING((1 1, 0 0, 1 4)),
MULTIPOLYGON(((0 0, 0 100, 100 100, 100 0, 0 0))),
GEOMETRYCOLLECTION EMPTY
)`},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{0, 0}),
				geom.NewLineStringFlat(geom.XY, []float64{1, 1, 0, 0, 1, 4}),
				geom.NewPolygonFlat(geom.XY,
					[]float64{0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 10, 10, 11, 11, 12, 10, 10, 10, 2, 2, 4, 4, 5, 1, 2, 2},
					[]int{10, 18, 26}),
				geom.NewMultiPointFlat(geom.XY, []float64{23, 24}, geom.NewMultiPointFlatOptionWithEnds([]int{2, 2})),
				geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 0, 0, 1, 4}, []int{6}),
				geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 0, 100, 100, 100, 100, 0, 0, 0}, [][]int{{10}}),
				geom.NewGeometryCollection(),
			),
		},
		{
			desc:        "parse 2D geometrycollection with nested geometrycollection",
			equivInputs: []string{"GEOMETRYCOLLECTION(POINT(0 0), GEOMETRYCOLLECTION(MULTIPOINT(EMPTY, 2 1)))"},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{0, 0}),
				geom.NewGeometryCollection().MustPush(
					geom.NewMultiPointFlat(geom.XY, []float64{2, 1}, geom.NewMultiPointFlatOptionWithEnds([]int{0, 2})),
				),
			),
		},
		{
			desc: "parse 2D+M geometrycollection",
			equivInputs: []string{
				`GEOMETRYCOLLECTION M (
POINT EMPTY,
POINT M (-2 0 0.5),
LINESTRING M (0 0 200, 0.1 -1 -20),
POLYGON M ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7)),
MULTIPOINT M (-1 5 -16, 0.23 7.0 0),
MULTILINESTRING M ((0 -1 -2, 2 5 7)),
MULTIPOLYGON M (((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
)`,
				`GEOMETRYCOLLECTION M (
POINT M EMPTY,
POINT M (-2 0 0.5),
LINESTRING M (0 0 200, 0.1 -1 -20),
POLYGON M ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7)),
MULTIPOINT M (-1 5 -16, 0.23 7.0 0),
MULTILINESTRING M ((0 -1 -2, 2 5 7)),
MULTIPOLYGON M (((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
)`,
				`GEOMETRYCOLLECTION(
POINT M EMPTY,
POINT M (-2 0 0.5),
LINESTRING M (0 0 200, 0.1 -1 -20),
POLYGON M ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7)),
MULTIPOINT M (-1 5 -16, 0.23 7.0 0),
MULTILINESTRING M ((0 -1 -2, 2 5 7)),
MULTIPOLYGON M (((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
)`,
			},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointEmpty(geom.XYM),
				geom.NewPointFlat(geom.XYM, []float64{-2, 0, 0.5}),
				geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 200, 0.1, -1, -20}),
				geom.NewPolygonFlat(geom.XYM, []float64{0, 0, 7, 1, -1, -50, 2, 0, 0, 0, 0, 7}, []int{12}),
				geom.NewMultiPointFlat(geom.XYM, []float64{-1, 5, -16, 0.23, 7, 0}),
				geom.NewMultiLineStringFlat(geom.XYM, []float64{0, -1, -2, 2, 5, 7}, []int{6}),
				geom.NewMultiPolygonFlat(geom.XYM, []float64{0, 0, 0, 1, 1, 1, 2, 3, 1, 0, 0, 0}, [][]int{{12}}),
			),
		},
		{
			desc: "parse 2D+M geometrycollection with nested geometrycollection",
			equivInputs: []string{
				"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION M (POINT EMPTY, LINESTRING M (0 0 0, 1 1 1)))",
				"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION M (POINT M EMPTY, LINESTRING M (0 0 0, 1 1 1)))",
				"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT M EMPTY, LINESTRING M (0 0 0, 1 1 1)))",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION(POINT M EMPTY, LINESTRING M (0 0 0, 1 1 1)))",
				"GEOMETRYCOLLECTION M (GEOMETRYCOLLECTION M (POINT M EMPTY, LINESTRING M (0 0 0, 1 1 1)))",
			},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewGeometryCollection().MustPush(
					geom.NewPointEmpty(geom.XYM),
					geom.NewLineStringFlat(geom.XYM, []float64{0, 0, 0, 1, 1, 1}),
				),
			),
		},
		{
			desc: "parse 3D geometrycollection",
			equivInputs: []string{`GEOMETRYCOLLECTION Z (
POINT Z (2 3 4),
LINESTRING Z (0 -1 1, 7 -1 -9),
POLYGON Z ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7)),
MULTIPOINT Z ((2 3 1), EMPTY),
MULTILINESTRING Z (EMPTY, EMPTY, (1 1 1, 2 2 2, 3 3 3)),
MULTIPOLYGON Z (((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
)`,
				`GEOMETRYCOLLECTION Z (
POINT(2 3 4),
LINESTRING(0 -1 1, 7 -1 -9),
POLYGON((0 0 7, 1 -1 -50, 2 0 0, 0 0 7)),
MULTIPOINT((2 3 1), EMPTY),
MULTILINESTRING(EMPTY, EMPTY, (1 1 1, 2 2 2, 3 3 3)),
MULTIPOLYGON(((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
)`,
				`GEOMETRYCOLLECTION(
POINT Z (2 3 4),
LINESTRING Z (0 -1 1, 7 -1 -9),
POLYGON Z ((0 0 7, 1 -1 -50, 2 0 0, 0 0 7)),
MULTIPOINT Z ((2 3 1), EMPTY),
MULTILINESTRING Z (EMPTY, EMPTY, (1 1 1, 2 2 2, 3 3 3)),
MULTIPOLYGON Z (((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
)`,
				`GEOMETRYCOLLECTION(
POINT(2 3 4),
LINESTRING(0 -1 1, 7 -1 -9),
POLYGON((0 0 7, 1 -1 -50, 2 0 0, 0 0 7)),
MULTIPOINT Z ((2 3 1), EMPTY),
MULTILINESTRING Z (EMPTY, EMPTY, (1 1 1, 2 2 2, 3 3 3)),
MULTIPOLYGON(((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
)`,
			},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XYZ, []float64{2, 3, 4}),
				geom.NewLineStringFlat(geom.XYZ, []float64{0, -1, 1, 7, -1, -9}),
				geom.NewPolygonFlat(geom.XYZ, []float64{0, 0, 7, 1, -1, -50, 2, 0, 0, 0, 0, 7}, []int{12}),
				geom.NewMultiPointFlat(geom.XYZ, []float64{2, 3, 1}, geom.NewMultiPointFlatOptionWithEnds([]int{3, 3})),
				geom.NewMultiLineStringFlat(geom.XYZ, []float64{1, 1, 1, 2, 2, 2, 3, 3, 3}, []int{0, 0, 9}),
				geom.NewMultiPolygonFlat(geom.XYZ, []float64{0, 0, 0, 1, 1, 1, 2, 3, 1, 0, 0, 0}, [][]int{{12}}),
			),
		},
		{
			desc: "parse 4D geometrycollection",
			equivInputs: []string{`GEOMETRYCOLLECTION ZM (
POINT ZM (0 5 -10 15),
LINESTRING ZM (0 0 0 0, 1 1 1 1),
POLYGON ZM ((0 0 12 7, 1 -1 12 -50, 2 0 12 0, 0 0 12 7)),
MULTIPOINT ZM ((2 -8 17 45), (0 0 0 0)),
MULTILINESTRING ZM ((0 0 0 0, 1 1 1 1), (-2 -3 -4 -5, 0.5 -0.75 1 -1.25, 0 1 5 7)),
MULTIPOLYGON ZM (((0 0 0 0, 1 1 1 -1, 2 3 1 -2, 0 0 0 0)))
)`,
				`GEOMETRYCOLLECTION ZM (
POINT(0 5 -10 15),
LINESTRING(0 0 0 0, 1 1 1 1),
POLYGON((0 0 12 7, 1 -1 12 -50, 2 0 12 0, 0 0 12 7)),
MULTIPOINT((2 -8 17 45), (0 0 0 0)),
MULTILINESTRING((0 0 0 0, 1 1 1 1), (-2 -3 -4 -5, 0.5 -0.75 1 -1.25, 0 1 5 7)),
MULTIPOLYGON(((0 0 0 0, 1 1 1 -1, 2 3 1 -2, 0 0 0 0)))
)`,
				`GEOMETRYCOLLECTION(
POINT(0 5 -10 15),
LINESTRING(0 0 0 0, 1 1 1 1),
POLYGON((0 0 12 7, 1 -1 12 -50, 2 0 12 0, 0 0 12 7)),
MULTIPOINT((2 -8 17 45), (0 0 0 0)),
MULTILINESTRING((0 0 0 0, 1 1 1 1), (-2 -3 -4 -5, 0.5 -0.75 1 -1.25, 0 1 5 7)),
MULTIPOLYGON(((0 0 0 0, 1 1 1 -1, 2 3 1 -2, 0 0 0 0)))
)`,
				`GEOMETRYCOLLECTION(
POINT ZM (0 5 -10 15),
LINESTRING(0 0 0 0, 1 1 1 1),
POLYGON((0 0 12 7, 1 -1 12 -50, 2 0 12 0, 0 0 12 7)),
MULTIPOINT((2 -8 17 45), (0 0 0 0)),
MULTILINESTRING((0 0 0 0, 1 1 1 1), (-2 -3 -4 -5, 0.5 -0.75 1 -1.25, 0 1 5 7)),
MULTIPOLYGON(((0 0 0 0, 1 1 1 -1, 2 3 1 -2, 0 0 0 0)))
)`,
			},
			expected: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XYZM, []float64{0, 5, -10, 15}),
				geom.NewLineStringFlat(geom.XYZM, []float64{0, 0, 0, 0, 1, 1, 1, 1}),
				geom.NewPolygonFlat(geom.XYZM, []float64{0, 0, 12, 7, 1, -1, 12, -50, 2, 0, 12, 0, 0, 0, 12, 7}, []int{16}),
				geom.NewMultiPointFlat(geom.XYZM, []float64{2, -8, 17, 45, 0, 0, 0, 0}),
				geom.NewMultiLineStringFlat(geom.XYZM,
					[]float64{0, 0, 0, 0, 1, 1, 1, 1, -2, -3, -4, -5, 0.5, -0.75, 1, -1.25, 0, 1, 5, 7}, []int{8, 20}),
				geom.NewMultiPolygonFlat(geom.XYZM, []float64{0, 0, 0, 0, 1, 1, 1, -1, 2, 3, 1, -2, 0, 0, 0, 0}, [][]int{{16}}),
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			want := tc.expected
			for _, input := range tc.equivInputs {
				got, err := Unmarshal(input)
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
}

func TestUnmarshalError(t *testing.T) {
	errorTestCases := []struct {
		desc           string
		input          string
		expectedErrStr string
	}{
		// LexError
		{
			desc:  "invalid character",
			input: "POINT{0 0}",
			expectedErrStr: `lex error: invalid character at pos 5
POINT{0 0}
     ^`,
		},
		{
			desc:  "invalid keyword",
			input: "DOT(0 0)",
			expectedErrStr: `lex error: invalid keyword at pos 0
DOT(0 0)
^`,
		},
		{
			desc:  "invalid number",
			input: "POINT(2 2.3.7)",
			expectedErrStr: `lex error: invalid number at pos 8
POINT(2 2.3.7)
        ^`,
		},
		{
			desc:  "invalid scientific notation number missing number before the e",
			input: "POINT(e-1 2)",
			expectedErrStr: `lex error: invalid keyword at pos 6
POINT(e-1 2)
      ^`,
		},
		{
			desc:  "invalid scientific notation number with non-integer power",
			input: "POINT(5e-1.5 2)",
			expectedErrStr: `lex error: invalid number at pos 6
POINT(5e-1.5 2)
      ^`,
		},
		{
			desc:  "invalid number with a + at the start (PostGIS does not allow this)",
			input: "POINT(+1 2)",
			expectedErrStr: `lex error: invalid character at pos 6
POINT(+1 2)
      ^`,
		},
		{
			desc:  "invalid keyword when extraneous spaces are present in ZM",
			input: "POINT Z M (1 1 1 1)",
			expectedErrStr: `lex error: invalid keyword at pos 8
POINT Z M (1 1 1 1)
        ^`,
		},
		// ParseError
		{
			desc:  "invalid point",
			input: "POINT POINT",
			expectedErrStr: `syntax error: unexpected POINT, expecting '(' at pos 6
POINT POINT
      ^`,
		},
		{
			desc:  "point missing closing bracket",
			input: "POINT(0 0",
			expectedErrStr: `syntax error: unexpected $end, expecting ')' at pos 9
POINT(0 0
         ^`,
		},
		{
			desc:  "2D point with extra comma",
			input: "POINT(0, 0)",
			expectedErrStr: `syntax error: not enough coordinates at pos 7
POINT(0, 0)
       ^
HINT: each point needs at least 2 coords`,
		},
		{
			desc:  "2D linestring with no points",
			input: "LINESTRING()",
			expectedErrStr: `syntax error: unexpected ')', expecting NUM at pos 11
LINESTRING()
           ^`,
		},
		{
			desc:  "2D linestring with not enough points",
			input: "LINESTRING(0 0)",
			expectedErrStr: `syntax error: non-empty linestring with only one point at pos 14
LINESTRING(0 0)
              ^
HINT: minimum number of points is 2`,
		},
		{
			desc:  "linestring with mixed dimensionality",
			input: "LINESTRING(0 0, 1 1 1)",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XY so expecting 2 coords but got 3 coords at pos 21
LINESTRING(0 0, 1 1 1)
                     ^`,
		},
		{
			desc:  "2D polygon with not enough points",
			input: "POLYGON((0 0, 1 1, 2 0))",
			expectedErrStr: `syntax error: polygon ring doesn't have enough points at pos 22
POLYGON((0 0, 1 1, 2 0))
                      ^
HINT: minimum number of points is 4`,
		},
		{
			desc:  "2D polygon with ring that isn't closed",
			input: "POLYGON((0 0, 1 1, 2 0, 1 -1))",
			expectedErrStr: `syntax error: polygon ring not closed at pos 28
POLYGON((0 0, 1 1, 2 0, 1 -1))
                            ^
HINT: ensure first and last point are the same`,
		},
		{
			desc:  "2D polygon with empty second ring",
			input: "POLYGON((0 0, 1 -1, 2 0, 0 0), ())",
			expectedErrStr: `syntax error: unexpected ')', expecting NUM at pos 32
POLYGON((0 0, 1 -1, 2 0, 0 0), ())
                                ^`,
		},
		{
			desc:  "2D polygon with EMPTY as second ring",
			input: "POLYGON((0 0, 1 -1, 2 0, 0 0), EMPTY)",
			expectedErrStr: `syntax error: unexpected EMPTY, expecting '(' at pos 31
POLYGON((0 0, 1 -1, 2 0, 0 0), EMPTY)
                               ^`,
		},
		{
			desc:  "2D polygon with invalid second ring",
			input: "POLYGON((0 0, 1 -1, 2 0, 0 0), (0.5 -0.5))",
			expectedErrStr: `syntax error: polygon ring doesn't have enough points at pos 40
POLYGON((0 0, 1 -1, 2 0, 0 0), (0.5 -0.5))
                                        ^
HINT: minimum number of points is 4`,
		},
		{
			desc:  "2D multipoint without any points",
			input: "MULTIPOINT()",
			expectedErrStr: `syntax error: unexpected ')', expecting EMPTY or NUM or '(' at pos 11
MULTIPOINT()
           ^`,
		},
		{
			desc:  "3D multipoint without comma separating points",
			input: "MULTIPOINT Z (0 0 0 0 0 0)",
			expectedErrStr: `syntax error: too many coordinates at pos 25
MULTIPOINT Z (0 0 0 0 0 0)
                         ^
HINT: each point can have at most 4 coords`,
		},
		{
			desc:  "2D multipoint with EMPTY inside extraneous parentheses",
			input: "MULTIPOINT((EMPTY))",
			expectedErrStr: `syntax error: unexpected EMPTY, expecting NUM at pos 12
MULTIPOINT((EMPTY))
            ^`,
		},
		{
			desc:  "3D multipoint using EMPTY as a point without using Z in type",
			input: "MULTIPOINT(0 0 0, EMPTY)",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYZ but encountered layout of XY at pos 18
MULTIPOINT(0 0 0, EMPTY)
                  ^
HINT: EMPTY is XY layout in base geometry type`,
		},
		{
			desc:  "multipoint with mixed dimensionality",
			input: "MULTIPOINT(0 0 0, 1 1)",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYZ so expecting 3 coords but got 2 coords at pos 21
MULTIPOINT(0 0 0, 1 1)
                     ^`,
		},
		{
			desc:  "2D multilinestring containing linestring with no points",
			input: "MULTILINESTRING(())",
			expectedErrStr: `syntax error: unexpected ')', expecting NUM at pos 17
MULTILINESTRING(())
                 ^`,
		},
		{
			desc:  "2D multilinestring containing linestring with only one point",
			input: "MULTILINESTRING((0 0))",
			expectedErrStr: `syntax error: non-empty linestring with only one point at pos 20
MULTILINESTRING((0 0))
                    ^
HINT: minimum number of points is 2`,
		},
		{
			desc:  "4D multilinestring using EMPTY without using ZM in type",
			input: "MULTILINESTRING(EMPTY, (0 0 0 0, 2 3 -2 -3))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XY so expecting 2 coords but got 4 coords at pos 31
MULTILINESTRING(EMPTY, (0 0 0 0, 2 3 -2 -3))
                               ^`,
		},
		{
			desc:  "2D multipolygon with no polygons",
			input: "MULTIPOLYGON()",
			expectedErrStr: `syntax error: unexpected ')', expecting EMPTY or '(' at pos 13
MULTIPOLYGON()
             ^`,
		},
		{
			desc:  "2D multipolygon with one polygon missing outer parentheses",
			input: "MULTIPOLYGON((1 0, 2 5, -2 5, 1 0))",
			expectedErrStr: `syntax error: unexpected NUM, expecting '(' at pos 14
MULTIPOLYGON((1 0, 2 5, -2 5, 1 0))
              ^`,
		},
		{
			desc:  "multipolygon with mixed dimensionality",
			input: "MULTIPOLYGON(((1 0, 2 5, -2 5, 1 0)), ((1 0 2, 2 5 1, -2 5 -1, 1 0 2)))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XY so expecting 2 coords but got 3 coords at pos 45
MULTIPOLYGON(((1 0, 2 5, -2 5, 1 0)), ((1 0 2, 2 5 1, -2 5 -1, 1 0 2)))
                                             ^`,
		},
		{
			desc:  "2D multipolygon with polygon that doesn't have enough points",
			input: "MULTIPOLYGON(((0 0, 1 1, 2 0)))",
			expectedErrStr: `syntax error: polygon ring doesn't have enough points at pos 28
MULTIPOLYGON(((0 0, 1 1, 2 0)))
                            ^
HINT: minimum number of points is 4`,
		},
		{
			desc:  "2D multipolygon with polygon with ring that isn't closed",
			input: "MULTIPOLYGON(((0 0, 1 1, 2 0, 1 -1)))",
			expectedErrStr: `syntax error: polygon ring not closed at pos 34
MULTIPOLYGON(((0 0, 1 1, 2 0, 1 -1)))
                                  ^
HINT: ensure first and last point are the same`,
		},
		{
			desc:  "2D multipolygon with polygon with empty second ring",
			input: "MULTIPOLYGON(((0 0, 1 -1, 2 0, 0 0), ()))",
			expectedErrStr: `syntax error: unexpected ')', expecting NUM at pos 38
MULTIPOLYGON(((0 0, 1 -1, 2 0, 0 0), ()))
                                      ^`,
		},
		{
			desc:  "2D multipolygon with polygon with EMPTY as second ring",
			input: "MULTIPOLYGON(((0 0, 1 -1, 2 0, 0 0), EMPTY))",
			expectedErrStr: `syntax error: unexpected EMPTY, expecting '(' at pos 37
MULTIPOLYGON(((0 0, 1 -1, 2 0, 0 0), EMPTY))
                                     ^`,
		},
		{
			desc:  "2D multipolygon with polygon with invalid second ring",
			input: "MULTIPOLYGON(((0 0, 1 -1, 2 0, 0 0), (0.5 -0.5)))",
			expectedErrStr: `syntax error: polygon ring doesn't have enough points at pos 46
MULTIPOLYGON(((0 0, 1 -1, 2 0, 0 0), (0.5 -0.5)))
                                              ^
HINT: minimum number of points is 4`,
		},
		{
			desc:  "3D multipolygon using EMPTY without using Z in its type",
			input: "MULTIPOLYGON(EMPTY, ((0 0 0, 1 1 1, 2 3 1, 0 0 0)))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XY so expecting 2 coords but got 3 coords at pos 27
MULTIPOLYGON(EMPTY, ((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
                           ^`,
		},
		{
			desc:  "2D geometrycollection with EMPTY item",
			input: "GEOMETRYCOLLECTION(EMPTY)",
			expectedErrStr: `syntax error: unexpected EMPTY at pos 19
GEOMETRYCOLLECTION(EMPTY)
                   ^`,
		},
		{
			desc:  "3D geometrycollection with no items",
			input: "GEOMETRYCOLLECTION Z ()",
			expectedErrStr: `syntax error: unexpected ')' at pos 22
GEOMETRYCOLLECTION Z ()
                      ^`,
		},
		{
			desc:  "base type geometrycollection with mixed dimensionality",
			input: "GEOMETRYCOLLECTION(POINT M (0 0 0), LINESTRING(0 0, 1 1))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 36
GEOMETRYCOLLECTION(POINT M (0 0 0), LINESTRING(0 0, 1 1))
                                    ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "2D+M geometrycollection with 3 coords point missing M type",
			input: "GEOMETRYCOLLECTION M (POINT(0 0 0))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 27
GEOMETRYCOLLECTION M (POINT(0 0 0))
                           ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "2D+M geometrycollection with 3 coords linestring missing M type",
			input: "GEOMETRYCOLLECTION M (LINESTRING(0 0 0, 1 1 1))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 32
GEOMETRYCOLLECTION M (LINESTRING(0 0 0, 1 1 1))
                                ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "2D+M geometrycollection with 3 coords polygon missing M type",
			input: "GEOMETRYCOLLECTION M (POLYGON((0 0 0, 1 1 1, 2 3 1, 0 0 0)))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 29
GEOMETRYCOLLECTION M (POLYGON((0 0 0, 1 1 1, 2 3 1, 0 0 0)))
                             ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "2D+M geometrycollection with 3 coords multipoint missing M type",
			input: "GEOMETRYCOLLECTION M (MULTIPOINT((0 0 0), 1 1 1))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 32
GEOMETRYCOLLECTION M (MULTIPOINT((0 0 0), 1 1 1))
                                ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "2D+M geometrycollection with 3 coords multilinestring missing M type",
			input: "GEOMETRYCOLLECTION M (MULTILINESTRING((0 0 0, 1 1 1)))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 37
GEOMETRYCOLLECTION M (MULTILINESTRING((0 0 0, 1 1 1)))
                                     ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "2D+M geometrycollection with 3 coords multipolygon missing M type",
			input: "GEOMETRYCOLLECTION M (MULTIPOLYGON(((0 0 0, 1 1 1, 2 3 1, 0 0 0))))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 34
GEOMETRYCOLLECTION M (MULTIPOLYGON(((0 0 0, 1 1 1, 2 3 1, 0 0 0))))
                                  ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "3D geometrycollection with mixed dimensionality in nested geometry collection",
			input: "GEOMETRYCOLLECTION Z (GEOMETRYCOLLECTION(POINT(0 0)))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYZ so expecting 3 coords but got 2 coords at pos 50
GEOMETRYCOLLECTION Z (GEOMETRYCOLLECTION(POINT(0 0)))
                                                  ^`,
		},
		{
			desc:  "base type geometrycollection with 3D geometry and base type EMPTY geometry",
			input: "GEOMETRYCOLLECTION(POINT(0 0 0), LINESTRING EMPTY)",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYZ but encountered layout of XY at pos 44
GEOMETRYCOLLECTION(POINT(0 0 0), LINESTRING EMPTY)
                                            ^
HINT: EMPTY is XY layout in base geometry type`,
		},
		{
			desc:  "base type geometrycollection with base type EMPTY geometry and 3D geometry",
			input: "GEOMETRYCOLLECTION(LINESTRING EMPTY, POINT(0 0 0))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XY so expecting 2 coords but got 3 coords at pos 48
GEOMETRYCOLLECTION(LINESTRING EMPTY, POINT(0 0 0))
                                                ^`,
		},
		{
			desc:  "2D+M geometrycollection with base type multipoint with mixed dimensionality",
			input: "GEOMETRYCOLLECTIONM(LINESTRING EMPTY, MULTIPOINT(EMPTY, (0 0 0)))",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 48
GEOMETRYCOLLECTIONM(LINESTRING EMPTY, MULTIPOINT(EMPTY, (0 0 0)))
                                                ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "geometrycollection with mixed dimensionality between nested geometrycollection and EMPTY linestring 1",
			input: "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION M (LINESTRING EMPTY), LINESTRING EMPTY)",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 60
GEOMETRYCOLLECTION(GEOMETRYCOLLECTION M (LINESTRING EMPTY), LINESTRING EMPTY)
                                                            ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "geometrycollection with mixed dimensionality between nested geometrycollection and EMPTY linestring 2",
			input: "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING M EMPTY), LINESTRING EMPTY)",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XYM but encountered layout of not XYM at pos 59
GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING M EMPTY), LINESTRING EMPTY)
                                                           ^
HINT: the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs`,
		},
		{
			desc:  "geometrycollection with mixed dimensionality between nested geometrycollection and EMPTY linestring 3",
			input: "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING EMPTY), LINESTRING M EMPTY)",
			expectedErrStr: `syntax error: mixed dimensionality, parsed layout is XY but encountered layout of XYM at pos 57
GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING EMPTY), LINESTRING M EMPTY)
                                                         ^`,
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := Unmarshal(tc.input)
			require.EqualError(t, err, tc.expectedErrStr)
		})
	}
}
