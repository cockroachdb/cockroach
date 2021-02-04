// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

%{

package wkt

import "github.com/twpayne/go-geom"

func isValidLineString(wktlex wktLexer, flatCoords []float64, stride int) bool {
	if len(flatCoords) < 2 * stride {
		wktlex.(*wktLex).Error("syntax error: non-empty linestring with only one point")
		return false
	}
	return true
}

func isValidPolygonRing(wktlex wktLexer, flatCoords []float64, stride int) bool {
	if len(flatCoords) < 4 * stride {
		wktlex.(*wktLex).Error("syntax error: polygon ring doesn't have enough points")
		return false
	}
	for i := 0; i < stride; i++ {
		if flatCoords[i] != flatCoords[len(flatCoords)-stride+i] {
			wktlex.(*wktLex).Error("syntax error: polygon ring not closed")
			return false
    }
	}
	return true
}

type geomPair struct {
	flatCoords []float64
	ends       []int
}

%}

%union {
	str       string
	geom      geom.T
	coord     float64
	coordList []float64
	pair      geomPair
}

%token <str> POINT POINTM POINTZ POINTZM
%token <str> LINESTRING LINESTRINGM LINESTRINGZ LINESTRINGZM
%token <str> POLYGON POLYGONM POLYGONZ POLYGONZM
%token <str> EMPTY
//%token <str> MULTIPOINT MULTILINESTRING MULTIPOLYGON GEOMETRYCOLLECTION
%token <coord> NUM

%type <geom> geometry
%type <geom> point linestring polygon
%type <coordList> two_coords three_coords four_coords
%type <coordList> two_coords_list three_coords_list four_coords_list
%type <coordList> two_coords_line three_coords_line four_coords_line
%type <pair> two_coords_ring three_coords_ring four_coords_ring
%type <pair> two_coords_ring_list three_coords_ring_list four_coords_ring_list

%%

start:
	geometry
	{
		wktlex.(*wktLex).ret = $1
	}

geometry:
	point
|	linestring
|	polygon

point:
	POINT '(' two_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XY, $3)
	}
| POINT '(' three_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XYZ, $3)
	}
| POINT '(' four_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XYZM, $3)
	}
| POINTM '(' three_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XYM, $3)
	}
| POINTZ '(' three_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XYZ, $3)
	}
| POINTZM '(' four_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XYZM, $3)
	}
| POINT EMPTY
	{
		$$ = geom.NewPointEmpty(geom.XY)
	}
| POINTM EMPTY
	{
		$$ = geom.NewPointEmpty(geom.XYM)
	}
| POINTZ EMPTY
	{
		$$ = geom.NewPointEmpty(geom.XYZ)
	}
| POINTZM EMPTY
	{
		$$ = geom.NewPointEmpty(geom.XYZM)
	}

linestring:
	LINESTRING two_coords_line
	{
		if !isValidLineString(wktlex, $2, 2) {
			return 1
		}
		$$ = geom.NewLineStringFlat(geom.XY, $2)
	}
|	LINESTRING three_coords_line
	{
		if !isValidLineString(wktlex, $2, 3) {
			return 1
		}
		$$ = geom.NewLineStringFlat(geom.XYZ, $2)
	}
|	LINESTRING four_coords_line
	{
		if !isValidLineString(wktlex, $2, 4) {
			return 1
		}
		$$ = geom.NewLineStringFlat(geom.XYZM, $2)
	}
|	LINESTRINGM three_coords_line
	{
		if !isValidLineString(wktlex, $2, 3) {
			return 1
		}
		$$ = geom.NewLineStringFlat(geom.XYM, $2)
	}
|	LINESTRINGZ three_coords_line
	{
		if !isValidLineString(wktlex, $2, 3) {
			return 1
		}
		$$ = geom.NewLineStringFlat(geom.XYZ, $2)
	}
|	LINESTRINGZM four_coords_line
	{
		if !isValidLineString(wktlex, $2, 4) {
			return 1
		}
		$$ = geom.NewLineStringFlat(geom.XYZM, $2)
	}
|	LINESTRING EMPTY
	{
		$$ = geom.NewLineString(geom.XY)
	}
|	LINESTRINGM EMPTY
	{
		$$ = geom.NewLineString(geom.XYM)
	}
|	LINESTRINGZ EMPTY
	{
		$$ = geom.NewLineString(geom.XYZ)
	}
|	LINESTRINGZM EMPTY
	{
		$$ = geom.NewLineString(geom.XYZM)
	}

polygon:
	POLYGON '(' two_coords_ring_list ')'
	{
		$$ = geom.NewPolygonFlat(geom.XY, $3.flatCoords, $3.ends)
	}
|	POLYGON '(' three_coords_ring_list ')'
	{
		$$ = geom.NewPolygonFlat(geom.XYZ, $3.flatCoords, $3.ends)
	}
|	POLYGON '(' four_coords_ring_list ')'
	{
		$$ = geom.NewPolygonFlat(geom.XYZM, $3.flatCoords, $3.ends)
	}
|	POLYGONM '(' three_coords_ring_list ')'
	{
		$$ = geom.NewPolygonFlat(geom.XYM, $3.flatCoords, $3.ends)
	}
|	POLYGONZ '(' three_coords_ring_list ')'
	{
		$$ = geom.NewPolygonFlat(geom.XYZ, $3.flatCoords, $3.ends)
	}
|	POLYGONZM '(' four_coords_ring_list ')'
	{
		$$ = geom.NewPolygonFlat(geom.XYZM, $3.flatCoords, $3.ends)
	}
|	POLYGON EMPTY
	{
		$$ = geom.NewPolygon(geom.XY)
	}
|	POLYGONM EMPTY
	{
		$$ = geom.NewPolygon(geom.XYM)
	}
|	POLYGONZ EMPTY
	{
		$$ = geom.NewPolygon(geom.XYZ)
	}
|	POLYGONZM EMPTY
	{
		$$ = geom.NewPolygon(geom.XYZM)
	}

two_coords_ring_list:
	two_coords_ring_list ',' two_coords_ring
	{
		$$ = geomPair{append($1.flatCoords, $3.flatCoords...), append($1.ends, $1.ends[len($1.ends)-1] + $3.ends[0])}
	}
|	two_coords_ring

three_coords_ring_list:
	three_coords_ring_list ',' three_coords_ring
	{
		$$ = geomPair{append($1.flatCoords, $3.flatCoords...), append($1.ends, $1.ends[len($1.ends)-1] + $3.ends[0])}
	}
|	three_coords_ring

four_coords_ring_list:
	four_coords_ring_list ',' four_coords_ring
	{
		$$ = geomPair{append($1.flatCoords, $3.flatCoords...), append($1.ends, $1.ends[len($1.ends)-1] + $3.ends[0])}
	}
|	four_coords_ring

two_coords_ring:
	two_coords_line
	{
		if !isValidPolygonRing(wktlex, $1, 2) {
			return 1
		}
		$$ = geomPair{$1, []int{len($1)}}
	}

three_coords_ring:
	three_coords_line
	{
		if !isValidPolygonRing(wktlex, $1, 3) {
			return 1
		}
		$$ = geomPair{$1, []int{len($1)}}
	}

four_coords_ring:
	four_coords_line
	{
		if !isValidPolygonRing(wktlex, $1, 4) {
			return 1
		}
		$$ = geomPair{$1, []int{len($1)}}
	}

two_coords_line:
	'(' two_coords_list ')'
	{
		$$ = $2
	}

three_coords_line:
	'(' three_coords_list ')'
	{
		$$ = $2
	}

four_coords_line:
	'(' four_coords_list ')'
	{
		$$ = $2
	}

two_coords_list:
	two_coords ',' two_coords_list
	{
		$$ = append($1, $3...)
	}
|	two_coords

three_coords_list:
	three_coords ',' three_coords_list
	{
		$$ = append($1, $3...)
	}
|	three_coords

four_coords_list:
	four_coords ',' four_coords_list
	{
		$$ = append($1, $3...)
	}
|	four_coords

two_coords:
	NUM NUM
	{
		$$ = []float64{$1, $2}
	}

three_coords:
	NUM NUM NUM
	{
		$$ = []float64{$1, $2, $3}
	}

four_coords:
	NUM NUM NUM NUM
	{
		$$ = []float64{$1, $2, $3, $4}
	}
