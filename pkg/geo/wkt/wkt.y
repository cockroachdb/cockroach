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

func appendGeomPairs(p1 geomPair, p2 geomPair) geomPair {
	if len(p1.ends) > 0 {
		p1LastEnd := p1.ends[len(p1.ends)-1]
		for i, _ := range p2.ends {
			p2.ends[i] += p1LastEnd
		}
	}
	return geomPair{append(p1.flatCoords, p2.flatCoords...), append(p1.ends, p2.ends...)}
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
%token <str> MULTIPOINT MULTIPOINTM MULTIPOINTZ MULTIPOINTZM
%token <str> EMPTY
//%token <str> MULTILINESTRING MULTIPOLYGON GEOMETRYCOLLECTION
%token <coord> NUM

%type <geom> geometry
%type <geom> point linestring polygon multipoint
%type <coordList> two_coords three_coords four_coords
%type <coordList> two_coords_point_with_parens three_coords_point_with_parens four_coords_point_with_parens
%type <coordList> two_coords_list three_coords_list four_coords_list
%type <coordList> two_coords_line three_coords_line four_coords_line
%type <pair> two_coords_ring three_coords_ring four_coords_ring
%type <pair> two_coords_ring_list three_coords_ring_list four_coords_ring_list
%type <coordList> two_coords_point three_coords_point four_coords_point
%type <coordList> three_coords_point_list four_coords_point_list
%type <pair> empty_point
%type <pair> two_coords_point_allowing_empty three_coords_point_allowing_empty four_coords_point_allowing_empty
%type <pair> two_coords_point_list_allowing_empty_points
%type <pair> three_coords_point_list_allowing_empty_points
%type <pair> four_coords_point_list_allowing_empty_points

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
|	multipoint

point:
	POINT two_coords_point_with_parens
	{
		$$ = geom.NewPointFlat(geom.XY, $2)
	}
|	POINT three_coords_point_with_parens
	{
		$$ = geom.NewPointFlat(geom.XYZ, $2)
	}
|	POINT four_coords_point_with_parens
	{
		$$ = geom.NewPointFlat(geom.XYZM, $2)
	}
|	POINTM three_coords_point_with_parens
	{
		$$ = geom.NewPointFlat(geom.XYM, $2)
	}
|	POINTZ three_coords_point_with_parens
	{
		$$ = geom.NewPointFlat(geom.XYZ, $2)
	}
|	POINTZM four_coords_point_with_parens
	{
		$$ = geom.NewPointFlat(geom.XYZM, $2)
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

multipoint:
	MULTIPOINT '(' two_coords_point_list_allowing_empty_points ')'
	{
		$$ = geom.NewMultiPointFlat(geom.XY, $3.flatCoords, geom.NewMultiPointFlatOptionWithEnds($3.ends))
	}
|	MULTIPOINT '(' three_coords_point_list ')'
	{
		$$ = geom.NewMultiPointFlat(geom.XYZ, $3)
	}
|	MULTIPOINT '(' four_coords_point_list ')'
	{
		$$ = geom.NewMultiPointFlat(geom.XYZM, $3)
	}
|	MULTIPOINTM '(' three_coords_point_list_allowing_empty_points ')'
	{
		$$ = geom.NewMultiPointFlat(geom.XYM, $3.flatCoords, geom.NewMultiPointFlatOptionWithEnds($3.ends))
	}
|	MULTIPOINTZ '(' three_coords_point_list_allowing_empty_points ')'
	{
		$$ = geom.NewMultiPointFlat(geom.XYZ, $3.flatCoords, geom.NewMultiPointFlatOptionWithEnds($3.ends))
	}
|	MULTIPOINTZM '(' four_coords_point_list_allowing_empty_points ')'
	{
		$$ = geom.NewMultiPointFlat(geom.XYZM, $3.flatCoords, geom.NewMultiPointFlatOptionWithEnds($3.ends))
	}
|	MULTIPOINT EMPTY
	{
		$$ = geom.NewMultiPoint(geom.XY)
	}
|	MULTIPOINTM EMPTY
	{
		$$ = geom.NewMultiPoint(geom.XYM)
	}
|	MULTIPOINTZ EMPTY
	{
		$$ = geom.NewMultiPoint(geom.XYZ)
	}
|	MULTIPOINTZM EMPTY
	{
		$$ = geom.NewMultiPoint(geom.XYZM)
	}

two_coords_ring_list:
	two_coords_ring_list ',' two_coords_ring
	{
		$$ = appendGeomPairs($1, $3)
	}
|	two_coords_ring

three_coords_ring_list:
	three_coords_ring_list ',' three_coords_ring
	{
		$$ = appendGeomPairs($1, $3)
	}
|	three_coords_ring

four_coords_ring_list:
	four_coords_ring_list ',' four_coords_ring
	{
		$$ = appendGeomPairs($1, $3)
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
	two_coords_list ',' two_coords
	{
		$$ = append($1, $3...)
	}
|	two_coords

three_coords_list:
	three_coords_list ',' three_coords
	{
		$$ = append($1, $3...)
	}
|	three_coords

four_coords_list:
	four_coords_list ',' four_coords
	{
		$$ = append($1, $3...)
	}
|	four_coords

// NB: A two_coords_point_list is not required since a 2D list inside a MULTIPOINT is always allowed to have EMPTYs.

three_coords_point_list:
	three_coords_point_list ',' three_coords_point
	{
		$$ = append($1, $3...)
	}
|	three_coords_point

four_coords_point_list:
	four_coords_point_list ',' four_coords_point
	{
		$$ = append($1, $3...)
	}
|	four_coords_point

two_coords_point_list_allowing_empty_points:
	two_coords_point_list_allowing_empty_points ',' two_coords_point_allowing_empty
	{
		$$ = appendGeomPairs($1, $3)
	}
|	two_coords_point_allowing_empty

three_coords_point_list_allowing_empty_points:
	three_coords_point_list_allowing_empty_points ',' three_coords_point_allowing_empty
	{
		$$ = appendGeomPairs($1, $3)
	}
|	three_coords_point_allowing_empty

four_coords_point_list_allowing_empty_points:
	four_coords_point_list_allowing_empty_points ',' four_coords_point_allowing_empty
	{
		$$ = appendGeomPairs($1, $3)
	}
|	four_coords_point_allowing_empty

two_coords_point_allowing_empty:
	two_coords_point
	{
		$$ = geomPair{$1, []int{2}}
	}
|	empty_point

three_coords_point_allowing_empty:
	three_coords_point
	{
		$$ = geomPair{$1, []int{3}}
	}
|	empty_point

four_coords_point_allowing_empty:
	four_coords_point
	{
		$$ = geomPair{$1, []int{4}}
	}
|	empty_point

two_coords_point:
	two_coords
|	two_coords_point_with_parens

three_coords_point:
	three_coords
|	three_coords_point_with_parens

four_coords_point:
	four_coords
|	four_coords_point_with_parens

empty_point:
	EMPTY
	{
		$$ = geomPair{nil, []int{0}}
	}

two_coords_point_with_parens:
	'(' two_coords ')'
	{
		$$ = $2
	}

three_coords_point_with_parens:
	'(' three_coords ')'
	{
		$$ = $2
	}

four_coords_point_with_parens:
	'(' four_coords ')'
	{
		$$ = $2
	}

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
