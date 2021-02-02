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

%}

%union {
	str       string
	geom      geom.T
	coord     float64
	coordList []float64
}

%token <str> POINT POINTZ POINTM POINTZM
%token <str> LINESTRING LINESTRINGZ LINESTRINGM LINESTRINGZM
%token <str> EMPTY
//%token <str> POLYGON MULTIPOINT MULTILINESTRING MULTIPOLYGON GEOMETRYCOLLECTION
%token <coord> NUM

%type <geom> geometry
%type <geom> point linestring
%type <coordList> two_coords three_coords four_coords
%type <coordList> two_coords_list three_coords_list four_coords_list

%%

start:
	geometry
	{
		wktlex.(*wktLex).ret = $1
	}

geometry:
	point
|	linestring
	{
		// Check that non-empty linestrings have at least 2 points.
		if !$1.Empty() && len($1.FlatCoords()) < 2 * $1.Stride() {
			return 1
		}
	}

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
| POINTZ '(' three_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XYZ, $3)
	}
| POINTM '(' three_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XYM, $3)
	}
| POINTZM '(' four_coords ')'
	{
		$$ = geom.NewPointFlat(geom.XYZM, $3)
	}
| POINT EMPTY
	{
		$$ = geom.NewPointEmpty(geom.XY)
	}
| POINTZ EMPTY
	{
		$$ = geom.NewPointEmpty(geom.XYZ)
	}
| POINTM EMPTY
	{
		$$ = geom.NewPointEmpty(geom.XYM)
	}
| POINTZM EMPTY
	{
		$$ = geom.NewPointEmpty(geom.XYZM)
	}

linestring:
	LINESTRING '(' two_coords_list ')'
	{
		$$ = geom.NewLineStringFlat(geom.XY, $3)
	}
|	LINESTRING '(' three_coords_list ')'
	{
		$$ = geom.NewLineStringFlat(geom.XYZ, $3)
	}
|	LINESTRING '(' four_coords_list ')'
	{
		$$ = geom.NewLineStringFlat(geom.XYZM, $3)
	}
|	LINESTRINGZ '(' three_coords_list ')'
	{
		$$ = geom.NewLineStringFlat(geom.XYZ, $3)
	}
|	LINESTRINGM '(' three_coords_list ')'
	{
		$$ = geom.NewLineStringFlat(geom.XYM, $3)
	}
|	LINESTRINGZM '(' four_coords_list ')'
	{
		$$ = geom.NewLineStringFlat(geom.XYZM, $3)
	}
|	LINESTRING EMPTY
	{
		$$ = geom.NewLineString(geom.XY)
	}
|	LINESTRINGZ EMPTY
	{
		$$ = geom.NewLineString(geom.XYZ)
	}
|	LINESTRINGM EMPTY
	{
		$$ = geom.NewLineString(geom.XYM)
	}
|	LINESTRINGZM EMPTY
	{
		$$ = geom.NewLineString(geom.XYZM)
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
