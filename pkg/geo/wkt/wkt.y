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
%token <str> EMPTY
//%token <str> LINESTRING POLYGON MULTIPOINT MULTILINESTRING MULTIPOLYGON GEOMETRYCOLLECTION
%token <coord> NUM

%type <geom> geometry
%type <geom> point
%type <coordList> two_coords three_coords four_coords

%%

start:
	geometry
	{
		wktlex.(*wktLex).ret = $1
	}

geometry:
	point

point:
	POINT two_coords
	{
		$$ = geom.NewPointFlat(geom.XY, $2)
	}
| POINT three_coords
	{
		$$ = geom.NewPointFlat(geom.XYZ, $2)
	}
| POINT four_coords
	{
		$$ = geom.NewPointFlat(geom.XYZM, $2)
	}
| POINTZ three_coords
	{
		$$ = geom.NewPointFlat(geom.XYZ, $2)
	}
| POINTM three_coords
	{
		$$ = geom.NewPointFlat(geom.XYM, $2)
	}
| POINTZM four_coords
	{
		$$ = geom.NewPointFlat(geom.XYZM, $2)
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

two_coords:
	'(' NUM NUM ')'
	{
		$$ = []float64{$2, $3}
	}

three_coords:
	'(' NUM NUM NUM ')'
	{
		$$ = []float64{$2, $3, $4}
	}

four_coords:
	'(' NUM NUM NUM NUM ')'
	{
		$$ = []float64{$2, $3, $4, $5}
	}
