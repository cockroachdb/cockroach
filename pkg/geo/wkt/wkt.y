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
		wktlex.(*wktLex).setParseError("non-empty linestring with only one point", "minimum number of points is 2")
		return false
	}
	return true
}

func isValidPolygonRing(wktlex wktLexer, flatCoords []float64, stride int) bool {
	if len(flatCoords) < 4 * stride {
		wktlex.(*wktLex).setParseError("polygon ring doesn't have enough points", "minimum number of points is 4")
		return false
	}
	for i := 0; i < stride; i++ {
		if flatCoords[i] != flatCoords[len(flatCoords)-stride+i] {
			wktlex.(*wktLex).setParseError("polygon ring not closed", "ensure first and last point are the same")
			return false
    }
	}
	return true
}

type geomFlatCoordsRepr struct {
	flatCoords []float64
	ends       []int
}

func makeGeomFlatCoordsRepr(flatCoords []float64) geomFlatCoordsRepr {
	return geomFlatCoordsRepr{flatCoords: flatCoords, ends: []int{len(flatCoords)}}
}

func appendGeomFlatCoordsReprs(p1 geomFlatCoordsRepr, p2 geomFlatCoordsRepr) geomFlatCoordsRepr {
	if len(p1.ends) > 0 {
		p1LastEnd := p1.ends[len(p1.ends)-1]
		for i, _ := range p2.ends {
			p2.ends[i] += p1LastEnd
		}
	}
	return geomFlatCoordsRepr{flatCoords: append(p1.flatCoords, p2.flatCoords...), ends: append(p1.ends, p2.ends...)}
}

type multiPolygonFlatCoordsRepr struct {
	flatCoords []float64
	endss      [][]int
}

func makeMultiPolygonFlatCoordsRepr(p geomFlatCoordsRepr) multiPolygonFlatCoordsRepr {
	if p.flatCoords == nil {
		return multiPolygonFlatCoordsRepr{flatCoords: nil, endss: [][]int{nil}}
	}
	return multiPolygonFlatCoordsRepr{flatCoords: p.flatCoords, endss: [][]int{p.ends}}
}

func appendMultiPolygonFlatCoordsRepr(
	p1 multiPolygonFlatCoordsRepr, p2 multiPolygonFlatCoordsRepr,
) multiPolygonFlatCoordsRepr {
	p1LastEndsLastEnd := 0
	for i := len(p1.endss) - 1; i >= 0; i-- {
		if len(p1.endss[i]) > 0 {
			p1LastEndsLastEnd = p1.endss[i][len(p1.endss[i])-1]
			break
		}
	}
	if p1LastEndsLastEnd > 0 {
		for i, _ := range p2.endss {
			for j, _ := range p2.endss[i] {
				p2.endss[i][j] += p1LastEndsLastEnd
			}
		}
	}
	return multiPolygonFlatCoordsRepr{
		flatCoords: append(p1.flatCoords, p2.flatCoords...), endss: append(p1.endss, p2.endss...),
	}
}

%}

%union {
	str               string
	geom              geom.T
	coord             float64
	coordList         []float64
	flatRepr          geomFlatCoordsRepr
	multiPolyFlatRepr multiPolygonFlatCoordsRepr
	geomList          []geom.T
}

// Tokens
%token <str> POINT POINTM POINTZ POINTZM
%token <str> LINESTRING LINESTRINGM LINESTRINGZ LINESTRINGZM
%token <str> POLYGON POLYGONM POLYGONZ POLYGONZM
%token <str> MULTIPOINT MULTIPOINTM MULTIPOINTZ MULTIPOINTZM
%token <str> MULTILINESTRING MULTILINESTRINGM MULTILINESTRINGZ MULTILINESTRINGZM
%token <str> MULTIPOLYGON MULTIPOLYGONM MULTIPOLYGONZ MULTIPOLYGONZM
%token <str> GEOMETRYCOLLECTION GEOMETRYCOLLECTIONM GEOMETRYCOLLECTIONZ GEOMETRYCOLLECTIONZM
%token <str> EMPTY
%token <coord> NUM

// Geometries
%type <geom> geometry
%type <geom> point linestring polygon multipoint multilinestring multipolygon geometry_collection

// Empty representations
%type <coordList> empty_in_base_type
%type <coordList> empty_in_non_base_type
%type <coordList> flat_coords_empty

// Points
%type <coordList> flat_coords
%type <coordList> flat_coords_point
%type <coordList> flat_coords_point_with_parens

// LineStrings
%type <coordList> flat_coords_point_list
%type <coordList> flat_coords_point_list_with_parens
%type <coordList> flat_coords_linestring

// Polygons
%type <flatRepr> flat_coords_polygon_ring
%type <flatRepr> flat_coords_polygon_ring_list
%type <flatRepr> flat_coords_polygon_ring_list_with_parens

// MultiPoints
%type <coordList> multipoint_point
%type <coordList> multipoint_base_type_point
%type <coordList> multipoint_non_base_type_point

%type <flatRepr> multipoint_base_type_point_flat_repr
%type <flatRepr> multipoint_non_base_point_flat_repr
%type <flatRepr> multipoint_base_type_point_list
%type <flatRepr> multipoint_non_base_type_point_list
%type <flatRepr> multipoint_base_type_point_list_with_parens
%type <flatRepr> multipoint_non_base_type_point_list_with_parens

// MultiLineStrings
%type <coordList> multilinestring_base_type_linestring
%type <coordList> multilinestring_non_base_type_linestring

%type <flatRepr> multilinestring_base_type_linestring_flat_repr
%type <flatRepr> multilinestring_non_base_type_linestring_flat_repr
%type <flatRepr> multilinestring_base_type_linestring_list
%type <flatRepr> multilinestring_non_base_type_linestring_list
%type <flatRepr> multilinestring_base_type_linestring_list_with_parens
%type <flatRepr> multilinestring_non_base_type_linestring_list_with_parens

// MultiPolygons
%type <flatRepr> multipolygon_base_type_polygon
%type <flatRepr> multipolygon_non_base_type_polygon

%type <multiPolyFlatRepr> multipolygon_base_type_polygon_multi_poly_repr
%type <multiPolyFlatRepr> multipolygon_non_base_type_polygon_multi_poly_repr
%type <multiPolyFlatRepr> multipolygon_base_type_polygon_list
%type <multiPolyFlatRepr> multipolygon_non_base_type_polygon_list
%type <multiPolyFlatRepr> multipolygon_base_type_polygon_list_with_parens
%type <multiPolyFlatRepr> multipolygon_non_base_type_polygon_list_with_parens

// GeometryCollections
%type <geomList> geometry_list
%type <geomList> geometry_list_with_parens

%%

start:
	geometry
	{
		wktlex.(*wktLex).checkLayoutStackHasNoGeometryCollectionFramesLeft()
		wktlex.(*wktLex).ret = $1
	}

geometry:
	point
|	linestring
|	polygon
|	multipoint
|	multilinestring
|	multipolygon
|	geometry_collection
	{
		wktlex.(*wktLex).popLayoutStack()
	}

point:
	point_type flat_coords_point_with_parens
	{
		$$ = geom.NewPointFlat(wktlex.(*wktLex).getCurLayout(), $2)
	}
|	point_base_type empty_in_base_type
	{
		$$ = geom.NewPointEmpty(wktlex.(*wktLex).getCurLayout())
	}
|	point_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewPointEmpty(wktlex.(*wktLex).getCurLayout())
	}

point_type:
	point_base_type
|	point_non_base_type

point_base_type:
	POINT
	{
		ok := wktlex.(*wktLex).setLayoutBaseType()
		if !ok {
			return 1
		}
	}

point_non_base_type:
	POINTM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	POINTZ
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	POINTZM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

linestring:
	linestring_type flat_coords_linestring
	{
		$$ = geom.NewLineStringFlat(wktlex.(*wktLex).getCurLayout(), $2)
	}
|	linestring_base_type empty_in_base_type
	{
		$$ = geom.NewLineString(wktlex.(*wktLex).getCurLayout())
	}
|	linestring_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewLineString(wktlex.(*wktLex).getCurLayout())
	}

linestring_type:
	linestring_base_type
|	linestring_non_base_type

linestring_base_type:
	LINESTRING
	{
		ok := wktlex.(*wktLex).setLayoutBaseType()
		if !ok {
			return 1
		}
	}

linestring_non_base_type:
	LINESTRINGM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	LINESTRINGZ
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	LINESTRINGZM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

polygon:
	polygon_type flat_coords_polygon_ring_list_with_parens
	{
		$$ = geom.NewPolygonFlat(wktlex.(*wktLex).getCurLayout(), $2.flatCoords, $2.ends)
	}
|	polygon_base_type empty_in_base_type
	{
		$$ = geom.NewPolygon(wktlex.(*wktLex).getCurLayout())
	}
|	polygon_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewPolygon(wktlex.(*wktLex).getCurLayout())
	}

polygon_type:
	polygon_base_type
|	polygon_non_base_type

polygon_base_type:
	POLYGON
	{
		ok := wktlex.(*wktLex).setLayoutBaseType()
		if !ok {
			return 1
		}
	}

polygon_non_base_type:
	POLYGONM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	POLYGONZ
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	POLYGONZM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

multipoint:
	multipoint_base_type multipoint_base_type_point_list_with_parens
	{
		$$ = geom.NewMultiPointFlat(
			wktlex.(*wktLex).getCurLayout(), $2.flatCoords, geom.NewMultiPointFlatOptionWithEnds($2.ends),
		)
	}
|	multipoint_non_base_type multipoint_non_base_type_point_list_with_parens
	{
		$$ = geom.NewMultiPointFlat(
			wktlex.(*wktLex).getCurLayout(), $2.flatCoords, geom.NewMultiPointFlatOptionWithEnds($2.ends),
		)
	}
|	multipoint_base_type empty_in_base_type
	{
		$$ = geom.NewMultiPoint(wktlex.(*wktLex).getCurLayout())
	}
|	multipoint_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewMultiPoint(wktlex.(*wktLex).getCurLayout())
	}

multipoint_base_type:
	MULTIPOINT
	{
		ok := wktlex.(*wktLex).setLayoutBaseType()
		if !ok {
			return 1
		}
	}

multipoint_non_base_type:
	MULTIPOINTM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	MULTIPOINTZ
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	MULTIPOINTZM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

multilinestring:
	multilinestring_base_type multilinestring_base_type_linestring_list_with_parens
	{
		$$ = geom.NewMultiLineStringFlat(wktlex.(*wktLex).getCurLayout(), $2.flatCoords, $2.ends)
	}
|	multilinestring_non_base_type multilinestring_non_base_type_linestring_list_with_parens
	{
		$$ = geom.NewMultiLineStringFlat(wktlex.(*wktLex).getCurLayout(), $2.flatCoords, $2.ends)
	}
|	multilinestring_base_type empty_in_base_type
	{
		$$ = geom.NewMultiLineString(wktlex.(*wktLex).getCurLayout())
	}
|	multilinestring_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewMultiLineString(wktlex.(*wktLex).getCurLayout())
	}

multilinestring_base_type:
	MULTILINESTRING
	{
		ok := wktlex.(*wktLex).setLayoutBaseType()
		if !ok {
			return 1
		}
	}

multilinestring_non_base_type:
	MULTILINESTRINGM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	MULTILINESTRINGZ
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	MULTILINESTRINGZM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

multipolygon:
	multipolygon_base_type multipolygon_base_type_polygon_list_with_parens
	{
		$$ = geom.NewMultiPolygonFlat(wktlex.(*wktLex).getCurLayout(), $2.flatCoords, $2.endss)
	}
|	multipolygon_non_base_type multipolygon_non_base_type_polygon_list_with_parens
	{
		$$ = geom.NewMultiPolygonFlat(wktlex.(*wktLex).getCurLayout(), $2.flatCoords, $2.endss)
	}
|	multipolygon_base_type empty_in_base_type
	{
		$$ = geom.NewMultiPolygon(wktlex.(*wktLex).getCurLayout())
	}
|	multipolygon_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewMultiPolygon(wktlex.(*wktLex).getCurLayout())
	}

multipolygon_base_type:
	MULTIPOLYGON
	{
		ok := wktlex.(*wktLex).setLayoutBaseType()
		if !ok {
			return 1
		}
	}

multipolygon_non_base_type:
	MULTIPOLYGONM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	MULTIPOLYGONZ
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	MULTIPOLYGONZM
	{
		ok := wktlex.(*wktLex).setLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

geometry_collection:
	geometry_collection_type geometry_list_with_parens
	{
		newCollection := geom.NewGeometryCollection()
		err := newCollection.Push($2...)
		if err != nil {
			wktlex.(*wktLex).setError(err)
			return 1
		}
		$$ = newCollection
	}
|	geometry_collection_base_type empty_in_base_type
	{
		$$ = geom.NewGeometryCollection()
	}
|	geometry_collection_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewGeometryCollection()
	}

geometry_list_with_parens:
	'(' geometry_list ')'
	{
		$$ = $2
	}

geometry_list:
	geometry_list ',' geometry
	{
		$$ = append($1, $3)
	}
|	geometry
	{
		$$ = []geom.T{$1}
	}

geometry_collection_type:
	geometry_collection_base_type
|	geometry_collection_non_base_type

geometry_collection_base_type:
	GEOMETRYCOLLECTION
	{
		ok := wktlex.(*wktLex).pushLayoutStack(geom.NoLayout)
		if !ok {
			return 1
		}
	}

geometry_collection_non_base_type:
	GEOMETRYCOLLECTIONM
	{
		ok := wktlex.(*wktLex).pushLayoutStack(geom.XYM)
		if !ok {
			return 1
		}
	}
|	GEOMETRYCOLLECTIONZ
	{
		ok := wktlex.(*wktLex).pushLayoutStack(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	GEOMETRYCOLLECTIONZM
	{
		ok := wktlex.(*wktLex).pushLayoutStack(geom.XYZM)
		if !ok {
			return 1
		}
	}

lparen_with_edge_case_check:
	'('
	{
		ok := wktlex.(*wktLex).isNonEmptyAllowedForLayout()
		if !ok {
			return 1
		}
	}

multipolygon_base_type_polygon_list_with_parens:
	lparen_with_edge_case_check multipolygon_base_type_polygon_list ')'
	{
		$$ = $2
	}

multipolygon_non_base_type_polygon_list_with_parens:
	'(' multipolygon_non_base_type_polygon_list ')'
	{
		$$ = $2
	}

multipolygon_non_base_type_polygon_list:
	multipolygon_non_base_type_polygon_list ',' multipolygon_non_base_type_polygon_multi_poly_repr
	{
		$$ = appendMultiPolygonFlatCoordsRepr($1, $3)
	}
|	multipolygon_non_base_type_polygon_multi_poly_repr

multipolygon_base_type_polygon_list:
	multipolygon_base_type_polygon_list ',' multipolygon_base_type_polygon_multi_poly_repr
	{
		$$ = appendMultiPolygonFlatCoordsRepr($1, $3)
	}
|	multipolygon_base_type_polygon_multi_poly_repr

multipolygon_base_type_polygon_multi_poly_repr:
	multipolygon_base_type_polygon
	{
		$$ = makeMultiPolygonFlatCoordsRepr($1)
	}

multipolygon_non_base_type_polygon_multi_poly_repr:
	multipolygon_non_base_type_polygon
	{
		$$ = makeMultiPolygonFlatCoordsRepr($1)
	}

multipolygon_base_type_polygon:
	flat_coords_polygon_ring_list_with_parens
|	empty_in_base_type
	{
		$$ = makeGeomFlatCoordsRepr($1)
	}

multipolygon_non_base_type_polygon:
	flat_coords_polygon_ring_list_with_parens
|	empty_in_non_base_type
	{
		$$ = makeGeomFlatCoordsRepr($1)
	}

multilinestring_base_type_linestring_list_with_parens:
	lparen_with_edge_case_check multilinestring_base_type_linestring_list ')'
	{
		$$ = $2
	}

multilinestring_non_base_type_linestring_list_with_parens:
	'(' multilinestring_non_base_type_linestring_list ')'
	{
		$$ = $2
	}

multilinestring_base_type_linestring_list:
	multilinestring_base_type_linestring_list ',' multilinestring_base_type_linestring_flat_repr
	{
		$$ = appendGeomFlatCoordsReprs($1, $3)
	}
|	multilinestring_base_type_linestring_flat_repr

multilinestring_non_base_type_linestring_list:
	multilinestring_non_base_type_linestring_list ',' multilinestring_non_base_type_linestring_flat_repr
	{
		$$ = appendGeomFlatCoordsReprs($1, $3)
	}
|	multilinestring_non_base_type_linestring_flat_repr

multilinestring_base_type_linestring_flat_repr:
	multilinestring_base_type_linestring
	{
		$$ = makeGeomFlatCoordsRepr($1)
	}

multilinestring_non_base_type_linestring_flat_repr:
	multilinestring_non_base_type_linestring
	{
		$$ = makeGeomFlatCoordsRepr($1)
	}

multilinestring_base_type_linestring:
	flat_coords_linestring
|	empty_in_base_type

multilinestring_non_base_type_linestring:
	flat_coords_linestring
|	empty_in_non_base_type

multipoint_base_type_point_list_with_parens:
	lparen_with_edge_case_check multipoint_base_type_point_list ')'
	{
		$$ = $2
	}

multipoint_non_base_type_point_list_with_parens:
	'(' multipoint_non_base_type_point_list ')'
	{
		$$ = $2
	}

multipoint_base_type_point_list:
	multipoint_base_type_point_list ',' multipoint_base_type_point_flat_repr
	{
		$$ = appendGeomFlatCoordsReprs($1, $3)
	}
|	multipoint_base_type_point_flat_repr

multipoint_non_base_type_point_list:
	multipoint_non_base_type_point_list ',' multipoint_non_base_point_flat_repr
	{
		$$ = appendGeomFlatCoordsReprs($1, $3)
	}
|	multipoint_non_base_point_flat_repr

multipoint_base_type_point_flat_repr:
	multipoint_base_type_point
	{
		$$ = makeGeomFlatCoordsRepr($1)
	}

multipoint_non_base_point_flat_repr:
	multipoint_non_base_type_point
	{
		$$ = makeGeomFlatCoordsRepr($1)
	}

multipoint_base_type_point:
	multipoint_point
|	empty_in_base_type

multipoint_non_base_type_point:
	multipoint_point
|	empty_in_non_base_type
multipoint_point:
	flat_coords_point
|	flat_coords_point_with_parens

flat_coords_polygon_ring_list_with_parens:
	lparen_with_edge_case_check flat_coords_polygon_ring_list ')'
	{
		$$ = $2
	}

flat_coords_polygon_ring_list:
	flat_coords_polygon_ring_list ',' flat_coords_polygon_ring
	{
		$$ = appendGeomFlatCoordsReprs($1, $3)
	}
|	flat_coords_polygon_ring

flat_coords_polygon_ring:
	flat_coords_point_list_with_parens
	{
		if !isValidPolygonRing(wktlex, $1, wktlex.(*wktLex).getCurLayout().Stride()) {
			return 1
		}
		$$ = makeGeomFlatCoordsRepr($1)
	}

flat_coords_linestring:
	flat_coords_point_list_with_parens
	{
		if !isValidLineString(wktlex, $1, wktlex.(*wktLex).getCurLayout().Stride()) {
			return 1
		}
	}

flat_coords_point_list_with_parens:
	lparen_with_edge_case_check flat_coords_point_list ')'
	{
		$$ = $2
	}

flat_coords_point_list:
	flat_coords_point_list ',' flat_coords_point
	{
		$$ = append($1, $3...)
	}
|	flat_coords_point

flat_coords_point_with_parens:
	lparen_with_edge_case_check flat_coords_point ')'
	{
		$$ = $2
	}

flat_coords_point:
	flat_coords
	{
		switch len($1) {
		case 1:
			wktlex.(*wktLex).setParseError("not enough coordinates", "each point needs at least 2 coords")
			return 1
		case 2, 3, 4:
			ok := wktlex.(*wktLex).validateStrideAndSetLayoutIfNoLayout(len($1))
			if !ok {
				return 1
			}
		default:
			wktlex.(*wktLex).setParseError("too many coordinates", "each point can have at most 4 coords")
			return 1
		}
	}

flat_coords:
	flat_coords NUM
	{
		$$ = append($1, $2)
	}
|	NUM
	{
		$$ = []float64{$1}
	}

empty_in_base_type:
	flat_coords_empty
	{
		ok := wktlex.(*wktLex).setLayoutBaseTypeEmpty()
		if !ok {
			return 1
		}
	}

empty_in_non_base_type:
	flat_coords_empty

flat_coords_empty:
	EMPTY
	{
		$$ = []float64(nil)
	}
