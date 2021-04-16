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
	str               string
	geom              geom.T
	coord             float64
	coordList         []float64
	flatRepr          geomFlatCoordsRepr
	multiPolyFlatRepr multiPolygonFlatCoordsRepr
	geomList          []geom.T
	geomCollect       *geom.GeometryCollection
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
%type <geom> point linestring polygon multipoint multilinestring multipolygon
%type <geomCollect> geometry_collection

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
		ok := wktlex.(*wktLex).validateLayoutStackAtEnd()
		if !ok {
			return 1
		}
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
		ok := wktlex.(*wktLex).validateAndPopLayoutStackFrame()
		if !ok {
			return 1
		}
		err := $1.SetLayout(wktlex.(*wktLex).curLayout())
		if err != nil {
			wktlex.(*wktLex).setError(err)
			return 1
		}
		$$ = $1
	}

point:
	point_type flat_coords_point_with_parens
	{
		$$ = geom.NewPointFlat(wktlex.(*wktLex).curLayout(), $2)
	}
|	point_base_type empty_in_base_type
	{
		$$ = geom.NewPointEmpty(wktlex.(*wktLex).curLayout())
	}
|	point_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewPointEmpty(wktlex.(*wktLex).curLayout())
	}

point_type:
	point_base_type
|	point_non_base_type

point_base_type:
	POINT
	{
		ok := wktlex.(*wktLex).validateBaseGeometryTypeAllowed()
		if !ok {
			return 1
		}
	}

point_non_base_type:
	POINTM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	POINTZ
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	POINTZM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

linestring:
	linestring_type flat_coords_linestring
	{
		$$ = geom.NewLineStringFlat(wktlex.(*wktLex).curLayout(), $2)
	}
|	linestring_base_type empty_in_base_type
	{
		$$ = geom.NewLineString(wktlex.(*wktLex).curLayout())
	}
|	linestring_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewLineString(wktlex.(*wktLex).curLayout())
	}

linestring_type:
	linestring_base_type
|	linestring_non_base_type

linestring_base_type:
	LINESTRING
	{
		ok := wktlex.(*wktLex).validateBaseGeometryTypeAllowed()
		if !ok {
			return 1
		}
	}

linestring_non_base_type:
	LINESTRINGM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	LINESTRINGZ
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	LINESTRINGZM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

polygon:
	polygon_type flat_coords_polygon_ring_list_with_parens
	{
		$$ = geom.NewPolygonFlat(wktlex.(*wktLex).curLayout(), $2.flatCoords, $2.ends)
	}
|	polygon_base_type empty_in_base_type
	{
		$$ = geom.NewPolygon(wktlex.(*wktLex).curLayout())
	}
|	polygon_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewPolygon(wktlex.(*wktLex).curLayout())
	}

polygon_type:
	polygon_base_type
|	polygon_non_base_type

polygon_base_type:
	POLYGON
	{
		ok := wktlex.(*wktLex).validateBaseGeometryTypeAllowed()
		if !ok {
			return 1
		}
	}

polygon_non_base_type:
	POLYGONM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	POLYGONZ
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	POLYGONZM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

multipoint:
	multipoint_base_type multipoint_base_type_point_list_with_parens
	{
		$$ = geom.NewMultiPointFlat(
			wktlex.(*wktLex).curLayout(), $2.flatCoords, geom.NewMultiPointFlatOptionWithEnds($2.ends),
		)
	}
|	multipoint_non_base_type multipoint_non_base_type_point_list_with_parens
	{
		$$ = geom.NewMultiPointFlat(
			wktlex.(*wktLex).curLayout(), $2.flatCoords, geom.NewMultiPointFlatOptionWithEnds($2.ends),
		)
	}
|	multipoint_base_type empty_in_base_type
	{
		$$ = geom.NewMultiPoint(wktlex.(*wktLex).curLayout())
	}
|	multipoint_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewMultiPoint(wktlex.(*wktLex).curLayout())
	}

multipoint_base_type:
	MULTIPOINT
	{
		ok := wktlex.(*wktLex).validateBaseGeometryTypeAllowed()
		if !ok {
			return 1
		}
	}

multipoint_non_base_type:
	MULTIPOINTM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	MULTIPOINTZ
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	MULTIPOINTZM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

multilinestring:
	multilinestring_base_type multilinestring_base_type_linestring_list_with_parens
	{
		$$ = geom.NewMultiLineStringFlat(wktlex.(*wktLex).curLayout(), $2.flatCoords, $2.ends)
	}
|	multilinestring_non_base_type multilinestring_non_base_type_linestring_list_with_parens
	{
		$$ = geom.NewMultiLineStringFlat(wktlex.(*wktLex).curLayout(), $2.flatCoords, $2.ends)
	}
|	multilinestring_base_type empty_in_base_type
	{
		$$ = geom.NewMultiLineString(wktlex.(*wktLex).curLayout())
	}
|	multilinestring_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewMultiLineString(wktlex.(*wktLex).curLayout())
	}

multilinestring_base_type:
	MULTILINESTRING
	{
		ok := wktlex.(*wktLex).validateBaseGeometryTypeAllowed()
		if !ok {
			return 1
		}
	}

multilinestring_non_base_type:
	MULTILINESTRINGM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	MULTILINESTRINGZ
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	MULTILINESTRINGZM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZM)
		if !ok {
			return 1
		}
	}

multipolygon:
	multipolygon_base_type multipolygon_base_type_polygon_list_with_parens
	{
		$$ = geom.NewMultiPolygonFlat(wktlex.(*wktLex).curLayout(), $2.flatCoords, $2.endss)
	}
|	multipolygon_non_base_type multipolygon_non_base_type_polygon_list_with_parens
	{
		$$ = geom.NewMultiPolygonFlat(wktlex.(*wktLex).curLayout(), $2.flatCoords, $2.endss)
	}
|	multipolygon_base_type empty_in_base_type
	{
		$$ = geom.NewMultiPolygon(wktlex.(*wktLex).curLayout())
	}
|	multipolygon_non_base_type empty_in_non_base_type
	{
		$$ = geom.NewMultiPolygon(wktlex.(*wktLex).curLayout())
	}

multipolygon_base_type:
	MULTIPOLYGON
	{
		ok := wktlex.(*wktLex).validateBaseGeometryTypeAllowed()
		if !ok {
			return 1
		}
	}

multipolygon_non_base_type:
	MULTIPOLYGONM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYM)
		if !ok {
			return 1
		}
	}
|	MULTIPOLYGONZ
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	MULTIPOLYGONZM
	{
		ok := wktlex.(*wktLex).validateAndSetLayoutIfNoLayout(geom.XYZM)
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
		ok := wktlex.(*wktLex).validateAndPushLayoutStackFrame(geom.NoLayout)
		if !ok {
			return 1
		}
	}

geometry_collection_non_base_type:
	GEOMETRYCOLLECTIONM
	{
		ok := wktlex.(*wktLex).validateAndPushLayoutStackFrame(geom.XYM)
		if !ok {
			return 1
		}
	}
|	GEOMETRYCOLLECTIONZ
	{
		ok := wktlex.(*wktLex).validateAndPushLayoutStackFrame(geom.XYZ)
		if !ok {
			return 1
		}
	}
|	GEOMETRYCOLLECTIONZM
	{
		ok := wktlex.(*wktLex).validateAndPushLayoutStackFrame(geom.XYZM)
		if !ok {
			return 1
		}
	}

geometry_opening_lparen:
	'('
	{
		ok := wktlex.(*wktLex).validateNonEmptyGeometryAllowed()
		if !ok {
			return 1
		}
	}

multipolygon_base_type_polygon_list_with_parens:
	geometry_opening_lparen multipolygon_base_type_polygon_list ')'
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
	geometry_opening_lparen multilinestring_base_type_linestring_list ')'
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
	geometry_opening_lparen multipoint_base_type_point_list ')'
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
	geometry_opening_lparen flat_coords_polygon_ring_list ')'
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
		if !wktlex.(*wktLex).isValidPolygonRing($1) {
			return 1
		}
		$$ = makeGeomFlatCoordsRepr($1)
	}

flat_coords_linestring:
	flat_coords_point_list_with_parens
	{
		if !wktlex.(*wktLex).isValidLineString($1) {
			return 1
		}
	}

flat_coords_point_list_with_parens:
	geometry_opening_lparen flat_coords_point_list ')'
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
	geometry_opening_lparen flat_coords_point ')'
	{
		$$ = $2
	}

flat_coords_point:
	flat_coords
	{
		if !wktlex.(*wktLex).isValidPoint($1) {
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
		ok := wktlex.(*wktLex).validateBaseTypeEmptyAllowed()
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
