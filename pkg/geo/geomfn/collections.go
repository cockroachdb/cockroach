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
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// CollectionExtract returns a (multi-)geometry consisting only of the specified type.
// The type can only be point, line, or polygon.
func CollectionExtract(g geo.Geometry, shapeType geopb.ShapeType) (geo.Geometry, error) {
	switch shapeType {
	case geopb.ShapeType_Point, geopb.ShapeType_LineString, geopb.ShapeType_Polygon:
	default:
		return geo.Geometry{}, errors.Newf("only point, linestring and polygon may be extracted (got %s)",
			shapeType)
	}

	// If the input is already of the correct (multi-)type, just return it before
	// decoding the geom.T below.
	if g.ShapeType() == shapeType || g.ShapeType() == shapeType.MultiType() {
		return g, nil
	}

	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	switch t := t.(type) {
	// If the input is not a collection then return an empty geometry of the expected type.
	case *geom.Point, *geom.LineString, *geom.Polygon:
		switch shapeType {
		case geopb.ShapeType_Point:
			return geo.MakeGeometryFromGeomT(geom.NewPointEmpty(t.Layout()).SetSRID(t.SRID()))
		case geopb.ShapeType_LineString:
			return geo.MakeGeometryFromGeomT(geom.NewLineString(t.Layout()).SetSRID(t.SRID()))
		case geopb.ShapeType_Polygon:
			return geo.MakeGeometryFromGeomT(geom.NewPolygon(t.Layout()).SetSRID(t.SRID()))
		default:
			return geo.Geometry{}, errors.AssertionFailedf("unexpected shape type %v", shapeType.String())
		}

	// If the input is a multitype then return an empty multi-geometry of the expected type.
	case *geom.MultiPoint, *geom.MultiLineString, *geom.MultiPolygon:
		switch shapeType.MultiType() {
		case geopb.ShapeType_MultiPoint:
			return geo.MakeGeometryFromGeomT(geom.NewMultiPoint(t.Layout()).SetSRID(t.SRID()))
		case geopb.ShapeType_MultiLineString:
			return geo.MakeGeometryFromGeomT(geom.NewMultiLineString(t.Layout()).SetSRID(t.SRID()))
		case geopb.ShapeType_MultiPolygon:
			return geo.MakeGeometryFromGeomT(geom.NewMultiPolygon(t.Layout()).SetSRID(t.SRID()))
		default:
			return geo.Geometry{}, errors.AssertionFailedf("unexpected shape type %v", shapeType.MultiType().String())
		}

	// If the input is a collection, recursively gather geometries of the right type.
	case *geom.GeometryCollection:
		// Empty geos.GeometryCollection has NoLayout, while PostGIS uses XY. Returned
		// multi-geometries cannot have NoLayout.
		layout := t.Layout()
		if layout == geom.NoLayout && t.Empty() {
			layout = geom.XY
		}
		iter := geo.NewGeomTIterator(t, geo.EmptyBehaviorOmit)
		srid := t.SRID()

		var (
			multi geom.T
			err   error
		)
		switch shapeType {
		case geopb.ShapeType_Point:
			multi, err = collectionExtractPoints(iter, layout, srid)
		case geopb.ShapeType_LineString:
			multi, err = collectionExtractLineStrings(iter, layout, srid)
		case geopb.ShapeType_Polygon:
			multi, err = collectionExtractPolygons(iter, layout, srid)
		default:
			return geo.Geometry{}, errors.AssertionFailedf("unexpected shape type %v", shapeType.String())
		}
		if err != nil {
			return geo.Geometry{}, err
		}
		return geo.MakeGeometryFromGeomT(multi)

	default:
		return geo.Geometry{}, errors.AssertionFailedf("unexpected shape type: %T", t)
	}
}

// collectionExtractPoints extracts points from an iterator.
func collectionExtractPoints(
	iter geo.GeomTIterator, layout geom.Layout, srid int,
) (*geom.MultiPoint, error) {
	points := geom.NewMultiPoint(layout).SetSRID(srid)
	for {
		if next, hasNext, err := iter.Next(); err != nil {
			return nil, err
		} else if !hasNext {
			break
		} else if point, ok := next.(*geom.Point); ok {
			if err = points.Push(point); err != nil {
				return nil, err
			}
		}
	}
	return points, nil
}

// collectionExtractLineStrings extracts line strings from an iterator.
func collectionExtractLineStrings(
	iter geo.GeomTIterator, layout geom.Layout, srid int,
) (*geom.MultiLineString, error) {
	lineStrings := geom.NewMultiLineString(layout).SetSRID(srid)
	for {
		if next, hasNext, err := iter.Next(); err != nil {
			return nil, err
		} else if !hasNext {
			break
		} else if lineString, ok := next.(*geom.LineString); ok {
			if err = lineStrings.Push(lineString); err != nil {
				return nil, err
			}
		}
	}
	return lineStrings, nil
}

// collectionExtractPolygons extracts polygons from an iterator.
func collectionExtractPolygons(
	iter geo.GeomTIterator, layout geom.Layout, srid int,
) (*geom.MultiPolygon, error) {
	polygons := geom.NewMultiPolygon(layout).SetSRID(srid)
	for {
		if next, hasNext, err := iter.Next(); err != nil {
			return nil, err
		} else if !hasNext {
			break
		} else if polygon, ok := next.(*geom.Polygon); ok {
			if err = polygons.Push(polygon); err != nil {
				return nil, err
			}
		}
	}
	return polygons, nil
}

// CollectionHomogenize returns the simplest representation of a collection.
func CollectionHomogenize(g geo.Geometry) (geo.Geometry, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	srid := t.SRID()
	t, err = collectionHomogenizeGeomT(t)
	if err != nil {
		return geo.Geometry{}, err
	}
	if srid != 0 {
		geo.AdjustGeomTSRID(t, geopb.SRID(srid))
	}
	return geo.MakeGeometryFromGeomT(t)
}

// collectionHomogenizeGeomT homogenizes a geom.T collection.
func collectionHomogenizeGeomT(t geom.T) (geom.T, error) {
	switch t := t.(type) {
	case *geom.Point, *geom.LineString, *geom.Polygon:
		return t, nil

	case *geom.MultiPoint:
		if t.NumPoints() == 1 {
			return t.Point(0), nil
		}
		return t, nil

	case *geom.MultiLineString:
		if t.NumLineStrings() == 1 {
			return t.LineString(0), nil
		}
		return t, nil

	case *geom.MultiPolygon:
		if t.NumPolygons() == 1 {
			return t.Polygon(0), nil
		}
		return t, nil

	case *geom.GeometryCollection:
		layout := t.Layout()
		if layout == geom.NoLayout && t.Empty() {
			layout = geom.XY
		}
		points := geom.NewMultiPoint(layout)
		linestrings := geom.NewMultiLineString(layout)
		polygons := geom.NewMultiPolygon(layout)
		iter := geo.NewGeomTIterator(t, geo.EmptyBehaviorOmit)
		for {
			next, hasNext, err := iter.Next()
			if err != nil {
				return nil, err
			}
			if !hasNext {
				break
			}
			switch next := next.(type) {
			case *geom.Point:
				err = points.Push(next)
			case *geom.LineString:
				err = linestrings.Push(next)
			case *geom.Polygon:
				err = polygons.Push(next)
			default:
				err = errors.AssertionFailedf("encountered unexpected geometry type: %T", next)
			}
			if err != nil {
				return nil, err
			}
		}
		homog := geom.NewGeometryCollection()
		switch points.NumPoints() {
		case 0:
		case 1:
			if err := homog.Push(points.Point(0)); err != nil {
				return nil, err
			}
		default:
			if err := homog.Push(points); err != nil {
				return nil, err
			}
		}
		switch linestrings.NumLineStrings() {
		case 0:
		case 1:
			if err := homog.Push(linestrings.LineString(0)); err != nil {
				return nil, err
			}
		default:
			if err := homog.Push(linestrings); err != nil {
				return nil, err
			}
		}
		switch polygons.NumPolygons() {
		case 0:
		case 1:
			if err := homog.Push(polygons.Polygon(0)); err != nil {
				return nil, err
			}
		default:
			if err := homog.Push(polygons); err != nil {
				return nil, err
			}
		}

		if homog.NumGeoms() == 1 {
			return homog.Geom(0), nil
		}
		return homog, nil

	default:
		return nil, errors.AssertionFailedf("unknown geometry type: %T", t)
	}
}

// Multi converts the given geometry into a new multi-geometry.
func Multi(g geo.Geometry) (geo.Geometry, error) {
	t, err := g.AsGeomT() // implicitly clones the input
	if err != nil {
		return geo.Geometry{}, err
	}
	switch t := t.(type) {
	case *geom.MultiPoint, *geom.MultiLineString, *geom.MultiPolygon, *geom.GeometryCollection:
		return geo.MakeGeometryFromGeomT(t)
	case *geom.Point:
		multi := geom.NewMultiPoint(t.Layout()).SetSRID(t.SRID())
		if !t.Empty() {
			if err = multi.Push(t); err != nil {
				return geo.Geometry{}, err
			}
		}
		return geo.MakeGeometryFromGeomT(multi)
	case *geom.LineString:
		multi := geom.NewMultiLineString(t.Layout()).SetSRID(t.SRID())
		if !t.Empty() {
			if err = multi.Push(t); err != nil {
				return geo.Geometry{}, err
			}
		}
		return geo.MakeGeometryFromGeomT(multi)
	case *geom.Polygon:
		multi := geom.NewMultiPolygon(t.Layout()).SetSRID(t.SRID())
		if !t.Empty() {
			if err = multi.Push(t); err != nil {
				return geo.Geometry{}, err
			}
		}
		return geo.MakeGeometryFromGeomT(multi)
	default:
		return geo.Geometry{}, errors.AssertionFailedf("unknown geometry type: %T", t)
	}
}
