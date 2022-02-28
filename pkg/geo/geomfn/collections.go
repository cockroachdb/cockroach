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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Collect collects two geometries into a GeometryCollection or multi-type.
//
// This is the binary version of `ST_Collect()`, but since it's not possible to
// have an aggregate and non-aggregate function with the same name (different
// args), this is not used. Code is left behind for when we add support for
// this. Be sure to handle NULL args when adding the builtin for this, where it
// should return the non-NULL arg unused like PostGIS.
func Collect(g1 geo.Geometry, g2 geo.Geometry) (geo.Geometry, error) {
	t1, err := g1.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	t2, err := g2.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	// First, try to generate multi-types
	switch t1 := t1.(type) {
	case *geom.Point:
		if t2, ok := t2.(*geom.Point); ok {
			multi := geom.NewMultiPoint(t1.Layout()).SetSRID(t1.SRID())
			if err := multi.Push(t1); err != nil {
				return geo.Geometry{}, err
			}
			if err := multi.Push(t2); err != nil {
				return geo.Geometry{}, err
			}
			return geo.MakeGeometryFromGeomT(multi)
		}
	case *geom.LineString:
		if t2, ok := t2.(*geom.LineString); ok {
			multi := geom.NewMultiLineString(t1.Layout()).SetSRID(t1.SRID())
			if err := multi.Push(t1); err != nil {
				return geo.Geometry{}, err
			}
			if err := multi.Push(t2); err != nil {
				return geo.Geometry{}, err
			}
			return geo.MakeGeometryFromGeomT(multi)
		}
	case *geom.Polygon:
		if t2, ok := t2.(*geom.Polygon); ok {
			multi := geom.NewMultiPolygon(t1.Layout()).SetSRID(t1.SRID())
			if err := multi.Push(t1); err != nil {
				return geo.Geometry{}, err
			}
			if err := multi.Push(t2); err != nil {
				return geo.Geometry{}, err
			}
			return geo.MakeGeometryFromGeomT(multi)
		}
	}

	// Otherwise, just put them in a collection
	gc := geom.NewGeometryCollection().SetSRID(t1.SRID())
	if err := gc.Push(t1); err != nil {
		return geo.Geometry{}, err
	}
	if err := gc.Push(t2); err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(gc)
}

// CollectionExtract returns a (multi-)geometry consisting only of the specified type.
// The type can only be point, line, or polygon.
func CollectionExtract(g geo.Geometry, shapeType geopb.ShapeType) (geo.Geometry, error) {
	switch shapeType {
	case geopb.ShapeType_Point, geopb.ShapeType_LineString, geopb.ShapeType_Polygon:
	default:
		return geo.Geometry{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"only point, linestring and polygon may be extracted (got %s)",
			shapeType,
		)
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
			return geo.Geometry{}, pgerror.Newf(
				pgcode.InvalidParameterValue,
				"unexpected shape type %v",
				shapeType.String(),
			)
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
			return geo.Geometry{}, pgerror.Newf(
				pgcode.InvalidParameterValue,
				"unexpected shape type %v",
				shapeType.MultiType().String(),
			)
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

// ForceCollection converts the input into a GeometryCollection.
func ForceCollection(g geo.Geometry) (geo.Geometry, error) {
	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	t, err = forceCollectionFromGeomT(t)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.MakeGeometryFromGeomT(t)
}

// forceCollectionFromGeomT converts a geom.T into a geom.GeometryCollection.
func forceCollectionFromGeomT(t geom.T) (geom.T, error) {
	gc := geom.NewGeometryCollection().SetSRID(t.SRID())
	switch t := t.(type) {
	case *geom.Point, *geom.LineString, *geom.Polygon:
		if err := gc.Push(t); err != nil {
			return nil, err
		}
	case *geom.MultiPoint:
		for i := 0; i < t.NumPoints(); i++ {
			if err := gc.Push(t.Point(i)); err != nil {
				return nil, err
			}
		}
	case *geom.MultiLineString:
		for i := 0; i < t.NumLineStrings(); i++ {
			if err := gc.Push(t.LineString(i)); err != nil {
				return nil, err
			}
		}
	case *geom.MultiPolygon:
		for i := 0; i < t.NumPolygons(); i++ {
			if err := gc.Push(t.Polygon(i)); err != nil {
				return nil, err
			}
		}
	case *geom.GeometryCollection:
		gc = t
	default:
		return nil, errors.AssertionFailedf("unknown geometry type: %T", t)
	}
	return gc, nil
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
