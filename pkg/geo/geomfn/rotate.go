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
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/twpayne/go-geom"
)

// Rotate returns a modified Geometry whose coordinates are rotated
// around the origin by a rotation angle.
func Rotate(geometry geo.Geometry, rotRadians float64) (geo.Geometry, error) {
	g, err := geometry.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	g, err = rotateGeomT(g, rotRadians)
	if err != nil {
		return geo.Geometry{}, err
	}

	return geo.MakeGeometryFromGeomT(g)
}

// rotateGeomT returns a modified geom.T whose coordinates are
// rotated around the origin by a rotation angle.
func rotateGeomT(g geom.T, rotRadians float64) (geom.T, error) {
	if geomCollection, ok := g.(*geom.GeometryCollection); ok {
		return rotateCollection(geomCollection, rotRadians)
	}

	newCoords, err := rotateCoords(g, rotRadians)
	if err != nil {
		return nil, err
	}

	switch t := g.(type) {
	case *geom.Point:
		g = geom.NewPointFlat(t.Layout(), newCoords).SetSRID(g.SRID())
	case *geom.LineString:
		g = geom.NewLineStringFlat(t.Layout(), newCoords).SetSRID(g.SRID())
	case *geom.Polygon:
		g = geom.NewPolygonFlat(t.Layout(), newCoords, t.Ends()).SetSRID(g.SRID())
	case *geom.MultiPoint:
		g = geom.NewMultiPointFlat(t.Layout(), newCoords).SetSRID(g.SRID())
	case *geom.MultiLineString:
		g = geom.NewMultiLineStringFlat(t.Layout(), newCoords, t.Ends()).SetSRID(g.SRID())
	case *geom.MultiPolygon:
		g = geom.NewMultiPolygonFlat(t.Layout(), newCoords, t.Endss()).SetSRID(g.SRID())
	default:
		return nil, geom.ErrUnsupportedType{Value: g}
	}

	return g, nil
}

// rotateCoords rotates the coordinates around the origin by
// a rotation angle.
func rotateCoords(g geom.T, rotRadians float64) ([]float64, error) {
	stride := g.Stride()
	coords := g.FlatCoords()
	layout := g.Layout()

	newCoords := make([]float64, len(coords))

	for i := 0; i < len(coords); i += stride {
		newCoords[i] = coords[i]*math.Cos(rotRadians) - coords[i+1]*math.Sin(rotRadians)
		newCoords[i+1] = coords[i]*math.Sin(rotRadians) + coords[i+1]*math.Cos(rotRadians)

		z := layout.ZIndex()
		if z != -1 {
			newCoords[i+z] = coords[i+z]
		}

		// m coords are only copied over
		m := layout.MIndex()
		if m != -1 {
			newCoords[i+m] = coords[i+m]
		}
	}

	return newCoords, nil
}

// rotateCollection iterates through a GeometryCollection and
// calls rotateGeomT() on each item.
func rotateCollection(
	geomCollection *geom.GeometryCollection, rotRadians float64,
) (*geom.GeometryCollection, error) {
	res := geom.NewGeometryCollection()
	for _, subG := range geomCollection.Geoms() {
		subGeom, err := rotateGeomT(subG, rotRadians)
		if err != nil {
			return nil, err
		}

		if err := res.Push(subGeom); err != nil {
			return nil, err
		}
	}
	return res, nil
}
