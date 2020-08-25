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
	"github.com/twpayne/go-geom"
)

// FlipCoordinates returns a modified Geometry whose X, Y coordinates are flipped.
func FlipCoordinates(geometry *geo.Geometry) (*geo.Geometry, error) {
	if geometry.Empty() {
		return geometry, nil
	}

	g, err := geometry.AsGeomT()
	if err != nil {
		return nil, err
	}

	g, err = flipCoordinates(g)
	if err != nil {
		return nil, err
	}

	return geo.NewGeometryFromGeomT(g)
}

func flipCoordinates(g geom.T) (geom.T, error) {
	if geomCollection, ok := g.(*geom.GeometryCollection); ok {
		return flipCollection(geomCollection)
	}

	newCoords, err := flipCoords(g)
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

// flipCoords swaps X,Y coordinates.
// M and Z coordinates are maintained
func flipCoords(g geom.T) ([]float64, error) {
	stride := g.Stride()
	coords := g.FlatCoords()

	for i := 0; i < len(coords); i += stride {
		coords[i], coords[i+1] = coords[i+1], coords[i]
	}

	return coords, nil
}

// flipCollection iterates through a GeometryCollection and calls flipCoordinates() on each item.
func flipCollection(geomCollection *geom.GeometryCollection) (*geom.GeometryCollection, error) {
	res := geom.NewGeometryCollection()

	for _, subG := range geomCollection.Geoms() {
		subGeom, err := flipCoordinates(subG)
		if err != nil {
			return nil, err
		}

		if err := res.Push(subGeom); err != nil {
			return nil, err
		}
	}

	return res, nil
}
