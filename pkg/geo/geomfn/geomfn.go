// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geomfn contains functions that are used for geometry-based builtins.
package geomfn

import "github.com/twpayne/go-geom"

// applyCoordFunc applies a function on src to copy onto dst.
// Both slices represent a single Coord within the FlatCoord array.
type applyCoordFunc func(l geom.Layout, dst []float64, src []float64) error

// applyOnCoords applies the applyCoordFunc on each coordinate, returning
// a new array for the coordinates.
func applyOnCoords(flatCoords []float64, l geom.Layout, f applyCoordFunc) ([]float64, error) {
	newCoords := make([]float64, len(flatCoords))
	for i := 0; i < len(flatCoords); i += l.Stride() {
		if err := f(l, newCoords[i:i+l.Stride()], flatCoords[i:i+l.Stride()]); err != nil {
			return nil, err
		}
	}
	return newCoords, nil
}

// applyOnCoordsForGeomT applies the applyCoordFunc on each coordinate in the geom.T,
// returning a copied over geom.T.
func applyOnCoordsForGeomT(g geom.T, f applyCoordFunc) (geom.T, error) {
	if geomCollection, ok := g.(*geom.GeometryCollection); ok {
		return applyOnCoordsForGeometryCollection(geomCollection, f)
	}

	newCoords, err := applyOnCoords(g.FlatCoords(), g.Layout(), f)
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

// applyOnCoordsForGeometryCollection applies the applyCoordFunc on each coordinate
// inside a geometry collection, returning a copied over geom.T.
func applyOnCoordsForGeometryCollection(
	geomCollection *geom.GeometryCollection, f applyCoordFunc,
) (*geom.GeometryCollection, error) {
	res := geom.NewGeometryCollection()
	for _, subG := range geomCollection.Geoms() {
		subGeom, err := applyOnCoordsForGeomT(subG, f)
		if err != nil {
			return nil, err
		}

		if err := res.Push(subGeom); err != nil {
			return nil, err
		}
	}
	return res, nil
}
