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
		g = geom.NewMultiPointFlat(t.Layout(), newCoords, geom.NewMultiPointFlatOptionWithEnds(t.Ends())).SetSRID(g.SRID())
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

// removeConsecutivePointsFromGeomT removes duplicate consecutive points from a given geom.T.
// If the resultant geometry is invalid, it will be EMPTY.
func removeConsecutivePointsFromGeomT(t geom.T) (geom.T, error) {
	if t.Empty() {
		return t, nil
	}
	switch t := t.(type) {
	case *geom.Point, *geom.MultiPoint:
		return t, nil
	case *geom.LineString:
		newCoords := make([]float64, 0, len(t.FlatCoords()))
		newCoords = append(newCoords, t.Coord(0)...)
		for i := 1; i < t.NumCoords(); i++ {
			if !t.Coord(i).Equal(t.Layout(), t.Coord(i-1)) {
				newCoords = append(newCoords, t.Coord(i)...)
			}
		}
		if len(newCoords) < t.Stride()*2 {
			newCoords = newCoords[:0]
		}
		return geom.NewLineStringFlat(t.Layout(), newCoords).SetSRID(t.SRID()), nil
	case *geom.Polygon:
		ret := geom.NewPolygon(t.Layout()).SetSRID(t.SRID())
		for ringIdx := 0; ringIdx < t.NumLinearRings(); ringIdx++ {
			ring := t.LinearRing(ringIdx)
			newCoords := make([]float64, 0, len(ring.FlatCoords()))
			newCoords = append(newCoords, ring.Coord(0)...)
			for i := 1; i < ring.NumCoords(); i++ {
				if !ring.Coord(i).Equal(ring.Layout(), ring.Coord(i-1)) {
					newCoords = append(newCoords, ring.Coord(i)...)
				}
			}
			if len(newCoords) < t.Stride()*4 {
				// If the outer ring is invalid, the polygon should be entirely empty.
				if ringIdx == 0 {
					return ret, nil
				}
				// Ignore any holes.
				continue
			}
			if err := ret.Push(geom.NewLinearRingFlat(t.Layout(), newCoords)); err != nil {
				return nil, err
			}
		}
		return ret, nil
	case *geom.MultiLineString:
		ret := geom.NewMultiLineString(t.Layout()).SetSRID(t.SRID())
		for i := 0; i < t.NumLineStrings(); i++ {
			ls, err := removeConsecutivePointsFromGeomT(t.LineString(i))
			if err != nil {
				return nil, err
			}
			if ls.Empty() {
				continue
			}
			if err := ret.Push(ls.(*geom.LineString)); err != nil {
				return nil, err
			}
		}
		return ret, nil
	case *geom.MultiPolygon:
		ret := geom.NewMultiPolygon(t.Layout()).SetSRID(t.SRID())
		for i := 0; i < t.NumPolygons(); i++ {
			p, err := removeConsecutivePointsFromGeomT(t.Polygon(i))
			if err != nil {
				return nil, err
			}
			if p.Empty() {
				continue
			}
			if err := ret.Push(p.(*geom.Polygon)); err != nil {
				return nil, err
			}
		}
		return ret, nil
	case *geom.GeometryCollection:
		ret := geom.NewGeometryCollection().SetSRID(t.SRID())
		for i := 0; i < t.NumGeoms(); i++ {
			g, err := removeConsecutivePointsFromGeomT(t.Geom(i))
			if err != nil {
				return nil, err
			}
			if g.Empty() {
				continue
			}
			if err := ret.Push(g); err != nil {
				return nil, err
			}
		}
		return ret, nil
	}
	return nil, geom.ErrUnsupportedType{Value: t}
}
