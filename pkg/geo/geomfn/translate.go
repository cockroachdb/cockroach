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
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Translate returns a modified Geometry whose coordinates are incremented or decremented by the deltas.
func Translate(geometry *geo.Geometry, deltas []float64) (*geo.Geometry, error) {
	g, err := geometry.AsGeomT()
	if err != nil {
		return nil, err
	}

	g, err = translate(g, deltas)
	if err != nil {
		return nil, err
	}

	res, err := geo.NewGeometryFromGeomT(g)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func translate(g geom.T, deltas []float64) (geom.T, error) {
	if geomCollection, ok := g.(*geom.GeometryCollection); ok {
		return translateCollection(geomCollection, deltas)
	}

	newCoords, err := translateCoords(g.FlatCoords(), g.Layout(), deltas)
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

// translateCoords increments or decrements the given flatCoords array and returns the translated coordinates.
// Note: M coordinates are not affected.
func translateCoords(
	flatCoords []float64, layout geom.Layout, deltas []float64,
) ([]float64, error) {
	if layout.Stride() != len(deltas) {
		err := geom.ErrStrideMismatch{
			Got:  len(deltas),
			Want: layout.Stride(),
		}
		return nil, errors.Wrap(err, "translating coordinates")
	}

	newCoords := make([]float64, len(flatCoords))

	for i := 0; i < len(flatCoords); i += layout.Stride() {
		newCoords[i] = flatCoords[i] + deltas[0]
		newCoords[i+1] = flatCoords[i+1] + deltas[1]

		z := layout.ZIndex()
		if z != -1 {
			newCoords[i+z] = flatCoords[i+z] + deltas[z]
		}

		// m coords are only copied over
		m := layout.MIndex()
		if m != -1 {
			newCoords[i+m] = deltas[m]
		}
	}

	return newCoords, nil
}

// translateCollection iterates through a GeometryCollection and calls Translate() on each item.
func translateCollection(
	geomCollection *geom.GeometryCollection, deltas []float64,
) (*geom.GeometryCollection, error) {
	res := geom.NewGeometryCollection()
	for _, subG := range geomCollection.Geoms() {
		subGeom, err := translate(subG, deltas)
		if err != nil {
			return nil, err
		}

		if err := res.Push(subGeom); err != nil {
			return nil, err
		}
	}
	return res, nil
}
