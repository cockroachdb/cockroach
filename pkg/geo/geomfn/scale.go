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

// Scale returns a modified Geometry whose coordinates are multiplied by the factors.
// If there are missing dimensions in factors, the corresponding dimensions are not scaled.
func Scale(geometry *geo.Geometry, factors []float64) (*geo.Geometry, error) {
	if geometry.Empty() {
		return geometry, nil
	}

	g, err := geometry.AsGeomT()
	if err != nil {
		return nil, err
	}

	g, err = scale(g, factors, nil)
	if err != nil {
		return nil, err
	}

	return geo.NewGeometryFromGeomT(g)
}

// ScaleRelativeToOrigin returns a modified Geometry whose coordinates are multiplied by the factors relative to the origin
func ScaleRelativeToOrigin(
	geometry *geo.Geometry, factor *geo.Geometry, origin *geo.Geometry,
) (*geo.Geometry, error) {
	if geometry.Empty() {
		return geometry, nil
	}

	g, err := geometry.AsGeomT()
	if err != nil {
		return nil, err
	}

	factorG, err := factor.AsGeomT()
	if err != nil {
		return nil, err
	}

	_, ok := factorG.(*geom.Point)
	if !ok {
		return nil, errors.Newf("the scaling factor must be a Point")
	}

	originG, err := origin.AsGeomT()
	if err != nil {
		return nil, err
	}

	_, ok = originG.(*geom.Point)
	if !ok {
		return nil, errors.Newf("the false origin must be a Point")
	}

	if factorG.Stride() != originG.Stride() {
		err := geom.ErrStrideMismatch{
			Got:  factorG.Stride(),
			Want: originG.Stride(),
		}
		return nil, errors.Wrap(err, "number of dimensions for the scaling factor and origin must be equal")
	}

	g, err = scale(g, factorG.FlatCoords(), originG)
	if err != nil {
		return nil, err
	}

	return geo.NewGeometryFromGeomT(g)
}

func scale(g geom.T, factors []float64, origin geom.T) (geom.T, error) {
	if geomCollection, ok := g.(*geom.GeometryCollection); ok {
		return scaleCollection(geomCollection, factors, origin)
	}

	newCoords, err := scaleCoords(g, factors, origin)
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

// scaleCoords multiplies g's coordinates relative to origin's coordinates.
// If origin is nil, g's coordinates are just multiplied by the factors.
// Note: M coordinates are not affected.
func scaleCoords(g geom.T, factors []float64, origin geom.T) ([]float64, error) {
	stride := g.Stride()
	if stride < len(factors) {
		err := geom.ErrStrideMismatch{
			Got:  len(factors),
			Want: stride,
		}
		return nil, errors.Wrap(err, "number of factors exceed number of dimensions")
	}

	var originCoords []float64
	if origin != nil {
		originCoords = origin.FlatCoords()
	} else {
		originCoords = make([]float64, len(factors))
	}

	coords := g.FlatCoords()
	newCoords := make([]float64, len(coords))

	for i := 0; i < len(coords); i += stride {
		newCoords[i] = coords[i]*factors[0] - originCoords[0]
		newCoords[i+1] = coords[i+1]*factors[1] - originCoords[1]

		z := g.Layout().ZIndex()
		if z != -1 {
			newCoords[i+z] = coords[i+z]

			if len(factors) > z {
				newCoords[i+z] *= factors[z]
				newCoords[i+z] -= originCoords[z]
			}
		}

		// m coords are only copied over
		m := g.Layout().MIndex()
		if m != -1 {
			newCoords[i+m] = factors[m]
		}
	}

	return newCoords, nil
}

// scaleCollection iterates through a GeometryCollection and calls scale() on each item.
func scaleCollection(
	geomCollection *geom.GeometryCollection, factors []float64, origin geom.T,
) (*geom.GeometryCollection, error) {
	res := geom.NewGeometryCollection()
	for _, subG := range geomCollection.Geoms() {
		subGeom, err := scale(subG, factors, origin)
		if err != nil {
			return nil, err
		}

		if err := res.Push(subGeom); err != nil {
			return nil, err
		}
	}
	return res, nil
}
