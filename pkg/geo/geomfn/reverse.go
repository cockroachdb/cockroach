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

// Reverse returns a modified geometry by reversing the order of its vertexes
func Reverse(geometry geo.Geometry) (geo.Geometry, error) {
	g, err := geometry.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	g, err = reverse(g)
	if err != nil {
		return geo.Geometry{}, err
	}

	return geo.MakeGeometryFromGeomT(g)
}

func reverse(g geom.T) (geom.T, error) {
	if geomCollection, ok := g.(*geom.GeometryCollection); ok {
		return reverseCollection(geomCollection)
	}

	switch t := g.(type) {
	case *geom.Point, *geom.MultiPoint: // cases where reverse does change the order
		return g, nil
	case *geom.LineString:
		g = geom.NewLineStringFlat(t.Layout(), reverseCoords(g.FlatCoords(), g.Stride())).SetSRID(g.SRID())
	case *geom.Polygon:
		g = geom.NewPolygonFlat(t.Layout(), reverseCoords(g.FlatCoords(), g.Stride()), t.Ends()).SetSRID(g.SRID())
	case *geom.MultiLineString:
		g = geom.NewMultiLineStringFlat(t.Layout(), reverseMulti(g, t.Ends()), t.Ends()).SetSRID(g.SRID())
	case *geom.MultiPolygon:
		var ends []int
		for _, e := range t.Endss() {
			ends = append(ends, e...)
		}
		g = geom.NewMultiPolygonFlat(t.Layout(), reverseMulti(g, ends), t.Endss()).SetSRID(g.SRID())

	default:
		return nil, geom.ErrUnsupportedType{Value: g}
	}

	return g, nil
}

func reverseCoords(coords []float64, stride int) []float64 {
	for i := 0; i < len(coords)/2; i += stride {
		for j := 0; j < stride; j++ {
			coords[i+j], coords[len(coords)-stride-i+j] = coords[len(coords)-stride-i+j], coords[i+j]
		}
	}

	return coords
}

// reverseMulti handles reversing coordinates of MULTI* geometries with nested sub-structures
func reverseMulti(g geom.T, ends []int) []float64 {
	coords := g.FlatCoords()
	prevEnd := 0

	for _, end := range ends {
		copy(
			coords[prevEnd:end],
			reverseCoords(coords[prevEnd:end], g.Stride()),
		)
		prevEnd = end
	}

	return coords
}

// reverseCollection iterates through a GeometryCollection and calls reverse() on each geometry.
func reverseCollection(geomCollection *geom.GeometryCollection) (*geom.GeometryCollection, error) {
	res := geom.NewGeometryCollection()
	for _, subG := range geomCollection.Geoms() {
		subGeom, err := reverse(subG)
		if err != nil {
			return nil, err
		}

		if err := res.Push(subGeom); err != nil {
			return nil, err
		}
	}
	return res, nil
}
