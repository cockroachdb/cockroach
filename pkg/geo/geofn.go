// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import (
	"encoding/json"

	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// STAreaGeometry exactly matches the semantics of ST_Area.
func STAreaGeometry(g *Geometry) (float64, error) {
	switch f := g.Figure.(type) {
	case *geom.Point:
		return f.Area(), nil
	default:
		return 0.0, errors.Errorf(`unhandled geom type %T: %s`, f, f)
	}
}

// STAreaGeography exactly matches the semantics of ST_Area.
func STAreaGeography(g *Geography) (float64, error) {
	var area float64
	for _, sf := range g.Figure {
		switch f := sf.(type) {
		case s2.Point:
			// No-op.
		default:
			return 0.0, errors.Errorf(`unhandled geog type %T: %s`, f, f)
		}
	}
	return area, nil
}

// STContainsGeography exactly matches the semantics of ST_Contains.
func STContainsGeography(g1, g2 *Geography) (bool, error) {
	sf1, err := assertSingleFigure(g1.Figure)
	if err != nil {
		return false, err
	}
	sf2, err := assertSingleFigure(g2.Figure)
	if err != nil {
		return false, err
	}
	switch f1 := sf1.(type) {
	case s2.Point:
		switch f2 := sf2.(type) {
		case s2.Point:
			return f1.ContainsPoint(f2), nil
		case *s2.Polygon:
			// TODO(dan): Pretty sure this is wrong if polygon is of size 0.
			return false, nil
		}
	case *s2.Polygon:
		switch f2 := sf2.(type) {
		case s2.Point:
			return f1.ContainsPoint(f2), nil
		case *s2.Polygon:
			return f1.Contains(f2), nil
		}
	}
	return false, errors.AssertionFailedf(
		`unhandled feature types %T %T: %v %v`, g1.Figure, g2.Figure, g1, g2)
}

// STAsGeoJSONGeography exactly matches the semantics of ST_AsGeoJSON.
func STAsGeoJSONGeography(g *Geography, properties map[string]interface{}) ([]byte, error) {
	var feature struct {
		Type     string `json:"type"`
		Geometry struct {
			Type        string      `json:"type"`
			Coordinates interface{} `json:"coordinates"`
		} `json:"geometry"`
		Properties map[string]interface{} `json:"properties,omitempty"`
	}
	feature.Type = `Feature`
	feature.Properties = properties
	sf, err := assertSingleFigure(g.Figure)
	if err != nil {
		return nil, err
	}
	switch f := sf.(type) {
	case s2.Point:
		ll := s2.LatLngFromPoint(f)
		feature.Geometry.Type = `Point`
		feature.Geometry.Coordinates = []float64{ll.Lng.Degrees(), ll.Lat.Degrees()}
	case *s2.Polygon:
		if f.NumLoops() != 1 {
			return nil, errors.Errorf(`unimplemented STAsGeoJSON for features with more than one loop`)
		}
		loop := f.Loop(0)
		coords := make([][][]float64, loop.NumVertices()+1)
		for idx, point := range append(loop.Vertices(), loop.Vertex(0)) {
			ll := s2.LatLngFromPoint(point)
			coords[idx] = [][]float64{
				{ll.Lng.Degrees(), ll.Lat.Degrees()},
			}
		}
		feature.Geometry.Type = `Polygon`
		feature.Geometry.Coordinates = coords
	default:
		return nil, errors.AssertionFailedf(`unhandled feature type %T: %v`, g.Figure, g)
	}
	return json.Marshal(feature)
}
