// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geofn contains implementations the logic of the PostGIS/OGC functions
// over GEOMETRY and GEOGRAPHY types.
package geofn

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
)

// Area exactly matches the semantics of ST_Area.
func Area(r geo.Region) float64 {
	switch r := r.Region.(type) {
	case s2.Point:
		return 0.0
	case s2.Rect:
		return r.Area()
	default:
		panic(errors.AssertionFailedf(`unhandled feature type %T: %v`, r, r))
	}
}

// Contains exactly matches the semantics of ST_Contains.
func Contains(r1, r2 geo.Region) bool {
	// WIP Hacks this is a terrible implementation.
	switch r1 := r1.Region.(type) {
	case s2.Point:
		switch r2 := r2.Region.(type) {
		case s2.Point:
			return r1.ContainsPoint(r2)
		case s2.Rect:
			// WIP pretty sure this is wrong if rect is of size 0.
			return false
		}
	case s2.Rect:
		switch r2 := r2.Region.(type) {
		case s2.Point:
			return r1.ContainsPoint(r2)
		case s2.Rect:
			return r1.Contains(r2)
		}
	}
	panic(errors.AssertionFailedf(`unhandled feature types %T %T: %v %v`,
		r1.Region, r2.Region, r1.Region, r2.Region))
}

// AsGeoJSON exactly matches the semantics of ST_AsGeoJSON.
func AsGeoJSON(r geo.Region, properties map[string]interface{}) ([]byte, error) {
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
	switch r := r.Region.(type) {
	case s2.Point:
		ll := s2.LatLngFromPoint(r)
		feature.Geometry.Type = `Point`
		feature.Geometry.Coordinates = []float64{ll.Lng.Degrees(), ll.Lat.Degrees()}
	case s2.Rect:
		feature.Geometry.Type = `Polygon`
		feature.Geometry.Coordinates = [][][]float64{{
			{r.Vertex(0).Lng.Degrees(), r.Vertex(0).Lat.Degrees()},
			{r.Vertex(1).Lng.Degrees(), r.Vertex(1).Lat.Degrees()},
			{r.Vertex(2).Lng.Degrees(), r.Vertex(2).Lat.Degrees()},
			{r.Vertex(3).Lng.Degrees(), r.Vertex(3).Lat.Degrees()},
			{r.Vertex(0).Lng.Degrees(), r.Vertex(0).Lat.Degrees()},
		}}
	default:
		return nil, errors.AssertionFailedf(`unhandled feature type %T: %v`, r, r)
	}
	return json.Marshal(feature)
}
