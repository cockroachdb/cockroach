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

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// flattenGeometry flattens a geo.Geometry object.
func flattenGeometry(g *geo.Geometry) ([]geom.T, error) {
	f, err := g.AsGeomT()
	if err != nil {
		return nil, err
	}
	return flattenGeomT(f)
}

// flattenGeomT decomposes geom.T collections to individual geom.T components.
func flattenGeomT(g geom.T) ([]geom.T, error) {
	switch g := g.(type) {
	case *geom.Point:
		return []geom.T{g}, nil
	case *geom.LineString:
		return []geom.T{g}, nil
	case *geom.Polygon:
		return []geom.T{g}, nil
	case *geom.MultiPoint:
		ret := make([]geom.T, g.NumPoints())
		for i := 0; i < g.NumPoints(); i++ {
			ret[i] = g.Point(i)
		}
		return ret, nil
	case *geom.MultiLineString:
		ret := make([]geom.T, g.NumLineStrings())
		for i := 0; i < g.NumLineStrings(); i++ {
			ret[i] = g.LineString(i)
		}
		return ret, nil
	case *geom.MultiPolygon:
		ret := make([]geom.T, g.NumPolygons())
		for i := 0; i < g.NumPolygons(); i++ {
			ret[i] = g.Polygon(i)
		}
		return ret, nil
	case *geom.GeometryCollection:
		ret := make([]geom.T, g.NumGeoms())
		for i := 0; i < g.NumGeoms(); i++ {
			ret[i] = g.Geom(i)
		}
		return ret, nil
	}
	return nil, errors.Newf("unknown geom: %T", g)
}
