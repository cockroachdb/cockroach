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
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// BoundingBoxFromGeom returns a bounding box from a given geom.T.
func BoundingBoxFromGeom(g geom.T) (*geopb.BoundingBox, error) {
	bbox := geopb.NewBoundingBox()
	if g.Empty() {
		return nil, nil
	}
	switch g := g.(type) {
	case *geom.Point:
		bbox.Update(g.X(), g.Y())
	case *geom.LineString:
		for _, coord := range g.Coords() {
			bbox.Update(coord.X(), coord.Y())
		}
	case *geom.Polygon:
		for _, coord := range g.LinearRing(0).Coords() {
			bbox.Update(coord.X(), coord.Y())
		}
	case *geom.MultiPoint:
		for i := 0; i < g.NumPoints(); i++ {
			point := g.Point(i)
			bbox.Update(point.X(), point.Y())
		}
	case *geom.MultiLineString:
		for i := 0; i < g.NumLineStrings(); i++ {
			lineString := g.LineString(i)
			for _, coord := range lineString.Coords() {
				bbox.Update(coord.X(), coord.Y())
			}
		}
	case *geom.MultiPolygon:
		for i := 0; i < g.NumPolygons(); i++ {
			polygon := g.Polygon(i)
			for _, coord := range polygon.LinearRing(0).Coords() {
				bbox.Update(coord.X(), coord.Y())
			}
		}
	case *geom.GeometryCollection:
		for i := 0; i < g.NumGeoms(); i++ {
			innerBBox, err := BoundingBoxFromGeom(g.Geom(i))
			if err != nil {
				return nil, err
			}
			if innerBBox == nil {
				return nil, nil
			}
			bbox.Update(innerBBox.MinX, innerBBox.MinY)
			bbox.Update(innerBBox.MaxX, innerBBox.MaxY)
		}
	default:
		return nil, errors.Newf("unknown geom type: %T", g)
	}
	return bbox, nil
}
