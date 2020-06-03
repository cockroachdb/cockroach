// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geosegmentize

import (
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// SegmentizeGeom returns a modified geom.T having no segment longer
// than the given maximum segment length.
// segmentMaxAngleOrLength represents two different things depending
// on the object, which is about to segmentize as in case of geography
// it represents maximum segment angle whereas, in case of geometry it
// represents maximum segment distance.
// segmentizeCoords represents the function's definition which allows
// us to segmentize given two-points. We have to specify segmentizeCoords
// explicitly, as the algorithm for segmentization is significantly
// different for geometry and geography.
func SegmentizeGeom(
	geometry geom.T,
	segmentMaxAngleOrLength float64,
	segmentizeCoords func(geom.Coord, geom.Coord, float64) []float64,
) (geom.T, error) {
	if geometry.Empty() {
		return geometry, nil
	}
	switch geometry := geometry.(type) {
	case *geom.Point, *geom.MultiPoint:
		return geometry, nil
	case *geom.LineString:
		var allFlatCoordinates []float64
		for pointIdx := 1; pointIdx < geometry.NumCoords(); pointIdx++ {
			allFlatCoordinates = append(
				allFlatCoordinates,
				segmentizeCoords(geometry.Coord(pointIdx-1), geometry.Coord(pointIdx), segmentMaxAngleOrLength)...,
			)
		}
		// Appending end point as it wasn't included in the iteration of coordinates.
		allFlatCoordinates = append(allFlatCoordinates, geometry.Coord(geometry.NumCoords()-1)...)
		return geom.NewLineStringFlat(geom.XY, allFlatCoordinates).SetSRID(geometry.SRID()), nil
	case *geom.MultiLineString:
		segMultiLine := geom.NewMultiLineString(geom.XY).SetSRID(geometry.SRID())
		for lineIdx := 0; lineIdx < geometry.NumLineStrings(); lineIdx++ {
			l, err := SegmentizeGeom(geometry.LineString(lineIdx), segmentMaxAngleOrLength, segmentizeCoords)
			if err != nil {
				return nil, err
			}
			err = segMultiLine.Push(l.(*geom.LineString))
			if err != nil {
				return nil, err
			}
		}
		return segMultiLine, nil
	case *geom.LinearRing:
		var allFlatCoordinates []float64
		for pointIdx := 1; pointIdx < geometry.NumCoords(); pointIdx++ {
			allFlatCoordinates = append(
				allFlatCoordinates,
				segmentizeCoords(geometry.Coord(pointIdx-1), geometry.Coord(pointIdx), segmentMaxAngleOrLength)...,
			)
		}
		// Appending end point as it wasn't included in the iteration of coordinates.
		allFlatCoordinates = append(allFlatCoordinates, geometry.Coord(geometry.NumCoords()-1)...)
		return geom.NewLinearRingFlat(geom.XY, allFlatCoordinates).SetSRID(geometry.SRID()), nil
	case *geom.Polygon:
		segPolygon := geom.NewPolygon(geom.XY).SetSRID(geometry.SRID())
		for loopIdx := 0; loopIdx < geometry.NumLinearRings(); loopIdx++ {
			l, err := SegmentizeGeom(geometry.LinearRing(loopIdx), segmentMaxAngleOrLength, segmentizeCoords)
			if err != nil {
				return nil, err
			}
			err = segPolygon.Push(l.(*geom.LinearRing))
			if err != nil {
				return nil, err
			}
		}
		return segPolygon, nil
	case *geom.MultiPolygon:
		segMultiPolygon := geom.NewMultiPolygon(geom.XY).SetSRID(geometry.SRID())
		for polygonIdx := 0; polygonIdx < geometry.NumPolygons(); polygonIdx++ {
			p, err := SegmentizeGeom(geometry.Polygon(polygonIdx), segmentMaxAngleOrLength, segmentizeCoords)
			if err != nil {
				return nil, err
			}
			err = segMultiPolygon.Push(p.(*geom.Polygon))
			if err != nil {
				return nil, err
			}
		}
		return segMultiPolygon, nil
	case *geom.GeometryCollection:
		segGeomCollection := geom.NewGeometryCollection().SetSRID(geometry.SRID())
		for geoIdx := 0; geoIdx < geometry.NumGeoms(); geoIdx++ {
			g, err := SegmentizeGeom(geometry.Geom(geoIdx), segmentMaxAngleOrLength, segmentizeCoords)
			if err != nil {
				return nil, err
			}
			err = segGeomCollection.Push(g)
			if err != nil {
				return nil, err
			}
		}
		return segGeomCollection, nil
	}
	return nil, errors.Newf("unknown type: %T", geometry)
}
