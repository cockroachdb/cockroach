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
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/twpayne/go-geom"
)

// Segmentize returns a modified geom.T having no segment longer
// than the given maximum segment length.
// segmentMaxAngleOrLength represents two different things depending
// on the object, which is about to segmentize as in case of geography
// it represents maximum segment angle whereas, in case of geometry it
// represents maximum segment distance.
// segmentizeCoords represents the function's definition which allows
// us to segmentize given two-points. We have to specify segmentizeCoords
// explicitly, as the algorithm for segmentation is significantly
// different for geometry and geography.
func Segmentize(
	geometry geom.T,
	segmentMaxAngleOrLength float64,
	segmentizeCoords func(geom.Coord, geom.Coord, float64) ([]float64, error),
) (geom.T, error) {
	if geometry.Empty() {
		return geometry, nil
	}
	layout := geometry.Layout()
	switch geometry := geometry.(type) {
	case *geom.Point, *geom.MultiPoint:
		return geometry, nil
	case *geom.LineString:
		var allFlatCoordinates []float64
		for pointIdx := 1; pointIdx < geometry.NumCoords(); pointIdx++ {
			coords, err := segmentizeCoords(geometry.Coord(pointIdx-1), geometry.Coord(pointIdx), segmentMaxAngleOrLength)
			if err != nil {
				return nil, err
			}
			allFlatCoordinates = append(
				allFlatCoordinates,
				coords...,
			)
		}
		// Appending end point as it wasn't included in the iteration of coordinates.
		allFlatCoordinates = append(allFlatCoordinates, geometry.Coord(geometry.NumCoords()-1)...)
		return geom.NewLineStringFlat(layout, allFlatCoordinates).SetSRID(geometry.SRID()), nil
	case *geom.MultiLineString:
		segMultiLine := geom.NewMultiLineString(layout).SetSRID(geometry.SRID())
		for lineIdx := 0; lineIdx < geometry.NumLineStrings(); lineIdx++ {
			l, err := Segmentize(geometry.LineString(lineIdx), segmentMaxAngleOrLength, segmentizeCoords)
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
			coords, err := segmentizeCoords(geometry.Coord(pointIdx-1), geometry.Coord(pointIdx), segmentMaxAngleOrLength)
			if err != nil {
				return nil, err
			}
			allFlatCoordinates = append(
				allFlatCoordinates,
				coords...,
			)
		}
		// Appending end point as it wasn't included in the iteration of coordinates.
		allFlatCoordinates = append(allFlatCoordinates, geometry.Coord(geometry.NumCoords()-1)...)
		return geom.NewLinearRingFlat(layout, allFlatCoordinates).SetSRID(geometry.SRID()), nil
	case *geom.Polygon:
		segPolygon := geom.NewPolygon(layout).SetSRID(geometry.SRID())
		for loopIdx := 0; loopIdx < geometry.NumLinearRings(); loopIdx++ {
			l, err := Segmentize(geometry.LinearRing(loopIdx), segmentMaxAngleOrLength, segmentizeCoords)
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
		segMultiPolygon := geom.NewMultiPolygon(layout).SetSRID(geometry.SRID())
		for polygonIdx := 0; polygonIdx < geometry.NumPolygons(); polygonIdx++ {
			p, err := Segmentize(geometry.Polygon(polygonIdx), segmentMaxAngleOrLength, segmentizeCoords)
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
		err := segGeomCollection.SetLayout(layout)
		if err != nil {
			return nil, err
		}
		for geoIdx := 0; geoIdx < geometry.NumGeoms(); geoIdx++ {
			g, err := Segmentize(geometry.Geom(geoIdx), segmentMaxAngleOrLength, segmentizeCoords)
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
	return nil, pgerror.Newf(pgcode.InvalidParameterValue, "unknown type: %T", geometry)
}

// CheckSegmentizeValidNumPoints checks whether segmentize would break down into
// too many points or NaN points.
func CheckSegmentizeValidNumPoints(numPoints float64, a geom.Coord, b geom.Coord) error {
	if math.IsNaN(numPoints) {
		return pgerror.Newf(pgcode.InvalidParameterValue, "cannot segmentize into %f points", numPoints)
	}
	if numPoints > float64(geo.MaxAllowedSplitPoints) {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"attempting to segmentize into too many coordinates; need %s points between %v and %v, max %d",
			strings.TrimRight(strconv.FormatFloat(numPoints, 'f', -1, 64), "."),
			a,
			b,
			geo.MaxAllowedSplitPoints,
		)
	}
	return nil
}
