// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// Segmentize return modified Geography having no segment longer
// that given maximum segment length.
func Segmentize(geography *geo.Geography, segmentMaxLength float64) (*geo.Geography, error) {
	spheroid := geographiclib.WGS84Spheroid
	// Convert segmentMaxLength to s1.ChordAngle as further calculation
	// is done considering segmentMaxLength as s1.ChordAngle.
	segmentMaxLength = segmentMaxLength / spheroid.SphereRadius
	geometry, err := geography.AsGeomT()
	if err != nil {
		return nil, err
	}
	segGeometry, err := segmentizeGeom(geometry, segmentMaxLength)
	if err != nil {
		return nil, err
	}
	return geo.NewGeographyFromGeom(segGeometry)
}

// segmentizeGeom returns a modified geom.T having no segment longer than
// the given maximum segment length.
func segmentizeGeom(geometry geom.T, segmentMaxLength float64) (geom.T, error) {
	if segmentMaxLength <= 0 {
		return nil, errors.Newf("maximum segment length must be positive")
	}
	switch geometry := geometry.(type) {
	case *geom.Point, *geom.MultiPoint:
		return geometry, nil
	case *geom.LineString:
		var allFlatCoordinates []float64
		for pointIdx := 1; pointIdx < geometry.NumCoords(); pointIdx++ {
			allFlatCoordinates = append(
				allFlatCoordinates,
				segmentizeCoords(geometry.Coord(pointIdx-1), geometry.Coord(pointIdx), segmentMaxLength)...,
			)
		}
		// Appending end point as it wasn't included in the iteration of coordinates.
		allFlatCoordinates = append(allFlatCoordinates, geometry.Coord(geometry.NumCoords()-1)...)
		return geom.NewLineStringFlat(geom.XY, allFlatCoordinates).SetSRID(geometry.SRID()), nil
	case *geom.MultiLineString:
		segMultiLine := geom.NewMultiLineString(geom.XY).SetSRID(geometry.SRID())
		for lineIdx := 0; lineIdx < geometry.NumLineStrings(); lineIdx++ {
			l, err := segmentizeGeom(geometry.LineString(lineIdx), segmentMaxLength)
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
				segmentizeCoords(geometry.Coord(pointIdx-1), geometry.Coord(pointIdx), segmentMaxLength)...,
			)
		}
		// Appending end point as it wasn't included in the iteration of coordinates.
		allFlatCoordinates = append(allFlatCoordinates, geometry.Coord(geometry.NumCoords()-1)...)
		return geom.NewLinearRingFlat(geom.XY, allFlatCoordinates).SetSRID(geometry.SRID()), nil
	case *geom.Polygon:
		segPolygon := geom.NewPolygon(geom.XY).SetSRID(geometry.SRID())
		for loopIdx := 0; loopIdx < geometry.NumLinearRings(); loopIdx++ {
			l, err := segmentizeGeom(geometry.LinearRing(loopIdx), segmentMaxLength)
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
			p, err := segmentizeGeom(geometry.Polygon(polygonIdx), segmentMaxLength)
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
			g, err := segmentizeGeom(geometry.Geom(geoIdx), segmentMaxLength)
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

// segmentizeCoords inserts multiple points between given two-coordinate and
// return resultant points as []float64. Such that distance between any two
// points is less than given maximum segment's length, the total number of
// segments is the power of 2, and all the segments are of the same length.
// NOTE: List of points does not consist of end point.
func segmentizeCoords(a geom.Coord, b geom.Coord, maxSegmentLength float64) []float64 {
	// Converted geom.Coord into s2.Point so we can segmentize the coordinates.
	point1 := s2.PointFromLatLng(s2.LatLngFromDegrees(a.Y(), a.X()))
	point2 := s2.PointFromLatLng(s2.LatLngFromDegrees(b.Y(), b.X()))

	var segmentizedPoints []s2.Point
	distanceBetweenPoints := s2.ChordAngleBetweenPoints(point1, point2).Angle().Radians()
	if maxSegmentLength <= distanceBetweenPoints {
		// This calculation is to determine the total number of segment between given
		// 2 coordinates. For that fraction by segment must be less than or equal to
		// the fraction of max segment length to distance between point, since the
		// total number of segment must be power of 2 therefore we can write as
		// 1 / (2^n)[numberOfSegmentToCreate] <= segmentMaxLength / distanceBetweenPoints < 1 / (2^(n-1))
		// (2^n)[numberOfSegmentToCreate] >= distanceBetweenPoints / segmentMaxLength > 2^(n-1)
		// therefore n = ceil(log2(segmentMaxLength/distanceBetweenPoints)). Hence
		// numberOfSegmentToCreate = 2^(ceil(log2(segmentMaxLength/distanceBetweenPoints))).
		numberOfSegmentToCreate := int(math.Pow(2, math.Ceil(math.Log2(distanceBetweenPoints/maxSegmentLength))))
		for pointInserted := 1; pointInserted < numberOfSegmentToCreate; pointInserted++ {
			newPoint := s2.Interpolate(float64(pointInserted)/float64(numberOfSegmentToCreate), point1, point2)
			segmentizedPoints = append(segmentizedPoints, newPoint)
		}
	}
	allSegmentizedCoordinates := a.Clone()
	for _, point := range segmentizedPoints {
		latLng := s2.LatLngFromPoint(point)
		allSegmentizedCoordinates = append(allSegmentizedCoordinates, latLng.Lng.Degrees(), latLng.Lat.Degrees())
	}
	return allSegmentizedCoordinates
}
