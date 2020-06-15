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
	"github.com/cockroachdb/cockroach/pkg/geo/geosegmentize"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// Segmentize return modified Geography having no segment longer
// that given maximum segment length.
// This works by dividing each segment by a power of 2 to find the
// smallest power less than or equal to the segmentMaxLength.
func Segmentize(geography *geo.Geography, segmentMaxLength float64) (*geo.Geography, error) {
	geometry, err := geography.AsGeomT()
	if err != nil {
		return nil, err
	}
	switch geometry := geometry.(type) {
	case *geom.Point, *geom.MultiPoint:
		return geography, nil
	default:
		if segmentMaxLength <= 0 {
			return nil, errors.Newf("maximum segment length must be positive")
		}
		spheroid, err := geography.Spheroid()
		if err != nil {
			return nil, err
		}
		// Convert segmentMaxLength to Angle with respect to earth sphere as
		// further calculation is done considering segmentMaxLength as Angle.
		segmentMaxAngle := segmentMaxLength / spheroid.SphereRadius
		segGeometry, err := geosegmentize.SegmentizeGeom(geometry, segmentMaxAngle, segmentizeCoords)
		if err != nil {
			return nil, err
		}
		return geo.NewGeographyFromGeom(segGeometry)
	}
}

// segmentizeCoords inserts multiple points between given two coordinates and
// return resultant point as flat []float64. Such that distance between any two
// points is less than given maximum segment's length, the total number of
// segments is the power of 2, and all the segments are of the same length.
// Note: List of points does not consist of end point.
func segmentizeCoords(a geom.Coord, b geom.Coord, segmentMaxAngle float64) []float64 {
	// Converted geom.Coord into s2.Point so we can segmentize the coordinates.
	pointA := s2.PointFromLatLng(s2.LatLngFromDegrees(a.Y(), a.X()))
	pointB := s2.PointFromLatLng(s2.LatLngFromDegrees(b.Y(), b.X()))

	chordAngleBetweenPoints := s2.ChordAngleBetweenPoints(pointA, pointB).Angle().Radians()
	// This calculation is to determine the total number of segment between given
	// 2 coordinates, ensuring that the segments are divided into parts divisible by
	// a power of 2.
	//
	// For that fraction by segment must be less than or equal to
	// the fraction of max segment length to distance between point, since the
	// total number of segment must be power of 2 therefore we can write as
	// 1 / (2^n)[numberOfSegmentToCreate] <= segmentMaxLength / distanceBetweenPoints < 1 / (2^(n-1))
	// (2^n)[numberOfSegmentToCreate] >= distanceBetweenPoints / segmentMaxLength > 2^(n-1)
	// therefore n = ceil(log2(segmentMaxLength/distanceBetweenPoints)). Hence
	// numberOfSegmentToCreate = 2^(ceil(log2(segmentMaxLength/distanceBetweenPoints))).
	numberOfSegmentToCreate := int(math.Pow(2, math.Ceil(math.Log2(chordAngleBetweenPoints/segmentMaxAngle))))
	allSegmentizedCoordinates := make([]float64, 0, 2*(1+numberOfSegmentToCreate))
	allSegmentizedCoordinates = append(allSegmentizedCoordinates, a.Clone()...)
	for pointInserted := 1; pointInserted < numberOfSegmentToCreate; pointInserted++ {
		newPoint := s2.Interpolate(float64(pointInserted)/float64(numberOfSegmentToCreate), pointA, pointB)
		latLng := s2.LatLngFromPoint(newPoint)
		allSegmentizedCoordinates = append(allSegmentizedCoordinates, latLng.Lng.Degrees(), latLng.Lat.Degrees())
	}
	return allSegmentizedCoordinates
}
