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
func Segmentize(geography geo.Geography, segmentMaxLength float64) (geo.Geography, error) {
	if math.IsNaN(segmentMaxLength) || math.IsInf(segmentMaxLength, 1 /* sign */) {
		return geography, nil
	}
	geometry, err := geography.AsGeomT()
	if err != nil {
		return geo.Geography{}, err
	}
	switch geometry := geometry.(type) {
	case *geom.Point, *geom.MultiPoint:
		return geography, nil
	default:
		if segmentMaxLength <= 0 {
			return geo.Geography{}, errors.Newf("maximum segment length must be positive")
		}
		spheroid, err := geography.Spheroid()
		if err != nil {
			return geo.Geography{}, err
		}
		// Convert segmentMaxLength to Angle with respect to earth sphere as
		// further calculation is done considering segmentMaxLength as Angle.
		segmentMaxAngle := segmentMaxLength / spheroid.SphereRadius
		ret, err := geosegmentize.Segmentize(geometry, segmentMaxAngle, segmentizeCoords)
		if err != nil {
			return geo.Geography{}, err
		}
		return geo.MakeGeographyFromGeomT(ret)
	}
}

// segmentizeCoords inserts multiple points between given two coordinates and
// return resultant point as flat []float64. Such that distance between any two
// points is less than given maximum segment's length, the total number of
// segments is the power of 2, and all the segments are of the same length.
// Note: List of points does not consist of end point.
func segmentizeCoords(a geom.Coord, b geom.Coord, segmentMaxAngle float64) ([]float64, error) {
	if segmentMaxAngle <= 0 {
		return nil, errors.Newf("maximum segment angle must be positive")
	}
	// Converted geom.Coord into s2.Point so we can segmentize the coordinates.
	pointA := s2.PointFromLatLng(s2.LatLngFromDegrees(a.Y(), a.X()))
	pointB := s2.PointFromLatLng(s2.LatLngFromDegrees(b.Y(), b.X()))

	chordAngleBetweenPoints := s2.ChordAngleBetweenPoints(pointA, pointB).Angle().Radians()
	// PostGIS' behavior appears to involve cutting this down into segments divisible
	// by a power of two. As such, we do not use ceil(chordAngleBetweenPoints/segmentMaxAngle).
	//
	// This calculation is to determine the total number of segment between given
	// 2 coordinates, ensuring that the segments are divided into parts divisible by
	// a power of 2.
	//
	// We can write that power as 2^n in the following inequality
	// 2^n >= ceil(chordAngleBetweenPoints/segmentMaxAngle) > 2^(n-1).
	// We can drop the ceil since 2^n must be an int
	// 2^n >= chordAngleBetweenPoints/segmentMaxAngle > 2^(n-1).
	// Then n = ceil(log2(chordAngleBetweenPoints/segmentMaxAngle)).
	// Hence numberOfSegmentsToCreate = 2^(ceil(log2(chordAngleBetweenPoints/segmentMaxAngle))).
	doubleNumberOfSegmentsToCreate := math.Pow(2, math.Ceil(math.Log2(chordAngleBetweenPoints/segmentMaxAngle)))
	doubleNumPoints := float64(len(a)) * (1 + doubleNumberOfSegmentsToCreate)
	if err := geosegmentize.CheckSegmentizeTooManyPoints(doubleNumPoints, a, b); err != nil {
		return nil, err
	}
	numberOfSegmentsToCreate := int(doubleNumberOfSegmentsToCreate)
	numPoints := int(doubleNumPoints)

	allSegmentizedCoordinates := make([]float64, 0, numPoints)
	allSegmentizedCoordinates = append(allSegmentizedCoordinates, a.X(), a.Y())
	for pointInserted := 1; pointInserted < numberOfSegmentsToCreate; pointInserted++ {
		newPoint := s2.Interpolate(float64(pointInserted)/float64(numberOfSegmentsToCreate), pointA, pointB)
		latLng := s2.LatLngFromPoint(newPoint)
		allSegmentizedCoordinates = append(allSegmentizedCoordinates, latLng.Lng.Degrees(), latLng.Lat.Degrees())
	}
	return allSegmentizedCoordinates, nil
}
