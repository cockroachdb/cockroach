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
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geosegmentize"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// Segmentize return modified Geometry having no segment longer
// that given maximum segment length.
// This works by inserting the extra points in such a manner that
// minimum number of new segments with equal length is created,
// between given two-points such that each segment has length less
// than or equal to given maximum segment length.
func Segmentize(g geo.Geometry, segmentMaxLength float64) (geo.Geometry, error) {
	geometry, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	switch geometry := geometry.(type) {
	case *geom.Point, *geom.MultiPoint:
		return g, nil
	default:
		if segmentMaxLength <= 0 {
			return geo.Geometry{}, errors.Newf("maximum segment length must be positive")
		}
		segGeometry, err := geosegmentize.SegmentizeGeom(geometry, segmentMaxLength, segmentizeCoords)
		if err != nil {
			return geo.Geometry{}, err
		}
		return geo.MakeGeometryFromGeomT(segGeometry)
	}
}

// segmentizeCoords inserts multiple points between given two coordinates and
// return resultant point as flat []float64. Points are inserted in such a
// way that they create minimum number segments of equal length such that each
// segment has a length less than or equal to given maximum segment length.
// Note: List of points does not consist of end point.
func segmentizeCoords(a geom.Coord, b geom.Coord, maxSegmentLength float64) ([]float64, error) {
	distanceBetweenPoints := math.Sqrt(math.Pow(a.X()-b.X(), 2) + math.Pow(b.Y()-a.Y(), 2))

	// numberOfSegmentsToCreate represent the total number of segments
	// in which given two coordinates will be divided.
	numberOfSegmentsToCreate := int(math.Ceil(distanceBetweenPoints / maxSegmentLength))
	numPoints := 2 * (1 + numberOfSegmentsToCreate)
	if numPoints > geosegmentize.MaxPoints {
		return nil, errors.Newf(
			"attempting to segmentize into too many coordinates; need %d points between %v and %v, max %d",
			numPoints,
			a,
			b,
			geosegmentize.MaxPoints,
		)
	} // segmentFraction represent the fraction of length each segment
	// has with respect to total length between two coordinates.
	allSegmentizedCoordinates := make([]float64, 0, 2*(1+numberOfSegmentsToCreate))
	allSegmentizedCoordinates = append(allSegmentizedCoordinates, a.Clone()...)
	segmentFraction := 1.0 / float64(numberOfSegmentsToCreate)
	for pointInserted := 1; pointInserted < numberOfSegmentsToCreate; pointInserted++ {
		allSegmentizedCoordinates = append(
			allSegmentizedCoordinates,
			b.X()*float64(pointInserted)*segmentFraction+a.X()*(1-float64(pointInserted)*segmentFraction),
			b.Y()*float64(pointInserted)*segmentFraction+a.Y()*(1-float64(pointInserted)*segmentFraction),
		)
	}

	return allSegmentizedCoordinates, nil
}
