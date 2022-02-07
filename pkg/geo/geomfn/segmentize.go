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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/twpayne/go-geom"
)

// Segmentize return modified Geometry having no segment longer
// that given maximum segment length.
// This works by inserting the extra points in such a manner that
// minimum number of new segments with equal length is created,
// between given two-points such that each segment has length less
// than or equal to given maximum segment length.
func Segmentize(g geo.Geometry, segmentMaxLength float64) (geo.Geometry, error) {
	if math.IsNaN(segmentMaxLength) || math.IsInf(segmentMaxLength, 1 /* sign */) {
		return g, nil
	}
	geometry, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}
	switch geometry := geometry.(type) {
	case *geom.Point, *geom.MultiPoint:
		return g, nil
	default:
		if segmentMaxLength <= 0 {
			return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "maximum segment length must be positive")
		}
		segGeometry, err := geosegmentize.Segmentize(geometry, segmentMaxLength, segmentizeCoords)
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
	if len(a) != len(b) {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "cannot segmentize two coordinates of different dimensions")
	}
	if maxSegmentLength <= 0 {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "maximum segment length must be positive")
	}

	// Only 2D distance is considered for determining number of segments.
	distanceBetweenPoints := math.Sqrt(math.Pow(a.X()-b.X(), 2) + math.Pow(b.Y()-a.Y(), 2))

	doubleNumberOfSegmentsToCreate := math.Ceil(distanceBetweenPoints / maxSegmentLength)
	doubleNumPoints := float64(len(a)) * (1 + doubleNumberOfSegmentsToCreate)
	if err := geosegmentize.CheckSegmentizeValidNumPoints(doubleNumPoints, a, b); err != nil {
		return nil, err
	}

	// numberOfSegmentsToCreate represent the total number of segments
	// in which given two coordinates will be divided.
	numberOfSegmentsToCreate := int(doubleNumberOfSegmentsToCreate)
	numPoints := int(doubleNumPoints)
	// segmentFraction represent the fraction of length each segment
	// has with respect to total length between two coordinates.
	allSegmentizedCoordinates := make([]float64, 0, numPoints)
	allSegmentizedCoordinates = append(allSegmentizedCoordinates, a.Clone()...)
	segmentFraction := 1.0 / float64(numberOfSegmentsToCreate)
	for pointInserted := 1; pointInserted < numberOfSegmentsToCreate; pointInserted++ {
		segmentPoint := make([]float64, 0, len(a))
		for i := 0; i < len(a); i++ {
			segmentPoint = append(
				segmentPoint,
				a[i]*(1-float64(pointInserted)*segmentFraction)+b[i]*(float64(pointInserted)*segmentFraction),
			)
		}
		allSegmentizedCoordinates = append(
			allSegmentizedCoordinates,
			segmentPoint...,
		)
	}

	return allSegmentizedCoordinates, nil
}
