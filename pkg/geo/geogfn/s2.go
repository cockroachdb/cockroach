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
	"fmt"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// s2RegionsToPointsAndShapeIndexes separates S2 regions into either being part of the ShapeIndex,
// or an array of points depending on their type.
// ShapeIndexes have useful operations that are optimized, which is why we sometimes
// desire this grouping.
func s2RegionsToPointsAndShapeIndexes(regions []s2.Region) (*s2.ShapeIndex, []s2.Point, error) {
	shapeIndex := s2.NewShapeIndex()
	points := []s2.Point{}
	for _, region := range regions {
		switch region := region.(type) {
		case s2.Point:
			points = append(points, region)
		case *s2.Polygon:
			shapeIndex.Add(region)
		case *s2.Polyline:
			shapeIndex.Add(region)
		default:
			return nil, nil, fmt.Errorf("invalid region: %#v", region)
		}
	}
	return shapeIndex, points, nil
}

// minChordAngle returns the minimum chord angle of a and b.
func minChordAngle(a s1.ChordAngle, b s1.ChordAngle) s1.ChordAngle {
	if a < b {
		return a
	}
	return b
}
