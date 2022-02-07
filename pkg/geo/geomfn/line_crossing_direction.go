// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/twpayne/go-geom"
)

// lineCrossingDirection corresponds to integer defining particular direction.
type lineCrossingDirection int

// LineCrossingDirectionValue maps to the return value of
// ST_LineCrossingDirection from PostGIS.
type LineCrossingDirectionValue int

// Assigning constant integer values to variables for different crossing behavior and directions
// throughout the functions.
const (
	LineNoCross                    LineCrossingDirectionValue = 0
	LineCrossLeft                  LineCrossingDirectionValue = -1
	LineCrossRight                 LineCrossingDirectionValue = 1
	LineMultiCrossToLeft           LineCrossingDirectionValue = -2
	LineMultiCrossToRight          LineCrossingDirectionValue = 2
	LineMultiCrossToSameFirstLeft  LineCrossingDirectionValue = -3
	LineMultiCrossToSameFirstRight LineCrossingDirectionValue = 3

	leftDir            lineCrossingDirection = -1
	noCrossOrCollinear lineCrossingDirection = 0
	rightDir           lineCrossingDirection = 1

	inLeft      lineCrossingDirection = -1
	isCollinear lineCrossingDirection = 0
	inRight     lineCrossingDirection = 1
)

// getPosition takes 3 points and returns the direction of third point from segment formed by first point and second point.
func getPosition(fromSeg, toSeg, givenPoint geom.Coord) lineCrossingDirection {

	// crossProductValueInZ is value of Cross Product of two vectors in z axis direction where
	// first vector is from givenPoint to toSeg and second vector is from givenPoint to fromSeg.
	crossProductValueInZ := (toSeg.X()-givenPoint.X())*(fromSeg.Y()-givenPoint.Y()) - (toSeg.Y()-givenPoint.Y())*(fromSeg.X()-givenPoint.X())

	if crossProductValueInZ < 0 {
		return inLeft
	}
	if crossProductValueInZ > 0 {
		return inRight
	}
	return isCollinear
}

// getSegCrossDirection takes 4 points and returns the crossing direction of second segment over first segment
// first segment is formed by first 2 points whereas second segment is formed by next 2 points.
func getSegCrossDirection(fromSeg1, toSeg1, fromSeg2, toSeg2 geom.Coord) lineCrossingDirection {
	posF1FromSeg2 := getPosition(fromSeg2, toSeg2, fromSeg1)
	posT1FromSeg2 := getPosition(fromSeg2, toSeg2, toSeg1)

	posF2FromSeg1 := getPosition(fromSeg1, toSeg1, fromSeg2)
	posT2FromSeg1 := getPosition(fromSeg1, toSeg1, toSeg2)

	// If both point of any segment is on same side of other segment
	// or second point of any segment is collinear with other segment
	// then return the value as noCrossOrCollinear else return the direction of segment2 over segment1.
	// To avoid double counting, collinearity condition is used with second points only.
	if posF1FromSeg2 == posT1FromSeg2 || posF2FromSeg1 == posT2FromSeg1 ||
		posT1FromSeg2 == isCollinear || posT2FromSeg1 == isCollinear {
		return noCrossOrCollinear
	}
	return posT2FromSeg1
}

// LineCrossingDirection takes two lines and returns an integer value defining behavior of crossing of lines:
// 0: lines do not cross,
// -1: line2 crosses line1 from right to left,
// 1: line2 crosses line1 from left to right,
// -2: line2 crosses line1 multiple times from right to left,
// 2: line2 crosses line1 multiple times from left to right,
// -3: line2 crosses line1 multiple times from left to left,
// 3: line2 crosses line1 multiple times from right to Right.
// Note that the top vertex of the segment touching another line does not count as a crossing,
// but the bottom vertex of segment touching another line is considered a crossing.
func LineCrossingDirection(geometry1, geometry2 geo.Geometry) (LineCrossingDirectionValue, error) {

	t1, err1 := geometry1.AsGeomT()
	if err1 != nil {
		return 0, err1
	}

	t2, err2 := geometry2.AsGeomT()
	if err2 != nil {
		return 0, err2
	}

	g1, ok1 := t1.(*geom.LineString)
	g2, ok2 := t2.(*geom.LineString)

	if !ok1 || !ok2 {
		return 0, pgerror.Newf(pgcode.InvalidParameterValue, "arguments must be LINESTRING")
	}

	line1, line2 := g1.Coords(), g2.Coords()

	firstSegCrossDirection := noCrossOrCollinear
	countDirection := make(map[lineCrossingDirection]int)

	// Iterating segment2 over line2 from tail to head.
	for idx2 := 1; idx2 < len(line2); idx2++ {
		fromSeg2, toSeg2 := line2[idx2-1], line2[idx2]

		// Iterating segment1 over line1 from tail to head.
		for idx1 := 1; idx1 < len(line1); idx1++ {
			fromSeg1, toSeg1 := line1[idx1-1], line1[idx1]

			// segCrossDirection is current crossing direction of segment2 over segment1.
			segCrossDirection := getSegCrossDirection(fromSeg1, toSeg1, fromSeg2, toSeg2)
			// update count of current crossing direction.
			countDirection[segCrossDirection]++

			// firstSegCrossDirection keeps direction of first segment2 which cross over segment1.
			if firstSegCrossDirection == noCrossOrCollinear {
				firstSegCrossDirection = segCrossDirection
			}
		}
	}

	if countDirection[leftDir] == 0 && countDirection[rightDir] == 0 {
		return LineNoCross, nil
	}
	if countDirection[leftDir] == 1 && countDirection[rightDir] == 0 {
		return LineCrossLeft, nil
	}
	if countDirection[leftDir] == 0 && countDirection[rightDir] == 1 {
		return LineCrossRight, nil
	}
	if countDirection[leftDir]-countDirection[rightDir] == 1 {
		return LineMultiCrossToLeft, nil
	}
	if countDirection[rightDir]-countDirection[leftDir] == 1 {
		return LineMultiCrossToRight, nil
	}
	if countDirection[leftDir] == countDirection[rightDir] {

		if firstSegCrossDirection == leftDir {
			return LineMultiCrossToSameFirstLeft, nil
		}
		return LineMultiCrossToSameFirstRight, nil
	}

	return LineNoCross, nil
}
