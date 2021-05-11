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
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// dirctn corrosponds to integer defining particular direction
type dirctn int

const (
	lineNoCross                    int = 0
	lineCrossLeft                  int = -1
	lineCrossRight                 int = 1
	lineMulticrossToLeft           int = -2
	lineMulticrossToRight          int = 2
	lineMulticrossToSameFirstLeft  int = -3
	lineMulticrossToSameFirstRight int = 3

	leftDir            dirctn = -1
	noCrossOrCollinear dirctn = 0
	rightDir           dirctn = 1

	inLeft      dirctn = -1
	isCollinear dirctn = 0
	inRight     dirctn = 1
)

// getPosition takes 3 point and Returns the direction of 3rd point from Segment formed by first point and second point
func getPosition(fromSeg, toSeg, givenPoint geom.Coord) dirctn {
	positionOfPoint := (toSeg.X()-givenPoint.X())*(fromSeg.Y()-givenPoint.Y()) - (toSeg.Y()-givenPoint.Y())*(fromSeg.X()-givenPoint.X())

	if positionOfPoint < 0 {
		return inLeft
	}
	if positionOfPoint > 0 {
		return inRight
	}
	return isCollinear
}

// getSegCrossDirection takes 4 points and Returns the crossing direction of second segment over first segment
// first segment is formed by first 2 points whereas second segment is formed by next 2 points
func getSegCrossDirection(fromSeg1, toSeg1, fromSeg2, toSeg2 geom.Coord) dirctn {
	posF1FrmSeg2 := getPosition(fromSeg2, toSeg2, fromSeg1)
	posT1FrmSeg2 := getPosition(fromSeg2, toSeg2, toSeg1)

	posF2FrmSeg1 := getPosition(fromSeg1, toSeg1, fromSeg2)
	posT2FrmSeg1 := getPosition(fromSeg1, toSeg1, toSeg2)

	// If both point of any segment is on same side of other segment
	// or second point of any segment is colliniear with other segment
	// then return the value as noCrossOrCollinear else return the direction of segment2 over segment1
	// to avoid double counting, collinearity condition is used with second points only
	if posF1FrmSeg2 == posT1FrmSeg2 || posF2FrmSeg1 == posT2FrmSeg1 ||
		posT1FrmSeg2 == isCollinear || posT2FrmSeg1 == isCollinear {
		return noCrossOrCollinear
	}
	return posT2FrmSeg1
}

// Returns diffrent interger value defining behaviour of crossing of lines
// 0: Line No Cross
// -1: Line2 crosses Line 1 from Right to Left
// 1: Line2 crosses Line 1 from Left to Right
// -2: Line2 Multicrosses Line1 from Right to Left
// 2: Line2 Multicrosses Line1 from Left to Right
// -3: Line2 Multicrosses Line1 from Left to Left
// 3: Line2 Multicrosses Line1 from Right to Right
// LineCrossingDirection Implements PostGIS behaviour where top vetex of segment touching the another line not counted as crossing
// but bottom vetex of segment touching the another line is counted as crossing
func LineCrossingDirection(geometry1, geometry2 geo.Geometry) (int, error) {

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
		return 0, errors.New("arguments must be LINESTRING")
	}

	line1, line2 := g1.Coords(), g2.Coords()

	firstSegCrossDirection := noCrossOrCollinear
	countDirection := make(map[dirctn]int)

	// Iterating segment2 over line2 from tail to head
	for indxSeg2 := 1; indxSeg2 < len(line2); indxSeg2++ {
		fromSeg2, toSeg2 := line2[indxSeg2-1], line2[indxSeg2]

		// Iterating segment1 over line1 from tail to head
		for indxSeg1 := 1; indxSeg1 < len(line1); indxSeg1++ {
			fromSeg1, toSeg1 := line1[indxSeg1-1], line1[indxSeg1]

			// segCrossDirection is current crossing direction of segment2 over segment1
			segCrossDirection := getSegCrossDirection(fromSeg1, toSeg1, fromSeg2, toSeg2)
			// update count of current crossing direction
			countDirection[segCrossDirection]++

			// firstSegCrossDirection keeps direction of first segment2 which cross over segment1
			if firstSegCrossDirection == noCrossOrCollinear {
				firstSegCrossDirection = segCrossDirection
			}
		}
	}

	if countDirection[leftDir] == 0 && countDirection[rightDir] == 0 {
		return lineNoCross, nil
	}
	if countDirection[leftDir] == 1 && countDirection[rightDir] == 0 {
		return lineCrossLeft, nil
	}
	if countDirection[leftDir] == 0 && countDirection[rightDir] == 1 {
		return lineCrossRight, nil
	}
	if countDirection[leftDir]-countDirection[rightDir] == 1 {
		return lineMulticrossToLeft, nil
	}
	if countDirection[rightDir]-countDirection[leftDir] == 1 {
		return lineMulticrossToRight, nil
	}
	if countDirection[leftDir] == countDirection[rightDir] {

		if firstSegCrossDirection == leftDir {
			return lineMulticrossToSameFirstLeft, nil
		}
		return lineMulticrossToSameFirstRight, nil
	}

	return lineNoCross, nil
}
