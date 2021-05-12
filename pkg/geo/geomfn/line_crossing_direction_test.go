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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestLineCrossingDirection(t *testing.T) {
	testCases := []struct {
		desc     string
		line1    geom.T
		line2    geom.T
		expected LineCrossingDirectionValue
	}{
		{
			desc:     "Two point lines, line1 vertical, line2 horizontal crossing toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 50}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{25, 0, -25, 0}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines with short lengths, line1 vertical, line2 horizontal crossing toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 0.05}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0.025, 0, -0.025, 0}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines, line1 horizontal, line2 vertical crossing toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{25, 0, -25, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, -5, 0, 5}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines with short lengths, line1 horizontal, line2 vertical crossing toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0.025, 0, -0.025, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, -0.05, 0, 0.05}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines, line1 vertical, line2 horizontal crossing toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -43, 0, 555}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-125, 0, 125, 0}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines with short lengths, line1 vertical, line2 horizontal crossing toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -0.03, 0, 0.005}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-0.025, 0, 0.025, 0}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines, line1 horizontal, line2 vertical crossing toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{-25, 0, 25, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, -2, 0, 5}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines with short lengths, line1 horizontal, line2 vertical crossing toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{-0.0025, 0, 0.0025, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, -0.1, 0, 0.005}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines with short lengths, line1 horizontal, line2 vertical toward left but no crossing",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0.25, 0, 0.5, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 0.5}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines with short lengths, line1 vertical, line2 horizontal toward right but no crossing",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 0.5}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0.25, 0, 0.5, 0}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines, line1 vertical, line2 horizontal toward left but no crossing",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 5}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{5, 0, 0.025, 0}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines, line1 slope 45 degree, line2 crossing like X from mid point toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{-125, -125, 125, 125}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{125, -125, -125, 125}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines, line1 slope 45 degree, line2 crossing like X from mid point toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{-25, -25, 25, 25}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-25, 25, 25, -25}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines, line2 slope 45 degree, line2 crossing like X from mid point toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{250, -250, -250, 250}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-250, -250, 250, 250}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines, line2 slope 45 degree, line2 crossing like X from mid point toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{215, -215, -215, 215}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0.215, 0.215, -0.215, -0.215}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines, line2 directed left, line1 touches top vertex of line2",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -112, 0, 525}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{65, 0, 0, 0}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines, line2 directed left, line1 touches bottom vertex of line2",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -325, 0, 525}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, -22, 0}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines, line2 directed right, line1 touches top vertex of line2",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -123, 0, 5}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-65, 0, 0, 0}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines, line2 directed right, line1 touches bottom vertex of line2",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -123, 0, 5}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 45, 0}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines, line2 directed right, line2 touches bottom vertex of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 65}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-325, 0, 525, 0}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines, line2 directed left, line2 touches bottom vertex of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 17}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{50, 0, -50, 0}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines, line2 directed right, line2 touches top vertex of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -327, 0, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-50, 0, 50, 0}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines, line2 directed left, line2 touches top vertex of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -15, 0, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{50, 0, -50, 0}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines, line1 and line2 same bottom point different top point, line2 toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 50}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 50, 0}),
			expected: LineCrossRight,
		},
		{
			desc:     "Two point lines, line1 and line2 same bottom point different top point, line2 toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 50}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, -50, 0}),
			expected: LineCrossLeft,
		},
		{
			desc:     "Two point lines, line1 and line2 same top point different bottom point, line2 toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -50, 0, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-50, 0, 0, 0}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines, line1 and line2 same top point different bottom point, line2 toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -50, 0, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{50, 0, 0, 0}),
			expected: LineNoCross,
		},
		{
			desc:     "Two point lines, line1 and line2 are collinear",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 100, 0, 200}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 50, 0, 150}),
			expected: LineNoCross,
		},
		{
			desc:     "line1: 2 points horizontal, line2: 3 points vertical no cross",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{-50, 0, 50, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 10, 0, 20, 0, 30}),
			expected: LineNoCross,
		},
		{
			desc:     "line1: 3 points horizontal, line2: 4 points vertical no cross",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{-50, 0, 50, 0, 25, 0}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 10, 0, 20, 0, 30, 0, 40, 0, 50}),
			expected: LineNoCross,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, total cross: 2, line2 first segment crossing line1 toward right, ends same",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{25, 169, 89, 114, 40, 70, 86, 43}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{171, 154, 20, 140, 71, 74, 161, 53}),
			expected: LineMultiCrossToSameFirstRight,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, total cross: 2 first crossing segment of line2 crossing line1 toward left, ends same",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{171, 154, 20, 140, 71, 74, 161, 53}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{25, 169, 89, 114, 40, 70, 86, 43}),
			expected: LineMultiCrossToSameFirstLeft,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, total cross: 3, first crossing segment of line2 crossing line1 toward right, ends opposite",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{25, 169, 89, 114, 40, 70, 86, 43}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{171, 154, 20, 140, 71, 74, 2.99, 90.16}),
			expected: LineMultiCrossToRight,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, total cross: 3, first crossing segment of line2 crossing line1 toward left, ends opposite",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{171, 154, 20, 140, 71, 74, 2.99, 90.16}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{25, 169, 89, 114, 40, 70, 86, 43}),
			expected: LineMultiCrossToLeft,
		},
		{
			desc:     "line1: 4 points, line2: 3 points, total cross: 1, line2 crosses line1 toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{25, 169, 89, 114, 40, 70, 86, 43}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{20, 140, 71, 74, 161, 53}),
			expected: LineCrossLeft,
		},
		{
			desc:     "line1: 3 points, line2: 4 points, total cross: 1, line2 single crosses line1 toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{20, 140, 71, 74, 161, 53}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{25, 169, 89, 114, 40, 70, 86, 43}),
			expected: LineCrossRight,
		},
		{
			desc:     "line1: 2 points, line2: 4 points, line2 first vertex left side, mid segment collinear, fourth vertex right side of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -100, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-50, 0, 0, 0, 0, 50, 50, 50}),
			expected: LineCrossRight,
		},
		{
			desc:     "line1: 2 points, line2: 4 points, line2 first vertex right side, mid segment collinear, fourth vertex left side of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -100, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{50, 0, 0, 0, 0, 50, -50, 50}),
			expected: LineCrossLeft,
		},
		{
			desc:     "line1: 2 points, line2: 3 points, line2 first segment collinear, 3th vertex right side of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 50, 50, 50}),
			expected: LineCrossRight,
		},
		{
			desc:     "line1: 2 points, line2: 3 points, line2 first segment collinear, 3th vertex left side of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 50, -50, 50}),
			expected: LineCrossLeft,
		},
		{
			desc:     "line1: 2 points, line2: 4 points, total cross : 3, first crossing segment of line2 crossing line1 toward right, ends opposite",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -100, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-50, 0, 50, 0, -50, 50, 50, 50}),
			expected: LineMultiCrossToRight,
		},
		{
			desc:     "line1: 2 points, line2: 4 points, total cross : 3, first crossing segment of line2 crossing line1 toward left, ends opposite",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -100, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{50, 0, -50, 0, 50, 50, -50, 50}),
			expected: LineMultiCrossToLeft,
		},
		{
			desc:     "line1: 2 points, line2: 4 points, total cross : 2, first crossing segment of line2 crossing line1 toward right, ends same",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -100, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-50, 0, 50, 0, -50, 50, -50, 200}),
			expected: LineMultiCrossToSameFirstRight,
		},
		{
			desc:     "line1: 2 points, line2: 4 points, total cross: 2, first crossing segment of line2 crossing line1 toward left, ends same",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -100, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{50, 0, -50, 0, 50, 50, 50, 200}),
			expected: LineMultiCrossToSameFirstLeft,
		},
		{
			desc:     "line1: 2 points, line2: 3 points, line2 first and last point on line2 and middle point left side of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -100, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, -50, -50, 0, 0, 50}),
			expected: LineCrossLeft,
		},
		{
			desc:     "line1: 2 points, line2: 3 points, line2 first and last point on line2 and middle point right side of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, -100, 0, 100}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{0, -50, 50, 0, 0, 50}),
			expected: LineCrossRight,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, middle segment overlap each other, remaining point different, first and last vertex of line2 on left side of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{50, -50, 0, -50, 0, 50, 50, 50}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-50, -50, 0, -50, 0, 50, -50, 50}),
			expected: LineNoCross,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, middle segment overlap each other, remaining point different, first and last vertex of line2 on right side of line1",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{-50, -50, 0, -50, 0, 50, -50, 50}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{50, -50, 0, -50, 0, 50, 50, 50}),
			expected: LineNoCross,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, total crosses: 3, seg1 cross seg1, seg2 cross seg2 and seg3 cross seg3, first segment direct toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 100, 0, 200, 0, 300}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-100, 50, 500, 50, -100, 150, 1, 250}),
			expected: LineMultiCrossToRight,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, total crosses: 3, seg1 cross seg1, seg2 cross seg2 and seg3 cross seg3, first segment direct toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 100, 0, 200, 0, 300}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{100, 50, -500, 50, 100, 150, -1, 250}),
			expected: LineMultiCrossToLeft,
		},
		{
			desc:     "line1: 4 points, line2: 5 points, total crosses: 4, line2 seg1 cross seg1, seg2 cross seg2, seg3 and seg4 cross seg3 of line1, first segment direct toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 100, 0, 200, 0, 300}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-100, 50, 500, 50, -100, 150, 1, 250, -1, -1}),
			expected: LineMultiCrossToSameFirstRight,
		},
		{
			desc:     "line1: 4 points, line2: 4 points, total crosses: 3, line2 seg1 cross seg1, seg2 cross seg2, seg3 and seg4 cross seg3 of line1, first segment direct toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 100, 0, 200, 0, 300}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{100, 50, -500, 50, 100, 150, -1, 250, 1, 1}),
			expected: LineMultiCrossToSameFirstLeft,
		},
		{
			desc:     "line1: 4 points, line2: 3 points, total crosses: 2, line2 second vertex same as line1 second vertex and line2 fourth vertex same as line1 third vertex, first crossing seg of line2 directed toward left",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 100, 0, 200, 0, 300}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{50, 50, -50, 150, 50, 250}),
			expected: LineMultiCrossToSameFirstLeft,
		},
		{
			desc:     "line1: 4 points, line2: 3 points, total crosses: 2, line2 second vertex same as line1 second vertex and line2 fourth vertex same as line1 third vertex, first crossing seg of line2 directed toward right",
			line1:    geom.NewLineStringFlat(geom.XY, []float64{0, 0, 0, 100, 0, 200, 0, 300}),
			line2:    geom.NewLineStringFlat(geom.XY, []float64{-50, 50, 50, 150, -50, 250}),
			expected: LineMultiCrossToSameFirstRight,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			geometry1, _ := geo.MakeGeometryFromGeomT(tC.line1)
			geometry2, _ := geo.MakeGeometryFromGeomT(tC.line2)
			got, _ := LineCrossingDirection(geometry1, geometry2)
			require.Equal(t, tC.expected, got)
		})
	}
}

func TestLineCrossingDirectionError(t *testing.T) {
	errorTestCases := []struct {
		geomType  string
		geometry1 geom.T
		geometry2 geom.T
	}{
		{
			geomType:  "line and point",
			geometry1: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1}),
			geometry2: geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			geomType:  "point and line",
			geometry1: geom.NewPointFlat(geom.XY, []float64{0, 0}),
			geometry2: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1}),
		},
		{
			geomType:  "point and point",
			geometry1: geom.NewPointFlat(geom.XY, []float64{0, 0}),
			geometry2: geom.NewPointFlat(geom.XY, []float64{0, 0}),
		},
		{
			geomType:  "line and polygon",
			geometry1: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
			geometry2: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, []int{8}),
		},
		{
			geomType:  "polygon and line",
			geometry1: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, []int{8}),
			geometry2: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
		},
		{
			geomType:  "polygon and polygon",
			geometry1: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, []int{8}),
			geometry2: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, []int{8}),
		},
		{
			geomType:  "line and multiline",
			geometry1: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
			geometry2: geom.NewMultiLineStringFlat(geom.XYZM, []float64{0, 0, -5, 23, 1, 1, 5, -23}, []int{8}),
		},
		{
			geomType:  "multiline and line",
			geometry1: geom.NewMultiLineStringFlat(geom.XYZM, []float64{0, 0, -5, 23, 1, 1, 5, -23}, []int{8}),
			geometry2: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
		},
		{
			geomType:  "multiline and multiline",
			geometry1: geom.NewMultiLineStringFlat(geom.XYZM, []float64{0, 0, -5, 23, 1, 1, 5, -23}, []int{8}),
			geometry2: geom.NewMultiLineStringFlat(geom.XYZM, []float64{0, 0, -5, 23, 1, 1, 5, -23}, []int{8}),
		},
		{
			geomType:  "line and multiline",
			geometry1: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, []int{8}),
			geometry2: geom.NewMultiLineStringFlat(geom.XYZM, []float64{0, 0, -5, 23, 1, 1, 5, -23}, []int{8}),
		},
		{
			geomType:  "line and multipolygon",
			geometry1: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
			geometry2: geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, [][]int{{8}}),
		},
		{
			geomType:  "multipolygon and line",
			geometry1: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
			geometry2: geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, [][]int{{8}}),
		},
		{
			geomType:  "multipolygon and multipolygon",
			geometry1: geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, [][]int{{8}}),
			geometry2: geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 1, 2, 2, 0, 0, 0}, [][]int{{8}}),
		},
		{
			geomType:  "line and geometrycollection",
			geometry1: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
			geometry2: geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1})),
		},
		{
			geomType:  "geometrycollection and line",
			geometry1: geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1})),
			geometry2: geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 2}),
		},
		{
			geomType:  "geometrycollection and geometrycollection",
			geometry1: geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1})),
			geometry2: geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1})),
		},
	}

	for _, tC := range errorTestCases {
		testName := "invalid attempt to line crossing direction to a " + tC.geomType
		t.Run(testName, func(t *testing.T) {
			g1, err1 := geo.MakeGeometryFromGeomT(tC.geometry1)
			require.NoError(t, err1)

			g2, err2 := geo.MakeGeometryFromGeomT(tC.geometry2)
			require.NoError(t, err2)

			_, err := LineCrossingDirection(g1, g2)
			require.EqualError(t, err, "arguments must be LINESTRING")
		})
	}
}
