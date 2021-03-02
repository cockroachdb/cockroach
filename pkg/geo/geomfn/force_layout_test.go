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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkt"
)

func TestForceLayout(t *testing.T) {
	// We only support 2D on geo.Geometry, so test the forceLayout function instead.
	testCases := []struct {
		g        geom.T
		layout   geom.Layout
		expected geom.T
	}{
		{
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					0, 0,
					0, 1,
					1, 1,
					1, 0,
					0, 0,
				},
				[]int{10},
			).SetSRID(4326),
			geom.XY,
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					0, 0,
					0, 1,
					1, 1,
					1, 0,
					0, 0,
				},
				[]int{10},
			).SetSRID(4326),
		},
		{
			geom.NewPolygonFlat(
				geom.XYZ,
				[]float64{
					0, 0, 1,
					0, 1, 2,
					1, 1, 3,
					1, 0, 4,
					0, 0, 1,
				},
				[]int{15},
			).SetSRID(4326),
			geom.XY,
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					0, 0,
					0, 1,
					1, 1,
					1, 0,
					0, 0,
				},
				[]int{10},
			).SetSRID(4326),
		},
		{
			geom.NewPolygonFlat(
				geom.XYM,
				[]float64{
					0, 0, 1,
					0, 1, 2,
					1, 1, 3,
					1, 0, 4,
					0, 0, 1,
				},
				[]int{15},
			).SetSRID(4326),
			geom.XYZM,
			geom.NewPolygonFlat(
				geom.XYZM,
				[]float64{
					0, 0, 0, 1,
					0, 1, 0, 2,
					1, 1, 0, 3,
					1, 0, 0, 4,
					0, 0, 0, 1,
				},
				[]int{20},
			).SetSRID(4326),
		},
		{
			geom.NewPolygonFlat(
				geom.XYZM,
				[]float64{
					0, 0, 0, 1,
					0, 1, 0, 2,
					1, 1, 0, 3,
					1, 0, 0, 4,
					0, 0, 0, 1,
				},
				[]int{20},
			).SetSRID(4326),
			geom.XYM,
			geom.NewPolygonFlat(
				geom.XYM,
				[]float64{
					0, 0, 1,
					0, 1, 2,
					1, 1, 3,
					1, 0, 4,
					0, 0, 1,
				},
				[]int{15},
			).SetSRID(4326),
		},
		{
			geom.NewPolygonFlat(
				geom.XYZM,
				[]float64{
					0, 0, 0, 1,
					0, 1, 0, 2,
					1, 1, 0, 3,
					1, 0, 0, 4,
					0, 0, 0, 1,
				},
				[]int{20},
			).SetSRID(4326),
			geom.XY,
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					0, 0,
					0, 1,
					1, 1,
					1, 0,
					0, 0,
				},
				[]int{10},
			).SetSRID(4326),
		},
		{
			geom.NewPolygonFlat(
				geom.XYZ,
				[]float64{
					0, 0, 1,
					0, 1, 2,
					1, 1, 3,
					1, 0, 4,
					0, 0, 1,
				},
				[]int{15},
			).SetSRID(4326),
			geom.XYM,
			geom.NewPolygonFlat(
				geom.XYM,
				[]float64{
					0, 0, 0,
					0, 1, 0,
					1, 1, 0,
					1, 0, 0,
					0, 0, 0,
				},
				[]int{15},
			).SetSRID(4326),
		},
		{
			geom.NewPolygonFlat(
				geom.XY,
				[]float64{
					0, 0,
					0, 1,
					1, 1,
					1, 0,
					0, 0,
				},
				[]int{10},
			).SetSRID(4326),
			geom.XYZ,
			geom.NewPolygonFlat(
				geom.XYZ,
				[]float64{
					0, 0, 0,
					0, 1, 0,
					1, 1, 0,
					1, 0, 0,
					0, 0, 0,
				},
				[]int{15},
			).SetSRID(4326),
		},
		{
			geom.NewGeometryCollection().MustSetLayout(geom.XYZ).MustPush(
				geom.NewPointFlat(geom.XYZ, []float64{1, 2, 3}),
				geom.NewLineStringFlat(geom.XYZ, []float64{1, 2, 3, 4, 5, 6}),
				geom.NewMultiPointFlat(geom.XYZ, []float64{1, 2, 3, 4, 5, 6}, geom.NewMultiPointFlatOptionWithEnds([]int{3, 3, 6})),
				geom.NewMultiLineStringFlat(geom.XYZ, []float64{1, 2, 3, 4, 5, 6}, []int{3, 3, 6, 6}),
				geom.NewMultiPolygonFlat(geom.XYZ, []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3}, [][]int{{12}, {12}}),
			).SetSRID(4326),
			geom.XY,
			geom.NewGeometryCollection().MustSetLayout(geom.XY).MustPush(
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewLineStringFlat(geom.XY, []float64{1, 2, 4, 5}),
				geom.NewMultiPointFlat(geom.XY, []float64{1, 2, 4, 5}, geom.NewMultiPointFlatOptionWithEnds([]int{2, 2, 4})),
				geom.NewMultiLineStringFlat(geom.XY, []float64{1, 2, 4, 5}, []int{2, 2, 4, 4}),
				geom.NewMultiPolygonFlat(geom.XY, []float64{1, 2, 4, 5, 7, 8, 1, 2}, [][]int{{8}, {8}}),
			).SetSRID(4326),
		},
		{
			geom.NewGeometryCollection().MustSetLayout(geom.XY).MustPush(
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewLineStringFlat(geom.XY, []float64{1, 2, 4, 5}),
				geom.NewMultiPointFlat(geom.XY, []float64{1, 2, 4, 5}, geom.NewMultiPointFlatOptionWithEnds([]int{2, 2, 4})),
				geom.NewMultiLineStringFlat(geom.XY, []float64{1, 2, 4, 5}, []int{2, 2, 4, 4}),
				geom.NewMultiPolygonFlat(geom.XY, []float64{1, 2, 4, 5, 7, 8, 1, 2}, [][]int{{8}, {8}}),
			).SetSRID(4326),
			geom.XYZ,
			geom.NewGeometryCollection().MustSetLayout(geom.XYZ).MustPush(
				geom.NewPointFlat(geom.XYZ, []float64{1, 2, 0}),
				geom.NewLineStringFlat(geom.XYZ, []float64{1, 2, 0, 4, 5, 0}),
				geom.NewMultiPointFlat(geom.XYZ, []float64{1, 2, 0, 4, 5, 0}, geom.NewMultiPointFlatOptionWithEnds([]int{3, 3, 6})),
				geom.NewMultiLineStringFlat(geom.XYZ, []float64{1, 2, 0, 4, 5, 0}, []int{3, 3, 6, 6}),
				geom.NewMultiPolygonFlat(geom.XYZ, []float64{1, 2, 0, 4, 5, 0, 7, 8, 0, 1, 2, 0}, [][]int{{12}, {12}}),
			).SetSRID(4326),
		},
	}

	for _, tc := range testCases {
		text, err := wkt.Marshal(tc.g)
		require.NoError(t, err)
		t.Run(fmt.Sprintf("%s->%s", text, tc.layout), func(t *testing.T) {
			ret, err := forceLayout(tc.g, tc.layout, 0, 0)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}
