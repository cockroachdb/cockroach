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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestSwapOrdinates(t *testing.T) {
	tests := []struct {
		name      string
		input1    geom.T
		input2    string
		want      geom.T
		errString string
	}{
		{
			name:      "swap ordinates with invalid ords spec",
			input1:    geom.NewPointFlat(geom.XY, []float64{10.2, 20.2}),
			input2:    "ax",
			want:      geom.NewPointFlat(geom.XY, []float64{10.2, 20.2}),
			errString: "invalid ordinate specification. need two letters from the set (x, y, z and m)",
		},

		{
			name:      "swap ordinates with the geometry doesn't have m ordinate",
			input1:    geom.NewPointFlat(geom.XY, []float64{10.2, 20.2}),
			input2:    "mx",
			want:      geom.NewPointFlat(geom.XY, []float64{10.2, 20.2}),
			errString: "geometry does not have an m ordinate",
		},
		{
			name:   "swap ordinate 2D point XY with the same X and Y value",
			input1: geom.NewPointFlat(geom.XY, []float64{20.2, 20.2}),
			input2: "yx",
			want:   geom.NewPointFlat(geom.XY, []float64{20.2, 20.2}),
		},
		{
			name:   "swap ordinate 2D point XY(capitalize)  with the same X and Y value",
			input1: geom.NewPointFlat(geom.XY, []float64{20.2, 20.2}),
			input2: "Yx",
			want:   geom.NewPointFlat(geom.XY, []float64{20.2, 20.2}),
		},
		{
			name:   "swap x to y from ordinate 2D POINT",
			input1: geom.NewPointFlat(geom.XY, []float64{10.2, 20.2}),
			input2: "xy",
			want:   geom.NewPointFlat(geom.XY, []float64{20.2, 10.2}),
		},
		{
			name:   "swap y to x from ordinate 2D Line String",
			input1: geom.NewLineStringFlat(geom.XY, []float64{1, 2, 3, 4}),
			input2: "yx",
			want:   geom.NewLineStringFlat(geom.XY, []float64{2, 1, 4, 3}),
		},
		{
			name:   "swap y to x from ordinate 2D Line String with same coordinates",
			input1: geom.NewLineStringFlat(geom.XY, []float64{2, 2, 3, 3}),
			input2: "yx",
			want:   geom.NewLineStringFlat(geom.XY, []float64{2, 2, 3, 3}),
		},
		{
			name:   "swap y to x from ordinate 2D Polygon",
			input1: geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5.55, 5.55, 0, 10, 0, 0}, []int{8}),
			input2: "yx",
			want:   geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5.55, 5.55, 10, 0, 0, 0}, []int{8}),
		},
		{
			name:   "swap y to x from coordinates of a multi polygon",
			input1: geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 4, 0, 4, 4, 0, 4, 0, 0, 1, 1, 2, 1, 2, 2, 1, 2, 1, 1, -1, -1, -1, -2, -2, -2, -2, -1, -1, -1}, [][]int{{10, 20}, {30}}),
			input2: "yx",
			want:   geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 0, 4, 4, 4, 4, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, -1, -1, -2, -1, -2, -2, -1, -2, -1, -1}, [][]int{{10, 20}, {30}}),
		},
		{
			name:   "swap y to x from coordinates of a multi line string",
			input1: geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 1, 2, 2, 3, 3, 2, 5, 4}, []int{6, 12}),
			input2: "yx",
			want:   geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 1, 3, 2, 2, 3, 4, 5}, []int{6, 12}),
		},
		{
			name:   "swap y to x from coordinates of an empty line string",
			input1: geom.NewLineString(geom.XY),
			input2: "yx",
			want:   geom.NewLineString(geom.XY),
		},
		{
			name:   "swap y to x from coordinates of an empty polygon",
			input1: geom.NewPolygon(geom.XY),
			input2: "yx",
			want:   geom.NewPolygon(geom.XY),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tt.input1)
			require.NoError(t, err)

			got, err := SwapOrdinates(geometry, tt.input2)
			if (err != nil) && (tt.errString != err.Error()) {
				t.Errorf("SwapOrdinates() error = %v, wantErr %v", err, tt.errString)
				return
			}

			geometryWant, err := geo.MakeGeometryFromGeomT(tt.want)
			require.NoError(t, err)
			require.Equal(t, geometryWant, got)
		})
	}
}

func TestSwapOrdinates_Collection(t *testing.T) {
	tests := []struct {
		name      string
		input1    *geom.GeometryCollection
		input2    string
		want      *geom.GeometryCollection
		errString string
	}{
		{
			name: "swap xy from coordinates of non-empty collection with no error",
			input1: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1.1, 3}),
				geom.NewPointFlat(geom.XY, []float64{3, 3}),
				geom.NewPolygonFlat(geom.XY, []float64{150, 85, -30, 85, -20, 85, 160, 85, 150, 85}, []int{10}),
			),
			input2: "xy",
			want: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{3, 1.1}),
				geom.NewPointFlat(geom.XY, []float64{3, 3}),
				geom.NewPolygonFlat(geom.XY, []float64{85, 150, 85, -30, 85, -20, 85, 160, 85, 150}, []int{10}),
			),
			errString: "",
		},
		{
			name: "swap xy from coordinates of non-empty collection with error",
			input1: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1.1, 3}),
				geom.NewPointFlat(geom.XY, []float64{3, 3}),
			),
			input2: "xz",
			want: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1.1, 3}),
				geom.NewPointFlat(geom.XY, []float64{3, 3}),
			),
			errString: "geometry does not have an z ordinate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tt.input1)
			require.NoError(t, err)

			got, err := SwapOrdinates(geometry, tt.input2)
			if (err != nil) && (tt.errString != err.Error()) {
				t.Errorf("SwapOrdinates() error = %v, wantErr %v", err, tt.errString)
				return
			}

			g, err := got.AsGeomT()
			require.NoError(t, err)

			gotCollection, ok := g.(*geom.GeometryCollection)
			require.True(t, ok)

			require.Equal(t, tt.want, gotCollection)
		})
	}
}

// Test_getOrdsIndices tests the index position of Z and M
// since currently the geo.Geometry doesn't support ordinates Z and M
func Test_getOrdsIndices(t *testing.T) {
	tests := []struct {
		name   string
		input1 geom.T
		input2 string
		want   [2]int
	}{
		{
			name:   "the index position of m is on index 2",
			input1: geom.NewPointFlat(geom.XYM, []float64{12, 2, 6}),
			input2: "xm",
			want:   [2]int{0, 2},
		},
		{
			name:   "the index position of m is on index 3",
			input1: geom.NewPointFlat(geom.XYZM, []float64{12, 2, 6, 7}),
			input2: "xm",
			want:   [2]int{0, 3},
		},
		{
			name:   "the index position of z is on index 2",
			input1: geom.NewPointFlat(geom.XYZM, []float64{12, 2, 6, 7}),
			input2: "zy",
			want:   [2]int{2, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getOrdsIndices(tt.input1.Layout(), tt.input2)
			assert.NoError(t, err)
			assert.Equal(t, got, tt.want)
		})
	}
}
