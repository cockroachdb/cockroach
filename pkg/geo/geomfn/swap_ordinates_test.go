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
		name                  string
		inGeomCollection      *geom.GeometryCollection
		inSwapOrdinatesString string
		wantGeomCollection    *geom.GeometryCollection
		errString             string
	}{
		{
			name:                  "swap ordinates with invalid ords spec",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{10.2, 20.2})),
			inSwapOrdinatesString: "ax",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{10.2, 20.2})),
			errString:             "invalid ordinate specification. need two letters from the set (x, y, z and m)",
		},
		{
			name:                  "swap ordinates with the geometry doesn't have m ordinate",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{10.2, 20.2})),
			inSwapOrdinatesString: "mx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{10.2, 20.2})),
			errString:             "geometry does not have a M ordinate",
		},
		{
			name:                  "swap ordinate 2D point XY with the same X and Y value",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{20.2, 20.2})),
			inSwapOrdinatesString: "yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{20.2, 20.2})),
		},
		{
			name:                  "swap ordinate 2D point XY(capitalize)  with the same X and Y value",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{20.2, 20.2})),
			inSwapOrdinatesString: "Yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{20.2, 20.2})),
		},
		{
			name:                  "swap x to y from ordinate 2D POINT",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{10.2, 20.2})),
			inSwapOrdinatesString: "xy",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewPointFlat(geom.XY, []float64{20.2, 10.2})),
		},
		{
			name:                  "swap y to x from ordinate 2D Line String",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{1, 2, 3, 4})),
			inSwapOrdinatesString: "yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{2, 1, 4, 3})),
		},
		{
			name:                  "swap y to x from ordinate 2D Line String with same coordinates",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{2, 2, 3, 3})),
			inSwapOrdinatesString: "yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewLineStringFlat(geom.XY, []float64{2, 2, 3, 3})),
		},
		{
			name:                  "swap y to x from ordinate 2D Polygon",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5.55, 5.55, 0, 10, 0, 0}, []int{8})),
			inSwapOrdinatesString: "yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5.55, 5.55, 10, 0, 0, 0}, []int{8})),
		},
		{
			name:                  "swap y to x from coordinates of a multi polygon",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 4, 0, 4, 4, 0, 4, 0, 0, 1, 1, 2, 1, 2, 2, 1, 2, 1, 1, -1, -1, -1, -2, -2, -2, -2, -1, -1, -1}, [][]int{{10, 20}, {30}})),
			inSwapOrdinatesString: "yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewMultiPolygonFlat(geom.XY, []float64{0, 0, 0, 4, 4, 4, 4, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, -1, -1, -2, -1, -2, -2, -1, -2, -1, -1}, [][]int{{10, 20}, {30}})),
		},
		{
			name:                  "swap y to x from coordinates of a multi line string",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 1, 2, 2, 3, 3, 2, 5, 4}, []int{6, 12})),
			inSwapOrdinatesString: "yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 2, 1, 3, 2, 2, 3, 4, 5}, []int{6, 12})),
		},
		{
			name:                  "swap y to x from coordinates of an empty line string",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewLineString(geom.XY)),
			inSwapOrdinatesString: "yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewLineString(geom.XY)),
		},
		{
			name:                  "swap y to x from coordinates of an empty polygon",
			inGeomCollection:      geom.NewGeometryCollection().MustPush(geom.NewPolygon(geom.XY)),
			inSwapOrdinatesString: "yx",
			wantGeomCollection:    geom.NewGeometryCollection().MustPush(geom.NewPolygon(geom.XY)),
		},
		{
			name: "swap xy from coordinates of non-empty collection with no error",
			inGeomCollection: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1.1, 3}),
				geom.NewPointFlat(geom.XY, []float64{3, 3}),
				geom.NewPolygonFlat(geom.XY, []float64{150, 85, -30, 85, -20, 85, 160, 85, 150, 85}, []int{10}),
			),
			inSwapOrdinatesString: "xy",
			wantGeomCollection: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{3, 1.1}),
				geom.NewPointFlat(geom.XY, []float64{3, 3}),
				geom.NewPolygonFlat(geom.XY, []float64{85, 150, 85, -30, 85, -20, 85, 160, 85, 150}, []int{10}),
			),
			errString: "",
		},
		{
			name: "swap xy from coordinates of non-empty collection with error",
			inGeomCollection: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1.1, 3}),
				geom.NewPointFlat(geom.XY, []float64{3, 3}),
			),
			inSwapOrdinatesString: "xz",
			wantGeomCollection: geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1.1, 3}),
				geom.NewPointFlat(geom.XY, []float64{3, 3}),
			),
			errString: "geometry does not have a Z ordinate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geometry, err := geo.MakeGeometryFromGeomT(tt.inGeomCollection)
			require.NoError(t, err)

			got, err := SwapOrdinates(geometry, tt.inSwapOrdinatesString)
			if (err != nil) && (tt.errString != err.Error()) {
				t.Errorf("SwapOrdinates() error = %v, wantErr %v", err, tt.errString)
				return
			}

			geometryWant, err := geo.MakeGeometryFromGeomT(tt.wantGeomCollection)
			require.NoError(t, err)
			require.Equal(t, geometryWant, got)
		})
	}
}

// TestGetOrdsIndices tests the index position of Z and M
// since currently the geo.Geometry doesn't support ordinates Z and M
func TestGetOrdsIndices(t *testing.T) {
	tests := []struct {
		name               string
		inputGeom          geom.T
		inputSwapOrdinates string
		wantIndices        [2]int
	}{
		{
			name:               "the index position of m is on index 2",
			inputGeom:          geom.NewPointFlat(geom.XYM, []float64{12, 2, 6}),
			inputSwapOrdinates: "xm",
			wantIndices:        [2]int{0, 2},
		},
		{
			name:               "the index position of m is on index 3",
			inputGeom:          geom.NewPointFlat(geom.XYZM, []float64{12, 2, 6, 7}),
			inputSwapOrdinates: "xm",
			wantIndices:        [2]int{0, 3},
		},
		{
			name:               "the index position of z is on index 2",
			inputGeom:          geom.NewPointFlat(geom.XYZM, []float64{12, 2, 6, 7}),
			inputSwapOrdinates: "zy",
			wantIndices:        [2]int{2, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getOrdsIndices(tt.inputGeom.Layout(), tt.inputSwapOrdinates)
			assert.NoError(t, err)
			assert.Equal(t, got, tt.wantIndices)
		})
	}
}
