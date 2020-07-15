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
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestTranslate(t *testing.T) {
	testCases := []struct {
		desc   string
		g      geom.T
		deltas []float64
	}{
		{
			desc:   "move a 2D point",
			g:      geom.NewPointFlat(geom.XY, []float64{10, 10}),
			deltas: []float64{5, -5},
		},
		{
			desc:   "move a line string",
			g:      geom.NewLineStringFlat(geom.XY, []float64{10, 15, 20, 30}),
			deltas: []float64{1, 5},
		},
		{
			desc:   "move a polygon",
			g:      geom.NewPolygonFlat(geom.XY, []float64{0, 0, 5, 5, 0, 10, 0, 0}, []int{8}),
			deltas: []float64{1, 1},
		},
		{
			desc:   "move multiple 2D points",
			g:      geom.NewMultiPointFlat(geom.XY, []float64{5, 10, -30, 40}),
			deltas: []float64{1, 0.5},
		},
		{
			desc:   "move multiple line strings",
			g:      geom.NewMultiLineStringFlat(geom.XY, []float64{1, 1, 2, 2, 3, 3, 4, 4}, []int{4, 8}),
			deltas: []float64{.1, .1},
		},
	}

	for _, tc := range testCases {
		geometry, err := geo.NewGeometryFromGeomT(tc.g)
		require.NoError(t, err)

		geometry, err = Translate(geometry, tc.deltas)
		require.NoError(t, err)

		got, err := geometry.AsGeomT()
		require.NoError(t, err)

		var want []float64
		for i, c := range tc.g.FlatCoords() {
			delta := tc.deltas[i%got.Stride()]
			want = append(want, delta+c)
		}

		require.Equal(t, want, got.FlatCoords())
	}
}

func TestTranslateCollection(t *testing.T) {
	collection := geom.NewGeometryCollection()
	geoms := []geom.T{
		geom.NewPointFlat(geom.XY, []float64{10, 10}),
		geom.NewLineStringFlat(geom.XY, []float64{0, 0, 5, 5}),
		geom.NewMultiPointFlat(geom.XY, []float64{0, 0, -5, -5, -10, -10}),
	}
	for _, g := range geoms {
		require.NoError(t, collection.Push(g))
	}

	deltas := []float64{1, 1}

	geometry, err := geo.NewGeometryFromGeomT(collection)
	require.NoError(t, err)

	geometry, err = Translate(geometry, deltas)
	require.NoError(t, err)

	g, err := geometry.AsGeomT()
	require.NoError(t, err)

	gotCollection, ok := g.(*geom.GeometryCollection)
	require.True(t, ok)
	require.Equal(t, len(geoms), gotCollection.NumGeoms())

	for i, geometry := range gotCollection.Geoms() {
		var want []float64
		for i, c := range geoms[i].FlatCoords() {
			delta := deltas[i%geometry.Stride()]
			want = append(want, delta+c)
		}
		require.Equal(t, want, geometry.FlatCoords())
	}
}

func TestTranslateErrorMismatchedDeltas(t *testing.T) {
	deltas := []float64{1, 1, 1}
	geometry, err := geo.NewGeometryFromPointCoords(0, 0)
	require.NoError(t, err)

	_, err = Translate(geometry, deltas)
	isErr := errors.Is(err, geom.ErrStrideMismatch{
		Got:  3,
		Want: 2,
	})
	require.True(t, isErr)
}
