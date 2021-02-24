// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geogen provides utilities for generating various geospatial types.
package geogen

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

const numRuns = 25

func TestRandomValidLinearRingCoords(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()

	for run := 0; run < numRuns; run++ {
		t.Run(strconv.Itoa(run), func(t *testing.T) {
			coords := RandomValidLinearRingCoords(rng, 10, MakeRandomGeomBoundsForGeography(), geom.NoLayout)
			require.Len(t, coords, 10+1)
			for _, coord := range coords {
				require.True(t, -180 <= coord.X() && coord.X() <= 180)
				require.True(t, -90 <= coord.Y() && coord.Y() <= 90)
			}
			require.Equal(t, coords[0], coords[len(coords)-1])
		})
	}
}

func TestRandomGeomT(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	for run := 0; run < numRuns; run++ {
		t.Run(strconv.Itoa(run), func(t *testing.T) {
			g := RandomGeomT(rng, MakeRandomGeomBoundsForGeography(), geopb.SRID(run), geom.NoLayout)
			require.Equal(t, run, g.SRID())
			require.True(t, g.Layout() != geom.NoLayout)
			if gc, ok := g.(*geom.GeometryCollection); ok {
				for gcIdx := 0; gcIdx < gc.NumGeoms(); gcIdx++ {
					require.True(t, gc.Geom(gcIdx).Layout() != geom.NoLayout)
					coords := gc.Geom(gcIdx).FlatCoords()
					for i := 0; i < len(coords); i += g.Stride() {
						x := coords[i]
						y := coords[i+1]
						require.True(t, -180 <= x && x <= 180)
						require.True(t, -90 <= y && y <= 90)
					}
				}
			} else {
				coords := g.FlatCoords()
				for i := 0; i < len(coords); i += g.Stride() {
					x := coords[i]
					y := coords[i+1]
					require.True(t, -180 <= x && x <= 180)
					require.True(t, -90 <= y && y <= 90)
				}
			}
		})
	}
}
