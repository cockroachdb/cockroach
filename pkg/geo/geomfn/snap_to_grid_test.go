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

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestSnapToGrid(t *testing.T) {
	testCases := []struct {
		inEWKT       string
		origin       geom.Coord
		gridSize     geom.Coord
		expectedEWKT string
	}{
		{
			inEWKT:       "POINT(1.5 1.5)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{0.5, 1, 0, 0},
			expectedEWKT: "POINT(1.5 2)",
		},
		{
			inEWKT:       "POINT(1.75 1.75)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{0.5, 0, 0, 0},
			expectedEWKT: "POINT(2 1.75)",
		},
		{
			inEWKT:       "POINT(1.75 1.75)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{0, 0.5, 0, 0},
			expectedEWKT: "POINT(1.75 2)",
		},
		{
			inEWKT:       "POINT(1.75 1.125)",
			origin:       geom.Coord{0.25, 0.25, 0, 0},
			gridSize:     geom.Coord{0.5, 1, 0, 0},
			expectedEWKT: "POINT(1.75 1.25)",
		},
		{
			inEWKT:       "POINT(1.75 1.375)",
			origin:       geom.Coord{0.25, 0.25, 0, 0},
			gridSize:     geom.Coord{0.5, 1, 0, 0},
			expectedEWKT: "POINT(1.75 1.25)",
		},
		{
			inEWKT:       "POINT(1.75 1.375)",
			origin:       geom.Coord{0.25, 0.25, 0, 0},
			gridSize:     geom.Coord{0.5, 1, 0, 0},
			expectedEWKT: "POINT(1.75 1.25)",
		},
		{
			inEWKT:       "LINESTRING(0 0, 1.5 1.5, 1.6 1.6, 2.25 2.75)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{0.5, 0.5, 0, 0},
			expectedEWKT: "LINESTRING(0 0, 1.5 1.5, 2 3)",
		},
		{
			inEWKT:       "POLYGON((0.01 0.01, 1.0 0.01, 1.01 1.11, 0.01 0.01))",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{0.1, 0.1, 0, 0},
			expectedEWKT: "POLYGON((0 0, 1 0, 1 1.1, 0 0))",
		},
		// Geometries with Z and M dimensions
		{
			inEWKT:       "POINT(2.5 36.1 2.3 8)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{2, 2, 1, 10},
			expectedEWKT: "POINT ZM (2 36 2 10)",
		},
		{
			inEWKT:       "POINT(2.5 36.1 2.3 8)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{2, 2, 0, 0},
			expectedEWKT: "POINT ZM (2 36 2.3 8)",
		},
		{
			inEWKT:       "POINT M EMPTY",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{1, 1, 1, 1},
			expectedEWKT: "POINT M EMPTY",
		},
		// Geometries with insufficient points after snapping
		{
			inEWKT:       "LINESTRING(1.5 1.5, 1.5 1.5, 1.6 1.6, 1.7 1.7)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{0.5, 0.5, 0, 0},
			expectedEWKT: "LINESTRING EMPTY",
		},
		{
			inEWKT:       "POLYGON((0 0, 0 0.5, 0.5 0.5, 0.5 0, 0 0))",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{2, 2, 2, 2},
			expectedEWKT: "POLYGON EMPTY",
		},
		{
			inEWKT:       "MULTILINESTRING((1.5 1.5, 1.5 1.5, 1.6 1.6, 1.7 1.7), EMPTY)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{0.5, 0.5, 0, 0},
			expectedEWKT: "MULTILINESTRING EMPTY",
		},
		{
			inEWKT:       "MULTIPOLYGON(((0 0, 0 0.5, 0.5 0.5, 0.5 0, 0 0)), ((0 0, 0.5 0, 0.25 0.25, 0 0)))",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{2, 2, 2, 2},
			expectedEWKT: "MULTIPOLYGON EMPTY",
		},
		{
			inEWKT: `GEOMETRYCOLLECTION(
LINESTRING(1.5 1.5, 1.5 1.5, 1.6 1.6, 1.7 1.7),
POLYGON((0 0, 0 0.5, 0.5 0.5, 0.5 0, 0 0)))`,
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{2, 2, 2, 2},
			expectedEWKT: "GEOMETRYCOLLECTION EMPTY",
		},
		// Negative grid size ordinates should not snap dimension
		{
			inEWKT:       "LINESTRING(1.5 1.5, 1.5 1.5, 1.6 1.6, 1.7 1.7)",
			origin:       geom.Coord{0, 0, 0, 0},
			gridSize:     geom.Coord{-0.5, -0.5, 0, 0},
			expectedEWKT: "LINESTRING(1.5 1.5, 1.6 1.6, 1.7 1.7)",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/origin:%v/grid:%v", tc.inEWKT, tc.origin, tc.gridSize), func(t *testing.T) {
			in, err := geo.ParseGeometry(tc.inEWKT)
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expectedEWKT)
			require.NoError(t, err)

			actual, err := SnapToGrid(in, tc.origin, tc.gridSize)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}
