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
	"github.com/stretchr/testify/require"
)

func TestMinDistance(t *testing.T) {
	testCases := []struct {
		desc     string
		a        string
		b        string
		expected float64
	}{
		{
			"Same POINTs",
			"POINT(1.0 1.0)",
			"POINT(1.0 1.0)",
			0,
		},
		{
			"Different POINTs",
			"POINT(1.0 1.0)",
			"POINT(2.0 1.0)",
			1,
		},
		{
			"POINT on LINESTRING",
			"POINT(0.5 0.5)",
			"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
			0,
		},
		{
			"POINT away from LINESTRING",
			"POINT(3.0 3.0)",
			"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
			1.4142135623730951,
		},
		{
			"Same LINESTRING",
			"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
			"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
			0,
		},
		{
			"Intersecting LINESTRING",
			"LINESTRING(0.0 0.0, 1.0 1.0, 2.0 2.0)",
			"LINESTRING(0.5 0.0, 0.5 3.0)",
			0,
		},
		{
			"LINESTRING does not meet",
			"LINESTRING(6.0 6.0, 7.0 7.0, 8.0 8.0)",
			"LINESTRING(0.0 0.0, 3.0 -3.0)",
			8.48528137423857,
		},
		{
			"POINT in POLYGON",
			"POINT(0.5 0.5)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			0,
		},
		{
			"POINT outside of POLYGON",
			"POINT(1.5 1.5)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			0.7071067811865476,
		},
		{
			"LINESTRING intersects POLYGON",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"LINESTRING(-0.5 -0.5, 0.5 0.5)",
			0,
		},
		{
			"LINESTRING outside of POLYGON",
			"LINESTRING(-0.5 -0.5, -0.5 0.5)",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			0.5,
		},
		{
			"POLYGON is the same",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			0,
		},
		{
			"POLYGON inside POLYGON",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
			0,
		},
		{
			"POLYGON inside POLYGON hole",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
			"POLYGON((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))",
			0.09999999999999998,
		},
		{
			"POLYGON outside POLYGON",
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))",
			"POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))",
			2.8284271247461903,
		},

		// TODO(otan): more will be filled out once we have our own implementation,
		// leaning on GEOS right now so expecting it to work ok.
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			a, err := geo.ParseGeometry(tc.a)
			require.NoError(t, err)
			b, err := geo.ParseGeometry(tc.b)
			require.NoError(t, err)

			// Try in both directions.
			ret, err := MinDistance(a, b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)

			ret, err = MinDistance(b, a)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}
