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

var orientationTestCases = []struct {
	desc         string
	wkt          string
	isCCW        bool
	isCW         bool
	forcedCCWWKT string
	forcedCWWKT  string
}{
	{
		desc:         "POINT",
		wkt:          "POINT(10 20)",
		isCCW:        true,
		isCW:         true,
		forcedCCWWKT: "POINT(10 20)",
		forcedCWWKT:  "POINT(10 20)",
	},
	{
		desc:         "MULTIPOINT",
		wkt:          "MULTIPOINT((10 20), (20 30))",
		isCCW:        true,
		isCW:         true,
		forcedCCWWKT: "MULTIPOINT((10 20), (20 30))",
		forcedCWWKT:  "MULTIPOINT((10 20), (20 30))",
	},
	{
		desc:         "LINESTRING",
		wkt:          "LINESTRING(10 20, 20 30)",
		isCCW:        true,
		isCW:         true,
		forcedCCWWKT: "LINESTRING(10 20, 20 30)",
		forcedCWWKT:  "LINESTRING(10 20, 20 30)",
	},
	{
		desc:         "MULTILINESTRING",
		wkt:          "MULTILINESTRING((10 20, 20 30), (20 30, 30 40))",
		isCCW:        true,
		isCW:         true,
		forcedCCWWKT: "MULTILINESTRING((10 20, 20 30), (20 30, 30 40))",
		forcedCWWKT:  "MULTILINESTRING((10 20, 20 30), (20 30, 30 40))",
	},
	{
		desc:         "POLYGON from wikipedia",
		wkt:          "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
		isCCW:        true,
		isCW:         false,
		forcedCCWWKT: "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
		forcedCWWKT:  "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))",
	},
	{
		desc:         "POLYGON with interior rings correctly CW",
		wkt:          "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
		isCCW:        false,
		isCW:         true,
		forcedCCWWKT: "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))",
		forcedCWWKT:  "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
	},
	{
		desc:         "POLYGON with interior rings correctly CCW",
		wkt:          "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))",
		isCCW:        true,
		isCW:         false,
		forcedCCWWKT: "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))",
		forcedCWWKT:  "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
	},
	{
		desc:         "POLYGON with all exterior and interior rings CW",
		wkt:          "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))",
		isCCW:        false,
		isCW:         false,
		forcedCCWWKT: "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))",
		forcedCWWKT:  "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))",
	},
	{
		desc: "MultiPolygon all CW",
		wkt: `MULTIPOLYGON(
			((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)),
			((10 10, 10 11, 11 11, 11 10, 10 10), (10.1 10.1, 10.9 10.1, 10.9 10.9, 10.1 10.9, 10.1 10.1))
		)`,
		isCCW: false,
		isCW:  true,
		forcedCCWWKT: `MULTIPOLYGON(
			((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)),
			((10 10, 11 10, 11 11, 10 11, 10 10), (10.1 10.1, 10.1 10.9, 10.9 10.9, 10.9 10.1, 10.1 10.1))
		)`,
		forcedCWWKT: `MULTIPOLYGON(
			((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)),
			((10 10, 10 11, 11 11, 11 10, 10 10), (10.1 10.1, 10.9 10.1, 10.9 10.9, 10.1 10.9, 10.1 10.1))
		)`,
	},
	{
		desc: "MultiPolygon mixed everything",
		wkt: `MULTIPOLYGON(
			((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)),
			((10 10, 11 10, 11 11, 10 11, 10 10), (10.1 10.1, 10.9 10.1, 10.9 10.9, 10.1 10.9, 10.1 10.1))
		)`,
		isCCW: false,
		isCW:  false,
		forcedCCWWKT: `MULTIPOLYGON(
			((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)),
			((10 10, 11 10, 11 11, 10 11, 10 10), (10.1 10.1, 10.1 10.9, 10.9 10.9, 10.9 10.1, 10.1 10.1))
		)`,
		forcedCWWKT: `MULTIPOLYGON(
			((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)),
			((10 10, 10 11, 11 11, 11 10, 10 10), (10.1 10.1, 10.9 10.1, 10.9 10.9, 10.1 10.9, 10.1 10.1))
		)`,
	},
	{
		desc: "GEOMETRYCOLLECTION all CW",
		wkt: `GEOMETRYCOLLECTION(
		LINESTRING(0 0, 1 0),
		MULTIPOLYGON(
			((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)),
			((10 10, 10 11, 11 11, 11 10, 10 10), (10.1 10.1, 10.9 10.1, 10.9 10.9, 10.1 10.9, 10.1 10.1))
		),
		POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))
	)`,
		isCCW: false,
		isCW:  true,
		forcedCCWWKT: `GEOMETRYCOLLECTION(
		LINESTRING(0 0, 1 0),
		MULTIPOLYGON(
			((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)),
			((10 10, 11 10, 11 11, 10 11, 10 10), (10.1 10.1, 10.1 10.9, 10.9 10.9, 10.9 10.1, 10.1 10.1))
		),
		POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))
		)`,
		forcedCWWKT: `GEOMETRYCOLLECTION(
		LINESTRING(0 0, 1 0),
		MULTIPOLYGON(
			((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)),
			((10 10, 10 11, 11 11, 11 10, 10 10), (10.1 10.1, 10.9 10.1, 10.9 10.9, 10.1 10.9, 10.1 10.1))
		),
		POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))
		)`,
	},
	{
		desc: "GEOMETRYCOLLECTION mixed everything",
		wkt: `GEOMETRYCOLLECTION(
		LINESTRING(0 0, 1 0),
		MULTIPOLYGON(
			((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)),
			((10 10, 11 10, 11 11, 10 11, 10 10), (10.1 10.1, 10.9 10.1, 10.9 10.9, 10.1 10.9, 10.1 10.1))
		),
		POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))
	)`,
		isCCW: false,
		isCW:  false,
		forcedCCWWKT: `GEOMETRYCOLLECTION(
		LINESTRING(0 0, 1 0),
		MULTIPOLYGON(
			((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)),
			((10 10, 11 10, 11 11, 10 11, 10 10), (10.1 10.1, 10.1 10.9, 10.9 10.9, 10.9 10.1, 10.1 10.1))
		),
		POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))
		)`,
		forcedCWWKT: `GEOMETRYCOLLECTION(
		LINESTRING(0 0, 1 0),
		MULTIPOLYGON(
			((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)),
			((10 10, 10 11, 11 11, 11 10, 10 10), (10.1 10.1, 10.9 10.1, 10.9 10.9, 10.1 10.9, 10.1 10.1))
		),
		POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))
		)`,
	},
}

func TestHasPolygonOrientation(t *testing.T) {
	for _, tc := range orientationTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)

			t.Run("ccw", func(t *testing.T) {
				ret, err := HasPolygonOrientation(g, OrientationCCW)
				require.NoError(t, err)
				require.Equal(t, tc.isCCW, ret)
			})

			t.Run("cw", func(t *testing.T) {
				ret, err := HasPolygonOrientation(g, OrientationCW)
				require.NoError(t, err)
				require.Equal(t, tc.isCW, ret)
			})
		})
	}
}

func TestForcePolygonOrientation(t *testing.T) {
	for _, tc := range orientationTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)

			t.Run("ccw", func(t *testing.T) {
				ret, err := ForcePolygonOrientation(g, OrientationCCW)
				require.NoError(t, err)
				require.Equal(t, geo.MustParseGeometry(tc.forcedCCWWKT), ret)
			})

			t.Run("cw", func(t *testing.T) {
				ret, err := ForcePolygonOrientation(g, OrientationCW)
				require.NoError(t, err)
				require.Equal(t, geo.MustParseGeometry(tc.forcedCWWKT), ret)
			})
		})
	}
}
