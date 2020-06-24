// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestCentroid(t *testing.T) {
	testCases := []struct {
		wkt                      string
		expectedSphereCentroid   string
		expectedSpheroidCentroid string
	}{
		{
			wkt:                      `POINT (1.0 1.0)`,
			expectedSphereCentroid:   `POINT (1.0 1.0)`,
			expectedSpheroidCentroid: `POINT (1.0 1.0)`,
		},
		{
			wkt:                      `SRID=4326;POINT (1.0 1.0)`,
			expectedSphereCentroid:   `SRID=4326;POINT (1.0 1.0)`,
			expectedSpheroidCentroid: `SRID=4326;POINT (1.0 1.0)`,
		},
		{
			wkt:                      `LINESTRING (1.0 1.0, 2.0 2.0, 3.0 3.0)`,
			expectedSphereCentroid:   `POINT (1.99961910254054 2.00007602871312)`,
			expectedSpheroidCentroid: `POINT (1.99962062254316 2.00007754894762)`,
		},
		{
			wkt:                      `LINESTRING (-1.0 -1.0, 1.0 -1.0, 1.0 -30.0, -1.0 -30.0)`,
			expectedSphereCentroid:   `POINT(0.885490683311801 -15.3793090065665)`,
			expectedSpheroidCentroid: `POINT(0.884862829191911 -15.3793503404433)`,
		},
		{
			wkt:                      `POLYGON ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))`,
			expectedSphereCentroid:   `POINT (0.499999916440062 0.500006427946943)`,
			expectedSpheroidCentroid: `POINT (0.500000001396299 0.500006342987471)`,
		},
		{
			wkt:                      `POLYGON ((1.0 1.0, 0.0 1.0, 0.0 0.0, 1.0 0.0, 1.0 1.0))`,
			expectedSphereCentroid:   `POINT (0.499999916440062 0.500006427946943)`,
			expectedSpheroidCentroid: `POINT (0.500000001396299 0.500006342987471)`,
		},
		{
			wkt:                      `POLYGON ((0.0 0.0, 80.0 0.0, 80.0 80.0, 0.0 0.0))`,
			expectedSphereCentroid:   `POINT (43.8350223319301 30.5453774409703)`,
			expectedSpheroidCentroid: `POINT (43.8350223319301 30.5453774409703)`,
		},
		{
			wkt:                      `POLYGON ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))`,
			expectedSphereCentroid:   `POINT (0.500000001981181 0.500010402851408)`,
			expectedSpheroidCentroid: `POINT (0.499999862652707 0.500010542184438)`,
		},
		{
			wkt:                      `POLYGON ((1.0 1.0, 0.0 1.0, 0.0 0.0, 1.0 0.0, 1.0 1.0), (0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1))`,
			expectedSphereCentroid:   `POINT (0.500000001981181 0.500010402851408)`,
			expectedSpheroidCentroid: `POINT (0.499999862652707 0.500010542184438)`,
		},
		{
			wkt:                      `POLYGON ((0.0 1.0, 0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0), (0.9 0.9, 0.1 0.9, 0.1 0.1, 0.9 0.1, 0.9 0.9))`,
			expectedSphereCentroid:   `POINT (0.500000001981181 0.500010402851408)`,
			expectedSpheroidCentroid: `POINT (0.499999862652707 0.500010542184438)`,
		},
		{
			wkt:                      `POLYGON ((0.0 0.0, -3.0 0.0, -4.0 -4.0, 0.0 -3.0, 0.0 0.0), (-1.0 -1.0, -2.0 -1.0, -2.0 -2.0, -1.0 -2.0, -1.0 -1.0))`,
			expectedSphereCentroid:   `POINT (-1.86284249441196 -1.864020960242764)`,
			expectedSpheroidCentroid: `POINT (-1.862840114700246 -1.864022599497527)`,
		},
		{
			wkt:                      `MULTIPOINT (0.0 0.0, 1.0 10.0, 2.0 30.0)`,
			expectedSphereCentroid:   `POINT (0.953002819213836 13.2963208764279)`,
			expectedSpheroidCentroid: `POINT (0.953002819213836 13.2963208764279)`,
		},
		{
			wkt:                      `MULTILINESTRING ((1.0 1.0, 2.0 2.0, 3.0 3.0), (0.0 0.0, 1.0 10.0, 2.0 30.0))`,
			expectedSphereCentroid:   `POINT (1.20651354391928 13.8361533064176)`,
			expectedSpheroidCentroid: `POINT (1.20691326039874 13.8369776044908)`,
		},
		{
			wkt:                      `MULTIPOLYGON (((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0)), ((10.0 10.0, 14.0 10.0, 14.0 14.0, 10.0 14.0, 10.0 10.0)))`,
			expectedSphereCentroid:   `POINT (11.298142253801448 11.327334347758455)`,
			expectedSpheroidCentroid: `POINT (11.298497717682135 11.327735897664535)`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			geog, err := geo.ParseGeography(tc.wkt)
			require.NoError(t, err)

			retSphereCentroid, err := Centroid(geog, UseSphere)
			require.NoError(t, err)
			retGeomSphereCentroid, err := retSphereCentroid.AsGeomT()
			require.NoError(t, err)
			retSpheroidCentroid, err := Centroid(geog, UseSpheroid)
			require.NoError(t, err)
			retGeomSpheroidCentroid, err := retSpheroidCentroid.AsGeomT()
			require.NoError(t, err)

			expSphereCentroid, err := geo.ParseGeography(tc.expectedSphereCentroid)
			require.NoError(t, err)
			expGeomSphereCentroid, err := expSphereCentroid.AsGeomT()
			require.NoError(t, err)
			expSpheroidCentroid, err := geo.ParseGeography(tc.expectedSpheroidCentroid)
			require.NoError(t, err)
			expGeomSpheroidCentroid, err := expSpheroidCentroid.AsGeomT()
			require.NoError(t, err)

			// Ensure points are close in terms of precision.
			require.InEpsilon(t, expGeomSphereCentroid.(*geom.Point).X(), retGeomSphereCentroid.(*geom.Point).X(), 1e-6)
			require.InEpsilon(t, expGeomSphereCentroid.(*geom.Point).Y(), retGeomSphereCentroid.(*geom.Point).Y(), 1e-6)
			require.Equal(t, retSpheroidCentroid.SRID(), expSphereCentroid.SRID())
			require.InEpsilon(t, expGeomSpheroidCentroid.(*geom.Point).X(), retGeomSpheroidCentroid.(*geom.Point).X(), 1e-6)
			require.InEpsilon(t, expGeomSpheroidCentroid.(*geom.Point).Y(), retGeomSpheroidCentroid.(*geom.Point).Y(), 1e-6)
			require.Equal(t, retSpheroidCentroid.SRID(), expSphereCentroid.SRID())
		})
	}

	t.Run("EMPTY GeometryCollection for EMPTY Geographical object", func(t *testing.T) {
		for _, wkt := range []string{
			`LINESTRING EMPTY`,
			`POLYGON EMPTY`,
			`GEOMETRYCOLLECTION EMPTY`,
			`GEOMETRYCOLLECTION (LINESTRING EMPTY, POLYGON EMPTY)`,
		} {
			for _, useSphereOrSpheroid := range []UseSphereOrSpheroid{UseSphere, UseSpheroid} {
				t.Run(fmt.Sprintf("Centroid(geography=%s, spheroid=%t)", wkt, useSphereOrSpheroid), func(t *testing.T) {
					geog, err := geo.ParseGeography(wkt)
					require.NoError(t, err)
					ret, err := Centroid(geog, useSphereOrSpheroid)
					require.NoError(t, err)
					exp, err := geo.ParseGeography(`GEOMETRYCOLLECTION EMPTY`)
					require.NoError(t, err)
					require.Equal(t, ret, exp)
				})
			}
		}
	})

	t.Run(`error for Non EMPTY GeometryCollection`, func(t *testing.T) {
		geog, err := geo.ParseGeography(`GEOMETRYCOLLECTION (POINT (0.0 0.0), LINESTRING EMPTY)`)
		require.NoError(t, err)
		_, err = Centroid(geog, UseSpheroid)
		require.EqualError(t, err, "unhandled geography type GeometryCollection")
	})
}
