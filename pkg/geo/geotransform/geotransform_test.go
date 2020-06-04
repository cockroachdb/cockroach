// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geotransform

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestTransform(t *testing.T) {
	testCases := []struct {
		desc string
		t    geom.T
		from geoprojbase.Proj4Text
		to   geoprojbase.Proj4Text
		srid geopb.SRID

		expectedFlatCoords []float64
	}{
		{
			desc: "3857 to 3785",
			t:    geom.NewPointFlat(geom.XYZM, []float64{1, 2, 3, 4}),
			from: geoprojbase.MakeProj4Text("+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"),
			to:   geoprojbase.MakeProj4Text("+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"),
			srid: 3785,

			expectedFlatCoords: []float64{1, 1.99999999937097, 3, 4},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret, err := transform(tc.t, tc.from, tc.to, tc.srid)
			require.NoError(t, err)
			require.InEpsilonSlicef(t, tc.expectedFlatCoords, ret.FlatCoords(), 0.001, "expected %#v, got %#v", tc.expectedFlatCoords, ret.FlatCoords())
			require.Equal(t, tc.srid, geopb.SRID(ret.SRID()))
		})
	}
}
