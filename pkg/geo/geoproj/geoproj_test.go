// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoproj

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProject(t *testing.T) {
	testCases := []struct {
		desc string

		from    geoprojbase.Proj4Text
		to      geoprojbase.Proj4Text
		xCoords []float64
		yCoords []float64
		zCoords []float64

		expectedXCoords []float64
		expectedYCoords []float64
		// Ignore Z Coord for now; it usually has a garbage value.
	}{
		{
			desc:            "SRID 4326 to 3857",
			from:            geoprojbase.MakeProj4Text("+proj=longlat +datum=WGS84 +no_defs"),
			to:              geoprojbase.MakeProj4Text("+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"),
			xCoords:         []float64{1},
			yCoords:         []float64{1},
			zCoords:         []float64{0},
			expectedXCoords: []float64{111319.490793274},
			expectedYCoords: []float64{111325.142866385},
		},
		{
			desc:            "SRID 3857 to 4326",
			from:            geoprojbase.MakeProj4Text("+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs"),
			to:              geoprojbase.MakeProj4Text("+proj=longlat +datum=WGS84 +no_defs"),
			xCoords:         []float64{1},
			yCoords:         []float64{1},
			zCoords:         []float64{0},
			expectedXCoords: []float64{0.000008983152841},
			expectedYCoords: []float64{0.000008983152841},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := Project(tc.from, tc.to, tc.xCoords, tc.yCoords, tc.zCoords)
			require.NoError(t, err)
			assert.InEpsilonSlicef(t, tc.expectedXCoords, tc.xCoords, 1e-10, "expected: %#v, found %#v", tc.expectedXCoords, tc.xCoords)
			assert.InEpsilonSlicef(t, tc.expectedYCoords, tc.yCoords, 1e-10, "expected: %#v, found %#v", tc.expectedYCoords, tc.yCoords)
		})
	}

	t.Run("test error handling", func(t *testing.T) {
		err := Project(
			geoprojbase.MakeProj4Text("+bad"),
			geoprojbase.MakeProj4Text("+bad"),
			[]float64{1},
			[]float64{2},
			[]float64{3},
		)
		require.Error(t, err)
	})
}
