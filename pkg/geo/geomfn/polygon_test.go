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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakePolygon(t *testing.T) {
	testCases := []struct {
		name     string
		outer    string
		interior []string
		expected string
		err      error
	}{
		{
			"Single input variant - 2D",
			"LINESTRING(75 29,77 29,77 29, 75 29)",
			[]string{},
			"POLYGON((75 29,77 29,77 29,75 29))",
			nil,
		},
		{
			"Single input variant - 2D Square",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			[]string{},
			"POLYGON((40 80, 80 80, 80 40, 40 40, 40 80))",
			nil,
		},
		{
			"With inner holes variant - 2D Single Ring",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
			},
			"POLYGON((40 80,80 80,80 40,40 40,40 80),(50 70,70 70,70 50,50 50,50 70))",
			nil,
		},
		{
			"With inner holes variant - 2D Two Rings",
			"LINESTRING(40 80, 80 80, 80 40, 40 40, 40 80)",
			[]string{
				"LINESTRING(50 70, 70 70, 70 50, 50 50, 50 70)",
				"LINESTRING(60 60, 75 60, 75 45, 60 45, 60 60)",
			},
			"POLYGON((40 80,80 80,80 40,40 40,40 80),(50 70,70 70,70 50,50 50,50 70),(60 60,75 60,75 45,60 45,60 60))",
			nil,
		},
		{
			"Invalid argument - POINT",
			"POINT(3 2)",
			[]string{},
			"",
			errors.Newf("Argument must be LINESTRING geometries"),
		},
		{
			"Invalid argument - POINT rings",
			"LINESTRING(75 29,77 29,77 29, 75 29)",
			[]string{"POINT(3 2)"},
			"",
			errors.Newf("Argument must be LINESTRING geometries"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			outer := geo.MustParseGeometry(tc.outer)
			interior := make([]*geo.Geometry, 0, len(tc.interior))
			for _, ring := range tc.interior {
				interior = append(interior, geo.MustParseGeometry(ring))
			}
			g, err := MakePolygon(outer, interior...)
			if tc.err != nil {
				require.Errorf(t, err, tc.err.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, geo.MustParseGeometry(tc.expected), g)
			}
		})
	}
}
