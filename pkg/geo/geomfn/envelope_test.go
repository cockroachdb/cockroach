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

func TestEnvelope(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT(1.0 1.0)", "POINT (1.0 1.0)"},
		{"SRID=4326;POINT(1.0 1.0)", "SRID=4326;POINT (1.0 1.0)"},
		{"SRID=4004;LINESTRING(5 4, 4 4)", "SRID=4004;LINESTRING(4 4, 5 4)"},
		{
			"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
			"POLYGON((0 0,0 1,1 1,1 0,0 0))",
		},
		{
			"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
			"POLYGON((10 10,10 45,45 45,45 10,10 10))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Envelope(g)
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)
			require.Equal(t, expected, ret)
		})
	}
}
