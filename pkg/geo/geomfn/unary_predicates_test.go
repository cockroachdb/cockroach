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

func TestIsCollection(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"POINT(1.0 1.0)", false},
		{"POINT EMPTY", false},
		{"LINESTRING(1.0 1.0, 2.0 2.0)", false},
		{"LINESTRING EMPTY", false},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", false},
		{"POLYGON EMPTY", false},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", true},
		{"MULTIPOINT EMPTY", true},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", true},
		{"MULTILINESTRING EMPTY", true},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", true},
		{"MULTIPOLYGON EMPTY", true},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40))", true},
		{"GEOMETRYCOLLECTION EMPTY", true},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(POINT (40 10),LINESTRING (10 10, 20 20, 10 40)))", true},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)", true},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsCollection(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestIsEmpty(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected bool
	}{
		{"POINT(1.0 1.0)", false},
		{"POINT EMPTY", true},
		{"LINESTRING(1.0 1.0, 2.0 2.0)", false},
		{"LINESTRING EMPTY", true},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", false},
		{"POLYGON EMPTY", true},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", false},
		{"MULTIPOINT EMPTY", true},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", false},
		{"MULTILINESTRING EMPTY", true},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", false},
		{"MULTIPOLYGON EMPTY", true},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40))", false},
		{"GEOMETRYCOLLECTION EMPTY", true},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION(POINT (40 10),LINESTRING (10 10, 20 20, 10 40)))", false},
		{"GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)", true},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := IsEmpty(g)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ret)
		})
	}
}
