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

func TestNode(t *testing.T) {
	tests := []struct {
		name    string
		arg     geo.Geometry
		want    geo.Geometry
		wantErr bool
	}{
		{
			"LineString, 3 nodes",
			geo.MustParseGeometry("LINESTRING(0 0, 10 10, 0 10, 10 0)"),
			geo.MustParseGeometry("MULTILINESTRING((0 0,5 5),(5 5,10 10,0 10,5 5),(5 5,10 0))"),
			false,
		},
		{
			"LineString, same point",
			geo.MustParseGeometry("SRID=4326;LINESTRING (-0.06435 -0.06948, -0.06435 -0.06948)"),
			geo.MustParseGeometry("SRID=4326;GEOMETRYCOLLECTION EMPTY"),
			false,
		},
		{
			"LineString, 4 nodes",
			geo.MustParseGeometry("LINESTRING(0 0, 10 10, 0 10, 10 0, 10 10)"),
			geo.MustParseGeometry("MULTILINESTRING((0 0,5 5),(5 5,10 10),(10 10,0 10,5 5),(5 5,10 0,10 10))"),
			false,
		},
		{
			"LineString, two lines intersection in one point",
			geo.MustParseGeometry("LINESTRING(0 0, 10 10, 0 10, 10 0, 10 10, 10 5, 0 5)"),
			geo.MustParseGeometry("MULTILINESTRING((0 0,5 5),(5 5,0 5),(10 5,5 5),(5 5,10 10),(10 10,0 10,5 5),(5 5,10 0,10 5),(10 5,10 10))"),
			false,
		},
		{
			"LineString consisting of duplicated points",
			geo.MustParseGeometry("LINESTRING(0 0, 10 10, 0 10, 10 0, 0 10, 0 0)"),
			geo.MustParseGeometry("MULTILINESTRING((5 5,10 10,0 10),(0 10,0 0),(0 0,5 5),(0 10,5 5),(5 5,10 0))"),
			false,
		},
		{
			"MultiLineString",
			geo.MustParseGeometry("MULTILINESTRING((1 1, 4 4), (1 3, 4 2))"),
			geo.MustParseGeometry("MULTILINESTRING((1 1, 2.5 2.5), (1 3, 2.5 2.5), (2.5 2.5, 4 4), (2.5 2.5, 4 2))"),
			false,
		},
		{
			"MultiLineString, one line intersected twice by the other",
			geo.MustParseGeometry("MULTILINESTRING((5 0, 5 5), (0 0, 10 2, 0 4))"),
			geo.MustParseGeometry("MULTILINESTRING((0 0,5 1),(5 3,0 4),(5 0,5 1),(5 1,10 2,5 3),(5 1,5 3),(5 3,5 5))"),
			false,
		},
		{
			"MultiLineString with one LineString included in the other",
			geo.MustParseGeometry("MULTILINESTRING((0 0, 10 10, 0 10, 10 0, 0 10), (1 1, 4 4), (1 3, 4 2), (0 0, 10 10, 0 10, 10 0))"),
			geo.MustParseGeometry("MULTILINESTRING((0 0,1 1),(1 1,2.5 2.5),(1 3,2.5 2.5),(2.5 2.5,4 4),(4 4,5 5),(2.5 2.5,4 2),(5 5,10 10,0 10),(0 10,5 5),(5 5,10 0))"),
			false,
		},
		{
			"MultiLineString with no nodes",
			geo.MustParseGeometry("MULTILINESTRING((1 1, 4 4), (0 0, -2 2))"),
			geo.MustParseGeometry("MULTILINESTRING((0 0,-2 2),(1 1,4 4))"),
			false,
		},
		{
			"LineString with no nodes",
			geo.MustParseGeometry("LINESTRING(0 0, -10 10, 0 10)"),
			geo.MustParseGeometry("MULTILINESTRING((0 0, -10 10, 0 10))"),
			false,
		},
		{
			"LineString with specified SRID",
			geo.MustParseGeometry("SRID=4269;LINESTRING(0 0, 10 10, 0 10, 10 0)"),
			geo.MustParseGeometry("SRID=4269;MULTILINESTRING((0 0,5 5),(5 5,10 10,0 10,5 5),(5 5,10 0))"),
			false,
		},
		{
			"unsupported type: Polygon",
			geo.MustParseGeometry("SRID=4269;POLYGON((-71.1776585052917 42.3902909739571,-71.1776820268866 42.3903701743239,-71.1776063012595 42.3903825660754,-71.1775826583081 42.3903033653531,-71.1776585052917 42.3902909739571))"),
			geo.Geometry{},
			true,
		},
		{
			"unsupported type: GeometryCollection",
			geo.MustParseGeometry("GEOMETRYCOLLECTION(POINT(2 0),POLYGON((0 0, 1 0, 1 1, 0 1, 0 0)))"),
			geo.Geometry{},
			true,
		},
		{
			"EMPTY LineString",
			geo.MustParseGeometry("SRID=4326;LINESTRING EMPTY"),
			geo.MustParseGeometry("SRID=4326;GEOMETRYCOLLECTION EMPTY"),
			false,
		},
		{
			"EMPTY MultiLineString",
			geo.MustParseGeometry("SRID=4326;MULTILINESTRING EMPTY"),
			geo.MustParseGeometry("SRID=4326;GEOMETRYCOLLECTION EMPTY"),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Node(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Node() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}
