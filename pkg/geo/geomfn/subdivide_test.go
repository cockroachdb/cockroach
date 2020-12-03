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
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/stretchr/testify/require"
)

func TestSubdivide(t *testing.T) {
	type args struct {
		g           geo.Geometry
		maxVertices int
	}
	tests := []struct {
		name string
		args args
		want []geo.Geometry
	}{
		{
			"empty geometry",
			args{geo.MustParseGeometry("POLYGON EMPTY"), 5},
			[]geo.Geometry{geo.MustParseGeometry("POLYGON EMPTY")},
		},
		{
			"width and height bounds equal to zero",
			args{geo.MustParseGeometry("POINT(1 10)"), 5},
			[]geo.Geometry{geo.MustParseGeometry("POINT (1 10)")},
		},
		{
			"single geometry, no. of vertices under tolerance",
			args{geo.MustParseGeometry("LINESTRING(0 0, 10 10, 0 10, 10 0)"), 5},
			[]geo.Geometry{geo.MustParseGeometry("LINESTRING(0 0, 10 10, 0 10, 10 0)")},
		},
		{
			"Polygon, width > height",
			args{geo.MustParseGeometry("POLYGON((-1 -1,-1 -0.5, -1 0, 1 0.5, 1 -1,-1 -1))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("POLYGON((-1 -1,-1 0,0 0.25,0 -1,-1 -1))"),
				geo.MustParseGeometry("POLYGON((0 0.25, 1 0.5, 1 -1, 0 -1, 0 0.25))"),
			},
		},
		{
			"invalid (bow-tie) Polygon, width > height",
			args{geo.MustParseGeometry("POLYGON((0 0, -2 -1, -2 1, 0 0, 2 1, 2 -1, 0 0))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("POLYGON((0 0, -2 -1, -2 1, 0 0))"),
				geo.MustParseGeometry("POLYGON((0 0, 2 1, 2 -1, 0 0))"),
			},
		},
		{
			"invalid (bow-tie) Polygon, height > width",
			args{geo.MustParseGeometry("POLYGON((0 0, -1 -2, -1 2, 0 0, 1 2, 1 -2, 0 0))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("POLYGON((0 0, 1 0, 1 -2, 0 0))"),
				geo.MustParseGeometry("POLYGON((0 0,-1 -2,-1 0,0 0))"),
				geo.MustParseGeometry("POLYGON((-1 0,-1 2,0 0,-1 0))"),
				geo.MustParseGeometry("POLYGON((0 0,1 2,1 0,0 0))"),
			},
		},
		{
			"Polygon, 12 decimal points precision",
			args{geo.MustParseGeometry("POLYGON((-0.1 -0.1,-0.1 -0.000000000005, -0.1 0, 0.1 0.000000000005, 0.1 -0.1,-0.1 -0.1))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("POLYGON((-0.1 -0.1,-0.1 0,0 0.0000000000025,0 -0.1,-0.1 -0.1))"),
				geo.MustParseGeometry("POLYGON((0 0.0000000000025, 0.1 0.000000000005, 0.1 -0.1, 0 -0.1, 0 0.0000000000025))"),
			},
		},
		{
			"Polygon, trapezoid+rectangle",
			args{geo.MustParseGeometry("POLYGON((-1 0, -1 1, 0 2, 3 2, 3 0, -1 0))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("POLYGON((-1 0,-1 1,0 2,0 0,-1 0))"),
				geo.MustParseGeometry("POLYGON((0 2,3 2,3 0,0 0,0 2))"),
			},
		},
		{
			"Polygon with a hole inside",
			args{geo.MustParseGeometry("POLYGON((-1 -1, -1 1, 0 2, 1 1, 1 -1, 0 -2, -1 -1),(-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, 0 -0.5, -0.5 -0.5))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("POLYGON((1 -1,0 -2,-1 -1,1 -1))"),
				geo.MustParseGeometry("POLYGON((-1 -1,-1 -0.5,1 -0.5,1 -1,-1 -1))"),
				geo.MustParseGeometry("POLYGON((-1 -0.5,-1 0.5,-0.5 0.5,-0.5 -0.5,-1 -0.5))"),
				geo.MustParseGeometry("POLYGON((-1 0.5,-1 1,0.5 1,0.5 0.5,-1 0.5))"),
				geo.MustParseGeometry("POLYGON((0.5 1,1 1,1 -0.5,0.5 -0.5,0.5 1))"),
				geo.MustParseGeometry("POLYGON((-1 1,0 2,1 1,-1 1))"),
			},
		},
		{
			"LineString, width < height ",
			args{geo.MustParseGeometry("LINESTRING(0 0, 10 15, 0 0, 10 15, 10 0, 10 15)"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("LINESTRING(0 0,5 7.5)"),
				geo.MustParseGeometry("LINESTRING(10 7.5,10 0)"),
				geo.MustParseGeometry("LINESTRING(5 7.5,10 15)"),
				geo.MustParseGeometry("LINESTRING(10 15,10 7.5)"),
			},
		},
		{
			"LineString, width > height",
			args{geo.MustParseGeometry("LINESTRING(0 0, 15 10, 0 0, 15 10, 15 0, 15 10)"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("LINESTRING(0 0,7.5 5)"),
				geo.MustParseGeometry("LINESTRING(7.5 5,15 10)"),
				geo.MustParseGeometry("LINESTRING(15 10,15 0)"),
			},
		},
		{
			"LineString with specified SRID",
			args{geo.MustParseGeometry("SRID=4269;LINESTRING(0 0, 10 15, 0 0, 10 15, 10 0, 10 15)"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("SRID=4269;LINESTRING(0 0,5 7.5)"),
				geo.MustParseGeometry("SRID=4269;LINESTRING(10 7.5,10 0)"),
				geo.MustParseGeometry("SRID=4269;LINESTRING(5 7.5,10 15)"),
				geo.MustParseGeometry("SRID=4269;LINESTRING(10 15,10 7.5)"),
			},
		},
		{
			"MultiLineString - horizontal and vertical lines",
			args{geo.MustParseGeometry("MULTILINESTRING((5 0, 5 1, 5 3, 5 4, 5 5, 5 6),(0 5, 1 5, 2 5, 3 5, 4 5, 6 5))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("LINESTRING(5 0,5 3)"),
				geo.MustParseGeometry("LINESTRING(5 3,5 6)"),
				geo.MustParseGeometry("LINESTRING(0 5,3 5)"),
				geo.MustParseGeometry("LINESTRING(3 5,6 5)"),
			},
		},
		{
			"MultiPoint, max vertices 6",
			args{geo.MustParseGeometry("MULTIPOINT((0 1),(1 2),(2 3),(3 4),(4 3),(6 2),(7 1),(8 0),(9 -1),(10 -2),(11 -3))"), 6},
			[]geo.Geometry{
				geo.MustParseGeometry("MULTIPOINT(0 1,1 2,2 3,3 4,4 3)"),
				geo.MustParseGeometry("MULTIPOINT(6 2,7 1,8 0,9 -1,10 -2,11 -3)"),
			},
		},
		{
			"GeometryCollection, types with different dimensions",
			args{geo.MustParseGeometry("GEOMETRYCOLLECTION(LINESTRING(0 0, 10 10, 0 10, 10 0), POLYGON((0 0, -2 -1, -2 1, 0 0, 2 1, 2 -1, 0 0)))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("POLYGON((0 0,-2 -1,-2 1,0 0))"),
				geo.MustParseGeometry("POLYGON((0 0,2 1,2 -1,0 0))"),
			},
		},
		{
			"GeometryCollection, types with different dimensions, point included",
			args{geo.MustParseGeometry("GEOMETRYCOLLECTION(LINESTRING(0 0, 10 10, 0 10, 10 0), POINT(1 10))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("LINESTRING(0 0,10 10,0 10,10 0)"),
			},
		},
		{
			"GeometryCollection, same types, one invalid",
			args{geo.MustParseGeometry("GEOMETRYCOLLECTION(POLYGON((0 0, -2 -1, -2 1, 0 0, 2 1, 2 -1, 0 0)),POLYGON((-1 -1,-1 -0.5, -1 0, 1 0.5, 1 -1,-1 -1)))"), 5},
			[]geo.Geometry{
				geo.MustParseGeometry("POLYGON((0 0,-2 -1,-2 1,0 0))"),
				geo.MustParseGeometry("POLYGON((0 0,2 1,2 -1,0 0))"),
				geo.MustParseGeometry("POLYGON((-1 -1,-1 0,0 0.25,0 -1,-1 -1))"),
				geo.MustParseGeometry("POLYGON((0 0.25, 1 0.5, 1 -1, 0 -1, 0 0.25))"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Subdivide(tt.args.g, tt.args.maxVertices)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	t.Run("less max vertices than minimum acceptable", func(t *testing.T) {
		_, err := Subdivide(geo.MustParseGeometry("LINESTRING(0 0, -10 10, 0 10)"), 4)
		require.Error(t, err)
		require.Equal(t, "max_vertices number cannot be less than 5", err.Error())
	})

	t.Run("would need to divide more than the maximum depth", func(t *testing.T) {
		g := geo.MustParseGeometry("POLYGON((-1 -1, -1 1, 0 2, 1 1, 1 -1, 0 -2, -1 -1),(-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, 0 -0.5, -0.5 -0.5))")
		gt, err := g.AsGeomT()
		require.NoError(t, err)
		dim, err := dimensionFromGeomT(gt)
		require.NoError(t, err)
		const maxDepth = 2
		const startDepth = 0
		const maxVertices = 5
		geomTs, err := subdivideRecursive(gt, maxVertices, startDepth, dim, maxDepth)
		require.NoError(t, err)
		got := []geo.Geometry{}
		for _, cg := range geomTs {
			g, err := geo.MakeGeometryFromGeomT(cg)
			require.NoError(t, err)
			g, err = g.CloneWithSRID(geopb.SRID(gt.SRID()))
			require.NoError(t, err)
			got = append(got, g)
		}
		want := []geo.Geometry{
			geo.MustParseGeometry("POLYGON((1 -1, 0 -2, -1 -1, 1 -1))"),
			geo.MustParseGeometry("POLYGON((-1 -1, -1 -0.5, 1 -0.5, 1 -1, -1 -1))"),
			geo.MustParseGeometry("POLYGON((-1 -0.5, -1 1, 1 1, 1 -0.5, 0.5 -0.5, 0.5 0.5, -0.5 0.5, -0.5 -0.5, -1 -0.5))"),
			geo.MustParseGeometry("POLYGON((-1 1, 0 2, 1 1, -1 1))"),
		}
		require.Equal(t, want, got)
	})
}
