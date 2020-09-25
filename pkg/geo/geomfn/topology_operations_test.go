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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestBoundary(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT EMPTY", "POINT EMPTY"},
		{"POINT (1 1)", "GEOMETRYCOLLECTION EMPTY"},
		{"LINESTRING EMPTY", "LINESTRING EMPTY"},
		{"LINESTRING (100 150, 50 60, 70 80, 160 170)", "MULTIPOINT (100 150, 160 170)"},
		{"SRID=4000;LINESTRING (100 150, 50 60, 70 80, 160 170)", "SRID=4000;MULTIPOINT (100 150, 160 170)"},
		{
			"POLYGON ((10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130), (70 40, 100 50, 120 80, 80 110, 50 90, 70 40))",
			"MULTILINESTRING ((10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130), (70 40, 100 50, 120 80, 80 110, 50 90, 70 40))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Boundary(g)
			require.NoError(t, err)

			wkt, err := geo.SpatialObjectToEWKT(ret.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
		})
	}
}

func TestCentroid(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT(1.0 1.0)", "POINT (1.0 1.0)"},
		{"SRID=4326;POINT(1.0 1.0)", "SRID=4326;POINT (1.0 1.0)"},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", "POINT (2.0 2.0)"},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", "POINT (0.666666666666667 0.333333333333333)"},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))", "POINT (0.671717171717172 0.335353535353535)"},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", "POINT (1.5 1.5)"},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", "POINT (3.17541743733684 3.04481549985497)"},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", "POINT (2.17671691792295 1.84187604690117)"},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))", "POINT (35 38.3333333333333)"},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Centroid(g)
			require.NoError(t, err)

			retAsGeomT, err := ret.AsGeomT()
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)
			expectedAsGeomT, err := expected.AsGeomT()
			require.NoError(t, err)

			// Ensure points are close in terms of precision.
			require.InEpsilon(t, expectedAsGeomT.(*geom.Point).X(), retAsGeomT.(*geom.Point).X(), 2e-10)
			require.InEpsilon(t, expectedAsGeomT.(*geom.Point).Y(), retAsGeomT.(*geom.Point).Y(), 2e-10)
			require.Equal(t, expected.SRID(), ret.SRID())
		})
	}
}

func TestConvexHull(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{
			"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
			"POLYGON((10 10,10 40,20 45,40 40,45 30,40 10,10 10))",
		},
		{
			"SRID=4326;GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
			"SRID=4326;POLYGON((10 10,10 40,20 45,40 40,45 30,40 10,10 10))",
		},
		{
			"MULTILINESTRING((100 190,10 8),(150 10, 20 30))",
			"POLYGON((10 8,20 30,100 190,150 10,10 8))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := ConvexHull(g)
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)

			require.Equal(t, expected, ret)
		})
	}
}

func TestDifference(t *testing.T) {
	testCases := []struct {
		wkt1     string
		wkt2     string
		expected string
	}{
		{"POINT EMPTY", "LINESTRING EMPTY", "POINT EMPTY"},
		{"LINESTRING EMPTY", "POINT EMPTY", "LINESTRING EMPTY"},
		{"LINESTRING (50 100, 50 200)", "LINESTRING(50 50, 50 150)", "LINESTRING (50 150, 50 200)"},
		{"SRID=4000;LINESTRING (50 100, 50 200)", "SRID=4000;LINESTRING(50 50, 50 150)", "SRID=4000;LINESTRING (50 150, 50 200)"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v - %v", tc.wkt2, tc.wkt1), func(t *testing.T) {
			g1, err := geo.ParseGeometry(tc.wkt1)
			require.NoError(t, err)
			g2, err := geo.ParseGeometry(tc.wkt2)
			require.NoError(t, err)
			ret, err := Difference(g1, g2)
			require.NoError(t, err)

			wkt, err := geo.SpatialObjectToEWKT(ret.SpatialObject(), 0)
			require.NoError(t, err)
			require.EqualValues(t, tc.expected, wkt)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Difference(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestSimplify(t *testing.T) {
	testCases := []struct {
		wkt       string
		tolerance float64
		expected  string
	}{
		{
			wkt:       "POLYGON ((20 10, 10 20, 20 20, 20 30, 30 30, 30 20, 40 20, 40 10, 30 0, 20 0, 20 10))",
			tolerance: 5,
			expected:  "POLYGON ((20 10, 10 20, 30 30, 40 10, 30 00, 20 0, 20 10))",
		},
		{
			wkt:       "POLYGON ((5 7, 2 5, 5 4, 13 4, 18 7, 16 11, 7 9, 11 7, 5 7), (13 8, 13 6, 14 6, 15 9, 13 8))",
			tolerance: 3,
			expected:  "POLYGON ((5 7, 16 11, 18 7, 2 5, 5 7))",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%f", tc.wkt, tc.tolerance), func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := Simplify(g, tc.tolerance)
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)

			require.Equal(t, expected, ret)
		})
	}
}

func TestSimplifyPreserveTopology(t *testing.T) {
	testCases := []struct {
		wkt       string
		tolerance float64
		expected  string
	}{
		{
			wkt:       "POLYGON ((5 7, 2 5, 5 4, 13 4, 18 7, 16 11, 7 9, 11 7, 5 7), (13 8, 13 6, 14 6, 15 9, 13 8))",
			tolerance: 3,
			expected:  "POLYGON ((5 7, 2 5, 13 4, 18 7, 16 11, 5 7), (13 8, 13 6, 14 6, 15 9, 13 8))",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%f", tc.wkt, tc.tolerance), func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := SimplifyPreserveTopology(g, tc.tolerance)
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)

			require.Equal(t, expected, ret)
		})
	}
}

func TestPointOnSurface(t *testing.T) {
	testCases := []struct {
		wkt      string
		expected string
	}{
		{"POINT(1.0 1.0)", "POINT (1.0 1.0)"},
		{"SRID=4326;POINT(1.0 1.0)", "SRID=4326;POINT (1.0 1.0)"},
		{"LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", "POINT (2.0 2.0)"},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0))", "POINT(0.75 0.5)"},
		{"POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))", "POINT(0.8 0.6)"},
		{"MULTIPOINT((1.0 1.0), (2.0 2.0))", "POINT (1 1)"},
		{"MULTILINESTRING((1.0 1.0, 2.0 2.0, 3.0 3.0), (6.0 6.0, 7.0 6.0))", "POINT (2 2)"},
		{"MULTIPOLYGON(((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 3.0)), ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 0.0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1)))", "POINT(3.75 3.5)"},
		{"GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))", "POINT(39.5833333333333 35)"},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)
			ret, err := PointOnSurface(g)
			require.NoError(t, err)

			retAsGeomT, err := ret.AsGeomT()
			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expected)
			require.NoError(t, err)
			expectedAsGeomT, err := expected.AsGeomT()
			require.NoError(t, err)

			// Ensure points are close in terms of precision.
			require.InEpsilon(t, expectedAsGeomT.(*geom.Point).X(), retAsGeomT.(*geom.Point).X(), 2e-10)
			require.InEpsilon(t, expectedAsGeomT.(*geom.Point).Y(), retAsGeomT.(*geom.Point).Y(), 2e-10)
			require.Equal(t, expected.SRID(), ret.SRID())
		})
	}
}

func TestIntersection(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected geo.Geometry
	}{
		{rightRect, rightRect, geo.MustParseGeometry("POLYGON ((1 0, 0 0, 0 1, 1 1, 1 0))")},
		{geo.MustParseGeometry("LINESTRING EMPTY"), geo.MustParseGeometry("POINT(5 5)"), geo.MustParseGeometry("LINESTRING EMPTY")},
		{geo.MustParseGeometry("POINT(5 5)"), geo.MustParseGeometry("LINESTRING EMPTY"), geo.MustParseGeometry("LINESTRING EMPTY")},
		{rightRect, rightRectPoint, rightRectPoint},
		{rightRectPoint, rightRectPoint, rightRectPoint},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Intersection(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Intersection(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestUnion(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected geo.Geometry
	}{
		{rightRect, rightRect, geo.MustParseGeometry("POLYGON ((1 0, 0 0, 0 1, 1 1, 1 0))")},
		{rightRect, rightRectPoint, geo.MustParseGeometry("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")},
		{rightRectPoint, rightRectPoint, rightRectPoint},
		{leftRect, rightRect, geo.MustParseGeometry("POLYGON ((0 0, -1 0, -1 1, 0 1, 1 1, 1 0, 0 0))")},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := Union(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := Union(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestSymDifference(t *testing.T) {
	testCases := []struct {
		a        geo.Geometry
		b        geo.Geometry
		expected geo.Geometry
	}{
		{rightRect, rightRect, emptyRect},
		{leftRect, rightRect, geo.MustParseGeometry("POLYGON((0 0, -1 0, -1 1, 0 1, 1 1, 1 0, 0 0))")},
		{leftRect, overlappingRightRect, geo.MustParseGeometry("MULTIPOLYGON(((-0.1 0, -1 0, -1 1, -0.1 1, -0.1 0)), ((0 0, 0 1, 1 1, 1 0, 0 0)))")},
		{rightRect, rightRectPoint, geo.MustParseGeometry("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("tc:%d", i), func(t *testing.T) {
			g, err := SymDifference(tc.a, tc.b)
			require.NoError(t, err)
			require.Equal(t, tc.expected, g)
		})
	}

	t.Run("errors if SRIDs mismatch", func(t *testing.T) {
		_, err := SymDifference(mismatchingSRIDGeometryA, mismatchingSRIDGeometryB)
		requireMismatchingSRIDError(t, err)
	})
}

func TestSharedPaths(t *testing.T) {
	type args struct {
		a geo.Geometry
		b geo.Geometry
	}
	tests := []struct {
		name    string
		args    args
		want    geo.Geometry
		wantErr error
	}{
		{
			name: "shared path between a MultiLineString and LineString",
			args: args{
				a: geo.MustParseGeometry("MULTILINESTRING((26 125,26 200,126 200,126 125,26 125)," +
					"(51 150,101 150,76 175,51 150))"),
				b: geo.MustParseGeometry("LINESTRING(151 100,126 156.25,126 125,90 161, 76 175)"),
			},
			want: geo.MustParseGeometry("GEOMETRYCOLLECTION(MULTILINESTRING((126 156.25,126 125)," +
				"(101 150,90 161),(90 161,76 175)),MULTILINESTRING EMPTY)"),
		},
		{
			name: "shared path between a Linestring and MultiLineString",
			args: args{
				a: geo.MustParseGeometry("LINESTRING(76 175,90 161,126 125,126 156.25,151 100)"),
				b: geo.MustParseGeometry("MULTILINESTRING((26 125,26 200,126 200,126 125,26 125), " +
					"(51 150,101 150,76 175,51 150))"),
			},
			want: geo.MustParseGeometry("GEOMETRYCOLLECTION(MULTILINESTRING EMPTY," +
				"MULTILINESTRING((76 175,90 161),(90 161,101 150),(126 125,126 156.25)))"),
		},
		{
			name: "shared path between non-lineal geometry",
			args: args{
				a: geo.MustParseGeometry("MULTIPOINT((0 0), (3 2))"),
				b: geo.MustParseGeometry("MULTIPOINT((0 1), (1 2))"),
			},
			wantErr: errors.New("geos error: IllegalArgumentException: Geometry is not lineal"),
		},
		{
			name: "no shared path between two Linestring",
			args: args{
				a: geo.MustParseGeometry("LINESTRING(0 0, 10 0)"),
				b: geo.MustParseGeometry("LINESTRING(-10 5, 10 5)"),
			},
			want: geo.MustParseGeometry("GEOMETRYCOLLECTION(MULTILINESTRING EMPTY, MULTILINESTRING EMPTY)"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SharedPaths(tt.args.a, tt.args.b)
			if tt.wantErr != nil && tt.wantErr.Error() != err.Error() {
				t.Errorf("SharedPaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}

}
