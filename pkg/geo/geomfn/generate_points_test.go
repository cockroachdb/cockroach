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

func TestGeneratePoints(t *testing.T) {
	type args struct {
		g       geo.Geometry
		npoints int
		seed    int64
	}
	tests := []struct {
		name    string
		args    args
		want    geo.Geometry
		wantErr bool
	}{
		{
			"number of points to generate less than minimum",
			args{geo.MustParseGeometry("POLYGON((0 0, 1 0, 1 1.1, 0 0))"), -1, 1},
			geo.Geometry{},
			false,
		},
		{
			"supported geometry, zero points to generate",
			args{geo.MustParseGeometry("POLYGON((0 0, 1 0, 1 1.1, 0 0))"), 0, 1},
			geo.Geometry{},
			false,
		},
		{
			"unsupported geometry, zero points to generate",
			args{geo.MustParseGeometry("LINESTRING(50 50,150 150,150 50)"), 0, 1},
			geo.Geometry{},
			true,
		},
		{
			"provided seed less than minimum",
			args{geo.MustParseGeometry("POLYGON((0 0, 1 0, 1 1.1, 0 0))"), 1, -1},
			geo.Geometry{},
			true,
		},
		{
			"unsupported geometry type: LineString",
			args{geo.MustParseGeometry("LINESTRING(50 50,150 150,150 50)"), 4, 1},
			geo.Geometry{},
			true,
		},
		{
			"Polygon with zero area",
			args{geo.MustParseGeometry("POLYGON((0 0, 1 1, 1 1, 0 0))"), 4, 1},
			geo.Geometry{},
			true,
		},
		{
			"empty geometry",
			args{geo.MustParseGeometry("POLYGON EMPTY"), 4, 1},
			geo.Geometry{},
			false,
		},
		{
			"Polygon - square",
			args{geo.MustParseGeometry("POLYGON((1 1,1 2,2 2,2 1,1 1))"), 4, 2},
			geo.MustParseGeometry("MULTIPOINT(1.55948870460174560115 1.30713530828859747501, 1.4638274793965568854 1.71250027479276534237, 1.6030771445518701146 1.60597601333107697918, 1.06344848690006488212 1.30988904426268293335)"),
			false,
		},
		{
			"Polygon, width > height",
			args{geo.MustParseGeometry("POLYGON((0 0,1 2,3 2,2 0,0 0))"), 7, 14},
			geo.MustParseGeometry("MULTIPOINT(0.3830566789833752539 0.23117069178437729682, 2.51179632398947294547 1.69060434239713908156, 1.94222431495883451902 0.56512577777958861169, 1.87408814538545565043 1.21241169406013726828, 2.29499038937696608897 1.19218670122114289711, 1.93144882885377144888 0.79976266805657403314, 1.33675111888047548625 1.70583597131752906506)"),
			false,
		},
		{
			"Polygon, width < height",
			args{geo.MustParseGeometry("POLYGON((0 0,2 5,2.5 4,3 5,3 1,0 0))"), 5, 1996},
			geo.MustParseGeometry("MULTIPOINT(0.69465779472271460548 0.92001319545446269554, 2.4417593921811042712 3.71642371685872197062, 2.79787890688424933927 3.8425013135166361522, 1.05776032659919683176 1.77173131482243051416, 1.79695770199420046254 2.42853164217679839965)"),
			false,
		},
		{
			"Polygon with a hole",
			args{geo.MustParseGeometry("POLYGON((-1 -1, -1 1, 0 2, 1 1, 1 -1, 0 -2, -1 -1),(-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, 0 -0.5, -0.5 -0.5))"), 7, 42},
			geo.MustParseGeometry("MULTIPOINT(0.62392209943416365725 1.14891034830739080519, 0.5066157824317096825 -1.39898866703452817717, -0.71853651145541486134 1.15646037366762399756, -0.03771709502060871522 -0.80984418343170694321, 0.56879738017966463559 -0.74897977355716416348, 0.05783156505961972726 1.37709385168907050279, 0.71334734167548885519 -0.61452515240937377605)"),
			false,
		},
		{
			"MultiPolygon",
			args{geo.MustParseGeometry("MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)), ((0 0,-4 0,-4 -4,0 -4,0 0),(1 1,2 1,2 2,1 2,1 1)))"), 9, 15},
			geo.MustParseGeometry("MULTIPOINT(3.22867259458672961614 3.29323429726158023456, 1.70359136534834165744 0.36049598512632602398, 2.71072342994228554502 1.20150277742601008235, 1.9059667543503933107 3.67598273180139756278, 2.54757976155147858321 3.35228130507990673692, -0.1495732283473727442 -3.65380934845110161291, -2.3391608436532123072 -0.37011413317119368216, -3.6902632348743011903 -3.45769500629712034367, -1.72346872742639400933 -1.83187553147969461875, -0.04275659844954238231 -2.64469612218461502806)"),
			false,
		},
		{
			"Polygon with a specified SRID",
			args{geo.MustParseGeometry("SRID=4326;POLYGON((0 0,2 5,2.5 4,3 5,3 1,0 0))"), 5, 1996},
			geo.MustParseGeometry("SRID=4326;MULTIPOINT(0.69465779472271460548 0.92001319545446269554, 2.4417593921811042712 3.71642371685872197062, 2.79787890688424933927 3.8425013135166361522, 1.05776032659919683176 1.77173131482243051416, 1.79695770199420046254 2.42853164217679839965)"),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GeneratePoints(tt.args.g, tt.args.npoints, tt.args.seed)
			if (err != nil) != tt.wantErr {
				t.Errorf("GeneratePoints() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}
