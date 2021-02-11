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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
)

func TestGenerateRandomPoints(t *testing.T) {
	type args struct {
		g       geo.Geometry
		nPoints int
		seed    int64
	}
	testCases := []struct {
		name string
		args args
		want geo.Geometry
	}{
		{
			"number of points to generate less than minimum",
			args{geo.MustParseGeometry("POLYGON((0 0, 1 0, 1 1.1, 0 0))"), -1, 1},
			geo.Geometry{},
		},
		{
			"supported geometry, zero points to generate",
			args{geo.MustParseGeometry("POLYGON((0 0, 1 0, 1 1.1, 0 0))"), 0, 1},
			geo.Geometry{},
		},
		{
			"empty geometry",
			args{geo.MustParseGeometry("POLYGON EMPTY"), 4, 1},
			geo.Geometry{},
		},
		{
			"Polygon - square",
			args{geo.MustParseGeometry("POLYGON((1 1,1 2,2 2,2 1,1 1))"), 4, 2},
			geo.MustParseGeometry("MULTIPOINT(1.55948870460174560115 1.30713530828859747501, 1.4638274793965568854 1.71250027479276534237, 1.6030771445518701146 1.60597601333107697918, 1.06344848690006488212 1.30988904426268293335)"),
		},
		{
			"Polygon, width > height",
			args{geo.MustParseGeometry("POLYGON((0 0,1 2,3 2,2 0,0 0))"), 7, 14},
			geo.MustParseGeometry("MULTIPOINT(0.3830566789833752539 0.23117069178437729682, 2.51179632398947294547 1.69060434239713908156, 1.94222431495883451902 0.56512577777958861169, 1.87408814538545565043 1.21241169406013726828, 2.29499038937696608897 1.19218670122114289711, 1.93144882885377144888 0.79976266805657403314, 1.33675111888047548625 1.70583597131752906506)"),
		},
		{
			"Polygon, width < height",
			args{geo.MustParseGeometry("POLYGON((0 0,2 5,2.5 4,3 5,3 1,0 0))"), 5, 1996},
			geo.MustParseGeometry("MULTIPOINT(0.69465779472271460548 0.92001319545446269554, 2.4417593921811042712 3.71642371685872197062, 2.79787890688424933927 3.8425013135166361522, 1.05776032659919683176 1.77173131482243051416, 1.79695770199420046254 2.42853164217679839965)"),
		},
		{
			"Polygon with a hole",
			args{geo.MustParseGeometry("POLYGON((-1 -1, -1 1, 0 2, 1 1, 1 -1, 0 -2, -1 -1),(-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, 0 -0.5, -0.5 -0.5))"), 7, 42},
			geo.MustParseGeometry("MULTIPOINT(0.62392209943416365725 1.14891034830739080519, 0.5066157824317096825 -1.39898866703452817717, -0.71853651145541486134 1.15646037366762399756, -0.03771709502060871522 -0.80984418343170694321, 0.56879738017966463559 -0.74897977355716416348, 0.05783156505961972726 1.37709385168907050279, 0.71334734167548885519 -0.61452515240937377605)"),
		},
		{
			"MultiPolygon",
			args{geo.MustParseGeometry("MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)), ((0 0,-4 0,-4 -4,0 -4,0 0),(1 1,2 1,2 2,1 2,1 1)))"), 9, 17},
			geo.MustParseGeometry("MULTIPOINT(0.17846255116778333982 3.74754647968727550023, 2.27256794763602965048 0.21170437107020742551, 3.78118853421934808523 3.80677821706058949758, 1.04599577267150789517 3.86644467452649553962, 3.25038586104516369346 1.99712423764354585209, -1.69784827781519487289 -0.41663496633749641518, -3.13128096103860187327 -0.52028622879791064371, -2.49857072552626657824 -2.30333494646855019283, -0.37133983304899031985 -3.89307989068656556952, -0.31767322799652175647 -1.36504243564259120092)"),
		},
		{
			"Polygon with a specified SRID",
			args{geo.MustParseGeometry("SRID=4326;POLYGON((0 0,2 5,2.5 4,3 5,3 1,0 0))"), 5, 1996},
			geo.MustParseGeometry("SRID=4326;MULTIPOINT(0.69465779472271460548 0.92001319545446269554, 2.4417593921811042712 3.71642371685872197062, 2.79787890688424933927 3.8425013135166361522, 1.05776032659919683176 1.77173131482243051416, 1.79695770199420046254 2.42853164217679839965)"),
		},
		{
			"Invalid Polygon",
			args{geo.MustParseGeometry("POLYGON((-1 -1,1 -1,0 0,0 1,0 -1,0 0,-1 0,-1 -1))"), 5, 1996},
			geo.MustParseGeometry("MULTIPOINT(-0.26747218234566871864 -0.88507288494238345322, -0.87713357493233190532 -0.97615754673482446613, -0.44243286372996359912 -0.3727648333352959753, 0.43580610882637776937 -0.55479608815084269224, -0.55844006437980153734 -0.47716362944650048128)"),
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(tt.args.seed))
			got, err := GenerateRandomPoints(tt.args.g, tt.args.nPoints, rng)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	t.Run("errors", func(t *testing.T) {
		errorTestCases := []struct {
			name     string
			args     args
			errMatch string
		}{
			{
				"unsupported geometry, zero points to generate",
				args{geo.MustParseGeometry("LINESTRING(50 50,150 150,150 50)"), 0, 1},
				"unsupported type: LineString",
			},
			{
				"unsupported geometry type: LineString",
				args{geo.MustParseGeometry("LINESTRING(50 50,150 150,150 50)"), 4, 1},
				"unsupported type: LineString",
			},
			{
				"Polygon with zero area",
				args{geo.MustParseGeometry("POLYGON((0 0, 1 1, 1 1, 0 0))"), 4, 1},
				"zero area input Polygon",
			},
			{
				"polygon too many points to generate",
				args{geo.MustParseGeometry("POLYGON((1 1,1 2,2 2,2 1,1 1))"), 65337, 1996},
				"failed to generate random points, too many points to generate: requires 65337 points, max 65336",
			},
			{
				"generated area is too large",
				args{geo.MustParseGeometry("POLYGON((0 0,0 100,0.00001 0.00000001,0.99999 0.00000001,1 100,1 0,0 0))"), 100, 1996},
				"generating random points error: generated area is too large: 10001406, max 6533600",
			},
		}
		for _, tt := range errorTestCases {
			t.Run(tt.name, func(t *testing.T) {
				rng := rand.New(rand.NewSource(tt.args.seed))
				_, err := GenerateRandomPoints(tt.args.g, tt.args.nPoints, rng)
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMatch)
			})
		}
	})
}
