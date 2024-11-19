// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestAsMVTGeometry(t *testing.T) {
	type args struct {
		g            geo.Geometry
		bounds       geo.CartesianBoundingBox
		extent       int
		buffer       int
		clipGeometry bool
	}
	tests := []struct {
		name       string
		args       args
		want       geo.Geometry
		wantErrMsg string
	}{
		{
			name: "geometric bounds are too small",
			args: args{
				g:            geo.Geometry{},
				bounds:       *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{0, 0})),
				extent:       4096,
				buffer:       256,
				clipGeometry: false,
			},
			wantErrMsg: "geometric bounds are too small",
		},
		{
			name: "extent lower than zero",
			args: args{
				g: geo.Geometry{},
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					100, 0,
					0, 100,
					0, 0,
				})),
				extent:       -1,
				buffer:       256,
				clipGeometry: false,
			},
			wantErrMsg: "extent must be greater than 0",
		},
		{
			name: "buffer lower than zero",
			args: args{
				g: geo.Geometry{},
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					100, 0,
					0, 100,
					0, 0,
				})),
				extent:       1,
				buffer:       -1,
				clipGeometry: false,
			},
			wantErrMsg: "buffer value cannot be negative",
		},
		{
			name: "geometry smaller than the precision specified",
			args: args{
				g: geo.MustParseGeometry("POLYGON((0 0, 1 0, 1 1.1, 0 0))"),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					100, 0,
					0, 100,
					0, 0,
				})),
				extent:       5,
				buffer:       256,
				clipGeometry: false,
			},
			want: geo.Geometry{},
		},
		{
			name: "empty geometry",
			args: args{
				g: geo.MustParseGeometry("POLYGON EMPTY"),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					100, 0,
					0, 100,
					0, 0,
				})),
				extent:       5,
				buffer:       256,
				clipGeometry: false,
			},
			want: geo.Geometry{},
		},
		{
			name: "basic type is empty",
			args: args{
				g: geo.MustParseGeometry(`GEOMETRYCOLLECTION(
					LINESTRING(1.5 1.5, 1.5 1.5, 1.6 1.6, 1.7 1.7),
					POLYGON EMPTY)`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					100, 0,
					0, 100,
					0, 0,
				})),
				extent:       5,
				buffer:       256,
				clipGeometry: false,
			},
			want: geo.Geometry{},
		},
		{
			name: "Linestring",
			args: args{
				g: geo.MustParseGeometry(`LINESTRING(0 0, 10 0, 10 5, 0 -5, 0 0)`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 20,
					20, 20,
					20, 0,
					0, 0,
				})),
				extent:       40,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`LINESTRING(0 40,20 40,20 30,0 50,0 40)`),
		},
		{
			name: "MultiLinestring",
			args: args{
				g: geo.MustParseGeometry(`MULTILINESTRING((0 0, 10 0, 10 5, 0 -5, 0 0),(0 0, 4 0, 6 6, -2 4, 0 0))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`MULTILINESTRING((0 20,20 20,20 10,0 30,0 20),(0 20,8 20,12 8,-4 12,0 20))`),
		},
		{
			name: "valid, solid Polygon",
			args: args{
				g: geo.MustParseGeometry(`POLYGON((0 0, 10 0, 12 5, 2 5, 0 0))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       10,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`POLYGON((0 10,2 5,12 5,10 10,0 10))`),
		},
		{
			name: "valid Polygon with an interior ring",
			args: args{
				g: geo.MustParseGeometry(`POLYGON((10 10, 20 10, 20 20, 10 20, 10 10),(12 12, 18 12, 18 18, 12 18, 12 12))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`POLYGON((20 0,20 -20,40 -20,40 0,20 0),(24 -4,36 -4,36 -16,24 -16,24 -4))`),
		},
		{
			name: "valid Polygon with repeated points",
			args: args{
				g: geo.MustParseGeometry(`POLYGON((10 10, 10 10, 20 10, 20 20, 20 20, 10 20, 10 10),(12 12, 18 12, 18 18, 18 18, 12 18, 12 12))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`POLYGON((20 0,20 -20,40 -20,40 0,20 0),(24 -4,36 -4,36 -16,24 -16,24 -4))`),
		},
		{
			name: "valid MultiPolygon",
			args: args{
				g: geo.MustParseGeometry(`MULTIPOLYGON(((10 10, 20 10, 20 20, 10 20, 10 10),(12 12, 18 12, 18 18, 12 18, 12 12)),((13 13, 17 13, 17 17, 13 17, 13 13)))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`MULTIPOLYGON(((20 0,20 -20,40 -20,40 0,20 0),(24 -4,36 -4,36 -16,24 -16,24 -4)),((26 -6,26 -14,34 -14,34 -6,26 -6)))`),
		},
		{
			name: "valid GeometryCollection, different subgeometry types",
			args: args{
				g: geo.MustParseGeometry(`GEOMETRYCOLLECTION(POINT(10 20),LINESTRING(30 40, 50 60),POLYGON((10 10, 20 10, 20 20, 10 20, 10 10),(12 12, 18 12, 18 18, 12 18, 12 12)))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`POLYGON((20 0,20 -20,40 -20,40 0,20 0),(24 -4,36 -4,36 -16,24 -16,24 -4))`),
		},
		{
			name: "valid GeometryCollection, same subgeometry types",
			args: args{
				g: geo.MustParseGeometry(`GEOMETRYCOLLECTION(POLYGON((0 0, 10 0, 12 5, 2 5, 0 0)), POLYGON((20 20, 24 20, 26 26, 18 25, 20 20)))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 30,
					30, 30,
					30, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`MULTIPOLYGON(((0 20,1 17,8 17,7 20,0 20)),((13 7,12 3,17 3,16 7,13 7)))`),
		},
		{
			name: "invalid Polygon (with point on straight line)",
			args: args{
				g: geo.MustParseGeometry(`POLYGON((10 20, 30 40, 50 60, 10 20))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.Geometry{},
		},
		{
			name: "invalid Polygon (self intersection)",
			args: args{
				g: geo.MustParseGeometry(`POLYGON ((10 20, 10 10, 20 20, 20 10, 10 20))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry("MULTIPOLYGON (((30 -10, 20 0, 20 -20, 30 -10)), ((40 -20, 40 0, 30 -10, 40 -20)))"),
		},
		{
			name: "clipping invalid Polygon (self intersection)",
			args: args{
				g: geo.MustParseGeometry(`POLYGON ((5 12, 5 5, 12 12, 12 5, 5 12))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: true,
			},
			want: geo.MustParseGeometry("POLYGON ((17 3, 14 0, 20 0, 20 6, 20 20, 0 20, 0 0, 10 0, 10 10, 17 3))"),
		},
		{
			name: "clipping valid MultiLinestring",
			args: args{
				g: geo.MustParseGeometry(`MULTILINESTRING((0 0, 10 0, 10 5, 0 -5, 0 0),(0 0, 4 0, 6 6, -2 4, 0 0))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: true,
			},
			want: geo.MustParseGeometry(`MULTILINESTRING((20 10,10 20),(8 20,12 8,0 11))`),
		},
		{
			name: "clipping valid GeometryCollection",
			args: args{
				g: geo.MustParseGeometry(`GEOMETRYCOLLECTION(POLYGON((0 0, 10 0, 12 5, 2 5, 0 0)), POLYGON((20 20, 24 20, 26 26, 18 25, 20 20)))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       10,
				buffer:       0,
				clipGeometry: true,
			},
			want: geo.MustParseGeometry(`POLYGON((0 10,2 5,10 5,10 10,0 10))`),
		},
		{
			name: "clipping valid GeometryCollection with buffer",
			args: args{
				g: geo.MustParseGeometry(`GEOMETRYCOLLECTION(POLYGON((0 0, 10 0, 12 5, 2 5, 0 0)), POLYGON((20 20, 24 20, 26 26, 18 25, 20 20)))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       10,
				buffer:       15,
				clipGeometry: true,
			},
			want: geo.MustParseGeometry(`MULTIPOLYGON(((0 10, 2 5, 12 5, 10 10, 0 10)),((18 -15, 25 -15, 25 -13, 24 -10, 20 -10, 18 -15)))`),
		},
		{
			name: "clipping valid MultiPolygon",
			args: args{
				g: geo.MustParseGeometry(`MULTIPOLYGON(((10 10, 20 10, 20 20, 10 20, 10 10),(12 12, 18 12, 18 18, 12 18, 12 12)),((13 13, 17 13, 17 17, 13 17, 13 13)))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 15,
					15, 15,
					15, 0,
					0, 0,
				})),
				extent:       10,
				clipGeometry: true,
			},
			want: geo.MustParseGeometry(`MULTIPOLYGON(((7 0, 8 0, 8 2, 10 2, 10 3, 7 3, 7 0)),((9 0, 10 0, 10 1, 9 1, 9 0)))`),
		},
		{
			name: "clipping valid MultiPolygon, geometry empty after clipping",
			args: args{
				g: geo.MustParseGeometry(`MULTIPOLYGON(((10 10, 20 10, 20 20, 10 20, 10 10),(12 12, 18 12, 18 18, 12 18, 12 12)),((13 13, 17 13, 17 17, 13 17, 13 13)))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       10,
				clipGeometry: true,
			},
			want: geo.Geometry{},
		},
		{
			name: "Polygon with non-integer coordinates",
			args: args{
				g: geo.MustParseGeometry(`POLYGON((-2.345 1.789, -0.123 4.567, 3.456 2.345, 1.234 -0.987, -2.345 1.789),(-1.234 1.234, 1.234 1.234, 0.123 3.456, -1.234 1.234))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`POLYGON((-5 16, 0 11, 7 15, 2 22, -5 16),(-2 18, 2 18, 0 13, -2 18))`),
		},
		{
			name: "valid GeometryCollection consisting of overlapping Polygons",
			args: args{
				g: geo.MustParseGeometry(`GEOMETRYCOLLECTION(POLYGON((0 0, 10 0, 12 5, 2 5, 0 0)), POLYGON((0 0, 4 0, 6 6, -2 5, 0 0)))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       20,
				buffer:       0,
				clipGeometry: false,
			},
			want: geo.MustParseGeometry(`POLYGON ((-4 10, 12 8, 11 10, 24 10, 20 20, 8 20, 0 20, -4 10))`),
		},
		{
			name: "snapping to grid after clipping",
			args: args{
				g: geo.MustParseGeometry(`POLYGON((0 0, 4 0, 6 6, -2 5, 0 0))`),
				bounds: *geo.BoundingBoxFromGeomTGeometryType(geom.NewPointFlat(geom.XY, []float64{
					0, 0,
					0, 10,
					10, 10,
					10, 0,
					0, 0,
				})),
				extent:       17,
				buffer:       0,
				clipGeometry: true,
			},
			want: geo.MustParseGeometry(`POLYGON ((0 8, 10 7, 7 17, 0 17, 0 8))`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AsMVTGeometry(tt.args.g, tt.args.bounds, tt.args.extent, tt.args.buffer, tt.args.clipGeometry)
			if tt.wantErrMsg != "" {
				require.EqualError(t, err, tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			assertGeomEqual(t, tt.want, got)
		})
	}
}
