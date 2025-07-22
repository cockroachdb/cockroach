// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mvt

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestNewSimpleMVTBuilder(t *testing.T) {
	builder := NewSimpleMVTBuilder("test_layer", 4096)
	require.Equal(t, "test_layer", builder.layerName)
	require.Equal(t, uint32(4096), builder.extent)
	require.Empty(t, builder.features)
}

func TestSimpleMVTBuilder_AddFeature_Point(t *testing.T) {
	builder := NewSimpleMVTBuilder("test_layer", 4096)
	
	point := geom.NewPointFlat(geom.XY, []float64{100, 200})
	geom, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)
	
	id := uint64(1)
	properties := map[string]tree.Datum{
		"name": tree.NewDString("test_point"),
		"value": tree.NewDInt(42),
	}
	
	err = builder.AddFeature(geom, &id, properties)
	require.NoError(t, err)
	
	require.Len(t, builder.features, 1)
	feature := builder.features[0]
	require.Equal(t, &id, feature.ID)
	require.Equal(t, GeomTypePoint, feature.GeomType)
	require.Equal(t, "test_point", feature.Properties["name"])
	require.Equal(t, int64(42), feature.Properties["value"])
}

func TestMVTBuilder_AddFeature_LineString(t *testing.T) {
	builder := NewMVTBuilder("test_layer", 4096)
	
	line := geom.NewLineStringFlat(geom.XY, []float64{0, 0, 10, 10, 20, 0})
	geom, err := geo.MakeGeometryFromGeomT(line)
	require.NoError(t, err)
	
	properties := map[string]tree.Datum{
		"highway": tree.NewDString("primary"),
	}
	
	err = builder.AddFeature(geom, nil, properties)
	require.NoError(t, err)
	
	require.Len(t, builder.features, 1)
	feature := builder.features[0]
	require.Nil(t, feature.ID)
	require.Equal(t, GeomTypeLineString, feature.GeomType)
	require.Equal(t, "primary", feature.Properties["highway"])
}

func TestMVTBuilder_AddFeature_Polygon(t *testing.T) {
	builder := NewMVTBuilder("test_layer", 4096)
	
	// Create a simple square polygon
	polygon := geom.NewPolygonFlat(geom.XY, []float64{
		0, 0, 10, 0, 10, 10, 0, 10, 0, 0,
	}, []int{10})
	geom, err := geo.MakeGeometryFromGeomT(polygon)
	require.NoError(t, err)
	
	properties := map[string]tree.Datum{
		"building": tree.NewDString("yes"),
		"height":   tree.NewDFloat(10.5),
	}
	
	err = builder.AddFeature(geom, nil, properties)
	require.NoError(t, err)
	
	require.Len(t, builder.features, 1)
	feature := builder.features[0]
	require.Equal(t, GeomTypePolygon, feature.GeomType)
	require.Equal(t, "yes", feature.Properties["building"])
	require.Equal(t, float64(10.5), feature.Properties["height"])
}

func TestMVTBuilder_AddFeature_EmptyGeometry(t *testing.T) {
	builder := NewMVTBuilder("test_layer", 4096)
	
	emptyPoint := geom.NewPoint(geom.XY)
	geom, err := geo.MakeGeometryFromGeomT(emptyPoint)
	require.NoError(t, err)
	
	err = builder.AddFeature(geom, nil, map[string]tree.Datum{})
	require.NoError(t, err)
	
	// Empty geometries should be skipped
	require.Empty(t, builder.features)
}

func TestMVTBuilder_Build_Empty(t *testing.T) {
	builder := NewMVTBuilder("test_layer", 4096)
	
	data, err := builder.Build()
	require.NoError(t, err)
	require.Nil(t, data) // Empty MVT should return nil
}

func TestMVTBuilder_Build_WithFeatures(t *testing.T) {
	builder := NewMVTBuilder("test_layer", 4096)
	
	// Add a point feature
	point := geom.NewPointFlat(geom.XY, []float64{100, 200})
	pointGeom, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)
	
	properties := map[string]tree.Datum{
		"name":    tree.NewDString("test"),
		"value":   tree.NewDInt(42),
		"visible": tree.NewDBool(true),
	}
	
	err = builder.AddFeature(pointGeom, nil, properties)
	require.NoError(t, err)
	
	data, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)
}

func TestEncodeCommand(t *testing.T) {
	// Test MoveTo command with count 1
	cmd := encodeCommand(1, 1)
	require.Equal(t, uint32(9), cmd) // (1 & 0x7) | (1 << 3) = 1 | 8 = 9
	
	// Test LineTo command with count 3
	cmd = encodeCommand(2, 3)
	require.Equal(t, uint32(26), cmd) // (2 & 0x7) | (3 << 3) = 2 | 24 = 26
	
	// Test ClosePath command with count 1
	cmd = encodeCommand(7, 1)
	require.Equal(t, uint32(15), cmd) // (7 & 0x7) | (1 << 3) = 7 | 8 = 15
}

func TestEncodeZigZag(t *testing.T) {
	tests := []struct {
		input    int32
		expected uint32
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{-3, 5},
	}
	
	for _, test := range tests {
		result := encodeZigZag(test.input)
		require.Equal(t, test.expected, result, "input: %d", test.input)
	}
}

func TestDatumToValue(t *testing.T) {
	tests := []struct {
		datum    tree.Datum
		expected interface{}
	}{
		{tree.NewDString("hello"), "hello"},
		{tree.NewDInt(42), int64(42)},
		{tree.NewDFloat(3.14), float64(3.14)},
		{tree.NewDBool(true), true},
		{tree.NewDBool(false), false},
	}
	
	for _, test := range tests {
		result := datumToValue(test.datum)
		require.Equal(t, test.expected, result)
	}
}