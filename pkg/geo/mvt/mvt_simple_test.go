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
	require.Equal(t, "test_point", feature.Properties["name"])
	require.Equal(t, int64(42), feature.Properties["value"])
}

func TestSimpleMVTBuilder_AddFeature_LineString(t *testing.T) {
	builder := NewSimpleMVTBuilder("test_layer", 4096)
	
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
	require.Equal(t, "primary", feature.Properties["highway"])
}

func TestSimpleMVTBuilder_AddFeature_Polygon(t *testing.T) {
	builder := NewSimpleMVTBuilder("test_layer", 4096)
	
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
	require.Equal(t, "yes", feature.Properties["building"])
	require.Equal(t, float64(10.5), feature.Properties["height"])
}

func TestSimpleMVTBuilder_AddFeature_EmptyGeometry(t *testing.T) {
	builder := NewSimpleMVTBuilder("test_layer", 4096)
	
	// Create an actual empty geometry using EWKT
	geom, err := geo.ParseGeometry("POINT EMPTY")
	require.NoError(t, err)
	
	// Check that the geometry is indeed empty
	require.True(t, geom.Empty())
	
	err = builder.AddFeature(geom, nil, map[string]tree.Datum{})
	require.NoError(t, err)
	
	// Empty geometries should be skipped
	require.Empty(t, builder.features)
}

func TestSimpleMVTBuilder_Build_Empty(t *testing.T) {
	builder := NewSimpleMVTBuilder("test_layer", 4096)
	
	data, err := builder.Build()
	require.NoError(t, err)
	require.Nil(t, data) // Empty MVT should return nil
}

func TestSimpleMVTBuilder_Build_WithFeatures(t *testing.T) {
	builder := NewSimpleMVTBuilder("test_layer", 4096)
	
	// Add a point feature
	point := geom.NewPointFlat(geom.XY, []float64{100, 200})
	pointGeom, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)
	
	properties := map[string]tree.Datum{
		"name":    tree.NewDString("test"),
		"value":   tree.NewDInt(42),
		"visible": tree.DBoolTrue,
	}
	
	err = builder.AddFeature(pointGeom, nil, properties)
	require.NoError(t, err)
	
	data, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)
	
	// Verify the binary format starts with MVT header
	require.Equal(t, "MVT", string(data[0:3]))
	require.Equal(t, byte(2), data[3]) // Version
}

func TestGetGeometryType(t *testing.T) {
	tests := []struct {
		name     string
		geom     geom.T
		expected int
	}{
		{
			name:     "Point",
			geom:     geom.NewPointFlat(geom.XY, []float64{0, 0}),
			expected: 1,
		},
		{
			name:     "LineString",
			geom:     geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1}),
			expected: 2,
		},
		{
			name:     "Polygon",
			geom:     geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 0, 1, 1, 0, 1, 0, 0}, []int{10}),
			expected: 3,
		},
		{
			name:     "MultiPoint",
			geom:     geom.NewMultiPointFlat(geom.XY, []float64{0, 0, 1, 1}),
			expected: 1,
		},
	}
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := getGeometryType(test.geom)
			require.Equal(t, test.expected, result)
		})
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
		{tree.DBoolTrue, true},
		{tree.DBoolFalse, false},
	}
	
	for _, test := range tests {
		result := datumToValue(test.datum)
		require.Equal(t, test.expected, result)
	}
}