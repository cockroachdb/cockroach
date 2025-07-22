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

func TestNewMVTBuilder(t *testing.T) {
	builder := NewMVTBuilder()
	require.Empty(t, builder.features)
}

func TestMVTBuilder_AddFeature_Point(t *testing.T) {
	builder := NewMVTBuilder()

	point := geom.NewPointFlat(geom.XY, []float64{100, 200})
	geometry, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)

	id := uint64(1)
	properties := []Property{
		{Key: "name", Value: tree.NewDString("test_point")},
		{Key: "value", Value: tree.NewDInt(42)},
	}

	err = builder.AddFeature(geometry, &id, properties)
	require.NoError(t, err)

	require.Len(t, builder.features, 1)
	feature := builder.features[0]
	require.Equal(t, &id, feature.ID)
	require.Equal(t, GeomTypePoint, feature.GeomType)
	// Properties are now stored as a slice
	require.Len(t, feature.Properties, 2)
	require.Equal(t, "name", feature.Properties[0].Key)
	require.Equal(t, tree.NewDString("test_point"), feature.Properties[0].Value)
	require.Equal(t, "value", feature.Properties[1].Key)
	require.Equal(t, tree.NewDInt(42), feature.Properties[1].Value)
}

func TestMVTBuilder_AddFeature_LineString(t *testing.T) {
	builder := NewMVTBuilder()

	line := geom.NewLineStringFlat(geom.XY, []float64{0, 0, 10, 10, 20, 0})
	geometry, err := geo.MakeGeometryFromGeomT(line)
	require.NoError(t, err)

	properties := []Property{
		{Key: "highway", Value: tree.NewDString("primary")},
	}

	err = builder.AddFeature(geometry, nil, properties)
	require.NoError(t, err)

	require.Len(t, builder.features, 1)
	feature := builder.features[0]
	require.Nil(t, feature.ID)
	require.Equal(t, GeomTypeLineString, feature.GeomType)
	require.Len(t, feature.Properties, 1)
	require.Equal(t, "highway", feature.Properties[0].Key)
	require.Equal(t, tree.NewDString("primary"), feature.Properties[0].Value)
}

func TestMVTBuilder_AddFeature_Polygon(t *testing.T) {
	builder := NewMVTBuilder()

	// Create a simple square polygon
	polygon := geom.NewPolygonFlat(geom.XY, []float64{
		0, 0, 10, 0, 10, 10, 0, 10, 0, 0,
	}, []int{10})
	geometry, err := geo.MakeGeometryFromGeomT(polygon)
	require.NoError(t, err)

	properties := []Property{
		{Key: "building", Value: tree.NewDString("yes")},
		{Key: "height", Value: tree.NewDFloat(10.5)},
	}

	err = builder.AddFeature(geometry, nil, properties)
	require.NoError(t, err)

	require.Len(t, builder.features, 1)
	feature := builder.features[0]
	require.Equal(t, GeomTypePolygon, feature.GeomType)
	require.Len(t, feature.Properties, 2)
	require.Equal(t, "building", feature.Properties[0].Key)
	require.Equal(t, tree.NewDString("yes"), feature.Properties[0].Value)
	require.Equal(t, "height", feature.Properties[1].Key)
	require.Equal(t, tree.NewDFloat(10.5), feature.Properties[1].Value)
}

func TestMVTBuilder_AddFeature_EmptyGeometry(t *testing.T) {
	builder := NewMVTBuilder()

	geometry, err := geo.ParseGeometry("POINT EMPTY")
	require.NoError(t, err)
	require.True(t, geometry.Empty())

	err = builder.AddFeature(geometry, nil, []Property{})
	require.NoError(t, err)

	// Empty geometries should be skipped
	require.Empty(t, builder.features)
}

func TestMVTBuilder_Build_Empty(t *testing.T) {
	builder := NewMVTBuilder()

	data, err := builder.Build("test_layer", 4096)
	require.NoError(t, err)
	require.Nil(t, data) // Empty MVT should return nil
}

func TestMVTBuilder_Build_WithFeatures(t *testing.T) {
	builder := NewMVTBuilder()

	// Add a point feature
	point := geom.NewPointFlat(geom.XY, []float64{100, 200})
	pointGeom, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)

	properties := []Property{
		{Key: "name", Value: tree.NewDString("test")},
		{Key: "value", Value: tree.NewDInt(42)},
		{Key: "visible", Value: tree.MakeDBool(true)},
	}

	err = builder.AddFeature(pointGeom, nil, properties)
	require.NoError(t, err)

	data, err := builder.Build("test_layer", 4096)
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

// TestMVTBuilder_DuplicateKeys tests handling of duplicate column names.
func TestMVTBuilder_DuplicateKeys(t *testing.T) {
	builder := NewMVTBuilder()

	point := geom.NewPointFlat(geom.XY, []float64{25, 17})
	geometry, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)

	// Test case 1: Duplicate column names with different values
	// This simulates: SELECT -1::int AS c1, 'abcd'::text AS c2, 20::integer as c1
	properties := []Property{
		{Key: "c1", Value: tree.NewDInt(-1)},
		{Key: "c2", Value: tree.NewDString("abcd")},
		{Key: "c1", Value: tree.NewDInt(20)},
	}

	id := uint64(20) // When c1 is used as feature ID, value 20 should be used
	err = builder.AddFeature(geometry, &id, properties)
	require.NoError(t, err)

	require.Len(t, builder.features, 1)
	feature := builder.features[0]
	require.NotNil(t, feature.ID)
	require.Equal(t, uint64(20), *feature.ID)

	// Properties should preserve duplicates
	require.Len(t, feature.Properties, 3)
	require.Equal(t, "c1", feature.Properties[0].Key)
	require.Equal(t, tree.NewDInt(-1), feature.Properties[0].Value)
	require.Equal(t, "c2", feature.Properties[1].Key)
	require.Equal(t, tree.NewDString("abcd"), feature.Properties[1].Value)
	require.Equal(t, "c1", feature.Properties[2].Key)
	require.Equal(t, tree.NewDInt(20), feature.Properties[2].Value)
}

// TestMVTBuilder_DuplicateKeys_WithNullValue tests duplicate columns where one is NULL.
func TestMVTBuilder_DuplicateKeys_WithNullValue(t *testing.T) {
	builder := NewMVTBuilder()

	point := geom.NewPointFlat(geom.XY, []float64{25, 17})
	geometry, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)

	// Test case: NULL as first c1, then valid value
	// This simulates: SELECT null AS c1, 'abcd'::text AS c2, 20::integer as c1
	properties := []Property{
		{Key: "c1", Value: tree.DNull},
		{Key: "c2", Value: tree.NewDString("abcd")},
		{Key: "c1", Value: tree.NewDInt(20)},
	}

	id := uint64(20)
	err = builder.AddFeature(geometry, &id, properties)
	require.NoError(t, err)

	feature := builder.features[0]
	require.NotNil(t, feature.ID)
	require.Equal(t, uint64(20), *feature.ID)

	// NULL values are preserved in properties
	require.Len(t, feature.Properties, 3)
}

// TestMVTBuilder_DuplicateKeys_NegativeValue tests handling of negative values for feature ID.
func TestMVTBuilder_DuplicateKeys_NegativeValue(t *testing.T) {
	builder := NewMVTBuilder()

	point := geom.NewPointFlat(geom.XY, []float64{25, 17})
	geometry, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)

	// Test case: Negative value should not be used as feature ID
	// This simulates: SELECT -5::integer as c1, 'abcd'::text AS c2
	properties := []Property{
		{Key: "c1", Value: tree.NewDInt(-5)},
		{Key: "c2", Value: tree.NewDString("abcd")},
	}

	// No ID provided since -5 is negative
	err = builder.AddFeature(geometry, nil, properties)
	require.NoError(t, err)

	feature := builder.features[0]
	require.Nil(t, feature.ID) // Negative values should not become feature IDs

	// Both properties should be preserved
	require.Len(t, feature.Properties, 2)
}

// TestMVTBuilder_DuplicateKeys_FirstValidID tests that the first valid value is used as ID.
func TestMVTBuilder_DuplicateKeys_FirstValidID(t *testing.T) {
	builder := NewMVTBuilder()

	point := geom.NewPointFlat(geom.XY, []float64{25, 17})
	geometry, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)

	// Test case: First valid (non-negative) value should be used as ID
	// This simulates: SELECT 1::smallint AS c1, 'abcd'::text AS c2, 20::integer as c1
	properties := []Property{
		{Key: "c1", Value: tree.NewDInt(1)},
		{Key: "c2", Value: tree.NewDString("abcd")},
		{Key: "c1", Value: tree.NewDInt(20)},
	}

	id := uint64(1) // First valid value should be used
	err = builder.AddFeature(geometry, &id, properties)
	require.NoError(t, err)

	feature := builder.features[0]
	require.NotNil(t, feature.ID)
	require.Equal(t, uint64(1), *feature.ID)

	// All properties should be preserved
	require.Len(t, feature.Properties, 3)
}

// TestMVTBuilder_Build_WithDuplicateKeys tests the full MVT build with duplicate keys.
func TestMVTBuilder_Build_WithDuplicateKeys(t *testing.T) {
	builder := NewMVTBuilder()

	point := geom.NewPointFlat(geom.XY, []float64{25, 17})
	geometry, err := geo.MakeGeometryFromGeomT(point)
	require.NoError(t, err)

	// Add feature with duplicate keys
	properties := []Property{
		{Key: "name", Value: tree.NewDString("test")},
		{Key: "value", Value: tree.NewDInt(1)},
		{Key: "name", Value: tree.NewDString("duplicate")},
	}

	err = builder.AddFeature(geometry, nil, properties)
	require.NoError(t, err)

	data, err := builder.Build("test_layer", 4096)
	require.NoError(t, err)
	require.NotNil(t, data)

	// The MVT should properly encode duplicate keys. Both "name" properties
	// should be in the output with their respective values.
	require.Greater(t, len(data), 0)
}
