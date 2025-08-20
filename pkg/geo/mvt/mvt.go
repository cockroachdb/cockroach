// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package mvt provides functionality for encoding Mapbox Vector Tiles.
package mvt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/twpayne/go-geom"
)

// Property represents a key-value pair in MVT features.
// Using a slice of properties instead of a map allows duplicate keys.
type Property struct {
	Key   string
	Value tree.Datum
}

// MVTBuilder accumulates features for a single MVT layer.
type MVTBuilder struct {
	features []*Feature
}

// Feature represents a single feature in an MVT layer.
type Feature struct {
	ID         *uint64
	Tags       []uint32
	GeomType   GeomType
	Geometry   []uint32
	Properties []Property
}

// GeomType represents the geometry type in MVT format.
type GeomType uint32

const (
	GeomTypeUnknown    GeomType = 0
	GeomTypePoint      GeomType = 1
	GeomTypeLineString GeomType = 2
	GeomTypePolygon    GeomType = 3
)

// NewMVTBuilder creates a new MVT builder.
func NewMVTBuilder() *MVTBuilder {
	return &MVTBuilder{
		features: make([]*Feature, 0),
	}
}

// AddFeature adds a feature to the MVT layer with properties as a slice.
// This allows duplicate column names to be preserved in the MVT output.
func (b *MVTBuilder) AddFeature(geom geo.Geometry, id *uint64, properties []Property) error {
	if geom.Empty() {
		return nil // Skip empty geometries
	}

	gt, err := geom.AsGeomT()
	if err != nil {
		return errors.Wrap(err, "failed to convert geometry to geom.T")
	}

	mvtGeomType, mvtGeometry, err := b.encodeGeometry(gt)
	if err != nil {
		return errors.Wrap(err, "failed to encode geometry")
	}

	feature := &Feature{
		ID:         id,
		GeomType:   mvtGeomType,
		Geometry:   mvtGeometry,
		Properties: properties,
	}

	b.features = append(b.features, feature)
	return nil
}

// Build generates the MVT binary data.
func (b *MVTBuilder) Build(layerName string, extent uint32) ([]byte, error) {
	if len(b.features) == 0 {
		return nil, nil // Return empty MVT for no features
	}

	// Build key/value tables
	keys := make([]string, 0)
	values := make([]interface{}, 0)
	keyIndex := make(map[string]uint32)
	valueIndex := make(map[interface{}]uint32)

	// Collect all unique keys and values.
	for _, feature := range b.features {
		for _, prop := range feature.Properties {
			if _, exists := keyIndex[prop.Key]; !exists {
				keyIndex[prop.Key] = uint32(len(keys))
				keys = append(keys, prop.Key)
			}
			value := datumToValue(prop.Value)
			// Nil values (which represent SQL NULL) don't go in the values table.
			if value == nil {
				continue
			}
			if _, exists := valueIndex[value]; !exists {
				valueIndex[value] = uint32(len(values))
				values = append(values, value)
			}
		}
	}

	// Create protobuf layer.
	layer := &VectorTile_Layer{
		Name:    &layerName,
		Extent:  &extent,
		Version: proto.Uint32(2),
		Keys:    keys,
		Values:  make([]*VectorTile_Value, len(values)),
	}

	// Convert values to protobuf values.
	for i, value := range values {
		layer.Values[i] = valueToTileValue(value)
	}

	// Convert features to protobuf features
	layer.Features = make([]*VectorTile_Feature, len(b.features))
	for i, feature := range b.features {
		var geomType VectorTile_GeomType
		switch feature.GeomType {
		case GeomTypePoint:
			geomType = VectorTile_POINT
		case GeomTypeLineString:
			geomType = VectorTile_LINESTRING
		case GeomTypePolygon:
			geomType = VectorTile_POLYGON
		default:
			geomType = VectorTile_UNKNOWN
		}

		pbFeature := &VectorTile_Feature{
			Type:     &geomType,
			Geometry: feature.Geometry,
		}

		if feature.ID != nil {
			pbFeature.Id = feature.ID
		}

		// Build tags (key-value pairs). Nil values (representing SQL NULL) don't
		// have any tags.
		tags := make([]uint32, 0)
		for _, prop := range feature.Properties {
			value := datumToValue(prop.Value)
			if value == nil {
				continue
			}
			keyIdx, keyExists := keyIndex[prop.Key]
			valueIdx, valueExists := valueIndex[value]
			if keyExists && valueExists {
				tags = append(tags, keyIdx, valueIdx)
			}
		}
		pbFeature.Tags = tags

		layer.Features[i] = pbFeature
	}

	tile := &VectorTile{
		Layers: []*VectorTile_Layer{layer},
	}

	data, err := protoutil.Marshal(tile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal MVT protobuf")
	}

	return data, nil
}

// encodeGeometry converts a geometry to MVT geometry encoding.
func (b *MVTBuilder) encodeGeometry(gt geom.T) (GeomType, []uint32, error) {
	switch g := gt.(type) {
	case *geom.Point:
		return b.encodePoint(g)
	case *geom.MultiPoint:
		return b.encodeMultiPoint(g)
	case *geom.LineString:
		return b.encodeLineString(g)
	case *geom.MultiLineString:
		return b.encodeMultiLineString(g)
	case *geom.Polygon:
		return b.encodePolygon(g)
	case *geom.MultiPolygon:
		return b.encodeMultiPolygon(g)
	default:
		return GeomTypeUnknown, nil, errors.Newf("unsupported geometry type: %T", gt)
	}
}

// encodePoint encodes a Point geometry.
func (b *MVTBuilder) encodePoint(g *geom.Point) (GeomType, []uint32, error) {
	coords := g.Coords()
	if len(coords) == 0 {
		return GeomTypePoint, []uint32{}, nil
	}

	x := int32(coords[0])
	y := int32(coords[1])

	commands := []uint32{
		encodeCommand(1, 1), // MoveTo command with count 1
		encodeZigZag(x),
		encodeZigZag(y),
	}

	return GeomTypePoint, commands, nil
}

// encodeMultiPoint encodes a MultiPoint geometry.
func (b *MVTBuilder) encodeMultiPoint(g *geom.MultiPoint) (GeomType, []uint32, error) {
	numPoints := g.NumPoints()
	if numPoints == 0 {
		return GeomTypePoint, []uint32{}, nil
	}

	commands := []uint32{encodeCommand(1, uint32(numPoints))} // MoveTo command

	for i := 0; i < numPoints; i++ {
		point := g.Point(i)
		coords := point.Coords()
		if len(coords) == 0 {
			continue
		}

		x := int32(coords[0])
		y := int32(coords[1])
		commands = append(commands, encodeZigZag(x), encodeZigZag(y))
	}

	return GeomTypePoint, commands, nil
}

// encodeLineString encodes a LineString geometry.
func (b *MVTBuilder) encodeLineString(g *geom.LineString) (GeomType, []uint32, error) {
	coords := g.Coords()
	if len(coords) < 2 {
		return GeomTypeLineString, []uint32{}, nil
	}

	commands := []uint32{}

	// MoveTo first point
	x := int32(coords[0][0])
	y := int32(coords[0][1])
	commands = append(commands, encodeCommand(1, 1), encodeZigZag(x), encodeZigZag(y))

	// LineTo remaining points
	if len(coords) > 1 {
		commands = append(commands, encodeCommand(2, uint32(len(coords)-1)))
		prevX, prevY := x, y
		for i := 1; i < len(coords); i++ {
			x = int32(coords[i][0])
			y = int32(coords[i][1])
			commands = append(commands, encodeZigZag(x-prevX), encodeZigZag(y-prevY))
			prevX, prevY = x, y
		}
	}

	return GeomTypeLineString, commands, nil
}

// encodeMultiLineString encodes a MultiLineString geometry.
func (b *MVTBuilder) encodeMultiLineString(g *geom.MultiLineString) (GeomType, []uint32, error) {
	commands := []uint32{}

	for i := 0; i < g.NumLineStrings(); i++ {
		lineString := g.LineString(i)
		_, lineCommands, err := b.encodeLineString(lineString)
		if err != nil {
			return GeomTypeLineString, nil, err
		}
		commands = append(commands, lineCommands...)
	}

	return GeomTypeLineString, commands, nil
}

// encodePolygon encodes a Polygon geometry.
func (b *MVTBuilder) encodePolygon(g *geom.Polygon) (GeomType, []uint32, error) {
	commands := []uint32{}

	// Encode exterior ring
	if g.NumLinearRings() > 0 {
		ring := g.LinearRing(0)
		ringCommands, err := b.encodeLinearRing(ring, true) // clockwise for exterior
		if err != nil {
			return GeomTypePolygon, nil, err
		}
		commands = append(commands, ringCommands...)
	}

	// Encode interior rings (holes)
	for i := 1; i < g.NumLinearRings(); i++ {
		ring := g.LinearRing(i)
		ringCommands, err := b.encodeLinearRing(ring, false) // counter-clockwise for holes
		if err != nil {
			return GeomTypePolygon, nil, err
		}
		commands = append(commands, ringCommands...)
	}

	return GeomTypePolygon, commands, nil
}

// encodeMultiPolygon encodes a MultiPolygon geometry.
func (b *MVTBuilder) encodeMultiPolygon(g *geom.MultiPolygon) (GeomType, []uint32, error) {
	commands := []uint32{}

	for i := 0; i < g.NumPolygons(); i++ {
		polygon := g.Polygon(i)
		_, polyCommands, err := b.encodePolygon(polygon)
		if err != nil {
			return GeomTypePolygon, nil, err
		}
		commands = append(commands, polyCommands...)
	}

	return GeomTypePolygon, commands, nil
}

// encodeLinearRing encodes a linear ring for polygon geometries.
func (b *MVTBuilder) encodeLinearRing(ring *geom.LinearRing, clockwise bool) ([]uint32, error) {
	coords := ring.Coords()
	if len(coords) < 4 {
		return []uint32{}, nil // Invalid ring
	}

	commands := []uint32{}

	// MoveTo first point
	x := int32(coords[0][0])
	y := int32(coords[0][1])
	commands = append(commands, encodeCommand(1, 1), encodeZigZag(x), encodeZigZag(y))

	// LineTo other points (except last which should equal first)
	if len(coords) > 2 {
		lineToCount := len(coords) - 2 // Exclude first and last points
		commands = append(commands, encodeCommand(2, uint32(lineToCount)))
		prevX, prevY := x, y
		for i := 1; i < len(coords)-1; i++ {
			x = int32(coords[i][0])
			y = int32(coords[i][1])
			commands = append(commands, encodeZigZag(x-prevX), encodeZigZag(y-prevY))
			prevX, prevY = x, y
		}
	}

	// ClosePath
	commands = append(commands, encodeCommand(7, 1))

	return commands, nil
}

// MVT geometry encoding utilities

// encodeCommand encodes an MVT command with its count.
func encodeCommand(id, count uint32) uint32 {
	return (id & 0x7) | (count << 3)
}

// encodeZigZag encodes signed integers using zigzag encoding.
func encodeZigZag(n int32) uint32 {
	return uint32((n << 1) ^ (n >> 31))
}

// datumToValue converts a tree.Datum to a simple value for MVT encoding.
func datumToValue(datum tree.Datum) interface{} {
	if datum == tree.DNull {
		return nil
	}
	switch d := datum.(type) {
	case *tree.DString:
		return string(*d)
	case *tree.DInt:
		return int64(*d)
	case *tree.DFloat:
		return float64(*d)
	case *tree.DBool:
		return bool(*d)
	case *tree.DDecimal:
		f, _ := d.Float64()
		return f
	default:
		return fmt.Sprintf("%v", datum)
	}
}

// valueToTileValue converts a Go value to a protobuf TileValue.
func valueToTileValue(value interface{}) *VectorTile_Value {
	switch v := value.(type) {
	case string:
		return &VectorTile_Value{ValueOneof: &VectorTile_Value_StringValue{StringValue: v}}
	case int64:
		return &VectorTile_Value{ValueOneof: &VectorTile_Value_IntValue{IntValue: v}}
	case float64:
		return &VectorTile_Value{ValueOneof: &VectorTile_Value_DoubleValue{DoubleValue: v}}
	case bool:
		return &VectorTile_Value{ValueOneof: &VectorTile_Value_BoolValue{BoolValue: v}}
	default:
		s := fmt.Sprintf("%v", v)
		return &VectorTile_Value{ValueOneof: &VectorTile_Value_StringValue{StringValue: s}}
	}
}
