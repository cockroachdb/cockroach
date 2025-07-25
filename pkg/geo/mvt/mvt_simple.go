// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package mvt provides functionality for encoding Mapbox Vector Tiles.
package mvt

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
)

// SimpleMVTBuilder creates a basic MVT without full protobuf implementation
type SimpleMVTBuilder struct {
	layerName string
	extent    uint32
	features  []SimpleMVTFeature
}

// SimpleMVTFeature represents a feature in simplified format
type SimpleMVTFeature struct {
	ID         *uint64
	Geometry   geo.Geometry
	Properties map[string]interface{}
}

// NewSimpleMVTBuilder creates a new simplified MVT builder
func NewSimpleMVTBuilder(layerName string, extent uint32) *SimpleMVTBuilder {
	if extent == 0 {
		extent = 4096
	}
	return &SimpleMVTBuilder{
		layerName: layerName,
		extent:    extent,
		features:  make([]SimpleMVTFeature, 0),
	}
}

// AddFeature adds a feature to the MVT layer
func (b *SimpleMVTBuilder) AddFeature(geom geo.Geometry, id *uint64, properties map[string]tree.Datum) error {
	if geom.Empty() {
		return nil // Skip empty geometries
	}

	// Convert properties to simple types
	props := make(map[string]interface{})
	for key, datum := range properties {
		if datum == tree.DNull {
			continue
		}
		props[key] = datumToValue(datum)
	}

	feature := SimpleMVTFeature{
		ID:         id,
		Geometry:   geom,
		Properties: props,
	}

	b.features = append(b.features, feature)
	return nil
}

// Build generates a basic MVT-like binary format
func (b *SimpleMVTBuilder) Build() ([]byte, error) {
	if len(b.features) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer

	// Write basic MVT header (simplified format)
	buf.WriteString("MVT")        // Magic bytes
	buf.WriteByte(2)              // Version
	
	// Write layer name length and name
	layerNameBytes := []byte(b.layerName)
	binary.Write(&buf, binary.LittleEndian, uint32(len(layerNameBytes)))
	buf.Write(layerNameBytes)
	
	// Write extent
	binary.Write(&buf, binary.LittleEndian, b.extent)
	
	// Write number of features
	binary.Write(&buf, binary.LittleEndian, uint32(len(b.features)))

	// Write features
	for _, feature := range b.features {
		if err := b.writeFeature(&buf, feature); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (b *SimpleMVTBuilder) writeFeature(buf *bytes.Buffer, feature SimpleMVTFeature) error {
	// Write feature ID (8 bytes, 0 if nil)
	if feature.ID != nil {
		binary.Write(buf, binary.LittleEndian, *feature.ID)
	} else {
		binary.Write(buf, binary.LittleEndian, uint64(0))
	}

	// Write geometry type and coordinates
	gt, err := feature.Geometry.AsGeomT()
	if err != nil {
		return err
	}

	geomType := getGeometryType(gt)
	buf.WriteByte(byte(geomType))

	coords, err := encodeGeometryCoords(gt)
	if err != nil {
		return err
	}

	binary.Write(buf, binary.LittleEndian, uint32(len(coords)))
	for _, coord := range coords {
		binary.Write(buf, binary.LittleEndian, coord)
	}

	// Write properties
	binary.Write(buf, binary.LittleEndian, uint32(len(feature.Properties)))
	for key, value := range feature.Properties {
		keyBytes := []byte(key)
		binary.Write(buf, binary.LittleEndian, uint32(len(keyBytes)))
		buf.Write(keyBytes)

		switch v := value.(type) {
		case string:
			buf.WriteByte(1) // String type
			valueBytes := []byte(v)
			binary.Write(buf, binary.LittleEndian, uint32(len(valueBytes)))
			buf.Write(valueBytes)
		case int64:
			buf.WriteByte(2) // Int type
			binary.Write(buf, binary.LittleEndian, v)
		case float64:
			buf.WriteByte(3) // Float type
			binary.Write(buf, binary.LittleEndian, v)
		case bool:
			buf.WriteByte(4) // Bool type
			if v {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		default:
			buf.WriteByte(1) // Default to string
			str := fmt.Sprintf("%v", v)
			valueBytes := []byte(str)
			binary.Write(buf, binary.LittleEndian, uint32(len(valueBytes)))
			buf.Write(valueBytes)
		}
	}

	return nil
}

func getGeometryType(gt geom.T) int {
	switch gt.(type) {
	case *geom.Point, *geom.MultiPoint:
		return 1
	case *geom.LineString, *geom.MultiLineString:
		return 2
	case *geom.Polygon, *geom.MultiPolygon:
		return 3
	default:
		return 0
	}
}

func encodeGeometryCoords(gt geom.T) ([]float64, error) {
	switch g := gt.(type) {
	case *geom.Point:
		return g.Coords(), nil
	case *geom.LineString:
		return g.FlatCoords(), nil
	case *geom.Polygon:
		return g.FlatCoords(), nil
	case *geom.MultiPoint:
		return g.FlatCoords(), nil
	case *geom.MultiLineString:
		return g.FlatCoords(), nil
	case *geom.MultiPolygon:
		return g.FlatCoords(), nil
	default:
		return nil, errors.Newf("unsupported geometry type: %T", gt)
	}
}

// datumToValue converts a tree.Datum to a simple value for MVT encoding.
func datumToValue(datum tree.Datum) interface{} {
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