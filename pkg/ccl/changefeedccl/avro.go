// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
)

// avroSchemaType is one of the set of avro primitive types.
type avroSchemaType interface{}

const (
	avroSchemaBoolean = `boolean`
	avroSchemaBytes   = `bytes`
	avroSchemaDouble  = `double`
	avroSchemaLong    = `long`
	avroSchemaNull    = `null`
	avroSchemaString  = `string`
)

// avroSchemaField is our representation of the schema of a field in an avro
// record. Serializing it to JSON gives the standard schema representation.
type avroSchemaField struct {
	SchemaType avroSchemaType `json:"type"`
	Name       string         `json:"name"`

	// TODO(dan): typ should be derivable from the json `type` and `logicalType`
	// fields. This would make it possible to roundtrip CockroachDB schemas
	// through avro.
	typ sqlbase.ColumnType

	encodeFn func(tree.Datum) interface{}
	decodeFn func(interface{}) tree.Datum
}

// avroSchemaRecord is our representation of the schema of an avro record.
// Serializing it to JSON gives the standard schema representation.
type avroSchemaRecord struct {
	SchemaType string             `json:"type"`
	Name       string             `json:"name"`
	Fields     []*avroSchemaField `json:"fields"`

	colIdxByFieldIdx map[int]int
	fieldIdxByName   map[string]int
	codec            *goavro.Codec
	alloc            sqlbase.DatumAlloc
}

func avroEscapeName(name string) string {
	// TODO(dan): Name escaping.
	return name
}

// columnDescToAvroSchema converts a column descriptor into its corresponding
// avro field schema.
func columnDescToAvroSchema(colDesc *sqlbase.ColumnDescriptor) (*avroSchemaField, error) {
	schema := &avroSchemaField{
		Name: avroEscapeName(colDesc.Name),
		typ:  colDesc.Type,
	}

	var avroType string
	switch colDesc.Type.SemanticType {
	case sqlbase.ColumnType_INT:
		avroType = avroSchemaLong
		schema.encodeFn = func(d tree.Datum) interface{} {
			return int64(*d.(*tree.DInt))
		}
		schema.decodeFn = func(x interface{}) tree.Datum {
			return tree.NewDInt(tree.DInt(x.(int64)))
		}
	case sqlbase.ColumnType_BOOL:
		avroType = avroSchemaBoolean
		schema.encodeFn = func(d tree.Datum) interface{} {
			return bool(*d.(*tree.DBool))
		}
		schema.decodeFn = func(x interface{}) tree.Datum {
			return tree.MakeDBool(tree.DBool(x.(bool)))
		}
	case sqlbase.ColumnType_FLOAT:
		avroType = avroSchemaDouble
		schema.encodeFn = func(d tree.Datum) interface{} {
			return float64(*d.(*tree.DFloat))
		}
		schema.decodeFn = func(x interface{}) tree.Datum {
			return tree.NewDFloat(tree.DFloat(x.(float64)))
		}
	case sqlbase.ColumnType_STRING:
		avroType = avroSchemaString
		schema.encodeFn = func(d tree.Datum) interface{} {
			return string(*d.(*tree.DString))
		}
		schema.decodeFn = func(x interface{}) tree.Datum {
			return tree.NewDString(x.(string))
		}
	case sqlbase.ColumnType_BYTES:
		avroType = avroSchemaBytes
		schema.encodeFn = func(d tree.Datum) interface{} {
			return []byte(*d.(*tree.DBytes))
		}
		schema.decodeFn = func(x interface{}) tree.Datum {
			return tree.NewDBytes(tree.DBytes(x.([]byte)))
		}
	default:
		// TODO(dan): Support the other column types.
		return nil, errors.Errorf(`unsupported column type: %s`, colDesc.Type.SemanticType)
	}
	schema.SchemaType = avroType

	if colDesc.Nullable {
		schema.SchemaType = []avroSchemaType{avroType, avroSchemaNull}
		encodeFn := schema.encodeFn
		decodeFn := schema.decodeFn
		schema.encodeFn = func(d tree.Datum) interface{} {
			if d == tree.DNull {
				return goavro.Union(avroSchemaNull, nil)
			}
			return goavro.Union(avroType, encodeFn(d))
		}
		schema.decodeFn = func(x interface{}) tree.Datum {
			if x == nil {
				return tree.DNull
			}
			return decodeFn(x.(map[string]interface{})[avroType])
		}
	}

	// TODO(dan): Handle default and computed values.

	return schema, nil
}

// indexToAvroSchema converts a column descriptor into its corresponding avro
// record schema. The fields are kept in the same order as columns in the index.
func indexToAvroSchema(
	tableDesc *sqlbase.TableDescriptor, indexDesc *sqlbase.IndexDescriptor,
) (*avroSchemaRecord, error) {
	schema := &avroSchemaRecord{
		Name:             avroEscapeName(tableDesc.Name),
		SchemaType:       `record`,
		fieldIdxByName:   make(map[string]int),
		colIdxByFieldIdx: make(map[int]int),
	}
	colIdxByID := tableDesc.ColumnIdxMap()
	for _, colID := range indexDesc.ColumnIDs {
		colIdx, ok := colIdxByID[colID]
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		col := tableDesc.Columns[colIdx]
		field, err := columnDescToAvroSchema(&col)
		if err != nil {
			return nil, err
		}
		schema.colIdxByFieldIdx[len(schema.Fields)] = colIdx
		schema.fieldIdxByName[field.Name] = len(schema.Fields)
		schema.Fields = append(schema.Fields, field)
	}
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	schema.codec, err = goavro.NewCodec(string(schemaJSON))
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// tableToAvroSchema converts a column descriptor into its corresponding avro
// record schema. The fields are kept in the same order as `tableDesc.Columns`.
func tableToAvroSchema(tableDesc *sqlbase.TableDescriptor) (*avroSchemaRecord, error) {
	schema := &avroSchemaRecord{
		Name:             avroEscapeName(tableDesc.Name),
		SchemaType:       `record`,
		fieldIdxByName:   make(map[string]int),
		colIdxByFieldIdx: make(map[int]int),
	}
	for colIdx, col := range tableDesc.Columns {
		field, err := columnDescToAvroSchema(&col)
		if err != nil {
			return nil, err
		}
		schema.colIdxByFieldIdx[len(schema.Fields)] = colIdx
		schema.fieldIdxByName[field.Name] = len(schema.Fields)
		schema.Fields = append(schema.Fields, field)
	}
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	schema.codec, err = goavro.NewCodec(string(schemaJSON))
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// TextualFromRow encodes the given row data into avro's defined JSON format.
func (r *avroSchemaRecord) TextualFromRow(row sqlbase.EncDatumRow) ([]byte, error) {
	native, err := r.nativeFromRow(row)
	if err != nil {
		return nil, err
	}
	return r.codec.TextualFromNative(nil /* buf */, native)
}

// BinaryFromRow encodes the given row data into avro's defined binary format.
func (r *avroSchemaRecord) BinaryFromRow(buf []byte, row sqlbase.EncDatumRow) ([]byte, error) {
	native, err := r.nativeFromRow(row)
	if err != nil {
		return nil, err
	}
	return r.codec.BinaryFromNative(buf, native)
}

// RowFromTextual decodes the given row data from avro's defined JSON format.
func (r *avroSchemaRecord) RowFromTextual(buf []byte) (sqlbase.EncDatumRow, error) {
	native, newBuf, err := r.codec.NativeFromTextual(buf)
	if err != nil {
		return nil, err
	}
	if len(newBuf) > 0 {
		return nil, errors.New(`only one row was expected`)
	}
	return r.rowFromNative(native)
}

// RowFromBinary decodes the given row data from avro's defined binary format.
func (r *avroSchemaRecord) RowFromBinary(buf []byte) (sqlbase.EncDatumRow, error) {
	native, newBuf, err := r.codec.NativeFromBinary(buf)
	if err != nil {
		return nil, err
	}
	if len(newBuf) > 0 {
		return nil, errors.New(`only one row was expected`)
	}
	return r.rowFromNative(native)
}

func (r *avroSchemaRecord) nativeFromRow(row sqlbase.EncDatumRow) (interface{}, error) {
	avroDatums := make(map[string]interface{}, len(row))
	for fieldIdx, field := range r.Fields {
		d := row[r.colIdxByFieldIdx[fieldIdx]]
		if err := d.EnsureDecoded(&field.typ, &r.alloc); err != nil {
			return nil, err
		}
		avroDatums[field.Name] = field.encodeFn(d.Datum)
	}
	return avroDatums, nil
}

func (r *avroSchemaRecord) rowFromNative(native interface{}) (sqlbase.EncDatumRow, error) {
	avroDatums, ok := native.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf(`unknown avro native type: %T`, native)
	}
	if len(r.Fields) != len(avroDatums) {
		return nil, errors.Errorf(
			`expected row with %d columns got %d`, len(r.Fields), len(avroDatums))
	}
	row := make(sqlbase.EncDatumRow, len(r.Fields))
	for fieldName, avroDatum := range avroDatums {
		fieldIdx := r.fieldIdxByName[fieldName]
		field := r.Fields[fieldIdx]
		row[r.colIdxByFieldIdx[fieldIdx]] = sqlbase.DatumToEncDatum(
			field.typ, field.decodeFn(avroDatum))
	}
	return row, nil
}
