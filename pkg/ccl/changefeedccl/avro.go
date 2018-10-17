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
	"fmt"
	"math/big"
	"time"

	"github.com/cockroachdb/apd"
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

type avroLogicalType struct {
	SchemaType  avroSchemaType `json:"type"`
	LogicalType string         `json:"logicalType"`
	Precision   int            `json:"precision,omitempty"`
	Scale       int            `json:"scale,omitempty"`
}

func avroUnionKey(t avroSchemaType) string {
	switch s := t.(type) {
	case string:
		return s
	case avroLogicalType:
		return avroUnionKey(s.SchemaType) + `.` + s.LogicalType
	default:
		panic(fmt.Sprintf(`unsupported type %T %v`, t, t))
	}
}

// avroSchemaField is our representation of the schema of a field in an avro
// record. Serializing it to JSON gives the standard schema representation.
type avroSchemaField struct {
	SchemaType avroSchemaType `json:"type"`
	Name       string         `json:"name"`

	// TODO(dan): typ should be derivable from the json `type` and `logicalType`
	// fields. This would make it possible to roundtrip CockroachDB schemas
	// through avro.
	typ sqlbase.ColumnType

	encodeFn func(tree.Datum) (interface{}, error)
	decodeFn func(interface{}) (tree.Datum, error)
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

func avroUnescapeName(name string) string {
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

	var avroType avroSchemaType
	switch colDesc.Type.SemanticType {
	case sqlbase.ColumnType_INT:
		avroType = avroSchemaLong
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return int64(*d.(*tree.DInt)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(x.(int64))), nil
		}
	case sqlbase.ColumnType_BOOL:
		avroType = avroSchemaBoolean
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return bool(*d.(*tree.DBool)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.MakeDBool(tree.DBool(x.(bool))), nil
		}
	case sqlbase.ColumnType_FLOAT:
		avroType = avroSchemaDouble
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return float64(*d.(*tree.DFloat)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(x.(float64))), nil
		}
	case sqlbase.ColumnType_STRING:
		avroType = avroSchemaString
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return string(*d.(*tree.DString)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDString(x.(string)), nil
		}
	case sqlbase.ColumnType_BYTES:
		avroType = avroSchemaBytes
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(*d.(*tree.DBytes)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDBytes(tree.DBytes(x.([]byte))), nil
		}
	case sqlbase.ColumnType_TIMESTAMP:
		avroType = avroLogicalType{
			SchemaType:  avroSchemaLong,
			LogicalType: `timestamp-micros`,
		}
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return d.(*tree.DTimestamp).Time, nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.MakeDTimestamp(x.(time.Time), time.Microsecond), nil
		}
	case sqlbase.ColumnType_DECIMAL:
		if colDesc.Type.Precision == 0 {
			return nil, errors.Errorf(
				`column %s: decimal with no precision not yet supported with avro`, colDesc.Name)
		}
		decimalType := avroLogicalType{
			SchemaType:  avroSchemaBytes,
			LogicalType: `decimal`,
			Precision:   int(colDesc.Type.Precision),
			Scale:       int(colDesc.Type.Width),
		}
		avroType = decimalType
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			dec := d.(*tree.DDecimal).Decimal
			// TODO(dan): For the cases that the avro defined decimal format
			// would not roundtrip, serialize the decimal as a string. Also
			// support the unspecified precision/scale case in this branch. We
			// can't currently do this without surgery to the avro library we're
			// using and that's too scary leading up to 2.1.0.
			rat, err := decimalToRat(dec, colDesc.Type.Width)
			if err != nil {
				return nil, err
			}
			return &rat, nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return &tree.DDecimal{Decimal: ratToDecimal(*x.(*big.Rat), colDesc.Type.Width)}, nil
		}
	default:
		// TODO(dan): Support the other column types.
		return nil, errors.Errorf(`column %s: type %s not yet supported with avro`,
			colDesc.Name, colDesc.Type.SemanticType)
	}
	schema.SchemaType = avroType

	if colDesc.Nullable {
		// The default for a union type is the default for the first element of
		// the union. For nullable fields with no default, we want null.
		schema.SchemaType = []avroSchemaType{avroSchemaNull, avroType}
		encodeFn := schema.encodeFn
		decodeFn := schema.decodeFn
		unionKey := avroUnionKey(avroType)
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			if d == tree.DNull {
				return goavro.Union(avroSchemaNull, nil), nil
			}
			encoded, err := encodeFn(d)
			if err != nil {
				return nil, err
			}
			return goavro.Union(unionKey, encoded), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			if x == nil {
				return tree.DNull, nil
			}
			return decodeFn(x.(map[string]interface{})[unionKey])
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

// textualFromRow encodes the given row data into avro's defined JSON format.
func (r *avroSchemaRecord) textualFromRow(row sqlbase.EncDatumRow) ([]byte, error) {
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

// rowFromTextual decodes the given row data from avro's defined JSON format.
func (r *avroSchemaRecord) rowFromTextual(buf []byte) (sqlbase.EncDatumRow, error) {
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
		var err error
		if avroDatums[field.Name], err = field.encodeFn(d.Datum); err != nil {
			return nil, err
		}
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
		decoded, err := field.decodeFn(avroDatum)
		if err != nil {
			return nil, err
		}
		row[r.colIdxByFieldIdx[fieldIdx]] = sqlbase.DatumToEncDatum(field.typ, decoded)
	}
	return row, nil
}

// decimalToRat converts one of our apd decimals to the format expected by the
// avro library we use. If the column has a fixed scale (which is always true if
// precision is set) this is roundtripable without information loss.
//
// TODO(dan): We really should just be controlling our own encoding destiny
// here. Make that possible.
func decimalToRat(dec apd.Decimal, scale int32) (big.Rat, error) {
	if dec.Form != apd.Finite {
		return big.Rat{}, errors.Errorf(`cannot convert %s form decimal`, dec.Form)
	}
	if scale > 0 && scale != -dec.Exponent {
		return big.Rat{}, errors.Errorf(`%s will not roundtrip at scale %d`, &dec, scale)
	}
	var r big.Rat
	if dec.Exponent >= 0 {
		exp := big.NewInt(10)
		exp = exp.Exp(exp, big.NewInt(int64(dec.Exponent)), nil)
		var coeff big.Int
		r.SetFrac(coeff.Mul(&dec.Coeff, exp), big.NewInt(1))
	} else {
		exp := big.NewInt(10)
		exp = exp.Exp(exp, big.NewInt(int64(-dec.Exponent)), nil)
		r.SetFrac(&dec.Coeff, exp)
	}
	if dec.Negative {
		r.Mul(&r, big.NewRat(-1, 1))
	}
	return r, nil
}

// ratToDecimal converts the output of decimalToRat back into the original apd
// decimal, given a fixed column scale. NB: big.Rat is lossy-compared to apd
// decimal, so this is not possible when the scale is not fixed.
func ratToDecimal(rat big.Rat, scale int32) apd.Decimal {
	num, denom := rat.Num(), rat.Denom()
	exp := big.NewInt(10)
	exp = exp.Exp(exp, big.NewInt(int64(scale)), nil)
	sf := denom.Div(exp, denom)
	coeff := num.Mul(num, sf)
	dec := apd.NewWithBigInt(coeff, -scale)
	return *dec
}
