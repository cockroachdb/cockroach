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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/errors"
	"github.com/linkedin/goavro/v2"
)

// The file contains a very specific marriage between avro and our SQL schemas.
// It's not intended to be a general purpose avro utility.
//
// Avro is a spec for data schemas, a binary format for encoding a record
// conforming to a given schema, and various container formats for those encoded
// records. It also has rules for determining backward and forward compatibility
// of schemas as they evolve.
//
// The Confluent ecosystem, Kafka plus other things, has first-class support for
// Avro, including a server for registering schemas and referencing which
// registered schema a Kafka record conforms to.
//
// We map a SQL table schema to an Avro record with 1:1 mapping between table
// columns and Avro fields. The type of the column is mapped to a native Avro
// type as faithfully as possible. This is then used to make an "optional" Avro
// field for that column by unioning with null and explicitly specifying null as
// default, regardless of whether the sql column allows NULLs. This may seem an
// odd choice, but it allows for all adjacent Avro schemas for a given SQL table
// to be backward and forward compatible with each other. Forward and backward
// compatibility drastically eases use of the resulting data by downstream
// systems, especially when working with long histories of archived data across
// many schema changes (such as a data lake).
//
// One downside of the above is that it's not possible to recover the original
// SQL table schema from an Avro one. (This is also true for other reasons, such
// as lossy mappings from sql types to avro types.) To partially address this,
// the SQL column type is embedded as metadata in the Avro field schema in a way
// that Avro ignores it but passes it along.

// avroSchemaType is one of the set of avro primitive types.
type avroSchemaType interface{}

const (
	avroSchemaBoolean = `boolean`
	avroSchemaBytes   = `bytes`
	avroSchemaDouble  = `double`
	avroSchemaInt     = `int`
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
	case *avroRecord:
		return s.Name
	default:
		panic(fmt.Sprintf(`unsupported type %T %v`, t, t))
	}
}

// avroSchemaField is our representation of the schema of a field in an avro
// record. Serializing it to JSON gives the standard schema representation.
type avroSchemaField struct {
	SchemaType avroSchemaType `json:"type"`
	Name       string         `json:"name"`
	Default    *string        `json:"default"`
	Metadata   string         `json:"__crdb__,omitempty"`

	typ *types.T

	encodeFn func(tree.Datum) (interface{}, error)
	decodeFn func(interface{}) (tree.Datum, error)
}

// avroRecord is our representation of the schema of an avro record. Serializing
// it to JSON gives the standard schema representation.
type avroRecord struct {
	SchemaType string             `json:"type"`
	Name       string             `json:"name"`
	Fields     []*avroSchemaField `json:"fields"`
	codec      *goavro.Codec
}

// avroDataRecord is an `avroRecord` that represents the schema of a SQL table
// or index.
type avroDataRecord struct {
	avroRecord

	colIdxByFieldIdx map[int]int
	fieldIdxByName   map[string]int
	alloc            sqlbase.DatumAlloc
}

// avroMetadata is the `avroEnvelopeRecord` metadata.
type avroMetadata map[string]interface{}

// avroEnvelopeOpts controls which fields in avroEnvelopeRecord are set.
type avroEnvelopeOpts struct {
	beforeField, afterField     bool
	updatedField, resolvedField bool
}

// avroEnvelopeRecord is an `avroRecord` that wraps a changed SQL row and some
// metadata.
type avroEnvelopeRecord struct {
	avroRecord

	opts          avroEnvelopeOpts
	before, after *avroDataRecord
}

// columnDescToAvroSchema converts a column descriptor into its corresponding
// avro field schema.
func columnDescToAvroSchema(colDesc *sqlbase.ColumnDescriptor) (*avroSchemaField, error) {
	schema := &avroSchemaField{
		Name:     SQLNameToAvroName(colDesc.Name),
		Metadata: colDesc.SQLString(),
		Default:  nil,
		typ:      colDesc.Type,
	}

	var avroType avroSchemaType
	switch colDesc.Type.Family() {
	case types.IntFamily:
		avroType = avroSchemaLong
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return int64(*d.(*tree.DInt)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(x.(int64))), nil
		}
	case types.BoolFamily:
		avroType = avroSchemaBoolean
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return bool(*d.(*tree.DBool)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.MakeDBool(tree.DBool(x.(bool))), nil
		}
	case types.FloatFamily:
		avroType = avroSchemaDouble
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return float64(*d.(*tree.DFloat)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(x.(float64))), nil
		}
	case types.GeographyFamily:
		avroType = avroSchemaBytes
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DGeography).EWKB()), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			g, err := geo.ParseGeographyFromEWKBUnsafe(geopb.EWKB(x.([]byte)))
			if err != nil {
				return nil, err
			}
			return &tree.DGeography{Geography: g}, nil
		}
	case types.GeometryFamily:
		avroType = avroSchemaBytes
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DGeometry).EWKB()), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			g, err := geo.ParseGeometryFromEWKBUnsafe(geopb.EWKB(x.([]byte)))
			if err != nil {
				return nil, err
			}
			return &tree.DGeometry{Geometry: g}, nil
		}
	case types.StringFamily:
		avroType = avroSchemaString
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return string(*d.(*tree.DString)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDString(x.(string)), nil
		}
	case types.BytesFamily:
		avroType = avroSchemaBytes
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(*d.(*tree.DBytes)), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDBytes(tree.DBytes(x.([]byte))), nil
		}
	case types.DateFamily:
		avroType = avroLogicalType{
			SchemaType:  avroSchemaInt,
			LogicalType: `date`,
		}
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			date := *d.(*tree.DDate)
			if !date.IsFinite() {
				return nil, errors.Errorf(
					`column %s: infinite date not yet supported with avro`, colDesc.Name)
			}
			// The avro library requires us to return this as a time.Time.
			return date.ToTime()
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			// The avro library hands this back as a time.Time.
			return tree.NewDDateFromTime(x.(time.Time))
		}
	case types.TimeFamily:
		avroType = avroLogicalType{
			SchemaType:  avroSchemaLong,
			LogicalType: `time-micros`,
		}
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			// The avro library requires us to return this as a time.Duration.
			duration := time.Duration(*d.(*tree.DTime)) * time.Microsecond
			return duration, nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			// The avro library hands this back as a time.Duration.
			micros := x.(time.Duration) / time.Microsecond
			return tree.MakeDTime(timeofday.TimeOfDay(micros)), nil
		}
	case types.TimeTZFamily:
		avroType = avroSchemaString
		// We cannot encode this as a long, as it does not encode
		// timezone correctly.
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return d.(*tree.DTimeTZ).TimeTZ.String(), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			d, _, err := tree.ParseDTimeTZ(nil, x.(string), time.Microsecond)
			return d, err
		}
	case types.TimestampFamily:
		avroType = avroLogicalType{
			SchemaType:  avroSchemaLong,
			LogicalType: `timestamp-micros`,
		}
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return d.(*tree.DTimestamp).Time, nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.MakeDTimestamp(x.(time.Time), time.Microsecond)
		}
	case types.TimestampTZFamily:
		avroType = avroLogicalType{
			SchemaType:  avroSchemaLong,
			LogicalType: `timestamp-micros`,
		}
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return d.(*tree.DTimestampTZ).Time, nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.MakeDTimestampTZ(x.(time.Time), time.Microsecond)
		}
	case types.DecimalFamily:
		if colDesc.Type.Precision() == 0 {
			return nil, errors.Errorf(
				`column %s: decimal with no precision not yet supported with avro`, colDesc.Name)
		}
		avroType = avroLogicalType{
			SchemaType:  avroSchemaBytes,
			LogicalType: `decimal`,
			Precision:   int(colDesc.Type.Precision()),
			Scale:       int(colDesc.Type.Width()),
		}
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			dec := d.(*tree.DDecimal).Decimal
			// TODO(dan): For the cases that the avro defined decimal format
			// would not roundtrip, serialize the decimal as a string. Also
			// support the unspecified precision/scale case in this branch. We
			// can't currently do this without surgery to the avro library we're
			// using and that's too scary leading up to 2.1.0.
			rat, err := decimalToRat(dec, colDesc.Type.Width())
			if err != nil {
				return nil, err
			}
			return &rat, nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return &tree.DDecimal{Decimal: ratToDecimal(*x.(*big.Rat), colDesc.Type.Width())}, nil
		}
	case types.UuidFamily:
		// Should be logical type of "uuid", but the avro library doesn't support
		// that yet.
		avroType = avroSchemaString
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return d.(*tree.DUuid).UUID.String(), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.ParseDUuidFromString(x.(string))
		}
	case types.INetFamily:
		avroType = avroSchemaString
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return d.(*tree.DIPAddr).IPAddr.String(), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.ParseDIPAddrFromINetString(x.(string))
		}
	case types.JsonFamily:
		avroType = avroSchemaString
		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			return d.(*tree.DJSON).JSON.String(), nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.ParseDJSON(x.(string))
		}
	default:
		return nil, errors.Errorf(`column %s: type %s not yet supported with avro`,
			colDesc.Name, colDesc.Type.SQLString())
	}
	schema.SchemaType = avroType

	// Make every field optional by unioning it with null, so that all schema
	// evolutions for a table are considered "backward compatible" by avro. This
	// means that the Avro type doesn't mirror the column's nullability, but it
	// makes it much easier to work with long histories of table data afterward,
	// especially for things like loading into analytics databases.
	{
		// The default for a union type is the default for the first element of
		// the union.
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

	return schema, nil
}

// indexToAvroSchema converts a column descriptor into its corresponding avro
// record schema. The fields are kept in the same order as columns in the index.
func indexToAvroSchema(
	tableDesc *sqlbase.TableDescriptor, indexDesc *sqlbase.IndexDescriptor,
) (*avroDataRecord, error) {
	schema := &avroDataRecord{
		avroRecord: avroRecord{
			Name:       SQLNameToAvroName(tableDesc.Name),
			SchemaType: `record`,
		},
		fieldIdxByName:   make(map[string]int),
		colIdxByFieldIdx: make(map[int]int),
	}
	colIdxByID := tableDesc.ColumnIdxMap()
	for _, colID := range indexDesc.ColumnIDs {
		colIdx, ok := colIdxByID[colID]
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		col := &tableDesc.Columns[colIdx]
		field, err := columnDescToAvroSchema(col)
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

const (
	// avroSchemaNoSuffix can be passed to tableToAvroSchema to indicate that
	// no suffix should be appended to the avro record's name.
	avroSchemaNoSuffix = ``
)

// tableToAvroSchema converts a column descriptor into its corresponding avro
// record schema. The fields are kept in the same order as `tableDesc.Columns`.
// If a name suffix is provided (as opposed to avroSchemaNoSuffix), it will be
// appended to the end of the avro record's name.
func tableToAvroSchema(
	tableDesc *sqlbase.TableDescriptor, nameSuffix string,
) (*avroDataRecord, error) {
	name := SQLNameToAvroName(tableDesc.Name)
	if nameSuffix != avroSchemaNoSuffix {
		name = name + `_` + nameSuffix
	}
	schema := &avroDataRecord{
		avroRecord: avroRecord{
			Name:       name,
			SchemaType: `record`,
		},
		fieldIdxByName:   make(map[string]int),
		colIdxByFieldIdx: make(map[int]int),
	}
	for colIdx := range tableDesc.Columns {
		col := &tableDesc.Columns[colIdx]
		field, err := columnDescToAvroSchema(col)
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
func (r *avroDataRecord) textualFromRow(row sqlbase.EncDatumRow) ([]byte, error) {
	native, err := r.nativeFromRow(row)
	if err != nil {
		return nil, err
	}
	return r.codec.TextualFromNative(nil /* buf */, native)
}

// BinaryFromRow encodes the given row data into avro's defined binary format.
func (r *avroDataRecord) BinaryFromRow(buf []byte, row sqlbase.EncDatumRow) ([]byte, error) {
	native, err := r.nativeFromRow(row)
	if err != nil {
		return nil, err
	}
	return r.codec.BinaryFromNative(buf, native)
}

// rowFromTextual decodes the given row data from avro's defined JSON format.
func (r *avroDataRecord) rowFromTextual(buf []byte) (sqlbase.EncDatumRow, error) {
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
func (r *avroDataRecord) RowFromBinary(buf []byte) (sqlbase.EncDatumRow, error) {
	native, newBuf, err := r.codec.NativeFromBinary(buf)
	if err != nil {
		return nil, err
	}
	if len(newBuf) > 0 {
		return nil, errors.New(`only one row was expected`)
	}
	return r.rowFromNative(native)
}

func (r *avroDataRecord) nativeFromRow(row sqlbase.EncDatumRow) (interface{}, error) {
	avroDatums := make(map[string]interface{}, len(row))
	for fieldIdx, field := range r.Fields {
		d := row[r.colIdxByFieldIdx[fieldIdx]]
		if err := d.EnsureDecoded(field.typ, &r.alloc); err != nil {
			return nil, err
		}
		var err error
		if avroDatums[field.Name], err = field.encodeFn(d.Datum); err != nil {
			return nil, err
		}
	}
	return avroDatums, nil
}

func (r *avroDataRecord) rowFromNative(native interface{}) (sqlbase.EncDatumRow, error) {
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

// envelopeToAvroSchema creates an avro record schema for an envelope containing
// before and after versions of a row change and metadata about that row change.
func envelopeToAvroSchema(
	topic string, opts avroEnvelopeOpts, before, after *avroDataRecord,
) (*avroEnvelopeRecord, error) {
	schema := &avroEnvelopeRecord{
		avroRecord: avroRecord{
			Name:       SQLNameToAvroName(topic) + `_envelope`,
			SchemaType: `record`,
		},
		opts: opts,
	}

	if opts.beforeField {
		schema.before = before
		beforeField := &avroSchemaField{
			Name:       `before`,
			SchemaType: []avroSchemaType{avroSchemaNull, before},
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, beforeField)
	}
	if opts.afterField {
		schema.after = after
		afterField := &avroSchemaField{
			Name:       `after`,
			SchemaType: []avroSchemaType{avroSchemaNull, after},
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, afterField)
	}
	if opts.updatedField {
		updatedField := &avroSchemaField{
			SchemaType: []avroSchemaType{avroSchemaNull, avroSchemaString},
			Name:       `updated`,
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, updatedField)
	}
	if opts.resolvedField {
		resolvedField := &avroSchemaField{
			SchemaType: []avroSchemaType{avroSchemaNull, avroSchemaString},
			Name:       `resolved`,
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, resolvedField)
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

// BinaryFromRow encodes the given metadata and row data into avro's defined
// binary format.
func (r *avroEnvelopeRecord) BinaryFromRow(
	buf []byte, meta avroMetadata, beforeRow, afterRow sqlbase.EncDatumRow,
) ([]byte, error) {
	native := map[string]interface{}{}
	if r.opts.beforeField {
		if beforeRow == nil {
			native[`before`] = nil
		} else {
			beforeNative, err := r.before.nativeFromRow(beforeRow)
			if err != nil {
				return nil, err
			}
			native[`before`] = goavro.Union(avroUnionKey(&r.before.avroRecord), beforeNative)
		}
	}
	if r.opts.afterField {
		if afterRow == nil {
			native[`after`] = nil
		} else {
			afterNative, err := r.after.nativeFromRow(afterRow)
			if err != nil {
				return nil, err
			}
			native[`after`] = goavro.Union(avroUnionKey(&r.after.avroRecord), afterNative)
		}
	}
	if r.opts.updatedField {
		native[`updated`] = nil
		if u, ok := meta[`updated`]; ok {
			delete(meta, `updated`)
			ts, ok := u.(hlc.Timestamp)
			if !ok {
				return nil, errors.Errorf(`unknown metadata timestamp type: %T`, u)
			}
			native[`updated`] = goavro.Union(avroUnionKey(avroSchemaString), ts.AsOfSystemTime())
		}
	}
	if r.opts.resolvedField {
		native[`resolved`] = nil
		if u, ok := meta[`resolved`]; ok {
			delete(meta, `resolved`)
			ts, ok := u.(hlc.Timestamp)
			if !ok {
				return nil, errors.Errorf(`unknown metadata timestamp type: %T`, u)
			}
			native[`resolved`] = goavro.Union(avroUnionKey(avroSchemaString), ts.AsOfSystemTime())
		}
	}
	for k := range meta {
		return nil, errors.AssertionFailedf(`unhandled meta key: %s`, k)
	}
	return r.codec.BinaryFromNative(buf, native)
}

// decimalToRat converts one of our apd decimals to the format expected by the
// avro library we use. If the column has a fixed scale (which is always true if
// precision is set) this is roundtripable without information loss.
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
