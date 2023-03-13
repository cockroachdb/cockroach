// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"io"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// MaxParquetRowGroupSize is the maximal number of rows which can be written out to
// a single row group in a parquet file.
// TODO(jayant): customers may want to adjust the row group size based on rows instead of bytes
var MaxParquetRowGroupSize = settings.RegisterIntSetting(
	settings.TenantWritable,
	"changefeed.format.parquet.max_row_group_size",
	"is the maximal number of rows which can be written out to a single"+
		" row group in a parquet file when running a changefeed using the parquet format",
	parquet.DefaultMaxRowGroupLen,
	settings.NonNegativeInt,
).WithPublic()

// A schema field is an internal identifier for schema nodes used by the parquet library.
// A value of -1 will let the library auto-assign values. This does not affect reading
// or writing parquet files.
const defaultParquetSchemaFieldID = int32(-1)

// The parquet library utilizes a type length of -1 for all types
// except for the type parquet.FixedLenByteArray, in which case the type
// length is the length of the array. See comment on (*schema.PrimitiveNode).TypeLength()
const defaultParquetTypeLength = -1

// parquetEncodeFn encodes a tree.datum to have a parquet datum.
type parquetEncodeFn func(datum tree.Datum) (interface{}, error)

// parquetDecodeFn decodes a parquet datum to a tree.Datum.
type parquetDecodeFn func(interface{}) (tree.Datum, error)

// A parquetColumn stores the parquet schema node and encoder for a
// column. See newParquetSchema for more info.
type parquetColumn struct {
	node schema.Node
	enc  parquetEncodeFn
}

// A ParquetSchemaDefinition stores an array of parquetColumn along with a
// parquet schema. The order of columns in cols will match the order of columns
// in schema. See newParquetSchema for more info.
//
// In the context of writing (and reading written parquet files),
// any reference to a column index will refer to its index in the
// elements of this struct.
//
// This schema definition only includes information required for writing parquet
// files. It should not be required for reading files since all the necessary
// metadata to interpret columns will be written out to the file and read by a
// reader.
type parquetSchemaDefinition struct {
	cols   []parquetColumn
	schema *schema.Schema
}

// newParquetSchema generates a parquetSchemaDefinition with the following
// traits:
//
// The columns are in the order they appear in the cdcevent.Row (specifically,
// calling ForEachColumn on the row).
//
// The schema is a root node with terminal children nodes which represent
// primitive types such as int or bool. The column schemas can be traversed
// using schema.Column(i). The children are indexed from [0, len(cols) + 1). The
// last child is an artificial column inserted by the writer which stores the
// event type.
func newParquetSchema(row cdcevent.Row) (*parquetSchemaDefinition, error) {
	fields := make([]schema.Node, 0)
	cols := make([]parquetColumn, 0)

	err := row.ForEachColumn().Col(func(column cdcevent.ResultColumn) error {
		parquetCol, err := newParquetColumn(column)
		if err != nil {
			return err
		}
		cols = append(cols, parquetCol)
		fields = append(fields, parquetCol.node)
		return nil
	})
	if err != nil {
		return nil, err
	}

	eventTypeField, err :=
		schema.NewPrimitiveNodeLogical(parquetCrdbEventTypeColName,
			parquet.Repetitions.Optional, schema.StringLogicalType{},
			parquet.Types.ByteArray, defaultParquetTypeLength,
			defaultParquetSchemaFieldID)
	if err != nil {
		return nil, err
	}
	fields = append(fields, eventTypeField)
	cols = append(cols, parquetColumn{
		node: eventTypeField,
		enc: func(d tree.Datum) (interface{}, error) {
			return parquet.ByteArray(*d.(*tree.DString)), nil
		},
	})
	groupNode, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		fields, defaultParquetSchemaFieldID)
	if err != nil {
		return nil, err
	}
	return &parquetSchemaDefinition{
		cols:   cols,
		schema: schema.NewSchema(groupNode),
	}, nil
}

// A ParquetWriter writes cdcevent.Row data into an io.Writer sink. The
// ParquetWriter should be Close()ed before attempting to read from the output
// sink so parquet metadata is available.
type ParquetWriter struct {
	sch      *parquetSchemaDefinition
	writer   *file.Writer
	settings *cluster.Settings

	currentRowGroupSize   int64
	currentRowGroupWriter file.BufferedRowGroupWriter
}

// NewCDCParquetWriterFromRow constructs a new parquet writer which outputs to
// the given sink. This function interprets the schema from the supplied row.
// Rows passed to ParquetWriter.AddData should have the same schema as the row
// passed to this function.
func NewCDCParquetWriterFromRow(
	row cdcevent.Row, sink io.Writer, s *cluster.Settings,
) (*ParquetWriter, error) {
	// TODO(#99028): support compression schemes
	// TODO(#99028): add options for more stability / performance (allocator, batch size, page size)
	sch, err := newParquetSchema(row)
	if err != nil {
		return nil, err
	}
	opts := []parquet.WriterProperty{parquet.WithCreatedBy("cockroachdb"),
		parquet.WithMaxRowGroupLength(1)}
	props := parquet.NewWriterProperties(opts...)
	writer := file.NewParquetWriter(sink, sch.schema.Root(), file.WithWriterProps(props))
	return &ParquetWriter{
		sch:      sch,
		writer:   writer,
		settings: s,
	}, nil
}

// nonNilDefLevel represents a def level of 1, meaning that the value is non-nil.
var nonNilDefLevel = []int16{1}

// nonNilDefLevel represents a def level of 0, meaning that the value is nil.
var nilDefLevel = []int16{0}

// writeColBatch writes a value to the provided column chunk writer.
func writeColBatch(colWriter file.ColumnChunkWriter, value interface{}) (int64, error) {
	switch w := colWriter.(type) {
	case *file.Int32ColumnChunkWriter:
		return w.WriteBatch([]int32{value.(int32)}, nonNilDefLevel, nil)
	case *file.Int64ColumnChunkWriter:
		return w.WriteBatch([]int64{value.(int64)}, nonNilDefLevel, nil)
	case *file.ByteArrayColumnChunkWriter:
		return w.WriteBatch([]parquet.ByteArray{value.(parquet.ByteArray)}, nonNilDefLevel, nil)
	case *file.BooleanColumnChunkWriter:
		return w.WriteBatch([]bool{value.(bool)}, nonNilDefLevel, nil)
	case *file.FixedLenByteArrayColumnChunkWriter:
		return w.WriteBatch([]parquet.FixedLenByteArray{value.(parquet.FixedLenByteArray)}, nonNilDefLevel, nil)
	default:
		panic("unimplemented")
	}
}

func writeNilColBatch(colWriter file.ColumnChunkWriter) (int64, error) {
	switch w := colWriter.(type) {
	case *file.Int32ColumnChunkWriter:
		return w.WriteBatch([]int32{}, nilDefLevel, nil)
	case *file.Int64ColumnChunkWriter:
		return w.WriteBatch([]int64{}, nilDefLevel, nil)
	case *file.ByteArrayColumnChunkWriter:
		return w.WriteBatch([]parquet.ByteArray{}, nilDefLevel, nil)
	case *file.BooleanColumnChunkWriter:
		return w.WriteBatch([]bool{}, nilDefLevel, nil)
	case *file.FixedLenByteArrayColumnChunkWriter:
		return w.WriteBatch([]parquet.FixedLenByteArray{}, nilDefLevel, nil)
	default:
		panic("unimplemented")
	}
}

func (w *ParquetWriter) writeDatumToColChunk(d tree.Datum, colIdx int) error {
	cw, err := w.currentRowGroupWriter.Column(colIdx)
	if err != nil {
		return err
	}
	if d == tree.DNull {
		_, err = writeNilColBatch(cw)
		if err != nil {
			return err
		}
		return nil
	}

	data, err := w.sch.cols[colIdx].enc(d)
	if err != nil {
		return err
	}
	_, err = writeColBatch(cw, data)
	if err != nil {
		return err
	}
	return nil
}

// AddData writes the updatedRow. There is no guarantee that the row will
// immediately be flushed to the output sink.
func (w *ParquetWriter) AddData(updatedRow cdcevent.Row, prevRow cdcevent.Row) error {
	if w.currentRowGroupWriter == nil {
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
	} else if w.currentRowGroupSize == MaxParquetRowGroupSize.Get(&w.settings.SV) {
		if err := w.currentRowGroupWriter.Close(); err != nil {
			return err
		}
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
		w.currentRowGroupSize = 0
	}

	colIdx := 0
	if err := updatedRow.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		if err := w.writeDatumToColChunk(d, colIdx); err != nil {
			return err
		}
		colIdx += 1
		return nil
	}); err != nil {
		return err
	}

	// Populate event type column.
	if err := w.writeDatumToColChunk(getEventTypeDatum(updatedRow, prevRow), colIdx); err != nil {
		return err
	}

	w.currentRowGroupSize += 1
	return nil
}

func getEventTypeDatum(updatedRow cdcevent.Row, prevRow cdcevent.Row) tree.Datum {
	eventTypeDatum := tree.NewDString(parquetEventInsert)
	if updatedRow.IsDeleted() {
		*eventTypeDatum = tree.DString(parquetEventDelete)
	} else if prevRow.IsInitialized() && !prevRow.IsDeleted() {
		*eventTypeDatum = tree.DString(parquetEventUpdate)
	}
	return eventTypeDatum
}

// Close closes the writer and flushes any buffered data to the sink.
// If the sink implements io.WriteCloser, it will be closed by this method.
func (w *ParquetWriter) Close() error {
	if err := w.currentRowGroupWriter.Close(); err != nil {
		return err
	}
	return w.writer.Close()
}

// NewParquetColumn generates a parquetColumn for the supplied column.
// By default, a column's repetitions are set to parquet.Repetitions.Optional,
// which means that the column is nullable.
func newParquetColumn(column cdcevent.ResultColumn) (parquetColumn, error) {
	colName := column.Name
	// Setting parquet.Repetitions.Optional makes parquet interpret all columns as nullable.
	// When writing data, we will specify a definition level of 0 (null) or 1 (not null).
	// See https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
	// for more information regarding definition levels.
	defaultRepetitions := parquet.Repetitions.Optional

	// In the below switch, we define the column as a schema.Node. A node has a
	// physical and logic type. The logical types are included so the parquet file
	// can be read without any context from the writer. For example, a parquet reader
	// may encounter a column with physical type int64. Depending on the logical type,
	// it can interpret is as timestamp datum, int64 datum, etc.
	result := parquetColumn{}
	typ := column.Typ
	var err error
	switch typ.Family() {
	case types.BoolFamily:
		result.node = schema.NewBooleanNode(colName, defaultRepetitions, defaultParquetSchemaFieldID)
		result.node.LogicalType()
		result.enc = parquetBoolEncoder
		return result, nil
	case types.StringFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			defaultRepetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultParquetTypeLength, defaultParquetSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.enc = parquetStringEncoder
		return result, nil
	case types.IntFamily:
		// Note: integer datums are always signed: https://www.cockroachlabs.com/docs/stable/int.html
		if typ.Oid() == oid.T_int8 {
			result.node, err = schema.NewPrimitiveNodeLogical(colName,
				defaultRepetitions, schema.NewIntLogicalType(64, true),
				parquet.Types.Int64, defaultParquetTypeLength,
				defaultParquetSchemaFieldID)
			if err != nil {
				return result, err
			}
			result.enc = parquetInt64Encoder
			return result, nil
		}

		result.node = schema.NewInt32Node(colName, defaultRepetitions, defaultParquetSchemaFieldID)
		result.enc = parquetInt32Encoder
		return result, nil
	case types.DecimalFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			defaultRepetitions, schema.NewDecimalLogicalType(typ.Precision(),
				typ.Scale()), parquet.Types.ByteArray, defaultParquetTypeLength,
			defaultParquetSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.enc = parquetDecimalEncoder
		return result, nil
	case types.UuidFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			defaultRepetitions, schema.UUIDLogicalType{},
			parquet.Types.FixedLenByteArray, uuid.Size, defaultParquetSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.enc = parquetUUIDEncoder
		return result, nil
	case types.TimestampFamily:
		// Note that all timestamp datums are in UTC: https://www.cockroachlabs.com/docs/stable/timestamp.html
		// Encoding the typestamp as unixmicros in an int64 should be safe from overflow until year 294246.
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			defaultRepetitions, schema.NewTimestampLogicalType(true,
				schema.TimeUnitMicros), parquet.Types.Int64, defaultParquetTypeLength,
			defaultParquetSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.enc = parquetTimestampEncoder
		return result, nil

		// TODO(#99028): implement support for the remaining types.
		//	case types.CollatedStringFamily:
		//	case types.INetFamily:
		//	case types.JsonFamily:
		//	case types.FloatFamily:
		//	case types.BytesFamily:
		//	case types.BitFamily:
		//	case types.EnumFamily:
		//	case types.Box2DFamily:
		//	case types.GeographyFamily:
		//	case types.GeometryFamily:
		//	case types.DateFamily:
		//	case types.TimeFamily:
		//	case types.TimeTZFamily:
		//	case types.IntervalFamily:
		//	case types.TimestampTZFamily:
		//	case types.ArrayFamily:
	default:
		return result, errors.Errorf("parquet export does not support the %v type yet", typ.Family())
	}
}

var parquetBoolEncoder parquetEncodeFn = func(d tree.Datum) (interface{}, error) {
	return bool(*d.(*tree.DBool)), nil
}

var parquetBoolDecoder parquetDecodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.MakeDBool(tree.DBool(i.(bool))), nil
}

var parquetStringEncoder parquetEncodeFn = func(d tree.Datum) (interface{}, error) {
	return parquet.ByteArray(*d.(*tree.DString)), nil
}

var parquetStringDecoder parquetDecodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.NewDString(string(i.(parquet.ByteArray))), nil
}

var parquetInt64Encoder parquetEncodeFn = func(d tree.Datum) (interface{}, error) {
	return int64(*d.(*tree.DInt)), nil
}

var parquetInt64Decoder parquetDecodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(i.(int64))), nil
}

var parquetInt32Encoder parquetEncodeFn = func(d tree.Datum) (interface{}, error) {
	return int32(*d.(*tree.DInt)), nil
}

var parquetInt32Decoder parquetDecodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(i.(int32))), nil
}

var parquetDecimalEncoder parquetEncodeFn = func(d tree.Datum) (interface{}, error) {
	dec := d.(*tree.DDecimal).Decimal
	return parquet.ByteArray(dec.String()), nil
}

var parquetDecimalDecoder parquetDecodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.ParseDDecimal(string(i.(parquet.ByteArray)))
}

var parquetUUIDEncoder parquetEncodeFn = func(d tree.Datum) (interface{}, error) {
	return parquet.FixedLenByteArray((*d.(*tree.DUuid)).UUID.GetBytes()), nil
}

var parquetUUIDDecoder parquetDecodeFn = func(i interface{}) (tree.Datum, error) {
	uid, err := uuid.FromBytes(i.(parquet.FixedLenByteArray))
	if err != nil {
		return nil, err
	}
	return tree.NewDUuid(tree.DUuid{UUID: uid}), nil
}

var parquetTimestampEncoder = func(d tree.Datum) (interface{}, error) {
	ts := d.(*tree.DTimestamp).UnixMicro()
	return ts, nil
}

var parquetTimestampDecoder parquetDecodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.MakeDTimestamp(timeutil.FromUnixMicros(i.(int64)), time.Microsecond)
}
