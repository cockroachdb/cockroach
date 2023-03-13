package changefeedccl

import (
	"io"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// A schema field is an internal identifier for schema nodes used by the parquet library.
// A value of -1 will let the library auto-assign values. This does not affect reading
// or writing parquet files.
const defaultParquetSchemaFieldID = int32(-1)

// The parquet library utilizes a type length of -1 for all types
// except for the type parquet.FixedLenByteArray, in which case the type
// length is the length of the array. See comment on (*schema.PrimitiveNode).TypeLength()
const defaultParquetTypeLength = -1

// encodeFn encodes a tree.datum to have a parquet datum.
type encodeFn func(datum tree.Datum) (interface{}, error)

// decodeFn decodes a parquet datum to a tree.Datum.
type decodeFn func(interface{}) (tree.Datum, error)

// A parquetColumn stores the parquet schema node and encoder for a column.
// See newParquetSchema for more info.
type parquetColumn struct {
	node schema.Node
	enc  encodeFn
	dec  decodeFn
}

// A ParquetSchemaDefinition stores an array of parquetColumn along with
// a parquet schema. The order of columns in cols will match the order of columns in
// schema. See newParquetSchema for more info.
type parquetSchemaDefinition struct {
	cols   []parquetColumn
	schema *schema.Schema
}

// newParquetSchema generates a parquetSchemaDefinition with the following traits:
//
// The columns are in the order they appear in the cdcevent.Row (specifically,
// calling ForEachColumn on the row).
//
// The schema is a root node with terminal children nodes which represent primitive types
// such as int or bool. The column schemas can be traversed using schema.Column(i).
// The children are indexed from [0, len(cols) + 1). The last child is an artificial column
// inserted by the writer which stores the event type.
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

	// Setting fieldID = -1
	eventTypeField, err := schema.NewPrimitiveNodeLogical(parquetCrdbEventTypeColName, parquet.Repetitions.Optional, schema.StringLogicalType{}, parquet.Types.ByteArray, defaultParquetTypeLength, defaultParquetSchemaFieldID)
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
	groupNode, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, defaultParquetSchemaFieldID)
	if err != nil {
		return nil, err
	}
	return &parquetSchemaDefinition{
		cols:   cols,
		schema: schema.NewSchema(groupNode),
	}, nil
}

// A ParquetWriter writes cdcevent.Row data into an io.Writer sink.
type ParquetWriter struct {
	sch    *parquetSchemaDefinition
	writer *file.Writer

	writerState struct {
		currentRowGroupSize   int
		addedCurrentRowGroup  bool
		currentRowGroupWriter file.BufferedRowGroupWriter
		totalBytes            int64
	}
}

func (w *ParquetWriter) getParquetSchemaDefinition() *parquetSchemaDefinition {
	return w.getParquetSchemaDefinition()
}

// NewCDCParquetWriterFromRow constructs a new parquet writer which outputs to
// the given sink. This function interprets the schema from the supplied row,
// but does not write it. Rows passed to ParquetWriter.AddData should have the
// same schema as the row passed to this function.
func NewCDCParquetWriterFromRow(row cdcevent.Row, sink io.Writer) (*ParquetWriter, error) {
	// TODO(#99028): support compression schemes
	// TODO(#99028): add options for more stability / performance (allocator, batch size, page size)
	sch, err := newParquetSchema(row)
	if err != nil {
		return nil, err
	}
	opts := []parquet.WriterProperty{parquet.WithCreatedBy("cockroachdb")}
	props := parquet.NewWriterProperties(opts...)
	writer := file.NewParquetWriter(sink, sch.schema.Root(), file.WithWriterProps(props))
	return &ParquetWriter{
		sch:    sch,
		writer: writer,
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
	case *file.Float32ColumnChunkWriter:
		return w.WriteBatch([]float32{value.(float32)}, nonNilDefLevel, nil)
	case *file.Float64ColumnChunkWriter:
		return w.WriteBatch([]float64{value.(float64)}, nonNilDefLevel, nil)
	case *file.Int96ColumnChunkWriter:
		return w.WriteBatch([]parquet.Int96{value.(parquet.Int96)}, nonNilDefLevel, nil)
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
	case *file.Float32ColumnChunkWriter:
		return w.WriteBatch([]float32{}, nilDefLevel, nil)
	case *file.Float64ColumnChunkWriter:
		return w.WriteBatch([]float64{}, nilDefLevel, nil)
	case *file.Int96ColumnChunkWriter:
		return w.WriteBatch([]parquet.Int96{}, nilDefLevel, nil)
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
	cw, err := w.writerState.currentRowGroupWriter.Column(colIdx)
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

// AddData writes the updatedRow .
func (w *ParquetWriter) AddData(updatedRow cdcevent.Row, prevRow cdcevent.Row) error {
	if !w.writerState.addedCurrentRowGroup {
		w.writerState.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
		w.writerState.addedCurrentRowGroup = true
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
func (w *ParquetWriter) Close() error {
	// TODO: the writer already manages the current writer. Maybe we can remove lines tha treference it.
	if err := w.writerState.currentRowGroupWriter.Close(); err != nil {
		return err
	}
	return w.writer.Close()
}

// CurrentSize returns the approximate amount which has been written to the sink.
func (w *ParquetWriter) CurrentSize() int64 {
	if w.writerState.currentRowGroupWriter == nil {
		return 0
	}

	//return w.writerState.totalBytes + w.writerState.currentRowGroupWriter.TotalBytesWritten()
	return w.writerState.currentRowGroupWriter.TotalBytesWritten()
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

	result := parquetColumn{}
	typ := column.Typ
	var err error
	switch typ.Family() {
	case types.BoolFamily:
		result.node = schema.NewBooleanNode(colName, defaultRepetitions, defaultParquetSchemaFieldID)
		result.node.LogicalType()
		result.enc = parquetBoolEncoder
		result.dec = parquetBoolDecoder
		return result, nil
	case types.StringFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName, defaultRepetitions, schema.StringLogicalType{}, parquet.Types.ByteArray, defaultParquetTypeLength, defaultParquetSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.enc = parquetStringEncoder
		result.dec = parquetStringDecoder
		return result, nil
	case types.IntFamily:
		// Note: integer datums are always signed: https://www.cockroachlabs.com/docs/stable/int.html
		if typ.Oid() == oid.T_int8 {
			result.node, err = schema.NewPrimitiveNodeLogical(colName, defaultRepetitions, schema.NewIntLogicalType(64, true), parquet.Types.Int64, defaultParquetTypeLength, defaultParquetSchemaFieldID)
			if err != nil {
				return result, err
			}
			result.enc = parquetInt64Encoder
			result.dec = parquetInt64Decoder
			return result, nil
		}

		result.node = schema.NewInt32Node(colName, defaultRepetitions, defaultParquetSchemaFieldID)
		result.enc = parquetInt32Encoder
		result.dec = parquetInt32Decoder
		return result, nil
	case types.DecimalFamily:
		schema.NewDecimalLogicalType(typ.Precision(), typ.Scale())
		result.node, err = schema.NewPrimitiveNodeLogical(colName, defaultRepetitions, schema.NewDecimalLogicalType(typ.Precision(), typ.Scale()), parquet.Types.ByteArray, defaultParquetTypeLength, defaultParquetSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.enc = parquetDecimalEncoder
		return result, nil
	case types.UuidFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName, defaultRepetitions, schema.UUIDLogicalType{}, parquet.Types.FixedLenByteArray, uuid.Size, defaultParquetSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.enc = parquetUUIDEncoder
		result.dec = parquetUUIDDecoder
		return result, nil
	case types.TimestampFamily:
		// Encoding the typestamp as unixmicros in an int64 should be safe from overflow until year 294246.
		// Note that all timestamp datums are in UTC: https://www.cockroachlabs.com/docs/stable/timestamp.html
		result.node, err = schema.NewPrimitiveNodeLogical(colName, defaultRepetitions, schema.NewTimestampLogicalType(true, schema.TimeUnitMicros), parquet.Types.Int64, defaultParquetTypeLength, defaultParquetSchemaFieldID)
		result.enc = parquetTimestampEncoder
		result.dec = parquetTimestampDecoder
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

var parquetBoolEncoder encodeFn = func(d tree.Datum) (interface{}, error) {
	return bool(*d.(*tree.DBool)), nil
}

var parquetBoolDecoder decodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.MakeDBool(tree.DBool(i.(bool))), nil
}

var parquetStringEncoder encodeFn = func(d tree.Datum) (interface{}, error) {
	return parquet.ByteArray(*d.(*tree.DString)), nil
}

var parquetStringDecoder decodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.NewDString(string(i.(parquet.ByteArray))), nil
}

var parquetInt64Encoder encodeFn = func(d tree.Datum) (interface{}, error) {
	return int64(*d.(*tree.DInt)), nil
}

var parquetInt64Decoder decodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(i.(int64))), nil
}

var parquetInt32Encoder encodeFn = func(d tree.Datum) (interface{}, error) {
	return int32(*d.(*tree.DInt)), nil
}

var parquetInt32Decoder decodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(i.(int32))), nil
}

var parquetDecimalEncoder encodeFn = func(d tree.Datum) (interface{}, error) {
	dec := d.(*tree.DDecimal).Decimal
	return parquet.ByteArray(dec.String()), nil
}

var parquetDecimalDecoder decodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.ParseDDecimal(string(i.(parquet.ByteArray)))
}

var parquetUUIDEncoder encodeFn = func(d tree.Datum) (interface{}, error) {
	return parquet.FixedLenByteArray((*d.(*tree.DUuid)).UUID.GetBytes()), nil
}

var parquetUUIDDecoder decodeFn = func(i interface{}) (tree.Datum, error) {
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

var parquetTimestampDecoder decodeFn = func(i interface{}) (tree.Datum, error) {
	return tree.MakeDTimestamp(timeutil.FromUnixMicros(i.(int64)), time.Microsecond)
}
