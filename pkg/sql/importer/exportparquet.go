// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/lib/pq/oid"
)

const exportParquetFilePatternDefault = exportFilePatternPart + ".parquet"

// parquetExporter is used to augment the parquetWriter, encapsulating the internals to make
// exporting oblivious for the consumers.
type parquetExporter struct {
	buf            *bytes.Buffer
	parquetWriter  *goparquet.FileWriter
	schema         *parquetschema.SchemaDefinition
	parquetColumns []ParquetColumn
	compression    roachpb.IOFileFormat_Compression
}

// Write appends a record to a parquet file.
func (c *parquetExporter) Write(record map[string]interface{}) error {
	return c.parquetWriter.AddData(record)
}

// Flush is merely a placeholder to mimic the CSV Exporter (we may add their methods
// to an interface in the near future). All flushing is done by parquetExporter.Close().
func (c *parquetExporter) Flush() error {
	return nil
}

// Close flushes all records to parquetExporter.buf and closes the parquet writer.
func (c *parquetExporter) Close() error {
	return c.parquetWriter.Close()
}

// Bytes results in the slice of bytes.
func (c *parquetExporter) Bytes() []byte {
	return c.buf.Bytes()
}

func (c *parquetExporter) ResetBuffer() {
	c.buf.Reset()
	c.parquetWriter = c.buildFileWriter()
}

// Len returns length of the buffer with content.
func (c *parquetExporter) Len() int {
	return c.buf.Len()
}

func (c *parquetExporter) FileName(spec execinfrapb.ExportSpec, part string) string {
	pattern := exportParquetFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)
	suffix := ""
	switch spec.Format.Compression {
	case roachpb.IOFileFormat_Gzip:
		suffix = ".gz"
	case roachpb.IOFileFormat_Snappy:
		suffix = ".snappy"
	}
	fileName += suffix
	return fileName
}

func (c *parquetExporter) buildFileWriter() *goparquet.FileWriter {
	var parquetCompression parquet.CompressionCodec
	switch c.compression {
	case roachpb.IOFileFormat_Gzip:
		parquetCompression = parquet.CompressionCodec_GZIP
	case roachpb.IOFileFormat_Snappy:
		parquetCompression = parquet.CompressionCodec_SNAPPY
	default:
		parquetCompression = parquet.CompressionCodec_UNCOMPRESSED
	}
	pw := goparquet.NewFileWriter(c.buf,
		goparquet.WithCompressionCodec(parquetCompression),
		goparquet.WithSchemaDefinition(c.schema),
	)
	return pw
}

// newParquetExporter creates a new parquet file writer, defines the parquet
// file schema, and initializes a new parquetExporter.
func newParquetExporter(sp execinfrapb.ExportSpec, typs []*types.T) (*parquetExporter, error) {
	var exporter *parquetExporter

	buf := bytes.NewBuffer([]byte{})
	parquetColumns, err := newParquetColumns(typs, sp)
	if err != nil {
		return nil, err
	}
	schema := newParquetSchema(parquetColumns)

	exporter = &parquetExporter{
		buf:            buf,
		schema:         schema,
		parquetColumns: parquetColumns,
		compression:    sp.Format.Compression,
	}
	return exporter, nil
}

// ParquetColumn contains the relevant data to map a crdb table column to a parquet table column.
type ParquetColumn struct {
	name     string
	crbdType *types.T

	// definition contains all relevant information around the parquet type for the table column
	definition *parquetschema.ColumnDefinition

	// encodeFn converts crdb table column value to a native go type that the
	// parquet vendor can ingest.
	encodeFn func(datum tree.Datum) (interface{}, error)

	// DecodeFn converts a native go type, created by the parquet vendor while
	// reading a parquet file, into a crdb column value
	DecodeFn func(interface{}) (tree.Datum, error)
}

// newParquetColumns creates a list of parquet columns, given the input relation's column types.
func newParquetColumns(typs []*types.T, sp execinfrapb.ExportSpec) ([]ParquetColumn, error) {
	parquetColumns := make([]ParquetColumn, len(typs))
	for i := 0; i < len(typs); i++ {
		parquetCol, err := NewParquetColumn(typs[i], sp.ColNames[i], sp.Format.Parquet.ColNullability[i])
		if err != nil {
			return nil, err
		}
		parquetColumns[i] = parquetCol
	}
	return parquetColumns, nil
}

// populateLogicalStringCol is a helper function for populating parquet schema
// info for a column that will get encoded as a string
func populateLogicalStringCol(schemaEl *parquet.SchemaElement) {
	schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	schemaEl.LogicalType = parquet.NewLogicalType()
	schemaEl.LogicalType.STRING = parquet.NewStringType()
	schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
}

// roundtripStringer pretty prints the datum's value as string, allowing the
// parser in certain decoders to work.
func roundtripStringer(d tree.Datum) string {
	fmtCtx := tree.NewFmtCtx(tree.FmtBareStrings)
	d.Format(fmtCtx)
	return fmtCtx.CloseAndGetString()
}

// NewParquetColumn populates a ParquetColumn by finding the right parquet type
// and defining the encoder and decoder.
func NewParquetColumn(typ *types.T, name string, nullable bool) (ParquetColumn, error) {
	col := ParquetColumn{}
	col.definition = new(parquetschema.ColumnDefinition)
	col.definition.SchemaElement = parquet.NewSchemaElement()
	col.name = name
	col.crbdType = typ

	schemaEl := col.definition.SchemaElement

	/*
			The type of a parquet column is either a group (i.e.
		  an array in crdb) or a primitive type (e.g., int, float, boolean,
		  string) and the repetition can be one of the three following cases:

		  - required: exactly one occurrence (i.e. the column value is a scalar, and
		  cannot have null values). A column is set to required if the user
		  specified the CRDB column as NOT NULL.
		  - optional: 0 or 1 occurrence (i.e. same as above, but can have values)
		  - repeated: 0 or more occurrences (the column value will be an array. A
				value within the array will have its own repetition type)

			See this blog post for more on parquet type specification:
			https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
	*/
	schemaEl.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	if !nullable {
		schemaEl.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
	}
	schemaEl.Name = col.name

	// MB figured out the low level properties of the encoding by running the goland debugger on
	// the following vendor example:
	// https://github.com/fraugster/parquet-go/blob/master/examples/write-low-level/main.go
	switch typ.Family() {
	case types.BoolFamily:
		schemaEl.Type = parquet.TypePtr(parquet.Type_BOOLEAN)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return bool(*d.(*tree.DBool)), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.MakeDBool(tree.DBool(x.(bool))), nil
		}

	case types.StringFamily:
		populateLogicalStringCol(schemaEl)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(*d.(*tree.DString)), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDString(string(x.([]byte))), nil
		}
	case types.CollatedStringFamily:
		populateLogicalStringCol(schemaEl)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DCollatedString).Contents), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDCollatedString(string(x.([]byte)), typ.Locale(), &tree.CollationEnvironment{})
		}
	case types.INetFamily:
		populateLogicalStringCol(schemaEl)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DIPAddr).IPAddr.String()), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.ParseDIPAddrFromINetString(string(x.([]byte)))
		}
	case types.JsonFamily:
		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		schemaEl.LogicalType = parquet.NewLogicalType()
		schemaEl.LogicalType.JSON = parquet.NewJsonType()
		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_JSON)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DJSON).JSON.String()), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			jsonStr := string(x.([]byte))
			return tree.ParseDJSON(jsonStr)
		}

	case types.IntFamily:
		schemaEl.LogicalType = parquet.NewLogicalType()
		schemaEl.LogicalType.INTEGER = parquet.NewIntType()
		schemaEl.LogicalType.INTEGER.IsSigned = true
		if typ.Oid() == oid.T_int8 {
			schemaEl.Type = parquet.TypePtr(parquet.Type_INT64)
			schemaEl.LogicalType.INTEGER.BitWidth = int8(64)
			schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
			col.encodeFn = func(d tree.Datum) (interface{}, error) {
				return int64(*d.(*tree.DInt)), nil
			}
			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(x.(int64))), nil
			}
		} else {
			schemaEl.Type = parquet.TypePtr(parquet.Type_INT32)
			schemaEl.LogicalType.INTEGER.BitWidth = int8(32)
			schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
			col.encodeFn = func(d tree.Datum) (interface{}, error) {
				return int32(*d.(*tree.DInt)), nil
			}
			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(x.(int32))), nil
			}
		}
	case types.FloatFamily:
		if typ.Oid() == oid.T_float4 {
			schemaEl.Type = parquet.TypePtr(parquet.Type_FLOAT)
			col.encodeFn = func(d tree.Datum) (interface{}, error) {
				h := float32(*d.(*tree.DFloat))
				return h, nil
			}
			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
				// must convert float32 to string before converting to float64 (the
				// underlying data type of a tree.Dfloat) because directly converting
				// a float32 to a float64 will add on trailing significant digits,
				// causing the round trip tests to fail.
				hS := fmt.Sprintf("%f", x.(float32))
				return tree.ParseDFloat(hS)
			}
		} else {
			schemaEl.Type = parquet.TypePtr(parquet.Type_DOUBLE)
			col.encodeFn = func(d tree.Datum) (interface{}, error) {
				return float64(*d.(*tree.DFloat)), nil
			}
			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(x.(float64))), nil
			}
		}
	case types.DecimalFamily:
		// TODO (MB): Investigate if the parquet vendor should enforce precision and
		// scale requirements. In a toy example, the parquet vendor was able to
		// write/read roundtrip the string "3235.5432" as a Decimal with Scale = 1,
		// Precision = 1, even though this decimal has a larger scale and precision.
		// I guess it's the responsibility of CRDB to enforce the Scale and
		// Precision conditions, and for the parquet vendor to NOT lose data, even if
		// the data doesn't follow the scale and precision conditions.

		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)

		schemaEl.LogicalType = parquet.NewLogicalType()
		schemaEl.LogicalType.DECIMAL = parquet.NewDecimalType()

		schemaEl.LogicalType.DECIMAL.Scale = typ.Scale()
		schemaEl.LogicalType.DECIMAL.Precision = typ.Precision()
		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)

		// According to PostgresSQL docs, scale or precision of 0 implies max
		// precision and scale. I assume this is what CRDB does, but this isn't
		// explicit in the docs https://www.postgresql.org/docs/10/datatype-numeric.html
		if typ.Scale() == 0 {
			schemaEl.LogicalType.DECIMAL.Scale = math.MaxInt32
		}
		if typ.Precision() == 0 {
			schemaEl.LogicalType.DECIMAL.Precision = math.MaxInt32
		}

		schemaEl.Scale = &schemaEl.LogicalType.DECIMAL.Scale
		schemaEl.Precision = &schemaEl.LogicalType.DECIMAL.Precision

		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			dec := d.(*tree.DDecimal).Decimal
			return []byte(dec.String()), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			// TODO (MB): investigative if crdb should gather decimal metadata from
			// parquet file during IMPORT PARQUET.
			return tree.ParseDDecimal(string(x.([]byte)))
		}
	case types.UuidFamily:
		// Vendor parquet documentation suggests that UUID maps to the [16]byte go type
		// https://github.com/fraugster/parquet-go#supported-logical-types
		schemaEl.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
		byteArraySize := int32(uuid.Size)
		schemaEl.TypeLength = &byteArraySize
		schemaEl.LogicalType = parquet.NewLogicalType()
		schemaEl.LogicalType.UUID = parquet.NewUUIDType()
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return d.(*tree.DUuid).UUID.GetBytes(), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.ParseDUuidFromBytes(x.([]byte))
		}
	case types.BytesFamily:
		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(*d.(*tree.DBytes)), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.NewDBytes(tree.DBytes(x.([]byte))), nil
		}
	case types.BitFamily:
		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			// TODO(MB): investigate whether bit arrays should be encoded as an array of longs,
			// like in avro changefeeds
			baS := roundtripStringer(d.(*tree.DBitArray))
			return []byte(baS), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			ba, err := bitarray.Parse(string(x.([]byte)))
			return &tree.DBitArray{BitArray: ba}, err
		}
	case types.EnumFamily:
		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		schemaEl.LogicalType = parquet.NewLogicalType()
		schemaEl.LogicalType.ENUM = parquet.NewEnumType()
		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DEnum).LogicalRep), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.MakeDEnumFromLogicalRepresentation(typ, string(x.([]byte)))
		}
	case types.Box2DFamily:
		populateLogicalStringCol(schemaEl)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DBox2D).CartesianBoundingBox.Repr()), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			b, err := geo.ParseCartesianBoundingBox(string(x.([]byte)))
			if err != nil {
				return nil, err
			}
			return tree.NewDBox2D(b), nil
		}
	case types.GeographyFamily:
		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DGeography).EWKB()), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			g, err := geo.ParseGeographyFromEWKB(geopb.EWKB(x.([]byte)))
			if err != nil {
				return nil, err
			}
			return &tree.DGeography{Geography: g}, nil
		}
	case types.GeometryFamily:
		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DGeometry).EWKB()), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			g, err := geo.ParseGeometryFromEWKBUnsafe(geopb.EWKB(x.([]byte)))
			if err != nil {
				return nil, err
			}
			return &tree.DGeometry{Geometry: g}, nil
		}
	case types.DateFamily:
		// Even though the parquet vendor supports Dates, we export Dates as strings
		// because the vendor only supports encoding them as an int32, the Days
		// since the Unix epoch, which according CRDB's `date.UnixEpochDays( )` (in
		// pgdate package) is vulnerable to overflow.
		populateLogicalStringCol(schemaEl)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			date := d.(*tree.DDate)
			ds := roundtripStringer(date)
			return []byte(ds), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			dStr := string(x.([]byte))
			d, dependCtx, err := tree.ParseDDate(nil, dStr)
			if dependCtx {
				return nil, errors.Newf("decoding date %s failed. depends on context", string(x.([]byte)))
			}
			return d, err
		}
	case types.TimeFamily:
		schemaEl.Type = parquet.TypePtr(parquet.Type_INT64)
		schemaEl.LogicalType = parquet.NewLogicalType()
		schemaEl.LogicalType.TIME = parquet.NewTimeType()
		t := parquet.NewTimeUnit()
		t.MICROS = parquet.NewMicroSeconds()
		schemaEl.LogicalType.TIME.Unit = t
		schemaEl.LogicalType.TIME.IsAdjustedToUTC = true // per crdb docs
		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)

		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			// Time of day is stored in microseconds since midnight,
			// which is also how parquet stores time
			time := d.(*tree.DTime)
			m := int64(*time)
			return m, nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.MakeDTime(timeofday.TimeOfDay(x.(int64))), nil
		}
	case types.TimeTZFamily:
		// The parquet vendor does not support an efficient encoding of TimeTZ
		// (i.e. a datetime field and a timezone field), so we must fall back to
		// encoding the whole TimeTZ as a string.
		populateLogicalStringCol(schemaEl)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DTimeTZ).TimeTZ.String()), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			d, dependsOnCtx, err := tree.ParseDTimeTZ(nil, string(x.([]byte)), time.Microsecond)
			if dependsOnCtx {
				return nil, errors.New("parsed time depends on context")
			}
			return d, err
		}
	case types.IntervalFamily:
		// The parquet vendor only supports intervals as a parquet converted type,
		// but converted types have been deprecated in the Apache Parquet format.
		// https://github.com/fraugster/parquet-go#supported-converted-types
		populateLogicalStringCol(schemaEl)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(d.(*tree.DInterval).ValueAsISO8601String()), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			return tree.ParseDInterval(duration.IntervalStyle_ISO_8601, string(x.([]byte)))
		}
	case types.TimestampFamily:
		// Didn't encode this as Microseconds since the unix epoch because of threat
		// of overflow. See comment associated with time.Time.UnixMicro().
		populateLogicalStringCol(schemaEl)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			ts := roundtripStringer(d.(*tree.DTimestamp))
			return []byte(ts), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			// return tree.MakeDTimestamp(time.UnixMicro(x.(int64)).UTC(), time.Microsecond)
			dtStr := string(x.([]byte))
			d, dependsOnCtx, err := tree.ParseDTimestamp(nil, dtStr, time.Microsecond)
			if dependsOnCtx {
				return nil, errors.New("TimestampTZ depends on context")
			}
			if err != nil {
				return nil, err
			}
			// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent,
			// allowing roundtrip tests to pass.
			d.Time = d.Time.UTC()
			return d, nil
		}

	case types.TimestampTZFamily:
		// Didn't encode this as Microseconds since the unix epoch because of threat
		// of overflow. See comment associated with time.Time.UnixMicro().
		populateLogicalStringCol(schemaEl)

		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			ts := roundtripStringer(d.(*tree.DTimestampTZ))
			return []byte(ts), nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			dtStr := string(x.([]byte))
			d, dependsOnCtx, err := tree.ParseDTimestampTZ(nil, dtStr, time.Microsecond)
			if dependsOnCtx {
				return nil, errors.New("TimestampTZ depends on context")
			}
			if err != nil {
				return nil, err
			}
			// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent,
			// allowing tests to pass.
			d.Time = d.Time.UTC()
			return d, nil
		}
	case types.ArrayFamily:

		// Define a list such that the parquet schema in json is:
		/*
			required group colName (LIST){ // parent
				repeated group list { // child
					required colType element; //grandChild
				}
			}
		*/
		// MB figured this out by running toy examples of the fraugster-parquet
		// vendor repository for added context, checkout this issue
		// https://github.com/fraugster/parquet-go/issues/18

		// First, define the grandChild definition, the schema for the array value.
		grandChild, err := NewParquetColumn(typ.ArrayContents(), "element", true)
		if err != nil {
			return col, err
		}
		// Next define the child definition, required by fraugster-parquet vendor library. Again,
		// there's little documentation on this. MB figured this out using a debugger.
		child := &parquetschema.ColumnDefinition{}
		child.SchemaElement = parquet.NewSchemaElement()
		child.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.
			FieldRepetitionType_REPEATED)
		child.SchemaElement.Name = "list"
		child.Children = []*parquetschema.ColumnDefinition{grandChild.definition}
		ngc := int32(len(child.Children))
		child.SchemaElement.NumChildren = &ngc

		// Finally, define the parent definition.
		col.definition.Children = []*parquetschema.ColumnDefinition{child}
		nc := int32(len(col.definition.Children))
		child.SchemaElement.NumChildren = &nc
		schemaEl.LogicalType = parquet.NewLogicalType()
		schemaEl.LogicalType.LIST = parquet.NewListType()
		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_LIST)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			datumArr := d.(*tree.DArray)
			els := make([]map[string]interface{}, datumArr.Len())
			for i, elt := range datumArr.Array {
				var el interface{}
				if elt.ResolvedType().Family() == types.UnknownFamily {
					// skip encoding the datum
				} else {
					el, err = grandChild.encodeFn(elt)
					if err != nil {
						return col, err
					}
				}
				els[i] = map[string]interface{}{"element": el}
			}
			encEl := map[string]interface{}{"list": els}
			return encEl, nil
		}
		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
			// The parquet vendor decodes an array into the native go type
			// map[string]interface{}, and the values of the array are stored in the
			// "list" key of the map. "list" maps to an array of maps
			// []map[string]interface{}, where the ith map contains a single key value
			// pair. The key is always "element" and the value is the ith value in the
			// array.

			// If the array of maps only contains an empty map, the array is empty. This
			// occurs IFF "element" is not in the map.

			// NB: there's a bug in the fraugster-parquet vendor library around
			// reading an ARRAY[NULL],
			// https://github.com/fraugster/parquet-go/issues/60 I already verified
			// that the vendor's parquet writer can write arrays with null values just
			// fine, so EXPORT PARQUET is bug free; however this roundtrip test would
			// fail. Ideally, once the bug gets fixed, ARRAY[NULL] will get read as
			// the kvp {"element":interface{}} while ARRAY[] will continue to get read
			// as an empty map.
			datumArr := tree.NewDArray(typ.ArrayContents())
			datumArr.Array = []tree.Datum{}

			intermediate := x.(map[string]interface{})
			vals := intermediate["list"].([]map[string]interface{})
			if _, nonEmpty := vals[0]["element"]; !nonEmpty {
				if len(vals) > 1 {
					return nil, errors.New("array is empty, it shouldn't have a length greater than 1")
				}
			} else {
				for _, elMap := range vals {
					itemDatum, err := grandChild.DecodeFn(elMap["element"])
					if err != nil {
						return nil, err
					}
					err = datumArr.Append(itemDatum)
					if err != nil {
						return nil, err
					}
				}
			}
			return datumArr, nil
		}
	default:
		return col, errors.Errorf("parquet export does not support the %v type yet", typ.Family())
	}

	return col, nil
}

// newParquetSchema creates the schema for the parquet file,
// see example schema:
//     https://github.com/fraugster/parquet-go/issues/18#issuecomment-946013210
// see docs here:
//     https://pkg.go.dev/github.com/fraugster/parquet-go/parquetschema#SchemaDefinition
func newParquetSchema(parquetFields []ParquetColumn) *parquetschema.SchemaDefinition {
	schemaDefinition := new(parquetschema.SchemaDefinition)
	schemaDefinition.RootColumn = new(parquetschema.ColumnDefinition)
	schemaDefinition.RootColumn.SchemaElement = parquet.NewSchemaElement()

	for i := 0; i < len(parquetFields); i++ {
		schemaDefinition.RootColumn.Children = append(schemaDefinition.RootColumn.Children,
			parquetFields[i].definition)
		schemaDefinition.RootColumn.SchemaElement.Name = "root"
	}
	return schemaDefinition
}

func newParquetWriterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ExportSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	c := &parquetWriterProcessor{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}
	semaCtx := tree.MakeSemaContext()
	if err := c.out.Init(&execinfrapb.PostProcessSpec{}, c.OutputTypes(), &semaCtx, flowCtx.NewEvalCtx()); err != nil {
		return nil, err
	}
	return c, nil
}

type parquetWriterProcessor struct {
	flowCtx     *execinfra.FlowCtx
	processorID int32
	spec        execinfrapb.ExportSpec
	input       execinfra.RowSource
	out         execinfra.ProcOutputHelper
	output      execinfra.RowReceiver
}

var _ execinfra.Processor = &parquetWriterProcessor{}

func (sp *parquetWriterProcessor) OutputTypes() []*types.T {
	res := make([]*types.T, len(colinfo.ExportColumns))
	for i := range res {
		res[i] = colinfo.ExportColumns[i].Typ
	}
	return res
}

// MustBeStreaming currently never gets called by the parquetWriterProcessor as
// the function only applies to implementation.
func (sp *parquetWriterProcessor) MustBeStreaming() bool {
	return false
}

func (sp *parquetWriterProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "parquetWriter")
	defer span.Finish()

	instanceID := sp.flowCtx.EvalCtx.NodeID.SQLInstanceID()
	uniqueID := builtins.GenerateUniqueInt(instanceID)

	err := func() error {
		typs := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := execinfra.MakeNoMetadataRowSource(sp.input, sp.output)
		alloc := &tree.DatumAlloc{}

		exporter, err := newParquetExporter(sp.spec, typs)
		if err != nil {
			return err
		}

		parquetRow := make(map[string]interface{}, len(typs))
		chunk := 0
		done := false
		for {
			var rows int64
			exporter.ResetBuffer()
			for {
				// If the bytes.Buffer sink exceeds the target size of a Parquet file, we
				// flush before exporting any additional rows.
				if int64(exporter.buf.Len()) >= sp.spec.ChunkSize {
					break
				}
				if sp.spec.ChunkRows > 0 && rows >= sp.spec.ChunkRows {
					break
				}
				row, err := input.NextRow()
				if err != nil {
					return err
				}
				if row == nil {
					done = true
					break
				}
				rows++

				for i, ed := range row {
					if ed.IsNull() {
						parquetRow[exporter.parquetColumns[i].name] = nil
					} else {
						if err := ed.EnsureDecoded(typs[i], alloc); err != nil {
							return err
						}
						edNative, err := exporter.parquetColumns[i].encodeFn(ed.Datum)
						if err != nil {
							return err
						}
						parquetRow[exporter.parquetColumns[i].name] = edNative
					}
				}
				if err := exporter.Write(parquetRow); err != nil {
					return err
				}
			}
			if rows < 1 {
				break
			}
			if err := exporter.Flush(); err != nil {
				return errors.Wrap(err, "failed to flush parquet exporter")
			}

			// Close exporter to ensure buffer and any compression footer is flushed.
			err = exporter.Close()
			if err != nil {
				return errors.Wrapf(err, "failed to close exporting exporter")
			}

			conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User())
			if err != nil {
				return err
			}
			es, err := sp.flowCtx.Cfg.ExternalStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()

			part := fmt.Sprintf("n%d.%d", uniqueID, chunk)
			chunk++
			filename := exporter.FileName(sp.spec, part)

			size := exporter.Len()

			if err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(exporter.Bytes())); err != nil {
				return err
			}
			res := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(
					types.String,
					tree.NewDString(filename),
				),
				rowenc.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(rows)),
				),
				rowenc.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(size)),
				),
			}

			cs, err := sp.out.EmitRow(ctx, res, sp.output)
			if err != nil {
				return err
			}
			if cs != execinfra.NeedMoreRows {
				// TODO(dt): presumably this is because our recv already closed due to
				// another error... so do we really need another one?
				return errors.New("unexpected closure of consumer")
			}
			if done {
				break
			}
		}

		return nil
	}()

	// TODO(dt): pick up tracing info in trailing meta
	execinfra.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func init() {
	rowexec.NewParquetWriterProcessor = newParquetWriterProcessor
}
