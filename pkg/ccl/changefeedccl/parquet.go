package changefeedccl

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	pqexporter "github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

const maxRowGroupSize = 512

type encodeFn func(datum tree.Datum) (interface{}, error)

type ParquetColumn struct {
	node *schema.PrimitiveNode
	enc  encodeFn
}

type ParquetWriter struct {
	sch    *ParquetSchemaDefinition
	writer *file.Writer

	writerState struct {
		currentRowGroupSize   int
		currentRowGroupWriter file.BufferedRowGroupWriter
	}
}

func NewParquetWriter(sch *ParquetSchemaDefinition, sink io.Writer) *ParquetWriter {
	// TODO add options - compression, allocator, encoding, batch size, parquet version, data page version, page size (limit before flushing)
	opts := []parquet.WriterProperty{parquet.WithCreatedBy("cockroachdb")}
	props := parquet.NewWriterProperties(opts...)
	writer := file.NewParquetWriter(sink, sch.sch.Root(), file.WithWriterProps(props))
	return &ParquetWriter{
		sch:    sch,
		writer: writer,
	}
}

// TODO deflevels and reflevels
func writeColBatch(colWriter file.ColumnChunkWriter, vals interface{}) (int64, error) {
	switch w := colWriter.(type) {
	case *file.Int32ColumnChunkWriter:
		return w.WriteBatch([]int32{vals.(int32)}, nil, nil)
	case *file.Int64ColumnChunkWriter:
		return w.WriteBatch([]int64{vals.(int64)}, nil, nil)
	case *file.Float32ColumnChunkWriter:
		return w.WriteBatch([]float32{vals.(float32)}, nil, nil)
	case *file.Float64ColumnChunkWriter:
		return w.WriteBatch([]float64{vals.(float64)}, nil, nil)
	case *file.Int96ColumnChunkWriter:
		return w.WriteBatch([]parquet.Int96{vals.(parquet.Int96)}, nil, nil)
	case *file.ByteArrayColumnChunkWriter:
		return w.WriteBatch([]parquet.ByteArray{vals.(parquet.ByteArray)}, nil, nil)
	case *file.BooleanColumnChunkWriter:
		return w.WriteBatch([]bool{vals.(bool)}, nil, nil)
	case *file.FixedLenByteArrayColumnChunkWriter:
		return w.WriteBatch([]parquet.FixedLenByteArray{vals.(parquet.FixedLenByteArray)}, nil, nil)
	default:
		panic("unimplemented")
	}
}

// TODO: there may be a more intelligent way to do this. Parquet supports writing a batch of column values at a time.
func (w *ParquetWriter) AddData(row cdcevent.Row) error {
	if w.writerState.currentRowGroupWriter == nil {
		w.writerState.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
	} else if w.writerState.currentRowGroupSize > maxRowGroupSize {
		// Implicitly close and flush the old row group.
		w.writerState.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
		w.writerState.currentRowGroupSize = 0
	}
	rgw := w.writerState.currentRowGroupWriter

	colIdx := 0
	if err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		cw, err := rgw.Column(colIdx)
		if err != nil {
			return err
		}
		// TODO: utilize the bytes written
		data, err := w.sch.cols[colIdx].enc(d)
		if err != nil {
			return err
		}
		_, err = writeColBatch(cw, data)
		if err != nil {
			return err
		}

		colIdx += 1
		return nil
	}); err != nil {
		return err
	}

	w.writerState.currentRowGroupSize += 1
	return nil
}

func (w *ParquetWriter) Close() error {
	if err := w.writerState.currentRowGroupWriter.Close(); err != nil {
		return err
	}
	return w.writer.Close()
}

type ParquetSchemaDefinition struct {
	cols []ParquetColumn
	// The schema will be a root node with child nodes equal to len(cols) + 1
	// The +1 is parquetCrdbEventTypeColName
	sch *schema.Schema
}

// parquet.Repetitions.Required - use for non nullable cols
// parquet.Repetitions.Optional - use for nullable cols
// parquet.Repetitions.Repeated - use for array types
func createColumnNode(
	colName string, repetitions parquet.Repetition, parquetTyp parquet.Type,
) (*schema.PrimitiveNode, error) {
	// TODO: fieldID and typeLength.
	return schema.NewPrimitiveNode(colName, repetitions, parquetTyp, -1, -1)

}

func NewParquetSchema(row cdcevent.Row) (*ParquetSchemaDefinition, error) {
	fields := make([]schema.Node, 0)
	cols := make([]ParquetColumn, 0)

	// Add datum cols.
	err := row.ForEachColumn().Col(func(column cdcevent.ResultColumn) error {
		parquetCol, err := NewParquetColumn(column)
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
	// Add the extra column which will store the type of event that generated that
	// particular row.
	eventTypeField, err := createColumnNode(parquetCrdbEventTypeColName, parquet.Repetitions.Required, parquet.Types.ByteArray)
	if err != nil {
		return nil, err
	}
	fields = append(fields, eventTypeField)

	groupNode, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		return nil, err
	}
	return &ParquetSchemaDefinition{
		cols: cols,
		sch:  schema.NewSchema(groupNode),
	}, nil
}

func getParquetColumnTypesV2(
	ctx context.Context, row cdcevent.Row,
) ([]pqexporter.ParquetColumn, error) {
	typs := make([]*types.T, 0)
	names := make([]string, 0)

	if err := row.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		typs = append(typs, col.Typ)
		names = append(names, col.Name)
		return nil
	}); err != nil {
		return nil, err
	}

	parquetColumns := make([]pqexporter.ParquetColumn, len(typs)+1)
	const nullable = true

	for i := 0; i < len(typs); i++ {
		// Make every field optional, so that all schema evolutions for a table are
		// considered "backward compatible" by parquet. This means that the parquet
		// type doesn't mirror the column's nullability, but it makes it much easier
		// to work with long histories of table data afterward, especially for
		// things like loading into analytics databases.
		parquetCol, err := pqexporter.NewParquetColumn(typs[i], names[i], nullable)
		if err != nil {
			return nil, err
		}
		parquetColumns[i] = parquetCol
	}

	// Add the extra column which will store the type of event that generated that
	// particular row.
	var err error
	parquetColumns[len(typs)], err = pqexporter.NewParquetColumn(types.String, parquetCrdbEventTypeColName, false)
	if err != nil {
		return nil, err
	}

	return parquetColumns, nil
}

// NewParquetColumn populates a ParquetColumn by finding the right parquet type
// and defining the encoder and decoder.
func NewParquetColumn(column cdcevent.ResultColumn) (ParquetColumn, error) {
	/*
		//			The type of a parquet column is either a group (i.e.
		//		  an array in crdb) or a primitive type (e.g., int, float, boolean,
		//		  string) and the repetition can be one of the three following cases:
		//
		//		  - required: exactly one occurrence (i.e. the column value is a scalar, and
		//		  cannot have null values). A column is set to required if the user
		//		  specified the CRDB column as NOT NULL.
		//		  - optional: 0 or 1 occurrence (i.e. same as above, but can have values)
		//		  - repeated: 0 or more occurrences (the column value will be an array. A
		//				value within the array will have its own repetition type)
		//
		//			See this blog post for more on parquet type specification:
		//			https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
		//	*/

	// MB figured out the low level properties of the encoding by running the goland debugger on
	// the following vendor example:
	// https://github.com/fraugster/parquet-go/blob/master/examples/write-low-level/main.go

	colName := column.Name
	// TODO: use optional for nullable columns.
	repetitions := parquet.Repetitions.Required

	// Fields populated by switch below:
	result := ParquetColumn{}

	var err error
	typ := column.Typ
	switch typ.Family() {
	case types.BoolFamily:
		result.node, err = createColumnNode(colName, repetitions, parquet.Types.Boolean)
		if err != nil {
			return ParquetColumn{}, err
		}
		result.enc = func(d tree.Datum) (interface{}, error) {
			return bool(*d.(*tree.DBool)), nil
		}
		return result, nil
	case types.StringFamily:
		result.node, err = createColumnNode(colName, repetitions, parquet.Types.ByteArray)
		if err != nil {
			return ParquetColumn{}, err
		}
		result.enc = func(d tree.Datum) (interface{}, error) {
			return parquet.ByteArray(*d.(*tree.DString)), nil
		}
		return result, nil
		//	//case types.CollatedStringFamily:
		//	//	populateLogicalStringCol(schemaEl)
		//	//	col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//	//		return []byte(d.(*tree.DCollatedString).Contents), nil
		//	//	}
		//	//	col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//	//		return tree.NewDCollatedString(string(x.([]byte)), typ.Locale(), &tree.CollationEnvironment{})
		//	//	}
		//	//case types.INetFamily:
		//	//	populateLogicalStringCol(schemaEl)
		//	//	col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//	//		return []byte(d.(*tree.DIPAddr).IPAddr.String()), nil
		//	//	}
		//	//	col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//	//		return tree.ParseDIPAddrFromINetString(string(x.([]byte)))
		//	//	}
		//	//case types.JsonFamily:
		//	//	schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		//	//	schemaEl.LogicalType = parquet.NewLogicalType()
		//	//	schemaEl.LogicalType.JSON = parquet.NewJsonType()
		//	//	schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_JSON)
		//	//	col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//	//		return []byte(d.(*tree.DJSON).JSON.String()), nil
		//	//	}
		//	//	col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//	//		jsonStr := string(x.([]byte))
		//	//		return tree.ParseDJSON(jsonStr)
		//	//	}
		//	//
	case types.IntFamily:
		if typ.Oid() == oid.T_int8 {
			result.node, err = createColumnNode(colName, repetitions, parquet.Types.Int64)
			if err != nil {
				return ParquetColumn{}, err
			}
			result.enc = func(d tree.Datum) (interface{}, error) {
				return int64(*d.(*tree.DInt)), nil
			}
			return result, nil
		}

		result.node, err = createColumnNode(colName, repetitions, parquet.Types.Int32)
		if err != nil {
			return ParquetColumn{}, err
		}
		result.enc = func(d tree.Datum) (interface{}, error) {
			return int32(*d.(*tree.DInt)), nil
		}
		return result, nil
		//	case types.FloatFamily:
		//		if typ.Oid() == oid.T_float4 {
		//			schemaEl.Type = parquet.TypePtr(parquet.Type_FLOAT)
		//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//				h := float32(*d.(*tree.DFloat))
		//				return h, nil
		//			}
		//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//				// must convert float32 to string before converting to float64 (the
		//				// underlying data type of a tree.Dfloat) because directly converting
		//				// a float32 to a float64 will add on trailing significant digits,
		//				// causing the round trip tests to fail.
		//				hS := fmt.Sprintf("%f", x.(float32))
		//				return tree.ParseDFloat(hS)
		//			}
		//		} else {
		//			schemaEl.Type = parquet.TypePtr(parquet.Type_DOUBLE)
		//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//				return float64(*d.(*tree.DFloat)), nil
		//			}
		//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//				return tree.NewDFloat(tree.DFloat(x.(float64))), nil
		//			}
		//		}
		//	case types.DecimalFamily:
		//		// TODO (MB): Investigate if the parquet vendor should enforce precision and
		//		// scale requirements. In a toy example, the parquet vendor was able to
		//		// write/read roundtrip the string "3235.5432" as a Decimal with Scale = 1,
		//		// Precision = 1, even though this decimal has a larger scale and precision.
		//		// I guess it's the responsibility of CRDB to enforce the Scale and
		//		// Precision conditions, and for the parquet vendor to NOT lose data, even if
		//		// the data doesn't follow the scale and precision conditions.
		//
		//		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		//
		//		schemaEl.LogicalType = parquet.NewLogicalType()
		//		schemaEl.LogicalType.DECIMAL = parquet.NewDecimalType()
		//
		//		schemaEl.LogicalType.DECIMAL.Scale = typ.Scale()
		//		schemaEl.LogicalType.DECIMAL.Precision = typ.Precision()
		//		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		//
		//		// According to PostgresSQL docs, scale or precision of 0 implies max
		//		// precision and scale. I assume this is what CRDB does, but this isn't
		//		// explicit in the docs https://www.postgresql.org/docs/10/datatype-numeric.html
		//		if typ.Scale() == 0 {
		//			schemaEl.LogicalType.DECIMAL.Scale = math.MaxInt32
		//		}
		//		if typ.Precision() == 0 {
		//			schemaEl.LogicalType.DECIMAL.Precision = math.MaxInt32
		//		}
		//
		//		schemaEl.Scale = &schemaEl.LogicalType.DECIMAL.Scale
		//		schemaEl.Precision = &schemaEl.LogicalType.DECIMAL.Precision
		//
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			dec := d.(*tree.DDecimal).Decimal
		//			return []byte(dec.String()), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			// TODO (MB): investigative if crdb should gather decimal metadata from
		//			// parquet file during IMPORT PARQUET.
		//			return tree.ParseDDecimal(string(x.([]byte)))
		//		}
		//	case types.UuidFamily:
		//		// Vendor parquet documentation suggests that UUID maps to the [16]byte go type
		//		// https://github.com/fraugster/parquet-go#supported-logical-types
		//		schemaEl.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
		//		byteArraySize := int32(uuid.Size)
		//		schemaEl.TypeLength = &byteArraySize
		//		schemaEl.LogicalType = parquet.NewLogicalType()
		//		schemaEl.LogicalType.UUID = parquet.NewUUIDType()
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			return d.(*tree.DUuid).UUID.GetBytes(), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			return tree.ParseDUuidFromBytes(x.([]byte))
		//		}
		//	case types.BytesFamily:
		//		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			return []byte(*d.(*tree.DBytes)), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			return tree.NewDBytes(tree.DBytes(x.([]byte))), nil
		//		}
		//	case types.BitFamily:
		//		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			// TODO(MB): investigate whether bit arrays should be encoded as an array of longs,
		//			// like in avro changefeeds
		//			baS := RoundtripStringer(d.(*tree.DBitArray))
		//			return []byte(baS), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			ba, err := bitarray.Parse(string(x.([]byte)))
		//			return &tree.DBitArray{BitArray: ba}, err
		//		}
		//	case types.EnumFamily:
		//		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		//		schemaEl.LogicalType = parquet.NewLogicalType()
		//		schemaEl.LogicalType.ENUM = parquet.NewEnumType()
		//		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			return []byte(d.(*tree.DEnum).LogicalRep), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			e, err := tree.MakeDEnumFromLogicalRepresentation(typ, string(x.([]byte)))
		//			if err != nil {
		//				return nil, err
		//			}
		//			return tree.NewDEnum(e), nil
		//		}
		//	case types.Box2DFamily:
		//		populateLogicalStringCol(schemaEl)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			return []byte(d.(*tree.DBox2D).CartesianBoundingBox.Repr()), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			b, err := geo.ParseCartesianBoundingBox(string(x.([]byte)))
		//			if err != nil {
		//				return nil, err
		//			}
		//			return tree.NewDBox2D(b), nil
		//		}
		//	case types.GeographyFamily:
		//		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			return []byte(d.(*tree.DGeography).EWKB()), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			g, err := geo.ParseGeographyFromEWKB(geopb.EWKB(x.([]byte)))
		//			if err != nil {
		//				return nil, err
		//			}
		//			return &tree.DGeography{Geography: g}, nil
		//		}
		//	case types.GeometryFamily:
		//		schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			return []byte(d.(*tree.DGeometry).EWKB()), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			g, err := geo.ParseGeometryFromEWKBUnsafe(geopb.EWKB(x.([]byte)))
		//			if err != nil {
		//				return nil, err
		//			}
		//			return &tree.DGeometry{Geometry: g}, nil
		//		}
		//	case types.DateFamily:
		//		// Even though the parquet vendor supports Dates, we export Dates as strings
		//		// because the vendor only supports encoding them as an int32, the Days
		//		// since the Unix epoch, which according CRDB's `date.UnixEpochDays( )` (in
		//		// pgdate package) is vulnerable to overflow.
		//		populateLogicalStringCol(schemaEl)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			date := d.(*tree.DDate)
		//			ds := RoundtripStringer(date)
		//			return []byte(ds), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			dStr := string(x.([]byte))
		//			d, dependCtx, err := tree.ParseDDate(nil, dStr)
		//			if dependCtx {
		//				return nil, errors.Newf("decoding date %s failed. depends on context", string(x.([]byte)))
		//			}
		//			return d, err
		//		}
		//	case types.TimeFamily:
		//		schemaEl.Type = parquet.TypePtr(parquet.Type_INT64)
		//		schemaEl.LogicalType = parquet.NewLogicalType()
		//		schemaEl.LogicalType.TIME = parquet.NewTimeType()
		//		t := parquet.NewTimeUnit()
		//		t.MICROS = parquet.NewMicroSeconds()
		//		schemaEl.LogicalType.TIME.Unit = t
		//		schemaEl.LogicalType.TIME.IsAdjustedToUTC = true // per crdb docs
		//		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)
		//
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			// Time of day is stored in microseconds since midnight,
		//			// which is also how parquet stores time
		//			time := d.(*tree.DTime)
		//			m := int64(*time)
		//			return m, nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			return tree.MakeDTime(timeofday.TimeOfDay(x.(int64))), nil
		//		}
		//	case types.TimeTZFamily:
		//		// The parquet vendor does not support an efficient encoding of TimeTZ
		//		// (i.e. a datetime field and a timezone field), so we must fall back to
		//		// encoding the whole TimeTZ as a string.
		//		populateLogicalStringCol(schemaEl)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			return []byte(d.(*tree.DTimeTZ).TimeTZ.String()), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			d, dependsOnCtx, err := tree.ParseDTimeTZ(nil, string(x.([]byte)), time.Microsecond)
		//			if dependsOnCtx {
		//				return nil, errors.New("parsed time depends on context")
		//			}
		//			return d, err
		//		}
		//	case types.IntervalFamily:
		//		// The parquet vendor only supports intervals as a parquet converted type,
		//		// but converted types have been deprecated in the Apache Parquet format.
		//		// https://github.com/fraugster/parquet-go#supported-converted-types
		//		populateLogicalStringCol(schemaEl)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			return []byte(d.(*tree.DInterval).ValueAsISO8601String()), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			return tree.ParseDInterval(duration.IntervalStyle_ISO_8601, string(x.([]byte)))
		//		}
		//	case types.TimestampFamily:
		//		// Didn't encode this as Microseconds since the unix epoch because of threat
		//		// of overflow. See comment associated with time.Time.UnixMicro().
		//		populateLogicalStringCol(schemaEl)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			ts := RoundtripStringer(d.(*tree.DTimestamp))
		//			return []byte(ts), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			// return tree.MakeDTimestamp(time.UnixMicro(x.(int64)).UTC(), time.Microsecond)
		//			dtStr := string(x.([]byte))
		//			d, dependsOnCtx, err := tree.ParseDTimestamp(nil, dtStr, time.Microsecond)
		//			if dependsOnCtx {
		//				return nil, errors.New("TimestampTZ depends on context")
		//			}
		//			if err != nil {
		//				return nil, err
		//			}
		//			// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent,
		//			// allowing roundtrip tests to pass.
		//			d.Time = d.Time.UTC()
		//			return d, nil
		//		}
		//
		//	case types.TimestampTZFamily:
		//		// Didn't encode this as Microseconds since the unix epoch because of threat
		//		// of overflow. See comment associated with time.Time.UnixMicro().
		//		populateLogicalStringCol(schemaEl)
		//
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			ts := RoundtripStringer(d.(*tree.DTimestampTZ))
		//			return []byte(ts), nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			dtStr := string(x.([]byte))
		//			d, dependsOnCtx, err := tree.ParseDTimestampTZ(nil, dtStr, time.Microsecond)
		//			if dependsOnCtx {
		//				return nil, errors.New("TimestampTZ depends on context")
		//			}
		//			if err != nil {
		//				return nil, err
		//			}
		//			// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent,
		//			// allowing tests to pass.
		//			d.Time = d.Time.UTC()
		//			return d, nil
		//		}
		//	case types.ArrayFamily:
		//
		//		// Define a list such that the parquet schema in json is:
		//		/*
		//			required group colName (LIST){ // parent
		//				repeated group list { // child
		//					required colType element; //grandChild
		//				}
		//			}
		//		*/
		//		// MB figured this out by running toy examples of the fraugster-parquet
		//		// vendor repository for added context, checkout this issue
		//		// https://github.com/fraugster/parquet-go/issues/18
		//
		//		// First, define the grandChild definition, the schema for the array value.
		//		grandChild, err := NewParquetColumn(typ.ArrayContents(), "element", true)
		//		if err != nil {
		//			return col, err
		//		}
		//		// Next define the child definition, required by fraugster-parquet vendor library. Again,
		//		// there's little documentation on this. MB figured this out using a debugger.
		//		child := &parquetschema.ColumnDefinition{}
		//		child.SchemaElement = parquet.NewSchemaElement()
		//		child.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.
		//			FieldRepetitionType_REPEATED)
		//		child.SchemaElement.Name = "list"
		//		child.Children = []*parquetschema.ColumnDefinition{grandChild.definition}
		//		ngc := int32(len(child.Children))
		//		child.SchemaElement.NumChildren = &ngc
		//
		//		// Finally, define the parent definition.
		//		col.definition.Children = []*parquetschema.ColumnDefinition{child}
		//		nc := int32(len(col.definition.Children))
		//		child.SchemaElement.NumChildren = &nc
		//		schemaEl.LogicalType = parquet.NewLogicalType()
		//		schemaEl.LogicalType.LIST = parquet.NewListType()
		//		schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_LIST)
		//		col.encodeFn = func(d tree.Datum) (interface{}, error) {
		//			datumArr := d.(*tree.DArray)
		//			els := make([]map[string]interface{}, datumArr.Len())
		//			for i, elt := range datumArr.Array {
		//				var el interface{}
		//				if elt.ResolvedType().Family() == types.UnknownFamily {
		//					// skip encoding the datum
		//				} else {
		//					el, err = grandChild.encodeFn(tree.UnwrapDOidWrapper(elt))
		//					if err != nil {
		//						return col, err
		//					}
		//				}
		//				els[i] = map[string]interface{}{"element": el}
		//			}
		//			encEl := map[string]interface{}{"list": els}
		//			return encEl, nil
		//		}
		//		col.DecodeFn = func(x interface{}) (tree.Datum, error) {
		//			// The parquet vendor decodes an array into the native go type
		//			// map[string]interface{}, and the values of the array are stored in the
		//			// "list" key of the map. "list" maps to an array of maps
		//			// []map[string]interface{}, where the ith map contains a single key value
		//			// pair. The key is always "element" and the value is the ith value in the
		//			// array.
		//
		//			// If the array of maps only contains an empty map, the array is empty. This
		//			// occurs IFF "element" is not in the map.
		//
		//			// NB: there's a bug in the fraugster-parquet vendor library around
		//			// reading an ARRAY[NULL],
		//			// https://github.com/fraugster/parquet-go/issues/60 I already verified
		//			// that the vendor's parquet writer can write arrays with null values just
		//			// fine, so EXPORT PARQUET is bug free; however this roundtrip test would
		//			// fail. Ideally, once the bug gets fixed, ARRAY[NULL] will get read as
		//			// the kvp {"element":interface{}} while ARRAY[] will continue to get read
		//			// as an empty map.
		//			datumArr := tree.NewDArray(typ.ArrayContents())
		//			datumArr.Array = []tree.Datum{}
		//
		//			intermediate := x.(map[string]interface{})
		//			vals := intermediate["list"].([]map[string]interface{})
		//			if _, nonEmpty := vals[0]["element"]; !nonEmpty {
		//				if len(vals) > 1 {
		//					return nil, errors.New("array is empty, it shouldn't have a length greater than 1")
		//				}
		//			} else {
		//				for _, elMap := range vals {
		//					itemDatum, err := grandChild.DecodeFn(elMap["element"])
		//					if err != nil {
		//						return nil, err
		//					}
		//					err = datumArr.Append(itemDatum)
		//					if err != nil {
		//						return nil, err
		//					}
		//				}
		//			}
		//			return datumArr, nil
		//		}
	default:
		return result, errors.Errorf("parquet export does not support the %v type yet", typ.Family())
	}
}
