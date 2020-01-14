// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/linkedin/goavro"
)

// nativeToDatum converts go native type (interface{} as
// returned by goavro library) to the datum of appropriate type.
//
// While Avro specification is fairly broad, and supports arbitrary complex
// data types, this method concerns itself only with the primitive avro types,
// which include:
//   null, boolean, int (32), long (64), float (32), double (64),
//   bytes, string, and arrays of the above.
//
// Avro record is, essentially, a key->value mapping from field name to field value.
// A field->value mapping may be represented directly (i.e. the
// interface{} pass in will have corresponding go primitive type):
//   user_id:123 -- that is the interface{} type will be int, and it's value is 123.
//
// Or, we could see field_name:null, if the field is nullable and is null.
//
// Or, we could see e.g. user_id:{"int":123}, if field called user_id can
// be either null, or an int and the value of the field is 123. The value in
// this case is another interface{} which should be a map[string]interface{},
// where the key is a primitive Avro type name ("string", "long", etc).
func nativeToDatum(
	x interface{}, targetT *types.T, avroT []string, evalCtx *tree.EvalContext,
) (tree.Datum, error) {
	var d tree.Datum

	switch v := x.(type) {
	case nil:
		// Immediately return DNull, and let target
		// table schema verify whether nulls are allowed.
		return tree.DNull, nil
	case bool:
		if v {
			d = tree.DBoolTrue
		} else {
			d = tree.DBoolFalse
		}
	case int:
		d = tree.NewDInt(tree.DInt(v))
	case int32:
		d = tree.NewDInt(tree.DInt(v))
	case int64:
		d = tree.NewDInt(tree.DInt(v))
	case float32:
		d = tree.NewDFloat(tree.DFloat(v))
	case float64:
		d = tree.NewDFloat(tree.DFloat(v))
	case []byte:
		d = tree.NewDBytes(tree.DBytes(v))
	case string:
		// We allow strings to be specified for any column, as
		// long as we can convert the string value to the target type.
		return tree.ParseStringAs(targetT, v, evalCtx)
	case map[string]interface{}:
		for _, aT := range avroT {
			// The value passed in is an avro schema.  Extract
			// possible primitive types from the dictionary and
			// attempt to convert those values to our target type.
			if val, ok := v[aT]; ok {
				return nativeToDatum(val, targetT, avroT, evalCtx)
			}
		}
	case []interface{}:
		// Verify target type is an array we know how to handle.
		if targetT.ArrayContents() == nil {
			return nil, fmt.Errorf("cannot convert array to non-array type %s", targetT)
		}
		eltAvroT, ok := familyToAvroT[targetT.ArrayContents().Family()]
		if !ok {
			return nil, fmt.Errorf("cannot convert avro array element to %s", targetT.ArrayContents())
		}

		// Convert each element.
		arr := tree.NewDArray(targetT.ArrayContents())
		for _, elt := range v {
			eltDatum, err := nativeToDatum(elt, targetT.ArrayContents(), eltAvroT, evalCtx)
			if err == nil {
				err = arr.Append(eltDatum)
			}
			if err != nil {
				return nil, err
			}
		}
		d = arr
	}

	if d == nil {
		return nil, fmt.Errorf("cannot handle type %T when converting to %s", x, targetT)
	}

	if !targetT.Equivalent(d.ResolvedType()) {
		return nil, fmt.Errorf("cannot convert type %s to %s", d.ResolvedType(), targetT)
	}

	return d, nil
}

// A mapping from supported types.Family to the list of avro
// type names that can be used to construct our target type.
var familyToAvroT = map[types.Family][]string{
	// Primitive avro types.
	types.BoolFamily:   {"bool", "boolean", "string"},
	types.IntFamily:    {"int", "long", "string"},
	types.FloatFamily:  {"float", "double", "string"},
	types.StringFamily: {"string"},
	types.BytesFamily:  {"bytes", "string"},

	// Arrays can be specified as avro array type, or we can try parsing string.
	types.ArrayFamily: {"array", "string"},

	// Families we can try to convert using string conversion.
	types.UuidFamily:           {"string"},
	types.DateFamily:           {"string"},
	types.TimeFamily:           {"string"},
	types.IntervalFamily:       {"string"},
	types.TimestampTZFamily:    {"string"},
	types.TimestampFamily:      {"string"},
	types.CollatedStringFamily: {"string"},
	types.INetFamily:           {"string"},
	types.JsonFamily:           {"string"},
	types.BitFamily:            {"string"},
	types.DecimalFamily:        {"string"},
}

type nativeConverter struct {
	conv           *row.DatumRowConverter
	fieldNameToIdx map[string]int
	strict         bool
}

// Converts avro record to datums as expected by DatumRowConverter.
func (c *nativeConverter) convertNative(x interface{}, evalCtx *tree.EvalContext) error {
	record, ok := x.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unexpected native type; expected map[string]interface{} found %T instead", x)
	}

	for f, v := range record {
		field := lex.NormalizeName(f)
		idx, ok := c.fieldNameToIdx[field]
		if !ok {
			if c.strict {
				return fmt.Errorf("could not find column for record field %s", field)
			}
			continue
		}

		typ := c.conv.VisibleColTypes[idx]
		avroT, ok := familyToAvroT[typ.Family()]
		if !ok {
			return fmt.Errorf("cannot convert avro value %v to col %s", v, c.conv.VisibleCols[idx].Type.Name())
		}

		datum, err := nativeToDatum(v, typ, avroT, evalCtx)
		if err != nil {
			return err
		}
		c.conv.Datums[idx] = datum
	}
	return nil
}

// Emits row into DatumRowConverter
func (c *nativeConverter) Row(
	ctx context.Context, native interface{}, sourceID int32, rowIndex int64,
) error {
	if err := c.convertNative(native, c.conv.EvalCtx); err != nil {
		return err
	}

	// Set any nil datums to DNull (in case native
	// record didn't have the value set at all)
	for i := range c.conv.Datums {
		if c.conv.Datums[i] == nil {
			if c.strict {
				return fmt.Errorf("field %s was not set in the avro import", c.conv.VisibleCols[i].Name)
			}
			c.conv.Datums[i] = tree.DNull
		}
	}

	return c.conv.Row(ctx, sourceID, rowIndex)
}

// A scanner over avro input.
type avroRowStream interface {
	// Scan returns true if there is more data available.
	// After Scan() returns false, the caller should verify
	// that the scanner has not encountered an error (Err() == nil).
	Scan() bool

	// Err returns an error (if any) encountered when processing avro stream.
	Err() error

	// Skip, as the name implies, skips the current record in this stream.
	Skip() error

	// Row emits current row (record).
	Row(ctx context.Context, sourceID int32, rowIndex int64) error
}

// An OCF (object container file) input scanner
type ocfStream struct {
	nativeConverter
	ocf *goavro.OCFReader
	err error
}

var _ avroRowStream = &ocfStream{}

// Scan implements avroRowStream interface.
func (o *ocfStream) Scan() bool {
	return o.ocf.Scan()
}

// Err implements avroRowStream interface.
func (o *ocfStream) Err() error {
	return o.err
}

// Row implements avroRowStream interface.
func (o *ocfStream) Row(ctx context.Context, sourceID int32, rowIndex int64) error {
	var native interface{}
	native, o.err = o.ocf.Read()
	if o.err == nil {
		o.err = o.nativeConverter.Row(ctx, native, sourceID, rowIndex)
	}
	return o.err
}

// Skip implements avroRowStream interface.
func (o *ocfStream) Skip() error {
	_, o.err = o.ocf.Read()
	return o.err
}

// A scanner over a file containing avro records in json or binary format.
type avroRecordStream struct {
	nativeConverter
	opts       roachpb.AvroOptions
	input      io.Reader
	buf        *bytes.Buffer
	codec      *goavro.Codec
	eof        bool  // Input eof reached
	err        error // Error, other than io.EOF
	maxBufSize int   // Error if buf exceeds this threshold
	minBufSize int   // Issue additional reads if buffer below this threshold
	readSize   int   // Read that many bytes at a time.
}

var _ avroRowStream = &avroRecordStream{}

func (r *avroRecordStream) trimRecordSeparator() {
	if r.opts.RecordSeparator == 0 {
		return
	}

	if r.buf.Len() == 0 {
		r.fill(r.readSize)
	}

	if r.buf.Len() > 0 {
		var c rune
		c, _, r.err = r.buf.ReadRune()
		if r.err == nil {
			if c != r.opts.RecordSeparator {
				r.err = r.buf.UnreadRune()
			}
		}
	}
}

func (r *avroRecordStream) fill(sz int) {
	if r.eof || r.err != nil {
		return
	}
	_, r.err = io.CopyN(r.buf, r.input, int64(sz))

	if r.err == io.EOF {
		r.eof = true
		r.err = nil
	}
}

// Scan implements avroRowStream interface.
func (r *avroRecordStream) Scan() bool {
	if !r.eof && r.buf.Len() < r.minBufSize {
		r.fill(r.readSize)
	}
	return r.err == nil && (!r.eof || r.buf.Len() > 0)
}

// Err implements avroRowStream interface.
func (r *avroRecordStream) Err() error {
	return r.err
}

func (r *avroRecordStream) decode() (interface{}, []byte, error) {
	if r.opts.Format == roachpb.AvroOptions_BIN_RECORDS {
		return r.codec.NativeFromBinary(r.buf.Bytes())
	}
	return r.codec.NativeFromTextual(r.buf.Bytes())
}

func (r *avroRecordStream) readNative() interface{} {
	native, remaining, err := r.decode()

	// Read more data if we get an error decoding
	// (in case we have partial record in the buffer).
	for sz := r.readSize; err != nil && !r.eof && r.buf.Len() < r.maxBufSize; sz *= 2 {
		err = nil
		r.fill(sz)
		native, remaining, err = r.decode()
	}

	if err != nil {
		r.err = err
		return nil
	}

	r.buf.Next(r.buf.Len() - len(remaining))
	r.trimRecordSeparator()
	return native
}

// Skip implements avroRowStream interface.
func (r *avroRecordStream) Skip() error {
	_ = r.readNative()
	return r.err
}

// Row implements avroRowStream interface.
func (r *avroRecordStream) Row(ctx context.Context, sourceID int32, rowIndex int64) error {
	native := r.readNative()
	if r.err == nil {
		r.err = r.nativeConverter.Row(ctx, native, sourceID, rowIndex)
	}
	return r.err
}

func newRowStream(
	avro roachpb.AvroOptions, conv *row.DatumRowConverter, input *fileReader,
) (avroRowStream, error) {
	fieldIdxByName := make(map[string]int)
	for idx, col := range conv.VisibleCols {
		fieldIdxByName[col.Name] = idx
	}

	if avro.Format == roachpb.AvroOptions_OCF {
		ocf, err := goavro.NewOCFReader(bufio.NewReaderSize(input, 64<<10))
		if err != nil {
			return nil, err
		}

		return &ocfStream{
			nativeConverter: nativeConverter{
				conv:           conv,
				fieldNameToIdx: fieldIdxByName,
				strict:         avro.StrictMode,
			},
			ocf: ocf,
		}, nil
	}

	codec, err := goavro.NewCodec(avro.SchemaJSON)
	if err != nil {
		return nil, err
	}

	stream := &avroRecordStream{
		nativeConverter: nativeConverter{
			conv:           conv,
			fieldNameToIdx: fieldIdxByName,
			strict:         avro.StrictMode,
		},
		opts:  avro,
		input: input,
		codec: codec,
		buf:   bytes.NewBuffer(nil),
		// We don't really know how large the records are, but if we have
		// "too little" data in our buffer, we would probably not be able to parse
		// avro record.  So, if our available bytes is below this threshold,
		// be proactive and read more data.
		minBufSize: 512,
		maxBufSize: 4 << 20, // bail out if we can't parse 4MB record.
		readSize:   4 << 10, // Just like bufio
	}

	if int(avro.MaxRecordSize) > stream.maxBufSize {
		stream.maxBufSize = int(avro.MaxRecordSize)
	}

	return stream, nil
}

type avroInputReader struct {
	opts roachpb.AvroOptions
	conv *row.DatumRowConverter
}

var _ inputConverter = &avroInputReader{}

func newAvroInputReader(
	kvCh chan row.KVBatch,
	tableDesc *sqlbase.TableDescriptor,
	avro roachpb.AvroOptions,
	evalCtx *tree.EvalContext,
) (*avroInputReader, error) {
	conv, err := row.NewDatumRowConverter(tableDesc, nil /* targetColNames */, evalCtx, kvCh)

	if err != nil {
		return nil, err
	}

	return &avroInputReader{
		opts: avro,
		conv: conv,
	}, nil
}

func (a *avroInputReader) start(group ctxgroup.Group) {}

func (a *avroInputReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, a.readFile, makeExternalStorage)
}

func (a *avroInputReader) readFile(
	ctx context.Context,
	input *fileReader,
	inputIdx int32,
	inputName string,
	resumePos int64,
	rejected chan string,
) error {
	stream, err := newRowStream(a.opts, a.conv, input)
	if err != nil {
		return err
	}

	var count int64
	a.conv.KvBatch.Source = inputIdx
	a.conv.FractionFn = input.ReadFraction
	a.conv.CompletedRowFn = func() int64 {
		return count
	}

	for stream.Scan() {
		count++
		if count <= resumePos {
			if err := stream.Skip(); err != nil {
				return err
			}
			continue
		}

		if err := stream.Row(ctx, inputIdx, count); err != nil {
			// TODO(yevgeniy): Report corrupt rows.
			return err
		}
	}

	if stream.Err() == nil {
		return a.conv.SendBatch(ctx)
	}

	return stream.Err()
}
