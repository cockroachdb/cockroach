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
	"runtime"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		if targetT.Equal(*types.Bytes) {
			d = tree.NewDBytes(tree.DBytes(v))
		} else {
			// []byte arrays are hard.  Sometimes we want []bytes, sometimes
			// we want StringFamily.  So, instead of creating DBytes datum,
			// parse this data to "cast" it to our expected type.
			return sqlbase.ParseDatumStringAs(targetT, string(v), evalCtx)
		}
	case string:
		// We allow strings to be specified for any column, as
		// long as we can convert the string value to the target type.
		return sqlbase.ParseDatumStringAs(targetT, v, evalCtx)
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
	types.StringFamily: {"string", "bytes"},
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

// nativeConverter is a base converter responsible for converting
// parsed avro data to the target Datums.
// This base type implements some parts of the importRowStream interface.
type nativeConverter struct {
	input          *namedInput
	fieldNameToIdx map[string]int
	strict         bool
	rejected       chan string
}

// Converts avro record to datums as expected by DatumRowConverter.
func (c *nativeConverter) convertNative(x interface{}, conv *row.DatumRowConverter) error {
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

		typ := conv.VisibleColTypes[idx]
		avroT, ok := familyToAvroT[typ.Family()]
		if !ok {
			return fmt.Errorf("cannot convert avro value %v to col %s", v, conv.VisibleCols[idx].Type.Name())
		}

		datum, err := nativeToDatum(v, typ, avroT, conv.EvalCtx)
		if err != nil {
			return err
		}
		conv.Datums[idx] = datum
	}
	return nil
}

// FillDatums implements importRowStream interface.
func (c *nativeConverter) FillDatums(
	native interface{}, rowIndex int64, conv *row.DatumRowConverter,
) error {
	if err := c.convertNative(native, conv); err != nil {
		return err
	}

	// Set any nil datums to DNull (in case native
	// record didn't have the value set at all)
	for i := range conv.Datums {
		if conv.Datums[i] == nil {
			if c.strict {
				return fmt.Errorf("field %s was not set in the avro import", conv.VisibleCols[i].Name)
			}
			conv.Datums[i] = tree.DNull
		}
	}
	return nil
}

// HandleCorruptRow implements importRowStream interface.
func (c *nativeConverter) HandleCorruptRow(
	ctx context.Context, row interface{}, rowNum int64, err error,
) bool {
	log.Error(ctx, err)
	if c.rejected == nil {
		return false
	}
	c.rejected <- fmt.Sprintf("%s\n", row)
	return true
}

// Input implements importRowStream interface
func (c *nativeConverter) Input() *namedInput {
	return c.input
}

// StreamProgress implements importRowStream interface.
func (c *nativeConverter) StreamProgress() float32 {
	return c.input.reader.ReadFraction()
}

// An OCF (object container file) input scanner
type ocfStream struct {
	nativeConverter
	ocf *goavro.OCFReader
	err error
}

var _ importRowStream = &ocfStream{}

// Scan implements importRowStream interface.
func (o *ocfStream) Scan() bool {
	return o.ocf.Scan()
}

// Err implements importRowStream interface.
func (o *ocfStream) Err() error {
	return o.err
}

// Row implements importRowStream interface.
func (o *ocfStream) Row() (interface{}, error) {
	return o.ocf.Read()
}

// Skip implements importRowStream interface.
func (o *ocfStream) Skip() error {
	_, o.err = o.ocf.Read()
	return o.err
}

// A scanner over a file containing avro records in json or binary format.
type avroRecordStream struct {
	nativeConverter
	opts       roachpb.AvroOptions
	input      io.Reader
	codec      *goavro.Codec
	buf        []byte // Buffered data from input.  See note in fill() method.
	eof        bool   // Input eof reached
	err        error  // Error, other than io.EOF
	maxBufSize int    // Error if buf exceeds this threshold
	minBufSize int    // Issue additional reads if buffer below this threshold
	readSize   int    // Read that many bytes at a time.
}

var _ importRowStream = &avroRecordStream{}

func (r *avroRecordStream) trimRecordSeparator() {
	if r.opts.RecordSeparator == 0 {
		return
	}

	if len(r.buf) == 0 {
		r.fill(r.readSize)
	}

	if len(r.buf) > 0 {
		c, n := utf8.DecodeRune(r.buf)
		if n > 0 && c == r.opts.RecordSeparator {
			r.buf = r.buf[n:]
		}
	}
}

func (r *avroRecordStream) fill(sz int) {
	if r.eof || r.err != nil {
		return
	}

	// NB: We use bytes.Buffer for writing into our internal buf, but we cannot
	// use bytes.Buffer for reading. The reason is that bytes.Buffer tries
	// to be efficient in its memory management. In particular, it can reuse
	// underlying memory if the buffer becomes empty (buf = buf[:0]).  This is
	// problematic for us because the avro stream sends interface{} objects
	// to the consumer workers. Those interface objects may (infrequently)
	// reference the underlying byte array from which those interface objects
	// were constructed (e.g. if we are decoding avro bytes data type, we may
	// actually return []byte as an interface{} referencing underlying buffer).
	// To avoid this unpleasant situation, we never reset the head of our
	// buffer.
	sink := bytes.NewBuffer(r.buf)
	_, r.err = io.CopyN(sink, r.input, int64(sz))
	r.buf = sink.Bytes()

	if r.err == io.EOF {
		r.eof = true
		r.err = nil
	}
}

// Scan implements importRowStream interface.
func (r *avroRecordStream) Scan() bool {
	if !r.eof && cap(r.buf)-len(r.buf) < r.minBufSize {
		r.fill(r.readSize)
	}
	return r.err == nil && (!r.eof || len(r.buf) > 0)
}

// Err implements importRowStream interface.
func (r *avroRecordStream) Err() error {
	return r.err
}

func (r *avroRecordStream) decode() (interface{}, []byte, error) {
	if r.opts.Format == roachpb.AvroOptions_BIN_RECORDS {
		return r.codec.NativeFromBinary(r.buf)
	}
	return r.codec.NativeFromTextual(r.buf)
}

func (r *avroRecordStream) readNative() interface{} {
	native, remaining, err := r.decode()

	// Read more data if we get an error decoding
	// (in case we have partial record in the buffer).
	for sz := r.readSize; err != nil && !r.eof && len(r.buf) < r.maxBufSize; sz *= 2 {
		err = nil
		r.fill(sz)
		native, remaining, err = r.decode()
	}

	if err != nil {
		r.err = err
		return nil
	}

	r.buf = remaining
	r.trimRecordSeparator()
	return native
}

// Skip implements importRowStream interface.
func (r *avroRecordStream) Skip() error {
	_ = r.readNative()
	return r.err
}

// Row implements importRowStream interface.
func (r *avroRecordStream) Row() (interface{}, error) {
	native := r.readNative()
	return native, r.err
}

func newRowStream(
	avro roachpb.AvroOptions,
	input *fileReader,
	inputName string,
	inputIdx int32,
	tableDesc *sqlbase.TableDescriptor,
	rejected chan string,
) (importRowStream, error) {
	fieldIdxByName := make(map[string]int)
	for idx, col := range tableDesc.VisibleColumns() {
		fieldIdxByName[col.Name] = idx
	}

	nc := nativeConverter{
		input: &namedInput{
			reader: input,
			name:   inputName,
			idx:    inputIdx,
		},
		fieldNameToIdx: fieldIdxByName,
		strict:         avro.StrictMode,
		rejected:       rejected,
	}

	if avro.Format == roachpb.AvroOptions_OCF {
		ocf, err := goavro.NewOCFReader(bufio.NewReaderSize(input, 64<<10))
		if err != nil {
			return nil, err
		}

		return &ocfStream{
			nativeConverter: nc,
			ocf:             ocf,
		}, nil
	}

	codec, err := goavro.NewCodec(avro.SchemaJSON)
	if err != nil {
		return nil, err
	}

	stream := &avroRecordStream{
		nativeConverter: nc,
		opts:            avro,
		input:           input,
		codec:           codec,
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
	evalCtx     *tree.EvalContext
	opts        roachpb.AvroOptions
	kvCh        chan row.KVBatch
	tableDesc   *sqlbase.TableDescriptor
	walltime    int64
	batchSize   int
	parallelism int
}

var _ inputConverter = &avroInputReader{}

func newAvroInputReader(
	kvCh chan row.KVBatch,
	tableDesc *sqlbase.TableDescriptor,
	avro roachpb.AvroOptions,
	walltime int64,
	parallelism int,
	evalCtx *tree.EvalContext,
) (*avroInputReader, error) {
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	return &avroInputReader{
		evalCtx:     evalCtx,
		opts:        avro,
		tableDesc:   tableDesc,
		kvCh:        kvCh,
		walltime:    walltime,
		parallelism: parallelism,
		batchSize:   parallelImporterReaderBatchSize,
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
	stream, err := newRowStream(a.opts, input, inputName, inputIdx, a.tableDesc, rejected)
	if err != nil {
		return err
	}

	return a.importAvro(ctx, stream, resumePos)
}

func (a *avroInputReader) importAvro(
	ctx context.Context, stream importRowStream, resumePos int64,
) error {
	visibleCols := make(tree.NameList, len(a.tableDesc.VisibleColumns()))
	for i, col := range a.tableDesc.VisibleColumns() {
		visibleCols[i] = tree.Name(col.Name)
	}

	return runParallelImport(
		ctx,
		parallelImportOptions{
			skip:       resumePos,
			walltime:   a.walltime,
			numWorkers: a.parallelism,
			batchSize:  a.batchSize,
		},
		stream,
		func(ctx context.Context) (*row.DatumRowConverter, error) {
			return row.NewDatumRowConverter(ctx, a.tableDesc, visibleCols, a.evalCtx.Copy(), a.kvCh)
		},
	)
}
