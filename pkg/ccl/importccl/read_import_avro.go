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
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/linkedin/goavro/v2"
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
		if targetT.Identical(types.Bytes) {
			d = tree.NewDBytes(tree.DBytes(v))
		} else {
			// []byte arrays are hard.  Sometimes we want []bytes, sometimes
			// we want StringFamily.  So, instead of creating DBytes datum,
			// parse this data to "cast" it to our expected type.
			return rowenc.ParseDatumStringAs(targetT, string(v), evalCtx)
		}
	case string:
		// We allow strings to be specified for any column, as
		// long as we can convert the string value to the target type.
		return rowenc.ParseDatumStringAs(targetT, v, evalCtx)
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
	types.EnumFamily:           {"string"},
}

// avroConsumer implements importRowConsumer interface.
type avroConsumer struct {
	importCtx      *parallelImportContext
	fieldNameToIdx map[string]int
	strict         bool
}

// Converts avro record to datums as expected by DatumRowConverter.
func (a *avroConsumer) convertNative(x interface{}, conv *row.DatumRowConverter) error {
	record, ok := x.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unexpected native type; expected map[string]interface{} found %T instead", x)
	}

	for f, v := range record {
		field := lexbase.NormalizeName(f)
		idx, ok := a.fieldNameToIdx[field]
		if !ok {
			if a.strict {
				return fmt.Errorf("could not find column for record field %s", field)
			}
			continue
		}

		typ := conv.VisibleColTypes[idx]
		avroT, ok := familyToAvroT[typ.Family()]
		if !ok {
			return fmt.Errorf("cannot convert avro value %v to col %s", v, conv.VisibleCols[idx].GetType().Name())
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
func (a *avroConsumer) FillDatums(
	native interface{}, rowIndex int64, conv *row.DatumRowConverter,
) error {
	if err := a.convertNative(native, conv); err != nil {
		return err
	}

	// Set any nil datums to DNull (in case native
	// record didn't have the value set at all)
	for i := range conv.Datums {
		if conv.TargetColOrds.Contains(i) && conv.Datums[i] == nil {
			if a.strict {
				return fmt.Errorf("field %s was not set in the avro import", conv.VisibleCols[i].GetName())
			}
			conv.Datums[i] = tree.DNull
		}
	}
	return nil
}

var _ importRowConsumer = &avroConsumer{}

// An OCF (object container file) input scanner
type ocfStream struct {
	ocf      *goavro.OCFReader
	progress func() float32
	err      error
}

var _ importRowProducer = &ocfStream{}

// Progress implements importRowProducer interface
func (o *ocfStream) Progress() float32 {
	if o.progress != nil {
		return o.progress()
	}
	return 0
}

// Scan implements importRowProducer interface.
func (o *ocfStream) Scan() bool {
	return o.ocf.Scan()
}

// Err implements importRowProducer interface.
func (o *ocfStream) Err() error {
	return o.err
}

// Row implements importRowProducer interface.
func (o *ocfStream) Row() (interface{}, error) {
	return o.ocf.Read()
}

// Skip implements importRowProducer interface.
func (o *ocfStream) Skip() error {
	_, o.err = o.ocf.Read()
	return o.err
}

// A scanner over a file containing avro records in json or binary format.
type avroRecordStream struct {
	importCtx  *parallelImportContext
	opts       *roachpb.AvroOptions
	input      *fileReader
	codec      *goavro.Codec
	row        interface{} // Row to return
	buf        []byte      // Buffered data from input.  See note in fill() method.
	eof        bool        // Input eof reached
	err        error       // Error, other than io.EOF
	trimLeft   bool        // Trim record separator at the start of the buffer.
	maxBufSize int         // Error if buf exceeds this threshold
	minBufSize int         // Issue additional reads if buffer below this threshold
	readSize   int         // Read that many bytes at a time.
}

var _ importRowProducer = &avroRecordStream{}

func (r *avroRecordStream) Progress() float32 {
	return r.input.ReadFraction()
}

func (r *avroRecordStream) trimRecordSeparator() bool {
	if r.opts.RecordSeparator == 0 {
		return true
	}

	if len(r.buf) > 0 {
		c, n := utf8.DecodeRune(r.buf)
		if n > 0 && c == r.opts.RecordSeparator {
			r.buf = r.buf[n:]
			return true
		}
	}
	return false
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

// Scan implements importRowProducer interface.
func (r *avroRecordStream) Scan() bool {
	if r.row != nil {
		panic("must call Row() or Skip() before calling Scan()")
	}

	r.readNative()
	return r.err == nil && (!r.eof || r.row != nil)
}

// Err implements importRowProducer interface.
func (r *avroRecordStream) Err() error {
	return r.err
}

func (r *avroRecordStream) decode() (interface{}, []byte, error) {
	if r.opts.Format == roachpb.AvroOptions_BIN_RECORDS {
		return r.codec.NativeFromBinary(r.buf)
	}
	return r.codec.NativeFromTextual(r.buf)
}

func (r *avroRecordStream) readNative() {
	var remaining []byte
	var decodeErr error
	r.row = nil

	canReadMoreData := func() bool {
		return !r.eof && len(r.buf) < r.maxBufSize
	}

	for sz := r.readSize; r.row == nil && (len(r.buf) > 0 || canReadMoreData()); sz *= 2 {
		r.fill(sz)

		if r.trimLeft {
			r.trimLeft = !r.trimRecordSeparator()
		}

		if len(r.buf) > 0 {
			r.row, remaining, decodeErr = r.decode()
		}
		// If we've already read all we can (either to eof or to max size), then
		// any error during decoding should just be returned as an error.
		if decodeErr != nil && (r.eof || len(r.buf) > r.maxBufSize) {
			break
		}
	}

	if decodeErr != nil {
		r.err = decodeErr
		return
	}

	r.buf = remaining
	r.trimLeft = !r.trimRecordSeparator()
}

// Skip implements importRowProducer interface.
func (r *avroRecordStream) Skip() error {
	r.row = nil
	return nil
}

// Row implements importRowProducer interface.
func (r *avroRecordStream) Row() (interface{}, error) {
	res := r.row
	r.row = nil
	return res, nil
}

func newImportAvroPipeline(
	avro *avroInputReader, input *fileReader,
) (importRowProducer, importRowConsumer, error) {
	fieldIdxByName := make(map[string]int)
	for idx, col := range avro.importContext.tableDesc.VisibleColumns() {
		fieldIdxByName[col.GetName()] = idx
	}

	consumer := &avroConsumer{
		importCtx:      avro.importContext,
		fieldNameToIdx: fieldIdxByName,
		strict:         avro.opts.StrictMode,
	}

	if avro.opts.Format == roachpb.AvroOptions_OCF {
		ocf, err := goavro.NewOCFReader(bufio.NewReaderSize(input, 64<<10))
		if err != nil {
			return nil, nil, err
		}
		producer := &ocfStream{
			ocf:      ocf,
			progress: func() float32 { return input.ReadFraction() },
		}
		return producer, consumer, nil
	}

	codec, err := goavro.NewCodec(avro.opts.SchemaJSON)
	if err != nil {
		return nil, nil, err
	}

	producer := &avroRecordStream{
		importCtx: avro.importContext,
		opts:      &avro.opts,
		input:     input,
		codec:     codec,
		// We don't really know how large the records are, but if we have
		// "too little" data in our buffer, we would probably not be able to parse
		// avro record.  So, if our available bytes is below this threshold,
		// be proactive and read more data.
		minBufSize: 512,
		maxBufSize: 4 << 20, // bail out if we can't parse 4MB record.
		readSize:   4 << 10, // Just like bufio
	}

	if int(avro.opts.MaxRecordSize) > producer.maxBufSize {
		producer.maxBufSize = int(avro.opts.MaxRecordSize)
	}

	return producer, consumer, nil
}

type avroInputReader struct {
	importContext *parallelImportContext
	opts          roachpb.AvroOptions
}

var _ inputConverter = &avroInputReader{}

func newAvroInputReader(
	kvCh chan row.KVBatch,
	tableDesc catalog.TableDescriptor,
	avroOpts roachpb.AvroOptions,
	walltime int64,
	parallelism int,
	evalCtx *tree.EvalContext,
) (*avroInputReader, error) {

	return &avroInputReader{
		importContext: &parallelImportContext{
			walltime:   walltime,
			numWorkers: parallelism,
			evalCtx:    evalCtx,
			tableDesc:  tableDesc,
			kvCh:       kvCh,
		},
		opts: avroOpts,
	}, nil
}

func (a *avroInputReader) start(group ctxgroup.Group) {}

func (a *avroInputReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
	user security.SQLUsername,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, a.readFile, makeExternalStorage, user)
}

func (a *avroInputReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	producer, consumer, err := newImportAvroPipeline(a, input)
	if err != nil {
		return err
	}

	fileCtx := &importFileContext{
		source:   inputIdx,
		skip:     resumePos,
		rejected: rejected,
		rowLimit: a.opts.RowLimit,
	}
	return runParallelImport(ctx, a.importContext, fileCtx, producer, consumer)
}
