// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/errors"
	"github.com/linkedin/goavro/v2"
)

// nativeTimeToDatum maps the time.Time object returned by goAvro to the proper CRL datum type.
func nativeTimeToDatum(t time.Time, targetT *types.T) (tree.Datum, error) {
	duration := tree.TimeFamilyPrecisionToRoundDuration(targetT.Precision())
	switch targetT.Family() {
	case types.DateFamily:
		return tree.NewDDateFromTime(t)
	case types.TimestampFamily:
		return tree.MakeDTimestamp(t, duration)
	default:
		return nil, errors.New("type not supported")
	}
}

// nativeToDatum converts go native types (interface{} as returned by goavro
// library) and logical time types to the datum with the appropriate type.
//
// While Avro's specification is fairly broad, and supports arbitrary complex
// data types, this method concerns itself with
//   - primitive avro types: null, boolean, int (32), long (64), float (32), double (64),
//     bytes, string, and arrays of the above.
//   - logical avro types (as defined by the go avro library): long.time-micros, int.time-millis,
//     long.timestamp-micros,long.timestamp-millis, and int.date
//
// An avro record is, essentially, a key->value mapping from field name to field value.
// A field->value mapping may be represented directly (i.e. the
// interface{} pass in will have corresponding go primitive type):
//
//	user_id:123 -- that is the interface{} type will be int, and it's value is 123.
//
// Or, we could see field_name:null, if the field is nullable and is null.
//
// Or, we could see e.g. user_id:{"int":123}, if field called user_id can be
// either null, or an int and the value of the field is 123. The value in this
// case is another interface{} which should be a map[string]interface{}, where
// the key is a primitive or logical Avro type name ("string",
// "long.time-millis", etc).
func nativeToDatum(
	ctx context.Context,
	x interface{},
	targetT *types.T,
	avroT []string,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
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
	case time.Time:
		return nativeTimeToDatum(v, targetT)
	case time.Duration:
		// goAvro returns avro cols of logical type time as time.duration
		dU := v / time.Microsecond
		d = tree.MakeDTime(timeofday.TimeOfDay(dU))
	case []byte:
		if targetT.Identical(types.Bytes) {
			d = tree.NewDBytes(tree.DBytes(v))
		} else {
			// []byte arrays are hard.  Sometimes we want []bytes, sometimes
			// we want StringFamily.  So, instead of creating DBytes datum,
			// parse this data to "cast" it to our expected type.
			return rowenc.ParseDatumStringAs(ctx, targetT, string(v), evalCtx, semaCtx)
		}
	case string:
		// We allow strings to be specified for any column, as
		// long as we can convert the string value to the target type.
		return rowenc.ParseDatumStringAs(ctx, targetT, v, evalCtx, semaCtx)
	case map[string]interface{}:
		for _, aT := range avroT {
			// The value passed in is an avro schema.  Extract
			// possible primitive types from the dictionary and
			// attempt to convert those values to our target type.
			if val, ok := v[aT]; ok {
				return nativeToDatum(ctx, val, targetT, avroT, evalCtx, semaCtx)
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
			eltDatum, err := nativeToDatum(ctx, elt, targetT.ArrayContents(), eltAvroT, evalCtx, semaCtx)
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

	// Time families with avro logical types. Avro logical type names pulled from:
	// https://github.com/linkedin/goavro/blob/master/logical_type.go and
	// https://avro.apache.org/docs/current/spec.html
	types.DateFamily:      {"string", "int.date"},
	types.TimeFamily:      {"string", "long.time-micros", "int.time-millis"},
	types.TimestampFamily: {"string", "long.timestamp-micros", "long.timestamp-millis"},

	// goavro does not yet support times with local timezones. So, CRDB can only
	// import these datum types if the goAvro type is string.
	types.TimeTZFamily:      {"string"},
	types.TimestampTZFamily: {"string"},

	// goavro does no support the interval logical type
	types.IntervalFamily: {"string"},

	// Families we can try to convert using string conversion.
	types.UuidFamily:           {"string"},
	types.CollatedStringFamily: {"string"},
	types.INetFamily:           {"string"},
	types.JsonFamily:           {"string"},
	types.BitFamily:            {"string"},
	types.DecimalFamily:        {"string"}, //TODO(Butler): import avro with logical decimal
	types.EnumFamily:           {"string"},
}

// avroConsumer implements importRowConsumer interface.
type avroConsumer struct {
	importCtx      *parallelImportContext
	fieldNameToIdx map[string]int
	strict         bool
}

// Converts avro record to datums as expected by DatumRowConverter.
func (a *avroConsumer) convertNative(
	ctx context.Context, x interface{}, conv *row.DatumRowConverter,
) error {
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
		datum, err := nativeToDatum(ctx, v, typ, avroT, conv.EvalCtx, conv.SemaCtx)
		if err != nil {
			return err
		}
		conv.Datums[idx] = datum
	}
	return nil
}

// FillDatums implements importRowStream interface.
func (a *avroConsumer) FillDatums(
	ctx context.Context, native interface{}, rowIndex int64, conv *row.DatumRowConverter,
) error {
	if err := a.convertNative(ctx, native, conv); err != nil {
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
	semaCtx *tree.SemaContext,
	kvCh chan row.KVBatch,
	tableDesc catalog.TableDescriptor,
	avroOpts roachpb.AvroOptions,
	walltime int64,
	parallelism int,
	evalCtx *eval.Context,
	db *kv.DB,
) (*avroInputReader, error) {

	return &avroInputReader{
		importContext: &parallelImportContext{
			semaCtx:    semaCtx,
			walltime:   walltime,
			numWorkers: parallelism,
			evalCtx:    evalCtx,
			tableDesc:  tableDesc,
			kvCh:       kvCh,
			db:         db,
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
	user username.SQLUsername,
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
