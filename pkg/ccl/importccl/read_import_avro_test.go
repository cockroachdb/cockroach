// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

// avroGen interface is an interface for generating avro test field.
type avroGen interface {
	Name() string
	Gen() interface{}
	AvroT() interface{} // nil if avro records should omit this field
	SQLT() interface{}  // nil if this column should not be created
}

// Base type for avro data generators.
type namedField struct {
	name        string
	excludeAvro bool
	excludeSQL  bool
}

func (g *namedField) Name() string {
	return g.name
}

// Generates nil or a string.
type nilOrStrGen struct {
	namedField
}

func (g *nilOrStrGen) Gen() interface{} {
	id := rand.Int()
	if id%2 == 0 {
		return nil
	}
	return map[string]interface{}{"string": fmt.Sprintf("%s %d", g.name, id)}
}

func (g *nilOrStrGen) AvroT() interface{} {
	if g.excludeAvro {
		return nil
	}
	return []string{"null", "string"}
}

func (g *nilOrStrGen) SQLT() interface{} {
	if g.excludeSQL {
		return nil
	}
	return "string"
}

// Generates a sequence number
type seqGen struct {
	namedField
	seq int
}

func (g *seqGen) Gen() interface{} {
	g.seq++
	return g.seq
}

func (g *seqGen) AvroT() interface{} {
	if g.excludeAvro {
		return nil
	}
	return "int"
}

func (g *seqGen) SQLT() interface{} {
	if g.excludeSQL {
		return nil
	}
	return "int"
}

// Generates array of integers (or nils)
type intArrGen struct {
	namedField
}

func (g *intArrGen) AvroT() interface{} {
	return []interface{}{
		// Each element is either a null or an array.
		"null",
		// And each array element is either a long or a null.
		map[string]interface{}{"type": "array", "items": []string{"null", "long"}}}
}

func (g *intArrGen) SQLT() interface{} {
	return "int[]"
}

func (g *intArrGen) Gen() interface{} {
	id := rand.Int()
	if id%2 == 0 {
		return nil
	}
	var arr []interface{}
	var val interface{}
	// Generate few integers, with some nils thrown in for good measure.
	for i := 0; i < 1+id%10; i++ {
		if i%3 == 0 {
			val = nil
		} else {
			val = map[string]interface{}{"long": i}
		}
		arr = append(arr, val)
	}
	return map[string]interface{}{"array": arr}
}

// A testHelper to generate avro data.
type testHelper struct {
	schemaJSON  string
	schemaTable catalog.TableDescriptor
	codec       *goavro.Codec
	gens        []avroGen
	settings    *cluster.Settings
	evalCtx     tree.EvalContext
}

var defaultGens = []avroGen{
	&seqGen{namedField: namedField{name: "uid"}},
	&nilOrStrGen{namedField{name: "uname"}},
	&nilOrStrGen{namedField{name: "notes"}},
}

func newTestHelper(ctx context.Context, t *testing.T, gens ...avroGen) *testHelper {
	if len(gens) == 0 {
		gens = defaultGens
	}

	// Generate avro schema specification as well as CREATE TABLE statement
	// based on the specified generators.
	schema := map[string]interface{}{
		"type": "record",
		"name": "users",
	}
	var avroFields []map[string]interface{}
	createStmt := "CREATE TABLE users ("

	for i, gen := range gens {
		avroT := gen.AvroT()
		sqlT := gen.SQLT()
		if avroT != nil {
			avroFields = append(avroFields, map[string]interface{}{
				"name": gen.Name(),
				"type": avroT,
			})
		}

		if sqlT != nil {
			createStmt += fmt.Sprintf("%s %s", gen.Name(), sqlT)
			if i < len(gens)-1 {
				createStmt += ","
			}
		}
	}

	createStmt += ")"
	schema["fields"] = avroFields
	schemaJSON, err := json.Marshal(schema)
	require.NoError(t, err)

	codec, err := goavro.NewCodec(string(schemaJSON))
	require.NoError(t, err)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	return &testHelper{
		schemaJSON: string(schemaJSON),
		schemaTable: descForTable(ctx, t, createStmt, 100, 200, NoFKs).
			ImmutableCopy().(catalog.TableDescriptor),
		codec:    codec,
		gens:     gens,
		settings: st,
		evalCtx:  evalCtx,
	}
}

type testRecordStream struct {
	producer importRowProducer
	consumer importRowConsumer
	rowNum   int64
	conv     *row.DatumRowConverter
}

// Combine Row() with FillDatums for error checking.
func (t *testRecordStream) Row() error {
	r, err := t.producer.Row()
	if err == nil {
		t.rowNum++
		err = t.consumer.FillDatums(r, t.rowNum, t.conv)
	}
	return err
}

// Generates test data with the specified format and returns avroRowStream object.
func (th *testHelper) newRecordStream(
	t *testing.T, format roachpb.AvroOptions_Format, strict bool, numRecords int,
) *testRecordStream {
	// Ensure datum converter doesn't flush (since
	// we're using nil kv channel for this test).
	defer row.TestingSetDatumRowConverterBatchSize(numRecords + 1)()

	opts := roachpb.AvroOptions{
		Format:     format,
		StrictMode: strict,
	}

	records := bytes.NewBufferString("")
	if format == roachpb.AvroOptions_OCF {
		th.genOcfData(t, numRecords, records)
	} else {
		opts.RecordSeparator = '\n'
		opts.SchemaJSON = th.schemaJSON
		th.genRecordsData(t, format, numRecords, opts.RecordSeparator, records)
	}

	avro, err := newAvroInputReader(nil, th.schemaTable, opts, 0, 1, &th.evalCtx)
	require.NoError(t, err)
	producer, consumer, err := newImportAvroPipeline(avro, &fileReader{Reader: records})
	require.NoError(t, err)

	conv, err := row.NewDatumRowConverter(
		context.Background(), th.schemaTable, nil, th.evalCtx.Copy(), nil,
		nil /* seqChunkProvider */)
	require.NoError(t, err)
	return &testRecordStream{
		producer: producer,
		consumer: consumer,
		conv:     conv,
	}
}

func (th *testHelper) genAvroRecord() interface{} {
	rec := make(map[string]interface{})
	for _, gen := range th.gens {
		if gen.AvroT() != nil {
			rec[gen.Name()] = gen.Gen()
		}
	}
	return rec
}

// Generates OCF test data.
func (th *testHelper) genOcfData(t *testing.T, numRecords int, records *bytes.Buffer) {
	ocf, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      records,
		Codec:  th.codec,
		Schema: th.schemaJSON,
	})

	for i := 0; err == nil && i < numRecords; i++ {
		err = ocf.Append([]interface{}{th.genAvroRecord()})
	}
	require.NoError(t, err)
}

// Generates test data with the specified format and returns avroRowStream object.
func (th *testHelper) genRecordsData(
	t *testing.T,
	format roachpb.AvroOptions_Format,
	numRecords int,
	recSeparator rune,
	records *bytes.Buffer,
) {
	var data []byte
	var err error

	for i := 0; i < numRecords; i++ {
		rec := th.genAvroRecord()

		if format == roachpb.AvroOptions_JSON_RECORDS {
			data, err = th.codec.TextualFromNative(nil, rec)
		} else if format == roachpb.AvroOptions_BIN_RECORDS {
			data, err = th.codec.BinaryFromNative(nil, rec)
		} else {
			t.Fatal("unexpected avro format")
		}

		require.NoError(t, err)

		records.Write(data)
		if recSeparator != 0 {
			records.WriteRune(recSeparator)
		}
	}
}

func TestReadsAvroRecords(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	th := newTestHelper(ctx, t)

	formats := []roachpb.AvroOptions_Format{
		roachpb.AvroOptions_BIN_RECORDS,
		roachpb.AvroOptions_JSON_RECORDS,
	}

	for _, format := range formats {
		for _, readSize := range []int{1, 16, 33, 64, 1024} {
			for _, skip := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v-%v-skip=%v", format, readSize, skip), func(t *testing.T) {
					stream := th.newRecordStream(t, format, false, 10)
					stream.producer.(*avroRecordStream).readSize = readSize

					var rowIdx int64
					for stream.producer.Scan() {
						var err error
						if skip {
							err = stream.producer.Skip()
						} else {
							err = stream.Row()
						}
						require.NoError(t, err)
						rowIdx++
					}

					require.NoError(t, stream.producer.Err())
					require.EqualValues(t, 10, rowIdx)
				})
			}
		}
	}
}

func TestReadsAvroOcf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	th := newTestHelper(ctx, t)

	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip=%v", skip), func(t *testing.T) {
			stream := th.newRecordStream(t, roachpb.AvroOptions_OCF, false, 10)
			var rowIdx int64
			for stream.producer.Scan() {
				var err error
				if skip {
					err = stream.producer.Skip()
				} else {
					err = stream.Row()
				}
				require.NoError(t, err)
				rowIdx++
			}

			require.NoError(t, stream.producer.Err())
			require.EqualValues(t, 10, rowIdx)
		})
	}
}

func TestRelaxedAndStrictImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		strict       bool
		excludeAvro  bool
		excludeTable bool
	}{
		{"relaxed-tolerates-missing-fields", false, true, false},
		{"relaxed-tolerates-extra-fields", false, false, true},
		{"relaxed-tolerates-missing-or-extra-fields", false, true, true},
		{"strict-returns-error-missing-fields", true, true, false},
		{"strict-returns-error-extra-fields", true, false, true},
		{"strict-returns-error-missing-or-extra-fields", true, true, true},
	}

	for f := range roachpb.AvroOptions_Format_name {
		for _, test := range tests {
			format := roachpb.AvroOptions_Format(f)
			t.Run(fmt.Sprintf("%s-%s", format, test.name), func(t *testing.T) {
				f1 := &seqGen{namedField: namedField{name: "f1"}}
				f2 := &seqGen{namedField: namedField{name: "f2"}}
				f1.excludeSQL = test.excludeTable
				f2.excludeAvro = test.excludeAvro

				th := newTestHelper(ctx, t, f1, f2)
				stream := th.newRecordStream(t, format, test.strict, 1)

				if !stream.producer.Scan() {
					t.Fatal("expected a record, found none")
				}
				err := stream.Row()
				if test.strict && err == nil {
					t.Fatal("expected to fail, but alas")
				}
				if !test.strict && err != nil {
					t.Fatal("expected to succeed, but alas;", err)
				}
			})
		}
	}
}

func TestHandlesArrayData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	th := newTestHelper(ctx, t, &intArrGen{namedField{
		name: "arr_of_ints",
	}})

	stream := th.newRecordStream(t, roachpb.AvroOptions_OCF, false, 10)
	var rowIdx int64
	for stream.producer.Scan() {
		if err := stream.Row(); err != nil {
			t.Fatal(err)
		}
		rowIdx++
	}

	require.NoError(t, stream.producer.Err())
	require.EqualValues(t, 10, rowIdx)
}

type limitAvroStream struct {
	avro       *avroInputReader
	limit      int
	readStream importRowProducer
	input      *os.File
	err        error
}

func (l *limitAvroStream) Skip() error {
	return nil
}

func (l *limitAvroStream) Progress() float32 {
	return 0
}

func (l *limitAvroStream) reopenStream() {
	_, l.err = l.input.Seek(0, 0)
	if l.err == nil {
		producer, _, err := newImportAvroPipeline(l.avro, &fileReader{Reader: l.input})
		l.err = err
		l.readStream = producer
	}
}

func (l *limitAvroStream) Scan() bool {
	l.limit--
	for l.limit >= 0 && l.err == nil {
		if l.readStream == nil {
			l.reopenStream()
			if l.err != nil {
				return false
			}
		}

		if l.readStream.Scan() {
			return true
		}

		// Force reopen the stream until we read enough data.
		l.err = l.readStream.Err()
		l.readStream = nil
	}
	return false
}

func (l *limitAvroStream) Err() error {
	return l.err
}

func (l *limitAvroStream) Row() (interface{}, error) {
	return l.readStream.Row()
}

var _ importRowProducer = &limitAvroStream{}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/ccl/importccl
// BenchmarkOCFImport-16    	  500000	      2612 ns/op	  45.93 MB/s
// BenchmarkOCFImport-16    	  500000	      2607 ns/op	  46.03 MB/s
// BenchmarkOCFImport-16    	  500000	      2719 ns/op	  44.13 MB/s
// BenchmarkOCFImport-16    	  500000	      2825 ns/op	  42.47 MB/s
// BenchmarkOCFImport-16    	  500000	      2924 ns/op	  41.03 MB/s
// BenchmarkOCFImport-16    	  500000	      2917 ns/op	  41.14 MB/s
// BenchmarkOCFImport-16    	  500000	      2926 ns/op	  41.01 MB/s
// BenchmarkOCFImport-16    	  500000	      2954 ns/op	  40.61 MB/s
// BenchmarkOCFImport-16    	  500000	      2942 ns/op	  40.78 MB/s
// BenchmarkOCFImport-16    	  500000	      2987 ns/op	  40.17 MB/s
func BenchmarkOCFImport(b *testing.B) {
	benchmarkAvroImport(b, roachpb.AvroOptions{
		Format: roachpb.AvroOptions_OCF,
	}, "testdata/avro/stock-10000.ocf")
}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/ccl/importccl
// BenchmarkBinaryJSONImport-16    	  500000	      3021 ns/op	  39.71 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      2991 ns/op	  40.11 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      3056 ns/op	  39.26 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      3075 ns/op	  39.02 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      3052 ns/op	  39.31 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      3101 ns/op	  38.69 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      3119 ns/op	  38.47 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      3237 ns/op	  37.06 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      3215 ns/op	  37.32 MB/s
// BenchmarkBinaryJSONImport-16    	  500000	      3235 ns/op	  37.09 MB/s
func BenchmarkBinaryJSONImport(b *testing.B) {
	schemaBytes, err := ioutil.ReadFile("testdata/avro/stock-schema.json")
	require.NoError(b, err)

	benchmarkAvroImport(b, roachpb.AvroOptions{
		Format:     roachpb.AvroOptions_BIN_RECORDS,
		SchemaJSON: string(schemaBytes),
	}, "testdata/avro/stock-10000.bjson")
}

func benchmarkAvroImport(b *testing.B, avroOpts roachpb.AvroOptions, testData string) {
	ctx := context.Background()

	b.SetBytes(120) // Raw input size. With 8 indexes, expect more on output side.

	stmt, err := parser.ParseOne(`CREATE TABLE stock (
    s_i_id       integer       not null,
    s_w_id       integer       not null,
    s_quantity   integer,
    s_dist_01    char(24),
    s_dist_02    char(24),
    s_dist_03    char(24),
    s_dist_04    char(24),
    s_dist_05    char(24),
    s_dist_06    char(24),
    s_dist_07    char(24),
    s_dist_08    char(24),
    s_dist_09    char(24),
    s_dist_10    char(24),
    s_ytd        integer,
    s_order_cnt  integer,
    s_remote_cnt integer,
    s_data       varchar(50),
    primary key (s_w_id, s_i_id)
)`)

	require.NoError(b, err)

	create := stmt.AST.(*tree.CreateTable)
	st := cluster.MakeTestingClusterSettings()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(st)

	tableDesc, err := MakeTestingSimpleTableDescriptor(ctx, &semaCtx, st, create, descpb.ID(100), keys.PublicSchemaID, descpb.ID(100), NoFKs, 1)
	require.NoError(b, err)

	kvCh := make(chan row.KVBatch)
	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	input, err := os.Open(testData)
	require.NoError(b, err)

	avro, err := newAvroInputReader(kvCh,
		tableDesc.ImmutableCopy().(catalog.TableDescriptor),
		avroOpts, 0, 0, &evalCtx)
	require.NoError(b, err)

	limitStream := &limitAvroStream{
		avro:  avro,
		limit: b.N,
		input: input,
	}
	_, consumer, err := newImportAvroPipeline(avro, &fileReader{Reader: input})
	require.NoError(b, err)
	b.ResetTimer()
	require.NoError(
		b, runParallelImport(ctx, avro.importContext, &importFileContext{}, limitStream, consumer))
	close(kvCh)
}
