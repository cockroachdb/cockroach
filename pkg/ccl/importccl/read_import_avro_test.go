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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/linkedin/goavro"
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
	schemaTable *sqlbase.TableDescriptor
	codec       *goavro.Codec
	gens        []avroGen
}

var defaultGens = []avroGen{
	&seqGen{namedField: namedField{name: "uid"}},
	&nilOrStrGen{namedField{name: "uname"}},
	&nilOrStrGen{namedField{name: "notes"}},
}

func newTestHelper(t *testing.T, gens ...avroGen) *testHelper {
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

	return &testHelper{
		schemaJSON:  string(schemaJSON),
		schemaTable: descForTable(t, createStmt, 10, 20, NoFKs),
		codec:       codec,
		gens:        gens,
	}
}

// Generates test data with the specified format and returns avroRowStream object.
func (th *testHelper) newRecordStream(
	t *testing.T, format roachpb.AvroOptions_Format, strict bool, numRecords int,
) avroRowStream {
	// Ensure datum converter doesn't flush (since
	// we're using nil kv channel for this test).
	defer row.TestingSetDatumRowConverterBatchSize(numRecords + 1)()
	evalCtx := tree.MakeTestingEvalContext(nil)
	conv, err := row.NewDatumRowConverter(context.Background(), th.schemaTable, nil, &evalCtx, nil)
	require.NoError(t, err)

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

	stream, err := newRowStream(opts, conv, &fileReader{Reader: records})
	require.NoError(t, err)
	return stream
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
	th := newTestHelper(t)

	formats := []roachpb.AvroOptions_Format{
		roachpb.AvroOptions_BIN_RECORDS,
		roachpb.AvroOptions_JSON_RECORDS,
	}

	for _, format := range formats {
		for _, readSize := range []int{1, 16, 33, 64, 1024} {
			for _, skip := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v-%v-skip=%v", format, readSize, skip), func(t *testing.T) {
					stream := th.newRecordStream(t, format, false, 10)
					stream.(*avroRecordStream).readSize = readSize

					var rowIdx int64
					for stream.Scan() {
						var err error
						if skip {
							err = stream.Skip()
						} else {
							err = stream.Row(context.TODO(), 0, rowIdx)
						}
						require.NoError(t, err)
						rowIdx++
					}

					require.NoError(t, stream.Err())
					require.EqualValues(t, 10, rowIdx)
				})
			}
		}
	}
}

func TestReadsAvroOcf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	th := newTestHelper(t)

	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip=%v", skip), func(t *testing.T) {
			stream := th.newRecordStream(t, roachpb.AvroOptions_OCF, false, 10)
			var rowIdx int64
			for stream.Scan() {
				var err error
				if skip {
					err = stream.Skip()
				} else {
					err = stream.Row(context.TODO(), 0, rowIdx)
				}
				require.NoError(t, err)
				rowIdx++
			}

			require.NoError(t, stream.Err())
			require.EqualValues(t, 10, rowIdx)
		})
	}
}

func TestRelaxedAndStrictImport(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

				th := newTestHelper(t, f1, f2)
				stream := th.newRecordStream(t, format, test.strict, 1)

				if !stream.Scan() {
					t.Fatal("expected a record, found none")
				}
				err := stream.Row(context.TODO(), 0, 0)
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
	th := newTestHelper(t, &intArrGen{namedField{
		name: "arr_of_ints",
	}})

	stream := th.newRecordStream(t, roachpb.AvroOptions_OCF, false, 10)
	var rowIdx int64
	for stream.Scan() {
		if err := stream.Row(context.TODO(), 0, rowIdx); err != nil {
			t.Fatal(err)
		}
		rowIdx++
	}

	require.NoError(t, stream.Err())
	require.EqualValues(t, 10, rowIdx)
}

type limitAvroStream struct {
	limit    int
	input    *os.File
	avroOpts roachpb.AvroOptions
	conv     *row.DatumRowConverter
	stream   avroRowStream
	err      error
}

func (l *limitAvroStream) reopenStream() {
	_, l.err = l.input.Seek(0, 0)
	if l.err == nil {
		l.stream, l.err = newRowStream(l.avroOpts, l.conv, &fileReader{Reader: l.input})
	}
}

func (l *limitAvroStream) Scan() bool {
	l.limit--
	for l.limit > 0 {
		if l.stream == nil {
			l.reopenStream()
			if l.err != nil {
				return false
			}
		}

		if l.stream.Scan() {
			return true
		}

		l.err = l.stream.Err()
		l.stream = nil
	}
	return false
}

func (l *limitAvroStream) Err() error {
	return l.err
}

func (l *limitAvroStream) Skip() error {
	return l.stream.Skip()
}

func (l *limitAvroStream) Row(ctx context.Context, sourceID int32, rowIndex int64) error {
	return l.stream.Row(ctx, sourceID, rowIndex)
}

var _ avroRowStream = &limitAvroStream{}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/ccl/importccl
// BenchmarkOCFImport-16    	  200000	     11698 ns/op	  10.26 MB/s
func BenchmarkOCFImport(b *testing.B) {
	benchmarkAvroImport(b, roachpb.AvroOptions{
		Format: roachpb.AvroOptions_OCF,
	}, "testdata/avro/stock-10000.ocf")
}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/ccl/importccl
// BenchmarkBinaryJSONImport-16    	  200000	     11774 ns/op	  10.19 MB/s
func BenchmarkBinaryJSONImport(b *testing.B) {
	schemaBytes, err := ioutil.ReadFile("testdata/avro/stock-schema.json")
	require.NoError(b, err)

	benchmarkAvroImport(b, roachpb.AvroOptions{
		Format:     roachpb.AvroOptions_BIN_RECORDS,
		SchemaJSON: string(schemaBytes),
	}, "testdata/avro/stock-10000.bjson")
}

func benchmarkAvroImport(b *testing.B, avroOpts roachpb.AvroOptions, testData string) {
	ctx := context.TODO()

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
		primary key (s_w_id, s_i_id),
    index stock_item_fk_idx (s_i_id))
  `)

	require.NoError(b, err)

	create := stmt.AST.(*tree.CreateTable)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	tableDesc, err := MakeSimpleTableDescriptor(ctx, st, create, sqlbase.ID(100), sqlbase.ID(100), NoFKs, 1)
	require.NoError(b, err)

	kvCh := make(chan row.KVBatch)
	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	conv, err := row.NewDatumRowConverter(ctx, tableDesc.TableDesc(), nil /* targetColNames */, &evalCtx, kvCh)
	require.NoError(b, err)

	input, err := os.Open(testData)
	require.NoError(b, err)

	limitStream := &limitAvroStream{
		limit:    b.N,
		input:    input,
		avroOpts: avroOpts,
		conv:     conv,
	}

	b.ResetTimer()
	require.NoError(b, importAvro(ctx, limitStream, 0, 0, conv))
	close(kvCh)
}
