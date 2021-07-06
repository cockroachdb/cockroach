// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func descForTable(
	ctx context.Context, t *testing.T, create string, parent, id descpb.ID, fks fkHandler,
) *tabledesc.Mutable {
	t.Helper()
	parsed, err := parser.Parse(create)
	if err != nil {
		t.Fatalf("could not parse %q: %v", create, err)
	}
	nanos := testEvalCtx.StmtTimestamp.UnixNano()

	settings := testEvalCtx.Settings

	var stmt *tree.CreateTable

	if len(parsed) == 2 {
		stmt = parsed[1].AST.(*tree.CreateTable)
		name := parsed[0].AST.(*tree.CreateSequence).Name.String()

		ts := hlc.Timestamp{WallTime: nanos}
		priv := descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName())
		desc, err := sql.NewSequenceTableDesc(
			ctx,
			name,
			tree.SequenceOptions{},
			parent,
			keys.PublicSchemaID,
			id-1,
			ts,
			priv,
			tree.PersistencePermanent,
			nil,   /* params */
			false, /* isMultiRegion */
		)
		if err != nil {
			t.Fatal(err)
		}
		fks.resolver.tableNameToDesc[name] = desc
	} else {
		stmt = parsed[0].AST.(*tree.CreateTable)
	}
	semaCtx := tree.MakeSemaContext()
	table, err := MakeTestingSimpleTableDescriptor(context.Background(), &semaCtx, settings, stmt, parent, keys.PublicSchemaID, id, fks, nanos)
	if err != nil {
		t.Fatalf("could not interpret %q: %v", create, err)
	}
	if err := fixDescriptorFKState(table); err != nil {
		t.Fatal(err)
	}
	return table
}

var testEvalCtx = &tree.EvalContext{
	SessionData: &sessiondata.SessionData{
		Location: time.UTC,
	},
	StmtTimestamp: timeutil.Unix(100000000, 0),
	Settings:      cluster.MakeTestingClusterSettings(),
	Codec:         keys.SystemSQLCodec,
}

// Value generator represents a value of some data at specified row/col.
type csvRow = int
type csvCol = int

type valueGenerator interface {
	getValue(r csvRow, c csvCol) string
}
type intGenerator struct{} // Returns row value
type strGenerator struct{} // Returns generated string

var _ valueGenerator = &intGenerator{}
var _ valueGenerator = &strGenerator{}

func (g *intGenerator) getValue(r csvRow, _ csvCol) string {
	return strconv.Itoa(r)
}

func (g *strGenerator) getValue(r csvRow, c csvCol) string {
	return fmt.Sprintf("data<r=%d;c=%d>", r, c)
}

// A breakpoint handler runs some breakpoint specific logic. A handler
// returns a boolean indicating if this breakpoint should remain active.
// An error result will cause csv generation to stop and the csv server
// to return that error.
type csvBpHandler = func() (bool, error)
type csvBreakpoint struct {
	row     csvRow
	enabled bool
	handler csvBpHandler
}

// csvGenerator generates csv data.
type csvGenerator struct {
	startRow    int
	numRows     int
	columns     []valueGenerator
	breakpoints map[int]*csvBreakpoint

	data      []string
	size      int
	rowPos    int // csvRow number we're emitting
	rowOffset int // Offset within the current row
}

const maxBreakpointPos = math.MaxInt32

var _ io.ReadCloser = &csvGenerator{}

func (csv *csvGenerator) Open() (io.ReadCloser, error) {
	csv.rowPos = 0
	csv.rowOffset = 0
	csv.maybeInitData()
	return csv, nil
}

// Note: we read one row at a time because reading as much as the
// buffer space allows might cause problems for breakpoints (e.g.
// a breakpoint may block until job progress is updated, but that
// update would never happen because we're still reading the data).
func (csv *csvGenerator) Read(p []byte) (n int, err error) {
	n = 0
	if n < cap(p) && csv.rowPos < len(csv.data) {
		if csv.rowOffset == 0 {
			if err = csv.handleBreakpoint(csv.rowPos); err != nil {
				return
			}
		}
		rowBytes := copy(p[n:], csv.data[csv.rowPos][csv.rowOffset:])
		csv.rowOffset += rowBytes
		n += rowBytes

		if rowBytes == len(csv.data[csv.rowPos]) {
			csv.rowOffset = 0
			csv.rowPos++
		}
	}

	if n == 0 {
		_ = csv.Close()
		err = io.EOF
	}

	return
}

func (csv *csvGenerator) maybeInitData() {
	if csv.data != nil {
		return
	}

	csv.data = make([]string, csv.numRows)
	csv.size = 0
	for i := 0; i < csv.numRows; i++ {
		csv.data[i] = ""
		for colIdx, gen := range csv.columns {
			if len(csv.data[i]) > 0 {
				csv.data[i] += ","
			}
			csv.data[i] += gen.getValue(i+csv.startRow, colIdx)
		}
		csv.data[i] += "\n"
		csv.size += len(csv.data[i])
	}
}

func (csv *csvGenerator) handleBreakpoint(idx int) (err error) {
	if bp, ok := csv.breakpoints[idx]; ok && bp.enabled {
		bp.enabled, err = bp.handler()
	}
	return
}

func (csv *csvGenerator) Close() error {
	// Execute breakpoint at the end of the stream.
	_ = csv.handleBreakpoint(maxBreakpointPos)
	csv.data = nil
	return nil
}

// Add a breakpoint to the generator.
func (csv *csvGenerator) addBreakpoint(r csvRow, handler csvBpHandler) *csvBreakpoint {
	csv.breakpoints[r] = &csvBreakpoint{
		row:     r,
		enabled: true,
		handler: handler,
	}
	return csv.breakpoints[r]
}

// Creates a new instance of csvGenerator, generating 'numRows', starting form
// the specified 'startRow'. The 'columns' list specifies data generators for
// each column. The optional 'breakpoints' specifies the list of csv breakpoints
// which allow caller to execute some code while generating csv data.
func newCsvGenerator(startRow int, numRows int, columns ...valueGenerator) *csvGenerator {
	return &csvGenerator{
		startRow:    startRow,
		numRows:     numRows,
		columns:     columns,
		breakpoints: make(map[int]*csvBreakpoint),
	}
}

// generatorExternalStorage is an external storage implementation
// that returns its data from the underlying generator.
type generatorExternalStorage struct {
	conf roachpb.ExternalStorage
	gen  *csvGenerator
}

var _ cloud.ExternalStorage = &generatorExternalStorage{}

func (es *generatorExternalStorage) Conf() roachpb.ExternalStorage {
	return es.conf
}

func (es *generatorExternalStorage) ReadFile(
	ctx context.Context, basename string,
) (io.ReadCloser, error) {
	return es.gen.Open()
}

func (es *generatorExternalStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (io.ReadCloser, int64, error) {
	panic("unimplemented")
}

func (es *generatorExternalStorage) Close() error {
	return nil
}

func (es *generatorExternalStorage) Size(ctx context.Context, basename string) (int64, error) {
	es.gen.maybeInitData()
	return int64(es.gen.size), nil
}

func (es *generatorExternalStorage) Writer(
	ctx context.Context, basename string,
) (io.WriteCloser, error) {
	return nil, errors.New("unsupported")
}

func (es *generatorExternalStorage) ListFiles(ctx context.Context, _ string) ([]string, error) {
	return nil, errors.New("unsupported")
}

func (es *generatorExternalStorage) List(
	ctx context.Context, _, _ string, _ cloud.ListingFn,
) error {
	return errors.New("unsupported")
}

func (es *generatorExternalStorage) Delete(ctx context.Context, basename string) error {
	return errors.New("unsupported")
}

func (es *generatorExternalStorage) ExternalIOConf() base.ExternalIODirConfig {
	return base.ExternalIODirConfig{}
}

func (es *generatorExternalStorage) Settings() *cluster.Settings {
	return cluster.NoSettings
}

// generatedStorage is a factory (cloud.ExternalStorageFactory)
// that returns the underlying csvGenerators The file name of the
// generated file doesn't matter: the first time we attempt to get a
// generator for a file, we return the next generator on the list.
type generatedStorage struct {
	generators []*csvGenerator
	nextID     int
	nameIDMap  map[string]int
}

func newGeneratedStorage(gens ...*csvGenerator) *generatedStorage {
	return &generatedStorage{
		generators: gens,
		nextID:     0,
		nameIDMap:  make(map[string]int),
	}
}

// Returns the list of file names (URIs) suitable for this factory.
func (ses *generatedStorage) getGeneratorURIs() []interface{} {
	// Names do not matter; all that matters is the number of generators.
	res := make([]interface{}, len(ses.generators))
	for i := range ses.generators {
		res[i] = fmt.Sprintf("http://host.does.not.matter/filename_is_meaningless%d", i)
	}
	return res
}
