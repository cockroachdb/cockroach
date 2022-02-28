// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package workload provides an abstraction for generators of sql query loads
// (and requisite initial data) as well as tools for working with these
// generators.
package workload

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// Generator represents one or more sql query loads and associated initial data.
type Generator interface {
	// Meta returns meta information about this generator, including a name,
	// description, and a function to create instances of it.
	Meta() Meta

	// Tables returns the set of tables for this generator, including schemas
	// and initial data.
	Tables() []Table
}

// FlagMeta is metadata about a workload flag.
type FlagMeta struct {
	// RuntimeOnly may be set to true only if the corresponding flag has no
	// impact on the behavior of any Tables in this workload.
	RuntimeOnly bool
	// CheckConsistencyOnly is expected to be true only if the corresponding
	// flag only has an effect on the CheckConsistency hook.
	CheckConsistencyOnly bool
}

// Flags is a container for flags and associated metadata.
type Flags struct {
	*pflag.FlagSet
	// Meta is keyed by flag name and may be nil if no metadata is needed.
	Meta map[string]FlagMeta
}

// Flagser returns the flags this Generator is configured with. Any randomness
// in the Generator must be deterministic from these options so that table data
// initialization, query work, etc can be distributed by sending only these
// flags.
type Flagser interface {
	Generator
	Flags() Flags
}

// Opser returns the work functions for this generator. The tables are required
// to have been created and initialized before running these.
type Opser interface {
	Generator
	Ops(ctx context.Context, urls []string, reg *histogram.Registry) (QueryLoad, error)
}

// Hookser returns any hooks associated with the generator.
type Hookser interface {
	Generator
	Hooks() Hooks
}

// Hooks stores functions to be called at points in the workload lifecycle.
type Hooks struct {
	// Validate is called after workload flags are parsed. It should return an
	// error if the workload configuration is invalid.
	Validate func() error
	// PreCreate is called before workload tables are created.
	// Implementations should be idempotent.
	PreCreate func(*gosql.DB) error
	// PreLoad is called after workload tables are created and before workload
	// data is loaded. It is not called when storing or loading a fixture.
	// Implementations should be idempotent.
	//
	// TODO(dan): Deprecate the PreLoad hook, it doesn't play well with fixtures.
	// It's only used in practice for zone configs, so it should be reasonably
	// straightforward to make zone configs first class citizens of
	// workload.Table.
	PreLoad func(*gosql.DB) error
	// PostLoad is called after workload tables are created workload data is
	// loaded. It called after restoring a fixture. This, for example, is where
	// creating foreign keys should go. Implementations should be idempotent.
	PostLoad func(*gosql.DB) error
	// PostRun is called after workload run has ended, with the duration of the
	// run. This is where any post-run special printing or validation can be done.
	PostRun func(time.Duration) error
	// CheckConsistency is called to run generator-specific consistency checks.
	// These are expected to pass after the initial data load as well as after
	// running queryload.
	CheckConsistency func(context.Context, *gosql.DB) error
	// Partition is used to run a partitioning step on the data created by the workload.
	// TODO (rohany): migrate existing partitioning steps (such as tpcc's) into here.
	Partition func(*gosql.DB) error
}

// Meta is used to register a Generator at init time and holds meta information
// about this generator, including a name, description, and a function to create
// instances of it.
type Meta struct {
	// Name is a unique name for this generator.
	Name string
	// Description is a short description of this generator.
	Description string
	// Details optionally allows specifying longer, more in-depth usage details.
	Details string
	// Version is a semantic version for this generator. It should be bumped
	// whenever InitialRowFn or InitialRowCount change for any of the tables.
	Version string
	// PublicFacing indicates that this workload is also intended for use by
	// users doing their own testing and evaluations. This allows hiding workloads
	// that are only expected to be used in CockroachDB's internal development to
	// avoid confusion. Workloads setting this to true should pay added attention
	// to their documentation and help-text.
	PublicFacing bool
	// New returns an unconfigured instance of this generator.
	New func() Generator
}

// Table represents a single table in a Generator. Included is a name, schema,
// and initial data.
type Table struct {
	// Name is the unqualified table name, pre-escaped for use directly in SQL.
	Name string
	// Schema is the SQL formatted schema for this table, with the `CREATE TABLE
	// <name>` prefix omitted.
	Schema string
	// InitialRows is the initial rows that will be present in the table after
	// setup is completed. Note that the default value of NumBatches (zero) is
	// special - such a Table will be skipped during `init`; non-zero NumBatches
	// with a nil FillBatch function will trigger an error during `init`.
	InitialRows BatchedTuples
	// Splits is the initial splits that will be present in the table after
	// setup is completed.
	Splits BatchedTuples
	// Stats is the pre-calculated set of statistics on this table. They can be
	// injected using `ALTER TABLE <name> INJECT STATISTICS ...`.
	Stats []JSONStatistic
}

// BatchedTuples is a generic generator of tuples (SQL rows, PKs to split at,
// etc). Tuples are generated in batches of arbitrary size. Each batch has an
// index in `[0,NumBatches)` and a batch can be generated given only its index.
type BatchedTuples struct {
	// NumBatches is the number of batches of tuples.
	NumBatches int
	// FillBatch is a function to deterministically compute a columnar-batch of
	// tuples given its index.
	//
	// To save allocations, the Vecs in the passed Batch are reused when possible,
	// so the results of this call are invalidated the next time the same Batch is
	// passed to FillBatch. Ditto the ByteAllocator, which can be reset in between
	// calls. If a caller needs the Batch and its contents to be long lived,
	// simply pass a new Batch to each call and don't reset the ByteAllocator.
	FillBatch func(int, coldata.Batch, *bufalloc.ByteAllocator)
}

// Tuples is like TypedTuples except that it tries to guess the type of each
// datum. However, if the function ever returns nil for one of the datums, you
// need to use TypedTuples instead and specify the types.
func Tuples(count int, fn func(int) []interface{}) BatchedTuples {
	return TypedTuples(count, nil /* typs */, fn)
}

const (
	// timestampOutputFormat is used to output all timestamps.
	timestampOutputFormat = "2006-01-02 15:04:05.999999-07:00"
)

// TypedTuples returns a BatchedTuples where each batch has size 1. It's
// intended to be easier to use than directly specifying a BatchedTuples, but
// the tradeoff is some bit of performance. If typs is nil, an attempt is
// made to infer them.
func TypedTuples(count int, typs []*types.T, fn func(int) []interface{}) BatchedTuples {
	// The FillBatch we create has to be concurrency safe, so we can't let it do
	// the one-time initialization of typs without this protection.
	var typesOnce sync.Once

	t := BatchedTuples{
		NumBatches: count,
	}
	if fn != nil {
		t.FillBatch = func(batchIdx int, cb coldata.Batch, _ *bufalloc.ByteAllocator) {
			row := fn(batchIdx)

			typesOnce.Do(func() {
				if typs == nil {
					typs = make([]*types.T, len(row))
					for i, datum := range row {
						if datum == nil {
							panic(fmt.Sprintf(
								`can't determine type of nil column; call TypedTuples directly: %v`, row))
						} else {
							switch datum.(type) {
							case time.Time:
								typs[i] = types.Bytes
							default:
								typs[i] = typeconv.UnsafeFromGoType(datum)
							}
						}
					}
				}
			})

			cb.Reset(typs, 1, coldata.StandardColumnFactory)
			for colIdx, col := range cb.ColVecs() {
				switch d := row[colIdx].(type) {
				case nil:
					col.Nulls().SetNull(0)
				case bool:
					col.Bool()[0] = d
				case int:
					col.Int64()[0] = int64(d)
				case float64:
					col.Float64()[0] = d
				case string:
					col.Bytes().Set(0, []byte(d))
				case []byte:
					col.Bytes().Set(0, d)
				case time.Time:
					col.Bytes().Set(0, []byte(d.Round(time.Microsecond).UTC().Format(timestampOutputFormat)))
				default:
					panic(errors.AssertionFailedf(`unhandled datum type %T`, d))
				}
			}
		}
	}
	return t
}

// BatchRows is a function to deterministically compute a row-batch of tuples
// given its index. BatchRows doesn't attempt any reuse and so is allocation
// heavy. In performance-critical code, FillBatch should be used directly,
// instead.
func (b BatchedTuples) BatchRows(batchIdx int) [][]interface{} {
	cb := coldata.NewMemBatchWithCapacity(nil, 0, coldata.StandardColumnFactory)
	var a bufalloc.ByteAllocator
	b.FillBatch(batchIdx, cb, &a)
	return ColBatchToRows(cb)
}

// ColBatchToRows materializes the columnar data in a coldata.Batch into rows.
func ColBatchToRows(cb coldata.Batch) [][]interface{} {
	numRows, numCols := cb.Length(), cb.Width()
	// Allocate all the []interface{} row slices in one go.
	datums := make([]interface{}, numRows*numCols)
	for colIdx, col := range cb.ColVecs() {
		nulls := col.Nulls()
		switch col.CanonicalTypeFamily() {
		case types.BoolFamily:
			for rowIdx, datum := range col.Bool()[:numRows] {
				if !nulls.NullAt(rowIdx) {
					datums[rowIdx*numCols+colIdx] = datum
				}
			}
		case types.IntFamily:
			switch col.Type().Width() {
			case 0, 64:
				for rowIdx, datum := range col.Int64()[:numRows] {
					if !nulls.NullAt(rowIdx) {
						datums[rowIdx*numCols+colIdx] = datum
					}
				}
			case 16:
				for rowIdx, datum := range col.Int16()[:numRows] {
					if !nulls.NullAt(rowIdx) {
						datums[rowIdx*numCols+colIdx] = datum
					}
				}
			default:
				panic(fmt.Sprintf(`unhandled type %s`, col.Type()))
			}
		case types.FloatFamily:
			for rowIdx, datum := range col.Float64()[:numRows] {
				if !nulls.NullAt(rowIdx) {
					datums[rowIdx*numCols+colIdx] = datum
				}
			}
		case types.BytesFamily:
			// HACK: workload's Table schemas are SQL schemas, but the initial data is
			// returned as a coldata.Batch, which has a more limited set of types.
			// (Or, in the case of simple workloads that return a []interface{}, it's
			// roundtripped through coldata.Batch by the `Tuples` helper.)
			//
			// Notably, this means a SQL STRING column is represented the same as a
			// BYTES column (ditto UUID, etc). We could get the fidelity back by
			// parsing the SQL schema, which in fact we do in
			// `importer.makeDatumFromColOffset`. At the moment, the set of types
			// used in workloads is limited enough that the users of initial
			// data/splits are okay with the fidelity loss. So, to avoid the
			// complexity and the undesirable pkg/sql/parser dep, we simply treat them
			// all as bytes and let the caller deal with the ambiguity.
			colBytes := col.Bytes()
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				if !nulls.NullAt(rowIdx) {
					datums[rowIdx*numCols+colIdx] = colBytes.Get(rowIdx)
				}
			}
		default:
			panic(fmt.Sprintf(`unhandled type %s`, col.Type()))
		}
	}
	rows := make([][]interface{}, numRows)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		rows[rowIdx] = datums[rowIdx*numCols : (rowIdx+1)*numCols]
	}
	return rows
}

// InitialDataLoader loads the initial data for all tables in a workload. It
// returns a measure of how many bytes were loaded.
//
// TODO(dan): It would be lovely if the number of bytes loaded was comparable
// between implementations but this is sadly not the case right now.
type InitialDataLoader interface {
	InitialDataLoad(context.Context, *gosql.DB, Generator) (int64, error)
}

// ImportDataLoader is a hook for binaries that include CCL code to inject an
// IMPORT-based InitialDataLoader implementation.
var ImportDataLoader InitialDataLoader = requiresCCLBinaryDataLoader(`IMPORT`)

type requiresCCLBinaryDataLoader string

func (l requiresCCLBinaryDataLoader) InitialDataLoad(
	context.Context, *gosql.DB, Generator,
) (int64, error) {
	return 0, errors.Errorf(`loading initial data with %s requires a CCL binary`, l)
}

// QueryLoad represents some SQL query workload performable on a database
// initialized with the requisite tables.
type QueryLoad struct {
	SQLDatabase string

	// WorkerFns is one function per worker. It is to be called once per unit of
	// work to be done.
	WorkerFns []func(context.Context) error

	// Close, if set, is called before the process exits, giving workloads a
	// chance to print some information.
	// It's guaranteed that the ctx passed to WorkerFns (if they're still running)
	// has been canceled by the time this is called (so an implementer can
	// synchronize with the WorkerFns if need be).
	Close func(context.Context)

	// ResultHist is the name of the NamedHistogram to use for the benchmark
	// formatted results output at the end of `./workload run`. The empty string
	// will use the sum of all histograms.
	//
	// TODO(dan): This will go away once more of run.go moves inside Operations.
	ResultHist string
}

var registered = make(map[string]Meta)

// Register is a hook for init-time registration of Generator implementations.
// This allows only the necessary generators to be compiled into a given binary.
func Register(m Meta) {
	if _, ok := registered[m.Name]; ok {
		panic(m.Name + " is already registered")
	}
	registered[m.Name] = m
}

// Get returns the registered Generator with the given name, if it exists.
func Get(name string) (Meta, error) {
	m, ok := registered[name]
	if !ok {
		return Meta{}, errors.Errorf("unknown generator: %s", name)
	}
	return m, nil
}

// Registered returns all registered Generators.
func Registered() []Meta {
	gens := make([]Meta, 0, len(registered))
	for _, gen := range registered {
		gens = append(gens, gen)
	}
	sort.Slice(gens, func(i, j int) bool { return strings.Compare(gens[i].Name, gens[j].Name) < 0 })
	return gens
}

// FromFlags returns a new validated generator with the given flags. If anything
// goes wrong, it panics. FromFlags is intended for use with unit test helpers
// in individual generators, see its callers for examples.
func FromFlags(meta Meta, flags ...string) Generator {
	gen := meta.New()
	if len(flags) > 0 {
		f, ok := gen.(Flagser)
		if !ok {
			panic(fmt.Sprintf(`generator %s does not accept flags: %v`, meta.Name, flags))
		}
		flagsStruct := f.Flags()
		if err := flagsStruct.Parse(flags); err != nil {
			panic(fmt.Sprintf(`generator %s parsing flags %v: %v`, meta.Name, flags, err))
		}
	}
	if h, ok := gen.(Hookser); ok {
		if err := h.Hooks().Validate(); err != nil {
			panic(fmt.Sprintf(`generator %s flags %s did not validate: %v`, meta.Name, flags, err))
		}
	}
	return gen
}

// ApproxDatumSize returns the canonical size of a datum as returned from a call
// to `Table.InitialRowFn`. NB: These datums end up getting serialized in
// different ways, which means there's no one size that will be correct for all
// of them.
func ApproxDatumSize(x interface{}) int64 {
	if x == nil {
		return 0
	}
	switch t := x.(type) {
	case bool:
		return 1
	case int:
		if t < 0 {
			t = -t
		}
		// This and float64 are `+8` so a `0` results in `1`. This function is
		// used to batch things by size and table of all `0`s should not get
		// infinite size batches.
		return int64(bits.Len(uint(t))+8) / 8
	case int64:
		return int64(bits.Len64(uint64(t))+8) / 8
	case int16:
		return int64(bits.Len64(uint64(t))+8) / 8
	case uint64:
		return int64(bits.Len64(t)+8) / 8
	case float64:
		return int64(bits.Len64(math.Float64bits(t))+8) / 8
	case string:
		return int64(len(t))
	case []byte:
		return int64(len(t))
	case time.Time:
		return 12
	default:
		panic(errors.AssertionFailedf("unsupported type %T: %v", x, x))
	}
}
