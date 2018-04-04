// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package workload provides an abstraction for generators of sql query loads
// (and requisite initial data) as well as tools for working with these
// generators.
package workload

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
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
	Ops(urls []string, reg *HistogramRegistry) (QueryLoad, error)
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
	// PreLoad is called after workload tables are created and before workload
	// data is loaded. It is not called when storing or loading a fixture.
	PreLoad func(*gosql.DB) error
	// PostLoad is called after workload tables are created workload data is
	// loaded. It called after restoring a fixture. This, for example, is where
	// creating foreign keys should go.
	PostLoad func(*gosql.DB) error
	// PostRun is called after workload run has ended, with the start time of the
	// run. This is where any post-run special printing or validation can be done.
	PostRun func(time.Time) error
	// CheckConsistency is called to run generator-specific consistency checks.
	// These are expected to pass after the initial data load as well as after
	// running queryload.
	CheckConsistency func(context.Context, *gosql.DB) error
}

// Meta is used to register a Generator at init time and holds meta information
// about this generator, including a name, description, and a function to create
// instances of it.
type Meta struct {
	// Name is a unique name for this generator.
	Name string
	// Description is a short description of this generator.
	Description string
	// Version is a semantic version for this generator. It should be bumped
	// whenever InitialRowFn or InitialRowCount change for any of the tables.
	Version string
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
	// setup is completed.
	InitialRows BatchedTuples
	// Splits is the initial splits that will be present in the table after
	// setup is completed.
	Splits BatchedTuples
}

// BatchedTuples is a generic generator of tuples (SQL rows, PKs to split at,
// etc). Tuples are generated in batches of arbitrary size. Each batch has an
// index in `[0,NumBatches)` and a batch can be generated given only its index.
type BatchedTuples struct {
	// NumBatches is the number of batches of tuples.
	NumBatches int
	// NumTotal is the total number of tuples in all batches. Not all generators
	// will know this, it's set to 0 when unknown.
	NumTotal int
	// Batch is a function to deterministically compute a batch of tuples given
	// its index.
	Batch func(int) [][]interface{}
}

// Tuples returns a BatchedTuples where each batch has size 1.
func Tuples(count int, fn func(int) []interface{}) BatchedTuples {
	return BatchedTuples{
		NumBatches: count,
		NumTotal:   count,
		Batch: func(batchIdx int) [][]interface{} {
			return [][]interface{}{fn(batchIdx)}
		},
	}
}

// QueryLoad represents some SQL query workload performable on a database
// initialized with the requisite tables.
type QueryLoad struct {
	SQLDatabase string

	// WorkerFns is one function per worker. It is to be called once per unit of
	// work to be done.
	WorkerFns []func(context.Context) error

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

// ApproxDatumSize returns the canonical size of a datum as returned from a call
// to `Table.InitialRowFn`. NB: These datums end up getting serialized in
// different ways, which means there's no one size that will be correct for all
// of them.
func ApproxDatumSize(x interface{}) int64 {
	if x == nil {
		return 0
	}
	switch t := x.(type) {
	case int:
		if t < 0 {
			t = -t
		}
		// This and float64 are `+8` so a `0` results in `1`. This function is
		// used to batch things by size and table of all `0`s should not get
		// infinite size batches.
		return int64(bits.Len(uint(t))+8) / 8
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
		panic(fmt.Sprintf("unsupported type %T: %v", x, x))
	}
}

// Setup creates the given tables and fills them with initial data via batched
// INSERTs. batchSize will only be used when positive (but INSERTs are batched
// either way).
//
// The size of the loaded data is returned in bytes, suitable for use with
// SetBytes of benchmarks. The exact definition of this is deferred to the
// ApproxDatumSize implementation.
func Setup(
	ctx context.Context, db *gosql.DB, gen Generator, batchSize, concurrency int,
) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if concurrency < 1 {
		concurrency = 1
	}

	tables := gen.Tables()
	var hooks Hooks
	if h, ok := gen.(Hookser); ok {
		hooks = h.Hooks()
	}

	for _, table := range tables {
		createStmt := fmt.Sprintf(`CREATE TABLE "%s" %s`, table.Name, table.Schema)
		if _, err := db.ExecContext(ctx, createStmt); err != nil {
			return 0, errors.Wrapf(err, "could not create table: %s", table.Name)
		}
	}

	if hooks.PreLoad != nil {
		if err := hooks.PreLoad(db); err != nil {
			return 0, err
		}
	}

	var size int64
	for _, table := range tables {
		if table.InitialRows.Batch == nil {
			// Some workloads don't support initial table data.
			continue
		}
		batchesPerWorker := table.InitialRows.NumBatches / concurrency
		g, gCtx := errgroup.WithContext(ctx)
		for i := 0; i < concurrency; i++ {
			startIdx := i * batchesPerWorker
			endIdx := startIdx + batchesPerWorker
			if i == concurrency-1 {
				// Account for any rounding error in batchesPerWorker.
				endIdx = table.InitialRows.NumBatches
			}
			g.Go(func() error {
				var insertStmtBuf bytes.Buffer
				var params []interface{}
				var numRows int
				flush := func() error {
					if len(params) > 0 {
						insertStmt := insertStmtBuf.String()
						if _, err := db.ExecContext(gCtx, insertStmt, params...); err != nil {
							return errors.Wrapf(err, "failed insert into %s", table.Name)
						}
					}
					insertStmtBuf.Reset()
					fmt.Fprintf(&insertStmtBuf, `INSERT INTO "%s" VALUES `, table.Name)
					params = params[:0]
					numRows = 0
					return nil
				}
				_ = flush()

				for rowBatchIdx := startIdx; rowBatchIdx < endIdx; rowBatchIdx++ {
					for _, row := range table.InitialRows.Batch(rowBatchIdx) {
						if len(params) != 0 {
							insertStmtBuf.WriteString(`,`)
						}
						insertStmtBuf.WriteString(`(`)
						for i, datum := range row {
							atomic.AddInt64(&size, ApproxDatumSize(datum))
							if i != 0 {
								insertStmtBuf.WriteString(`,`)
							}
							fmt.Fprintf(&insertStmtBuf, `$%d`, len(params)+i+1)
						}
						params = append(params, row...)
						insertStmtBuf.WriteString(`)`)
						if numRows++; numRows >= batchSize {
							if err := flush(); err != nil {
								return err
							}
						}
					}
				}
				return flush()
			})
		}
		if err := g.Wait(); err != nil {
			return 0, err
		}
	}

	if hooks.PostLoad != nil {
		if err := hooks.PostLoad(db); err != nil {
			return 0, err
		}
	}

	return size, nil
}

// Split creates the range splits defined by the given table.
func Split(ctx context.Context, db *gosql.DB, table Table, concurrency int) error {
	if table.Splits.NumBatches <= 0 {
		return nil
	}
	splitPoints := make([][]interface{}, 0, table.Splits.NumBatches)
	for splitIdx := 0; splitIdx < table.Splits.NumBatches; splitIdx++ {
		splitPoints = append(splitPoints, table.Splits.Batch(splitIdx)...)
	}
	sort.Sort(sliceSliceInterface(splitPoints))

	type pair struct {
		lo, hi int
	}
	splitCh := make(chan pair, concurrency)
	splitCh <- pair{0, len(splitPoints)}
	doneCh := make(chan struct{})
	errCh := make(chan error)

	log.Infof(ctx, `starting %d splits`, len(splitPoints))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		var buf bytes.Buffer
		go func() {
			defer wg.Done()
			for {
				var p pair
				select {
				case p = <-splitCh:
				case <-doneCh:
					return
				}
				m := (p.lo + p.hi) / 2
				split := strings.Join(StringTuple(splitPoints[m]), `,`)

				buf.Reset()
				fmt.Fprintf(&buf, `ALTER TABLE %s SPLIT AT VALUES (%s)`, table.Name, split)
				if _, err := db.Exec(buf.String()); err != nil {
					errCh <- errors.Wrap(err, buf.String())
					return
				}

				buf.Reset()
				fmt.Fprintf(&buf, `ALTER TABLE %s SCATTER FROM (%s) TO (%s)`,
					table.Name, split, split)
				if _, err := db.Exec(buf.String()); err != nil {
					// SCATTER can collide with normal replicate queue
					// operations and fail spuriously, so only print the
					// error.
					log.Warningf(ctx, `%s: %s`, buf.String(), err)
				}

				errCh <- nil
				go func() {
					if p.lo < m {
						splitCh <- pair{p.lo, m}
					}
					if m+1 < p.hi {
						splitCh <- pair{m + 1, p.hi}
					}
				}()
			}
		}()
	}

	defer func() {
		close(doneCh)
		wg.Wait()
	}()
	for finished := 1; finished <= len(splitPoints); finished++ {
		if err := <-errCh; err != nil {
			return err
		}
		if finished%1000 == 0 {
			log.Infof(ctx, "finished %d of %d splits", finished, len(splitPoints))
		}
	}
	return nil
}

// StringTuple returns the given datums as strings suitable for use in directly
// in SQL.
//
// TODO(dan): Remove this once SCATTER supports placeholders.
func StringTuple(datums []interface{}) []string {
	s := make([]string, len(datums))
	for i, datum := range datums {
		if datum == nil {
			s[i] = `NULL`
			continue
		}
		switch x := datum.(type) {
		case int:
			s[i] = strconv.Itoa(x)
		case string:
			s[i] = lex.EscapeSQLString(x)
		case float64:
			s[i] = fmt.Sprintf(`%f`, x)
		case []byte:
			s[i] = fmt.Sprintf(`X'%x'`, x)
		default:
			panic(fmt.Sprintf("unsupported type %T: %v", x, x))
		}
	}
	return s
}

type sliceSliceInterface [][]interface{}

func (s sliceSliceInterface) Len() int      { return len(s) }
func (s sliceSliceInterface) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sliceSliceInterface) Less(i, j int) bool {
	for offset := 0; ; offset++ {
		iLen, jLen := len(s[i]), len(s[j])
		if iLen <= offset || jLen <= offset {
			return iLen < jLen
		}
		var cmp int
		switch x := s[i][offset].(type) {
		case int:
			if y := s[j][offset].(int); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case uint64:
			if y := s[j][offset].(uint64); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case string:
			cmp = strings.Compare(x, s[j][offset].(string))
		default:
			panic(fmt.Sprintf("unsupported type %T: %v", x, x))
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
}
