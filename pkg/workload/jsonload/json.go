// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jsonload

import (
	"bytes"
	"context"
	"crypto/sha1"
	gosql "database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	jsonSchema                  = `(k BIGINT NOT NULL PRIMARY KEY, v JSONB NOT NULL)`
	jsonSchemaWithInvertedIndex = `(k BIGINT NOT NULL PRIMARY KEY, v JSONB NOT NULL, INVERTED INDEX (v))`
	jsonSchemaWithComputed      = `(k BIGINT AS (v->>'key')::BIGINT STORED PRIMARY KEY, v JSONB NOT NULL)`
)

type jsonLoad struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	batchSize      int
	cycleLength    int64
	readPercent    int
	writeSeq, seed int64
	sequential     bool
	splits         int
	complexity     int
	inverted       bool
	computed       bool
}

func init() {
	workload.Register(jsonLoadMeta)
}

var jsonLoadMeta = workload.Meta{
	Name: `json`,
	Description: `JSON reads and writes to keys spread (by default, uniformly` +
		` at random) across the cluster`,
	Version: `1.0.0`,
	New: func() workload.Generator {
		g := &jsonLoad{}
		g.flags.FlagSet = pflag.NewFlagSet(`json`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`batch`: {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.batchSize, `batch`, 1, `Number of blocks to insert in a single SQL statement`)
		g.flags.Int64Var(&g.cycleLength, `cycle-length`, math.MaxInt64, `Number of keys repeatedly accessed by each writer`)
		g.flags.IntVar(&g.readPercent, `read-percent`, 0, `Percent (0-100) of operations that are reads of existing keys`)
		g.flags.Int64Var(&g.writeSeq, `write-seq`, 0, `Initial write sequence value.`)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.BoolVar(&g.sequential, `sequential`, false, `Pick keys sequentially instead of randomly`)
		g.flags.IntVar(&g.splits, `splits`, 0, `Number of splits to perform before starting normal operations`)
		g.flags.IntVar(&g.complexity, `complexity`, 20, `Complexity of generated JSON data`)
		g.flags.BoolVar(&g.inverted, `inverted`, false, `Whether to include an inverted index`)
		g.flags.BoolVar(&g.computed, `computed`, false, `Whether to use a computed primary key`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*jsonLoad) Meta() workload.Meta { return jsonLoadMeta }

// Flags implements the Flagser interface.
func (w *jsonLoad) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *jsonLoad) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.computed && w.inverted {
				return errors.Errorf("computed and inverted cannot be used together")
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (w *jsonLoad) Tables() []workload.Table {
	schema := jsonSchema
	if w.inverted {
		schema = jsonSchemaWithInvertedIndex
	} else if w.computed {
		schema = jsonSchemaWithComputed
	}
	table := workload.Table{
		Name:   `j`,
		Schema: schema,
		Splits: workload.Tuples(
			w.splits,
			func(splitIdx int) []interface{} {
				rng := rand.New(rand.NewSource(w.seed + int64(splitIdx)))
				g := newHashGenerator(&sequence{config: w, val: w.writeSeq})
				return []interface{}{
					int(g.hash(rng.Int63())),
				}
			},
		),
	}
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (w *jsonLoad) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	var buf bytes.Buffer
	buf.WriteString(`SELECT k, v FROM j WHERE k IN (`)
	for i := 0; i < w.batchSize; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `$%d`, i+1)
	}
	buf.WriteString(`)`)
	readStmt, err := db.Prepare(buf.String())
	if err != nil {
		return workload.QueryLoad{}, err
	}

	buf.Reset()
	if w.computed {
		buf.WriteString(`UPSERT INTO j (v) VALUES`)
	} else {
		buf.WriteString(`UPSERT INTO j (k, v) VALUES`)
	}

	for i := 0; i < w.batchSize; i++ {
		j := i * 2
		if i > 0 {
			buf.WriteString(", ")
		}
		if w.computed {
			fmt.Fprintf(&buf, ` ($%d)`, i+1)
		} else {
			fmt.Fprintf(&buf, ` ($%d, $%d)`, j+1, j+2)
		}
	}

	writeStmt, err := db.Prepare(buf.String())
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		op := jsonOp{
			config:    w,
			hists:     reg.GetHandle(),
			db:        db,
			readStmt:  readStmt,
			writeStmt: writeStmt,
		}
		seq := &sequence{config: w, val: w.writeSeq}
		if w.sequential {
			op.g = newSequentialGenerator(seq)
		} else {
			op.g = newHashGenerator(seq)
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

type jsonOp struct {
	config    *jsonLoad
	hists     *histogram.Histograms
	db        *gosql.DB
	readStmt  *gosql.Stmt
	writeStmt *gosql.Stmt
	g         keyGenerator
}

func (o *jsonOp) run(ctx context.Context) error {
	if o.g.rand().Intn(100) < o.config.readPercent {
		args := make([]interface{}, o.config.batchSize)
		for i := 0; i < o.config.batchSize; i++ {
			args[i] = o.g.readKey()
		}
		start := timeutil.Now()
		rows, err := o.readStmt.Query(args...)
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		elapsed := timeutil.Since(start)
		o.hists.Get(`read`).Record(elapsed)
		return rows.Err()
	}
	argCount := 2
	if o.config.computed {
		argCount = 1
	}
	args := make([]interface{}, argCount*o.config.batchSize)
	for i := 0; i < o.config.batchSize*argCount; i += argCount {
		j := i
		if !o.config.computed {
			args[j] = o.g.writeKey()
			j++
		}
		js, err := json.Random(o.config.complexity, o.g.rand())
		if err != nil {
			return err
		}
		if o.config.computed {
			builder := json.NewObjectBuilder(2)
			builder.Add("key", json.FromInt64(o.g.writeKey()))
			builder.Add("data", js)
			js = builder.Build()
		}
		args[j] = js.String()
	}
	start := timeutil.Now()
	_, err := o.writeStmt.Exec(args...)
	elapsed := timeutil.Since(start)
	o.hists.Get(`write`).Record(elapsed)
	return err
}

type sequence struct {
	config *jsonLoad
	val    int64
}

func (s *sequence) write() int64 {
	return (atomic.AddInt64(&s.val, 1) - 1) % s.config.cycleLength
}

// read returns the last key index that has been written. Note that the returned
// index might not actually have been written yet, so a read operation cannot
// require that the key is present.
func (s *sequence) read() int64 {
	return atomic.LoadInt64(&s.val) % s.config.cycleLength
}

// keyGenerator generates read and write keys. Read keys may not yet exist and
// write keys may already exist.
type keyGenerator interface {
	writeKey() int64
	readKey() int64
	rand() *rand.Rand
}

type hashGenerator struct {
	seq    *sequence
	random *rand.Rand
	hasher hash.Hash
	buf    [sha1.Size]byte
}

func newHashGenerator(seq *sequence) *hashGenerator {
	return &hashGenerator{
		seq:    seq,
		random: rand.New(rand.NewSource(seq.config.seed)),
		hasher: sha1.New(),
	}
}

func (g *hashGenerator) hash(v int64) int64 {
	binary.BigEndian.PutUint64(g.buf[:8], uint64(v))
	binary.BigEndian.PutUint64(g.buf[8:16], uint64(g.seq.config.seed))
	g.hasher.Reset()
	_, _ = g.hasher.Write(g.buf[:16])
	g.hasher.Sum(g.buf[:0])
	return int64(binary.BigEndian.Uint64(g.buf[:8]))
}

func (g *hashGenerator) writeKey() int64 {
	return g.hash(g.seq.write())
}

func (g *hashGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.hash(g.random.Int63n(v))
}

func (g *hashGenerator) rand() *rand.Rand {
	return g.random
}

type sequentialGenerator struct {
	seq    *sequence
	random *rand.Rand
}

func newSequentialGenerator(seq *sequence) *sequentialGenerator {
	return &sequentialGenerator{
		seq:    seq,
		random: rand.New(rand.NewSource(seq.config.seed)),
	}
}

func (g *sequentialGenerator) writeKey() int64 {
	return g.seq.write()
}

func (g *sequentialGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.random.Int63n(v)
}

func (g *sequentialGenerator) rand() *rand.Rand {
	return g.random
}
