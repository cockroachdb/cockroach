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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package kv

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
	"sync/atomic"

	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/pkg/errors"
)

const (
	kvSchema = `(k BIGINT NOT NULL PRIMARY KEY, v BYTES NOT NULL)`
)

type kv struct {
	flags *pflag.FlagSet

	batchSize                            int
	minBlockSizeBytes, maxBlockSizeBytes int
	cycleLength                          int64
	readPercent                          int
	writeSeq, seed                       int64
	sequential                           bool
}

func init() {
	workload.Register(kvMeta)
}

var kvMeta = workload.Meta{
	Name: `kv`,
	Description: `KV reads and writes to keys spread (by default, uniformly` +
		` at random) across the cluster`,
	Version: `1.0.0`,
	New: func() workload.Generator {
		g := &kv{flags: pflag.NewFlagSet(`kv`, pflag.ContinueOnError)}
		g.flags.IntVar(&g.batchSize, `batch`, 1, `Number of blocks to insert in a single SQL statement`)
		g.flags.IntVar(&g.minBlockSizeBytes, `min-block-bytes`, 1, `Minimum amount of raw data written with each insertion`)
		g.flags.IntVar(&g.maxBlockSizeBytes, `max-block-bytes`, 2, `Maximum amount of raw data written with each insertion`)
		g.flags.Int64Var(&g.cycleLength, `cycle-length`, math.MaxInt64, `Number of keys repeatedly accessed by each writer`)
		g.flags.IntVar(&g.readPercent, `read-percent`, 0, `Percent (0-100) of operations that are reads of existing keys`)
		g.flags.Int64Var(&g.writeSeq, `write-seq`, 0, `Initial write sequence value.`)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.BoolVar(&g.sequential, `sequential`, false, `Pick keys sequentially instead of randomly.`)
		return g
	},
}

// Meta implements the Generator interface.
func (*kv) Meta() workload.Meta { return kvMeta }

// Flags implements the Generator interface.
func (w *kv) Flags() *pflag.FlagSet {
	return w.flags
}

// Configure implements the Generator interface.
func (w *kv) Configure(flags []string) error {
	if w.flags.Parsed() {
		return errors.New("Configure was already called")
	}
	if err := w.flags.Parse(flags); err != nil {
		return err
	}
	if w.maxBlockSizeBytes < w.minBlockSizeBytes {
		return errors.Errorf("Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)",
			w.maxBlockSizeBytes, w.minBlockSizeBytes)
	}
	// TODO(dan): Re-enable this check once splits are supported.
	// if w.sequential && w.splits > 0 {
	// 	log.Fatalf("'sequential' and 'splits' cannot both be enabled")
	// }
	return nil
}

// Tables implements the Generator interface.
func (*kv) Tables() []workload.Table {
	table := workload.Table{
		Name:            `kv`,
		Schema:          kvSchema,
		InitialRowCount: 0,
		// TODO(dan): Support initializing kv with data.
	}
	return []workload.Table{table}
}

// Ops implements the Generator interface.
func (w *kv) Ops() []workload.Operation {
	opFn := func(db *gosql.DB) (func(context.Context) error, error) {
		var buf bytes.Buffer
		buf.WriteString(`SELECT k, v FROM test.kv WHERE k IN (`)
		for i := 0; i < w.batchSize; i++ {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, `$%d`, i+1)
		}
		buf.WriteString(`)`)
		readStmt, err := db.Prepare(buf.String())
		if err != nil {
			return nil, err
		}

		buf.Reset()
		buf.WriteString(`UPSERT INTO test. kv (k, v) VALUES`)

		for i := 0; i < w.batchSize; i++ {
			j := i * 2
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, ` ($%d, $%d)`, j+1, j+2)
		}

		writeStmt, err := db.Prepare(buf.String())
		if err != nil {
			return nil, err
		}

		op := kvOp{
			config:    w,
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
		return op.run, nil
	}

	return []workload.Operation{{
		Name: fmt.Sprintf(`r%02dw%02d`, w.readPercent, 100-w.readPercent),
		Fn:   opFn,
	}}
}

type kvOp struct {
	config    *kv
	db        *gosql.DB
	readStmt  *gosql.Stmt
	writeStmt *gosql.Stmt
	g         keyGenerator
}

func (o *kvOp) run(ctx context.Context) error {
	if o.g.rand().Intn(100) < o.config.readPercent {
		args := make([]interface{}, o.config.batchSize)
		for i := 0; i < o.config.batchSize; i++ {
			args[i] = o.g.readKey()
		}
		rows, err := o.readStmt.Query(args...)
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		return rows.Err()
	}
	const argCount = 2
	args := make([]interface{}, argCount*o.config.batchSize)
	for i := 0; i < o.config.batchSize; i++ {
		j := i * argCount
		args[j+0] = o.g.writeKey()
		args[j+1] = randomBlock(o.config, o.g.rand())
	}
	_, err := o.writeStmt.Exec(args...)
	return err
}

type sequence struct {
	config *kv
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

func randomBlock(config *kv, r *rand.Rand) []byte {
	blockSize := r.Intn(config.maxBlockSizeBytes-config.minBlockSizeBytes) + config.minBlockSizeBytes
	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = byte(r.Int() & 0xff)
	}
	return blockData
}
