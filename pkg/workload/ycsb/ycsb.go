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

// Package ycsb is the workload specified by the Yahoo! Cloud Serving Benchmark.
package ycsb

import (
	"context"
	gosql "database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	numTableFields = 10
	fieldLength    = 100 // In characters
	zipfIMin       = 1

	usertableSchemaRelational = `(
		ycsb_key VARCHAR(255) PRIMARY KEY NOT NULL,
		FIELD0 TEXT,
		FIELD1 TEXT,
		FIELD2 TEXT,
		FIELD3 TEXT,
		FIELD4 TEXT,
		FIELD5 TEXT,
		FIELD6 TEXT,
		FIELD7 TEXT,
		FIELD8 TEXT,
		FIELD9 TEXT
	)`
	usertableSchemaJSON = `(
		ycsb_key VARCHAR(255) PRIMARY KEY NOT NULL,
		FIELD JSONB
	)`
)

type ycsb struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed        int64
	initialRows int
	json        bool
	splits      int

	workload                                   string
	readFreq, insertFreq, updateFreq, scanFreq float32
}

func init() {
	workload.Register(ycsbMeta)
}

var ycsbMeta = workload.Meta{
	Name:        `ycsb`,
	Description: `YCSB is the Yahoo! Cloud Serving Benchmark`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &ycsb{}
		g.flags.FlagSet = pflag.NewFlagSet(`ycsb`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`workload`: {RuntimeOnly: true},
		}
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.initialRows, `initial-rows`, 10000,
			`Initial number of rows to sequentially insert before beginning Zipfian workload`)
		g.flags.BoolVar(&g.json, `json`, false, `Use JSONB rather than relational data`)
		g.flags.IntVar(&g.splits, `splits`, 0, `Number of splits to perform before starting normal operations`)
		g.flags.StringVar(&g.workload, `workload`, `B`, `Workload type. Choose from A-F.`)
		// TODO(dan): g.flags.Uint64Var(&g.maxWrites, `max-writes`,
		//     7*24*3600*1500,  // 7 days at 5% writes and 30k ops/s
		//     `Maximum number of writes to perform before halting. This is required for `+
		// 	   `accurately generating keys that are uniformly distributed across the keyspace.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*ycsb) Meta() workload.Meta { return ycsbMeta }

// Flags implements the Flagser interface.
func (g *ycsb) Flags() workload.Flags { return g.flags }

// Hooks implements the Hookser interface.
func (g *ycsb) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			switch g.workload {
			case "A", "a":
				g.readFreq = 0.5
				g.updateFreq = 0.5
			case "B", "b":
				g.readFreq = 0.95
				g.updateFreq = 0.05
			case "C", "c":
				g.readFreq = 1.0
			case "D", "d":
				g.readFreq = 0.95
				g.insertFreq = 0.95
				return errors.New("Workload D (read latest) not implemented yet")
				// TODO(arjun): workload D (read latest) requires modifying the
				// RNG to skew to the latest keys, so not done yet.
			case "E", "e":
				g.scanFreq = 0.95
				g.insertFreq = 0.05
				return errors.New("Workload E (scans) not implemented yet")
			case "F", "f":
				g.insertFreq = 1.0
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (g *ycsb) Tables() []workload.Table {
	usertable := workload.Table{
		Name: `usertable`,
		InitialRows: workload.Tuples(
			g.initialRows,
			func(rowIdx int) []interface{} {
				w := ycsbWorker{config: g, hashFunc: fnv.New64()}
				key := w.buildKeyName(uint64(rowIdx))
				if g.json {
					return []interface{}{key, "{}"}
				}
				return []interface{}{key, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}
			},
		),
		Splits: workload.Tuples(
			g.splits,
			func(splitIdx int) []interface{} {
				w := ycsbWorker{config: g, hashFunc: fnv.New64()}
				return []interface{}{
					w.hashKey(uint64(splitIdx)),
				}
			},
		),
	}
	if g.json {
		usertable.Schema = usertableSchemaJSON
	} else {
		usertable.Schema = usertableSchemaRelational
	}
	return []workload.Table{usertable}
}

// Ops implements the Opser interface.
func (g *ycsb) Ops(urls []string, reg *workload.HistogramRegistry) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(g, g.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(g.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(g.connFlags.Concurrency + 1)

	readStmt, err := db.Prepare(`SELECT * FROM ycsb.usertable WHERE ycsb_key = $1`)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	var insertStmt *gosql.Stmt
	if g.json {
		insertStmt, err = db.Prepare(`INSERT INTO ycsb.usertable VALUES ($1, json_build_object(
			'field0',  $2:::text,
			'field1',  $3:::text,
			'field2',  $4:::text,
			'field3',  $5:::text,
			'field4',  $6:::text,
			'field5',  $7:::text,
			'field6',  $8:::text,
			'field7',  $9:::text,
			'field8',  $10:::text,
			'field9',  $11:::text
		))`)
	} else {
		insertStmt, err = db.Prepare(`INSERT INTO ycsb.usertable VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)`)
	}
	if err != nil {
		return workload.QueryLoad{}, err
	}

	updateStmts := make([]*gosql.Stmt, numTableFields)
	if g.json {
		stmt, err := db.Prepare(`UPDATE ycsb.usertable SET field = field || $2 WHERE ycsb_key = $1`)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		updateStmts[0] = stmt
	} else {
		for i := 0; i < numTableFields; i++ {
			q := fmt.Sprintf(`UPDATE ycsb.usertable SET field%d = $2 WHERE ycsb_key = $1`, i)
			stmt, err := db.Prepare(q)
			if err != nil {
				return workload.QueryLoad{}, err
			}
			updateStmts[i] = stmt
		}
	}

	zipfRng := rand.New(rand.NewSource(g.seed))
	zipfR, err := NewZipfGenerator(
		zipfRng, zipfIMin, defaultIMax, defaultTheta, false /* verbose */)
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(g.seed + int64(i)))
		if err != nil {
			return workload.QueryLoad{}, err
		}
		w := &ycsbWorker{
			config:      g,
			hists:       reg.GetHandle(),
			db:          db,
			readStmt:    readStmt,
			insertStmt:  insertStmt,
			updateStmts: updateStmts,
			zipfR:       zipfR,
			rng:         rng,
			hashFunc:    fnv.New64(),
		}
		ql.WorkerFns = append(ql.WorkerFns, w.run)
	}
	return ql, nil
}

type ycsbWorker struct {
	config               *ycsb
	hists                *workload.Histograms
	db                   *gosql.DB
	readStmt, insertStmt *gosql.Stmt

	// In normal mode this is one statement per field, since the field name cannot
	// be parametrized. In JSON mode it's a single statement.
	updateStmts []*gosql.Stmt

	zipfR    *ZipfGenerator // used to generate random keys
	rng      *rand.Rand     // used to generate random strings for the values
	hashFunc hash.Hash64
	hashBuf  [8]byte
}

func (yw *ycsbWorker) run(ctx context.Context) error {
	op := yw.chooseOp()
	var err error

	start := timeutil.Now()
	switch op {
	case updateOp:
		err = yw.updateRow()
	case readOp:
		err = yw.readRow()
	case insertOp:
		err = yw.insertRow(yw.nextInsertKey(), true)
	case scanOp:
		err = yw.scanRows()
	default:
		return errors.Errorf(`unknown operation: %s`, op)
	}
	if err != nil {
		return err
	}

	yw.hists.Get(string(op)).Record(timeutil.Since(start))
	return nil
}

var readOnly int32

type operation string

const (
	updateOp operation = `update`
	insertOp operation = `insert`
	readOp   operation = `read`
	scanOp   operation = `scan`
)

func (yw *ycsbWorker) hashKey(key uint64) uint64 {
	yw.hashBuf = [8]byte{} // clear hashBuf
	binary.PutUvarint(yw.hashBuf[:], key)
	yw.hashFunc.Reset()
	if _, err := yw.hashFunc.Write(yw.hashBuf[:]); err != nil {
		panic(err)
	}
	return yw.hashFunc.Sum64()
}

func (yw *ycsbWorker) buildKeyName(keynum uint64) string {
	hashedKey := yw.hashKey(keynum)
	return fmt.Sprintf("user%d", hashedKey)
}

// Keys are chosen by first drawing from a Zipf distribution, hashing the drawn
// value, and modding by the total number of rows, so that not all hot keys are
// close together.
// See YCSB paper section 5.3 for a complete description of how keys are chosen.
func (yw *ycsbWorker) nextReadKey() string {
	// TODO: In order to support workloads with INSERT, this would need to account
	// for the number of rows growing over time. See the YCSB paper/code for how
	// this should work. (Basically repeatedly drawing from the distribution until
	// a sufficiently low value is chosen, but with some complications.)
	rownum := yw.hashKey(yw.zipfR.Uint64()) % uint64(yw.config.initialRows)
	return yw.buildKeyName(rownum)
}

func (yw *ycsbWorker) nextInsertKey() string {
	// TODO: This logic is no longer valid now that we are using a large YCSB
	// distribution and modding the samples. To properly support INSERTS, we need
	// to maintain a separate rownum counter.
	rownum := yw.zipfR.IMaxHead()
	return yw.buildKeyName(rownum)
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Gnerate a random string of alphabetic characters.
func (yw *ycsbWorker) randString(length int) string {
	str := make([]byte, length)
	for i := range str {
		str[i] = letters[yw.rng.Intn(len(letters))]
	}
	return string(str)
}

func (yw *ycsbWorker) insertRow(key string, increment bool) error {
	args := make([]interface{}, numTableFields+1)
	args[0] = key
	for i := 1; i <= numTableFields; i++ {
		args[i] = yw.randString(fieldLength)
	}
	if _, err := yw.insertStmt.Exec(args...); err != nil {
		return err
	}

	if increment {
		if err := yw.zipfR.IncrementIMax(); err != nil {
			return err
		}
	}
	return nil
}

func (yw *ycsbWorker) updateRow() error {
	var stmt *gosql.Stmt
	args := make([]interface{}, 2)
	args[0] = yw.nextReadKey()
	fieldIdx := yw.rng.Intn(numTableFields)
	value := yw.randString(fieldLength)
	if yw.config.json {
		stmt = yw.updateStmts[0]
		args[1] = fmt.Sprintf(`{"field%d": "%s"}`, fieldIdx, value)
	} else {
		stmt = yw.updateStmts[fieldIdx]
		args[1] = value
	}
	if _, err := stmt.Exec(args...); err != nil {
		return err
	}
	return nil
}

func (yw *ycsbWorker) readRow() error {
	key := yw.nextReadKey()
	res, err := yw.readStmt.Query(key)
	if err != nil {
		return err
	}
	defer res.Close()
	for res.Next() {
	}
	return res.Err()
}

func (yw *ycsbWorker) scanRows() error {
	return errors.New("not implemented yet")
}

// Choose an operation in proportion to the frequencies.
func (yw *ycsbWorker) chooseOp() operation {
	p := yw.rng.Float32()
	if atomic.LoadInt32(&readOnly) == 0 && p <= yw.config.updateFreq {
		return updateOp
	}
	p -= yw.config.updateFreq
	if atomic.LoadInt32(&readOnly) == 0 && p <= yw.config.insertFreq {
		return insertOp
	}
	p -= yw.config.insertFreq
	// If both scanFreq and readFreq are 0 default to readOp if we've reached
	// this point because readOnly is true.
	if p <= yw.config.scanFreq {
		return scanOp
	}
	return readOp
}
