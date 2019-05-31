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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	numTableFields = 10
	fieldLength    = 100 // In characters
	zipfIMin       = 0

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
	usertableSchemaRelationalWithFamilies = `(
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
		FIELD9 TEXT,
		FAMILY (ycsb_key),
		FAMILY (FIELD0),
		FAMILY (FIELD1),
		FAMILY (FIELD2),
		FAMILY (FIELD3),
		FAMILY (FIELD4),
		FAMILY (FIELD5),
		FAMILY (FIELD6),
		FAMILY (FIELD7),
		FAMILY (FIELD8),
		FAMILY (FIELD9)
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
	families    bool
	splits      int

	workload                                   string
	requestDistribution                        string
	scanLengthDistribution                     string
	minScanLength, maxScanLength               uint64
	readFreq, insertFreq, updateFreq, scanFreq float32
}

func init() {
	workload.Register(ycsbMeta)
}

var ycsbMeta = workload.Meta{
	Name:         `ycsb`,
	Description:  `YCSB is the Yahoo! Cloud Serving Benchmark`,
	Version:      `1.0.0`,
	PublicFacing: true,
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
		g.flags.BoolVar(&g.families, `families`, true, `Place each column in its own column family`)
		g.flags.IntVar(&g.splits, `splits`, 0, `Number of splits to perform before starting normal operations`)
		g.flags.StringVar(&g.workload, `workload`, `B`, `Workload type. Choose from A-F.`)
		g.flags.StringVar(&g.requestDistribution, `request-distribution`, ``, `Distribution for request key generation [zipfian, uniform, latest]. The default for workloads A, B, C, E, and F is zipfian, and the default for workload D is latest.`)
		g.flags.StringVar(&g.scanLengthDistribution, `scan-length-distribution`, `uniform`, `Distribution for scan length generation [zipfian, uniform]. Primarily used for workload E.`)
		g.flags.Uint64Var(&g.minScanLength, `min-scan-length`, 1, `The minimum length for scan operations. Primarily used for workload E.`)
		g.flags.Uint64Var(&g.maxScanLength, `max-scan-length`, 1000, `The maximum length for scan operations. Primarily used for workload E.`)

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
				g.requestDistribution = "zipfian"
			case "B", "b":
				g.readFreq = 0.95
				g.updateFreq = 0.05
				g.requestDistribution = "zipfian"
			case "C", "c":
				g.readFreq = 1.0
				g.requestDistribution = "zipfian"
			case "D", "d":
				g.readFreq = 0.95
				g.insertFreq = 0.05
				g.requestDistribution = "latest"
			case "E", "e":
				g.scanFreq = 0.95
				g.insertFreq = 0.05
				g.requestDistribution = "zipfian"
			case "F", "f":
				g.insertFreq = 1.0
				g.requestDistribution = "zipfian"
			default:
				return errors.Errorf("Unknown workload: %q", g.workload)
			}
			return nil
		},
	}
}

var usertableColTypes = []types.T{
	types.Bytes, types.Bytes, types.Bytes, types.Bytes, types.Bytes, types.Bytes,
	types.Bytes, types.Bytes, types.Bytes, types.Bytes, types.Bytes,
}

// Tables implements the Generator interface.
func (g *ycsb) Tables() []workload.Table {
	usertable := workload.Table{
		Name: `usertable`,
		Splits: workload.Tuples(
			g.splits,
			func(splitIdx int) []interface{} {
				w := ycsbWorker{config: g, hashFunc: fnv.New64()}
				return []interface{}{
					w.buildKeyName(uint64(splitIdx)),
				}
			},
		),
	}
	usertableInitialRowsFn := func(rowIdx int) []interface{} {
		w := ycsbWorker{config: g, hashFunc: fnv.New64()}
		key := w.buildKeyName(uint64(rowIdx))
		if g.json {
			return []interface{}{key, "{}"}
		}
		return []interface{}{key, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}
	}
	if g.json {
		usertable.Schema = usertableSchemaJSON
		usertable.InitialRows = workload.Tuples(
			g.initialRows,
			usertableInitialRowsFn,
		)
	} else {
		if g.families {
			usertable.Schema = usertableSchemaRelationalWithFamilies
		} else {
			usertable.Schema = usertableSchemaRelational
		}
		usertable.InitialRows = workload.TypedTuples(
			g.initialRows,
			usertableColTypes,
			usertableInitialRowsFn,
		)
	}
	return []workload.Table{usertable}
}

// Ops implements the Opser interface.
func (g *ycsb) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
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

	scanStmt, err := db.Prepare(`SELECT * FROM ycsb.usertable WHERE ycsb_key >= $1 LIMIT $2`)
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
	var requestGen randGenerator
	var scanLengthGen randGenerator
	var rowIndex = new(uint64)
	*rowIndex = uint64(g.initialRows)

	switch strings.ToLower(g.requestDistribution) {
	case "zipfian":
		requestGen, err = NewZipfGenerator(
			zipfRng, zipfIMin, defaultIMax-1, defaultTheta, false /* verbose */)
	case "uniform":
		requestGen, err = NewUniformGenerator(zipfRng, 0, uint64(g.initialRows)-1)
	case "latest":
		requestGen, err = NewSkewedLatestGenerator(
			zipfRng, zipfIMin, uint64(g.initialRows)-1, defaultTheta, false /* verbose */)
	default:
		return workload.QueryLoad{}, errors.Errorf("Unknown request distribution: %s", g.requestDistribution)
	}
	if err != nil {
		return workload.QueryLoad{}, err
	}

	switch strings.ToLower(g.scanLengthDistribution) {
	case "zipfian":
		scanLengthGen, err = NewZipfGenerator(zipfRng, g.minScanLength, g.maxScanLength, defaultTheta, false /* verbose */)
	case "uniform":
		scanLengthGen, err = NewUniformGenerator(zipfRng, g.minScanLength, g.maxScanLength)
	default:
		return workload.QueryLoad{}, errors.Errorf("Unknown scan length distribution: %s", g.scanLengthDistribution)
	}
	if err != nil {
		return workload.QueryLoad{}, err
	}

	rowCounter := NewAcknowledgedCounter((uint64)(g.initialRows))
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(g.seed + int64(i)))
		w := &ycsbWorker{
			config:          g,
			hists:           reg.GetHandle(),
			db:              db,
			readStmt:        readStmt,
			scanStmt:        scanStmt,
			insertStmt:      insertStmt,
			updateStmts:     updateStmts,
			rowIndex:        rowIndex,
			rowCounter:      rowCounter,
			nextInsertIndex: nil,
			requestGen:      requestGen,
			scanLengthGen:   scanLengthGen,
			rng:             rng,
			hashFunc:        fnv.New64(),
		}
		ql.WorkerFns = append(ql.WorkerFns, w.run)
	}
	return ql, nil
}

type randGenerator interface {
	Uint64() uint64
	IncrementIMax(count uint64) error
}

type ycsbWorker struct {
	config                         *ycsb
	hists                          *histogram.Histograms
	db                             *gosql.DB
	readStmt, scanStmt, insertStmt *gosql.Stmt

	// In normal mode this is one statement per field, since the field name cannot
	// be parametrized. In JSON mode it's a single statement.
	updateStmts []*gosql.Stmt

	// The next row index to insert.
	rowIndex *uint64
	// Counter to keep track of which rows have been inserted.
	rowCounter *AcknowledgedCounter
	// Next insert index to use if non-nil.
	nextInsertIndex *uint64

	requestGen    randGenerator // used to generate random keys for requests
	scanLengthGen randGenerator // used to generate length of scan operations
	rng           *rand.Rand    // used to generate random strings for the values
	hashFunc      hash.Hash64
	hashBuf       [8]byte
}

func (yw *ycsbWorker) run(ctx context.Context) error {
	op := yw.chooseOp()
	var err error

	start := timeutil.Now()
	switch op {
	case updateOp:
		err = yw.updateRow(ctx)
	case readOp:
		err = yw.readRow(ctx)
	case insertOp:
		err = yw.insertRow(ctx, yw.nextInsertKeyIndex(), true)
	case scanOp:
		err = yw.scanRows(ctx)
	default:
		return errors.Errorf(`unknown operation: %s`, op)
	}
	if err != nil {
		return err
	}

	elapsed := timeutil.Since(start)
	yw.hists.Get(string(op)).Record(elapsed)
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
	rowCount := yw.rowCounter.Last()
	rowIndex := yw.hashKey(yw.requestGen.Uint64()) % rowCount
	return yw.buildKeyName(rowIndex)
}

func (yw *ycsbWorker) nextInsertKeyIndex() uint64 {
	if yw.nextInsertIndex != nil {
		result := *yw.nextInsertIndex
		yw.nextInsertIndex = nil
		return result
	}
	return atomic.AddUint64(yw.rowIndex, 1) - 1
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

func (yw *ycsbWorker) insertRow(ctx context.Context, keyIndex uint64, increment bool) error {
	args := make([]interface{}, numTableFields+1)
	args[0] = yw.buildKeyName(keyIndex)
	for i := 1; i <= numTableFields; i++ {
		args[i] = yw.randString(fieldLength)
	}
	if _, err := yw.insertStmt.ExecContext(ctx, args...); err != nil {
		yw.nextInsertIndex = new(uint64)
		*yw.nextInsertIndex = keyIndex
		return err
	}

	if increment {
		count, err := yw.rowCounter.Acknowledge(keyIndex)
		if err != nil {
			return err
		}
		if err := yw.requestGen.IncrementIMax(count); err != nil {
			return err
		}
	}
	return nil
}

func (yw *ycsbWorker) updateRow(ctx context.Context) error {
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
	if _, err := stmt.ExecContext(ctx, args...); err != nil {
		return err
	}
	return nil
}

func (yw *ycsbWorker) readRow(ctx context.Context) error {
	key := yw.nextReadKey()
	res, err := yw.readStmt.QueryContext(ctx, key)
	if err != nil {
		return err
	}
	defer res.Close()
	for res.Next() {
	}
	return res.Err()
}

func (yw *ycsbWorker) scanRows(ctx context.Context) error {
	key := yw.nextReadKey()
	scanLength := yw.scanLengthGen.Uint64()
	res, err := yw.scanStmt.QueryContext(ctx, key, scanLength)
	if err != nil {
		return err
	}
	defer res.Close()
	for res.Next() {
	}
	return res.Err()
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
