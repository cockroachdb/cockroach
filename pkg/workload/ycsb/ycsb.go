// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ycsb is the workload specified by the Yahoo! Cloud Serving Benchmark.
package ycsb

import (
	"context"
	gosql "database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
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
	insertStart int
	insertCount int
	recordCount int
	json        bool
	families    bool
	splits      int

	workload                                                        string
	requestDistribution                                             string
	scanLengthDistribution                                          string
	minScanLength, maxScanLength                                    uint64
	readFreq, insertFreq, updateFreq, scanFreq, readModifyWriteFreq float32
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
		g.flags.IntVar(&g.insertStart, `insert-start`, 0, `Key to start initial sequential insertions from. (default 0)`)
		g.flags.IntVar(&g.insertCount, `insert-count`, 10000, `Number of rows to sequentially insert before beginning workload.`)
		g.flags.IntVar(&g.recordCount, `record-count`, 0, `Key to start workload insertions from. Must be >= insert-start + insert-count. (Default: insert-start + insert-count)`)
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
				g.readFreq = 0.5
				g.readModifyWriteFreq = 0.5
				g.requestDistribution = "zipfian"
			default:
				return errors.Errorf("Unknown workload: %q", g.workload)
			}

			if g.recordCount == 0 {
				g.recordCount = g.insertStart + g.insertCount
			}
			if g.insertStart+g.insertCount > g.recordCount {
				return errors.Errorf("insertStart + insertCount (%d) must be <= recordCount (%d)", g.insertStart+g.insertCount, g.recordCount)
			}
			return nil
		},
	}
}

var usertableColTypes = []coltypes.T{
	coltypes.Bytes, coltypes.Bytes, coltypes.Bytes, coltypes.Bytes, coltypes.Bytes, coltypes.Bytes,
	coltypes.Bytes, coltypes.Bytes, coltypes.Bytes, coltypes.Bytes, coltypes.Bytes,
}

// Tables implements the Generator interface.
func (g *ycsb) Tables() []workload.Table {
	usertable := workload.Table{
		Name: `usertable`,
		Splits: workload.Tuples(
			g.splits,
			func(splitIdx int) []interface{} {
				step := math.MaxUint64 / uint64(g.splits+1)
				return []interface{}{
					keyNameFromHash(step * uint64(splitIdx+1)),
				}
			},
		),
	}
	usertableInitialRowsFn := func(rowIdx int) []interface{} {
		w := ycsbWorker{config: g, hashFunc: fnv.New64()}
		key := w.buildKeyName(uint64(g.insertStart + rowIdx))
		if g.json {
			return []interface{}{key, "{}"}
		}
		return []interface{}{key, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}
	}
	if g.json {
		usertable.Schema = usertableSchemaJSON
		usertable.InitialRows = workload.Tuples(
			g.insertCount,
			usertableInitialRowsFn,
		)
	} else {
		if g.families {
			usertable.Schema = usertableSchemaRelationalWithFamilies
		} else {
			usertable.Schema = usertableSchemaRelational
		}
		usertable.InitialRows = workload.TypedTuples(
			g.insertCount,
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

	readStmt, err := db.Prepare(`SELECT * FROM usertable WHERE ycsb_key = $1`)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	readFieldStmts := make([]*gosql.Stmt, numTableFields)
	for i := 0; i < numTableFields; i++ {
		var q string
		if g.json {
			q = fmt.Sprintf(`SELECT field->>'field%d' FROM usertable WHERE ycsb_key = $1`, i)
		} else {
			q = fmt.Sprintf(`SELECT field%d FROM usertable WHERE ycsb_key = $1`, i)
		}

		stmt, err := db.Prepare(q)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		readFieldStmts[i] = stmt
	}

	scanStmt, err := db.Prepare(`SELECT * FROM usertable WHERE ycsb_key >= $1 LIMIT $2`)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	var insertStmt *gosql.Stmt
	if g.json {
		insertStmt, err = db.Prepare(`INSERT INTO usertable VALUES ($1, json_build_object(
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
		insertStmt, err = db.Prepare(`INSERT INTO usertable VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)`)
	}
	if err != nil {
		return workload.QueryLoad{}, err
	}

	updateStmts := make([]*gosql.Stmt, numTableFields)
	if g.json {
		stmt, err := db.Prepare(`UPDATE usertable SET field = field || $2 WHERE ycsb_key = $1`)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		updateStmts[0] = stmt
	} else {
		for i := 0; i < numTableFields; i++ {
			q := fmt.Sprintf(`UPDATE usertable SET field%d = $2 WHERE ycsb_key = $1`, i)
			stmt, err := db.Prepare(q)
			if err != nil {
				return workload.QueryLoad{}, err
			}
			updateStmts[i] = stmt
		}
	}

	rowIndexVal := uint64(g.recordCount)
	rowIndex := &rowIndexVal
	rowCounter := NewAcknowledgedCounter((uint64)(g.recordCount))

	var requestGen randGenerator
	requestGenRng := rand.New(rand.NewSource(g.seed))
	switch strings.ToLower(g.requestDistribution) {
	case "zipfian":
		requestGen, err = NewZipfGenerator(
			requestGenRng, zipfIMin, defaultIMax-1, defaultTheta, false /* verbose */)
	case "uniform":
		requestGen, err = NewUniformGenerator(requestGenRng, 0, uint64(g.recordCount)-1)
	case "latest":
		requestGen, err = NewSkewedLatestGenerator(
			requestGenRng, zipfIMin, uint64(g.recordCount)-1, defaultTheta, false /* verbose */)
	default:
		return workload.QueryLoad{}, errors.Errorf("Unknown request distribution: %s", g.requestDistribution)
	}
	if err != nil {
		return workload.QueryLoad{}, err
	}

	var scanLengthGen randGenerator
	scanLengthGenRng := rand.New(rand.NewSource(g.seed + 1))
	switch strings.ToLower(g.scanLengthDistribution) {
	case "zipfian":
		scanLengthGen, err = NewZipfGenerator(scanLengthGenRng, g.minScanLength, g.maxScanLength, defaultTheta, false /* verbose */)
	case "uniform":
		scanLengthGen, err = NewUniformGenerator(scanLengthGenRng, g.minScanLength, g.maxScanLength)
	default:
		return workload.QueryLoad{}, errors.Errorf("Unknown scan length distribution: %s", g.scanLengthDistribution)
	}
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(g.seed + int64(i)))
		w := &ycsbWorker{
			config:          g,
			hists:           reg.GetHandle(),
			db:              db,
			readStmt:        readStmt,
			readFieldStmts:  readFieldStmts,
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
	config *ycsb
	hists  *histogram.Histograms
	db     *gosql.DB
	// Statement to read all the fields of a row. Used for read requests.
	readStmt *gosql.Stmt
	// Statements to read a specific field of a row. Used for read-modify-write
	// requests.
	readFieldStmts       []*gosql.Stmt
	scanStmt, insertStmt *gosql.Stmt
	// In normal mode this is one statement per field, since the field name
	// cannot be parametrized. In JSON mode it's a single statement.
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
		err = yw.insertRow(ctx)
	case scanOp:
		err = yw.scanRows(ctx)
	case readModifyWriteOp:
		err = yw.readModifyWriteRow(ctx)
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
	updateOp          operation = `update`
	insertOp          operation = `insert`
	readOp            operation = `read`
	scanOp            operation = `scan`
	readModifyWriteOp operation = `readModifyWrite`
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
	return keyNameFromHash(yw.hashKey(keynum))
}

func keyNameFromHash(hashedKey uint64) string {
	return fmt.Sprintf("user%d", hashedKey)
}

// Keys are chosen by first drawing from a Zipf distribution, hashing the drawn
// value, and modding by the total number of rows, so that not all hot keys are
// close together.
// See YCSB paper section 5.3 for a complete description of how keys are chosen.
func (yw *ycsbWorker) nextReadKey() string {
	rowCount := yw.rowCounter.Last()
	// TODO(jeffreyxiao): The official YCSB implementation creates a very large
	// key space for the zipfian distribution, hashes, mods it by the number of
	// expected number of keys at the end of the workload to obtain the key index
	// to read. The key index is then hashed again to obtain the key. If the
	// generated key index is greater than the number of acknowledged rows, then
	// the key is regenerated. Unfortunately, we cannot match this implementation
	// since we run YCSB for a set amount of time rather than number of
	// operations, so we don't know the expected number of keys at the end of the
	// workload. Instead we directly use the zipfian generator to generate our
	// key indexes by modding it by the actual number of rows. We don't hash so
	// the hotspot is consistent and randomly distributed in the key space like
	// with the official implementation. However, unlike the official
	// implementation, this causes the hotspot to not be random w.r.t the
	// insertion order of the keys (the hottest key is always the first inserted
	// key). The distribution is also slightly different than the official YCSB's
	// distribution, so it might be worthwhile to exactly emulate what they're
	// doing.
	rowIndex := yw.requestGen.Uint64() % rowCount
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

func (yw *ycsbWorker) insertRow(ctx context.Context) error {
	var args [numTableFields + 1]interface{}
	keyIndex := yw.nextInsertKeyIndex()
	args[0] = yw.buildKeyName(keyIndex)
	for i := 1; i <= numTableFields; i++ {
		args[i] = yw.randString(fieldLength)
	}
	if _, err := yw.insertStmt.ExecContext(ctx, args[:]...); err != nil {
		yw.nextInsertIndex = new(uint64)
		*yw.nextInsertIndex = keyIndex
		return err
	}

	count, err := yw.rowCounter.Acknowledge(keyIndex)
	if err != nil {
		return err
	}
	return yw.requestGen.IncrementIMax(count)
}

func (yw *ycsbWorker) updateRow(ctx context.Context) error {
	var stmt *gosql.Stmt
	var args [2]interface{}
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
	if _, err := stmt.ExecContext(ctx, args[:]...); err != nil {
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

func (yw *ycsbWorker) readModifyWriteRow(ctx context.Context) error {
	key := yw.nextReadKey()
	newValue := yw.randString(fieldLength)
	fieldIdx := yw.rng.Intn(numTableFields)
	var args [2]interface{}
	args[0] = key
	err := crdb.ExecuteTx(ctx, yw.db, nil, func(tx *gosql.Tx) error {
		var oldValue []byte
		if err := tx.StmtContext(ctx, yw.readFieldStmts[fieldIdx]).QueryRowContext(ctx, key).Scan(&oldValue); err != nil {
			return err
		}
		var updateStmt *gosql.Stmt
		if yw.config.json {
			updateStmt = yw.updateStmts[0]
			args[1] = fmt.Sprintf(`{"field%d": "%s"}`, fieldIdx, newValue)
		} else {
			updateStmt = yw.updateStmts[fieldIdx]
			args[1] = newValue
		}
		_, err := tx.StmtContext(ctx, updateStmt).ExecContext(ctx, args[:]...)
		return err
	})
	if err == gosql.ErrNoRows && ctx.Err() != nil {
		// Sometimes a context cancellation during a transaction can result in
		// sql.ErrNoRows instead of the appropriate context.DeadlineExceeded. In
		// this case, we just return ctx.Err(). See
		// https://github.com/lib/pq/issues/874.
		return ctx.Err()
	}
	return err
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
	if atomic.LoadInt32(&readOnly) == 0 && p <= yw.config.readModifyWriteFreq {
		return readModifyWriteOp
	}
	p -= yw.config.readModifyWriteFreq
	// If both scanFreq and readFreq are 0 default to readOp if we've reached
	// this point because readOnly is true.
	if p <= yw.config.scanFreq {
		return scanOp
	}
	return readOp
}
