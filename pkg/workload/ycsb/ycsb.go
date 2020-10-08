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
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/exp/rand"
)

const (
	numTableFields = 10
	fieldLength    = 100 // In characters
	zipfIMin       = 0

	usertableSchemaRelational = `(
		ycsb_key VARCHAR(255) PRIMARY KEY NOT NULL,
		FIELD0 TEXT NOT NULL,
		FIELD1 TEXT NOT NULL,
		FIELD2 TEXT NOT NULL,
		FIELD3 TEXT NOT NULL,
		FIELD4 TEXT NOT NULL,
		FIELD5 TEXT NOT NULL,
		FIELD6 TEXT NOT NULL,
		FIELD7 TEXT NOT NULL,
		FIELD8 TEXT NOT NULL,
		FIELD9 TEXT NOT NULL
	)`
	usertableSchemaRelationalWithFamilies = `(
		ycsb_key VARCHAR(255) PRIMARY KEY NOT NULL,
		FIELD0 TEXT NOT NULL,
		FIELD1 TEXT NOT NULL,
		FIELD2 TEXT NOT NULL,
		FIELD3 TEXT NOT NULL,
		FIELD4 TEXT NOT NULL,
		FIELD5 TEXT NOT NULL,
		FIELD6 TEXT NOT NULL,
		FIELD7 TEXT NOT NULL,
		FIELD8 TEXT NOT NULL,
		FIELD9 TEXT NOT NULL,
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

	timeFormatTemplate = `2006-01-02 15:04:05.000000-07:00`
)

type ycsb struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed        uint64
	timeString  bool
	insertHash  bool
	zeroPadding int
	insertStart int
	insertCount int
	recordCount int
	json        bool
	families    bool
	sfu         bool
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
		g.flags.Uint64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.BoolVar(&g.timeString, `time-string`, false, `Prepend field[0-9] data with current time in microsecond precision.`)
		g.flags.BoolVar(&g.insertHash, `insert-hash`, true, `Key to be hashed or ordered.`)
		g.flags.IntVar(&g.zeroPadding, `zero-padding`, 1, `Key using "insert-hash=false" has zeros padded to left to make this length of digits.`)
		g.flags.IntVar(&g.insertStart, `insert-start`, 0, `Key to start initial sequential insertions from. (default 0)`)
		g.flags.IntVar(&g.insertCount, `insert-count`, 10000, `Number of rows to sequentially insert before beginning workload.`)
		g.flags.IntVar(&g.recordCount, `record-count`, 0, `Key to start workload insertions from. Must be >= insert-start + insert-count. (Default: insert-start + insert-count)`)
		g.flags.BoolVar(&g.json, `json`, false, `Use JSONB rather than relational data`)
		g.flags.BoolVar(&g.families, `families`, true, `Place each column in its own column family`)
		g.flags.BoolVar(&g.sfu, `select-for-update`, true, `Use SELECT FOR UPDATE syntax in read-modify-write transactions`)
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
			g.workload = strings.ToUpper(g.workload)
			switch g.workload {
			case "A":
				g.readFreq = 0.5
				g.updateFreq = 0.5
				g.requestDistribution = "zipfian"
			case "B":
				g.readFreq = 0.95
				g.updateFreq = 0.05
				g.requestDistribution = "zipfian"
			case "C":
				g.readFreq = 1.0
				g.requestDistribution = "zipfian"
			case "D":
				g.readFreq = 0.95
				g.insertFreq = 0.05
				g.requestDistribution = "latest"
			case "E":
				g.scanFreq = 0.95
				g.insertFreq = 0.05
				g.requestDistribution = "zipfian"
			case "F":
				g.readFreq = 0.5
				g.readModifyWriteFreq = 0.5
				g.requestDistribution = "zipfian"
			default:
				return errors.Errorf("Unknown workload: %q", g.workload)
			}

			if !g.flags.Lookup(`families`).Changed {
				// If `--families` was not specified, default its value to the
				// configuration that we expect to lead to better performance.
				g.families = preferColumnFamilies(g.workload)
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

// preferColumnFamilies returns whether we expect the use of column families to
// improve performance for a given workload.
func preferColumnFamilies(workload string) bool {
	// These determinations were computed on 80da27b (04/04/2020) while running
	// the ycsb roachtests.
	//
	// ycsb/[A-F]/nodes=3 (3x n1-standard-8 VMs):
	//
	// | workload | --families=false | --families=true | better with families? |
	// |----------|-----------------:|----------------:|-----------------------|
	// | A        |         11,743.5 |        17,760.5 | true                  |
	// | B        |         35,232.3 |        32,982.2 | false                 |
	// | C        |         45,454.7 |        44,112.5 | false                 |
	// | D        |         36,091.0 |        35,615.1 | false                 |
	// | E        |          5,774.9 |         2,604.8 | false                 |
	// | F        |          4,933.1 |         8,259.7 | true                  |
	//
	// ycsb/[A-F]/nodes=3/cpu=32 (3x n1-standard-32 VMs):
	//
	// | workload | --families=false | --families=true | better with families? |
	// |----------|-----------------:|----------------:|-----------------------|
	// | A        |         14,144.1 |        27,179.4 | true                  |
	// | B        |         96,669.6 |       104,567.5 | true                  |
	// | C        |        137,463.3 |       131,953.7 | false                 |
	// | D        |        103,188.6 |        95,285.7 | false                 |
	// | E        |         10,417.5 |         7,913.6 | false                 |
	// | F        |          5,782.3 |        15,532.1 | true                  |
	//
	switch workload {
	case "A":
		// Workload A is highly contended. It performs 50% single-row lookups
		// and 50% single-column updates. Using column families breaks the
		// contention between all updates to different columns of the same row,
		// so we use them by default.
		return true
	case "B":
		// Workload B is less contended than Workload A, but still bottlenecks
		// on contention as concurrency grows. It performs 95% single-row
		// lookups and 5% single-column updates. Using column families slows
		// down the single-row lookups but speeds up the updates (see above).
		// This trade-off favors column families for higher concurrency levels
		// but does not at lower concurrency levels. We prefer larger YCSB
		// deployments, so we use column families by default.
		return true
	case "C":
		// Workload C has no contention. It consistent entirely of single-row
		// lookups. Using column families slows down single-row lookups, so we
		// do not use them by default.
		return false
	case "D":
		// Workload D has no contention. It performs 95% single-row lookups and
		// 5% single-row insertion. Using column families slows down single-row
		// lookups and single-row insertion, so we do not use them by default.
		return false
	case "E":
		// Workload E has moderate contention. It performs 95% multi-row scans
		// and 5% single-row insertion. Using column families slows down
		// multi-row scans and single-row insertion, so we do not use them by
		// default.
		return false
	case "F":
		// Workload F is highly contended. It performs 50% single-row lookups
		// and 50% single-column updates expressed as multi-statement
		// read-modify-write transactions. Using column families breaks the
		// contention between all updates to different columns of the same row,
		// so we use them by default.
		return true
	default:
		panic(fmt.Sprintf("unexpected workload: %s", workload))
	}
}

var usertableTypes = []*types.T{
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
				step := math.MaxUint64 / uint64(g.splits+1)
				return []interface{}{
					keyNameFromHash(step * uint64(splitIdx+1)),
				}
			},
		),
	}
	if g.json {
		usertable.Schema = usertableSchemaJSON
		usertable.InitialRows = workload.Tuples(
			g.insertCount,
			func(rowIdx int) []interface{} {
				w := ycsbWorker{
					config:   g,
					hashFunc: fnv.New64(),
				}
				key := w.buildKeyName(uint64(g.insertStart + rowIdx))
				// TODO(peter): Need to fill in FIELD here, rather than an empty JSONB
				// value.
				return []interface{}{key, "{}"}
			})
	} else {
		if g.families {
			usertable.Schema = usertableSchemaRelationalWithFamilies
		} else {
			usertable.Schema = usertableSchemaRelational
		}

		const batchSize = 1000
		usertable.InitialRows = workload.BatchedTuples{
			NumBatches: (g.insertCount + batchSize - 1) / batchSize,
			FillBatch: func(batchIdx int, cb coldata.Batch, _ *bufalloc.ByteAllocator) {
				rowBegin, rowEnd := batchIdx*batchSize, (batchIdx+1)*batchSize
				if rowEnd > g.insertCount {
					rowEnd = g.insertCount
				}
				cb.Reset(usertableTypes, rowEnd-rowBegin, coldata.StandardColumnFactory)

				key := cb.ColVec(0).Bytes()
				// coldata.Bytes only allows appends so we have to reset it.
				key.Reset()

				var fields [numTableFields]*coldata.Bytes
				for i := range fields {
					fields[i] = cb.ColVec(i + 1).Bytes()
					// coldata.Bytes only allows appends so we have to reset it.
					fields[i].Reset()
				}

				w := ycsbWorker{
					config:   g,
					hashFunc: fnv.New64(),
				}
				rng := rand.NewSource(g.seed + uint64(batchIdx))

				var tmpbuf [fieldLength]byte
				for rowIdx := rowBegin; rowIdx < rowEnd; rowIdx++ {
					rowOffset := rowIdx - rowBegin

					key.Set(rowOffset, []byte(w.buildKeyName(uint64(rowIdx))))

					for i := range fields {
						randStringLetters(rng, tmpbuf[:])
						fields[i].Set(rowOffset, tmpbuf[:])
					}
				}
			},
		}
	}
	return []workload.Table{usertable}
}

// Ops implements the Opser interface.
func (g *ycsb) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
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

	readFieldForUpdateStmts := make([]*gosql.Stmt, numTableFields)
	for i := 0; i < numTableFields; i++ {
		var q string
		if g.json {
			q = fmt.Sprintf(`SELECT field->>'field%d' FROM usertable WHERE ycsb_key = $1`, i)
		} else {
			q = fmt.Sprintf(`SELECT field%d FROM usertable WHERE ycsb_key = $1`, i)
		}
		if g.sfu {
			q = fmt.Sprintf(`%s FOR UPDATE`, q)
		}

		stmt, err := db.Prepare(q)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		readFieldForUpdateStmts[i] = stmt
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
		rng := rand.New(rand.NewSource(g.seed + uint64(i)))
		w := &ycsbWorker{
			config:                  g,
			hists:                   reg.GetHandle(),
			db:                      db,
			readStmt:                readStmt,
			readFieldForUpdateStmts: readFieldForUpdateStmts,
			scanStmt:                scanStmt,
			insertStmt:              insertStmt,
			updateStmts:             updateStmts,
			rowIndex:                rowIndex,
			rowCounter:              rowCounter,
			nextInsertIndex:         nil,
			requestGen:              requestGen,
			scanLengthGen:           scanLengthGen,
			rng:                     rng,
			hashFunc:                fnv.New64(),
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
	// Statements to read a specific field of a row in preparation for
	// updating it. Used for read-modify-write requests.
	readFieldForUpdateStmts []*gosql.Stmt
	scanStmt, insertStmt    *gosql.Stmt
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
	if yw.config.insertHash {
		return keyNameFromHash(yw.hashKey(keynum))
	}
	return keyNameFromOrder(keynum, yw.config.zeroPadding)
}

func keyNameFromHash(hashedKey uint64) string {
	return fmt.Sprintf("user%d", hashedKey)
}

func keyNameFromOrder(keynum uint64, zeroPadding int) string {
	return fmt.Sprintf("user%0*d", zeroPadding, keynum)
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
	// prepend current timestamp matching the default CRDB UTC time format
	strStart := 0
	if yw.config.timeString {
		currentTime := timeutil.Now().UTC()
		str = currentTime.AppendFormat(str[:0], timeFormatTemplate)
		strStart = len(str)
		str = str[:length]
	}
	// the rest of data is random str
	for i := strStart; i < length; i++ {
		str[i] = letters[yw.rng.Intn(len(letters))]
	}
	return string(str)
}

// NOTE: The following is intentionally duplicated with the ones in
// workload/tpcc/generate.go. They're a very hot path in restoring a fixture and
// hardcoding the consts seems to trigger some compiler optimizations that don't
// happen if those things are params. Don't modify these without consulting
// BenchmarkRandStringFast.

func randStringLetters(rng rand.Source, buf []byte) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = uint64(11) // floor(log(math.MaxUint64)/log(lettersLen))

	var r, charsLeft uint64
	for i := 0; i < len(buf); i++ {
		if charsLeft == 0 {
			r = rng.Uint64()
			charsLeft = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		charsLeft--
	}
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
	// We run the SELECT statement in a retry loop to handle retryable errors. The
	// scan is large enough that it occasionally begins streaming results back to
	// the client, and so if it then hits a ReadWithinUncertaintyIntervalError
	// then it will return this error even if it is being run as an implicit
	// transaction.
	return crdb.Execute(func() error {
		res, err := yw.scanStmt.QueryContext(ctx, key, scanLength)
		if err != nil {
			return err
		}
		defer res.Close()
		for res.Next() {
		}
		return res.Err()
	})
}

func (yw *ycsbWorker) readModifyWriteRow(ctx context.Context) error {
	key := yw.nextReadKey()
	newValue := yw.randString(fieldLength)
	fieldIdx := yw.rng.Intn(numTableFields)
	var args [2]interface{}
	args[0] = key
	err := crdb.ExecuteTx(ctx, yw.db, nil, func(tx *gosql.Tx) error {
		var oldValue []byte
		readStmt := yw.readFieldForUpdateStmts[fieldIdx]
		if err := tx.StmtContext(ctx, readStmt).QueryRowContext(ctx, key).Scan(&oldValue); err != nil {
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
	if errors.Is(err, gosql.ErrNoRows) && ctx.Err() != nil {
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
