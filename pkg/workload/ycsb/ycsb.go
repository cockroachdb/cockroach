// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package ycsb is the workload specified by the Yahoo! Cloud Serving Benchmark.
package ycsb

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"strings"
	"sync/atomic"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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

var RandomSeed = workload.NewUint64RandomSeed()

type ycsb struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	timeString  bool
	insertHash  bool
	zeroPadding int
	insertStart int
	insertCount int
	recordCount int
	json        bool
	families    bool
	rmwInTxn    bool
	sfu         bool
	splits      int

	workload                                                string
	requestDistribution                                     string
	scanLengthDistribution                                  string
	minScanLength, maxScanLength                            uint64
	readFreq, scanFreq                                      float32
	insertFreq, updateFreq, readModifyWriteFreq, deleteFreq float32
}

func init() {
	workload.Register(ycsbMeta)
}

var ycsbMeta = workload.Meta{
	Name:        `ycsb`,
	Description: `YCSB is the Yahoo! Cloud Serving Benchmark.`,
	Version:     `1.0.0`,
	RandomSeed:  RandomSeed,
	New: func() workload.Generator {
		g := &ycsb{}
		g.flags.FlagSet = pflag.NewFlagSet(`ycsb`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`read-modify-write-in-txn`: {RuntimeOnly: true},
			`workload`:                 {RuntimeOnly: true},
			`read-freq`:                {RuntimeOnly: true},
			`delete-freq`:              {RuntimeOnly: true},
			`insert-freq`:              {RuntimeOnly: true},
			`update-freq`:              {RuntimeOnly: true},
			`scan-freq`:                {RuntimeOnly: true},
			`read-modify-write-freq`:   {RuntimeOnly: true},
		}
		g.flags.BoolVar(&g.timeString, `time-string`, false, `Prepend field[0-9] data with current time in microsecond precision.`)
		g.flags.BoolVar(&g.insertHash, `insert-hash`, true, `Key to be hashed or ordered.`)
		g.flags.IntVar(&g.zeroPadding, `zero-padding`, 1, `Key using "insert-hash=false" has zeros padded to left to make this length of digits.`)
		g.flags.IntVar(&g.insertStart, `insert-start`, 0, `Key to start initial sequential insertions from. (default 0)`)
		g.flags.IntVar(&g.insertCount, `insert-count`, 10000, `Number of rows to sequentially insert before beginning workload.`)
		g.flags.IntVar(&g.recordCount, `record-count`, 0, `Key to start workload insertions from. Must be >= insert-start + insert-count. (Default: insert-start + insert-count)`)
		g.flags.BoolVar(&g.json, `json`, false, `Use JSONB rather than relational data.`)
		g.flags.BoolVar(&g.families, `families`, true, `Place each column in its own column family.`)
		g.flags.BoolVar(&g.rmwInTxn, `read-modify-write-in-txn`, false, `Run workload F's read-modify-write operation in an explicit transaction.`)
		g.flags.BoolVar(&g.sfu, `select-for-update`, true, `Use SELECT FOR UPDATE syntax in read-modify-write operation, if run in an explicit transactions.`)
		g.flags.IntVar(&g.splits, `splits`, 0, `Number of splits to perform before starting normal operations.`)
		g.flags.StringVar(&g.workload, `workload`, `B`, `Workload type. Choose from A-F or CUSTOM. (default B)`)
		g.flags.StringVar(&g.requestDistribution, `request-distribution`, ``, `Distribution for request key generation [zipfian, uniform, latest]. The default for workloads A, B, C, E, F and CUSTOM is zipfian, and the default for workload D is latest.`)
		g.flags.StringVar(&g.scanLengthDistribution, `scan-length-distribution`, `uniform`, `Distribution for scan length generation [zipfian, uniform]. Primarily used for workload E.`)
		g.flags.Uint64Var(&g.minScanLength, `min-scan-length`, 1, `The minimum length for scan operations. Primarily used for workload E.`)
		g.flags.Uint64Var(&g.maxScanLength, `max-scan-length`, 1000, `The maximum length for scan operations. Primarily used for workload E.`)
		g.flags.Float32Var(&g.readFreq, `read-freq`, 0.0, `Percentage of reads in the workload. Used in conjunction with --workload=CUSTOM to specify an alternative workload mix. (default 0.0)`)
		g.flags.Float32Var(&g.insertFreq, `insert-freq`, 0.0, `Percentage of inserts in the workload. Used in conjunction --workload=CUSTOM to specify an alternative workload mix. (default 0.0)`)
		g.flags.Float32Var(&g.updateFreq, `update-freq`, 0.0, `Percentage of updates in the workload. Used in conjunction with --workload=CUSTOM to specify an alternative workload mix. (default 0.0)`)
		g.flags.Float32Var(&g.scanFreq, `scan-freq`, 0.0, `Percentage of scans in the workload. Used in conjunction with --workload=CUSTOM to specify an alternative workload mix. (default 0.0)`)
		g.flags.Float32Var(&g.readModifyWriteFreq, `read-modify-write-freq`, 0.0, `Percentage of read-modify-writes in the workload. Used in conjunction with --workload=CUSTOM to specify an alternative workload mix. (default 0.0)`)
		g.flags.Float32Var(&g.deleteFreq, `delete-freq`, 0.0, `Percentage of deletes in the workload. Used in conjunction with --workload=CUSTOM to specify an alternative workload mix. (default 0.0)`)
		RandomSeed.AddFlag(&g.flags)

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

// ConnFlags implements the ConnFlagser interface.
func (g *ycsb) ConnFlags() *workload.ConnFlags { return g.connFlags }

// Hooks implements the Hookser interface.
func (g *ycsb) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			g.workload = strings.ToUpper(g.workload)
			var defaultReqDist string
			incomingFreqSum := g.readFreq + g.insertFreq + g.updateFreq + g.deleteFreq + g.scanFreq + g.readModifyWriteFreq
			if g.workload == "CUSTOM" {
				if math.Abs(float64(incomingFreqSum)-1.0) > 0.000001 {
					return errors.Errorf("Custom workload frequencies do not sum to 1.0")
				}
			} else {
				if incomingFreqSum != 0.0 {
					return errors.Errorf("Custom workload frequencies set with non-custom workload")
				}
			}
			switch g.workload {
			case "A":
				g.readFreq = 0.5
				g.updateFreq = 0.5
				defaultReqDist = "zipfian"
			case "B":
				g.readFreq = 0.95
				g.updateFreq = 0.05
				defaultReqDist = "zipfian"
			case "C":
				g.readFreq = 1.0
				defaultReqDist = "zipfian"
			case "D":
				g.readFreq = 0.95
				g.insertFreq = 0.05
				defaultReqDist = "latest"
			case "E":
				g.scanFreq = 0.95
				g.insertFreq = 0.05
				defaultReqDist = "zipfian"
			case "F":
				g.readFreq = 0.5
				g.readModifyWriteFreq = 0.5
				defaultReqDist = "zipfian"
			case "CUSTOM":
				defaultReqDist = "zipfian"
			default:
				return errors.Errorf("Unknown workload: %q", g.workload)
			}
			if g.requestDistribution == "" {
				g.requestDistribution = defaultReqDist
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
	case "CUSTOM":
		// We have no idea what workload is going to be run with the "custom"
		// option. Default to true and rely on the user to set the --families
		// flag if they don't want column families.
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
				rng := rand.NewSource(RandomSeed.Seed() + uint64(batchIdx))

				var tmpbuf [fieldLength]byte
				for rowIdx := rowBegin; rowIdx < rowEnd; rowIdx++ {
					rowOffset := rowIdx - rowBegin

					key.Set(rowOffset, []byte(w.buildKeyName(uint64(g.insertStart+rowIdx))))

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
	const readStmtStr = `SELECT * FROM usertable WHERE ycsb_key = $1`
	const deleteStmtStr = `DELETE FROM usertable WHERE ycsb_key = $1`

	readFieldForUpdateStmtStrs := make([]string, numTableFields)
	for i := range readFieldForUpdateStmtStrs {
		var q string
		if g.json {
			q = fmt.Sprintf(`SELECT field->>'field%d' FROM usertable WHERE ycsb_key = $1`, i)
		} else {
			q = fmt.Sprintf(`SELECT field%d FROM usertable WHERE ycsb_key = $1`, i)
		}
		if g.rmwInTxn && g.sfu {
			q = fmt.Sprintf(`%s FOR UPDATE`, q)
		}
		readFieldForUpdateStmtStrs[i] = q
	}

	const scanStmtStr = `SELECT * FROM usertable WHERE ycsb_key >= $1 LIMIT $2`

	var insertStmtStr string
	if g.json {
		insertStmtStr = `INSERT INTO usertable VALUES ($1, json_build_object(
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
		))`
	} else {
		insertStmtStr = `INSERT INTO usertable VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)`
	}

	var updateStmtStrs []string
	if g.json {
		updateStmtStrs = []string{`UPDATE usertable SET field = field || $2 WHERE ycsb_key = $1`}
	} else {
		updateStmtStrs = make([]string, numTableFields)
		for i := range updateStmtStrs {
			updateStmtStrs[i] = fmt.Sprintf(`UPDATE usertable SET field%d = $2 WHERE ycsb_key = $1`, i)
		}
	}

	var rowIndex atomic.Uint64
	rowIndex.Store(uint64(g.recordCount))
	rowCounter := NewAcknowledgedCounter((uint64)(g.recordCount))

	var requestGen randGenerator
	var err error
	requestGenRng := rand.New(rand.NewSource(RandomSeed.Seed()))
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
	scanLengthGenRng := rand.New(rand.NewSource(RandomSeed.Seed() + 1))
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
	cfg := workload.NewMultiConnPoolCfgFromFlags(g.connFlags)
	pool, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{}

	const (
		readStmt                     stmtKey = "read"
		scanStmt                     stmtKey = "scan"
		deleteStmt                   stmtKey = "delete"
		insertStmt                   stmtKey = "insert"
		readFieldForUpdateStmtFormat         = "readFieldForUpdate%d"
		updateStmtFormat                     = "update%d"
	)
	pool.AddPreparedStatement(readStmt, readStmtStr)
	readFieldForUpdateStmts := make([]stmtKey, len(readFieldForUpdateStmtStrs))
	for i, q := range readFieldForUpdateStmtStrs {
		key := fmt.Sprintf(readFieldForUpdateStmtFormat, i)
		pool.AddPreparedStatement(key, q)
		readFieldForUpdateStmts[i] = key
	}
	pool.AddPreparedStatement(scanStmt, scanStmtStr)
	pool.AddPreparedStatement(deleteStmt, deleteStmtStr)
	pool.AddPreparedStatement(insertStmt, insertStmtStr)
	updateStmts := make([]stmtKey, len(updateStmtStrs))
	for i, q := range updateStmtStrs {
		key := fmt.Sprintf(updateStmtFormat, i)
		pool.AddPreparedStatement(key, q)
		updateStmts[i] = key
	}

	for i := 0; i < g.connFlags.Concurrency; i++ {
		// We want to have 1 connection per worker, however the
		// multi-connection pool round robins access to different pools so it
		// is always possible that we hit a pool that is full, unless we create
		// more connections than necessary. To avoid this, first check if the
		// pool we are attempting to acquire a connection from is full. If
		// full, skip this pool and continue to the next one, otherwise grab
		// the connection and move on.
		var pl *pgxpool.Pool
		var try int
		for try = 0; try < g.connFlags.Concurrency; try++ {
			pl = pool.Get()
			plStat := pl.Stat()
			if plStat.MaxConns()-plStat.AcquiredConns() > 0 {
				break
			}
		}
		if try == g.connFlags.Concurrency {
			return workload.QueryLoad{},
				errors.AssertionFailedf("Unable to acquire connection for worker %d", i)
		}

		conn, err := pl.Acquire(ctx)
		if err != nil {
			return workload.QueryLoad{}, err
		}

		rng := rand.New(rand.NewSource(RandomSeed.Seed() + uint64(i)))
		w := &ycsbWorker{
			config:                  g,
			hists:                   reg.GetHandle(),
			pool:                    pool,
			conn:                    conn,
			readStmt:                readStmt,
			readFieldForUpdateStmts: readFieldForUpdateStmts,
			scanStmt:                scanStmt,
			deleteStmt:              deleteStmt,
			insertStmt:              insertStmt,
			updateStmts:             updateStmts,
			rowIndex:                &rowIndex,
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

type stmtKey = string

type ycsbWorker struct {
	config *ycsb
	hists  *histogram.Histograms
	pool   *workload.MultiConnPool
	conn   *pgxpool.Conn
	// Statement to read all the fields of a row. Used for read requests.
	readStmt stmtKey
	// Statements to read a specific field of a row in preparation for
	// updating it. Used for read-modify-write requests.
	readFieldForUpdateStmts []stmtKey
	scanStmt, insertStmt    stmtKey

	// Statement to use for deletes. Deletes are NOT part of the yscb benchmark
	// standard, but are added here for testing purposes only. Deletes can only
	// be triggered under workload=CUSTOM.
	deleteStmt stmtKey

	// In normal mode this is one statement per field, since the field name
	// cannot be parametrized. In JSON mode it's a single statement.
	updateStmts []stmtKey

	// The next row index to insert.
	rowIndex *atomic.Uint64
	// Counter to keep track of which rows have been inserted.
	rowCounter *AcknowledgedCounter
	// Next insert index to use if non-nil.
	nextInsertIndex *uint64

	requestGen    randGenerator // used to generate random keys for requests
	scanLengthGen randGenerator // used to generate length of scan operations
	rng           *rand.Rand    // used to generate random strings for the values
	hashFunc      hash.Hash64
	hashBuf       [binary.MaxVarintLen64]byte
}

func (yw *ycsbWorker) run(ctx context.Context) error {
	// If we enter this function without a connection, that implies that our
	// connection was dropped due to a connection error the last time we were
	// in here. Grab a new connection for future operations.
	if yw.conn == nil {
		conn, err := yw.pool.Get().Acquire(ctx)
		if err != nil {
			return err
		}
		yw.conn = conn
	}

	var err error
	op := yw.chooseOp()
	start := timeutil.Now()
	switch op {
	case updateOp:
		err = yw.updateRow(ctx)
	case readOp:
		err = yw.readRow(ctx)
	case insertOp:
		err = yw.insertRow(ctx)
	case deleteOp:
		err = yw.deleteRow(ctx)
	case scanOp:
		err = yw.scanRows(ctx)
	case readModifyWriteOp:
		err = yw.readModifyWriteRow(ctx)
	default:
		return errors.Errorf(`unknown operation: %s`, op)
	}

	// If we get an error, check to see if the connection has been closed
	// underneath us. If that's the case, we need to release the connection
	// back to the pool since we obtained this connection using Acquire() and
	// as a result, the connection will not be automatically reestablished for
	// us. Once the connection is returned to the pool, we can reestablish it
	// the next time we come through this function. We don't reestablish the
	// connection here, as we're already dealing with an error which we don't
	// want to overwrite (and reacquiring the connection could result in a new
	// error).
	if err != nil {
		if yw.conn.Conn().IsClosed() {
			yw.conn.Release()
			yw.conn = nil
		}
		return err
	}

	elapsed := timeutil.Since(start)
	yw.hists.Get(string(op)).Record(elapsed)
	return nil
}

type operation string

const (
	updateOp          operation = `update`
	insertOp          operation = `insert`
	deleteOp          operation = `delete`
	readOp            operation = `read`
	scanOp            operation = `scan`
	readModifyWriteOp operation = `readModifyWrite`
)

func (yw *ycsbWorker) hashKey(key uint64) uint64 {
	yw.hashBuf = [binary.MaxVarintLen64]byte{} // clear hashBuf
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
	// To derive the count of actually inserted rows, take the value stored in
	// the insert counter, and subtract out the insertStart point.
	rowCount := yw.rowCounter.Last() - uint64(yw.config.insertStart)
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
	// Now that we have a index constrained to the range of keys actually
	// inserted, add back in the starting point to get to the start-biased
	// index.
	rowIndex += uint64(yw.config.insertStart)
	return yw.buildKeyName(rowIndex)
}

func (yw *ycsbWorker) nextInsertKeyIndex() uint64 {
	if yw.nextInsertIndex != nil {
		result := *yw.nextInsertIndex
		yw.nextInsertIndex = nil
		return result
	}
	return yw.rowIndex.Add(1) - 1
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Generate a random string of alphabetic characters.
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
	if _, err := yw.conn.Exec(ctx, yw.insertStmt, args[:]...); err != nil {
		var pgErr *pgconn.PgError
		// In cases where we've received a unique violation error, we don't want
		// to preserve the key index. Doing so will retry the same insert the
		// next iteration, which will hit the same unique violation error. This
		// situation can be hit in cases where an insert is sent to the cluster
		// and the connection is terminated after the insert was received and
		// processed, but before it was acknowledged. In this case, we should
		// just move on to the next key for inserting.
		if !errors.As(err, &pgErr) ||
			pgcode.MakeCode(pgErr.Code) != pgcode.UniqueViolation {
			yw.nextInsertIndex = new(uint64)
			*yw.nextInsertIndex = keyIndex
		}
		return err
	}

	count, err := yw.rowCounter.Acknowledge(keyIndex)
	if err != nil {
		return err
	}
	return yw.requestGen.IncrementIMax(count)
}

func (yw *ycsbWorker) updateRow(ctx context.Context) error {
	var stmt stmtKey
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
	if _, err := yw.conn.Exec(ctx, stmt, args[:]...); err != nil {
		return err
	}
	return nil
}

func (yw *ycsbWorker) deleteRow(ctx context.Context) error {
	key := yw.nextReadKey()
	_, err := yw.conn.Exec(ctx, yw.deleteStmt, key)
	if err != nil {
		return err
	}
	return nil
}

func (yw *ycsbWorker) readRow(ctx context.Context) error {
	key := yw.nextReadKey()
	res, err := yw.conn.Query(ctx, yw.readStmt, key)
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
	return execute(func() error {
		res, err := yw.conn.Query(ctx, yw.scanStmt, key, scanLength)
		if err != nil {
			return err
		}
		defer res.Close()
		for res.Next() {
		}
		return res.Err()
	})
}

// execute is like crdb.Execute from cockroach-go, but for pgx. This function
// should ultimately be moved to crdbpgx.
//
// TODO(ajwerner): Move this function to crdbpgx and adopt that.
func execute(fn func() error) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) &&
			pgcode.MakeCode(pgErr.Code) == pgcode.SerializationFailure {
			continue
		}
		return err
	}
}

func (yw *ycsbWorker) readModifyWriteRow(ctx context.Context) error {
	type conn interface {
		QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
		Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	}
	run := func(db conn) error {
		key := yw.nextReadKey()
		fieldIdx := yw.rng.Intn(numTableFields)
		// Read.
		var oldValue []byte
		readStmt := yw.readFieldForUpdateStmts[fieldIdx]
		if err := db.QueryRow(ctx, readStmt, key).Scan(&oldValue); err != nil {
			return err
		}
		// Modify.
		_ = oldValue
		newValue := yw.randString(fieldLength)
		// Write.
		var updateStmt stmtKey
		var args [2]interface{}
		args[0] = key
		if yw.config.json {
			updateStmt = yw.updateStmts[0]
			args[1] = fmt.Sprintf(`{"field%d": "%s"}`, fieldIdx, newValue)
		} else {
			updateStmt = yw.updateStmts[fieldIdx]
			args[1] = newValue
		}
		_, err := db.Exec(ctx, updateStmt, args[:]...)
		return err
	}

	var err error
	if yw.config.rmwInTxn {
		err = crdbpgx.ExecuteTx(ctx, yw.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return run(tx)
		})
	} else {
		err = run(yw.conn)
	}
	if errors.Is(err, pgx.ErrNoRows) && ctx.Err() != nil {
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
	// To get to an operation which matches the provided distribution, we first
	// obtain a pseudo-random float value in the range of [0.0,1.0). Then, for
	// each of the operation distributions (which sum to 1.0), we determine if
	// the float value is less than or equal to the given operation
	// distribution. If it is, we select that operation. If it's not, then we
	// subtract the given distribution from the float value obtained, and
	// compare the resultant value against the next operation's distribution.
	// This is guaranteed to find an operation with correct distribution because
	// the distributions sum to 1.0 and the pseudo-random float range is
	// exclusive of the value 1.0.
	p := yw.rng.Float32()
	if p <= yw.config.updateFreq {
		return updateOp
	}
	p -= yw.config.updateFreq
	if p <= yw.config.insertFreq {
		return insertOp
	}
	p -= yw.config.insertFreq
	if p <= yw.config.deleteFreq {
		return deleteOp
	}
	p -= yw.config.deleteFreq
	if p <= yw.config.readModifyWriteFreq {
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
