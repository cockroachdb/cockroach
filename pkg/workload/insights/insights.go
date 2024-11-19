// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"
	gosql "database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	insightsTableSchema = `(
		id INT PRIMARY KEY,
		balance DECIMAL NOT NULL,
    payload STRING NOT NULL,
		shared_key INT NOT NULL
	)`

	tableNamePrefix     = "insights_workload_table_"
	defaultDbName       = "insights"
	defaultRows         = 1000
	defaultBatchSize    = 1000
	defaultPayloadBytes = 100
	defaultRanges       = 10
	defaultScaleStats   = false
	minTotalTableCount  = 2
	maxTransfer         = 999
)

var RandomSeed = workload.NewUint64RandomSeed()

type insights struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	rowCount, batchSize  int
	payloadBytes, ranges int
	totalTableCount      int
	scaleStats           bool
	appCount             atomic.Uint64
}

func init() {
	workload.Register(insightsMeta)
}

var insightsMeta = workload.Meta{
	Name:        `insights`,
	Description: `This workload executes queries that will be detected by the database insights in the web UI.`,
	Version:     `1.0.0`,
	RandomSeed:  RandomSeed,
	New: func() workload.Generator {
		g := &insights{}
		g.flags.FlagSet = pflag.NewFlagSet(`insights`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`batch-size`: {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.rowCount, `rows`, defaultRows, `Initial number of accounts in insights table.`)
		g.flags.IntVar(&g.batchSize, `batch-size`, defaultBatchSize, `Number of rows in each batch of initial data.`)
		g.flags.IntVar(&g.payloadBytes, `payload-bytes`, defaultPayloadBytes, `Size of the payload field in each initial row.`)
		g.flags.IntVar(&g.ranges, `ranges`, defaultRanges, `Initial number of ranges in insights table.`)
		g.flags.IntVar(
			&g.totalTableCount,
			`table-count`,
			minTotalTableCount,
			`Number of tables to create on default database insights. 100 takes roughly 30 seconds to create and populate.`)
		g.flags.BoolVar(&g.scaleStats, `scaleStats`, defaultScaleStats, `The workload will create a large number of unique statistics seen on SQL Activity page.`)

		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// FromRows returns Insights testdata with the given number of rows and default
// payload size and range count.
func FromRows(rows int) workload.Generator {
	return FromConfig(rows, 1, defaultPayloadBytes, defaultRanges)
}

// FromConfig returns a one table testdata with three columns: an `id INT
// PRIMARY KEY` representing an account number, a `balance` INT, and a `payload`
// BYTES to pad the size of the rows for various tests.
func FromConfig(rows int, batchSize int, payloadBytes int, ranges int) workload.Generator {
	if ranges > rows {
		ranges = rows
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return workload.FromFlags(insightsMeta,
		fmt.Sprintf(`--rows=%d`, rows),
		fmt.Sprintf(`--batch-size=%d`, batchSize),
		fmt.Sprintf(`--payload-bytes=%d`, payloadBytes),
		fmt.Sprintf(`--ranges=%d`, ranges),
	)
}

// Meta implements the Generator interface.
func (*insights) Meta() workload.Meta { return insightsMeta }

// Flags implements the Flagser interface.
func (b *insights) Flags() workload.Flags { return b.flags }

// ConnFlags implements the ConnFlagser interface.
func (b *insights) ConnFlags() *workload.ConnFlags { return b.connFlags }

// Hooks implements the Hookser interface.
func (b *insights) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if b.rowCount < b.ranges {
				return errors.Errorf(
					"Value of 'rows' (%d) must be greater than or equal to value of 'ranges' (%d)",
					b.rowCount, b.ranges)
			}
			if b.batchSize <= 0 {
				return errors.Errorf(`Value of batch-size must be greater than zero; was %d`, b.batchSize)
			}
			return nil
		},
	}
}

var insightsTypes = []*types.T{
	types.Int,
	types.Int,
	types.Bytes,
	types.Int,
}

// Tables implements the Generator interface.
func (b *insights) Tables() []workload.Table {
	numBatches := (b.rowCount + b.batchSize - 1) / b.batchSize // ceil(b.rows/b.batchSize)

	if b.totalTableCount < 2 {
		b.totalTableCount = 2
	}
	var tables = make([]workload.Table, b.totalTableCount)

	for i := 0; i < b.totalTableCount; i++ {
		tableName := generateTableName(i)
		tables[i] = workload.Table{
			Name:   tableName,
			Schema: insightsTableSchema,
			InitialRows: workload.BatchedTuples{
				NumBatches: numBatches,
				FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
					rng := rand.NewSource(RandomSeed.Seed() + uint64(batchIdx))

					rowBegin, rowEnd := batchIdx*b.batchSize, (batchIdx+1)*b.batchSize
					if rowEnd > b.rowCount {
						rowEnd = b.rowCount
					}
					cb.Reset(insightsTypes, rowEnd-rowBegin, coldata.StandardColumnFactory)
					idCol := cb.ColVec(0).Int64()
					balanceCol := cb.ColVec(1).Int64()
					payloadCol := cb.ColVec(2).Bytes()
					// coldata.Bytes only allows appends so we have to reset it
					payloadCol.Reset()

					sharedKeyCol := cb.ColVec(3).Int64()
					// fill the table with rows
					for rowIdx := rowBegin; rowIdx < rowEnd; rowIdx++ {
						payload := generateRandomBase64Bytes(b.payloadBytes)

						rowOffset := rowIdx - rowBegin
						idCol[rowOffset] = int64(rowIdx)
						balanceCol[rowOffset] = 0
						payloadCol.Set(rowOffset, payload)
						sharedKeyCol[rowOffset] = int64(rng.Uint64() % 4)
					}
				},
			},
			Splits: workload.Tuples(
				b.ranges-1,
				func(splitIdx int) []interface{} {
					return []interface{}{
						(splitIdx + 1) * (b.rowCount / b.ranges),
					}
				},
			),
		}
	}

	return tables
}

// Ops implements the Opser interface.
func (b *insights) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(b.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(b.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{}
	rng := rand.New(rand.NewSource(RandomSeed.Seed()))

	// Most of the insight queries are slow by design. This prevents them from
	// having enough volume to hit the memory limits of statistics before
	// a flush occurs. To scale the number of statistics being generated
	// the first 2 threads will do all the normal insight workloads. The rest of
	// the threads will do a simple query where the app name is changed for each
	// iteration.
	concurrencyForAllInsights := b.connFlags.Concurrency
	if b.scaleStats {
		concurrencyForAllInsights = 2
	}

	for i := 0; i < concurrencyForAllInsights; i++ {
		useRandomTable := i < 4
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			tableNameA := generateTableName(0)
			tableNameB := generateTableName(1)

			// First 4 threads should target same table to cause more contention
			// scenarios. The rest will target random tables.
			if useRandomTable {
				tableNameA = generateTableName(rng.Intn(b.totalTableCount))
				tableNameB = generateTableName(rng.Intn(b.totalTableCount))
			}

			err = b.incrementAppName(db)
			if err != nil {
				return err
			}

			start := timeutil.Now()
			err = useTxnToMoveBalance(ctx, db, rng, b.rowCount, tableNameA)
			if err != nil {
				return err
			}

			elapsed := timeutil.Since(start)
			hists.Get(`transfer`).Record(elapsed)

			start = timeutil.Now()
			// May hit contention from balance being moved in
			// other threads when there is concurrency
			err = orderByOnNonIndexColumn(ctx, db, b.rowCount, tableNameA)
			if err != nil {
				return err
			}
			elapsed = timeutil.Since(start)
			hists.Get(`orderByOnNonIndexColumn`).Record(elapsed)

			start = timeutil.Now()
			err = joinOnNonIndexColumn(ctx, db, tableNameA, tableNameB)
			elapsed = timeutil.Since(start)
			hists.Get(`joinOnNonIndexColumn`).Record(elapsed)
			if err != nil {
				return err
			}

			start = timeutil.Now()
			err = updateWithContention(ctx, db, rng, b.rowCount, tableNameA)
			elapsed = timeutil.Since(start)
			hists.Get(`contention`).Record(elapsed)
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}

	if b.scaleStats {
		hists := reg.GetHandle()
		numAppNameThreads := b.connFlags.Concurrency - concurrencyForAllInsights
		for i := 0; i < numAppNameThreads; i++ {
			workerFn := func(ctx context.Context) error {
				tableNameA := generateTableName(0)

				err = b.incrementAppName(db)
				if err != nil {
					return err
				}

				start := timeutil.Now()
				err = useTxnToMoveBalance(ctx, db, rng, b.rowCount, tableNameA)
				elapsed := timeutil.Since(start)
				hists.Get(`transfer-uniqueAppName`).Record(elapsed)
				return err
			}
			ql.WorkerFns = append(ql.WorkerFns, workerFn)
		}
	}
	return ql, nil
}

func generateTableName(index int) string {
	return fmt.Sprintf("%s%d", tableNamePrefix, index)
}

func (b *insights) incrementAppName(db *gosql.DB) error {
	if !b.scaleStats {
		return nil
	}
	appNum := b.appCount.Add(1)
	appName := fmt.Sprintf("%s %d", "insightsapp", appNum)
	_, err := db.Exec("SET application_name = $1;", appName)
	return err
}

func generateRandomBase64Bytes(size int) []byte {
	payload := make([]byte, size)
	_, err := rand.Read(payload)
	if err != nil {
		fmt.Println(err)
	}
	base64Size := base64.StdEncoding.EncodedLen(size)
	payloadBase64 := make([]byte, base64Size)
	base64.StdEncoding.Encode(payloadBase64, payload)
	return payloadBase64
}

func joinOnNonIndexColumn(
	ctx context.Context, db *gosql.DB, tableName1 string, tableName2 string,
) error {
	query := fmt.Sprintf(`
				SELECT a.balance, b.balance FROM %s a
				LEFT JOIN %s b ON a.shared_key = b.shared_key
				WHERE a.balance < 0;`, tree.NameString(tableName1), tree.NameString(tableName2))
	_, err := db.ExecContext(ctx, query)
	return err
}

func orderByOnNonIndexColumn(
	ctx context.Context, db *gosql.DB, rowCount int, tableName string,
) error {
	rowLimit := (rand.Uint32() % uint32(rowCount)) + 1
	query := fmt.Sprintf(`SELECT balance
			FROM %s ORDER BY balance DESC limit $1;`, tree.NameString(tableName))
	_, err := db.ExecContext(ctx, query, rowLimit)
	return err
}

func useTxnToMoveBalance(
	ctx context.Context, db *gosql.DB, rng *rand.Rand, rowCount int, tableName string,
) error {
	amount := rng.Intn(maxTransfer)
	from := rng.Intn(rowCount)
	to := rng.Intn(rowCount - 1)
	// Change the 'to' row if they are the same row.
	for from == to && rowCount != 1 {
		to = rng.Intn(rowCount - 1)
	}

	txn, err := db.BeginTx(ctx, &gosql.TxOptions{})
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
			UPDATE %s
			SET balance = balance - $1 WHERE id = $2`, tree.NameString(tableName))
	_, err = txn.ExecContext(ctx, query, amount, from)
	if err != nil {
		return err
	}

	_, err = txn.ExecContext(ctx, "SELECT pg_sleep(.01);")
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`
			UPDATE %s
					SET balance = balance + $1 WHERE id = $2`, tree.NameString(tableName))
	_, err = txn.ExecContext(ctx, query, amount, to)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func updateWithContention(
	ctx context.Context, db *gosql.DB, rng *rand.Rand, rowCount int, tableName string,
) error {
	// Pick random row to cause contention on
	rowToBlock := rng.Intn(rowCount)

	// In a go routine have it start a transaction, update a row,
	// sleep for a time, and then complete the transaction.
	// With original connection attempt to update the same row being updated concurrently
	// in the separate go routine, this will be blocked until the original transaction completes.
	var wgTxnStarted sync.WaitGroup
	wgTxnStarted.Add(1)

	// Lock to wait for the txn to complete to avoid the test finishing before the txn is committed
	var wgTxnDone sync.WaitGroup
	wgTxnDone.Add(1)

	var wgMainThread sync.WaitGroup
	wgMainThread.Add(1)

	var errTxn error
	go func() {
		defer wgTxnDone.Done()

		var tx *gosql.Tx
		tx, errTxn = db.BeginTx(ctx, &gosql.TxOptions{})
		if errTxn != nil {
			fmt.Printf("background task txn failed %s\n", errTxn)
			wgTxnStarted.Done()
			return
		}

		backgroundQuery := fmt.Sprintf("UPDATE %s SET balance = $1 WHERE id = $2;", tree.NameString(tableName))
		_, errTxn = tx.ExecContext(ctx, backgroundQuery, 42, rowToBlock)
		wgTxnStarted.Done()
		if errTxn != nil {
			return
		}

		// Random sleep up to 5 seconds
		sleepDuration := time.Duration(rng.Intn(5000)) * time.Millisecond

		// insights by default has a threshold of 100 milliseconds
		// this guarantees it will be detected all the time
		sleepDuration = sleepDuration + 100*time.Millisecond
		time.Sleep(sleepDuration)

		errTxn = tx.Commit()
	}()

	// Need to wait for the txn to start to ensure lock contention
	wgTxnStarted.Wait()

	// This will be blocked until the background go func commits the txn.
	amount := rng.Intn(maxTransfer)
	query := fmt.Sprintf("UPDATE %s SET balance = $1 WHERE id = $2;", tree.NameString(tableName))
	_, err := db.ExecContext(ctx, query, amount, rowToBlock)

	// wait for the background go func to complete
	wgTxnDone.Wait()
	return errors.CombineErrors(err, errTxn)
}
