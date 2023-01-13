// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import (
	"context"
	gosql "database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

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
	insightsTableSchema = `(
		id INT PRIMARY KEY,
		balance DECIMAL NOT NULL,
    payload STRING NOT NULL,
		shared_key INT NOT NULL
	)`

	tableNameA          = "insights_workload_table_a"
	tableNameB          = "insights_workload_table_b"
	defaultRows         = 1000
	defaultBatchSize    = 1000
	defaultPayloadBytes = 100
	defaultRanges       = 10
	maxTransfer         = 999
)

var tableNames = []string{tableNameA, tableNameB}
var RandomSeed = workload.NewUint64RandomSeed()

type insights struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	rowCount, batchSize  int
	payloadBytes, ranges int
}

func init() {
	workload.Register(insightsMeta)
}

var insightsMeta = workload.Meta{
	Name:         `insights`,
	Description:  `This workload executes queries that will be detected by insights`,
	Version:      `1.0.0`,
	RandomSeed:   RandomSeed,
	PublicFacing: false,
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

	var tables = make([]workload.Table, len(tableNames))

	for i, tableName := range tableNames {
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
	sqlDatabase, err := workload.SanitizeUrls(b, b.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(b.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(b.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	rng := rand.New(rand.NewSource(RandomSeed.Seed()))
	for i := 0; i < b.connFlags.Concurrency; i++ {
		temp := i
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			start := timeutil.Now()
			err = useTxnToMoveBalance(ctx, db, rng, b.rowCount)
			if err != nil {
				return err
			}

			elapsed := timeutil.Since(start)
			hists.Get(`transfer`).Record(elapsed)

			start = timeutil.Now()
			// May hit contention from balance being moved in
			// other threads when there is concurrency
			err = orderByOnNonIndexColumn(ctx, db, b.rowCount)
			if err != nil {
				return err
			}
			elapsed = timeutil.Since(start)
			hists.Get(`orderByOnNonIndexColumn`).Record(elapsed)

			start = timeutil.Now()
			err = joinOnNonIndexColumn(ctx, db)
			elapsed = timeutil.Since(start)
			hists.Get(`joinOnNonIndexColumn`).Record(elapsed)
			if err != nil {
				return err
			}

			start = timeutil.Now()
			err = updateWithContention(ctx, db, rng, b.rowCount, temp)
			elapsed = timeutil.Since(start)
			hists.Get(`contention`).Record(elapsed)
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
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

func joinOnNonIndexColumn(ctx context.Context, db *gosql.DB) error {
	_, err := db.ExecContext(ctx, `
				select a.balance, b.balance from insights_workload_table_a a
				left join insights_workload_table_b b on a.shared_key = b.shared_key
				where a.balance < 0;`)
	return err
}

func orderByOnNonIndexColumn(ctx context.Context, db *gosql.DB, rowCount int) error {
	rowLimit := (rand.Uint32() % uint32(rowCount)) + 1
	_, err := db.ExecContext(ctx, `
			select balance 
			from insights_workload_table_a order by balance desc limit $1;`, rowLimit)
	return err
}

func useTxnToMoveBalance(ctx context.Context, db *gosql.DB, rng *rand.Rand, rowCount int) error {
	amount := rng.Intn(maxTransfer)
	from := rng.Intn(rowCount)
	to := rng.Intn(rowCount - 1)
	// Change the 'to' row if they are the same row
	for from == to && rowCount != 1 {
		to = rng.Intn(rowCount - 1)
	}

	txn, err := db.BeginTx(ctx, &gosql.TxOptions{})
	if err != nil {
		return err
	}

	_, err = txn.ExecContext(ctx, `
			UPDATE insights_workload_table_a
			SET balance = balance - $1 WHERE id = $2`,
		amount, from)
	if err != nil {
		return err
	}

	_, err = txn.ExecContext(ctx, "select pg_sleep(.01);")
	if err != nil {
		return err
	}

	_, err = txn.ExecContext(ctx, `
					UPDATE insights_workload_table_a
					SET balance = balance + $1 WHERE id = $2`,
		amount, to)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func updateWithContention(
	ctx context.Context, db *gosql.DB, rng *rand.Rand, rowCount int, thread int,
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

		_, errTxn = tx.ExecContext(ctx, "UPDATE insights_workload_table_a SET balance = $1 where id = $2;", 42, rowToBlock)
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
	_, err := db.ExecContext(ctx, "UPDATE insights_workload_table_a SET balance = $1 where id = $2;", amount, rowToBlock)

	// wait for the background go func to complete
	wgTxnDone.Wait()
	return errors.CombineErrors(err, errTxn)
}
