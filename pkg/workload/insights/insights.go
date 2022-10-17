// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"strings"

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

type insights struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed                 uint64
	rows, batchSize      int
	payloadBytes, ranges int
}

func init() {
	workload.Register(insightsMeta)
}

var insightsMeta = workload.Meta{
	Name:         `insights`,
	Description:  `This workload executes queries that will be detected by cockroach db insights`,
	Version:      `1.0.0`,
	PublicFacing: false,
	New: func() workload.Generator {
		g := &insights{}
		g.flags.FlagSet = pflag.NewFlagSet(`insights`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`batch-size`: {RuntimeOnly: true},
		}
		g.flags.Uint64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.rows, `rows`, defaultRows, `Initial number of accounts in insights table.`)
		g.flags.IntVar(&g.batchSize, `batch-size`, defaultBatchSize, `Number of rows in each batch of initial data.`)
		g.flags.IntVar(&g.payloadBytes, `payload-bytes`, defaultPayloadBytes, `Size of the payload field in each initial row.`)
		g.flags.IntVar(&g.ranges, `ranges`, defaultRanges, `Initial number of ranges in insights table.`)
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
			if b.rows < b.ranges {
				return errors.Errorf(
					"Value of 'rows' (%d) must be greater than or equal to value of 'ranges' (%d)",
					b.rows, b.ranges)
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
	numBatches := (b.rows + b.batchSize - 1) / b.batchSize // ceil(b.rows/b.batchSize)

	var tables = make([]workload.Table, len(tableNames))

	for i, tableName := range tableNames {
		tables[i] = workload.Table{
			Name:   tableName,
			Schema: insightsTableSchema,
			InitialRows: workload.BatchedTuples{
				NumBatches: numBatches,
				FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
					rng := rand.NewSource(b.seed + uint64(batchIdx))

					rowBegin, rowEnd := batchIdx*b.batchSize, (batchIdx+1)*b.batchSize
					if rowEnd > b.rows {
						rowEnd = b.rows
					}
					cb.Reset(insightsTypes, rowEnd-rowBegin, coldata.StandardColumnFactory)
					idCol := cb.ColVec(0).Int64()
					balanceCol := cb.ColVec(1).Int64()
					payloadCol := cb.ColVec(2).Bytes()
					sharedKeyCol := cb.ColVec(3).Int64()
					// coldata.Bytes only allows appends so we have to reset it
					payloadCol.Reset()
					for rowIdx := rowBegin; rowIdx < rowEnd; rowIdx++ {
						var payload []byte
						*a, payload = a.Alloc(b.payloadBytes, 0 /* extraCap */)
						const initialPrefix = `initial-`
						copy(payload[:len(initialPrefix)], []byte(initialPrefix))
						randStringLetters(rng, payload[len(initialPrefix):])

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
						(splitIdx + 1) * (b.rows / b.ranges),
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

	selectJoinStmt, err := db.Prepare(fmt.Sprintf(`
				select a.balance, b.balance from %s a
				left join %s b on a.shared_key = b.shared_key
				where b.balance < 0;
					`, tableNameA, tableNameB))
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < b.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(b.seed))
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			var indexOfTableName = rand.Int() % len(tableNames)
			var tableToHitContention = tableNames[indexOfTableName]

			start := timeutil.Now()

			rowLimit := (rand.Uint32() % 500) + 1
			_, errSelect := db.Exec(fmt.Sprintf(`
					select balance from %s order by balance desc limit $1;
				`, tableToHitContention), rowLimit)
			if errSelect != nil {
				return errSelect
			}

			from := rng.Intn(b.rows)
			to := rng.Intn(b.rows - 1)
			for from == to && b.rows != 1 {
				to = rng.Intn(b.rows - 1)
			}
			amount := rand.Intn(maxTransfer)

			txn, errTxn := db.Begin()
			if errTxn != nil {
				return errTxn
			}

			_, errTxn = txn.Exec(fmt.Sprintf(`
			UPDATE %s
			SET balance = balance - $1 WHERE id = $2`, tableToHitContention),
				amount, from)
			if errTxn != nil {
				return errTxn
			}

			_, errTxn = txn.Exec("select pg_sleep(.01);")
			if errTxn != nil {
				return errTxn
			}

			_, errTxn = txn.Exec(fmt.Sprintf(`
					UPDATE %s
					SET balance = balance + $1 WHERE id = $2`, tableToHitContention),
				amount, to)
			if errTxn != nil {
				return errTxn
			}

			errTxn = txn.Commit()
			if errTxn != nil {
				return errTxn
			}

			elapsed := timeutil.Since(start)
			hists.Get(`transfer`).Record(elapsed)

			_, errJoin := selectJoinStmt.Exec()
			return errJoin
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}

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
