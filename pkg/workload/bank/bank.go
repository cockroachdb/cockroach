// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bank

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
	bankSchema = `(
		id INT PRIMARY KEY,
		balance INT,
		payload STRING,
		FAMILY (id, balance, payload)
	)`

	defaultRows         = 1000
	defaultBatchSize    = 1000
	defaultPayloadBytes = 100
	defaultRanges       = 10
	maxTransfer         = 999
)

type bank struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed                 uint64
	rows, batchSize      int
	payloadBytes, ranges int
}

func init() {
	workload.Register(bankMeta)
}

var bankMeta = workload.Meta{
	Name:         `bank`,
	Description:  `Bank models a set of accounts with currency balances`,
	Version:      `1.0.0`,
	PublicFacing: true,
	New: func() workload.Generator {
		g := &bank{}
		g.flags.FlagSet = pflag.NewFlagSet(`bank`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`batch-size`: {RuntimeOnly: true},
		}
		g.flags.Uint64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.rows, `rows`, defaultRows, `Initial number of accounts in bank table.`)
		g.flags.IntVar(&g.batchSize, `batch-size`, defaultBatchSize, `Number of rows in each batch of initial data.`)
		g.flags.IntVar(&g.payloadBytes, `payload-bytes`, defaultPayloadBytes, `Size of the payload field in each initial row.`)
		g.flags.IntVar(&g.ranges, `ranges`, defaultRanges, `Initial number of ranges in bank table.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// FromRows returns Bank testdata with the given number of rows and default
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
	return workload.FromFlags(bankMeta,
		fmt.Sprintf(`--rows=%d`, rows),
		fmt.Sprintf(`--batch-size=%d`, batchSize),
		fmt.Sprintf(`--payload-bytes=%d`, payloadBytes),
		fmt.Sprintf(`--ranges=%d`, ranges),
	)
}

// Meta implements the Generator interface.
func (*bank) Meta() workload.Meta { return bankMeta }

// Flags implements the Flagser interface.
func (b *bank) Flags() workload.Flags { return b.flags }

// Hooks implements the Hookser interface.
func (b *bank) Hooks() workload.Hooks {
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

var bankTypes = []*types.T{
	types.Int,
	types.Int,
	types.Bytes,
}

// Tables implements the Generator interface.
func (b *bank) Tables() []workload.Table {
	numBatches := (b.rows + b.batchSize - 1) / b.batchSize // ceil(b.rows/b.batchSize)
	table := workload.Table{
		Name:   `bank`,
		Schema: bankSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numBatches,
			FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
				rng := rand.NewSource(b.seed + uint64(batchIdx))

				rowBegin, rowEnd := batchIdx*b.batchSize, (batchIdx+1)*b.batchSize
				if rowEnd > b.rows {
					rowEnd = b.rows
				}
				cb.Reset(bankTypes, rowEnd-rowBegin, coldata.StandardColumnFactory)
				idCol := cb.ColVec(0).Int64()
				balanceCol := cb.ColVec(1).Int64()
				payloadCol := cb.ColVec(2).Bytes()
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
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (b *bank) Ops(
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

	// TODO(dan): Move the various queries in the backup/restore tests here.
	updateStmt, err := db.Prepare(`
		UPDATE bank
		SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
		WHERE id IN ($1, $2)
	`)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < b.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(b.seed))
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			from := rng.Intn(b.rows)
			to := rng.Intn(b.rows - 1)
			for from == to && b.rows != 1 {
				to = rng.Intn(b.rows - 1)
			}
			amount := rand.Intn(maxTransfer)
			start := timeutil.Now()
			_, err := updateStmt.Exec(from, to, amount)
			elapsed := timeutil.Since(start)
			hists.Get(`transfer`).Record(elapsed)
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
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
