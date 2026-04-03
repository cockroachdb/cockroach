// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uniquekv

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	uniqueKVSchema = `(
		id INT PRIMARY KEY,
		unique_val INT UNIQUE,
		data BYTES
	)`
)

var RandomSeed = workload.NewUint64RandomSeed()

type uniqueKV struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	rows       int
	dataSize   int
	splitEvery int
	scatter    bool
}

func init() {
	workload.Register(uniqueKVMeta)
}

var uniqueKVMeta = workload.Meta{
	Name: `uniquekv`,
	Description: `UniqueKV stresses unique constraint updates across range splits.

	This workload creates a table with a unique constraint on a non-primary-key
	column, then continuously updates that column with random values that span
	multiple range splits. This pattern stresses cross-range delete+insert
	operations that occur when updating unique indexes.`,
	Version:    `1.0.0`,
	RandomSeed: RandomSeed,
	New: func() workload.Generator {
		g := &uniqueKV{}
		g.flags.FlagSet = pflag.NewFlagSet(`uniquekv`, pflag.ContinueOnError)
		g.flags.IntVar(&g.rows, `rows`, 600,
			`Number of rows to insert initially.`)
		g.flags.IntVar(&g.dataSize, `data-size`, 1024,
			`Size of data column in bytes.`)
		g.flags.IntVar(&g.splitEvery, `split-every`, 0,
			`Split the unique index at regular intervals of this value.`)
		g.flags.BoolVar(&g.scatter, `scatter`, false,
			`Scatter the unique index after splits.`)
		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*uniqueKV) Meta() workload.Meta { return uniqueKVMeta }

// Flags implements the Flagser interface.
func (u *uniqueKV) Flags() workload.Flags { return u.flags }

// ConnFlags implements the ConnFlagser interface.
func (u *uniqueKV) ConnFlags() *workload.ConnFlags { return u.connFlags }

// Hooks implements the Hookser interface.
func (u *uniqueKV) Hooks() workload.Hooks {
	return workload.Hooks{
		PostLoad: func(_ context.Context, db *gosql.DB) error {
			// Generate and apply splits if splitEvery is specified.
			if u.splitEvery > 0 {
				var splitPoints []string
				maxUniqueVal := u.rows * 10
				for i := u.splitEvery; i < maxUniqueVal; i += u.splitEvery {
					splitPoints = append(splitPoints, fmt.Sprintf("%d", i))
				}

				// Apply splits.
				if len(splitPoints) > 0 {
					splitValues := make([]string, len(splitPoints))
					for i, point := range splitPoints {
						splitValues[i] = fmt.Sprintf("(%s)", strings.TrimSpace(point))
					}
					splitSQL := fmt.Sprintf(
						"ALTER INDEX uniquekv@uniquekv_unique_val_key SPLIT AT VALUES %s",
						strings.Join(splitValues, ", "),
					)
					if _, err := db.Exec(splitSQL); err != nil {
						return errors.Wrap(err, "splitting unique index")
					}
				}
			}

			// Scatter the unique index if requested.
			if u.scatter {
				if _, err := db.Exec("ALTER INDEX uniquekv@uniquekv_unique_val_key SCATTER"); err != nil {
					return errors.Wrap(err, "scattering unique index")
				}
			}

			return nil
		},
		Validate: func() error {
			if u.rows < 1 {
				return errors.Errorf("rows must be at least 1; was %d", u.rows)
			}
			if u.dataSize < 0 {
				return errors.Errorf("data-size must be non-negative; was %d", u.dataSize)
			}
			return nil
		},
	}
}

var uniqueKVTypes = []*types.T{
	types.Int,
	types.Int,
	types.Bytes,
}

// Tables implements the Generator interface.
func (u *uniqueKV) Tables() []workload.Table {
	table := workload.Table{
		Name:   `uniquekv`,
		Schema: uniqueKVSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: 1,
			FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
				cb.Reset(uniqueKVTypes, u.rows, coldata.StandardColumnFactory)
				idCol := cb.ColVec(0).Int64()
				uniqueValCol := cb.ColVec(1).Int64()
				dataCol := cb.ColVec(2).Bytes()
				dataCol.Reset()

				for rowIdx := 0; rowIdx < u.rows; rowIdx++ {
					// id: sequential from 0 to rows-1
					idCol[rowIdx] = int64(rowIdx)
					// unique_val: id * 10 (leaves gaps for workload to use non-colliding values)
					uniqueValCol[rowIdx] = int64(rowIdx * 10)
					// data: repeated 'x' characters
					var data []byte
					*a, data = a.Alloc(u.dataSize)
					for i := range data {
						data[i] = 'x'
					}
					dataCol.Set(rowIdx, data)
				}
			},
		},
	}
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (u *uniqueKV) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db.SetMaxOpenConns(u.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(u.connFlags.Concurrency + 1)

	updateStmt, err := db.Prepare(`UPDATE uniquekv SET unique_val = $1 WHERE id = 0`)
	if err != nil {
		return workload.QueryLoad{}, errors.CombineErrors(err, db.Close())
	}

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error {
			return db.Close()
		},
	}

	for i := 0; i < u.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewPCG(RandomSeed.Seed(), uint64(i)))
		hists := reg.GetHandle()

		workerFn := func(ctx context.Context) error {
			// Generate random value spanning range splits (0 to rows*10).
			// Use offset (+1 to +9) to avoid collisions with pre-populated values (multiples of 10).
			randomVal := rng.IntN(u.rows)*10 + rng.IntN(9) + 1

			start := timeutil.Now()
			_, err := updateStmt.ExecContext(ctx, randomVal)
			elapsed := timeutil.Since(start)
			hists.Get(`update`).Record(elapsed)

			// Tolerate unique constraint violations.
			if err != nil && !isUniqueViolation(err) {
				return err
			}
			return nil
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}

// isUniqueViolation checks if the error is a unique constraint violation.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	// Check for CRDB unique violation error code 23505.
	return strings.Contains(err.Error(), "duplicate key value") ||
		strings.Contains(err.Error(), "violates unique constraint") ||
		strings.Contains(err.Error(), "23505")
}
