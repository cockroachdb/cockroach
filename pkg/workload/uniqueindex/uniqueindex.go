// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uniqueindex

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/spf13/pflag"
)

const (
	uniqueIndexSchema = `(
		id INT PRIMARY KEY,
		unique_val INT UNIQUE,
		data BYTES
	)`
)

var RandomSeed = workload.NewUint64RandomSeed()

type uniqueIndex struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	rows       int
	dataSize   int
	splitEvery int
	scatter    bool
}

func init() {
	workload.Register(uniqueIndexMeta)
}

var uniqueIndexMeta = workload.Meta{
	Name: `uniqueindex`,
	Description: `UniqueIndex stresses unique constraint updates across range splits.

	This workload creates a table with a unique constraint on a non-primary-key
	column, then continuously updates that column with random values that span
	multiple range splits. This pattern stresses cross-range delete+insert
	operations that occur when updating unique indexes.`,
	Version:    `1.0.0`,
	RandomSeed: RandomSeed,
	New: func() workload.Generator {
		g := &uniqueIndex{}
		g.flags.FlagSet = pflag.NewFlagSet(`uniqueindex`, pflag.ContinueOnError)
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
func (*uniqueIndex) Meta() workload.Meta { return uniqueIndexMeta }

// Flags implements the Flagser interface.
func (u *uniqueIndex) Flags() workload.Flags { return u.flags }

// ConnFlags implements the ConnFlagser interface.
func (u *uniqueIndex) ConnFlags() *workload.ConnFlags { return u.connFlags }

// Hooks implements the Hookser interface.
func (u *uniqueIndex) Hooks() workload.Hooks {
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
						"ALTER INDEX uniqueindex@uniqueindex_unique_val_key SPLIT AT VALUES %s",
						strings.Join(splitValues, ", "),
					)
					if _, err := db.Exec(splitSQL); err != nil {
						return errors.Wrap(err, "splitting unique index")
					}
				}
			}

			// Scatter the unique index if requested.
			if u.scatter {
				if _, err := db.Exec("ALTER INDEX uniqueindex@uniqueindex_unique_val_key SCATTER"); err != nil {
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
			if u.splitEvery < 0 {
				return errors.Errorf("split-every must be non-negative; was %d", u.splitEvery)
			}
			return nil
		},
	}
}

var uniqueIndexTypes = []*types.T{
	types.Int,
	types.Int,
	types.Bytes,
}

// Tables implements the Generator interface.
func (u *uniqueIndex) Tables() []workload.Table {
	table := workload.Table{
		Name:   `uniqueindex`,
		Schema: uniqueIndexSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: 1,
			FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
				cb.Reset(uniqueIndexTypes, u.rows, coldata.StandardColumnFactory)
				idCol := cb.ColVec(0).Int64()
				uniqueValCol := cb.ColVec(1).Int64()
				dataCol := cb.ColVec(2).Bytes()
				dataCol.Reset()

				for rowIdx := range u.rows {
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
func (u *uniqueIndex) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db.SetMaxOpenConns(u.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(u.connFlags.Concurrency + 1)

	updateStmt, err := db.Prepare(`UPDATE uniqueindex SET unique_val = $1 WHERE id = $2`)
	if err != nil {
		return workload.QueryLoad{}, errors.CombineErrors(err, db.Close())
	}

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error {
			return db.Close()
		},
	}

	numWorkers := min(u.connFlags.Concurrency, u.rows)
	if numWorkers < u.connFlags.Concurrency {
		log.Dev.Warningf(ctx, "warning: concurrency %d exceeds number of rows %d, capping workers to %d",
			u.connFlags.Concurrency, u.rows, numWorkers)
	}
	for i := range numWorkers {
		rng := rand.New(rand.NewPCG(RandomSeed.Seed(), uint64(i)))
		hists := reg.GetHandle()
		rowID := i

		workerFn := func(ctx context.Context) error {
			// Each worker updates its own row.
			// Use offset (+1 to +9) to avoid collisions with pre-populated values (multiples of 10).
			randomVal := rng.IntN(u.rows)*10 + rng.IntN(9) + 1

			start := timeutil.Now()
			_, err := updateStmt.ExecContext(ctx, randomVal, rowID)
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
	var pgErr *pq.Error
	if errors.As(err, &pgErr) {
		return pgcode.MakeCode(string(pgErr.Code)) == pgcode.UniqueViolation
	}
	return false
}
