// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License.

package workload

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// InitialDataLoader loads the initial data for all tables in a workload. It
// returns a measure of how many bytes were loaded.
//
// TODO(dan): It would be lovely if the number of bytes loaded was comparable
// between implementations but this is sadly not the case right now.
type InitialDataLoader interface {
	InitialDataLoad(context.Context, *gosql.DB, Generator) (int64, error)
}

// InsertsDataLoader is an InitialDataLoader implementation that loads data with
// batched INSERTs. The zero-value gets some sane defaults for the tunable
// settings.
type InsertsDataLoader struct {
	BatchSize   int
	Concurrency int
}

// InitialDataLoad implements the InitialDataLoader interface.
func (l InsertsDataLoader) InitialDataLoad(
	ctx context.Context, db *gosql.DB, gen Generator,
) (int64, error) {
	if l.BatchSize <= 0 {
		l.BatchSize = 1000
	}
	if l.Concurrency < 1 {
		l.Concurrency = 1
	}

	tables := gen.Tables()
	var hooks Hooks
	if h, ok := gen.(Hookser); ok {
		hooks = h.Hooks()
	}

	for _, table := range tables {
		createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" %s`, table.Name, table.Schema)
		if _, err := db.ExecContext(ctx, createStmt); err != nil {
			return 0, errors.Wrapf(err, "could not create table: %s", table.Name)
		}
	}

	if hooks.PreLoad != nil {
		if err := hooks.PreLoad(db); err != nil {
			return 0, errors.Wrapf(err, "Could not preload")
		}
	}

	var bytesAtomic int64
	for _, table := range tables {
		if table.InitialRows.NumBatches == 0 {
			continue
		} else if table.InitialRows.Batch == nil {
			return 0, errors.Errorf(
				`initial data is not supported for workload %s`, gen.Meta().Name)
		}
		tableStart := timeutil.Now()
		var tableRowsAtomic int64

		batchesPerWorker := table.InitialRows.NumBatches / l.Concurrency
		g, gCtx := errgroup.WithContext(ctx)
		for i := 0; i < l.Concurrency; i++ {
			startIdx := i * batchesPerWorker
			endIdx := startIdx + batchesPerWorker
			if i == l.Concurrency-1 {
				// Account for any rounding error in batchesPerWorker.
				endIdx = table.InitialRows.NumBatches
			}
			g.Go(func() error {
				var insertStmtBuf bytes.Buffer
				var params []interface{}
				var numRows int
				flush := func() error {
					if len(params) > 0 {
						insertStmt := insertStmtBuf.String()
						if _, err := db.ExecContext(gCtx, insertStmt, params...); err != nil {
							return errors.Wrapf(err, "failed insert into %s", table.Name)
						}
					}
					insertStmtBuf.Reset()
					fmt.Fprintf(&insertStmtBuf, `INSERT INTO "%s" VALUES `, table.Name)
					params = params[:0]
					numRows = 0
					return nil
				}
				_ = flush()

				for rowBatchIdx := startIdx; rowBatchIdx < endIdx; rowBatchIdx++ {
					for _, row := range table.InitialRows.Batch(rowBatchIdx) {
						atomic.AddInt64(&tableRowsAtomic, 1)
						if len(params) != 0 {
							insertStmtBuf.WriteString(`,`)
						}
						insertStmtBuf.WriteString(`(`)
						for i, datum := range row {
							atomic.AddInt64(&bytesAtomic, ApproxDatumSize(datum))
							if i != 0 {
								insertStmtBuf.WriteString(`,`)
							}
							fmt.Fprintf(&insertStmtBuf, `$%d`, len(params)+i+1)
						}
						params = append(params, row...)
						insertStmtBuf.WriteString(`)`)
						if numRows++; numRows >= l.BatchSize {
							if err := flush(); err != nil {
								return err
							}
						}
					}
				}
				return flush()
			})
		}
		if err := g.Wait(); err != nil {
			return 0, err
		}
		tableRows := int(atomic.LoadInt64(&tableRowsAtomic))
		log.Infof(ctx, `imported %s (%s, %d rows)`,
			table.Name, timeutil.Since(tableStart).Round(time.Second), tableRows,
		)
	}
	return atomic.LoadInt64(&bytesAtomic), nil
}

// ImportDataLoader is a hook for binaries that include CCL code to inject an
// IMPORT-based InitialDataLoader implementation.
var ImportDataLoader InitialDataLoader = requiresCCLBinaryDataLoader(`IMPORT`)

type requiresCCLBinaryDataLoader string

func (l requiresCCLBinaryDataLoader) InitialDataLoad(
	context.Context, *gosql.DB, Generator,
) (int64, error) {
	return 0, errors.Errorf(`loading initial data with %s requires a CCL binary`, l)
}
