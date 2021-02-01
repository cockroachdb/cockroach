// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workloadsql

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// InsertsDataLoader is an InitialDataLoader implementation that loads data with
// batched INSERTs. The zero-value gets some sane defaults for the tunable
// settings.
type InsertsDataLoader struct {
	BatchSize   int
	Concurrency int
}

// InitialDataLoad implements the InitialDataLoader interface.
func (l InsertsDataLoader) InitialDataLoad(
	ctx context.Context, db *gosql.DB, gen workload.Generator,
) (int64, error) {
	if gen.Meta().Name == `tpch` {
		return 0, errors.New(
			`tpch currently doesn't work with the inserts data loader. try --data-loader=import`)
	}

	if l.BatchSize <= 0 {
		l.BatchSize = 1000
	}
	if l.Concurrency < 1 {
		l.Concurrency = 1
	}

	tables := gen.Tables()
	var hooks workload.Hooks
	if h, ok := gen.(workload.Hookser); ok {
		hooks = h.Hooks()
	}

	if hooks.PreCreate != nil {
		if err := hooks.PreCreate(db); err != nil {
			return 0, errors.Wrapf(err, "Could not precreate")
		}
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
		} else if table.InitialRows.FillBatch == nil {
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

				for batchIdx := startIdx; batchIdx < endIdx; batchIdx++ {
					for _, row := range table.InitialRows.BatchRows(batchIdx) {
						atomic.AddInt64(&tableRowsAtomic, 1)
						if len(params) != 0 {
							insertStmtBuf.WriteString(`,`)
						}
						insertStmtBuf.WriteString(`(`)
						for i, datum := range row {
							atomic.AddInt64(&bytesAtomic, workload.ApproxDatumSize(datum))
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
