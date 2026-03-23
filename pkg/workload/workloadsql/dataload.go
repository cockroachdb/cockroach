// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadsql

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
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

// SchemaOnlyDataLoader is an InitialDataLoader implementation that only creates
// tables without loading any data. This is useful for benchmarks that only need
// the schema structure without any row data.
type SchemaOnlyDataLoader struct {
	// URLs is the list of node URLs to distribute table creation across.
	// If empty, the provided db connection is used directly.
	URLs []string
}

// InitialDataLoad implements the InitialDataLoader interface.
func (l SchemaOnlyDataLoader) InitialDataLoad(
	ctx context.Context, db *gosql.DB, gen workload.Generator,
) (int64, error) {
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

	log.Dev.Infof(ctx, `creating %d tables (schema only, no data)`, len(tables))
	startTime := timeutil.Now()

	// Create connections to each node if URLs are provided.
	var dbs []*gosql.DB
	if len(l.URLs) > 1 {
		for _, url := range l.URLs {
			nodeDB, err := gosql.Open("cockroach", url)
			if err != nil {
				// Close any already-opened connections before returning.
				for _, openDB := range dbs {
					_ = openDB.Close()
				}
				return 0, errors.Wrapf(err, "opening connection to %s", url)
			}
			dbs = append(dbs, nodeDB)
		}
		defer func() {
			for _, nodeDB := range dbs {
				_ = nodeDB.Close()
			}
		}()
		log.Dev.Infof(ctx, `distributing table creation across %d nodes`, len(dbs))
	} else {
		dbs = []*gosql.DB{db}
	}

	const maxTableBatchSize = 5000
	currentTable := 0
	batchNum := 0
	// When dealing with large number of tables, opt to use transactions
	// to minimize the round trips involved, which can be bad on multi-region
	// clusters.
	for currentTable < len(tables) {
		batchStart := timeutil.Now()
		batchEnd := min(currentTable+maxTableBatchSize, len(tables))
		nextBatch := tables[currentTable:batchEnd]
		batchNum++

		// Distribute batches across nodes round-robin.
		nodeDB := dbs[(batchNum-1)%len(dbs)]
		if err := crdb.ExecuteTx(ctx, nodeDB, &gosql.TxOptions{}, func(tx *gosql.Tx) error {
			// Run the operations in a single txn so they complete more quickly.
			if _, err := tx.Exec("SET LOCAL autocommit_before_ddl = false"); err != nil {
				return err
			}
			currentDatabase := ""
			for _, table := range nextBatch {
				// Switch databases if one is explicitly specified for multi-region
				// configurations with multiple databases.
				if table.ObjectPrefix != nil &&
					table.ObjectPrefix.ExplicitCatalog &&
					currentDatabase != table.ObjectPrefix.Catalog() {
					_, err := tx.ExecContext(ctx, "USE $1", table.ObjectPrefix.Catalog())
					if err != nil {
						return err
					}
					currentDatabase = table.ObjectPrefix.Catalog()
				}
				tableName := table.GetResolvedName()
				createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s %s`, tableName.String(), table.Schema)
				if _, err := tx.ExecContext(ctx, createStmt); err != nil {
					return errors.WithDetailf(errors.Wrapf(err, "could not create table: %q", table.Name),
						"SQL: %s", createStmt)
				}
			}
			return nil
		}); err != nil {
			return 0, err
		}
		currentTable = batchEnd
		log.Dev.Infof(ctx, `created %d/%d tables (batch %d on node %d, %d tables in %s)`,
			currentTable, len(tables), batchNum, (batchNum-1)%len(dbs)+1, len(nextBatch), timeutil.Since(batchStart).Round(time.Millisecond))
	}

	if hooks.PreLoad != nil {
		if err := hooks.PreLoad(db); err != nil {
			return 0, errors.Wrapf(err, "Could not preload")
		}
	}

	// SchemaOnlyDataLoader does not load any data, so we return 0 bytes.
	log.Dev.Infof(ctx, `finished creating %d tables in %s (schema only, no data loaded)`,
		len(tables), timeutil.Since(startTime).Round(time.Second))
	return 0, nil
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

	const maxTableBatchSize = 5000
	currentTable := 0
	// When dealing with large number of tables, opt to use transactions
	// to minimize the round trips involved, which can be bad on multi-region
	// clusters.
	for currentTable < len(tables) {
		batchEnd := min(currentTable+maxTableBatchSize, len(tables))
		nextBatch := tables[currentTable:batchEnd]
		if err := crdb.ExecuteTx(ctx, db, &gosql.TxOptions{}, func(tx *gosql.Tx) error {
			// Run the operations in a single txn so they complete more quickly.
			if _, err := tx.Exec("SET LOCAL autocommit_before_ddl = false"); err != nil {
				return err
			}
			currentDatabase := ""
			for _, table := range nextBatch {
				// Switch databases if one is explicitly specified for multi-region
				// configurations with multiple databases.
				if table.ObjectPrefix != nil &&
					table.ObjectPrefix.ExplicitCatalog &&
					currentDatabase != table.ObjectPrefix.Catalog() {
					_, err := tx.ExecContext(ctx, "USE $1", table.ObjectPrefix.Catalog())
					if err != nil {
						return err
					}
					currentDatabase = table.ObjectPrefix.Catalog()
				}
				tableName := table.GetResolvedName()
				createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s %s`, tableName.String(), table.Schema)
				if _, err := tx.ExecContext(ctx, createStmt); err != nil {
					return errors.WithDetailf(errors.Wrapf(err, "could not create table: %q", table.Name),
						"SQL: %s", createStmt)
				}
			}
			return nil
		}); err != nil {
			return 0, err
		}
		currentTable += maxTableBatchSize
	}

	if hooks.PreLoad != nil {
		if err := hooks.PreLoad(db); err != nil {
			return 0, errors.Wrapf(err, "Could not preload")
		}
	}

	var bytesAtomic atomic.Int64
	for _, table := range tables {
		if table.InitialRows.NumBatches == 0 {
			continue
		} else if table.InitialRows.FillBatch == nil {
			return 0, errors.Errorf(
				`initial data is not supported for workload %s`, gen.Meta().Name)
		}
		tableStart := timeutil.Now()
		var tableRowsAtomic atomic.Int64

		batchesPerWorker := table.InitialRows.NumBatches / l.Concurrency
		g, gCtx := errgroup.WithContext(ctx)
		for i := 0; i < l.Concurrency; i++ {
			startIdx := i * batchesPerWorker
			endIdx := startIdx + batchesPerWorker
			if i == l.Concurrency-1 {
				// Account for any rounding error in batchesPerWorker.
				endIdx = table.InitialRows.NumBatches
			}
			table := table // copy for safe reference in Go routine
			tableName := table.GetResolvedName()
			g.Go(func() error {
				var insertStmtBuf bytes.Buffer
				var params []interface{}
				var numRows int
				flush := func() error {
					if len(params) > 0 {
						if table.InitialRows.MayContainDuplicates {
							fmt.Fprint(&insertStmtBuf, ` ON CONFLICT DO NOTHING`)
						}
						insertStmt := insertStmtBuf.String()
						if _, err := db.ExecContext(gCtx, insertStmt, params...); err != nil {
							return errors.Wrapf(err, "failed insert into %s", tableName.String())
						}
					}
					insertStmtBuf.Reset()
					fmt.Fprintf(&insertStmtBuf, `INSERT INTO %s VALUES `, tableName.String())
					params = params[:0]
					numRows = 0
					return nil
				}
				_ = flush()

				for batchIdx := startIdx; batchIdx < endIdx; batchIdx++ {
					for _, row := range table.InitialRows.BatchRows(batchIdx) {
						tableRowsAtomic.Add(1)
						if len(params) != 0 {
							insertStmtBuf.WriteString(`,`)
						}
						insertStmtBuf.WriteString(`(`)
						for i, datum := range row {
							bytesAtomic.Add(workload.ApproxDatumSize(datum))
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
		tableRows := int(tableRowsAtomic.Load())
		log.Dev.Infof(ctx, `imported %s (%s, %d rows)`,
			table.Name, timeutil.Since(tableStart).Round(time.Second), tableRows,
		)
	}
	return bytesAtomic.Load(), nil
}
