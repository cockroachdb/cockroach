// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"

	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

// createBenchmarkChangefeed starts a changefeed with some extra hooks. It
// watches `database.table` and outputs to `sinkURI`. The given `feedClock` is
// only used for the internal ExportRequest polling, so a benchmark can write
// data with different timestamps beforehand and simulate the changefeed going
// through them in steps.
//
// The closure handed back cancels the changefeed (blocking until it's shut
// down) and returns an error if the changefeed had failed before the closure
// was called.
func createBenchmarkChangefeed(
	ctx context.Context,
	s serverutils.TestServerInterface,
	feedClock *hlc.Clock,
	database, table string,
	resultsCh chan<- tree.Datums,
) func() error {
	execCfg := &sql.ExecutorConfig{
		DB:           s.DB(),
		Settings:     s.ClusterSettings(),
		Clock:        feedClock,
		LeaseManager: s.LeaseManager().(*sql.LeaseManager),
	}
	tableDesc := sqlbase.GetTableDescriptor(execCfg.DB, database, table)
	details := jobspb.ChangefeedDetails{
		Targets: map[sqlbase.ID]string{tableDesc.ID: tableDesc.Name},
	}
	progress := jobspb.Progress{}
	metrics := MakeMetrics().(*Metrics)

	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- runChangefeedFlow(ctx, execCfg, details, progress, metrics, resultsCh, nil)
	}()
	return func() error {
		select {
		case err := <-errCh:
			return err
		default:
		}
		cancel()
		wg.Wait()
		return nil
	}
}

// loadWorkloadBatches inserts a workload.Table's row batches, each in one
// transaction. It returns the timestamps of these transactions and the byte
// size for use with b.SetBytes.
func loadWorkloadBatches(sqlDB *gosql.DB, table workload.Table) ([]time.Time, int64, error) {
	if _, err := sqlDB.Exec(`CREATE TABLE "` + table.Name + `" ` + table.Schema); err != nil {
		return nil, 0, err
	}

	var now time.Time
	var timestamps []time.Time
	var benchBytes int64

	var insertStmtBuf bytes.Buffer
	var params []interface{}
	for batchIdx := 0; batchIdx < table.InitialRows.NumBatches; batchIdx++ {
		if _, err := sqlDB.Exec(`BEGIN`); err != nil {
			return nil, 0, err
		}

		params = params[:0]
		insertStmtBuf.Reset()
		insertStmtBuf.WriteString(`INSERT INTO "` + table.Name + `" VALUES `)
		for _, row := range table.InitialRows.Batch(batchIdx) {
			if len(params) != 0 {
				insertStmtBuf.WriteString(`,`)
			}
			insertStmtBuf.WriteString(`(`)
			for colIdx, datum := range row {
				if colIdx != 0 {
					insertStmtBuf.WriteString(`,`)
				}
				benchBytes += workload.ApproxDatumSize(datum)
				params = append(params, datum)
				fmt.Fprintf(&insertStmtBuf, `$%d`, len(params))
			}
			insertStmtBuf.WriteString(`)`)
		}
		if _, err := sqlDB.Exec(insertStmtBuf.String(), params...); err != nil {
			return nil, 0, err
		}

		if err := sqlDB.QueryRow(`SELECT transaction_timestamp(); COMMIT;`).Scan(&now); err != nil {
			return nil, 0, err
		}
		timestamps = append(timestamps, now)
	}

	if table.InitialRows.NumTotal != 0 {
		var totalRows int
		if err := sqlDB.QueryRow(
			`SELECT count(*) FROM "` + table.Name + `"`,
		).Scan(&totalRows); err != nil {
			return nil, 0, err
		}
		if table.InitialRows.NumTotal != totalRows {
			return nil, 0, errors.Errorf(`sanity check failed: expected %d rows got %d`,
				table.InitialRows.NumTotal, totalRows)
		}
	}

	return timestamps, benchBytes, nil
}
