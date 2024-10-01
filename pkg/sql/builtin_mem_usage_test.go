// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestAggregatesMonitorMemory verifies that the aggregates report their memory
// usage to the memory accounting system. This test works by blocking the query
// with the aggregate when it is in the "draining metadata" state in one
// goroutine and observing the memory monitoring system via
// crdb_internal.node_memory_monitors virtual table in another.
func TestAggregatesMonitorMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	statements := []string{
		// By avoiding printing the aggregate results we prevent anything
		// besides the aggregate itself from using a lot of memory.
		`SELECT length(concat_agg(a)) FROM d.t`,
		`SELECT array_length(array_agg(a), 1) FROM d.t`,
		`SELECT json_typeof(json_agg(a)) FROM d.t`,
	}

	// blockMainCh is used to block the main goroutine until the worker
	// goroutine is trapped by the callback.
	blockMainCh := make(chan struct{})
	// blockWorkerCh is used to block the worker goroutine until the main
	// goroutine checks the memory monitoring state.
	blockWorkerCh := make(chan struct{})
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &ExecutorTestingKnobs{
				DistSQLReceiverPushCallbackFactory: func(_ context.Context, query string) func(rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
					var block bool
					for _, testQuery := range statements {
						block = block || query == testQuery
					}
					if !block {
						return nil
					}
					var seenMeta bool
					return func(row rowenc.EncDatumRow, batch coldata.Batch, meta *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
						if meta != nil && !seenMeta {
							// If this is the first metadata object, then we
							// know that the test query is almost done
							// executing, so unblock the main goroutine and then
							// wait for that goroutine to signal us to proceed.
							blockMainCh <- struct{}{}
							<-blockWorkerCh
							seenMeta = true
						}
						return row, batch, meta
					}
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	// Create a table with a modest number of long strings.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TABLE d.t (a STRING)
`); err != nil {
		t.Fatal(err)
	}
	const numRows, rowSize = 100, 50000
	for i := 0; i < numRows; i++ {
		if _, err := sqlDB.Exec(`INSERT INTO d.t VALUES (repeat('a', $1))`, rowSize); err != nil {
			t.Fatal(err)
		}
	}
	const expectedMemUsage = numRows * rowSize

	for _, statement := range statements {
		errCh := make(chan error)
		go func(statement string) {
			dbConn, err := s.ApplicationLayer().SQLConnE()
			if err != nil {
				errCh <- err
				return
			}
			defer dbConn.Close()
			_, err = dbConn.Exec(statement)
			errCh <- err
		}(statement)
		// Block this goroutine until the worker is at the end of its query
		// execution.
		<-blockMainCh
		// Now verify that we have at least one memory monitor that uses more
		// than the expected memory usage.
		rows, err := sqlDB.Query("SELECT name, used FROM crdb_internal.node_memory_monitors")
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for rows.Next() {
			var name string
			var used int64
			if err = rows.Scan(&name, &used); err != nil {
				t.Fatal(err)
			}
			log.Infof(context.Background(), "%s: %d", name, used)
			// We are likely to not have a separate monitor for the aggregator,
			// so instead we look at the flow monitor for the query. "Our" flow
			// monitor could be uniquely identified by the FlowID, but we can't
			// easily get that information here, so we just assume that if we
			// find the monitor for some flow, and it has large enough memory
			// usage, then this is "ours" (this assumption sounds reasonable
			// since we don't expect internal queries to use this much memory).
			if strings.HasPrefix(name, "flow") && used >= expectedMemUsage {
				found = true
			}
		}
		blockWorkerCh <- struct{}{}
		if err = <-errCh; err != nil {
			t.Fatal(err)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("didn't find a memory monitor with at least %d bytes used", expectedMemUsage)
		}
	}
}
