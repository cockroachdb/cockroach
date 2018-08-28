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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"

	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

type benchSink struct {
	syncutil.Mutex
	cond      *sync.Cond
	emits     int
	emitBytes int64
}

func makeBenchSink() *benchSink {
	s := &benchSink{}
	s.cond = sync.NewCond(&s.Mutex)
	return s
}

func (s *benchSink) EmitRow(ctx context.Context, _ string, k, v []byte) error {
	return s.emit(int64(len(k) + len(v)))
}
func (s *benchSink) EmitResolvedTimestamp(_ context.Context, p []byte) error {
	return s.emit(int64(len(p)))
}
func (s *benchSink) Flush(_ context.Context) error { return nil }
func (s *benchSink) Close() error                  { return nil }
func (s *benchSink) emit(bytes int64) error {
	s.Lock()
	defer s.Unlock()
	s.emits++
	s.emitBytes += bytes
	s.cond.Broadcast()
	return nil
}

// WaitForEmit blocks until at least one thing is emitted by the sink. It
// returns the number of emitted messages and bytes since the last WaitForEmit.
func (s *benchSink) WaitForEmit() (int, int64) {
	s.Lock()
	defer s.Unlock()
	for s.emits == 0 {
		s.cond.Wait()
	}
	emits, emitBytes := s.emits, s.emitBytes
	s.emits, s.emitBytes = 0, 0
	return emits, emitBytes
}

// createBenchmarkChangefeed starts a stripped down changefeed. It watches
// `database.table` and outputs to `sinkURI`. The given `feedClock` is only used
// for the internal ExportRequest polling, so a benchmark can write data with
// different timestamps beforehand and simulate the changefeed going through
// them in steps.
//
// The returned sink can be used to count emits and the closure handed back
// cancels the changefeed (blocking until it's shut down) and returns an error
// if the changefeed had failed before the closure was called.
//
// This intentionally skips the distsql and sink parts to keep the benchmark
// focused on the core changefeed work, but it does include the poller.
func createBenchmarkChangefeed(
	ctx context.Context,
	s serverutils.TestServerInterface,
	feedClock *hlc.Clock,
	database, table string,
) (*benchSink, func() error) {
	tableDesc := sqlbase.GetTableDescriptor(s.DB(), database, table)
	spans := []roachpb.Span{tableDesc.PrimaryIndexSpan()}
	details := jobspb.ChangefeedDetails{
		Targets: jobspb.ChangefeedTargets{tableDesc.ID: jobspb.ChangefeedTarget{
			StatementTimeName: tableDesc.Name,
		}},
		Opts: map[string]string{
			optEnvelope: string(optEnvelopeRow),
		},
	}
	initialHighWater := hlc.Timestamp{}
	sink := makeBenchSink()

	buf := makeBuffer()
	poller := makePoller(
		s.ClusterSettings(), s.DB(), feedClock, s.Gossip(), spans, details, initialHighWater, buf)

	th := makeTableHistory(func(*sqlbase.TableDescriptor) error { return nil }, initialHighWater)
	thUpdater := &tableHistoryUpdater{
		settings: s.ClusterSettings(),
		db:       s.DB(),
		targets:  details.Targets,
		m:        th,
	}
	rowsFn := kvsToRows(s.LeaseManager().(*sql.LeaseManager), th, details, buf.Get)
	tickFn := emitEntries(details, sink, rowsFn, TestingKnobs{})

	ctx, cancel := context.WithCancel(ctx)
	go func() { _ = poller.Run(ctx) }()
	go func() { _ = thUpdater.PollTableDescs(ctx) }()

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := func() error {
			sf := makeSpanFrontier(spans...)
			for {
				// This is basically the ChangeAggregator processor.
				resolvedSpans, err := tickFn(ctx)
				if err != nil {
					return err
				}
				// This is basically the ChangeFrontier processor, the resolved
				// spans are normally sent using distsql, so we're missing a bit
				// of overhead here.
				for _, rs := range resolvedSpans {
					if sf.Forward(rs.Span, rs.Timestamp) {
						if err := emitResolvedTimestamp(ctx, details, sink, nil, sf); err != nil {
							return err
						}
					}
				}
			}
		}()
		errCh <- err
	}()
	cancelFn := func() error {
		select {
		case err := <-errCh:
			return err
		default:
		}
		cancel()
		wg.Wait()
		return nil
	}
	return sink, cancelFn
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
