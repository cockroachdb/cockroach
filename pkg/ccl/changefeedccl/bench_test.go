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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func BenchmarkChangefeedTicks(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// In PR #38211, we removed the polling based data watcher in changefeeds in
	// favor of RangeFeed. This benchmark worked by writing a bunch of data at
	// certain timestamps and manipulating clocks at runtime so the polling
	// grabbed a little of it at a time. There's fundamentally no way for this to
	// work with RangeFeed without a rewrite, but it's not being used for anything
	// right now, so the rewrite isn't worth it. We should fix this if we need to
	// start doing changefeed perf work at some point.
	b.Skip(`broken in #38211`)

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(b, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(b, `CREATE DATABASE d`)
	sqlDB.Exec(b, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ms'`)

	numRows := 1000
	if testing.Short() {
		numRows = 100
	}
	bankTable := bank.FromRows(numRows).Tables()[0]
	timestamps, _, err := loadWorkloadBatches(sqlDBRaw, bankTable)
	if err != nil {
		b.Fatal(err)
	}

	runBench := func(b *testing.B, feedClock *hlc.Clock) {
		var sinkBytes int64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			sink, cancelFeed, err := createBenchmarkChangefeed(ctx, s, feedClock, `d`, `bank`)
			require.NoError(b, err)
			for rows := 0; rows < numRows; {
				r, sb := sink.WaitForEmit()
				rows += r
				sinkBytes += sb
			}
			b.StopTimer()
			if err := cancelFeed(); err != nil {
				b.Errorf(`%+v`, err)
			}
		}
		b.SetBytes(sinkBytes / int64(b.N))
	}

	b.Run(`InitialScan`, func(b *testing.B) {
		// Use a clock that's immediately larger than any timestamp the data was
		// loaded at to catch it all in the initial scan.
		runBench(b, s.Clock())
	})

	b.Run(`SteadyState`, func(b *testing.B) {
		// TODO(dan): This advances the clock through the timestamps of the ingested
		// data every time it's called, but that's a little unsatisfying. Instead,
		// wait for each batch to come out of the feed before advancing the
		// timestamp.
		var feedTimeIdx int
		feedClock := hlc.NewClock(func() int64 {
			if feedTimeIdx < len(timestamps) {
				feedTimeIdx++
				return timestamps[feedTimeIdx-1].UnixNano()
			}
			return timeutil.Now().UnixNano()
		}, time.Nanosecond)
		runBench(b, feedClock)
	})
}

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

func (s *benchSink) EmitRow(
	ctx context.Context, _ *sqlbase.TableDescriptor, k, v []byte, _ hlc.Timestamp,
) error {
	return s.emit(int64(len(k) + len(v)))
}
func (s *benchSink) EmitResolvedTimestamp(_ context.Context, e Encoder, ts hlc.Timestamp) error {
	var noTopic string
	p, err := e.EncodeResolvedTimestamp(noTopic, ts)
	if err != nil {
		return err
	}
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
) (*benchSink, func() error, error) {
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
	encoder, err := makeJSONEncoder(details.Opts)
	if err != nil {
		return nil, nil, err
	}
	sink := makeBenchSink()

	settings := s.ClusterSettings()
	metrics := MakeMetrics(server.DefaultHistogramWindowInterval).(*Metrics)
	buf := makeBuffer()
	leaseMgr := s.LeaseManager().(*sql.LeaseManager)
	mm := mon.MakeUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource,
		nil /* curCount */, nil /* maxHist */, math.MaxInt64, settings,
	)
	poller := makePoller(
		settings, s.DB(), feedClock, s.GossipI().(*gossip.Gossip), spans, details, initialHighWater, buf,
		leaseMgr, metrics, &mm,
	)

	th := makeTableHistory(func(context.Context, *sqlbase.TableDescriptor) error { return nil }, initialHighWater)
	thUpdater := &tableHistoryUpdater{
		settings: settings,
		db:       s.DB(),
		targets:  details.Targets,
		m:        th,
	}
	rowsFn := kvsToRows(s.LeaseManager().(*sql.LeaseManager), details, buf.Get)
	sf := makeSpanFrontier(spans...)
	tickFn := emitEntries(
		s.ClusterSettings(), details, sf, encoder, sink, rowsFn, TestingKnobs{}, metrics)

	ctx, cancel := context.WithCancel(ctx)
	go func() { _ = poller.RunUsingRangefeeds(ctx) }()
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
						frontier := sf.Frontier()
						if err := emitResolvedTimestamp(ctx, encoder, sink, frontier); err != nil {
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
	return sink, cancelFn, nil
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
	var numRows int

	var insertStmtBuf bytes.Buffer
	var params []interface{}
	for batchIdx := 0; batchIdx < table.InitialRows.NumBatches; batchIdx++ {
		if _, err := sqlDB.Exec(`BEGIN`); err != nil {
			return nil, 0, err
		}

		params = params[:0]
		insertStmtBuf.Reset()
		insertStmtBuf.WriteString(`INSERT INTO "` + table.Name + `" VALUES `)
		for _, row := range table.InitialRows.BatchRows(batchIdx) {
			numRows++
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

	var totalRows int
	if err := sqlDB.QueryRow(
		`SELECT count(*) FROM "` + table.Name + `"`,
	).Scan(&totalRows); err != nil {
		return nil, 0, err
	}
	if numRows != totalRows {
		return nil, 0, errors.Errorf(`sanity check failed: expected %d rows got %d`, numRows, totalRows)
	}

	return timestamps, benchBytes, nil
}
