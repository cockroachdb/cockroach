// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

type sqlStatsTestSink struct {
	mu struct {
		syncutil.RWMutex
		stmts []sqlstats.RecordedStmtStats
		txns  []sqlstats.RecordedTxnStats
	}
}

var _ SQLStatsSink = &sqlStatsTestSink{}

func (s *sqlStatsTestSink) getTotalEventsCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.mu.stmts) + len(s.mu.txns)
}

func getStmtFingerprintIDs(stmts []sqlstats.RecordedStmtStats) []uint64 {
	stmtIds := make([]uint64, len(stmts))
	for i, stmt := range stmts {
		stmtIds[i] = uint64(stmt.FingerprintID)
	}
	return stmtIds
}

func getTxnFingerprintIDs(txns []sqlstats.RecordedTxnStats) []uint64 {
	txnIds := make([]uint64, len(txns))
	for i, txn := range txns {
		txnIds[i] = uint64(txn.FingerprintID)
	}
	return txnIds
}

// ObserveTransaction implements the SQLStatsSink interface.
func (s *sqlStatsTestSink) ObserveTransaction(
	_ctx context.Context,
	transactionStats *sqlstats.RecordedTxnStats,
	statements []*sqlstats.RecordedStmtStats,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stmt := range statements {
		s.mu.stmts = append(s.mu.stmts, *stmt)
	}
	if transactionStats != nil {
		s.mu.txns = append(s.mu.txns, *transactionStats)
	}
}

type testEvent struct {
	sessionID     string
	transactionID uint64
	statementID   uint64
}

func ingestEventsSync(ingester *SQLStatsIngester, events []testEvent) {
	for _, e := range events {
		if e.statementID != 0 {
			ingester.BufferStatement(&sqlstats.RecordedStmtStats{
				SessionID:     clusterunique.IDFromBytes([]byte(e.sessionID)),
				FingerprintID: appstatspb.StmtFingerprintID(e.statementID),
			})
		} else {
			ingester.BufferTransaction(&sqlstats.RecordedTxnStats{
				SessionID:     clusterunique.IDFromBytes([]byte(e.sessionID)),
				FingerprintID: appstatspb.TransactionFingerprintID(e.transactionID),
			})
		}
	}
}

func getOrderingFromTestEvents(events []testEvent) (stmtIds, txnIds []uint64) {
	stmtsBySession := make(map[string][]uint64)
	for _, e := range events {
		if e.statementID != 0 {
			stmtsBySession[e.sessionID] = append(stmtsBySession[e.sessionID], e.statementID)
		}
		if e.transactionID != 0 {
			txnIds = append(txnIds, e.transactionID)
			stmtIds = append(stmtIds, stmtsBySession[e.sessionID]...)
			stmtsBySession[e.sessionID] = nil
		}
	}
	return stmtIds, txnIds

}

func TestSQLIngester(t *testing.T) {
	testCases := []struct {
		name         string
		observations []testEvent
	}{
		{
			name: "One session",
			observations: []testEvent{
				{sessionID: "aaaaaaaaaaaaaaaa", statementID: 10},
				{sessionID: "aaaaaaaaaaaaaaaa", transactionID: 100},
			},
		},
		{
			name: "Interleaved sessions",
			observations: []testEvent{
				{sessionID: "aaaaaaaaaaaaaaaa", statementID: 10},
				{sessionID: "bbbbbbbbbbbbbbbb", statementID: 20},
				{sessionID: "aaaaaaaaaaaaaaaa", statementID: 11},
				{sessionID: "bbbbbbbbbbbbbbbb", statementID: 21},
				{sessionID: "aaaaaaaaaaaaaaaa", transactionID: 100},
				{sessionID: "bbbbbbbbbbbbbbbb", transactionID: 200},
			},
		},
		{
			name: "Multiple transaction sessions",
			observations: []testEvent{
				{sessionID: "aaaaaaaaaaaaaaaa", statementID: 10},
				{sessionID: "aaaaaaaaaaaaaaaa", statementID: 11},
				{sessionID: "bbbbbbbbbbbbbbbb", statementID: 20},
				{sessionID: "bbbbbbbbbbbbbbbb", statementID: 21},
				{sessionID: "bbbbbbbbbbbbbbbb", transactionID: 1},
				{sessionID: "aaaaaaaaaaaaaaaa", transactionID: 2},
				{sessionID: "aaaaaaaaaaaaaaaa", statementID: 10},
				{sessionID: "aaaaaaaaaaaaaaaa", statementID: 11},
				{sessionID: "bbbbbbbbbbbbbbbb", statementID: 20},
				{sessionID: "bbbbbbbbbbbbbbbb", statementID: 21},
				{sessionID: "bbbbbbbbbbbbbbbb", transactionID: 1},
				{sessionID: "aaaaaaaaaaaaaaaa", transactionID: 2},
			},
		},
	}

	settings := cluster.MakeTestingClusterSettings()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			testSink := &sqlStatsTestSink{}
			ingester := NewSQLStatsIngester(settings, nil, NewIngesterMetrics(), nil /* parentMon */, testSink)

			ingester.Start(ctx, stopper, WithFlushInterval(10))
			ingestEventsSync(ingester, tc.observations)

			// Wait for the insights to come through.
			testutils.SucceedsSoon(t, func() error {
				if testSink.getTotalEventsCount() != len(tc.observations) {
					return fmt.Errorf("expected %d events, got %d", len(tc.observations), testSink.getTotalEventsCount())
				}
				return nil
			})

			// We got all the events - no need to acquire lock as no more writes are happening.
			expectedStmts, expectedTxns := getOrderingFromTestEvents(tc.observations)
			require.Equal(t, expectedStmts, getStmtFingerprintIDs(testSink.mu.stmts))
			require.Equal(t, expectedTxns, getTxnFingerprintIDs(testSink.mu.txns))
		})
	}
}

// TestSQLIngester_Clear tests that ingester.Clear does the following:
// - Flushes any events in the buffer for event ingestion.
// - Clears any remaining underlying cached data after ingestion of flushed buffer.
func TestSQLIngester_Clear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	ingesterCtx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ingesterCtx)
	settings := cluster.MakeTestingClusterSettings()
	testSink := &sqlStatsTestSink{}
	ingester := NewSQLStatsIngester(settings, nil, NewIngesterMetrics(), nil /* parentMon */, testSink)
	ingester.Start(ingesterCtx, stopper, WithoutTimedFlush())

	// Fill the ingester's buffer with some data.
	// When we force a flush, the data should be ingested into the sink.
	// Only 3 events will make it into the sink since we only write 1 transaction.
	expectedSinkEvents := 3
	ingesterObservations := []testEvent{
		{sessionID: "aaaaaaaaaaaaaaaa", statementID: 10},
		{sessionID: "bbbbbbbbbbbbbbbb", statementID: 20},
		{sessionID: "aaaaaaaaaaaaaaaa", statementID: 11},
		{sessionID: "bbbbbbbbbbbbbbbb", statementID: 21},
		{sessionID: "aaaaaaaaaaaaaaaa", transactionID: 100},
		{sessionID: "aaaaaaaaaaaaaaaa", statementID: 2},
	}
	ingestEventsSync(ingester, ingesterObservations)

	emptyEvent := event{}
	for i := range ingesterObservations {
		require.NotEqual(t, emptyEvent, ingester.guard.eventBuffer[i])
	}

	// This should flush the current buffer and clear any leftover registered statements after ingestion.
	ingester.Clear()

	testutils.SucceedsSoon(t, func() error {
		if testSink.getTotalEventsCount() != expectedSinkEvents {
			return fmt.Errorf("expected %d events, got %d", expectedSinkEvents, testSink.getTotalEventsCount())
		}
		return nil
	})
	stopper.Stop(ctx)
	require.Empty(t, ingester.statementsBySessionID)
}

func TestSQLIngester_DoesNotBlockWhenReceivingManyObservationsAfterShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	sink := &sqlStatsTestSink{}
	ingester := NewSQLStatsIngester(settings, nil, NewIngesterMetrics(), nil /* parentMon */, sink)
	ingester.Start(ctx, stopper)

	// Simulate a shutdown and wait for the consumer of the ingester's channel to stop.
	stopper.Stop(ctx)
	<-stopper.IsStopped()

	// Send a high volume of SQL observations into the ingester.
	done := make(chan struct{})
	go func() {
		// We push enough observations to fill the ingester's channel at least
		// twice. With no consumer of the channel running and no safeguards in
		// place, this operation would block, which would be bad.
		for i := 0; i < 2*bufferSize+1; i++ {
			ingester.BufferStatement(&sqlstats.RecordedStmtStats{})
		}
		done <- struct{}{}
	}()

	select {
	case <-done:
		// Success!
	case <-time.After(time.Second):
		t.Fatal("Did not finish writing observations into the ingester within the expected time; the operation is probably blocked.")
	}
}

// We had an issue with the insights ingester flush task being blocked
// forever on shutdown. This was because of a bug where the order of
// operations during stopper quiescence could cause `ForceSync()` to be
// triggered twice without an intervening ingest operation. The second
// `ForceSync()` would block forever because the buffer channel has a
// capacity of 2.
func TestSQLIngesterBlockedForceSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	sink := &sqlStatsTestSink{}
	ingester := NewSQLStatsIngester(settings, nil, NewIngesterMetrics(), nil /* parentMon */, sink)

	// We queue up a bunch of sync operations because it's unclear how
	// many will proceed between the `Start()` and `Stop()` calls below.
	ingester.guard.ForceSync()

	wg := sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ingester.guard.ForceSync()
		}()
	}

	ingester.Start(ctx, stopper, WithoutTimedFlush())
	stopper.Stop(ctx)
	<-stopper.IsStopped()
	wg.Wait()
}

func TestSQLIngester_ClearSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("clears per session cache", func(t *testing.T) {
		// Initialize the registry.
		// Create some test data.
		sessionA := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		sessionB := clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
		statementA := &sqlstats.RecordedStmtStats{
			SessionID:     sessionA,
			FingerprintID: 1,
		}
		statementB := &sqlstats.RecordedStmtStats{
			SessionID:     sessionB,
			FingerprintID: 1,
		}
		// Create an ingester with no sinks.
		stopper := stop.NewStopper()
		ctx := context.Background()
		defer stopper.Stop(ctx)
		ingestCh := make(chan struct{})
		sessionClearCh := make(chan struct{}, 2)
		defer close(sessionClearCh)
		knobs := &sqlstats.TestingKnobs{
			OnIngesterFlush: func() {
				select {
				case ingestCh <- struct{}{}:
				default:

				}
			},
			OnIngesterSessionClear: func(_ clusterunique.ID) {
				sessionClearCh <- struct{}{}
			},
		}
		settings := cluster.MakeTestingClusterSettings()
		ingester := NewSQLStatsIngester(settings, knobs, NewIngesterMetrics(), nil /* parentMon */)
		ingester.Start(ctx, stopper)
		ingester.BufferStatement(statementA)
		ingester.BufferStatement(statementB)
		// Wait for the flush.
		<-ingestCh
		require.Len(t, ingester.statementsBySessionID, 2)
		// Clear the cache.
		ingester.ClearSession(sessionA)
		<-sessionClearCh
		require.Len(t, ingester.statementsBySessionID, 1)
		ingester.ClearSession(sessionB)
		<-sessionClearCh
		require.Len(t, ingester.statementsBySessionID, 0)
	})
}

type observedStmt struct {
	FingerprintID            appstatspb.StmtFingerprintID
	TransactionFingerprintID appstatspb.TransactionFingerprintID
}

type capturingSink struct {
	syncutil.Mutex
	observed []observedStmt
}

var _ SQLStatsSink = &capturingSink{}

func (s *capturingSink) ObserveTransaction(
	ctx context.Context,
	transactionStats *sqlstats.RecordedTxnStats,
	statements []*sqlstats.RecordedStmtStats,
) {
	s.Lock()
	defer s.Unlock()
	for _, stmt := range statements {
		s.observed = append(s.observed, observedStmt{
			FingerprintID:            stmt.FingerprintID,
			TransactionFingerprintID: stmt.TransactionFingerprintID,
		})
	}
}

// TestStatsCollectorIngester validates that all statements recorded as part of a
// transaction through the StatsCollector are ingested into the SQLStatsIngester
// with the correct TransactionFingerprintID.
func TestStatsCollectorIngester(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	fakeSink := &capturingSink{}
	ingester := NewSQLStatsIngester(settings, nil, NewIngesterMetrics(), nil /* parentMon */, fakeSink)
	ingester.Start(ctx, stopper, WithFlushInterval(10))

	// Set up a StatsCollector with the ingester.
	st := cluster.MakeTestingClusterSettings()
	appStats := ssmemstorage.New(st, nil, nil, "test", nil)
	uniqueServerCounts := &ssmemstorage.SQLStatsAtomicCounters{}
	phaseTimes := sessionphase.NewTimes()
	statsCollector := NewStatsCollector(
		st,
		appStats,
		ingester,
		phaseTimes,
		uniqueServerCounts,
	)

	sessionID := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	statsCollector.StartTransaction()
	for i := range 100 {
		statsCollector.RecordStatement(ctx, &sqlstats.RecordedStmtStats{
			FingerprintID: appstatspb.StmtFingerprintID(i),
			Query:         fmt.Sprintf("SELECT %d", i),
			ImplicitTxn:   false,
			SessionID:     sessionID,
		})
	}
	txnFingerprintID := appstatspb.TransactionFingerprintID(999)

	statsCollector.RecordTransaction(ctx, &sqlstats.RecordedTxnStats{
		SessionID:     sessionID,
		FingerprintID: txnFingerprintID,
	})
	statsCollector.Close(ctx, sessionID)

	// Wait for the ingester to process the events.
	testutils.SucceedsSoon(t, func() error {
		fakeSink.Lock()
		defer fakeSink.Unlock()
		if len(fakeSink.observed) != 100 {
			return fmt.Errorf("expected 100 statements, got %d", len(fakeSink.observed))
		}
		for _, obs := range fakeSink.observed {
			if obs.TransactionFingerprintID != txnFingerprintID && obs.TransactionFingerprintID != appstatspb.InvalidTransactionFingerprintID {
				return fmt.Errorf("unexpected TransactionFingerprintID: %d", obs.TransactionFingerprintID)
			}
		}
		return nil
	})
}

type stmtToRecord struct {
	stmt           sqlstats.RecordedStmtStats
	expectRecorded bool
}

// TestSQLStatsIngesterMemoryAccounting verifies memory tracking behavior
// including buffering, flushing, limits, and multi-session scenarios.
func TestSQLStatsIngesterMemoryAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sessionA := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaa"))
	sessionB := clusterunique.IDFromBytes([]byte("bbbbbbbbbbbbbbbb"))
	sessionC := clusterunique.IDFromBytes([]byte("cccccccccccccccc"))
	sessionD := clusterunique.IDFromBytes([]byte("dddddddddddddddd"))
	sessionE := clusterunique.IDFromBytes([]byte("eeeeeeeeeeeeeeee"))

	largeQuery := strings.Repeat("SELECT * FROM table_with_very_long_name_to_increase_size ", 50)
	largeIndexRec := strings.Repeat("CREATE INDEX idx ON table(column) ", 20)

	testCases := []struct {
		name                  string
		memoryLimit           int64
		statements            []stmtToRecord
		expectedRecordedCount int // number of statements expected to be successfully recorded
	}{
		{
			name:                  "records statements in a single session successfully",
			memoryLimit:           10 * 1024, // 10KB
			expectedRecordedCount: 2,
			statements: []stmtToRecord{
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM foo WHERE bar = 1 AND baz = 2",
						SessionID:     sessionA,
						FingerprintID: appstatspb.StmtFingerprintID(1),
						App:           "testapp",
						Database:      "testdb",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "UPDATE foo SET baz = 3 WHERE id = 4",
						SessionID:     sessionA,
						FingerprintID: appstatspb.StmtFingerprintID(2),
						App:           "testapp",
					},
					expectRecorded: true,
				},
			},
		},
		{
			name:                  "respects memory limits and drops statements when exhausted in a single session",
			memoryLimit:           30 * 1024, // 30KB
			expectedRecordedCount: 7,         // approximately 7 large statements should fit in 30KB
			statements: []stmtToRecord{
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(1),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(2),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(3),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(4),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(5),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(6),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(7),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(8),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: false, // This and subsequent should fail due to memory limit
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(9),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: false,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(10),
						App:                  "testapp",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: false,
				},
			},
		},
		{
			name:                  "records statements across multiple sessions successfully",
			memoryLimit:           100 * 1024, // 100KB
			expectedRecordedCount: 10,
			statements: []stmtToRecord{
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_0_0",
						SessionID:     sessionA,
						FingerprintID: appstatspb.StmtFingerprintID(1),
						App:           "app0",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_0_1",
						SessionID:     sessionA,
						FingerprintID: appstatspb.StmtFingerprintID(2),
						App:           "app0",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_1_0",
						SessionID:     sessionB,
						FingerprintID: appstatspb.StmtFingerprintID(11),
						App:           "app1",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_1_1",
						SessionID:     sessionB,
						FingerprintID: appstatspb.StmtFingerprintID(12),
						App:           "app1",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_2_0",
						SessionID:     sessionC,
						FingerprintID: appstatspb.StmtFingerprintID(21),
						App:           "app2",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_2_1",
						SessionID:     sessionC,
						FingerprintID: appstatspb.StmtFingerprintID(22),
						App:           "app2",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_3_0",
						SessionID:     sessionD,
						FingerprintID: appstatspb.StmtFingerprintID(31),
						App:           "app3",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_3_1",
						SessionID:     sessionD,
						FingerprintID: appstatspb.StmtFingerprintID(32),
						App:           "app3",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_4_0",
						SessionID:     sessionE,
						FingerprintID: appstatspb.StmtFingerprintID(41),
						App:           "app4",
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:         "SELECT * FROM table_4_1",
						SessionID:     sessionE,
						FingerprintID: appstatspb.StmtFingerprintID(42),
						App:           "app4",
					},
					expectRecorded: true,
				},
			},
		},
		{
			name:                  "recording of statements should drop across multiple sessions when memory limit is reached",
			memoryLimit:           50 * 1024, // 50KB
			expectedRecordedCount: 12,        // approximately 12 large statements should fit in 50KB
			statements: []stmtToRecord{
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(1),
						App:                  "app0",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionB,
						FingerprintID:        appstatspb.StmtFingerprintID(11),
						App:                  "app1",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionC,
						FingerprintID:        appstatspb.StmtFingerprintID(21),
						App:                  "app2",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(2),
						App:                  "app0",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionB,
						FingerprintID:        appstatspb.StmtFingerprintID(12),
						App:                  "app1",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionC,
						FingerprintID:        appstatspb.StmtFingerprintID(22),
						App:                  "app2",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(3),
						App:                  "app0",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionB,
						FingerprintID:        appstatspb.StmtFingerprintID(13),
						App:                  "app1",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionC,
						FingerprintID:        appstatspb.StmtFingerprintID(23),
						App:                  "app2",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(4),
						App:                  "app0",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionB,
						FingerprintID:        appstatspb.StmtFingerprintID(14),
						App:                  "app1",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionC,
						FingerprintID:        appstatspb.StmtFingerprintID(24),
						App:                  "app2",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: true,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionA,
						FingerprintID:        appstatspb.StmtFingerprintID(5),
						App:                  "app0",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: false, // This and subsequent should fail due to memory limit
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionB,
						FingerprintID:        appstatspb.StmtFingerprintID(15),
						App:                  "app1",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: false,
				},
				{
					stmt: sqlstats.RecordedStmtStats{
						Query:                largeQuery,
						SessionID:            sessionC,
						FingerprintID:        appstatspb.StmtFingerprintID(25),
						App:                  "app2",
						IndexRecommendations: []string{largeIndexRec},
					},
					expectRecorded: false,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			settings := cluster.MakeTestingClusterSettings()

			parentMon := mon.NewMonitor(mon.Options{
				Name:     mon.MakeName("test-sql-stats-mon"),
				Settings: settings,
			})
			parentMon.Start(ctx, nil, mon.NewStandaloneBudget(tc.memoryLimit))
			defer parentMon.Stop(ctx)

			testSink := &sqlStatsTestSink{}
			knobs := &sqlstats.TestingKnobs{
				SynchronousSQLStats: true,
			}
			ingester := NewSQLStatsIngester(settings, knobs, NewIngesterMetrics(), parentMon, testSink)
			ingester.Start(ctx, stopper, WithoutTimedFlush())

			// Record all statements and track which sessions had successful recordings.
			sessionsWithRecordings := make(map[clusterunique.ID]bool)
			var recordedCount int
			for _, sr := range tc.statements {
				memBefore := ingester.acc.Used()
				ingester.RecordStatement(&sr.stmt)
				memAfter := ingester.acc.Used()

				if sr.expectRecorded {
					require.Greater(t, memAfter, memBefore,
						"expected memory to be tracked after recording statements")

					sessionsWithRecordings[sr.stmt.SessionID] = true
					recordedCount++
				} else {
					require.Equal(t, memAfter, memBefore,
						"expected memory to stay the same when statement recording is dropped")
				}
			}

			require.Equal(t, tc.expectedRecordedCount, recordedCount,
				"expected %d statements to be recorded, got %d", tc.expectedRecordedCount, recordedCount)

			// Flush each session and verify memory is released.
			memBeforeFlush := ingester.acc.Used()
			for sessionID := range sessionsWithRecordings {
				ingester.FlushBuffer(sessionID)
				ingester.guard.ForceSync()
				<-ingester.syncStatsTestingCh

				memAfterFlush := ingester.acc.Used()
				require.Less(t, memAfterFlush, memBeforeFlush,
					"expected memory to be released or stay the same after flushing session")
				memBeforeFlush = memAfterFlush
			}

			require.Equal(t, int64(0), ingester.acc.Used(),
				"expected all memory to be released after flushing all sessions")

			// Verify sink received expected number of statements after flushing.
			// The tests above verify the memory accounting - here we verify the
			// ingester indeed only processed the expected number of statements
			// and the number processed is in-sync with the memory accounting.
			testSink.mu.RLock()
			sinkStmtCount := len(testSink.mu.stmts)
			testSink.mu.RUnlock()
			require.Equal(t, tc.expectedRecordedCount, sinkStmtCount,
				"expected %d statements to be sent to sink, got %d", tc.expectedRecordedCount, sinkStmtCount)

			if ingester.acc != nil {
				ingester.acc.Close(ctx)
			}
		})
	}
}
