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

// TestSQLStatsIngesterMemoryAccounting verifies that memory is properly tracked
// when buffering and flushing statements.
func TestSQLStatsIngesterMemoryAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	parentMon := mon.NewUnlimitedMonitor(
		ctx,
		mon.Options{
			Name:     mon.MakeName("test-sql-stats-mon"),
			Settings: settings,
		},
	)

	testSink := &sqlStatsTestSink{}
	knobs := &sqlstats.TestingKnobs{
		SynchronousSQLStats: true, // make ingestion synchronous for testing
	}
	ingester := NewSQLStatsIngester(settings, knobs, NewIngesterMetrics(), parentMon, testSink)
	ingester.Start(ctx, stopper, WithoutTimedFlush())

	sessionID := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaa"))

	// Create and buffer a statement
	stmt := &sqlstats.RecordedStmtStats{
		Query:         "SELECT * FROM foo WHERE bar = 1 AND baz = 2",
		SessionID:     sessionID,
		FingerprintID: appstatspb.StmtFingerprintID(1),
		App:           "testapp",
		Database:      "testdb",
	}
	stmtSize := stmt.Size()
	ingester.RecordStatement(stmt)

	// Verify memory increased
	bytesUsed1 := ingester.acc.Used()

	require.Greater(t, bytesUsed1, int64(0),
		"expected account to track memory after buffering statement")
	require.GreaterOrEqual(t, bytesUsed1, stmtSize,
		"account should track at least the statement size")

	// Buffer another statement to the same session
	stmt2 := &sqlstats.RecordedStmtStats{
		Query:         "UPDATE foo SET baz = 3 WHERE id = 4",
		SessionID:     sessionID,
		FingerprintID: appstatspb.StmtFingerprintID(2),
		App:           "testapp",
	}
	ingester.RecordStatement(stmt2)

	bytesUsed2 := ingester.acc.Used()

	require.Greater(t, bytesUsed2, bytesUsed1,
		"expected account to increase after buffering second statement")

	// Flush should release memory
	// Note: FlushBuffer is async, so we need to force sync to wait for completion
	ingester.FlushBuffer(sessionID)
	ingester.guard.ForceSync()
	// Wait for the ingester to process the flush
	<-ingester.syncStatsTestingCh

	bytesUsedAfterFlush := ingester.acc.Used()

	require.Equal(t, bytesUsedAfterFlush, int64(0),
		"expected account memory to decrease after flushing")

	stopper.Stop(ctx)
}

// TestSQLStatsIngesterMemoryLimit verifies that the ingester respects memory limits.
func TestSQLStatsIngesterMemoryLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	// Use a limited monitor to enforce actual memory limits
	const memoryLimit = 50 * 1024 // 50KB
	parentMon := mon.NewMonitor(mon.Options{
		Name:     mon.MakeName("test-sql-stats-limited-mon"),
		Settings: settings,
	})
	parentMon.Start(ctx, nil /* reserved */, mon.NewStandaloneBudget(memoryLimit))

	testSink := &sqlStatsTestSink{}
	knobs := &sqlstats.TestingKnobs{
		SynchronousSQLStats: true,
	}
	ingester := NewSQLStatsIngester(settings, knobs, NewIngesterMetrics(), parentMon, testSink)
	ingester.Start(ctx, stopper, WithoutTimedFlush())

	sessionID := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaa"))

	// Create large statements that will exceed the memory limit
	largeQuery := strings.Repeat("SELECT * FROM table_with_very_long_name_to_increase_size ", 50)
	largeIndexRec := strings.Repeat("CREATE INDEX idx ON table(column) ", 20)

	statementsBuffered := 0
	accountUsedBefore := ingester.acc.Used()

	// Keep buffering statements until we hit the memory limit
	for i := 0; i < 100; i++ {
		stmt := &sqlstats.RecordedStmtStats{
			Query:                largeQuery,
			SessionID:            sessionID,
			FingerprintID:        appstatspb.StmtFingerprintID(i + 1),
			App:                  "testapp",
			IndexRecommendations: []string{largeIndexRec},
		}

		beforeUsed := ingester.acc.Used()
		ingester.RecordStatement(stmt)
		afterUsed := ingester.acc.Used()

		// If memory didn't increase, we've hit the limit and the statement was dropped
		if afterUsed == beforeUsed && i > 0 {
			t.Logf("Hit memory limit after buffering %d statements (used %d bytes, limit %d)",
				statementsBuffered, afterUsed, memoryLimit)
			break
		}
		statementsBuffered++
	}

	// Verify we buffered some statements but hit the limit
	require.Greater(t, statementsBuffered, 0, "should have buffered at least one statement")
	require.Less(t, statementsBuffered, 100, "should have hit memory limit before buffering all statements")

	accountUsedAfter := ingester.acc.Used()
	t.Logf("Successfully buffered %d statements (used %d bytes, increase: %d, limit: %d)",
		statementsBuffered, accountUsedAfter, accountUsedAfter-accountUsedBefore, memoryLimit)

	// Clean up: flush and clear the ingester's account before stopping the monitor
	ingester.FlushBuffer(sessionID)
	ingester.guard.ForceSync()
	<-ingester.syncStatsTestingCh

	// Close the ingester's account
	if ingester.acc != nil {
		ingester.acc.Close(ctx)
	}

	// Now safe to stop the monitor
	parentMon.Stop(ctx)
	stopper.Stop(ctx)
}

// TestSQLStatsIngesterMemoryExhaustion verifies that when memory is exhausted,
// statements are properly dropped and don't make it to the sink.
func TestSQLStatsIngesterMemoryExhaustion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()

	// Set a moderate memory limit to trigger exhaustion after a few statements
	const memoryLimit = 10 * 1024 // 10KB
	parentMon := mon.NewMonitor(mon.Options{
		Name:     mon.MakeName("test-sql-stats-exhaustion-mon"),
		Settings: settings,
	})
	parentMon.Start(ctx, nil /* reserved */, mon.NewStandaloneBudget(memoryLimit))

	testSink := &sqlStatsTestSink{}
	knobs := &sqlstats.TestingKnobs{
		SynchronousSQLStats: true,
	}
	ingester := NewSQLStatsIngester(settings, knobs, NewIngesterMetrics(), parentMon, testSink)
	ingester.Start(ctx, stopper, WithoutTimedFlush())

	sessionID := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaa"))

	// Create large statements to quickly exhaust memory
	largeQuery := strings.Repeat("SELECT * FROM very_long_table_name ", 50)

	// Try to buffer many statements - some will be dropped due to memory limits
	const totalAttempts = 20
	for i := 0; i < totalAttempts; i++ {
		stmt := &sqlstats.RecordedStmtStats{
			Query:         largeQuery,
			SessionID:     sessionID,
			FingerprintID: appstatspb.StmtFingerprintID(i + 1),
			App:           "testapp",
		}
		ingester.RecordStatement(stmt)
	}

	memUsedAfterBuffering := ingester.acc.Used()
	t.Logf("Memory used after buffering: %d bytes (limit: %d)", memUsedAfterBuffering, memoryLimit)

	// Verify some memory was used (at least one statement was buffered)
	require.Greater(t, memUsedAfterBuffering, int64(0),
		"at least one statement should have been buffered")

	// Flush the buffer - this will send buffered statements to the sink
	ingester.FlushBuffer(sessionID)
	ingester.guard.ForceSync()
	<-ingester.syncStatsTestingCh

	// Verify that statements made it to the sink
	testSink.mu.RLock()
	sinkStmtCount := len(testSink.mu.stmts)
	testSink.mu.RUnlock()

	t.Logf("Sink received %d statements (attempted %d)", sinkStmtCount, totalAttempts)

	// We should have received at least one statement but not all of them
	require.Greater(t, sinkStmtCount, 0,
		"sink should have received at least one statement")
	require.Less(t, sinkStmtCount, totalAttempts,
		"sink should not have received all statements due to memory limits")

	// Clean up the account
	if ingester.acc != nil {
		ingester.acc.Close(ctx)
	}
	parentMon.Stop(ctx)
	stopper.Stop(ctx)
}

// TestSQLStatsIngesterMemoryReleaseOnClearSession verifies that memory is properly
// released when a session is cleared.
func TestSQLStatsIngesterMemoryReleaseOnClearSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()

	parentMon := mon.NewUnlimitedMonitor(
		ctx,
		mon.Options{
			Name:     mon.MakeName("test-sql-stats-clear-mon"),
			Settings: settings,
		},
	)

	testSink := &sqlStatsTestSink{}
	knobs := &sqlstats.TestingKnobs{
		SynchronousSQLStats: true,
	}
	ingester := NewSQLStatsIngester(settings, knobs, NewIngesterMetrics(), parentMon, testSink)
	ingester.Start(ctx, stopper, WithoutTimedFlush())

	sessionID := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaa"))

	memUsed := int64(0)
	// Buffer multiple statements for the session
	for i := 0; i < 10; i++ {
		stmt := &sqlstats.RecordedStmtStats{
			Query:         fmt.Sprintf("SELECT %d FROM table_name", i),
			SessionID:     sessionID,
			FingerprintID: appstatspb.StmtFingerprintID(i + 1),
		}
		ingester.RecordStatement(stmt)
		memUsed += stmt.Size()
	}

	allocAfterBuffer := ingester.acc.Used()

	require.GreaterOrEqual(t, allocAfterBuffer, memUsed,
		"expected account memory to increase the correct amount")

	// Clear the session (simulating session termination)
	ingester.FlushBuffer(sessionID)
	ingester.guard.ForceSync()
	<-ingester.syncStatsTestingCh

	allocAfterClear := ingester.acc.Used()

	require.Less(t, allocAfterClear, allocAfterBuffer,
		"expected memory to be released after clearing session")

	t.Logf("after clear: %d bytes", allocAfterClear)
	require.Equal(t, allocAfterClear, int64(0),
		"expected account memory to decrease after flushing")

	stopper.Stop(ctx)
}

// TestSQLStatsIngesterMultipleSessionsMemoryTracking verifies that memory
// accounting works correctly with multiple sessions.
func TestSQLStatsIngesterMultipleSessionsMemoryTracking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()

	parentMon := mon.NewUnlimitedMonitor(
		ctx,
		mon.Options{
			Name:     mon.MakeName("test-sql-stats-multi-session-mon"),
			Settings: settings,
		},
	)

	testSink := &sqlStatsTestSink{}
	knobs := &sqlstats.TestingKnobs{
		SynchronousSQLStats: true,
	}
	ingester := NewSQLStatsIngester(settings, knobs, NewIngesterMetrics(), parentMon, testSink)
	ingester.Start(ctx, stopper, WithoutTimedFlush())

	// Create multiple sessions
	sessions := make([]clusterunique.ID, 5)
	for i := range sessions {
		sessions[i] = clusterunique.IDFromBytes([]byte(fmt.Sprintf("session%d%s", i, strings.Repeat("0", 20))))
	}

	accountBefore := ingester.acc.Used()

	// Buffer statements for each session
	for sessionIdx, sessionID := range sessions {
		for stmtIdx := 0; stmtIdx < 5; stmtIdx++ {
			stmt := &sqlstats.RecordedStmtStats{
				Query:         fmt.Sprintf("SELECT * FROM table_%d_%d", sessionIdx, stmtIdx),
				SessionID:     sessionID,
				FingerprintID: appstatspb.StmtFingerprintID(sessionIdx*10 + stmtIdx + 1),
				App:           fmt.Sprintf("app%d", sessionIdx),
			}
			ingester.RecordStatement(stmt)
		}
	}

	accountAfterBuffer := ingester.acc.Used()
	t.Logf("After buffering 25 statements (5 sessions × 5 statements): %d bytes (increase: %d)",
		accountAfterBuffer, accountAfterBuffer-accountBefore)

	// Flush one session at a time and verify memory is released
	for i, sessionID := range sessions {
		ingester.FlushBuffer(sessionID)
		ingester.guard.ForceSync()
		<-ingester.syncStatsTestingCh

		currentAccount := ingester.acc.Used()
		t.Logf("After flushing session %d: %d bytes", i, currentAccount)
	}

	finalAccount := ingester.acc.Used()
	require.Less(t, finalAccount, accountAfterBuffer,
		"expected memory to be released as sessions are flushed")

	t.Logf("Final memory: %d bytes (released: %d bytes)",
		finalAccount, accountAfterBuffer-finalAccount)

	stopper.Stop(ctx)
}
