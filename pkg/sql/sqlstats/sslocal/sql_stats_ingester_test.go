// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
			ingester.IngestStatement(&sqlstats.RecordedStmtStats{
				SessionID:     clusterunique.IDFromBytes([]byte(e.sessionID)),
				FingerprintID: appstatspb.StmtFingerprintID(e.statementID),
			})
		} else {
			ingester.IngestTransaction(&sqlstats.RecordedTxnStats{
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			testSink := &sqlStatsTestSink{}
			ingester := NewSQLStatsIngester(nil, testSink)

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
	testSink := &sqlStatsTestSink{}
	ingester := NewSQLStatsIngester(nil, testSink)
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

	sink := &sqlStatsTestSink{}
	ingester := NewSQLStatsIngester(nil, sink)
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
			ingester.IngestStatement(&sqlstats.RecordedStmtStats{})
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

	sink := &sqlStatsTestSink{}
	ingester := NewSQLStatsIngester(nil, sink)

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
		ingester := NewSQLStatsIngester(knobs)
		ingester.Start(ctx, stopper)
		ingester.IngestStatement(statementA)
		ingester.IngestStatement(statementB)
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
