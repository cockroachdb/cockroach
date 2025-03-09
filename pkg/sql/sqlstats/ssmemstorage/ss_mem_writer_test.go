// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssmemstorage

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestRecordStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()

	t.Run("skips recording insights when txn stats disabled", func(t *testing.T) {
		// Disable the txn stats cluster setting, which should prevent insights from being recorded.
		settings.Manual.Store(true)
		sqlstats.TxnStatsEnable.Override(ctx, &settings.SV, false)
		// Initialize knobs & mem container.
		memContainer := New(settings,
			nil, /* uniqueServerCount */
			testMonitor(ctx, "test-mon", settings),
			"test-app",
			nil,
		)
		err := memContainer.RecordStatement(ctx, &sqlstats.RecordedStmtStats{
			Query: "SELECT _",
		})
		require.NoError(t, err)
	})
}

func TestRecordTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()

	t.Run("skips recording insights when txn stats disabled", func(t *testing.T) {
		// Disable the txn stats cluster setting, which should prevent insights from being recorded.
		settings.Manual.Store(true)
		sqlstats.TxnStatsEnable.Override(ctx, &settings.SV, false)
		// Initialize knobs & mem container.
		memContainer := New(settings,
			nil, /* uniqueServerCount */
			testMonitor(ctx, "test-mon", settings),
			"test-app",
			nil,
		)
		// Record a transaction, ensure no insights are generated.
		require.NoError(t, memContainer.RecordTransaction(ctx, appstatspb.TransactionFingerprintID(123), sqlstats.RecordedTxnStats{}))
	})
}

// TestContainer_Add verifies that the Container's Add method correctly merges statistics
// from a source container into a destination container. It tests two scenarios via adding
// the same container repeatedly to sanity check that the add operation works as expected:
//  1. Multiple adds of the same stats (values should accumulate, stats averages should not change)
//  2. Multiple adds of a stats with zero values (values should accumulate, stats averages should decrease)
func TestContainer_Add(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("basic statement and transaction stats merge", func(t *testing.T) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()

		// Create source container and dest container.
		src := New(settings,
			nil, /* uniqueServerCount */
			testMonitor(ctx, "test-mon", settings),
			"test-app-src",
			nil,
		)
		dest := New(settings,
			nil, /* uniqueServerCount */
			testMonitor(ctx, "test-mon", settings),
			"test-app-dest",
			nil,
		)

		// Add some statement stats to the source container
		mockedStmtKey := stmtKey{
			fingerprintID: 1,
		}
		stmtStats := &sqlstats.RecordedStmtStats{
			FingerprintID:      1,
			Query:              "SELECT * FROM test_table",
			Database:           "test_db",
			ServiceLatencySec:  0.1,
			RowsAffected:       10,
			IdleLatencySec:     0.01,
			ParseLatencySec:    0.02,
			PlanLatencySec:     0.03,
			RunLatencySec:      0.04,
			OverheadLatencySec: 0.5,
			BytesRead:          100,
			RowsRead:           20,
			RowsWritten:        5,
			Failed:             true,
			StatementError:     errors.New("test error"),
		}
		require.NoError(t, src.RecordStatement(ctx, stmtStats))

		// Add some transaction stats to the source container
		txnFingerprintID := appstatspb.TransactionFingerprintID(123)
		txnStats := sqlstats.RecordedTxnStats{
			RowsAffected:   10,
			ServiceLatency: 1,
			RetryLatency:   2,
			CommitLatency:  3,
			IdleLatency:    4,
			RetryCount:     1,
			RowsRead:       20,
			RowsWritten:    5,
			BytesRead:      100,
		}
		require.NoError(t, src.RecordTransaction(ctx, txnFingerprintID, txnStats))

		// In the src destination, these 'reduced' entries will have
		// 0 values.
		// The Add() calls should increment the count and other counters,
		// but decrease any appstatspb.NumericStats averages.
		emptyStmtStatsKey := stmtKey{
			fingerprintID: 321,
		}
		reducedStmtStats := &sqlstats.RecordedStmtStats{
			FingerprintID:      appstatspb.StmtFingerprintID(321),
			Query:              "SELECT * FROM test_table",
			ServiceLatencySec:  50,
			RowsAffected:       1000,
			IdleLatencySec:     10,
			ParseLatencySec:    20,
			PlanLatencySec:     30,
			RunLatencySec:      40,
			OverheadLatencySec: 58,
			BytesRead:          60,
			RowsRead:           70,
			RowsWritten:        80,
			Failed:             true,
			StatementError:     errors.New("test error"),
		}
		reducedTxnFingerprintID := appstatspb.TransactionFingerprintID(321)
		reducedTxnStats := sqlstats.RecordedTxnStats{
			RowsAffected:   100,
			ServiceLatency: 500 * time.Millisecond,
			RetryLatency:   100 * time.Millisecond,
			CommitLatency:  200 * time.Millisecond,
			IdleLatency:    50 * time.Millisecond,
			RetryCount:     53,
			RowsRead:       20,
			RowsWritten:    5,
			BytesRead:      100,
		}
		require.NoError(t, dest.RecordStatement(ctx, reducedStmtStats))
		require.NoError(t, dest.RecordTransaction(ctx, reducedTxnFingerprintID, reducedTxnStats))
		require.NoError(t, src.RecordStatement(ctx, &sqlstats.RecordedStmtStats{
			FingerprintID: appstatspb.StmtFingerprintID(321),
		}))
		require.NoError(t, src.RecordTransaction(ctx, reducedTxnFingerprintID, sqlstats.RecordedTxnStats{}))

		for i := 0; i < 10; i++ {
			require.NoError(t, dest.Add(ctx, src))
			// Check results.
			verifyStmtStatsMultiple(t, i+1, stmtStats, dest.getStatsForStmtWithKey(mockedStmtKey))
			verifyStmtStatsReduced(t, i+2, reducedStmtStats, dest.getStatsForStmtWithKey(emptyStmtStatsKey))
			verifyTxnStatsMultiple(t, i+1, &txnStats, dest.getStatsForTxnWithKey(txnFingerprintID))
			verifyTxnStatsReduced(t, i+2, &reducedTxnStats, dest.getStatsForTxnWithKey(reducedTxnFingerprintID))
		}
	})
}

const epsilon = 0.0000001

// verifyStmtStatsMultiple verifies that statement statistics have been recorded
// exactly 'count' times in destStmtStats. The averaged values should match the
// original stmtStats values.
func verifyStmtStatsMultiple(
	t *testing.T, count int, stmtStats *sqlstats.RecordedStmtStats, destStmtStats *stmtStats,
) {
	require.NotNil(t, destStmtStats)
	require.Equal(t, destStmtStats.mu.data.Count, int64(count))
	require.Equal(t, destStmtStats.mu.data.FailureCount, int64(count))
	require.InEpsilon(t, float64(stmtStats.RowsAffected), destStmtStats.mu.data.NumRows.Mean, epsilon)
	require.InEpsilon(t, float64(stmtStats.RowsAffected), destStmtStats.mu.data.NumRows.Mean, epsilon)
	require.InEpsilon(t, stmtStats.IdleLatencySec, destStmtStats.mu.data.IdleLat.Mean, epsilon)
	require.InEpsilon(t, stmtStats.ParseLatencySec, destStmtStats.mu.data.ParseLat.Mean, epsilon)
	require.InEpsilon(t, stmtStats.PlanLatencySec, destStmtStats.mu.data.PlanLat.Mean, epsilon)
	require.InEpsilon(t, stmtStats.RunLatencySec, destStmtStats.mu.data.RunLat.Mean, epsilon)
	require.InEpsilon(t, stmtStats.OverheadLatencySec, destStmtStats.mu.data.OverheadLat.Mean, epsilon)
	require.InEpsilon(t, float64(stmtStats.BytesRead), destStmtStats.mu.data.BytesRead.Mean, epsilon)
	require.InEpsilon(t, float64(stmtStats.RowsRead), destStmtStats.mu.data.RowsRead.Mean, epsilon)
	require.InEpsilon(t, float64(stmtStats.RowsWritten), destStmtStats.mu.data.RowsWritten.Mean, epsilon)
}

// verifyStmtStatsReduced verifies that statement statistics have been properly
// averaged over 'count' recordings in destStmtStats.
func verifyStmtStatsReduced(
	t *testing.T, count int, stmtStats *sqlstats.RecordedStmtStats, destStmtStats *stmtStats,
) {
	cnt := float64(count)
	require.NotNil(t, destStmtStats)
	require.Equal(t, destStmtStats.mu.data.Count, int64(count))
	require.Equal(t, destStmtStats.mu.data.FailureCount, int64(1))
	require.InEpsilon(t, float64(stmtStats.RowsAffected)/cnt, destStmtStats.mu.data.NumRows.Mean, epsilon)
	require.InEpsilon(t, stmtStats.IdleLatencySec/cnt, destStmtStats.mu.data.IdleLat.Mean, epsilon)
	require.InEpsilon(t, stmtStats.ParseLatencySec/cnt, destStmtStats.mu.data.ParseLat.Mean, epsilon)
	require.InEpsilon(t, stmtStats.PlanLatencySec/cnt, destStmtStats.mu.data.PlanLat.Mean, epsilon)
	require.InEpsilon(t, stmtStats.RunLatencySec/cnt, destStmtStats.mu.data.RunLat.Mean, epsilon)
	require.InEpsilon(t, stmtStats.OverheadLatencySec/cnt, destStmtStats.mu.data.OverheadLat.Mean, epsilon)
	require.InEpsilon(t, float64(stmtStats.BytesRead)/cnt, destStmtStats.mu.data.BytesRead.Mean, epsilon)
	require.InEpsilon(t, float64(stmtStats.RowsRead)/cnt, destStmtStats.mu.data.RowsRead.Mean, epsilon)
	require.InEpsilon(t, float64(stmtStats.RowsWritten)/cnt, destStmtStats.mu.data.RowsWritten.Mean, epsilon)
}

// verifyTxnStatsMultiple verifies that transaction statistics have been recorded
// exactly 'count' times in destTxnStats. The averaged values should match the original
// txnStats values.
func verifyTxnStatsMultiple(
	t *testing.T, count int, txnStats *sqlstats.RecordedTxnStats, destTxnStats *txnStats,
) {
	require.NotNil(t, destTxnStats)
	require.Equal(t, destTxnStats.mu.data.Count, int64(count))
	require.InEpsilon(t, float64(txnStats.RowsAffected), destTxnStats.mu.data.NumRows.Mean, epsilon)
	require.InEpsilon(t, txnStats.ServiceLatency.Seconds(), destTxnStats.mu.data.ServiceLat.Mean, epsilon)
	require.InEpsilon(t, txnStats.RetryLatency.Seconds(), destTxnStats.mu.data.RetryLat.Mean, epsilon)
	require.InEpsilon(t, txnStats.CommitLatency.Seconds(), destTxnStats.mu.data.CommitLat.Mean, epsilon)
	require.InEpsilon(t, txnStats.IdleLatency.Seconds(), destTxnStats.mu.data.IdleLat.Mean, epsilon)
	require.InEpsilon(t, float64(txnStats.RowsRead), destTxnStats.mu.data.RowsRead.Mean, epsilon)
	require.InEpsilon(t, float64(txnStats.RowsWritten), destTxnStats.mu.data.RowsWritten.Mean, epsilon)
	require.InEpsilon(t, float64(txnStats.BytesRead), destTxnStats.mu.data.BytesRead.Mean, epsilon)
}

// verifyTxnStatsReduced verifies that transaction statistics have been properly
// averaged over 'count' recordings in destTxnStats.
func verifyTxnStatsReduced(
	t *testing.T, count int, txnStats *sqlstats.RecordedTxnStats, destTxnStats *txnStats,
) {
	cnt := float64(count)
	require.NotNil(t, destTxnStats)
	require.Equal(t, destTxnStats.mu.data.Count, int64(count))
	require.InEpsilon(t, float64(txnStats.RowsAffected)/cnt, destTxnStats.mu.data.NumRows.Mean, epsilon)
	require.InEpsilon(t, txnStats.ServiceLatency.Seconds()/cnt, destTxnStats.mu.data.ServiceLat.Mean, epsilon)
	require.InEpsilon(t, txnStats.RetryLatency.Seconds()/cnt, destTxnStats.mu.data.RetryLat.Mean, epsilon)
	require.InEpsilon(t, txnStats.CommitLatency.Seconds()/cnt, destTxnStats.mu.data.CommitLat.Mean, epsilon)
	require.InEpsilon(t, txnStats.IdleLatency.Seconds()/cnt, destTxnStats.mu.data.IdleLat.Mean, epsilon)
	require.InEpsilon(t, float64(txnStats.RowsRead)/cnt, destTxnStats.mu.data.RowsRead.Mean, epsilon)
	require.InEpsilon(t, float64(txnStats.RowsWritten)/cnt, destTxnStats.mu.data.RowsWritten.Mean, epsilon)
	require.InEpsilon(t, float64(txnStats.BytesRead)/cnt, destTxnStats.mu.data.BytesRead.Mean, epsilon)
}

func testMonitor(
	ctx context.Context, name redact.SafeString, settings *cluster.Settings,
) *mon.BytesMonitor {
	return mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     mon.MakeMonitorName(name),
		Settings: settings,
	})
}
