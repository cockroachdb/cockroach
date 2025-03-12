// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssmemstorage

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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
		// Record a statement, ensure no insights are generated.
		statsKey := appstatspb.StatementStatisticsKey{
			Query: "SELECT _",
		}
		err := memContainer.RecordStatement(ctx, statsKey, sqlstats.RecordedStmtStats{})
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

func testMonitor(
	ctx context.Context, name redact.SafeString, settings *cluster.Settings,
) *mon.BytesMonitor {
	return mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     mon.MakeMonitorName(name),
		Settings: settings,
	})
}

// TestContainerMemoryAccounting verifies that the memory account is properly
// cleared after calling Clear and Free methods.
func TestContainerMemoryAccountClearing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	memMonitor := testMonitor(ctx, "test-mem", st)
	memMonitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))
	defer memMonitor.Stop(ctx)

	// Create a container with the memory monitor.
	container := New(st, nil, memMonitor, "test-app", nil)

	// Create statement keys
	stmt1Key := appstatspb.StatementStatisticsKey{
		Query:                    "SELECT * FROM table1",
		ImplicitTxn:              true,
		Database:                 "testdb",
		TransactionFingerprintID: appstatspb.TransactionFingerprintID(100),
	}
	stmt1Stats := sqlstats.RecordedStmtStats{
		FingerprintID: appstatspb.StmtFingerprintID(1),
	}

	stmt2Key := appstatspb.StatementStatisticsKey{
		Query:                    "SELECT * FROM table2",
		ImplicitTxn:              true,
		Database:                 "testdb",
		TransactionFingerprintID: appstatspb.TransactionFingerprintID(100),
	}
	stmt2Stats := sqlstats.RecordedStmtStats{
		FingerprintID: appstatspb.StmtFingerprintID(2),
	}

	// Record statements to allocate memory.
	err := container.RecordStatement(ctx, stmt1Key, stmt1Stats)
	require.NoError(t, err)

	err = container.RecordStatement(ctx, stmt2Key, stmt2Stats)
	require.NoError(t, err)

	// Record a transaction to allocate more memory.
	txnKey := appstatspb.TransactionFingerprintID(100)
	txnStats := sqlstats.RecordedTxnStats{}

	err = container.RecordTransaction(ctx, txnKey, txnStats)
	require.NoError(t, err)

	// Verify memory is allocated
	memUsedBefore := container.acc.Used()
	require.Greater(t, memUsedBefore, int64(0), "Expected memory to be allocated")

	// Test Clear method
	container.Clear(ctx)

	// Verify memory account is cleared after Clear().
	memUsedAfterClear := container.acc.Used()
	require.Equal(t, int64(0), memUsedAfterClear, "Memory account should be cleared after Clear")

	// Add more statements to allocate memory again.
	stmt3Key := appstatspb.StatementStatisticsKey{
		Query:                    "SELECT * FROM table3",
		ImplicitTxn:              true,
		Database:                 "testdb",
		TransactionFingerprintID: appstatspb.TransactionFingerprintID(100),
	}
	stmt3Stats := sqlstats.RecordedStmtStats{
		FingerprintID: appstatspb.StmtFingerprintID(3),
	}

	err = container.RecordStatement(ctx, stmt3Key, stmt3Stats)
	require.NoError(t, err)

	// Verify memory is allocated again
	memUsedAfterRealloc := container.acc.Used()
	require.Greater(t, memUsedAfterRealloc, int64(0), "Expected memory to be allocated again")

	// Ensure Free() clears the memory account.
	container.Free(ctx)

	// Verify memory account is cleared after Free().
	memUsedAfterFree := container.acc.Used()
	require.Equal(t, int64(0), memUsedAfterFree, "Memory account should be cleared after Free")

	// Verify that the container can still be used after Free
	// by adding more statements.
	stmt4Key := appstatspb.StatementStatisticsKey{
		Query:                    "SELECT * FROM table4",
		ImplicitTxn:              true,
		Database:                 "testdb",
		TransactionFingerprintID: appstatspb.TransactionFingerprintID(100),
	}
	stmt4Stats := sqlstats.RecordedStmtStats{
		FingerprintID: appstatspb.StmtFingerprintID(4),
	}

	err = container.RecordStatement(ctx, stmt4Key, stmt4Stats)
	require.NoError(t, err)

	// Verify memory is allocated again
	memUsedAfterFreeThenRealloc := container.acc.Used()
	require.Greater(t, memUsedAfterFreeThenRealloc, int64(0), "Expected memory to be allocated after Free")

	container.Clear(ctx)
}
