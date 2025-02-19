// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssmemstorage

import (
	"context"
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
