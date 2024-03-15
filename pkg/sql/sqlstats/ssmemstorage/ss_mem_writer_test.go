// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ssmemstorage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
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
		numStmtInsights := 0
		knobs := &sqlstats.TestingKnobs{
			InsightsWriterStmtInterceptor: func(sessionID clusterunique.ID, statement *insights.Statement) {
				numStmtInsights++
			},
		}
		memContainer := New(settings,
			nil, /* uniqueServerCount */
			testMonitor(ctx, "test-mon", settings),
			"test-app",
			knobs,
			nil, /* insightsWriter */
			insights.New(settings, insights.NewMetrics()).LatencyInformation(),
		)
		// Record a statement, ensure no insights are generated.
		statsKey := appstatspb.StatementStatisticsKey{
			Query: "SELECT _",
		}
		_, err := memContainer.RecordStatement(ctx, statsKey, sqlstats.RecordedStmtStats{})
		require.NoError(t, err)
		require.Zero(t, numStmtInsights)
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
		numTxnInsights := 0
		knobs := &sqlstats.TestingKnobs{
			InsightsWriterTxnInterceptor: func(ctx context.Context, sessionID clusterunique.ID, transaction *insights.Transaction) {
				numTxnInsights++
			},
		}
		memContainer := New(settings,
			nil, /* uniqueServerCount */
			testMonitor(ctx, "test-mon", settings),
			"test-app",
			knobs,
			nil, /* insightsWriter */
			insights.New(settings, insights.NewMetrics()).LatencyInformation(),
		)
		// Record a transaction, ensure no insights are generated.
		require.NoError(t, memContainer.RecordTransaction(ctx, appstatspb.TransactionFingerprintID(123), sqlstats.RecordedTxnStats{}))
		require.Zero(t, numTxnInsights)
	})
}

func testMonitor(ctx context.Context, name string, settings *cluster.Settings) *mon.BytesMonitor {
	return mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     redact.RedactableString(name),
		Settings: settings,
	})
}
