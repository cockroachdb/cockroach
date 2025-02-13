// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsIteratorWithTelemetryFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, goDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlstats.CreateTestingKnobs(),
		},
	})
	defer s.Stopper().Stop(ctx)

	testCases := map[string]string{
		"SELECT _":    "SELECT 1",
		"SELECT _, _": "SELECT 1, 1",
	}

	sqlConn := sqlutils.MakeSQLRunner(goDB)

	for _, stmt := range testCases {
		sqlConn.Exec(t, stmt)
	}

	sqlStats := s.SQLServer().(*sql.Server).GetLocalSQLStatsProvider()

	// We collect all the statement fingerprint IDs so that we can test the
	// transaction stats later.
	fingerprintIDs := make(map[appstatspb.StmtFingerprintID]struct{})
	require.NoError(t,
		sqlStats.IterateStatementStats(ctx, sqlstats.IteratorOptions{},
			func(_ context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
				fingerprintIDs[statistics.ID] = struct{}{}
				return nil
			}))

	t.Run("statement_iterator", func(t *testing.T) {
		require.NoError(t,
			sqlStats.IterateStatementStats(
				ctx,
				sqlstats.IteratorOptions{},
				func(_ context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
					require.NotNil(t, statistics)
					// If we are running our test case, we reset the SQL Stats. The iterator
					// should gracefully handle that.
					if _, ok := testCases[statistics.Key.Query]; ok {
						require.NoError(t, sqlStats.Reset(ctx))
					}
					return nil
				}))
	})

	t.Run("transaction_iterator", func(t *testing.T) {
		for _, stmt := range testCases {
			sqlConn.Exec(t, stmt)
		}
		require.NoError(t,
			sqlStats.IterateTransactionStats(
				ctx,
				sqlstats.IteratorOptions{},
				func(
					ctx context.Context,
					statistics *appstatspb.CollectedTransactionStatistics,
				) error {
					require.NotNil(t, statistics)

					for _, stmtFingerprintID := range statistics.StatementFingerprintIDs {
						if _, ok := fingerprintIDs[stmtFingerprintID]; ok {
							require.NoError(t, sqlStats.Reset(ctx))
						}
					}
					return nil
				}))
	})
}
