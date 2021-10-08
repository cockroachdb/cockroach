// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
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
	serverParams, _ := tests.CreateTestServerParams()
	s, goDB, _ := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(ctx)

	testCases := map[string]string{
		"SELECT _":    "SELECT 1",
		"SELECT _, _": "SELECT 1, 1",
	}

	sqlConn := sqlutils.MakeSQLRunner(goDB)

	for _, stmt := range testCases {
		sqlConn.Exec(t, stmt)
	}

	sqlStats := s.SQLServer().(*sql.Server).GetSQLStatsProvider()

	// We collect all the statement fingerprint IDs so that we can test the
	// transaction stats later.
	fingerprintIDs := make(map[roachpb.StmtFingerprintID]struct{})
	require.NoError(t,
		sqlStats.IterateStatementStats(ctx, &sqlstats.IteratorOptions{},
			func(_ context.Context, statistics *roachpb.CollectedStatementStatistics) error {
				fingerprintIDs[statistics.ID] = struct{}{}
				return nil
			}))

	t.Run("statement_iterator", func(t *testing.T) {
		require.NoError(t,
			sqlStats.IterateStatementStats(
				ctx,
				&sqlstats.IteratorOptions{},
				func(_ context.Context, statistics *roachpb.CollectedStatementStatistics) error {
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
				&sqlstats.IteratorOptions{},
				func(
					ctx context.Context,
					statistics *roachpb.CollectedTransactionStatistics,
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
