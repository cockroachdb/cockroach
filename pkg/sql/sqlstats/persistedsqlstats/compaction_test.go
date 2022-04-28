// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsCompactorNilTestingKnobCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	statsCompactor := persistedsqlstats.NewStatsCompactor(
		server.ClusterSettings(),
		server.InternalExecutor().(sqlutil.InternalExecutor),
		server.DB(),
		metric.NewCounter(metric.Metadata{}),
		nil, /* knobs */
	)

	// We run the compactor without disabling the follower read. This can possibly
	// fail due to descriptor not found.
	err := statsCompactor.DeleteOldestEntries(ctx)
	if err != nil {
		require.ErrorIs(t, err, catalog.ErrDescriptorNotFound)
	}
}

func TestSQLStatsCompactor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		stmtCount            int
		maxPersistedRowLimit int
		rowsToDeletePerTxn   int
	}{
		{
			stmtCount:            10,
			maxPersistedRowLimit: 2,
			rowsToDeletePerTxn:   1,
		},
		{
			stmtCount:            10,
			maxPersistedRowLimit: 2,
			rowsToDeletePerTxn:   4,
		},
		{
			stmtCount:            10,
			maxPersistedRowLimit: 2,
			rowsToDeletePerTxn:   128,
		},
		{
			stmtCount:            10,
			maxPersistedRowLimit: 2,
			rowsToDeletePerTxn:   1024,
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: 40,
			rowsToDeletePerTxn:   1,
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: 40,
			rowsToDeletePerTxn:   2,
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: 40,
			rowsToDeletePerTxn:   1024,
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: 205,
		},
	}

	testCluster := serverutils.StartNewTestCluster(
		t, 3 /* numNodes */, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SQLStatsKnobs: &sqlstats.TestingKnobs{
						AOSTClause: "AS OF SYSTEM TIME '-1us'",
					},
				},
			},
		})

	defer testCluster.Stopper().Stop(ctx)

	firstServer := testCluster.Server(0 /* idx */)
	firstPgURL, firstServerConnCleanup := sqlutils.PGUrl(
		t, firstServer.ServingSQLAddr(), "CreateConnections", /* prefix */
		url.User(username.RootUser))
	defer firstServerConnCleanup()

	pgFirstSQLConn, err := gosql.Open("postgres", firstPgURL.String())
	require.NoError(t, err)
	firstSQLConn := sqlutils.MakeSQLRunner(pgFirstSQLConn)
	internalExecutor := firstServer.InternalExecutor().(sqlutil.InternalExecutor)

	defer func() {
		err := pgFirstSQLConn.Close()
		require.NoError(t, err)
	}()

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("stmtCount=%d/maxPersistedRowLimit=%d/rowsDeletePerTxn=%d",
			tc.stmtCount,
			tc.maxPersistedRowLimit,
			tc.rowsToDeletePerTxn,
		), func(t *testing.T) {
			_, err := internalExecutor.ExecEx(
				ctx,
				"truncate-stmt-stats",
				nil,
				sessiondata.InternalExecutorOverride{
					User: username.NodeUserName(),
				},
				"TRUNCATE system.statement_statistics",
			)
			require.NoError(t, err)
			_, err = internalExecutor.ExecEx(
				ctx,
				"truncate-txn-stats",
				nil,
				sessiondata.InternalExecutorOverride{
					User: username.NodeUserName(),
				},
				"TRUNCATE system.transaction_statistics",
			)
			require.NoError(t, err)
			firstServerSQLStats :=
				firstServer.
					SQLServer().(*sql.Server).
					GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
			firstSQLConn.Exec(t,
				"SET CLUSTER SETTING sql.stats.persisted_rows.max = $1",
				tc.maxPersistedRowLimit)

			if tc.rowsToDeletePerTxn > 0 {
				firstSQLConn.Exec(t,
					"SET CLUSTER SETTING sql.stats.cleanup.rows_to_delete_per_txn = $1",
					tc.rowsToDeletePerTxn)
			} else {
				firstSQLConn.Exec(t, "RESET CLUSTER SETTING sql.stats.cleanup.rows_to_delete_per_txn")
			}

			stmt := "SELECT 1"
			for i := 0; i < tc.stmtCount; i++ {
				firstSQLConn.Exec(t, stmt)
				// Mutate the stmt to create different fingerprint.
				stmt = fmt.Sprintf("%s, 1", stmt)
			}

			firstServerSQLStats.Flush(ctx)

			statsCompactor := persistedsqlstats.NewStatsCompactor(
				firstServer.ClusterSettings(),
				firstServer.InternalExecutor().(sqlutil.InternalExecutor),
				firstServer.DB(),
				metric.NewCounter(metric.Metadata{}),
				&sqlstats.TestingKnobs{
					AOSTClause: "AS OF SYSTEM TIME '-1us'",
				},
			)

			// Initial compaction should remove the all the oldest entries.
			expectedDeletedStmtFingerprints, expectedDeletedTxnFingerprints :=
				getTopSortedFingerprints(t, firstSQLConn, tc.maxPersistedRowLimit)
			err = statsCompactor.DeleteOldestEntries(ctx)
			require.NoError(t, err)

			// Sanity check.
			require.Equal(t, tc.maxPersistedRowLimit, len(expectedDeletedStmtFingerprints))
			require.Equal(t, tc.maxPersistedRowLimit, len(expectedDeletedTxnFingerprints))

			actualStmtFingerprints, actualTxnFingerprints :=
				getTopSortedFingerprints(t, firstSQLConn, 0 /* limit */)

			require.GreaterOrEqual(t, tc.maxPersistedRowLimit, len(actualStmtFingerprints))
			require.GreaterOrEqual(t, tc.maxPersistedRowLimit, len(actualTxnFingerprints))

			for fingerprintID := range actualStmtFingerprints {
				for _, deletedFingerprintID := range expectedDeletedStmtFingerprints {
					require.NotEqual(t, deletedFingerprintID, fingerprintID)
				}
			}

			for fingerprintID := range actualTxnFingerprints {
				for _, deletedFingerprintID := range expectedDeletedTxnFingerprints {
					require.NotEqual(t, deletedFingerprintID, fingerprintID)
				}
			}

			// Calling it again should be a noop.
			err = statsCompactor.DeleteOldestEntries(ctx)
			require.NoError(t, err)
			stmtStatsCnt, txnStatsCnt := getPersistedStatsEntry(t, firstSQLConn)
			require.GreaterOrEqual(t, tc.maxPersistedRowLimit, stmtStatsCnt)
			require.GreaterOrEqual(t, tc.maxPersistedRowLimit, txnStatsCnt)
		})
	}
}

func TestSQLStatsCompactionJobMarkedAsAutomatic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer tc.Stopper().Stop(ctx)

	server := tc.Server(0 /* idx */)
	conn := tc.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	jobID, err := launchSQLStatsCompactionJob(server)
	require.NoError(t, err)

	// Ensure the sqlstats job is hidden from the SHOW JOBS command.
	sqlDB.CheckQueryResults(
		t,
		"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = '"+jobspb.TypeAutoSQLStatsCompaction.String()+"'",
		[][]string{{"0"}},
	)

	// Ensure the sqlstats job is displayed in SHOW AUTOMATIC JOBS command.
	sqlDB.CheckQueryResults(
		t,
		fmt.Sprintf("SELECT count(*) FROM [SHOW AUTOMATIC JOBS] WHERE job_id = %d", jobID),
		[][]string{{"1"}},
	)
}

func launchSQLStatsCompactionJob(server serverutils.TestServerInterface) (jobspb.JobID, error) {
	return persistedsqlstats.CreateCompactionJob(
		context.Background(), nil /* createdByInfo */, nil, /* txn */
		server.JobRegistry().(*jobs.Registry),
	)
}

func getPersistedStatsEntry(
	t *testing.T, sqlConn *sqlutils.SQLRunner,
) (stmtStatsCnt, txnStatsCnt int) {
	stmt := "SELECT count(*) FROM %s"

	row := sqlConn.QueryRow(t, fmt.Sprintf(stmt, "system.statement_statistics"))
	row.Scan(&stmtStatsCnt)

	row = sqlConn.QueryRow(t, fmt.Sprintf(stmt, "system.transaction_statistics"))
	row.Scan(&txnStatsCnt)

	return stmtStatsCnt, txnStatsCnt
}

func getTopSortedFingerprints(
	t *testing.T, sqlDb *sqlutils.SQLRunner, limit int,
) (stmtFingerprints, txnFingerprints []uint64) {
	query := `
SELECT fingerprint_id
FROM %s
ORDER BY aggregated_ts`

	if limit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}

	stmtFingerprints = make([]uint64, 0)
	txnFingerprints = make([]uint64, 0)

	fingerprintIDBuffer := make([]byte, 0, 8)
	rows := sqlDb.Query(t, fmt.Sprintf(query, "system.statement_statistics"))
	for rows.Next() {
		fingerprintIDBuffer = fingerprintIDBuffer[:0]
		require.NoError(t, rows.Scan(&fingerprintIDBuffer))
		_, fingerprintID, err := encoding.DecodeUint64Ascending(fingerprintIDBuffer)
		require.NoError(t, err)
		stmtFingerprints = append(stmtFingerprints, fingerprintID)
	}
	require.NoError(t, rows.Close())

	rows = sqlDb.Query(t, fmt.Sprintf(query, "system.transaction_statistics"))
	for rows.Next() {
		fingerprintIDBuffer = fingerprintIDBuffer[:0]
		require.NoError(t, rows.Scan(&fingerprintIDBuffer))
		_, fingerprintID, err := encoding.DecodeUint64Ascending(fingerprintIDBuffer)
		require.NoError(t, err)
		txnFingerprints = append(txnFingerprints, fingerprintID)
	}
	require.NoError(t, rows.Close())

	return stmtFingerprints, txnFingerprints
}
