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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
		maxPersistedRowLimit []int
	}{
		{
			stmtCount:            10,
			maxPersistedRowLimit: []int{2, 9},
		},
		{
			stmtCount:            200,
			maxPersistedRowLimit: []int{160, 205},
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
		url.User(security.RootUser))
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
		t.Run(fmt.Sprintf("stmtCount=%d", tc.stmtCount), func(t *testing.T) {
			for _, maxPersistedRowLimit := range tc.maxPersistedRowLimit {
				t.Run(fmt.Sprintf("maxPersistedRowLimit=%d", maxPersistedRowLimit), func(t *testing.T) {
					_, err := internalExecutor.ExecEx(
						ctx,
						"truncate-stmt-stats",
						nil,
						sessiondata.InternalExecutorOverride{
							User: security.NodeUserName(),
						},
						"TRUNCATE system.statement_statistics",
					)
					require.NoError(t, err)
					_, err = internalExecutor.ExecEx(
						ctx,
						"truncate-txn-stats",
						nil,
						sessiondata.InternalExecutorOverride{
							User: security.NodeUserName(),
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
						maxPersistedRowLimit)

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
						getTopSortedFingerprints(t, firstSQLConn, maxPersistedRowLimit)
					err = statsCompactor.DeleteOldestEntries(ctx)
					require.NoError(t, err)

					actualStmtFingerprints, actualTxnFingerprints :=
						getTopSortedFingerprints(t, firstSQLConn, 0 /* limit */)

					require.GreaterOrEqual(t, maxPersistedRowLimit, len(actualStmtFingerprints))
					require.GreaterOrEqual(t, maxPersistedRowLimit, len(actualTxnFingerprints))

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
					require.GreaterOrEqual(t, maxPersistedRowLimit, stmtStatsCnt)
					require.GreaterOrEqual(t, maxPersistedRowLimit, txnStatsCnt)
				})
			}
		})
	}
}

func TestAtMostOneSQLStatsCompactionJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var serverArgs base.TestServerArgs
	var allowRequest chan struct{}

	serverArgs.Knobs.SQLStatsKnobs = &sqlstats.TestingKnobs{
		AOSTClause: "AS OF SYSTEM TIME '-1us'",
	}

	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	allowRequest = make(chan struct{})
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: createStatsRequestFilter(t, allowRequest),
	}

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 3 /* numNodes */, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(conn)
	server := tc.Server(0 /* idx */)

	jobID, err := launchSQLStatsCompactionJob(server)
	require.NoError(t, err)

	// We wait until the job appears in the system.jobs table.
	sqlDB.CheckQueryResultsRetry(
		t,
		fmt.Sprintf(`SELECT count(*) FROM system.jobs where id = %d`, jobID),
		[][]string{{"1"}},
	)

	allowRequest <- struct{}{}

	// Launching a second job should fail here since we are still blocking the
	// the first job's execution through allowRequest. (Since we need to send
	// struct{}{} twice into the channel to fully unblock it.)
	_, err = launchSQLStatsCompactionJob(server)
	expected := persistedsqlstats.ErrConcurrentSQLStatsCompaction.Error()
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected '%s' error, but got %+v", expected, err)
	}

	allowRequest <- struct{}{}
	close(allowRequest)

	// We wait until the first job finishes.
	sqlDB.CheckQueryResultsRetry(
		t,
		fmt.Sprintf(`SELECT count(*) FROM system.jobs where id = %d AND status = 'succeeded'`, jobID),
		[][]string{{"1"}},
	)

	// Launching the job now should succeed.
	jobID, err = launchSQLStatsCompactionJob(server)
	require.NoError(t, err)

	// Wait until the second job to finish for sanity check.
	server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
	sqlDB.CheckQueryResultsRetry(
		t,
		fmt.Sprintf(`SELECT count(*) FROM system.jobs where id = %d AND status = 'succeeded'`, jobID),
		[][]string{{"1"}},
	)
}

func launchSQLStatsCompactionJob(server serverutils.TestServerInterface) (jobspb.JobID, error) {
	return persistedsqlstats.CreateCompactionJob(
		context.Background(), nil /* createdByInfo */, nil, /* txn */
		server.InternalExecutor().(sqlutil.InternalExecutor),
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

func createStatsRequestFilter(
	t *testing.T, allowToProgress chan struct{},
) kvserverbase.ReplicaRequestFilter {
	// Start a test server here, so we can get the descriptor ID for the system
	// table. This allows us to not hardcode the descriptor ID.
	s, sqlConn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer func() {
		s.Stopper().Stop(context.Background())
		err := sqlConn.Close()
		require.NoError(t, err)
	}()
	sqlDB := sqlutils.MakeSQLRunner(sqlConn)

	stmtStatsTableID, txnStatsTableID := getStatsTablesIDs(t, sqlDB)
	return func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if req, ok := ba.GetArg(roachpb.Scan); ok {
			_, tableID, _ := encoding.DecodeUvarintAscending(req.(*roachpb.ScanRequest).Key)
			if descpb.ID(tableID) == stmtStatsTableID || descpb.ID(tableID) == txnStatsTableID {
				<-allowToProgress
				<-allowToProgress
			}
		}
		return nil
	}
}

func getStatsTablesIDs(
	t *testing.T, sqlDB *sqlutils.SQLRunner,
) (stmtStatsTableID, txnStatsTableID descpb.ID) {
	stmt :=
		"select 'system.statement_statistics'::regclass::oid, 'system.transaction_statistics'::regclass::oid"
	sqlDB.QueryRow(t, stmt).Scan(&stmtStatsTableID, &txnStatsTableID)
	return stmtStatsTableID, txnStatsTableID
}
