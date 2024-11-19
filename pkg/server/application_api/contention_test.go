// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStatusAPIContentionEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})

	defer testCluster.Stopper().Stop(ctx)

	s0 := testCluster.Server(0).ApplicationLayer()
	s1 := testCluster.Server(1).ApplicationLayer()
	s2 := testCluster.Server(2).ApplicationLayer()
	server1Conn := sqlutils.MakeSQLRunner(s0.SQLConn(t))
	server2Conn := sqlutils.MakeSQLRunner(s1.SQLConn(t))

	contentionCountBefore := s1.SQLServer().(*sql.Server).Metrics.EngineMetrics.SQLContendedTxns.Count()

	sqlutils.CreateTable(
		t,
		testCluster.ServerConn(0),
		"test",
		"x INT PRIMARY KEY",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	testTableID, err :=
		strconv.Atoi(server1Conn.QueryStr(t, "SELECT 'test.test'::regclass::oid")[0][0])
	require.NoError(t, err)

	server1Conn.Exec(t, "USE test")
	server2Conn.Exec(t, "USE test")
	server2Conn.Exec(t, "SET application_name = 'contentionTest'")

	server1Conn.Exec(t, `
SET TRACING=on;
BEGIN;
UPDATE test SET x = 100 WHERE x = 1;
`)
	server2Conn.Exec(t, `
SET TRACING=on;
BEGIN PRIORITY HIGH;
UPDATE test SET x = 1000 WHERE x = 1;
COMMIT;
SET TRACING=off;
`)
	server1Conn.ExpectErr(
		t,
		"^pq: restart transaction.+",
		`
COMMIT;
SET TRACING=off;
`,
	)

	var resp serverpb.ListContentionEventsResponse
	require.NoError(t,
		srvtestutils.GetStatusJSONProtoWithAdminOption(
			s2,
			"contention_events",
			&resp,
			true /* isAdmin */),
	)

	require.GreaterOrEqualf(t, len(resp.Events.IndexContentionEvents), 1,
		"expecting at least 1 contention event, but found none")

	found := false
	for _, event := range resp.Events.IndexContentionEvents {
		if event.TableID == descpb.ID(testTableID) && event.IndexID == descpb.IndexID(1) {
			found = true
			break
		}
	}

	require.True(t, found,
		"expect to find contention event for table %d, but found %+v", testTableID, resp)

	server1Conn.CheckQueryResults(t, `
  SELECT count(*)
  FROM crdb_internal.statement_statistics
  WHERE
    (statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::FLOAT > 0
    AND app_name = 'contentionTest'
`, [][]string{{"1"}})

	server1Conn.CheckQueryResults(t, `
  SELECT count(*)
  FROM crdb_internal.transaction_statistics
  WHERE
    (statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::FLOAT > 0
    AND app_name = 'contentionTest'
`, [][]string{{"1"}})

	contentionCountNow := s1.SQLServer().(*sql.Server).Metrics.EngineMetrics.SQLContendedTxns.Count()

	require.Greaterf(t, contentionCountNow, contentionCountBefore,
		"expected txn contention count to be more than %d, but it is %d",
		contentionCountBefore, contentionCountNow)
}

func TestTransactionContentionEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn1, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlutils.CreateTable(
		t,
		conn1,
		"test",
		"x INT PRIMARY KEY",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	conn2 := s.SQLConn(t)
	defer func() {
		require.NoError(t, conn2.Close())
	}()

	sqlConn1 := sqlutils.MakeSQLRunner(conn1)
	sqlConn1.Exec(t, "SET CLUSTER SETTING sql.contention.txn_id_cache.max_size = '1GB'")
	sqlConn1.Exec(t, "USE test")
	sqlConn1.Exec(t, "SET application_name='conn1'")

	sqlConn2 := sqlutils.MakeSQLRunner(conn2)
	sqlConn2.Exec(t, "USE test")
	sqlConn2.Exec(t, "SET application_name='conn2'")

	// Start the first transaction.
	sqlConn1.Exec(t, `
	SET TRACING=on;
	BEGIN;
	`)

	txnID1 := sqlConn1.QueryStr(t, `
	SELECT txn_id
	FROM [SHOW TRANSACTIONS]
	WHERE application_name = 'conn1'`)[0][0]

	sqlConn1.Exec(t, "UPDATE test SET x = 100 WHERE x = 1")

	// Start the second transaction with higher priority. This will cause the
	// first transaction to be aborted.
	sqlConn2.Exec(t, `
	SET TRACING=on;
	BEGIN PRIORITY HIGH;
	`)

	txnID2 := sqlConn1.QueryStr(t, `
	SELECT txn_id
	FROM [SHOW TRANSACTIONS]
	WHERE application_name = 'conn2'`)[0][0]

	sqlConn2.Exec(t, `
	UPDATE test SET x = 1000 WHERE x = 1;
	COMMIT;`)

	// Ensure that the first transaction is aborted.
	sqlConn1.ExpectErr(
		t,
		"^pq: restart transaction.+",
		`
		COMMIT;
		SET TRACING=off;`,
	)

	// Sanity check to see the first transaction has been aborted.
	sqlConn1.CheckQueryResults(t, "SELECT * FROM test",
		[][]string{{"1000"}})

	txnIDCache := s.SQLServer().(*sql.Server).GetTxnIDCache()

	// Since contention event store's resolver only retries once in the case of
	// missing txn fingerprint ID for a given txnID, we ensure that the txnIDCache
	// write buffer is properly drained before we go on to test the contention
	// registry.
	testutils.SucceedsSoon(t, func() error {
		txnIDCache.DrainWriteBuffer()

		txnID, err := uuid.FromString(txnID1)
		require.NoError(t, err)

		if _, found := txnIDCache.Lookup(txnID); !found {
			return errors.Newf("expected the txn fingerprint ID for txn %s to be "+
				"stored in txnID cache, but it is not", txnID1)
		}

		txnID, err = uuid.FromString(txnID2)
		require.NoError(t, err)

		if _, found := txnIDCache.Lookup(txnID); !found {
			return errors.Newf("expected the txn fingerprint ID for txn %s to be "+
				"stored in txnID cache, but it is not", txnID2)
		}

		return nil
	})

	testutils.SucceedsWithin(t, func() error {
		err := s.ExecutorConfig().(sql.ExecutorConfig).ContentionRegistry.FlushEventsForTest(ctx)
		require.NoError(t, err)

		notEmpty := sqlConn1.QueryStr(t, `
		SELECT count(*) > 0
		FROM crdb_internal.transaction_contention_events
		WHERE
		  blocking_txn_id = $1::UUID AND
		  waiting_txn_id = $2::UUID AND
		  encode(blocking_txn_fingerprint_id, 'hex') != '0000000000000000' AND
		  encode(waiting_txn_fingerprint_id, 'hex') != '0000000000000000' AND
		  length(contending_key) > 0`, txnID1, txnID2)[0][0]

		if notEmpty != "true" {
			return errors.Newf("expected at least one contention events, but " +
				"none was found")
		}

		return nil
	}, 10*time.Second)

	nonAdminUser := apiconstants.TestingUserNameNoAdmin().Normalized()
	adminUser := apiconstants.TestingUserName().Normalized()

	// N.B. We need both test users to be created before establishing SQL
	//      connections with their usernames. We use
	//      srvtestutils.GetStatusJSONProtoWithAdminOption() to implicitly create those
	//      usernames instead of regular CREATE USER statements, since the helper
	//      srvtestutils.GetStatusJSONProtoWithAdminOption() couldn't handle the case where
	//      those two usernames already exist.
	//      This is the reason why we don't check for returning errors.
	_ = srvtestutils.GetStatusJSONProtoWithAdminOption(
		s,
		"transactioncontentionevents",
		&serverpb.TransactionContentionEventsResponse{},
		true, /* isAdmin */
	)
	_ = srvtestutils.GetStatusJSONProtoWithAdminOption(
		s,
		"transactioncontentionevents",
		&serverpb.TransactionContentionEventsResponse{},
		false, /* isAdmin */
	)

	type testCase struct {
		testName             string
		userName             string
		canViewContendingKey bool
		grantPerm            string
		revokePerm           string
		isAdmin              bool
	}

	tcs := []testCase{
		{
			testName:             "nopermission",
			userName:             nonAdminUser,
			canViewContendingKey: false,
		},
		{
			testName:             "viewactivityredacted",
			userName:             nonAdminUser,
			canViewContendingKey: false,
			grantPerm:            fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", nonAdminUser),
			revokePerm:           fmt.Sprintf("ALTER USER %s NOVIEWACTIVITYREDACTED", nonAdminUser),
		},
		{
			testName:             "viewactivity",
			userName:             nonAdminUser,
			canViewContendingKey: true,
			grantPerm:            fmt.Sprintf("ALTER USER %s VIEWACTIVITY", nonAdminUser),
			revokePerm:           fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", nonAdminUser),
		},
		{
			testName:             "viewactivity_and_viewactivtyredacted",
			userName:             nonAdminUser,
			canViewContendingKey: false,
			grantPerm: fmt.Sprintf(`ALTER USER %s VIEWACTIVITY;
																		 ALTER USER %s VIEWACTIVITYREDACTED;`,
				nonAdminUser, nonAdminUser),
			revokePerm: fmt.Sprintf(`ALTER USER %s NOVIEWACTIVITY;
																		 ALTER USER %s NOVIEWACTIVITYREDACTED;`,
				nonAdminUser, nonAdminUser),
		},
		{
			testName:             "adminuser",
			userName:             adminUser,
			canViewContendingKey: true,
			isAdmin:              true,
		},
	}

	expectationStringHelper := func(canViewContendingKey bool) string {
		if canViewContendingKey {
			return "able to view contending keys"
		}
		return "not able to view contending keys"
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			if tc.grantPerm != "" {
				sqlConn1.Exec(t, tc.grantPerm)
			}
			if tc.revokePerm != "" {
				defer sqlConn1.Exec(t, tc.revokePerm)
			}

			expectationStr := expectationStringHelper(tc.canViewContendingKey)
			t.Run("sql_cli", func(t *testing.T) {
				// Check we have proper permission control in SQL CLI. We use internal
				// executor here since we can easily override the username without opening
				// new SQL sessions.
				row, err := s.InternalExecutor().(*sql.InternalExecutor).QueryRowEx(
					ctx,
					"test-contending-key-redaction",
					nil, /* txn */
					sessiondata.InternalExecutorOverride{
						User: username.MakeSQLUsernameFromPreNormalizedString(tc.userName),
					},
					`
				SELECT count(*)
				FROM crdb_internal.transaction_contention_events
				WHERE length(contending_key) > 0`,
				)
				if tc.testName == "nopermission" {
					require.Contains(t, err.Error(), "does not have VIEWACTIVITY")
				} else {
					require.NoError(t, err)
					visibleContendingKeysCount := tree.MustBeDInt(row[0])

					require.Equal(t, tc.canViewContendingKey, visibleContendingKeysCount > 0,
						"expected to %s, but %d keys have been retrieved",
						expectationStr, visibleContendingKeysCount)
				}
			})

			t.Run("http", func(t *testing.T) {
				// Check we have proper permission control in RPC/HTTP endpoint.
				resp := serverpb.TransactionContentionEventsResponse{}
				err := srvtestutils.GetStatusJSONProtoWithAdminOption(
					s,
					"transactioncontentionevents",
					&resp,
					tc.isAdmin,
				)

				if tc.testName == "nopermission" {
					require.Contains(t, err.Error(), "status: 403")
				} else {
					require.NoError(t, err)
				}

				for _, event := range resp.Events {
					require.NotEqual(t, event.WaitingStmtFingerprintID, 0)
					require.NotEqual(t, event.WaitingStmtID.String(), clusterunique.ID{}.String())

					require.Equal(t, tc.canViewContendingKey, len(event.BlockingEvent.Key) > 0,
						"expected to %s, but the contending key has length of %d",
						expectationStr,
						len(event.BlockingEvent.Key),
					)
				}
			})

		})
	}
}
