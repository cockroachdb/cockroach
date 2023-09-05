// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/stretchr/testify/require"
)

// TestStatusServer_StatementExecutionInsights tests that StatementExecutionInsights endpoint
// returns list of statement insights that also include contention information
// for contended queries.
func TestStatusServer_StatementExecutionInsights(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // speeds up test
		ServerArgs: base.TestServerArgs{
			Settings: settings,
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Enable contention detection by setting a latencyThreshold > 0.
	insights.LatencyThreshold.Override(ctx, &settings.SV, 30*time.Millisecond)
	// Enable contention events collection by setting resolution duration > 0.
	contention.TxnIDResolutionInterval.Override(ctx, &settings.SV, 20*time.Millisecond)

	// Generate insight with contention event
	sqlConn := tc.ServerConn(0)
	tableName := "t1"
	_, err := sqlConn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (k INT, i INT, f FLOAT, s STRING)", tableName))
	require.NoError(t, err)
	// Open transaction to insert values into table
	tx, err := sqlConn.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Insert statement within transaction will block access to table.
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 2, 3.0, '4')", tableName))
	require.NoError(t, err)

	// Make SELECT query to wait for some period of time to simulate contention.
	go func() {
		<-time.After(100 * time.Millisecond)
		err = tx.Commit()
		require.NoError(t, err)
	}()

	// Query the table while above transaction is still open
	_, err = sqlConn.ExecContext(ctx, fmt.Sprintf("SELECT * FROM %s", tableName))
	require.NoError(t, err)

	srv := tc.ApplicationLayer(0)
	sc := srv.StatusServer().(serverpb.StatusServer)

	var resp *serverpb.StatementExecutionInsightsResponse
	testutils.SucceedsWithin(t, func() error {
		resp, err = sc.StatementExecutionInsights(ctx, &serverpb.StatementExecutionInsightsRequest{})
		require.NoError(t, err)
		if len(resp.StatementInsights) == 0 {
			return fmt.Errorf("waiting for response with statement insights")
		}
		if resp.StatementInsights[0].ContentionInfo == nil {
			return fmt.Errorf("waiting for contention info")
		}
		return nil
	}, 5*time.Second)

	require.GreaterOrEqual(t, len(resp.StatementInsights), 1)

	stmtInsight := resp.StatementInsights[0]
	require.Greater(t, stmtInsight.Contention.Nanoseconds(), int64(0))
	require.NotNil(t, stmtInsight.ContentionInfo)
	require.Equal(t, stmtInsight.ContentionInfo.TableName, tableName)

	t.Run("request_with_filter", func(t *testing.T) {
		log.Info(context.Background(), "222222 request_with_filter")
		// Test that StatementExecutionInsights endpoint returns results that satisfy
		// provided filters in request payload.
		testCases := []struct {
			name string
			// tReq is a request that should return response with statement insight
			tReq serverpb.StatementExecutionInsightsRequest
			// fReq is a request that doesn't return any statement results
			fReq serverpb.StatementExecutionInsightsRequest
		}{
			{
				name: "statement_id",
				tReq: serverpb.StatementExecutionInsightsRequest{StatementID: &stmtInsight.ID},
				fReq: serverpb.StatementExecutionInsightsRequest{StatementID: &clusterunique.ID{Uint128: uint128.FromInts(1, 1)}},
			},
			{
				name: "statement_fingerprint_id",
				tReq: serverpb.StatementExecutionInsightsRequest{StmtFingerprintID: stmtInsight.FingerprintID},
				fReq: serverpb.StatementExecutionInsightsRequest{StmtFingerprintID: appstatspb.StmtFingerprintID(123)},
			},
			// TODO (koorosh): add test case with req by time range.
			// TODO (koorosh): add test where start time is specified but end time isn't.
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				resp, err = sc.StatementExecutionInsights(ctx, &testCase.tReq)
				require.NoError(t, err)
				require.Equal(t, 1, len(resp.StatementInsights))

				resp, err = sc.StatementExecutionInsights(ctx, &testCase.fReq)
				require.NoError(t, err)
				require.Equal(t, 0, len(resp.StatementInsights))
			})
		}
	})

	t.Run("permissions", func(t *testing.T) {
		roles := []string{"VIEWACTIVITY", "VIEWACTIVITYREDACTED"}
		req := serverpb.StatementExecutionInsightsRequest{}
		var resp serverpb.StatementExecutionInsightsResponse

		err = srvtestutils.PostStatusJSONProtoWithAdminOption(srv, "insights/statements", &req, &resp, false)
		require.True(t, testutils.IsError(err, "status: 403"))
		require.Nil(t, resp.StatementInsights)

		for _, role := range roles {
			t.Run(role, func(t *testing.T) {
				// Clean up all roles that allow users to request insights to
				// ensure that only specified permission is assigned to user
				// below.
				for _, r := range roles {
					_, err = sqlConn.Exec(fmt.Sprintf("ALTER USER %s NO%s", apiconstants.TestingUserNameNoAdmin().Normalized(), r))
					require.NoError(t, err)
				}

				// Assign user's role
				_, err = sqlConn.Exec(fmt.Sprintf("ALTER USER %s %s", apiconstants.TestingUserNameNoAdmin().Normalized(), role))
				require.NoError(t, err)

				req := serverpb.StatementExecutionInsightsRequest{}
				var resp serverpb.StatementExecutionInsightsResponse
				err = srvtestutils.PostStatusJSONProtoWithAdminOption(srv, "insights/statements", &req, &resp, false)
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(resp.StatementInsights), 1)
			})
		}
	})
}
