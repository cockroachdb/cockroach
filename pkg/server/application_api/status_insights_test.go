// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package application_api_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
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

	srv := tc.ApplicationLayer(0)
	sqlConn := tc.ServerConn(0)

	_, err := sqlConn.Exec("SET CLUSTER SETTING sql.insights.latency_threshold = '30ms'")
	require.NoError(t, err)
	_, err = sqlConn.Exec("SET CLUSTER SETTING sql.contention.event_store.resolution_interval = '10ms'")
	require.NoError(t, err)

	sessionID := clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	origTxnInsight := &insights.Transaction{
		ID:            uuid.FastMakeV4(),
		FingerprintID: 222,
		Contention:    pointerOf(5 * time.Second),
	}
	startTime := time.Date(2000, 1, 1, 9, 30, 0, 0, time.UTC)
	endTime := time.Date(2000, 1, 1, 10, 0, 0, 0, time.UTC)
	origStmtInsight := &insights.Statement{
		ID:            clusterunique.IDFromBytes([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		FingerprintID: appstatspb.StmtFingerprintID(111),
		Contention:    pointerOf(5 * time.Second),
		StartTime:     startTime,
		EndTime:       endTime,
		ErrorMsg:      redact.RedactableString("err message ‹redacted text›"),
	}
	writer := srv.SQLServer().(*sql.Server).GetInsightsWriter(false)
	writer.ObserveStatement(sessionID, origStmtInsight)
	writer.ObserveTransaction(sessionID, origTxnInsight)

	key := roachpb.Key([]byte{254, 146, 240, 137, 253, 12, 138, 126, 78, 141, 64, 0, 1, 136}) // Pretty key: /Tenant/10/Table/104/1/903673551083536385/0
	srv.SQLServer().(*sql.Server).GetExecutorConfig().ContentionRegistry.AddContentionEvent(contentionpb.ExtendedContentionEvent{
		BlockingEvent: kvpb.ContentionEvent{
			Key: key,
			TxnMeta: enginepb.TxnMeta{
				Key: key,
				ID:  uuid.FastMakeV4(),
			},
			Duration: 1 * time.Minute,
		},
		BlockingTxnFingerprintID: 9001,
		WaitingTxnID:             origTxnInsight.ID,
		WaitingTxnFingerprintID:  9002,
		WaitingStmtID:            origStmtInsight.ID,
		WaitingStmtFingerprintID: 9004,
	})

	sc := srv.StatusServer().(serverpb.StatusServer)

	var stmtInsight *serverpb.StatementExecutionInsightsResponse_Statement
	testutils.SucceedsWithin(t, func() error {
		resp, err := sc.StatementExecutionInsights(ctx, &serverpb.StatementExecutionInsightsRequest{})
		require.NoError(t, err)
		if len(resp.Statements) == 0 {
			return fmt.Errorf("waiting for response with statement insights")
		}
		stmtInsight = resp.Statements[0] // It must contain single statement insight.
		if len(stmtInsight.ContentionEvents) == 0 {
			return fmt.Errorf("waiting for insight with contention info")
		}
		return nil
	}, 5*time.Second)
	require.Equal(t, origStmtInsight.ID, *stmtInsight.ID)

	t.Run("request_with_filter", func(t *testing.T) {
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
				tReq: serverpb.StatementExecutionInsightsRequest{StatementID: stmtInsight.ID},
				fReq: serverpb.StatementExecutionInsightsRequest{StatementID: &clusterunique.ID{Uint128: uint128.FromInts(1, 1)}},
			},
			{
				name: "statement_fingerprint_id",
				tReq: serverpb.StatementExecutionInsightsRequest{StmtFingerprintID: stmtInsight.FingerprintID},
				fReq: serverpb.StatementExecutionInsightsRequest{StmtFingerprintID: appstatspb.StmtFingerprintID(123)},
			},
			{
				name: "start_end_time",
				tReq: serverpb.StatementExecutionInsightsRequest{
					StartTime: pointerOf(startTime.Add(1 * time.Minute)),
					EndTime:   pointerOf(endTime.Add(-1 * time.Minute)),
				},
				fReq: serverpb.StatementExecutionInsightsRequest{
					StartTime: pointerOf(startTime.Add(-10 * time.Minute)),
					EndTime:   pointerOf(startTime.Add(-1 * time.Minute)),
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				resp, err := sc.StatementExecutionInsights(ctx, &testCase.tReq)
				require.NoError(t, err)
				require.Equal(t, 1, len(resp.Statements))

				resp, err = sc.StatementExecutionInsights(ctx, &testCase.fReq)
				require.NoError(t, err)
				require.Equal(t, 0, len(resp.Statements))
			})
		}
	})

	t.Run("permissions", func(t *testing.T) {
		roles := []string{"VIEWACTIVITY", "VIEWACTIVITYREDACTED"}
		req := serverpb.StatementExecutionInsightsRequest{}
		var resp serverpb.StatementExecutionInsightsResponse

		err := srvtestutils.PostStatusJSONProtoWithAdminOption(srv, "insights/statements", &req, &resp, false)
		require.True(t, testutils.IsError(err, "status: 403"))
		require.Nil(t, resp.Statements)

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
				require.GreaterOrEqual(t, len(resp.Statements), 1)

				if role == "VIEWACTIVITYREDACTED" {
					require.Equal(t, "err message ‹×›", resp.Statements[0].LastErrorMsg)
					require.Nil(t, resp.Statements[0].ContentionEvents[0].Key)
					require.Nil(t, resp.Statements[0].ContentionEvents[0].PrettyKey)
				}
			})
		}
	})
}

func pointerOf[T any](val T) *T {
	return &val
}
