// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTransactionDiagnosticsReportAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		name            string
		createRequests  []*serverpb.CreateTransactionDiagnosticsReportRequest
		cancelRequests  []int64
		expectedReports int
		validateFunc    func(t *testing.T, reports []serverpb.TransactionDiagnosticsReport, createRequests []*serverpb.CreateTransactionDiagnosticsReportRequest)
	}

	testCases := []testCase{
		{
			name: "basic creation with minimal fields",
			createRequests: []*serverpb.CreateTransactionDiagnosticsReportRequest{
				{
					TransactionFingerprintId: sqlstatsutil.EncodeUint64ToBytes(123),
				},
			},
			cancelRequests:  nil,
			expectedReports: 1,
			validateFunc: func(t *testing.T, reports []serverpb.TransactionDiagnosticsReport, createRequests []*serverpb.CreateTransactionDiagnosticsReportRequest) {
				require.Len(t, reports, 1)
				require.Equal(t, reports[0].TransactionFingerprintId, createRequests[0].TransactionFingerprintId)
				require.False(t, reports[0].Completed)
				require.Greater(t, reports[0].Id, int64(0))
			},
		},
		{
			name: "creation with all fields populated",
			createRequests: []*serverpb.CreateTransactionDiagnosticsReportRequest{
				{
					TransactionFingerprintId: sqlstatsutil.EncodeUint64ToBytes(456),
					StatementFingerprintIds: [][]byte{
						sqlstatsutil.EncodeUint64ToBytes(789),
						sqlstatsutil.EncodeUint64ToBytes(101112),
					},
					MinExecutionLatency: time.Second * 5,
					ExpiresAt:           time.Hour * 24,
					SamplingProbability: 0.5,
					Redacted:            true,
				},
			},
			cancelRequests:  nil,
			expectedReports: 1,
			validateFunc: func(t *testing.T, reports []serverpb.TransactionDiagnosticsReport, createRequests []*serverpb.CreateTransactionDiagnosticsReportRequest) {
				require.Len(t, reports, 1)
				req := createRequests[0]
				report := reports[0]

				require.Equal(t, report.TransactionFingerprintId, req.TransactionFingerprintId)
				require.Equal(t, report.StatementFingerprintIds, req.StatementFingerprintIds)
				require.Equal(t, report.MinExecutionLatency, req.MinExecutionLatency)
				require.False(t, report.Completed)
				require.Greater(t, report.Id, int64(0))
				require.True(t, report.RequestedAt.After(time.Time{}))
				require.True(t, report.ExpiresAt.After(time.Time{}))
			},
		},
		{
			name: "multiple reports creation",
			createRequests: []*serverpb.CreateTransactionDiagnosticsReportRequest{
				{
					TransactionFingerprintId: sqlstatsutil.EncodeUint64ToBytes(111),
				},
				{
					TransactionFingerprintId: sqlstatsutil.EncodeUint64ToBytes(222),
					SamplingProbability:      0.8,
					MinExecutionLatency:      time.Millisecond * 100,
				},
			},
			cancelRequests:  nil,
			expectedReports: 2,
			validateFunc: func(t *testing.T, reports []serverpb.TransactionDiagnosticsReport, createRequests []*serverpb.CreateTransactionDiagnosticsReportRequest) {
				require.Len(t, reports, 2)

				txnIDs := make(map[string]bool)
				for _, req := range createRequests {
					txnIDs[string(req.TransactionFingerprintId)] = true
				}

				for _, report := range reports {
					require.True(t, txnIDs[string(report.TransactionFingerprintId)], "unexpected transaction fingerprint ID")
					require.Greater(t, report.Id, int64(0))
					require.False(t, report.Completed)
				}
			},
		},
		{
			name: "cancel one of two reports",
			createRequests: []*serverpb.CreateTransactionDiagnosticsReportRequest{
				{
					TransactionFingerprintId: sqlstatsutil.EncodeUint64ToBytes(333),
				},
				{
					TransactionFingerprintId: sqlstatsutil.EncodeUint64ToBytes(444),
				},
			},
			cancelRequests:  []int64{0},
			expectedReports: 1,
			validateFunc: func(t *testing.T, reports []serverpb.TransactionDiagnosticsReport, createRequests []*serverpb.CreateTransactionDiagnosticsReportRequest) {
				require.Len(t, reports, 1)
				require.Equal(t, reports[0].TransactionFingerprintId, createRequests[1].TransactionFingerprintId)
				require.False(t, reports[0].Completed)
			},
		},
		{
			name: "completed request with transaction fingerprint",
			createRequests: []*serverpb.CreateTransactionDiagnosticsReportRequest{
				{
					TransactionFingerprintId: sqlstatsutil.EncodeUint64ToBytes(555),
					StatementFingerprintIds:  [][]byte{sqlstatsutil.EncodeUint64ToBytes(666)},
				},
			},
			cancelRequests:  nil,
			expectedReports: 1,
			validateFunc: func(t *testing.T, reports []serverpb.TransactionDiagnosticsReport, createRequests []*serverpb.CreateTransactionDiagnosticsReportRequest) {
				require.Len(t, reports, 1)
				require.Equal(t, reports[0].TransactionFingerprintId, createRequests[0].TransactionFingerprintId)
				require.True(t, reports[0].Completed)
				require.Greater(t, reports[0].TransactionDiagnosticsId, int64(0))
				require.Equal(t, "SELECT 1; SELECT 2", reports[0].TransactionFingerprint)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := serverutils.StartServerOnly(t, base.TestServerArgs{})
			defer s.Stopper().Stop(context.Background())

			ts := s.ApplicationLayer()

			var createdIDs []int64

			for _, req := range tc.createRequests {
				var resp serverpb.CreateTransactionDiagnosticsReportResponse
				err := srvtestutils.PostStatusJSONProto(ts, "txndiagreports", req, &resp)
				require.NoError(t, err)
				require.Greater(t, resp.Report.Id, int64(0))
				createdIDs = append(createdIDs, resp.Report.Id)
			}

			for _, cancelIndex := range tc.cancelRequests {
				require.Less(t, int(cancelIndex), len(createdIDs), "cancel index out of range")
				cancelReq := &serverpb.CancelTransactionDiagnosticsReportRequest{
					RequestID: createdIDs[cancelIndex],
				}
				var cancelResp serverpb.CancelTransactionDiagnosticsReportResponse
				err := srvtestutils.PostStatusJSONProto(ts, "txndiagreports/cancel", cancelReq, &cancelResp)
				require.NoError(t, err)
				require.True(t, cancelResp.Canceled)
			}

			// Special handling for "completed request with transaction fingerprint" test case
			if tc.name == "completed request with transaction fingerprint" {
				require.Len(t, createdIDs, 1, "expected exactly one request for this test case")
				requestID := createdIDs[0]

				// First insert a diagnostic record
				stmtFingerprintArray := tree.NewDArray(types.BytesArray)
				stmtFingerprintArray.Array = append(stmtFingerprintArray.Array,
					tree.NewDBytes(tree.DBytes(sqlstatsutil.EncodeStmtFingerprintIDToString(appstatspb.StmtFingerprintID(666)))))

				_, err := ts.InternalDB().(isql.DB).Executor().ExecEx(
					context.Background(), "test-insert-txn-diagnostic", nil,
					sessiondata.NodeUserSessionDataOverride,
					`INSERT INTO system.transaction_diagnostics 
					(transaction_fingerprint_id, statement_fingerprint_ids, transaction_fingerprint, collected_at, bundle_chunks)
					VALUES ($1, $2, $3, now(), ARRAY[1])
					RETURNING id`,
					[]byte(sqlstatsutil.EncodeTxnFingerprintIDToString(appstatspb.TransactionFingerprintID(555))),
					stmtFingerprintArray,
					"SELECT 1; SELECT 2",
				)
				require.NoError(t, err)

				// Then update the request to mark it as completed with the diagnostic ID
				_, err = ts.InternalDB().(isql.DB).Executor().ExecEx(
					context.Background(), "test-mark-request-completed", nil,
					sessiondata.NodeUserSessionDataOverride,
					`UPDATE system.transaction_diagnostics_requests 
					SET completed = true, transaction_diagnostics_id = (
						SELECT id FROM system.transaction_diagnostics 
						WHERE transaction_fingerprint_id = $1
						ORDER BY id DESC LIMIT 1
					)
					WHERE id = $2`,
					[]byte(sqlstatsutil.EncodeTxnFingerprintIDToString(appstatspb.TransactionFingerprintID(555))),
					requestID,
				)
				require.NoError(t, err)
			}

			var respGet serverpb.TransactionDiagnosticsReportsResponse
			err := srvtestutils.GetStatusJSONProto(ts, "txndiagreports", &respGet)
			require.NoError(t, err)

			require.Len(t, respGet.Reports, tc.expectedReports)

			if tc.validateFunc != nil {
				tc.validateFunc(t, respGet.Reports, tc.createRequests)
			}
		})
	}
}
