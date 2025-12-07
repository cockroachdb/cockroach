// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

func TestAdminAPITransactionDiagnosticsBundle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name              string
		grantRole         string
		isAdmin           bool
		expectedStatus    int
		shouldValidateZip bool
	}{
		{
			name:              "admin_role",
			isAdmin:           true,
			expectedStatus:    http.StatusOK,
			shouldValidateZip: true,
		},
		{
			name:              "no_permissions",
			isAdmin:           false,
			expectedStatus:    http.StatusForbidden,
			shouldValidateZip: false,
		},
		{
			name:              "viewactivityredacted_role",
			grantRole:         "VIEWACTIVITYREDACTED",
			isAdmin:           false,
			expectedStatus:    http.StatusForbidden,
			shouldValidateZip: false,
		},
		{
			name:              "viewactivity_role",
			grantRole:         "VIEWACTIVITY",
			isAdmin:           false,
			expectedStatus:    http.StatusOK,
			shouldValidateZip: true,
		},
	}

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()
	conn := sqlutils.MakeSQLRunner(ts.SQLConn(t))

	// Set up the test table
	conn.Exec(t, "CREATE TABLE test (id INT)")

	// Insert into transaction_diagnostics first
	txnFingerprintID, err := sqlstatsutil.DecodeStringToTxnFingerprintID("5a808c2f3780b2c8")
	require.NoError(t, err)
	stmt1FingerprintID, err := sqlstatsutil.DecodeStringToStmtFingerprintID("2ca050d725bfd5f0")
	require.NoError(t, err)
	stmt2FingerprintID, err := sqlstatsutil.DecodeStringToStmtFingerprintID("fece62580b006715")
	require.NoError(t, err)

	// Create a transaction diagnostic request that should match our data
	req := &serverpb.CreateTransactionDiagnosticsReportRequest{
		TransactionFingerprintId: sqlstatsutil.EncodeUint64ToBytes(uint64(txnFingerprintID)),
		StatementFingerprintIds: [][]byte{
			sqlstatsutil.EncodeUint64ToBytes(uint64(stmt1FingerprintID)),
			sqlstatsutil.EncodeUint64ToBytes(uint64(stmt2FingerprintID)),
		},
	}

	var resp serverpb.CreateTransactionDiagnosticsReportResponse
	err = srvtestutils.PostStatusJSONProto(ts, "txndiagreports", req, &resp)
	require.NoError(t, err)
	require.NotZero(t, resp.Report.Id)

	txn := conn.Begin(t)
	_, err = txn.Exec("INSERT INTO test VALUES (123)")
	require.NoError(t, err)
	_, err = txn.Query("SELECT * FROM test")
	require.NoError(t, err)
	err = txn.Commit()
	require.NoError(t, err)

	// Wait a bit for the diagnostic to be processed
	require.Eventually(t, func() bool {
		rows := conn.Query(t, `
			SELECT id FROM system.transaction_diagnostics
			WHERE bundle_chunks IS NOT NULL
			ORDER BY collected_at DESC
			LIMIT 1
		`)
		defer rows.Close()
		return rows.Next()
	}, 5*time.Second, 100*time.Millisecond, "Expected transaction diagnostic bundle to be created")

	// Get the bundle ID
	bundleRows := conn.Query(t, `
		SELECT id FROM system.transaction_diagnostics
		WHERE bundle_chunks IS NOT NULL
		ORDER BY collected_at DESC
		LIMIT 1
	`)
	require.True(t, bundleRows.Next(), "Expected to find transaction diagnostic bundle")
	var bundleID int64
	err = bundleRows.Scan(&bundleID)
	require.NoError(t, err)
	bundleRows.Close()

	expectedStmtCount := 2

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Grant role if specified
			if tc.grantRole != "" {
				conn.Exec(t, fmt.Sprintf("GRANT SYSTEM %s TO %s",
					tc.grantRole, apiconstants.TestingUserNameNoAdmin().Normalized()))
				defer conn.Exec(t, fmt.Sprintf("REVOKE SYSTEM %s FROM %s",
					tc.grantRole, apiconstants.TestingUserNameNoAdmin().Normalized()))
			}

			// Download bundle
			resp, body, err := downloadTransactionBundle(ts, bundleID, tc.isAdmin)
			require.NoError(t, err)
			defer resp.Body.Close()

			// Log the response for debugging
			if resp.StatusCode != tc.expectedStatus {
				t.Logf("Expected status %d but got %d with body %s", tc.expectedStatus, resp.StatusCode, body)
			}
			require.Equal(t, tc.expectedStatus, resp.StatusCode)

			// Validate ZIP contents if successful
			if tc.shouldValidateZip {
				validateTransactionBundle(t, body, expectedStmtCount)
			}
		})
	}
}

// downloadTransactionBundle downloads a transaction diagnostic bundle with the specified auth level
func downloadTransactionBundle(
	ts serverutils.ApplicationLayerInterface, bundleID int64, isAdmin bool,
) (*http.Response, []byte, error) {
	client, err := ts.GetAuthenticatedHTTPClient(isAdmin, serverutils.SingleTenantSession)
	if err != nil {
		return nil, nil, err
	}

	url := ts.AdminURL().WithPath("/_admin/v1/txnbundle/" + strconv.FormatInt(bundleID, 10)).String()
	resp, err := client.Get(url)
	if err != nil {
		return nil, nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, err
	}

	return resp, body, nil
}

// validateTransactionBundle unzips and validates the contents of a transaction diagnostic bundle
func validateTransactionBundle(t *testing.T, data []byte, expectedStmtCount int) {
	// Open the ZIP archive
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	// Track what we find
	var foundTxnBundle bool
	var foundStmtBundles int
	var foundCommitBundle bool

	for _, file := range reader.File {
		if strings.HasPrefix(file.Name, "transaction-") && strings.HasSuffix(file.Name, ".zip") {
			foundTxnBundle = true

			// Validate transaction bundle contents
			rc, err := file.Open()
			require.NoError(t, err)
			txnData, err := io.ReadAll(rc)
			require.NoError(t, err)
			rc.Close()

			// Verify it's a valid ZIP
			txnReader, err := zip.NewReader(bytes.NewReader(txnData), int64(len(txnData)))
			require.NoError(t, err)
			require.Greater(t, len(txnReader.File), 0, "Transaction bundle should contain files")

		} else if strings.HasSuffix(file.Name, "-INSERT.zip") || strings.HasSuffix(file.Name, "-SELECT.zip") {
			foundStmtBundles++

			// Validate statement bundle contents
			rc, err := file.Open()
			require.NoError(t, err)
			stmtData, err := io.ReadAll(rc)
			require.NoError(t, err)
			rc.Close()

			// Verify it's a valid ZIP
			stmtReader, err := zip.NewReader(bytes.NewReader(stmtData), int64(len(stmtData)))
			require.NoError(t, err)
			require.Greater(t, len(stmtReader.File), 0, "Statement bundle should contain files")
		} else if strings.HasSuffix(file.Name, "-COMMIT.zip") {
			foundCommitBundle = true

			// Validate commit bundle contents
			rc, err := file.Open()
			require.NoError(t, err)
			txnData, err := io.ReadAll(rc)
			require.NoError(t, err)
			rc.Close()

			// Verify it's a valid ZIP
			txnReader, err := zip.NewReader(bytes.NewReader(txnData), int64(len(txnData)))
			require.NoError(t, err)
			require.Greater(t, len(txnReader.File), 0, "Commit bundle should contain files")
		}
	}

	// Verify we found the expected components
	require.True(t, foundTxnBundle, "Should find transaction bundle in archive")
	require.True(t, foundCommitBundle, "Should find commit bundle in archive")
	require.Equal(t, foundStmtBundles, expectedStmtCount, "Should find at least one statement bundle")
}
