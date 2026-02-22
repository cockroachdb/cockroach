// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stmtdiagnostics

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestTxnRegistry_ShouldStartTxnDiagnostic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseRequest := TxnRequest{
		txnFingerprintId:   1111,
		stmtFingerprintIds: []uint64{1111, 2222, 3333},
		redacted:           false,
		username:           "",
	}

	testCases := []struct {
		name                string
		expiresAt           time.Time
		minExecutionLatency time.Duration
		samplingProbability float64
		queryFingerprintId  uint64
		expectedShouldStart bool
	}{
		{
			name:                "request_found",
			expiresAt:           time.Time{},
			queryFingerprintId:  1111,
			expectedShouldStart: true,
		},
		{
			name:                "request_not_found",
			expiresAt:           time.Time{},
			queryFingerprintId:  2222, // Different fingerprint
			expectedShouldStart: false,
		},
		{
			name:                "request_expired",
			expiresAt:           timeutil.Now().Add(-time.Hour),
			queryFingerprintId:  1111,
			expectedShouldStart: false,
		},
		{
			name:                "request_conditional",
			expiresAt:           time.Time{},
			minExecutionLatency: time.Millisecond,
			samplingProbability: 1.0,
			queryFingerprintId:  1111,
			expectedShouldStart: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			request := baseRequest
			request.expiresAt = tc.expiresAt
			request.minExecutionLatency = tc.minExecutionLatency
			request.samplingProbability = tc.samplingProbability

			registry := NewTxnRegistry(noopDb{}, nil, nil, timeutil.NewManualTime(timeutil.Now()))
			requestId := RequestID(1)
			registry.mu.requests[requestId] = request

			matches := registry.ShouldStartTxnDiagnostic(context.Background(), tc.queryFingerprintId)

			if tc.expectedShouldStart {
				require.Len(t, matches, 1)
				require.Equal(t, requestId, matches[0].ID)
				require.Equal(t, request, matches[0].Request)
				// Request should remain in the requests map (no exclusive ownership).
				requestInMap, ok := registry.mu.requests[requestId]
				require.True(t, ok)
				require.Equal(t, request, requestInMap)
			} else {
				require.Empty(t, matches)
			}
		})
	}

	t.Run("request_conditional_shouldnt_collect", func(t *testing.T) {
		registry := NewTxnRegistry(noopDb{}, nil, nil, timeutil.NewManualTime(timeutil.Now()))
		requestId := RequestID(1)
		expectedRequest := TxnRequest{
			txnFingerprintId:    1111,
			stmtFingerprintIds:  []uint64{1111, 2222, 3333},
			minExecutionLatency: 1,
			samplingProbability: .01,
		}
		registry.mu.requests[requestId] = expectedRequest
		testutils.SucceedsSoon(t, func() error {
			matches := registry.ShouldStartTxnDiagnostic(context.Background(), 1111)
			if len(matches) > 0 {
				require.Contains(t, registry.mu.requests, matches[0].ID)
				return errors.New("waiting till we don't find")
			}

			require.Contains(t, registry.mu.requests, requestId)
			return nil
		})
	})

	t.Run("multiple_matching_requests", func(t *testing.T) {
		registry := NewTxnRegistry(noopDb{}, nil, nil, timeutil.NewManualTime(timeutil.Now()))
		req1 := TxnRequest{
			txnFingerprintId:   1111,
			stmtFingerprintIds: []uint64{1111, 2222, 3333},
		}
		req2 := TxnRequest{
			txnFingerprintId:   2222,
			stmtFingerprintIds: []uint64{1111, 4444, 5555},
		}
		registry.mu.requests[RequestID(1)] = req1
		registry.mu.requests[RequestID(2)] = req2

		matches := registry.ShouldStartTxnDiagnostic(context.Background(), 1111)
		require.Len(t, matches, 2)

		// Both requests should still be in the map.
		require.Contains(t, registry.mu.requests, RequestID(1))
		require.Contains(t, registry.mu.requests, RequestID(2))
	})

	t.Run("expired_filtered_non_expired_returned", func(t *testing.T) {
		now := timeutil.Now()
		registry := NewTxnRegistry(noopDb{}, nil, nil, timeutil.NewManualTime(now))
		expiredReq := TxnRequest{
			txnFingerprintId:   1111,
			stmtFingerprintIds: []uint64{1111, 2222},
			expiresAt:          now.Add(-time.Hour),
		}
		validReq := TxnRequest{
			txnFingerprintId:   2222,
			stmtFingerprintIds: []uint64{1111, 3333},
		}
		registry.mu.requests[RequestID(1)] = expiredReq
		registry.mu.requests[RequestID(2)] = validReq

		matches := registry.ShouldStartTxnDiagnostic(context.Background(), 1111)
		require.Len(t, matches, 1)
		require.Equal(t, RequestID(2), matches[0].ID)
		require.Equal(t, validReq, matches[0].Request)

		// Expired request should have been removed.
		require.NotContains(t, registry.mu.requests, RequestID(1))
		// Valid request should still be present.
		require.Contains(t, registry.mu.requests, RequestID(2))
	})
}

func TestTxnRegistry_InsertTxnRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	now := timeutil.Unix(0, 0)
	db := s.InternalDB().(isql.DB)
	registry := NewTxnRegistry(db, s.ClusterSettings(), nil, timeutil.NewManualTime(now))
	for i, tc := range []struct {
		name                string
		samplingProbability float64
		minExecutionLatency time.Duration
		expiresAfter        time.Duration
		redacted            bool
		expectedError       string
	}{
		{
			name:                "valid",
			samplingProbability: 0.5,
			minExecutionLatency: time.Millisecond * 100,
			expiresAfter:        time.Hour,
			redacted:            false,
			expectedError:       "",
		}, {
			name:                "valid redacted",
			samplingProbability: 0.5,
			minExecutionLatency: time.Millisecond * 100,
			expiresAfter:        time.Hour,
			redacted:            true,
			expectedError:       "",
		}, {
			name:                "valid 1.0 sampling",
			samplingProbability: 1,
			minExecutionLatency: time.Millisecond * 100,
			expiresAfter:        0,
			redacted:            false,
			expectedError:       "",
		}, {
			name:                "valid no expiration",
			samplingProbability: 0.5,
			minExecutionLatency: time.Millisecond * 100,
			expiresAfter:        0,
			redacted:            false,
			expectedError:       "",
		}, {
			name:                "valid not conditional",
			samplingProbability: 0,
			minExecutionLatency: 0,
			expiresAfter:        0,
			redacted:            false,
			expectedError:       "",
		}, {
			name:                "invalid sampling probability",
			samplingProbability: 1.5,
			minExecutionLatency: time.Millisecond * 100,
			expiresAfter:        time.Hour,
			redacted:            false,
			expectedError:       "expected sampling probability in range [0.0, 1.0]",
		}, {
			name:                "sampling without latency",
			samplingProbability: 0.5,
			minExecutionLatency: 0,
			expiresAfter:        time.Hour,
			redacted:            false,
			expectedError:       "got non-zero sampling probability",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := registry.InsertTxnRequest(
				ctx,
				uint64(i),
				[]uint64{1111, 2222, 3333},
				"testuser",
				tc.samplingProbability,
				tc.minExecutionLatency,
				tc.expiresAfter,
				tc.redacted,
			)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			} else {
				var expectedExpiresAt time.Time
				if tc.expiresAfter != 0 {
					expectedExpiresAt = now.Add(tc.expiresAfter)
				}
				expectedRequest := TxnRequest{
					txnFingerprintId:    uint64(i),
					stmtFingerprintIds:  []uint64{1111, 2222, 3333},
					redacted:            tc.redacted,
					username:            "testuser",
					minExecutionLatency: tc.minExecutionLatency,
					samplingProbability: tc.samplingProbability,
					expiresAt:           expectedExpiresAt,
				}
				require.NoError(t, err)
				id, req, ok := registry.GetRequestForFingerprint(uint64(i))
				require.True(t, ok)
				require.Equal(t, expectedRequest, req)
				checkDatabaseForRequest(t, id, expectedRequest, sqlutils.MakeSQLRunner(srv.SQLConn(t)))
			}
		})
	}
}

func TestTxnRegistry_InsertTxnDiagnostic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	// Create a statement registry for testing
	stmtRegistry := NewRegistry(s.InternalDB().(isql.DB), s.ClusterSettings())
	now := timeutil.Unix(0, 0)
	registry := NewTxnRegistry(s.InternalDB().(isql.DB), s.ClusterSettings(), stmtRegistry, timeutil.NewManualTime(now))

	runner := sqlutils.MakeSQLRunner(srv.SQLConn(t))

	request := TxnRequest{
		txnFingerprintId:    1111,
		stmtFingerprintIds:  []uint64{1111, 2222},
		redacted:            false,
		username:            "testuser",
		expiresAt:           time.Time{},
		minExecutionLatency: 0,
		samplingProbability: 0,
	}

	t.Run("successful", func(t *testing.T) {
		requestID, err := registry.insertTxnRequestInternal(
			ctx,
			request.txnFingerprintId,
			request.stmtFingerprintIds,
			request.username,
			request.samplingProbability,
			request.minExecutionLatency,
			0, // no expiration
			request.redacted,
		)

		require.NoError(t, err)
		// Create mock statement diagnostics for the transaction
		req1 := Request{
			fingerprint:         "SELECT _ FROM test",
			planGist:            "test-gist-1",
			antiPlanGist:        false,
			samplingProbability: 0,
			minExecutionLatency: 0,
			expiresAt:           time.Time{},
			redacted:            false,
			username:            "testuser",
		}
		req2 := Request{
			fingerprint:         "INSERT INTO test VALUES (_)",
			planGist:            "test-gist-2",
			antiPlanGist:        false,
			samplingProbability: 0,
			minExecutionLatency: 0,
			expiresAt:           time.Time{},
			redacted:            false,
			username:            "testuser",
		}

		stmtDiag1 := NewStmtDiagnostic(
			RequestID(0),
			req1,
			"SELECT _ FROM test",
			"SELECT 1 FROM test",
			[]byte("mock bundle data 1"),
			nil,
		)
		stmtDiag2 := NewStmtDiagnostic(
			RequestID(0),
			req2,
			"INSERT INTO test VALUES (_)",
			"INSERT INTO test VALUES (1)",
			[]byte("mock bundle data 2"),
			nil,
		)

		txnDiagnostic := NewTxnDiagnostic([]StmtDiagnostic{stmtDiag1, stmtDiag2}, []byte("mock txn bundle"))

		// Insert the transaction diagnostic
		diagID, err := registry.InsertTxnDiagnostic(ctx, requestID, request, txnDiagnostic)
		require.NoError(t, err)
		require.NotZero(t, diagID)

		var count int
		// Verify that statement_diagnostics table now has entries for the transaction diagnostic
		runner.QueryRow(t, "SELECT count(*) FROM system.statement_diagnostics WHERE transaction_diagnostics_id=$1", diagID).Scan(&count)
		// Verify we have exactly 2 new statement diagnostic entries (one for each statement in the transaction)
		require.Equal(t, 2, count, "should have 2 statement diagnostic entries")

		var bundleChunkCount int
		// Verify that statement_bundle_chunks table has entries for the transaction diagnostic
		runner.QueryRow(t, `
  SELECT count(*)
  FROM system.statement_bundle_chunks sbc
  JOIN system.transaction_diagnostics td ON sbc.id = ANY(td.bundle_chunks)
  WHERE td.id = $1
`, diagID).Scan(&bundleChunkCount)
		require.Equal(t, 1, bundleChunkCount, "should have added 1 transaction diagnostic bundle entry")

		// Verify the transaction_diagnostics entry has correct data
		var (
			dbTxnFingerprintBytes     []byte
			dbTxnFingerprint          string
			dbStmtFingerprintIdsBytes [][]byte
			dbCollectedAt             time.Time
			dbRequestCompleted        bool
		)

		row := runner.QueryRow(t, `
		SELECT td.transaction_fingerprint_id,
		       td.transaction_fingerprint,
		       td.statement_fingerprint_ids,
		       td.collected_at,
		       tdr.completed
		FROM system.transaction_diagnostics td
		JOIN system.transaction_diagnostics_requests tdr ON td.id = tdr.transaction_diagnostics_id
		WHERE td.id = $1
	`, diagID)

		row.Scan(&dbTxnFingerprintBytes, &dbTxnFingerprint, pq.Array(&dbStmtFingerprintIdsBytes), &dbCollectedAt, &dbRequestCompleted)

		require.Equal(t, request.txnFingerprintId, toUint64(t, dbTxnFingerprintBytes), "transaction fingerprint ID should match")
		require.Equal(t, request.stmtFingerprintIds, ToUint64Slice(t, dbStmtFingerprintIdsBytes), "statement fingerprint IDs should match")
		require.Contains(t, dbTxnFingerprint, req1.fingerprint, "transaction fingerprint string should contain statement 1 fingerprint")
		require.Contains(t, dbTxnFingerprint, req2.fingerprint, "transaction fingerprint string should contain statement 2 fingerprint")
		require.True(t, dbRequestCompleted, "request should be completed")
		require.Equal(t, now, dbCollectedAt, "collection time should match current time")
		require.NotContains(t, registry.mu.requests, requestID, "request should be removed from registry")
	})

	t.Run("already completed", func(t *testing.T) {
		requestID, err := registry.insertTxnRequestInternal(
			ctx,
			request.txnFingerprintId,
			request.stmtFingerprintIds,
			request.username,
			request.samplingProbability,
			request.minExecutionLatency,
			0, // no expiration
			request.redacted,
		)

		require.NoError(t, err)
		runner.Exec(t, "UPDATE system.transaction_diagnostics_requests SET completed = true WHERE id = $1", requestID)
		diagId, err := registry.InsertTxnDiagnostic(ctx, requestID, request, NewTxnDiagnostic(nil, []byte("mock txn bundle")))
		require.Zero(t, diagId)
		require.ErrorContains(t, err, "transaction diagnostics request was already completed in another execution")
	})
}

type noopDb struct{}

func (n noopDb) KV() *kv.DB {
	return nil
}

func (n noopDb) Txn(
	ctx context.Context, f func(context.Context, isql.Txn) error, option ...isql.TxnOption,
) error {
	return nil
}

func (n noopDb) Executor(option ...isql.ExecutorOption) isql.Executor {
	return nil
}

func (n noopDb) Session(
	ctx context.Context, name string, options ...isql.ExecutorOption,
) (isql.Session, error) {
	return nil, nil
}

var _ isql.DB = noopDb{}

func toUint64(t *testing.T, bytes []byte) uint64 {
	t.Helper()
	_, fpId, err := encoding.DecodeUint64Ascending(bytes)
	require.NoError(t, err)
	return fpId
}

func ToUint64Slice(t *testing.T, bytes [][]byte) []uint64 {
	t.Helper()
	var ids []uint64
	for _, b := range bytes {
		ids = append(ids, toUint64(t, b))
	}
	return ids
}

func checkDatabaseForRequest(
	t *testing.T, id RequestID, expectedRequest TxnRequest, runner *sqlutils.SQLRunner,
) {
	t.Helper()
	row := runner.QueryRow(t, `
			SELECT transaction_fingerprint_id,
			       CAST(EXTRACT(EPOCH FROM min_execution_latency) * 1000000000 AS INT8),
			       expires_at,
			       sampling_probability,
			       redacted,
			       username,
			       statement_fingerprint_ids
			FROM system.transaction_diagnostics_requests
			WHERE id = $1
		`, id)
	// Query the database to get the inserted TxnRequest data
	var (
		txnFingerprintBytes      []byte
		minExecutionLatencyNanos *int64 // Scan as nanoseconds to avoid driver issues
		expiresAt                *time.Time
		samplingProbability      *float64
		redacted                 bool
		username                 string
		statementFpIdBytes       [][]byte
		statementFpIds           []uint64
	)
	row.Scan(&txnFingerprintBytes,
		&minExecutionLatencyNanos, &expiresAt, &samplingProbability, &redacted, &username, pq.Array(&statementFpIdBytes))

	statementFpIds = ToUint64Slice(t, statementFpIdBytes)

	actualRequest := TxnRequest{
		txnFingerprintId:   toUint64(t, txnFingerprintBytes),
		stmtFingerprintIds: statementFpIds,
		redacted:           redacted,
		username:           username,
	}

	if minExecutionLatencyNanos != nil {
		actualRequest.minExecutionLatency = time.Duration(*minExecutionLatencyNanos)
	}
	if expiresAt != nil {
		actualRequest.expiresAt = *expiresAt
	}
	if samplingProbability != nil {
		actualRequest.samplingProbability = *samplingProbability
	}

	require.Equal(t, expectedRequest, actualRequest)
}

func TestTxnRegistry_CancelRequest_MultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	// Set polling interval to 50ms for faster test execution
	PollingInterval.Override(ctx, &settings.SV, 50*time.Millisecond)

	cluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
		},
	})
	defer cluster.Stopper().Stop(ctx)

	// Get registries from all 3 nodes
	registry1 := cluster.Server(0).ApplicationLayer().TxnRegistry().(*TxnRegistry)
	registry2 := cluster.Server(1).ApplicationLayer().TxnRegistry().(*TxnRegistry)
	registry3 := cluster.Server(2).ApplicationLayer().TxnRegistry().(*TxnRegistry)

	testCases := []struct {
		name                string
		samplingProbability float64
		minExecutionLatency time.Duration
		expiresAfter        time.Duration
		expectedError       string
	}{
		{
			name:                "valid unconditional request",
			samplingProbability: 0,
			minExecutionLatency: 0,
			expiresAfter:        time.Hour,
			expectedError:       "",
		},
		{
			name:                "valid conditional request",
			samplingProbability: 0.5,
			minExecutionLatency: time.Millisecond * 100,
			expiresAfter:        time.Hour,
			expectedError:       "",
		},
		{
			name:                "nonexistent request",
			samplingProbability: 0,
			minExecutionLatency: 0,
			expiresAfter:        0,
			expectedError:       "no pending request found for the request",
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var requestID int64
			var err error

			if tc.expectedError == "" {
				requestID, err = registry1.InsertRequest(
					ctx,
					appstatspb.TransactionFingerprintID(1000+i), // Unique transaction fingerprint
					[]appstatspb.StmtFingerprintID{
						appstatspb.StmtFingerprintID(1111 + i),
						appstatspb.StmtFingerprintID(2222 + i),
						appstatspb.StmtFingerprintID(3333 + i),
					},
					tc.samplingProbability,
					tc.minExecutionLatency,
					tc.expiresAfter,
					false, // not redacted
					"testuser",
				)
				require.NoError(t, err)
				require.NotZero(t, requestID)

				// Wait for the request to propagate to all nodes by checking the database
				// Since polling works asynchronously, we'll verify the request exists in the DB
				testutils.SucceedsSoon(t, func() error {
					_, shouldStart1 := registry1.GetRequest(RequestID(requestID))
					_, shouldStart2 := registry2.GetRequest(RequestID(requestID))
					_, shouldStart3 := registry3.GetRequest(RequestID(requestID))

					if shouldStart1 && shouldStart2 && shouldStart3 {
						return nil
					}
					return errors.Newf("missing diagnostic on at least one of the nodes (%t, %t, %t)", shouldStart1, shouldStart2, shouldStart3)
				})

			} else {
				// Use a nonexistent request ID
				requestID = 99999
			}

			// Cancel the request using node 2 (different from the one that created it)
			err = registry2.CancelRequest(ctx, requestID)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}
			require.NoError(t, err)

			// Verify the request is marked as expired in the database
			runner := sqlutils.MakeSQLRunner(cluster.Server(0).SQLConn(t))
			var expiresAt time.Time
			err = runner.DB.QueryRowContext(ctx,
				"SELECT expires_at FROM system.transaction_diagnostics_requests WHERE id = $1",
				requestID).Scan(&expiresAt)
			require.NoError(t, err)

			// The expires_at should be set to '1970-01-01' (Unix epoch)
			expectedExpiration := time.Unix(0, 0).UTC()
			require.True(t, expiresAt.Equal(expectedExpiration) || expiresAt.Before(expectedExpiration.Add(time.Second)),
				"Expected expires_at to be set to epoch time, got %v", expiresAt)

			// Verify the request is eventually removed from all nodes' registries
			testutils.SucceedsSoon(t, func() error {
				_, shouldStart1 := registry1.GetRequest(RequestID(requestID))
				_, shouldStart2 := registry2.GetRequest(RequestID(requestID))
				_, shouldStart3 := registry3.GetRequest(RequestID(requestID))

				if shouldStart1 || shouldStart2 || shouldStart3 {
					return errors.New("request still found in registry after cancellation")
				}
				return nil
			})
		})
	}
}
