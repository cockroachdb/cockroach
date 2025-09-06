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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestTxnRegistry_ShouldStartTxnDiagnostic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseRequest := TxnRequest{
		txnFingerprintId:   1111,
		stmtFingerprintsId: []uint64{1111, 2222, 3333},
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
			samplingProbability: 0.01,
			queryFingerprintId:  1111,
			expectedShouldStart: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			request := baseRequest
			request.expiresAt = tc.expiresAt
			request.minExecutionLatency = tc.minExecutionLatency
			request.samplingProbability = tc.samplingProbability

			registry := NewTxnRegistry(noopDb{}, nil, nil)
			requestId := RequestID(1)
			registry.mu.requests[requestId] = request

			shouldCollect, id, actual := registry.ShouldStartTxnDiagnostic(context.Background(), tc.queryFingerprintId)

			require.Equal(t, tc.expectedShouldStart, shouldCollect)
			var expectedRequest TxnRequest
			var expectedRequestId RequestID
			if tc.expectedShouldStart {
				expectedRequestId = requestId
				expectedRequest = request
				if actual.isConditional() {
					requestInMap, ok := registry.mu.requests[requestId]
					require.True(t, ok)
					require.Equal(t, expectedRequest, requestInMap)
					require.Empty(t, registry.mu.unconditionalOngoingRequests)
				} else {
					requestInOngoing, ok := registry.mu.unconditionalOngoingRequests[requestId]
					require.True(t, ok)
					require.Equal(t, expectedRequest, requestInOngoing)
					require.Empty(t, registry.mu.requests)

				}
			}
			require.Equal(t, expectedRequestId, id)
			require.Equal(t, expectedRequest, actual)
		})
	}
}

func TestTxnRegistry_InsertTxnRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	registry := NewTxnRegistry(s.InternalDB().(isql.DB), s.ClusterSettings(), nil)

	t.Run("valid request", func(t *testing.T) {
		err := registry.InsertTxnRequest(
			ctx,
			1111,
			[]uint64{1111, 2222, 3333},
			"testuser",
			0.5,
			time.Millisecond*100,
			time.Hour,
			false,
		)
		require.NoError(t, err)
		require.NotEmpty(t, registry.mu.requests)
	})

	t.Run("invalid sampling probability", func(t *testing.T) {
		err := registry.InsertTxnRequest(
			ctx,
			1111,
			[]uint64{1111, 2222, 3333},
			"testuser",
			1.5,
			time.Millisecond*100,
			time.Hour,
			false,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected sampling probability in range [0.0, 1.0]")
	})

	t.Run("sampling without latency", func(t *testing.T) {
		err := registry.InsertTxnRequest(
			ctx,
			1111,
			[]uint64{1111, 2222, 3333},
			"testuser",
			0.5,
			0,
			time.Hour,
			false,
		)
		require.ErrorContains(t, err, "got non-zero sampling probability")
	})

}

func TestTxnRegistry_InsertTxnRequest_Polling(t *testing.T) {
	// TODO: create a multi-node cluster to test that it propagates correctly, once persistence is added.
}

func TestTxnRegistry_ResetTxnRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("request_reset", func(t *testing.T) {
		registry := NewTxnRegistry(noopDb{}, nil, nil)
		requestId := RequestID(1)
		expectedRequest := TxnRequest{
			txnFingerprintId:    1111,
			stmtFingerprintsId:  []uint64{1111, 2222, 3333},
			redacted:            false,
			username:            "",
			expiresAt:           time.Time{},
			minExecutionLatency: 0,
			samplingProbability: 0,
		}
		registry.mu.unconditionalOngoingRequests[requestId] = expectedRequest

		require.Empty(t, registry.mu.requests)
		actualReq, ok := registry.ResetTxnRequest(requestId)
		require.True(t, ok)
		require.Equal(t, expectedRequest, actualReq)
		// unconditionalOngoingRequests should be empty now
		require.Empty(t, registry.mu.unconditionalOngoingRequests)

		// Should be in the requests map now
		req, ok := registry.mu.requests[requestId]
		require.True(t, ok)
		require.Equal(t, expectedRequest, req)
	})

	t.Run("request_not_found", func(t *testing.T) {
		registry := NewTxnRegistry(noopDb{}, nil, nil)
		requestId := RequestID(1)
		expectedRequest := TxnRequest{
			txnFingerprintId:    1111,
			stmtFingerprintsId:  []uint64{1111, 2222, 3333},
			redacted:            false,
			username:            "",
			expiresAt:           time.Time{},
			minExecutionLatency: 0,
			samplingProbability: 0,
		}
		registry.mu.unconditionalOngoingRequests[requestId] = expectedRequest
		actualReq, ok := registry.ResetTxnRequest(RequestID(2))
		require.False(t, ok)
		require.Equal(t, TxnRequest{}, actualReq)
		require.Contains(t, registry.mu.unconditionalOngoingRequests, requestId)
	})
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
	registry := NewTxnRegistry(s.InternalDB().(isql.DB), s.ClusterSettings(), stmtRegistry)

	runner := sqlutils.MakeSQLRunner(srv.SQLConn(t))

	// Verify table is initially empty
	var count int
	runner.QueryRow(t, "SELECT count(*) FROM system.statement_diagnostics").Scan(&count)
	initialCount := count
	require.Zero(t, initialCount)
	// Create test data
	requestID := RequestID(123)
	request := TxnRequest{
		txnFingerprintId:    1111,
		stmtFingerprintsId:  []uint64{1111, 2222},
		redacted:            false,
		username:            "testuser",
		expiresAt:           time.Time{},
		minExecutionLatency: 0,
		samplingProbability: 0,
	}

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

	// Verify that statement_diagnostics table now has entries
	runner.QueryRow(t, "SELECT count(*) FROM system.statement_diagnostics").Scan(&count)
	require.Greater(t, count, initialCount, "statement_diagnostics table should have new entries")

	// Verify we have exactly 2 new statement diagnostic entries (one for each statement in the transaction)
	require.Equal(t, initialCount+2, count, "should have added 2 statement diagnostic entries")

	var bundleChunkCount int
	// Verify that statement_diagnostics table now has entries
	runner.QueryRow(t, "SELECT count(*) FROM system.statement_bundle_chunks WHERE description ='transaction diagnostics bundle'").Scan(&bundleChunkCount)
	require.Equal(t, 1, bundleChunkCount, "should have added 1 transaction diagnostic bundle entry")
	// TODO: Verify that data is in the txn_diagnostics table once it is created

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

var _ isql.DB = noopDb{}
