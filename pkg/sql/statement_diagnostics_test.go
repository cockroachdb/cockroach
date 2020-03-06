// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestTraceRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	// Ask to trace a particular query.
	reqID, err := s.ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.InsertRequest(
		ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	reqRow := db.QueryRow(
		"SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1", reqID)
	var completed bool
	var traceID gosql.NullInt64
	require.NoError(t, reqRow.Scan(&completed, &traceID))
	require.False(t, completed)
	require.False(t, traceID.Valid) // traceID should be NULL

	// Run the query.
	_, err = db.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)

	// Check that the row from statement_diagnostics_request was marked as completed.
	var completedAt *time.Time
	traceRow := db.QueryRow(
		"SELECT completed, completed_at, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1", reqID)
	require.NoError(t, traceRow.Scan(&completed, &completedAt, &traceID))
	require.True(t, completed)
	require.NotNil(t, completedAt)
	require.True(t, traceID.Valid)

	// Check the trace.
	row := db.QueryRow("SELECT jsonb_pretty(trace) FROM system.statement_diagnostics WHERE ID = $1", traceID.Int64)
	var json string
	require.NoError(t, row.Scan(&json))
	require.Contains(t, json, "traced statement")
	require.Contains(t, json, "statement execution committed the txn")
}

// Test that a different node can service a trace request.
func TestTraceRequestDifferentNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := serverutils.StartTestCluster(t, 2, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	db0 := tc.ServerConn(0)
	db1 := tc.ServerConn(1)
	_, err := db0.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	// Ask to trace a particular query using node 0.
	reqID, err := tc.Server(0).ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.InsertRequest(
		ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	reqRow := db0.QueryRow(
		"SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1", reqID)
	var completed bool
	var traceID gosql.NullInt64
	require.NoError(t, reqRow.Scan(&completed, &traceID))
	require.False(t, completed)
	require.False(t, traceID.Valid) // traceID should be NULL

	// Repeatedly run the query through node 1 until we get a trace.
	testutils.SucceedsSoon(t, func() error {
		// Run the query using node 1.
		_, err = db1.Exec("INSERT INTO test VALUES (1)")
		require.NoError(t, err)

		// Check that the row from statement_diagnostics_request was marked as completed.
		traceRow := db0.QueryRow(
			"SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1",
			reqID)
		require.NoError(t, traceRow.Scan(&completed, &traceID))
		if !completed {
			_, err := db0.Exec("DELETE FROM test")
			require.NoError(t, err)
			return fmt.Errorf("not completed yet")
		}
		return nil
	})
	require.True(t, traceID.Valid)

	// Check the trace.
	row := db0.QueryRow("SELECT jsonb_pretty(trace) FROM system.statement_diagnostics WHERE ID = $1",
		traceID.Int64)
	var json string
	require.NoError(t, row.Scan(&json))
	require.Contains(t, json, "traced statement")
	require.Contains(t, json, "statement execution committed the txn")
}

func TestStmtDiagnosticsRequestRegistry_GetAllRequests_singleRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := serverutils.StartTestCluster(t, 2, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	testFingerprint := "INSERT INTO test VALUES (_)"
	_, err := tc.Server(0).ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.InsertRequest(
		ctx, testFingerprint)
	require.NoError(t, err)

	requests, err := tc.Server(0).ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.GetAllRequests(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(requests))
	require.Equal(t, testFingerprint, requests[0].StatementFingerprint)
}

func TestStmtDiagnosticsRequestRegistry_GetAllRequests_manyRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := serverutils.StartTestCluster(t, 2, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	for i := 0; i < 3; i++ {
		testFingerprint := fmt.Sprintf("INSERT INTO test%d VALUES (_)", i)
		_, err := tc.Server(0).ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.InsertRequest(
			ctx, testFingerprint)
		require.NoError(t, err)
	}

	requests, err := tc.Server(0).ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.GetAllRequests(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(requests))
}

func TestStmtDiagnosticsRequestRegistry_GetAllRequests_and_GetStatementDiagnostics_completed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	// Ask to trace a particular query using node 0.
	testFingerprint := "INSERT INTO test VALUES (_)"
	_, err = s.ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.InsertRequest(
		ctx, testFingerprint)
	require.NoError(t, err)

	requests, err := s.ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.GetAllRequests(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(requests))
	require.Equal(t, testFingerprint, requests[0].StatementFingerprint)

	// complete the request
	// Run the query.
	_, err = db.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)

	// Check that the row from statement_diagnostics_request was marked as completed.
	requests, err = s.ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.GetAllRequests(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(requests))
	require.True(t, requests[0].Completed)
	require.NotNil(t, requests[0].CompletedAt)

	statementDiagnosticsID := requests[0].StatementDiagnosticsID

	// Check that we can fetch the trace as well
	request, err :=
		s.ExecutorConfig().(ExecutorConfig).stmtInfoRequestRegistry.GetStatementDiagnostics(
			ctx, statementDiagnosticsID,
		)
	require.NoError(t, err)
	require.NotNil(t, request.CollectedAt)

	json := request.Trace
	require.Contains(t, json, "traced statement")
	require.Contains(t, json, "statement execution committed the txn")
}
