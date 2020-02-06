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
	traceRow := db.QueryRow(
		"SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1", reqID)
	require.NoError(t, traceRow.Scan(&completed, &traceID))
	require.True(t, completed)
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
