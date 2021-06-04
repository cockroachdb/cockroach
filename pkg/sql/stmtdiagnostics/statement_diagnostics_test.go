// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stmtdiagnostics_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	// Ask to trace a particular query.
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	reqID, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)")
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
	checkCompleted := func(reqID int64) {
		traceRow := db.QueryRow(
			"SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1", reqID)
		require.NoError(t, traceRow.Scan(&completed, &traceID))
		require.True(t, completed)
		require.True(t, traceID.Valid)
	}

	checkCompleted(reqID)

	// Verify that we can handle multiple requests at the same time.
	id1, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	id2, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test")
	require.NoError(t, err)
	id3, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test WHERE x > _")
	require.NoError(t, err)

	// Run the queries in a different order.
	_, err = db.Exec("SELECT x FROM test")
	require.NoError(t, err)
	checkCompleted(id2)

	_, err = db.Exec("SELECT x FROM test WHERE x > 1")
	require.NoError(t, err)
	checkCompleted(id3)

	_, err = db.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	checkCompleted(id1)

	// Verify that EXECUTE triggers diagnostics collection (#66048).
	id4, err := registry.InsertRequestInternal(ctx, "SELECT x + $1 FROM test")
	require.NoError(t, err)
	_, err = db.Exec("PREPARE stmt AS SELECT x + $1 FROM test")
	require.NoError(t, err)
	_, err = db.Exec("EXECUTE stmt(1)")
	require.NoError(t, err)
	checkCompleted(id4)
}

// Test that a different node can service a diagnostics request.
func TestDiagnosticsRequestDifferentNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	db0 := tc.ServerConn(0)
	db1 := tc.ServerConn(1)
	_, err := db0.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	// Ask to trace a particular query using node 0.
	registry := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	reqID, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	reqRow := db0.QueryRow(
		`SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests
		 WHERE ID = $1`, reqID)
	var completed bool
	var traceID gosql.NullInt64
	require.NoError(t, reqRow.Scan(&completed, &traceID))
	require.False(t, completed)
	require.False(t, traceID.Valid) // traceID should be NULL

	// Repeatedly run the query through node 1 until we get a trace.
	runUntilTraced := func(query string, reqID int64) {
		testutils.SucceedsSoon(t, func() error {
			// Run the query using node 1.
			_, err = db1.Exec(query)
			require.NoError(t, err)

			// Check that the row from statement_diagnostics_request was marked as completed.
			traceRow := db0.QueryRow(
				`SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests
				 WHERE ID = $1`, reqID)
			require.NoError(t, traceRow.Scan(&completed, &traceID))
			if !completed {
				_, err := db0.Exec("DELETE FROM test")
				require.NoError(t, err)
				return fmt.Errorf("not completed yet")
			}
			return nil
		})
		require.True(t, traceID.Valid)
	}
	runUntilTraced("INSERT INTO test VALUES (1)", reqID)

	// Verify that we can handle multiple requests at the same time.
	id1, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	id2, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test")
	require.NoError(t, err)
	id3, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test WHERE x > _")
	require.NoError(t, err)

	// Run the queries in a different order.
	runUntilTraced("SELECT x FROM test", id2)
	runUntilTraced("SELECT x FROM test WHERE x > 1", id3)
	runUntilTraced("INSERT INTO test VALUES (2)", id1)
}

// TestChangePollInterval ensures that changing the polling interval takes effect.
func TestChangePollInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// We'll inject a request filter to detect scans due to the polling.
	tableStart := keys.SystemSQLCodec.TablePrefix(keys.StatementDiagnosticsRequestsTableID)
	tableSpan := roachpb.Span{
		Key:    tableStart,
		EndKey: tableStart.PrefixEnd(),
	}
	var scanState = struct {
		syncutil.Mutex
		m map[uuid.UUID]struct{}
	}{
		m: map[uuid.UUID]struct{}{},
	}
	recordScan := func(id uuid.UUID) {
		scanState.Lock()
		defer scanState.Unlock()
		scanState.m[id] = struct{}{}
	}
	numScans := func() int {
		scanState.Lock()
		defer scanState.Unlock()
		return len(scanState.m)
	}
	waitForScans := func(atLeast int) (seen int) {
		testutils.SucceedsSoon(t, func() error {
			if seen = numScans(); seen < atLeast {
				return errors.Errorf("expected at least %d scans, saw %d", atLeast, seen)
			}
			return nil
		})
		return seen
	}
	settings := cluster.MakeTestingClusterSettings()

	// Set an extremely long initial polling interval to not hit flakes due to
	// server startup taking more than 10s.
	stmtdiagnostics.PollingInterval.Override(ctx, &settings.SV, time.Hour)
	args := base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
					if request.Txn == nil {
						return nil
					}
					for _, req := range request.Requests {
						if scan := req.GetScan(); scan != nil && scan.Span().Overlaps(tableSpan) {
							recordScan(request.Txn.ID)
							return nil
						}
					}
					return nil
				},
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	require.Equal(t, 1, waitForScans(1))
	time.Sleep(time.Millisecond) // ensure no unexpected scan occur
	require.Equal(t, 1, waitForScans(1))
	_, err := db.Exec("SET CLUSTER SETTING sql.stmt_diagnostics.poll_interval = '200us'")
	require.NoError(t, err)
	waitForScans(10) // ensure several scans occur
}
