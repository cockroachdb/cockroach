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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params := base.TestServerArgs{}
	s, db, _ := serverutils.StartServer(t, params)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	completedQuery := "SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1"
	checkNotCompleted := func(reqID int64) {
		reqRow := db.QueryRow(completedQuery, reqID)
		var completed bool
		var traceID gosql.NullInt64
		require.NoError(t, reqRow.Scan(&completed, &traceID))
		require.False(t, completed)
		require.False(t, traceID.Valid) // traceID should be NULL
	}
	checkCompleted := func(reqID int64) {
		var completed bool
		var traceID gosql.NullInt64
		traceRow := db.QueryRow(completedQuery, reqID)
		require.NoError(t, traceRow.Scan(&completed, &traceID))
		require.True(t, completed)
		require.True(t, traceID.Valid)
	}
	// checkMaybeCompleted returns an error if 'completed' value for the given
	// request is different from expectedCompleted.
	checkMaybeCompleted := func(reqID int64, expectedCompleted bool) error {
		var completed bool
		var traceID gosql.NullInt64
		traceRow := db.QueryRow(completedQuery, reqID)
		require.NoError(t, traceRow.Scan(&completed, &traceID))
		if completed != expectedCompleted {
			return errors.Newf("expected completed to be %t, but found %t", expectedCompleted, completed)
		}
		// traceID is NULL when the request hasn't been completed yet.
		require.True(t, traceID.Valid == expectedCompleted)
		return nil
	}

	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	var minExecutionLatency, expiresAfter time.Duration

	// Ask to trace a particular query.
	t.Run("basic", func(t *testing.T) {
		reqID, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)", minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Run the query.
		_, err = db.Exec("INSERT INTO test VALUES (1)")
		require.NoError(t, err)

		// Check that the row from statement_diagnostics_request was marked as
		// completed.
		checkCompleted(reqID)
	})

	// Verify that we can handle multiple requests at the same time.
	t.Run("multiple", func(t *testing.T) {
		id1, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)", minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		id2, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test", minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		id3, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test WHERE x > _", minExecutionLatency, expiresAfter)
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
	})

	// Verify that EXECUTE triggers diagnostics collection (#66048).
	t.Run("execute", func(t *testing.T) {
		id, err := registry.InsertRequestInternal(ctx, "SELECT x + $1 FROM test", minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		_, err = db.Exec("PREPARE stmt AS SELECT x + $1 FROM test")
		require.NoError(t, err)
		_, err = db.Exec("EXECUTE stmt(1)")
		require.NoError(t, err)
		checkCompleted(id)
	})

	// Verify that if the traced query times out, the bundle is still saved.
	t.Run("timeout", func(t *testing.T) {
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)", minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Set the statement timeout (as well as clean it up in a defer).
		_, err = db.Exec("SET statement_timeout = '100ms';")
		require.NoError(t, err)
		defer func() {
			_, err = db.Exec("RESET statement_timeout;")
			require.NoError(t, err)
		}()

		// Run the query that times out.
		_, err = db.Exec("SELECT pg_sleep(999999)")
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), sqlerrors.QueryTimeoutError.Error()))
		checkCompleted(reqID)
	})

	// Verify that the bundle for a conditional request is only created when the
	// condition is satisfied.
	t.Run("conditional", func(t *testing.T) {
		minExecutionLatency := 100 * time.Millisecond
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)", minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Run the fast query.
		_, err = db.Exec("SELECT pg_sleep(0)")
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Run the slow query.
		_, err = db.Exec("SELECT pg_sleep(0.2)")
		require.NoError(t, err)
		checkCompleted(reqID)
	})

	// Verify that if a conditional request expired, the bundle for it is not
	// created even if the condition is satisfied.
	t.Run("conditional expired", func(t *testing.T) {
		minExecutionLatency := 100 * time.Millisecond
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", minExecutionLatency, time.Nanosecond,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Sleep for a bit and then run the slow query.
		time.Sleep(100 * time.Millisecond)
		_, err = db.Exec("SELECT pg_sleep(0.2)")
		require.NoError(t, err)
		// The request must have expired by now.
		checkNotCompleted(reqID)
	})

	// Verify that if we have an influx of queries matching the fingerprint of
	// a conditional diagnostics request and at least one instance satisfies the
	// conditional, then the bundle is collected.
	t.Run("conditional with concurrency", func(t *testing.T) {
		minExecutionLatency := 100 * time.Millisecond
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep($1)", minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Spin up 10 goroutines where only the last one executes the query slow
		// enough to satisfy the conditional.
		const numGoroutines = 10
		var wg sync.WaitGroup
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(i int) {
				defer wg.Done()
				sleepDuration := fmt.Sprintf("0.0%d", i)
				if i == numGoroutines-1 {
					sleepDuration = "0.2"
				}
				_, err := db.Exec("SELECT pg_sleep($1)", sleepDuration)
				require.NoError(t, err)
			}(i)
		}

		// Wait for all goroutines to finish and check that the bundle was
		// collected.
		wg.Wait()
		checkCompleted(reqID)
	})

	// Verify that an error is returned when attempting to cancel non-existent
	// request.
	t.Run("cancel non-existent request", func(t *testing.T) {
		require.NotNil(t, registry.CancelRequest(ctx, 123456789))
	})

	// Verify that if a request (either conditional or unconditional, w/ or w/o
	// expiration) is canceled, the bundle for it is not created afterwards.
	t.Run("request canceled", func(t *testing.T) {
		const fprint = "SELECT pg_sleep(_)"
		for _, conditional := range []bool{false, true} {
			t.Run(fmt.Sprintf("conditional=%t", conditional), func(t *testing.T) {
				var minExecutionLatency time.Duration
				if conditional {
					minExecutionLatency = 100 * time.Millisecond
				}
				for _, expiresAfter := range []time.Duration{0, time.Second} {
					t.Run(fmt.Sprintf("expiresAfter=%s", expiresAfter), func(t *testing.T) {
						// TODO(yuzefovich): for some reason occasionally the
						// bundle for the request is collected, so we use
						// SucceedsSoon. Figure it out.
						testutils.SucceedsSoon(t, func() error {
							reqID, err := registry.InsertRequestInternal(
								ctx, fprint, minExecutionLatency, expiresAfter,
							)
							require.NoError(t, err)
							checkNotCompleted(reqID)

							err = registry.CancelRequest(ctx, reqID)
							require.NoError(t, err)
							checkNotCompleted(reqID)

							// Run the query that is slow enough to satisfy the
							// conditional request.
							_, err = db.Exec("SELECT pg_sleep(0.2)")
							require.NoError(t, err)
							return checkMaybeCompleted(reqID, false /* expectedCompleted */)
						})
					})
				}
			})
		}
	})

	// Verify that if a request (either conditional or unconditional) is
	// canceled, the ongoing bundle for it is still created.
	t.Run("ongoing request canceled", func(t *testing.T) {
		const fprint = "SELECT pg_sleep(_)"
		for _, conditional := range []bool{false, true} {
			t.Run("conditional", func(t *testing.T) {
				// There is a possibility that the request is canceled before
				// the query starts, so we allow for SucceedsSoon on this test.
				testutils.SucceedsSoon(t, func() error {
					var minExecutionLatency time.Duration
					if conditional {
						minExecutionLatency = 100 * time.Millisecond
					}
					reqID, err := registry.InsertRequestInternal(
						ctx, fprint, minExecutionLatency, expiresAfter,
					)
					require.NoError(t, err)
					checkNotCompleted(reqID)

					// waitCh is used to block the cancellation goroutine before
					// the query starts executing.
					waitCh := make(chan struct{})

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-waitCh
						err := registry.CancelRequest(ctx, reqID)
						require.NoError(t, err)
					}()

					// Now run the query that is slow enough to satisfy the
					// conditional request.
					close(waitCh)
					_, err = db.Exec("SELECT pg_sleep(0.2)")
					require.NoError(t, err)

					wg.Wait()
					return checkMaybeCompleted(reqID, true /* expectedCompleted */)
				})
			})
		}
	})
}

// Test that a different node can service a diagnostics request.
func TestDiagnosticsRequestDifferentNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	db0 := tc.ServerConn(0)
	db1 := tc.ServerConn(1)
	_, err := db0.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	var minExecutionLatency, expiresAfter time.Duration

	// Ask to trace a particular query using node 0.
	registry := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	reqID, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)", minExecutionLatency, expiresAfter)
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
	id1, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)", minExecutionLatency, expiresAfter)
	require.NoError(t, err)
	id2, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test", minExecutionLatency, expiresAfter)
	require.NoError(t, err)
	id3, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test WHERE x > _", minExecutionLatency, expiresAfter)
	require.NoError(t, err)

	// Run the queries in a different order.
	runUntilTraced("SELECT x FROM test", id2)
	runUntilTraced("SELECT x FROM test WHERE x > 1", id3)
	runUntilTraced("INSERT INTO test VALUES (2)", id1)
}

// TestChangePollInterval ensures that changing the polling interval takes effect.
func TestChangePollInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// We'll inject a request filter to detect scans due to the polling.
	tableStart := keys.SystemSQLCodec.TablePrefix(uint32(systemschema.StatementDiagnosticsRequestsTable.GetID()))
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
