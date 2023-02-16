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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	var collectUntilExpirationEnabled bool
	isCompleted := func(reqID int64) (completed bool, diagnosticsID gosql.NullInt64) {
		completedQuery := "SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1"
		reqRow := db.QueryRow(completedQuery, reqID)
		require.NoError(t, reqRow.Scan(&completed, &diagnosticsID))
		if completed && !collectUntilExpirationEnabled {
			// Ensure that if the request was completed and the continuous
			// collection is not enabled, the local registry no longer has the
			// request.
			require.False(
				t, registry.TestingFindRequest(reqID), "request was "+
					"completed and should have been removed from the registry",
			)
		}
		return completed, diagnosticsID
	}
	checkNotCompleted := func(reqID int64) {
		completed, diagnosticsID := isCompleted(reqID)
		require.False(t, completed)
		require.False(t, diagnosticsID.Valid) // diagnosticsID should be NULL
	}
	checkCompleted := func(reqID int64) {
		completed, diagnosticsID := isCompleted(reqID)
		require.True(t, completed)
		require.True(t, diagnosticsID.Valid)
	}
	// checkMaybeCompleted returns an error if 'completed' value for the given
	// request is different from expectedCompleted.
	checkMaybeCompleted := func(reqID int64, expectedCompleted bool) error {
		completed, diagnosticsID := isCompleted(reqID)
		if completed != expectedCompleted {
			return errors.Newf("expected completed to be %t, but found %t", expectedCompleted, completed)
		}
		// diagnosticsID is NULL when the request hasn't been completed yet.
		require.True(t, diagnosticsID.Valid == expectedCompleted)
		return nil
	}
	setCollectUntilExpiration := func(v bool) {
		collectUntilExpirationEnabled = v
		_, err := db.Exec(
			fmt.Sprintf("SET CLUSTER SETTING sql.stmt_diagnostics.collect_continuously.enabled = %t", v))
		require.NoError(t, err)
	}
	setPollInterval := func(d time.Duration) {
		_, err := db.Exec(
			fmt.Sprintf("SET CLUSTER SETTING sql.stmt_diagnostics.poll_interval = '%s'", d))
		require.NoError(t, err)
	}

	var minExecutionLatency, expiresAfter time.Duration
	var samplingProbability float64

	// Ask to trace a particular query.
	t.Run("basic", func(t *testing.T) {
		reqID, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)", samplingProbability, minExecutionLatency, expiresAfter)
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
		id1, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)", samplingProbability, minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		id2, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test", samplingProbability, minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		id3, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test WHERE x > _", samplingProbability, minExecutionLatency, expiresAfter)
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
		id, err := registry.InsertRequestInternal(ctx, "SELECT x + $1 FROM test", samplingProbability, minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		_, err = db.Exec("PREPARE stmt AS SELECT x + $1 FROM test")
		require.NoError(t, err)
		_, err = db.Exec("EXECUTE stmt(1)")
		require.NoError(t, err)
		checkCompleted(id)
	})

	// Verify that if the traced query times out, the bundle is still saved.
	t.Run("timeout", func(t *testing.T) {
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)", samplingProbability, minExecutionLatency, expiresAfter)
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
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)", samplingProbability, minExecutionLatency, expiresAfter)
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
			ctx, "SELECT pg_sleep(_)", samplingProbability, minExecutionLatency, time.Nanosecond,
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
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep($1)", samplingProbability, minExecutionLatency, expiresAfter)
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
								ctx, fprint, samplingProbability, minExecutionLatency, expiresAfter,
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
						ctx, fprint, samplingProbability, minExecutionLatency, expiresAfter,
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

	// Ask to trace a statement probabilistically.
	t.Run("probabilistic sample", func(t *testing.T) {
		samplingProbability, minExecutionLatency := 0.9999, time.Microsecond
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)",
			samplingProbability, minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		testutils.SucceedsSoon(t, func() error {
			_, err = db.Exec("SELECT pg_sleep(0.01)") // run the query
			require.NoError(t, err)
			completed, _ := isCompleted(reqID)
			if completed {
				return nil
			}
			return errors.New("expected to capture diagnostic bundle")
		})
	})

	t.Run("sampling without latency threshold disallowed", func(t *testing.T) {
		samplingProbability, expiresAfter := 0.5, time.Second
		_, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)",
			samplingProbability, 0 /* minExecutionLatency */, expiresAfter)
		testutils.IsError(err, "empty min exec latency")
	})

	t.Run("continuous capture disabled without sampling probability", func(t *testing.T) {
		// We validate that continuous captures is disabled when a sampling
		// probability of 0.0 is used. We know that it's disabled given the
		// diagnostic request is marked as completed despite us not getting to
		// the expiration point +1h from now (we don't mark continuous captures
		// as completed until they've expired).
		samplingProbability, minExecutionLatency, expiresAfter := 0.0, time.Microsecond, time.Hour
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)",
			samplingProbability, minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		setCollectUntilExpiration(true)
		defer setCollectUntilExpiration(false)

		testutils.SucceedsSoon(t, func() error {
			_, err := db.Exec("SELECT pg_sleep(0.01)") // run the query
			require.NoError(t, err)
			completed, _ := isCompleted(reqID)
			if completed {
				return nil
			}
			return errors.New("expected request to have been completed")
		})
	})

	t.Run("continuous capture disabled without expiration timestamp", func(t *testing.T) {
		// We don't mark continuous captures as completed until they've expired,
		// so we require an explicit expiration set. See previous test case for
		// some commentary.
		samplingProbability, minExecutionLatency, expiresAfter := 0.999, time.Microsecond, 0*time.Hour
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)",
			samplingProbability, minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		setCollectUntilExpiration(true)
		defer setCollectUntilExpiration(false)

		testutils.SucceedsSoon(t, func() error {
			_, err := db.Exec("SELECT pg_sleep(0.01)") // run the query
			require.NoError(t, err)
			completed, _ := isCompleted(reqID)
			if completed {
				return nil
			}
			return errors.New("expected request to have been completed")
		})
	})

	t.Run("continuous capture", func(t *testing.T) {
		samplingProbability, minExecutionLatency, expiresAfter := 0.9999, time.Microsecond, time.Hour
		reqID, err := registry.InsertRequestInternal(ctx, "SELECT pg_sleep(_)",
			samplingProbability, minExecutionLatency, expiresAfter)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		setCollectUntilExpiration(true)
		defer setCollectUntilExpiration(false)

		var firstDiagnosticID int64
		testutils.SucceedsSoon(t, func() error {
			_, err := db.Exec("SELECT pg_sleep(0.01)") // run the query
			require.NoError(t, err)
			completed, diagnosticID := isCompleted(reqID)
			if !diagnosticID.Valid {
				return errors.New("expected to capture diagnostic bundle")
			}
			require.False(t, completed) // should not be marked as completed
			if firstDiagnosticID == 0 {
				firstDiagnosticID = diagnosticID.Int64
			}
			if firstDiagnosticID == diagnosticID.Int64 {
				return errors.New("waiting to capture second bundle")
			}
			return nil
		})

		require.NoError(t, registry.CancelRequest(ctx, reqID))
	})

	t.Run("continuous capture until expiration", func(t *testing.T) {
		samplingProbability, minExecutionLatency, expiresAfter := 0.9999, time.Microsecond, 100*time.Millisecond
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", samplingProbability, minExecutionLatency, expiresAfter,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		setCollectUntilExpiration(true)
		defer setCollectUntilExpiration(false)

		// Sleep until expiration (and then some), and then run the query.
		time.Sleep(expiresAfter + 100*time.Millisecond)

		setPollInterval(10 * time.Millisecond)
		defer setPollInterval(stmtdiagnostics.PollingInterval.Default())

		// We should not find the request and a subsequent executions should not
		// capture anything.
		testutils.SucceedsSoon(t, func() error {
			if found := registry.TestingFindRequest(reqID); found {
				return errors.New("expected expired request to no longer be tracked")
			}
			return nil
		})

		_, err = db.Exec("SELECT pg_sleep(0.01)") // run the query
		require.NoError(t, err)
		checkNotCompleted(reqID)
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

	// Lower the polling interval to speed up the test.
	_, err = db0.Exec("SET CLUSTER SETTING sql.stmt_diagnostics.poll_interval = '1ms'")
	require.NoError(t, err)

	var minExecutionLatency, expiresAfter time.Duration
	var samplingProbability float64

	// Ask to trace a particular query using node 0.
	registry := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	reqID, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)", samplingProbability, minExecutionLatency, expiresAfter)
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
	id1, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)", samplingProbability, minExecutionLatency, expiresAfter)
	require.NoError(t, err)
	id2, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test", samplingProbability, minExecutionLatency, expiresAfter)
	require.NoError(t, err)
	id3, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test WHERE x > _", samplingProbability, minExecutionLatency, expiresAfter)
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
				TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
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
