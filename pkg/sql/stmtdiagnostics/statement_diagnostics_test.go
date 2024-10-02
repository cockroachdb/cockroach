// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

	skip.UnderShort(t)
	skip.UnderDuress(t, "the test is too slow")

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, "CREATE TABLE test (x int PRIMARY KEY)")

	// Disable polling interval since we're inserting requests directly into the
	// registry manually and want precise control of updating the registry.
	stmtdiagnostics.PollingInterval.Override(ctx, &s.ClusterSettings().SV, 0)

	var collectUntilExpirationEnabled bool
	isCompleted := func(reqID int64) (completed bool, diagnosticsID gosql.NullInt64) {
		completedQuery := "SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1"
		reqRow := runner.QueryRow(t, completedQuery, reqID)
		reqRow.Scan(&completed, &diagnosticsID)
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
	setCollectUntilExpiration := func(v bool) {
		collectUntilExpirationEnabled = v
		runner.Exec(t, fmt.Sprintf("SET CLUSTER SETTING sql.stmt_diagnostics.collect_continuously.enabled = %t", v))
	}

	var anyPlan string
	var noAntiMatch bool
	var sampleAll float64
	var noLatencyThreshold, noExpiration time.Duration

	// Ask to trace a particular query.
	t.Run("basic", func(t *testing.T) {
		reqID, err := registry.InsertRequestInternal(
			ctx, "INSERT INTO test VALUES (_)", anyPlan, noAntiMatch,
			sampleAll, noLatencyThreshold, noExpiration,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Run the query.
		runner.Exec(t, "INSERT INTO test VALUES (1)")

		// Check that the row from statement_diagnostics_request was marked as
		// completed.
		checkCompleted(reqID)
	})

	// Verify that we can handle multiple requests at the same time.
	t.Run("multiple", func(t *testing.T) {
		id1, err := registry.InsertRequestInternal(
			ctx, "INSERT INTO test VALUES (_)", anyPlan, noAntiMatch,
			sampleAll, noLatencyThreshold, noExpiration,
		)
		require.NoError(t, err)
		id2, err := registry.InsertRequestInternal(
			ctx, "SELECT x FROM test", anyPlan, noAntiMatch,
			sampleAll, noLatencyThreshold, noExpiration,
		)
		require.NoError(t, err)
		id3, err := registry.InsertRequestInternal(
			ctx, "SELECT x FROM test WHERE x > _", anyPlan, noAntiMatch,
			sampleAll, noLatencyThreshold, noExpiration,
		)
		require.NoError(t, err)

		// Run the queries in a different order.
		runner.Exec(t, "SELECT x FROM test")
		checkCompleted(id2)

		runner.Exec(t, "SELECT x FROM test WHERE x > 1")
		checkCompleted(id3)

		runner.Exec(t, "INSERT INTO test VALUES (2)")
		checkCompleted(id1)
	})

	// Verify that EXECUTE triggers diagnostics collection (#66048).
	t.Run("execute", func(t *testing.T) {
		id, err := registry.InsertRequestInternal(
			ctx, "SELECT x + _ FROM test", anyPlan, noAntiMatch,
			sampleAll, noLatencyThreshold, noExpiration,
		)
		require.NoError(t, err)
		runner.Exec(t, "PREPARE stmt AS SELECT x + $1 FROM test")
		runner.Exec(t, "EXECUTE stmt(1)")
		checkCompleted(id)
	})

	// Verify that if the traced query times out, the bundle is still saved.
	t.Run("timeout", func(t *testing.T) {
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			sampleAll, noLatencyThreshold, noExpiration,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Set the statement timeout.
		runner.Exec(t, "SET statement_timeout = '100ms';")

		// Run the query that times out.
		_, err = db.Exec("SELECT pg_sleep(999999)")
		require.True(t, strings.Contains(err.Error(), sqlerrors.QueryTimeoutError.Error()))

		// Reset the stmt timeout so that it doesn't affect the query in
		// checkCompleted.
		runner.Exec(t, "RESET statement_timeout;")
		checkCompleted(reqID)
	})

	// Verify that the bundle for a conditional request is only created when the
	// condition is satisfied.
	t.Run("conditional", func(t *testing.T) {
		minExecutionLatency := 100 * time.Millisecond
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			sampleAll, minExecutionLatency, noExpiration,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Run the fast query.
		runner.Exec(t, "SELECT pg_sleep(0)")
		checkNotCompleted(reqID)

		// Run the slow query.
		runner.Exec(t, "SELECT pg_sleep(0.2)")
		checkCompleted(reqID)
	})

	// Verify that if a conditional request expired, the bundle for it is not
	// created even if the condition is satisfied.
	t.Run("conditional expired", func(t *testing.T) {
		minExecutionLatency, expiresAfter := 100*time.Millisecond, time.Nanosecond
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			sampleAll, minExecutionLatency, expiresAfter,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		// Sleep for a bit and then run the slow query.
		time.Sleep(100 * time.Millisecond)
		runner.Exec(t, "SELECT pg_sleep(0.2)")
		// The request must have expired by now.
		checkNotCompleted(reqID)
	})

	// Verify that if we have an influx of queries matching the fingerprint of
	// a conditional diagnostics request and at least one instance satisfies the
	// conditional, then the bundle is collected.
	t.Run("conditional with concurrency", func(t *testing.T) {
		minExecutionLatency := 100 * time.Millisecond
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			sampleAll, minExecutionLatency, noExpiration,
		)
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
				runner.Exec(t, "SELECT pg_sleep($1)", sleepDuration)
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
						reqID, err := registry.InsertRequestInternal(
							ctx, fprint, anyPlan, noAntiMatch,
							sampleAll, minExecutionLatency, expiresAfter,
						)
						require.NoError(t, err)
						checkNotCompleted(reqID)

						err = registry.CancelRequest(ctx, reqID)
						require.NoError(t, err)
						checkNotCompleted(reqID)

						// Run the query that is slow enough to satisfy the
						// conditional request.
						runner.Exec(t, "SELECT pg_sleep(0.2)")
						checkNotCompleted(reqID)
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
						ctx, fprint, anyPlan, noAntiMatch,
						sampleAll, minExecutionLatency, noExpiration,
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
					runner.Exec(t, "SELECT pg_sleep(0.2)")

					wg.Wait()

					completed, diagnosticsID := isCompleted(reqID)
					if !completed {
						return errors.New("expected request to be completed")
					}
					require.True(t, diagnosticsID.Valid)
					return nil
				})
			})
		}
	})

	// Ask to trace a statement probabilistically.
	t.Run("probabilistic sample", func(t *testing.T) {
		samplingProbability, minExecutionLatency := 0.9999, time.Microsecond
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			samplingProbability, minExecutionLatency, noExpiration,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		testutils.SucceedsSoon(t, func() error {
			runner.Exec(t, "SELECT pg_sleep(0.01)") // run the query
			completed, _ := isCompleted(reqID)
			if completed {
				return nil
			}
			return errors.New("expected to capture diagnostic bundle")
		})
	})

	t.Run("sampling without latency threshold disallowed", func(t *testing.T) {
		samplingProbability, expiresAfter := 0.5, time.Second
		_, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			samplingProbability, noLatencyThreshold, expiresAfter,
		)
		testutils.IsError(err, "empty min exec latency")
	})

	t.Run("continuous capture disabled without sampling probability", func(t *testing.T) {
		// We validate that continuous captures is disabled when a sampling
		// probability of 0.0 is used. We know that it's disabled given the
		// diagnostic request is marked as completed despite us not getting to
		// the expiration point +1h from now (we don't mark continuous captures
		// as completed until they've expired).
		samplingProbability, minExecutionLatency, expiresAfter := 0.0, time.Microsecond, time.Hour
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			samplingProbability, minExecutionLatency, expiresAfter,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		setCollectUntilExpiration(true)
		defer setCollectUntilExpiration(false)

		testutils.SucceedsSoon(t, func() error {
			runner.Exec(t, "SELECT pg_sleep(0.01)") // run the query
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
		samplingProbability, minExecutionLatency := 0.999, time.Microsecond
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			samplingProbability, minExecutionLatency, noExpiration,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		setCollectUntilExpiration(true)
		defer setCollectUntilExpiration(false)

		testutils.SucceedsSoon(t, func() error {
			runner.Exec(t, "SELECT pg_sleep(0.01)") // run the query
			completed, _ := isCompleted(reqID)
			if completed {
				return nil
			}
			return errors.New("expected request to have been completed")
		})
	})

	t.Run("continuous capture", func(t *testing.T) {
		samplingProbability, minExecutionLatency, expiresAfter := 0.9999, time.Microsecond, time.Hour
		reqID, err := registry.InsertRequestInternal(
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			samplingProbability, minExecutionLatency, expiresAfter,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		setCollectUntilExpiration(true)
		defer setCollectUntilExpiration(false)

		var firstDiagnosticID int64
		testutils.SucceedsSoon(t, func() error {
			runner.Exec(t, "SELECT pg_sleep(0.01)") // run the query
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
			ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
			samplingProbability, minExecutionLatency, expiresAfter,
		)
		require.NoError(t, err)
		checkNotCompleted(reqID)

		setCollectUntilExpiration(true)
		defer setCollectUntilExpiration(false)

		// Sleep until expiration (and then some), and then run the query.
		time.Sleep(expiresAfter + 100*time.Millisecond)

		// Even though the request has expired, it hasn't been removed from the
		// registry yet (because we disabled the polling interval). When we run
		// the query that matches the fingerprint, the expired request is
		// removed, and the bundle is not collected.
		runner.Exec(t, "SELECT pg_sleep(0.01)") // run the query
		checkNotCompleted(reqID)

		// Sanity check that the request is no longer in the registry.
		require.False(t, registry.TestingFindRequest(reqID))
	})

	t.Run("plan-gist matching", func(t *testing.T) {
		// Set up two tables such that the same query fingerprint would get
		// different plans based on the placeholder values.
		runner.Exec(t, "CREATE TABLE small (k PRIMARY KEY) AS VALUES (1), (2);")
		runner.Exec(t, "ANALYZE small;")
		runner.Exec(t, "CREATE TABLE large (v INT, INDEX (v));")
		runner.Exec(t, "INSERT INTO large VALUES (1);")
		runner.Exec(t, "INSERT INTO large SELECT 2 FROM generate_series(1, 100);")
		runner.Exec(t, "ANALYZE large;")

		// query1 results in scan + lookup join whereas query2 does two scans +
		// merge join.
		const (
			fprint = `SELECT v FROM small INNER JOIN large ON (k = v) AND (k = _)`
			query1 = "SELECT v FROM small INNER JOIN large ON k = v AND k = 0;"
			query2 = "SELECT v FROM small INNER JOIN large ON k = v AND k = 1;"
		)
		getGist := func(query string) string {
			row := runner.QueryRow(t, "EXPLAIN (GIST) "+query)
			var gist string
			row.Scan(&gist)
			return gist
		}
		for _, antiMatch := range []bool{false, true} {
			t.Run(fmt.Sprintf("anti-match=%t", antiMatch), func(t *testing.T) {
				for _, tc := range []struct {
					target, other string
				}{
					{target: query1, other: query2},
					{target: query2, other: query1},
				} {
					targetGist, otherGist := getGist(tc.target), getGist(tc.other)
					// Sanity check that two queries have different plans.
					require.NotEqual(t, targetGist, otherGist)
					target, other := tc.target, tc.other
					if antiMatch {
						// Flip the queries when testing the anti-match.
						target, other = other, target
					}

					reqID, err := registry.InsertRequestInternal(
						ctx, fprint, targetGist, antiMatch, sampleAll, noLatencyThreshold, noExpiration,
					)
					require.NoError(t, err)
					checkNotCompleted(reqID)

					// Run other query several times and ensure that the bundle
					// wasn't collected because the plan gist didn't match.
					for i := 0; i < 3; i++ {
						runner.Exec(t, other)
					}
					checkNotCompleted(reqID)

					// Now run our target query and verify that the bundle is
					// now collected.
					runner.Exec(t, target)
					checkCompleted(reqID)
				}
			})
		}
	})
}

// Test that a different node can service a diagnostics request.
func TestDiagnosticsRequestDifferentNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := serverutils.StartCluster(t, 2, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	s0 := tc.ApplicationLayer(0)
	db0 := s0.SQLConn(t)
	db1 := tc.ApplicationLayer(1).SQLConn(t)
	_, err := db0.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	// Lower the polling interval to speed up the test.
	stmtdiagnostics.PollingInterval.Override(ctx, &s0.ClusterSettings().SV, time.Millisecond)
	require.NoError(t, err)

	var anyPlan string
	var noAntiMatch bool
	var sampleAll float64
	var noLatencyThreshold, noExpiration time.Duration

	// Ask to trace a particular query using node 0.
	registry := s0.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	reqID, err := registry.InsertRequestInternal(
		ctx, "INSERT INTO test VALUES (_)", anyPlan, noAntiMatch,
		sampleAll, noLatencyThreshold, noExpiration,
	)
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
	id1, err := registry.InsertRequestInternal(
		ctx, "INSERT INTO test VALUES (_)", anyPlan, noAntiMatch,
		sampleAll, noLatencyThreshold, noExpiration,
	)
	require.NoError(t, err)
	id2, err := registry.InsertRequestInternal(
		ctx, "SELECT x FROM test", anyPlan, noAntiMatch,
		sampleAll, noLatencyThreshold, noExpiration,
	)
	require.NoError(t, err)
	id3, err := registry.InsertRequestInternal(
		ctx, "SELECT x FROM test WHERE x > _", anyPlan, noAntiMatch,
		sampleAll, noLatencyThreshold, noExpiration,
	)
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
	// TODO(yuzefovich): it is suspicious that we're using the system codec, yet
	// the test passes with the test tenant. Investigate this.
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
	srv := serverutils.StartServerOnly(t, args)
	defer srv.Stopper().Stop(ctx)

	require.Equal(t, 1, waitForScans(1))
	time.Sleep(time.Millisecond) // ensure no unexpected scan occur
	require.Equal(t, 1, waitForScans(1))
	stmtdiagnostics.PollingInterval.Override(ctx, &settings.SV, 200*time.Microsecond)
	waitForScans(10) // ensure several scans occur
}
