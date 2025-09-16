// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stmtdiagnostics_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"strconv"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
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
		runner.Exec(t, "SET optimizer_min_row_count = 0;")

		// query1 results in scan + lookup join whereas query2 does two scans +
		// merge join (after adjusting optimizer_min_row_count).
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

func TestTxnRegistry_InsertTxnRequest_Polling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	// Set to 1s so that we can quickly pick up the request.
	stmtdiagnostics.TxnPollingInterval.Override(ctx, &settings.SV, time.Second)

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
		},
	})
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0).ApplicationLayer()
	registry := s0.ExecutorConfig().(sql.ExecutorConfig).TxnDiagnosticsRecorder
	registry2 := tc.Server(1).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).TxnDiagnosticsRecorder
	registry3 := tc.Server(2).ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).TxnDiagnosticsRecorder

	id, err := registry.InsertTxnRequestInternal(
		ctx,
		1111,
		[]uint64{1111, 2222, 3333},
		"testuser",
		0.5,
		time.Millisecond*100,
		0,
		false,
	)
	require.NoError(t, err)
	require.NotEqual(t, stmtdiagnostics.RequestID(0), id)

	var expectedRequest, req stmtdiagnostics.TxnRequest
	var ok bool
	expectedRequest, ok = registry.GetRequest(id)
	require.True(t, ok)
	testutils.SucceedsSoon(t, func() error {

		req, ok = registry2.GetRequest(id)
		if !ok {
			return errors.New("request not found on server 2")
		}
		require.Equal(t, expectedRequest, req)
		if !assert.Equal(t, expectedRequest, req) {
			return errors.Newf("request on server2 doesnt match expected request. Expected: %+v, got: %+v", expectedRequest, req)
		}

		req, ok = registry3.GetRequest(id)
		if !ok {
			return errors.New("request not found on server 3")
		}
		require.Equal(t, expectedRequest, req)
		if !assert.Equal(t, expectedRequest, req) {
			return errors.Newf("request on server3 doesnt match expected request. Expected: %+v, got: %+v", expectedRequest, req)
		}

		return nil
	})

	// mark the request as complete and ensure that it is removed from all 3 nodes
	runner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	runner.Exec(t, "UPDATE system.transaction_diagnostics_requests "+
		"SET completed = true, transaction_diagnostics_id = 12345 WHERE id = $1", id)
	testutils.SucceedsSoon(t, func() error {
		_, ok = registry.GetRequest(id)
		if ok {
			return errors.New("request still found on server 1")
		}
		_, ok = registry2.GetRequest(id)
		if ok {
			return errors.New("request still found on server 2")
		}
		_, ok = registry3.GetRequest(id)
		if ok {
			return errors.New("request still found on server 3")
		}
		return nil
	})
}

func TestTxnBundleCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	txnStatements := []string{"SELECT 'aaaaaa', 'bbbbbb'",
		"SELECT 'aaaaaa', 'bbbbbb', 'ccccc' UNION select 'ddddd', 'eeeee', 'fffff'"}

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	// Set to 1s so that we can quickly pick up the request.
	stmtdiagnostics.TxnPollingInterval.Override(ctx, &settings.SV, time.Second)

	knobs := sqlstats.CreateTestingKnobs()
	knobs.SynchronousSQLStats = true
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: knobs,
			},
		},
	})

	defer tc.Stopper().Stop(ctx)

	// SETUP
	sqlConn := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Execute to transaction so that it is in transaction_statistics
	executeTransactions(t, sqlConn, txnStatements)

	// Get the fingerprint id and statement fingerprint ids for the transaction
	// to make the txn diagnostics request
	row := sqlConn.QueryRow(t,
		`
SELECT 
  ts.fingerprint_id AS fingerprint_id,
  ts.metadata->'stmtFingerprintIDs' as fingerprint_ids
FROM crdb_internal.transaction_statistics ts
JOIN crdb_internal.statement_statistics ss on ss.transaction_fingerprint_id = ts.fingerprint_id
WHERE ss.metadata->>'query' LIKE 'SELECT _, _' 
LIMIT 1`)
	var fingerprintId []byte
	var fingerprintIdsBytes []byte

	row.Scan(&fingerprintId, &fingerprintIdsBytes)
	_, txnFingerprintId, err := encoding.DecodeUint64Ascending(fingerprintId)
	require.NoError(t, err)

	var statementFingerprintStrs []string

	err = json.Unmarshal(fingerprintIdsBytes, &statementFingerprintStrs)
	require.NoError(t, err)

	var statementFingerprintIds []uint64
	for _, s := range statementFingerprintStrs {
		value, err := strconv.ParseUint(s, 16, 64)
		require.NoError(t, err)
		statementFingerprintIds = append(statementFingerprintIds, value)
	}

	registry := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).TxnDiagnosticsRecorder

	t.Run("txn diagnostic request", func(t *testing.T) {
		// Insert a request for the transaction fingerprint
		reqId, err := registry.InsertTxnRequestInternal(
			ctx,
			txnFingerprintId,
			statementFingerprintIds,
			"testuser",
			0,
			0,
			0,
			false,
		)
		require.NoError(t, err)

		// Ensure that the request shows up in the system.transaction_diagnostics_requests table
		var count int
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.transaction_diagnostics_requests WHERE completed=false and id=$1", reqId).Scan(&count)
		require.Equal(t, 1, count)

		// Execute the transaction until the request is marked as complete.
		// We use a connection to a different server than the diagnostics request
		// was made on to ensure that the request is properly gossiped.
		runTxnUntilDiagnosticsCollected(t, tc, sqlutils.MakeSQLRunner(tc.ServerConn(1)), txnStatements, reqId)

	})

	t.Run("txn diagnostic with statement diagnostic request", func(t *testing.T) {

		stmtReqId, err := registry.StmtRegistry.InsertRequestInternal(
			ctx,
			"SELECT _, _",
			"", false, 0, 0, 0,
		)
		require.NoError(t, err)
		var stmtcount int
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.statement_diagnostics_requests WHERE completed=false and id=$1", stmtReqId).Scan(&stmtcount)
		require.Equal(t, 1, stmtcount)

		// execute the statement to ensure that the correct statement diagnostics
		// request is actually being created.
		sqlConn.Exec(t, txnStatements[0])
		// should be complete now
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.statement_diagnostics_requests WHERE completed=true and id=$1", stmtReqId).Scan(&stmtcount)
		require.Equal(t, 1, stmtcount)

		// Make a new request for the same statement
		stmtReqId, err = registry.StmtRegistry.InsertRequestInternal(
			ctx,
			"SELECT _, _",
			"", false, 0, 0, 0,
		)
		require.NoError(t, err)
		// make sure the statement diagnostics request is also marked as
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.statement_diagnostics_requests WHERE completed=false and id=$1", stmtReqId).Scan(&stmtcount)
		require.Equal(t, 1, stmtcount)

		// Insert a request for the transaction fingerprint
		reqId, err := registry.InsertTxnRequestInternal(
			ctx,
			txnFingerprintId,
			statementFingerprintIds,
			"testuser",
			0,
			0,
			0,
			false,
		)
		require.NoError(t, err)

		// Ensure that the request shows up in the system.transaction_diagnostics_requests table
		var count int
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.transaction_diagnostics_requests WHERE completed=false and id=$1", reqId).Scan(&count)
		require.Equal(t, 1, count)
		runTxnUntilDiagnosticsCollected(t, tc, sqlConn, txnStatements, reqId)
		// Ensure there are no more active transaction diagnostics requests
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.transaction_diagnostics_requests WHERE completed=false and id=$1", reqId).Scan(&count)
		require.Equal(t, 0, count)
		// make sure the statement diagnostics request is still incomplete
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.statement_diagnostics_requests WHERE completed=false and id=$1", stmtReqId).Scan(&stmtcount)
		require.Equal(t, 1, stmtcount)

		executeTransactions(t, sqlConn, txnStatements)
		// should be complete now that there is no transaction diagnostics request
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.statement_diagnostics_requests WHERE completed=true and id=$1", stmtReqId).Scan(&stmtcount)
		require.Equal(t, 1, stmtcount)
	})

	t.Run("multiple txn executions", func(t *testing.T) {
		// Insert a request for the transaction fingerprint
		reqId, err := registry.InsertTxnRequestInternal(
			ctx,
			txnFingerprintId,
			statementFingerprintIds,
			"testuser",
			1,
			1,
			0,
			false,
		)
		require.NoError(t, err)

		// Ensure that the request shows up in the system.transaction_diagnostics_requests table
		var count int
		sqlConn.QueryRow(t, "SELECT count(*) FROM system.transaction_diagnostics_requests WHERE completed=false and id=$1", reqId).Scan(&count)
		require.Equal(t, 1, count)

		conn2 := sqlutils.MakeSQLRunner(tc.ServerConn(1))
		conn3 := sqlutils.MakeSQLRunner(tc.ServerConn(2))

		// Wait for the request to be in all node registries
		testutils.SucceedsSoon(t, func() error {
			for i := range tc.NumServers() {
				innerRegistry := tc.Server(i).ExecutorConfig().(sql.ExecutorConfig).TxnDiagnosticsRecorder
				_, ok := innerRegistry.GetRequest(reqId)
				if !ok {
					return errors.New("expected request to be in registry")
				}
			}
			return nil
		})

		conn2.Exec(t, "BEGIN")
		conn3.Exec(t, "BEGIN")

		conn2.Exec(t, txnStatements[0])
		conn2.Exec(t, txnStatements[1])

		conn3.Exec(t, txnStatements[0])
		conn3.Exec(t, txnStatements[1])
		conn3.Exec(t, "COMMIT")
		conn2.Exec(t, "COMMIT")

		sqlConn.QueryRow(t, "SELECT count(*) FROM system.transaction_diagnostics_requests WHERE completed=true and id=$1", reqId).Scan(&count)
		require.Equal(t, 1, count)

		sqlConn.QueryRow(t, ""+
			"SELECT count(*) FROM system.statement_diagnostics sd "+
			"JOIN system.transaction_diagnostics_requests tdr on tdr.transaction_diagnostics_id = sd.transaction_diagnostics_id "+
			"WHERE tdr.id=$1", reqId).Scan(&count)

		require.Equal(t, 3, count)
	})
}

func TestRequestTxnBundleBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	knobs := sqlstats.CreateTestingKnobs()
	knobs.SynchronousSQLStats = true

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{Knobs: base.TestingKnobs{
		SQLStatsKnobs: knobs,
	}})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(srv.SQLConn(t))

	txnStatements := []string{"SELECT 'aaaaaa', 'bbbbbb'",
		"SELECT 'aaaaaa', 'bbbbbb', 'ccccc' UNION select 'ddddd', 'eeeee', 'fffff'"}
	executeTransactions(t, runner, txnStatements)

	// Get the fingerprint id and statement fingerprint ids for the transaction
	// to make the txn diagnostics request
	row := runner.QueryRow(t,
		`
SELECT 
  encode(ts.fingerprint_id, 'hex') AS fingerprint_id,
  ts.metadata->'stmtFingerprintIDs' as fingerprint_ids
FROM crdb_internal.transaction_statistics ts
JOIN crdb_internal.statement_statistics ss on ss.transaction_fingerprint_id = ts.fingerprint_id
WHERE ss.metadata->>'query' LIKE 'SELECT _, _' 
LIMIT 1`)
	var expectedTxnFpId string
	var fingerprintIdsBytes []byte

	row.Scan(&expectedTxnFpId, &fingerprintIdsBytes)
	var statementFingerprintIds []uint64
	var statementFingerprintStrs []string
	err := json.Unmarshal(fingerprintIdsBytes, &statementFingerprintStrs)
	require.NoError(t, err)

	for _, s := range statementFingerprintStrs {
		value, err := strconv.ParseUint(s, 16, 64)
		require.NoError(t, err)
		statementFingerprintIds = append(statementFingerprintIds, value)
	}

	runner.Exec(t, "CREATE USER alloweduser")
	runner.Exec(t, "GRANT SYSTEM VIEWACTIVITY TO alloweduser")

	runner.Exec(t, "CREATE USER alloweduserredacted")
	runner.Exec(t, "GRANT SYSTEM VIEWACTIVITYREDACTED TO alloweduserredacted")

	runner.Exec(t, "CREATE USER notalloweduser")

	for _, tc := range []struct {
		name                        string
		expectedMinExecutionLatency string
		expectedExpiresAt           time.Duration
		expectedSampleProbability   float64
		expectedRedacted            bool
		expectedError               string
		expectedUser                string
	}{
		{
			name:                        "conditional",
			expectedMinExecutionLatency: "00:00:00.1",
			expectedSampleProbability:   0.5,
			expectedExpiresAt:           0,
			expectedRedacted:            false,
		},
		{
			name:                        "conditional_no_min_latency",
			expectedSampleProbability:   0.5,
			expectedMinExecutionLatency: "0",
			expectedExpiresAt:           0,
			expectedRedacted:            false,
			expectedError:               "got non-zero sampling probability",
		},
		{
			name:                        "conditional_probability_out_of_bounds",
			expectedSampleProbability:   1.5,
			expectedMinExecutionLatency: "0",
			expectedExpiresAt:           0,
			expectedRedacted:            false,
			expectedError:               "expected sampling probability in range",
		},
		{
			name:                        "not_conditional",
			expectedMinExecutionLatency: "0",
			expectedExpiresAt:           0,
			expectedSampleProbability:   0,
			expectedRedacted:            false,
		},
		{
			name:                        "with_expiration",
			expectedExpiresAt:           time.Hour,
			expectedMinExecutionLatency: "0",
			expectedSampleProbability:   0,
			expectedRedacted:            false,
		},
		{
			name:                        "redacted",
			expectedMinExecutionLatency: "0",
			expectedExpiresAt:           0,
			expectedSampleProbability:   0,
			expectedRedacted:            true,
		},
		{
			name:                        "alloweduser",
			expectedMinExecutionLatency: "0",
			expectedSampleProbability:   0,
			expectedExpiresAt:           0,
			expectedRedacted:            false,
			expectedUser:                "alloweduser",
		},
		{
			name:                        "alloweduserredacted",
			expectedMinExecutionLatency: "0",
			expectedSampleProbability:   0,
			expectedExpiresAt:           0,
			expectedRedacted:            true,
			expectedUser:                "alloweduserredacted",
		},
		{
			name:                        "alloweduserredacted must be redacted",
			expectedMinExecutionLatency: "0",
			expectedSampleProbability:   0,
			expectedExpiresAt:           0,
			expectedRedacted:            false,
			expectedError:               "users with VIEWACTIVITYREDACTED privilege can only request redacted statement bundles",
			expectedUser:                "alloweduserredacted",
		},
		{
			name:                        "notalloweduser",
			expectedMinExecutionLatency: "0",
			expectedSampleProbability:   0,
			expectedExpiresAt:           0,
			expectedRedacted:            false,
			expectedError:               "requesting statement bundle requires VIEWACTIVITY privilege",
			expectedUser:                "notalloweduser",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				runner.Exec(t, "RESET ROLE")
				runner.Exec(t, "DELETE FROM system.transaction_diagnostics_requests")
			}()

			if tc.expectedUser != "" {
				runner.Exec(t, "SET ROLE "+tc.expectedUser)
			} else {
				tc.expectedUser = "root"
			}

			if tc.expectedError != "" {
				runner.ExpectErr(t, tc.expectedError, "SELECT crdb_internal.request_transaction_bundle($1, $2, $3, $4, $5)",
					expectedTxnFpId,
					tc.expectedSampleProbability,
					tc.expectedMinExecutionLatency,
					tc.expectedExpiresAt,
					tc.expectedRedacted)
			} else {
				runner.Exec(t, "SELECT crdb_internal.request_transaction_bundle($1, $2, $3::INTERVAL, $4, $5)",
					expectedTxnFpId,
					tc.expectedSampleProbability,
					tc.expectedMinExecutionLatency,
					tc.expectedExpiresAt,
					tc.expectedRedacted,
				)

				var (
					txnFpId             string
					stmtFingerprintIds  [][]byte
					minExecutionLatency gosql.NullString
					expiresAt           gosql.NullTime
					sampleProbability   *float64
					redacted            bool
					username            string
				)

				runner.Exec(t, "RESET ROLE")
				runner.QueryRow(t, "SELECT "+
					"encode(transaction_fingerprint_id, 'hex') as txn_fingerprint_id, "+
					"statement_fingerprint_ids, "+
					"min_execution_latency, "+
					"expires_at, "+
					"sampling_probability, "+
					"redacted, "+
					"username "+
					"FROM system.transaction_diagnostics_requests").Scan(&txnFpId, pq.Array(&stmtFingerprintIds), &minExecutionLatency, &expiresAt, &sampleProbability, &redacted, &username)
				var expectedSampleProbability *float64
				if tc.expectedSampleProbability != 0 {
					expectedSampleProbability = &tc.expectedSampleProbability
				}

				if tc.expectedMinExecutionLatency != "0" {
					require.True(t, minExecutionLatency.Valid)
					require.Equal(t, tc.expectedMinExecutionLatency, minExecutionLatency.String)
				}

				if tc.expectedExpiresAt != 0 {
					require.True(t, expiresAt.Valid, "expiresAt should not be NULL when expectedExpiresAt is set")
					require.Greaterf(t, expiresAt.Time, timeutil.Now().Add(-time.Minute).Add(tc.expectedExpiresAt),
						"expected expiresAt to be roughly %s from now, got %s", tc.expectedExpiresAt, expiresAt.Time)
				}

				require.Equal(t, expectedTxnFpId, txnFpId)
				require.Equal(t, statementFingerprintIds, stmtdiagnostics.ToUint64Slice(t, stmtFingerprintIds))
				require.Equal(t, expectedSampleProbability, sampleProbability)
				require.Equal(t, tc.expectedRedacted, redacted)
				require.Equal(t, tc.expectedUser, username)
			}
		})
	}

	t.Run("non-existent transaction fingerprint id", func(t *testing.T) {
		var found bool
		runner.QueryRow(t, "SELECT crdb_internal.request_transaction_bundle($1, $2, $3, $4, $5)",
			"ffffffffffffffff",
			0,
			"0",
			0,
			false).Scan(&found)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("non-hex encoded transaction fingerprint id", func(t *testing.T) {
		runner.ExpectErr(t, "invalid transaction fingerprint id", "SELECT crdb_internal.request_transaction_bundle($1, $2, $3, $4, $5)",
			"zzzz",
			0,
			"0",
			0,
			false)
	})
}

func executeTransactions(t *testing.T, runner *sqlutils.SQLRunner, statements []string) {
	t.Helper()
	runner.Exec(t, "BEGIN")
	for _, stmt := range statements {
		runner.Exec(t, stmt)
	}
	runner.Exec(t, "COMMIT")
}

func runTxnUntilDiagnosticsCollected(
	t *testing.T,
	tc serverutils.TestClusterInterface,
	sqlConn *sqlutils.SQLRunner,
	statements []string,
	reqId stmtdiagnostics.RequestID,
) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		executeTransactions(t, sqlConn, statements)
		r := tc.ServerConn(0).QueryRow("SELECT count(*) FROM system.transaction_diagnostics_requests WHERE completed=true and id=$1", reqId)
		if r.Err() != nil {
			return r.Err()
		}
		var count int
		err := r.Scan(&count)
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.Newf("expected 1 completed bundle, got %d", count)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		for i := range tc.NumServers() {
			registry := tc.Server(i).ExecutorConfig().(sql.ExecutorConfig).TxnDiagnosticsRecorder
			_, ok := registry.GetRequest(reqId)
			if ok {
				return errors.Newf("expected request to be removed from registry. Still exits in registry %d", i)
			}
		}
		return nil
	})
}
