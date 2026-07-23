// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stmtdiagnostics_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestIsConditionSatisfied tests the core logic for latency range filtering.
// This is a unit test - no database required, just pure logic testing.
func TestIsConditionSatisfied(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name        string
		minLatency  time.Duration
		maxLatency  time.Duration
		execLatency time.Duration
		satisfied   bool
		description string
	}{
		{
			name:        "below min threshold",
			minLatency:  100 * time.Millisecond,
			maxLatency:  300 * time.Millisecond,
			execLatency: 50 * time.Millisecond,
			satisfied:   false,
			description: "execution too fast, below minimum",
		},
		{
			name:        "within range",
			minLatency:  100 * time.Millisecond,
			maxLatency:  300 * time.Millisecond,
			execLatency: 200 * time.Millisecond,
			satisfied:   true,
			description: "execution latency in [min, max) range",
		},
		{
			name:        "exceeds max threshold",
			minLatency:  100 * time.Millisecond,
			maxLatency:  300 * time.Millisecond,
			execLatency: 500 * time.Millisecond,
			satisfied:   false,
			description: "execution too slow, exceeds maximum",
		},
		{
			name:        "equals min (inclusive lower bound)",
			minLatency:  100 * time.Millisecond,
			maxLatency:  300 * time.Millisecond,
			execLatency: 100 * time.Millisecond,
			satisfied:   true,
			description: "lower bound is inclusive: min <= latency",
		},
		{
			name:        "equals max (exclusive upper bound)",
			minLatency:  100 * time.Millisecond,
			maxLatency:  300 * time.Millisecond,
			execLatency: 300 * time.Millisecond,
			satisfied:   false,
			description: "upper bound is exclusive: latency < max",
		},
		{
			name:        "just below max",
			minLatency:  100 * time.Millisecond,
			maxLatency:  300 * time.Millisecond,
			execLatency: 299 * time.Millisecond,
			satisfied:   true,
			description: "1ms below max should satisfy condition",
		},
		{
			name:        "zero max (no upper bound)",
			minLatency:  100 * time.Millisecond,
			maxLatency:  0, // zero means no max
			execLatency: 999 * time.Second,
			satisfied:   true,
			description: "zero max means unlimited upper bound",
		},
		{
			name:        "zero max below min",
			minLatency:  100 * time.Millisecond,
			maxLatency:  0,
			execLatency: 50 * time.Millisecond,
			satisfied:   false,
			description: "zero max, but still below min threshold",
		},
		{
			name:        "both zero (unconditional)",
			minLatency:  0,
			maxLatency:  0,
			execLatency: 123 * time.Millisecond,
			satisfied:   true,
			description: "no latency constraints, always satisfied",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a minimal registry just for testing IsConditionSatisfied
			registry := stmtdiagnostics.NewRegistry(nil, nil)

			req := stmtdiagnostics.MakeRequest(tc.minLatency, tc.maxLatency)

			result := registry.IsConditionSatisfied(req, tc.execLatency)
			require.Equal(t, tc.satisfied, result,
				"Test: %s\nDescription: %s\nmin=%v, max=%v, exec=%v\nExpected: %v, Got: %v",
				tc.name, tc.description, tc.minLatency, tc.maxLatency, tc.execLatency, tc.satisfied, result)
		})
	}
}

// TestMaxExecutionLatencyBasic tests end-to-end bundle capture with max latency thresholds.
func TestMaxExecutionLatencyBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder

	const (
		anyPlan      = ""
		noAntiMatch  = false
		sampleAll    = 0.0
		noExpiration = 0 * time.Second
	)

	// Use wide timing margins to avoid flakes under the race detector, which
	// adds 10-20x overhead to execution. For "below threshold" tests, use very
	// short sleeps with high thresholds. For "within range" tests, use long
	// sleeps with wide ranges.
	tests := []struct {
		name          string
		fingerprint   string
		query         string
		minLatency    time.Duration
		maxLatency    time.Duration
		shouldCapture bool
		description   string
	}{
		{
			name:          "latency below min",
			fingerprint:   "SELECT pg_sleep(_)",
			query:         "SELECT pg_sleep(0.01)",
			minLatency:    5 * time.Second,
			maxLatency:    10 * time.Second,
			shouldCapture: false,
			description:   "query too fast, below minimum threshold",
		},
		{
			name:          "latency within bounds",
			fingerprint:   "SELECT pg_sleep(_), _",
			query:         "SELECT pg_sleep(0.5), 1",
			minLatency:    100 * time.Millisecond,
			maxLatency:    10 * time.Second,
			shouldCapture: true,
			description:   "query latency in target range [min, max)",
		},
		{
			name:          "latency exceeds max",
			fingerprint:   "SELECT pg_sleep(_), _, _",
			query:         "SELECT pg_sleep(0.5), 1, 2",
			minLatency:    100 * time.Millisecond,
			maxLatency:    200 * time.Millisecond,
			shouldCapture: false,
			description:   "query too slow, exceeds maximum threshold",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reqID, err := registry.InsertRequestInternal(
				ctx, tc.fingerprint, anyPlan, noAntiMatch,
				sampleAll, tc.minLatency, tc.maxLatency, noExpiration,
			)
			require.NoError(t, err)

			// Execute query with target latency
			runner.Exec(t, tc.query)

			// Check if request was completed and bundle was collected
			var completed bool
			var diagID *int64
			err = db.QueryRow(`
				SELECT completed, statement_diagnostics_id
				FROM system.statement_diagnostics_requests
				WHERE id = $1
			`, reqID).Scan(&completed, &diagID)
			require.NoError(t, err)

			if tc.shouldCapture {
				require.True(t, completed, tc.description)
				require.NotNil(t, diagID, "bundle should exist for %s", tc.description)
			} else {
				require.False(t, completed, tc.description)
			}
		})
	}
}

// TestMaxExecutionLatencyBackwardCompatibility ensures existing behavior is unchanged
// when max threshold is not specified (zero value).
func TestMaxExecutionLatencyBackwardCompatibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder

	const (
		anyPlan      = ""
		noAntiMatch  = false
		sampleAll    = 0.0
		noExpiration = 0 * time.Second
	)

	tests := []struct {
		name          string
		fingerprint   string
		query         string
		minLatency    time.Duration
		maxLatency    time.Duration // always 0 for backward compat
		shouldCapture bool
		description   string
	}{
		{
			name:          "no thresholds (unconditional)",
			fingerprint:   "SELECT pg_sleep(_)",
			query:         "SELECT pg_sleep(0.05)",
			minLatency:    0,
			maxLatency:    0,
			shouldCapture: true,
			description:   "zero thresholds = capture all (existing behavior)",
		},
		{
			name:          "min only - below threshold",
			fingerprint:   "SELECT _, pg_sleep(_)",
			query:         "SELECT 1, pg_sleep(0.01)",
			minLatency:    5 * time.Second,
			maxLatency:    0,
			shouldCapture: false,
			description:   "below min, no max (existing behavior)",
		},
		{
			name:          "min only - above threshold",
			fingerprint:   "SELECT pg_sleep(_), _",
			query:         "SELECT pg_sleep(0.5), 1",
			minLatency:    100 * time.Millisecond,
			maxLatency:    0,
			shouldCapture: true,
			description:   "above min, no max (existing behavior)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reqID, err := registry.InsertRequestInternal(
				ctx, tc.fingerprint, anyPlan, noAntiMatch,
				sampleAll, tc.minLatency, tc.maxLatency, noExpiration,
			)
			require.NoError(t, err)

			runner.Exec(t, tc.query)

			var completed bool
			var diagID *int64
			err = db.QueryRow(`
				SELECT completed, statement_diagnostics_id
				FROM system.statement_diagnostics_requests
				WHERE id = $1
			`, reqID).Scan(&completed, &diagID)
			require.NoError(t, err)

			if tc.shouldCapture {
				require.True(t, completed, tc.description)
				require.NotNil(t, diagID, "bundle should exist for %s", tc.description)
			} else {
				require.False(t, completed, tc.description)
			}
		})
	}
}

// TestMaxExecutionLatencyWithSampling tests that max latency filtering works
// correctly with continuous sampling. With sampling enabled, the request stays
// active across multiple executions, so we verify that bundles are only captured
// when execution latency falls within [min, max).
func TestMaxExecutionLatencyWithSampling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder

	// Enable continuous collection so the request stays active after capture.
	runner.Exec(t, "SET CLUSTER SETTING sql.stmt_diagnostics.collect_continuously.enabled = true")

	const (
		anyPlan     = ""
		noAntiMatch = false
	)

	// Use sampling probability of 1.0 (deterministic) with an expiration so
	// that continueCollecting returns true. Wide latency range avoids flakes
	// under -race.
	reqID, err := registry.InsertRequestInternal(
		ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
		1.0 /* samplingProbability */, 100*time.Millisecond, 10*time.Second,
		time.Hour, /* expiresAfter */
	)
	require.NoError(t, err)

	bundleCount := func() int {
		var count int
		err := db.QueryRow(`
			SELECT count(*)
			FROM system.statement_diagnostics
			WHERE statement_fingerprint = 'SELECT pg_sleep(_)'
		`).Scan(&count)
		require.NoError(t, err)
		return count
	}

	// Execute a query within range — should capture a bundle.
	runner.Exec(t, "SELECT pg_sleep(0.5)")
	require.Equal(t, 1, bundleCount(), "should capture bundle when latency in range")

	// Request should still be active (not completed) because sampling is enabled.
	var completed bool
	err = db.QueryRow(`
		SELECT completed FROM system.statement_diagnostics_requests WHERE id = $1
	`, reqID).Scan(&completed)
	require.NoError(t, err)
	require.False(t, completed, "request should remain active with continuous sampling")

	// Execute a query below min — should NOT capture.
	runner.Exec(t, "SELECT pg_sleep(0.001)")
	require.Equal(t, 1, bundleCount(), "should not capture bundle when latency below min")

	// Execute another query within range — should capture a second bundle.
	runner.Exec(t, "SELECT pg_sleep(0.5)")
	require.Equal(t, 2, bundleCount(), "should capture second bundle when latency in range")

	require.NoError(t, registry.CancelRequest(ctx, reqID))
}

// TestMaxExecutionLatencyMultipleRequests tests that multiple concurrent requests
// with different max thresholds work independently.
func TestMaxExecutionLatencyMultipleRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder

	const (
		anyPlan      = ""
		noAntiMatch  = false
		sampleAll    = 0.0
		noExpiration = 0 * time.Second
	)

	// Use wide latency ranges to avoid flakes under the race detector.
	// Each request uses a different fingerprint and a wide [min, max) range
	// so that execution overhead doesn't push latencies across boundaries.

	// Request 1: capture queries >100ms for "SELECT pg_sleep(_)"
	reqID1, err := registry.InsertRequestInternal(
		ctx, "SELECT pg_sleep(_)", anyPlan, noAntiMatch,
		sampleAll, 100*time.Millisecond, 0 /* no upper bound */, noExpiration,
	)
	require.NoError(t, err)

	// Request 2: capture queries in [100ms, 10s) for "SELECT pg_sleep(_), _"
	reqID2, err := registry.InsertRequestInternal(
		ctx, "SELECT pg_sleep(_), _", anyPlan, noAntiMatch,
		sampleAll, 100*time.Millisecond, 10*time.Second, noExpiration,
	)
	require.NoError(t, err)

	// Request 3: capture queries <200ms for "SELECT pg_sleep(_), _, _"
	// (use high min so fast query won't match)
	reqID3, err := registry.InsertRequestInternal(
		ctx, "SELECT pg_sleep(_), _, _", anyPlan, noAntiMatch,
		sampleAll, 5*time.Second, 10*time.Second, noExpiration,
	)
	require.NoError(t, err)

	// Execute queries: 500ms sleep is well above 100ms even under -race.
	runner.Exec(t, "SELECT pg_sleep(0.5)")        // should match req1 [100ms, ∞)
	runner.Exec(t, "SELECT pg_sleep(0.5), 1")     // should match req2 [100ms, 10s)
	runner.Exec(t, "SELECT pg_sleep(0.01), 1, 2") // 10ms, should NOT match req3 [5s, 10s)

	// Check which requests completed
	checkCompleted := func(reqID int64, expectedCompleted bool, desc string) {
		var completed bool
		err := db.QueryRow(`
			SELECT completed FROM system.statement_diagnostics_requests WHERE id = $1
		`, reqID).Scan(&completed)
		require.NoError(t, err)
		require.Equal(t, expectedCompleted, completed, desc)
	}

	checkCompleted(reqID1, true, "request 1 should have captured 500ms query [100ms, ∞)")
	checkCompleted(reqID2, true, "request 2 should have captured 500ms query [100ms, 10s)")
	checkCompleted(reqID3, false, "request 3 should NOT have captured 10ms query [5s, 10s)")
}

// TestMaxExecutionLatencyValidation tests error conditions and invalid configurations.
func TestMaxExecutionLatencyValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder

	const (
		anyPlan      = ""
		noAntiMatch  = false
		sampleAll    = 0.0
		noExpiration = 0 * time.Second
	)

	tests := []struct {
		name        string
		minLatency  time.Duration
		maxLatency  time.Duration
		expectError bool
		description string
	}{
		{
			name:        "valid range",
			minLatency:  100 * time.Millisecond,
			maxLatency:  200 * time.Millisecond,
			expectError: false,
			description: "max > min is valid",
		},
		{
			name:        "max equals min",
			minLatency:  100 * time.Millisecond,
			maxLatency:  100 * time.Millisecond,
			expectError: true,
			description: "max == min creates empty range, rejected",
		},
		{
			name:        "max less than min",
			minLatency:  200 * time.Millisecond,
			maxLatency:  100 * time.Millisecond,
			expectError: true,
			description: "max < min is invalid (impossible range)",
		},
		{
			name:        "zero max with positive min",
			minLatency:  100 * time.Millisecond,
			maxLatency:  0,
			expectError: false,
			description: "max=0 means no upper bound (valid)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := registry.InsertRequestInternal(
				ctx, fmt.Sprintf("SELECT %s", tc.name), anyPlan, noAntiMatch,
				sampleAll, tc.minLatency, tc.maxLatency, noExpiration,
			)

			if tc.expectError {
				require.Error(t, err, tc.description)
			} else {
				require.NoError(t, err, tc.description)
			}
		})
	}
}
