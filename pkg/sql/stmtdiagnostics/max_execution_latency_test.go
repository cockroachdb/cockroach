// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stmtdiagnostics_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

// TestMaxExecutionLatency verifies that the max_execution_latency parameter
// on statement bundle requests correctly filters bundles by execution time.
// The condition for collection is:
//
//	min <= execLatency AND (max == 0 OR execLatency <= max)
func TestMaxExecutionLatency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder
	runner := sqlutils.MakeSQLRunner(db)

	// Disable polling so that requests inserted via InsertRequestInternal
	// are immediately visible to the execution engine.
	stmtdiagnostics.PollingInterval.Override(
		ctx, &s.ClusterSettings().SV, 0,
	)

	var curReqID int64

	datadriven.RunTest(t, "testdata/max_execution_latency",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "request":
				return runRequestCmd(ctx, t, d, registry, &curReqID)
			case "exec":
				return runExecCmd(t, d, runner, curReqID)
			case "reset":
				return runResetCmd(ctx, t, runner, registry, &curReqID)
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		},
	)
}

// runRequestCmd handles the "request" directive, creating a new diagnostics
// request with the specified fingerprint, min, and max execution latencies.
func runRequestCmd(
	ctx context.Context,
	t *testing.T,
	d *datadriven.TestData,
	registry *stmtdiagnostics.Registry,
	curReqID *int64,
) string {
	fingerprint := strings.TrimSpace(d.Input)
	var minLatency, maxLatency time.Duration

	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "min":
			dur, err := time.ParseDuration(arg.Vals[0])
			if err != nil {
				t.Fatalf("parsing min: %v", err)
			}
			minLatency = dur
		case "max":
			dur, err := time.ParseDuration(arg.Vals[0])
			if err != nil {
				t.Fatalf("parsing max: %v", err)
			}
			maxLatency = dur
		}
	}

	reqID, err := registry.InsertRequestInternal(
		ctx, fingerprint,
		"",    // planGist
		false, // antiPlanGist
		0,     // samplingProbability
		minLatency,
		maxLatency,
		0, // expiresAfter
	)
	if err != nil {
		return fmt.Sprintf("error: %s", err.Error())
	}
	*curReqID = reqID
	return "ok"
}

// runExecCmd handles the "exec" directive, executing the SQL in d.Input
// and reporting whether the current request was completed.
func runExecCmd(
	t *testing.T, d *datadriven.TestData, runner *sqlutils.SQLRunner, reqID int64,
) string {
	stmt := strings.TrimSpace(d.Input)
	runner.Exec(t, stmt)

	var completed bool
	runner.QueryRow(t,
		"SELECT completed FROM system.statement_diagnostics_requests WHERE id = $1",
		reqID,
	).Scan(&completed)

	return fmt.Sprintf("completed: %t", completed)
}

// runResetCmd handles the "reset" directive, cleaning up all pending
// requests so the next test case starts fresh. It also forces the
// registry to re-poll so the in-memory state reflects the cleanup.
func runResetCmd(
	ctx context.Context,
	t *testing.T,
	runner *sqlutils.SQLRunner,
	registry *stmtdiagnostics.Registry,
	curReqID *int64,
) string {
	runner.Exec(t, "DELETE FROM system.statement_diagnostics_requests WHERE true")
	runner.Exec(t, "DELETE FROM system.statement_diagnostics WHERE true")
	if err := registry.TestingPollRequests(ctx); err != nil {
		t.Fatalf("polling requests: %v", err)
	}
	*curReqID = 0
	return "ok"
}
