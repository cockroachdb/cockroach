// Copyright 2024 The Cockroach Authors.
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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

/**
 * TestTelemetryLoggingDataDriven runs the data-driven tests in telemetryLogging.
 * In these tests, sql is either executed in the unobserved connection or the observed connection.
 * The observed connection is one for which we have set the application_name to a specific value that
 * a log spy is filtering on. The log spy will only capture logs that contain the application_name.
 * This allows us to reduce noise in the output by executing sql for which we don't care about in the
 * unobserved connection.
 *
 * It has the following directives:
 *   - exec-sql: executes SQL statements in the unobserved connection.
 *   - spy-sql: executes SQL statements in the observed connection. Returns any new stmt logs generated.
 *              Takes in the following arguments:
 *                - unixSecs: sets the current time used for telemetry log sampling to the given unix time in seconds.
 *                            If omitted, the current time is automatically changed by 0.1 seconds.
 *                - restartUnixSecs: sets the stub time on txn restarts.
 *                - tracing: sets the tracing status to the given value
 *   - reset-last-sampled: resets the last sampled time.
 */
func TestTelemetryLoggingDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Some queries may be retried under stress.
	skip.UnderStressRace(t, "results inconsistent under stress")

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	appName := "telemetry-logging-datadriven"
	ctx := context.Background()
	stmtSpy := logtestutils.NewSampledQueryLogScrubVolatileFields(t)
	stmtSpy.AddFilter(func(ev logpb.Entry) bool {
		return strings.Contains(ev.Message, appName)
	})
	cleanup := log.InterceptWith(ctx, stmtSpy)
	defer cleanup()

	txnsSpy := logtestutils.NewSampledTransactionLogScrubVolatileFields(t)
	txnsSpy.AddFilter(func(ev logpb.Entry) bool {
		return strings.Contains(ev.Message, appName)
	})
	cleanupTxnSpy := log.InterceptWith(ctx, txnsSpy)
	defer cleanupTxnSpy()

	st := logtestutils.StubTime{}
	st.SetTime(timeutil.FromUnixMicros(0))
	sts := logtestutils.StubTracingStatus{}
	stubTimeOnRestart := int64(0)
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &ExecutorTestingKnobs{
					BeforeRestart: func(_ context.Context, _ error) {
						st.SetTime(timeutil.FromUnixMicros(stubTimeOnRestart * 1e6))
					},
				},
				TelemetryLoggingKnobs: &TelemetryLoggingTestingKnobs{
					getTimeNow:       st.TimeNow,
					getTracingStatus: sts.TracingStatus,
				},
			},
		},
	})
	defer tc.Stopper().Stop(context.Background())
	s := tc.ApplicationLayer(0)

	telemetryLogging := s.SQLServer().(*Server).TelemetryLoggingMetrics
	setupConn := s.SQLConn(t)
	_, err := setupConn.Exec("CREATE USER testuser")
	require.NoError(t, err)

	spiedConnRootUser := s.SQLConn(t)
	spiedConnTestUser := s.SQLConn(t, serverutils.User("testuser"))
	spiedConn := spiedConnRootUser

	// Set spied connections to the app name observed by the log spy.
	_, err = spiedConn.Exec("SET application_name = $1", appName)
	require.NoError(t, err)
	_, err = spiedConnTestUser.Exec("SET application_name = $1", appName)
	require.NoError(t, err)

	datadriven.Walk(t, datapathutils.TestDataPath(t, "telemetryLogging"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			spiedConn = spiedConnRootUser
			switch d.Cmd {
			case "exec-sql":
				sts.SetTracingStatus(false)
				_, err := setupConn.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
			case "spy-sql":
				stmtLogCount := stmtSpy.Count()
				txnLogCount := txnsSpy.Count()
				var stubTimeUnixSecs float64
				var tracing bool

				d.MaybeScanArgs(t, "tracing", &tracing)
				sts.SetTracingStatus(tracing)

				// Set stubbed stubbed time if this txn is restarted.
				scanned := d.MaybeScanArgs(t, "restartUnixSecs", &stubTimeOnRestart)
				if !scanned {
					// If we didn't scan a restart time, we should reset it to 0.
					stubTimeOnRestart = 0
				}

				// Set stubbed sampling time for telemetry logging. note this doesn't effect
				// the query execution time.
				scanned = d.MaybeScanArgs(t, "unixSecs", &stubTimeUnixSecs)
				stubTimeMicros := int64(stubTimeUnixSecs * 1e6)
				if !scanned {
					// If we didn't scan a time, we should set it to 0.1 seconds after the last time.
					stubTimeMicros = st.TimeNow().Add(10 * time.Millisecond).UnixMicro()
				}
				st.SetTime(timeutil.FromUnixMicros(stubTimeMicros))

				// Setup the sql user.
				user := "root"
				d.MaybeScanArgs(t, "user", &user)
				switch user {
				case "root":
				case "testuser":
					spiedConn = spiedConnTestUser
				}

				// Execute query input.
				_, err := spiedConn.Exec(d.Input)
				var sb strings.Builder

				if err != nil {
					sb.WriteString(err.Error())
					sb.WriteString("\n")
				}

				newStmtLogCount := stmtSpy.Count()
				sb.WriteString(stmtSpy.GetLastNLogs(newStmtLogCount - stmtLogCount))
				sb.WriteString("\n")

				newTxnLogCount := txnsSpy.Count()
				sb.WriteString(txnsSpy.GetLastNLogs(newTxnLogCount - txnLogCount))
				return sb.String()
			case "reset-last-sampled":
				telemetryLogging.resetLastSampledTime()
				return ""
			default:
				return "unknown command"
			}
			return ""
		})
	})

}
