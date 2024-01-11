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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
 * TestTelemetryLoggingDataDriven runs the data-driven tests in telemetry_logging.
 * In these tests, sql is either executed in the unobserved connection or the observed connection.
 * The observed connection is one for which we have set the application_name to a specific value that
 * a log spy is filtering on. The log spy will only capture logs that contain the application_name.
 * This allows us to reduce noise in the output by executing sql for which we don't care about in the
 * unobserved connection.
 *
 * Commands:
 *
 * exec-sql: executes SQL statements in the unobserved connection.
 *
 * spy-sql: executes SQL statements in the observed connection. Returns any new stmt logs generated.
 *    Args:
 *    - unixSecs: sets the current time used for telemetry log sampling to the given unix time in seconds.
 *               If omitted, the current time is automatically changed by 0.1 seconds.
 *    - restartUnixSecs: sets the stub time on txn restarts.
 *
 * tracing: sets the tracing status to the given value
 *
 * reset-last-sampled: resets the last sampled time.
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

	datadriven.Walk(t, datapathutils.TestDataPath(t, "telemetry_logging/logging"), func(t *testing.T, path string) {
		stmtSpy.Reset()

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

		spiedConn := s.SQLConn(t)
		_, err := spiedConn.Exec("SET application_name = $1", appName)
		require.NoError(t, err)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				sts.SetTracingStatus(false)
				_, err := setupConn.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
				return ""
			case "spy-sql":
				logCount := stmtSpy.Count()
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

				// Execute query input.
				_, err := spiedConn.Exec(d.Input)
				if err != nil {
					return err.Error()
				}

				// Display any new statement logs have been generated since executing the query.
				newLogCount := stmtSpy.Count()
				return stmtSpy.GetLastNLogs(newLogCount - logCount)
			case "reset-last-sampled":
				telemetryLogging.resetLastSampledTime()
				return ""
			default:
				t.Fatal("unknown command")
				return ""
			}
		})
	})

}

/**
 * TestTelemetryLoggingDecision runs the data-driven tests in telemetry_logging_decisions.
 * It provides unit testing for the functions that decide whether to log a statement/transaction.
 * No cluster is initialized in these tests, instead we create the telemetry logging struct in
 * isolation and call the functions directly with the provided arguments under different cluster setting
 * conditions.
 *
 * Commands:
 *
 * set-cluster-settings: sets all the relevant cluster settings to the provided values
 *    Args:
 *    - telemetryLoggingEnabled: value for 'telemetryLoggingEnabled' cluster setting
 *    - samplingMode: value for 'telemetrySamplingMode' cluster setting
 *    - stmtSamplingFreq: value for 'TelemetryMaxStatementEventFrequency' cluster setting
 *    - txnSamplingFreq: value for 'telemetryTransactionSamplingFrequency' cluster setting
 *    - stmtsPerTxnMax: value for 'telemetryStatementsPerTransactionMax' cluster setting
 *    - internalStmtsOn: value for 'telemetryInternalQueriesEnabled' cluster setting
 *
 * shouldEmitStatementLog: calls shouldEmitStatementLog with the provided arguments
 *    Args:
 * 	 - unixSecs: stubbed sampling time in unix seconds
 *    - isTrackedTxn: value for 'isTrackedTxn' param
 *    - stmtNum: value for 'stmtNum' param
 *   - force: value for the 'force' param
 *
 * shouldEmitTransactionLog: calls shouldEmitTransactionLog with the provided arguments
 *    Args:
 *    - unixSecs: stubbed sampling time in unix seconds
 *    - force: value for the 'force' param
 *    - isInternal: value for the 'isInternal' param
 */
func TestTelemetryLoggingDecision(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cs := cluster.MakeTestingClusterSettings()
	st := logtestutils.StubTime{}
	testingKnobs := NewTelemetryLoggingTestingKnobs(st.TimeNow, nil, nil)
	telemetryLogging := newTelemetryLoggingMetrics(testingKnobs, cs)
	ctx := context.Background()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "telemetry_logging/logging_decisions"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			t.Cleanup(func() {
				telemetryLogging.resetLastSampledTime()
			})

			switch d.Cmd {
			case "set-cluster-settings":
				var telemLoggingEnabled, internalStmtsEnabled bool
				var samplingMode string
				var stmtSampleFreq, txnSampleFreq int
				stmtsPerTxnMax := 10
				d.ScanArgs(t, "telemetryLoggingEnabled", &telemLoggingEnabled)
				d.ScanArgs(t, "samplingMode", &samplingMode)
				d.MaybeScanArgs(t, "stmtSampleFreq", &stmtSampleFreq)
				d.MaybeScanArgs(t, "txnSampleFreq", &txnSampleFreq)
				d.MaybeScanArgs(t, "stmtsPerTxnMax", &stmtsPerTxnMax)
				d.MaybeScanArgs(t, "internalStmtsOn", &internalStmtsEnabled)

				mode := telemetryModeStatement
				if samplingMode == "transaction" {
					mode = telemetryModeTransaction
				}
				telemetrySamplingMode.Override(ctx, &cs.SV, int64(mode))
				telemetryStatementsPerTransactionMax.Override(ctx, &cs.SV, int64(stmtsPerTxnMax))
				TelemetryMaxStatementEventFrequency.Override(ctx, &cs.SV, int64(stmtSampleFreq))
				telemetryTransactionSamplingFrequency.Override(ctx, &cs.SV, int64(txnSampleFreq))
				telemetryLoggingEnabled.Override(ctx, &cs.SV, telemLoggingEnabled)
				telemetryInternalQueriesEnabled.Override(ctx, &cs.SV, internalStmtsEnabled)

				return ""
			case "shouldEmitStatementLog":
				var unixSecs float64
				var isTrackedTxn, force bool
				var stmtNum int
				d.ScanArgs(t, "unixSecs", &unixSecs)
				d.ScanArgs(t, "isTrackedTxn", &isTrackedTxn)
				d.ScanArgs(t, "stmtNum", &stmtNum)
				d.ScanArgs(t, "force", &force)
				st.SetTime(timeutil.FromUnixMicros(int64(unixSecs * 1e6)))

				shouldEmit, _ := telemetryLogging.shouldEmitStatementLog(isTrackedTxn, stmtNum, force)
				return strconv.FormatBool(shouldEmit)
			case "shouldEmitTransactionLog":
				var unixSecs float64
				var force, isInternal bool
				d.ScanArgs(t, "unixSecs", &unixSecs)
				d.ScanArgs(t, "force", &force)
				d.MaybeScanArgs(t, "isInternal", &isInternal)
				st.SetTime(timeutil.FromUnixMicros(int64(unixSecs * 1e6)))

				shouldEmit := telemetryLogging.shouldEmitTransactionLog(force, isInternal)
				return strconv.FormatBool(shouldEmit)
			default:
				t.Fatal("unknown command")
				return ""
			}
		})
	})

}
