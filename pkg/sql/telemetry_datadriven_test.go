// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
 *    - tracing: sets the tracing status to the given value. If omitted, the tracing status is set to false.
 *    - useRealTracing: if set, the real tracing status is used instead of the stubbed one.
 *    - user: sets the user for the connection. If omitted, the root user is used.
 *    - reset-telemetry-cluster-settings: resets the cluster settings for telemetry logging to default values.
 *
 * reset-last-sampled: resets the last sampled time.
 */
func TestTelemetryLoggingDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Some queries may be retried under stress.
	skip.UnderRace(t, "results inconsistent under stress")

	sc := log.Scope(t)
	defer sc.Close(t)

	appName := "telemetry-logging-datadriven"
	ignoredAppname := "telemetry-datadriven-ignored-appname"
	ctx := context.Background()
	stmtSpy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_TELEMETRY},
		[]string{"sampled_query"},
		logtestutils.FormatEntryAsJSON,
		func(_ logpb.Entry, logStr string) bool {
			return !strings.Contains(logStr, ignoredAppname)
		},
	)

	cleanup := log.InterceptWith(ctx, stmtSpy)
	defer cleanup()

	txnsSpy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_TELEMETRY},
		[]string{"sampled_transaction"},
		logtestutils.FormatEntryAsJSON,
		func(_ logpb.Entry, logStr string) bool {
			return strings.Contains(logStr, appName) || strings.Contains(logStr, internalConsoleAppName)
		},
	)
	cleanupTxnSpy := log.InterceptWith(ctx, txnsSpy)
	defer cleanupTxnSpy()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "telemetry_logging/logging"), func(t *testing.T, path string) {
		stmtSpy.Reset()
		txnsSpy.Reset()

		st := logtestutils.StubTime{}
		st.SetTime(timeutil.FromUnixMicros(0))
		sts := logtestutils.StubTracingStatus{}
		stubTimeOnRestart := int64(0)
		telemetryKnobs := &TelemetryLoggingTestingKnobs{
			getTimeNow:       st.TimeNow,
			getTracingStatus: sts.TracingStatus,
		}
		tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SQLExecutor: &ExecutorTestingKnobs{
						BeforeRestart: func(_ context.Context, _ error) {
							st.SetTime(timeutil.FromUnixMicros(stubTimeOnRestart * 1e6))
						},
					},
					TelemetryLoggingKnobs: telemetryKnobs,
				},
			},
		})
		defer tc.Stopper().Stop(context.Background())
		s := tc.ApplicationLayer(0)

		telemetryLogging := s.SQLServer().(*Server).TelemetryLoggingMetrics
		setupConn := s.SQLConn(t)
		_, err := setupConn.Exec("CREATE USER testuser")
		require.NoError(t, err)
		_, err = setupConn.Exec("SET application_name = $1", ignoredAppname)
		require.NoError(t, err)

		spiedConnRootUser := s.SQLConn(t)
		spiedConnTestUser := s.SQLConn(t, serverutils.User("testuser"))
		spiedConn := spiedConnRootUser

		// Set spied connections to the app name observed by the log spy.
		_, err = spiedConn.Exec("SET application_name = $1", appName)
		require.NoError(t, err)
		_, err = spiedConnTestUser.Exec("SET application_name = $1", appName)
		require.NoError(t, err)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			spiedConn = spiedConnRootUser
			switch d.Cmd {
			case "exec-sql":
				sts.SetTracingStatus(false)
				_, err := setupConn.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
				return ""
			case "spy-sql":
				stmtLogCount := stmtSpy.Count()
				txnLogCount := txnsSpy.Count()
				var stubTimeUnixSecs float64
				var tracing, useRealTracing bool
				var stubStatementFingerprintId string

				d.MaybeScanArgs(t, "stubStatementFingerprintId", &stubStatementFingerprintId)
				if stubStatementFingerprintId != "" {
					defer testutils.TestingHook(&appstatspb.ConstructStatementFingerprintID,
						func(stmtNoConstants string, implicitTxn bool, database string) appstatspb.StmtFingerprintID {
							parseUint, e := strconv.ParseUint(stubStatementFingerprintId, 10, 64)
							if e != nil {
								panic(e.Error())
							}
							return appstatspb.StmtFingerprintID(parseUint)
						})()
				}

				d.MaybeScanArgs(t, "tracing", &tracing)
				sts.SetTracingStatus(tracing)

				d.MaybeScanArgs(t, "useRealTracing", &useRealTracing)
				if useRealTracing {
					telemetryKnobs.getTracingStatus = nil
					defer func() {
						telemetryKnobs.getTracingStatus = sts.TracingStatus
					}()
				}

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
				sb.WriteString(strings.Join(stmtSpy.GetLastNLogs(logpb.Channel_TELEMETRY, newStmtLogCount-stmtLogCount), "\n"))
				if newStmtLogCount > stmtLogCount {
					sb.WriteString("\n")
				}

				newTxnLogCount := txnsSpy.Count()
				sb.WriteString(strings.Join(txnsSpy.GetLastNLogs(logpb.Channel_TELEMETRY, newTxnLogCount-txnLogCount), "\n"))
				return sb.String()
			case "reset-last-sampled":
				telemetryLogging.resetLastSampledTime()
				return ""
			case "show-skipped-transactions":
				return strconv.FormatUint(telemetryLogging.getSkippedTransactionCount(), 10)
			case "reset-telemetry-cluster-settings":
				// Set the default cluster settings for telemetry logging.
				clusterSettings := []string{
					"sql.telemetry.query_sampling.max_event_frequency",
					"sql.telemetry.transaction_sampling.max_event_frequency",
					"sql.telemetry.transaction_sampling.statement_events_per_transaction.max",
					"sql.telemetry.query_sampling.internal.enabled",
					"sql.telemetry.query_sampling.internal_console.enabled",
					"sql.telemetry.query_sampling.mode",
				}
				for _, setting := range clusterSettings {
					if _, err := setupConn.Exec("RESET CLUSTER SETTING " + setting); err != nil {
						return err.Error()
					}
				}
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
 *    - isTracing: value for the 'isTracing' param
 *    - isInternal: value for the 'isInternal' param
 *    - appName: value for the 'appName' param
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
				telemetryLogging.resetCounters()
			})

			switch d.Cmd {
			case "set-cluster-settings":
				var telemLoggingEnabled, internalStmtsEnabled, consoleQueriesEnabled bool
				var samplingMode string
				var stmtSampleFreq, txnSampleFreq int
				stmtsPerTxnMax := 10
				d.ScanArgs(t, "telemetryLoggingEnabled", &telemLoggingEnabled)
				d.ScanArgs(t, "samplingMode", &samplingMode)
				d.MaybeScanArgs(t, "stmtSampleFreq", &stmtSampleFreq)
				d.MaybeScanArgs(t, "txnSampleFreq", &txnSampleFreq)
				d.MaybeScanArgs(t, "stmtsPerTxnMax", &stmtsPerTxnMax)
				d.MaybeScanArgs(t, "internalStmtsOn", &internalStmtsEnabled)
				d.MaybeScanArgs(t, "telemetryInternalConsoleQueriesEnabled", &consoleQueriesEnabled)

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
				telemetryInternalConsoleQueriesEnabled.Override(ctx, &cs.SV, consoleQueriesEnabled)

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

				shouldEmit, skipped := telemetryLogging.shouldEmitStatementLog(isTrackedTxn, stmtNum, force)
				return fmt.Sprintf(`emit: %t, skippedQueries: %d`, shouldEmit, skipped)
			case "shouldEmitTransactionLog":
				var unixSecs float64
				var isTracing, isInternal bool
				var appName string
				d.ScanArgs(t, "unixSecs", &unixSecs)
				d.ScanArgs(t, "isTracing", &isTracing)
				d.MaybeScanArgs(t, "isInternal", &isInternal)
				st.SetTime(timeutil.FromUnixMicros(int64(unixSecs * 1e6)))
				d.ScanArgs(t, "appName", &appName)

				shouldEmit, skipped := telemetryLogging.shouldEmitTransactionLog(isTracing, isInternal, appName)
				return fmt.Sprintf(`emit: %t, skippedTxns: %d`, shouldEmit, skipped)
			default:
				t.Fatal("unknown command")
				return ""
			}
		})
	})

}
