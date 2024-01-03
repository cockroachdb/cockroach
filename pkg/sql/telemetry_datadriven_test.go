// Copyright 2023 The Cockroach Authors.
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
 * It has the following directives:
 *   - exec-sql: executes SQL statements in the unobserved connection.
 *   - spy-sql: executes SQL statements in the observed connection. Returns any new stmt logs generated.
 *              Takes in the following arguments:
 *                - unixSecs: sets the current time to the given unix time in seconds. If omitted, the current time is
 *                            automatically changed by 0.1 seconds.
 *   - reset-last-sampled: resets the last sampled time.
 */
func TestTelemetryLoggingDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	st := logtestutils.StubTime{}
	st.SetTime(timeutil.FromUnixMicros(0))
	sts := logtestutils.StubTracingStatus{}
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
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

	datadriven.Walk(t, datapathutils.TestDataPath(t, "telemetryLogging"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				sts.SetTracingStatus(false)
				_, err := setupConn.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
			case "spy-sql":
				logCount := stmtSpy.Count()
				var stubTimeUnixSecs float64
				var tracing bool
				scannedTime := d.MaybeScanArgs(t, "unixSecs", &stubTimeUnixSecs)
				stubTimeMicros := int64(stubTimeUnixSecs * 1e6)
				d.MaybeScanArgs(t, "tracing", &tracing)

				if !scannedTime {
					stubTimeMicros = st.TimeNow().Add(10 * time.Millisecond).UnixMicro()
				}
				st.SetTime(timeutil.FromUnixMicros(stubTimeMicros))
				sts.SetTracingStatus(tracing)
				_, err := spiedConn.Exec(d.Input)
				if err != nil {
					return err.Error()
				}

				newLogCount := stmtSpy.Count()
				if newLogCount > logCount {
					return stmtSpy.GetLastNLogs(newLogCount - logCount)
				}

				return ""
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
