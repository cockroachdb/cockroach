// Copyright 2021 The Cockroach Authors.
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
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/errors"
)

func installTelemetryLogFileSink(sc *log.TestLogScope, t *testing.T) func() {
	// Enable logging channels.
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	// Make a sink for just the session log.
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"telemetry": {
			Channels: logconfig.ChannelList{Channels: []log.Channel{channel.TELEMETRY}},
		}}
	dir := sc.GetDirectory()
	if err := cfg.Validate(&dir); err != nil {
		t.Fatal(err)
	}
	cleanup, err := log.ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return cleanup
}

// TestTelemetryEventLogging verifies that telemetry events are logged to the telemetry log
// and their "Sampled" status is logged correctly.
func TestTelemetryEventLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installTelemetryLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})

	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	db.Exec(t, `SET CLUSTER SETTING sql.trace.log_statement_execute = true;`)

	samplingRateFail := float64(0)
	samplingRatePass := float64(1)
	qpsThresholdExceed := int64(-1)
	qpsThresholdNotExceed := int64(1000)

	// Testing Cases:
	// - entries that are NOT sampled
	// 	- cases include:
	//		- statement type not DML
	//		- below QPS threshold
	//		- sampling rate does not pass
	// - entries that ARE sampled
	// 	- cases include:
	//		- statement type DML, above QPS threshold, and sampling rate passes

	testData := []struct {
		name                 string
		query                string
		expectedLogStatement string
		stubQpsThreshold     int64
		stubSamplingRate     float64
		expectedSampleRate   string
	}{
		{
			// Test case with statement that is not of type DML.
			// Therefore, expected sample rate is 1.
			"create-table-query",
			"CREATE TABLE t();",
			"CREATE TABLE ‹defaultdb›.public.‹t› ()",
			qpsThresholdExceed,
			samplingRatePass,
			`"EffectiveSampleRate":1`,
		},
		{
			// Test case with statement that is of type DML.
			// QPS threshold is not expected to be exceeded, therefore,
			// no sampling will occur. Expected sample rate is 1.
			"select-*-limit-1-query",
			"SELECT * FROM t LIMIT 1;",
			`SELECT * FROM ‹\"\"›.‹\"\"›.‹t› LIMIT ‹1›`,
			qpsThresholdNotExceed,
			samplingRatePass,
			`"EffectiveSampleRate":1`,
		},
		{
			// Test case with statement that is of type DML.
			// Sampling selection will guaranteed fail, therefore,
			// no log will appear.
			"select-*-limit-2-query",
			"SELECT * FROM t LIMIT 2;",
			`SELECT * FROM ‹\"\"›.‹\"\"›.‹t› LIMIT ‹2›`,
			qpsThresholdExceed,
			samplingRateFail,
			`"EffectiveSampleRate":0`,
		},
		{
			// Test case with statement that is of type DML.
			// QPS threshold is expected to be exceeded, and sampling
			// selection is guaranteed. Expected sample rate is 1.
			"select-*-limit-3-query",
			"SELECT * FROM t LIMIT 3;",
			`SELECT * FROM ‹\"\"›.‹\"\"›.‹t› LIMIT ‹3›`,
			qpsThresholdExceed,
			samplingRatePass,
			`"EffectiveSampleRate":1`,
		},
	}

	for _, tc := range testData {
		telemetryQpsThreshold.Override(context.Background(), &s.ClusterSettings().SV, tc.stubQpsThreshold)
		telemetrySampleRate.Override(context.Background(), &s.ClusterSettings().SV, tc.stubSamplingRate)
		db.Exec(t, tc.query)
	}

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"telemetry_event"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	for _, tc := range testData {
		found := false
		for _, e := range entries {
			if strings.Contains(e.Message, tc.expectedLogStatement) && strings.Contains(e.Message, tc.expectedSampleRate) {
				found = true
				break
			}
		}
		if !found && tc.name != "select-*-limit-2-query" {
			t.Fatal(errors.Newf("no matching entry found for test case %s", tc.name))
		}
	}
}
