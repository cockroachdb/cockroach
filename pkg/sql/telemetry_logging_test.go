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
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func installTelemetryLogFileSink(sc *log.TestLogScope, t *testing.T) func() {
	// Enable logging channels.
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	// Make a sink for just the session log.
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"telemetry": {
			Channels: logconfig.SelectChannels(channel.TELEMETRY),
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

type fakeInterval struct {
	syncutil.RWMutex
	interval int64
}

func (i *fakeInterval) setInterval(length int64) {
	i.RWMutex.Lock()
	defer i.RWMutex.Unlock()
	i.interval = length
}

func (i *fakeInterval) getInterval() int64 {
	i.RWMutex.RLock()
	defer i.RWMutex.RUnlock()
	return i.interval
}

// TestTelemetryLogging verifies that telemetry events are logged to the telemetry log
// and their "EffectiveSampleRate" value is logged correctly.
func TestTelemetryLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installTelemetryLogFileSink(sc, t)
	defer cleanup()

	st := stubTime{}
	st.setTime(timeutil.Now())
	stubInterval := fakeInterval{}
	stubInterval.setInterval(1)
	stubLastEmittedTime := stubTime{}

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			TelemetryLoggingKnobs: &TelemetryLoggingTestingKnobs{
				getRollingIntervalLength: stubInterval.getInterval,
				getTimeNow:               st.TimeNow,
				getLastEmittedTime:       stubLastEmittedTime.TimeNow,
			},
		},
	})

	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	db.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)

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
		name                  string
		query                 string
		execTimestampsSeconds []float64 // Execute the query with the following timestamps.
		expectedLogStatement  string
		stubSamplingRate      float64
		expectedSkipped       int
	}{
		{
			// Test case with statement that is not of type DML.
			// Even though the queries are executed within the required
			// elapsed interval, we should still see that they were all
			// logged, we log all statements that are not of type DML.
			"create-table-query",
			"CREATE TABLE t();",
			[]float64{1, 1.1, 1.2},
			"CREATE TABLE ‹defaultdb›.public.‹t› ()",
			float64(1),
			0,
		},
		{
			// Test case with statement that is of type DML.
			// The first statement should be logged.
			"select-*-limit-1-query",
			"SELECT * FROM t LIMIT 1;",
			[]float64{1},
			`SELECT * FROM ‹\"\"›.‹\"\"›.‹t› LIMIT ‹1›`,
			float64(1),
			0,
		},
		{
			// Test case with statement that is of type DML.
			// All timestamps are within the required elapsed interval,
			// and so only one statement should be logged.
			"select-*-limit-2-query",
			"SELECT * FROM t LIMIT 2;",
			[]float64{1, 1.1, 1.2},
			`SELECT * FROM ‹\"\"›.‹\"\"›.‹t› LIMIT ‹2›`,
			float64(1),
			2,
		},
		{
			// Test case with statement that is of type DML.
			// QPS threshold is expected to be exceeded, and sampling
			// selection is guaranteed.
			"select-*-limit-3-query",
			"SELECT * FROM t LIMIT 3;",
			[]float64{1, 1.1, 1.2},
			`SELECT * FROM ‹\"\"›.‹\"\"›.‹t› LIMIT ‹3›`,
			float64(1),
			2, // sum of exec counts of previous test.
		},
		{
			// Test case with statement that is of type DML.
			// QPS threshold is expected to be exceeded, and sampling
			// selection is guaranteed.
			// Test case executes multiple queries in multiple 1s intervals.
			"select-*-limit-4-query",
			"SELECT * FROM t LIMIT 4;",
			[]float64{1, 1.1, 1.2},
			`SELECT * FROM ‹\"\"›.‹\"\"›.‹t› LIMIT ‹4›`,
			float64(1),
			0,
		},
	}

	for _, tc := range testData {
		telemetrySampleRate.Override(context.Background(), &s.ClusterSettings().SV, tc.stubSamplingRate)
		st.setTime(st.TimeNow().Add(time.Second))
		stubLastEmittedTime.setTime(st.TimeNow())
		for _, execTimestamp := range tc.execTimestampsSeconds {
			stubLastEmittedTime.setTime(st.TimeNow())
			db.Exec(t, tc.query)
			st.setTime(st.TimeNow().Add(time.Second))
		}
	}

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"sampled_query"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	for _, e := range entries {
		if strings.Contains(e.Message, `"ExecMode":"`+executorTypeInternal.logLabel()) {
			t.Errorf("unexpected telemetry event for internal statement:\n%s", e.Message)
		}
	}

	for _, tc := range testData {
		logStatementFound := false
		firstMatch := true
		// NB: FetchEntriesFromFiles delivers entries in reverse order.
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if strings.Contains(e.Message, tc.expectedLogStatement) {
				t.Logf("%s: found entry:\n%s", tc.name, e.Message)
				logStatementFound = true
				if firstMatch {
					firstMatch = false
					if tc.expectedSkipped == 0 {
						if strings.Contains(e.Message, "SkippedQueries") {
							t.Errorf("%s: expected no skipped queries, found:\n%s", tc.name, e.Message)
						}
					} else {
						if expected := fmt.Sprintf(`"SkippedQueries":%d`, tc.expectedSkipped); !strings.Contains(e.Message, expected) {
							t.Errorf("%s: expected %s in first log entry, found:\n%s", tc.name, expected, e.Message)
						}
					}
				}
			}
		}
		if !logStatementFound && tc.name != "select-*-limit-2-query" {
			t.Errorf("%s: no matching log entry found", tc.name)
		}
		if logStatementFound && tc.name == "select-*-limit-2-query" {
			t.Errorf("%s: found log entry, was expecting no entry", tc.name)
		}
	}
}
