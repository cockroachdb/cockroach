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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type stubTime struct {
	syncutil.RWMutex
	t time.Time
}

func (s *stubTime) setTime(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.t = t
}

func (s *stubTime) TimeNow() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.t
}

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

// TestTelemetryLogging verifies that telemetry events are logged to the telemetry log
// and are sampled according to the configured sample rate.
func TestTelemetryLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installTelemetryLogFileSink(sc, t)
	defer cleanup()

	st := stubTime{}

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			TelemetryLoggingKnobs: &TelemetryLoggingTestingKnobs{
				getTimeNow: st.TimeNow,
			},
		},
	})

	defer s.Stopper().Stop(context.Background())

	var sessionID string
	var databaseName string

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.QueryRow(t, `SHOW session_id`).Scan(&sessionID)
	db.QueryRow(t, `SHOW database`).Scan(&databaseName)
	db.Exec(t, `SET application_name = 'telemetry-logging-test'`)
	db.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)
	db.Exec(t, "CREATE TABLE t();")
	db.Exec(t, "CREATE TABLE u(x int);")
	db.Exec(t, "INSERT INTO u SELECT generate_series(1, 100);")
	// Use INJECT STATISTICS instead of ANALYZE to avoid test flakes.
	db.Exec(t, `ALTER TABLE u INJECT STATISTICS '[{
      "avg_size": 3,
      "columns": ["x"],
      "created_at": "2022-07-28 12:54:13.915054",
      "distinct_count": 100,
      "null_count": 0,
      "row_count": 100
  }]';`)

	// Testing Cases:
	// - entries that are NOT sampled
	// 	- cases include:
	//		- statement type not DML
	// - entries that ARE sampled
	// 	- cases include:
	//		- statement type DML, enough time has elapsed

	testData := []struct {
		name                    string
		query                   string
		queryNoConstants        string
		execTimestampsSeconds   []float64 // Execute the query with the following timestamps.
		expectedLogStatement    string
		stubMaxEventFrequency   int64
		expectedSkipped         []int // Expected skipped query count per expected log line.
		expectedUnredactedTags  []string
		expectedApplicationName string
		expectedFullScan        bool
		expectedStatsAvailable  bool
		expectedRead            bool
		expectedWrite           bool
		expectedErr             string // Empty string means no error is expected.
	}{
		{
			// Test case with statement that is not of type DML.
			// Even though the queries are executed within the required
			// elapsed interval, we should still see that they were all
			// logged since  we log all statements that are not of type DML.
			name:                    "truncate-table-query",
			query:                   "TRUNCATE t;",
			queryNoConstants:        "TRUNCATE TABLE t",
			execTimestampsSeconds:   []float64{1, 1.1, 1.2, 2},
			expectedLogStatement:    `TRUNCATE TABLE`,
			stubMaxEventFrequency:   1,
			expectedSkipped:         []int{0, 0, 0, 0},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        false,
			expectedStatsAvailable:  false,
			expectedRead:            false,
			expectedWrite:           false,
		},
		{
			// Test case with statement that is of type DML.
			// The first statement should be logged.
			name:                    "select-*-limit-1-query",
			query:                   "SELECT * FROM t LIMIT 1;",
			queryNoConstants:        "SELECT * FROM t LIMIT _",
			execTimestampsSeconds:   []float64{3},
			expectedLogStatement:    `SELECT * FROM \"\".\"\".t LIMIT ‹1›`,
			stubMaxEventFrequency:   1,
			expectedSkipped:         []int{0},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        false,
			expectedStatsAvailable:  false,
			expectedRead:            false,
			expectedWrite:           false,
		},
		{
			// Test case with statement that is of type DML.
			// Two timestamps are within the required elapsed interval,
			// thus 2 log statements are expected, with 2 skipped queries.
			name:                    "select-*-limit-2-query",
			query:                   "SELECT * FROM u LIMIT 2;",
			queryNoConstants:        "SELECT * FROM u LIMIT _",
			execTimestampsSeconds:   []float64{4, 4.1, 4.2, 5},
			expectedLogStatement:    `SELECT * FROM \"\".\"\".u LIMIT ‹2›`,
			stubMaxEventFrequency:   1,
			expectedSkipped:         []int{0, 2},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        false,
			expectedStatsAvailable:  true,
			expectedRead:            true,
			expectedWrite:           false,
		},
		{
			// Test case with statement that is of type DML.
			// Once required time has elapsed, the next statement should be logged.
			name:                    "select-*-limit-3-query",
			query:                   "SELECT * FROM u LIMIT 3;",
			queryNoConstants:        "SELECT * FROM u LIMIT _",
			execTimestampsSeconds:   []float64{6, 6.01, 6.05, 6.06, 6.1, 6.2},
			expectedLogStatement:    `SELECT * FROM \"\".\"\".u LIMIT ‹3›`,
			stubMaxEventFrequency:   10,
			expectedSkipped:         []int{0, 3, 0},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        false,
			expectedStatsAvailable:  true,
			expectedRead:            true,
			expectedWrite:           false,
		},
		{
			// Test case with a full scan.
			// The first statement should be logged.
			name:                    "select-x-query",
			query:                   "SELECT x FROM u;",
			queryNoConstants:        "SELECT x FROM u",
			execTimestampsSeconds:   []float64{7},
			expectedLogStatement:    `SELECT x FROM \"\".\"\".u`,
			stubMaxEventFrequency:   10,
			expectedSkipped:         []int{0},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        true,
			expectedStatsAvailable:  true,
			expectedRead:            true,
			expectedWrite:           false,
		},
		{
			// Test case with a write.
			// The first statement should be logged.
			name:                    "update-u-query",
			query:                   "UPDATE u SET x = 5 WHERE x > 50 RETURNING x;",
			queryNoConstants:        "UPDATE u SET x = _ WHERE x > _ RETURNING x",
			execTimestampsSeconds:   []float64{8},
			expectedLogStatement:    `UPDATE \"\".\"\".u SET x = ‹5› WHERE x > ‹50› RETURNING x`,
			stubMaxEventFrequency:   10,
			expectedSkipped:         []int{0},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        true,
			expectedStatsAvailable:  true,
			expectedRead:            true,
			expectedWrite:           true,
		},
		// Not of type DML so not sampled
		{
			name:                    "sql-error",
			query:                   "CREATE USER root;",
			queryNoConstants:        "CREATE USER root",
			execTimestampsSeconds:   []float64{9},
			expectedLogStatement:    `CREATE USER root`,
			stubMaxEventFrequency:   1,
			expectedSkipped:         []int{0},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        false,
			expectedStatsAvailable:  false,
			expectedRead:            false,
			expectedWrite:           false,
			expectedErr:             "a role/user named ‹root› already exists",
		},
	}

	for _, tc := range testData {
		telemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, tc.stubMaxEventFrequency)
		for _, execTimestamp := range tc.execTimestampsSeconds {
			stubTime := timeutil.FromUnixMicros(int64(execTimestamp * 1e6))
			st.setTime(stubTime)
			_, err := db.DB.ExecContext(context.Background(), tc.query)
			if err != nil && tc.expectedErr == "" {
				t.Errorf("unexpected error executing query `%s`: %v", tc.query, err)
			}
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
		t.Run(tc.name, func(t *testing.T) {
			logCount := 0
			expectedLogCount := len(tc.expectedSkipped)
			// NB: FetchEntriesFromFiles delivers entries in reverse order.
			for i := len(entries) - 1; i >= 0; i-- {
				e := entries[i]
				if strings.Contains(e.Message, tc.expectedLogStatement) {
					if logCount == expectedLogCount {
						t.Errorf("%s: found more than %d expected log entries", tc.name, expectedLogCount)
						break
					}
					expectedSkipped := tc.expectedSkipped[logCount]
					logCount++
					if expectedSkipped == 0 {
						if strings.Contains(e.Message, "SkippedQueries") {
							t.Errorf("%s: expected no skipped queries, found:\n%s", tc.name, e.Message)
						}
					} else {
						if expected := fmt.Sprintf(`"SkippedQueries":%d`, expectedSkipped); !strings.Contains(e.Message, expected) {
							t.Errorf("%s: expected %s found:\n%s", tc.name, expected, e.Message)
						}
					}
					costRe := regexp.MustCompile("\"CostEstimate\":[0-9]*\\.[0-9]*")
					if !costRe.MatchString(e.Message) {
						t.Errorf("expected to find CostEstimate but none was found")
					}
					distRe := regexp.MustCompile("\"Distribution\":(\"full\"|\"local\")")
					if !distRe.MatchString(e.Message) {
						t.Errorf("expected to find Distribution but none was found")
					}
					// Match plan gist on any non-empty string value.
					planGist := regexp.MustCompile("\"PlanGist\":(\"\\S+\")")
					if !planGist.MatchString(e.Message) {
						t.Errorf("expected to find PlanGist but none was found in: %s", e.Message)
					}
					// Match StatementID on any non-empty string value.
					stmtID := regexp.MustCompile("\"StatementID\":(\"\\S+\")")
					if !stmtID.MatchString(e.Message) {
						t.Errorf("expected to find StatementID but none was found in: %s", e.Message)
					}
					// Match TransactionID on any non-empty string value.
					txnID := regexp.MustCompile("\"TransactionID\":(\"\\S+\")")
					if !txnID.MatchString(e.Message) {
						t.Errorf("expected to find TransactionID but none was found in: %s", e.Message)
					}
					for _, eTag := range tc.expectedUnredactedTags {
						for _, tag := range strings.Split(e.Tags, ",") {
							kv := strings.Split(tag, "=")
							if kv[0] == eTag && strings.ContainsAny(kv[0], fmt.Sprintf("%s%s", redact.StartMarker(), redact.EndMarker())) {
								t.Errorf("expected tag %s to be redacted within tags: %s", tag, e.Tags)
							}
						}
					}
					if !strings.Contains(e.Message, "\"ApplicationName\":\""+tc.expectedApplicationName+"\"") {
						t.Errorf("expected to find unredacted Application Name: %s", tc.expectedApplicationName)
					}
					if !strings.Contains(e.Message, "\"SessionID\":\""+sessionID+"\"") {
						t.Errorf("expected to find sessionID: %s", sessionID)
					}
					if !strings.Contains(e.Message, "\"Database\":\""+databaseName+"\"") {
						t.Errorf("expected to find Database: %s", databaseName)
					}
					stmtFingerprintID := roachpb.ConstructStatementFingerprintID(tc.queryNoConstants, tc.expectedErr != "", true, databaseName)
					if !strings.Contains(e.Message, "\"StatementFingerprintID\":"+strconv.FormatUint(uint64(stmtFingerprintID), 10)) {
						t.Errorf("expected to find StatementFingerprintID: %v", stmtFingerprintID)
					}
					maxFullScanRowsRe := regexp.MustCompile("\"MaxFullScanRowsEstimate\":[0-9]*")
					foundFullScan := maxFullScanRowsRe.MatchString(e.Message)
					if tc.expectedFullScan && !foundFullScan {
						t.Errorf("expected to find MaxFullScanRowsEstimate but none was found in: %s", e.Message)
					} else if !tc.expectedFullScan && foundFullScan {
						t.Errorf("expected not to find MaxFullScanRowsEstimate but it was found in: %s", e.Message)
					}
					totalScanRowsRe := regexp.MustCompile("\"TotalScanRowsEstimate\":[0-9]*")
					outputRowsRe := regexp.MustCompile("\"OutputRowsEstimate\":[0-9]*")
					statsAvailableRe := regexp.MustCompile("\"StatsAvailable\":(true|false)")
					nanosSinceStatsCollectedRe := regexp.MustCompile("\"NanosSinceStatsCollected\":[0-9]*")
					if tc.expectedStatsAvailable {
						if !totalScanRowsRe.MatchString(e.Message) {
							t.Errorf("expected to find TotalScanRowsEstimate but none was found in: %s", e.Message)
						}
						if !outputRowsRe.MatchString(e.Message) {
							t.Errorf("expected to find OutputRowsEstimate but none was found in: %s", e.Message)
						}
						if !statsAvailableRe.MatchString(e.Message) {
							t.Errorf("expected to find StatsAvailable but none was found in: %s", e.Message)
						}
						if !nanosSinceStatsCollectedRe.MatchString(e.Message) {
							t.Errorf("expected to find NanosSinceStatsCollected but none was found in: %s", e.Message)
						}
					} else {
						if totalScanRowsRe.MatchString(e.Message) {
							t.Errorf("expected not to find TotalScanRowsEstimate but it was found in: %s", e.Message)
						}
						if outputRowsRe.MatchString(e.Message) {
							t.Errorf("expected not to find OutputRowsEstimate but it was found in: %s", e.Message)
						}
						if statsAvailableRe.MatchString(e.Message) {
							t.Errorf("expected not to find StatsAvailable but it was found in: %s", e.Message)
						}
						if nanosSinceStatsCollectedRe.MatchString(e.Message) {
							t.Errorf("expected not to find NanosSinceStatsCollected but it was found in: %s", e.Message)
						}
					}
					BytesReadRe := regexp.MustCompile("\"BytesRead\":[0-9]*")
					RowsReadRe := regexp.MustCompile("\"RowsRead\":[0-9]*")
					if tc.expectedRead {
						if !BytesReadRe.MatchString(e.Message) {
							t.Errorf("expected to find BytesRead but none was found in: %s", e.Message)
						}
						if !RowsReadRe.MatchString(e.Message) {
							t.Errorf("expected to find RowsRead but none was found in: %s", e.Message)
						}
					} else {
						if BytesReadRe.MatchString(e.Message) {
							t.Errorf("expected not to find BytesRead but it was found in: %s", e.Message)
						}
						if RowsReadRe.MatchString(e.Message) {
							t.Errorf("expected not to find RowsRead but it was found in: %s", e.Message)
						}

					}
					RowsWrittenRe := regexp.MustCompile("\"RowsWritten\":[0-9]*")
					if tc.expectedWrite {
						if !RowsWrittenRe.MatchString(e.Message) {
							t.Errorf("expected to find RowsWritten but none was found in: %s", e.Message)
						}
					} else {
						if RowsWrittenRe.MatchString(e.Message) {
							t.Errorf("expected not to find RowsWritten but it was found in: %s", e.Message)
						}
					}
					if tc.expectedErr != "" {
						if !strings.Contains(e.Message, tc.expectedErr) {
							t.Errorf("%s: missing error %s in message %s", tc.name, tc.expectedErr, e.Message)
							break
						}
					}
				}
			}
			if logCount != expectedLogCount {
				t.Errorf("%s: expected %d log entries, found %d", tc.name, expectedLogCount, logCount)
			}
		})
	}
}

func TestNoTelemetryLogOnTroubleshootMode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installTelemetryLogFileSink(sc, t)
	defer cleanup()

	st := stubTime{}

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			TelemetryLoggingKnobs: &TelemetryLoggingTestingKnobs{
				getTimeNow: st.TimeNow,
			},
		},
	})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	db.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)
	db.Exec(t, "CREATE TABLE t();")

	stubMaxEventFrequency := int64(1)
	telemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, stubMaxEventFrequency)

	/*
		Testing Cases:
			- run query when troubleshoot mode is enabled
				- ensure no log appears
			- run another query when troubleshoot mode is disabled
				- ensure log appears
	*/
	testData := []struct {
		name                      string
		query                     string
		expectedLogStatement      string
		enableTroubleshootingMode bool
		expectedNumLogs           int
	}{
		{
			"select-troubleshooting-enabled",
			"SELECT * FROM t LIMIT 1;",
			`SELECT * FROM \"\".\"\".t LIMIT ‹1›`,
			true,
			0,
		},
		{
			"select-troubleshooting-disabled",
			"SELECT * FROM t LIMIT 2;",
			`SELECT * FROM \"\".\"\".t LIMIT ‹2›`,
			false,
			1,
		},
	}

	for idx, tc := range testData {
		// Set the time for when we issue a query to enable/disable
		// troubleshooting mode.
		setTroubleshootModeTime := timeutil.FromUnixMicros(int64(idx * 1e6))
		st.setTime(setTroubleshootModeTime)
		if tc.enableTroubleshootingMode {
			db.Exec(t, `SET troubleshooting_mode = true;`)
		} else {
			db.Exec(t, `SET troubleshooting_mode = false;`)
		}
		// Advance time 1 second from previous query. Ensure enough time has passed
		// from when we set troubleshooting mode for this query to be sampled.
		setQueryTime := timeutil.FromUnixMicros(int64((idx + 1) * 1e6))
		st.setTime(setQueryTime)
		db.Exec(t, tc.query)
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

	for _, tc := range testData {
		numLogsFound := 0
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if strings.Contains(e.Message, tc.expectedLogStatement) {
				if tc.enableTroubleshootingMode {
					t.Errorf("%s: unexpected log entry when troubleshooting mode enabled:\n%s", tc.name, entries[0].Message)
				} else {
					numLogsFound++
				}
			}
		}
		if numLogsFound != tc.expectedNumLogs {
			t.Errorf("%s: expected %d log entries, found %d", tc.name, tc.expectedNumLogs, numLogsFound)
		}
	}
}

func TestTelemetryLogJoinTypesAndAlgorithms(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installTelemetryLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	db.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)
	db.Exec(t, "CREATE TABLE t ("+
		"pk INT PRIMARY KEY,"+
		"col1 INT,"+
		"col2 INT,"+
		"other STRING,"+
		"j JSON,"+
		"INDEX other_index (other),"+
		"INVERTED INDEX j_index (j)"+
		");")
	db.Exec(t, "CREATE TABLE u ("+
		"pk INT PRIMARY KEY,"+
		"fk INT REFERENCES t (pk),"+
		"j JSON,"+
		"INDEX fk_index (fk),"+
		"INVERTED INDEX j_index (j)"+
		");")

	stubMaxEventFrequency := int64(1000000)
	telemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, stubMaxEventFrequency)

	testData := []struct {
		name                   string
		query                  string
		expectedLogStatement   string
		expectedNumLogs        int
		expectedJoinTypes      map[string]int
		expectedJoinAlgorithms map[string]int
	}{
		{
			"no-index-join",
			"SELECT * FROM t LIMIT 1;",
			`SELECT * FROM \"\".\"\".t LIMIT ‹1›`,
			1,
			map[string]int{},
			map[string]int{},
		},
		{
			"index-join",
			"SELECT * FROM t WHERE other='other';",
			`"SELECT * FROM \"\".\"\".t WHERE other = ‹'other'›"`,
			1,
			map[string]int{},
			map[string]int{"IndexJoin": 1},
		},
		{
			"inner-hash-join",
			"SELECT * FROM t INNER HASH JOIN u ON t.pk = u.fk;",
			`"SELECT * FROM \"\".\"\".t INNER HASH JOIN \"\".\"\".u ON t.pk = u.fk"`,
			1,
			map[string]int{"InnerJoin": 1},
			map[string]int{"HashJoin": 1},
		},
		{
			"cross-join",
			"SELECT * FROM t CROSS JOIN u",
			`"SELECT * FROM \"\".\"\".t CROSS JOIN \"\".\"\".u"`,
			1,
			map[string]int{"InnerJoin": 1},
			map[string]int{"CrossJoin": 1},
		},
		{
			"left-hash-join",
			"SELECT * FROM t LEFT OUTER HASH JOIN u ON t.pk = u.fk;",
			`"SELECT * FROM \"\".\"\".t LEFT HASH JOIN \"\".\"\".u ON t.pk = u.fk"`,
			1,
			map[string]int{"LeftOuterJoin": 1},
			map[string]int{"HashJoin": 1},
		},
		{
			"full-hash-join",
			"SELECT * FROM t FULL OUTER HASH JOIN u ON t.pk = u.fk;",
			`"SELECT * FROM \"\".\"\".t FULL HASH JOIN \"\".\"\".u ON t.pk = u.fk"`,
			1,
			map[string]int{"FullOuterJoin": 1},
			map[string]int{"HashJoin": 1},
		},
		{
			"anti-merge-join",
			"SELECT * FROM t@t_pkey WHERE NOT EXISTS (SELECT * FROM u@fk_index WHERE t.pk = u.fk);",
			`"SELECT * FROM \"\".\"\".t@t_pkey WHERE NOT EXISTS (SELECT * FROM \"\".\"\".u@fk_index WHERE t.pk = u.fk)"`,
			1,
			map[string]int{"AntiJoin": 1},
			map[string]int{"MergeJoin": 1},
		},
		{
			"inner-lookup-join",
			"SELECT * FROM t INNER LOOKUP JOIN u ON t.pk = u.fk;",
			`"SELECT * FROM \"\".\"\".t INNER LOOKUP JOIN \"\".\"\".u ON t.pk = u.fk"`,
			1,
			map[string]int{"InnerJoin": 2},
			map[string]int{"LookupJoin": 2},
		},
		{
			"inner-merge-join",
			"SELECT * FROM t INNER MERGE JOIN u ON t.pk = u.fk;",
			`"SELECT * FROM \"\".\"\".t INNER MERGE JOIN \"\".\"\".u ON t.pk = u.fk"`,
			1,
			map[string]int{"InnerJoin": 1},
			map[string]int{"MergeJoin": 1},
		},
		{
			"inner-inverted-join",
			"SELECT * FROM t INNER INVERTED JOIN u ON t.j @> u.j;",
			`"SELECT * FROM \"\".\"\".t INNER INVERTED JOIN \"\".\"\".u ON t.j @> u.j"`,
			1,
			map[string]int{"InnerJoin": 2},
			map[string]int{"InvertedJoin": 1, "LookupJoin": 1},
		},
		{
			"semi-apply-join",
			"SELECT * FROM t WHERE col1 IN (SELECT generate_series(col1, col2) FROM u);",
			`"SELECT * FROM \"\".\"\".t WHERE col1 IN (SELECT generate_series(col1, col2) FROM \"\".\"\".u)"`,
			1,
			map[string]int{"SemiJoin": 1},
			map[string]int{"ApplyJoin": 1},
		},
		{
			"zig-zag-join",
			"SELECT * FROM t@{FORCE_ZIGZAG} WHERE t.j @> '{\"a\":\"b\"}' AND t.j @> '{\"c\":\"d\"}';",
			`"SELECT * FROM \"\".\"\".t@{FORCE_ZIGZAG} WHERE (t.j @> ‹'{\"a\":\"b\"}'›) AND (t.j @> ‹'{\"c\":\"d\"}'›)"`,
			1,
			map[string]int{"InnerJoin": 1},
			map[string]int{"ZigZagJoin": 1, "LookupJoin": 1},
		},
		{
			"intersect-all-merge-join",
			"SELECT * FROM (SELECT t.pk FROM t INTERSECT ALL SELECT u.pk FROM u) ORDER BY pk;",
			`"SELECT * FROM (SELECT t.pk FROM \"\".\"\".t INTERSECT ALL SELECT u.pk FROM \"\".\"\".u) ORDER BY pk"`,
			1,
			map[string]int{"IntersectAllJoin": 1},
			map[string]int{"MergeJoin": 1},
		},
		{
			"except-all-hash-join",
			"SELECT t.col1 FROM t EXCEPT SELECT u.fk FROM u;",
			`"SELECT t.col1 FROM \"\".\"\".t EXCEPT SELECT u.fk FROM \"\".\"\".u"`,
			1,
			map[string]int{"ExceptAllJoin": 1},
			map[string]int{"HashJoin": 1},
		},
		{
			// UNION is not implemented with a join.
			"union",
			"SELECT t.col1 FROM t UNION SELECT u.fk FROM u;",
			`"SELECT t.col1 FROM \"\".\"\".t UNION SELECT u.fk FROM \"\".\"\".u"`,
			1,
			map[string]int{},
			map[string]int{},
		},
	}

	for _, tc := range testData {
		db.Exec(t, tc.query)
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

	for _, tc := range testData {
		numLogsFound := 0
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if strings.Contains(e.Message, tc.expectedLogStatement) {
				numLogsFound++
				for joinType, count := range tc.expectedJoinTypes {
					msg := fmt.Sprintf("\"%sCount\":%d", joinType, count)
					containsJoinType := strings.Contains(e.Message, msg)
					if !containsJoinType {
						t.Errorf("%s: expected %s to be found, but found none in: %s", tc.name, msg, e.Message)
					}
				}
				for _, joinType := range []string{
					"InnerJoin", "LeftOuterJoin", "FullOuterJoin", "SemiJoin", "AntiJoin", "IntersectAllJoin",
					"ExceptAllJoin",
				} {
					if _, ok := tc.expectedJoinTypes[joinType]; !ok {
						containsJoinType := strings.Contains(e.Message, joinType)
						if containsJoinType {
							t.Errorf("%s: unexpected \"%s\" found in: %s", tc.name, joinType, e.Message)
						}
					}
				}
				for joinAlg, count := range tc.expectedJoinAlgorithms {
					msg := fmt.Sprintf("\"%sCount\":%d", joinAlg, count)
					containsJoinAlg := strings.Contains(e.Message, msg)
					if !containsJoinAlg {
						t.Errorf("%s: expected %s to be found, but found none in: %s", tc.name, msg, e.Message)
					}
				}
				for _, joinAlg := range []string{
					"HashJoin", "CrossJoin", "IndexJoin", "LookupJoin", "MergeJoin", "InvertedJoin",
					"ApplyJoin", "ZigZagJoin",
				} {
					if _, ok := tc.expectedJoinAlgorithms[joinAlg]; !ok {
						containsJoinAlg := strings.Contains(e.Message, joinAlg)
						if containsJoinAlg {
							t.Errorf("%s: unexpected \"%s\" found in: %s", tc.name, joinAlg, e.Message)
						}
					}
				}
			}
		}
		if numLogsFound != tc.expectedNumLogs {
			t.Errorf("%s: expected %d log entries, found %d", tc.name, tc.expectedNumLogs, numLogsFound)
		}
	}
}
