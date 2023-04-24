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
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestTelemetryLogging verifies that telemetry events are logged to the telemetry log
// and are sampled according to the configured sample rate.
func TestTelemetryLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallTelemetryLogFileSink(sc, t)
	defer cleanup()

	st := logtestutils.StubTime{}
	sqm := logtestutils.StubQueryStats{}
	sts := logtestutils.StubTracingStatus{}

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			EventLog: &EventLogTestingKnobs{
				// The sampling checks below need to have a deterministic
				// number of statements run by internal executor.
				SyncWrites: true,
			},
			TelemetryLoggingKnobs: &TelemetryLoggingTestingKnobs{
				getTimeNow:         st.TimeNow,
				getQueryLevelStats: sqm.QueryLevelStats,
				getTracingStatus:   sts.TracingStatus,
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
		expectedIndexes         bool
		expectedErr             string // Empty string means no error is expected.
		queryLevelStats         execstats.QueryLevelStats
		enableTracing           bool
		enableInjectTxErrors    bool
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
			expectedIndexes:         false,
			queryLevelStats: execstats.QueryLevelStats{
				ContentionTime:   0 * time.Nanosecond,
				NetworkBytesSent: 1,
				MaxMemUsage:      2,
				MaxDiskUsage:     3,
				KVBytesRead:      4,
				KVRowsRead:       5,
				NetworkMessages:  6,
			},
			enableTracing: false,
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
			expectedIndexes:         true,
			queryLevelStats: execstats.QueryLevelStats{
				ContentionTime: 1 * time.Nanosecond,
			},
			enableTracing: false,
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
			expectedIndexes:         true,
			queryLevelStats: execstats.QueryLevelStats{
				ContentionTime:   2 * time.Nanosecond,
				NetworkBytesSent: 1,
				MaxMemUsage:      2,
				NetworkMessages:  6,
			},
			enableTracing: false,
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
			expectedIndexes:         true,
			queryLevelStats: execstats.QueryLevelStats{
				ContentionTime:   3 * time.Nanosecond,
				NetworkBytesSent: 1124,
				MaxMemUsage:      132,
				MaxDiskUsage:     3,
				KVBytesRead:      4,
				KVRowsRead:       2345,
				NetworkMessages:  36,
			},
			enableTracing: false,
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
			expectedIndexes:         true,
			queryLevelStats: execstats.QueryLevelStats{
				ContentionTime:   0 * time.Nanosecond,
				NetworkBytesSent: 124235,
				MaxMemUsage:      12412,
				MaxDiskUsage:     3,
				KVRowsRead:       5,
				NetworkMessages:  6235,
			},
			enableTracing: false,
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
			expectedIndexes:         true,
			queryLevelStats: execstats.QueryLevelStats{
				ContentionTime:   0 * time.Nanosecond,
				NetworkBytesSent: 1,
				KVBytesRead:      4,
				KVRowsRead:       5,
				NetworkMessages:  6,
			},
			enableTracing: false,
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
			expectedIndexes:         false,
			expectedErr:             "a role/user named ‹root› already exists",
			enableTracing:           false,
		},
		{
			// Test case with statement that is of type DML.
			// All statements should be logged despite not exceeding the necessary elapsed time (0.1s)
			// due to tracing being enabled.
			name:                    "select-with-tracing",
			query:                   "SELECT * FROM u LIMIT 4;",
			queryNoConstants:        "SELECT * FROM u LIMIT _",
			execTimestampsSeconds:   []float64{10, 10.01, 10.02, 10.03, 10.04, 10.05},
			expectedLogStatement:    `SELECT * FROM \"\".\"\".u LIMIT ‹4›`,
			stubMaxEventFrequency:   10,
			expectedSkipped:         []int{0, 0, 0, 0, 0, 0},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        false,
			expectedStatsAvailable:  true,
			expectedRead:            true,
			expectedWrite:           false,
			expectedIndexes:         true,
			queryLevelStats: execstats.QueryLevelStats{
				ContentionTime:   2 * time.Nanosecond,
				NetworkBytesSent: 10,
				MaxMemUsage:      20,
				MaxDiskUsage:     33,
				KVBytesRead:      24,
				KVRowsRead:       55,
				NetworkMessages:  66,
			},
			enableTracing: true,
		},
		{
			name:                    "sql-transaction-error",
			query:                   "SELECT * FROM u WHERE x > 10 LIMIT 3;",
			queryNoConstants:        "SELECT * FROM u WHERE x > _ LIMIT _",
			execTimestampsSeconds:   []float64{11, 11.01, 11.02, 11.03, 11.04, 11.05},
			expectedLogStatement:    `SELECT * FROM \"\".\"\".u WHERE x > ‹10› LIMIT ‹3›`,
			stubMaxEventFrequency:   10,
			expectedSkipped:         []int{0},
			expectedUnredactedTags:  []string{"client"},
			expectedApplicationName: "telemetry-logging-test",
			expectedFullScan:        true,
			expectedStatsAvailable:  true,
			expectedRead:            true,
			expectedWrite:           false,
			expectedIndexes:         false,
			expectedErr:             "TransactionRetryWithProtoRefreshError: injected by `inject_retry_errors_enabled` session variable",
			enableTracing:           false,
			enableInjectTxErrors:    true,
		},
	}

	for _, tc := range testData {
		TelemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, tc.stubMaxEventFrequency)
		if tc.enableInjectTxErrors {
			_, err := db.DB.ExecContext(context.Background(), "SET inject_retry_errors_enabled = 'true'")
			require.NoError(t, err)
		}
		for _, execTimestamp := range tc.execTimestampsSeconds {
			stubTime := timeutil.FromUnixMicros(int64(execTimestamp * 1e6))
			st.SetTime(stubTime)
			sqm.SetQueryLevelStats(tc.queryLevelStats)
			sts.SetTracingStatus(tc.enableTracing)
			_, err := db.DB.ExecContext(context.Background(), tc.query)
			if err != nil && tc.expectedErr == "" {
				t.Errorf("unexpected error executing query `%s`: %v", tc.query, err)
			}
		}
		if tc.enableInjectTxErrors {
			_, err := db.DB.ExecContext(context.Background(), "SET inject_retry_errors_enabled = 'false'")
			require.NoError(t, err)
		}
	}

	log.FlushFileSinks()

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
					stmtFingerprintID := appstatspb.ConstructStatementFingerprintID(tc.queryNoConstants, tc.expectedErr != "", true, databaseName)
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
					contentionNanos := regexp.MustCompile("\"ContentionNanos\":[0-9]*")
					if tc.queryLevelStats.ContentionTime.Nanoseconds() > 0 && !contentionNanos.MatchString(e.Message) {
						// If we have contention, we expect the ContentionNanos field to be populated.
						t.Errorf("expected to find ContentionNanos but none was found")
					} else if tc.queryLevelStats.ContentionTime.Nanoseconds() == 0 && contentionNanos.MatchString(e.Message) {
						// If we do not have contention, expect no ContentionNanos field.
						t.Errorf("expected no ContentionNanos field, but was found")
					}
					networkBytesSent := regexp.MustCompile("\"NetworkBytesSent\":[0-9]*")
					if tc.queryLevelStats.NetworkBytesSent > 0 && !networkBytesSent.MatchString(e.Message) {
						// If we have sent network bytes, we expect the NetworkBytesSent field to be populated.
						t.Errorf("expected to find NetworkBytesSent but none was found")
					} else if tc.queryLevelStats.NetworkBytesSent == 0 && networkBytesSent.MatchString(e.Message) {
						// If we have not sent network bytes, expect no NetworkBytesSent field.
						t.Errorf("expected no NetworkBytesSent field, but was found")
					}
					maxMemUsage := regexp.MustCompile("\"MaxMemUsage\":[0-9]*")
					if tc.queryLevelStats.MaxMemUsage > 0 && !maxMemUsage.MatchString(e.Message) {
						// If we have a max memory usage, we expect the MaxMemUsage field to be populated.
						t.Errorf("expected to find MaxMemUsage but none was found")
					} else if tc.queryLevelStats.MaxMemUsage == 0 && maxMemUsage.MatchString(e.Message) {
						// If we do not have a max memory usage, expect no MaxMemUsage field.
						t.Errorf("expected no MaxMemUsage field, but was found")
					}
					maxDiskUsage := regexp.MustCompile("\"MaxDiskUsage\":[0-9]*")
					if tc.queryLevelStats.MaxDiskUsage > 0 && !maxDiskUsage.MatchString(e.Message) {
						// If we have a max disk usage, we expect the MaxDiskUsage field to be populated.
						t.Errorf("expected to find MaxDiskUsage but none was found")
					} else if tc.queryLevelStats.MaxDiskUsage == 0 && maxDiskUsage.MatchString(e.Message) {
						// If we do not a max disk usage, expect no MaxDiskUsage field.
						t.Errorf("expected no MaxDiskUsage field, but was found")
					}
					kvBytesRead := regexp.MustCompile("\"KVBytesRead\":[0-9]*")
					if tc.queryLevelStats.KVBytesRead > 0 && !kvBytesRead.MatchString(e.Message) {
						// If we have read bytes from KV, we expect the KVBytesRead field to be populated.
						t.Errorf("expected to find KVBytesRead but none was found")
					} else if tc.queryLevelStats.KVBytesRead == 0 && kvBytesRead.MatchString(e.Message) {
						// If we have not read bytes from KV, expect no KVBytesRead field.
						t.Errorf("expected no KVBytesRead field, but was found")
					}
					kvRowsRead := regexp.MustCompile("\"KVRowsRead\":[0-9]*")
					if tc.queryLevelStats.KVRowsRead > 0 && !kvRowsRead.MatchString(e.Message) {
						// If we have read rows from KV, we expect the KVRowsRead field to be populated.
						t.Errorf("expected to find KVRowsRead but none was found")
					} else if tc.queryLevelStats.KVRowsRead == 0 && kvRowsRead.MatchString(e.Message) {
						// If we have not read rows from KV, expect no KVRowsRead field.
						t.Errorf("expected no KVRowsRead field, but was found")
					}
					networkMessages := regexp.MustCompile("\"NetworkMessages\":[0-9]*")
					if tc.queryLevelStats.NetworkMessages > 0 && !networkMessages.MatchString(e.Message) {
						// If we have network messages, we expect the NetworkMessages field to be populated.
						t.Errorf("expected to find NetworkMessages but none was found")
					} else if tc.queryLevelStats.NetworkMessages == 0 && networkMessages.MatchString(e.Message) {
						// If we do not have network messages, expect no NetworkMessages field.
						t.Errorf("expected no NetworkMessages field, but was found")
					}
					if tc.expectedErr != "" {
						if !strings.Contains(e.Message, tc.expectedErr) {
							t.Errorf("%s: missing error %s in message %s", tc.name, tc.expectedErr, e.Message)
							break
						}
					}
					if tc.expectedIndexes {
						// Match indexes on any non-empty string value.
						indexes := regexp.MustCompile("\"Indexes\":")
						if !indexes.MatchString(e.Message) {
							t.Errorf("expected to find Indexes but none was found in: %s", e.Message)
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

	cleanup := logtestutils.InstallTelemetryLogFileSink(sc, t)
	defer cleanup()

	st := logtestutils.StubTime{}

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
	TelemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, stubMaxEventFrequency)

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
		st.SetTime(setTroubleshootModeTime)
		if tc.enableTroubleshootingMode {
			db.Exec(t, `SET troubleshooting_mode = true;`)
		} else {
			db.Exec(t, `SET troubleshooting_mode = false;`)
		}
		// Advance time 1 second from previous query. Ensure enough time has passed
		// from when we set troubleshooting mode for this query to be sampled.
		setQueryTime := timeutil.FromUnixMicros(int64((idx + 1) * 1e6))
		st.SetTime(setQueryTime)
		db.Exec(t, tc.query)
	}

	log.FlushFileSinks()

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

	cleanup := logtestutils.InstallTelemetryLogFileSink(sc, t)
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
	TelemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, stubMaxEventFrequency)

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

	log.FlushFileSinks()

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

// TestTelemetryScanCounts tests that scans with and without forecasted
// statistics are counted correctly. It also tests that other statistics
// forecasting telemetry is counted correctly.
func TestTelemetryScanCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallTelemetryLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	db.Exec(t, "SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;")
	db.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;")
	db.Exec(t, "CREATE TABLE d (d PRIMARY KEY) AS SELECT generate_series(10, 16);")
	db.Exec(t, "CREATE TABLE e (e PRIMARY KEY) AS SELECT generate_series(0, 19);")
	db.Exec(t, "CREATE TABLE f (f PRIMARY KEY) AS SELECT generate_series(5, 8) * 2;")
	db.Exec(t, `ALTER TABLE e INJECT STATISTICS '[
      {
          "avg_size": 1,
          "columns": [
              "e"
          ],
          "created_at": "2017-08-05 00:00:00.000000",
          "distinct_count": 20,
          "histo_buckets": [
              {
                  "distinct_range": 0,
                  "num_eq": 1,
                  "num_range": 0,
                  "upper_bound": "0"
              },
              {
                  "distinct_range": 18,
                  "num_eq": 1,
                  "num_range": 18,
                  "upper_bound": "20"
              }
          ],
          "histo_col_type": "INT8",
          "histo_version": 2,
          "name": "__auto__",
          "null_count": 0,
          "row_count": 20
      }
]';`)
	db.Exec(t, `ALTER TABLE f INJECT STATISTICS '[
      {
          "avg_size": 1,
          "columns": [
              "f"
          ],
          "created_at": "2017-05-07 00:00:00.000000",
          "distinct_count": 1,
          "histo_buckets": [
              {
                  "distinct_range": 0,
                  "num_eq": 0,
                  "num_range": 0,
                  "upper_bound": "1"
              },
              {
                  "distinct_range": 1,
                  "num_eq": 0,
                  "num_range": 1,
                  "upper_bound": "11"
              }
          ],
          "histo_col_type": "INT8",
          "histo_version": 2,
          "name": "__auto__",
          "null_count": 0,
          "row_count": 1
      },
      {
          "avg_size": 1,
          "columns": [
              "f"
          ],
          "created_at": "2017-05-08 00:00:00.000000",
          "distinct_count": 2,
          "histo_buckets": [
              {
                  "distinct_range": 0,
                  "num_eq": 0,
                  "num_range": 0,
                  "upper_bound": "3"
              },
              {
                  "distinct_range": 2,
                  "num_eq": 0,
                  "num_range": 2,
                  "upper_bound": "13"
              }
          ],
          "histo_col_type": "INT8",
          "histo_version": 2,
          "name": "__auto__",
          "null_count": 0,
          "row_count": 2
      },
      {
          "avg_size": 1,
          "columns": [
              "f"
          ],
          "created_at": "2017-05-09 00:00:00.000000",
          "distinct_count": 3,
          "histo_buckets": [
              {
                  "distinct_range": 0,
                  "num_eq": 0,
                  "num_range": 0,
                  "upper_bound": "5"
              },
              {
                  "distinct_range": 3,
                  "num_eq": 0,
                  "num_range": 3,
                  "upper_bound": "15"
              }
          ],
          "histo_col_type": "INT8",
          "histo_version": 2,
          "name": "__auto__",
          "null_count": 0,
          "row_count": 3
      }
]';`)

	testData := []struct {
		query                                 string
		logStmt                               string
		scanCount                             float64
		scanWithStatsCount                    float64
		scanWithStatsForecastCount            float64
		totalScanRowsEstimate                 float64
		totalScanRowsWithoutForecastsEstimate float64
	}{
		{
			query:   "SELECT 1",
			logStmt: "SELECT ‹1›",
		},
		{
			query:   "SELECT * FROM d WHERE true",
			logStmt: `SELECT * FROM \"\".\"\".d WHERE ‹true›`,

			scanCount: 1,
		},
		{
			query:   "SELECT * FROM e WHERE true",
			logStmt: `SELECT * FROM \"\".\"\".e WHERE ‹true›`,

			scanCount:                             1,
			scanWithStatsCount:                    1,
			totalScanRowsEstimate:                 20,
			totalScanRowsWithoutForecastsEstimate: 20,
		},
		{
			query:   "SELECT * FROM f WHERE true",
			logStmt: `SELECT * FROM \"\".\"\".f WHERE ‹true›`,

			scanCount:                             1,
			scanWithStatsCount:                    1,
			scanWithStatsForecastCount:            1,
			totalScanRowsEstimate:                 4,
			totalScanRowsWithoutForecastsEstimate: 3,
		},
		{
			query:   "SELECT * FROM d INNER HASH JOIN e ON d = e INNER HASH JOIN f ON e = f",
			logStmt: `SELECT * FROM \"\".\"\".d INNER HASH JOIN \"\".\"\".e ON d = e INNER HASH JOIN \"\".\"\".f ON e = f`,

			scanCount:                             3,
			scanWithStatsCount:                    2,
			scanWithStatsForecastCount:            1,
			totalScanRowsEstimate:                 24,
			totalScanRowsWithoutForecastsEstimate: 23,
		},
	}

	for _, tc := range testData {
		db.Exec(t, tc.query)
	}

	log.FlushFileSinks()

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

	t.Log("testcases")
cases:
	for _, tc := range testData {
		for i := len(entries) - 1; i >= 0; i-- {
			if strings.Contains(entries[i].Message, tc.logStmt) {
				var entry map[string]interface{}
				if err := json.Unmarshal([]byte(entries[i].Message), &entry); err != nil {
					t.Error(err)
					continue cases
				}
				get := func(key string) float64 {
					if val, ok := entry[key]; ok {
						return val.(float64)
					}
					return 0
				}

				if get("ScanCount") != tc.scanCount {
					t.Errorf(
						"query `%s` expected ScanCount %v, was: %v",
						tc.query, tc.scanCount, get("ScanCount"),
					)
				}
				if get("ScanWithStatsCount") != tc.scanWithStatsCount {
					t.Errorf(
						"query `%s` expected ScanWithStatsCount %v, was: %v",
						tc.query, tc.scanWithStatsCount, get("ScanWithStatsCount"),
					)
				}
				if get("ScanWithStatsForecastCount") != tc.scanWithStatsForecastCount {
					t.Errorf(
						"query `%s` expected ScanWithStatsForecastCount %v, was: %v",
						tc.query, tc.scanWithStatsForecastCount, get("ScanWithStatsForecastCount"),
					)
				}
				if get("TotalScanRowsEstimate") != tc.totalScanRowsEstimate {
					t.Errorf(
						"query `%s` expected TotalScanRowsEstimate %v, was: %v",
						tc.query, tc.totalScanRowsEstimate, get("TotalScanRowsEstimate"),
					)
				}
				if get("TotalScanRowsWithoutForecastsEstimate") != tc.totalScanRowsWithoutForecastsEstimate {
					t.Errorf(
						"query `%s` expected TotalScanRowsWithoutForecastsEstimate %v, was: %v",
						tc.query, tc.totalScanRowsWithoutForecastsEstimate, get("TotalScanRowsWithoutForecastsEstimate"),
					)
				}
				if tc.scanWithStatsForecastCount > 0 {
					if get("NanosSinceStatsForecasted") <= 0 {
						t.Errorf(
							"query `%s` expected NanosSinceStatsForecasted > 0, was: %v",
							tc.query, get("NanosSinceStatsForecasted"),
						)
					}
					if get("NanosSinceStatsForecasted") >= get("NanosSinceStatsCollected") {
						t.Errorf(
							"query `%s` expected NanosSinceStatsForecasted < NanosSinceStatsCollected: %v, %v",
							tc.query, get("NanosSinceStatsForecasted"), get("NanosSinceStatsCollected"),
						)
					}
				}
				continue cases
			}
		}
		t.Errorf("couldn't find log entry containing `%s`", tc.logStmt)
	}
}

func TestFunctionBodyRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallTelemetryLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	db.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)
	db.Exec(t, `CREATE TABLE kv (k STRING, v INT)`)
	stubMaxEventFrequency := int64(1000000)
	TelemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, stubMaxEventFrequency)

	stmt := `CREATE FUNCTION f() RETURNS INT 
LANGUAGE SQL 
AS $$ 
SELECT k FROM kv WHERE v = 1;
SELECT v FROM kv WHERE k = 'Foo';
$$`

	expectedLogStmt := `CREATE FUNCTION defaultdb.public.f()\n\tRETURNS INT8\n\tLANGUAGE SQL\n\tAS $$SELECT k FROM defaultdb.public.kv WHERE v = ‹1›; SELECT v FROM defaultdb.public.kv WHERE k = ‹'Foo'›;$$`

	db.Exec(t, stmt)

	log.FlushFileSinks()

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

	numLogsFound := 0
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if strings.Contains(e.Message, expectedLogStmt) {
			numLogsFound++
		}
	}
	if numLogsFound != 1 {
		t.Errorf("expected 1 log entries, found %d", numLogsFound)
	}
}
