// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scheduledlogging

import (
	"context"
	"encoding/json"
	"math"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type stubDurations struct {
	syncutil.RWMutex
	loggingDuration time.Duration
	overlapDuration time.Duration
}

func (s *stubDurations) setLoggingDuration(d time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.loggingDuration = d
}

func (s *stubDurations) getLoggingDuration() time.Duration {
	s.RLock()
	defer s.RUnlock()
	return s.loggingDuration
}

func (s *stubDurations) setOverlapDuration(d time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.overlapDuration = d
}

func (s *stubDurations) getOverlapDuration() time.Duration {
	s.RLock()
	defer s.RUnlock()
	return s.overlapDuration
}

func TestCaptureIndexUsageStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallTelemetryLogFileSink(sc, t)
	defer cleanup()

	firstScheduleLoggingDuration := 1 * time.Second
	sd := stubDurations{}
	sd.setLoggingDuration(firstScheduleLoggingDuration)
	sd.setOverlapDuration(time.Second)
	stubScheduleInterval := 2 * time.Second
	stubScheduleCheckEnabledInterval := time.Second
	stubLoggingDelay := 0 * time.Second

	// timeBuffer is a short time buffer to account for non-determinism in the logging timings.
	const timeBuffer = 3 * time.Second

	settings := cluster.MakeTestingClusterSettings()
	// Configure capture index usage statistics to be disabled. This is to test
	// whether the disabled interval works correctly. We start in a disabled
	// state, once the disabled interval expires, we check whether we have
	// transitioned to an enabled state, if we have, we check that the expected
	// logs have been emitted.
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &settings.SV, false)
	// Configure the schedule interval at which we capture index usage
	// statistics.
	telemetryCaptureIndexUsageStatsInterval.Override(context.Background(), &settings.SV, stubScheduleInterval)
	// Configure the schedule interval at which we check whether capture index
	// usage statistics has been enabled.
	telemetryCaptureIndexUsageStatsStatusCheckEnabledInterval.Override(context.Background(), &settings.SV, stubScheduleCheckEnabledInterval)
	// Configure the delay between each emission of index usage stats logs.
	telemetryCaptureIndexUsageStatsLoggingDelay.Override(context.Background(), &settings.SV, stubLoggingDelay)

	scheduleCompleteChan := make(chan struct{})

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			CapturedIndexUsageStatsKnobs: &CaptureIndexUsageStatsTestingKnobs{
				getLoggingDuration: sd.getLoggingDuration,
				getOverlapDuration: sd.getOverlapDuration,
				onScheduleComplete: func() {
					scheduleCompleteChan <- struct{}{}
					<-scheduleCompleteChan
				},
			},
		},
	})

	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	// Create test databases.
	db.Exec(t, "CREATE DATABASE test")
	db.Exec(t, "CREATE DATABASE test2")

	// Test fix for #85577.
	db.Exec(t, `CREATE DATABASE "mIxEd-CaSe""woo☃"`)
	db.Exec(t, `CREATE DATABASE "index"`)

	// Create a table for each database.
	db.Exec(t, "CREATE TABLE test.test_table (num INT PRIMARY KEY, letter char)")
	db.Exec(t, "CREATE TABLE test2.test2_table (num INT PRIMARY KEY, letter char)")
	db.Exec(t, `CREATE TABLE "mIxEd-CaSe""woo☃"."sPe-CiAl✔" (num INT PRIMARY KEY, "HeLlO☀" char)`)
	db.Exec(t, `CREATE TABLE "index"."index" (num INT PRIMARY KEY, "index" char)`)

	// Create an index on each created table (each table now has two indices:
	// primary and this one)
	db.Exec(t, `CREATE INDEX ON test.test_table (letter)`)
	db.Exec(t, `CREATE INDEX ON test2.test2_table (letter)`)
	db.Exec(t, `CREATE INDEX "IdX✏" ON "mIxEd-CaSe""woo☃"."sPe-CiAl✔" ("HeLlO☀")`)
	db.Exec(t, `CREATE INDEX "index" ON "index"."index" ("index")`)

	expectedIndexNames := []string{
		"test2_table_letter_idx",
		"test2_table_pkey",
		"test_table_letter_idx",
		"test_table_pkey",
		"sPe-CiAl✔_pkey",
		"IdX✏",
		"index",
		"index_pkey",
	}

	// Check that telemetry log file contains all the entries we're expecting, at the scheduled intervals.

	// Enable capture of index usage stats.
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &s.ClusterSettings().SV, true)

	expectedTotalNumEntriesInSingleInterval := 8
	expectedNumberOfIndividualIndexEntriesInSingleInterval := 1

	// Wait for channel value from end of 1st schedule.
	<-scheduleCompleteChan

	// Check the expected number of entries from the 1st schedule.
	require.NoError(t, checkNumTotalEntriesAndNumIndexEntries(t,
		expectedIndexNames,
		expectedTotalNumEntriesInSingleInterval,
		expectedNumberOfIndividualIndexEntriesInSingleInterval,
		scheduleCompleteChan,
	), "error encountered checking the number of total entries and number of index entries")

	scheduleCompleteChan <- struct{}{}

	// Expect number of total entries to hold 2 times the number of entries in a
	// single interval.
	expectedTotalNumEntriesAfterTwoIntervals := expectedTotalNumEntriesInSingleInterval * 2
	// Expect number of individual index entries to hold 2 times the number of
	// entries in a single interval.
	expectedNumberOfIndividualIndexEntriesAfterTwoIntervals := expectedNumberOfIndividualIndexEntriesInSingleInterval * 2
	// Set the logging duration for the next run to be longer than the schedule
	// interval duration.
	stubLoggingDuration := stubScheduleInterval * 2
	sd.setLoggingDuration(stubLoggingDuration)

	// Wait for channel value from end of 2nd schedule.
	<-scheduleCompleteChan

	// Check the expected number of entries from the 2nd schedule.
	require.NoError(t, checkNumTotalEntriesAndNumIndexEntries(t,
		expectedIndexNames,
		expectedTotalNumEntriesAfterTwoIntervals,
		expectedNumberOfIndividualIndexEntriesAfterTwoIntervals,
		scheduleCompleteChan,
	), "error encountered checking the number of total entries and number of index entries")

	scheduleCompleteChan <- struct{}{}

	// Expect number of total entries to hold 3 times the number of entries in a
	// single interval.
	expectedTotalNumEntriesAfterThreeIntervals := expectedTotalNumEntriesInSingleInterval * 3
	// Expect number of individual index entries to hold 3 times the number of
	// entries in a single interval.
	expectedNumberOfIndividualIndexEntriesAfterThreeIntervals := expectedNumberOfIndividualIndexEntriesInSingleInterval * 3

	// Wait for channel value from end of 3rd schedule.
	<-scheduleCompleteChan

	// Check the expected number of entries from the 3rd schedule.
	require.NoError(t, checkNumTotalEntriesAndNumIndexEntries(t,
		expectedIndexNames,
		expectedTotalNumEntriesAfterThreeIntervals,
		expectedNumberOfIndividualIndexEntriesAfterThreeIntervals,
		scheduleCompleteChan,
	), "error encountered checking the number of total entries and number of index entries")

	// Stop capturing index usage statistics.
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &settings.SV, false)

	scheduleCompleteChan <- struct{}{}

	// Iterate through entries, ensure that the timestamp difference between each
	// schedule is as expected.
	startTimestamp := int64(0)
	endTimestamp := int64(math.MaxInt64)
	maxEntries := 10000
	entries, err := log.FetchEntriesFromFiles(
		startTimestamp,
		endTimestamp,
		maxEntries,
		regexp.MustCompile(`"EventType":"captured_index_usage_stats"`),
		log.WithMarkedSensitiveData,
	)

	require.NoError(t, err, "expected no error fetching entries from files")

	// Sort slice by timestamp, ascending order.
	sort.Slice(entries, func(a int, b int) bool {
		return entries[a].Time < entries[b].Time
	})

	testData := []time.Duration{
		0 * time.Second,
		// the difference in number of seconds between first and second schedule
		stubScheduleInterval - firstScheduleLoggingDuration,
		// the difference in number of seconds between second and third schedule
		sd.getOverlapDuration(),
	}

	var (
		previousTimestamp = int64(0)
		currentTimestamp  = int64(0)
	)

	// Check the timestamp differences between schedules.
	for idx, expectedDuration := range testData {
		entriesLowerBound := idx * expectedTotalNumEntriesInSingleInterval
		entriesUpperBound := (idx + 1) * expectedTotalNumEntriesInSingleInterval
		scheduleEntryBlock := entries[entriesLowerBound:entriesUpperBound]
		// Take the first log entry from the schedule.
		currentTimestamp = scheduleEntryBlock[0].Time
		// If this is the first iteration, initialize the previous timestamp.
		if idx == 0 {
			previousTimestamp = currentTimestamp
		}

		actualDuration := time.Duration(currentTimestamp - previousTimestamp)
		// Use a time window to afford some non-determinism in the test.
		require.Greaterf(t, expectedDuration, actualDuration-timeBuffer, "%v <= %v-%v", expectedDuration, actualDuration, timeBuffer)
		require.Greater(t, actualDuration+timeBuffer, expectedDuration, "%v+%v <= %v", expectedDuration, actualDuration, timeBuffer)
		previousTimestamp = currentTimestamp
	}
}

// checkNumTotalEntriesAndNumIndexEntries is a helper function that verifies that
// we are getting the correct number of total log entries and correct number of
// log entries for each index. Also checks that each log entry contains a node_id
// field, used to filter node-duplicate logs downstream.
func checkNumTotalEntriesAndNumIndexEntries(
	t *testing.T,
	expectedIndexNames []string,
	expectedTotalEntries int,
	expectedIndividualIndexEntries int,
	scheduleCompleteChan chan struct{},
) error {
	log.Flush()
	// Fetch log entries.
	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"captured_index_usage_stats"`),
		log.WithMarkedSensitiveData,
	)
	if err != nil {
		close(scheduleCompleteChan)
		return err
	}

	// Assert that we have the correct number of entries.
	if expectedTotalEntries != len(entries) {
		close(scheduleCompleteChan)
		return errors.Newf("expected %d total entries, got %d", expectedTotalEntries, len(entries))
	}

	countByIndex := make(map[string]int)

	for _, e := range entries {
		t.Logf("checking entry: %v", e)
		// Check that the entry has a tag for a node ID of 1.
		if !strings.Contains(e.Tags, `n1`) {
			t.Fatalf("expected the entry's tags to include n1, but include got %s", e.Tags)
		}

		var s struct {
			IndexName string `json:"IndexName"`
		}
		err := json.Unmarshal([]byte(e.Message), &s)
		if err != nil {
			close(scheduleCompleteChan)
			t.Fatal(err)
		}
		countByIndex[s.IndexName] = countByIndex[s.IndexName] + 1
	}

	t.Logf("found index counts: %+v", countByIndex)

	if expected, actual := expectedTotalEntries/expectedIndividualIndexEntries, len(countByIndex); actual != expected {
		close(scheduleCompleteChan)
		return errors.Newf("expected %d indexes, got %d", expected, actual)
	}

	for idxName, c := range countByIndex {
		if c != expectedIndividualIndexEntries {
			close(scheduleCompleteChan)
			return errors.Newf("for index %s: expected entry count %d, got %d",
				idxName, expectedIndividualIndexEntries, c)
		}
	}

	for _, idxName := range expectedIndexNames {
		if _, ok := countByIndex[idxName]; !ok {
			close(scheduleCompleteChan)
			return errors.Newf("no entry found for index %s", idxName)
		}
	}
	return nil
}
