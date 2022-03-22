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
	"math"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
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

func installTelemetryLogFileSink(t *testing.T, sc *log.TestLogScope) func() {
	// Enable logging channels.
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	// Make a sink for just the session log.
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"telemetry": {
			Channels: logconfig.SelectChannels(channel.TELEMETRY),
		}}
	dir := sc.GetDirectory()
	require.NoError(t, cfg.Validate(&dir), "expected no errors validating log config")
	cleanup, err := log.ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return cleanup
}

func TestCaptureIndexUsageStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installTelemetryLogFileSink(t, sc)
	defer cleanup()

	sd := stubDurations{}
	sd.setLoggingDuration(1 * time.Second)
	sd.setOverlapDuration(10 * time.Second)
	stubScheduleInterval := 20 * time.Second
	stubScheduleCheckEnabledInterval := 1 * time.Second
	stubLoggingDelay := 0 * time.Second

	// timeBuffer is a short time buffer to account for delays in the schedule
	// timings when running tests. The time buffer is smaller than the difference
	// between each schedule interval to ensure that there is no overlap.
	timeBuffer := 5 * time.Second

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

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			CapturedIndexUsageStatsKnobs: &CaptureIndexUsageStatsTestingKnobs{
				getLoggingDuration: sd.getLoggingDuration,
				getOverlapDuration: sd.getOverlapDuration,
			},
		},
	})

	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	// Create test databases.
	db.Exec(t, "CREATE DATABASE test")
	db.Exec(t, "CREATE DATABASE test2")

	// Create a table for each database.
	db.Exec(t, "CREATE TABLE test.test_table (num INT PRIMARY KEY, letter char)")
	db.Exec(t, "CREATE TABLE test2.test2_table (num INT PRIMARY KEY, letter char)")

	// Create an index on each created table (each table now has two indices:
	// primary and this one)
	db.Exec(t, "CREATE INDEX ON test.test_table (letter)")
	db.Exec(t, "CREATE INDEX ON test2.test2_table (letter)")

	// Check that telemetry log file contains all the entries we're expecting, at the scheduled intervals.

	// Enable capture of index usage stats.
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &s.ClusterSettings().SV, true)

	expectedTotalNumEntriesInSingleInterval := 4
	expectedNumberOfIndividualIndexEntriesInSingleInterval := 1

	// Expect index usage statistics logs once the schedule disabled interval has passed.
	// Assert that we have the expected number of total logs and expected number
	// of logs for each index.
	testutils.SucceedsWithin(t, func() error {
		return checkNumTotalEntriesAndNumIndexEntries(
			expectedTotalNumEntriesInSingleInterval,
			expectedNumberOfIndividualIndexEntriesInSingleInterval,
		)
	}, stubScheduleCheckEnabledInterval+timeBuffer)

	// Verify that a second schedule has run after the enabled interval has passed.
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

	// Expect index usage statistics logs once the schedule enabled interval has passed.
	// Assert that we have the expected number of total logs and expected number
	// of logs for each index.
	testutils.SucceedsWithin(t, func() error {
		return checkNumTotalEntriesAndNumIndexEntries(
			expectedTotalNumEntriesAfterTwoIntervals,
			expectedNumberOfIndividualIndexEntriesAfterTwoIntervals,
		)
	}, stubScheduleInterval+timeBuffer)

	// Verify that a third schedule has run after the overlap duration has passed.
	// Expect number of total entries to hold 3 times the number of entries in a
	// single interval.
	expectedTotalNumEntriesAfterThreeIntervals := expectedTotalNumEntriesInSingleInterval * 3
	// Expect number of individual index entries to hold 3 times the number of
	// entries in a single interval.
	expectedNumberOfIndividualIndexEntriesAfterThreeIntervals := expectedNumberOfIndividualIndexEntriesInSingleInterval * 3

	// Assert that we have the expected number of total logs and expected number
	// of logs for each index.
	testutils.SucceedsWithin(t, func() error {
		return checkNumTotalEntriesAndNumIndexEntries(
			expectedTotalNumEntriesAfterThreeIntervals,
			expectedNumberOfIndividualIndexEntriesAfterThreeIntervals,
		)
	}, sd.getOverlapDuration()+timeBuffer)
	// Stop capturing index usage statistics.
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &settings.SV, false)

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
		stubScheduleInterval,
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

		nanoSecondDiff := currentTimestamp - previousTimestamp
		// We allow for integer division to remove any miscellaneous nanosecond
		// delay from the logging.
		secondDiff := nanoSecondDiff / 1e9
		actualDuration := time.Duration(secondDiff) * time.Second
		require.Equal(t, expectedDuration, actualDuration)
		previousTimestamp = currentTimestamp
	}
}

// checkNumTotalEntriesAndNumIndexEntries is a helper function that verifies that
// we are getting the correct number of total log entries and correct number of
// log entries for each index. Also checks that each log entry contains a node_id
// field, used to filter node-duplicate logs downstream.
func checkNumTotalEntriesAndNumIndexEntries(
	expectedTotalEntries int, expectedIndividualIndexEntries int,
) error {
	// Fetch log entries.
	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"captured_index_usage_stats"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		return err
	}

	// Assert that we have the correct number of entries.
	if expectedTotalEntries != len(entries) {
		return errors.Newf("expected %d total entries, got %d", expectedTotalEntries, len(entries))
	}

	var (
		numEntriesForTestTablePrimaryKeyIndex  int
		numEntriesForTestTableLetterIndex      int
		numEntriesForTest2TablePrimaryKeyIndex int
		numEntriesForTest2TableLetterIndex     int
	)

	for _, e := range entries {
		if strings.Contains(e.Message, `"IndexName":"‹test_table_pkey›"`) {
			numEntriesForTestTablePrimaryKeyIndex++
		}
		if strings.Contains(e.Message, `"IndexName":"‹test_table_letter_idx›"`) {
			numEntriesForTestTableLetterIndex++
		}
		if strings.Contains(e.Message, `"TableName":"‹test2_table_pkey›"`) {
			numEntriesForTest2TablePrimaryKeyIndex++
		}
		if strings.Contains(e.Message, `"TableName":"‹test2_table_letter_idx›"`) {
			numEntriesForTest2TableLetterIndex++
		}
		// Check that the entry has a tag for a node ID of 1.
		if !strings.Contains(e.Tags, `n1`) {
			return errors.Newf("expected the entry's tags to include n1, but include got %s", e.Tags)
		}
	}

	// Assert that we have the correct number index usage statistic entries for
	// each index we created across the tables in each database.
	if expectedIndividualIndexEntries != numEntriesForTestTablePrimaryKeyIndex {
		return errors.Newf("expected %d test_table primary key index entries, got %d", expectedIndividualIndexEntries, numEntriesForTestTablePrimaryKeyIndex)
	}
	if expectedIndividualIndexEntries != numEntriesForTestTablePrimaryKeyIndex {
		return errors.Newf("expected %d test_table letter index entries, got %d", expectedIndividualIndexEntries, numEntriesForTestTableLetterIndex)
	}
	if expectedIndividualIndexEntries != numEntriesForTestTablePrimaryKeyIndex {
		return errors.Newf("expected %d test2_table primary key index entries, got %d", expectedIndividualIndexEntries, numEntriesForTest2TablePrimaryKeyIndex)
	}
	if expectedIndividualIndexEntries != numEntriesForTestTablePrimaryKeyIndex {
		return errors.Newf("expected %d test2_table letter index entries, got %d", expectedIndividualIndexEntries, numEntriesForTest2TableLetterIndex)
	}
	return nil
}
