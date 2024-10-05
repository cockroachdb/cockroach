// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
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

	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_TELEMETRY)
	defer cleanup()

	firstScheduleLoggingDuration := 1 * time.Second
	sd := stubDurations{}
	sd.setLoggingDuration(firstScheduleLoggingDuration)
	sd.setOverlapDuration(time.Second)
	stubScheduleInterval := 2 * time.Second
	stubScheduleCheckEnabledInterval := time.Second
	stubLoggingDelay := 0 * time.Second

	schedulesFinishedChan := make(chan struct{})
	logsVerifiedChan := make(chan struct{})

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			CapturedIndexUsageStatsKnobs: &CaptureIndexUsageStatsTestingKnobs{
				getLoggingDuration: sd.getLoggingDuration,
				getOverlapDuration: sd.getOverlapDuration,
				onScheduleComplete: func() {
					// Notify that the schedule has completed.
					schedulesFinishedChan <- struct{}{}
					// Wait for logs to be verified before proceeding.
					<-logsVerifiedChan
				},
			},
		},
	})
	defer srv.Stopper().Stop(context.Background())
	// Close must be called before the server stops, or we will block indefinitely in the callback above.
	defer close(logsVerifiedChan)

	numTenants := 1
	if srv.TenantController().StartedDefaultTestTenant() {
		numTenants = 2
	}

	waitForCompletedSchedules := func(count int) {
		for i := 0; i < count; i++ {
			<-schedulesFinishedChan
		}
	}

	tenantSettings := srv.ApplicationLayer().ClusterSettings()

	// Configure capture index usage statistics to be disabled after the first schedule runs.
	// This is to test whether the disabled interval works correctly. We will disable the the setting for
	// the system tenant (if multi-tenant). Because the index usage stats scheduler starts immediately
	// on server startup, we wait for the first schedule of each tenant to execute before setting all
	// necessary cluster settings.
	waitForCompletedSchedules(numTenants)
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &tenantSettings.SV, false)
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &srv.SystemLayer().ClusterSettings().SV, false)
	// Configure the schedule interval at which we capture index usage statistics.
	telemetryCaptureIndexUsageStatsInterval.Override(context.Background(), &tenantSettings.SV, stubScheduleInterval)
	// Configure the schedule interval at which we check whether capture index
	// usage statistics has been enabled.
	telemetryCaptureIndexUsageStatsStatusCheckEnabledInterval.Override(context.Background(), &tenantSettings.SV,
		stubScheduleCheckEnabledInterval)
	// Configure the delay between each emission of index usage stats logs.
	telemetryCaptureIndexUsageStatsLoggingDelay.Override(context.Background(), &tenantSettings.SV, stubLoggingDelay)

	// Allow all tenants to continue.
	logsVerifiedChan <- struct{}{}
	if srv.TenantController().StartedDefaultTestTenant() {
		logsVerifiedChan <- struct{}{}
	}

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

	// Enable capture of index usage stats.
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &tenantSettings.SV, true)

	// Check that telemetry log file contains all the entries we're expecting, at the scheduled intervals.
	expectedTotalNumEntriesInSingleInterval := 8
	expectedNumberOfIndividualIndexEntriesInSingleInterval := 1

	// Wait for channel value from end of 1st schedule.
	waitForCompletedSchedules(1)

	// Check the expected number of entries from the 1st schedule.
	require.NoError(t, checkNumTotalEntriesAndNumIndexEntries(t,
		expectedIndexNames,
		expectedTotalNumEntriesInSingleInterval,
		expectedNumberOfIndividualIndexEntriesInSingleInterval,
	), "error encountered checking the number of total entries and number of index entries")

	// Set the logging duration for the next run to be longer than the schedule
	// interval duration.
	stubLoggingDuration := stubScheduleInterval * 2
	sd.setLoggingDuration(stubLoggingDuration)

	// Allow schedules to proceed.
	logsVerifiedChan <- struct{}{}

	// Expect number of total entries to hold 2 times the number of entries in a
	// single interval.
	expectedTotalNumEntriesAfterTwoIntervals := expectedTotalNumEntriesInSingleInterval * 2
	// Expect number of individual index entries to hold 2 times the number of
	// entries in a single interval.
	expectedNumberOfIndividualIndexEntriesAfterTwoIntervals := expectedNumberOfIndividualIndexEntriesInSingleInterval * 2

	// Wait for channel value from end of 2nd schedule.
	waitForCompletedSchedules(1)

	// Check the expected number of entries from the 2nd schedule.
	require.NoError(t, checkNumTotalEntriesAndNumIndexEntries(t,
		expectedIndexNames,
		expectedTotalNumEntriesAfterTwoIntervals,
		expectedNumberOfIndividualIndexEntriesAfterTwoIntervals,
	), "error encountered checking the number of total entries and number of index entries")

	logsVerifiedChan <- struct{}{}

	// Expect number of total entries to hold 3 times the number of entries in a
	// single interval.
	expectedTotalNumEntriesAfterThreeIntervals := expectedTotalNumEntriesInSingleInterval * 3
	// Expect number of individual index entries to hold 3 times the number of
	// entries in a single interval.
	expectedNumberOfIndividualIndexEntriesAfterThreeIntervals := expectedNumberOfIndividualIndexEntriesInSingleInterval * 3

	// Wait for channel value from end of 3rd schedule.
	waitForCompletedSchedules(1)

	// Check the expected number of entries from the 3rd schedule.
	require.NoError(t, checkNumTotalEntriesAndNumIndexEntries(t,
		expectedIndexNames,
		expectedTotalNumEntriesAfterThreeIntervals,
		expectedNumberOfIndividualIndexEntriesAfterThreeIntervals,
	), "error encountered checking the number of total entries and number of index entries")

	// Stop capturing index usage statistics.
	telemetryCaptureIndexUsageStatsEnabled.Override(context.Background(), &tenantSettings.SV, false)

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
) error {
	log.FlushFiles()
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

	countByIndex := make(map[string]int)

	for _, e := range entries {
		t.Logf("checking entry: %v", e)
		// Check that the entry has a tag for a node ID of 1.
		if !strings.Contains(e.Tags, `n1`) && !strings.Contains(e.Tags, `nsql1`) {
			t.Fatalf("expected the entry's tags to include n1, but include got %s", e.Tags)
		}

		var s struct {
			IndexName string `json:"IndexName"`
		}
		err := json.Unmarshal([]byte(e.Message), &s)
		if err != nil {
			t.Fatal(err)
		}
		countByIndex[s.IndexName] = countByIndex[s.IndexName] + 1
	}

	t.Logf("found index counts: %+v", countByIndex)

	if expected, actual := expectedTotalEntries/expectedIndividualIndexEntries, len(countByIndex); actual != expected {
		return errors.Newf("expected %d indexes, got %d", expected, actual)
	}

	for idxName, c := range countByIndex {
		if c != expectedIndividualIndexEntries {
			return errors.Newf("for index %s: expected entry count %d, got %d",
				idxName, expectedIndividualIndexEntries, c)
		}
	}

	for _, idxName := range expectedIndexNames {
		if _, ok := countByIndex[idxName]; !ok {
			return errors.Newf("no entry found for index %s", idxName)
		}
	}
	return nil
}
