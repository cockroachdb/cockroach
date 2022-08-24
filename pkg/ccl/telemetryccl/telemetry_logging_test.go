// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package telemetryccl

import (
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/errors"
)

func TestTelemetryLogRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallTelemetryLogFileSink(sc, t)
	defer cleanup()

	_, db, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{},
		multiregionccltestutils.WithReplicationMode(base.ReplicationManual),
	)
	defer cleanup()
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create three tables, with each table touching one, two, and three
	// regions, respectively.
	sqlDB.Exec(t, `CREATE TABLE one_region (k INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO one_region SELECT generate_series(1, 1)`)
	sqlDB.Exec(t, `ALTER TABLE one_region SPLIT AT SELECT generate_series(1, 1)`)
	sqlDB.Exec(t, "ALTER TABLE one_region EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 1)")
	sqlDB.Exec(t, `CREATE TABLE two_regions (k INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO two_regions SELECT generate_series(1, 2)`)
	sqlDB.Exec(t, `ALTER TABLE two_regions SPLIT AT SELECT generate_series(1, 2)`)
	sqlDB.Exec(t, "ALTER TABLE two_regions EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 1), (ARRAY[2], 2)")
	sqlDB.Exec(t, `CREATE TABLE three_regions (k INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO three_regions SELECT generate_series(1, 3)`)
	sqlDB.Exec(t, `ALTER TABLE three_regions SPLIT AT SELECT generate_series(1, 3)`)
	sqlDB.Exec(t, "ALTER TABLE three_regions EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 1), (ARRAY[2], 2), (ARRAY[3], 3)")

	// Enable the telemetry logging and increase the sampling frequency so that
	// all statements are captured.
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.max_event_frequency = 1000000`)

	testData := []struct {
		name                 string
		query                string
		expectedLogStatement string
		expectedRegions      []string
	}{
		{
			name:                 "one-region",
			query:                "SELECT * FROM one_region",
			expectedLogStatement: `SELECT * FROM \"\".\"\".one_region`,
			expectedRegions:      []string{"us-east1"},
		},
		{
			name:                 "two-regions",
			query:                "SELECT * FROM two_regions",
			expectedLogStatement: `SELECT * FROM \"\".\"\".two_regions`,
			expectedRegions:      []string{"us-east1", "us-east2"},
		},
		{
			name:                 "three-regions",
			query:                "SELECT * FROM three_regions",
			expectedLogStatement: `SELECT * FROM \"\".\"\".three_regions`,
			expectedRegions:      []string{"us-east1", "us-east2", "us-east3"},
		},
	}

	for _, tc := range testData {
		sqlDB.Exec(t, tc.query)
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
		var logEntriesCount int
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if strings.Contains(e.Message, tc.expectedLogStatement) {
				logEntriesCount++
				for _, region := range tc.expectedRegions {
					if !strings.Contains(e.Message, region) {
						t.Errorf("didn't find region %q in the log entry %s", region, e.Message)
					}
				}
			}
		}
		if logEntriesCount != 1 {
			t.Errorf("expected to find a single entry for %q: %v", tc.name, entries)
		}
	}
}
