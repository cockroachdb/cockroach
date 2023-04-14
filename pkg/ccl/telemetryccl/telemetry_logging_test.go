// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package telemetryccl

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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

	// Enable the sampling of all statements so that execution statistics
	// (including the regions information) is collected.
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.txn_stats.sample_rate = 1.0`)

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

type expectedRecoveryEvent struct {
	recoveryType string
	bulkJobId    uint64
	numRows      int64
}

type expectedSampleQueryEvent struct {
	eventType string
	stmt      string
}

// TODO(janexing): add event telemetry tests for failed or canceled bulk jobs.
func TestBulkJobTelemetryLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	ctx := context.Background()

	cleanup := logtestutils.InstallTelemetryLogFileSink(sc, t)
	defer cleanup()

	st := logtestutils.StubTime{}
	sqm := logtestutils.StubQueryStats{}
	sts := logtestutils.StubTracingStatus{}

	dir, dirCleanupFn := testutils.TempDir(t)

	testCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				EventLog: &sql.EventLogTestingKnobs{
					// The sampling checks below need to have a deterministic
					// number of statements run by internal executor.
					SyncWrites: true,
				},
				TelemetryLoggingKnobs: sql.NewTelemetryLoggingTestingKnobs(st.TimeNow, sqm.QueryLevelStats, sts.TracingStatus),
			},
			ExternalIODir: dir,
		},
	})
	sqlDB := testCluster.ServerConn(0)
	defer func() {
		testCluster.Stopper().Stop(context.Background())
		dirCleanupFn()
	}()

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)

	db.Exec(t, "CREATE TABLE a(x int);")

	// data is to be imported into the table a.
	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()
	data = "100\n200\n300"

	// mydb is to be back-uped and restored.
	db.Exec(t, "CREATE DATABASE mydb;")
	db.Exec(t, "CREATE TABLE mydb.public.t1 (x int);")
	db.Exec(t, "INSERT INTO mydb.public.t1 VALUES (1), (2), (3);")

	testData := []struct {
		name             string
		query            string
		recoveryEvent    expectedRecoveryEvent
		sampleQueryEvent expectedSampleQueryEvent
	}{
		{
			name:  "import",
			query: fmt.Sprintf(`IMPORT INTO a CSV DATA ('%s')`, srv.URL),
			sampleQueryEvent: expectedSampleQueryEvent{
				eventType: "import",
				stmt:      fmt.Sprintf(`IMPORT INTO defaultdb.public.a CSV DATA ('%s')`, srv.URL),
			},
			recoveryEvent: expectedRecoveryEvent{
				numRows:      3,
				recoveryType: "import_job",
			},
		},
		{
			name:  "import-with-detached",
			query: fmt.Sprintf(`IMPORT INTO a CSV DATA ('%s') WITH detached`, srv.URL),
			sampleQueryEvent: expectedSampleQueryEvent{
				eventType: "import",
				stmt:      fmt.Sprintf(`IMPORT INTO defaultdb.public.a CSV DATA ('%s') WITH detached`, srv.URL),
			},
			recoveryEvent: expectedRecoveryEvent{
				numRows:      3,
				recoveryType: "import_job",
			},
		},
		{
			name:  "backup",
			query: fmt.Sprintf(`BACKUP DATABASE mydb INTO '%s'`, nodelocal.MakeLocalStorageURI("test1")),
			sampleQueryEvent: expectedSampleQueryEvent{
				eventType: "backup",
				stmt:      fmt.Sprintf(`BACKUP DATABASE mydb INTO '%s'`, nodelocal.MakeLocalStorageURI("test1")),
			},
			recoveryEvent: expectedRecoveryEvent{
				numRows:      3,
				recoveryType: "backup_job",
			},
		},
		{
			name:  "backup-with-detached",
			query: fmt.Sprintf(`BACKUP DATABASE mydb INTO '%s' WITH detached`, nodelocal.MakeLocalStorageURI("test1")),
			sampleQueryEvent: expectedSampleQueryEvent{
				eventType: "backup",
				stmt:      fmt.Sprintf(`BACKUP DATABASE mydb INTO '%s' WITH detached`, nodelocal.MakeLocalStorageURI("test1")),
			},
			recoveryEvent: expectedRecoveryEvent{
				numRows:      3,
				recoveryType: "backup_job",
			},
		},
		{
			name:  "restore",
			query: fmt.Sprintf(`RESTORE DATABASE mydb FROM LATEST IN '%s'`, nodelocal.MakeLocalStorageURI("test1")),
			sampleQueryEvent: expectedSampleQueryEvent{
				eventType: "restore",
				stmt:      fmt.Sprintf(`RESTORE DATABASE mydb FROM 'latest' IN '%s'`, nodelocal.MakeLocalStorageURI("test1")),
			},
			recoveryEvent: expectedRecoveryEvent{
				numRows:      3,
				recoveryType: "restore_job",
			},
		},
		{
			name:  "restore-with-detached",
			query: fmt.Sprintf(`RESTORE DATABASE mydb FROM LATEST IN '%s' WITH detached`, nodelocal.MakeLocalStorageURI("test1")),
			sampleQueryEvent: expectedSampleQueryEvent{
				eventType: "restore",
				stmt:      fmt.Sprintf(`RESTORE DATABASE mydb FROM 'latest' IN '%s' WITH detached`, nodelocal.MakeLocalStorageURI("test1")),
			},
			recoveryEvent: expectedRecoveryEvent{
				numRows:      3,
				recoveryType: "restore_job",
			},
		},
	}

	sql.TelemetryMaxEventFrequency.Override(context.Background(), &testCluster.Server(0).ClusterSettings().SV, 10)

	// Run all the queries, one after the previous one is finished.
	var jobID int
	var unused interface{}
	var err error
	execTimestamp := 0
	for _, tc := range testData {
		if strings.HasPrefix(tc.query, "RESTORE") {
			cleanUpObjectsBeforeRestore(ctx, t, tc.query, db.DB)
			// We need to ensure RESTORE job happens after the DROP DATABASE and
			// DROP TABLE events got emitted.
			execTimestamp++
		}
		stubTime := timeutil.FromUnixMicros(int64(execTimestamp * 1e6))
		st.SetTime(stubTime)

		if strings.Contains(tc.query, "WITH detached") {
			err = db.DB.QueryRowContext(ctx, tc.query).Scan(&jobID)
		} else {
			err = db.DB.QueryRowContext(ctx, tc.query).Scan(&jobID, &unused, &unused, &unused, &unused, &unused)
		}
		if err != nil {
			t.Errorf("unexpected error executing query `%s`: %v", tc.query, err)
		}
		waitForJobResult(t, testCluster, jobspb.JobID(jobID), jobs.StatusSucceeded)
		t.Logf("finished:%q\n", tc.query)

		execTimestamp++
	}

	log.FlushFileSinks()

	var filteredSampleQueries []logpb.Entry
	testutils.SucceedsSoon(t, func() error {
		filteredSampleQueries = []logpb.Entry{}
		sampleQueryEntries, err := log.FetchEntriesFromFiles(
			0,
			math.MaxInt64,
			10000,
			regexp.MustCompile(`"EventType":"sampled_query"`),
			log.WithMarkedSensitiveData,
		)
		require.NoError(t, err)

		for _, sq := range sampleQueryEntries {
			if !(strings.Contains(sq.Message, "IMPORT") || strings.Contains(sq.Message, "RESTORE") || strings.Contains(sq.Message, "BACKUP")) {
				continue
			}
			filteredSampleQueries = append(filteredSampleQueries, sq)
		}
		if len(filteredSampleQueries) < len(testData) {
			return errors.New("not enough sample query events fetched")
		}
		return nil
	})

	var recoveryEventEntries []logpb.Entry
	testutils.SucceedsSoon(t, func() error {
		recoveryEventEntries, err = log.FetchEntriesFromFiles(
			0,
			math.MaxInt64,
			10000,
			regexp.MustCompile(`"EventType":"recovery_event"`),
			log.WithMarkedSensitiveData,
		)
		require.NoError(t, err)
		if len(recoveryEventEntries) < len(testData) {
			return errors.New("not enough recovery events fetched")
		}
		return nil
	})

	for _, tc := range testData {
		t.Run(tc.name, func(t *testing.T) {
			var foundSampleQuery bool
			for i := len(filteredSampleQueries) - 1; i >= 0; i-- {
				e := filteredSampleQueries[i]
				var sq eventpb.SampledQuery
				jsonPayload := []byte(e.Message)
				if err := json.Unmarshal(jsonPayload, &sq); err != nil {
					t.Errorf("unmarshalling %q: %v", e.Message, err)
				}
				if sq.Statement.StripMarkers() == tc.sampleQueryEvent.stmt {
					foundSampleQuery = true
					if strings.Contains(e.Message, "NumRows:") {
						t.Errorf("for bulk jobs (IMPORT/BACKUP/RESTORE), "+
							"there shouldn't be NumRows entry in the event message: %s",
							e.Message)
					}
					require.Greater(t, sq.BulkJobId, uint64(0))
					tc.recoveryEvent.bulkJobId = sq.BulkJobId
					break
				}
			}
			if !foundSampleQuery {
				t.Errorf("cannot find sample query event for %q", tc.query)
			}

			var foundRecoveryEvent bool
			for i := len(recoveryEventEntries) - 1; i >= 0; i-- {
				e := recoveryEventEntries[i]
				var re eventpb.RecoveryEvent
				jsonPayload := []byte(e.Message)
				if err := json.Unmarshal(jsonPayload, &re); err != nil {
					t.Errorf("unmarshalling %q: %v", e.Message, err)
				}
				if string(re.RecoveryType) == tc.recoveryEvent.recoveryType &&
					tc.recoveryEvent.bulkJobId == re.JobID &&
					re.ResultStatus == "succeeded" {
					foundRecoveryEvent = true
					require.Equal(t, tc.recoveryEvent.numRows, re.NumRows)
					break
				}
			}
			if !foundRecoveryEvent {
				t.Errorf("cannot find recovery event for %q", tc.query)
			}
		})
	}
}

func waitForJobResult(
	t *testing.T, tc serverutils.TestClusterInterface, id jobspb.JobID, expected jobs.Status,
) {
	// Force newly created job to be adopted and verify its result.
	tc.Server(0).JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
	testutils.SucceedsSoon(t, func() error {
		var unused int64
		return tc.ServerConn(0).QueryRow(
			"SELECT job_id FROM [SHOW JOBS] WHERE job_id = $1 AND status = $2",
			id, expected).Scan(&unused)
	})
}

func cleanUpObjectsBeforeRestore(
	ctx context.Context, t *testing.T, query string, db sqlutils.DBHandle,
) {
	dbRegex := regexp.MustCompile(`RESTORE\s+DATABASE\s+(\S+)`)
	dbMatch := dbRegex.FindStringSubmatch(query)
	if len(dbMatch) > 0 {
		dbName := dbMatch[1]
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName)); err != nil {
			t.Errorf(errors.Wrapf(err, "failed to drop database %q before restore", dbName).Error())
		}
	}

	tableRegex := regexp.MustCompile(`RESTORE\s+TABLE\s+(\S+)`)
	tableMatch := tableRegex.FindStringSubmatch(query)
	if len(tableMatch) > 0 {
		tableName := tableMatch[1]
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			t.Errorf(errors.Wrapf(err, "failed to drop table %q before restore", tableName).Error())
		}
	}
}
