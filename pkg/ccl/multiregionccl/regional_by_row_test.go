// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestAlterTableLocalityRegionalByRowCorrectZoneConfigBeforeBackfill tests that
// the zone configurations are properly set up before the LOCALITY REGIONAL BY ROW
// backfill begins.
func TestAlterTableLocalityRegionalByRowCorrectZoneConfigBeforeBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []testutilsccl.AlterPrimaryKeyCorrectZoneConfigTestCase{
		{
			Desc:       "REGIONAL BY TABLE to REGIONAL BY ROW",
			SetupQuery: `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY TABLE`,
			AlterQuery: `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			ExpectedIntermediateZoneConfigs: []testutilsccl.AlterPrimaryKeyCorrectZoneConfigIntermediateZoneConfig{
				{
					ShowConfigStatement: `SHOW ZONE CONFIGURATION FOR TABLE t.test`,
					ExpectedTarget:      `DATABASE t`,
					ExpectedSQL: `ALTER DATABASE t CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 90000,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=ajstorm-1: 1}',
	voter_constraints = '[+region=ajstorm-1]',
	lease_preferences = '[[+region=ajstorm-1]]'`,
				},
				{
					ShowConfigStatement: `SHOW ZONE CONFIGURATION FOR PARTITION "ajstorm-1" OF INDEX t.test@new_primary_key`,
					ExpectedTarget:      `PARTITION "ajstorm-1" OF INDEX t.public.test@new_primary_key`,
					ExpectedSQL: `ALTER PARTITION "ajstorm-1" OF INDEX t.public.test@new_primary_key CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 90000,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=ajstorm-1: 1}',
	voter_constraints = '[+region=ajstorm-1]',
	lease_preferences = '[[+region=ajstorm-1]]'`,
				},
			},
		},
		{
			Desc:       "GLOBAL to REGIONAL BY ROW",
			SetupQuery: `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY GLOBAL`,
			AlterQuery: `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			ExpectedIntermediateZoneConfigs: []testutilsccl.AlterPrimaryKeyCorrectZoneConfigIntermediateZoneConfig{
				{
					ShowConfigStatement: `SHOW ZONE CONFIGURATION FOR TABLE t.test`,
					ExpectedTarget:      `TABLE t.public.test`,
					ExpectedSQL: `ALTER TABLE t.public.test CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 90000,
	global_reads = true,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=ajstorm-1: 1}',
	voter_constraints = '[+region=ajstorm-1]',
	lease_preferences = '[[+region=ajstorm-1]]'`,
				},
				{
					ShowConfigStatement: `SHOW ZONE CONFIGURATION FOR PARTITION "ajstorm-1" OF INDEX t.test@new_primary_key`,
					ExpectedTarget:      `PARTITION "ajstorm-1" OF INDEX t.public.test@new_primary_key`,
					ExpectedSQL: `ALTER PARTITION "ajstorm-1" OF INDEX t.public.test@new_primary_key CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 90000,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=ajstorm-1: 1}',
	voter_constraints = '[+region=ajstorm-1]',
	lease_preferences = '[[+region=ajstorm-1]]'`,
				},
			},
		},
		{
			Desc:       "REGIONAL BY ROW to REGIONAL BY TABLE",
			SetupQuery: `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY ROW`,
			AlterQuery: `ALTER TABLE t.test SET LOCALITY REGIONAL BY TABLE`,
			ExpectedIntermediateZoneConfigs: []testutilsccl.AlterPrimaryKeyCorrectZoneConfigIntermediateZoneConfig{
				{
					ShowConfigStatement: `SHOW ZONE CONFIGURATION FOR TABLE t.test`,
					ExpectedTarget:      `DATABASE t`,
					ExpectedSQL: `ALTER DATABASE t CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 90000,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=ajstorm-1: 1}',
	voter_constraints = '[+region=ajstorm-1]',
	lease_preferences = '[[+region=ajstorm-1]]'`,
				},
			},
		},
		{
			Desc:       "REGIONAL BY ROW to GLOBAL",
			SetupQuery: `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY ROW`,
			AlterQuery: `ALTER TABLE t.test SET LOCALITY GLOBAL`,
			ExpectedIntermediateZoneConfigs: []testutilsccl.AlterPrimaryKeyCorrectZoneConfigIntermediateZoneConfig{
				{
					ShowConfigStatement: `SHOW ZONE CONFIGURATION FOR TABLE t.test`,
					ExpectedTarget:      `DATABASE t`,
					ExpectedSQL: `ALTER DATABASE t CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 90000,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=ajstorm-1: 1}',
	voter_constraints = '[+region=ajstorm-1]',
	lease_preferences = '[[+region=ajstorm-1]]'`,
				},
			},
		},
	}
	testutilsccl.AlterPrimaryKeyCorrectZoneConfigTest(
		t,
		`CREATE DATABASE t PRIMARY REGION "ajstorm-1"`,
		testCases,
	)
}

// TestAlterTableLocalityRegionalByRowError tests an alteration involving
// REGIONAL BY ROW which gets its async job interrupted by some sort of
// error or cancellation. After this, we expect the table to retain
// its original form with no extra columns or implicit partitioning added.
func TestAlterTableLocalityRegionalByRowError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	var chunkSize int64 = 100
	var maxValue = 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		maxValue = 200
		chunkSize = 5
	}
	// BulkInsertIntoTable adds testCase 0 to maxValue inclusive, so
	// we round (maxValue + 1) / chunkSize to the nearest int.
	// To round up x / y using integers, we do (x + y - 1) / y.
	// In this case, since x=maxValue+1, we do (maxValue + chunkSize) / chunkSize.
	var chunksPerBackfill = (maxValue + int(chunkSize)) / int(chunkSize)
	ctx := context.Background()

	const showCreateTableStringSQL = `SELECT create_statement FROM [SHOW CREATE TABLE t.test]`
	const zoneConfigureSQLStatements = `
		SELECT coalesce(string_agg(raw_config_sql, ';' ORDER BY raw_config_sql), 'NULL')
		FROM crdb_internal.zones
		WHERE database_name = 't' AND table_name = 'test'
	`

	// alterState is a struct that contains an action for a base test case
	// to execute ALTER TABLE t.test SET LOCALITY <locality> against.
	type alterState struct {
		desc       string
		alterQuery string
		// cancelOnBackfillChunk on which chunk the cancel query should run.
		cancelOnBackfillChunk int
	}

	// nonRegionalByRowAlterStates contains SET LOCALITY operations that exercise
	// the async ALTER PRIMARY KEY path for non-REGIONAL BY ROW base test cases.
	nonRegionalByRowAlterStates := []alterState{
		{
			desc:                  "alter to REGIONAL BY ROW AS cr",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW AS cr`,
			cancelOnBackfillChunk: 1,
		},
		{
			desc:                  "alter to REGIONAL BY ROW, interrupt during add column",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			cancelOnBackfillChunk: 1,
		},
		{
			desc:                  "alter to REGIONAL BY ROW, interrupt during pk swap",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
			cancelOnBackfillChunk: chunksPerBackfill + 1,
		},
	}
	// nonRegionalByRowAlterStates contains SET LOCALITY operations that exercise
	// the async ALTER PRIMARY KEY path for REGIONAL BY ROW base test cases.
	regionalByRowAlterStates := []alterState{
		{
			desc:                  "alter to GLOBAL",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY GLOBAL`,
			cancelOnBackfillChunk: 1,
		},
		{
			desc:                  "alter to REGIONAL BY TABLE",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY TABLE`,
			cancelOnBackfillChunk: 1,
		},
		{
			desc:                  "alter to REGIONAL BY TABLE IN ajstorm-1",
			alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY TABLE IN "ajstorm-1"`,
			cancelOnBackfillChunk: 1,
		},
	}

	// testCases contain a base table structure to start off as.
	// For each of these test cases, we pick either the alter state corresponding
	// to the base table's locality -- for REGIONAL BY ROW tables we pick
	// regionalyByRowAlterStates and for GLOBAL/REGIONAL BY TABLE tables we pick
	// nonRegionalByRowAlterStates.
	testCases := []struct {
		desc           string
		setupQuery     string
		rowidIdx       int
		alterStates    []alterState
		originalPKCols []string
	}{
		{
			desc:           "GLOBAL",
			setupQuery:     `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY GLOBAL`,
			originalPKCols: []string{"rowid"},
			alterStates:    nonRegionalByRowAlterStates,
		},
		{
			desc:           "REGIONAL BY TABLE",
			setupQuery:     `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY TABLE`,
			originalPKCols: []string{"rowid"},
			alterStates:    nonRegionalByRowAlterStates,
		},
		{
			desc:           "REGIONAL BY TABLE IN ajstorm-1",
			setupQuery:     `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY TABLE IN "ajstorm-1"`,
			originalPKCols: []string{"rowid"},
			alterStates:    nonRegionalByRowAlterStates,
		},
		{
			desc:           "REGIONAL BY ROW",
			setupQuery:     `CREATE TABLE t.test (k INT NOT NULL, v INT) LOCALITY REGIONAL BY ROW`,
			originalPKCols: []string{"crdb_region", "rowid"},
			alterStates: append(
				regionalByRowAlterStates,
				alterState{
					desc:                  "REGIONAL BY ROW AS cr",
					alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW AS cr`,
					cancelOnBackfillChunk: 1,
				},
			),
		},
		{
			desc: "REGIONAL BY ROW AS",
			setupQuery: `CREATE TABLE t.test (
				k INT NOT NULL, v INT, cr2 t.public.crdb_internal_region NOT NULL DEFAULT 'ajstorm-1'
			) LOCALITY REGIONAL BY ROW AS cr2`,
			originalPKCols: []string{"cr2", "rowid"},
			alterStates: append(
				regionalByRowAlterStates,
				alterState{
					desc:                  "REGIONAL BY ROW, cancel during column addition",
					alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
					cancelOnBackfillChunk: 1,
				},
				alterState{
					desc:                  "REGIONAL BY ROW, cancel during PK swap",
					alterQuery:            `ALTER TABLE t.test SET LOCALITY REGIONAL BY ROW`,
					cancelOnBackfillChunk: chunksPerBackfill + 1,
				},
			),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			for _, alterState := range testCase.alterStates {
				t.Run(alterState.desc, func(t *testing.T) {
					for _, errorMode := range []struct {
						desc          string
						runOnChunk    func(db *gosql.DB) error
						errorContains string
					}{
						{
							desc: "cancel",
							runOnChunk: func(db *gosql.DB) error {
								_, err := db.Exec(`CANCEL JOB (
					SELECT job_id FROM [SHOW JOBS]
					WHERE
						job_type = 'SCHEMA CHANGE' AND
						status = $1 AND
						description NOT LIKE 'ROLL BACK%'
				)`, jobs.StatusRunning)
								return err
							},
							errorContains: "job canceled by user",
						},
						{
							desc: "arbitrary error",
							runOnChunk: func(db *gosql.DB) error {
								return errors.Newf("arbitrary error during backfill")
							},
							errorContains: "arbitrary error during backfill",
						},
					} {
						t.Run(errorMode.desc, func(t *testing.T) {
							var db *gosql.DB
							// set backfill chunk to -chunksPerBackfill, to allow the ALTER TABLE ... ADD COLUMN
							// to backfill successfully.
							currentBackfillChunk := -(chunksPerBackfill + 1)
							params, _ := tests.CreateTestServerParams()
							params.Locality.Tiers = []roachpb.Tier{
								{Key: "region", Value: "ajstorm-1"},
							}
							params.Knobs = base.TestingKnobs{
								SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
									BackfillChunkSize: chunkSize,
								},
								DistSQL: &execinfra.TestingKnobs{
									RunBeforeBackfillChunk: func(sp roachpb.Span) error {
										currentBackfillChunk += 1
										if currentBackfillChunk != alterState.cancelOnBackfillChunk {
											return nil
										}
										return errorMode.runOnChunk(db)
									},
								},
							}
							s, sqlDB, kvDB := serverutils.StartServer(t, params)
							db = sqlDB
							defer s.Stopper().Stop(ctx)

							// Disable strict GC TTL enforcement because we're going to shove a zero-value
							// TTL into the system with AddImmediateGCZoneConfig.
							defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

							// Drop the closed timestamp target lead for GLOBAL tables.
							// The test passes with it configured to its default, but it
							// is very slow due to #61444 (2.5s vs. 35s).
							// TODO(nvanbenschoten): We can remove this when that issue
							// is addressed.
							if _, err := sqlDB.Exec(`SET CLUSTER SETTING kv.closed_timestamp.lead_for_global_reads_override = '5ms'`); err != nil {
								t.Fatal(err)
							}

							if _, err := sqlDB.Exec(fmt.Sprintf(`
CREATE DATABASE t PRIMARY REGION "ajstorm-1";
USE t;
%s;
`, testCase.setupQuery)); err != nil {
								t.Fatal(err)
							}

							if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
								t.Fatal(err)
							}

							// We add the "cr" column, which can be used for REGIONAL BY ROW AS.
							if _, err := sqlDB.Exec(`
		ALTER TABLE t.test ADD COLUMN cr t.crdb_internal_region
		NOT NULL
		DEFAULT gateway_region()::t.crdb_internal_region
	`); err != nil {
								t.Fatal(err)
							}
							// This will fail, so we don't want to check the error.
							_, err := sqlDB.Exec(alterState.alterQuery)
							require.Error(t, err)
							require.Contains(t, err.Error(), errorMode.errorContains)

							// Grab a copy of SHOW CREATE TABLE and zone configuration data before we run
							// any ALTER query. The result should match if the operation fails.
							var originalCreateTableOutput string
							require.NoError(
								t,
								sqlDB.QueryRow(showCreateTableStringSQL).Scan(&originalCreateTableOutput),
							)

							var originalZoneConfig string
							require.NoError(
								t,
								sqlDB.QueryRow(zoneConfigureSQLStatements).Scan(&originalZoneConfig),
							)

							// Ensure that the mutations corresponding to the primary key change are cleaned up and
							// that the job did not succeed even though it was canceled.
							testutils.SucceedsSoon(t, func() error {
								tableDesc := catalogkv.TestingGetTableDescriptor(
									kvDB, keys.SystemSQLCodec, "t", "test",
								)
								if len(tableDesc.AllMutations()) != 0 {
									return errors.Errorf(
										"expected 0 mutations after cancellation, found %d",
										len(tableDesc.AllMutations()),
									)
								}
								if tableDesc.GetPrimaryIndex().NumColumns() != len(testCase.originalPKCols) {
									return errors.Errorf("expected primary key change to not succeed after cancellation")
								}
								for i, name := range testCase.originalPKCols {
									if tableDesc.GetPrimaryIndex().GetColumnName(i) != name {
										return errors.Errorf(
											"expected primary key change to not succeed after cancellation\nmismatch idx %d: exp %s, got %s",
											i,
											name,
											tableDesc.GetPrimaryIndex().GetColumnName(i),
										)
									}
								}

								// Ensure SHOW CREATE TABLE for the table has not changed.
								var createTableString string
								if err := sqlDB.QueryRow(showCreateTableStringSQL).Scan(&createTableString); err != nil {
									return err
								}
								if createTableString != originalCreateTableOutput {
									return errors.Errorf(
										"expected SHOW CREATE TABLE to be %s\ngot %s",
										originalCreateTableOutput,
										createTableString,
									)
								}

								// Ensure SHOW ZONE CONFIGURATION has not changed.
								var zoneConfig string
								require.NoError(
									t,
									sqlDB.QueryRow(zoneConfigureSQLStatements).Scan(&zoneConfig),
								)
								if zoneConfig != originalZoneConfig {
									return errors.Errorf(
										"expected zone configuration statements to not have changed, got %s, sql %s",
										originalZoneConfig,
										zoneConfig,
									)
								}

								return nil
							})

							tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
							if _, err := sqltestutils.AddImmediateGCZoneConfig(db, tableDesc.GetID()); err != nil {
								t.Fatal(err)
							}
							// Ensure that the writes from the partial new indexes are cleaned up.
							testutils.SucceedsSoon(t, func() error {
								return sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue)
							})
						})
					}
				})
			}
		})
	}
}

// TestRepartitionFailureRollback adds and removes a region from a multi-region
// database, but injects a non-retryable error before regional by row tables
// can be repartitioned. The expectation is that we should roll back changes to
// the multi-region enum, reverting to the state before the region add/remove
// transaction was executed.
func TestRepartitionFailureRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	var mu syncutil.Mutex
	errorReturned := false
	knobs := base.TestingKnobs{
		SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
			RunBeforeMultiRegionUpdates: func() error {
				mu.Lock()
				defer mu.Unlock()
				if !errorReturned {
					errorReturned = true
					return errors.New("boom")
				}
				return nil
			},
		},
	}
	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, knobs, nil, /* baseDir */
	)
	defer cleanup()

	_, err := sqlDB.Exec(
		`CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2";
CREATE TABLE db.t(k INT PRIMARY KEY) LOCALITY REGIONAL BY ROW`)
	require.NoError(t, err)
	if err != nil {
		t.Error(err)
	}

	// Overriding this operation until we get a fix for #60620. When that fix is
	// ready, we can construct the view of the zone config as it was at the
	// beginning of the transaction, and the checks for override should work
	// again, and we won't require an explicit override here.
	_, err = sqlDB.Exec(`BEGIN;
SET override_multi_region_zone_config = true;
ALTER DATABASE db ADD REGION "us-east3";
ALTER DATABASE db DROP REGION "us-east2";
SET override_multi_region_zone_config = false;
COMMIT;`)
	require.Error(t, err, "boom")

	// The cleanup job should kick in and revert the changes that happened to the
	// type descriptor in the user txn. We should eventually be able to add
	// "us-east3" and remove "us-east2".
	// Overriding this operation until we get a fix for #60620. When that fix is
	// ready, we can construct the view of the zone config as it was at the
	// beginning of the transaction, and the checks for override should work
	// again, and we won't require an explicit override here.
	testutils.SucceedsSoon(t, func() error {
		_, err = sqlDB.Exec(`BEGIN;
	SET override_multi_region_zone_config = true;
	ALTER DATABASE db ADD REGION "us-east3";
	ALTER DATABASE db DROP REGION "us-east2";
	SET override_multi_region_zone_config = false;
	COMMIT;`)
		return err
	})
}

// TestIndexCleanupAfterAlterFromRegionalByRow ensures that old indexes for
// REGIONAL BY ROW transitions get cleaned up correctly.
func TestIndexCleanupAfterAlterFromRegionalByRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{}, nil, /* baseDir */
	)
	defer cleanup()

	_, err := sqlDB.Exec(
		`CREATE DATABASE "mr-zone-configs" WITH PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
USE "mr-zone-configs";
CREATE TABLE regional_by_row (
  pk INT PRIMARY KEY,
  i INT,
  INDEX(i),
  FAMILY (pk, i)
) LOCALITY REGIONAL BY ROW`)
	require.NoError(t, err)

	// Alter the table to REGIONAL BY TABLE, and then back to REGIONAL BY ROW, to
	// create some indexes that need cleaning up.
	_, err = sqlDB.Exec(`ALTER TABLE regional_by_row SET LOCALITY REGIONAL BY TABLE;
ALTER TABLE regional_by_row SET LOCALITY REGIONAL BY ROW`)
	require.NoError(t, err)

	// Validate that the indexes requiring cleanup exist.
	type row struct {
		status  string
		details string
	}

	for {
		// First confirm that the schema change job has completed
		res := sqlDB.QueryRow(`WITH jobs AS (
      SELECT status, crdb_internal.pb_to_json(
			'cockroach.sql.jobs.jobspb.Payload',
			payload,
			false
		) AS job
		FROM system.jobs
		)
    SELECT count(*)
    FROM jobs
    WHERE (job->>'schemaChange') IS NOT NULL AND status = 'running'`)

		require.NoError(t, res.Err())

		numJobs := 0
		err = res.Scan(&numJobs)
		require.NoError(t, err)
		if numJobs == 0 {
			break
		}
	}

	queryIndexGCJobsAndValidateCount := func(status string, expectedCount int) error {
		query := `WITH jobs AS (
      SELECT status, crdb_internal.pb_to_json(
			'cockroach.sql.jobs.jobspb.Payload',
			payload,
			false
		) AS job
		FROM system.jobs
		)
    SELECT status, job->'schemaChangeGC' as details
    FROM jobs
    WHERE (job->>'schemaChangeGC') IS NOT NULL AND status = '%s'`

		res, err := sqlDB.Query(fmt.Sprintf(query, status))
		require.NoError(t, err)

		var rows []row
		for res.Next() {
			r := row{}
			err = res.Scan(&r.status, &r.details)
			require.NoError(t, err)
			rows = append(rows, r)
		}
		actualCount := len(rows)
		if actualCount != expectedCount {
			return errors.Newf("expected %d jobs with status %q, found %d. Jobs found: %v",
				expectedCount,
				status,
				actualCount,
				rows)
		}
		return nil
	}

	// Now check that we have the right number of index GC jobs pending.
	err = queryIndexGCJobsAndValidateCount(`running`, 4)
	require.NoError(t, err)
	err = queryIndexGCJobsAndValidateCount(`succeeded`, 0)
	require.NoError(t, err)

	// Change gc.ttlseconds to speed up the cleanup.
	_, err = sqlDB.Exec(`ALTER TABLE regional_by_row CONFIGURE ZONE USING gc.ttlseconds = 1`)
	require.NoError(t, err)

	// Validate that indexes are cleaned up.
	queryAndEnsureThatFourIndexGCJobsSucceeded := func() error {
		return queryIndexGCJobsAndValidateCount(`succeeded`, 4)
	}
	testutils.SucceedsSoon(t, queryAndEnsureThatFourIndexGCJobsSucceeded)
	err = queryIndexGCJobsAndValidateCount(`running`, 0)
	require.NoError(t, err)
}
