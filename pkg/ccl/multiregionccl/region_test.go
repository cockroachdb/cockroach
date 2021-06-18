// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestConcurrentAddDropRegions tests all combinations of add/drop as if they
// were executed by two concurrent sessions. The general sketch of the test is
// as follows:
// - First operation is executed and blocks before the enum members are promoted.
// - The second operation starts once the first operation has reached the type
//   schema changer. It continues to completion. It may succeed/fail depending
//   on the specific test setup.
// - The first operation is resumed and allowed to complete. We expect it to
//   succeed.
// - Verify database regions are as expected.
// Operations act on a multi-region database that contains a REGIONAL BY ROW
// table, so as to exercise the repartitioning semantics.
func TestConcurrentAddDropRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	testCases := []struct {
		name            string
		firstOp         string
		secondOp        string
		expectedRegions []string
		secondOpErr     string
	}{
		{
			"concurrent-drops",
			`ALTER DATABASE db DROP REGION "us-east2"`,
			`ALTER DATABASE db DROP REGION "us-east3"`,
			[]string{"us-east1"},
			"",
		},
		{
			"concurrent-adds",
			`ALTER DATABASE db ADD REGION "us-east4"`,
			`ALTER DATABASE db ADD REGION "us-east5"`,
			[]string{"us-east1", "us-east2", "us-east3", "us-east4", "us-east5"},
			"",
		},
		{
			"concurrent-add-drop",
			`ALTER DATABASE db ADD REGION "us-east4"`,
			`ALTER DATABASE db DROP REGION "us-east3"`,
			[]string{"us-east1", "us-east2", "us-east4"},
			"",
		},
		{
			"concurrent-drop-add",
			`ALTER DATABASE db DROP REGION "us-east2"`,
			`ALTER DATABASE db ADD REGION "us-east5"`,
			[]string{"us-east1", "us-east3", "us-east5"},
			"",
		},
		{
			"concurrent-add-same-region",
			`ALTER DATABASE db ADD REGION "us-east5"`,
			`ALTER DATABASE db ADD REGION "us-east5"`,
			[]string{"us-east1", "us-east2", "us-east3", "us-east5"},
			`region "us-east5" already added to database`,
		},
		{
			"concurrent-drop-same-region",
			`ALTER DATABASE db DROP REGION "us-east2"`,
			`ALTER DATABASE db DROP REGION "us-east2"`,
			[]string{"us-east1", "us-east3"},
			`enum value "us-east2" is already being dropped`,
		},
		{
			"concurrent-add-drop-same-region",
			`ALTER DATABASE db ADD REGION "us-east5"`,
			`ALTER DATABASE db DROP REGION "us-east5"`,
			[]string{"us-east1", "us-east2", "us-east3", "us-east5"},
			`enum value "us-east5" is being added, try again later`,
		},
		{
			"concurrent-drop-add-same-region",
			`ALTER DATABASE db DROP REGION "us-east2"`,
			`ALTER DATABASE db ADD REGION "us-east2"`,
			[]string{"us-east1", "us-east3"},
			`enum value "us-east2" is being dropped, try again later`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			firstOp := true
			firstOpStarted := make(chan struct{})
			secondOpFinished := make(chan struct{})
			firstOpFinished := make(chan struct{})
			var mu syncutil.Mutex

			knobs := base.TestingKnobs{
				SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
					RunBeforeEnumMemberPromotion: func() error {
						mu.Lock()
						if firstOp {
							firstOp = false
							close(firstOpStarted)
							mu.Unlock()
							// Don't promote any members before the second operation reaches
							// the schema changer as well.
							<-secondOpFinished
						} else {
							mu.Unlock()
						}
						return nil
					},
				},
			}

			_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
				t, 5 /* numServers */, knobs,
			)
			defer cleanup()

			// Create a multi-region database with a REGIONAL BY ROW table inside of it
			// which needs to be re-partitioned on add/drop operations.
			_, err := sqlDB.Exec(`
CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
CREATE TABLE db.rbr () LOCALITY REGIONAL BY ROW`)
			require.NoError(t, err)

			go func() {
				if _, err := sqlDB.Exec(tc.firstOp); err != nil {
					t.Error(err)
				}
				close(firstOpFinished)
			}()

			// Wait for the first operation to reach the type schema changer.
			<-firstOpStarted

			// Start the second operation.
			_, err = sqlDB.Exec(tc.secondOp)
			close(secondOpFinished)
			if tc.secondOpErr == "" {
				require.NoError(t, err)
			} else {
				require.True(t, testutils.IsError(err, tc.secondOpErr))
			}

			<-firstOpFinished

			dbRegions := make([]string, 0, len(tc.expectedRegions))
			rows, err := sqlDB.Query("SELECT region FROM [SHOW REGIONS FROM DATABASE db]")
			require.NoError(t, err)
			for {
				done := rows.Next()
				if !done {
					require.NoError(t, rows.Err())
					break
				}
				var region string
				err := rows.Scan(&region)
				require.NoError(t, err)

				dbRegions = append(dbRegions, region)
			}

			if len(dbRegions) != len(tc.expectedRegions) {
				t.Fatalf("unexpected number of regions, expected: %v found %v",
					tc.expectedRegions,
					dbRegions,
				)
			}

			for i, expectedRegion := range tc.expectedRegions {
				if expectedRegion != dbRegions[i] {
					t.Fatalf("unexpected regions, expected: %v found %v",
						tc.expectedRegions,
						dbRegions,
					)
				}
			}
		})
	}
}

// TestRegionAddDropEnclosingRegionalByRowOps tests adding/dropping regions
// (which may or may not succeed) with a concurrent operation on a regional by
// row table. The sketch of the test is as follows:
// - Client 1 performs an ALTER ADD / DROP REGION. Let the user txn commit.
// - Block in the type schema changer.
// - Client 2 performs an operation on a REGIONAL BY ROW table, such as creating
// an index, unique constraint, altering the primary key etc.
// - Test both a rollback in the type schema changer and a successful execution.
// - Ensure the partitions on the REGIONAL BY ROW table are sane.
func TestRegionAddDropEnclosingRegionalByRowOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	regionAlterCmds := []struct {
		name          string
		cmd           string
		shouldSucceed bool
	}{
		{
			name:          "drop-region-fail",
			cmd:           `ALTER DATABASE db DROP REGION "us-east3"`,
			shouldSucceed: false,
		},
		{
			name:          "drop-region-succeed",
			cmd:           `ALTER DATABASE db DROP REGION "us-east3"`,
			shouldSucceed: true,
		},
		{
			name:          "add-region",
			cmd:           `ALTER DATABASE db ADD REGION "us-east4"`,
			shouldSucceed: false,
		},
		{
			name:          "add-region",
			cmd:           `ALTER DATABASE db ADD REGION "us-east4"`,
			shouldSucceed: true,
		},
	}

	// We only have one test case for now as we only allow CREATE TABLE
	// to race with ADD/DROP regions.
	testCases := []struct {
		name            string
		op              string
		expectedIndexes []string
	}{
		{
			name:            "create-rbr-table",
			op:              `DROP TABLE IF EXISTS db.rbr; CREATE TABLE db.rbr() LOCALITY REGIONAL BY ROW`,
			expectedIndexes: []string{"rbr@primary"},
		},
	}

	for _, tc := range testCases {
		for _, regionAlterCmd := range regionAlterCmds {
			t.Run(regionAlterCmd.name+"-"+tc.name, func(t *testing.T) {
				var mu syncutil.Mutex
				typeChangeStarted := make(chan struct{})
				typeChangeFinished := make(chan struct{})
				rbrOpFinished := make(chan struct{})

				knobs := base.TestingKnobs{
					SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
						RunBeforeEnumMemberPromotion: func() error {
							mu.Lock()
							defer mu.Unlock()
							close(typeChangeStarted)
							<-rbrOpFinished
							if !regionAlterCmd.shouldSucceed {
								// Trigger a roll-back.
								return errors.New("boom")
							}
							// Trod on.
							return nil
						},
					},
					// Decrease the adopt loop interval so that retries happen quickly.
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				}

				_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
					t, 4 /* numServers */, knobs,
				)
				defer cleanup()

				_, err := sqlDB.Exec(`
DROP DATABASE IF EXISTS db;
CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
CREATE TABLE db.rbr(k INT PRIMARY KEY, v INT NOT NULL) LOCALITY REGIONAL BY ROW;
`)
				require.NoError(t, err)

				go func() {
					defer func() {
						close(typeChangeFinished)
					}()
					_, err := sqlDB.Exec(regionAlterCmd.cmd)
					if regionAlterCmd.shouldSucceed {
						if err != nil {
							t.Errorf("expected success, got %v", err)
						}
					} else {
						if !testutils.IsError(err, "boom") {
							t.Errorf("expected error boom, found %v", err)
						}
					}
				}()

				<-typeChangeStarted

				_, err = sqlDB.Exec(tc.op)
				close(rbrOpFinished)
				require.NoError(t, err)

				<-typeChangeFinished

				testutils.SucceedsSoon(t, func() error {
					return multiregionccltestutils.TestingEnsureCorrectPartitioning(
						sqlDB, "db" /* dbName */, "rbr" /* tableName */, tc.expectedIndexes,
					)
				})
			})
		}
	}
}

func TestSettingPrimaryRegionAmidstDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var mu syncutil.Mutex
	dropRegionStarted := make(chan struct{})
	waitForPrimaryRegionSwitch := make(chan struct{})
	dropRegionFinished := make(chan struct{})
	knobs := base.TestingKnobs{
		SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
			RunBeforeEnumMemberPromotion: func() error {
				mu.Lock()
				defer mu.Unlock()
				if dropRegionStarted != nil {
					close(dropRegionStarted)
					<-waitForPrimaryRegionSwitch
					dropRegionStarted = nil
				}
				return nil
			},
		},
	}

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 2 /* numServers */, knobs,
	)
	defer cleanup()

	// Setup the test.
	_, err := sqlDB.Exec(`CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if _, err := sqlDB.Exec(`ALTER DATABASE db DROP REGION "us-east2"`); err != nil {
			t.Error(err)
		}
		close(dropRegionFinished)
	}()

	// Wait for the drop region to start and move the enum member "us-east2" in
	// read-only state.
	<-dropRegionStarted

	_, err = sqlDB.Exec(`
ALTER DATABASE db PRIMARY REGION "us-east2";
`)

	if err == nil {
		t.Fatalf("expected error, found nil")
	}
	if !testutils.IsError(err, `"us-east2" has not been added to the database`) {
		t.Fatalf(`expected secondOpErr, got %v`, err)
	}

	close(waitForPrimaryRegionSwitch)
	<-dropRegionFinished

	// We expect the async region drop job to succeed soon.
	testutils.SucceedsSoon(t, func() error {
		rows, err := sqlDB.Query("SELECT region FROM [SHOW REGIONS FROM DATABASE db]")
		if err != nil {
			return err
		}
		defer rows.Close()

		const expectedRegion = "us-east1"
		var region string
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return errors.New("no rows returned")
		}
		if err := rows.Scan(&region); err != nil {
			return err
		}

		if region != expectedRegion {
			return errors.Newf("expected region to be: %q, got %q", expectedRegion, region)
		}

		if rows.Next() {
			return errors.New("unexpected number of rows returned")
		}
		return nil
	})
}

// TestDroppingPrimaryRegionAsyncJobFailure drops the primary region of the
// database, which results in dropping the multi-region type descriptor. Then,
// it errors out the async job associated with the type descriptor cleanup and
// ensures the namespace entry is reclaimed back despite the injected error.
// We rely on this behavior to be able to add multi-region capability in the
// future.
func TestDroppingPrimaryRegionAsyncJobFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Protects expectedCleanupRuns
	var mu syncutil.Mutex
	// We need to cleanup 2 times, once for the multi-region type descriptor and
	// once for the array alias of the multi-region type descriptor.
	haveWePerformedFirstRoundOfCleanup := false
	cleanupFinished := make(chan struct{})
	knobs := base.TestingKnobs{
		SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
			RunBeforeExec: func() error {
				return errors.New("yikes")
			},
			RunAfterOnFailOrCancel: func() error {
				mu.Lock()
				defer mu.Unlock()
				if haveWePerformedFirstRoundOfCleanup {
					close(cleanupFinished)
				}
				haveWePerformedFirstRoundOfCleanup = true
				return nil
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 1 /* numServers */, knobs,
	)
	defer cleanup()

	// Setup the test.
	_, err := sqlDB.Exec(`
CREATE DATABASE db WITH PRIMARY REGION "us-east1";
CREATE TABLE db.t(k INT) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`ALTER DATABASE db DROP REGION "us-east1"`)
	testutils.IsError(err, "yikes")

	<-cleanupFinished

	rows := sqlDB.QueryRow(`SELECT count(*) FROM system.namespace WHERE name = 'crdb_internal_region'`)
	var count int
	err = rows.Scan(&count)
	require.NoError(t, err)
	if count != 0 {
		t.Fatal("expected crdb_internal_region not to be present in system.namespace")
	}

	_, err = sqlDB.Exec(`ALTER DATABASE db PRIMARY REGION "us-east1"`)
	require.NoError(t, err)

	rows = sqlDB.QueryRow(`SELECT count(*) FROM system.namespace WHERE name = 'crdb_internal_region'`)
	err = rows.Scan(&count)
	require.NoError(t, err)
	if count != 1 {
		t.Fatal("expected crdb_internal_region to be present in system.namespace")
	}
}

// TestRollbackDuringAddDropRegionAsyncJobFailure ensures that rollback when
// an ADD REGION/DROP REGION fails asynchronously is handled appropriately.
// This also ensures that zone configuration changes on the database are
// transactional.
func TestRollbackDuringAddDropRegionAsyncJobFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	knobs := base.TestingKnobs{
		SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
			RunBeforeMultiRegionUpdates: func() error {
				return errors.New("boom")
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	// Setup.
	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, knobs,
	)
	defer cleanup()
	_, err := sqlDB.Exec(`CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`)
	require.NoError(t, err)

	testCases := []struct {
		name  string
		query string
	}{
		{
			"add-region",
			`ALTER DATABASE db ADD REGION "us-east3"`,
		},
		{
			"drop-region",
			`ALTER DATABASE db DROP REGION "us-east2"`,
		},
		{
			"add-drop-region-in-txn",
			`BEGIN;
	ALTER DATABASE db DROP REGION "us-east2";
	ALTER DATABASE db ADD REGION "us-east3";
	COMMIT`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var originalZoneConfig string
			res := sqlDB.QueryRow(`SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FOR DATABASE db]`)
			err = res.Scan(&originalZoneConfig)
			require.NoError(t, err)

			_, err = sqlDB.Exec(tc.query)
			testutils.IsError(err, "boom")

			var jobStatus string
			var jobErr string
			row := sqlDB.QueryRow("SELECT status, error FROM [SHOW JOBS] WHERE job_type = 'TYPEDESC SCHEMA CHANGE'")
			require.NoError(t, row.Scan(&jobStatus, &jobErr))
			require.Contains(t, "boom", jobErr)
			require.Contains(t, "failed", jobStatus)

			// Ensure the zone configuration didn't change.
			var newZoneConfig string
			res = sqlDB.QueryRow(`SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FOR DATABASE db]`)
			err = res.Scan(&newZoneConfig)
			require.NoError(t, err)

			if newZoneConfig != originalZoneConfig {
				t.Fatalf("expected zone config to not have changed, expected %q found %q",
					originalZoneConfig,
					newZoneConfig,
				)
			}
		})
	}
}

// TestRegionAddDropEnclosingBackupOps tests adding/dropping regions
// (which may or may not succeed) with a concurrent backup operation
// The sketch of the test is as follows:
// - Client 1 performs an ALTER ADD / DROP REGION. Let the user txn commit.
// - Block in the type schema changer.
// - Client 2 performs a backup operation.
// - Resume blocked schema change job.
// - Startup a new cluster.
// - Restore the database, block in the schema changer.
// - Fail or succeed the schema change job.
// - Validate that the database and its tables look as expected.
func TestRegionAddDropWithConcurrentBackupOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	regionAlterCmds := []struct {
		name               string
		cmd                string
		shouldSucceed      bool
		expectedPartitions []string
	}{
		{
			name:               "drop-region-fail",
			cmd:                `ALTER DATABASE db DROP REGION "us-east3"`,
			shouldSucceed:      false,
			expectedPartitions: []string{"us-east1", "us-east2", "us-east3"},
		},
		{
			name:               "drop-region-succeed",
			cmd:                `ALTER DATABASE db DROP REGION "us-east3"`,
			shouldSucceed:      true,
			expectedPartitions: []string{"us-east1", "us-east2"},
		},
		{
			name:               "add-region-fail",
			cmd:                `ALTER DATABASE db ADD REGION "us-east4"`,
			shouldSucceed:      false,
			expectedPartitions: []string{"us-east1", "us-east2", "us-east3"},
		},
		{
			name:               "add-region-succeed",
			cmd:                `ALTER DATABASE db ADD REGION "us-east4"`,
			shouldSucceed:      true,
			expectedPartitions: []string{"us-east1", "us-east2", "us-east3", "us-east4"},
		},
	}

	testCases := []struct {
		name      string
		backupOp  string
		restoreOp string
	}{
		{
			name:      "backup-database",
			backupOp:  `BACKUP DATABASE db TO 'nodelocal://0/db_backup'`,
			restoreOp: `RESTORE DATABASE db FROM 'nodelocal://0/db_backup'`,
		},
	}

	for _, tc := range testCases {
		for _, regionAlterCmd := range regionAlterCmds {
			t.Run(regionAlterCmd.name+"-"+tc.name, func(t *testing.T) {
				var mu syncutil.Mutex
				typeChangeStarted := make(chan struct{})
				typeChangeFinished := make(chan struct{})
				backupOpFinished := make(chan struct{})
				waitInTypeSchemaChangerDuringBackup := true

				backupKnobs := base.TestingKnobs{
					SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
						RunBeforeEnumMemberPromotion: func() error {
							mu.Lock()
							defer mu.Unlock()
							if waitInTypeSchemaChangerDuringBackup {
								waitInTypeSchemaChangerDuringBackup = false
								close(typeChangeStarted)
								<-backupOpFinished
							}
							// Always return success here. The goal of this test isn't to
							// fail during the backup, but to do so during the restore.
							return nil
						},
					},
					// Decrease the adopt loop interval so that retries happen quickly.
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				}

				tempExternalIODir, tempDirCleanup := testutils.TempDir(t)
				defer tempDirCleanup()

				_, sqlDBBackup, cleanupBackup := multiregionccltestutils.TestingCreateMultiRegionCluster(
					t,
					4, /* numServers */
					backupKnobs,
					multiregionccltestutils.WithBaseDirectory(tempExternalIODir),
				)
				defer cleanupBackup()

				_, err := sqlDBBackup.Exec(`
DROP DATABASE IF EXISTS db;
CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
USE db;
CREATE TABLE db.rbr(k INT PRIMARY KEY, v INT NOT NULL) LOCALITY REGIONAL BY ROW;
INSERT INTO db.rbr VALUES (1,1),(2,2),(3,3);
`)
				require.NoError(t, err)

				go func() {
					defer func() {
						close(typeChangeFinished)
					}()
					_, err := sqlDBBackup.Exec(regionAlterCmd.cmd)
					if err != nil {
						t.Errorf("expected success, got %v when executing %s", err, regionAlterCmd.cmd)
					}
				}()

				<-typeChangeStarted

				_, err = sqlDBBackup.Exec(tc.backupOp)
				close(backupOpFinished)
				require.NoError(t, err)

				<-typeChangeFinished

				restoreKnobs := base.TestingKnobs{
					SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
						RunBeforeEnumMemberPromotion: func() error {
							mu.Lock()
							defer mu.Unlock()
							if !regionAlterCmd.shouldSucceed {
								// Trigger a roll-back.
								return errors.New("nope")
							}
							// Trod on.
							return nil
						},
					},
					// Decrease the adopt loop interval so that retries happen quickly.
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				}

				// Start a new cluster (with new testing knobs) for restore.
				_, sqlDBRestore, cleanupRestore := multiregionccltestutils.TestingCreateMultiRegionCluster(
					t,
					4, /* numServers */
					restoreKnobs,
					multiregionccltestutils.WithBaseDirectory(tempExternalIODir),
				)
				defer cleanupRestore()

				_, err = sqlDBRestore.Exec(tc.restoreOp)
				require.NoError(t, err)

				// First ensure that the data was restored correctly.
				numRows := sqlDBRestore.QueryRow(`SELECT count(*) from db.rbr`)
				require.NoError(t, numRows.Err())
				var count int
				err = numRows.Scan(&count)
				require.NoError(t, err)
				if count != 3 {
					t.Logf("unexpected number of rows after restore: expected 3, found %d", count)
				}

				// Now validate that the background job has completed and the
				// regions are in the expected state.
				testutils.SucceedsSoon(t, func() error {
					dbRegions := make([]string, 0, len(regionAlterCmd.expectedPartitions))
					rowsRegions, err := sqlDBRestore.Query("SELECT region FROM [SHOW REGIONS FROM DATABASE db]")
					require.NoError(t, err)
					defer rowsRegions.Close()
					for {
						done := rowsRegions.Next()
						if !done {
							require.NoError(t, rowsRegions.Err())
							break
						}
						var region string
						err := rowsRegions.Scan(&region)
						require.NoError(t, err)
						dbRegions = append(dbRegions, region)
					}
					if len(dbRegions) != len(regionAlterCmd.expectedPartitions) {
						return errors.Newf("unexpected number of regions, expected: %v found %v",
							regionAlterCmd.expectedPartitions,
							dbRegions,
						)
					}
					for i, expectedRegion := range regionAlterCmd.expectedPartitions {
						if expectedRegion != dbRegions[i] {
							return errors.Newf("unexpected regions, expected: %v found %v",
								regionAlterCmd.expectedPartitions,
								dbRegions,
							)
						}
					}
					return nil
				})

				// Finally, confirm that all of the tables were repartitioned
				// correctly by the above ADD/DROP region job.
				testutils.SucceedsSoon(t, func() error {
					return multiregionccltestutils.TestingEnsureCorrectPartitioning(
						sqlDBRestore,
						"db",
						"rbr",
						[]string{"rbr@primary"},
					)
				})
			})
		}
	}
}
