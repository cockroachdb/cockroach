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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
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
					RunBeforeEnumMemberPromotion: func() {
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
					},
				},
			}

			_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
				t, 5 /* numServers */, knobs, nil, /* baseDir */
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
			if tc.secondOpErr == "" {
				require.NoError(t, err)
			} else {
				require.True(t, testutils.IsError(err, tc.secondOpErr))
			}
			close(secondOpFinished)

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

func TestSettingPrimaryRegionAmidstDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var mu syncutil.Mutex
	dropRegionStarted := make(chan struct{})
	waitForPrimaryRegionSwitch := make(chan struct{})
	dropRegionFinished := make(chan struct{})
	knobs := base.TestingKnobs{
		SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
			RunBeforeEnumMemberPromotion: func() {
				mu.Lock()
				defer mu.Unlock()
				if dropRegionStarted != nil {
					close(dropRegionStarted)
					<-waitForPrimaryRegionSwitch
					dropRegionStarted = nil
				}
			},
		},
	}

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 2 /* numServers */, knobs, nil, /* baseDir */
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

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

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
	}

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 1 /* numServers */, knobs, nil, /* baseDir */
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

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	knobs := base.TestingKnobs{
		SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
			RunBeforeMultiRegionUpdates: func() error {
				return errors.New("boom")
			},
		},
	}

	// Setup.
	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, knobs, nil, /* baseDir */
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
