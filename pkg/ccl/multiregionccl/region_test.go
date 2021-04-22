// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"fmt"
	"regexp"
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

// TestRegionAddDropEnclosingRegionalByRowAlters tests adding/dropping regions
// (expected to fail) with a concurrent alter to a regional by row table. The
// sketch of the test is as follows:
// - Client 1 performs an ALTER ADD / DROP REGION. Let the user txn commit.
// - Block in the type schema changer.
// - Client 2 alters a REGIONAL / GLOBAL table to a REGIONAL BY ROW table. Let
// this operation finish.
// - Force a rollback on the REGION ADD / DROP by injecting an error.
// - Ensure the partitions on the REGIONAL BY ROW table are sane.
func TestRegionAddDropFailureEnclosingRegionalByRowAlters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	regionAlterCmds := []struct {
		name string
		cmd  string
	}{
		{
			name: "drop-region",
			cmd:  `ALTER DATABASE db DROP REGION "us-east3"`,
		},
		{
			name: "add-region",
			cmd:  `ALTER DATABASE db ADD REGION "us-east4"`,
		},
	}

	testCases := []struct {
		name  string
		setup string
	}{
		{
			name:  "alter-from-global",
			setup: `CREATE TABLE db.t () LOCALITY GLOBAL`,
		},
		{
			name:  "alter-from-explicit-regional",
			setup: `CREATE TABLE db.t () LOCALITY REGIONAL IN "us-east2"`,
		},
		{
			name:  "alter-from-regional",
			setup: `CREATE TABLE db.t () LOCALITY REGIONAL IN PRIMARY REGION`,
		},
		{
			name:  "alter-from-rbr",
			setup: `CREATE TABLE db.t (reg db.crdb_internal_region) LOCALITY REGIONAL BY ROW AS reg`,
		},
	}

	var mu syncutil.Mutex
	typeChangeStarted := make(chan struct{}, 1)
	typeChangeFinished := make(chan struct{}, 1)
	rbrOpFinished := make(chan struct{}, 1)

	knobs := base.TestingKnobs{
		SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
			RunBeforeEnumMemberPromotion: func() error {
				mu.Lock()
				defer mu.Unlock()
				typeChangeStarted <- struct{}{}
				<-rbrOpFinished
				// Trigger a roll-back.
				return errors.New("boom")
			},
		},
	}

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 4 /* numServers */, knobs, nil, /* baseDir */
	)
	defer cleanup()

	for _, tc := range testCases {
		for _, regionAlterCmd := range regionAlterCmds {
			t.Run(fmt.Sprintf("%s/%s", regionAlterCmd.name, tc.name), func(t *testing.T) {

				_, err := sqlDB.Exec(`
DROP DATABASE IF EXISTS db;
CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
`)
				require.NoError(t, err)

				_, err = sqlDB.Exec(tc.setup)
				require.NoError(t, err)

				go func() {
					_, err := sqlDB.Exec(regionAlterCmd.cmd)
					if !testutils.IsError(err, "boom") {
						t.Errorf("expected error boom, found %v", err)
					}
					typeChangeFinished <- struct{}{}
				}()

				<-typeChangeStarted

				_, err = sqlDB.Exec(`ALTER TABLE db.t SET LOCALITY REGIONAL BY ROW`)
				rbrOpFinished <- struct{}{}
				require.NoError(t, err)

				testutils.SucceedsSoon(t, func() error {
					return multiregionccltestutils.TestingEnsureCorrectPartitioning(
						sqlDB,
						"db",                  /* dbName */
						"t",                   /* tableName */
						[]string{"t@primary"}, /* expectedIndexes */
					)
				})
				<-typeChangeFinished
			})
		}
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

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

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

	testCases := []struct {
		name            string
		op              string
		expectedIndexes []string
	}{
		{
			name:            "create-rbr-table",
			op:              `DROP TABLE db.rbr; CREATE TABLE db.rbr() LOCALITY REGIONAL BY ROW`,
			expectedIndexes: []string{"rbr@primary"},
		},
		{
			name:            "create-index",
			op:              `CREATE INDEX idx ON db.rbr(v)`,
			expectedIndexes: []string{"rbr@primary", "rbr@idx"},
		},
		{
			name:            "add-column",
			op:              `ALTER TABLE db.rbr ADD COLUMN v2 INT`,
			expectedIndexes: []string{"rbr@primary"},
		},
		{
			name:            "alter-pk",
			op:              `ALTER TABLE db.rbr ALTER PRIMARY KEY USING COLUMNS (k, v)`,
			expectedIndexes: []string{"rbr@primary"},
		},
		{
			name:            "drop-column",
			op:              `ALTER TABLE db.rbr DROP COLUMN v`,
			expectedIndexes: []string{"rbr@primary"},
		},
		{
			name:            "unique-index",
			op:              `CREATE UNIQUE INDEX uniq ON db.rbr(v)`,
			expectedIndexes: []string{"rbr@primary", "rbr@uniq"},
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
				}

				_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
					t, 4 /* numServers */, knobs, nil, /* baseDir */
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

// TestAlterRegionalByRowEnclosingRegionAddDrop tests altering a
// (which may or may not succeed) with a concurrent operation on a regional by
// row table. The sketch of the test is as follows:
// - Client 1 performs an ALTER TABLE ... SET LOCALITY REGIONAL BY ROW TABLE.
// Let the user txn commit.
// - Block in the (table) schema changer.
// - Client 2 performs an ALTER ADD / DROP REGION. Let the operation complete
// (succeed or fail, depending on the particular setup).
// - Resume the ALTER operation in the schema changer.
// Currently, this ALTER operation is bound to fail because of
// https://github.com/cockroachdb/cockroach/issues/64011. This test ensures that
// the rollback completes successfully. Once the issue is addressed, this test
// should be updated to assert the correct partitioning values (in all cases).
func TestAlterRegionalByRowEnclosingRegionAddDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	// If the schema change succeeds, the alter may fail. The reason for the alter
	// failing is dependent on the particular region change operation being
	// performed. That is why we have the `expectedErrorRe` field on this struct.
	// See https://github.com/cockroachdb/cockroach/issues/64011 for more details.
	regionAlterCmds := []struct {
		name            string
		cmd             string
		shouldSucceed   bool
		expectedErrorRE string
	}{
		{
			name:          "drop-region-fail",
			cmd:           `ALTER DATABASE db DROP REGION "us-east3"`,
			shouldSucceed: false,
		},
		{
			name:            "drop-region-succeed",
			cmd:             `ALTER DATABASE db DROP REGION "us-east3"`,
			shouldSucceed:   true,
			expectedErrorRE: "in enum \"public.crdb_internal_region\"",
		},
		{
			name:          "add-region-fail",
			cmd:           `ALTER DATABASE db ADD REGION "us-east4"`,
			shouldSucceed: false,
		},
		{
			name:            "add-region-succeed",
			cmd:             `ALTER DATABASE db ADD REGION "us-east4"`,
			shouldSucceed:   true,
			expectedErrorRE: "missing partition us-east4 on PRIMARY INDEX of table t",
		},
	}

	testCases := []struct {
		name  string
		setup string
	}{
		{
			name:  "alter-from-global",
			setup: `CREATE TABLE db.t () LOCALITY GLOBAL`,
		},
		{
			name:  "alter-from-explicit-regional",
			setup: `CREATE TABLE db.t () LOCALITY REGIONAL IN "us-east2"`,
		},
		{
			name:  "alter-from-regional",
			setup: `CREATE TABLE db.t () LOCALITY REGIONAL IN PRIMARY REGION`,
		},
		// TODO(arul): Add a test for RBR -> RBR transition here. That test is more
		// complex because unlike the other scenarios, the region change job is
		// bound to fail in this test setup. This is because region finalization
		// tries to re-partition tables but is unable to create the partitioning.
		// Not sure what's going on there completely.
	}

	for _, tc := range testCases {
		for _, regionAlterCmd := range regionAlterCmds {
			t.Run(tc.name+"-"+regionAlterCmd.name, func(t *testing.T) {
				var mu syncutil.Mutex
				alterStarted := make(chan struct{})
				alterFinished := make(chan struct{})
				regionAlterFinished := make(chan struct{})

				knobs := base.TestingKnobs{
					SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
						RunBeforeEnumMemberPromotion: func() error {
							if regionAlterCmd.shouldSucceed {
								return nil
							}
							return errors.New("boom")
						},
					},
					SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
						RunBeforePublishWriteAndDelete: func() {
							mu.Lock()
							defer mu.Unlock()
							if alterStarted != nil {
								close(alterStarted)
								alterStarted = nil
							}
							<-regionAlterFinished
						},
					},
				}

				_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
					t, 4 /* numServers */, knobs, nil, /* baseDir */
				)
				defer cleanup()

				_, err := sqlDB.Exec(`
CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
`)
				require.NoError(t, err)

				_, err = sqlDB.Exec(tc.setup)
				require.NoError(t, err)

				go func() {
					defer func() {
						close(alterFinished)
					}()
					_, err = sqlDB.Exec(`ALTER TABLE db.t SET LOCALITY REGIONAL BY ROW`)
					if !testutils.IsError(err, regionAlterCmd.expectedErrorRE) {
						t.Errorf("unexpected error; exected %q, got %v", regionAlterCmd.expectedErrorRE, err)
					}
				}()

				<-alterStarted

				_, err = sqlDB.Exec(regionAlterCmd.cmd)
				close(regionAlterFinished)
				if regionAlterCmd.shouldSucceed {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.True(t, testutils.IsError(err, "boom"))
				}
				<-alterFinished

				testutils.SucceedsSoon(t, func() error {
					rows := sqlDB.QueryRow("SELECT status, error FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE'")
					var jobStatus, jobErr string
					if err := rows.Scan(&jobStatus, &jobErr); err != nil {
						return err
					}

					if regionAlterCmd.expectedErrorRE != "" {
						matched, err := regexp.MatchString("failed", jobStatus)
						if err != nil {
							return err
						}
						if !matched {
							return errors.Newf("expected job to fail, but got status %q", jobStatus)
						}

						matched, err = regexp.MatchString(regionAlterCmd.expectedErrorRE, jobErr)
						if err != nil {
							return err
						}
						if !matched {
							return errors.Newf(
								"expected jobErr %q but got %q",
								regionAlterCmd.expectedErrorRE,
								jobErr,
							)
						}

						// Ensure that a manual cleanup isn't required.
						matched, err = regexp.MatchString("manual cleanup", jobErr)
						if err != nil {
							return err
						}
						if matched {
							return errors.Newf("should not require manual cleanup, but got %q", jobErr)
						}
						return nil
					}

					matched, err := regexp.MatchString("succeeded", jobStatus)
					if err != nil {
						return err
					}
					if !matched {
						return errors.Newf("expected job to succeed, but found %q", jobStatus)
					}
					return multiregionccltestutils.TestingEnsureCorrectPartitioning(
						sqlDB,
						"db",                  /* dbName */
						"t",                   /* tableName */
						[]string{"t@primary"}, /* expectedIndexes */
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
