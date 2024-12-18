// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// startTestCluster starts a 3 node cluster.
//
// Note, if a testfeed depends on particular testing knobs, those may
// need to be applied to each of the servers in the test cluster
// returned from this function.
func TestMultiRegionDatabaseStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	ctx := context.Background()
	knobs := base.TestingKnobs{}

	tc, db, cleanup := multiregionccltestutils.TestingCreateMultiRegionClusterWithRegionList(
		t,
		[]string{"us-east", "us-west"},
		3, /* serversPerRegion */
		knobs,
		multiregionccltestutils.WithUseDatabase("d"),
	)

	defer cleanup()

	_, err := db.ExecContext(ctx,
		`CREATE DATABASE test PRIMARY REGION "us-west";
    USE test;
    CREATE TABLE a(id uuid primary key);`)
	require.NoError(t, err)

	s := tc.Server(0)

	testutils.SucceedsWithin(t, func() error {
		// Get the list of nodes from ranges table
		row := db.QueryRowContext(ctx,
			`USE test;
    WITH x AS (SHOW RANGES FROM TABLE a) SELECT replicas FROM x;`)
		var nodesStr string
		err = row.Scan(&nodesStr)
		if err != nil {
			return err
		}

		// string comes back "{1,2,3}".
		nodesStr = strings.TrimLeft(nodesStr, "{")
		nodesStr = strings.TrimRight(nodesStr, "}")
		nodes := strings.Split(nodesStr, ",")

		var resp serverpb.DatabaseDetailsResponse
		require.NoError(t, serverutils.GetJSONProto(s, "/_admin/v1/databases/test?include_stats=true", &resp))

		if resp.Stats.RangeCount != int64(1) {
			return errors.Newf("expected range-count=1, got %d", resp.Stats.RangeCount)
		}

		if len(resp.Stats.NodeIDs) != len(nodes) {
			return errors.Newf("expected node-ids=%s, got %s", nodes, resp.Stats.NodeIDs)
		}

		for i, n := range resp.Stats.NodeIDs {
			if nodes[i] != fmt.Sprint(n) {
				return errors.Newf("The nodes returned from endpoint do not match nodes listed in the show range from table. expected: %s; actual: %s", nodes, resp.Stats.NodeIDs)
			}
		}

		return nil
	}, 45*time.Second)
}

// TestConcurrentAddDropRegions tests all combinations of add/drop as if they
// were executed by two concurrent sessions. The general sketch of the test is
// as follows:
//   - First operation is executed and blocks before the enum members are promoted.
//   - The second operation starts once the first operation has reached the type
//     schema changer. It continues to completion. It may succeed/fail depending
//     on the specific test setup.
//   - The first operation is resumed and allowed to complete. We expect it to
//     succeed.
//   - Verify database regions are as expected.
//
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
					RunBeforeEnumMemberPromotion: func(context.Context) error {
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

			go func(firstOp string) {
				if _, err := sqlDB.Exec(firstOp); err != nil {
					t.Error(err)
				}
				close(firstOpFinished)
			}(tc.firstOp)

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

			sort.Strings(tc.expectedRegions)
			sort.Strings(dbRegions)
			require.Equal(t, tc.expectedRegions, dbRegions)
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
			expectedIndexes: []string{"rbr@rbr_pkey"},
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
						RunBeforeEnumMemberPromotion: func(ctx context.Context) error {
							mu.Lock()
							defer mu.Unlock()
							close(typeChangeStarted)
							<-rbrOpFinished
							if !regionAlterCmd.shouldSucceed {
								// Trigger a roll-back.
								return jobs.MarkAsPermanentJobError(errors.New("boom"))
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

				go func(cmd string, shouldSucceed bool) {
					defer func() {
						close(typeChangeFinished)
					}()
					_, err := sqlDB.Exec(cmd)
					if shouldSucceed {
						if err != nil {
							t.Errorf("expected success, got %v", err)
						}
					} else {
						if !testutils.IsError(err, "boom") {
							t.Errorf("expected error boom, found %v", err)
						}
					}
				}(regionAlterCmd.cmd, regionAlterCmd.shouldSucceed)

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

// TestSettingPlacementAmidstAddDrop starts a region add/drop and alters the
// database to PLACEMENT RESTRICTED before enum promotion in the add/drop. This
// is required to ensure that global tables are refreshed as a result of the
// add/drop.
// This test is built to protect against cases where PLACEMENT RESTRICTED is
// applied but the database finalizer doesn't realize, meaning that global
// tables don't have their constraints refreshed. This is only a problem in the
// DEFAULT -> RESTRICTED path as in the RESTRICTED -> DEFAULT path, global
// tables will always be refreshed, either by virtue of inheriting the database
// zone config or being explicitly refreshed under RESTRICTED.
func TestSettingPlacementAmidstAddDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name         string
		placementOp  string
		regionOp     string
		expectedZcfg string
	}{
		{
			name:        "add then set restricted",
			placementOp: "ALTER DATABASE db PLACEMENT RESTRICTED",
			regionOp:    `ALTER DATABASE db ADD REGION "us-east3"`,
			expectedZcfg: `
ALTER TABLE db.public.global CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 14400,
	global_reads = true,
	num_replicas = 5,
	num_voters = 3,
	constraints = '{+region=us-east1: 1, +region=us-east2: 1, +region=us-east3: 1}',
	voter_constraints = '[+region=us-east1]',
	lease_preferences = '[[+region=us-east1]]'`,
		},
		{
			name:        "drop then set restricted",
			placementOp: "ALTER DATABASE db PLACEMENT RESTRICTED",
			regionOp:    `ALTER DATABASE db DROP REGION "us-east2"`,
			expectedZcfg: `
ALTER TABLE db.public.global CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 14400,
	global_reads = true,
	num_replicas = 3,
	num_voters = 3,
	constraints = '{+region=us-east1: 1}',
	voter_constraints = '[+region=us-east1]',
	lease_preferences = '[[+region=us-east1]]'`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			regionOpStarted := make(chan struct{})
			regionOpFinished := make(chan struct{})
			placementOpFinished := make(chan struct{})

			knobs := base.TestingKnobs{
				SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
					RunBeforeEnumMemberPromotion: func(context.Context) error {
						close(regionOpStarted)
						<-placementOpFinished
						return nil
					},
				},
				// Decrease the adopt loop interval so that retries happen quickly.
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			}

			_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
				t, 3 /* numServers */, knobs,
			)
			defer cleanup()

			_, err := sqlDB.Exec(`
CREATE DATABASE db PRIMARY REGION "us-east1" REGIONS "us-east2";
CREATE TABLE db.global () LOCALITY GLOBAL;`)
			require.NoError(t, err)
			_, err = sqlDB.Exec(`SET CLUSTER SETTING sql.defaults.multiregion_placement_policy.enabled = true;`)
			require.NoError(t, err)
			go func(regionOp string) {
				defer func() {
					close(regionOpFinished)
				}()

				_, err := sqlDB.Exec(regionOp)
				require.NoError(t, err)
			}(tc.regionOp)

			<-regionOpStarted
			_, err = sqlDB.Exec(tc.placementOp)
			require.NoError(t, err)
			close(placementOpFinished)

			<-regionOpFinished
			var actualZoneConfig string
			res := sqlDB.QueryRow(
				`SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FOR TABLE db.global]`,
			)
			err = res.Scan(&actualZoneConfig)
			require.NoError(t, err)

			expectedZcfg := strings.TrimSpace(tc.expectedZcfg)
			if expectedZcfg != actualZoneConfig {
				t.Fatalf("expected zone config %q but got %q", expectedZcfg, actualZoneConfig)
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
			RunBeforeEnumMemberPromotion: func(context.Context) error {
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
				return jobs.MarkAsPermanentJobError(errors.New("yikes"))
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
				return jobs.MarkAsPermanentJobError(errors.New("boom"))
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

// TestRollbackDuringAddDropRegionPlacementRestricted ensures that rollback when
// an ADD REGION/DROP REGION fails asynchronously is handled appropriately when
// the database has been configured with PLACEMENT RESTRICTED.
// This also ensures that zone configuration changes on the database are
// transactional.
func TestRollbackDuringAddDropRegionPlacementRestricted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race")

	knobs := base.TestingKnobs{
		SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
			RunBeforeMultiRegionUpdates: func() error {
				return jobs.MarkAsPermanentJobError(errors.New("boom"))
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
	_, err := sqlDB.Exec(
		`SET enable_multiregion_placement_policy = true;
CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2" PLACEMENT RESTRICTED;`,
	)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`CREATE TABLE db.test_global () LOCALITY GLOBAL`)
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
			res := sqlDB.QueryRow(
				`SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FOR TABLE db.test_global]`,
			)
			err = res.Scan(&originalZoneConfig)
			require.NoError(t, err)

			_, err = sqlDB.Exec(tc.query)
			testutils.IsError(err, "boom")

			var jobStatus string
			var jobErr string
			row := sqlDB.QueryRow(
				"SELECT status, error FROM [SHOW JOBS] WHERE job_type = 'TYPEDESC SCHEMA CHANGE'",
			)
			require.NoError(t, row.Scan(&jobStatus, &jobErr))
			require.Contains(t, "boom", jobErr)
			require.Contains(t, "failed", jobStatus)

			// Ensure the zone configuration didn't change.
			var newZoneConfig string
			res = sqlDB.QueryRow(
				`SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FOR TABLE db.test_global]`,
			)
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

func TestDropRegionFailWithConcurrentBackupOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testRegionAddDropWithConcurrentBackupOps(t, struct {
		cmd                string
		shouldSucceed      bool
		expectedPartitions []string
	}{
		cmd:                `ALTER DATABASE db DROP REGION "us-east3"`,
		shouldSucceed:      false,
		expectedPartitions: []string{"us-east1", "us-east2", "us-east3"},
	})
}

func TestDropRegionSucceedWithConcurrentBackupOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testRegionAddDropWithConcurrentBackupOps(t, struct {
		cmd                string
		shouldSucceed      bool
		expectedPartitions []string
	}{
		cmd:                `ALTER DATABASE db DROP REGION "us-east3"`,
		shouldSucceed:      true,
		expectedPartitions: []string{"us-east1", "us-east2"},
	})
}

func TestAddRegionFailWithConcurrentBackupOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testRegionAddDropWithConcurrentBackupOps(t, struct {
		cmd                string
		shouldSucceed      bool
		expectedPartitions []string
	}{
		cmd:                `ALTER DATABASE db ADD REGION "us-east4"`,
		shouldSucceed:      false,
		expectedPartitions: []string{"us-east1", "us-east2", "us-east3"},
	})
}

func TestAddRegionSucceedWithConcurrentBackupOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testRegionAddDropWithConcurrentBackupOps(t, struct {
		cmd                string
		shouldSucceed      bool
		expectedPartitions []string
	}{
		cmd:                `ALTER DATABASE db ADD REGION "us-east4"`,
		shouldSucceed:      true,
		expectedPartitions: []string{"us-east1", "us-east2", "us-east3", "us-east4"},
	})
}

// testRegionAddDropWithConcurrentBackupOps tests adding/dropping regions
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
func testRegionAddDropWithConcurrentBackupOps(
	t *testing.T,
	regionAlterCmd struct {
		cmd                string
		shouldSucceed      bool
		expectedPartitions []string
	},
) {
	skip.UnderRace(t, "times out under race")
	skip.UnderDeadlock(t)

	testCases := []struct {
		name      string
		backupOp  string
		restoreOp string
	}{
		{
			name:      "backup-database",
			backupOp:  `BACKUP DATABASE db INTO 'nodelocal://1/db_backup'`,
			restoreOp: `RESTORE DATABASE db FROM LATEST IN 'nodelocal://1/db_backup'`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var mu syncutil.Mutex
			typeChangeStarted := make(chan struct{})
			typeChangeFinished := make(chan struct{})
			backupOpFinished := make(chan struct{})
			waitInTypeSchemaChangerDuringBackup := true

			backupKnobs := base.TestingKnobs{
				SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
					RunBeforeEnumMemberPromotion: func(ctx context.Context) error {
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

			go func(cmd string) {
				defer func() {
					close(typeChangeFinished)
				}()
				_, err := sqlDBBackup.Exec(cmd)
				if err != nil {
					t.Errorf("expected success, got %v when executing %s", err, cmd)
				}
			}(regionAlterCmd.cmd)

			<-typeChangeStarted

			_, err = sqlDBBackup.Exec(tc.backupOp)
			close(backupOpFinished)
			require.NoError(t, err)

			<-typeChangeFinished

			restoreKnobs := base.TestingKnobs{
				SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
					RunBeforeEnumMemberPromotion: func(context.Context) error {
						mu.Lock()
						defer mu.Unlock()
						if !regionAlterCmd.shouldSucceed {
							// Trigger a roll-back.
							return jobs.MarkAsPermanentJobError(errors.New("nope"))
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
				defer func() {
					require.NoError(t, rowsRegions.Close())
				}()
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
					[]string{"rbr@rbr_pkey"},
				)
			})
		})
	}
}
