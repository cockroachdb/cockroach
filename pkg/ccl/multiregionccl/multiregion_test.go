// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMultiRegionNoLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingDisableEnterprise()()

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{},
	)
	defer cleanup()

	_, err := sqlDB.Exec(`CREATE DATABASE test`)
	require.NoError(t, err)

	for _, errorStmt := range []string{
		`CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`,
		`ALTER DATABASE test PRIMARY REGION "us-east2"`,
	} {
		t.Run(errorStmt, func(t *testing.T) {
			_, err := sqlDB.Exec(errorStmt)
			require.NoError(t, err)
		})
	}
}

func TestMultiRegionAfterEnterpriseDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingEnableEnterprise()()

	skip.UnderRace(t, "times out under race")

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{},
	)
	defer cleanup()

	for _, setupQuery := range []string{
		`SET create_table_with_schema_locked=false;`,
		`CREATE DATABASE test PRIMARY REGION "us-east1" REGIONS "us-east2"`,
		`USE test`,
		`CREATE TABLE t1 () LOCALITY GLOBAL`,
		`CREATE TABLE t2 () LOCALITY REGIONAL BY TABLE`,
		`CREATE TABLE t3 () LOCALITY REGIONAL BY TABLE IN "us-east2"`,
		`CREATE TABLE t4 () LOCALITY REGIONAL BY ROW`,
	} {
		t.Run(setupQuery, func(t *testing.T) {
			_, err := sqlDB.Exec(setupQuery)
			require.NoError(t, err)
		})
	}

	defer ccl.TestingDisableEnterprise()()

	// Test certain commands are still supported with enterprise disabled
	t.Run("no new multi-region items", func(t *testing.T) {
		for _, tc := range []struct {
			stmt string
		}{
			{
				stmt: `CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`,
			},
			{
				stmt: `ALTER DATABASE db ADD REGION "us-east3"`,
			},
		} {
			t.Run(tc.stmt, func(t *testing.T) {
				_, err := sqlDB.Exec(tc.stmt)
				require.NoError(t, err)
			})
		}
	})

	// Test we can still drop multi-region functionality.
	t.Run("drop multi-region", func(t *testing.T) {
		for _, tc := range []struct {
			stmt string
		}{
			{stmt: `ALTER DATABASE test PRIMARY REGION "us-east2"`},
			{stmt: `ALTER TABLE t2 SET LOCALITY REGIONAL BY TABLE`},
			{stmt: `ALTER TABLE t3 SET LOCALITY REGIONAL BY TABLE`},
			{stmt: `ALTER TABLE t4 SET LOCALITY REGIONAL BY TABLE`},
			{stmt: `ALTER TABLE t4 DROP COLUMN crdb_region`},
			{stmt: `ALTER DATABASE test DROP REGION "us-east1"`},
			{stmt: `ALTER DATABASE test DROP REGION "us-east2"`},
		} {
			t.Run(tc.stmt, func(t *testing.T) {
				_, err := sqlDB.Exec(tc.stmt)
				require.NoError(t, err)
			})
		}
	})
}

func TestGlobalReadsAfterEnterpriseDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingEnableEnterprise()()

	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 1 /* numServers */, base.TestingKnobs{},
	)
	defer cleanup()

	for _, setupQuery := range []string{
		`CREATE DATABASE test`,
		`USE test`,
		`CREATE TABLE t1 ()`,
		`CREATE TABLE t2 ()`,
	} {
		_, err := sqlDB.Exec(setupQuery)
		require.NoError(t, err)
	}

	// Can set global_reads with enterprise license enabled.
	_, err := sqlDB.Exec(`ALTER TABLE t1 CONFIGURE ZONE USING global_reads = true`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`ALTER TABLE t2 CONFIGURE ZONE USING global_reads = true`)
	require.NoError(t, err)

	// Can unset global_reads with enterprise license enabled.
	_, err = sqlDB.Exec(`ALTER TABLE t1 CONFIGURE ZONE USING global_reads = false`)
	require.NoError(t, err)

	defer ccl.TestingDisableEnterprise()()

	// Can set global_reads with enterprise license disabled.
	_, err = sqlDB.Exec(`ALTER TABLE t1 CONFIGURE ZONE USING global_reads = true`)
	require.NoError(t, err)

	// Can unset global_reads with enterprise license disabled.
	_, err = sqlDB.Exec(`ALTER TABLE t2 CONFIGURE ZONE USING global_reads = false`)
	require.NoError(t, err)
}

// This test verifies closed timestamp policy behavior with and without the
// auto-tuning. With the auto-tuning enabled, we expect latency-based policies
// on global tables. Without it, no latency-based policies should exist.
func TestReplicaClosedTSPolicyWithPolicyRefresher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	// Helper function to check if a policy is a newly introduced latency-based policy.
	isLatencyBasedPolicy := func(policy ctpb.RangeClosedTimestampPolicy) bool {
		return policy >= ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS &&
			policy <= ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS
	}

	// Set small intervals for faster testing.
	closedts.LeadForGlobalReadsAutoTuneEnabled.Override(ctx, &st.SV, true)
	closedts.RangeClosedTimestampPolicyRefreshInterval.Override(ctx, &st.SV, 5*time.Millisecond)
	closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.Override(ctx, &st.SV, 5*time.Millisecond)

	// Create a multi-region cluster with manual replication.
	numServers := 3
	tc, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, numServers, base.TestingKnobs{}, multiregionccltestutils.WithReplicationMode(base.ReplicationManual),
		multiregionccltestutils.WithSettings(st),
	)
	defer cleanup()

	// Create a multi-region database and global table.
	_, err := sqlDB.Exec(`CREATE DATABASE t PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3"`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE TABLE t.test_table (k INT PRIMARY KEY) LOCALITY GLOBAL`)
	require.NoError(t, err)

	// Look up the table ID and get its key prefix.
	var tableID uint32
	err = sqlDB.QueryRow(`SELECT id from system.namespace WHERE name='test_table'`).Scan(&tableID)
	require.NoError(t, err)
	tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))
	// Split the range at the table prefix and replicate it across all nodes.
	tc.SplitRangeOrFatal(t, tablePrefix.AsRawKey())
	tc.AddVotersOrFatal(t, tablePrefix.AsRawKey(), tc.Target(1), tc.Target(2))

	// Get the store and replica for testing.
	store := tc.GetFirstStoreFromServer(t, 0)
	replica := store.LookupReplica(roachpb.RKey(tablePrefix.AsRawKey()))
	require.NotNil(t, replica)

	// Wait for the closed timestamp policies to stabilize and verify the
	// expected behavior.
	testutils.SucceedsSoon(t, func() error {
		snapshot := store.GetStoreConfig().ClosedTimestampSender.GetSnapshot()
		if len(snapshot.ClosedTimestamps) != int(ctpb.MAX_CLOSED_TIMESTAMP_POLICY) {
			return errors.Errorf("expected %d closed timestamps, got %d",
				ctpb.MAX_CLOSED_TIMESTAMP_POLICY, len(snapshot.ClosedTimestamps))
		}

		// Ensure there are ranges to check.
		if len(snapshot.AddedOrUpdated) == 0 {
			return errors.Errorf("no ranges")
		}

		// With policy refresher enabled: expect to find latency-based policy.
		for _, policy := range snapshot.AddedOrUpdated {
			if policy.RangeID != replica.GetRangeID() {
				continue
			}
			if isLatencyBasedPolicy(policy.Policy) {
				return nil
			}
		}
		return errors.Errorf("no ranges with latency based policy")
	})

	// Without policy refresher: expect to NOT find latency-based policy.
	_, err = sqlDB.Exec(`SET CLUSTER SETTING kv.closed_timestamp.lead_for_global_reads_auto_tune.enabled = false`)
	require.NoError(t, err)

	// Wait for policy refresher to disable and verify no latency-based policies exist.
	testutils.SucceedsSoon(t, func() error {
		snapshot := store.GetStoreConfig().ClosedTimestampSender.GetSnapshot()
		if len(snapshot.ClosedTimestamps) != int(ctpb.MAX_CLOSED_TIMESTAMP_POLICY) {
			return errors.Errorf("expected %d closed timestamps, got %d",
				ctpb.MAX_CLOSED_TIMESTAMP_POLICY, len(snapshot.ClosedTimestamps))
		}

		// Ensure there are ranges to check.
		if len(snapshot.AddedOrUpdated) == 0 {
			return errors.Errorf("no ranges")
		}

		// Verify no latency-based policies remain.
		for _, policy := range snapshot.AddedOrUpdated {
			if isLatencyBasedPolicy(policy.Policy) {
				return errors.Errorf("range %d has latency based policy %s", policy.RangeID, policy.Policy)
			}
		}
		return nil
	})
}

// TestSuperRegionConstraintConformance is a regression test for #108127. It
// verifies that REGIONAL BY ROW and REGIONAL BY TABLE tables in a database
// with super regions and SURVIVE REGION FAILURE eventually reach full
// constraint conformance.
func TestSuperRegionConstraintConformance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)
	skip.UnderDuress(t)

	// The topology uses 4 regions across 8 nodes (2 per region):
	//
	//   - Super region "americas": {us-east-1, us-central-1, us-west-1} — a
	//     non-primary super region, exercising the AddConstraintsForSuperRegion
	//     code path where the primary region lives outside the super region.
	//   - Primary region: {ap-southeast-1} — outside any super region, which
	//     also ensures RBR partitions and RBT tables homing here get normal
	//     inherited constraints rather than super-region-confined ones.
	//
	// With exactly 3 regions per super region + SURVIVE REGION FAILURE, we get
	// 5 voters and 0 non-voters — the tightest possible constraint configuration
	// where every replica is doubly constrained. This is the specific scenario
	// that triggered the original bug.
	regionNames := []string{
		"ap-southeast-1",                         // primary (standalone)
		"us-east-1", "us-central-1", "us-west-1", // super region "americas"
	}
	tc, sqlDB, cleanup :=
		multiregionccltestutils.TestingCreateMultiRegionClusterWithRegionList(
			t,
			regionNames,
			2, /* serversPerRegion */
			base.TestingKnobs{},
		)
	defer cleanup()

	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Speed up replication reporting and closed timestamps.
	tdb.Exec(t, "SET CLUSTER SETTING kv.replication_reports.interval = '1ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '10ms'")

	// Setup MR database with all 4 regions.
	tdb.Exec(t, `CREATE DATABASE testdb PRIMARY REGION "ap-southeast-1"
		REGIONS "us-east-1", "us-central-1", "us-west-1"`)
	tdb.Exec(t, `SET enable_super_regions = 'on'`)
	tdb.Exec(t, `ALTER DATABASE testdb ADD SUPER REGION "americas"
		VALUES "us-east-1", "us-central-1", "us-west-1"`)
	tdb.Exec(t, `ALTER DATABASE testdb SURVIVE REGION FAILURE`)

	// Create a REGIONAL BY ROW table with rows in all 4 regions, exercising
	// every partition's subzone config — both super-region-confined and normal.
	tdb.Exec(t,
		`CREATE TABLE testdb.rbr(k INT PRIMARY KEY, v INT)
		 LOCALITY REGIONAL BY ROW`)
	tdb.Exec(t, `INSERT INTO testdb.rbr(crdb_region, k, v) VALUES
		('us-east-1', 1, 10), ('us-central-1', 2, 20), ('us-west-1', 3, 30),
		('ap-southeast-1', 4, 40)`)

	// RBT in a super region (non-primary). Tests AddConstraintsForSuperRegion
	// at the table level.
	tdb.Exec(t,
		`CREATE TABLE testdb.rbt_sr(k INT PRIMARY KEY, v INT)
		 LOCALITY REGIONAL BY TABLE IN "us-east-1"`)
	tdb.Exec(t, `INSERT INTO testdb.rbt_sr(k, v) VALUES (1, 10)`)

	// RBT in the primary region (outside any super region). Tests that
	// table-level constraints work correctly without super region confinement.
	tdb.Exec(t,
		`CREATE TABLE testdb.rbt_standalone(k INT PRIMARY KEY, v INT)
		 LOCALITY REGIONAL BY TABLE IN "ap-southeast-1"`)
	tdb.Exec(t, `INSERT INTO testdb.rbt_standalone(k, v) VALUES (1, 10)`)

	// Look up zone_ids for all 3 tables.
	var rbrZoneID, rbtSRZoneID, rbtStandaloneZoneID int
	tdb.QueryRow(t, "SELECT zone_id FROM crdb_internal.zones WHERE table_name = 'rbr'").Scan(&rbrZoneID)
	tdb.QueryRow(t, "SELECT zone_id FROM crdb_internal.zones WHERE table_name = 'rbt_sr'").Scan(&rbtSRZoneID)
	tdb.QueryRow(t, "SELECT zone_id FROM crdb_internal.zones WHERE table_name = 'rbt_standalone'").Scan(&rbtStandaloneZoneID)
	zoneIDs := []int{rbrZoneID, rbtSRZoneID, rbtStandaloneZoneID}

	// Verify that zone configs themselves have the expected constraints, so
	// the conformance check below cannot pass vacuously due to misconfigured
	// zones.
	verifyZoneConfig := func(query string, expectedFragments []string) {
		t.Helper()
		var target, rawSQL string
		tdb.QueryRow(t, query).Scan(&target, &rawSQL)
		for _, frag := range expectedFragments {
			require.Contains(t, rawSQL, frag,
				"zone config for %s missing expected fragment", target)
		}
	}
	// RBT in super region: all replicas confined to americas super region.
	verifyZoneConfig(
		`SHOW ZONE CONFIGURATION FOR TABLE testdb.rbt_sr`,
		[]string{
			"voter_constraints", "us-east-1",
			"constraints", "us-central-1", "us-west-1",
		},
	)
	// RBT in primary region (standalone): voters in ap-southeast-1, no super
	// region confinement.
	verifyZoneConfig(
		`SHOW ZONE CONFIGURATION FOR TABLE testdb.rbt_standalone`,
		[]string{"voter_constraints", "ap-southeast-1"},
	)
	// RBR partitions inside the super region.
	for _, region := range []string{"us-east-1", "us-central-1", "us-west-1"} {
		verifyZoneConfig(
			fmt.Sprintf(
				`SHOW ZONE CONFIGURATION FOR PARTITION "%s" OF TABLE testdb.rbr`,
				region),
			[]string{
				"voter_constraints", region,
				"constraints", "us-east-1", "us-central-1", "us-west-1",
			},
		)
	}
	// RBR partition outside any super region.
	verifyZoneConfig(
		`SHOW ZONE CONFIGURATION FOR PARTITION "ap-southeast-1" OF TABLE testdb.rbr`,
		[]string{"voter_constraints", "ap-southeast-1"},
	)

	forceProcess := func() error {
		for i := 0; i < tc.NumServers(); i++ {
			if err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(
				func(s *kvserver.Store) error {
					return s.ForceReplicationScanAndProcess()
				},
			); err != nil {
				return err
			}
		}
		return nil
	}

	// First, wait for the constraint stats report to have been generated for
	// all zones. Without this gate the subsequent zero-violations check could
	// pass vacuously before the report has ever run.
	testutils.SucceedsSoon(t, func() error {
		if err := forceProcess(); err != nil {
			return err
		}
		for _, zid := range zoneIDs {
			var count int
			if err := sqlDB.QueryRow(
				`SELECT count(*) FROM system.replication_constraint_stats
				  WHERE zone_id = $1`, zid,
			).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				return fmt.Errorf(
					"constraint stats report not yet generated for zone %d", zid)
			}
		}
		return nil
	})

	// Now verify that every reported constraint has zero violating ranges for
	// all locality types.
	testutils.SucceedsSoon(t, func() error {
		if err := forceProcess(); err != nil {
			return err
		}
		checkZone := func(zid int) error {
			rows, err := sqlDB.Query(
				`SELECT type, violating_ranges
				   FROM system.replication_constraint_stats
				  WHERE zone_id = $1 AND violating_ranges > 0`,
				zid,
			)
			if err != nil {
				return err
			}
			defer rows.Close()

			var violations []string
			for rows.Next() {
				var cType string
				var count int
				if err := rows.Scan(&cType, &count); err != nil {
					return err
				}
				violations = append(violations,
					fmt.Sprintf("%s: %d violating ranges", cType, count))
			}
			if err := rows.Err(); err != nil {
				return err
			}
			if len(violations) > 0 {
				return fmt.Errorf(
					"constraint violations still present for zone %d: %v",
					zid, violations)
			}
			return nil
		}
		for _, zid := range zoneIDs {
			if err := checkZone(zid); err != nil {
				return err
			}
		}
		return nil
	})
}
