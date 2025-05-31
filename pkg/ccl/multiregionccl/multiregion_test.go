// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
